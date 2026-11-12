namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// S — snapshot-accelerated recovery (counting trigger, opt-in).
//
// Events live in the store, never in the heap, so recovery replays them all:
// the measured worst case is 11,308 events / 24MB (and another aggregate at
// 79,772 events). A snapshot is ACCELERATION ONLY: the event stream is never
// truncated, at most one snapshot per actor is kept, and nothing happens unless
// the host opts in with ActorSystemOptions.SnapshotEveryNEvents.
// ═══════════════════════════════════════════════════════════

internal sealed record SSnapshotCreate(int Initial) : ICommand;

internal sealed record SSnapshotAdd(int Delta) : ICommand;

internal sealed record SSnapshotGet : ICommand;

internal sealed record SSnapshotAdded(int Delta) : IDomainEvent;

/// <summary>
/// Counter aggregate that can snapshot itself. The payload is a magic byte plus the
/// value, so a CORRUPT payload is detectable inside <see cref="RestoreSnapshot"/> —
/// exactly the shape a real serializer's version mismatch produces.
/// <para>
/// <see cref="MutateCalls"/> measures replay work: snapshot-accelerated recovery must
/// call Mutate only for the events AFTER the snapshot version.
/// </para>
/// </summary>
internal sealed class SSnapshotCounter : EventSourcedActor
{
    private const byte Magic = 0x5A;

    private int _value;

    public SSnapshotCounter(SSnapshotCreate cmd)
        : base(cmd) { }

    public SSnapshotCounter() { }

    public int Value => _value;

    /// <summary>Number of Mutate calls on THIS instance (live and replay alike).</summary>
    public int MutateCalls { get; private set; }

    /// <summary>Value of <see cref="EventSourcedActor.IsReplaying"/> at each Mutate call.</summary>
    public List<bool> ReplayingDuringMutate { get; } = new();

    /// <summary>When false, <see cref="TakeSnapshot"/> reports "this type does not snapshot".</summary>
    public bool SnapshotsEnabled { get; set; } = true;

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case SSnapshotCreate c:
                RaiseEvent(new SSnapshotAdded(c.Initial));
                return default;
            case SSnapshotAdd a:
                RaiseEvent(new SSnapshotAdded(a.Delta));
                return default;
            case SSnapshotGet:
                return new ValueTask<object?>(_value);
            default:
                return default;
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        MutateCalls++;
        ReplayingDuringMutate.Add(IsReplaying);

        if (@event is SSnapshotAdded e)
            _value += e.Delta;
    }

    protected override byte[]? TakeSnapshot()
    {
        if (!SnapshotsEnabled)
            return null;

        var payload = new byte[5];
        payload[0] = Magic;
        BitConverter.TryWriteBytes(payload.AsSpan(1), _value);
        return payload;
    }

    protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload)
    {
        if (payload.Length != 5 || payload[0] != Magic)
            throw new InvalidOperationException("corrupt snapshot payload");

        _value = BitConverter.ToInt32(payload[1..]);
    }
}

public sealed class FixSSnapshotTests
{
    private const int CreationEvents = 1;

    /// <summary>
    /// Builds a system with the snapshot trigger configured and a counter carrying
    /// <paramref name="adds"/> increments on top of its creation event — i.e.
    /// <c>1 + adds</c> persisted events.
    /// </summary>
    private static async Task<(
        ActorSystem System,
        InMemoryEventStore Store,
        SSnapshotCounter Actor
    )> BuildCounterAsync(
        int? everyNEvents,
        int adds,
        bool snapshotsEnabled = true,
        bool fireAndForget = false
    )
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = everyNEvents }
        );
        system.Register<SSnapshotCounter>(
            cmd => new SSnapshotCounter((SSnapshotCreate)cmd)
            {
                SnapshotsEnabled = snapshotsEnabled,
            },
            () => new SSnapshotCounter { SnapshotsEnabled = snapshotsEnabled }
        );

        var actor = await system.CreateAsync<SSnapshotCounter>(new SSnapshotCreate(0));
        for (var i = 0; i < adds; i++)
        {
            if (fireAndForget)
                system.Send(actor.Id, new SSnapshotAdd(1));
            else
                await system.AskAsync<object?>(actor.Id, new SSnapshotAdd(1));
        }

        // The mailbox is FIFO and the loop dispatches one envelope at a time, so this ask
        // completes only after every queued command above was persisted — one flush per
        // command, which is what makes the counted trigger deterministic.
        if (fireAndForget)
            await system.AskAsync<int>(actor.Id, new SSnapshotGet());

        return (system, store, actor);
    }

    /// <summary>
    /// 1. Threshold counting: 12 persisted events with a 5-event threshold produce exactly
    /// one snapshot, whose version is the last multiple of the threshold (10).
    /// </summary>
    [Test]
    public async Task Every5Events_After12Events_KeepsSingleSnapshotAtVersion10()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);

        var snapshot = await store.LoadSnapshotAsync(actor.Id);

        await Assert.That(snapshot).IsNotNull();
        await Assert.That(snapshot!.Version).IsEqualTo(10UL);
        await Assert.That(snapshot.Payload.Length).IsGreaterThan(0);
        await Assert.That(actor.Version).IsEqualTo(12UL);

        await system.StopAsync(actor.Id);
    }

    /// <summary>
    /// 2. Recovery replays only the tail: the rebuilt instance runs Mutate
    /// <c>events.Count - snapshot.Version</c> times and still reaches the full state.
    /// </summary>
    [Test]
    public async Task Rebuild_ReplaysOnlyEventsAfterSnapshot_AndReachesSameState()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);
        var id = actor.Id;
        var snapshotVersion = (await store.LoadSnapshotAsync(id))!.Version;
        var total = (await store.LoadAsync(id)).Count;
        await system.StopAsync(id);

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(total - (int)snapshotVersion);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(2);
        await Assert.That(rebuilt.Value).IsEqualTo(11);
        await Assert.That(rebuilt.Version).IsEqualTo((ulong)total);

        await system.StopAsync(id);
    }

    /// <summary>
    /// 3. A snapshot the aggregate cannot deserialize must never break recovery: the
    /// warning path falls back to a full replay, and the actor stays usable afterwards.
    /// </summary>
    [Test]
    public async Task CorruptSnapshot_FallsBackToFullReplay_AndActorStaysUsable()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);
        var id = actor.Id;
        var total = (await store.LoadAsync(id)).Count;
        await system.StopAsync(id);

        // Same version a good snapshot would carry, undecodable payload.
        await store.SaveSnapshotAsync(id, new ActorSnapshot(10UL, [0xFF, 0x00, 0x01]));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(total);
        await Assert.That(rebuilt.Value).IsEqualTo(11);

        // Usable: the rebuilt instance keeps persisting on top of the untouched stream.
        await Assert.That(await system.AskAsync<object?>(id, new SSnapshotAdd(1))).IsNull();
        await Assert.That(await system.AskAsync<int>(id, new SSnapshotGet())).IsEqualTo(12);
        await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(total + 1);

        await system.StopAsync(id);
    }

    /// <summary>
    /// 4. A snapshot that claims more events than the stream holds is ignored
    /// (truncated / rolled-back stream): full replay, and the bogus state is never applied.
    /// </summary>
    [Test]
    public async Task SnapshotVersionBeyondEventCount_IsIgnored()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);
        var id = actor.Id;
        var total = (await store.LoadAsync(id)).Count;
        await system.StopAsync(id);

        // Well-formed payload, impossible version.
        var payload = new byte[5];
        payload[0] = 0x5A;
        BitConverter.TryWriteBytes(payload.AsSpan(1), 999);
        await store.SaveSnapshotAsync(id, new ActorSnapshot((ulong)total + 1, payload));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(total);
        await Assert.That(rebuilt.Value).IsEqualTo(11);

        await system.StopAsync(id);
    }

    /// <summary>
    /// 5. Opt-in per type: TakeSnapshot() returning null means "no snapshotting", and the
    /// behavior is exactly the pre-snapshot one (every event replayed, nothing stored).
    /// </summary>
    [Test]
    public async Task TakeSnapshotReturnsNull_WritesNoSnapshot_AndReplaysEverything()
    {
        var (system, store, actor) = await BuildCounterAsync(
            everyNEvents: 3,
            adds: 11,
            snapshotsEnabled: false
        );
        var id = actor.Id;
        var total = (await store.LoadAsync(id)).Count;
        await system.StopAsync(id);

        await Assert.That(await store.LoadSnapshotAsync(id)).IsNull();

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(total);
        await Assert.That(rebuilt.Value).IsEqualTo(11);

        await system.StopAsync(id);
    }

    /// <summary>
    /// 6. Acceleration only: snapshotting NEVER truncates history — the store still
    /// returns every event, in order.
    /// </summary>
    [Test]
    public async Task Snapshotting_DoesNotTruncateTheEventStream()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);
        var id = actor.Id;
        await system.StopAsync(id);

        var events = await store.LoadAsync(id);

        await Assert.That(events.Count).IsEqualTo(12);
        await Assert.That(events.Count).IsEqualTo(CreationEvents + 11);
        foreach (var e in events)
            await Assert.That(e).IsTypeOf<SSnapshotAdded>();
        await Assert.That(await store.PeekFirstAsync(id)).IsNotNull();
    }

    /// <summary>
    /// 7. IsReplaying is visible to subclasses and honest: false for live dispatch, true
    /// for everything replayed during recovery.
    /// </summary>
    [Test]
    public async Task IsReplaying_IsTrueDuringRecovery_AndFalseForLiveDispatch()
    {
        var (system, store, actor) = await BuildCounterAsync(everyNEvents: 5, adds: 11);
        var id = actor.Id;

        await Assert.That(actor.MutateCalls).IsEqualTo(12);
        foreach (var replaying in actor.ReplayingDuringMutate)
            await Assert.That(replaying).IsFalse();

        var snapshotVersion = (await store.LoadSnapshotAsync(id))!.Version;
        await system.StopAsync(id);

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12 - (int)snapshotVersion);
        foreach (var replaying in rebuilt.ReplayingDuringMutate)
            await Assert.That(replaying).IsTrue();

        // Live dispatch on the rebuilt instance is NOT a replay.
        var liveCalls = rebuilt.MutateCalls;
        await system.AskAsync<object?>(id, new SSnapshotAdd(1));
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(liveCalls + 1);
        await Assert.That(rebuilt.ReplayingDuringMutate[^1]).IsFalse();

        await system.StopAsync(id);
    }

    /// <summary>
    /// 8. Realistic size: 5,001 persisted events with a 1,000-event threshold leave one
    /// snapshot at the very end of the stream, so a rebuild replays exactly ONE event instead
    /// of 5,001. Production streams are 11k-79k events; this is the shape that has to shrink.
    /// </summary>
    [Test]
    public async Task LargeStream_RebuildReplaysOnlyTheEventsAfterTheLastSnapshot()
    {
        const int total = 5_001; // 1 creation event + 5,000 increments
        const int threshold = 1_000;

        var (system, store, actor) = await BuildCounterAsync(
            everyNEvents: threshold,
            adds: total - 1,
            fireAndForget: true
        );
        var id = actor.Id;

        await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(total);
        await Assert.That(actor.Version).IsEqualTo((ulong)total);

        var snapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(snapshot).IsNotNull();
        await Assert.That(snapshot!.Version).IsEqualTo(5_000UL);

        await system.StopAsync(id);
        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(total - 5_000);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(1);
        await Assert.That(rebuilt.Value).IsEqualTo(5_000);

        await system.StopAsync(id);
    }

    /// <summary>
    /// 9. Configuration is validated at the option setter: a threshold below 1 can only
    /// mean a host bug, so it fails at configuration time instead of silently snapshotting
    /// on every event (or never).
    /// </summary>
    [Test]
    public async Task SnapshotEveryNEvents_NonPositive_Throws()
    {
        var store = new InMemoryEventStore();

        await Assert
            .That(() => new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = 0 })
            .Throws<ArgumentOutOfRangeException>();
        await Assert
            .That(() => new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = -5 })
            .Throws<ArgumentOutOfRangeException>();
        await Assert
            .That(() => new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = 1 })
            .ThrowsNothing();
    }
}
