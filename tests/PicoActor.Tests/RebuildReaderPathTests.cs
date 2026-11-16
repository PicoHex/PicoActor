using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>
/// A store whose fallback read members (<see cref="IEventStore.LoadAsync"/>,
/// <see cref="IEventStore.PeekFirstAsync"/>) FAIL while its cursor read port works. Every test
/// built on it therefore proves the runtime reached the reader: nothing here can pass through the
/// old <c>LoadAsync</c> path by accident.
/// <para>
/// Seeding still works — the create/append path, the id enumeration and snapshot
/// save/load all delegate to the inner in-memory store; only the two fallback READS throw.
/// </para>
/// </summary>
internal class ReaderOnlyStore
    : IEventStore,
        IEventStoreEnumerator,
        IEventLogReader,
        IEventStoreSnapshot
{
    private readonly InMemoryEventStore _inner = new();
    private int _opens;

    /// <summary>
    /// <see cref="IEventCursor.End"/> the FIRST open reports (null = the stream's real length).
    /// A smaller value stands in for a stream that was rolled back under the rebuild.
    /// </summary>
    public ulong? EndOverride { get; set; }

    /// <summary>How many events the FIRST open may yield (null = every event).</summary>
    public int? YieldLimit { get; set; }

    /// <summary>Apply the knobs to EVERY open: a stream that is gone for good.</summary>
    public bool KnobsOnEveryOpen { get; set; }

    /// <summary>Versions the first open yielded, in order (the tail-only cost probe).</summary>
    public List<ulong> FirstOpenVersions { get; } = new();

    public int Opens => Volatile.Read(ref _opens);

    private IEventLogReader InnerReader => _inner;

    // ── IEventStore: write path delegates, read path refuses ────────────────
    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => _inner.AppendAsync(actorId, expectedVersion, events);

    /// <summary>The parameterless read is the RUNTIME FALLBACK: a reader store must never use it.</summary>
    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
        throw new NotSupportedException("reader-only probe");

    /// <summary>The fallback first-event read: recovery enumeration must use the reader instead.</summary>
    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
        throw new NotSupportedException("reader-only probe");

    // ── IEventStoreEnumerator ──────────────────────────────────────────────
    public ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType) =>
        _inner.ListAggregateIdsAsync(firstEventType);

    // ── IEventStoreSnapshot ────────────────────────────────────────────────
    public ValueTask<ActorSnapshot?> LoadSnapshotAsync(
        Guid actorId,
        CancellationToken ct = default
    ) => _inner.LoadSnapshotAsync(actorId, ct);

    public ValueTask SaveSnapshotAsync(
        Guid actorId,
        ActorSnapshot snapshot,
        CancellationToken ct = default
    ) => _inner.SaveSnapshotAsync(actorId, snapshot, ct);

    // ── IEventLogReader ────────────────────────────────────────────────────
    public ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default) =>
        _inner.GetVersionAsync(actorId, ct);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId, CancellationToken ct = default) =>
        InnerReader.PeekFirstAsync(actorId, ct);

    public async ValueTask<IEventCursor> OpenAsync(
        Guid actorId,
        ulong fromVersion,
        CancellationToken ct = default
    )
    {
        var inner = await _inner.OpenAsync(actorId, fromVersion, ct).ConfigureAwait(false);

        // The knobs are per-CALL state (a stream that shrinks/vanishes between the first open and
        // the full-replay re-open), so they only touch the opens this probe opted into.
        var first = Interlocked.Increment(ref _opens) == 1;
        var knobbed = first || KnobsOnEveryOpen;

        var end = knobbed && EndOverride is { } reportedEnd ? reportedEnd : inner.End;
        var yieldLimit = knobbed && YieldLimit is { } limit ? limit : int.MaxValue;

        return new ProbeCursor(inner, end, yieldLimit, first ? FirstOpenVersions : null);
    }
}

/// <summary>
/// A stream that is GONE: every open reports <c>End = 6</c> (a real length) and delivers nothing.
/// The full-replay re-open must therefore also come up empty, without constructing an actor.
/// </summary>
internal sealed class VanishingStore : ReaderOnlyStore
{
    public VanishingStore()
    {
        KnobsOnEveryOpen = true;
        EndOverride = 6;
        YieldLimit = 0;
    }
}

/// <summary>
/// Wraps a cursor and applies the probe knobs: a reported <see cref="End"/> that need not match
/// the stream (rollback / shrink) and a yield budget (a stream that stops delivering).
/// </summary>
internal sealed class ProbeCursor(
    IEventCursor inner,
    ulong end,
    int yieldLimit,
    List<ulong>? versions
) : IEventCursor
{
    private int _left = yieldLimit;

    public ulong End => end;

    public IDomainEvent Current => inner.Current;

    public ulong CurrentVersion => inner.CurrentVersion;

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_left <= 0)
            return false;

        if (!await inner.MoveNextAsync(ct).ConfigureAwait(false))
            return false;

        _left--;
        versions?.Add(inner.CurrentVersion);
        return true;
    }

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

/// <summary>
/// The runtime PREFERS the cursor read port: a rebuild must go through
/// <see cref="IEventLogReader.OpenAsync"/> on a reader store (never the <c>LoadAsync</c>
/// fallback), keep the rollback/shrink guards non-throwing, and materialize only the tail a
/// snapshot does not already cover.
/// </summary>
public sealed class RebuildReaderPathTests
{
    private static ActorSystem NewSystem(IEventStore store) =>
        new(new ActorSystemOptions { EventStore = store });

    private static void Register(ActorSystem system, Func<SSnapshotCounter>? rebuild = null)
    {
        system.Register<SSnapshotCounter>(
            cmd => new SSnapshotCounter((SSnapshotCreate)cmd),
            rebuild ?? (() => new SSnapshotCounter())
        );
    }

    /// <summary>
    /// Seed a then-STOPPED aggregate with exactly <paramref name="events"/> events, each worth +1
    /// (so the aggregate value equals the event count — the numbers the assertions use).
    /// </summary>
    private static async Task<Guid> SeedStoppedAsync(ActorSystem system, int events)
    {
        var actor = await system.CreateAsync<SSnapshotCounter>(new SSnapshotCreate(1));
        for (var i = 1; i < events; i++)
            await system.AskAsync<object?>(actor.Id, new SSnapshotAdd(1));
        await system.StopAsync(actor.Id);
        return actor.Id;
    }

    /// <summary>The aggregate's own snapshot format: magic 0x5A followed by the value.</summary>
    private static ActorSnapshot Snapshot(ulong version, int value)
    {
        var payload = new byte[5];
        payload[0] = 0x5A;
        BitConverter.GetBytes(value).CopyTo(payload, 1);
        return new ActorSnapshot(version, payload);
    }

    [Test, Timeout(15000)]
    public async Task ReaderOnlyStore_RebuildsWithoutLoadAsync(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new ReaderOnlyStore();
        var system = NewSystem(store);
        Register(system);
        var id = await SeedStoppedAsync(system, events: 4);

        // LoadAsync throws on this store: the rebuild can only succeed through the cursor.
        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(4);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(4);
        await Assert.That(rebuilt.Version).IsEqualTo(4UL);

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task ReaderOnlyStore_PeekFirst_UsesReader(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new ReaderOnlyStore();
        var system = NewSystem(store);
        Register(system);
        var id = await SeedStoppedAsync(system, events: 4);

        // Recovery enumeration peeks the first event: on this store only the READER can serve it.
        var found = await system.FindAggregateIds(nameof(SSnapshotAdded), _ => true);

        await Assert.That(found).Contains(id);
        // ... and the store's own fallback member stays the throwing one, i.e. it was not used.
        await Assert
            .That(async () => await store.PeekFirstAsync(id))
            .Throws<NotSupportedException>();

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task RollbackGuard_ReplaysFromOne_NoThrow(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new ReaderOnlyStore();
        var system = NewSystem(store);
        Register(system);
        var id = await SeedStoppedAsync(system, events: 3);

        // A snapshot from a stream that no longer exists: v5 for a 3-event stream. The rebuild
        // must DISCARD it and replay from version 1 — degrading, never throwing.
        await store.SaveSnapshotAsync(id, Snapshot(version: 5, value: 4));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(3);
        await Assert.That(rebuilt.Value).IsEqualTo(3);
        await Assert.That(rebuilt.Version).IsEqualTo(3UL);

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task ShrinkingStream_DegradesToFullReplay(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new ReaderOnlyStore();
        var system = NewSystem(store);
        Register(system);
        var id = await SeedStoppedAsync(system, events: 6);
        await store.SaveSnapshotAsync(id, Snapshot(version: 5, value: 5));

        // The first open reports the real length but the tail drain stops after zero events:
        // v + tail.Count != End is a mismatch, not an exception.
        store.YieldLimit = 0;

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(6);
        await Assert.That(rebuilt.Value).IsEqualTo(6);
        await Assert.That(rebuilt.Version).IsEqualTo(6UL);

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task VanishingStream_ReturnsNull_WithoutConstructing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new VanishingStore();
        var system = NewSystem(store);
        var factoryCalls = new int[1];
        Register(
            system,
            () =>
            {
                Interlocked.Increment(ref factoryCalls[0]);
                return new SSnapshotCounter();
            }
        );
        var id = await SeedStoppedAsync(system, events: 6);
        await store.SaveSnapshotAsync(id, Snapshot(version: 5, value: 5));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        // Both opens came up empty: no actor may be built for a stream that is gone.
        await Assert.That(rebuilt).IsNull();
        await Assert.That(factoryCalls[0]).IsEqualTo(0);
    }

    [Test, Timeout(15000)]
    public async Task SnapshotRebuild_DrainsOnlyTheTail_ThroughTheCursor(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new ReaderOnlyStore();
        var system = NewSystem(store);
        Register(system);
        var id = await SeedStoppedAsync(system, events: 100);
        await store.SaveSnapshotAsync(id, Snapshot(version: 90, value: 90));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(100);
        // The head (events 1..90) is never turned into domain events.
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(10);
        await Assert.That(store.FirstOpenVersions.Count).IsEqualTo(10);
        await Assert.That(store.FirstOpenVersions.Min()).IsEqualTo(91UL);

        await system.StopAsync(id);
    }
}
