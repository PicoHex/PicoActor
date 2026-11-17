namespace PicoActor.EventStore.Jsonl.Tests;

internal sealed record ProbeCreate(int Initial) : ICommand;

internal sealed record ProbeAdd(int Delta) : ICommand;

internal sealed record ProbeGet : ICommand;

/// <summary>
/// A runtime-driven counter over the JSONL store's own stream type: it uses the store's
/// <see cref="ConformanceEvent"/> history, so a snapshot the runtime asks for is written by
/// <see cref="JsonlEventStore{TEventBase}.SaveSnapshotAsync"/> and read back by
/// <see cref="JsonlEventStore{TEventBase}.LoadSnapshotAsync"/> — the two members that only exist
/// behind <see cref="IEventStoreSnapshot"/>.
/// </summary>
internal sealed class ProbeCounter : EventSourcedActor
{
    private const byte Magic = 0x51;

    private int _value;

    public ProbeCounter(ProbeCreate command)
        : base(command) { }

    public ProbeCounter() { }

    internal int Value => _value;

    /// <summary>Mutate calls on THIS instance — a rebuild that restored a snapshot pays only the tail.</summary>
    internal int MutateCalls { get; private set; }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case ProbeCreate create:
                RaiseEvent(new SSnapshotAdded(create.Initial));
                return default;
            case ProbeAdd add:
                RaiseEvent(new SSnapshotAdded(add.Delta));
                return default;
            case ProbeGet:
                return new ValueTask<object?>(_value);
            default:
                return default;
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        MutateCalls++;
        if (@event is SSnapshotAdded added)
            _value += added.Delta;
    }

    protected override byte[]? TakeSnapshot()
    {
        var payload = new byte[5];
        payload[0] = Magic;
        BitConverter.TryWriteBytes(payload.AsSpan(1), _value);
        return payload;
    }

    protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload)
    {
        _ = version;
        if (payload.Length != 5 || payload[0] != Magic)
            throw new InvalidOperationException("corrupt snapshot payload");

        _value = BitConverter.ToInt32(payload[1..]);
    }
}

/// <summary>
/// The SNAPSHOT FEATURE AT RUNTIME, over the durable store.
/// <para>
/// <c>ActorSystem</c> and <c>EventSourcedActor</c> reach snapshot persistence ONLY through
/// <c>is IEventStoreSnapshot</c>. A store that persists snapshots but does not DECLARE the
/// interface is dead code at runtime: the host sets <c>SnapshotEveryNEvents</c>, every flush
/// silently skips the trigger, no sidecar is ever written and every recovery is a full replay —
/// with no error and no warning. These tests drive the real runtime, so the declaration itself is
/// what they pin.
/// </para>
/// </summary>
public sealed class JsonlRuntimeSnapshotTests
{
    /// <summary>
    /// The store IS the snapshot store the runtime ages out: a threshold of two flushed events
    /// produces <c>&lt;root&gt;-snapshots/&lt;id&gt;.snap</c>, and the rebuild the system performs
    /// afterwards restores it instead of replaying the stream.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task SnapshotEveryTwoEvents_WritesSidecar_AndRebuildRestoresIt(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = 2 }
        );
        system.Register<ProbeCounter>(
            command => new ProbeCounter((ProbeCreate)command),
            () => new ProbeCounter()
        );

        // Flush 1 (creation) then flush 2: the second one crosses the threshold.
        var actor = await system.CreateAsync<ProbeCounter>(new ProbeCreate(0));
        await system.AskAsync<object?>(actor.Id, new ProbeAdd(1));
        var id = actor.Id;

        var snapshotPath = Path.Combine(temp.Root + "-snapshots", id.ToString("D") + ".snap");
        await Assert.That(File.Exists(snapshotPath)).IsTrue();
        await Assert.That(store).IsAssignableTo<IEventStoreSnapshot>();

        var snapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(snapshot).IsNotNull();
        await Assert.That(snapshot!.Version).IsEqualTo(2UL);

        // One event AFTER the snapshot: below the threshold, so no second snapshot.
        await system.AskAsync<object?>(id, new ProbeAdd(10));
        await Assert.That(actor.Version).IsEqualTo(3UL);
        await Assert.That(actor.Value).IsEqualTo(11);

        await system.StopAsync(id);

        // The rebuild reads the sidecar: only the one event after v2 is deserialized or mutated.
        var deserializedBefore = store.DeserializedEventCount;
        var rebuilt = await system.GetAsync<ProbeCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(11);
        await Assert.That(rebuilt.Version).IsEqualTo(3UL);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(1);
        await Assert.That(store.DeserializedEventCount - deserializedBefore).IsEqualTo(1L);

        await system.StopAsync(id);
    }

    /// <summary>
    /// The same wiring with snapshots OFF (<c>SnapshotEveryNEvents = null</c>): the store offers
    /// the capability, the host does not opt in, so nothing is written — the threshold, not the
    /// store, is what decides.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task SnapshotEveryNEventsNull_WritesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<ProbeCounter>(
            command => new ProbeCounter((ProbeCreate)command),
            () => new ProbeCounter()
        );

        var actor = await system.CreateAsync<ProbeCounter>(new ProbeCreate(0));
        await system.AskAsync<object?>(actor.Id, new ProbeAdd(1));
        await system.AskAsync<object?>(actor.Id, new ProbeAdd(2));

        var snapshotPath = Path.Combine(temp.Root + "-snapshots", actor.Id.ToString("D") + ".snap");
        await Assert.That(File.Exists(snapshotPath)).IsFalse();
        await Assert.That(await store.LoadSnapshotAsync(actor.Id)).IsNull();

        await system.StopAsync(actor.Id);
    }
}
