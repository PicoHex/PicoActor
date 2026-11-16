using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>
/// A list that records which indices are read, standing in for a store whose indexer
/// deserializes on demand: a rebuild that only replays the tail must never touch the head.
/// </summary>
internal sealed class IndexProbeList(IReadOnlyList<IDomainEvent> inner)
    : IReadOnlyList<IDomainEvent>
{
    private readonly List<int> _accessed = new();

    public IReadOnlyList<int> Accessed
    {
        get
        {
            lock (_accessed)
                return _accessed.ToArray();
        }
    }

    public IDomainEvent this[int index]
    {
        get
        {
            lock (_accessed)
                _accessed.Add(index);
            return inner[index];
        }
    }

    public int Count => inner.Count;

    public IEnumerator<IDomainEvent> GetEnumerator()
    {
        for (var i = 0; i < inner.Count; i++)
            yield return this[i];
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
        GetEnumerator();
}

/// <summary>
/// Wraps the in-memory store and hands back an <see cref="IndexProbeList"/> from LoadAsync,
/// exposing the list the runtime actually consumed.
/// </summary>
internal sealed class TailProbeStore : IEventStore, IEventStoreSnapshot
{
    private readonly InMemoryEventStore _inner = new();

    public IndexProbeList? LastLoaded { get; private set; }

    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        var events = await _inner.LoadAsync(actorId).ConfigureAwait(false);
        LastLoaded = new IndexProbeList(events);
        return LastLoaded;
    }

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => _inner.AppendAsync(actorId, expectedVersion, events);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => _inner.PeekFirstAsync(actorId);

    public ValueTask<ActorSnapshot?> LoadSnapshotAsync(
        Guid actorId,
        CancellationToken ct = default
    ) => _inner.LoadSnapshotAsync(actorId, ct);

    public ValueTask SaveSnapshotAsync(
        Guid actorId,
        ActorSnapshot snapshot,
        CancellationToken ct = default
    ) => _inner.SaveSnapshotAsync(actorId, snapshot, ct);
}

/// <summary>
/// The store decides how much it deserializes: the runtime must pass the tail as a window and
/// must not materialize the events a snapshot already covers.
/// </summary>
internal sealed class RebuildTailViewTests
{
    private static async Task<(ActorSystem System, TailProbeStore Store, Guid Id)> BuildAsync(
        int adds
    )
    {
        var store = new TailProbeStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<SSnapshotCounter>(
            cmd => new SSnapshotCounter((SSnapshotCreate)cmd),
            () => new SSnapshotCounter()
        );
        var actor = await system.CreateAsync<SSnapshotCounter>(new SSnapshotCreate(0));
        for (var i = 0; i < adds; i++)
            await system.AskAsync<object?>(actor.Id, new SSnapshotAdd(1));
        await system.StopAsync(actor.Id);
        return (system, store, actor.Id);
    }

    [Test, Timeout(15000)]
    public async Task SnapshotRebuild_TouchesOnlyTheTailOfTheStoreList(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (system, store, id) = await BuildAsync(adds: 99); // 100 events, values 0..99

        // Snapshot at version 90 (value 89): the rebuild must replay events 91..100 only.
        var payload = new byte[5];
        payload[0] = 0x5A; // SSnapshotCounter's magic
        BitConverter.GetBytes(89).CopyTo(payload, 1);
        await store.SaveSnapshotAsync(id, new ActorSnapshot(90, payload));

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(99);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(10); // tail only

        var accessed = store.LastLoaded!.Accessed;
        await Assert.That(accessed.Count).IsEqualTo(10);
        await Assert.That(accessed.All(i => i >= 90)).IsTrue();

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task FullReplayWithoutSnapshot_ReadsTheWholeList(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (system, store, id) = await BuildAsync(adds: 9); // 10 events

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(9);
        await Assert.That(store.LastLoaded!.Accessed.Count).IsEqualTo(10);

        await system.StopAsync(id);
    }
}
