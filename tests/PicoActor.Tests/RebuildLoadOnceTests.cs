using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>
/// Counts <see cref="IEventStore.LoadAsync"/> calls so a test can prove a rebuild reads the
/// stream once. Snapshot capability is intentionally NOT forwarded: these tests measure the
/// full-replay path only.
/// </summary>
internal sealed class CountingEventStore(InMemoryEventStore inner) : IEventStore
{
    private int _loads;

    public int Loads => Volatile.Read(ref _loads);

    public void Reset() => Interlocked.Exchange(ref _loads, 0);

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => inner.AppendAsync(actorId, expectedVersion, events);

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        Interlocked.Increment(ref _loads);
        return inner.LoadAsync(actorId);
    }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => inner.PeekFirstAsync(actorId);
}

/// <summary>
/// Guards for the "materialize a missing aggregate" path: one stream read per miss, one
/// rebuild for concurrent callers, and a bounded rebuild-gate table.
/// </summary>
internal sealed class RebuildLoadOnceTests
{
    private static async Task<(ActorSystem System, CountingEventStore Store, Guid Id)> BuildStoppedAsync(
        int adds = 4
    )
    {
        var store = new CountingEventStore(new InMemoryEventStore());
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<SSnapshotCounter>(
            cmd => new SSnapshotCounter((SSnapshotCreate)cmd),
            () => new SSnapshotCounter()
        );
        var actor = await system.CreateAsync<SSnapshotCounter>(new SSnapshotCreate(0));
        for (var i = 0; i < adds; i++)
            await system.AskAsync<object?>(actor.Id, new SSnapshotAdd(1));
        await system.StopAsync(actor.Id);
        store.Reset();
        return (system, store, actor.Id);
    }

    [Test, Timeout(15000)]
    public async Task MissRebuild_LoadsTheStreamExactlyOnce(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (system, store, id) = await BuildStoppedAsync();

        var rebuilt = await system.GetAsync<SSnapshotCounter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(4);
        // The regression: the miss path used to read the stream twice (prefetch + in-gate).
        await Assert.That(store.Loads).IsEqualTo(1);

        await system.StopAsync(id);
    }

    [Test, Timeout(15000)]
    public async Task ConcurrentMisses_RebuildOnceAndReadOnce(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (system, store, id) = await BuildStoppedAsync();

        var first = system.GetAsync<SSnapshotCounter>(id).AsTask();
        var second = system.GetAsync<SSnapshotCounter>(id).AsTask();
        var results = await Task.WhenAll(first, second);

        await Assert.That(results[0]).IsNotNull();
        await Assert.That(ReferenceEquals(results[0], results[1])).IsTrue();
        await Assert.That(store.Loads).IsEqualTo(1);

        await system.StopAsync(id);
    }

    [Test, Timeout(20000)]
    public async Task RebuildGateTable_StaysBounded(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var (system, _, _) = await BuildStoppedAsync(adds: 0);

        // Misses for ids that have no stream are the cheapest way to touch the gate table;
        // an unbounded table grows with every distinct id this process ever looks up.
        for (var i = 0; i < 1200; i++)
            await system.GetAsync<SSnapshotCounter>(Guid.NewGuid());

        await Assert.That(system.RebuildGateCount).IsLessThanOrEqualTo(1024);
    }

    [Test, Timeout(15000)]
    public async Task WriteAfterStop_IsIncludedInTheRebuild(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var store = new CountingEventStore(new InMemoryEventStore());
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<SSnapshotCounter>(
            cmd => new SSnapshotCounter((SSnapshotCreate)cmd),
            () => new SSnapshotCounter()
        );
        var actor = await system.CreateAsync<SSnapshotCounter>(new SSnapshotCreate(0));
        await system.AskAsync<object?>(actor.Id, new SSnapshotAdd(1)); // v2, value 1
        await system.StopAsync(actor.Id);

        // A write that lands after the stop but before the rebuild: the rebuild must replay
        // the stream as it is under the gate, never an older snapshot taken earlier.
        await store.AppendAsync(actor.Id, 2, [new SSnapshotAdded(5)]);

        var rebuilt = await system.GetAsync<SSnapshotCounter>(actor.Id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Value).IsEqualTo(6);
        await Assert.That(rebuilt.Version).IsEqualTo(3UL);

        await system.StopAsync(actor.Id);
    }
}
