namespace PicoActor.Tests;

/// <summary>
/// Regression tests for the GetAsync rebuild race: two concurrent GetAsync calls
/// for an unregistered id must still produce ONE instance and ONE initialization
/// (no business logic, no saga resume, no event-stream write runs twice).
/// <para>
/// Scope note (I-4): with the per-id single-flight gate in place these tests no
/// longer exercise the <c>TryAdd</c> duplicate branch in GetAsync — that branch is
/// kept as defence and is unreachable while the gate holds. What they verify is the
/// observable contract: both callers miss the registry, only one rebuild runs
/// (OnReadyAsync once), and both resolve to the same instance (the second one as a
/// registry hit after the gate is released). The assertions are unchanged; they
/// simply no longer take the path through the discarded-duplicate cleanup.
/// </para>
/// </summary>
public sealed class ActorSystemGetAsyncRaceTests
{
    internal sealed record RaceCreated : IDomainEvent;

    /// <summary>Counts OnReadyAsync executions — single-flight verification: only the winning copy runs the recovery path.</summary>
    internal sealed class CountingReadyActor : EventSourcedActor
    {
        public static int ReadyCount;

        public CountingReadyActor() { }

        protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

        protected override async ValueTask OnReadyAsync()
        {
            Interlocked.Increment(ref ReadyCount);
            await base.OnReadyAsync();
        }

        protected override void Mutate(IDomainEvent @event) { }
    }

    /// <summary>
    /// Store that forces a deterministic ordering while guaranteeing BOTH callers
    /// have passed the in-memory registry check, so both take the rebuild path:
    ///   - first LoadAsync entry (the pre-gate prefetch) waits for the second to
    ///     enter, then returns — this caller rebuilds and registers.
    ///   - second entry signals the first, then waits for the test to release it —
    ///     this caller is then resolved against the freshly registered instance
    ///     instead of rebuilding a copy.
    /// The later loads are GetAsync's unconditional re-read under the per-id gate and
    /// must never park (see LoadAsync).
    /// </summary>
    private sealed class RaceEventStore : IEventStore
    {
        private readonly IReadOnlyList<IDomainEvent> _events;
        private readonly TaskCompletionSource<bool> _loadEntered;
        private readonly TaskCompletionSource<bool> _releaseLoad;
        private int _enteredCount;

        public RaceEventStore(
            IReadOnlyList<IDomainEvent> events,
            TaskCompletionSource<bool> loadEntered,
            TaskCompletionSource<bool> releaseLoad
        )
        {
            _events = events;
            _loadEntered = loadEntered;
            _releaseLoad = releaseLoad;
        }

        /// <summary>Number of loads observed; a single-flight rebuild must show exactly 1.</summary>
        public int Loads => Volatile.Read(ref _enteredCount);

        public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
        {
            // The FIRST load is the winner's single in-gate read. Park it so the test can
            // start the second caller against a still-empty registry: that caller then has to
            // wait on the same gate (and must not load at all). Later loads never park.
            if (Interlocked.Increment(ref _enteredCount) == 1)
            {
                _loadEntered.TrySetResult(true);
                await _releaseLoad.Task.ConfigureAwait(false);
            }
            return _events;
        }

        public ValueTask<ulong> AppendAsync(
            Guid actorId,
            ulong expectedVersion,
            IReadOnlyList<IDomainEvent> events
        ) => throw new NotImplementedException();

        public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
            new(_events.Count > 0 ? _events[0] : null);
    }

    /// <summary>Two rebuildable aggregate types for the cross-type race — deliberately unrelated to each other and to <see cref="CountingReadyActor"/> (no shared statics).</summary>
    internal sealed class CrossTypeActorOne : EventSourcedActor
    {
        public CrossTypeActorOne() { }

        protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

        protected override void Mutate(IDomainEvent @event) { }
    }

    internal sealed class CrossTypeActorTwo : EventSourcedActor
    {
        public CrossTypeActorTwo() { }

        protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

        protected override void Mutate(IDomainEvent @event) { }
    }

    /// <summary>
    /// Two concurrent GetAsync calls for the same unregistered id: single-flight — one
    /// instance is built and initialized, OnReadyAsync (the recovery/replay path) runs
    /// exactly once, and both calls return that same instance.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_ConcurrentRebuilds_SingleFlight_NoDuplicateResume(
        CancellationToken cancellationToken = default
    )
    {
        CountingReadyActor.ReadyCount = 0;
        var id = Guid.CreateVersion7();
        var events = (IReadOnlyList<IDomainEvent>)new IDomainEvent[] { new RaceCreated() };
        var bothEntered = new TaskCompletionSource<bool>();
        var releaseSecond = new TaskCompletionSource<bool>();
        var store = new RaceEventStore(events, bothEntered, releaseSecond);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<CountingReadyActor>(
            _ => throw new InvalidOperationException(),
            () => new CountingReadyActor()
        );

        // Start the winner and park it INSIDE its in-gate load: at that point the registry is
        // still empty, so the second caller that starts now cannot see a registered instance.
        var taskA = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());
        await bothEntered.Task;
        var taskB = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());
        await Task.Delay(100); // let B reach the gate (it must not load: it waits on A's gate)

        releaseSecond.TrySetResult(true);
        var both = await Task.WhenAll(taskA, taskB);

        await Assert.That(both[0]).IsNotNull();
        await Assert.That(both[1]).IsNotNull();
        await Assert.That(ReferenceEquals(both[0], both[1])).IsTrue();

        // Single-flight: exactly one instance runs OnReadyAsync (recovery/replay path) and
        // the stream is read exactly once (the loser resolves through the registry).
        await Task.Delay(200);
        await Assert.That(CountingReadyActor.ReadyCount).IsEqualTo(1);
        await Assert.That(store.Loads).IsEqualTo(1);
    }

    /// <summary>
    /// Cross-type concurrent lookup of the same id (two different aggregates asking for
    /// the same framework-owned id — the scenario behind the type guard): exactly one
    /// caller gets an instance, the other degrades to `default` instead of throwing
    /// InvalidCastException from the cast.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_ConcurrentCrossTypeLookups_OnlyOneCallerGetsInstance(
        CancellationToken cancellationToken = default
    )
    {
        var id = Guid.CreateVersion7();
        var events = (IReadOnlyList<IDomainEvent>)new IDomainEvent[] { new RaceCreated() };
        var bothEntered = new TaskCompletionSource<bool>();
        var releaseSecond = new TaskCompletionSource<bool>();
        var store = new RaceEventStore(events, bothEntered, releaseSecond);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<CrossTypeActorOne>(
            _ => throw new InvalidOperationException(),
            () => new CrossTypeActorOne()
        );
        system.Register<CrossTypeActorTwo>(
            _ => throw new InvalidOperationException(),
            () => new CrossTypeActorTwo()
        );

        Exception? failure = null;
        CrossTypeActorOne? first = null;
        CrossTypeActorTwo? second = null;

        var taskA = Task.Run(async () =>
        {
            try
            {
                first = await system.GetAsync<CrossTypeActorOne>(id);
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });
        var taskB = Task.Run(async () =>
        {
            try
            {
                second = await system.GetAsync<CrossTypeActorTwo>(id);
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });

        await bothEntered.Task; // A is parked inside its in-gate load; the registry is empty
        releaseSecond.TrySetResult(true);
        await Task.WhenAll(taskA, taskB);

        // No exception: the caller that found the other aggregate type degrades to default
        await Assert.That(failure).IsNull();

        // Exactly one caller won the rebuild; the other got default (wrong aggregate type for that id)
        await Assert.That((first is null) != (second is null)).IsTrue();
    }
}
