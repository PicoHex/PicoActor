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
        private readonly TaskCompletionSource<bool> _bothEntered;
        private readonly TaskCompletionSource<bool> _releaseSecond;
        private int _enteredCount;

        public RaceEventStore(
            IReadOnlyList<IDomainEvent> events,
            TaskCompletionSource<bool> bothEntered,
            TaskCompletionSource<bool> releaseSecond
        )
        {
            _events = events;
            _bothEntered = bothEntered;
            _releaseSecond = releaseSecond;
        }

        public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
        {
            // n == 1 / n == 2 are the two PRE-gate prefetches of the two concurrent callers
            // (the ordering this store manufactures). Every later load is GetAsync's
            // unconditional re-read under the per-id gate, which must never park: the
            // ordering it exists to create is already established by then, and parking
            // there would deadlock the very callers this store lines up.
            var n = Interlocked.Increment(ref _enteredCount);
            if (n == 1)
                await _bothEntered.Task.ConfigureAwait(false);
            else if (n == 2)
            {
                _bothEntered.TrySetResult(true);
                await _releaseSecond.Task.ConfigureAwait(false);
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

        var taskA = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());
        var taskB = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());

        // The first caller into LoadAsync (n==1) waits on bothEntered; the second
        // (n==2) sets bothEntered and waits on releaseSecond — so both callers have
        // provably passed the empty-registry check before either rebuilds, independent of
        // Task.Run scheduling.
        var firstCompleted = await Task.WhenAny(taskA, taskB);
        var registered = await firstCompleted;
        var secondTask = ReferenceEquals(firstCompleted, taskA) ? taskB : taskA;

        // Release the second caller: it takes the gate, finds the registered instance and
        // resolves to it instead of rebuilding (and re-initializing) a copy.
        releaseSecond.TrySetResult(true);
        var secondResult = await secondTask;

        await Assert.That(registered).IsNotNull();
        await Assert.That(secondResult).IsNotNull();

        // Single-flight: exactly one instance runs OnReadyAsync (recovery/replay path).
        await Task.Delay(200);
        await Assert.That(CountingReadyActor.ReadyCount).IsEqualTo(1);
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

        await Task.WhenAny(taskA, taskB);
        releaseSecond.TrySetResult(true);
        await Task.WhenAll(taskA, taskB);

        // No exception: the caller that found the other aggregate type degrades to default
        await Assert.That(failure).IsNull();

        // Exactly one caller won the rebuild; the other got default (wrong aggregate type for that id)
        await Assert.That((first is null) != (second is null)).IsTrue();
    }
}
