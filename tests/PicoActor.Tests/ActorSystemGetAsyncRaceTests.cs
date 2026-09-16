namespace PicoActor.Tests;

/// <summary>
/// Regression tests for the GetAsync rebuild race: when two concurrent
/// GetAsync calls rebuild the same id, one wins the TryAdd; the loser is
/// marked discarded and its duplicate is stopped WITHOUT running OnReadyAsync
/// (no business logic, no saga resume, no event-stream writes).
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
    /// Store that forces a deterministic winner-then-loser ordering while
    /// guaranteeing BOTH callers have passed the in-memory registry check (so
    /// both take the rebuild path):
    ///   - first LoadAsync entry waits for the second to enter, then returns
    ///     (this caller rebuilds first and wins the TryAdd).
    ///   - second entry signals the first, then waits for the test to release it
    ///     (this caller rebuilds second and loses TryAdd).
    /// </summary>
    private sealed class RaceEventStore : IEventStore
    {
        private readonly IReadOnlyList<IDomainEvent> _events;
        private readonly TaskCompletionSource<bool> _bothEntered;
        private readonly TaskCompletionSource<bool> _releaseLoser;
        private int _enteredCount;

        public RaceEventStore(
            IReadOnlyList<IDomainEvent> events,
            TaskCompletionSource<bool> bothEntered,
            TaskCompletionSource<bool> releaseLoser
        )
        {
            _events = events;
            _bothEntered = bothEntered;
            _releaseLoser = releaseLoser;
        }

        public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
        {
            var n = Interlocked.Increment(ref _enteredCount);
            if (n == 1)
                await _bothEntered.Task.ConfigureAwait(false);
            else
            {
                _bothEntered.TrySetResult(true);
                await _releaseLoser.Task.ConfigureAwait(false);
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
    /// Two concurrent GetAsync calls rebuild the same id: single-flight semantics —
    /// one wins the rebuild, the loser is marked discarded and skips OnReadyAsync
    /// (no recovery/replay path logic runs). Both calls return the same instance
    /// successfully, and OnReadyAsync runs exactly once.
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
        var releaseLoser = new TaskCompletionSource<bool>();
        var store = new RaceEventStore(events, bothEntered, releaseLoser);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<CountingReadyActor>(
            _ => throw new InvalidOperationException(),
            () => new CountingReadyActor()
        );

        var taskA = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());
        var taskB = Task.Run(() => system.GetAsync<CountingReadyActor>(id).AsTask());

        // The first caller into LoadAsync (n==1) waits on bothEntered; the second
        // (n==2) sets bothEntered and waits on releaseLoser — so the caller that
        // finishes LoadAsync first always wins TryAdd (independent of Task.Run scheduling).
        var firstCompleted = await Task.WhenAny(taskA, taskB);
        var winner = await firstCompleted;
        var loserTask = ReferenceEquals(firstCompleted, taskA) ? taskB : taskA;

        // Release the loser → its rebuild fails TryAdd → marked discarded → cleaned up → returns the winner
        releaseLoser.TrySetResult(true);
        var loserResult = await loserTask;

        await Assert.That(winner).IsNotNull();
        await Assert.That(loserResult).IsNotNull();

        // Single-flight: only the winning copy runs OnReadyAsync (recovery/replay path); the loser is skipped as discarded
        await Task.Delay(200);
        await Assert.That(CountingReadyActor.ReadyCount).IsEqualTo(1);
    }

    /// <summary>
    /// Cross-type concurrent rebuild of the same id (two different aggregates asking
    /// for the same framework-owned id — the scenario behind the registry-hit type
    /// guard): the loser of TryAdd must degrade to `default` like the registry-hit
    /// path does, NOT throw InvalidCastException from the winner cast.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_ConcurrentCrossTypeRebuilds_LoserReturnsDefault(
        CancellationToken cancellationToken = default
    )
    {
        var id = Guid.CreateVersion7();
        var events = (IReadOnlyList<IDomainEvent>)new IDomainEvent[] { new RaceCreated() };
        var bothEntered = new TaskCompletionSource<bool>();
        var releaseLoser = new TaskCompletionSource<bool>();
        var store = new RaceEventStore(events, bothEntered, releaseLoser);
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
        releaseLoser.TrySetResult(true);
        await Task.WhenAll(taskA, taskB);

        // No exception: the loser degrades to default instead of throwing
        await Assert.That(failure).IsNull();

        // Exactly one caller won the rebuild; the other got default (wrong aggregate type for that id)
        await Assert.That((first is null) != (second is null)).IsTrue();
    }
}
