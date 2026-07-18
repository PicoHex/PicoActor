using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

/// <summary>
/// Regression tests for the GetAsync rebuild race: when two concurrent
/// GetAsync calls rebuild the same id, the loser discards its duplicate and
/// read the winner via the throwing indexer <c>_registry[id]</c>. If the winner
/// was stopped during the loser's <c>await actor.StopAsync()</c>, that indexer
/// threw KeyNotFoundException. The fix must read safely and not throw.
/// </summary>
public sealed class ActorSystemGetAsyncRaceTests
{
    internal sealed record RaceCreated : IDomainEvent;

    /// <summary>
    /// Actor whose OnReadyAsync blocks on a TCS when configured. The loser
    /// duplicate's StopAsync runs OnReadyAsync, so blocking there deterministically
    /// suspends the loser inside the race window.
    /// </summary>
    internal sealed class BlockingReadyActor : EventSourcedActor
    {
        private readonly TaskCompletionSource<bool>? _loserArrived;
        private readonly TaskCompletionSource<bool>? _proceed;

        public BlockingReadyActor(
            TaskCompletionSource<bool>? loserArrived = null,
            TaskCompletionSource<bool>? proceed = null
        )
            : base()
        {
            _loserArrived = loserArrived;
            _proceed = proceed;
        }

        protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

        protected override async ValueTask OnReadyAsync()
        {
            // Winner instance has _proceed == null → returns immediately.
            // Loser instance blocks here (reached via StopAsync's SignalReady).
            if (_proceed is not null)
            {
                _loserArrived?.TrySetResult(true);
                await _proceed.Task.ConfigureAwait(false);
            }
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
    ///     (this caller rebuilds second, loses TryAdd, and suspends in StopAsync).
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
    }

    /// <summary>
    /// Two concurrent GetAsync rebuilds: the loser's duplicate is stopped while
    /// the winner is concurrently stopped. The loser must NOT throw
    /// KeyNotFoundException when reading the (now-gone) winner.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_LoserWhenWinnerStoppedConcurrently_DoesNotThrow()
    {
        var id = Guid.CreateVersion7();
        var events = (IReadOnlyList<IDomainEvent>)new IDomainEvent[] { new RaceCreated() };
        var bothEntered = new TaskCompletionSource<bool>();
        var releaseLoser = new TaskCompletionSource<bool>();
        var loserArrived = new TaskCompletionSource<bool>();
        var proceed = new TaskCompletionSource<bool>();
        var store = new RaceEventStore(events, bothEntered, releaseLoser);
        var system = new ActorSystem(store);

        var rebuildCount = 0;
        system.Register<BlockingReadyActor>(
            _ => throw new InvalidOperationException(),
            () =>
            {
                // First rebuild = winner (fast OnReadyAsync); second = loser (blocks).
                var n = Interlocked.Increment(ref rebuildCount);
                return n == 1
                    ? new BlockingReadyActor()
                    : new BlockingReadyActor(loserArrived, proceed);
            }
        );

        // Start two concurrent GetAsync — both find id NOT in memory, both enter LoadAsync.
        var taskA = Task.Run(() => system.GetAsync<BlockingReadyActor>(id).AsTask());
        var taskB = Task.Run(() => system.GetAsync<BlockingReadyActor>(id).AsTask());

        // The first to complete is the winner (it rebuilt first and won TryAdd).
        var winnerTask = await Task.WhenAny(taskA, taskB);
        var winner = await winnerTask;
        var loserTask = ReferenceEquals(winnerTask, taskA) ? taskB : taskA;

        // Release the loser's LoadAsync → it rebuilds (instance 2), TryAdd FAILS
        // (winner present), and StopAsync suspends inside OnReadyAsync.
        releaseLoser.TrySetResult(true);
        await loserArrived.Task; // loser is now suspended in its duplicate's StopAsync

        // Remove the winner from the registry while the loser is suspended.
        await system.StopAsync(winner!.Id);

        // Release the loser → its StopAsync completes → it reads the registry.
        proceed.TrySetResult(true);

        // The loser must NOT throw KeyNotFoundException (the bug).
        Exception? loserEx = null;
        try
        {
            _ = await loserTask;
        }
        catch (Exception ex)
        {
            loserEx = ex;
        }

        await Assert.That(loserEx).IsNull();
    }
}
