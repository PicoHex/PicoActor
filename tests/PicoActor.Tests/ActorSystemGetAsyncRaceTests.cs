using PicoActor.Abs;

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

    /// <summary>OnReadyAsync 执行计数——单飞验证:只有胜出副本执行恢复路径。</summary>
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

    /// <summary>
    /// 两个并发 GetAsync 重建同一 id:单飞语义——一个胜出重建,loser 标记 discarded
    /// 跳过 OnReadyAsync(不执行恢复/重放路径逻辑)。两个调用都成功返回同一实例,
    /// OnReadyAsync 只执行一次。
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

        // 先进入 LoadAsync(n==1) 的调用者等 bothEntered,后进入的(n==2)set bothEntered 并等
        // releaseLoser——因此先完成 LoadAsync 的必然胜出 TryAdd(不依赖 Task.Run 调度顺序)。
        var firstCompleted = await Task.WhenAny(taskA, taskB);
        var winner = await firstCompleted;
        var loserTask = ReferenceEquals(firstCompleted, taskA) ? taskB : taskA;

        // 释放 loser → 它重建后 TryAdd 失败 → 标记 discarded → 清理 → 返回胜出实例
        releaseLoser.TrySetResult(true);
        var loserResult = await loserTask;

        await Assert.That(winner).IsNotNull();
        await Assert.That(loserResult).IsNotNull();

        // 单飞:只有胜出副本执行 OnReadyAsync(恢复/重放路径),loser 被 discarded 跳过
        await Task.Delay(200);
        await Assert.That(CountingReadyActor.ReadyCount).IsEqualTo(1);
    }
}
