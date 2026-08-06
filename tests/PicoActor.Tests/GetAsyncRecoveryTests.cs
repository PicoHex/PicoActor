using System.IO;
using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>Mutate 抛异常的 actor——验证 GetAsync 重放失败路径的资源清理。</summary>
internal sealed record PoisonEvent : IDomainEvent;

internal sealed class PoisonReplayActor : EventSourcedActor
{
    public PoisonReplayActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is PoisonEvent)
            throw new InvalidOperationException("poisoned replay");
    }
}

/// <summary>append 抛异常的 store——验证恢复失败时 saga 保持可重试(非终态)。</summary>
internal sealed class FailAppendStore : IEventStore
{
    private readonly IReadOnlyList<IDomainEvent> _existing;

    public int AppendAttempts;

    public FailAppendStore(IReadOnlyList<IDomainEvent> existing) => _existing = existing;

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        Interlocked.Increment(ref AppendAttempts);
        throw new IOException("store down");
    }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) => new(_existing);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
        new(_existing.Count > 0 ? _existing[0] : null);
}

public sealed class GetAsyncRecoveryTests
{
    [Test]
    public async Task GetAsync_ReplayFailure_Propagates_AndCleansUp()
    {
        var id = Guid.CreateVersion7();
        var store = new InMemoryEventStore();
        await store.AppendAsync(id, 0, [new PoisonEvent()]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<PoisonReplayActor>(
            _ => new PoisonReplayActor(),
            () => new PoisonReplayActor()
        );

        await Assert
            .That(async () => await system.GetAsync<PoisonReplayActor>(id))
            .Throws<InvalidOperationException>();

        // 清理验证:重复 GetAsync 不应累积(不再抛 KeyNotFoundException 或泄漏)
        await Assert
            .That(async () => await system.GetAsync<PoisonReplayActor>(id))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task GetAsync_ResumeStoreFailure_PropagatesOriginal_NotTerminal()
    {
        var sagaId = Guid.CreateVersion7();
        var store = new FailAppendStore(new IDomainEvent[] { new SagaStep1Started("partial") });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        // resume 追加 SagaStep2Done + SagaCompleted 时 append 失败 → 原始 store 异常传播
        await Assert
            .That(async () => await system.GetAsync<TestSaga>(sagaId))
            .Throws<IOException>();
        await Assert.That(store.AppendAttempts).IsEqualTo(1);

        // 非终态:saga 未产生 SagaFailed 事件,store 恢复后可重试
        // (FailAppendStore 的 LoadAsync 返回固定事件——重试仍会失败,但异常类型证明走的是基础设施路径)
    }
}
