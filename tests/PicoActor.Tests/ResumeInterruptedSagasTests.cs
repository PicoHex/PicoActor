using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>LoadAsync 对指定 id 抛异常的 store——模拟恢复期间 store 抖动(fail-fast 验证)。</summary>
internal sealed class ThrowingLoadStore : IEventStore, IEventStoreEnumerator
{
    private readonly IEventStore _inner;
    private readonly Guid _throwingId;

    public ThrowingLoadStore(IEventStore inner, Guid throwingId)
    {
        _inner = inner;
        _throwingId = throwingId;
    }

    public async ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => await _inner.AppendAsync(actorId, expectedVersion, events);

    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        if (actorId == _throwingId)
            throw new IOException("store down");
        return await _inner.LoadAsync(actorId);
    }

    public IReadOnlyList<Guid> ListAggregateIds(string firstEventType) =>
        ((IEventStoreEnumerator)_inner).ListAggregateIds(firstEventType);
}

public sealed class ResumeInterruptedSagasTests
{
    [Test]
    public async Task Resume_InterruptedSaga_Completes_AndReturnsCompleted()
    {
        var store = new InMemoryEventStore();

        // 中断:只持久化步骤 1
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("recover")]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(sagaId);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);

        // 事件流现在含框架完成事件
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
    }

    [Test]
    public async Task Resume_NoInterrupted_ReturnsEmpty()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));
        await Assert.That(results).IsEmpty();
    }

    [Test]
    public async Task Resume_TerminalSagas_AreSkipped()
    {
        var store = new InMemoryEventStore();

        // 已完成 saga(含框架完成事件)——不应复活、不计入结果
        var doneId = Guid.CreateVersion7();
        await store.AppendAsync(doneId, 0, [new SagaStep1Started("done"), new SagaCompleted("x")]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));
        await Assert.That(results).IsEmpty();
    }

    [Test]
    public async Task Resume_InterruptedWithAllStepsDone_CompletesViaResume()
    {
        var store = new InMemoryEventStore();

        // 步骤 2 已持久化但无终态事件 → resume 时 _step=2 → 业务步骤全守卫跳过,MarkComplete → 完成
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("x"), new SagaStep2Done()]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
    }

    [Test]
    public async Task Resume_ResumeWithoutTerminal_ReturnsRunning()
    {
        var store = new InMemoryEventStore();

        // 需要 resume 后仍等待外部命令的 saga:用 RunningSaga(步骤完成后不 MarkComplete,等 WaitCmd)
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new RunningStep1Started("w")]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<RunningSaga>(_ => new RunningSaga(), () => new RunningSaga());

        var results = await system.ResumeInterruptedSagasAsync<RunningSaga>(
            nameof(RunningStep1Started)
        );

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(sagaId);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Running);

        // 恢复后仍存活:发推进命令可完成(等 auto-stop 后再验证)
        system.Send(sagaId, new RunningCompleteCmd());
        await Task.Delay(300);
        var gone = await system.GetAsync<RunningSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }

    [Test]
    public async Task Resume_BatchFailure_FailsFast()
    {
        // 两个中断 saga:第一个可恢复,第二个 LoadAsync 抛异常
        // → fail-fast:整个调用抛异常(而非部分恢复后静默继续)
        var inner = new InMemoryEventStore();
        var id1 = Guid.CreateVersion7();
        var id2 = Guid.CreateVersion7();
        await inner.AppendAsync(id1, 0, [new SagaStep1Started("a")]);
        await inner.AppendAsync(id2, 0, [new SagaStep1Started("b")]);

        var store = new ThrowingLoadStore(inner, id2);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        await Assert
            .That(async () =>
                await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started))
            )
            .Throws<IOException>();
    }

    [Test]
    public async Task Resume_BusinessFailure_ClassifiesFailed_WithReason()
    {
        // 中断 saga 的 ResumeAsync 抛业务异常 → 框架追加 SagaFailed(reason) → 归 Failed(reason),
        // GetAsync 不复活(失败 = 终态)
        var store = new InMemoryEventStore();
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new ResumeBoomStep1()]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<ResumeBoomSaga>(_ => new ResumeBoomSaga(), () => new ResumeBoomSaga());

        var results = await system.ResumeInterruptedSagasAsync<ResumeBoomSaga>(
            nameof(ResumeBoomStep1)
        );

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(sagaId);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Failed);
        await Assert.That(results[0].Reason).Contains("resume boom");

        // 事件流含框架 SagaFailed(reason)
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<ResumeBoomStep1>();
        await Assert.That(events[1]).IsTypeOf<SagaFailed>();

        // 失败 = 终态:auto-stop 后 GetAsync 不复活
        await Task.Delay(300);
        var gone = await system.GetAsync<ResumeBoomSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }
}

/// <summary>resume 后仍等待外部命令的 saga(Running 分类验证)。</summary>
internal sealed record RunningStartCmd(string Name) : ICommand;

internal sealed record RunningStep1Started(string Name) : IDomainEvent;

internal sealed record RunningStep2Done : IDomainEvent;

internal sealed record RunningCompleteCmd : ICommand;

internal sealed class RunningSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public RunningSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case RunningStartCmd c:
                if (_step < 1)
                {
                    RaiseEvent(new RunningStep1Started(c.Name));
                    _name = c.Name;
                }
                return new ValueTask<object?>(_name);
            case RunningCompleteCmd:
                if (_step < 2)
                {
                    RaiseEvent(new RunningStep2Done());
                    MarkComplete(_name);
                }
                return new ValueTask<object?>(_name);
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        // 恢复后无终态:不推进,继续等待 RunningCompleteCmd
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case RunningStep1Started e:
                _step = 1;
                _name = e.Name;
                break;
            case RunningStep2Done:
                _step = 2;
                break;
        }
    }
}

/// <summary>resume 时抛业务异常的 saga——恢复 API Failed 分类验证。</summary>
internal sealed record ResumeBoomStep1 : IDomainEvent;

internal sealed class ResumeBoomSaga : SagaActor
{
    public ResumeBoomSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask ResumeAsync()
    {
        throw new InvalidOperationException("resume boom");
    }

    protected override void Mutate(IDomainEvent @event) { }
}
