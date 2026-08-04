using PicoActor.Abs;

namespace PicoActor.Tests;

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
