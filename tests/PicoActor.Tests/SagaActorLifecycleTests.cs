using PicoActor.Abs;

namespace PicoActor.Tests;

public sealed class SagaActorLifecycleTests
{
    // ═══════════════════════════════════════════════════════════
    // Auto-stop after completion
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task MarkComplete_AutoStops_SendThrows()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<TestSaga>(_ => new TestSaga());

        var saga = await system.CreateAsync<TestSaga>(new StartSaga("init"));
        var sagaId = saga.Id;

        var result = await system.AskAsync<string>(sagaId, new StartSaga("test"));
        await Assert.That(result).IsEqualTo("test");

        await Task.Delay(300);

        await Assert
            .That(() => system.Send(sagaId, new GetSagaStep()))
            .Throws<KeyNotFoundException>();
    }

    // ═══════════════════════════════════════════════════════════
    // Completed sagas don't return from GetAsync (no rebuild)
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task GetAsync_CompletedSaga_ReturnsNull()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var saga = await system.CreateAsync<TestSaga>(new StartSaga("done"));
        var sagaId = saga.Id;
        await system.AskAsync<string>(sagaId, new StartSaga("done"));
        await Task.Delay(300);

        var rebuilt = await system.GetAsync<TestSaga>(sagaId);
        await Assert.That(rebuilt).IsNull();
    }

    // ═══════════════════════════════════════════════════════════
    // Framework terminal events in the stream
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task Completion_AppendsFrameworkSagaCompletedEvent()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<TestSaga>(_ => new TestSaga());

        var saga = await system.CreateAsync<TestSaga>(new StartSaga("x"));
        await system.AskAsync<string>(saga.Id, new StartSaga("x"));
        await Task.Delay(300);

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[0]).IsTypeOf<SagaStep1Started>();
        await Assert.That(events[1]).IsTypeOf<SagaStep2Done>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
        await Assert.That(((SagaCompleted)events[2]).Result).IsEqualTo("x");
    }

    // ═══════════════════════════════════════════════════════════
    // Idempotent replay: partial completion → resume finishes
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task ResumeAsync_ContinuesFromInterruptedStep_AndCompletes()
    {
        var store = new InMemoryEventStore();

        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("partial")]);

        var system2 = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system2.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var rebuilt = await system2.GetAsync<TestSaga>(sagaId);
        await Assert.That(rebuilt).IsNotNull();

        // resume 推进到完成:GetAsync await init 后 saga 已 auto-stop
        await Task.Delay(300);
        await Assert
            .That(() => system2.Send(sagaId, new GetSagaStep()))
            .Throws<KeyNotFoundException>();

        // 事件流含 resume 追加的框架完成事件
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[1]).IsTypeOf<SagaStep2Done>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
    }
}
