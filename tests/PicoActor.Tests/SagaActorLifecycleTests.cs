using PicoActor.Abs;

namespace PicoActor.Tests;

// ── Minimal test saga for framework tests ──

internal sealed record StartSaga(string Name) : ICommand;

internal sealed record GetSagaStep : ICommand;

internal sealed record SagaStep1Started(string Name) : IDomainEvent;

internal sealed record SagaStep2Done : IDomainEvent;

internal sealed record SagaCompleted : IDomainEvent;

internal sealed class TestSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public TestSaga() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is StartSaga s)
        {
            if (_step < 1)
            {
                RaiseEvent(new SagaStep1Started(s.Name));
                _name = s.Name; // set immediately for return value (Mutate comes later)
            }
            if (_step < 2)
                RaiseEvent(new SagaStep2Done());
            if (_step < 3)
                RaiseEvent(new SagaCompleted()); // must persist so Mutate restores _completed
            MarkComplete();
            return _name;
        }
        if (command is GetSagaStep)
            return _step;
        return null;
    }

    protected override async ValueTask ResumeAsync()
    {
        await OnMessageAsync(new StartSaga(_name));
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case SagaStep1Started e:
                _step = 1;
                _name = e.Name;
                break;
            case SagaStep2Done:
                _step = 2;
                break;
            case SagaCompleted:
                MarkComplete();
                break;
        }
    }
}

/// <summary>
/// Tests for the SagaActor base class: auto-stop after MarkComplete,
/// crash recovery via ResumeAsync, and idempotent replay.
/// </summary>
public sealed class SagaActorLifecycleTests
{
    // ═══════════════════════════════════════════════════════════
    // Auto-stop after completion
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// After MarkComplete, the saga auto-stops. Sending a command
    /// to the stopped saga must throw — proves it was removed from registry.
    /// </summary>
    [Test]
    public async Task MarkComplete_AutoStops_SendThrows()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<TestSaga>(_ => new TestSaga());

        var saga = await system.CreateAsync<TestSaga>(new StartSaga("init"));
        var sagaId = saga.Id;

        // Execute the command — saga should MarkComplete internally
        var result = await system.AskAsync<string>(sagaId, new StartSaga("test"));

        await Assert.That(result).IsEqualTo("test");

        // Give the async stop time to complete (ScheduleStop → Task.Yield → StopAsync)
        await Task.Delay(300);

        // Verify: saga is gone — sending a command throws
        await Assert
            .That(() => system.Send(sagaId, new GetSagaStep()))
            .Throws<KeyNotFoundException>();
    }

    // ═══════════════════════════════════════════════════════════
    // Completed sagas don't return from GetAsync (no rebuild)
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// A completed saga must not be rebuilt by GetAsync.
    /// Its JSONL file is an audit trail, not a signal to resurrect.
    /// </summary>
    [Test]
    public async Task GetAsync_CompletedSaga_ReturnsNull()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        // Run saga to completion
        var saga = await system.CreateAsync<TestSaga>(new StartSaga("done"));
        var sagaId = saga.Id;
        await system.AskAsync<string>(sagaId, new StartSaga("done"));
        await Task.Delay(300); // let stop complete

        // Rebuild attempt: completed saga should not be resurrected
        var rebuilt = await system.GetAsync<TestSaga>(sagaId);
        await Assert.That(rebuilt).IsNull();
    }

    // ═══════════════════════════════════════════════════════════
    // Repeated GetAsync on completed saga — no resource leak
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// Repeated GetAsync calls on a completed saga must not crash or
    /// accumulate unstopped actors. Before fix, each call leaked a
    /// _loopTask/Channel/CTS. After fix, StopAsync cleans up each
    /// rebuilt-but-discarded actor.
    /// </summary>
    [Test]
    public async Task GetAsync_CompletedSaga_RepeatedCalls_DoesNotCrash()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var saga = await system.CreateAsync<TestSaga>(new StartSaga("stress"));
        var sagaId = saga.Id;
        await system.AskAsync<string>(sagaId, new StartSaga("stress"));
        await Task.Delay(300); // let auto-stop complete

        for (int i = 0; i < 500; i++)
        {
            var rebuilt = await system.GetAsync<TestSaga>(sagaId);
            await Assert.That(rebuilt).IsNull();
        }
    }

    // ═══════════════════════════════════════════════════════════
    // Idempotent replay: partial completion → resume finishes
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// If a saga persisted step 1 then crashed, rebuilding it
    /// must resume from step 2 and complete.
    /// </summary>
    [Test]
    public async Task ResumeAsync_ContinuesFromInterruptedStep()
    {
        var store = new InMemoryEventStore();

        // Simulate a crash: manually persist only step 1 (not step 2 or SagaCompleted)
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("partial")]);

        // Rebuild from events (simulating restart)
        var system2 = new ActorSystem(store);
        system2.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var rebuilt = await system2.GetAsync<TestSaga>(sagaId);
        await Assert.That(rebuilt).IsNotNull();

        // ResumeAsync should have been called by OnReadyAsync
        // and completed the saga. Verify by checking step.
        var step = await system2.AskAsync<int>(sagaId, new GetSagaStep());
        await Assert.That(step).IsEqualTo(2);

        // Saga should auto-stop (it completed in OnReadyAsync)
        await Task.Delay(300);
        await Assert
            .That(() => system2.Send(sagaId, new GetSagaStep()))
            .Throws<KeyNotFoundException>();
    }
}
