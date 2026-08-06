using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>Store whose LoadAsync throws for a given id — simulates store flakiness during recovery (fail-fast verification).</summary>
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

    public async ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    {
        if (actorId == _throwingId)
            throw new IOException("store down");
        return await _inner.PeekFirstAsync(actorId);
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

        // Interrupted: only step 1 was persisted
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("recover")]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(sagaId);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);

        // The stream now contains the framework completion event
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

        // Completed saga (includes the framework completion event) — must not be resurrected or counted
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

        // Step 2 persisted but no terminal event → on resume _step=2 → all business steps
        // skipped by guards, MarkComplete → completed
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

        // A saga that keeps waiting for external commands after resume: use RunningSaga
        // (no MarkComplete after its steps; waits for WaitCmd)
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

        // Still alive after recovery: a push command can complete it (verify after auto-stop)
        system.Send(sagaId, new RunningCompleteCmd());
        await Task.Delay(300);
        var gone = await system.GetAsync<RunningSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }

    [Test]
    public async Task Resume_BatchFailure_FailsFast()
    {
        // Two interrupted sagas: the first is recoverable, the second throws in LoadAsync
        // → fail-fast: the whole call throws (instead of silently continuing after partial recovery)
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
    public async Task Resume_OnItemError_IsolatesBadFiles()
    {
        // Per-item error strategy: a bad stream only skips itself (onItemError receives the id),
        // remaining sagas of the same type keep recovering — the batch call is not interrupted.
        var inner = new InMemoryEventStore();
        var good = Guid.CreateVersion7();
        var bad = Guid.CreateVersion7();
        await inner.AppendAsync(good, 0, [new SagaStep1Started("good")]);
        await inner.AppendAsync(bad, 0, [new SagaStep1Started("bad")]);

        var store = new ThrowingLoadStore(inner, bad);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var errors = new List<Guid>();
        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(
            nameof(SagaStep1Started),
            onItemError: (ex, id) => errors.Add(id)
        );

        // The bad stream is isolated (not propagated); the good saga is still recovered
        await Assert.That(errors).Contains(bad);
        await Assert.That(results.Select(r => r.Id)).Contains(good);
        await Assert.That(results.Select(r => r.Id)).DoesNotContain(bad);
    }

    [Test]
    public async Task Resume_WithoutOnItemError_Propagates()
    {
        // Without onItemError, the bad stream's exception propagates outward (legacy-compatible behavior)
        var inner = new InMemoryEventStore();
        var bad = Guid.CreateVersion7();
        await inner.AppendAsync(bad, 0, [new SagaStep1Started("bad")]);

        var store = new ThrowingLoadStore(inner, bad);
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
        // The interrupted saga's ResumeAsync throws a business exception → the framework
        // appends SagaFailed(reason) → classified Failed(reason), GetAsync does not
        // resurrect (failure = terminal)
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

        // The stream contains the framework SagaFailed(reason)
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<ResumeBoomStep1>();
        await Assert.That(events[1]).IsTypeOf<SagaFailed>();

        // Failure = terminal: after auto-stop, GetAsync does not resurrect
        await Task.Delay(300);
        var gone = await system.GetAsync<ResumeBoomSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }
}

/// <summary>Saga that keeps waiting for external commands after resume (Running classification verification).</summary>
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
        // No terminal state after recovery: do not advance; keep waiting for RunningCompleteCmd
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

/// <summary>Saga whose ResumeAsync throws a business exception — recovery API Failed classification verification.</summary>
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
