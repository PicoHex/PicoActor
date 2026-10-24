namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A5 — "this instance came from replay" was inferred from Version > 0, which is only
// true for sagas that take no construction command. A saga seeded in its constructor
// is NOT a rebuild, yet it resumed on creation; and ReplayEvents kept the constructor's
// uncommitted events, so the rebuild's first flush computed a stale expectedVersion and
// the saga was permanently stuck on ConcurrencyException(1, 2).
// ═══════════════════════════════════════════════════════════

internal sealed record FixASeedCreate : ICommand;

internal sealed record FixASeeded(int Value) : IDomainEvent;

internal sealed record FixAResumed : IDomainEvent;

/// <summary>Saga that raises a construction-time seed event (born with state).</summary>
internal sealed class FixASeededSaga : SagaActor
{
    private bool _resumed;

    public FixASeededSaga()
    {
        RaiseEvent(new FixASeeded(7));
    }

    /// <summary>How many times THIS instance ran the recovery hook.</summary>
    public int ResumeCalls { get; private set; }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override ValueTask ResumeAsync()
    {
        ResumeCalls++;
        if (!_resumed)
        {
            _resumed = true;
            RaiseEvent(new FixAResumed());
        }
        return default;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is FixAResumed)
            _resumed = true;
    }
}

/// <summary>
/// Reaches a terminal state during initialization (framework event restored by replay):
/// the auto-stop must fire after init completes, not from inside OnReadyAsync.
/// </summary>
internal sealed class FixATerminalOnInitSaga : SagaActor
{
    public FixATerminalOnInitSaga()
    {
        RaiseEvent(new FixASeeded(1));
    }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override ValueTask ResumeAsync() => default;

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>
/// Records RequestStop and whether initialization had already completed when it was
/// called. Everything else is out of scope for this probe.
/// </summary>
internal sealed class FixARecordingSystem : IActorSystem
{
    public Task? InitTask { get; set; }
    public int StopRequests { get; private set; }
    public bool StopRequestedAfterInit { get; private set; }

    public void RequestStop(Guid id)
    {
        StopRequests++;
        StopRequestedAfterInit = InitTask?.IsCompleted == true;
    }

    public ValueTask StopAsync(Guid id) => default;

    public void Register<T>(Func<ICommand, T> createFactory, Func<T>? rebuildFactory = null)
        where T : IActor => throw new NotSupportedException();

    public ValueTask<T> CreateAsync<T>(ICommand command)
        where T : IActor => throw new NotSupportedException();

    public ValueTask<T?> GetAsync<T>(Guid id)
        where T : IActor => throw new NotSupportedException();

    public void Send(Guid id, ICommand command) => throw new NotSupportedException();

    public ValueTask<TResult> AskAsync<TResult>(Guid id, ICommand command) =>
        throw new NotSupportedException();

    public ValueTask StopAllAsync() => throw new NotSupportedException();

    public ValueTask<IReadOnlyList<Guid>> FindAggregateIds(
        string firstEventType,
        Func<IDomainEvent, bool> firstEventMatch
    ) => throw new NotSupportedException();

    public ValueTask<IReadOnlyList<SagaResumeResult>> ResumeInterruptedSagasAsync<TSaga>(
        string firstEventType,
        Func<IDomainEvent, bool>? firstEventMatch = null,
        Action<Exception, Guid>? onItemError = null
    )
        where TSaga : SagaActor => throw new NotSupportedException();

    public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command)
        where TSaga : SagaActor => throw new NotSupportedException();
}

public sealed class FixAReplayMarkerTests
{
    /// <summary>
    /// A saga with construction-time events is a CREATION, not a rebuild: the recovery
    /// hook must not run.
    /// </summary>
    [Test]
    public async Task ConstructorSeededSaga_DoesNotResumeOnCreation()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixASeededSaga>(_ => new FixASeededSaga(), () => new FixASeededSaga());

        var saga = await system.CreateAsync<FixASeededSaga>(new FixASeedCreate());

        await Assert.That(saga.ResumeCalls).IsEqualTo(0);

        // Only the constructor's seed was persisted — the resume did not run.
        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(1);
        await Assert.That(events[0]).IsTypeOf<FixASeeded>();
        await Assert.That(saga.Version).IsEqualTo(1UL);

        await system.StopAsync(saga.Id);
    }

    /// <summary>
    /// The rebuild is a real replay: the recovery hook runs exactly once and the
    /// constructor's discarded events must not skew the flush's expectedVersion.
    /// </summary>
    [Test]
    public async Task RebuiltSeededSaga_ResumesOnce_WithoutConcurrencyException()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixASeededSaga>(_ => new FixASeededSaga(), () => new FixASeededSaga());

        var created = await system.CreateAsync<FixASeededSaga>(new FixASeedCreate());
        var id = created.Id;
        await system.StopAsync(id);

        FixASeededSaga? rebuilt = null;
        Exception? rebuildError = null;
        try
        {
            rebuilt = await system.GetAsync<FixASeededSaga>(id);
        }
        catch (Exception ex)
        {
            rebuildError = ex;
        }

        await Assert.That(rebuildError).IsNull();
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.ResumeCalls).IsEqualTo(1);

        // seed (creation) + resumed (rebuild) — no duplicated constructor seed.
        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<FixASeeded>();
        await Assert.That(events[1]).IsTypeOf<FixAResumed>();
        await Assert.That(rebuilt.Version).IsEqualTo(2UL);

        await system.StopAsync(id);
    }

    /// <summary>
    /// A saga that is terminal after replay still auto-stops — but only once init has
    /// completed, so the runtime never loses the registry entry mid-initialization.
    /// </summary>
    [Test]
    public async Task TerminalSagaDuringInit_AutoStopsAfterInitCompletes()
    {
        var system = new FixARecordingSystem();
        var saga = new FixATerminalOnInitSaga();
        var id = Guid.CreateVersion7();
        saga.AttachToSystem(id, system);
        system.InitTask = saga.InitCompletedTask;

        // Replay a stream that already carries the framework terminal event.
        ((IEventSourcedActor)saga).ReplayEvents([new FixASeeded(1), new SagaCompleted("done")]);
        saga.SignalReady();

        await saga.InitCompletedTask;

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (system.StopRequests == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(5);

        await Assert.That(saga.IsCompleted).IsTrue();
        await Assert.That(system.StopRequests).IsEqualTo(1);
        await Assert.That(system.StopRequestedAfterInit).IsTrue();

        saga.SignalStop();
        try
        {
            await saga.StopAsync();
        }
        catch (Exception)
        { /* loop may fault on the probe system; cleanup only */
        }
    }

    /// <summary>No behavior change for ordinary sagas: replay still resumes them.</summary>
    [Test]
    public async Task OrdinaryInterruptedSaga_StillResumesOnRebuild()
    {
        var store = new InMemoryEventStore();
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("ordinary")]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var results = await system.ResumeInterruptedSagasAsync<TestSaga>(nameof(SagaStep1Started));

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);
    }
}
