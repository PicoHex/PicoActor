namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Failure layering (saga spec §2.6 / §7.1): business failure = terminal
// (SagaFailed), infrastructure (IEventStore persistence) failure = NON-terminal
// (batch rolls back, saga stays Running, original exception propagates).
// Regression: the command path used to route flush failures into FailAsync,
// turning a transient store failure into a permanent terminal state.
// ═══════════════════════════════════════════════════════════

internal sealed record InfraStart : ICommand;

internal sealed record InfraFinish : ICommand;

internal sealed record InfraStarted : IDomainEvent;

internal sealed record InfraFinished : IDomainEvent;

internal sealed class InfraProbeSaga : SagaActor
{
    private bool _started;
    private bool _finished;

    public InfraProbeSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case InfraStart:
                if (!_started)
                    RaiseEvent(new InfraStarted());
                return new ValueTask<object?>("started");
            case InfraFinish:
                if (!_finished)
                    RaiseEvent(new InfraFinished());
                MarkComplete("done");
                return new ValueTask<object?>("done");
        }
        return default;
    }

    protected override ValueTask ResumeAsync() => default;

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case InfraStarted:
                _started = true;
                break;
            case InfraFinished:
                _finished = true;
                break;
        }
    }
}

internal sealed class BusinessBoomSaga : SagaActor
{
    public BusinessBoomSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        throw new InvalidOperationException("business boom");

    protected override ValueTask ResumeAsync() => default;

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>Store that throws on the Nth append (transient infrastructure failure), then recovers.</summary>
internal sealed class TransientAppendFailureStore : IEventStore
{
    private readonly InMemoryEventStore _inner = new();
    private int _callCount;

    public int FailOnCall { get; init; } = 1;

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        if (Interlocked.Increment(ref _callCount) == FailOnCall)
            throw new IOException("transient store failure");
        return _inner.AppendAsync(actorId, expectedVersion, events);
    }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
        _inner.LoadAsync(actorId);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => _inner.PeekFirstAsync(actorId);
}

/// <summary>Store whose every append fails (persistently down).</summary>
internal sealed class AlwaysFailAppendStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => throw new IOException("store down");

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
        new(Array.Empty<IDomainEvent>());

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
        ValueTask.FromResult<IDomainEvent?>(null);
}

public sealed class SagaFailureClassificationTests
{
    [Test]
    public async Task InfraFailure_OnBusinessBatch_IsNonTerminal_AndRetrySucceeds()
    {
        var store = new TransientAppendFailureStore { FailOnCall = 1 };
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<InfraProbeSaga>(_ => new InfraProbeSaga(), () => new InfraProbeSaga());

        var saga = await system.CreateAsync<InfraProbeSaga>(new InfraStart());

        // append #1 fails → the ORIGINAL store exception reaches the caller, not SagaExecutionException
        await Assert
            .That(async () => await system.AskAsync<string>(saga.Id, new InfraStart()))
            .Throws<IOException>();

        // non-terminal: nothing persisted, no terminal flag
        var afterFailure = await store.LoadAsync(saga.Id);
        await Assert.That(afterFailure.Count).IsEqualTo(0);
        await Assert.That(saga.IsFailed).IsFalse();
        await Assert.That(saga.IsCompleted).IsFalse();

        // retry after the store recovers: the saga advances and completes normally
        await system.AskAsync<string>(saga.Id, new InfraStart());
        var result = await system.AskAsync<string>(saga.Id, new InfraFinish());
        await Assert.That(result).IsEqualTo("done");

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[0]).IsTypeOf<InfraStarted>();
        await Assert.That(events[1]).IsTypeOf<InfraFinished>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
    }

    [Test]
    public async Task BusinessFailure_WhenSagaFailedAppendAlsoFails_PropagatesOriginalException()
    {
        var store = new AlwaysFailAppendStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<BusinessBoomSaga>(
            _ => new BusinessBoomSaga(),
            () => new BusinessBoomSaga()
        );

        var saga = await system.CreateAsync<BusinessBoomSaga>(new InfraStart());

        // OnMessageAsync throws a business exception; persisting SagaFailed fails too →
        // spec §5.2: the saga stays Running and the ORIGINAL exception propagates
        var ex = await Assert
            .That(async () => await system.AskAsync<string>(saga.Id, new InfraStart()))
            .Throws<InvalidOperationException>();
        await Assert.That(ex!.Message).Contains("business boom");
        await Assert.That(saga.IsFailed).IsFalse();
    }

    [Test]
    public async Task ResumeBusinessFailure_WhenSagaFailedAppendAlsoFails_PropagatesOriginalException()
    {
        var sagaId = Guid.CreateVersion7();
        // Seeded stream → Version > 0 → the recovery path runs ResumeAsync on GetAsync;
        // every append fails (store down) — spec §5.2 covers BOTH entry paths.
        var store = new FailAppendStore(new IDomainEvent[] { new ResumeBoomStep1() });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<ResumeBoomSaga>(_ => new ResumeBoomSaga(), () => new ResumeBoomSaga());

        var ex = await Assert
            .That(async () => await system.GetAsync<ResumeBoomSaga>(sagaId))
            .Throws<InvalidOperationException>();
        await Assert.That(ex!.Message).Contains("resume boom");
    }
}
