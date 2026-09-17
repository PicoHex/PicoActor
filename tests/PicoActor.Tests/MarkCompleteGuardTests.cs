namespace PicoActor.Tests;

internal sealed record GuardProbeEvent : IDomainEvent;

/// <summary>Calls MarkComplete inside Mutate — the legacy migration-era pattern that must fail loudly.</summary>
internal sealed class MutateMarkCompleteSaga : SagaActor
{
    public MutateMarkCompleteSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        MarkComplete(); // legacy pattern: marking complete inside Mutate
    }
}

internal sealed record PendingStart : ICommand;

internal sealed record PendingFinish : ICommand;

internal sealed record PendingStep1 : IDomainEvent;

internal sealed record PendingStep2 : IDomainEvent;

/// <summary>First command only advances; second advances + MarkComplete — verifies pending is cleared after a failed flush.</summary>
internal sealed class PendingProbeSaga : SagaActor
{
    public PendingProbeSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case PendingStart:
                RaiseEvent(new PendingStep1());
                return default;
            case PendingFinish:
                RaiseEvent(new PendingStep2());
                MarkComplete("done");
                return new ValueTask<object?>("done");
            default:
                return default;
        }
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>MarkComplete called twice within the same message — only one SagaCompleted may be produced.</summary>
internal sealed record DoubleCompleteCmd : ICommand;

internal sealed class DoubleCompleteSaga : SagaActor
{
    public DoubleCompleteSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is DoubleCompleteCmd)
        {
            RaiseEvent(new PendingStep1());
            MarkComplete("first");
            MarkComplete("second"); // idempotent: no second completion event is appended
            return new ValueTask<object?>("first");
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>Store that throws on the Nth AppendAsync call — pending-failure clearing verification.</summary>
internal sealed class FailOnceStore : IEventStore
{
    private readonly InMemoryEventStore _inner = new();
    private int _callCount;

    public int FailOnCall { get; init; } = 2;

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        if (Interlocked.Increment(ref _callCount) == FailOnCall)
            throw new IOException("store down");
        return _inner.AppendAsync(actorId, expectedVersion, events);
    }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
        _inner.LoadAsync(actorId);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => _inner.PeekFirstAsync(actorId);
}

public sealed class MarkCompleteGuardTests
{
    // ═══════════════════════════════════════════════════════════
    // I2: MarkComplete location constraint — calling from Mutate must fail loudly
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task MarkComplete_InMutate_ThrowsInvalidOperation()
    {
        var actor = new MutateMarkCompleteSaga();
        actor.AttachToSystem(Guid.CreateVersion7());
        actor.SignalReady();

        try
        {
            var ex = await Assert
                .That(() =>
                    ((IEventSourcedActor)actor).ReplayEvents(
                        new IDomainEvent[] { new GuardProbeEvent() }
                    )
                )
                .Throws<InvalidOperationException>();

            await Assert.That(ex!.Message).Contains("MarkComplete");
        }
        finally
        {
            await actor.StopAsync();
        }
    }

    // ═══════════════════════════════════════════════════════════
    // M1: pending is cleared after a failed flush — the next flush appends no spurious SagaCompleted
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task FailedFlush_ClearsPending_NoSpuriousSagaCompletedOnNextFlush()
    {
        var store = new FailOnceStore { FailOnCall = 2 }; // the 2nd append (PendingFinish batch) fails
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<PendingProbeSaga>(
            _ => new PendingProbeSaga(),
            () => new PendingProbeSaga()
        );

        var saga = await system.CreateAsync<PendingProbeSaga>(new PendingStart());
        await system.AskAsync<object?>(saga.Id, new PendingStart()); // append #1 OK

        // append #2 fails: the whole [PendingStep2 + SagaCompleted] batch is dropped and
        // pending is cleared. Infrastructure failure = NON-terminal (spec §2.6/§7.1):
        // the original store exception propagates and the saga stays Running.
        await Assert
            .That(async () => await system.AskAsync<string>(saga.Id, new PendingFinish()))
            .Throws<IOException>();

        // A later flush must not carry a stale pending completion: a neutral command
        // (no events, no MarkComplete) triggers no append at all.
        await system.AskAsync<object?>(saga.Id, new GetSagaStep());
        var afterNeutral = await store.LoadAsync(saga.Id);
        await Assert.That(afterNeutral.Count).IsEqualTo(1);
        await Assert.That(afterNeutral[0]).IsTypeOf<PendingStep1>();
        await Assert.That(saga.IsCompleted).IsFalse();

        // Retry after the store recovers: exactly one SagaCompleted is appended.
        await system.AskAsync<string>(saga.Id, new PendingFinish());
        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[0]).IsTypeOf<PendingStep1>();
        await Assert.That(events[1]).IsTypeOf<PendingStep2>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
        await Assert.That(events.OfType<SagaCompleted>().Count()).IsEqualTo(1);
    }

    // ═══════════════════════════════════════════════════════════
    // M2: repeated MarkComplete within one message is idempotent — only one SagaCompleted is appended
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task MarkComplete_RepeatedInSameMessage_AppendsSingleCompletion()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<DoubleCompleteSaga>(
            _ => new DoubleCompleteSaga(),
            () => new DoubleCompleteSaga()
        );

        var saga = await system.CreateAsync<DoubleCompleteSaga>(new DoubleCompleteCmd());
        await system.AskAsync<string>(saga.Id, new DoubleCompleteCmd());

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.OfType<SagaCompleted>().Count()).IsEqualTo(1);
        await Assert.That(events[^1]).IsTypeOf<SagaCompleted>();
    }
}
