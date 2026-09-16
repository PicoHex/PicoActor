namespace PicoActor.Tests;

/// <summary>Actor whose Mutate throws — verifies resource cleanup on the GetAsync replay-failure path.</summary>
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

/// <summary>Store whose append throws — verifies a saga stays retryable (non-terminal) when recovery fails.</summary>
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

        // Cleanup verification: repeated GetAsync must not accumulate (no KeyNotFoundException or leaks)
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

        // Append fails while resume persists SagaStep2Done + SagaCompleted → the original store exception propagates
        await Assert
            .That(async () => await system.GetAsync<TestSaga>(sagaId))
            .Throws<IOException>();
        await Assert.That(store.AppendAttempts).IsEqualTo(1);

        // Non-terminal: the saga produced no SagaFailed event, so it can be retried
        // once the store recovers (FailAppendStore.LoadAsync returns fixed events —
        // a retry still fails, but the exception type proves it went through the
        // infrastructure path)
    }
}
