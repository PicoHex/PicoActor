namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Mutate is documented as a pure function that must not throw. When it does, the
// batch is ALREADY durable but in-memory state can no longer be trusted. The actor
// must fail loud (stop, registry removal) instead of retrying the leaked batch and
// failing every later command with a misleading ConcurrencyException.
// ═══════════════════════════════════════════════════════════

internal sealed record MutateBoomCreate : ICommand;

internal sealed record MutateBoomTrigger : ICommand;

internal sealed record MutateBoomHealthy : IDomainEvent;

internal sealed record MutateBoomPoison : IDomainEvent;

internal sealed class MutateBoomActor : EventSourcedActor
{
    public MutateBoomActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case MutateBoomCreate:
                RaiseEvent(new MutateBoomHealthy());
                return default;
            case MutateBoomTrigger:
                RaiseEvent(new MutateBoomPoison());
                return default;
        }
        return default;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is MutateBoomPoison)
            throw new InvalidOperationException("mutate boom");
    }
}

public sealed class ActorMutateFailureTests
{
    [Test]
    public async Task MutateFailure_StopsActor_NextCommandFailsLoudly_NotWithConcurrencyException()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<MutateBoomActor>(_ => new MutateBoomActor(), () => new MutateBoomActor());

        var actor = await system.CreateAsync<MutateBoomActor>(new MutateBoomCreate());
        await system.AskAsync<object?>(actor.Id, new MutateBoomCreate());

        // the caller observes the Mutate failure itself (not a derived stream error)
        var ex = await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new MutateBoomTrigger()))
            .Throws<InvalidOperationException>();
        await Assert.That(ex!.Message).Contains("mutate boom");

        // fail loud: the actor is stopped and removed from the registry, so no later
        // command can hit the misleading ConcurrencyException of the leaked batch
        await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new MutateBoomTrigger()))
            .Throws<KeyNotFoundException>();

        // the durable audit trail keeps the already-appended events
        var events = await store.LoadAsync(actor.Id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<MutateBoomHealthy>();
        await Assert.That(events[1]).IsTypeOf<MutateBoomPoison>();
    }
}
