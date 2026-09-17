namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Registration must fail loudly for shapes the runtime cannot actually serve:
//  - a hand-rolled IEventSourcedActor (not EventSourcedActor) passes every
//    interface gate but never gets event-store wiring → events silently never
//    persist (data loss with no error).
//  - a non-Actor IActor implementation fails late with a bare InvalidCastException
//    and no hint about the real contract.
// Both are rejected at the composition root (Register) with an actionable message.
// ═══════════════════════════════════════════════════════════

/// <summary>Passes every interface gate, never gets persistence wiring.</summary>
internal sealed class HandRolledEsActor : Actor, IEventSourcedActor
{
    public HandRolledEsActor() { }

    public ulong Version { get; private set; }

    public void CommitEvents() { }

    public void ReplayEvents(IReadOnlyList<IDomainEvent> events) => Version = (ulong)events.Count;

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;
}

/// <summary>Implements IActor without deriving from Actor — cannot be served by the runtime.</summary>
internal sealed class BareActor : IActor
{
    public Guid Id => Guid.Empty;
}

internal sealed record ValidationProbe : ICommand;

public sealed class ActorRegistrationValidationTests
{
    private static ActorSystem CreateSystem() =>
        new(new ActorSystemOptions { EventStore = new InMemoryEventStore() });

    [Test]
    public async Task Register_HandRolledEventSourcedActor_ThrowsActionable()
    {
        var system = CreateSystem();

        var ex = await Assert
            .That(() =>
                system.Register<HandRolledEsActor>(
                    _ => new HandRolledEsActor(),
                    () => new HandRolledEsActor()
                )
            )
            .Throws<InvalidOperationException>();

        await Assert.That(ex!.Message).Contains(nameof(HandRolledEsActor));
        await Assert.That(ex.Message).Contains("EventSourcedActor");
    }

    [Test]
    public async Task Register_NonActorImplementation_ThrowsActionable()
    {
        var system = CreateSystem();

        var ex = await Assert
            .That(() => system.Register<BareActor>(_ => new BareActor(), () => new BareActor()))
            .Throws<InvalidOperationException>();

        await Assert.That(ex!.Message).Contains(nameof(BareActor));
        await Assert.That(ex.Message).Contains("Actor");
    }

    [Test]
    public async Task Register_ConcreteEventSourcedActor_StillWorks()
    {
        var system = CreateSystem();
        system.Register<Counter>(
            cmd =>
                cmd switch
                {
                    CreateCounter c => new Counter(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new Counter()
        );

        var actor = await system.CreateAsync<Counter>(new CreateCounter(7));
        await Assert.That(await system.AskAsync<int>(actor.Id, new GetValue())).IsEqualTo(7);
    }
}
