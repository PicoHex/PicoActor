namespace PicoActor.Tests;

internal sealed record DiProbeCmd : ICommand;

internal sealed record DiProbeEvent(int Value) : IDomainEvent;

internal sealed class DiProbeActor : EventSourcedActor
{
    public DiProbeActor() { }

    public DiProbeActor(DiProbeCmd cmd)
        : base(cmd) { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is DiProbeCmd)
            RaiseEvent(new DiProbeEvent(1));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class PicoActorDiExtensionsTests
{
    [Test]
    public async Task AddPicoActor_WithPublisher_WiresEventOutflow()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var publisher = new RecordingMediatorPublisher();
        container.AddPicoActor(publisher);
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<DiProbeActor>(
            cmd => new DiProbeActor((DiProbeCmd)cmd),
            () => new DiProbeActor()
        );

        var actor = await system.CreateAsync<DiProbeActor>(new DiProbeCmd());
        await system.AskAsync<object?>(actor.Id, new DiProbeCmd());

        // Events flowed to the publisher as envelopes: one batch from the construction-time flush, one from the mailbox command
        await Assert.That(publisher.Published.Count).IsEqualTo(2);
        await Assert.That(publisher.Published[0]).IsTypeOf<DomainEventEnvelope>();
        await Assert
            .That(((DomainEventEnvelope)publisher.Published[0]).Event)
            .IsTypeOf<DiProbeEvent>();
        await Assert
            .That(((DomainEventEnvelope)publisher.Published[1]).Event)
            .IsTypeOf<DiProbeEvent>();
    }
}

public sealed class PicoActorDiExtensionsStoreTests
{
    /// <summary>
    /// The default store registration must not clobber a store the application already
    /// registered on the container (documented precedence: a container-registered
    /// IEventStore wins over the implicit InMemory default).
    /// </summary>
    [Test]
    public async Task AddPicoActor_NoArgs_PreservesContainerRegisteredStore()
    {
        var provided = new InMemoryEventStore();
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.Register(typeof(IEventStore), _ => (IEventStore)provided, SvcLifetime.Singleton);

        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var resolved = scope.GetService(typeof(IEventStore));

        await Assert.That(ReferenceEquals(resolved, provided)).IsTrue();
    }

    /// <summary>Same precedence rule for the publisher overload (explicit publisher, implicit store).</summary>
    [Test]
    public async Task AddPicoActor_WithPublisher_PreservesContainerRegisteredStore()
    {
        var provided = new InMemoryEventStore();
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.Register(typeof(IEventStore), _ => (IEventStore)provided, SvcLifetime.Singleton);

        container.AddPicoActor(new RecordingMediatorPublisher());
        container.Build();
        await using var scope = container.CreateScope();

        var resolved = scope.GetService(typeof(IEventStore));

        await Assert.That(ReferenceEquals(resolved, provided)).IsTrue();
    }

    /// <summary>Same precedence rule for the PicoCfg-bound overload.</summary>
    [Test]
    public async Task AddPicoActor_WithConfig_PreservesContainerRegisteredStore()
    {
        var provided = new InMemoryEventStore();
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.Register(typeof(IEventStore), _ => (IEventStore)provided, SvcLifetime.Singleton);

        container.AddPicoActor(
            new ActorConfig { EventStore = new EventStoreConfig { Type = "InMemory" } }
        );
        container.Build();
        await using var scope = container.CreateScope();

        var resolved = scope.GetService(typeof(IEventStore));

        await Assert.That(ReferenceEquals(resolved, provided)).IsTrue();
    }

    /// <summary>Explicit-store overload: the passed instance is explicit intent and always wins.</summary>
    [Test]
    public async Task AddPicoActor_WithCustomStore_OverridesEarlierContainerRegistration()
    {
        var earlier = new InMemoryEventStore();
        var explicitStore = new InMemoryEventStore();
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.Register(typeof(IEventStore), _ => (IEventStore)earlier, SvcLifetime.Singleton);

        container.AddPicoActor(explicitStore);
        container.Build();
        await using var scope = container.CreateScope();

        var resolved = scope.GetService(typeof(IEventStore));

        await Assert.That(ReferenceEquals(resolved, explicitStore)).IsTrue();
    }

    [Test]
    public async Task AddPicoActor_NoArgs_RegistersDefaultInMemoryStore()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var store = scope.GetService(typeof(IEventStore));

        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }

    [Test]
    public async Task AddPicoActor_WithCustomStore_UsesProvidedInstance()
    {
        var provided = new InMemoryEventStore();
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoActor(provided);
        container.Build();
        await using var scope = container.CreateScope();

        var resolved = scope.GetService(typeof(IEventStore));

        await Assert.That(ReferenceEquals(resolved, provided)).IsTrue();
    }
}
