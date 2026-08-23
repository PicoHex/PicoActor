using PicoActor.Abs;
using PicoDI;

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
