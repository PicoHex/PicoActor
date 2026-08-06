using PicoActor.Abs;
using PicoMediator.Abs;

namespace PicoActor.Abs.Tests;

internal sealed record SubscriptionSampleEvent(int Value) : IDomainEvent;

public sealed class EventSubscriptionAbstractionTests
{
    [Test]
    public async Task TypedEnvelope_CarriesActorContextAndEvent()
    {
        var id = Guid.CreateVersion7();
        var e = new SubscriptionSampleEvent(5);
        var envelope = new DomainEventEnvelope<SubscriptionSampleEvent>(id, 42, e);

        await Assert.That(envelope.ActorId).IsEqualTo(id);
        await Assert.That(envelope.Version).IsEqualTo(42ul);
        await Assert.That(ReferenceEquals(envelope.Event, e)).IsTrue();
    }

    [Test]
    public async Task TransportEnvelope_IsIEventButNotIDomainEvent()
    {
        var e = new SubscriptionSampleEvent(5);
        var envelope = new DomainEventEnvelope(Guid.CreateVersion7(), 1ul, e);

        await Assert.That(envelope).IsAssignableTo<IEvent>();
        await Assert.That(envelope is IDomainEvent).IsFalse();
        await Assert.That(ReferenceEquals(envelope.Event, e)).IsTrue();
    }

    [Test]
    public async Task Subscriber_HandlesTypedEnvelope()
    {
        var handler = new SampleSubscriber();
        var envelope = new DomainEventEnvelope<SubscriptionSampleEvent>(Guid.CreateVersion7(), 3, new SubscriptionSampleEvent(9));

        await handler.Handle(envelope, new NoopSender(), CancellationToken.None);

        await Assert.That(ReferenceEquals(handler.Last, envelope)).IsTrue();
        await Assert.That(handler.Last!.Event.Value).IsEqualTo(9);
    }

    private sealed class SampleSubscriber : IDomainEventSubscriber<SubscriptionSampleEvent>
    {
        public DomainEventEnvelope<SubscriptionSampleEvent>? Last { get; private set; }

        public ValueTask Handle(
            DomainEventEnvelope<SubscriptionSampleEvent> envelope,
            ICommandSender sender,
            CancellationToken ct = default
        )
        {
            Last = envelope;
            return default;
        }
    }

    private sealed class NoopSender : ICommandSender
    {
        public void Send(Guid actorId, ICommand command) { }
        public ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command) => default;
        public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command) => default;
    }
}
