namespace PicoActor.Abs.Tests;

internal sealed record SampleEvent(int Value) : IDomainEvent;

public sealed class DomainEventIsEventTests
{
    [Test]
    public async Task DomainEvent_IsMediatorEvent()
    {
        IDomainEvent domainEvent = new SampleEvent(42);
        await Assert.That(domainEvent).IsAssignableTo<IEvent>();
    }
}
