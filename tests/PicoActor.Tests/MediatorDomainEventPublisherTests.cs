using PicoActor.Abs;
using PicoMediator.Abs;

namespace PicoActor.Tests;

/// <summary>记录发布内容并可注入失败的假 IPublisher。</summary>
internal sealed class RecordingMediatorPublisher : IPublisher
{
    public List<object> Published = [];
    public int FailOnCall = -1;
    private int _callCount;

    public ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent
    {
        if (Interlocked.Increment(ref _callCount) == FailOnCall)
            throw new AggregateException(new InvalidOperationException("subscriber boom"));
        Published.Add(@event!);
        return default;
    }

    public ValueTask PublishParallel<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent => throw new NotImplementedException();
}

internal sealed record PubEventA(int Value) : IDomainEvent;

internal sealed record PubEventB(string Name) : IDomainEvent;

public sealed class MediatorDomainEventPublisherTests
{
    private static readonly Guid ActorId = Guid.CreateVersion7();

    [Test]
    public async Task PublishAsync_PublishesEachEvent_WithActorContext()
    {
        var publisher = new RecordingMediatorPublisher();
        var sut = new MediatorDomainEventPublisher(publisher);

        var events =
            (IReadOnlyList<IDomainEvent>)
                new IDomainEvent[] { new PubEventA(1), new PubEventB("x") };
        await sut.PublishAsync(ActorId, 7, events);

        await Assert.That(publisher.Published.Count).IsEqualTo(2);
        await Assert.That(publisher.Published[0]).IsTypeOf<PubEventA>();
        await Assert.That(publisher.Published[1]).IsTypeOf<PubEventB>();
    }

    [Test]
    public async Task PublishAsync_OneEventFailure_DoesNotStopOthers()
    {
        var publisher = new RecordingMediatorPublisher { FailOnCall = 2 }; // 第 2 个事件失败
        var sut = new MediatorDomainEventPublisher(publisher);

        var events =
            (IReadOnlyList<IDomainEvent>)
                new IDomainEvent[] { new PubEventA(1), new PubEventB("x"), new PubEventA(2) };
        await sut.PublishAsync(ActorId, 7, events); // 不抛——逐事件隔离

        await Assert.That(publisher.Published.Count).IsEqualTo(2); // 第 1、3 个到达
        await Assert.That(publisher.Published[0]).IsTypeOf<PubEventA>();
        await Assert.That(publisher.Published[1]).IsTypeOf<PubEventA>();
    }

    [Test]
    public async Task PublishAsync_EmptyBatch_IsNoOp()
    {
        var publisher = new RecordingMediatorPublisher();
        var sut = new MediatorDomainEventPublisher(publisher);

        await sut.PublishAsync(ActorId, 0, Array.Empty<IDomainEvent>());

        await Assert.That(publisher.Published.Count).IsEqualTo(0);
    }
}
