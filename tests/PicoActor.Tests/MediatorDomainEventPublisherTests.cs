// TUnit0055: the ODE diagnostic tests deliberately redirect Console.Error (to verify the no-logger stderr diagnostic); finally restores it.
// [NotInParallel]: Console.Error is process-global — the redirecting tests must not run
// concurrently with each other, or one test's restore races the other's capture.
#pragma warning disable TUnit0055

namespace PicoActor.Tests;

/// <summary>Fake IPublisher that records published events and can inject failures.</summary>
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

/// <summary>Publish throws ObjectDisposedException — the captive-dependency scenario (Mediator bound to a disposed scope).</summary>
internal sealed class ThrowingDisposedPublisher : IPublisher
{
    public ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent => throw new ObjectDisposedException("SvcScope");

    public ValueTask PublishParallel<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent => throw new NotImplementedException();
}

/// <summary>Fake ILogger that records log messages.</summary>
internal sealed class RecordingLogSink : ILogger
{
    public List<string> Messages = [];

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Messages.Add(message);

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Messages.Add(message);

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Messages.Add(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message.ToString());
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Messages.Add(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message.ToString());
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose() { }
    }
}

internal sealed record PubEventA(int Value) : IDomainEvent;

internal sealed record PubEventB(string Name) : IDomainEvent;

[NotInParallel]
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
        await Assert.That(publisher.Published[0]).IsTypeOf<DomainEventEnvelope>();
        var envelope0 = (DomainEventEnvelope)publisher.Published[0];
        await Assert.That(envelope0.ActorId).IsEqualTo(ActorId);
        await Assert.That(envelope0.Version).IsEqualTo(7ul);
        await Assert.That(envelope0.Event).IsTypeOf<PubEventA>();
        var envelope1 = (DomainEventEnvelope)publisher.Published[1];
        await Assert.That(envelope1.Event).IsTypeOf<PubEventB>();
    }

    [Test]
    public async Task PublishAsync_OneEventFailure_DoesNotStopOthers()
    {
        var publisher = new RecordingMediatorPublisher { FailOnCall = 2 }; // the 2nd event fails
        var sut = new MediatorDomainEventPublisher(publisher);

        var events =
            (IReadOnlyList<IDomainEvent>)
                new IDomainEvent[] { new PubEventA(1), new PubEventB("x"), new PubEventA(2) };
        await sut.PublishAsync(ActorId, 7, events); // does not throw — per-event isolation

        await Assert.That(publisher.Published.Count).IsEqualTo(2); // events 1 and 3 arrive
        await Assert
            .That(((DomainEventEnvelope)publisher.Published[0]).Event)
            .IsTypeOf<PubEventA>();
        await Assert
            .That(((DomainEventEnvelope)publisher.Published[1]).Event)
            .IsTypeOf<PubEventA>();
    }

    [Test]
    public async Task PublishAsync_EmptyBatch_IsNoOp()
    {
        var publisher = new RecordingMediatorPublisher();
        var sut = new MediatorDomainEventPublisher(publisher);

        await sut.PublishAsync(ActorId, 0, Array.Empty<IDomainEvent>());

        await Assert.That(publisher.Published.Count).IsEqualTo(0);
    }

    [Test]
    public async Task PublishAsync_ObjectDisposedException_LogsRootScopeGuidance()
    {
        var publisher = new ThrowingDisposedPublisher();
        var log = new RecordingLogSink();
        var sut = new MediatorDomainEventPublisher(publisher, log);

        await sut.PublishAsync(ActorId, 7, new IDomainEvent[] { new PubEventA(1) });

        // Dedicated diagnostic: hints at the captive dependency (Mediator bound to a disposed scope)
        await Assert.That(log.Messages.Count).IsEqualTo(1);
        await Assert.That(log.Messages[0]).Contains("root scope");
        await Assert.That(log.Messages[0]).Contains("PubEventA");
    }

    [Test]
    public async Task PublishAsync_SubscriberFailure_NoLogger_WritesDiagnostic()
    {
        // A subscriber failure with no logger configured must not be silent: the events are
        // durable but downstream consumers missed them, so the failure has to be observable.
        var publisher = new RecordingMediatorPublisher { FailOnCall = 1 };
        var sut = new MediatorDomainEventPublisher(publisher);

        var original = Console.Error;
        try
        {
            using var writer = new StringWriter();
            Console.SetError(writer);

            await sut.PublishAsync(ActorId, 7, new IDomainEvent[] { new PubEventA(1) });

            var output = writer.ToString();
            await Assert.That(output).Contains("[PicoActor]");
            await Assert.That(output).Contains("PubEventA");
        }
        finally
        {
            Console.SetError(original);
        }
    }

    [Test]
    public async Task PublishAsync_ObjectDisposedException_NoLogger_WritesDiagnostic()
    {
        var publisher = new ThrowingDisposedPublisher();
        var sut = new MediatorDomainEventPublisher(publisher);

        var original = Console.Error;
        try
        {
            using var writer = new StringWriter();
            Console.SetError(writer);

            await sut.PublishAsync(ActorId, 7, new IDomainEvent[] { new PubEventA(1) });

            var output = writer.ToString();
            await Assert.That(output).Contains("[PicoActor]");
            await Assert.That(output).Contains("root scope");
        }
        finally
        {
            Console.SetError(original);
        }
    }
}
