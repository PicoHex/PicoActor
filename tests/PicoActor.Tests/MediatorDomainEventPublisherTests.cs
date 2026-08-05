using PicoActor.Abs;
using PicoLog.Abs;
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

/// <summary>Publish 抛 ObjectDisposedException——captive dependency 场景(Mediator 绑定已释放 scope)。</summary>
internal sealed class ThrowingDisposedPublisher : IPublisher
{
    public ValueTask Publish<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent => throw new ObjectDisposedException("SvcScope");

    public ValueTask PublishParallel<TEvent>(TEvent @event, CancellationToken ct = default)
        where TEvent : IEvent => throw new NotImplementedException();
}

/// <summary>记录日志消息的假 ILogger。</summary>
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

    [Test]
    public async Task PublishAsync_ObjectDisposedException_LogsRootScopeGuidance()
    {
        var publisher = new ThrowingDisposedPublisher();
        var log = new RecordingLogSink();
        var sut = new MediatorDomainEventPublisher(publisher, log);

        await sut.PublishAsync(ActorId, 7, new IDomainEvent[] { new PubEventA(1) });

        // 专门的诊断信息:提示 captive dependency(Mediator 绑定已释放的 scope)
        await Assert.That(log.Messages.Count).IsEqualTo(1);
        await Assert.That(log.Messages[0]).Contains("root scope");
        await Assert.That(log.Messages[0]).Contains("PubEventA");
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
