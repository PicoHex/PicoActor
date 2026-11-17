using System.Collections.Concurrent;
using PicoLog.Abs;

namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// Recording <see cref="ILogger"/>: keeps every message so a test can assert that a warning the store
/// owes its operator was actually emitted (the seam is silent by default, so "nothing was logged" is
/// not evidence of anything unless a logger was attached).
/// </summary>
internal sealed class RecordingLogger : ILogger
{
    private readonly ConcurrentQueue<string> _messages = new();

    internal IReadOnlyList<string> Messages => _messages.ToArray();

    internal bool Contains(string fragment) =>
        Messages.Any(message => message.Contains(fragment, StringComparison.OrdinalIgnoreCase));

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _messages.Enqueue(message);

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _messages.Enqueue(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _messages.Enqueue(message);

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _messages.Enqueue(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _messages.Enqueue(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _messages.Enqueue(message.ToString());
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _messages.Enqueue(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _messages.Enqueue(message.ToString());
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        internal static readonly NullScope Instance = new();

        public void Dispose() { }
    }
}
