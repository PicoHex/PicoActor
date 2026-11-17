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
    // The LEVEL is kept, not just the text (D11): a test that asserts "this store warned" must not be
    // satisfied by any line at all, and a test that asserts "nothing was warned" must be able to say so
    // while an informational start-up line is present.
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    internal IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries.ToArray();

    internal IReadOnlyList<string> Messages => [.. Entries.Select(entry => entry.Message)];

    internal bool Contains(string fragment) =>
        Messages.Any(message => message.Contains(fragment, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Did anything at <see cref="LogLevel.Warning"/> or worse get logged? The store's contract is
    /// "no warning" far more often than "some warning", and the start-up line is Info, so this is the
    /// question that separates the two.
    /// </summary>
    internal bool AnyAtOrAbove(LogLevel level) => Entries.Any(entry => entry.Level >= level);

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _entries.Enqueue((logLevel, message));

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _entries.Enqueue((logLevel, message));
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _entries.Enqueue((logLevel, message));

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _entries.Enqueue((logLevel, message));
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _entries.Enqueue((logLevel, message.ToString()));

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _entries.Enqueue((logLevel, message.ToString()));
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => _entries.Enqueue((logLevel, message.ToString()));

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        _entries.Enqueue((logLevel, message.ToString()));
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        internal static readonly NullScope Instance = new();

        public void Dispose() { }
    }
}
