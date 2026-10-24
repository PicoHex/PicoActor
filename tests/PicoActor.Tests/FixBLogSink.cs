namespace PicoActor.Tests;

/// <summary>
/// Recording ILogger for the FixB lifecycle tests: keeps every message and exposes a
/// deterministic signal for the "initialization failed" diagnostic, so a test can wait
/// for the failure-cleanup branch instead of guessing with a delay.
/// </summary>
internal sealed class FixBLogSink : ILogger
{
    private readonly ConcurrentQueue<string> _messages = new();

    private readonly TaskCompletionSource<bool> _initFailureLogged = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    /// <summary>Completes once a message mentioning the failed initialization was recorded.</summary>
    public Task InitFailureLogged => _initFailureLogged.Task;

    public IReadOnlyList<string> Messages => _messages.ToArray();

    private void Record(string message)
    {
        _messages.Enqueue(message);

        // Both cleanup branches (removed / left the replacement in place) report this
        // phrase — the test only needs to know the branch finished.
        if (message.Contains("initialization failed", StringComparison.OrdinalIgnoreCase))
            _initFailureLogged.TrySetResult(true);
    }

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(message);

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(message);

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(message.ToString());
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(message.ToString());
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose() { }
    }
}
