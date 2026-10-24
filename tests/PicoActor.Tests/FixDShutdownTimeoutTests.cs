namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// D — bounded shutdown (I9). A message handler that ignores its
//     actor's stop token used to hang host disposal FOREVER:
//     StopAllAsync awaited every actor serially and StopAsync(id)
//     awaited the loop with no bound. ActorSystemOptions.StopTimeout
//     now bounds that wait; the default (null) keeps the unbounded
//     semantics, including rethrowing a faulted loop's exception.
// ═══════════════════════════════════════════════════════════

internal sealed record FixDCreate : ICommand;

/// <summary>
/// Starts a handler turn that parks until <paramref name="Release"/> is completed. A release
/// nobody completes models a handler that never observes its actor's stop token.
/// </summary>
internal sealed record FixDHang(
    TaskCompletionSource<bool> Entered,
    TaskCompletionSource<bool> Release
) : ICommand;

/// <summary>
/// Handler that parks on a task nothing completes and never observes
/// <c>StopToken</c> — the exact shape that hung host shutdown.
/// </summary>
internal sealed class FixDHangActor : Actor
{
    public FixDHangActor(FixDCreate cmd)
        : base(cmd) { }

    public FixDHangActor() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is FixDHang hang)
        {
            hang.Entered.TrySetResult(true);
            await hang.Release.Task.ConfigureAwait(false);
        }

        return null;
    }
}

/// <summary>Two gates for the failing-initialization actor: "loop is in init" and "fail now".</summary>
internal sealed record FixDInitGate(
    TaskCompletionSource<Guid> Entered,
    TaskCompletionSource<bool> Release
);

/// <summary>
/// Actor whose initialization parks (inside the loop, ignoring the stop token) and then
/// throws: stopping it while parked awaits a loop that FAULTS — the default
/// (no <c>StopTimeout</c>) path must still surface that fault.
/// </summary>
internal sealed class FixDInitFaultActor : Actor
{
    private readonly FixDInitGate _gate;

    public FixDInitFaultActor(FixDCreate cmd, FixDInitGate gate)
        : base(cmd) => _gate = gate;

    public FixDInitFaultActor()
        : this(new FixDCreate(), new FixDInitGate(new(), new())) { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask OnReadyAsync()
    {
        _gate.Entered.TrySetResult(Id);
        await _gate.Release.Task.ConfigureAwait(false);
        throw new IOException("init boom");
    }
}

/// <summary>
/// Records every report with its level, so a test can pin "the shutdown reported a
/// WARNING" instead of merely "some message was written".
/// </summary>
internal sealed class FixDRecordingLogSink : ILogger
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    public IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries.ToArray();

    public bool HasWarning(string fragment) =>
        Entries.Any(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains(fragment, StringComparison.Ordinal)
        );

    private void Record(LogLevel level, string message) => _entries.Enqueue((level, message));

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => NullScope.Instance;

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(logLevel, message);

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(logLevel, message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(logLevel, message);

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(logLevel, message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(logLevel, message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(logLevel, message.ToString());
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Record(logLevel, message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Record(logLevel, message.ToString());
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose() { }
    }
}

public sealed class FixDShutdownTimeoutTests
{
    /// <summary>
    /// I9: a handler that never observes its stop token used to make host shutdown hang
    /// forever. With <c>StopTimeout</c> configured the mass stop returns, reports the
    /// abandoned wait as a WARNING, and still deregisters the actor.
    /// </summary>
    [Test]
    [Timeout(20000)]
    public async Task StopAllAsync_WithStopTimeout_ReturnsEvenIfHandlerIgnoresToken(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixDRecordingLogSink();
        var system = BoundedSystem(log, TimeSpan.FromMilliseconds(200));

        var actor = await system.CreateAsync<FixDHangActor>(new FixDCreate());
        // Never released: the turn ignores the stop token, so the loop cannot exit.
        _ = await EnterHangAsync(system, actor.Id, cancellationToken);

        // The bounded wait: an unbounded implementation parks here until the process dies,
        // so the outer WaitAsync turns "not implemented" into a test FAILURE, not a hang.
        await system.StopAllAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);

        await Assert.That(log.HasWarning("did not stop within")).IsTrue();
        await Assert
            .That(() => system.Send(actor.Id, new FixDCreate()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// The same bound applies to a single-actor stop: <see cref="ActorSystem.StopAsync"/>
    /// gives up waiting (best effort) instead of hanging the caller — the registry entry is
    /// already gone, so the abandoned actor stays unreachable.
    /// </summary>
    [Test]
    [Timeout(20000)]
    public async Task StopAsync_WithStopTimeout_IsBounded(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixDRecordingLogSink();
        var system = BoundedSystem(log, TimeSpan.FromMilliseconds(200));

        var actor = await system.CreateAsync<FixDHangActor>(new FixDCreate());
        // Never released: the turn ignores the stop token, so the loop cannot exit.
        _ = await EnterHangAsync(system, actor.Id, cancellationToken);

        await system
            .StopAsync(actor.Id)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);

        await Assert.That(log.HasWarning("did not stop within")).IsTrue();
        await Assert
            .That(() => system.Send(actor.Id, new FixDCreate()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// Default semantics are unchanged: with no <c>StopTimeout</c> the mass stop still waits
    /// for a cooperating actor to finish and does not report a timeout.
    /// </summary>
    [Test]
    [Timeout(20000)]
    public async Task StopAllAsync_WithoutStopTimeout_StillWaits(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixDRecordingLogSink();
        var options = new ActorSystemOptions
        {
            EventStore = new InMemoryEventStore(),
            Logger = log,
        };
        // The default is unbounded waiting — the pre-existing contract.
        await Assert.That(options.StopTimeout).IsNull();

        var system = new ActorSystem(options);
        system.Register<FixDHangActor>(cmd => new FixDHangActor((FixDCreate)cmd));

        var actor = await system.CreateAsync<FixDHangActor>(new FixDCreate());
        // A COOPERATING turn: it finishes as soon as it is released.
        var release = await EnterHangAsync(system, actor.Id, cancellationToken);

        var stopAll = system.StopAllAsync().AsTask();
        // Without a timeout the stop waits for that turn instead of abandoning it.
        await Assert.That(stopAll.IsCompleted).IsFalse();

        release.TrySetResult(true);
        await stopAll.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);

        await Assert
            .That(() => system.Send(actor.Id, new FixDCreate()))
            .Throws<KeyNotFoundException>();
        await Assert.That(log.HasWarning("did not stop within")).IsFalse();
    }

    /// <summary>
    /// Guard on the default path: bounding the wait must not swallow a FAULTED loop. With no
    /// <c>StopTimeout</c>, stopping an actor whose initialization blows up still rethrows the
    /// original initialization exception from <see cref="ActorSystem.StopAsync"/>.
    /// </summary>
    [Test]
    [Timeout(20000)]
    public async Task StopTimeout_DoesNotSwallowFaultedLoopException_WhenNoTimeout(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        var entered = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var gate = new FixDInitGate(entered, release);
        system.Register<FixDInitFaultActor>(
            cmd => new FixDInitFaultActor((FixDCreate)cmd, gate),
            () => new FixDInitFaultActor()
        );

        // Creation parks inside the loop (init ignores the stop token) — CreateAsync only
        // returns after initialization, so drive it off-thread and take the id from the loop.
        var create = Task.Run(
            () => system.CreateAsync<FixDInitFaultActor>(new FixDCreate()).AsTask(),
            cancellationToken
        );
        var id = await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);

        var stop = system.StopAsync(id).AsTask();
        // Still parked: with no timeout the stop waits for the loop, as before.
        await Assert.That(stop.IsCompleted).IsFalse();

        release.TrySetResult(true);

        // The loop faults with the initialization exception — rethrown unchanged.
        await Assert.That(async () => await stop).Throws<IOException>();
        await Assert.That(async () => await create).Throws<IOException>();
    }

    private static ActorSystem BoundedSystem(ILogger log, TimeSpan stopTimeout)
    {
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = new InMemoryEventStore(),
                Logger = log,
                StopTimeout = stopTimeout,
            }
        );
        system.Register<FixDHangActor>(cmd => new FixDHangActor((FixDCreate)cmd));
        return system;
    }

    /// <summary>
    /// Park an actor inside a handler turn. The returned task completes when the turn is
    /// released; a caller that ignores it models a handler that never observes its stop token.
    /// </summary>
    private static async Task<TaskCompletionSource<bool>> EnterHangAsync(
        ActorSystem system,
        Guid id,
        CancellationToken cancellationToken
    )
    {
        var entered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Send(id, new FixDHang(entered, release));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
        return release;
    }
}
