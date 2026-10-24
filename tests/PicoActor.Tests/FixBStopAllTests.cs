namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// B4 — StopAllAsync is best-effort (never throws, isolates
//      per-actor failures, retries until the registry drains)
//      and closes a stopping window for creation/lookup.
// ═══════════════════════════════════════════════════════════

/// <summary>Blocks the actor loop until released — keeps StopAsync(id) parked.</summary>
internal sealed record FixBBlockCmd(
    TaskCompletionSource<bool> Entered,
    TaskCompletionSource<bool> Release
) : ICommand;

/// <summary>Plain actor for the mass-stop tests.</summary>
internal sealed class FixBStopAllActor : ActorBase
{
    public FixBStopAllActor(FixBCreateCmd cmd)
        : base(cmd) { }

    public FixBStopAllActor() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is FixBBlockCmd blocked)
        {
            blocked.Entered.TrySetResult(true);
            await blocked.Release.Task.ConfigureAwait(false);
        }

        return null;
    }
}

/// <summary>
/// Log sink whose first "Stopping actor" report throws — injects a per-actor failure into
/// the mass-stop loop (a real log sink can fail; the rest of the shutdown must not).
/// </summary>
internal sealed class FixBFailingLogSink : ILogger
{
    private readonly FixBLogSink _inner = new();
    private readonly bool _throwAlways;
    private int _stopReports;

    public FixBFailingLogSink(bool throwAlways = false) => _throwAlways = throwAlways;

    public IReadOnlyList<string> Messages => _inner.Messages;

    private void Record(string message)
    {
        if (message.Contains("Stopping actor", StringComparison.Ordinal))
        {
            var seen = Interlocked.Increment(ref _stopReports);
            if (_throwAlways || seen == 1)
                throw new IOException("log sink down");
        }

        _inner.RecordPublic(message);
    }

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => _inner.BeginScope(state);

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
}

public sealed class FixBStopAllTests
{
    /// <summary>
    /// The shutdown window must refuse new work: creating an actor or looking one up while
    /// StopAllAsync is running would add an actor after the snapshot (and hand out a handle
    /// that is about to be stopped). Once the mass stop returns, the system is usable again.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_DuringShutdown_RejectsCreation_AndReopensAfterwards(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<FixBStopAllActor>(cmd => new FixBStopAllActor((FixBCreateCmd)cmd));

        var blocked = await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd());
        var idle = await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd());

        var entered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Send(blocked.Id, new FixBBlockCmd(entered, release));
        await entered.Task; // the loop is parked inside the handler, so its stop parks too

        var stopAll = system.StopAllAsync().AsTask(); // the stopping window opens synchronously

        // Creation and lookup are refused while the shutdown runs. `blocked` is still
        // registered here (the mass stop is parked on it), so the lookup cannot degrade to
        // "not found" for another reason.
        await Assert
            .That(async () => await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd()))
            .Throws<InvalidOperationException>();
        await Assert.That(await system.GetAsync<FixBStopAllActor>(blocked.Id)).IsNull();

        release.TrySetResult(true);
        await stopAll;

        await Assert
            .That(() => system.Send(blocked.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
        await Assert
            .That(() => system.Send(idle.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();

        // The window closed: the system is usable again (a second mass stop is a no-op, and
        // a fresh actor can be created and stopped).
        var fresh = await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd());
        await Assert.That(fresh.Id).IsNotEqualTo(Guid.Empty);
        await system.StopAllAsync();
        await Assert
            .That(() => system.Send(fresh.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// One failing actor must not abort the shutdown: the mass stop reports the failure and
    /// keeps stopping the rest (a single failure used to leave the remaining loops running
    /// and escape as an exception).
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_PerActorFailure_DoesNotAbortShutdown(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBFailingLogSink();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore(), Logger = log }
        );
        system.Register<FixBStopAllActor>(cmd => new FixBStopAllActor((FixBCreateCmd)cmd));

        var ids = new List<Guid>();
        for (var i = 0; i < 3; i++)
            ids.Add((await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd())).Id);

        // The first "Stopping actor" report throws inside the shutdown loop.
        await system.StopAllAsync();

        foreach (var id in ids)
            await Assert
                .That(() => system.Send(id, new FixBCreateCmd()))
                .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// An actor registered AFTER the shutdown snapshot is taken must still be stopped: the
    /// mass stop keeps draining the registry instead of sweeping a stale snapshot once.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_ActorRegisteredAfterSnapshot_IsAlsoStopped(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );

        var factoryEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseFactory = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var createCount = 0;

        system.Register<FixBStopAllActor>(cmd =>
        {
            // The second creation parks in its factory: registered after the snapshot.
            if (Interlocked.Increment(ref createCount) == 2)
            {
                factoryEntered.TrySetResult(true);
                releaseFactory.Task.GetAwaiter().GetResult();
            }

            return new FixBStopAllActor((FixBCreateCmd)cmd);
        });

        var blocked = await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd());

        var entered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Send(blocked.Id, new FixBBlockCmd(entered, release));
        await entered.Task;

        // This creation is already past the stopping check and parks before registering.
        var lateCreate = Task.Run(() =>
            system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd()).AsTask()
        );
        await factoryEntered.Task;

        var stopAll = system.StopAllAsync().AsTask(); // snapshot = { blocked } only
        releaseFactory.TrySetResult(true);

        var late = await lateCreate; // registers + initializes while the mass stop runs

        release.TrySetResult(true); // unblock the first actor → the first round can finish
        await stopAll;

        await Assert
            .That(() => system.Send(late.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
        await Assert
            .That(() => system.Send(blocked.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
    }
    /// <summary>
    /// A log sink that throws on EVERY shutdown report must not break the "StopAllAsync
    /// never throws" contract: the diagnostics are best-effort like the shutdown itself.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_ThrowingLogSink_DoesNotThrow(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBFailingLogSink(throwAlways: true);
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore(), Logger = log }
        );
        system.Register<FixBStopAllActor>(cmd => new FixBStopAllActor((FixBCreateCmd)cmd));

        var ids = new List<Guid>();
        for (var i = 0; i < 3; i++)
            ids.Add((await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd())).Id);

        await system.StopAllAsync(); // must not throw despite the sink

        foreach (var id in ids)
            await Assert
                .That(() => system.Send(id, new FixBCreateCmd()))
                .Throws<KeyNotFoundException>();
    }
}

/// <summary>
/// Log sink that throws on EVERY report, whatever its text (P1). A throwing diagnostic
/// channel must never be able to keep an actor loop alive: the registry entry is already
/// gone by then, so a surviving loop is unreachable — no later sweep can find it.
/// Counts the calls so a test can prove the sink was really consulted.
/// </summary>
internal sealed class FixBPoisonLogSink : ILogger
{
    private int _calls;
    private int _armed;

    public int Calls => Volatile.Read(ref _calls);

    /// <summary>Start throwing. Before this the sink only records — the scenario under test is the STOP, not creation.</summary>
    public void Arm() => Volatile.Write(ref _armed, 1);

    private void Throw(string message)
    {
        Interlocked.Increment(ref _calls);
        if (Volatile.Read(ref _armed) != 0)
            throw new IOException($"log sink down: {message}");
    }

    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => new NullScope();

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Throw(message);

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Throw(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Throw(message);

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Throw(message);
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Throw(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Throw(message.ToString());
        return Task.CompletedTask;
    }

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => Throw(message.ToString());

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    )
    {
        Throw(message.ToString());
        return Task.CompletedTask;
    }

    private sealed class NullScope : IDisposable
    {
        public void Dispose() { }
    }
}

public sealed class FixBPoisonSinkStopTests
{
    /// <summary>
    /// P1 — StopAllAsync: every loop must really exit even when the log sink throws on
    /// every report. The proof is <c>Post</c> returning false (the mailbox write end is
    /// closed by the loop's exit path); <c>Send</c> throwing KeyNotFoundException proves
    /// nothing, because the registry entry is removed BEFORE the stop is signalled.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_PoisonLogSink_StillStopsEveryLoop(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBPoisonLogSink();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore(), Logger = log }
        );
        system.Register<FixBStopAllActor>(cmd => new FixBStopAllActor((FixBCreateCmd)cmd));

        var actors = new List<FixBStopAllActor>();
        for (var i = 0; i < 3; i++)
            actors.Add(await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd()));

        log.Arm();
        await system.StopAllAsync(); // must not throw despite the sink

        foreach (var actor in actors)
        {
            await Assert.That(actor.Post(new Envelope { Command = new FixBCreateCmd() })).IsFalse();
            await Assert
                .That(() => system.Send(actor.Id, new FixBCreateCmd()))
                .Throws<KeyNotFoundException>();
        }

        // The sink was really consulted: the loops above were stopped DESPITE it throwing.
        await Assert.That(log.Calls).IsGreaterThan(0);
    }

    /// <summary>
    /// P1 — single-actor StopAsync(id): the same invariant, per actor. The sink throwing
    /// must not leave a registry-less loop running, and must not escape as an exception.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAsync_PoisonLogSink_StillStopsLoop(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBPoisonLogSink();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore(), Logger = log }
        );
        system.Register<FixBStopAllActor>(cmd => new FixBStopAllActor((FixBCreateCmd)cmd));

        var actor = await system.CreateAsync<FixBStopAllActor>(new FixBCreateCmd());

        log.Arm();
        await system.StopAsync(actor.Id); // must not throw despite the sink

        await Assert.That(actor.Post(new Envelope { Command = new FixBCreateCmd() })).IsFalse();
        await Assert
            .That(() => system.Send(actor.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
        await Assert.That(log.Calls).IsGreaterThan(0);
    }
}
