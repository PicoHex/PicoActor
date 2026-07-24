namespace PicoActor.Abs;

/// <summary>
/// Saga Actor — a finite-life coordinator for cross-aggregate operations.
///
/// Unlike a regular <see cref="EventSourcedActor"/> whose Mailbox loop runs forever,
/// a SagaActor automatically stops itself after the coordinator completes.
///
/// Lifecycle:
///   1. External caller sends a command via IActorSystem.
///   2. Subclass processes the command in <see cref="OnMessageAsync"/>.
///   3. When coordination is done, subclass calls <see cref="MarkComplete"/>.
///   4. After the current message returns, the framework schedules an async stop,
///      avoiding the deadlock of calling StopAsync from inside the Mailbox loop.
///
/// Crash recovery:
///   - Persisted events replay through <see cref="Mutate"/>, which restores the
///     _completed flag from persisted state.
///   - <see cref="OnReadyAsync"/> checks _completed: if true, the saga schedules
///     its own stop; if false, it calls <see cref="ResumeAsync"/> to re-execute
///     from the interrupted step.
///   - Subclasses must make every step idempotent (guarded by _step checks).
/// </summary>
public abstract class SagaActor : EventSourcedActor
{
    private volatile bool _completed;

    /// <summary>Creation path. Chains to EventSourcedActor(ICommand).</summary>
    protected SagaActor(ICommand creationCommand)
        : base(creationCommand) { }

    /// <summary>Rebuild path. Chains to EventSourcedActor().</summary>
    protected SagaActor() { }

    /// <summary>
    /// Mark this saga as complete.
    /// After the current <see cref="Actor.ProcessAsync"/> returns, the framework
    /// will schedule an async stop of this actor.
    /// Idempotent — safe to call multiple times.
    /// </summary>
    protected void MarkComplete()
    {
        _completed = true;
    }

    /// <summary>
    /// True if the saga has been marked complete. Used by <see cref="ActorSystem"/>
    /// to skip rebuilding completed sagas from the event store.
    /// </summary>
    internal bool IsCompleted => _completed;

    /// <summary>
    /// Resume the saga from the interrupted step after crash recovery.
    /// Called by <see cref="OnReadyAsync"/> when _completed is false.
    /// Subclasses re-dispatch the persisted command, guarded by idempotent
    /// step checks so already-completed steps are skipped.
    /// </summary>
    protected abstract ValueTask ResumeAsync();

    /// <summary>
    /// Lifecycle hook: flush events, then resume or stop based on completion state.
    /// </summary>
    protected override async ValueTask OnReadyAsync()
    {
        // Flush any events produced during construction (EventSourcedActor behavior)
        await base.OnReadyAsync().ConfigureAwait(false);

        // Only resume if this is a recovery (Version > 0 means events were replayed).
        // For a brand-new saga, skip ResumeAsync — the first mailbox message
        // will drive the initial execution.
        if (!_completed && Version > 0)
        {
            await ResumeAsync().ConfigureAwait(false);
            // Flush events raised during ResumeAsync before any mailbox message
            // reads stale state through AskAsync.
            await FlushEventsAsync().ConfigureAwait(false);
        }

        if (_completed)
        {
            ScheduleStop();
        }
    }

    /// <summary>
    /// Process a mailbox message, then check completion and schedule stop if needed.
    /// Overrides <see cref="EventSourcedActor.ProcessAsync"/> to add the completion check.
    /// Sealed — subclasses should override <see cref="Actor.OnMessageAsync"/> instead.
    /// </summary>
    protected sealed override async ValueTask ProcessAsync(Envelope envelope)
    {
        await base.ProcessAsync(envelope).ConfigureAwait(false);

        if (_completed)
        {
            ScheduleStop();
        }
    }

    /// <summary>
    /// Schedule an async stop of this actor on a background thread.
    /// Uses Task.Run + Task.Yield to ensure the current ProcessAsync / OnReadyAsync
    /// fully returns before StopAsync tries to wait for _loopTask.
    /// </summary>
    private void ScheduleStop()
    {
        if (System is null)
            return;

        var sys = System;
        var id = Id;

        _ = Task.Run(async () =>
        {
            // Yield to let the current synchronous caller fully unwind.
            // Without this, StopAsync's await _loopTask would deadlock
            // because _loopTask is the caller's stack frame.
            await Task.Yield();

            try
            {
                await sys.StopAsync(id).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort cleanup — saga events are already persisted.
                // If stop fails, the actor leaks memory but data is safe.
            }
        });
    }
}
