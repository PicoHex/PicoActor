namespace PicoActor.Abs;

/// <summary>
/// Saga Actor — a finite-lifetime coordination actor (covers both the saga and
/// the process-manager patterns). Inherits EventSourcedActor; progress is recorded
/// by business events, and the terminal state (completed/failed) is carried by
/// framework-generated events (SagaCompleted/SagaFailed) that the framework
/// restores on replay — no subclass discipline required.
///
/// Lifecycle:
///   CreateAsync/ExecuteSaga → commands driven through the mailbox (no
///   constructor-time command handling)
///   → MarkComplete(result) → framework appends SagaCompleted(result) atomically
///   in the same batch → auto-stop (synchronous registry removal via
///   IActorSystem.RequestStop; the loop exits asynchronously)
///   Or OnMessageAsync/ResumeAsync throws an unhandled business exception →
///   framework appends SagaFailed(reason) → auto-stop → AskAsync caller receives
///   SagaExecutionException(Id, Reason)
///
/// Crash recovery:
///   GetAsync rebuild → ReplayEvents (framework restores terminal-state flags) →
///   if no terminal event, OnReadyAsync automatically calls ResumeAsync()
///   (only when the instance is a rebuild — see EventSourcedActor.IsReplaying; this is
///   the mechanism behind "background recovery" — ResumeInterruptedSagasAsync is just
///   its batched, state-classifying entry point) — the framework's flush wrapper
///   consumes pending events raised during resume (SagaCompleted is persisted in the
///   same batch when the saga reaches completion) → auto-stop once terminal.
/// </summary>
public abstract class SagaActor : EventSourcedActor
{
    private bool _completed;
    private bool _failed;
    private string? _failedReason;
    private object? _pendingResult;
    private bool _pendingComplete;
    private bool _inCommandContext;

    /// <summary>The only construction path. Commands are driven through the mailbox.</summary>
    protected SagaActor() { }

    /// <summary>
    /// A saga is NEVER passivated, whatever its idle time says.
    /// <para>
    /// "Waiting for an external reply" is a saga's normal state: it is idle by definition while
    /// the outside world works, so an idle-budget sweep would reclaim a saga that is mid-flight.
    /// The reply command would then arrive at an id nobody owns (Send/AskAsync throw
    /// <see cref="KeyNotFoundException"/>) and the saga would stay stranded exactly at the
    /// moment it was supposed to progress. Coordination state is live work, not a cache.
    /// </para>
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public override bool CanBePassivated => false;

    /// <summary>Restored by the framework from the event stream on replay.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool IsCompleted => _completed;

    /// <summary>Restored by the framework from the event stream on replay.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool IsFailed => _failed;

    /// <summary>Restored by the framework from SagaFailed(reason) on replay; used for recovery-API classification.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string? FailedReason => _failedReason;

    /// <summary>
    /// Mark the saga as complete. Only records a pending completion — the framework's
    /// flush wrapper appends SagaCompleted(result) in the current flush.
    /// May only be called from OnMessageAsync or ResumeAsync; calling it anywhere
    /// else (Mutate/replay/hook) throws. Repeated calls within the same message
    /// are ignored idempotently.
    /// </summary>
    protected void MarkComplete(object? result = null)
    {
        if (!_inCommandContext)
            throw new InvalidOperationException(
                "MarkComplete must be called from OnMessageAsync or ResumeAsync."
            );
        _pendingComplete = true;
        _pendingResult = result;
    }

    /// <summary>
    /// Re-evaluation hook after recovery: may advance the saga, query external
    /// aggregates via AskAsync before deciding, or no-op (keep waiting for events).
    /// Every step must be idempotent (_step &lt; N guard or external query) — the
    /// framework guarantees events are never persisted twice, single-flight, serial.
    /// </summary>
    protected abstract ValueTask ResumeAsync();

    protected override bool TryHandleFrameworkEvent(IDomainEvent @event)
    {
        switch (@event)
        {
            case SagaCompleted:
                _completed = true;
                return true;
            case SagaFailed f:
                _failed = true;
                _failedReason = f.Reason;
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Framework flush wrapper: consumes pending (appends SagaCompleted to the same
    /// batch), then defers to the base-class persistence.
    /// Pending lifetime = one flush attempt; cleared on success or failure.
    /// Shared by the ProcessAsync and OnReadyAsync entry points.
    /// </summary>
    protected override async ValueTask FlushEventsAsync()
    {
        if (_pendingComplete)
            RaiseEvent(new SagaCompleted(_pendingResult));
        _pendingComplete = false;
        _pendingResult = null;

        await base.FlushEventsAsync().ConfigureAwait(false);
    }

    protected override async ValueTask OnReadyAsync()
    {
        await base.OnReadyAsync().ConfigureAwait(false);

        // Recovery only: an instance rebuilt from the persisted stream resumes. "Version > 0"
        // was the old proxy for this and is wrong for a saga that raises events in its own
        // constructor — that is a creation (and already ran no resume), so it must not
        // resume again here.
        if (!_completed && !_failed && IsReplaying)
        {
            _inCommandContext = true;
            try
            {
                await ResumeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (IsStopInterrupted(ex))
                {
                    // Shutdown interrupt (not a business failure): drop the in-flight
                    // batch and pending completion, record no terminal event — the saga
                    // stays resumable. Propagate as-is.
                    DiscardPendingWork();
                    throw;
                }

                // Business failure on the recovery path = terminal: record SagaFailed.
                try
                {
                    await FailAsync(ex).ConfigureAwait(false);
                }
                catch
                {
                    // SagaFailed itself could not be persisted (store down): no terminal
                    // state, and spec §5.2 requires the ORIGINAL exception to propagate
                    // (GetAsync's init-failure cleanup handles the rest).
                    ExceptionDispatchInfo.Capture(ex).Throw();
                    throw; // unreachable — keeps flow analysis explicit
                }
            }
            finally
            {
                _inCommandContext = false;
            }
            await FlushEventsAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Terminal-state auto-stop. Runs after initialization completed (not from inside
    /// <see cref="OnReadyAsync"/>), so the runtime never drops the registry entry while a
    /// caller is still initializing this saga — a concurrent GetAsync observes this
    /// instance instead of racing a rebuild.
    /// </summary>
    internal override void OnInitialized()
    {
        base.OnInitialized();

        if (_completed || _failed)
            ScheduleStop();
    }

    /// <summary>
    /// Terminal-state guard + failure handling. Commands arriving after the saga is
    /// terminal are refused (Ask faults / Send silently drops, subclasses are not
    /// called) — prevents terminal-state events from polluting the event stream
    /// (messages already queued in the mailbox when the terminal message runs
    /// still arrive after it; the registry removal itself is synchronous).
    /// </summary>
    protected sealed override async ValueTask ProcessAsync(Envelope envelope)
    {
        if (_completed || _failed)
        {
            envelope.Tcs?.TrySetException(
                new InvalidOperationException($"Saga {Id} already terminated.")
            );
            return;
        }

        _inCommandContext = true;
        object? result;
        try
        {
            // Dispatch ONLY — this is the business boundary. A persistence failure from
            // FlushEventsAsync below is an INFRASTRUCTURE failure (spec §2.6/§7.1): the
            // batch rolls back, the saga stays Running, and the original exception propagates.
            // Routing it into FailAsync would make a transient store failure a permanent
            // terminal state (the regression this shape prevents).
            result = await OnMessageAsync(envelope.Command).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (IsStopInterrupted(ex))
            {
                // Shutdown interrupt (not a business failure): the command was aborted
                // by the stop, not refused by business logic. Drop the in-flight batch
                // and any pending completion, record NO terminal event, and propagate
                // as-is (the caller observes cancellation, and the saga remains
                // resumable instead of being permanently buried as Failed).
                DiscardPendingWork();
                throw;
            }

            try
            {
                await FailAsync(ex).ConfigureAwait(false);
            }
            catch
            {
                // SagaFailed itself could not be persisted (store down): no terminal state,
                // and spec §5.2 requires the ORIGINAL exception to propagate.
                ExceptionDispatchInfo.Capture(ex).Throw();
                throw; // unreachable — keeps flow analysis explicit
            }

            var failure = new SagaExecutionException(Id, MakeReason(ex));
            if (envelope.Tcs is not null)
                envelope.Tcs.TrySetException(failure);
            else
                UnhandledErrorHandler?.Invoke(failure, envelope.Command);
            ScheduleStop();
            return;
        }
        finally
        {
            _inCommandContext = false;
        }

        // Infrastructure failures propagate from here (non-terminal).
        await FlushEventsAsync().ConfigureAwait(false);

        // Reply only after successful persistence (same contract as EventSourcedActor).
        envelope.Tcs?.TrySetResult(result);

        if (_completed || _failed)
            ScheduleStop();
    }

    /// <summary>
    /// True when <paramref name="ex"/> is cancellation raised by this actor being
    /// stopped — a shutdown interrupt rather than a business failure. Only the actor's
    /// own <see cref="Actor.StopToken"/> counts: a caller-owned token (timeout, request
    /// abort) stays on the business-failure path, which keeps the documented
    /// "business failure = terminal" semantics.
    /// </summary>
    private bool IsStopInterrupted(Exception ex) =>
        ex is OperationCanceledException && StopToken.IsCancellationRequested;

    /// <summary>
    /// Drop everything the interrupted turn staged: uncommitted business events (Version
    /// rolled back with them) and any pending completion, so no half-finished work and
    /// no fake success can reach the event stream.
    /// </summary>
    private void DiscardPendingWork()
    {
        _pendingComplete = false;
        _pendingResult = null;
        RollbackUncommitted();
    }

    private async ValueTask FailAsync(Exception ex)
    {
        var reason = MakeReason(ex);

        // Discard uncommitted business events and pending (existing atomicity
        // semantics: on failure the events were never persisted).
        // Version must be rolled back in sync — RollbackUncommitted clears the list
        // and restores the last persisted baseline, so the subsequent SagaFailed
        // flush computes the correct expectedVersion (no ConcurrencyException).
        RollbackUncommitted();
        _pendingComplete = false;
        _pendingResult = null;

        RaiseEvent(new SagaFailed(reason));
        // May throw (store down) — ProcessAsync's catch converts that into "no terminal
        // state + propagate the ORIGINAL business exception" (spec §5.2).
        await FlushEventsAsync().ConfigureAwait(false);
    }

    private static string MakeReason(Exception ex)
    {
        var message = ex.Message ?? string.Empty;
        if (message.Length > 512)
            message = message.Substring(0, 512);
        return $"{ex.GetType().Name}: {message}";
    }

    /// <summary>
    /// Terminal-state cleanup: remove the saga from the registry and signal its
    /// loop to stop. <see cref="IActorSystem.RequestStop"/> is synchronous and
    /// loop-safe (no self-await), so it can be called directly from the message
    /// turn — the loop exits asynchronously after the current message returns.
    /// </summary>
    private void ScheduleStop()
    {
        if (System is null)
            return;

        System.RequestStop(Id);
    }
}
