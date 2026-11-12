namespace PicoActor.Abs;

/// <summary>
/// Base class for operational (non-ES) actors.
/// Starts a consumption loop in the constructor, but the loop is gated by a ready signal.
/// Messages posted before SignalReady() queue in the Channel but are not processed.
/// Subclasses override OnMessageAsync for command dispatch.
///
/// Two constructor paths:
/// - Creation: Actor(ICommand) — synchronously processes the creation command.
///   State is initialized atomically before SignalReady.
/// - Rebuild:  Actor() — no business logic. State is restored later via ReplayEvents.
///   Must be accessible via new() constraint on IActorSystem for GetAsync.
/// </summary>
public abstract class Actor : IActor, IAsyncDisposable
{
    private readonly Channel<Envelope> _mailbox;
    private readonly CancellationTokenSource _cts = new();

    // Cached at construction: the CTS is disposed by the loop's exit path, and reading
    // CancellationTokenSource.Token after disposal throws ObjectDisposedException. The
    // token itself stays usable (and observable as cancelled) after disposal.
    private readonly CancellationToken _stopToken;

    // RunContinuationsAsynchronously: the completing thread is the actor's own
    // loop — continuations must never run inline on it.
    private readonly TaskCompletionSource<bool> _ready = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource<bool> _initCompleted = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly Task _loopTask;
    private int _stopped;
    private int _discarded;

    /// <summary>
    /// UTC ticks of the last observed activity: a successful <see cref="Post"/> (a command was
    /// accepted) and the END of every dispatched command (successful or not). This is the clock
    /// the idle sweeper reads. It deliberately does NOT move while a command is running - that is
    /// what <see cref="IsProcessing"/> is for: a long handler is not an idle actor.
    /// </summary>
    private long _lastActivityTicks = DateTime.UtcNow.Ticks;

    /// <summary>1 while a command is being dispatched by the loop, 0 otherwise.</summary>
    private int _processing;

    /// <summary>
    /// 1 when the owning system reads <see cref="LastActivityUtc"/> (idle passivation is on), 0
    /// otherwise. The idle clock is only paid for when somebody reads it: the hot paths (every
    /// accepted <see cref="Post"/> and the end of every dispatched command) must be free of a
    /// <c>DateTime.UtcNow</c> nobody consumes.
    /// </summary>
    private int _trackIdleActivity;

    /// <summary>
    /// Framework-generated UUID v7 identity.
    /// Assigned by IActorSystem via <see cref="AttachToSystem"/> after construction,
    /// before SignalReady(). Immutable for application code thereafter.
    /// </summary>
    public Guid Id { get; private set; }

    /// <summary>
    /// Framework wiring (identity + owning system). Called by IActorSystem right after
    /// construction and before <see cref="SignalReady"/>; <paramref name="system"/> is
    /// optional so an actor can be driven manually (tests, embedded hosts).
    /// Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void AttachToSystem(Guid id, IActorSystem? system = null)
    {
        Id = id;
        System = system;
    }

    /// <summary>
    /// Creation path. Constructs the actor and synchronously processes the creation command.
    /// The consumption loop is started gated — it waits for SignalReady() before dispatching
    /// any mailbox messages. State is initialized atomically in the constructor.
    /// </summary>
    protected Actor(ICommand creationCommand)
        : this()
    {
        // Process creation command synchronously for atomic initialization.
        // Subclasses must complete OnMessageAsync synchronously for creation commands —
        // use RaiseEvent (EventSourcedActor) or return a value inline.
        //
        // If the creation command throws, clean up the consumption loop started by this()
        // to prevent a zombie actor waiting forever on the gate.
        try
        {
            var task = OnMessageAsync(creationCommand);
            // IsCompletedSuccessfully, not IsCompleted: a faulted or canceled ValueTask
            // is "completed" too, and treating it as success swallowed the creation
            // failure (CreateAsync returned a half-initialized actor whose
            // construction events still flushed). Re-raise the ORIGINAL exception so
            // the caller sees why creation failed.
            if (!task.IsCompletedSuccessfully)
            {
                if (task.IsCompleted)
                    task.GetAwaiter().GetResult();

                throw new InvalidOperationException(
                    "OnMessageAsync must complete synchronously for creation commands. "
                        + "Use RaiseEvent or return a value directly; do not await."
                );
            }
        }
        catch
        {
            // Release the gate so RunAsync can exit, then clean up.
            // Mark discarded FIRST: releasing the gate lets the loop resume, and a
            // failed-construction actor must skip OnReadyAsync (it is dead).
            MarkDiscarded();
            // Same three signals as SignalStop — reuse it instead of duplicating them.
            SignalStop();
            throw;
        }
    }

    /// <summary>
    /// Rebuild path. Constructs the actor with no business logic.
    /// State is restored later by the caller via ReplayEvents.
    /// Must be chainable from a public parameterless constructor on subclasses
    /// (used by GetAsync with the registered rebuildFactory).
    /// </summary>
    protected Actor()
    {
        _mailbox = Channel.CreateUnbounded<Envelope>();
        _stopToken = _cts.Token;
        _loopTask = RunAsync(_stopToken);
    }

    /// <summary>
    /// Called by IActorSystem once the actor is fully initialized:
    /// Id assigned, registered in the system, and (for ES actors) replayed from events.
    /// Releases the gated consumption loop to start processing queued messages.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void SignalReady() => _ready.TrySetResult(true);

    /// <summary>
    /// The actor's cancellation token. Canceled when StopAsync is called.
    /// Subclasses pass this to async operations so Stop can interrupt long-running work.
    /// </summary>
    protected CancellationToken StopToken => _stopToken;

    /// <summary>
    /// Reference to the owning IActorSystem (set by <see cref="AttachToSystem"/>).
    /// Subclasses use this for spawn operations.
    /// </summary>
    protected IActorSystem? System { get; private set; }

    /// <summary>
    /// Outbound channel writer. External subscribers (e.g., SSE Server) set this
    /// to receive progress/diagnostic events from the actor.
    /// Symmetric to the Mailbox: Mailbox receives commands (inbound),
    /// OutputChannel broadcasts events (outbound).
    /// Weak guarantee — TryWrite silently drops if no subscriber or channel full.
    /// Actor state is never affected by output events.
    /// </summary>
    public ChannelWriter<ActorOutputEvent>? OutputWriter { get; set; }

    /// <summary>
    /// Hook invoked when a fire-and-forget (Send) message throws and there is no
    /// TCS to propagate the exception to. Set by IActorSystem to route unhandled
    /// message errors to logging. Ask-style failures still fault the TCS first.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Action<Exception, ICommand>? UnhandledErrorHandler { get; set; }

    /// <summary>
    /// Write an event to the OutputChannel. No-op if no subscriber.
    /// Subclasses call this during OnMessageAsync to notify external observers.
    /// </summary>
    protected void WriteOutput(string type, string? data = null, string? turnId = null) =>
        OutputWriter?.TryWrite(new ActorOutputEvent(type, data, turnId));

    /// <summary>
    /// Task that completes when initialization finishes (OnReadyAsync succeeds or fails).
    /// Used by ActorSystem.CreateAsync to wait for persistence before returning.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task InitCompletedTask => _initCompleted.Task;

    /// <summary>
    /// Marks this actor as a discarded duplicate (the loser of a concurrent rebuild).
    /// RunAsync skips OnReadyAsync — a discarded copy must not run business logic
    /// (including saga resume) or write to the event stream; it only cleans up resources.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void MarkDiscarded() => Interlocked.Exchange(ref _discarded, 1);

    private bool IsDiscarded => _discarded != 0;

    /// <summary>Called by IActorSystem to deliver an envelope. False = delivery failed (stopping).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool Post(Envelope envelope)
    {
        // Accepted work counts as activity: an actor that keeps receiving commands must never
        // look idle just because its turns happen to be short.
        if (!_mailbox.Writer.TryWrite(envelope))
            return false;

        Touch();
        return true;
    }

    /// <summary>
    /// When this actor was last active (UTC): the last accepted <see cref="Post"/> or the end of
    /// the last dispatched command. Read by the ActorSystem idle sweeper; not for application
    /// code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DateTime LastActivityUtc => new(Volatile.Read(ref _lastActivityTicks), DateTimeKind.Utc);

    /// <summary>
    /// True while the loop is dispatching a command (set before <see cref="ProcessAsync"/> and
    /// cleared after it, including on failure). An actor that is working is never a passivation
    /// candidate. Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool IsProcessing => Volatile.Read(ref _processing) != 0;

    /// <summary>
    /// Commands buffered in the mailbox and not yet dispatched. A non-zero backlog means the actor
    /// still has work waiting, so it is not idle. Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int PendingCount => _mailbox.Reader.Count;

    /// <summary>
    /// Ask this actor to give its materialized state back, THROUGH THE MAILBOX (a message
    /// boundary) instead of an external <see cref="SignalStop"/>: the request is queued behind
    /// everything already accepted, the loop stops itself when it reaches it, and no command is
    /// ever dispatched on an actor that is being reclaimed.
    /// <para>
    /// Only meaningful for actors the runtime can rebuild (an EventSourcedActor with a registered
    /// rebuild factory) - the framework's idle sweeper is the caller. The actor is removed from
    /// the registry (so Send/AskAsync throw <see cref="KeyNotFoundException"/>) and its state is
    /// rebuilt from the event stream by the next GetAsync. Not for application code.
    /// </para>
    /// <para>
    /// Returns whether the request was actually QUEUED. False means the mailbox is already closed
    /// (the actor is stopping or stopped), so no reclaim will happen and the caller must not count
    /// one: the request is only a request, and this is the only honest way to report a refusal.
    /// </para>
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool RequestPassivation() =>
        _mailbox.Writer.TryWrite(new Envelope { Command = PassivateSentinel.Instance });

    /// <summary>
    /// Whether this actor's idle clock is observed. Set by the owning system when idle passivation
    /// is configured (nothing else reads the clock); false by default, so a system that never
    /// passivates pays nothing for timestamps. Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool TrackIdleActivity
    {
        get => Volatile.Read(ref _trackIdleActivity) != 0;
        set => Volatile.Write(ref _trackIdleActivity, value ? 1 : 0);
    }

    /// <summary>Record that activity just happened on this actor (UTC ticks, lock-free).</summary>
    private void Touch()
    {
        // Passivation disabled: nobody will ever read the value, so skip the clock read
        // entirely. Read per call (not cached) so the switch stays honest.
        if (Volatile.Read(ref _trackIdleActivity) == 0)
            return;

        Interlocked.Exchange(ref _lastActivityTicks, DateTime.UtcNow.Ticks);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // The loop owns its CTS: dispose on every exit path (normal stop,
        // creation failure, discard, init failure).
        try
        {
            await RunCoreAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _cts.Dispose();
        }
    }

    private async Task RunCoreAsync(CancellationToken ct)
    {
        // Any exit from this method ends message dispatch. Envelopes still buffered at
        // that point would otherwise be abandoned with their TCS uncompleted — an Ask
        // caller would wait forever (there is no Ask timeout by design). Fault them.
        try
        {
            // Gate: wait until ActorSystem calls SignalReady().
            // This guarantees that Id is assigned, the actor is in the registry,
            // and (for ES actors) ReplayEvents has completed before any message is dispatched.
            // Messages posted during this wait safely queue in the Channel.
            await _ready.Task.ConfigureAwait(false);

            // Hook: flush any events produced during construction (EventSourcedActor).
            // Base implementation is a no-op. Discarded copies skip it entirely.
            if (!IsDiscarded)
            {
                try
                {
                    await OnReadyAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _initCompleted.TrySetException(ex);
                    throw;
                }
            }
            _initCompleted.TrySetResult(true);

            // Initialization is complete and already observable by the caller: let
            // subclasses react (SagaActor auto-stops when the state it reached during
            // replay/resume is terminal). Running it AFTER the init signal — and before
            // the first mailbox message — keeps the actor registered for the whole
            // initialization, so a concurrent GetAsync can observe this instance instead
            // of racing a rebuild. Discarded copies run no lifecycle hooks.
            if (!IsDiscarded)
            {
                try
                {
                    OnInitialized();
                }
                catch (Exception ex)
                {
                    // The hook runs OUTSIDE any message turn, so a throwing override has no
                    // caller to surface to: the loop task would fault silently (unobserved
                    // exception) while the actor still looked alive — a half-initialized
                    // actor that never dispatches again. Report through the same error seam
                    // as a failed message handler and stop explicitly: dead, and loud.
                    ReportInitializationHookError(ex);
                    // The actor is dead, so it must LEAVE the registry, not merely stop its
                    // loop: SignalStop alone left the dead instance registered, and every
                    // lookup kept routing to it (Send/Ask threw "is stopping" and GetAsync
                    // handed out a corpse instead of missing). RequestStop removes the entry
                    // and signals the stop — the same shape as SagaActor.ScheduleStop.
                    // Unattached actors (manually driven probes, and discarded copies built
                    // before wiring) have no system to deregister from: stop directly.
                    if (System is not null)
                        System.RequestStop(Id);
                    else
                        SignalStop();
                    return;
                }
            }

            try
            {
                await foreach (
                    var envelope in _mailbox.Reader.ReadAllAsync(ct).ConfigureAwait(false)
                )
                {
                    // Framework sentinel: passivation is a MESSAGE, not an external stop. It is
                    // handled strictly BEFORE dispatch, and it is not a command - the subclass
                    // never sees it, nothing is persisted for it, and no TCS can be waiting on it.
                    // The removal is ownership-checked (see RequestOwnStop): it takes THIS
                    // instance out, so later lookups miss (and a concurrent GetAsync rebuilds
                    // from the stream) instead of reaching a corpse.
                    if (envelope.Command is PassivateSentinel)
                    {
                        RequestOwnStop();
                        continue;
                    }

                    Volatile.Write(ref _processing, 1);
                    try
                    {
                        await ProcessAsync(envelope).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // Propagate failure to Ask callers; single message failure does not stop the loop
                        if (envelope.Tcs is not null)
                            envelope.Tcs.TrySetException(ex);
                        else
                            InvokeErrorHandler(ex, envelope.Command);
                    }
                    finally
                    {
                        // Cleared in a finally: a throwing handler (or a failed persist) must not
                        // leave the actor looking permanently "processing" - that would exempt it
                        // from passivation for the rest of the process lifetime.
                        Volatile.Write(ref _processing, 0);
                        Touch();
                    }
                }
            }
            catch (OperationCanceledException)
            { /* expected on Stop */
            }
        }
        finally
        {
            // Close the write end on EVERY exit path (including a loop that dies on its
            // own, e.g. an init failure): otherwise Post keeps returning true with no
            // reader left and an Ask queued afterwards hangs forever.
            _mailbox.Writer.TryComplete();
            FailPendingEnvelopes();
        }
    }

    /// <summary>
    /// Report a fire-and-forget failure through the error handler without letting a
    /// throwing handler kill the consumption loop. The handler is the logging seam
    /// (ActorSystem routes to ILogger); a failing log sink must not black-hole the actor.
    /// </summary>
    private void InvokeErrorHandler(Exception ex, ICommand command)
    {
        try
        {
            UnhandledErrorHandler?.Invoke(ex, command);
        }
        catch (Exception handlerEx)
        {
            // Abs has no logger dependency: report on stderr (same fallback as
            // MediatorDomainEventPublisher) instead of rethrowing and losing the loop.
            // The sink itself may fail (broken pipe, disposed writer) — that must never
            // kill the loop either, so this write is guarded too.
            try
            {
                Console.Error.WriteLine(
                    $"[PicoActor] Error handler failed for {command.GetType().Name} on actor {Id}: {handlerEx.Message}"
                );
            }
            catch
            {
                // Last-resort diagnostic sink failed — swallow: the loop is the actor's lifeline.
            }
        }
    }

    /// <summary>
    /// Fault every envelope still buffered in the mailbox. A graceful stop has already
    /// drained the queue (messages are processed before the loop exits), so this only
    /// fires when the loop ends early — an initialization failure, or any future
    /// unexpected exit. Without it those Ask callers never complete.
    /// </summary>
    private void FailPendingEnvelopes()
    {
        while (_mailbox.Reader.TryRead(out var pending))
        {
            pending.Tcs?.TrySetException(
                new InvalidOperationException(
                    $"Actor {Id} stopped before processing {pending.Command.GetType().Name}."
                )
            );
        }
    }

    /// <summary>
    /// Override to hook into each message (e.g., ES persistence).
    /// Default: dispatch command, return result to TCS.
    /// </summary>
    protected virtual async ValueTask ProcessAsync(Envelope envelope)
    {
        var result = await OnMessageAsync(envelope.Command).ConfigureAwait(false);
        envelope.Tcs?.TrySetResult(result);
    }

    /// <summary>Override to dispatch commands via switch/pattern match.</summary>
    protected abstract ValueTask<object?> OnMessageAsync(ICommand command);

    /// <summary>
    /// Lifecycle hook called after SignalReady() and before the first mailbox message.
    /// EventSourcedActor overrides this to flush uncommitted construction events.
    /// </summary>
    protected virtual ValueTask OnReadyAsync() => default;

    /// <summary>
    /// Lifecycle hook called once initialization has completed and been published to the
    /// caller (<see cref="InitCompletedTask"/>), immediately before the mailbox loop
    /// starts dispatching. Subclasses react to the state init produced — SagaActor
    /// auto-stops when it is already terminal. Discarded copies (losing duplicates) skip it.
    /// Overrides must call the base implementation.
    /// MUST NOT THROW: there is no caller to propagate to (initialization has already been
    /// reported as successful). A throwing override is reported through
    /// <see cref="UnhandledErrorHandler"/> (stderr when no host wired one) and the actor is
    /// stopped — it does not keep running half-initialized.
    /// </summary>
    internal virtual void OnInitialized() { }

    /// <summary>
    /// Leave the registry and stop, on behalf of THIS instance (the passivation sentinel path).
    /// <para>
    /// The host is asked to remove the instance it is holding, not "whatever owns my id": by the
    /// time a queued sentinel is consumed, the id may already belong to a rebuilt replacement, and
    /// an id-only removal would unregister that live actor while leaving this one (already
    /// stopping) behind. A refused request therefore does NOT fall back to an id-only stop: "that
    /// id is not mine anymore" means there is nothing left for this instance to reclaim, and it is
    /// already on its way out. A host that does not implement
    /// <see cref="IActorPassivationHost"/> keeps the id-only behavior.
    /// </para>
    /// </summary>
    private void RequestOwnStop()
    {
        if (System is IActorPassivationHost host)
        {
            host.TryRequestStop(Id, this);
            return;
        }

        System?.RequestStop(Id);
    }

    /// <summary>
    /// Report a throwing <see cref="OnInitialized"/> override without letting the report
    /// itself escape: the loop is already unwinding and a broken diagnostic channel must not
    /// turn a hook bug into an unobserved task fault.
    /// </summary>
    private void ReportInitializationHookError(Exception ex)
    {
        if (UnhandledErrorHandler is null)
        {
            // No host wired an error handler (manually driven actor): same stderr fallback
            // as InvokeErrorHandler.
            try
            {
                Console.Error.WriteLine(
                    $"[PicoActor] OnInitialized threw on actor {Id}: {ex.Message}"
                );
            }
            catch
            {
                // Last-resort diagnostic sink failed — swallow: the stop below is the point.
            }

            return;
        }

        // The error seam is typed on a command (it exists for message dispatch). The hook
        // runs outside one, so hand it an explicit token instead of passing null or an
        // unrelated command: the report reads "on InitializationHookCommand".
        InvokeErrorHandler(ex, InitializationHookCommand.Instance);
    }

    /// <summary>
    /// Signal the consumption loop to stop: release the ready gate (in case it
    /// was never signaled — e.g., discarded after failed creation or duplicate
    /// GetAsync rebuild), cancel the stop token, and complete the mailbox.
    /// Synchronous and safe from any thread, including the actor's own message
    /// turn (the loop exits asynchronously — no self-await). Idempotent.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void SignalStop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        // Order matters: close the mailbox BEFORE cancelling. The loop disposes its CTS
        // on exit, so Cancel() can throw ObjectDisposedException — running it last keeps
        // the writer closure (and therefore the stop) effective even then.
        _ready.TrySetResult(true);
        _mailbox.Writer.TryComplete();
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The loop already exited and disposed its CTS: the stop is a no-op.
        }
    }

    /// <summary>
    /// Signal the loop to stop and wait for it to finish. Idempotent; safe after the
    /// loop already exited. If the loop faulted (e.g. an initialization failure), the
    /// fault is rethrown here — callers decide whether to propagate it.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public async ValueTask StopAsync()
    {
        SignalStop();
        // A faulted loop (e.g. init failure) rethrows here — callers decide
        // whether to propagate.
        await _loopTask.ConfigureAwait(false);
    }

    /// <summary>IAsyncDisposable — delegates to StopAsync.</summary>
    async ValueTask IAsyncDisposable.DisposeAsync() => await StopAsync();

    /// <summary>
    /// Framework-only command that asks the loop to reclaim this actor's materialized state once
    /// the mailbox reaches it (see <see cref="RequestPassivation"/>). Never dispatched to
    /// OnMessageAsync, never persisted, never handed to application code.
    /// </summary>
    private sealed record PassivateSentinel : ICommand
    {
        public static PassivateSentinel Instance { get; } = new();
    }
}
