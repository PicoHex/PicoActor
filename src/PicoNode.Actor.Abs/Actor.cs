namespace PicoNode.Actor.Abs;

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

    // TaskCompletionSource (non-generic) not available on netstandard2.0.
    private readonly TaskCompletionSource<bool> _ready = new();
    private readonly TaskCompletionSource<bool> _initCompleted = new();
    private readonly Task _loopTask;
    private int _stopped;

    /// <summary>
    /// Framework-generated UUID v7 identity.
    /// Set by IActorSystem after construction, before SignalReady().
    /// Immutable thereafter.
    /// </summary>
    public Guid Id { get; internal set; }

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
            if (!task.IsCompleted)
                throw new InvalidOperationException(
                    "OnMessageAsync must complete synchronously for creation commands. "
                        + "Use RaiseEvent or return a value directly; do not await."
                );
        }
        catch
        {
            // Release gate so RunAsync can exit, then clean up
            _ready.TrySetResult(true);
            _cts.Cancel();
            _mailbox.Writer.Complete();
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
        _loopTask = RunAsync(_cts.Token);
    }

    /// <summary>
    /// Called by IActorSystem once the actor is fully initialized:
    /// Id assigned, registered in the system, and (for ES actors) replayed from events.
    /// Releases the gated consumption loop to start processing queued messages.
    /// </summary>
    internal void SignalReady() => _ready.TrySetResult(true);

    /// <summary>
    /// The actor's cancellation token. Canceled when StopAsync is called.
    /// Subclasses pass this to async operations so Stop can interrupt long-running work.
    /// </summary>
    protected CancellationToken StopToken => _cts.Token;

    /// <summary>
    /// Reference to the owning IActorSystem. Set by ActorSystem during CreateAsync/GetAsync.
    /// Subclasses use this for spawn operations.
    /// </summary>
    protected internal IActorSystem? System { get; set; }

    /// <summary>
    /// Outbound channel writer. External subscribers (e.g., SSE Server) set this
    /// to receive progress/diagnostic events from the actor.
    /// Symmetric to the Mailbox: Mailbox receives commands (入站),
    /// OutputChannel broadcasts events (出站).
    /// Weak guarantee — TryWrite silently drops if no subscriber or channel full.
    /// Actor state is never affected by output events.
    /// </summary>
    public ChannelWriter<ActorOutputEvent>? OutputWriter { get; set; }

    /// <summary>
    /// Hook invoked when a fire-and-forget (Send) message throws and there is no
    /// TCS to propagate the exception to. Set by IActorSystem to route unhandled
    /// message errors to logging. Ask-style failures still fault the TCS first.
    /// </summary>
    internal Action<Exception, ICommand>? UnhandledErrorHandler { get; set; }

    /// <summary>
    /// Write an event to the OutputChannel. No-op if no subscriber.
    /// Subclasses call this during OnMessageAsync to notify external observers.
    /// </summary>
    protected void WriteOutput(
        string type,
        string? data = null,
        string? toolCallId = null,
        string? toolName = null,
        string? turnId = null
    ) => OutputWriter?.TryWrite(new ActorOutputEvent(type, data, toolCallId, toolName, turnId));

    /// <summary>
    /// Task that completes when initialization finishes (OnReadyAsync succeeds or fails).
    /// Used by ActorSystem.CreateAsync to wait for persistence before returning.
    /// </summary>
    internal Task InitCompletedTask => _initCompleted.Task;

    /// <summary>Called by IActorSystem to deliver an envelope.</summary>
    internal void Post(Envelope envelope) => _mailbox.Writer.TryWrite(envelope);

    private async Task RunAsync(CancellationToken ct)
    {
        // Gate: wait until ActorSystem calls SignalReady().
        // This guarantees that Id is assigned, the actor is in the registry,
        // and (for ES actors) ReplayEvents has completed before any message is dispatched.
        // Messages posted during this wait safely queue in the Channel.
        await _ready.Task.ConfigureAwait(false);

        // Hook: flush any events produced during construction (EventSourcedActor).
        // Base implementation is a no-op.
        try
        {
            await OnReadyAsync().ConfigureAwait(false);
            _initCompleted.TrySetResult(true);
        }
        catch (Exception ex)
        {
            _initCompleted.TrySetException(ex);
            throw;
        }

        try
        {
            await foreach (var envelope in _mailbox.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
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
                        UnhandledErrorHandler?.Invoke(ex, envelope.Command);
                }
            }
        }
        catch (OperationCanceledException)
        { /* expected on Stop */
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

    /// <summary>Stop the consumption loop, wait for it to finish, and dispose resources.</summary>
    internal async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        // Release the gate in case it was never signaled (e.g., discarded after failed creation
        // or duplicate GetAsync rebuild). This allows RunAsync to reach the cancellation.
        _ready.TrySetResult(true);

        _cts.Cancel();
        _mailbox.Writer.Complete();
        await _loopTask.ConfigureAwait(false);
        _cts.Dispose();
    }

    /// <summary>IAsyncDisposable — delegates to StopAsync.</summary>
    async ValueTask IAsyncDisposable.DisposeAsync() => await StopAsync();
}
