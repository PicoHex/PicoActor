namespace PicoActor.Abs;

/// <summary>
/// Actor runtime. Entry point for creating, locating, messaging, and stopping actors.
/// Registered as a PicoDI Singleton.
/// </summary>
public interface IActorSystem
{
    /// <summary>
    /// Register factories for creating and rebuilding an actor.
    /// <paramref name="createFactory"/> receives the caller's command and returns a new actor.
    /// <paramref name="rebuildFactory"/> returns an actor without a command — used by GetAsync
    /// to rebuild from persisted events. If null, GetAsync cannot rebuild this type.
    /// Both factories should inject the same infrastructure dependencies (ILlmClient, etc.).
    /// Registering the same actor type twice throws — a duplicate registration is a
    /// startup bug, and silently overwriting the first factory would hide it.
    /// Types the runtime cannot serve are rejected here as well: an implementation that
    /// does not derive from <see cref="Actor"/>, or an <see cref="IEventSourcedActor"/> that
    /// does not derive from <see cref="EventSourcedActor"/> (persistence is only wired for
    /// the concrete bases).
    /// </summary>
    void Register<T>(Func<ICommand, T> createFactory, Func<T>? rebuildFactory = null)
        where T : IActor;

    /// <summary>
    /// Create a new actor using the registered factory.
    /// 1. Calls the factory with the command to construct the actor (pure, no DI).
    /// 2. Assigns a UUID v7 Id.
    /// 3. Registers in the system.
    /// 4. Calls SignalReady() to release the consumption loop.
    /// Returns only after the actor finished initializing (construction events persisted).
    /// Two failure classes, told apart by the exception type:
    /// <para>
    /// - <see cref="InvalidOperationException"/> — lifecycle refusal: no factory registered
    ///   for T, creation while the system is stopping (creating an actor during
    ///   <see cref="StopAllAsync"/> is refused, because it would outlive the shutdown), and
    ///   an actor stopped DURING its initialization by a concurrent
    ///   <see cref="StopAsync"/>/<see cref="RequestStop"/> — a handle to an actor that is
    ///   already gone is never returned.
    /// </para>
    /// <para>
    /// - The ORIGINAL exception, rethrown unchanged (not wrapped in
    ///   <see cref="InvalidOperationException"/>), when initialization itself fails — a
    ///   store failure surfaces as the store's own exception type.
    /// </para>
    /// </summary>
    ValueTask<T> CreateAsync<T>(ICommand command)
        where T : IActor;

    /// <summary>
    /// Get an actor by UUID v7. Returns from memory if active;
    /// otherwise rebuilds from persisted events using the registered rebuildFactory.
    /// Returns null if not found or no rebuildFactory was registered.
    /// <para>
    /// Contract details callers can rely on:
    /// - Returns <c>default</c> for the whole duration of <see cref="StopAllAsync"/>. The
    ///   stopping window is not a moment to resurrect or hand out an actor that the mass
    ///   stop is terminating.
    /// - A registry hit WAITS for that instance's initialization to finish; an instance
    ///   that is registered but still initializing is never handed out early.
    /// - MAY return an already-unregistered TERMINAL saga instance (completed/failed):
    ///   its state (IsCompleted / IsFailed / FailedReason) is readable, but it is out of the
    ///   system by design — <see cref="Send"/> and <see cref="AskAsync{TResult}"/> on it
    ///   throw <see cref="KeyNotFoundException"/>. Never a live handle to a non-terminal
    ///   actor that stopped during initialization.
    /// - An instance whose initialization FAILED is not returned once its cleanup has
    ///   removed it from the registry. BEST EFFORT, not a guarantee: "resolve" and "remove"
    ///   are separate steps, so a lookup that completes inside that narrow window can still
    ///   see the failed instance (its init-completion signal has already fired either way).
    /// - Concurrent calls for the same id are single-flight: one rebuild, one
    ///   initialization, one resume — the other callers resolve to that instance.
    /// - When idle passivation is enabled (<c>ActorSystemOptions.IdleTimeout</c>), this is the
    ///   REACTIVATION path: an actor that was idle long enough gave its materialized state back
    ///   and is absent from the registry until this call rebuilds it from its event stream (same
    ///   id, same version, same state). While it is absent it behaves exactly like any
    ///   unregistered actor: <see cref="Send"/> and <see cref="AskAsync{TResult}"/> throw
    ///   <see cref="KeyNotFoundException"/>, because they have no way to know which rebuild
    ///   factory applies (there is no type argument to read it from). Only actors the runtime can
    ///   rebuild are ever reclaimed — an event-sourced type registered with a rebuild factory;
    ///   everything else stays in memory for the lifetime of the system.
    /// </para>
    /// <para>
    /// Do NOT call this from inside your own initialization (or from initializations that
    /// reference each other in a cycle) for your own id: the per-id gate is held through
    /// initialization, so the nested call waits for an initialization that cannot finish.
    /// </para>
    /// </summary>
    ValueTask<T?> GetAsync<T>(Guid id)
        where T : IActor;

    /// <summary>
    /// Send a fire-and-forget command to an actor.
    /// Throws <see cref="KeyNotFoundException"/> when the actor is not registered, and
    /// <see cref="InvalidOperationException"/> when the mailbox is already closed (the
    /// actor is stopping — the stop-race window); a silently dropped command is worse.
    /// </summary>
    void Send(Guid id, ICommand command);

    /// <summary>Send a command and await the result.</summary>
    ValueTask<TResult> AskAsync<TResult>(Guid id, ICommand command);

    /// <summary>
    /// Stop an actor: remove it from the registry, then signal its loop to stop, and
    /// wait for the loop to exit. Messages already buffered in the mailbox are still
    /// dispatched before the loop exits (a graceful stop drains the queue — nothing is
    /// silently dropped); messages sent after the stop fail with KeyNotFoundException
    /// because the registry entry is removed first. Idempotent.
    /// <para>
    /// The wait is bounded by <c>ActorSystemOptions.StopTimeout</c> when that is configured:
    /// a handler that does not observe its stop token then no longer parks this call — the
    /// wait is abandoned (with a warning) and this method returns. The loop itself cannot be
    /// aborted, so treat that actor as untrusted. A faulted loop's exception is rethrown
    /// either way.
    /// </para>
    /// </summary>
    ValueTask StopAsync(Guid id);

    /// <summary>
    /// Remove the actor from the registry and signal its loop to stop, without
    /// waiting for the loop to finish. Safe to call from within the actor's own
    /// message turn (no self-await — the loop exits asynchronously). Idempotent:
    /// no-op if the actor is already removed. Use <see cref="StopAsync"/> when the
    /// caller must wait for full termination.
    /// </summary>
    void RequestStop(Guid id);

    /// <summary>
    /// Gracefully stop every registered actor (host disposal — actor loops/CTSs
    /// must not leak until process exit). Idempotent per actor.
    /// <para>
    /// Best effort: this method does NOT throw — a failing actor (or a throwing log sink) is
    /// reported and the remaining actors are still stopped. The registry is drained in
    /// rounds, so an actor that registers while the shutdown runs (a creation that already
    /// passed its guard) is stopped too. While it runs, <see cref="CreateAsync{T}"/> throws
    /// and <see cref="GetAsync{T}"/> returns <c>default</c>; the window stays open until the
    /// last concurrent call returns, so the system is usable again afterwards.
    /// </para>
    /// <para>
    /// By default every actor is awaited WITHOUT a bound, so a message handler that ignores
    /// its actor's stop token (it keeps awaiting something else) blocks this method until
    /// that handler returns. Configure <c>ActorSystemOptions.StopTimeout</c> to make the
    /// shutdown bounded: each actor then gets that budget, an actor that misses it is
    /// reported as a warning and abandoned, and the remaining actors are still stopped. The
    /// abandoned loop cannot be aborted by the framework — treat that actor as untrusted.
    /// </para>
    /// <para>
    /// Idle passivation stops with the shutdown: the background sweeper is cancelled first and
    /// awaited to a full stop (so no sweep can reclaim an actor while the mass stop runs), and it
    /// is not restarted afterwards. A system that stays alive can still create and look up
    /// actors — it just no longer reclaims idle ones; build a new system to re-enable it.
    /// </para>
    /// </summary>
    ValueTask StopAllAsync();

    /// <summary>
    /// Enumerate aggregate ids whose FIRST event type name matches, then
    /// filter by <paramref name="firstEventMatch"/>. Empty if the store does not
    /// support enumeration. Recovery-path API — not for hot paths.
    /// </summary>
    ValueTask<IReadOnlyList<Guid>> FindAggregateIds(
        string firstEventType,
        Func<IDomainEvent, bool> firstEventMatch
    );

    /// <summary>
    /// Explicitly resumes all interrupted sagas: enumerates by first-event type name
    /// (Type.Name exact match, case-sensitive) and recovers each via GetAsync
    /// single-flight. Sagas already terminal (Completed/Failed) before the rebuild
    /// are filtered out (not resurrected); terminal states newly produced on the
    /// resume path are classified by status. Returns an empty list when nothing matches.
    /// Batch failure semantics: without <paramref name="onItemError"/>, any saga
    /// recovery exception fails fast and propagates upward (the caller can retry the
    /// whole batch); with it, items are isolated — the failing item is reported to
    /// the callback (exception, sagaId) and the remaining sagas of the same type
    /// continue recovery.
    /// </summary>
    ValueTask<IReadOnlyList<SagaResumeResult>> ResumeInterruptedSagasAsync<TSaga>(
        string firstEventType,
        Func<IDomainEvent, bool>? firstEventMatch = null,
        Action<Exception, Guid>? onItemError = null
    )
        where TSaga : SagaActor;

    /// <summary>
    /// Creates a SagaActor, sends a command, waits for the result, and the saga
    /// auto-stops when finished.
    /// On success returns SagaExecution(Id, Result); on business failure throws
    /// SagaExecutionException(Id, Reason). Callers do not need to call StopAsync —
    /// the saga terminates itself.
    /// </summary>
    ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command)
        where TSaga : SagaActor;
}
