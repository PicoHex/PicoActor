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
    /// </summary>
    void Register<T>(Func<ICommand, T> createFactory, Func<T>? rebuildFactory = null)
        where T : IActor;

    /// <summary>
    /// Create a new actor using the registered factory.
    /// 1. Calls the factory with the command to construct the actor (pure, no DI).
    /// 2. Assigns a UUID v7 Id.
    /// 3. Registers in the system.
    /// 4. Calls SignalReady() to release the consumption loop.
    /// </summary>
    ValueTask<T> CreateAsync<T>(ICommand command)
        where T : IActor;

    /// <summary>
    /// Get an actor by UUID v7. Returns from memory if active;
    /// otherwise rebuilds from persisted events using the registered rebuildFactory.
    /// Returns null if not found or no rebuildFactory was registered.
    /// </summary>
    ValueTask<T?> GetAsync<T>(Guid id)
        where T : IActor;

    /// <summary>Send a fire-and-forget command to an actor.</summary>
    void Send(Guid id, ICommand command);

    /// <summary>Send a command and await the result.</summary>
    ValueTask<TResult> AskAsync<TResult>(Guid id, ICommand command);

    /// <summary>Stop an actor. Waits for the current message to complete, discards remaining queue.</summary>
    ValueTask StopAsync(Guid id);

    /// <summary>
    /// Gracefully stop every registered actor (host disposal — actor loops/CTSs
    /// must not leak until process exit). Idempotent per actor.
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
