namespace PicoNode.Actor.Abs;

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
}
