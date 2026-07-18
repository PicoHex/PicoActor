namespace PicoNode.Actor;

/// <summary>
/// Default IActorSystem implementation. Manages actor lifecycle:
/// creation, retrieval, message routing, and stopping.
/// Registered as a PicoDI Singleton.
/// </summary>
public sealed class ActorSystem : IActorSystem
{
    private readonly ConcurrentDictionary<Guid, ActorBase> _registry = new();
    private readonly ConcurrentDictionary<Type, Func<ICommand, object>> _factories = new();
    private readonly ConcurrentDictionary<Type, Func<object>> _rebuildFactories = new();
    private readonly IEventStore _eventStore;
    private readonly ILogger? _logger;

    /// <summary>
    /// Creates an ActorSystem with the given event store and optional logger.
    /// Pass <see cref="InMemoryEventStore"/> for pure in-memory mode.
    /// </summary>
    public ActorSystem(IEventStore eventStore, ILogger? logger = null)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    // ═══════════════════════════════════════════════════════════
    // Registration
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public void Register<T>(Func<ICommand, T> createFactory, Func<T>? rebuildFactory = null)
        where T : IActor
    {
        _factories[typeof(T)] = cmd => createFactory(cmd)!;
        if (rebuildFactory is not null)
            _rebuildFactories[typeof(T)] = () => rebuildFactory()!;
    }

    // ═══════════════════════════════════════════════════════════
    // Creation
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public async ValueTask<T> CreateAsync<T>(ICommand command)
        where T : IActor
    {
        if (!_factories.TryGetValue(typeof(T), out var factory))
            throw new InvalidOperationException(
                $"No factory registered for {typeof(T).Name}. Call Register<T> first."
            );

        // 1. Call factory with creation command → constructor processes atomically
        var actor = (ActorBase)factory(command);

        // 2. Assign framework-generated UUID v7
        var id = Guid.CreateVersion7();
        actor.Id = id;

        // 2b. Set system reference for spawn operations
        actor.System = this;
        WireErrorHandler(actor);

        // 3. Wire event store if this is an ES actor
        if (actor is EventSourcedActor es)
            es.EventStore = _eventStore;

        // 4. Register in the system
        _registry[id] = actor;

        // 5. Release the consumption loop gate
        actor.SignalReady();

        // 6. Wait for initialization (OnReadyAsync: persist + mutate).
        //    If persistence fails, remove from registry and propagate exception.
        try
        {
            await actor.InitCompletedTask.ConfigureAwait(false);
        }
        catch
        {
            _registry.TryRemove(id, out _);
            _logger?.Error($"Actor {typeof(T).Name} {id} initialization failed, removed");
            throw;
        }

        _logger?.Info($"Actor {typeof(T).Name} created: {id}");

        return (T)(IActor)actor;
    }

    // ═══════════════════════════════════════════════════════════
    // Retrieval
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public async ValueTask<T?> GetAsync<T>(Guid id)
        where T : IActor
    {
        if (_registry.TryGetValue(id, out var existing))
            return (T)(IActor)existing;

        if (!_rebuildFactories.TryGetValue(typeof(T), out var rebuildFactory))
            return default;

        if (!typeof(IEventSourcedActor).IsAssignableFrom(typeof(T)))
            return default;

        var events = await _eventStore.LoadAsync(id).ConfigureAwait(false);
        if (events.Count == 0)
            return default;

        var actor = (ActorBase)(IActor)rebuildFactory();
        actor.Id = id;
        actor.System = this;
        WireErrorHandler(actor);

        var es = (IEventSourcedActor)actor;
        ((EventSourcedActor)actor).EventStore = _eventStore;
        es.ReplayEvents(events);

        if (!_registry.TryAdd(id, actor))
        {
            _logger?.Warning(
                $"Actor {typeof(T).Name} {id} already rebuilt by another thread, discarding duplicate"
            );
            // Read the winner safely. The winner may have been stopped concurrently
            // while we were stopping our duplicate (the await below yields). Using the
            // throwing indexer _registry[id] would throw KeyNotFoundException in that
            // window; TryGetValue avoids it and returns null if the actor is gone.
            if (_registry.TryGetValue(id, out var winner))
            {
                await actor.StopAsync().ConfigureAwait(false);
                return (T)(IActor)winner;
            }

            // Winner was stopped concurrently; no live actor remains.
            await actor.StopAsync().ConfigureAwait(false);
            return default;
        }

        actor.SignalReady();

        _logger?.Info($"Actor {typeof(T).Name} rebuilt from events: {id} (v{es.Version})");

        return (T)(IActor)actor;
    }

    /// <summary>
    /// Route unhandled fire-and-forget message errors to the logger so they are
    /// not silently swallowed. Ask-style failures still fault their TCS.
    /// </summary>
    private void WireErrorHandler(ActorBase actor)
    {
        if (_logger is null)
            return;
        actor.UnhandledErrorHandler = (ex, cmd) =>
            _logger.Error(
                $"Unhandled error in actor {actor.Id} on {cmd.GetType().Name}: {ex.Message}"
            );
    }

    // ═══════════════════════════════════════════════════════════
    // Messaging
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public void Send(Guid id, ICommand command)
    {
        if (!_registry.TryGetValue(id, out var actor))
            throw new KeyNotFoundException($"Actor {id} not found.");

        actor.Post(new Envelope { Command = command });
    }

    /// <inheritdoc/>
    public async ValueTask<TResult> AskAsync<TResult>(Guid id, ICommand command)
    {
        if (!_registry.TryGetValue(id, out var actor))
            throw new KeyNotFoundException($"Actor {id} not found.");

        var tcs = new TaskCompletionSource<object?>();
        actor.Post(new Envelope { Command = command, Tcs = tcs });

        var result = await tcs.Task.ConfigureAwait(false);
        return (TResult)result!;
    }

    // ═══════════════════════════════════════════════════════════
    // Turn cancellation
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// Cancel the currently running long-running operation on an actor
    /// without stopping the actor. Only works on actors implementing ICancellable.
    /// This bypasses the mailbox — immediate effect.
    /// Safe to call when no operation is running (no-op).
    /// </summary>
    public void CancelTurn(Guid id)
    {
        if (_registry.TryGetValue(id, out var actor) && actor is ICancelable c)
            c.CancelCurrentTurn();
    }

    // ═══════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public async ValueTask StopAsync(Guid id)
    {
        if (!_registry.TryRemove(id, out var actor))
            return; // Idempotent: already stopped or never existed

        _logger?.Info($"Stopping actor {id}");
        await actor.StopAsync().ConfigureAwait(false);
        _logger?.Info($"Actor {id} stopped");
    }
}
