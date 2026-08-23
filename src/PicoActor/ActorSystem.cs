namespace PicoActor;

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
    private readonly IDomainEventPublisher? _publisher;
    private readonly ILogger? _logger;

    /// <summary>
    /// Creates an ActorSystem from <paramref name="options"/>. This is the single
    /// construction path — the event store must be chosen explicitly via
    /// <see cref="ActorSystemOptions.EventStore"/>.
    /// </summary>
    public ActorSystem(ActorSystemOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _eventStore = options.EventStore;
        _publisher = options.DomainEventPublisher;
        _logger = options.Logger;
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
        var id = Guid.CreateVersion7();
        if (!_factories.TryGetValue(typeof(T), out var factory))
            throw new InvalidOperationException(
                $"No factory registered for {typeof(T).Name}. Call Register<T> first."
            );

        // 1. Call factory with creation command → constructor processes atomically
        var actor = (ActorBase)factory(command);

        // 2. Wire into the system: framework-generated UUID v7 identity (actor ids
        //    are framework-owned), system reference, error routing, ES plumbing
        WireActor(actor, id);

        // 4. Register in the system — a conflict means a duplicate-id bug; fail loudly.
        //    Discard: the creation constructor already staged events (RaiseEvent
        //    in Actor(ICommand)) that flush in the loop's OnReadyAsync. Without
        //    clearing them, the discarded actor's init flush would race the
        //    winner's flush on the same stream — surfacing ConcurrencyException
        //    here instead of the documented InvalidOperationException, or (given
        //    the store's check-then-act gap) silently appending duplicate events.
        if (!_registry.TryAdd(id, actor))
        {
            if (actor is IEventSourcedActor eventSourced)
                eventSourced.CommitEvents();
            await actor.StopAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"Actor {id} already exists.");
        }

        // 5. Release the consumption loop gate and wait for initialization
        //    (OnReadyAsync: persist + mutate).
        //    If persistence fails, remove from registry and propagate exception.
        await CompleteInitializationAsync(actor, id);

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
        WireActor(actor, id);

        var es = (IEventSourcedActor)actor;

        // Full-path resource cleanup: any failure point after the rebuild (replay/init)
        // marks the actor discarded + StopAsync + rethrow.
        // The actor is already constructed (_loopTask started, gate not released) —
        // without cleanup it leaks; without the discarded marker, StopAsync releasing
        // the gate would let partially replayed state run OnReadyAsync (including saga resume).
        try
        {
            es.ReplayEvents(events);
        }
        catch
        {
            actor.MarkDiscarded();
            await actor.StopAsync().ConfigureAwait(false);
            throw;
        }

        // Terminal events that already existed before the rebuild (Completed/Failed) → do not resurrect
        if (actor is SagaActor { IsCompleted: true } or SagaActor { IsFailed: true })
        {
            await actor.StopAsync().ConfigureAwait(false);
            return default;
        }

        if (!_registry.TryAdd(id, actor))
        {
            _logger?.Warning(
                $"Actor {typeof(T).Name} {id} already rebuilt by another thread, discarding duplicate"
            );
            actor.MarkDiscarded();
            await actor.StopAsync().ConfigureAwait(false);

            if (_registry.TryGetValue(id, out var winner))
                return (T)(IActor)winner;

            return default;
        }

        // Release the consumption loop gate and wait for initialization
        // (OnReadyAsync: persist + mutate). The helper removes the actor from the
        // registry and stops it on failure, then rethrows the original exception.
        await CompleteInitializationAsync(actor, id);

        _logger?.Info($"Actor {typeof(T).Name} rebuilt from events: {id} (v{es.Version})");

        return (T)(IActor)actor;
    }

    /// <summary>
    /// Wire an actor into the system before it becomes visible: assign the
    /// framework-generated UUID v7 identity (actor ids are framework-owned),
    /// attach the system reference for spawn operations, route unhandled errors
    /// to the logger, and connect the event store/publisher for ES actors.
    /// Shared by <see cref="CreateAsync{T}"/> and <see cref="GetAsync{T}"/>.
    /// </summary>
    private void WireActor(ActorBase actor, Guid id)
    {
        actor.Id = id;
        actor.System = this;
        WireErrorHandler(actor);

        if (actor is EventSourcedActor es)
        {
            es.EventStore = _eventStore;
            es.Publisher = _publisher;
        }
    }

    /// <summary>
    /// Release the consumption loop gate and wait for initialization
    /// (OnReadyAsync: persist + mutate). On failure: remove the actor from the
    /// registry, stop it so its loop task terminates and is observed (RunAsync
    /// faults with the init exception) and its CTS is disposed, then rethrow the
    /// original exception. Shared by <see cref="CreateAsync{T}"/> and <see cref="GetAsync{T}"/>.
    /// </summary>
    private async ValueTask CompleteInitializationAsync(ActorBase actor, Guid id)
    {
        actor.SignalReady();

        try
        {
            await actor.InitCompletedTask.ConfigureAwait(false);
        }
        catch
        {
            _registry.TryRemove(id, out _);
            _logger?.Error($"Actor {actor.GetType().Name} {id} initialization failed, removed");
            // Stop the failed actor so its loop task terminates and is observed
            // (RunAsync faults with the init exception) and its CTS is disposed.
            // Swallow: the original exception must reach the caller.
            try
            {
                await actor.StopAsync().ConfigureAwait(false);
            }
            catch
            {
                // _loopTask already faulted with the init exception
            }
            throw;
        }
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

        var tcs = new TaskCompletionSource<object?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        if (!actor.Post(new Envelope { Command = command, Tcs = tcs }))
            tcs.TrySetException(new InvalidOperationException($"Actor {id} is stopping."));

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

    /// <inheritdoc/>
    public void RequestStop(Guid id)
    {
        if (!_registry.TryRemove(id, out var actor))
            return; // Idempotent: already stopped or never existed

        _logger?.Info($"Stopping actor {id} (requested)");
        actor.SignalStop();
    }

    /// <summary>
    /// Gracefully stop every registered actor (code review #10 — host disposal
    /// must not leak actor loops/CTSs until process exit).
    /// </summary>
    public async ValueTask StopAllAsync()
    {
        var ids = _registry.Keys.ToList();
        foreach (var id in ids)
            await StopAsync(id).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<Guid>> FindAggregateIds(
        string firstEventType,
        Func<IDomainEvent, bool> firstEventMatch
    )
    {
        if (_eventStore is not IEventStoreEnumerator enumerator)
            return Array.Empty<Guid>();

        var result = new List<Guid>();
        foreach (var id in enumerator.ListAggregateIds(firstEventType))
        {
            var first = await _eventStore.PeekFirstAsync(id).ConfigureAwait(false);
            if (first is not null && firstEventMatch(first))
                result.Add(id);
        }
        return result;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<SagaResumeResult>> ResumeInterruptedSagasAsync<TSaga>(
        string firstEventType,
        Func<IDomainEvent, bool>? firstEventMatch = null,
        Action<Exception, Guid>? onItemError = null
    )
        where TSaga : SagaActor
    {
        if (_eventStore is not IEventStoreEnumerator enumerator)
            return Array.Empty<SagaResumeResult>();

        var match = firstEventMatch ?? (_ => true);
        var results = new List<SagaResumeResult>();
        foreach (var id in enumerator.ListAggregateIds(firstEventType))
        {
            try
            {
                // First-event match first (peek only — no full-file read), so a
                // non-matching stream is skipped without a rebuild.
                var first = await _eventStore.PeekFirstAsync(id).ConfigureAwait(false);
                if (first is null || !match(first))
                    continue;

                // Active hit → classify directly without rebuilding; already terminal
                // before rebuild → GetAsync returns null, skip; new terminal state
                // from resume → read from the returned instance
                var saga = await GetAsync<TSaga>(id).ConfigureAwait(false);
                if (saga is null)
                    continue;

                if (saga.IsCompleted)
                    results.Add(new SagaResumeResult(id, SagaResumeStatus.Completed));
                else if (saga.IsFailed)
                    results.Add(
                        new SagaResumeResult(id, SagaResumeStatus.Failed, saga.FailedReason)
                    );
                else
                    results.Add(new SagaResumeResult(id, SagaResumeStatus.Running));
            }
            catch (Exception ex)
            {
                if (onItemError is null)
                    throw;
                onItemError(ex, id);
            }
        }
        return results;
    }

    /// <inheritdoc/>
    public async ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command)
        where TSaga : SagaActor
    {
        var saga = await CreateAsync<TSaga>(command).ConfigureAwait(false);
        var result = await AskAsync<TResult>(saga.Id, command).ConfigureAwait(false);
        return new SagaExecution<TResult>(saga.Id, result);
        // SagaExecutionException propagates through AskAsync after SagaActor.ProcessAsync
        // faults the TCS. Saga auto-stops via SagaActor.ProcessAsync → ScheduleStop.
    }
}
