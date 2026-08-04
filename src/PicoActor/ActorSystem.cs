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

        // 2. Assign a framework-generated UUID v7 — actor ids are framework-owned
        actor.Id = id;

        // 2b. Set system reference for spawn operations
        actor.System = this;
        WireErrorHandler(actor);

        // 3. Wire event store if this is an ES actor
        if (actor is EventSourcedActor es)
        {
            es.EventStore = _eventStore;
            es.Publisher = _publisher;
        }

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
        ((EventSourcedActor)actor).Publisher = _publisher;

        // 全路径资源清理:重建后任一失败点(重放/init)都标记 discarded + StopAsync + rethrow。
        // actor 已构造(_loopTask 已启动、gate 未释放)——不清理则泄漏;不带 discarded 标记时
        // StopAsync 释放 gate 会让部分重放状态误执行 OnReadyAsync(含 saga resume)。
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

        // 重建前已存在的终态事件(Completed/Failed)→ 不复活
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

        actor.SignalReady();

        try
        {
            await actor.InitCompletedTask.ConfigureAwait(false);
        }
        catch
        {
            // init 失败(如 resume 的 SagaFailed 落盘失败)→ 对称清理
            _registry.TryRemove(id, out _);
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
            var events = await _eventStore.LoadAsync(id).ConfigureAwait(false);
            if (events.Count > 0 && firstEventMatch(events[0]))
                result.Add(id);
        }
        return result;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<SagaResumeResult>> ResumeInterruptedSagasAsync<TSaga>(
        string firstEventType,
        Func<IDomainEvent, bool>? firstEventMatch = null
    )
        where TSaga : SagaActor
    {
        var ids = await FindAggregateIds(firstEventType, firstEventMatch ?? (_ => true))
            .ConfigureAwait(false);

        var results = new List<SagaResumeResult>(ids.Count);
        foreach (var id in ids)
        {
            // 活跃命中 → 直接分类不重建;重建前已终态 → GetAsync 返回 null 跳过;
            // resume 新终态 → 从返回实例读取
            var saga = await GetAsync<TSaga>(id).ConfigureAwait(false);
            if (saga is null)
                continue;

            if (saga.IsCompleted)
                results.Add(new SagaResumeResult(id, SagaResumeStatus.Completed));
            else if (saga.IsFailed)
                results.Add(new SagaResumeResult(id, SagaResumeStatus.Failed, saga.FailedReason));
            else
                results.Add(new SagaResumeResult(id, SagaResumeStatus.Running));
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
        // SagaExecutionException 由 SagaActor.ProcessAsync fault TCS 后经 AskAsync 透传。
        // Saga auto-stops via SagaActor.ProcessAsync → ScheduleStop.
    }
}
