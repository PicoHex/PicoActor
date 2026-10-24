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

    /// <summary>
    /// Per-id single-flight gates for <see cref="GetAsync{T}"/> (rebuild + initialization).
    /// Entries are never reclaimed — see the gate comment in <see cref="GetAsync{T}"/>.
    /// </summary>
    private readonly ConcurrentDictionary<Guid, RebuildGate> _rebuildGates = new();

    /// <summary>1 while <see cref="StopAllAsync"/> runs: new work is refused, lookups miss.</summary>
    private int _stopping;

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
        ArgumentNullException.ThrowIfNull(createFactory);

        // Composition-root validation: the runtime owns the mailbox/lifecycle and only
        // wires event-store persistence for Actor / EventSourcedActor. Reject shapes it
        // cannot serve BEFORE anything is built — otherwise a hand-rolled IEventSourcedActor
        // would silently never persist, and a non-Actor implementation would fail later
        // with a bare InvalidCastException that hides the real contract.
        if (!typeof(ActorBase).IsAssignableFrom(typeof(T)))
            throw new InvalidOperationException(
                $"{typeof(T).Name} implements IActor but does not derive from Actor; "
                    + "actor types must derive from Actor (the runtime owns the mailbox and lifecycle)."
            );

        if (
            typeof(IEventSourcedActor).IsAssignableFrom(typeof(T))
            && !typeof(EventSourcedActor).IsAssignableFrom(typeof(T))
        )
            throw new InvalidOperationException(
                $"{typeof(T).Name} implements IEventSourcedActor but does not derive from "
                    + "EventSourcedActor; the runtime only wires event-store persistence for "
                    + "EventSourcedActor — a hand-rolled implementation would silently never persist."
            );

        // TryAdd, not assignment: a duplicate registration is a startup bug, and silently
        // replacing the first factory would hide it. Atomic — concurrent Register calls
        // for the same type cannot both win.
        if (!_factories.TryAdd(typeof(T), cmd => createFactory(cmd)!))
            throw new InvalidOperationException(
                $"{typeof(T).Name} is already registered. Register each actor type exactly once."
            );

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
        // Refuse new actors while the system is shutting down: one registered after the
        // mass stop's snapshot would outlive the shutdown (and be handed to a caller that
        // is about to be disposed). Checked before anything is constructed.
        if (Volatile.Read(ref _stopping) != 0)
            throw new InvalidOperationException("Actor system is stopping.");

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

        // 3. Register in the system — a conflict means a duplicate-id bug; fail loudly.
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

        // 4. Release the consumption loop gate and wait for initialization
        //    (OnReadyAsync: persist + mutate).
        //    If persistence fails, remove from registry and propagate exception.
        await CompleteInitializationAsync(actor, id);

        // Initialization can finish AFTER the actor was taken out of the system (a
        // concurrent StopAsync/RequestStop, or the actor removing itself). Its init still
        // succeeds, so returning it would hand the caller a live-looking handle to a dead
        // actor - fail loudly and clean the loop up instead.
        if (!_registry.TryGetValue(id, out var registered) || !ReferenceEquals(registered, actor))
        {
            try
            {
                await actor.StopAsync().ConfigureAwait(false);
            }
            catch
            {
                // Cleanup only: the documented exception must be the one the caller sees.
            }

            throw new InvalidOperationException(
                $"Actor {id} was stopped during initialization; it is not registered."
            );
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
        // A lookup during shutdown must not resurrect or hand out an actor that the mass
        // stop is about to terminate (or has just terminated).
        if (Volatile.Read(ref _stopping) != 0)
            return default;

        if (_registry.TryGetValue(id, out var existing))
            return await ResolveRegisteredAsync<T>(id, existing).ConfigureAwait(false);

        if (!_rebuildFactories.TryGetValue(typeof(T), out var rebuildFactory))
            return default;

        if (!typeof(IEventSourcedActor).IsAssignableFrom(typeof(T)))
            return default;

        // Prefetch the stream the rebuild replays. Read BEFORE taking the per-id gate: an
        // id with no stream at all must not take the gate, and reading outside the gate
        // keeps lookups of different ids fully parallel.
        // This snapshot is NEVER trusted for the rebuild itself: it can already be stale
        // by the time the gate is acquired (it is always re-read below).
        var events = await _eventStore.LoadAsync(id).ConfigureAwait(false);
        if (events.Count == 0)
            return default;

        // Single-flight per id: callers that miss the registry must not each build and
        // initialize their own copy of the same aggregate. A duplicate copy replays the
        // stream twice and, for a saga, resumes twice - so the second resume flushes on
        // top of the first and dies with ConcurrencyException. The gate is held THROUGH
        // initialization: that is what makes the re-check below meaningful (a registered
        // but still initializing instance must not be handed out).
        // Gates are NOT reclaimed for the process lifetime - the same trade-off
        // InMemoryEventStore._locks makes for its per-aggregate append locks (one
        // SemaphoreSlim per id, bounded by the number of aggregates this process touches).
        var gate = _rebuildGates.GetOrAdd(id, static _ => new RebuildGate());
        await gate.Semaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Another caller may have completed the rebuild while we waited.
            if (_registry.TryGetValue(id, out var winner))
                return await ResolveRegisteredAsync<T>(id, winner).ConfigureAwait(false);

            // Re-read UNCONDITIONALLY under the gate - the prefetch above is never the
            // baseline we replay. Two independent writes can land between the prefetch and
            // this line:
            //   - a previous caller's rebuild (it left nothing registered: a terminal saga
            //     that unregistered itself, a stopped copy, a failed init), and
            //   - the SHUTDOWN side: StopAsync removes the registry entry first and only
            //     then lets the draining loop flush its terminal event - that write is
            //     invisible to the registry, and the first caller into the gate ("entry 1")
            //     is exactly the caller that can be holding the pre-shutdown snapshot.
            // Replaying a stale snapshot resumes a saga that has already completed and dies
            // with ConcurrencyException (or appends a second terminal batch). Reads are
            // idempotent and cheap; correctness wins over saving one read.
            events = await _eventStore.LoadAsync(id).ConfigureAwait(false);
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
                await DiscardDuplicateAsync(actor);
                throw;
            }

            // Terminal events that already existed before the rebuild (Completed/Failed) → do not resurrect
            if (actor is SagaActor saga && (saga.IsCompleted || saga.IsFailed))
            {
                await actor.StopAsync().ConfigureAwait(false);
                return default;
            }

            if (!_registry.TryAdd(id, actor))
            {
                _logger?.Warning(
                    $"Actor {typeof(T).Name} {id} already rebuilt by another thread, discarding duplicate"
                );
                await DiscardDuplicateAsync(actor);

                if (_registry.TryGetValue(id, out var owner))
                {
                    // Same type guard as the registry-hit path above: if another thread won
                    // the rebuild with a DIFFERENT aggregate type, degrade to default instead
                    // of throwing InvalidCastException from the cast.
                    return owner is T typedOwner ? typedOwner : default;
                }

                return default;
            }

            // Release the consumption loop gate and wait for initialization
            // (OnReadyAsync: persist + mutate). The helper removes the actor from the
            // registry and stops it on failure, then rethrows the original exception.
            await CompleteInitializationAsync(actor, id);

            // Init can end with the instance no longer registered: a terminal saga removes
            // itself, and a concurrent stop can take it out. Resolve against the CURRENT
            // registry state instead of returning a dead handle.
            _logger?.Info($"Actor {typeof(T).Name} rebuilt from events: {id} (v{es.Version})");

            return ResolveAfterInitialization<T>(id, actor);
        }
        finally
        {
            gate.Semaphore.Release();
        }
    }

    /// <summary>
    /// Resolve a REGISTRY HIT. A registered instance appears before its initialization
    /// finishes, so a hit can hand out an actor that is still initializing - and therefore
    /// still able to remove itself (a terminal saga) or to be removed by its own init
    /// failure. Wait for init, then resolve against the CURRENT registry state.
    /// The type guard: an id is framework-owned per-actor, so a registry hit on a
    /// DIFFERENT aggregate type means the caller asked for the wrong T (e.g. querying
    /// ChatActor with a SessionActor id). Return default instead of throwing
    /// InvalidCastException so callers can fall back / degrade gracefully (regression:
    /// create_subagent's session-liveness probe died with "Specified cast is not valid").
    /// </summary>
    private async ValueTask<T?> ResolveRegisteredAsync<T>(Guid id, ActorBase registered)
        where T : IActor
    {
        if (registered is not T typed)
            return default;

        if (!registered.InitCompletedTask.IsCompleted)
        {
            try
            {
                await registered.InitCompletedTask.ConfigureAwait(false);
            }
            catch
            {
                // Init failed: the instance is gone (or about to be) and a replacement may
                // already own the id - resolve instead of returning a dead handle.
            }

            return ResolveAfterInitialization<T>(id, registered);
        }

        return typed;
    }

    /// <summary>
    /// Resolve what the caller should receive after <paramref name="actor"/> finished
    /// initializing: the instance currently owning <paramref name="id"/>; else
    /// <paramref name="actor"/> itself when it is a terminal saga (its final state is
    /// readable and it is unregistered BY DESIGN); else <c>default</c> — an unregistered
    /// non-terminal actor is dead (stopped during init) and must not be handed out.
    /// </summary>
    private T? ResolveAfterInitialization<T>(Guid id, ActorBase actor)
        where T : IActor
    {
        if (_registry.TryGetValue(id, out var current))
            return current is T typed ? typed : default;

        return actor is SagaActor { IsCompleted: true } or SagaActor { IsFailed: true }
            ? (T)(IActor)actor
            : default;
    }

    /// <summary>
    /// Mark a losing/duplicate actor copy as discarded and stop its loop so its
    /// resources are released. Shared by the GetAsync cleanup paths.
    /// </summary>
    private static async ValueTask DiscardDuplicateAsync(ActorBase actor)
    {
        actor.MarkDiscarded();
        await actor.StopAsync().ConfigureAwait(false);
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
        actor.AttachToSystem(id, this);
        WireErrorHandler(actor);

        if (actor is EventSourcedActor es)
            es.AttachPersistence(_eventStore, _publisher);
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
            // Ownership-checked removal: a blind TryRemove(id) deletes whatever currently
            // owns that id. By the time this failure surfaces, the failed instance may
            // already have been stopped and replaced in the registry by a concurrent
            // rebuild of the SAME id — deleting that entry would orphan a live actor
            // (registry-less, unreachable, never stopped). Remove only if the registered
            // instance is still THE failed one.
            if (RemoveIfSame(id, actor))
                _logger?.Error(
                    $"Actor {actor.GetType().Name} {id} initialization failed, removed"
                );
            else
                _logger?.Warning(
                    $"Actor {actor.GetType().Name} {id} initialization failed; the registry "
                        + "entry belongs to another instance now, left untouched"
                );
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
    /// Remove <paramref name="actor"/> from the registry only if it is still the
    /// registered instance for <paramref name="id"/>. Atomic: the key and the value are
    /// compared under the dictionary's own lock, so a concurrent replacement cannot slip
    /// between the check and the removal (a TryGetValue + TryRemove pair could).
    /// </summary>
    private bool RemoveIfSame(Guid id, ActorBase actor) =>
        ((ICollection<KeyValuePair<Guid, ActorBase>>)_registry).Remove(
            new KeyValuePair<Guid, ActorBase>(id, actor)
        );

    /// <summary>
    /// Route unhandled fire-and-forget message errors to the logger so they are
    /// not silently swallowed. Ask-style failures still fault their TCS.
    /// Without a logger the diagnostic still goes to stderr — consistent with
    /// <see cref="MediatorDomainEventPublisher"/>'s no-logger fallback: a lost
    /// command must not vanish without a trace.
    /// </summary>
    private void WireErrorHandler(ActorBase actor)
    {
        if (_logger is not null)
        {
            actor.UnhandledErrorHandler = (ex, cmd) =>
                _logger.Error(
                    $"Unhandled error in actor {actor.Id} on {cmd.GetType().Name}: {ex.Message}"
                );
            return;
        }

        actor.UnhandledErrorHandler = (ex, cmd) =>
            Console.Error.WriteLine(
                $"[PicoActor] Unhandled error in actor {actor.Id} on {cmd.GetType().Name}: {ex.Message}"
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

        // A closed mailbox means the actor is stopping but not yet removed from the
        // registry (stop-race window). Fail loud — a silently dropped command is worse.
        if (!actor.Post(new Envelope { Command = command }))
            throw new InvalidOperationException(
                $"Actor {id} is stopping; the command was not delivered."
            );
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

        // Result-shape guard: `(TResult)result!` used to throw a bare
        // NullReferenceException when the handler produced null for a value-type
        // result. Keep null valid for reference/nullable results, and fail with a
        // diagnostic InvalidCastException otherwise.
        if (result is TResult typed)
            return typed;
        if (result is null && default(TResult) is null)
            return default!;

        throw new InvalidCastException(
            $"AskAsync<{typeof(TResult).Name}> on actor {id} with {command.GetType().Name}: "
                + $"the handler produced {(result is null ? "no value (null)" : result.GetType().Name)}."
        );
    }

    // ═══════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public async ValueTask StopAsync(Guid id)
    {
        if (!_registry.TryRemove(id, out var actor))
            return; // Idempotent: already stopped or never existed

        // Signal the stop BEFORE anything else. The registry entry is already gone, so a
        // loop kept alive by a report that throws (or hangs) is unreachable for the rest
        // of the process lifetime — no later sweep, StopAllAsync included, could ever find
        // it again. The diagnostics are therefore best effort (TryLog swallows a broken
        // sink) and come after the signal.
        actor.SignalStop();
        TryLog(l => l.Info($"Stopping actor {id}"));

        await actor.StopAsync().ConfigureAwait(false);
        TryLog(l => l.Info($"Actor {id} stopped"));
    }

    /// <inheritdoc/>
    public void RequestStop(Guid id)
    {
        if (!_registry.TryRemove(id, out var actor))
            return; // Idempotent: already stopped or never existed

        // Same ordering as StopAsync: signal first, report afterwards (best effort).
        actor.SignalStop();
        TryLog(l => l.Info($"Stopping actor {id} (requested)"));
    }

    /// <summary>
    /// Gracefully stop every registered actor (host disposal must not leak actor
    /// loops/CTSs until process exit). Best effort: this method does NOT throw — a failing
    /// actor is reported through the logger and the remaining ones are still stopped.
    /// Drains the registry in rounds (bounded) instead of sweeping one snapshot, so an
    /// actor that registers while the shutdown runs is stopped too. Creation is refused
    /// (<see cref="CreateAsync{T}"/>) and lookups return default for the duration of the
    /// call; the stopping window closes when it returns.
    /// </summary>
    public async ValueTask StopAllAsync()
    {
        Interlocked.Exchange(ref _stopping, 1);
        try
        {
            // Bounded: each round stops everything registered at its start, so a registry
            // that keeps gaining actors (a concurrent CreateAsync that already passed its
            // check) is retried a few times and then reported instead of spinning forever.
            for (var round = 0; round < 10; round++)
            {
                if (_registry.IsEmpty)
                    return;

                foreach (var id in _registry.Keys.ToList())
                {
                    // Per-actor isolation: one failing stop (a faulted loop, a throwing log
                    // sink) must not leave every later actor running.
                    try
                    {
                        await StopAsync(id).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // The report is diagnostics, not control flow: a sink that throws
                        // must not resurrect the exception this method promises not to throw.
                        TryLog(l => l.Error($"Stopping actor {id} failed during shutdown: {ex.Message}"));
                    }
                }
            }

            if (!_registry.IsEmpty)
                TryLog(l =>
                    l.Warning($"{_registry.Count} actor(s) still registered after shutdown attempts")
                );
        }
        finally
        {
            // The window ends with the call: a host (or a test) that keeps the system alive
            // after a mass stop can create actors again.
            Volatile.Write(ref _stopping, 0);
        }
    }

    /// <summary>
    /// Best-effort diagnostic: report through the logger without letting a failing sink
    /// escape (shutdown reporting must never become a shutdown failure).
    /// </summary>
    private void TryLog(Action<ILogger> report)
    {
        if (_logger is null)
            return;

        try
        {
            report(_logger);
        }
        catch
        {
            // Diagnostics only: a broken sink is not a reason to fail the caller.
        }
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
        foreach (
            var id in await enumerator.ListAggregateIdsAsync(firstEventType).ConfigureAwait(false)
        )
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
        foreach (
            var id in await enumerator.ListAggregateIdsAsync(firstEventType).ConfigureAwait(false)
        )
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

    /// <summary>
    /// Per-id single-flight gate for <see cref="GetAsync{T}"/>: rebuild + initialization of
    /// one id run one caller at a time. The gate carries no state — every caller re-reads
    /// the stream under it (see GetAsync), so "who went first" never has to be inferred.
    /// </summary>
    private sealed class RebuildGate
    {
        public SemaphoreSlim Semaphore { get; } = new(1, 1);
    }
}
