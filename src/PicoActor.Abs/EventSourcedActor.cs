namespace PicoActor.Abs;

/// <summary>
/// Base class for Event-Sourced actors.
/// Inherits Actor; adds RaiseEvent, event management, version tracking, and persistence.
///
/// Follows the Persist-then-Mutate pattern:
///   OnMessageAsync → RaiseEvent (record only, no state change)
///   → Persist to IEventStore
///   → Mutate (apply events to in-memory state)
///   → CommitEvents
///
/// State is only mutated after successful persistence, guaranteeing that
/// in-memory state is always consistent with the event stream.
/// </summary>
public abstract class EventSourcedActor : Actor, IEventSourcedActor
{
    private readonly List<IDomainEvent> _events = new();

    /// <inheritdoc/>
    public ulong Version { get; protected set; }

    /// <summary>
    /// True while this instance is a rebuild whose state came from the persisted stream
    /// (<see cref="IEventSourcedActor.ReplayEvents"/>) rather than from a live creation
    /// command. Set by replay, cleared by <see cref="OnInitialized"/> once initialization
    /// finishes. "Version &gt; 0" is NOT a rebuild marker: a subclass may raise events in
    /// its constructor, which is a creation.
    /// </summary>
    internal bool IsReplaying { get; private set; }

    /// <summary>
    /// May be null in tests or non-persistent scenarios (pure in-memory mode).
    /// </summary>
    private IEventStore? _eventStore;

    /// <summary>Publish hook; default null (no publishing).</summary>
    private IDomainEventPublisher? _publisher;

    /// <summary>
    /// Framework wiring: attaches the event store and the publish hook.
    /// Called by IActorSystem via <see cref="Actor.AttachToSystem"/>'s flow, before
    /// SignalReady(). Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void AttachPersistence(IEventStore? eventStore, IDomainEventPublisher? publisher)
    {
        _eventStore = eventStore;
        _publisher = publisher;
    }

    /// <summary>Creation path. Chains to Actor(ICommand).</summary>
    protected EventSourcedActor(ICommand creationCommand)
        : base(creationCommand) { }

    /// <summary>Rebuild path. Chains to Actor().</summary>
    protected EventSourcedActor() { }

    /// <summary>
    /// Record a domain event and increment Version.
    /// Does NOT mutate state — Mutate is called later, after successful persistence.
    /// Called by subclasses within OnMessageAsync.
    /// </summary>
    protected void RaiseEvent<T>(T @event)
        where T : notnull, IDomainEvent
    {
        _events.Add(@event);
        Version++;
    }

    /// <summary>
    /// Mutate in-memory state from a domain event.
    /// Called by FlushEventsAsync (after persistence) and ReplayEvents (recovery).
    /// Must be a pure function — no validation, no side effects.
    /// Throwing is a FATAL actor fault: the batch is already durable and cannot be
    /// un-applied, so the actor is stopped (fail loud) instead of continuing with
    /// untrusted state or retrying the leaked batch on the next flush.
    /// </summary>
    protected abstract void Mutate(IDomainEvent @event);

    /// <summary>
    /// Framework event handling hook: when it returns true, the event is handled
    /// internally by the framework and is NOT delivered to the subclass Mutate.
    /// Base default is false (all events go to Mutate); SagaActor overrides it to
    /// handle SagaCompleted/SagaFailed. Effective on both paths:
    /// FlushEventsAsync (after persistence) and ReplayEvents (recovery).
    /// </summary>
    protected virtual bool TryHandleFrameworkEvent(IDomainEvent @event) => false;

    /// <summary>
    /// Flush uncommitted events produced during construction.
    /// Called by the consumption loop after SignalReady, before the first mailbox message.
    /// </summary>
    protected override async ValueTask OnReadyAsync()
    {
        await FlushEventsAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Process a mailbox message: dispatch to subclasses, then persist and mutate.
    /// Overrides Actor.ProcessAsync to insert the Persist→Mutate step.
    /// </summary>
    protected override async ValueTask ProcessAsync(Envelope envelope)
    {
        // Snapshot BEFORE dispatch: a throwing handler must not leak the events it
        // already raised into the next command's flush. Those events were never
        // persisted, so keeping them would append a failed command's events as part
        // of the next successful batch (stream corruption) and inflate Version.
        var eventCount = _events.Count;
        // Version is snapshotted too: the count alone is not a reliable rollback baseline
        // (a handler can call CommitEvents() mid-dispatch, which empties the list while
        // Version stays inflated). Restoring the snapshot is exact in every case.
        var versionSnapshot = Version;

        // 1. Dispatch to subclass — RaiseEvent records events without mutating state
        object? result;
        try
        {
            result = await OnMessageAsync(envelope.Command).ConfigureAwait(false);
        }
        catch
        {
            // Only the dispatch is rolled back here — FlushEventsAsync owns its own
            // rollback (doubling it would underflow Version).
            RollbackDispatchEvents(eventCount, versionSnapshot);
            throw;
        }

        // 2. Persist → Mutate → Commit
        await FlushEventsAsync().ConfigureAwait(false);

        // 3. Reply only after successful persistence
        envelope.Tcs?.TrySetResult(result);
    }

    /// <summary>
    /// Drop the events raised by a dispatch that threw (the batch was never persisted) and
    /// restore <c>Version</c> to <paramref name="versionSnapshot"/> — the value captured
    /// before the dispatch. Restoring (instead of subtracting a count) is exact even when
    /// the handler cleared its own staged events with
    /// <see cref="IEventSourcedActor.CommitEvents"/> before throwing: the list is then empty
    /// while the Version is already inflated, and a count-based rollback would leave the
    /// phantom baseline behind for the next flush (permanent ConcurrencyException).
    /// <para>
    /// HANDLERS MUST NOT CALL <see cref="FlushEventsAsync"/> FROM INSIDE A DISPATCH. The
    /// rollback restores <c>Version</c> to the value captured before the dispatch, so an
    /// inner flush that SUCCEEDED (events persisted, Version advanced) followed by a throw
    /// out of the handler would push <c>Version</c> back BELOW the freshly persisted
    /// baseline. The next flush would then compute <c>expectedVersion = Version - count</c>
    /// from that regressed value and either re-append events the store already holds or die
    /// with ConcurrencyException, permanently. Persist after the dispatch returns, or
    /// return normally so the outer flush runs.
    /// </para>
    /// </summary>
    private void RollbackDispatchEvents(int eventCount, ulong versionSnapshot)
    {
        var added = _events.Count - eventCount;
        if (added > 0)
            _events.RemoveRange(eventCount, added);

        Version = versionSnapshot;
    }

    /// <summary>
    /// Persist uncommitted events (if any store is configured), then Mutate each event,
    /// then clear the uncommitted list. No-op if there are no uncommitted events.
    /// Protected so subclasses (e.g., <see cref="SagaActor"/>) can flush after
    /// resuming from an interrupted step.
    /// </summary>
    protected virtual async ValueTask FlushEventsAsync()
    {
        if (_events.Count == 0)
            return;

        // Persist first — if this fails, state remains unchanged (RaiseEvent didn't Mutate).
        // On failure, discard the uncommitted events and roll Version back to the
        // last persisted version. Otherwise Version stays inflated and the failed
        // events leak into the next append, permanently poisoning the actor:
        // expectedVersion = Version - _events.Count would recompute to a stale
        // baseline forever, and a later successful append would reapply discarded
        // commands whose callers already saw an exception.
        if (_eventStore is not null)
        {
            var expectedVersion = Version - (ulong)_events.Count;
            try
            {
                await _eventStore.AppendAsync(Id, expectedVersion, _events).ConfigureAwait(false);
            }
            catch
            {
                RollbackUncommitted();
                throw;
            }
        }

        // Persistence succeeded (or no store) — now safe to mutate state.
        // Mutate is a pure function that must not throw. If it does, the events are
        // already durable but in-memory state can no longer be trusted: fail loud by
        // stopping the actor (registry removal + stop signal). Keeping it alive would
        // retry the leaked, un-mutated batch on the next flush and fail every later
        // command with a misleading ConcurrencyException.
        try
        {
            foreach (var e in _events)
            {
                if (!TryHandleFrameworkEvent(e))
                    Mutate(e);
            }
        }
        catch
        {
            StopAfterMutateFailure();
            throw;
        }

        // Publish AFTER state is consistent. Replay never reaches this path
        // (ReplayEvents bypasses FlushEventsAsync), so recovery is silent by
        // construction. Failures are isolated — events are already durable.
        // (_events.Count > 0 is guaranteed here — empty list early-returned above.)
        if (_publisher is not null)
        {
            var actorId = Id;
            var version = Version;
            var toPublish = _events.ToList();
            try
            {
                await _publisher.PublishAsync(actorId, version, toPublish).ConfigureAwait(false);
            }
            catch
            {
                // Contract: implementations must not throw; this is defense in
                // depth only — never let a publisher failure break the actor.
            }
        }

        ClearEvents();
    }

    /// <summary>
    /// Fatal Mutate failure: the batch is durable but state is not. Stop the actor so
    /// no later command can observe half-applied state or retry the leaked batch.
    /// <see cref="IActorSystem.RequestStop"/> also removes the registry entry, so later
    /// messages fail with <see cref="KeyNotFoundException"/> instead of a misleading
    /// ConcurrencyException.
    /// </summary>
    private void StopAfterMutateFailure()
    {
        if (System is not null)
            System.RequestStop(Id);
        else
            SignalStop();
    }

    /// <summary>
    /// Discard uncommitted events and roll Version back to the last persisted
    /// version. Used when persistence fails (<see cref="FlushEventsAsync"/>) or a
    /// business failure makes the current batch non-atomic (<c>SagaActor.FailAsync</c>):
    /// the next append must compute expectedVersion from the last persisted baseline,
    /// otherwise the discarded events would leak into the next batch permanently.
    /// The lower bound matches <see cref="RollbackDispatchEvents"/>: an inconsistent
    /// Version must never underflow into a huge expectedVersion.
    /// </summary>
    protected void RollbackUncommitted()
    {
        var delta = (ulong)_events.Count;
        Version = Version >= delta ? Version - delta : 0UL;
        ClearEvents();
    }

    private void ClearEvents() => _events.Clear();

    /// <summary>
    /// Initialization is done: the instance is live, so the replay marker's job is
    /// over. Subclass overrides must call the base implementation.
    /// </summary>
    internal override void OnInitialized()
    {
        IsReplaying = false;
        base.OnInitialized();
    }

    void IEventSourcedActor.CommitEvents() => ClearEvents();

    void IEventSourcedActor.ReplayEvents(IReadOnlyList<IDomainEvent> events)
    {
        IsReplaying = true;

        // A rebuilt instance must drop whatever its constructor staged: the persisted
        // stream is authoritative. Keeping those events (a constructor-seeded saga is the
        // real case) would append them again and make every later flush compute
        // expectedVersion from a phantom baseline — a permanent ConcurrencyException.
        ClearEvents();

        // Replay bypasses persistence — events are already in the store.
        // Mutate directly, then set Version.
        foreach (var e in events)
        {
            if (!TryHandleFrameworkEvent(e))
                Mutate(e);
        }
        Version = (ulong)events.Count;
    }
}
