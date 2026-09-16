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
        // 1. Dispatch to subclass — RaiseEvent records events without mutating state
        var result = await OnMessageAsync(envelope.Command).ConfigureAwait(false);

        // 2. Persist → Mutate → Commit
        await FlushEventsAsync().ConfigureAwait(false);

        // 3. Reply only after successful persistence
        envelope.Tcs?.TrySetResult(result);
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

        // Persistence succeeded (or no store) — now safe to mutate state
        foreach (var e in _events)
        {
            if (!TryHandleFrameworkEvent(e))
                Mutate(e);
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
    /// Discard uncommitted events and roll Version back to the last persisted
    /// version. Used when persistence fails (<see cref="FlushEventsAsync"/>) or a
    /// business failure makes the current batch non-atomic (<c>SagaActor.FailAsync</c>):
    /// the next append must compute expectedVersion from the last persisted baseline,
    /// otherwise the discarded events would leak into the next batch permanently.
    /// </summary>
    protected void RollbackUncommitted()
    {
        Version -= (ulong)_events.Count;
        ClearEvents();
    }

    private void ClearEvents() => _events.Clear();

    void IEventSourcedActor.CommitEvents() => ClearEvents();

    void IEventSourcedActor.ReplayEvents(IReadOnlyList<IDomainEvent> events)
    {
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
