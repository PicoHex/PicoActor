namespace PicoNode.Actor.Abs;

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
    /// Set by IActorSystem after construction, before SignalReady.
    /// May be null in tests or non-persistent scenarios (pure in-memory mode).
    /// </summary>
    internal IEventStore? EventStore { get; set; }

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
    /// </summary>
    private async ValueTask FlushEventsAsync()
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
        if (EventStore is not null)
        {
            var expectedVersion = Version - (ulong)_events.Count;
            try
            {
                await EventStore.AppendAsync(Id, expectedVersion, _events).ConfigureAwait(false);
            }
            catch
            {
                Version -= (ulong)_events.Count;
                ClearEvents();
                throw;
            }
        }

        // Persistence succeeded (or no store) — now safe to mutate state
        foreach (var e in _events)
            Mutate(e);

        ClearEvents();
    }

    private void ClearEvents() => _events.Clear();

    IReadOnlyList<IDomainEvent> IEventSourcedActor.GetUncommittedEvents() => _events.AsReadOnly();

    void IEventSourcedActor.CommitEvents() => ClearEvents();

    void IEventSourcedActor.ReplayEvents(IReadOnlyList<IDomainEvent> events)
    {
        // Replay bypasses persistence — events are already in the store.
        // Mutate directly, then set Version.
        foreach (var e in events)
            Mutate(e);
        Version = (ulong)events.Count;
    }
}
