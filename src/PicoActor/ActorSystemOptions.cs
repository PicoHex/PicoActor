namespace PicoActor;

/// <summary>
/// Configuration options for the Actor system — the single construction path
/// (see <see cref="ActorSystem.ActorSystem(ActorSystemOptions)"/>).
/// <see cref="EventStore"/> is required: the storage scheme must be chosen
/// explicitly, there is no hidden default.
/// </summary>
public sealed class ActorSystemOptions
{
    /// <summary>The event store to use. Required — no implicit default.</summary>
    public required IEventStore EventStore { get; set; }

    /// <summary>Optional logger. When set, lifecycle events are logged.</summary>
    public ILogger? Logger { get; set; }

    /// <summary>Optional domain-event publisher. When set, events are published
    /// after each successful persist+mutate. Default null (no publishing).</summary>
    public IDomainEventPublisher? DomainEventPublisher { get; set; }

    /// <summary>
    /// Optional bound on how long a stop waits for one actor's loop to exit
    /// (<see cref="ActorSystem.StopAsync"/> and, per actor,
    /// <see cref="ActorSystem.StopAllAsync"/>). Default <c>null</c> = wait forever,
    /// the original (unbounded) semantics.
    /// <para>
    /// When set, an actor whose handler does not observe its stop token (it keeps awaiting
    /// something else) can no longer hang host shutdown: after this budget the stop
    /// ABANDONS the wait and reports a warning. The framework cannot abort the loop —
    /// it may still be running after the stop returns, so the host must treat that actor as
    /// untrusted (its mailbox and stop token are already closed/cancelled; the registry
    /// entry was removed first, so it is unreachable from the system).
    /// </para>
    /// <para>
    /// The budget applies per actor in a mass stop, not to the shutdown as a whole. It only
    /// bounds the WAIT: a healthy actor stops as soon as its loop exits.
    /// </para>
    /// </summary>
    public TimeSpan? StopTimeout { get; set; }

    private int? _snapshotEveryNEvents;

    /// <summary>
    /// Opt-in snapshot acceleration for event-sourced actors. When set to N, an actor that
    /// returns a payload from <c>EventSourcedActor.TakeSnapshot</c> saves a snapshot every N
    /// persisted events, and recovery restores the snapshot and replays only the events after
    /// it. Default <c>null</c> = off (every actor replays its whole stream).
    /// <para>
    /// Snapshotting is acceleration ONLY: the event stream is never truncated, at most one
    /// snapshot per actor is kept, and a missing/corrupt/out-of-range snapshot falls back to a
    /// full replay. A store that does not implement <see cref="Abs.IEventStoreSnapshot"/>
    /// disables the feature regardless of this value.
    /// </para>
    /// <para>A value below 1 is rejected here: it can only be a host bug.</para>
    /// </summary>
    public int? SnapshotEveryNEvents
    {
        get => _snapshotEveryNEvents;
        set
        {
            if (value is <= 0)
                throw new ArgumentOutOfRangeException(
                    nameof(SnapshotEveryNEvents),
                    value,
                    "SnapshotEveryNEvents must be a positive event count (null disables it)."
                );
            _snapshotEveryNEvents = value;
        }
    }
}
