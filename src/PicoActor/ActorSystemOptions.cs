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
}
