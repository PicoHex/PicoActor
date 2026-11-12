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

    /// <summary>
    /// How long an actor may stay idle before the runtime RECLAIMS its materialized state
    /// (idle passivation). Default <c>null</c> = disabled, the original semantics: every actor
    /// stays in memory until it is stopped explicitly.
    /// <para>
    /// Opt-in, and deliberately narrow: only actors the runtime can rebuild are candidates - an
    /// <c>EventSourcedActor</c> whose concrete type was registered WITH a rebuild factory. The
    /// event stream is the truth, so the in-memory state is only a cache: passivating an idle
    /// aggregate frees it, and the next <see cref="ActorSystem.GetAsync{T}"/> replays the stream
    /// (same id, same state, same version). Non-ES actors, ES types without a rebuild factory,
    /// actors that are mid-command, and actors with a mailbox backlog are never touched.
    /// </para>
    /// <para>
    /// The reclaim itself is a mailbox sentinel: the actor stops at a message boundary
    /// (<see cref="ActorSystem.RequestStop"/>), never through an external signal racing a
    /// dispatch. Because the registry entry is removed, <c>Send</c>/<c>AskAsync</c> on a
    /// passivated id throw <see cref="KeyNotFoundException"/> until it is reactivated - which is
    /// exactly what happens today for any actor that is not registered.
    /// </para>
    /// <para>A non-positive value is rejected at construction: "disabled" is <c>null</c>.</para>
    /// </summary>
    public TimeSpan? IdleTimeout { get; set; }

    /// <summary>
    /// How often the background sweeper looks for idle actors. Default <c>null</c> = derive it
    /// from <see cref="IdleTimeout"/> (half of it, never less than one second) so an actor is
    /// reclaimed within roughly 1.5x <see cref="IdleTimeout"/> of going idle.
    /// <para>
    /// Only meaningful together with <see cref="IdleTimeout"/>. Tests and operations can also
    /// drive the sweep explicitly with <see cref="ActorSystem.SweepIdleActorsAsync"/>.
    /// </para>
    /// </summary>
    public TimeSpan? IdleSweepInterval { get; set; }
}
