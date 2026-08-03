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
}
