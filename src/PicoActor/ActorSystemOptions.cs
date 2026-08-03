namespace PicoActor;

/// <summary>
/// Configuration options for the Actor system.
/// </summary>
[Obsolete(
    "Not consumed by ActorSystem or AddPicoActor. Use the ActorSystem(IEventStore, ILogger) "
        + "constructor or the AddPicoActor overloads instead."
)]
public sealed class ActorSystemOptions
{
    /// <summary>The event store to use. Defaults to <see cref="InMemoryEventStore"/>.</summary>
    public IEventStore? EventStore { get; set; }

    /// <summary>Optional logger. When set, lifecycle events are logged.</summary>
    public ILogger? Logger { get; set; }
}
