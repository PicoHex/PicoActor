namespace PicoNode.Actor;

/// <summary>Configuration options for the Actor system.</summary>
public sealed class ActorSystemOptions
{
    /// <summary>The event store to use. Defaults to <see cref="InMemoryEventStore"/>.</summary>
    public IEventStore? EventStore { get; set; }

    /// <summary>Optional logger. When set, lifecycle events are logged.</summary>
    public ILogger? Logger { get; set; }
}
