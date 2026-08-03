namespace PicoActor;

/// <summary>Configuration for the Actor system. Can be bound from PicoCfg.</summary>
public sealed class ActorConfig
{
    /// <summary>Event store configuration. Defaults to InMemory.</summary>
    public EventStoreConfig? EventStore { get; set; }
}

/// <summary>Event store configuration.</summary>
public sealed class EventStoreConfig
{
    /// <summary>Event store type. Supported: "InMemory".</summary>
    public string Type { get; set; } = "InMemory";

    /// <summary>Connection string for external stores. Unused by InMemory.</summary>
    [Obsolete(
        "External event stores are not supported (only InMemory is implemented); "
            + "this property is never read."
    )]
    public string? ConnectionString { get; set; }
}
