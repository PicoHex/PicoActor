namespace PicoActor;

/// <summary>Configuration for the Actor system. Can be bound from PicoCfg.</summary>
public sealed class ActorConfig
{
    /// <summary>
    /// Event store configuration. The core registers no store of its own: this section only
    /// records which registration form the host used.
    /// </summary>
    public EventStoreConfig? EventStore { get; set; }
}

/// <summary>Event store configuration.</summary>
public sealed class EventStoreConfig
{
    /// <summary>
    /// Event store type. Supported: "InMemory" — which means an <c>IEventStore</c> is (or will
    /// be) registered on the container, e.g. via <c>AddPicoActorInMemoryStore()</c>. Any other
    /// name throws: the core constructs no store, so a store is either registered or absent.
    /// </summary>
    public string Type { get; set; } = "InMemory";
}
