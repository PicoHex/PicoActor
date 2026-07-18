namespace PicoNode.Actor;

/// <summary>PicoDI registration extensions for PicoNode.Actor.</summary>
public static class PicoActorDiExtensions
{
    /// <summary>
    /// Registers PicoNode.Actor services with configuration.
    /// The event store type is determined by <see cref="ActorConfig.EventStore"/>.
    /// Users can bind <see cref="ActorConfig"/> from PicoCfg:
    /// <code>var cfg = CfgBind.Bind&lt;ActorConfig&gt;(configuration, "Actor");</code>
    /// </summary>
    public static SvcContainer AddPicoActor(this SvcContainer container, ActorConfig? config)
    {
        var storeType = config?.EventStore?.Type ?? "InMemory";

        IEventStore store = storeType switch
        {
            "InMemory" => new InMemoryEventStore(),
            _ => throw new NotSupportedException(
                $"Event store type '{storeType}' is not supported. " + "Supported types: InMemory."
            ),
        };

        return AddPicoActor(container, store);
    }

    /// <summary>
    /// Registers PicoNode.Actor services in the DI container.
    /// IEventStore defaults to <see cref="InMemoryEventStore"/>.
    /// If <see cref="ILoggerFactory"/> is registered in the container,
    /// a logger is automatically resolved and injected into <see cref="ActorSystem"/>.
    /// </summary>
    public static SvcContainer AddPicoActor(this SvcContainer container)
    {
        return AddPicoActor(container, eventStore: null);
    }

    /// <summary>
    /// Registers PicoNode.Actor services with a custom event store.
    /// </summary>
    public static SvcContainer AddPicoActor(this SvcContainer container, IEventStore? eventStore)
    {
        // Register event store (custom or default in-memory)
        container.Register(
            typeof(IEventStore),
            scope => eventStore ?? new InMemoryEventStore(),
            SvcLifetime.Singleton
        );

        // Register ActorSystem singleton
        container.Register(
            typeof(IActorSystem),
            scope =>
            {
                var store = (IEventStore)scope.GetService(typeof(IEventStore));

                // Optional: resolve logger if PicoLog is registered in DI
                ILogger? logger = null;
                if (
                    scope.TryGetService(typeof(ILoggerFactory), out var factoryObj)
                    && factoryObj is ILoggerFactory loggerFactory
                )
                    logger = loggerFactory.CreateLogger(nameof(ActorSystem));

                return new ActorSystem(store, logger);
            },
            SvcLifetime.Singleton
        );

        return container;
    }
}
