namespace PicoActor;

using PicoMediator.Abs;

/// <summary>PicoDI registration extensions for PicoActor.</summary>
public static class PicoActorDiExtensions
{
    /// <summary>
    /// Registers PicoActor services with configuration.
    /// The event store type is determined by <see cref="ActorConfig.EventStore"/>.
    /// Users can bind <see cref="ActorConfig"/> from PicoCfg:
    /// <code>var cfg = CfgBind.Bind&lt;ActorConfig&gt;(configuration, "Actor");</code>
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, ActorConfig? config)
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
    /// Registers PicoActor services in the DI container.
    /// IEventStore defaults to <see cref="InMemoryEventStore"/>.
    /// If <see cref="ILoggerFactory"/> is registered in the container,
    /// a logger is automatically resolved and injected into <see cref="ActorSystem"/>.
    /// If <see cref="IMediator"/> (e.g. via AddPicoMediator) is registered,
    /// event outflow is auto-wired through <see cref="MediatorDomainEventPublisher"/>.
    /// <para>
    /// Auto-wiring is safe from any resolving scope: since PicoDI 2026.8.1 (E1)
    /// singleton factories run against the container-internal root scope, so the
    /// IMediator is bound to a scope that lives until container disposal.
    /// </para>
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container)
    {
        return AddPicoActor(container, eventStore: null);
    }

    /// <summary>
    /// Registers PicoActor services with a custom event store.
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IEventStore? eventStore)
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

                // Event outflow: if the container has IMediator registered (e.g. via
                // AddPicoMediator), auto-wire MediatorDomainEventPublisher. Lazy
                // resolution inside the factory is compatible with scoped lifetimes —
                // no publisher instance is needed before Build.
                // Since PicoDI 2026.8.1 (E1), singleton factories run against the
                // container-internal root scope, so first resolution from any scope is
                // safe; the ODE diagnostic remains as defense (user-built publisher scenario).
                IDomainEventPublisher? domainEventPublisher = null;
                if (
                    scope.TryGetService(typeof(IMediator), out var mediatorObj)
                    && mediatorObj is IMediator mediator
                )
                    domainEventPublisher = new MediatorDomainEventPublisher(mediator, logger);

                return new ActorSystem(
                    new ActorSystemOptions
                    {
                        EventStore = store,
                        Logger = logger,
                        DomainEventPublisher = domainEventPublisher,
                    }
                );
            },
            SvcLifetime.Singleton
        );

        // ICommandSender — narrow port for event handlers (Send/AskAsync/ExecuteSaga).
        // Resolves the singleton IActorSystem lazily so any registration order works.
        container.Register(
            typeof(ICommandSender),
            scope =>
                new ActorSystemCommandSender(
                    (IActorSystem)scope.GetService(typeof(IActorSystem))
                ),
            SvcLifetime.Singleton
        );

        return container;
    }

    /// <summary>
    /// Registers PicoActor services and wires PicoMediator event outflow.
    /// ActorSystem's DomainEventPublisher = MediatorDomainEventPublisher(publisher).
    /// IEventStore defaults to InMemory; a container-registered IEventStore takes precedence.
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(publisher);

        // Reuse the main path (InMemory EventStore default + ActorSystem registration), then override the publisher wiring
        AddPicoActor(container, eventStore: null);

        container.Register(
            typeof(IActorSystem),
            scope =>
            {
                var store = (IEventStore)scope.GetService(typeof(IEventStore));

                // Optional: resolve PicoLog logger
                ILogger? logger = null;
                if (
                    scope.TryGetService(typeof(ILoggerFactory), out var factoryObj)
                    && factoryObj is ILoggerFactory loggerFactory
                )
                    logger = loggerFactory.CreateLogger(nameof(ActorSystem));

                return new ActorSystem(
                    new ActorSystemOptions
                    {
                        EventStore = store,
                        Logger = logger,
                        DomainEventPublisher = new MediatorDomainEventPublisher(publisher, logger),
                    }
                );
            },
            SvcLifetime.Singleton
        );

        return container;
    }
}
