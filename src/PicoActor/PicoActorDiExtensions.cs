namespace PicoActor;

/// <summary>PicoDI registration extensions for PicoActor.</summary>
public static class PicoActorDiExtensions
{
    /// <summary>
    /// Registers PicoActor services with configuration.
    /// The event store type is determined by <see cref="ActorConfig.EventStore"/>.
    /// Users can bind <see cref="ActorConfig"/> from PicoCfg:
    /// <code>var cfg = CfgBind.Bind&lt;ActorConfig&gt;(configuration, "Actor");</code>
    /// A store already registered on the container takes precedence over the
    /// config-derived default (see <see cref="AddPicoActor(ISvcContainer)"/>).
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, ActorConfig? config)
    {
        var storeType = config?.EventStore?.Type ?? "InMemory";

        // "InMemory" is the implicit default: pass null so the core probes for an existing
        // registration instead of clobbering it. Future store types are explicit choices
        // and pass a real instance.
        IEventStore? store = storeType switch
        {
            "InMemory" => null,
            _ => throw new NotSupportedException(
                $"Event store type '{storeType}' is not supported. " + "Supported types: InMemory."
            ),
        };

        return AddPicoActorCore(container, store, publisher: null);
    }

    /// <summary>
    /// Registers PicoActor services in the DI container.
    /// IEventStore defaults to <see cref="InMemoryEventStore"/> — but only when the
    /// container has no <see cref="IEventStore"/> registration yet: a store the
    /// application registered itself always takes precedence (the implicit default
    /// never clobbers explicit intent). Use the <see cref="IEventStore"/> overload to
    /// override a previously registered store.
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
        return AddPicoActorCore(container, eventStore: null, publisher: null);
    }

    /// <summary>
    /// Registers PicoActor services with a custom event store.
    /// The explicit instance always wins — it overrides a store already registered
    /// on the container (unlike the implicit InMemory default of the other overloads).
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IEventStore? eventStore)
    {
        return AddPicoActorCore(container, eventStore, publisher: null);
    }

    /// <summary>
    /// Registers PicoActor services and wires PicoMediator event outflow.
    /// ActorSystem's DomainEventPublisher = MediatorDomainEventPublisher(publisher).
    /// IEventStore is only defaulted to InMemory when the container has no IEventStore
    /// registration — a container-registered IEventStore takes precedence.
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(publisher);

        return AddPicoActorCore(container, eventStore: null, publisher);
    }

    /// <summary>
    /// Single registration path shared by all overloads: IEventStore (explicit instance
    /// or implicit in-memory default), IActorSystem singleton, and the ICommandSender
    /// narrow port. Event outflow wiring: an explicit <paramref name="publisher"/> wins;
    /// otherwise the container is probed for <see cref="IMediator"/> (auto-wire).
    /// Precedence: an explicit store instance is registered unconditionally; the
    /// implicit default is registered only when the container has no IEventStore yet.
    /// </summary>
    private static ISvcContainer AddPicoActorCore(
        ISvcContainer container,
        IEventStore? eventStore,
        IPublisher? publisher
    )
    {
        // Register event store (explicit instance or default in-memory unless one is
        // already registered — never silently replace the application's store)
        if (eventStore is not null || !container.IsRegistered(typeof(IEventStore)))
        {
            container.Register(
                typeof(IEventStore),
                scope => eventStore ?? new InMemoryEventStore(),
                SvcLifetime.Singleton
            );
        }

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

                // Event outflow: explicit publisher wins; otherwise auto-wire
                // MediatorDomainEventPublisher if the container has IMediator
                // registered (e.g. via AddPicoMediator). Lazy resolution inside
                // the factory is compatible with scoped lifetimes — no publisher
                // instance is needed before Build. Since PicoDI 2026.8.1 (E1),
                // singleton factories run against the container-internal root
                // scope, so first resolution from any scope is safe; the ODE
                // diagnostic in the publisher remains as defense (user-built
                // publisher scenario).
                IDomainEventPublisher? domainEventPublisher;
                if (publisher is not null)
                    domainEventPublisher = new MediatorDomainEventPublisher(publisher, logger);
                else if (
                    scope.TryGetService(typeof(IMediator), out var mediatorObj)
                    && mediatorObj is IMediator mediator
                )
                    domainEventPublisher = new MediatorDomainEventPublisher(mediator, logger);
                else
                    domainEventPublisher = null;

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
            scope => new ActorSystemCommandSender(
                (IActorSystem)scope.GetService(typeof(IActorSystem))
            ),
            SvcLifetime.Singleton
        );

        return container;
    }
}
