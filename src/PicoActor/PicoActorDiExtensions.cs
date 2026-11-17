namespace PicoActor;

/// <summary>PicoDI registration extensions for PicoActor.</summary>
public static class PicoActorDiExtensions
{
    /// <summary>
    /// Registers PicoActor services with configuration.
    /// The runtime resolves the container's <see cref="IEventStore"/>; this overload picks no
    /// implementation, so <see cref="ActorConfig.EventStore"/>'s type name only says which
    /// registration form the host used — <c>"InMemory"</c> means "a store is (or will be)
    /// registered", any other name is refused.
    /// Users can bind <see cref="ActorConfig"/> from PicoCfg:
    /// <code>var cfg = CfgBind.Bind&lt;ActorConfig&gt;(configuration, "Actor");</code>
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, ActorConfig? config)
    {
        var storeType = config?.EventStore?.Type ?? "InMemory";

        // No implementation is selected here, not even for "InMemory": null means "register
        // nothing" (never "clobber whatever the host registered"), and the factory resolves the
        // store. Unknown names still fail at registration — that is a configuration typo the
        // host must see immediately.
        IEventStore? store = storeType switch
        {
            "InMemory" => null,
            _ => throw new NotSupportedException(
                $"Event store type '{storeType}' is not supported. "
                    + "Supported: whatever IEventStore is already registered "
                    + "(e.g. AddPicoActorInMemoryStore()); other stores are constructed explicitly."
            ),
        };

        return AddPicoActorCore(container, store, publisher: null);
    }

    /// <summary>
    /// Registers PicoActor services in the DI container.
    /// <para>
    /// No event store is registered here — the core carries no concrete store. The
    /// <see cref="IActorSystem"/> factory resolves the container's <see cref="IEventStore"/>, so
    /// the host registers the store it means: <c>AddPicoActorInMemoryStore()</c> from the
    /// <c>PicoActor.EventStore.InMemory</c> package, or an explicit instance through
    /// <see cref="AddPicoActor(ISvcContainer, IEventStore?)"/>. Those registrations and this one
    /// may be made in either order. With no store registered at all, resolving
    /// <see cref="IActorSystem"/> throws <see cref="InvalidOperationException"/> naming that
    /// extension — the reason the mistake is reported at resolve time, not at registration time.
    /// </para>
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
    /// The explicit instance always wins — it is registered unconditionally, so it replaces a
    /// store already registered on the container (unlike the other overloads, which register
    /// none and take whatever is there).
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IEventStore? eventStore)
    {
        return AddPicoActorCore(container, eventStore, publisher: null);
    }

    /// <summary>
    /// Registers PicoActor services and wires PicoMediator event outflow.
    /// ActorSystem's DomainEventPublisher = MediatorDomainEventPublisher(publisher).
    /// No event store is registered: the factory resolves the container's
    /// <see cref="IEventStore"/>, exactly as in <see cref="AddPicoActor(ISvcContainer)"/>.
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(publisher);

        return AddPicoActorCore(container, eventStore: null, publisher);
    }

    /// <summary>
    /// Single registration path shared by all overloads: IEventStore (only when an instance was
    /// passed explicitly), IActorSystem singleton, and the ICommandSender narrow port.
    /// Event outflow wiring: an explicit <paramref name="publisher"/> wins; otherwise the
    /// container is probed for <see cref="IMediator"/> (auto-wire).
    /// The store is never defaulted: it is resolved inside the IActorSystem factory, so any
    /// registration order works and a missing store is reported where the host can still act.
    /// </summary>
    private static ISvcContainer AddPicoActorCore(
        ISvcContainer container,
        IEventStore? eventStore,
        IPublisher? publisher
    )
    {
        // Explicit intent only: an instance the caller passed is registered as-is. Without one
        // nothing is registered here, so a store the host registered itself is never replaced.
        if (eventStore is not null)
            container.Register(typeof(IEventStore), _ => eventStore, SvcLifetime.Singleton);

        // Register ActorSystem singleton
        container.Register(
            typeof(IActorSystem),
            scope =>
            {
                // The core ships no concrete store, so THIS is the throw point: at registration
                // time the host may simply not have registered its store yet (order is free).
                if (
                    !scope.TryGetService(typeof(IEventStore), out var storeObj)
                    || storeObj is not IEventStore store
                )
                    throw new InvalidOperationException(
                        "No IEventStore is registered. Call AddPicoActorInMemoryStore() "
                            + "(PicoActor.EventStore.InMemory) or register your own store."
                    );

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
