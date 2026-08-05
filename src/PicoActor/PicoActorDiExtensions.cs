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

                // 事件流出:若容器已注册 IMediator(如 AddPicoMediator),自动接线
                // MediatorDomainEventPublisher。工厂内延迟解析与 Scoped 生命周期兼容——
                // 无需 Build 前的 publisher 实例。
                // PicoDI 2026.8.1(E1)起 Singleton 工厂使用容器内部根 scope——任意 scope
                // 首次解析均安全;ODE 诊断保留为防御(用户自建 publisher 场景)。
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

        return container;
    }

    /// <summary>
    /// 注册 PicoActor 服务并接线 PicoMediator 事件流出。
    /// ActorSystem 的 DomainEventPublisher = MediatorDomainEventPublisher(publisher)。
    /// IEventStore 默认 InMemory;若容器已注册 IEventStore 则优先使用。
    /// </summary>
    public static ISvcContainer AddPicoActor(this ISvcContainer container, IPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(publisher);

        // 复用主路径(EventStore 默认 InMemory + ActorSystem 注册),再覆盖 publisher 接线
        AddPicoActor(container, eventStore: null);

        container.Register(
            typeof(IActorSystem),
            scope =>
            {
                var store = (IEventStore)scope.GetService(typeof(IEventStore));

                // 可选:解析 PicoLog 日志
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
