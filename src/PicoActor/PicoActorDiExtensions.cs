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
    /// <b>Captive dependency note:</b> the auto-wired IMediator is bound to the
    /// scope that first resolves <see cref="IActorSystem"/> (PicoDI singleton
    /// factories run in the first resolving scope). Resolve IActorSystem from an
    /// application-level (root) scope; first resolution from a short-lived
    /// request scope kills event outflow after that scope is disposed.
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
                // 无需 Build 前的 publisher 实例(修复前 AddPicoActor(IPublisher) 无法用于真实 Mediator)。
                // 注意 captive dependency:Mediator 绑定首次解析 ActorSystem 的 scope——
                // 应从应用级(根)scope 解析(见 AddPicoActor() XML 文档)。
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
