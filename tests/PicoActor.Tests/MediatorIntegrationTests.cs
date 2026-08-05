using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

/// <summary>声明即订阅:Gen 扫描 → AddPicoMediator 自动注册。switch 翻译是业务层职责。</summary>
public sealed class DomainEventRouter : ISubscriber<IDomainEvent>
{
    public static readonly List<IDomainEvent> Received = [];

    public ValueTask Handle(IDomainEvent @event, CancellationToken ct = default)
    {
        Received.Add(@event);
        return default;
    }
}

/// <summary>类型化订阅(2026.8.1 bridge 路由)——代替统一 handler switch。</summary>
internal sealed class MedIntegrationStartedSub : ISubscriber<MedIntegrationStarted>
{
    public static readonly List<MedIntegrationStarted> Received = [];

    public ValueTask Handle(MedIntegrationStarted e, CancellationToken ct = default)
    {
        Received.Add(e);
        return default;
    }
}

internal sealed class SagaCompletedSub : ISubscriber<SagaCompleted>
{
    public static readonly List<SagaCompleted> Received = [];

    public ValueTask Handle(SagaCompleted e, CancellationToken ct = default)
    {
        Received.Add(e);
        return default;
    }
}

internal sealed record MedIntegrationCmd(string Name) : ICommand;

internal sealed record MedIntegrationStarted(string Name) : IDomainEvent;

internal sealed class MedIntegrationSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public MedIntegrationSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is MedIntegrationCmd c)
        {
            if (_step < 1)
            {
                RaiseEvent(new MedIntegrationStarted(c.Name));
                _name = c.Name;
            }
            MarkComplete(_name);
            return new ValueTask<object?>(_name);
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is MedIntegrationStarted e)
        {
            _step = 1;
            _name = e.Name;
        }
    }
}

/// <summary>共享静态 DomainEventRouter.Received——同类测试必须串行。</summary>
[NotInParallel]
public sealed class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        DomainEventRouter.Received.Clear();

        // 1. PicoDI 容器 + declare-and-subscribe(Gen 扫描 → DomainEventRouter 自动注册,零手动注册)
        // 2. AddPicoActor() 在 ActorSystem 工厂内自动解析已注册的 IMediator 并接线事件流出
        //    (与 Scoped 生命周期兼容——修复前只能手工构造适配器或传 Build 前实例)
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // 3. saga 完成 → 事件流出 → 自动注册的订阅者收到
        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("hello")
        );
        await Task.Delay(300); // 发布异步(在 ProcessAsync 的 flush 内——ExecuteSaga 返回前已发布;Delay 防御性保留)

        await Assert.That(DomainEventRouter.Received.Count).IsEqualTo(2);
        await Assert.That(DomainEventRouter.Received[0]).IsTypeOf<MedIntegrationStarted>();
        await Assert.That(DomainEventRouter.Received[1]).IsTypeOf<SagaCompleted>();
        await Assert.That(((SagaCompleted)DomainEventRouter.Received[1]).Result).IsEqualTo("hello");
    }

    [Test]
    public async Task AutoWiring_RootScopeBinding_SurvivesChildScopeDisposal()
    {
        DomainEventRouter.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // 推荐用法:ActorSystem 从应用级(root)scope 首次解析 → Mediator 绑定该 scope
        await using var rootScope = container.CreateScope();
        var system = (IActorSystem)rootScope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // 短命子 scope 的创建与释放不影响 root 绑定的流出
        await using (var childScope = container.CreateScope()) { }

        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("root")
        );
        await Task.Delay(300);

        await Assert.That(DomainEventRouter.Received.Count).IsEqualTo(2);
        await Assert.That(DomainEventRouter.Received[1]).IsTypeOf<SagaCompleted>();
    }

    [Test]
    public async Task AutoWiring_FirstResolutionFromChildScope_DisposedScope_KillsOutflow()
    {
        DomainEventRouter.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // 已知限制(captive dependency):首次解析 ActorSystem 的 scope 决定 Mediator 绑定。
        // 从短命 scope 首次解析 → dispose 后 Publish 抛 ObjectDisposedException →
        // 适配器逐事件隔离吞掉 → 事件流出失效(文档化契约,非 bug——推荐从 root scope 解析)。
        IActorSystem system;
        Guid sagaId;
        await using (var childScope = container.CreateScope())
        {
            system = (IActorSystem)childScope.GetService(typeof(IActorSystem));
            system.Register<MedIntegrationSaga>(
                _ => new MedIntegrationSaga(),
                () => new MedIntegrationSaga()
            );
            var saga = await system.CreateAsync<MedIntegrationSaga>(new MedIntegrationCmd("init"));
            sagaId = saga.Id;
        }

        // childScope 已释放:actor 本身不受影响(事件仍落盘),但发布静默失效
        await system.AskAsync<string>(sagaId, new MedIntegrationCmd("x"));

        await Assert.That(DomainEventRouter.Received.Count).IsEqualTo(0);
    }

    [Test]
    public async Task EventOutflow_BasePublishBridge_ReachesTypedSubscribers()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // 适配器 Publish<IDomainEvent>(静态基类型)→ bridge → 具体类型订阅者
        await system.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("hello"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].Name).IsEqualTo("hello");

        // 已知限制(2026.8.1):框架事件(SagaCompleted/SagaFailed 定义于 netstandard2.0 的
        // PicoActor.Abs)无法被类型化订阅——bridge 生成代码引用 PicoMediator 主包(net10.0),
        // Abs 无法生成 bridge(见 docs/superpowers/notes 缺陷报告)。
        // 框架事件订阅走统一订阅者 ISubscriber<IDomainEvent>(既有测试覆盖)。
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(0);
    }
}
