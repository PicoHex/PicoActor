using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

/// <summary>类型化订阅(bridge 路由)——代替统一 handler switch。事件→命令的翻译是业务层职责。</summary>
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

/// <summary>共享静态订阅者 Received 列表——同类测试必须串行。</summary>
[NotInParallel]
public sealed class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        // 1. PicoDI 容器 + declare-and-subscribe(Gen 扫描 → 类型化订阅者自动注册,零手动注册)
        // 2. AddPicoActor() 在 ActorSystem 工厂内自动解析已注册的 IMediator 并接线事件流出
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

        // 3. saga 完成 → 事件流出 → 类型化订阅者收到(业务事件 + 框架终态事件经 Abs bridge)
        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("hello")
        );
        await Task.Delay(300); // 发布异步(在 ProcessAsync 的 flush 内——ExecuteSaga 返回前已发布;Delay 防御性保留)

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].Name).IsEqualTo("hello");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Result).IsEqualTo("hello");
    }

    [Test]
    public async Task AutoWiring_RootScopeBinding_SurvivesChildScopeDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

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

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task AutoWiring_ChildScopeFirstResolution_SurvivesDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // E1(2026.8.1):Singleton 工厂使用容器内部根 scope——子 scope 首次解析也安全
        // (2026.8.0 的 captive dependency 契约测试:dispose 后流出失效——已由 E1 修复)
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

        // 子 scope 已释放:Mediator 绑定根 scope(存活)——事件流出不受影响
        // (创建命令经参数less工厂不产生事件;AskAsync("x") 产生 1 个 MedIntegrationStarted)
        await system.AskAsync<string>(sagaId, new MedIntegrationCmd("x"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1); // Abs 为 net10.0——框架事件可类型化订阅
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

        // Abs 改为 net10.0 后生成自己的 bridge——框架事件可类型化订阅
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Result).IsEqualTo("hello");
    }
}
