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

public sealed class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        DomainEventRouter.Received.Clear();

        // 1. PicoDI 容器 + declare-and-subscribe(Gen 扫描 → DomainEventRouter 自动注册,零手动注册)
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.Build();
        await using var scope = container.CreateScope();

        // 2. 接线 Mediator 事件流出。IMediator 是 Scoped,只能在 Build 后从 scope 解析;
        //    AddPicoActor(IPublisher) 需要实例先于 Build 存在——此处用与 DI 重载内部相同的
        //    适配器路径手工接线(DI 重载本身由 PicoActorDiExtensionsTests 覆盖)。
        var mediator = (IMediator)scope.GetService(typeof(IMediator));
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = new InMemoryEventStore(),
                DomainEventPublisher = new MediatorDomainEventPublisher(mediator),
            }
        );
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
}
