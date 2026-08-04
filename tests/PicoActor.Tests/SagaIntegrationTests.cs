using PicoActor.Abs;

namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// 端到端集成测试:spec 第 3 节应用架构
//   聚合 actor ← 命令;事件 → Publisher → MiniMediator(fanout)
//   → eventhandler 翻译 → 命令 → saga mailbox → 完成 → 事件流出
// 覆盖:ExecuteSaga 启动、事件回环推进、恢复时主动查询聚合状态(spec 5.6)
// ═══════════════════════════════════════════════════════════

/// <summary>测试内 PicoMediator 替代——IDomainEventPublisher fanout 分发。</summary>
internal sealed class MiniMediator : IDomainEventPublisher
{
    public Action<Guid, IReadOnlyList<IDomainEvent>>? OnEvents;

    public ValueTask PublishAsync(
        Guid actorId,
        ulong version,
        IReadOnlyList<IDomainEvent> events
    )
    {
        OnEvents?.Invoke(actorId, events);
        return default;
    }
}

// ── 聚合 ──

internal sealed record CreateOrder(Guid OrderId) : ICommand;

internal sealed record MarkPaid(Guid OrderId) : ICommand;

internal sealed record GetPaymentStatus : ICommand;

internal sealed record OrderCreated(Guid OrderId) : IDomainEvent;

internal sealed record OrderPaid(Guid OrderId) : IDomainEvent;

internal sealed class OrderActor : EventSourcedActor
{
    private bool _paid;

    public OrderActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case CreateOrder c:
                RaiseEvent(new OrderCreated(c.OrderId));
                return default;
            case MarkPaid c:
                RaiseEvent(new OrderPaid(c.OrderId));
                return default;
            case GetPaymentStatus:
                return new ValueTask<object?>(_paid);
        }
        return default;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case OrderPaid:
                _paid = true;
                break;
        }
    }
}

// ── saga(process manager 模式:等待事件回环推进)──

internal sealed record StartPayment(Guid OrderId) : ICommand;

internal sealed record PaymentReceived(Guid OrderId) : ICommand;

internal sealed record PaymentStep1Started(Guid OrderId) : IDomainEvent;

internal sealed record PaymentStep2Done : IDomainEvent;

internal sealed class PaymentSaga : SagaActor
{
    private int _step;
    private Guid _orderId;

    public PaymentSaga() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case StartPayment c:
                if (_step < 1)
                {
                    RaiseEvent(new PaymentStep1Started(c.OrderId));
                    _orderId = c.OrderId;
                }
                return "started"; // async 方法:直接返回值,不包 ValueTask(否则装箱成 object)
            case PaymentReceived:
                if (_step < 2)
                {
                    RaiseEvent(new PaymentStep2Done());
                    MarkComplete("paid");
                }
                return "paid";
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        // spec 5.6 恢复语义推论:外部聚合状态需主动查询(事件不会重放给恢复的 saga)
        var order = await System!.GetAsync<OrderActor>(_orderId);
        if (order is null)
            return; // 聚合不存在——继续等待外部事件
        var paid = await System.AskAsync<bool>(_orderId, new GetPaymentStatus());
        if (paid && _step < 2)
        {
            RaiseEvent(new PaymentStep2Done());
            MarkComplete("paid");
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case PaymentStep1Started e:
                _step = 1;
                _orderId = e.OrderId;
                break;
            case PaymentStep2Done:
                _step = 2;
                break;
        }
    }
}

public sealed class SagaIntegrationTests
{
    [Test]
    public async Task Saga_EventLoopback_Completes()
    {
        var store = new InMemoryEventStore();
        var mediator = new MiniMediator();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = store, DomainEventPublisher = mediator }
        );
        system.Register<OrderActor>(_ => new OrderActor(), () => new OrderActor());
        system.Register<PaymentSaga>(_ => new PaymentSaga(), () => new PaymentSaga());

        var order = await system.CreateAsync<OrderActor>(new CreateOrder(Guid.NewGuid()));
        var sagaId = Guid.Empty;

        // eventhandler 翻译层(普通类):OrderPaid → PaymentReceived 命令 → saga mailbox
        mediator.OnEvents = (actorId, events) =>
        {
            foreach (var e in events)
            {
                if (e is OrderPaid op && sagaId != Guid.Empty)
                    system.Send(sagaId, new PaymentReceived(op.OrderId));
            }
        };

        var execution = await system.ExecuteSaga<PaymentSaga, string>(new StartPayment(order.Id));
        sagaId = execution.Id;
        await Assert.That(execution.Result).IsEqualTo("started");

        // 模拟外部支付网关:驱动聚合 → OrderPaid 事件流出 → 翻译 → saga 推进 → 完成
        await system.AskAsync<object?>(order.Id, new MarkPaid(order.Id));
        await Task.Delay(300); // auto-stop 异步

        var gone = await system.GetAsync<PaymentSaga>(sagaId);
        await Assert.That(gone).IsNull();

        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[0]).IsTypeOf<PaymentStep1Started>();
        await Assert.That(events[1]).IsTypeOf<PaymentStep2Done>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();
        await Assert.That(((SagaCompleted)events[2]).Result).IsEqualTo("paid");
    }

    [Test]
    public async Task Saga_Recovery_QueriesAggregateState_Completes()
    {
        var store = new InMemoryEventStore();
        var sagaId = Guid.CreateVersion7();
        var orderId = Guid.CreateVersion7();

        // 中断场景:步骤 1 已持久化(StartPayment 已处理),PaymentReceived 未到达前"崩溃"
        await store.AppendAsync(sagaId, 0, [new PaymentStep1Started(orderId)]);
        // 聚合独立事件流:订单已支付(外部世界的事实,saga 不知道)
        await store.AppendAsync(orderId, 0, [new OrderCreated(orderId), new OrderPaid(orderId)]);

        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<OrderActor>(_ => new OrderActor(), () => new OrderActor());
        system.Register<PaymentSaga>(_ => new PaymentSaga(), () => new PaymentSaga());

        var results = await system.ResumeInterruptedSagasAsync<PaymentSaga>(
            nameof(PaymentStep1Started)
        );

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(sagaId);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);

        // resume 主动查询聚合状态 → 已支付 → 直接完成,事件流含框架完成事件
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[1]).IsTypeOf<PaymentStep2Done>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();

        await Task.Delay(300);
        var gone = await system.GetAsync<PaymentSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }
}
