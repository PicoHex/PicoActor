using PicoActor.Abs;

namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// End-to-end integration tests: spec section 3 application architecture
//   aggregate actor ← commands; events → Publisher → MiniMediator (fanout)
//   → event-handler translation → commands → saga mailbox → completion → event outflow
// Covers: ExecuteSaga startup, event-loopback advancement, active aggregate-state
// query during recovery (spec 5.6)
// ═══════════════════════════════════════════════════════════

/// <summary>In-test PicoMediator stand-in — IDomainEventPublisher fanout dispatch.</summary>
internal sealed class MiniMediator : IDomainEventPublisher
{
    public Action<Guid, IReadOnlyList<IDomainEvent>>? OnEvents;

    public ValueTask PublishAsync(Guid actorId, ulong version, IReadOnlyList<IDomainEvent> events)
    {
        OnEvents?.Invoke(actorId, events);
        return default;
    }
}

// ── aggregate ──

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

// ── saga (process-manager pattern: waits for event loopback to advance) ──

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
                return "started"; // async method: return the value directly, do not wrap in ValueTask (it would box into object)
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
        // Spec 5.6 recovery-semantics corollary: external aggregate state must be queried
        // actively (events are not replayed to a recovering saga)
        var order = await System!.GetAsync<OrderActor>(_orderId);
        if (order is null)
            return; // aggregate does not exist — keep waiting for external events
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

        // Event-handler translation layer (plain class): OrderPaid → PaymentReceived command → saga mailbox
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

        // Simulate the external payment gateway: drive the aggregate → OrderPaid flows
        // out → translated → saga advances → completes
        await system.AskAsync<object?>(order.Id, new MarkPaid(order.Id));
        await Task.Delay(300); // auto-stop is async

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

        // Interrupted scenario: step 1 was persisted (StartPayment handled);
        // "crashed" before PaymentReceived arrived
        await store.AppendAsync(sagaId, 0, [new PaymentStep1Started(orderId)]);
        // The aggregate's independent stream: the order was paid (a fact of the
        // outside world, unknown to the saga)
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

        // Resume actively queries aggregate state → already paid → completes directly;
        // the stream contains the framework completion event
        var events = await store.LoadAsync(sagaId);
        await Assert.That(events.Count).IsEqualTo(3);
        await Assert.That(events[1]).IsTypeOf<PaymentStep2Done>();
        await Assert.That(events[2]).IsTypeOf<SagaCompleted>();

        await Task.Delay(300);
        var gone = await system.GetAsync<PaymentSaga>(sagaId);
        await Assert.That(gone).IsNull();
    }
}
