// ═══════════════════════════════════════════════════════════════
// PicoActor Sample — Event-Sourced Counter
//
// Demonstrates:
//   - ActorSystem + InMemoryEventStore setup
//   - Register with create/rebuild factories
//   - CreateAsync, Send (fire-and-forget), AskAsync (request-reply)
//   - Multi-event scenario (Reset → CounterReset event)
//   - OutputChannel subscription
//   - StopAsync + GetAsync (event-sourced rebuild from persistence)
//   - Idempotent StopAsync
//   - Saga + event outflow: envelope context (ActorId/Version), multi-subscriber
//     (two IDomainEventSubscriber<OrderPaid> handlers), ICommandSender ports
//     (Send / AskAsync; ExecuteSaga from the main flow, also callable from handlers)
//
// This file is ALSO the CI native-AOT gate, so the observations below are ASSERTIONS, not
// decoration: every value a trimmed-away registration would zero out (generated subscribers
// receiving their events, the saga loopback stopping itself, the recovery classification,
// the replayed version) is checked with Gate(...), and a mismatch exits non-zero instead of
// printing "0" next to the Done marker. Waits are bounded polls (GateWaitAsync), never a
// fixed sleep whose length decides whether the assertion sees the work or not.
// ═══════════════════════════════════════════════════════════════

Console.WriteLine("=== PicoActor Sample: Event-Sourced Counter ===\n");

// ── Setup: ActorSystem with InMemoryEventStore ─────────────────
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

system.Register<Counter>(
    createFactory: cmd =>
        cmd switch
        {
            CreateCounter c => new Counter(c),
            _ => throw new InvalidOperationException(
                $"Unexpected creation command: {cmd.GetType().Name}"
            ),
        },
    rebuildFactory: () => new Counter()
);

// ── CreateAsync ────────────────────────────────────────────────
var counter = await system.CreateAsync<Counter>(new CreateCounter(10));
Console.WriteLine($"[CreateAsync] Counter created: Id={counter.Id}");
Console.WriteLine(
    $"[AskAsync]    GetValue → {await system.AskAsync<int>(counter.Id, new GetValue())}"
);

// ── Send (fire-and-forget) ─────────────────────────────────────
system.Send(counter.Id, new Increment(5));
system.Send(counter.Id, new Increment(3));

// No sleep: AskAsync is queued behind the Sends in the same mailbox, so the value it answers
// with is the one produced AFTER they were applied — the gate must not depend on how long a
// delay happened to be.
var afterTwoIncrements = await system.AskAsync<int>(counter.Id, new GetValue());
Console.WriteLine($"[Send+Ask]    After +5, +3 → {afterTwoIncrements}");
Gate(afterTwoIncrements == 18, $"10 + 5 + 3 should be 18, got {afterTwoIncrements}");

system.Send(counter.Id, new Decrement(2));
var afterDecrement = await system.AskAsync<int>(counter.Id, new GetValue());
Console.WriteLine($"[Send+Ask]    After -2 → {afterDecrement}");
Gate(afterDecrement == 16, $"18 - 2 should be 16, got {afterDecrement}");

// ── Multi-event scenario (Reset) ───────────────────────────────
system.Send(counter.Id, new Reset(100));
var afterReset = await system.AskAsync<int>(counter.Id, new GetValue());
Console.WriteLine($"[MultiEvent]  After Reset(100) → {afterReset}");
Gate(afterReset == 100, $"Reset(100) should read 100, got {afterReset}");
Console.WriteLine($"              Version: {counter.Version} (5 events applied)");
Gate(counter.Version == 5, $"5 events should leave Version 5, got {counter.Version}");

// ── Subscribe to OutputChannel ─────────────────────────────────
Console.WriteLine("\n── OutputChannel events (live) ──");
var outChannel = System.Threading.Channels.Channel.CreateUnbounded<ActorOutputEvent>();
counter.OutputWriter = outChannel.Writer;

system.Send(counter.Id, new Increment(42));
system.Send(counter.Id, new Decrement(7));

// Both commands write their output event while they run, and the AskAsync below is queued
// behind them: once it answers, the channel already holds both events.
var finalValue = await system.AskAsync<int>(counter.Id, new GetValue());
Console.WriteLine($"[Final]       GetValue → {finalValue}");
Gate(finalValue == 135, $"100 + 42 - 7 should be 135, got {finalValue}");

var outputEvents = 0;
while (outChannel.Reader.TryRead(out var evt))
{
    outputEvents++;
    Console.WriteLine($"  OutputEvent: {evt.Type}, Data={evt.Data}");
}
Gate(
    outputEvents == 2,
    $"2 commands on a subscribed channel should emit 2 output events, got {outputEvents}"
);

// ── StopAsync & GetAsync (event-sourced rebuild) ───────────────
Console.WriteLine("\n── Stop & Rebuild (event-sourced recovery) ──");
var oldId = counter.Id;
await system.StopAsync(oldId);
Console.WriteLine($"[StopAsync]   Actor {oldId} stopped");

// Rebuild from persisted events
var rebuilt = await system.GetAsync<Counter>(oldId);
Console.WriteLine($"[GetAsync]    Actor {oldId} rebuilt from event stream");
Console.WriteLine(
    $"[AskAsync]    Rebuilt GetValue → {await system.AskAsync<int>(rebuilt!.Id, new GetValue())}"
);
Console.WriteLine($"              Version: {rebuilt.Version} (7 events replayed)");
Gate(rebuilt.Version == 7, $"replaying 7 events should rebuild Version 7, got {rebuilt.Version}");
Console.WriteLine($"              Same Id: {rebuilt.Id == oldId}");
Gate(rebuilt.Id == oldId, "a rebuilt actor must keep its aggregate id");

// ── Idempotent StopAsync ───────────────────────────────────────
await system.StopAsync(rebuilt.Id);
await system.StopAsync(rebuilt.Id); // second call: no-op, no exception
Console.WriteLine("[StopAsync]   Second StopAsync is idempotent (no-op)");

// ── GetAsync returns null after Stop (non-ES) ──────────────────
// Note: Counter is an EventSourcedActor, so GetAsync will rebuild it again.
// This is by design — ES actors are persistent.
var reRebuilt = await system.GetAsync<Counter>(oldId);
Console.WriteLine(
    $"[GetAsync]    Re-rebuild (ES actor persists) → {(reRebuilt is not null ? "found" : "null (non-ES)")}"
);
Gate(reRebuilt is not null, "an event-sourced actor must be rebuildable after StopAsync");

// ═══════════════════════════════════════════════════════════════
// Saga + Event Outflow (PicoMediator) — order payment process manager
// ═══════════════════════════════════════════════════════════════

await SagaAndEventOutflowDemoAsync();

Console.WriteLine("\n=== Done ===");

// ═══════════════════════════════════════════════════════════════
// Demo: saga + event outflow via PicoMediator (process-manager pattern)
// ═══════════════════════════════════════════════════════════════

static async Task SagaAndEventOutflowDemoAsync()
{
    Console.WriteLine("\n=== Saga + Event Outflow (PicoMediator) ===\n");

    // 1. DI wiring: AddPicoMediator (applies PicoActor.Gen registrations for OrderPaidSub/SagaCompletedSub)
    //    + AddPicoActor (auto-wires the registered IMediator for event outflow)
    var store = new InMemoryEventStore();
    var container = new SvcContainer(autoConfigureFromGenerator: false);
    container.AddPicoMediator();
    container.AddPicoActor(store);
    container.Build();
    await using var scope = container.CreateScope();

    var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
    SampleContext.System = system;
    system.Register<OrderActor>(_ => new OrderActor(), () => new OrderActor());
    system.Register<PaymentSaga>(_ => new PaymentSaga(), () => new PaymentSaga());
    system.Register<PaymentLedger>(_ => new PaymentLedger(), () => new PaymentLedger());

    var ledger = await system.CreateAsync<PaymentLedger>(new RecordPayment(Guid.Empty));
    SampleContext.LedgerId = ledger.Id;

    var order = await system.CreateAsync<OrderActor>(new CreateOrder(Guid.NewGuid()));

    // 2. ExecuteSaga starts the process manager (step 1: order registered for payment)
    var execution = await system.ExecuteSaga<PaymentSaga, string>(new StartPayment(order.Id));
    Console.WriteLine($"[ExecuteSaga] result={execution.Result}, sagaId={execution.Id}");
    SampleContext.PaymentSagaId = execution.Id;

    // 3. External payment gateway marks the order paid → OrderPaid flows out as an envelope
    //    → OrderPaidSub translates it into PaymentReceived → saga mailbox + RecordPayment → ledger
    //    → OrderPaidAuditSub (second subscriber, same event) queries the ledger via AskAsync
    await system.AskAsync<object?>(order.Id, new MarkPaid(order.Id));
    // The envelope is published synchronously, but the saga works through its own mailbox:
    // wait for the terminal event it raises AFTER the translated command lands. Bounded, and
    // on the observation itself instead of a sleep that guesses how long that takes.
    await GateWaitAsync(
        () => SagaCompletedSub.Received.Count > 0,
        "SagaCompleted to reach its generated subscriber (event loopback)"
    );

    // 4. Saga completed via event loopback; terminal event flowed out; auto-stopped
    var gone = await system.GetAsync<PaymentSaga>(execution.Id);
    // 4d. Terminal state stops the saga on its own (the loopback, not a caller, closed it).
    Console.WriteLine($"[Loopback]   saga auto-stopped: {gone is null}");
    Gate(gone is null, "the completed saga should be gone (auto-stopped)");
    // 4a. Both generated subscribers for OrderPaid fired; the terminal event reached its own.
    var auditCount = OrderPaidAuditSub.Received.Count;
    Console.WriteLine(
        $"[Outflow]    OrderPaid envelopes received (multi-subscriber): {auditCount}"
    );
    Gate(
        auditCount == 1,
        $"one OrderPaid should reach the second subscriber once, got {auditCount}"
    );
    Gate(
        OrderPaidSub.Handled == 1,
        $"one OrderPaid should reach the first subscriber once, got {OrderPaidSub.Handled}"
    );

    var completedCount = SagaCompletedSub.Received.Count;
    Console.WriteLine($"[Outflow]    SagaCompleted events received: {completedCount}");
    Gate(completedCount == 1, $"exactly one SagaCompleted should flow out, got {completedCount}");

    var completedResult = SagaCompletedSub.Received.FirstOrDefault()?.Result;
    Console.WriteLine($"[Outflow]    SagaCompleted result: {completedResult}");
    Gate(
        completedResult is "paid",
        $"SagaCompleted should carry the MarkComplete result paid, got {completedResult ?? "null"}"
    );

    // 4b. The write-side projection was fed by the event-to-command translation.
    var ledgerCount = await system.AskAsync<int>(ledger.Id, new GetLedgerCount());
    Console.WriteLine($"[Outflow]    Ledger records (write-side projection): {ledgerCount}");
    Gate(ledgerCount == 1, $"the ledger should hold 1 payment record, got {ledgerCount}");

    // 5. Recovery: interrupt a saga after step 1 (no terminal event), then resume explicitly
    var interruptedId = Guid.CreateVersion7();
    await store.AppendAsync(interruptedId, 0, [new PaymentStep1Started(order.Id)]);
    var results = await system.ResumeInterruptedSagasAsync<PaymentSaga>(
        nameof(PaymentStep1Started)
    );
    foreach (var r in results)
        Console.WriteLine($"[Resume]     saga {r.Id}: {r.Status}");

    // The already-completed saga's stream starts with the same event, so the recovery scan
    // sees it and must SKIP it (terminal before rebuild): exactly one result — the
    // interrupted saga — classified Completed because ResumeAsync found the order paid.
    Gate(
        results.Count == 1,
        $"recovery should resume exactly the 1 interrupted saga, got {results.Count}"
    );
    Gate(
        results[0].Id == interruptedId,
        $"recovery should resume the interrupted saga {interruptedId}, got {results[0].Id}"
    );
    Gate(
        results[0].Status == SagaResumeStatus.Completed,
        $"the resumed saga should classify as Completed, got {results[0].Status}"
    );
}

// ═══════════════════════════════════════════════════════════════
// Gate helpers: this sample doubles as the CI native-AOT gate
// ═══════════════════════════════════════════════════════════════

// A mismatch is a GATE failure, not a note in the log: report it on stderr and exit
// non-zero, so a registration that ILC trimmed away (it shows up as a subscriber that never
// fires, i.e. a count of 0) fails the CI job instead of printing "0" next to a Done marker.
static void Gate(bool condition, string message)
{
    if (condition)
        return;

    Console.Error.WriteLine($"GATE FAILED: {message}");
    Console.Error.WriteLine("GATE FAILED: the sample flow did not complete");
    Environment.Exit(1);
}

// Bounded poll on the OBSERVATION instead of a fixed sleep: it returns as soon as the
// condition holds (a few milliseconds of mailbox latency) and fails the gate loudly after
// the timeout, so a trimmed or hung path cannot be mistaken for a slow machine.
static async Task GateWaitAsync(Func<bool> condition, string message, int timeoutMs = 10000)
{
    var deadline = Environment.TickCount64 + timeoutMs;
    while (!condition())
    {
        if (Environment.TickCount64 > deadline)
        {
            Gate(false, $"timed out after {timeoutMs} ms waiting for: {message}");
            return;
        }

        await Task.Delay(10);
    }
}

// ═══════════════════════════════════════════════════════════════
// Domain types (below top-level statements)
// ═══════════════════════════════════════════════════════════════

// ── Commands ───────────────────────────────────────────────────
public sealed record CreateCounter(int InitialValue) : ICommand;

public sealed record Increment(int Delta) : ICommand;

public sealed record Decrement(int Delta) : ICommand;

public sealed record Reset(int NewValue) : ICommand;

public sealed record GetValue : ICommand;

// ── Domain Events ──────────────────────────────────────────────
public sealed record CounterCreated(int InitialValue) : IDomainEvent;

public sealed record CounterIncremented(int Delta) : IDomainEvent;

public sealed record CounterDecremented(int Delta) : IDomainEvent;

public sealed record CounterReset(int OldValue, int NewValue) : IDomainEvent;

// ── Event-Sourced Counter Actor ───────────────────────────────
public sealed class Counter : EventSourcedActor
{
    private int _value;

    /// <summary>Creation path — atomic initialization from CreateCounter.</summary>
    public Counter(CreateCounter cmd)
        : base(cmd) { }

    /// <summary>Rebuild path — required for GetAsync event-sourced recovery.</summary>
    public Counter() { }

    /// <summary>
    /// Dispatch commands. RaiseEvent records events but does NOT mutate state.
    /// After this returns, the framework persists events to IEventStore,
    /// then calls Mutate to apply state changes.
    /// </summary>
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case CreateCounter c:
                RaiseEvent(new CounterCreated(c.InitialValue));
                WriteOutput("CounterCreated", data: $"InitialValue={c.InitialValue}");
                return default;

            case Increment i when i.Delta <= 0:
                throw new ArgumentException("Delta must be positive", nameof(i));
            case Increment i:
                RaiseEvent(new CounterIncremented(i.Delta));
                WriteOutput("Incremented", data: $"Delta={i.Delta}");
                return default;

            case Decrement d when d.Delta <= 0:
                throw new ArgumentException("Delta must be positive", nameof(d));
            case Decrement d:
                RaiseEvent(new CounterDecremented(d.Delta));
                WriteOutput("Decremented", data: $"Delta={d.Delta}");
                return default;

            case Reset r:
                // Multi-event in one message
                RaiseEvent(new CounterReset(_value, r.NewValue));
                WriteOutput("CounterReset", data: $"Old={_value},New={r.NewValue}");
                return default;

            case GetValue:
                return new ValueTask<object?>(_value);
        }

        return default;
    }

    /// <summary>
    /// Mutate is a pure function — called after persistence succeeds
    /// (normal flow) and during ReplayEvents (recovery).
    /// No validation, no I/O, no side effects.
    /// </summary>
    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case CounterCreated e:
                _value = e.InitialValue;
                break;
            case CounterIncremented e:
                _value += e.Delta;
                break;
            case CounterDecremented e:
                _value -= e.Delta;
                break;
            case CounterReset e:
                _value = e.NewValue;
                break;
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// Order payment domain — aggregate + process-manager saga + typed subscriber
// ═══════════════════════════════════════════════════════════════

// ── Commands ───────────────────────────────────────────────────
public sealed record CreateOrder(Guid OrderId) : ICommand;

public sealed record MarkPaid(Guid OrderId) : ICommand;

public sealed record GetPaymentStatus : ICommand;

public sealed record StartPayment(Guid OrderId) : ICommand;

public sealed record PaymentReceived(Guid OrderId) : ICommand;

public sealed record RecordPayment(Guid OrderId) : ICommand;

public sealed record GetLedgerCount : ICommand;

// ── Domain Events ──────────────────────────────────────────────
public sealed record OrderCreated(Guid OrderId) : IDomainEvent;

public sealed record OrderPaid(Guid OrderId) : IDomainEvent;

public sealed record PaymentStep1Started(Guid OrderId) : IDomainEvent;

public sealed record PaymentStep2Done : IDomainEvent;

/// <summary>Shared context for the typed subscriber (demo-only wiring).</summary>
public static class SampleContext
{
    public static IActorSystem? System;
    public static Guid? PaymentSagaId;
    public static Guid? LedgerId;
}

/// <summary>Order aggregate — command-driven, produces business events.</summary>
public sealed class OrderActor : EventSourcedActor
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

/// <summary>Payment saga (process-manager style) — driven by the translated PaymentReceived
/// command. Terminal state is a framework event (SagaCompleted("paid")).</summary>
public sealed class PaymentSaga : SagaActor
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
                return "started";
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
        // Recovery semantics: the saga replays only its own stream — actively query
        // the aggregate to learn external state (events are not replayed to the saga).
        if (_step < 2 && _orderId != Guid.Empty && System is not null)
        {
            var order = await System.GetAsync<OrderActor>(_orderId);
            if (order is not null)
            {
                var paid = await System.AskAsync<bool>(_orderId, new GetPaymentStatus());
                if (paid)
                {
                    RaiseEvent(new PaymentStep2Done());
                    MarkComplete("paid");
                }
            }
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

/// <summary>Write-side projection (plain in-memory actor). Receives RecordPayment via Send;
/// GetLedgerCount answers read-model queries. Lives OUTSIDE the publish path, so
/// AskAsync from a subscriber is safe (no self-deadlock — see OrderPaidAuditSub).</summary>
public sealed class PaymentLedger : Actor
{
    private int _count;

    public PaymentLedger() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case RecordPayment:
                _count++;
                return default;
            case GetLedgerCount:
                return new ValueTask<object?>(_count);
        }
        return default;
    }
}

/// <summary>Domain-event subscriber — event-to-command translation is a business-layer concern.
/// PicoActor.Gen auto-registers it; the envelope carries the source aggregate context
/// (ActorId/Version) plus the narrow ICommandSender port.</summary>
public sealed class OrderPaidSub : IDomainEventSubscriber<OrderPaid>
{
    public static int Handled;

    public ValueTask Handle(
        DomainEventEnvelope<OrderPaid> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Handled++;
        Console.WriteLine(
            $"  [OrderPaidSub]     envelope: ActorId={envelope.ActorId}, Version={envelope.Version}"
        );
        if (SampleContext.PaymentSagaId is { } sagaId)
            sender.Send(sagaId, new PaymentReceived(envelope.Event.OrderId)); // translation → saga mailbox
        if (SampleContext.LedgerId is { } ledgerId)
            sender.Send(ledgerId, new RecordPayment(envelope.Event.OrderId)); // write-side projection
        return default;
    }
}

/// <summary>Second subscriber for the SAME event — every envelope reaches all subscribers
/// (multi-subscriber contract). Uses the AskAsync port to query a read-side projection.
/// NEVER AskAsync the publishing aggregate from its own publish path: the source
/// mailbox is busy flushing the event, so the request would self-deadlock.</summary>
public sealed class OrderPaidAuditSub : IDomainEventSubscriber<OrderPaid>
{
    public static readonly List<DomainEventEnvelope<OrderPaid>> Received = [];

    public async ValueTask Handle(
        DomainEventEnvelope<OrderPaid> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        var count = await sender.AskAsync<int>(SampleContext.LedgerId!.Value, new GetLedgerCount());
        Console.WriteLine(
            $"  [OrderPaidAuditSub] ActorId={envelope.ActorId} v{envelope.Version} → AskAsync(GetLedgerCount)={count}"
        );
    }
}

/// <summary>Domain-event subscriber for the framework terminal event.</summary>
public sealed class SagaCompletedSub : IDomainEventSubscriber<SagaCompleted>
{
    public static readonly List<SagaCompleted> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<SagaCompleted> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope.Event);
        return default;
    }
}
