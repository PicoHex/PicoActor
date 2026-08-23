# PicoActor

AOT-compatible, in-memory Actor framework with Event Sourcing for .NET.
Lightweight, zero-reflection, designed for agentic AI systems and
workflow orchestration. Runs under NativeAOT and trimming.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## Computational Model

```
┌─────────────────────────────────────────────────┐
│                  ActorSystem                     │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐         │
│  │ Counter │  │  Agent  │  │ Session │  ...     │
│  │ (Actor) │  │ (Actor) │  │ (Actor) │         │
│  └────┬────┘  └────┬────┘  └────┬────┘         │
│       │            │            │               │
│       ▼            ▼            ▼               │
│  ┌─────────────────────────────────────────┐    │
│  │            IEventStore                   │    │
│  │   AppendAsync / LoadAsync               │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

Each actor owns a **mailbox** (in-memory `Channel<Envelope>`), a **UUID v7**
identity, and a single-threaded consumption loop. Commands are delivered
via `Send` (fire-and-forget) or `AskAsync` (request-reply).

PicoActor is **message-driven**: every interaction is a message. Commands
(`ICommand`) are directed messages delivered through the mailbox (1:1,
optional response); domain events (`IDomainEvent`) are broadcast messages
published through PicoMediator (1:N, no response). Events are messages too —
the cross-aggregate collaboration pattern is a single loop:
event → subscriber → translated command → mailbox. There is no other way to
interact with an actor.

Event-Sourced actors follow **Persist-then-Mutate**:
`OnMessageAsync → RaiseEvent → Persist to IEventStore → Mutate state`.
State is only mutated after successful persistence — the in-memory state
is always consistent with the event stream.

---

## Why PicoActor

| Concern | Existing Options | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans all require reflection | ✅ Full NativeAOT support |
| Event Sourcing | ❌ Proto.Actor, Orleans lack built-in ES | ✅ Persist-then-Mutate, automatic rollback |
| Dependency Size | ❌ Akka.NET (8+ packages), Orleans (10+ packages) | ✅ 4 packages — PicoActor + PicoActor.Abs + PicoMediator.Abs + PicoDI.Abs; no other runtime dependencies |
| DI Integration | ❌ Tied to Microsoft.Extensions.DI | ✅ Native PicoDI, zero-reflection resolution |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor only | ❌ net10.0-only (PicoMediator runtime requires net10.0+) |
| Learning Curve | ❌ Steep — supervision trees, clustering, remoting | ✅ Minimal — actors + events + mailbox |

---

## Quick Start

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Setup
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Register actor factories
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Create, message, stop, rebuild
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Module Details

### PicoActor.Abs — Core Abstractions

Targets `net10.0` (PicoMediator runtime + generated bridge code require net10.0+). Contains all
interfaces and base classes.

| Type | Role |
|------|------|
| `IActor` | Base interface — provides `Id` (UUID v7) |
| `IActorSystem` | Runtime contract — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, RequestStop, StopAllAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | Marker interface for commands |
| `IDomainEvent` | Marker interface for domain events |
| `IEventSourcedActor` | Optional interface — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Persistence contract — AppendAsync (optimistic concurrency), LoadAsync, PeekFirstAsync |
| `ICancelable` | Optional — CancelCurrentTurn for long-running operations |
| `Actor` | Abstract base — mailbox, consumption loop, SignalReady, StopAsync |
| `EventSourcedActor` | ES base — RaiseEvent, Mutate, Persist-then-Mutate pipeline |
| `SagaActor` | Finite-life ES coordinator — framework terminal events (SagaCompleted/SagaFailed), auto-stop, explicit batch recovery via ResumeInterruptedSagasAsync |
| `IDomainEventSubscriber<TEvent>` | Subscriber contract — typed envelope + `ICommandSender`; auto-registered by PicoActor.Gen (declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | Context envelope — `ActorId`, `Version`, `Event` (transport / typed delivery) |
| `ICommandSender` | Narrow command port for handlers — Send, AskAsync, ExecuteSaga |
| `Envelope` | Internal — wraps ICommand with optional TaskCompletionSource |
| `ActorOutputEvent` | Outbound notification — Type, Data, optional ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | Thrown by IEventStore on version mismatch |

### PicoActor — Runtime

Targets `net10.0`, AOT-compatible.

| Type | Role |
|------|------|
| `ActorSystem` | Default `IActorSystem` — ConcurrentDictionary registry, factory registration, message routing, CancelTurn |
| `InMemoryEventStore` | Lock-free in-memory store — ConcurrentDictionary-backed |
| `ActorConfig` | Configuration POCO — bind from PicoCfg |
| `ActorSystemOptions` | Options — required EventStore, optional Logger, optional DomainEventPublisher; consumed by the `ActorSystem` constructor |
| `MediatorDomainEventPublisher` | Default `IDomainEventPublisher` — publishes `DomainEventEnvelope` per event with per-event isolation |
| `PicoActorDiExtensions` | `AddPicoActor()` extension method for PicoDI |

### Actor (Non-ES)

Inherit from `Actor` for pure in-memory operational actors.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Event-Sourced Actor

Inherit from `EventSourcedActor`. Override `OnMessageAsync` to call
`RaiseEvent`, and `Mutate` to apply state changes.

```csharp
public sealed class Counter : EventSourcedActor
{
    private int _value;

    public Counter(CreateCounter cmd) : base(cmd) { }
    public Counter() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case Increment i:
                RaiseEvent(new CounterIncremented(i.Delta));
                break;
            case GetValue:
                return new ValueTask<object?>(_value);
        }
        return default;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case CounterIncremented e: _value += e.Delta; break;
        }
    }
}
```

### SagaActor (Finite-Life Coordinator)

`SagaActor extends EventSourcedActor` — for cross-aggregate operations with a
finite lifecycle. Unlike a regular EventSourcedActor whose mailbox runs
forever, a SagaActor **auto-stops** after the framework persists its terminal
event.

Terminal state is **framework-generated**: `MarkComplete(result)` appends a
`SagaCompleted(result)` event to the same batch as your business events
(atomic append — completion and persistence cannot diverge); an uncaught
business exception appends `SagaFailed(reason)` (`"ExceptionType: message"`,
truncated to 512 chars) and propagates `SagaExecutionException(Id, Reason)` to
the caller. Subclasses never raise or handle these events — `Mutate` only sees
business events.

```csharp
public sealed class OrderSaga : SagaActor
{
    private int _step;
    private Guid _orderId;

    public OrderSaga() { }  // Parameterless — commands via mailbox

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is PlaceOrder cmd)
        {
            if (_step < 1) { RaiseEvent(new OrderPlaced(cmd.OrderId)); _orderId = cmd.OrderId; }
            if (_step < 2) RaiseEvent(new PaymentReserved(_orderId));
            MarkComplete(_orderId);  // framework appends SagaCompleted(_orderId) atomically
            return _orderId;
        }
        return null;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case OrderPlaced e: _step = 1; _orderId = e.OrderId; break;
            case PaymentReserved: _step = 2; break;
            // SagaCompleted/SagaFailed are filtered by the framework — never here
        }
    }

    protected override async ValueTask ResumeAsync()
    {
        // Re-evaluate after crash recovery. Idempotent — _step guards skip
        // completed steps; may AskAsync external aggregates or no-op to wait.
        await OnMessageAsync(new PlaceOrder(_orderId));
    }
}
```

**Lifecycle:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

Business failure: `OnMessageAsync`/`ResumeAsync` throws → framework discards
uncommitted business events, appends `SagaFailed(reason)`, auto-stops, and
faults the Ask caller with `SagaExecutionException(Id, Reason)`. Infrastructure
failures (store down) are **not** terminal — events roll back and the saga
stays Running.

**Crash recovery:** `GetAsync` replays events → the framework restores
`Completed`/`Failed` from `SagaCompleted`/`SagaFailed` (terminal sagas stay
dead — `GetAsync` returns null). Sagas without a terminal event get
`ResumeAsync()` called, and if that reaches the terminal state the framework
persists `SagaCompleted` in the same flush. Recovery is explicit pull, not
background magic.

**Explicit batch recovery:**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` enumerates sagas
by first-event type name, single-flights each through `GetAsync`, and returns
the post-resume classification: `Completed` / `Failed` (with the framework
recovered reason) / `Running` (still waiting for external input). Already
terminal sagas are never resurrected; already-live sagas are classified
in-place. Idempotent and safe to retry (single-flight per id); a store failure
fails fast so the caller can retry the whole batch.

**Process manager pattern:** the same base class covers process managers —
external events are translated to commands by an application-level event
handler (e.g. a PicoMediator subscriber), which then sends them to the saga's
mailbox. Events never go directly into actors; the saga only sees commands.

**Convenience API:**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` does `CreateAsync` + `AskAsync` in one
call. Returns `SagaExecution<TResult>(Id, Result)`; on business failure it
throws `SagaExecutionException(SagaId, Reason)` so the caller always gets the
saga id.

### Persist-then-Mutate Pipeline

```
OnMessageAsync → RaiseEvent (record only, no state change)
              → AppendAsync (persist to IEventStore)
              → Mutate (apply events to in-memory state)
              → Reply to caller (only after success)
```

If `AppendAsync` fails, uncommitted events are discarded and `Version`
is rolled back. The actor is **not poisoned** — the next message
processes cleanly.

### Messaging: Ask vs Send

| Pattern | Method | Semantics |
|---------|--------|-----------|
| Request-Reply | `AskAsync<TResult>(id, command)` | Returns result after message processing |
| Fire-and-Forget | `Send(id, command)` | No reply; exceptions routed to UnhandledErrorHandler |

### OutputChannel

Actors broadcast `ActorOutputEvent` messages to external subscribers:

```csharp
counter.OutputWriter = channel.Writer;
// Inside OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Cancel long-running operations without stopping the actor:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... long-running work with _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
```

### Spawn

Actors create child actors via `System` reference:

```csharp
protected override async ValueTask<object?> OnMessageAsync(ICommand command)
{
    if (command is SpawnChild s)
    {
        var child = await System!.CreateAsync<Worker>(s.Cmd);
        return child.Id;
    }
    return default;
}
```

---

## Module Integration

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Registers IActorSystem + InMemoryEventStore

// Custom event store
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// From PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` is a **Singleton**. If `ILoggerFactory` is registered,
a logger is automatically injected.

### Custom IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT with concurrency check */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT ordered by version */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* SELECT first event (recovery enumeration) */ }
}
```

### Event Outflow (PicoMediator)

`IDomainEvent : IEvent` — domain events are first-class PicoMediator
notifications. After persist+mutate, the framework publishes them as context
envelopes through the `IDomainEventPublisher` hook; replay (recovery) never
republishes.

`MediatorDomainEventPublisher` is the out-of-the-box adapter: it wraps each
event in a `DomainEventEnvelope(actorId, version, event)` and publishes it via
`Publish<DomainEventEnvelope>` (compile-time generic, AOT-safe) with per-event
isolation — one failing subscriber never blocks later events.

### Subscribing to Domain Events (declare-and-subscribe)

Event handlers are plain classes implementing `IDomainEventSubscriber<TEvent>`
— PicoActor.Gen (embedded in PicoActor.Abs) scans and auto-registers them;
no manual wiring. The handler receives a typed envelope carrying the source
aggregate context (`ActorId`, `Version`) plus a narrow `ICommandSender` port:

```csharp
public sealed class OrderPaidHandler : IDomainEventSubscriber<OrderPaid>
{
    public ValueTask Handle(DomainEventEnvelope<OrderPaid> envelope, ICommandSender sender, CancellationToken ct)
    {
        sender.Send(envelope.Event.OrderId, new ShipOrder(envelope.Event.OrderId));
        return default;
    }
}
```

// Wiring: AddPicoMediator registers IMediator; AddPicoActor() auto-detects it in the
// ActorSystem factory and wires event outflow (lazy — Scoped-compatible).
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe: auto-registers subscribers
container.AddPicoActor();     // auto-wires MediatorDomainEventPublisher
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// Explicit wiring for custom publishers: AddPicoActor(IPublisher)
// (the publisher instance must be available before Build()).
```

> **Local development (ProjectReference):** analyzers do not flow through
> `ProjectReference` chains — project consumers must add a direct reference to
> `PicoActor.Gen` (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj"
> OutputItemType="Analyzer" />`, mirroring `tests/PicoActor.Tests`). NuGet
> consumers get the generator automatically via `PicoActor.Abs`'s
> `buildTransitive` props — no extra reference needed.

Events flow out as envelopes through PicoMediator after persist+mutate; replay
never publishes. Handler failures never affect the actor (per-event isolation).
Translation loops (event → command → event) are intended; keep handlers
idempotent and bounded.

> **Breaking change:** direct `ISubscriber<TEvent>` (PicoMediator)
> subscribers no longer receive PicoActor domain events. Migrate to
> `IDomainEventSubscriber<TEvent>`; the envelope's `ActorId`/`Version` replace
> any manually embedded aggregate id. Custom publishers
> (`AddPicoActor(IPublisher)`) now receive `DomainEventEnvelope` instances
> instead of raw events — adapt `Publish<TEvent>` implementations accordingly
> (only the observed payload shape changed; the actor pipeline is unaffected).

Notes:
- **Event → command translation is the subscriber's (business-layer) job** —
  PicoActor only publishes; commands enter actors exclusively via the mailbox.
- Publish runs **after persist+mutate** — a failed publish never corrupts
  actor state; events are already durable.
- Recovery is silent: replay never republishes events.
- Framework events (`SagaCompleted`/`SagaFailed`) are typed-subscribable
  like any other event (Abs targets net10.0).
- **Auto-wiring is safe from any resolving scope**: since PicoDI 2026.8.1 (E1)
  singleton factories run against the container-internal root scope, the
  auto-wired IMediator lives until container disposal.
- **Never `AskAsync` the publishing aggregate from its own publish path** — the
  source mailbox is busy flushing the event, so the request would self-deadlock.
  Query read-side projections (separate actors) instead; `Send` to the source
  aggregate is safe (fire-and-forget).

---

## Design Philosophy (克制 / 专注 / 优雅 / 高效)

| Principle | In Practice |
|-----------|------------|
| **克制 (Restraint)** | No distributed consensus, no supervision trees — just actors and events. |
| **专注 (Focus)** | Single-threaded per-actor. One message at a time. |
| **优雅 (Elegance)** | Persist-then-Mutate: state changes only after persistence. Rollback is automatic. |
| **高效 (Efficiency)** | AOT-compatible, zero reflection, `net10.0` abstractions. |

---

## Use Cases

- **Agentic AI systems** — each AI agent is an actor with conversation state
- **Workflow orchestration** — SagaActor coordinates cross-aggregate operations with framework terminal events, auto-stop, and explicit batch recovery
- **Game server state** — event-sourced actors for player/game state
- **IoT device state** — in-memory actors with periodic snapshotting

---

## Packages

| Package | Target | Description |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | Core abstractions: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor` — plus subscription types (`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`) and the embedded `PicoActor.Gen` analyzer (declare-and-subscribe) |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `InMemoryEventStore`, `MediatorDomainEventPublisher` (envelope event outflow), PicoDI integration (`AddPicoActor` auto-wires IMediator + ICommandSender) |

---

## Comparison

| Feature | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| In-memory only | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 abstractions | ❌ | ✅ | ✅ | ❌ |
| PicoDI integration | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Distributed / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded per actor | ✅ | ✅ | ✅ | ❌ |
| Packages | 4 | 8+ | 3+ | 10+ |

---

## License

MIT — see [LICENSE](LICENSE).
