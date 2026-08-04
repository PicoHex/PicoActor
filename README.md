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
| Dependency Size | ❌ Akka.NET (8+ packages), Orleans (10+ packages) | ✅ 2 packages, zero dependency beyond Channels |
| DI Integration | ❌ Tied to Microsoft.Extensions.DI | ✅ Native PicoDI, zero-reflection resolution |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor only | ✅ Abstractions target netstandard2.0 |
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

Targets `netstandard2.0` for maximum compatibility. Contains all
interfaces and base classes.

| Type | Role |
|------|------|
| `IActor` | Base interface — provides `Id` (UUID v7) |
| `IActorSystem` | Runtime contract — Register, CreateAsync, GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | Marker interface for commands |
| `IDomainEvent` | Marker interface for domain events |
| `IEventSourcedActor` | Optional interface — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Persistence contract — AppendAsync (optimistic concurrency), LoadAsync |
| `ICancelable` | Optional — CancelCurrentTurn for long-running operations |
| `Actor` | Abstract base — mailbox, consumption loop, SignalReady, StopAsync |
| `EventSourcedActor` | ES base — RaiseEvent, Mutate, Persist-then-Mutate pipeline |
| `SagaActor` | Finite-life ES coordinator — auto-stop after MarkComplete, crash recovery via ResumeAsync |
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
| `ActorSystemOptions` | Options — required EventStore, optional Logger; consumed by the `ActorSystem` constructor |
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
forever, a SagaActor **auto-stops** after `MarkComplete()` is called.

```csharp
public sealed class OrderSaga : SagaActor
{
    private int _step;

    public OrderSaga() { }  // Parameterless — commands via mailbox

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is PlaceOrder cmd)
        {
            if (_step < 1) RaiseEvent(new OrderPlaced(cmd.OrderId));
            if (_step < 2) RaiseEvent(new PaymentReserved(cmd.OrderId));
            if (_step < 3) { RaiseEvent(new OrderCompleted(cmd.OrderId)); MarkComplete(); }
            return cmd.OrderId;
        }
        if (command is GetStep) return _step;
        return null;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case OrderPlaced: _step = 1; break;
            case PaymentReserved: _step = 2; break;
            case OrderCompleted: MarkComplete(); break;
        }
    }

    protected override async ValueTask ResumeAsync()
    {
        // Re-drive from persisted state after crash recovery.
        // Idempotent — _step guards skip completed steps.
        await OnMessageAsync(new PlaceOrder(/* restored from state */));
    }
}
```

**Lifecycle:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete()
→ ProcessAsync returns → ScheduleStop (fire-and-forget)
→ Actor auto-stops, removed from registry
```

**Crash recovery:** events replay via `Mutate` → `OnReadyAsync` detects
`_completed == false && Version > 0` → calls `ResumeAsync` → re-executes
from the interrupted step. Every step must be guarded by `_step` checks
for idempotency.

**Convenience API:**

```csharp
var result = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// Saga auto-stops — no need to call StopAsync
```

`ExecuteSaga<TSaga, TResult>(command)` does `CreateAsync` + `AskAsync` in
one call. The saga self-terminates via `MarkComplete`.

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
}
```

---

## Design Philosophy (克制 / 专注 / 优雅 / 高效)

| Principle | In Practice |
|-----------|------------|
| **克制 (Restraint)** | No distributed consensus, no supervision trees — just actors and events. |
| **专注 (Focus)** | Single-threaded per-actor. One message at a time. |
| **优雅 (Elegance)** | Persist-then-Mutate: state changes only after persistence. Rollback is automatic. |
| **高效 (Efficiency)** | AOT-compatible, zero reflection, `netstandard2.0` abstractions. |

---

## Use Cases

- **Agentic AI systems** — each AI agent is an actor with conversation state
- **Workflow orchestration** — SagaActor coordinates cross-aggregate operations with auto-stop and crash recovery
- **Game server state** — event-sourced actors for player/game state
- **IoT device state** — in-memory actors with periodic snapshotting

---

## Packages

| Package | Target | Description |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Core abstractions: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `InMemoryEventStore`, PicoDI integration |

---

## Comparison

| Feature | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| In-memory only | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 abstractions | ✅ | ✅ | ✅ | ❌ |
| PicoDI integration | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Distributed / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded per actor | ✅ | ✅ | ✅ | ❌ |
| Packages | 2 | 8+ | 3+ | 10+ |

---

## License

MIT — see [LICENSE](LICENSE).
