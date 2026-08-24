# PicoActor

AOT 兼容的内存 Actor 框架，支持事件溯源（Event Sourcing）。轻量、零反射，专为 AI Agent 系统和工作流编排设计。可在 NativeAOT 和裁剪环境下运行。

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## 计算模型

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

每个 Actor 拥有一个 **邮箱**（内存 `Channel<Envelope>`）、一个 **UUID v7**
标识以及单线程消费循环。命令通过 `Send`（发后不理）或 `AskAsync`
（请求-回复）投递。

PicoActor 是**消息驱动**的：一切交互都是消息。命令（`ICommand`）是定向消息，经邮箱投递（1:1，可带响应）；领域事件（`IDomainEvent`）是广播消息，经 PicoMediator 发布（1:N，无响应）。事件也是消息——跨聚合协作只有一种模式：事件 → 订阅者 → 翻译命令 → 邮箱。除此之外没有其他与 actor 交互的方式。

事件溯源 Actor 遵循 **先持久化再变更（Persist-then-Mutate）**：
`OnMessageAsync → RaiseEvent → 持久化到 IEventStore → Mutate 状态`。
状态仅在持久化成功后变更——内存状态始终与事件流保持一致。

---

## 为什么选择 PicoActor

| 关注点 | 现有方案 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 裁剪 | ❌ Akka.NET、Proto.Actor、Orleans 均依赖反射 | ✅ 完整 NativeAOT 支持 |
| 事件溯源 | ❌ Proto.Actor、Orleans 无内置 ES | ✅ Persist-then-Mutate，自动回滚 |
| 依赖体积 | ❌ Akka.NET（8+ 包）、Orleans（10+ 包） | ✅ 4 个包——PicoActor + PicoActor.Abs + PicoMediator.Abs + PicoDI.Abs；无其他运行时依赖 |
| DI 集成 | ❌ 绑定 Microsoft.Extensions.DI | ✅ 原生 PicoDI，零反射解析 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 仅部分支持 | ❌ 仅 net10.0(PicoMediator 运行时要求 net10.0+) |
| 学习曲线 | ❌ 陡峭——监督树、集群、远程 | ✅ 极简——Actor + Event + Mailbox |

---

## 快速开始

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. 初始化
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. 注册 Actor 工厂
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. 创建、发消息、停止、重建
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## 模块详情

### PicoActor.Abs — 核心抽象

目标框架 `net10.0`(PicoMediator 运行时与生成的 bridge 代码要求 net10.0+)。

| 类型 | 角色 |
|------|------|
| `IActor` | 基础接口——提供 `Id`（UUID v7） |
| `IActorSystem` | 运行时契约——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、RequestStop、StopAllAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | 命令标记接口 |
| `IDomainEvent` | 领域事件标记接口 |
| `IEventSourcedActor` | 可选接口——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 持久化契约——AppendAsync（乐观并发）、LoadAsync、PeekFirstAsync |
| `Actor` | 抽象基类——邮箱、消费循环、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基类——RaiseEvent、Mutate、Persist-then-Mutate 管线 |
| `SagaActor` | 有限生命周期 ES 协调器——框架终态事件（SagaCompleted/SagaFailed）、自动停止、经 ResumeInterruptedSagasAsync 显式批量恢复 |
| `IDomainEventSubscriber<TEvent>` | 订阅者契约——类型化信封 + `ICommandSender`；由 PicoActor.Gen 自动注册（declare-and-subscribe） |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | 上下文信封——`ActorId`、`Version`、`Event`（传输 / 类型化交付） |
| `ICommandSender` | 处理器的窄命令端口——Send、AskAsync、ExecuteSaga |
| `Envelope` | 内部——包装 ICommand 与可选 TaskCompletionSource |
| `ActorOutputEvent` | 出站通知——Type、Data、可选 TurnId |
| `ConcurrencyException` | 版本不匹配时由 IEventStore 抛出 |

### PicoActor — 运行时

目标框架 `net10.0`，AOT 兼容。

| 类型 | 角色 |
|------|------|
| `ActorSystem` | 默认 `IActorSystem`——ConcurrentDictionary 注册表、工厂注册、消息路由 |
| `InMemoryEventStore` | 内存存储——每流加锁，乐观并发（基于 ConcurrentDictionary） |
| `ActorConfig` | 配置 POCO——可从 PicoCfg 绑定 |
| `ActorSystemOptions` | 选项——必填 EventStore、可选 Logger、可选 DomainEventPublisher;由 `ActorSystem` 构造函数消费 |
| `MediatorDomainEventPublisher` | 默认 `IDomainEventPublisher`——逐事件发布 `DomainEventEnvelope`，逐事件隔离 |
| `PicoActorDiExtensions` | PicoDI 的 `AddPicoActor()` 扩展方法 |

### Actor（非 ES）

继承 `Actor` 实现纯内存操作型 Actor。

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### 事件溯源 Actor

继承 `EventSourcedActor`。重写 `OnMessageAsync` 调用 `RaiseEvent`，
重写 `Mutate` 应用状态变更。

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

### Persist-then-Mutate 管线

```
OnMessageAsync → RaiseEvent（仅记录，不改变状态）
              → AppendAsync（持久化到 IEventStore）
              → Mutate（应用事件到内存状态）
              → 回复调用方（仅在成功后）
```

若 `AppendAsync` 失败，未提交的事件被丢弃，`Version` 回滚。
Actor **不会被毒化**——下一条消息正常处理。

### SagaActor（有限生命周期协调器）

`SagaActor extends EventSourcedActor`——面向生命周期有限的跨聚合操作。与邮箱永久运行的普通 EventSourcedActor 不同，SagaActor 在框架持久化其终态事件后会**自动停止**。

终态由**框架生成**：`MarkComplete(result)` 会把 `SagaCompleted(result)` 事件追加到与业务事件相同的批次（原子追加——完成与持久化不会分叉）；未捕获的业务异常会追加 `SagaFailed(reason)`（`"ExceptionType: message"`，截断至 512 字符），并向调用方抛出 `SagaExecutionException(Id, Reason)`。子类从不 raise 也不处理这些事件——`Mutate` 只看到业务事件。

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
            var orderId = cmd.OrderId;   // local — state changes only via Mutate
            if (_step < 1) RaiseEvent(new OrderPlaced(orderId));
            if (_step < 2) RaiseEvent(new PaymentReserved(orderId));
            MarkComplete(orderId);  // framework appends SagaCompleted(orderId) atomically
            return orderId;
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

**生命周期：**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

业务失败：`OnMessageAsync`/`ResumeAsync` 抛出异常 → 框架丢弃未提交的业务事件、追加 `SagaFailed(reason)`、自动停止，并以 `SagaExecutionException(Id, Reason)` 使 Ask 调用方收到故障。基础设施失败（存储不可用）**不是**终态——事件回滚，saga 保持 Running。

**崩溃恢复：** `GetAsync` 重放事件 → 框架从 `SagaCompleted`/`SagaFailed` 恢复 `Completed`/`Failed`（已终态的 saga 保持死亡——`GetAsync` 返回 null）。没有终态事件的 saga 会收到 `ResumeAsync()` 调用；若因此到达终态，框架在同一 flush 批次中持久化 `SagaCompleted`。恢复是显式拉取，没有后台魔法。

**显式批量恢复：**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` 按首事件类型名枚举 saga，逐个经 `GetAsync` 单飞恢复，并返回恢复后的分类：`Completed` / `Failed`（含框架恢复的 reason）/ `Running`（仍在等待外部输入）。已终态的 saga 永不复活；仍存活的 saga 原地分类。幂等且可安全重试（每个 id 单飞）；存储失败会快速失败，便于调用方重试整批。

**Process Manager 模式：**

同一基类同样覆盖 process manager——外部事件由应用层事件处理器（如 PicoMediator 订阅者）翻译为命令，再发送到 saga 的邮箱。事件永不直接进入 actor；saga 只看到命令。

**便捷 API：**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` 一次调用完成 `CreateAsync` + `AskAsync`。成功返回 `SagaExecution<TResult>(Id, Result)`；业务失败抛出 `SagaExecutionException(SagaId, Reason)`——调用方总能拿到 saga id。

### 消息模式：Ask vs Send

| 模式 | 方法 | 语义 |
|---------|--------|-----------|
| 请求-回复 | `AskAsync<TResult>(id, command)` | 消息处理后返回结果 |
| 发后不理 | `Send(id, command)` | 无回复；异常路由到 UnhandledErrorHandler |

### OutputChannel

Actor 可向外部订阅者广播 `ActorOutputEvent` 消息：

```csharp
counter.OutputWriter = channel.Writer;
// 在 OnMessageAsync 中：
WriteOutput("Incremented", data: "Delta=5");
```

### Spawn

Actor 可通过 `System` 引用创建子 Actor：

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

## 模块集成

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // 注册 IActorSystem + InMemoryEventStore

// 自定义事件存储
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// 通过 PicoCfg 配置
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` 注册为 **Singleton**。若注册了 `ILoggerFactory`，
日志记录器会自动注入。

### 自定义 IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* 带并发检查的 INSERT */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* 按版本排序的 SELECT */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* 查询首个事件（恢复枚举） */ }
}
```

### 事件流出(PicoMediator)

`IDomainEvent : IEvent`——领域事件是 PicoMediator 一等公民通知。persist+mutate 之后,框架以**上下文信封**形式经 `IDomainEventPublisher` 钩子发布;replay(恢复)不重复发布。

`MediatorDomainEventPublisher` 是开箱即用适配器:将每个事件包装为 `DomainEventEnvelope(actorId, version, event)` 后逐事件 `Publish<DomainEventEnvelope>`(编译期泛型,AOT 安全),逐事件隔离——单个订阅者失败不影响后续事件。

### 订阅领域事件(declare-and-subscribe)

事件处理器是实现 `IDomainEventSubscriber<TEvent>` 的普通类——PicoActor.Gen(内嵌于 PicoActor.Abs)扫描并自动注册,零手动接线。处理器收到携带源聚合上下文(`ActorId`、`Version`)的类型化信封,外加窄端口 `ICommandSender`:

```csharp
public sealed class OrderPaidHandler : IDomainEventSubscriber<OrderPaid>
{
    public ValueTask Handle(DomainEventEnvelope<OrderPaid> envelope, ICommandSender sender, CancellationToken ct)
    {
        sender.Send(envelope.Event.OrderId, new ShipOrder(envelope.Event.OrderId));
        return default;
    }
}

// 接线:AddPicoMediator 注册 IMediator;AddPicoActor() 在 ActorSystem 工厂内
// 自动检测并接线事件流出(延迟解析,与 Scoped 生命周期兼容)。
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe:自动注册订阅者
container.AddPicoActor();     // 自动接线 MediatorDomainEventPublisher
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// 自定义 publisher 显式接线:AddPicoActor(IPublisher)(实例需在 Build() 前可用)。
```

> **本地开发(ProjectReference):** analyzer 不随 ProjectReference 链传递——工程消费者需直接引用 `PicoActor.Gen`(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`,镜像 `tests/PicoActor.Tests`)。NuGet 消费者经 `PicoActor.Abs` 包的 `buildTransitive` props 自动注入生成器,无需额外引用。

事件以信封形式经 PicoMediator 在 persist+mutate 之后流出;replay 不重复发布。处理器失败不影响 actor(逐处理器隔离)。事件→命令→事件的翻译循环是预期用法——保持处理器幂等且有界。

> **破坏性变更:** 直连 `ISubscriber<TEvent>`(PicoMediator)订阅者不再收到 PicoActor 领域事件。迁移到 `IDomainEventSubscriber<TEvent>`;信封的 `ActorId`/`Version` 取代任何手工内嵌的聚合 id。自定义 publisher(`AddPicoActor(IPublisher)`)现在收到的是 `DomainEventEnvelope` 实例而非裸事件——相应适配 `Publish<TEvent>` 实现(仅观察到的载荷形状变化;actor 管线不受影响)。

注意:
- **事件→命令翻译是订阅者(业务层)职责**——PicoActor 只发布;命令只能经 mailbox 进入 actor。
- 发布发生在 **persist+mutate 之后**——发布失败不影响 actor 状态(事件已落盘)。
- 恢复静默:replay 不重复发布。
- 框架事件(`SagaCompleted`/`SagaFailed`)与其他事件一样可类型化订阅(Abs 目标 net10.0)。
- **自动接线任意 scope 安全**:PicoDI 2026.8.1(E1)起 Singleton 工厂使用容器内部根 scope——自动接线的 IMediator 存活至容器释放。
- **禁止在发布路径内对发布源聚合 `AskAsync`**——源 mailbox 正忙于 flush 事件,请求会自我死锁。读模型查询应指向独立的投影 actor;对源聚合 `Send` 安全(fire-and-forget)。

---

## 设计哲学（克制 / 专注 / 优雅 / 高效）

| 原则 | 实践 |
|-----------|------------|
| **克制 (Restraint)** | 无分布式共识，无监督树——只有 Actor 和 Event。 |
| **专注 (Focus)** | 每个 Actor 单线程。一次一条消息。 |
| **优雅 (Elegance)** | Persist-then-Mutate：状态仅在持久化后变更。回滚自动完成。 |
| **高效 (Efficiency)** | AOT 兼容、零反射、`net10.0` 抽象层。 |

---

## 使用场景

- **AI Agent 系统**——每个 AI Agent 是一个带对话状态的 Actor
- **工作流编排**——Actor 建模长时间运行的业务流程
- **游戏服务器状态**——事件溯源 Actor 管理玩家/游戏状态
- **IoT 设备状态**——内存 Actor + 定期快照

---

## 包列表

| 包 | 目标框架 | 描述 |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | 核心抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor`、`SagaActor`——另含订阅类型（`IDomainEventSubscriber<TEvent>`、`DomainEventEnvelope`、`ICommandSender`）与内嵌 `PicoActor.Gen` 分析器（declare-and-subscribe） |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 运行时：`ActorSystem`、`InMemoryEventStore`、`MediatorDomainEventPublisher`（信封事件流出）、PicoDI 集成（`AddPicoActor` 自动接线 IMediator + ICommandSender） |

---

## 对比

| 特性 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| 纯内存 | ✅ | ✅ | ✅ | ❌ |
| AOT / 裁剪 | ✅ | ❌ | ❌ | ❌ |
| 事件溯源 | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 抽象层 | ❌ | ✅ | ✅ | ❌ |
| PicoDI 集成 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 分布式 / 集群 | ❌ | ✅ | ✅ | ✅ |
| 单线程每 Actor | ✅ | ✅ | ✅ | ❌ |
| 包数量 | 4 | 8+ | 3+ | 10+ |

---

## 许可证

MIT — 详见 [LICENSE](LICENSE)。
