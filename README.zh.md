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

事件溯源 Actor 遵循 **先持久化再变更（Persist-then-Mutate）**：
`OnMessageAsync → RaiseEvent → 持久化到 IEventStore → Mutate 状态`。
状态仅在持久化成功后变更——内存状态始终与事件流保持一致。

---

## 为什么选择 PicoActor

| 关注点 | 现有方案 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 裁剪 | ❌ Akka.NET、Proto.Actor、Orleans 均依赖反射 | ✅ 完整 NativeAOT 支持 |
| 事件溯源 | ❌ Proto.Actor、Orleans 无内置 ES | ✅ Persist-then-Mutate，自动回滚 |
| 依赖体积 | ❌ Akka.NET（8+ 包）、Orleans（10+ 包） | ✅ 2 个包，除 Channels 外零依赖 |
| DI 集成 | ❌ 绑定 Microsoft.Extensions.DI | ✅ 原生 PicoDI，零反射解析 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 仅部分支持 | ✅ 抽象层目标 netstandard2.0 |
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

目标框架 `netstandard2.0`，最大兼容性。

| 类型 | 角色 |
|------|------|
| `IActor` | 基础接口——提供 `Id`（UUID v7） |
| `IActorSystem` | 运行时契约——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | 命令标记接口 |
| `IDomainEvent` | 领域事件标记接口 |
| `IEventSourcedActor` | 可选接口——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 持久化契约——AppendAsync（乐观并发）、LoadAsync |
| `ICancelable` | 可选——CancelCurrentTurn 用于长时间运行操作 |
| `Actor` | 抽象基类——邮箱、消费循环、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基类——RaiseEvent、Mutate、Persist-then-Mutate 管线 |
| `Envelope` | 内部——包装 ICommand 与可选 TaskCompletionSource |
| `ActorOutputEvent` | 出站通知——Type、Data、可选 ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | 版本不匹配时由 IEventStore 抛出 |

### PicoActor — 运行时

目标框架 `net10.0`，AOT 兼容。

| 类型 | 角色 |
|------|------|
| `ActorSystem` | 默认 `IActorSystem`——ConcurrentDictionary 注册表、工厂注册、消息路由、CancelTurn |
| `InMemoryEventStore` | 无锁内存存储——基于 ConcurrentDictionary |
| `ActorConfig` | 配置 POCO——可从 PicoCfg 绑定 |
| `ActorSystemOptions` | 选项——必填 EventStore、可选 Logger、可选 DomainEventPublisher;由 `ActorSystem` 构造函数消费 |
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

### CancelTurn

取消长时间运行操作而不停止 Actor：

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... 使用 _currentTurnCts.Token 的长时间工作
    }
}
system.CancelTurn(actor.Id);
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
}
```

---

## 设计哲学（克制 / 专注 / 优雅 / 高效）

| 原则 | 实践 |
|-----------|------------|
| **克制 (Restraint)** | 无分布式共识，无监督树——只有 Actor 和 Event。 |
| **专注 (Focus)** | 每个 Actor 单线程。一次一条消息。 |
| **优雅 (Elegance)** | Persist-then-Mutate：状态仅在持久化后变更。回滚自动完成。 |
| **高效 (Efficiency)** | AOT 兼容、零反射、`netstandard2.0` 抽象层。 |

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | 核心抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 运行时：`ActorSystem`、`InMemoryEventStore`、PicoDI 集成 |

---

## 对比

| 特性 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| 纯内存 | ✅ | ✅ | ✅ | ❌ |
| AOT / 裁剪 | ✅ | ❌ | ❌ | ❌ |
| 事件溯源 | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 抽象层 | ✅ | ✅ | ✅ | ❌ |
| PicoDI 集成 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 分布式 / 集群 | ❌ | ✅ | ✅ | ✅ |
| 单线程每 Actor | ✅ | ✅ | ✅ | ❌ |
| 包数量 | 2 | 8+ | 3+ | 10+ |

---

## 许可证

MIT — 详见 [LICENSE](LICENSE)。
