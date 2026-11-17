<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

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
| 依赖体积 | ❌ Akka.NET（8+ 包）、Orleans（10+ 包） | ✅ 1 个包——PicoActor(自带 PicoActor.Abs 与 PicoDI/PicoLog/PicoMediator 抽象包);声明领域事件订阅者时加 PicoMediator,使用容器接线时加 PicoDI |
| DI 集成 | ❌ 绑定 Microsoft.Extensions.DI | ✅ 原生 PicoDI，零反射解析 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 仅部分支持 | ❌ 仅 net10.0(PicoMediator 运行时要求 net10.0+) |
| 学习曲线 | ❌ 陡峭——监督树、集群、远程 | ✅ 极简——Actor + Event + Mailbox |

---

## 快速开始

```bash
dotnet add package PicoActor
dotnet add package PicoActor.EventStore.InMemory   # 存储已是独立包(v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

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
| `InMemoryEventStore` | 已移入 **`PicoActor.EventStore.InMemory`**（v2026.9）——内存存储：每流加锁，乐观并发（基于 ConcurrentDictionary）；用 `AddPicoActorInMemoryStore()` 注册 |
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
`Mutate` 必须是纯函数且不得抛异常;一旦抛出,批次已落盘、状态不可信——actor 会被**停止**(移出 registry),而不是带着不可信状态继续运行。

### Snapshot Acceleration (opt-in)（快照加速，可选）

对于长事件流，每次重建都重放全部事件，这就是恢复成本的来源。
快照（snapshot）是 Actor 每隔 N 个已持久化事件保存的一份不透明载荷，
重建时先恢复它，然后只重放尾部事件。快照**仅用于加速**：
事件流永远不会被截断，每个聚合最多保留一份快照，
快照缺失、损坏或版本超出范围时会回退到完整重放。

```csharp
public sealed class Counter : EventSourcedActor
{
    private int _value;

    // Payload = the state this actor needs to restore (encoding is your choice:
    // PicoActor has no serializer dependency). Return null to opt out.
    protected override byte[]? TakeSnapshot() => BitConverter.GetBytes(_value);

    // MUST be overridden whenever TakeSnapshot is: the framework restores the
    // payload and then replays only the events after `version`.
    protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload) =>
        _value = BitConverter.ToInt32(payload);
}

var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,               // must implement IEventStoreSnapshot
    SnapshotEveryNEvents = 1_000,     // default null = disabled
});
```

- 触发条件：每 N 个**已持久化**事件（按事件计数，而非按 flush 计数）。
- 线上格式：`IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`；
  `version` 等于该载荷覆盖的事件数（`version == event count`）。
- 失败永不影响命令：`TakeSnapshot` 抛异常、存储失败，
  或 `RestoreSnapshot` 抛异常，都会通过 `OnSnapshotFailed` /
  一条警告日志上报，恢复则回退为重放整个事件流。
- `IsReplaying` 是 `protected`：用 `if (IsReplaying) return;` 重写由
  `Mutate` 驱动的读模型发布逻辑，避免每个重放事件都做 O(n) 的工作
  （否则一次重建就是 O(n²)）。
- Schema 标签：重写 `SnapshotSchema`（默认 `null`），返回一个版本字符串，例如
  `"CounterState/v1"`；**只要载荷的布局或其含义发生变化就要递增它**。
  改动它会让已存的快照作废一次——也就是一次完整重放——等该聚合再落盘
  `SnapshotEveryNEvents` 个事件后恢复正常速度。它必须是**类型级常量**：
  如果写成实例取值，且写入实例与重建实例取值不同，每次恢复都会静默退化为完整重放。
- 标签非 `null` 时，框架会把载荷包进信封，并在恢复时比对标签；不匹配
  （或载荷没有信封、而 Actor 声明了标签）就丢弃该快照并重放整个事件流。
  只有 `null` 是关闭开关——`""` 也算真实标签。标签超过 255 个 UTF-8 字节时
  干脆不写快照（通过 `OnSnapshotFailed` 上报）。
  存储必须始终把载荷当作**不透明字节**：不要解析它，也不要假设它等于
  `TakeSnapshot()` 的输出。

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader)（事件日志读取面）

业务代码只面向 `IActorSystem`，拿到的是 Actor——没有任何面向业务的成员会返回事件集合。
`IEventStore` 是**存储作者**的持久化 SPI：把追加的事件落盘、按需取回，
它的 `LoadAsync` 保留为运行时重建的回退读取。工具与投影改用 `IEventLogReader` 读取——
`GetVersionAsync`、`OpenAsync`、`PeekFirstAsync`——其中 `OpenAsync` 返回一个前向游标：

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- 单次观察：`End` 是 `OpenAsync` 时取到的流长度，在游标的整个生命周期内不再变化，
  因此回滚护栏与尾部长度来自同一次打开，游标产出的数量永不超过 `End`。
- 能力发现只有一条规则——`eventStore is IEventLogReader`。实现了读取面的存储，
  让带快照的重建只读快照之后的尾部（文件存储可以只分类头部、只反序列化尾部）；
  没有实现读取面的存储继续走 `LoadAsync`，即读出并反序列化整条事件流。
- `InMemoryEventStore` 已实现 `IEventLogReader`；对其它存储而言，实现它是可选的、纯增量的。

### JSONL Event Store (opt-in)（JSONL 事件存储，可选）

`PicoActor` 只负责运行 Actor,自身不再内置存储。提供存储的是两个包:
`PicoActor.EventStore.InMemory`(内存默认实现,用 `AddPicoActorInMemoryStore()` 注册)
与 `PicoActor.EventStore.Jsonl`(持久化,本节即讲它)。

`PicoActor.EventStore.Jsonl` 为每个聚合保存一个只追加的 JSONL 文件
(`<root>/<aggregateId>.jsonl`,一行一个 JSON 对象),快照作为 sidecar 原子地写入同级的
`<root>-snapshots` 目录。它实现 `IEventStore`、`IEventLogReader`、`IEventStoreSnapshot`
与 `IEventStoreEnumerator`,AOT 安全(PicoJetson 源生成),并且是**单进程**的:同一目录上的
两个 store 实例不共享门闩,并发写入会直接报错,而不是撕裂事件流。

```bash
dotnet add package PicoActor.EventStore.Jsonl
```

```csharp
using PicoActor.EventStore.Jsonl;

// Registration lives on the event BASE, in YOUR assembly: one [PicoDerivedType] per concrete
// event. [PicoSerializable] on the derived type is NOT enough — the generator ignores type
// parameters, so a bare derived type leaves TEventBase without a serializer and the FIRST
// append throws.
[PicoDerivedType(typeof(CounterCreated), "counter-created")]
[PicoDerivedType(typeof(CounterIncremented), "counter-incremented")]
public abstract record CounterEvent : IDomainEvent;

public sealed record CounterCreated(int Initial) : CounterEvent;

public sealed record CounterIncremented(int Delta) : CounterEvent;

var store = new JsonlEventStore<CounterEvent>(
    "/var/lib/myapp/streams",
    new JsonlEventStoreOptions { FlushToDisk = true }
);
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
```

- **漏写注册在构建期暴露,而不是首次写入时才炸**:包里内嵌了 `PICOSTORE001` 分析器,
  会标出没有任何 `[PicoDerivedType]` 列出的领域事件(只用内存存储的项目不为它付出代价)。
- **撕裂尾部与损坏是两件事。** 未以换行结尾的最后一行——进程崩溃唯一会留下的形状——被忽略,
  既不计入也不抛出;下一次追加会把它截断。而**已终止**却无法反序列化的行属于损坏,会抛出
  (包括最后一行),存储不会去修复它,因为修复就是猜。这就是把 `FlushToDisk` 留在默认
  `false` 的书面代价:字节到达操作系统(另一个读取者立刻能看到该事件)却没有落盘,
  掉电就可能留下一个已终止的垃圾行。设 `FlushToDisk = true` 即每次追加都 fsync。
- **游标与仅尾部重建。** `GetVersionAsync`、游标的 `End` 及其字节定位来自同一次廉价扫描
  (完整、非空白、已终止的行——不校验 JSON,不经过反序列化器),因此带快照的重建从
  `snapshot.Version + 1` 打开,只反序列化尾部(`N − v` 个事件)。省下的是反序列化与分配,
  而不是磁盘 I/O:头部仍要被读一次(只读字节)。不需要序列化层的"跳过"探针——通用事件流里
  只有事件,读不出来的行就是损坏,不是外来记录。
- `MaxAggregateLocks`(默认 `1024`)为每聚合门闩表设上界;被淘汰的条目只会让该聚合的下一次
  追加多扫一遍,不影响正确性。`Logger` 独立于 DI 的 `ILoggerFactory`:要看到存储的告警,
  就把同一个 logger 显式传进来。
- 本次发布不含:多进程写入、归档/保留/压缩、外来(非事件)行、加密/压缩。框架事件**不需要第二套编解码器**：
  绑定只是把它们路由到 store 包内生成的序列化器（见下文「Framework events」）。

#### Stream naming（流文件命名，`FileNameFormat`）

`JsonlEventStoreOptions.FileNameFormat` 决定流**文件如何命名**;两种模式的行格式完全一致，
只有名字不同，因此已有目录始终可读。

| 取值 | 流文件 | 换来什么 |
|---|---|---|
| `IdOnly`（默认） | `<id:D>.jsonl` | 历史布局，逐字节复现——无需迁移，已有目录毫无变化 |
| `TypePrefix` | `<type>-<id:D>.jsonl` | 类型写进了文件名，`ListAggregateIdsAsync` 仅凭文件名即可还原 id：每次枚举**零次文件打开** |

`{type}` 是 `events[0].GetType().Name`，即这条流**第一条落盘事件**的具体类型（泛型类型保留其反引号
参数个数），并且在**创建时固定**：后续批次的首个事件若是别的类型，仍然追加到这条流出生时的名字上。

- **迁移到 `TypePrefix`。** PicoAgent 本来就把流命名成 `{type}-{id}.jsonl`，所以把本存储指向这样的
  目录并设 `FileNameFormat = TypePrefix` **完全不需要重命名**——目录原样即可读，快照也原样沿用
  （快照以聚合 id 为键，`<id:D>.snap`，从不以流名为键）。反向不提供：前缀是名字的一部分，`IdOnly`
  存储无法从 id 推导出来，只能把每个文件都改名。
- **混合目录只诊断，不合并。** 仅在**另一种**布局下合法的条目会计数（`ForeignFormatFiles`，按**名字**
  且单调递增：它是 `IdOnly` 重复守卫的闸门），并且每次扫描只报**一条** Warning（检测是双向对称的：
  `TypePrefix` 下的 `<id>.jsonl` 与 `IdOnly` 下的 `<type>-<id>.jsonl` 都算“外来格式”）。整目录诊断
  数的是**历史**而不是名字：若目录里配置格式的历史为零、而**非空**的另一种格式历史有若干个，则抛
  `InvalidOperationException`，消息同时给出两侧计数、两种格式和几条出路——枚举、构造、lazy pass
  一律如此——而不是回一个空的系统；而只有一堆零长度残留文件（失败的追加会留下）的目录是**空**存储，
  保持沉默。属于另一个**事件类型**的流不算混合：那是常态，不计数也不写日志。对某个 id 的追加，
  若它在另一布局下已有非空流——或已存在两个不同的前缀——则在写出任何一个字节之前就被拒绝。
- **成本。** `TypePrefix` 在构造时付一次文件名扫描（建立 id → 类型的映射），每次解析失配再付一次，
  每次追加为重复守卫付一次 `stat`。`IdOnly` 在构造时**零成本**，对已存在的流追加也是零成本；它唯一
  的一次目录扫描是惰性的——在首次观察到一条自己没有的流时执行，且只执行一次。空批次在两种模式下都不
  写、不建文件，但它**和写入型追加一样解析路径**，并依然校验 `expectedVersion`。
- **失配缓存只服务读路径。** `TypePrefix` 的失配会被记住——一个有界的集合，上限 4096 个 id，超过就
  整表清空而不是继续增长——并且是**单飞**的：对**同一个**谁都没有的 id **并发**探测的一整批只共享
  一次扫描，**该 id 的重复探测**不付代价——**另一个**谁都没有的 id 得付它自己的那次扫描。
  **追加绝不信任这份记忆**：在那里把 id 解析成“没有流”，会在一份外部写入者建出的流旁边再开第二个
  名字（那是映射建成之后才出现的文件），所以追加遇到失配会**重扫一次**，成功后只丢弃**它自己**那一条
  记录——从不整表清空，也不去拿扫描闸门。代价这里写明：*读*可能对一条刚出现在旁边的流回答“空”，
  直到下一次扫描——而一个再也不会扫描的宿主，会一直看到这个陈旧答案，直到**新实例**重新读一遍目录。
  `NegativeCacheInvalidations` 数的是追加真正证伪的条目数（在 `IdOnly` 下恒为 `0`，因为它根本不记
  失配）。
- **过期的映射由下一次扫描纠正。** 在映射建成之后才出现的竞争前缀，读路径发现不了（映射命中不付
  scan），**追加也不会**为它做目录扫描：那会正好把映射换来的收益还回去，而且还是在每次追加都要付的
  那条路径上。下一次分类扫描——一次枚举，或某个未知 id 的失配——会记下这个冲突，此后该 id 的每次追加
  都会被拒绝，并在消息里点名两个文件。
- **坏掉的根目录或被占用的名字会被点名，而不是被吞掉。** 根目录本身是一个**文件**时抛 `IOException`
  （那里既不能枚举、不能打开、也不能创建）；某条流的名字被一个**目录**占用时抛
  `InvalidOperationException` 并指出障碍物——读路径与追加路径、两种布局都一样——而不是“空聚合”或
  一个裸的共享冲突。失败的 lazy pass（误配、根目录不可读、根目录是文件）会被 **latch**：之后每一次
  观察都抛同一个异常，而且目录不会再被扫描一次，因此唯一的恢复方式是目录修好后**重建实例**。

#### Framework events (event type bindings)（框架事件：事件类型绑定）

`SagaCompleted`/`SagaFailed` 是 `sealed`，派生自 `IDomainEvent` 而不是你的 `TEventBase`，因此 saga 流里躺着一个
你的序列化器从未见过的类型。`JsonlEventStoreOptions.EventTypeBindings` 把框架事件路由进同一个 store——框架
预注册 `SagaCompleted → PicoActorSagaCompleted` 与 `SagaFailed → PicoActorSagaFailed`（`TryAdd`，所以你自己
的 `Add<T>(tag)` 优先；`FrameworkDefaultCount` 是实际应用了几个默认绑定，`0…2`，`RegisteredCount` 是总数）。

- **`Add<T>(tag)` 只路由，不生成。** tag 就是行内的 `$type`，也是读侧嗅探的依据（首个成员、大小写不敏感、
  认不出就无条件回落 `TEventBase`），但该类型仍需要一个已生成的序列化器。对 **positional record** 而言
  `[GenerateSerializer(typeof(T))]` 不可用（PicoSerDe 的属性管线读不到主构造器：`CS7036`/`CS8852`）；
  可用形式是**闭合调用点**——`FrameworkEventSerializers.Generate()` 里的字面调用。没有生成点的 bound
  类型会在**首写**被拒绝（`InvalidOperationException`，消息里给出 remedy）。
- **混批就是一次写：** `TEventBase` 与 bound 事件共用同一个 batch——一条 buffer、一次写、一次 flush。
- **命名。** 两个框架 tag 就是生产语料已有的 tag，是永久线格式（无 alias）。流仍按首事件的 CLR 短名命名；
  在 `TypePrefix` 下，落在所有已注册**类型**前缀之外的名字（`unknown-<id>.jsonl`，宿主自己的分类）**按 id 可达**、按类型枚举不可见、
  不计入 `ForeignFormatFiles`、也不会触发整目录抛。前缀不含 `-`；同短名共用一个前缀。
- **`SagaCompleted.Result` 是 `object?`，而序列化器根本写不出非 null 的值**（生成的 writer 只写出成员名、
  不写值）。`null`——**持久化存储**今天唯一接受的形状——能正常往返；其它任何值都会当场失败，且一个字节也
  不会写入。带非 null 结果的 `MarkComplete(result)` 无法持久化：终态批次直接抛，什么都不会写入。
  持久化存储里请让 `Result` 保持 `null`。
- **注册规则。** 绑定挂在 options 实例上——在构造 store **之前**调用
  `options.EventTypeBindings.Add<T>(tag)`：构造函数会**冻结并拷贝**这张表，之后的 `Add` 会抛异常，而不是被
  静默忽略。一个类型只有一个 tag，一个 tag 只命名一个类型（同一对重复调用是 no-op，但仅限**冻结前**；
  tag 非空且不含引号、反斜杠或控制字符）。会把框架事件一并覆盖的基类（例如 `IDomainEvent` 本身）在构造
  时被拒绝——否则它们既是普通事件又是 bound 事件；可赋值给 `TEventBase` 的 bound 类型同样被拒绝。`with`
  复制的是**引用**：副本与原件共用同一张表（含冻结状态），因此副本上的 `Add` 会抛，且消息点名「另一个」
  store——要独立的一张表请用 `new JsonlEventStoreOptions()`。
- **绑定的 tag 必须与 `TEventBase` 上每一个 `[PicoDerivedType]` 判别值都不同。** sniffer 优先，所以一旦
  撞上，基类的行会被读成 bound 类型——不抛异常、不警告，完全静默。

### Idle Passivation (opt-in)（空闲钝化，可选）

Actor 会一直驻留内存，直到被停止。对于拥有大量长期存活聚合的工作负载，
一段空闲预算可以回收那些能够重建的 Actor 的具体化状态：

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- 候选对象**仅限可重建的事件溯源 Actor**：即具体类型是以
  **重建工厂**注册过的 `EventSourcedActor`。非 ES Actor 永远不会被触碰
  （它们的状态无法重建）。
- 候选对象必须已初始化、未在处理命令、邮箱为空，
  并且在整个预算时间内都处于空闲。停止操作以框架哨兵的形式
  穿过邮箱，因此落在消息边界上；停止请求会做所有权校验，
  所以过期的请求无法移除同一 id 的较新实例。
- `SagaActor` 豁免（`CanBePassivated => false`）：等待外部应答
  看起来就像空闲，回收它会丢掉那个应答。
- 重新激活是显式的：`GetAsync<T>(id)` 重放事件流（或恢复快照
  并重放尾部）。在此之前 `Send`/`AskAsync` 的行为与任何未注册 id
  完全一致（`KeyNotFoundException`）——`AskAsync` 没有类型参数，
  因此无法选择重建工厂。
- 可观测性：`SweepIdleActorsAsync()`、`PassivatedCount`、`ReactivatedCount`，
  以及每次重建带重放事件数的一行日志。

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
若 `SagaFailed` 本身落盘失败(store 仍不可用),则不产生终态,向调用方传播**原始异常**。

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
| 发后不理 | `Send(id, command)` | 无回复；失败路由到 UnhandledErrorHandler(无 logger 时写 stderr)；actor 不存在或 mailbox 已关闭(停机竞态)时抛异常 |

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
container.AddPicoActor();               // 注册 IActorSystem——不再注册存储
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory:内存默认实现

// 自定义事件存储
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// 通过 PicoCfg 配置
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` 注册为 **Singleton**。若注册了 `ILoggerFactory`，
日志记录器会自动注入。

`AddPicoActor()` 不再注册任何存储,但注册时不会失败:只在解析 `IActorSystem` 时抛出
`InvalidOperationException`,消息里点名 `AddPicoActorInMemoryStore()`(注册顺序因此仍然自由,
显式 `AddPicoActor(store)` 依旧优先)。

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

handler 可以是 class 或 record;无可访问(public/internal)实例构造函数的实现会在构建期以 **PICA001** 报错,而不是生成不可编译的注册代码。

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

> **必需包:** 需要添加的只有 `PicoActor`(自带 `PicoActor.Abs` 与 `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator` 抽象)。声明 `IDomainEventSubscriber<TEvent>` 处理器无需额外加包——`PicoMediator` 本身就是 `PicoActor` 的依赖(生成的桥代码调用它的 `MediatorAutoSubscriptionRegistry`);使用容器接线(`SvcContainer`、`AddPicoMediator`)时需再加 `PicoDI` 与 `PicoMediator.DI`。

> **本地开发(ProjectReference):** analyzer 不随 ProjectReference 链传递——工程消费者需直接引用 `PicoActor.Gen`(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`,镜像 `tests/PicoActor.Tests`)。NuGet 消费者经 `PicoActor.Abs` 包的 `buildTransitive` props 自动注入生成器,无需额外引用。

事件以信封形式经 PicoMediator 在 persist+mutate 之后流出;replay 不重复发布。处理器失败不影响 actor(逐处理器隔离)。事件→命令→事件的翻译循环是预期用法——保持处理器幂等且有界。

> **破坏性变更:** 直连 `ISubscriber<TEvent>`(PicoMediator)订阅者不再收到 PicoActor 领域事件。迁移到 `IDomainEventSubscriber<TEvent>`;信封的 `ActorId`/`Version` 取代任何手工内嵌的聚合 id。自定义 publisher(`AddPicoActor(IPublisher)`)现在收到的是 `DomainEventEnvelope` 实例而非裸事件——相应适配 `Publish<TEvent>` 实现(仅观察到的载荷形状变化;actor 管线不受影响)。 `IEventStoreEnumerator.ListAggregateIds(string)` 改为异步 `ListAggregateIdsAsync(string)`——自定义枚举器需更新签名。

注意:
- `Register<T>` 每个 actor 类型只能调用一次:重复注册现在会抛异常,而不是静默替换先前的工厂。
- `Register<T>` 会拒绝运行时无法服务的类型:不派生自 `Actor` 的实现,以及不派生自 `EventSourcedActor` 的 `IEventSourcedActor`(持久化只为具体基类接线)。
- `StopAsync`/`RequestStop` 先从注册表移除 actor,再排空 mailbox 中已缓冲的消息(优雅停机);停机之后发送的消息会抛 `KeyNotFoundException`(若 mailbox 已关闭则为 `InvalidOperationException`——停机竞态窗口)。
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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 运行时：`ActorSystem`、`MediatorDomainEventPublisher`（信封事件流出）、PicoDI 集成（`AddPicoActor` 自动接线 IMediator + ICommandSender） |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | 内存存储（`InMemoryEventStore`，v2026.9 从 `PicoActor` 移出）+ `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | 持久化 JSONL 事件存储：只追加、乐观并发、经 `IEventLogReader` 的仅尾部重建、原子快照 sidecar、AOT 安全（PicoJetson）——另含内嵌 `PICOSTORE001` 注册分析器 |

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
| 包数量 | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## 许可证

MIT — 详见 [LICENSE](LICENSE)。
