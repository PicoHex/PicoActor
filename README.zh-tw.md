# PicoActor

AOT 相容的記憶體 Actor 框架，支援事件溯源（Event Sourcing）。輕量、零反射，專為 AI Agent 系統和工作流程編排設計。可在 NativeAOT 和修剪環境下執行。

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## 計算模型

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

每個 Actor 擁有一個**信箱**（記憶體 `Channel<Envelope>`）、一個 **UUID v7**
識別碼以及單執行緒消費迴圈。命令透過 `Send`（發後不理）或 `AskAsync`
（請求-回覆）投遞。

PicoActor 是**訊息驅動**的：一切互動都是訊息。命令（`ICommand`）是定向訊息，經信箱投遞（1:1，可帶回應）；領域事件（`IDomainEvent`）是廣播訊息，經 PicoMediator 發佈（1:N，無回應）。事件也是訊息——跨聚合協作只有一種模式：事件 → 訂閱者 → 翻譯命令 → 信箱。除此之外沒有其他與 actor 互動的方式。

事件溯源 Actor 遵循**先持久化再變更（Persist-then-Mutate）**：
`OnMessageAsync → RaiseEvent → 持久化到 IEventStore → Mutate 狀態`。
狀態僅在持久化成功後變更——記憶體狀態始終與事件流保持一致。

---

## 為什麼選擇 PicoActor

| 關注點 | 現有方案 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 修剪 | ❌ Akka.NET、Proto.Actor、Orleans 皆依賴反射 | ✅ 完整 NativeAOT 支援 |
| 事件溯源 | ❌ Proto.Actor、Orleans 無內建 ES | ✅ Persist-then-Mutate，自動復原 |
| 依賴體積 | ❌ Akka.NET（8+ 套件）、Orleans（10+ 套件） | ✅ 1 個套件——PicoActor(自帶 PicoActor.Abs 與 PicoDI/PicoLog/PicoMediator 抽象套件);宣告領域事件訂閱者時加 PicoMediator,使用容器接線時加 PicoDI |
| DI 整合 | ❌ 綁定 Microsoft.Extensions.DI | ✅ 原生 PicoDI，零反射解析 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 僅部分支援 | ❌ 僅 net10.0(PicoMediator 執行時要求 net10.0+) |
| 學習曲線 | ❌ 陡峭——監督樹、叢集、遠端 | ✅ 極簡——Actor + Event + Mailbox |

---

## 快速開始

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. 初始化
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. 註冊 Actor 工廠
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. 建立、發送訊息、停止、重建
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## 模組詳情

### PicoActor.Abs — 核心抽象

目標框架 `net10.0`(PicoMediator 執行時與產生的 bridge 程式碼要求 net10.0+)。

| 類型 | 角色 |
|------|------|
| `IActor` | 基礎介面——提供 `Id`（UUID v7） |
| `IActorSystem` | 執行時契約——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、RequestStop、StopAllAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | 命令標記介面 |
| `IDomainEvent` | 領域事件標記介面 |
| `IEventSourcedActor` | 可選介面——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 持久化契約——AppendAsync（樂觀併發）、LoadAsync、PeekFirstAsync |
| `Actor` | 抽象基底類別——信箱、消費迴圈、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基底類別——RaiseEvent、Mutate、Persist-then-Mutate 管線 |
| `SagaActor` | 有限生命週期 ES 協調器——框架終態事件（SagaCompleted/SagaFailed）、自動停止、經 ResumeInterruptedSagasAsync 顯式批次恢復 |
| `IDomainEventSubscriber<TEvent>` | 訂閱者契約——型別化信封 + `ICommandSender`;由 PicoActor.Gen 自動註冊（declare-and-subscribe） |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | 上下文信封——`ActorId`、`Version`、`Event`（傳輸 / 型別化交付） |
| `ICommandSender` | 處理器的窄命令埠——Send、AskAsync、ExecuteSaga |
| `Envelope` | 內部——包裝 ICommand 與可選 TaskCompletionSource |
| `ActorOutputEvent` | 出站通知——Type、Data、可選 TurnId |
| `ConcurrencyException` | 版本不符時由 IEventStore 擲出 |

### PicoActor — 執行時

目標框架 `net10.0`，AOT 相容。

| 類型 | 角色 |
|------|------|
| `ActorSystem` | 預設 `IActorSystem`——ConcurrentDictionary 註冊表、工廠註冊、訊息路由 |
| `InMemoryEventStore` | 記憶體儲存——每串流加鎖，樂觀並行（基於 ConcurrentDictionary） |
| `ActorConfig` | 設定 POCO——可從 PicoCfg 綁定 |
| `ActorSystemOptions` | 選項——必填 EventStore、選用 Logger、選用 DomainEventPublisher;由 `ActorSystem` 建構函式使用 |
| `MediatorDomainEventPublisher` | 預設 `IDomainEventPublisher`——逐事件發佈 `DomainEventEnvelope`,逐事件隔離 |
| `PicoActorDiExtensions` | PicoDI 的 `AddPicoActor()` 擴充方法 |

### Actor（非 ES）

繼承 `Actor` 實作純記憶體操作型 Actor。

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### 事件溯源 Actor

繼承 `EventSourcedActor`。覆寫 `OnMessageAsync` 呼叫 `RaiseEvent`，
覆寫 `Mutate` 套用狀態變更。

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

### Persist-then-Mutate 管線

```
OnMessageAsync → RaiseEvent（僅記錄，不改變狀態）
              → AppendAsync（持久化到 IEventStore）
              → Mutate（套用事件到記憶體狀態）
              → 回覆呼叫方（僅在成功後）
```

若 `AppendAsync` 失敗，未提交的事件被丟棄，`Version` 復原。
Actor **不會被毒化**——下一條訊息正常處理。

### SagaActor（有限生命週期協調器）

`SagaActor extends EventSourcedActor`——面向生命週期有限的跨聚合操作。與信箱永久運行的普通 EventSourcedActor 不同，SagaActor 在框架持久化其終態事件後會**自動停止**。

終態由**框架生成**：`MarkComplete(result)` 會把 `SagaCompleted(result)` 事件附加到與業務事件相同的批次（原子附加——完成與持久化不會分歧）；未捕捉的業務例外會附加 `SagaFailed(reason)`（`"ExceptionType: message"`，截斷至 512 字元），並向呼叫方擲回 `SagaExecutionException(Id, Reason)`。子類別從不 raise 也不處理這些事件——`Mutate` 只看到業務事件。

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

**生命週期：**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

業務失敗：`OnMessageAsync`/`ResumeAsync` 擲出例外 → 框架捨棄未提交的業務事件、附加 `SagaFailed(reason)`、自動停止，並以 `SagaExecutionException(Id, Reason)` 讓 Ask 呼叫方收到錯誤。基礎設施失敗（儲存不可用）**不是**終態——事件復原，saga 保持 Running。

**當機復原：** `GetAsync` 重放事件 → 框架從 `SagaCompleted`/`SagaFailed` 還原 `Completed`/`Failed`（已終態的 saga 保持死亡——`GetAsync` 回傳 null）。沒有終態事件的 saga 會收到 `ResumeAsync()` 呼叫；若因此到達終態，框架在同一 flush 批次中持久化 `SagaCompleted`。復原是明確拉取，沒有背景魔法。

**明確批次復原：**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` 按首事件型別名稱列舉 saga，逐一經 `GetAsync` 單飛復原，並回傳復原後的分類：`Completed` / `Failed`（含框架還原的 reason）/ `Running`（仍在等待外部輸入）。已終態的 saga 永不復活；仍存活的 saga 原地分類。冪等且可安全重試（每個 id 單飛）；儲存失敗會快速失敗，便於呼叫方重試整批。

**Process Manager 模式：**

同一基底類別同樣涵蓋 process manager——外部事件由應用層事件處理器（如 PicoMediator 訂閱者）翻譯為命令，再傳送到 saga 的信箱。事件永不直接進入 actor；saga 只看到命令。

**便利 API：**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` 一次呼叫完成 `CreateAsync` + `AskAsync`。成功回傳 `SagaExecution<TResult>(Id, Result)`；業務失敗擲出 `SagaExecutionException(SagaId, Reason)`——呼叫方總能拿到 saga id。

### 訊息模式：Ask vs Send

| 模式 | 方法 | 語意 |
|---------|--------|-----------|
| 請求-回覆 | `AskAsync<TResult>(id, command)` | 訊息處理後回傳結果 |
| 發後不理 | `Send(id, command)` | 無回覆；例外路由到 UnhandledErrorHandler |

### OutputChannel

Actor 可向外部訂閱者廣播 `ActorOutputEvent` 訊息：

```csharp
counter.OutputWriter = channel.Writer;
// 在 OnMessageAsync 中：
WriteOutput("Incremented", data: "Delta=5");
```

### Spawn

Actor 可透過 `System` 參考建立子 Actor：

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

## 模組整合

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // 註冊 IActorSystem + InMemoryEventStore

// 自訂事件儲存
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// 透過 PicoCfg 設定
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` 註冊為 **Singleton**。若已註冊 `ILoggerFactory`，
日誌記錄器會自動注入。

### 自訂 IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* 帶併發檢查的 INSERT */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* 按版本排序的 SELECT */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* 查詢首個事件（恢復列舉） */ }
}
```

### 事件流出(PicoMediator)

`IDomainEvent : IEvent`——領域事件是 PicoMediator 一級公民通知。persist+mutate 之後,框架以**上下文信封**形式經 `IDomainEventPublisher` 鉤子發布;replay(恢復)不重複發布。

`MediatorDomainEventPublisher` 是開箱即用介面卡:將每個事件包裝為 `DomainEventEnvelope(actorId, version, event)` 後逐事件 `Publish<DomainEventEnvelope>`(編譯期泛型,AOT 安全),逐事件隔離——單一訂閱者失敗不影響後續事件。

### 訂閱領域事件(declare-and-subscribe)

事件處理器是實作 `IDomainEventSubscriber<TEvent>` 的普通類別——PicoActor.Gen(內嵌於 PicoActor.Abs)掃描並自動註冊,零手動接線。處理器收到攜帶源聚合上下文(`ActorId`、`Version`)的型別化信封,外加窄埠 `ICommandSender`:

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

// 接線:AddPicoMediator 註冊 IMediator;AddPicoActor() 在 ActorSystem 工廠內
// 自動偵測並接線事件流出(延遲解析,與 Scoped 生命週期相容)。
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe:自動註冊訂閱者
container.AddPicoActor();     // 自動接線 MediatorDomainEventPublisher
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// 自訂 publisher 顯式接線:AddPicoActor(IPublisher)(實例需在 Build() 前可用)。
```

> **必需套件:** 需要加入的只有 `PicoActor`(自帶 `PicoActor.Abs` 與 `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs` 抽象)。宣告 `IDomainEventSubscriber<TEvent>` 處理器時需再加 `PicoMediator`(生成的橋接碼呼叫該套件的 `MediatorAutoSubscriptionRegistry`);使用容器接線(`SvcContainer`、`AddPicoMediator`)時需再加 `PicoDI` 與 `PicoMediator.DI`。

> **本機開發(ProjectReference):** analyzer 不隨 ProjectReference 鏈傳遞——工程消費者需直接引用 `PicoActor.Gen`(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`,鏡像 `tests/PicoActor.Tests`)。NuGet 消費者經 `PicoActor.Abs` 包的 `buildTransitive` props 自動注入生成器,無需額外引用。

事件以信封形式經 PicoMediator 在 persist+mutate 之後流出;replay 不重複發布。處理器失敗不影響 actor(逐處理器隔離)。事件→命令→事件的翻譯迴圈是預期用法——保持處理器冪等且有界。

> **破壞性變更:** 直連 `ISubscriber<TEvent>`(PicoMediator)訂閱者不再收到 PicoActor 領域事件。遷移到 `IDomainEventSubscriber<TEvent>`;信封的 `ActorId`/`Version` 取代任何手工內嵌的聚合 id。自訂 publisher(`AddPicoActor(IPublisher)`)現在收到的是 `DomainEventEnvelope` 實例而非裸事件——相應適配 `Publish<TEvent>` 實作(僅觀察到的載荷形狀變化;actor 管線不受影響)。 `IEventStoreEnumerator.ListAggregateIds(string)` 改為非同步 `ListAggregateIdsAsync(string)`——自訂列舉器需更新簽章。

注意:
- `Register<T>` 每個 actor 型別只能呼叫一次:重複註冊現在會拋出例外,而不是靜默取代先前的工廠。
- `StopAsync`/`RequestStop` 先從註冊表移除 actor,再排空 mailbox 中已緩衝的訊息(優雅停機);停機之後發送的訊息會拋出 `KeyNotFoundException`。
- **事件→命令翻譯是訂閱者(業務層)職責**——PicoActor 只發布;命令只能經 mailbox 進入 actor。
- 發布發生在 **persist+mutate 之後**——發布失敗不影響 actor 狀態(事件已落盤)。
- 恢復靜默:replay 不重複發布。
- 框架事件(`SagaCompleted`/`SagaFailed`)與其他事件一樣可型別化訂閱(Abs 目標 net10.0)。
- **自動接線任意 scope 安全**:PicoDI 2026.8.1(E1)起 Singleton 工廠使用容器內部根 scope——自動接線的 IMediator 存活至容器釋放。
- **切勿對發布中的聚合從其自身發布路徑呼叫 `AskAsync`**——源信箱正忙於 flush 事件,請求會自我死鎖。請查詢讀側投影(獨立 actor);`Send` 給源聚合是安全的(發後不理)。

---

## 設計哲學（克制 / 专注 / 优雅 / 高效）

| 原則 | 實踐 |
|-----------|------------|
| **克制 (Restraint / 節制)** | 無分散式共識，無監督樹——只有 Actor 和 Event。 |
| **专注 (Focus / 專注)** | 每個 Actor 單執行緒。一次一條訊息。 |
| **优雅 (Elegance / 優雅)** | Persist-then-Mutate：狀態僅在持久化後變更。復原自動完成。 |
| **高效 (Efficiency / 效率)** | AOT 相容、零反射、`net10.0` 抽象層。 |

---

## 使用場景

- **AI Agent 系統**——每個 AI Agent 是一個帶對話狀態的 Actor
- **工作流程編排**——Actor 建模長時間執行的業務流程
- **遊戲伺服器狀態**——事件溯源 Actor 管理玩家/遊戲狀態
- **IoT 裝置狀態**——記憶體 Actor + 定期快照

---

## 套件列表

| 套件 | 目標框架 | 描述 |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | 核心抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor`、`SagaActor`——另含訂閱型別（`IDomainEventSubscriber<TEvent>`、`DomainEventEnvelope`、`ICommandSender`）與內嵌 `PicoActor.Gen` 分析器（declare-and-subscribe） |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 執行時：`ActorSystem`、`InMemoryEventStore`、`MediatorDomainEventPublisher`（信封事件流出）、PicoDI 整合（`AddPicoActor` 自動接線 IMediator + ICommandSender） |

---

## 對比

| 特性 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| 純記憶體 | ✅ | ✅ | ✅ | ❌ |
| AOT / 修剪 | ✅ | ❌ | ❌ | ❌ |
| 事件溯源 | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 抽象層 | ❌ | ✅ | ✅ | ❌ |
| PicoDI 整合 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 分散式 / 叢集 | ❌ | ✅ | ✅ | ✅ |
| 單執行緒每 Actor | ✅ | ✅ | ✅ | ❌ |
| 套件數量 | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## 授權條款

MIT — 詳見 [LICENSE](LICENSE)。
