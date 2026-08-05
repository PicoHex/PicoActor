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

事件溯源 Actor 遵循**先持久化再變更（Persist-then-Mutate）**：
`OnMessageAsync → RaiseEvent → 持久化到 IEventStore → Mutate 狀態`。
狀態僅在持久化成功後變更——記憶體狀態始終與事件流保持一致。

---

## 為什麼選擇 PicoActor

| 關注點 | 現有方案 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 修剪 | ❌ Akka.NET、Proto.Actor、Orleans 皆依賴反射 | ✅ 完整 NativeAOT 支援 |
| 事件溯源 | ❌ Proto.Actor、Orleans 無內建 ES | ✅ Persist-then-Mutate，自動復原 |
| 依賴體積 | ❌ Akka.NET（8+ 套件）、Orleans（10+ 套件） | ✅ 2 個套件，除 Channels 外零依賴 |
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
| `IActorSystem` | 執行時契約——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | 命令標記介面 |
| `IDomainEvent` | 領域事件標記介面 |
| `IEventSourcedActor` | 可選介面——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 持久化契約——AppendAsync（樂觀併發）、LoadAsync |
| `ICancelable` | 可選——CancelCurrentTurn 用於長時間執行操作 |
| `Actor` | 抽象基底類別——信箱、消費迴圈、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基底類別——RaiseEvent、Mutate、Persist-then-Mutate 管線 |
| `Envelope` | 內部——包裝 ICommand 與可選 TaskCompletionSource |
| `ActorOutputEvent` | 出站通知——Type、Data、可選 ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | 版本不符時由 IEventStore 擲出 |

### PicoActor — 執行時

目標框架 `net10.0`，AOT 相容。

| 類型 | 角色 |
|------|------|
| `ActorSystem` | 預設 `IActorSystem`——ConcurrentDictionary 註冊表、工廠註冊、訊息路由、CancelTurn |
| `InMemoryEventStore` | 無鎖記憶體儲存——基於 ConcurrentDictionary |
| `ActorConfig` | 設定 POCO——可從 PicoCfg 綁定 |
| `ActorSystemOptions` | 選項——必填 EventStore、選用 Logger、選用 DomainEventPublisher;由 `ActorSystem` 建構函式使用 |
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

### CancelTurn

取消長時間執行操作而不停止 Actor：

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... 使用 _currentTurnCts.Token 的長時間工作
    }
}
system.CancelTurn(actor.Id);
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
}
```

### 事件流出(PicoMediator)

`IDomainEvent : IEvent`——領域事件是 PicoMediator 一級公民通知。persist+mutate 之後,框架經 `IDomainEventPublisher` 鉤子發布;replay(恢復)不重複發布。

`MediatorDomainEventPublisher` 是開箱即用介面卡:逐事件 `Publish<IDomainEvent>`(編譯期泛型,AOT 安全),逐事件隔離——單一訂閱者失敗不影響後續事件。

```csharp
// 訂閱者:宣告即訂閱——Gen 掃描自動註冊,零手動註冊。事件→命令的翻譯是業務層職責。
public sealed class OrderPaidSub : ISubscriber<OrderPaid>
{
    public ValueTask Handle(OrderPaid e, CancellationToken ct) { /* 翻譯成命令 */ return default; }
}

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

注意:
- **事件→命令翻譯是訂閱者(業務層)職責**——PicoActor 只發布;命令只能經 mailbox 進入 actor。
- 發布發生在 **persist+mutate 之後**——發布失敗不影響 actor 狀態(事件已落盤)。
- 恢復靜默:replay 不重複發布。
- **類型化訂閱(base-type bridge)**:介面卡 `Publish<IDomainEvent>`;生成的 bridge 路由到具體類型訂閱者。基類型聲明的訂閱者(`ISubscriber<IDomainEvent>`)也能收到基類型發布,但收不到具體類型發布。框架事件(`SagaCompleted`/`SagaFailed`)與其他事件一樣可類型化訂閱(Abs 目標 net10.0)。
- **自動接線任意 scope 安全**:PicoDI 2026.8.1(E1)起 Singleton 工廠使用容器內部根 scope——自動接線的 IMediator 存活至容器釋放。

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | 核心抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 執行時：`ActorSystem`、`InMemoryEventStore`、PicoDI 整合 |

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
| 套件數量 | 2 | 8+ | 3+ | 10+ |

---

## 授權條款

MIT — 詳見 [LICENSE](LICENSE)。
