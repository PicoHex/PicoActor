<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

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
dotnet add package PicoActor.EventStore.InMemory   # 儲存已是獨立套件(v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

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
| `InMemoryEventStore` | 已移入 **`PicoActor.EventStore.InMemory`**（v2026.9）——記憶體儲存：每串流加鎖，樂觀並行（基於 ConcurrentDictionary）；用 `AddPicoActorInMemoryStore()` 註冊 |
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
`Mutate` 必須是純函式且不得拋出例外;一旦拋出,批次已落盤、狀態不可信——actor 會被**停止**(移出 registry),而不是帶著不可信狀態繼續執行。

### Snapshot Acceleration (opt-in)（快照加速，可選）

對於長事件流，每次重建都重放全部事件，這就是復原成本的來源。
快照（snapshot）是 Actor 每隔 N 個已持久化事件儲存的一份不透明載荷，
重建時先還原它，然後只重放尾部事件。快照**僅用於加速**：
事件流永遠不會被截斷，每個聚合最多保留一份快照，
快照缺失、損壞或版本超出範圍時會回退到完整重放。

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

- 觸發條件：每 N 個**已持久化**事件（按事件計數，而非按 flush 計數）。
- 線上格式：`IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`；
  `version` 等於該載荷覆蓋的事件數（`version == event count`）。
- 失敗永不影響命令：`TakeSnapshot` 拋出例外、存放區失敗，
  或 `RestoreSnapshot` 拋出例外，都會透過 `OnSnapshotFailed` /
  一則警告日誌上報，復原則回退為重放整個事件流。
- `IsReplaying` 是 `protected`：用 `if (IsReplaying) return;` 覆寫由
  `Mutate` 驅動的讀模型發佈邏輯，避免每個重放事件都做 O(n) 的工作
  （否則一次重建就是 O(n²)）。
- Schema 標籤：覆寫 `SnapshotSchema`（預設 `null`），回傳一個版本字串，例如
  `"CounterState/v1"`；**只要載荷的佈局或其含義改變就必須遞增它**。
  變更它會讓已存的快照作廢一次——也就是一次完整重放——等該聚合再落盤
  `SnapshotEveryNEvents` 個事件後恢復正常速度。它必須是**型別層級常數**：
  若寫成執行個體的取值，且寫入執行個體與重建執行個體取值不同，每次復原都會靜默退化為完整重放。
- 標籤非 `null` 時，框架會把載荷包進信封，並在還原時比對標籤；不一致
  （或載荷沒有信封、而 Actor 宣告了標籤）就丟棄該快照並重放整個事件流。
  只有 `null` 是關閉開關——`""` 也算真實標籤。標籤超過 255 個 UTF-8 位元組時
  乾脆不寫快照（透過 `OnSnapshotFailed` 上報）。
  存放區必須始終把載荷當作**不透明位元組**：不要解析它，也不要假設它等於
  `TakeSnapshot()` 的輸出。

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader)（事件日誌讀取面）

業務程式碼只面向 `IActorSystem`，拿到的是 Actor——沒有任何面向業務的成員會回傳事件集合。
`IEventStore` 是**存放區作者**的持久化 SPI：把追加的事件落盤、按需取回，
它的 `LoadAsync` 保留為執行時重建的回退讀取。工具與投影改用 `IEventLogReader` 讀取——
`GetVersionAsync`、`OpenAsync`、`PeekFirstAsync`——其中 `OpenAsync` 回傳一個前向游標：

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- 單次觀察：`End` 是 `OpenAsync` 時取得的串流長度，在游標的整個生命週期內不再變動，
  因此回滾護欄與尾端長度來自同一次開啟，游標產出的數量永不超過 `End`。
- 能力探索只有一條規則——`eventStore is IEventLogReader`。實作了讀取面的存放區，
  讓帶快照的重建只讀快照之後的尾端（檔案存放區可以只分類頭部、只反序列化尾端）；
  沒有實作讀取面的存放區繼續走 `LoadAsync`，即讀出並反序列化整條事件串流。
- `InMemoryEventStore` 已實作 `IEventLogReader`；對其它存放區而言，實作它是可選的、純增量的。

### JSONL Event Store (opt-in)（JSONL 事件儲存，可選）

`PicoActor` 只負責執行 Actor,本身不再內建儲存。提供儲存的是兩個套件:
`PicoActor.EventStore.InMemory`(記憶體預設實作,用 `AddPicoActorInMemoryStore()` 註冊)
與 `PicoActor.EventStore.Jsonl`(持久化,本節即講它)。

`PicoActor.EventStore.Jsonl` 為每個聚合保存一個只追加的 JSONL 檔案
(`<root>/<aggregateId>.jsonl`,一行一個 JSON 物件),快照作為 sidecar 原子地寫入同級的
`<root>-snapshots` 目錄。它實作 `IEventStore`、`IEventLogReader`、`IEventStoreSnapshot`
與 `IEventStoreEnumerator`,AOT 安全(PicoJetson 源生成),並且是**單一程序**的:同一目錄上的
兩個 store 實例不共用門閂,並行寫入會直接報錯,而不是撕裂事件流。

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

- **漏寫註冊在建置期就暴露,而不是首次寫入時才爆**:套件內嵌了 `PICOSTORE001` 分析器,
  會標出沒有任何 `[PicoDerivedType]` 列出的領域事件(只用記憶體儲存的專案不為它付出代價)。
- **撕裂尾部與損壞是兩件事。** 未以換行結尾的最後一行——程序崩潰唯一會留下的形狀——被忽略,
  既不計入也不拋出;下一次追加會把它截斷。而**已終止**卻無法反序列化的行屬於損壞,會拋出
  (包括最後一行),儲存不會去修復它,因為修復就是猜。這就是把 `FlushToDisk` 留在預設
  `false` 的書面代價:位元組到達作業系統(另一個讀取者立刻能看到該事件)卻沒有落盤,
  掉電就可能留下一個已終止的垃圾行。設 `FlushToDisk = true` 即每次追加都 fsync。
- **游標與僅尾部重建。** `GetVersionAsync`、游標的 `End` 及其位元組定位來自同一次廉價掃描
  (完整、非空白、已終止的行——不校驗 JSON,不經過反序列化器),因此帶快照的重建從
  `snapshot.Version + 1` 開啟,只反序列化尾部(`N − v` 個事件)。省下的是反序列化與配置,
  而不是磁碟 I/O:頭部仍要被讀一次(只讀位元組)。不需要序列化層的「跳過」探針——通用事件流裡
  只有事件,讀不出來的行就是損壞,不是外來記錄。
- `MaxAggregateLocks`(預設 `1024`)為每聚合門閂表設上界;被淘汰的條目只會讓該聚合的下一次
  追加多掃一遍,不影響正確性。`Logger` 獨立於 DI 的 `ILoggerFactory`:要看到儲存的告警,
  就把同一個 logger 顯式傳進來。
- 本次發佈不含:多程序寫入、歸檔/保留/壓縮、外來(非事件)行、加密/壓縮,以及框架事件的
  第二套編解碼器。

#### Stream naming（串流檔案命名，`FileNameFormat`）

`JsonlEventStoreOptions.FileNameFormat` 決定串流**檔案如何命名**;兩種模式的列格式完全一致，
只有名稱不同，因此既有目錄始終可讀。

| 取值 | 串流檔案 | 換來什麼 |
|---|---|---|
| `IdOnly`（預設） | `<id:D>.jsonl` | 歷史佈局，逐位元組重現——無需遷移，既有目錄毫無變化 |
| `TypePrefix` | `<type>-<id:D>.jsonl` | 型別寫進了檔名，`ListAggregateIdsAsync` 僅憑檔名即可還原 id：每次列舉**零次檔案開啟** |

`{type}` 是 `events[0].GetType().Name`，即這條串流**第一筆落盤事件**的具體型別（泛型保留其反引號
參數個數），並且在**建立時固定**：後續批次的第一筆事件若是別的型別，仍然附加到這條串流出生時的名稱上。

- **遷移到 `TypePrefix`。** PicoAgent 本來就把串流命名成 `{type}-{id}.jsonl`，所以把本儲存指向這樣的
  目錄並設 `FileNameFormat = TypePrefix` **完全不需要重新命名**——目錄原樣即可讀，快照也原樣沿用
  （快照以聚合 id 為鍵，`<id:D>.snap`，從不以串流名稱為鍵）。反向不提供：前綴是名稱的一部分，
  `IdOnly` 儲存無法從 id 推導出來，只能把每個檔案都改名。
- **混合目錄只診斷，不合併。** 僅在**另一種**佈局下合法的項目會計數，並且每次掃描只報
  **一條** Warning（偵測是雙向對稱的：`TypePrefix` 下的 `<id>.jsonl` 與 `IdOnly` 下的
  `<type>-<id>.jsonl` 都算「外來格式」）。若目錄裡**設定格式**的名稱為零、**另一種格式**的名稱
  有若干個，則擲出 `InvalidOperationException`，訊息同時給出兩側計數、兩種格式與兩條出路——
  列舉、建構、lazy pass 一律如此——而不是回一個空的系統。屬於另一個**事件型別**的串流不算混合：
  那是常態，不計數也不寫日誌。對某個 id 的附加，若它在另一佈局下已有非空串流——或已存在兩個
  不同的前綴——則在寫出任何一個位元組之前就被拒絕。
- **成本。** `TypePrefix` 在建構時付一次檔名掃描（建立 id → 型別的對映），每次解析失配再付一次，
  每次附加為重複防護付一次 `stat`。`IdOnly` 在建構時**零成本**，對已存在的串流附加也是零成本；它唯一
  的一次目錄掃描是惰性的——在首次觀察到一條自己沒有的串流時執行，且只執行一次；掃描失敗會被 latch
  並在下次重新擲出，因此錯配的根目錄不會悄悄退化成「空聚合」。空批次在兩種模式下都不寫、不建檔案，
  但依然校驗 `expectedVersion`。

### Idle Passivation (opt-in)（閒置鈍化，可選）

Actor 會一直駐留記憶體，直到被停止。對於擁有大量長期存活聚合的工作負載，
一段閒置預算可以回收那些能夠重建的 Actor 的具體化狀態：

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- 候選物件**僅限可重建的事件溯源 Actor**：即具體型別是以
  **重建工廠**註冊過的 `EventSourcedActor`。非 ES Actor 永遠不會被觸碰
  （它們的狀態無法重建）。
- 候選物件必須已初始化、未在處理命令、信箱為空，
  並且在整個預算時間內都處於閒置。停止操作以框架哨兵的形式
  穿過信箱，因此落在訊息邊界上；停止請求會做所有權校驗，
  所以過期的請求無法移除同一 id 的較新實例。
- `SagaActor` 豁免（`CanBePassivated => false`）：等待外部回應
  看起來就像閒置，回收它會丟掉那個回應。
- 重新啟用是顯式的：`GetAsync<T>(id)` 重放事件流（或還原快照
  並重放尾部）。在此之前 `Send`/`AskAsync` 的行為與任何未註冊 id
  完全一致（`KeyNotFoundException`）——`AskAsync` 沒有型別參數，
  因此無法選擇重建工廠。
- 可觀測性：`SweepIdleActorsAsync()`、`PassivatedCount`、`ReactivatedCount`，
  以及每次重建帶重放事件數的一行日誌。

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
若 `SagaFailed` 本身落盤失敗(store 仍不可用),則不產生終態,向呼叫方傳播**原始例外**。

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
| 發後不理 | `Send(id, command)` | 無回覆；失敗路由到 UnhandledErrorHandler(無 logger 時寫 stderr);actor 不存在或 mailbox 已關閉(停機競態)時拋出例外 |

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
container.AddPicoActor();               // 註冊 IActorSystem——不再註冊儲存
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory:記憶體預設實作

// 自訂事件儲存
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// 透過 PicoCfg 設定
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`AddPicoActor()` 不再註冊任何儲存,但註冊時不會失敗:只在解析 `IActorSystem` 時拋出
`InvalidOperationException`,訊息裡點名 `AddPicoActorInMemoryStore()`(註冊順序因此仍然自由,
顯式 `AddPicoActor(store)` 依舊優先)。

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

handler 可以是 class 或 record;無可存取(public/internal)實例建構子的實作會在建置期以 **PICA001** 報錯,而不是產生無法編譯的註冊程式碼。

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

> **必需套件:** 需要加入的只有 `PicoActor`(自帶 `PicoActor.Abs` 與 `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator` 抽象)。宣告 `IDomainEventSubscriber<TEvent>` 處理器無需額外加入套件——`PicoMediator` 本身就是 `PicoActor` 的相依套件(生成的橋接碼呼叫它的 `MediatorAutoSubscriptionRegistry`);使用容器接線(`SvcContainer`、`AddPicoMediator`)時需再加 `PicoDI` 與 `PicoMediator.DI`。

> **本機開發(ProjectReference):** analyzer 不隨 ProjectReference 鏈傳遞——工程消費者需直接引用 `PicoActor.Gen`(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`,鏡像 `tests/PicoActor.Tests`)。NuGet 消費者經 `PicoActor.Abs` 包的 `buildTransitive` props 自動注入生成器,無需額外引用。

事件以信封形式經 PicoMediator 在 persist+mutate 之後流出;replay 不重複發布。處理器失敗不影響 actor(逐處理器隔離)。事件→命令→事件的翻譯迴圈是預期用法——保持處理器冪等且有界。

> **破壞性變更:** 直連 `ISubscriber<TEvent>`(PicoMediator)訂閱者不再收到 PicoActor 領域事件。遷移到 `IDomainEventSubscriber<TEvent>`;信封的 `ActorId`/`Version` 取代任何手工內嵌的聚合 id。自訂 publisher(`AddPicoActor(IPublisher)`)現在收到的是 `DomainEventEnvelope` 實例而非裸事件——相應適配 `Publish<TEvent>` 實作(僅觀察到的載荷形狀變化;actor 管線不受影響)。 `IEventStoreEnumerator.ListAggregateIds(string)` 改為非同步 `ListAggregateIdsAsync(string)`——自訂列舉器需更新簽章。

注意:
- `Register<T>` 每個 actor 型別只能呼叫一次:重複註冊現在會拋出例外,而不是靜默取代先前的工廠。
- `Register<T>` 會拒絕執行階段無法服務的型別:不衍生自 `Actor` 的實作,以及不衍生自 `EventSourcedActor` 的 `IEventSourcedActor`(持久化只為具體基底類別接線)。
- `StopAsync`/`RequestStop` 先從註冊表移除 actor,再排空 mailbox 中已緩衝的訊息(優雅停機);停機之後發送的訊息會拋出 `KeyNotFoundException`(若 mailbox 已關閉則為 `InvalidOperationException`——停機競態窗口)。
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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 執行時：`ActorSystem`、`MediatorDomainEventPublisher`（信封事件流出）、PicoDI 整合（`AddPicoActor` 自動接線 IMediator + ICommandSender） |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | 記憶體儲存（`InMemoryEventStore`，v2026.9 從 `PicoActor` 移出）+ `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | 持久化 JSONL 事件儲存：只追加、樂觀並行、經 `IEventLogReader` 的僅尾部重建、原子快照 sidecar、AOT 安全（PicoJetson）——另含內嵌 `PICOSTORE001` 註冊分析器 |

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
