# PicoActor

AOT互換のインメモリ Actor フレームワーク。Event Sourcing 対応。
軽量・ゼロリフレクション、AI エージェントシステムとワークフロー
オーケストレーション向け。NativeAOT およびトリミング環境で動作。

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## 計算モデル

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

各 Actor は**メールボックス**（インメモリ `Channel<Envelope>`）、**UUID v7**
ID、シングルスレッド消費ループを持ちます。コマンドは `Send`（fire-and-forget）
または `AskAsync`（リクエスト-リプライ）で配信されます。

PicoActor は**メッセージ駆動**です：すべての相互作用はメッセージです。コマンド（`ICommand`）はメールボックス経由で配信される宛先指定メッセージ（1:1、応答は任意）であり、ドメインイベント（`IDomainEvent`）は PicoMediator 経由で公開されるブロードキャストメッセージ（1:N、応答なし）です。イベントもメッセージです——集約をまたぐ協調パターンは単一のループのみ：イベント → サブスクライバー → 翻訳されたコマンド → メールボックス。これ以外にアクターと相互作用する方法はありません。

Event Sourcing Actor は**Persist-then-Mutate**（永続化してから変更）に従います：
`OnMessageAsync → RaiseEvent → IEventStore に永続化 → Mutate 状態`。
状態は永続化成功後にのみ変更されます——インメモリ状態は常にイベントストリームと一致します。

---

## なぜ PicoActor か

| 観点 | 既存の選択肢 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / トリミング | ❌ Akka.NET、Proto.Actor、Orleans はリフレクション必須 | ✅ 完全 NativeAOT 対応 |
| Event Sourcing | ❌ Proto.Actor、Orleans は ES 非内蔵 | ✅ Persist-then-Mutate、自動ロールバック |
| 依存サイズ | ❌ Akka.NET（8+ パッケージ）、Orleans（10+ パッケージ） | ✅ 1 パッケージ——PicoActor(`PicoActor.Abs` と PicoDI/PicoLog/PicoMediator 抽象を同梱);購読者を宣言する場合は PicoMediator、コンテナ配線時は PicoDI を追加 |
| DI 統合 | ❌ Microsoft.Extensions.DI に依存 | ✅ ネイティブ PicoDI、ゼロリフレクション |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor は一部のみ | ❌ net10.0 のみ(PicoMediator ランタイムは net10.0+ 必須) |
| 学習曲線 | ❌ 急峻——監視ツリー、クラスタリング、リモート | ✅ 最小限——Actor + Event + Mailbox |

---

## クイックスタート

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. セットアップ
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Actor ファクトリ登録
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. 作成、メッセージ送信、停止、再構築
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## モジュール詳細

### PicoActor.Abs — コア抽象

`net10.0` ターゲット(PicoMediator ランタイムと生成 bridge コードは net10.0+ 必須)。

| 型 | 役割 |
|------|------|
| `IActor` | 基本インターフェース——`Id`（UUID v7）を提供 |
| `IActorSystem` | ランタイム契約——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、RequestStop、StopAllAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | コマンド用マーカーインターフェース |
| `IDomainEvent` | ドメインイベント用マーカーインターフェース |
| `IEventSourcedActor` | オプショナル——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 永続化契約——AppendAsync（楽観的並行性）、LoadAsync、PeekFirstAsync |
| `Actor` | 抽象基底クラス——メールボックス、消費ループ、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基底——RaiseEvent、Mutate、Persist-then-Mutate パイプライン |
| `SagaActor` | 有限寿命 ES コーディネータ——フレームワーク終端イベント（SagaCompleted/SagaFailed）、自動停止、ResumeInterruptedSagasAsync による明示的バッチ復旧 |
| `IDomainEventSubscriber<TEvent>` | サブスクライバー契約——型付きエンベロープ + `ICommandSender`;PicoActor.Gen が自動登録（declare-and-subscribe） |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | コンテキストエンベロープ——`ActorId`、`Version`、`Event`（転送 / 型付き配信） |
| `ICommandSender` | ハンドラー用ナローポート——Send、AskAsync、ExecuteSaga |
| `Envelope` | 内部——ICommand とオプショナル TaskCompletionSource をラップ |
| `ActorOutputEvent` | 送信通知——Type、Data、オプショナル TurnId |
| `ConcurrencyException` | バージョン不一致時に IEventStore がスロー |

### PicoActor — ランタイム

`net10.0` ターゲット、AOT 互換。

| 型 | 役割 |
|------|------|
| `ActorSystem` | デフォルト `IActorSystem`——ConcurrentDictionary レジストリ、ファクトリ登録、メッセージルーティング |
| `InMemoryEventStore` | インメモリストア——ストリーム単位のロック、楽観的同時実行制御（ConcurrentDictionary ベース） |
| `ActorConfig` | 設定 POCO——PicoCfg からバインディング可能 |
| `ActorSystemOptions` | オプション——必須 EventStore、任意 Logger、任意 DomainEventPublisher;`ActorSystem` コンストラクタで使用 |
| `MediatorDomainEventPublisher` | デフォルトの `IDomainEventPublisher`——イベントごとに `DomainEventEnvelope` を発行、イベント単位の分離 |
| `PicoActorDiExtensions` | PicoDI 用 `AddPicoActor()` 拡張メソッド |

### Actor（非 ES）

`Actor` を継承して純粋なインメモリ運用型 Actor を作成します。

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Event Sourcing Actor

`EventSourcedActor` を継承します。`OnMessageAsync` で `RaiseEvent` を呼び、
`Mutate` で状態変更を適用します。

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

### Persist-then-Mutate パイプライン

```
OnMessageAsync → RaiseEvent（記録のみ、状態変更なし）
              → AppendAsync（IEventStore に永続化）
              → Mutate（イベントをインメモリ状態に適用）
              → 呼び出し元に返信（成功後のみ）
```

`AppendAsync` が失敗した場合、未コミットイベントは破棄され `Version` が
ロールバックされます。Actor は**毒化されません**——次のメッセージは正常に処理されます。

### SagaActor（有限ライフタイムのコーディネーター）

`SagaActor extends EventSourcedActor` —— ライフサイクルが有限の集約間操作向けです。メールボックスが永久に動作し続ける通常の EventSourcedActor と異なり、SagaActor はフレームワークが終端イベントを永続化した後**自動的に停止**します。

終端状態は**フレームワーク生成**です：`MarkComplete(result)` は `SagaCompleted(result)` イベントをビジネスイベントと同じバッチへアペンドし（アトミック —— 完了と永続化が乖離することはありません）、捕捉されなかったビジネス例外は `SagaFailed(reason)`（`"ExceptionType: message"`、512 文字に切り詰め）をアペンドし、呼び出し元に `SagaExecutionException(Id, Reason)` をスローします。サブクラスがこれらのイベントを raise したり handle したりすることはありません —— `Mutate` が見るのはビジネスイベントだけです。

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

**ライフサイクル：**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

ビジネス失敗：`OnMessageAsync`/`ResumeAsync` がスロー → フレームワークは未コミットのビジネスイベントを破棄し、`SagaFailed(reason)` をアペンドして自動停止し、Ask 呼び出し元に `SagaExecutionException(Id, Reason)` をフォールトとして返します。インフラ障害（ストア停止）は**終端ではありません** —— イベントはロールバックされ、saga は Running のままです。

**クラッシュリカバリ：** `GetAsync` がイベントをリプレイ → フレームワークが `SagaCompleted`/`SagaFailed` から `Completed`/`Failed` を復元します（終端済みの saga は決して復活しません —— `GetAsync` は null を返します）。終端イベントのない saga には `ResumeAsync()` が呼ばれ、そこで終端に達すればフレームワークが同一フラッシュバッチで `SagaCompleted` を永続化します。リカバリは明示的なプルであり、バックグラウンドの魔法ではありません。

**明示的な一括リカバリ：**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` は先頭イベントの型名で saga を列挙し、それぞれを `GetAsync` でシングルフライト復元した後、リカバリ後の分類を返します：`Completed` / `Failed`（フレームワークが復元した reason 付き）/ `Running`（外部入力待ち）。既に終端の saga は決して復活せず、生存中の saga はその場で分類されます。冪等で安全に再試行でき（id ごとにシングルフライト）、ストア障害時はフェイルファストで呼び出し元がバッチ全体を再試行できます。

**Process Manager パターン：**

同じ基底クラスが process manager もカバーします —— 外部イベントはアプリケーション層のイベントハンドラー（PicoMediator サブスクライバーなど）によってコマンドへ変換され、saga のメールボックスへ送られます。イベントが actor に直接入ることはなく、saga が見るのはコマンドだけです。

**便利 API：**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` は `CreateAsync` + `AskAsync` を 1 回の呼び出しで行います。成功すると `SagaExecution<TResult>(Id, Result)` を返し、ビジネス失敗時は `SagaExecutionException(SagaId, Reason)` をスローします —— 呼び出し元は常に saga id を取得できます。

### メッセージング: Ask vs Send

| パターン | メソッド | 意味 |
|---------|--------|-----------|
| リクエスト-リプライ | `AskAsync<TResult>(id, command)` | メッセージ処理後に結果を返す |
| Fire-and-Forget | `Send(id, command)` | 返信なし；例外は UnhandledErrorHandler にルーティング |

### OutputChannel

Actor は `ActorOutputEvent` メッセージを外部サブスクライバにブロードキャストできます：

```csharp
counter.OutputWriter = channel.Writer;
// OnMessageAsync 内：
WriteOutput("Incremented", data: "Delta=5");
```

### Spawn

Actor は `System` 参照を通じて子 Actor を作成できます：

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

## モジュール統合

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // IActorSystem + InMemoryEventStore 登録

// カスタムイベントストア
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// PicoCfg 経由
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` は **Singleton** として登録されます。`ILoggerFactory` が
登録されている場合、ロガーが自動的に注入されます。

### カスタム IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* 同時実行チェック付き INSERT */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* バージョン順 SELECT */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* 最初のイベント取得（復旧列挙） */ }
}
```

### イベント流出(PicoMediator)

`IDomainEvent : IEvent`——ドメインイベントは PicoMediator の一級市民通知。persist+mutate の後、フレームワークは**コンテキストエンベロープ**として `IDomainEventPublisher` フック経由で公開します;replay(リカバリ)は再公開しません。

`MediatorDomainEventPublisher` はすぐ使えるアダプタ:各イベントを `DomainEventEnvelope(actorId, version, event)` にラップしてイベントごとに `Publish<DomainEventEnvelope>`(コンパイル時ジェネリック、AOT 安全)、イベントごとに分離——1 つのサブスクライバ失敗が後続イベントを妨げません。

### ドメインイベントの購読(declare-and-subscribe)

イベントハンドラーは `IDomainEventSubscriber<TEvent>` を実装するプレーンなクラスです——PicoActor.Gen(PicoActor.Abs に内蔵)がスキャンして自動登録、手動配線ゼロ。ハンドラーはソース集約コンテキスト(`ActorId`、`Version`)を運ぶ型付きエンベロープと、狭いポート `ICommandSender` を受け取ります:

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

// 配線:AddPicoMediator が IMediator を登録;AddPicoActor() が ActorSystem ファクトリ内で
// 自動検出してイベント流出を配線(遅延解決、Scoped ライフサイクルと互換)。
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe:サブスクライバ自動登録
container.AddPicoActor();     // MediatorDomainEventPublisher を自動配線
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// カスタム publisher の明示的配線:AddPicoActor(IPublisher)(インスタンスは Build() 前に必要)。
```

> **必要なパッケージ:** 追加が必要なのは `PicoActor` のみです(`PicoActor.Abs` と `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs` を同梱)。`IDomainEventSubscriber<TEvent>` を宣言する場合は `PicoMediator` を追加してください(生成されるブリッジは同パッケージの `MediatorAutoSubscriptionRegistry` を呼び出します)。コンテナ配線(`SvcContainer`、`AddPicoMediator`)を使う場合は `PicoDI` と `PicoMediator.DI` を追加します。

> **ローカル開発(ProjectReference):** analyzer は ProjectReference チェーンを伝播しません——プロジェクト消費者は `PicoActor.Gen` への直接参照が必要です(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`、`tests/PicoActor.Tests` をミラー)。NuGet 消費者は `PicoActor.Abs` パッケージの `buildTransitive` props 経由で生成器を自動取得——追加参照不要。

イベントは persist+mutate の後にエンベロープとして PicoMediator を経由して流出します;replay は決して公開しません。ハンドラー失敗はアクターに影響しません(ハンドラー単位の分離)。イベント→コマンド→イベントの翻訳ループは意図された使い方です——ハンドラーを冪等かつ有界に保ってください。

> **破壊的変更:** 直接の `ISubscriber<TEvent>`(PicoMediator)サブスクライバは PicoActor ドメインイベントを受信しなくなります。`IDomainEventSubscriber<TEvent>` に移行してください;エンベロープの `ActorId`/`Version` が手動で埋め込んだ集約 id を置き換えます。カスタム publisher(`AddPicoActor(IPublisher)`)は現在、生イベントの代わりに `DomainEventEnvelope` インスタンスを受信します——`Publish<TEvent>` 実装をそれに合わせて適応してください(観測されるペイロード形状のみ変化;アクターパイプラインは影響なし)。 `IEventStoreEnumerator.ListAggregateIds(string)` は非同期の `ListAggregateIdsAsync(string)` になりました——独自の列挙子はシグネチャを更新してください。

注意:
- `Register<T>` は actor 型ごとに 1 回だけ呼び出します:重複登録は最初のファクトリを黙って置き換えず、例外をスローします。
- `StopAsync`/`RequestStop` はまずレジストリから actor を削除し、その後メールボックスに既にバッファされたメッセージを排出します(グレースフル停止)。停止後に送信したメッセージは `KeyNotFoundException` になります。
- **イベント→コマンド変換はサブスクライバ(業務層)の責務**——PicoActor は公開のみ;コマンドは mailbox 経由でのみ actor に入ります。
- 公開は **persist+mutate の後**——公開失敗は actor 状態に影響しません(イベントは永続化済み)。
- リカバリは静粛:replay は再公開しません。
- フレームワークイベント(`SagaCompleted`/`SagaFailed`)は他のイベントと同様に型付きサブスクリプション可能(Abs は net10.0 ターゲット)。
- **自動配線は任意の scope から安全**:PicoDI 2026.8.1(E1)以降、Singleton ファクトリはコンテナ内部ルート scope を使用——自動配線された IMediator はコンテナ破棄まで生存します。
- **公開中の集約に対して自身の公開パスから `AskAsync` を呼ばないでください**——ソース mailbox はイベントの flush で忙しく、リクエストが自己デッドロックします。読み取り側プロジェクション(別の actor)を照会してください;ソース集約への `Send` は安全です(発火後忘却)。

---

## 設計哲学（克制 / 专注 / 优雅 / 高效）

| 原則 | 実践 |
|-----------|------------|
| **克制 (Restraint / 抑制)** | 分散コンセンサスなし、監視ツリーなし——Actor と Event だけ。 |
| **专注 (Focus / 集中)** | 各 Actor シングルスレッド。一度に一つのメッセージ。 |
| **优雅 (Elegance / 優雅)** | Persist-then-Mutate：永続化成功後にのみ状態変更。ロールバックは自動。 |
| **高效 (Efficiency / 効率)** | AOT 互換、ゼロリフレクション、`net10.0` 抽象層. |

---

## ユースケース

- **AI エージェントシステム**——各 AI エージェントが会話状態を持つ Actor
- **ワークフローオーケストレーション**——長時間実行ビジネスプロセスを Actor でモデル化
- **ゲームサーバー状態**——プレイヤー/ゲーム状態を Event Sourcing Actor で管理
- **IoT デバイス状態**——定期スナップショット付きインメモリ Actor

---

## パッケージ

| パッケージ | ターゲット | 説明 |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | コア抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor`、`SagaActor`——さらにサブスクリプション型（`IDomainEventSubscriber<TEvent>`、`DomainEventEnvelope`、`ICommandSender`）と内蔵 `PicoActor.Gen` アナライザー（declare-and-subscribe） |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | ランタイム：`ActorSystem`、`InMemoryEventStore`、`MediatorDomainEventPublisher`（エンベロープイベント流出）、PicoDI 統合（`AddPicoActor` が IMediator + ICommandSender を自動配線） |

---

## 比較

| 機能 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| インメモリのみ | ✅ | ✅ | ✅ | ❌ |
| AOT / トリミング | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 抽象層 | ❌ | ✅ | ✅ | ❌ |
| PicoDI 統合 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 分散 / クラスタリング | ❌ | ✅ | ✅ | ✅ |
| シングルスレッド/Actor | ✅ | ✅ | ✅ | ❌ |
| パッケージ数 | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## ライセンス

MIT — [LICENSE](LICENSE) 参照。
