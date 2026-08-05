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

Event Sourcing Actor は**Persist-then-Mutate**（永続化してから変更）に従います：
`OnMessageAsync → RaiseEvent → IEventStore に永続化 → Mutate 状態`。
状態は永続化成功後にのみ変更されます——インメモリ状態は常にイベントストリームと一致します。

---

## なぜ PicoActor か

| 観点 | 既存の選択肢 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / トリミング | ❌ Akka.NET、Proto.Actor、Orleans はリフレクション必須 | ✅ 完全 NativeAOT 対応 |
| Event Sourcing | ❌ Proto.Actor、Orleans は ES 非内蔵 | ✅ Persist-then-Mutate、自動ロールバック |
| 依存サイズ | ❌ Akka.NET（8+ パッケージ）、Orleans（10+ パッケージ） | ✅ 2 パッケージ、Channels 以外ゼロ依存 |
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
| `IActorSystem` | ランタイム契約——Register、CreateAsync、FindAggregateIds、GetAsync、Send、AskAsync、StopAsync、ExecuteSaga、ResumeInterruptedSagasAsync |
| `ICommand` | コマンド用マーカーインターフェース |
| `IDomainEvent` | ドメインイベント用マーカーインターフェース |
| `IEventSourcedActor` | オプショナル——Version、ReplayEvents、CommitEvents |
| `IEventStore` | 永続化契約——AppendAsync（楽観的並行性）、LoadAsync |
| `ICancelable` | オプショナル——長時間実行操作用 CancelCurrentTurn |
| `Actor` | 抽象基底クラス——メールボックス、消費ループ、SignalReady、StopAsync |
| `EventSourcedActor` | ES 基底——RaiseEvent、Mutate、Persist-then-Mutate パイプライン |
| `Envelope` | 内部——ICommand とオプショナル TaskCompletionSource をラップ |
| `ActorOutputEvent` | 送信通知——Type、Data、オプショナル ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | バージョン不一致時に IEventStore がスロー |

### PicoActor — ランタイム

`net10.0` ターゲット、AOT 互換。

| 型 | 役割 |
|------|------|
| `ActorSystem` | デフォルト `IActorSystem`——ConcurrentDictionary レジストリ、ファクトリ登録、メッセージルーティング、CancelTurn |
| `InMemoryEventStore` | ロックフリーインメモリストア——ConcurrentDictionary ベース |
| `ActorConfig` | 設定 POCO——PicoCfg からバインディング可能 |
| `ActorSystemOptions` | オプション——必須 EventStore、任意 Logger、任意 DomainEventPublisher;`ActorSystem` コンストラクタで使用 |
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

### CancelTurn

Actor を停止せずに長時間実行操作をキャンセルします：

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... _currentTurnCts.Token を使用した長時間処理
    }
}
system.CancelTurn(actor.Id);
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
}
```

### イベント流出(PicoMediator)

`IDomainEvent : IEvent`——ドメインイベントは PicoMediator の一級市民通知。persist+mutate の後、フレームワークは `IDomainEventPublisher` フック経由で公開します;replay(リカバリ)は再公開しません。

`MediatorDomainEventPublisher` はすぐ使えるアダプタ:イベントごとに `Publish<IDomainEvent>`(コンパイル時ジェネリック、AOT 安全)、イベントごとに分離——1 つのサブスクライバ失敗が後続イベントを妨げません。

```csharp
// サブスクライバ:宣言即登録——Gen スキャンで自動登録、手動登録ゼロ。イベント→コマンド変換は業務層の責務。
public sealed class OrderPaidSub : ISubscriber<OrderPaid>
{
    public ValueTask Handle(OrderPaid e, CancellationToken ct) { /* コマンドに変換 */ return default; }
}

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

注意:
- **イベント→コマンド変換はサブスクライバ(業務層)の責務**——PicoActor は公開のみ;コマンドは mailbox 経由でのみ actor に入ります。
- 公開は **persist+mutate の後**——公開失敗は actor 状態に影響しません(イベントは永続化済み)。
- リカバリは静粛:replay は再公開しません。
- **型付きサブスクリプション(base-type bridge)**:アダプタは `Publish<IDomainEvent>`;生成された bridge が具象型サブスクライバへルーティングします。ベース型宣言のサブスクライバ(`ISubscriber<IDomainEvent>`)もベース型パブリッシュを受信しますが、具象型パブリッシュは受信しません。フレームワークイベント(`SagaCompleted`/`SagaFailed`)は他のイベントと同様に型付きサブスクリプション可能(Abs は net10.0 ターゲット)。
- **自動配線は任意の scope から安全**:PicoDI 2026.8.1(E1)以降、Singleton ファクトリはコンテナ内部ルート scope を使用——自動配線された IMediator はコンテナ破棄まで生存します。

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | コア抽象：`IActor`、`IActorSystem`、`ICommand`、`IDomainEvent`、`IEventStore`、`Actor`、`EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | ランタイム：`ActorSystem`、`InMemoryEventStore`、PicoDI 統合 |

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
| パッケージ数 | 2 | 8+ | 3+ | 10+ |

---

## ライセンス

MIT — [LICENSE](LICENSE) 参照。
