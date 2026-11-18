<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

# PicoActor

AOT 호환 인메모리 Actor 프레임워크, Event Sourcing 지원.
경량, 제로 리플렉션, AI 에이전트 시스템 및 워크플로우 오케스트레이션용.
NativeAOT 및 트리밍 환경에서 실행 가능.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## 계산 모델

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

각 Actor는 **메일박스**(인메모리 `Channel<Envelope>`), **UUID v7** ID,
단일 스레드 소비 루프를 가집니다. 명령은 `Send`(fire-and-forget) 또는
`AskAsync`(요청-응답)로 전달됩니다.

PicoActor는 **메시지 기반**입니다: 모든 상호작용은 메시지입니다. 명령(`ICommand`)은 mailbox를 통해 전달되는 지시형 메시지(1:1, 응답 선택적)이며, 도메인 이벤트(`IDomainEvent`)는 PicoMediator를 통해 게시되는 브로드캐스트 메시지(1:N, 응답 없음)입니다. 이벤트도 메시지입니다 — 애그리거트 간 협업 패턴은 단 하나의 루프입니다: 이벤트 → 구독자 → 번역된 명령 → mailbox. 액터와 상호작용하는 다른 방법은 없습니다.

Event Sourcing Actor는 **Persist-then-Mutate**(영속화 후 변경)를 따릅니다:
`OnMessageAsync → RaiseEvent → IEventStore에 영속화 → Mutate 상태`.
상태는 영속화 성공 후에만 변경됩니다——인메모리 상태는 항상 이벤트 스트림과 일치합니다.

---

## 왜 PicoActor인가

| 관점 | 기존 옵션 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 트리밍 | ❌ Akka.NET, Proto.Actor, Orleans는 리플렉션 필요 | ✅ 완전 NativeAOT 지원 |
| Event Sourcing | ❌ Proto.Actor, Orleans는 ES 미내장 | ✅ Persist-then-Mutate, 자동 롤백 |
| 의존성 크기 | ❌ Akka.NET(8+ 패키지), Orleans(10+ 패키지) | ✅ 패키지 1개 — PicoActor(PicoActor.Abs 및 PicoDI/PicoLog/PicoMediator 추상 포함); 구독자를 선언할 때 PicoMediator, 컨테이너 배선 시 PicoDI 추가 |
| DI 통합 | ❌ Microsoft.Extensions.DI에 종속 | ✅ 네이티브 PicoDI, 제로 리플렉션 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 일부만 지원 | ❌ net10.0 전용(PicoMediator 런타임은 net10.0+ 필요) |
| 학습 곡선 | ❌ 가파름——감독 트리, 클러스터링, 리모팅 | ✅ 최소——Actor + Event + Mailbox |

---

## 빠른 시작

```bash
dotnet add package PicoActor
dotnet add package PicoActor.EventStore.InMemory   # 저장소는 독립 패키지(v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

// 1. 설정
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Actor 팩토리 등록
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. 생성, 메시지 전송, 중지, 재구축
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## 모듈 상세

### PicoActor.Abs — 핵심 추상화

`net10.0` 타겟(PicoMediator 런타임과 생성된 bridge 코드는 net10.0+ 필요).

| 타입 | 역할 |
|------|------|
| `IActor` | 기본 인터페이스——`Id`(UUID v7) 제공 |
| `IActorSystem` | 런타임 계약——Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, RequestStop, StopAllAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | 명령용 마커 인터페이스 |
| `IDomainEvent` | 도메인 이벤트용 마커 인터페이스 |
| `IEventSourcedActor` | 선택적——Version, ReplayEvents, CommitEvents |
| `IEventStore` | 영속화 계약——AppendAsync(낙관적 동시성), LoadAsync, PeekFirstAsync |
| `Actor` | 추상 기본 클래스——메일박스, 소비 루프, SignalReady, StopAsync |
| `EventSourcedActor` | ES 기본——RaiseEvent, Mutate, Persist-then-Mutate 파이프라인 |
| `SagaActor` | 유한 수명 ES 코디네이터 — 프레임워크 터미널 이벤트(SagaCompleted/SagaFailed), 자동 중지, ResumeInterruptedSagasAsync를 통한 명시적 일괄 복구 |
| `IDomainEventSubscriber<TEvent>` | 구독자 계약 — 타입화된 엔벨로프 + `ICommandSender`; PicoActor.Gen이 자동 등록(declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | 컨텍스트 엔벨로프 — `ActorId`, `Version`, `Event`(전송 / 타입화된 전달) |
| `ICommandSender` | 핸들러용 좁은 명령 포트 — Send, AskAsync, ExecuteSaga |
| `Envelope` | 내부——ICommand를 선택적 TaskCompletionSource로 래핑 |
| `ActorOutputEvent` | 발신 알림——Type, Data, 선택적 TurnId |
| `ConcurrencyException` | 버전 불일치 시 IEventStore가 발생 |

### PicoActor — 런타임

`net10.0` 타겟, AOT 호환.

| 타입 | 역할 |
|------|------|
| `ActorSystem` | 기본 `IActorSystem`——ConcurrentDictionary 레지스트리, 팩토리 등록, 메시지 라우팅 |
| `InMemoryEventStore` | **`PicoActor.EventStore.InMemory`**로 이동(v2026.9)——인메모리 저장소: 스트림 단위 락, 낙관적 동시성(ConcurrentDictionary 기반); `AddPicoActorInMemoryStore()`로 등록 |
| `ActorConfig` | 설정 POCO——PicoCfg에서 바인딩 가능 |
| `ActorSystemOptions` | 옵션 — 필수 EventStore, 선택 Logger, 선택 DomainEventPublisher;`ActorSystem` 생성자에서 사용 |
| `MediatorDomainEventPublisher` | 기본 `IDomainEventPublisher` — 이벤트별로 `DomainEventEnvelope` 게시, 이벤트별 격리 |
| `PicoActorDiExtensions` | PicoDI용 `AddPicoActor()` 확장 메서드 |

### Actor (비-ES)

`Actor`를 상속하여 순수 인메모리 운영형 Actor를 만듭니다.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Event Sourcing Actor

`EventSourcedActor`를 상속합니다. `OnMessageAsync`에서 `RaiseEvent`를 호출하고,
`Mutate`에서 상태 변경을 적용합니다.

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

### Persist-then-Mutate 파이프라인

```
OnMessageAsync → RaiseEvent(기록만, 상태 변경 없음)
              → AppendAsync(IEventStore에 영속화)
              → Mutate(이벤트를 인메모리 상태에 적용)
              → 호출자에게 응답(성공 후에만)
```

`AppendAsync`가 실패하면 미커밋 이벤트는 폐기되고 `Version`이
롤백됩니다. Actor는 **오염되지 않습니다**——다음 메시지가 정상 처리됩니다.
`Mutate`는 순수 함수여야 하며 예외를 던지면 안 됩니다. 던지면 배치는 이미 영속화되어 상태를 신뢰할 수 없으므로, actor는 **중지**되고(레지스트리에서 제거) 신뢰할 수 없는 상태로 계속 실행되지 않습니다.

### Snapshot Acceleration (opt-in)(스냅샷 가속, 선택 사항)

긴 스트림에서는 재구성할 때마다 모든 이벤트를 리플레이하는 것이 복구 비용이 됩니다.
스냅샷은 액터가 N개의 영속화된 이벤트마다 저장하는 불투명한 페이로드이며,
재구성 시 이를 복원하고 꼬리 부분만 리플레이합니다. 스냅샷은 **오직 가속**을 위한
것입니다: 이벤트 스트림은 절대 잘리지 않고, 애그리거트당 최대 하나의 스냅샷만
유지되며, 스냅샷이 없거나 손상되었거나 범위를 벗어나면 전체 리플레이로
폴백합니다.

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

- 트리거: **영속화된** 이벤트 N개마다(플러시 단위가 아니라 이벤트 단위로 카운트).
- 와이어 포맷: `IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`;
  `version`은 페이로드가 포괄하는 이벤트 수와 같습니다(`version == event count`).
- 실패는 명령에 절대 영향을 주지 않습니다: `TakeSnapshot`에서 예외가 발생하거나,
  스토어가 실패하거나, `RestoreSnapshot`에서 예외가 발생하면 `OnSnapshotFailed` /
  경고 로그로 보고되고, 복구는 전체 스트림 리플레이로 폴백합니다.
- `IsReplaying`은 `protected`입니다: `Mutate` 기반 리드 모델 발행을
  `if (IsReplaying) return;`으로 재정의하여 리플레이된 이벤트마다 O(n) 작업을
  피하세요(그렇지 않으면 재구성은 O(n²)이 됩니다).
- 스키마 태그: `SnapshotSchema`(기본값 `null`)를 `"CounterState/v1"` 같은 버전
  문자열로 재정의하고 **페이로드의 레이아웃이나 의미가 바뀔 때마다 올리세요**.
  값을 바꾸면 저장된 스냅샷이 한 번 무효화됩니다(전체 리플레이 한 번).
  정상 속도로 돌아오는 시점은 해당 애그리거트가 `SnapshotEveryNEvents`개의 이벤트를
  다시 기록한 뒤입니다. 반드시 타입 수준 상수로 두세요. 인스턴스마다 다른 값이 되어
  기록 인스턴스와 재구성 인스턴스가 어긋나면 모든 복구가 조용히 전체 리플레이로 퇴화합니다.
- 태그가 `null`이 아니면 프레임워크가 페이로드를 봉투로 감싸고 복원 시 태그를 비교합니다.
  불일치이거나, actor가 태그를 선언했는데 페이로드에 봉투가 없으면
  해당 스냅샷을 버리고 전체 스트림을 리플레이합니다. `null`만이 유일한 해제 값이며
  `""`도 실제 태그입니다. 태그가 UTF-8로 255바이트를 넘으면 스냅샷을 아예 쓰지 않습니다
  (`OnSnapshotFailed`로 보고됩니다).
  스토어는 이 페이로드를 **불투명 바이트**로 다뤄야 합니다. 파싱하거나
  `TakeSnapshot()`의 출력과 같다고 가정하지 마세요.

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader)(이벤트 로그 읽기 인터페이스)

비즈니스 코드는 `IActorSystem`에 대고 작성하며 돌려받는 것은 액터입니다. 비즈니스용 멤버가
이벤트 컬렉션을 반환하는 일은 없습니다. `IEventStore`는 **스토어 작성자**를 위한 영속화 SPI로,
추가된 이벤트를 영속화하고 필요할 때 돌려주며, `LoadAsync`는 런타임 재구성의 폴백 읽기로
남습니다. 도구와 프로젝션은 대신 `IEventLogReader`(`GetVersionAsync`, `OpenAsync`,
`PeekFirstAsync`)로 읽으며, `OpenAsync`는 전방 전용 커서를 반환합니다:

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- 단일 관측: `End`는 `OpenAsync` 시점에 얻은 스트림 길이이며 커서 수명 동안 바뀌지 않습니다.
  따라서 롤백 가드와 꼬리 길이가 같은 한 번의 열기에서 나오고, 커서가 내놓는 수가 `End`를
  넘지 않습니다.
- 능력 판별은 규칙 하나입니다 — `eventStore is IEventLogReader`. 읽기 인터페이스를 구현한
  스토어에서는 스냅샷 가속 재구성이 스냅샷 이후의 꼬리만 읽습니다(파일 스토어라면 머리를
  분류하고 꼬리만 역직렬화할 수 있습니다). 구현하지 않은 스토어는 `LoadAsync`로 폴백하며
  스트림 전체를 읽어 역직렬화합니다.
- `InMemoryEventStore`는 `IEventLogReader`를 구현합니다. 다른 스토어에게 구현은 선택 사항이며
  순수하게 추가적인 변경입니다.

### JSONL Event Store (opt-in)(JSONL 이벤트 저장소, 선택 사항)

`PicoActor`는 Actor를 실행할 뿐 저장소를 함께 제공하지 않습니다. 저장소를 제공하는 패키지는
두 개입니다: `PicoActor.EventStore.InMemory`(인메모리 기본 구현, `AddPicoActorInMemoryStore()`로
등록)와 `PicoActor.EventStore.Jsonl`(영속화 — 이 절에서 설명).

`PicoActor.EventStore.Jsonl`은 집계마다 추가 전용 JSONL 파일 하나
(`<root>/<aggregateId>.jsonl`, 한 줄에 JSON 객체 하나)를 유지하고, 스냅샷은 인접한
`<root>-snapshots` 디렉터리에 sidecar로 원자적으로 기록합니다. `IEventStore`,
`IEventLogReader`, `IEventStoreSnapshot`, `IEventStoreEnumerator`를 구현하고 AOT 안전
(PicoJetson 소스 생성)이며 **단일 프로세스**입니다: 같은 디렉터리에 대한 두 store 인스턴스는
게이트를 공유하지 않으므로 동시 쓰기는 스트림을 찢지 않고 명확하게 실패합니다.

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

- **등록 누락은 빌드 시점에 드러납니다(첫 쓰기의 놀라움이 아님)**: 패키지에는 `PICOSTORE001`
  분석기가 포함되어, 어떤 `[PicoDerivedType]`에도 열거되지 않은 도메인 이벤트를 지적합니다
  (인메모리 저장소만 쓰는 프로젝트는 비용을 지불하지 않습니다).
- **찢어진 꼬리와 손상은 다릅니다.** 개행으로 끝나지 않는 마지막 줄—프로세스 크래시만 남기는
  형태—은 무시되며 집계되지도, 예외가 되지도 않습니다. 다음 추가가 그것을 잘라냅니다. 반면
  **종료된** 줄인데 역직렬화에 실패하면 손상이며 예외입니다(마지막 줄 포함). 저장소는 이를
  복구하지 않습니다—복구는 추측이기 때문입니다. 이것이 `FlushToDisk`를 기본값 `false`로 둘 때의
  문서화된 비용입니다: 바이트는 OS에 도달하지만(다른 판독자는 즉시 그 이벤트를 봄) 플래터에는
  도달하지 않으므로, 전원 손실이 종료된 쓰레기 줄을 남길 수 있습니다. `FlushToDisk = true`로
  설정하면 추가마다 fsync합니다.
- **커서와 꼬리만 재구성.** `GetVersionAsync`, 커서의 `End`, 그 바이트 위치는 한 번의 저렴한
  스캔(완전하고 비어 있지 않으며 종료된 줄—JSON 검증 없음, 역직렬화기 없음)에서 나오므로,
  스냅샷 가속 재구성은 `snapshot.Version + 1`에서 열어 꼬리(`N − v`개)만 역직렬화합니다.
  절약되는 것은 역직렬화와 할당이며 디스크 I/O가 아닙니다: 머리 부분도 바이트로 한 번은 읽힙니다.
  직렬화기의 "건너뛰기" 프로브가 필요 없습니다—일반 스트림에는 이벤트만 있으므로, 읽을 수 없는
  줄은 외래 레코드가 아니라 손상입니다.
- `MaxAggregateLocks`(기본 `1024`)가 집계별 게이트 테이블의 상한입니다; 밀려난 항목은 그 집계의
  다음 추가에서 한 번 다시 스캔할 뿐이며 정확성에는 영향이 없습니다. `Logger`는 DI의
  `ILoggerFactory`와 독립적입니다: 저장소의 경고를 자신의 로그에서 보려면 같은 logger를
  명시적으로 전달하세요.
- 이번 릴리스 범위 밖: 다중 프로세스 쓰기, 아카이브/보존/컴팩션, 외래(비이벤트) 줄, 암호화/압축.
  프레임워크 이벤트에 **두 번째 코덱은 필요 없습니다**: 바인딩은 store 패키지에서 생성된 직렬화기로
  라우팅할 뿐입니다(아래 "Framework events" 참고).

#### Stream naming（스트림 파일 이름 규칙, `FileNameFormat`）

`JsonlEventStoreOptions.FileNameFormat` 은 스트림 **파일의 이름을 어떻게 붙일지** 고릅니다. 줄
포맷은 두 모드가 동일하므로 이름만 달라지고, 기존 디렉터리는 그대로 읽힙니다.

| 값 | 스트림 파일 | 얻는 것 |
|---|---|---|
| `IdOnly` (기본값) | `<id:D>.jsonl` | 기존 레이아웃을 바이트 단위로 재현 — 마이그레이션 없음, 기존 디렉터리 변화 없음 |
| `TypePrefix` | `<type>-<id:D>.jsonl` | 타입이 이름에 실리므로 `ListAggregateIdsAsync` 가 파일 이름만으로 id 를 해석합니다: 열거당 **파일 열기 0회** |

`{type}` 은 `events[0].GetType().Name`, 즉 스트림에서 **처음 영속화된 이벤트**의 구체 타입입니다
(제네릭 타입은 백틱 아리티를 유지합니다). 이 이름은 **생성 시 고정**됩니다: 이후 배치의 첫 이벤트가
다른 타입이어도 스트림이 태어난 이름으로 계속 추가됩니다.

- **`TypePrefix` 로의 마이그레이션.** PicoAgent 는 이미 스트림을 `{type}-{id}.jsonl` 로 명명하므로,
  그런 디렉터리를 `FileNameFormat = TypePrefix` 로 가리키면 **이름 변경이 전혀 필요 없습니다** —
  디렉터리는 그대로 읽히고 스냅샷도 그대로 이어집니다(스냅샷은 집계 id 를 키로 쓰는 `<id:D>.snap`
  이며 스트림 이름을 키로 쓰지 않습니다). 반대 방향은 제공하지 않습니다: 접두사는 이름의 일부라
  `IdOnly` 스토어는 id 로부터 유도할 수 없고 모든 파일을 이름 변경해야 합니다.
- **혼합 디렉터리는 진단할 뿐, 병합하지 않습니다.** *다른* 레이아웃에서만 유효한 항목은
  계산되고(`ForeignFormatFiles`, **이름** 단위이며 단조 증가: `IdOnly` 중복 가드의 게이트입니다)
  스캔마다 **한 줄**의 Warning 으로 보고됩니다(탐지는 양방향 대칭입니다: `TypePrefix` 의
  `<id>.jsonl` 과 `IdOnly` 의 `<type>-<id>.jsonl` 은 둘 다 "다른 포맷"입니다). 디렉터리 전체 진단이
  세는 것은 이름이 아니라 **이력**입니다: 설정 포맷의 이력이 **0개**이고 다른 포맷의 이력이
  **비어 있지 않게 여럿**인 디렉터리는 `InvalidOperationException` 을 던지며 양쪽 개수, 두 포맷,
  몇 가지 해법을 함께 알려 줍니다 — 열거, 생성, lazy pass 모두 마찬가지입니다 — 빈 시스템을
  보고하는 대신에. 반면 길이 0 인 잔여 파일(실패한 추가가 남기는 것)만 있는 디렉터리는 **빈**
  저장소이며 조용히 지나갑니다. *다른 이벤트 타입*의 스트림은 혼합이 아닙니다: 정상적인 경우이며
  카운터도 로그도 소모하지 않습니다. 어떤 id 에 대해 다른 레이아웃의 비어 있지 않은 스트림이 이미
  있거나 접두사가 두 개이면, 그 추가는 바이트를 하나도 쓰기 전에 거부됩니다.
- **비용.** `TypePrefix` 는 생성 시 파일 이름 패스 1회(id → 타입 맵), 해석 실패마다 1회, 그리고
  추가마다 중복 가드용 `stat` 1회를 지불합니다. `IdOnly` 는 생성 시 **아무것도** 지불하지 않고
  이미 가진 스트림에 대한 추가도 무료입니다. 유일한 디렉터리 패스는 지연 실행으로, 자신이 갖지 않은
  스트림을 처음 관측할 때 한 번만 돌고 다시는 돌지 않습니다. 빈 배치는 두 모드 모두 아무것도 쓰거나
  만들지 않지만, **쓰기 추가와 똑같이 경로를 해석**하며 `expectedVersion` 도 여전히 검증합니다.
- **미스 캐시는 읽기 경로 전용입니다.** `TypePrefix` 의 미스는 기억됩니다 — 상한 4096개 id 의
  유계 집합이며, 넘으면 늘리지 않고 통째로 비웁니다 — 그리고 **단일 비행(single-flight)** 입니다:
  **같은** 존재하지 않는 id 에 대한 **동시** 탐색 묶음은 디렉터리 패스 하나를 공유하고, **그 id 의
  반복**은 전혀 지불하지 않습니다 — **다른** 존재하지 않는 id 는 자기 몫의 패스를 한 번 지불합니다.
  **추가는 이 기억을 결코 신뢰하지 않습니다**: 거기서 "스트림 없음"으로 해석하면, 외부 기록자가 맵이
  만들어진 뒤 생성한 스트림 옆에 두 번째 이름을 fork 하게 되기 때문입니다. 그래서 추가는 미스일 때
  **다시 스캔**하고, 성공하면 **자기 자신**의 항목만 버립니다 — 통째로 지우지 않고, 스캔 게이트도
  잡지 않습니다. 대가는 명시합니다: *읽기*는 옆에 나타난 스트림에 대해 다음 패스까지 "비어 있음"이라고
  답할 수 있습니다 — 그리고 다시는 패스를 하지 않는 호스트는 **새 인스턴스**가 디렉터리를 다시 읽을
  때까지 그 오래된 답을 봅니다. `NegativeCacheInvalidations` 는 추가가 실제로 반증한 항목 수입니다
  (`IdOnly` 에서는 미스를 기억하지 않으므로 항상 `0`).
- **오래된 맵은 다음 스캔이 바로잡습니다.** 맵이 만들어진 뒤 등장한 경쟁 접두사는 읽기로는 발견되지
  않으며(맵 히트는 패스를 지불하지 않습니다) **추가도** 그 창을 닫기 위해 디렉터리 패스를 하지
  않습니다: 맵이 벌어 주는 것을, 추가마다 지불하는 바로 그 경로에서 되돌려 주는 셈이기 때문입니다.
  다음 분류 패스 — 열거, 또는 알 수 없는 id 의 미스 — 가 충돌을 기록하고, 그때부터 그 id 의 모든
  추가는 거부되며 두 파일 이름을 함께 알려 줍니다.
- **깨진 루트나 점유된 이름은 삼키지 않고 지목합니다.** 루트가 **파일**로 존재하면 `IOException`
  (거기서는 열거도 열기도 생성도 불가능합니다)을, 스트림 이름을 **디렉터리**가 점유하면
  `InvalidOperationException` 으로 장애물을 지목합니다 — 읽기 경로와 추가 경로, 두 레이아웃 모두
  마찬가지입니다 — "빈 집계"나 맨 공유 위반이 아니라. 실패한 lazy pass(오설정, 읽을 수 없는 루트,
  파일인 루트)는 **latch** 됩니다: 이후 모든 관측이 같은 예외를 던지고 디렉터리는 다시 스캔되지
  않으므로, 유일한 복구 방법은 디렉터리를 고친 뒤 **인스턴스를 다시 만드는** 것입니다.

#### Framework events (event type bindings)(프레임워크 이벤트 타입 바인딩)

`SagaCompleted`/`SagaFailed` 는 `sealed` 이고 당신의 `TEventBase` 가 아니라 `IDomainEvent` 에서 파생되므로,
saga 스트림에는 직렬화기가 한 번도 본 적 없는 타입이 들어갑니다. `JsonlEventStoreOptions.EventTypeBindings`
는 프레임워크 이벤트를 같은 store 로 라우팅합니다. 프레임워크는
`SagaCompleted → PicoActorSagaCompleted` 와 `SagaFailed → PicoActorSagaFailed` 를 미리 등록하며(`TryAdd`
이므로 당신의 `Add<T>(tag)` 가 우선합니다. `FrameworkDefaultCount` 는 실제 적용된 기본값 개수 `0…2`,
`RegisteredCount` 는 전체 개수입니다).

- **`Add<T>(tag)` 는 라우팅만 하고 생성하지 않습니다.** tag 는 줄의 `$type` 이자 읽기 측이 냄새 맡는 값입니다
  (첫 멤버, 대소문자 무시, 판별하지 못하면 무조건 `TEventBase` 로 폴백). 하지만 그 타입에는 여전히 생성된
  직렬화기가 필요합니다. **positional record** 에 대한 `[GenerateSerializer(typeof(T))]` 는 사용할 수 없습니다
  (PicoSerDe 의 속성 파이프라인이 주 생성자를 읽지 못해 `CS7036`/`CS8852`). 쓸 수 있는 형태는 **닫힌 호출
  지점**, 즉 `FrameworkEventSerializers.Generate()` 안의 리터럴 호출입니다. 생성 지점이 없는 bound 타입은
  **첫 쓰기**에서 거부됩니다(`InvalidOperationException`, 해결책을 함께 제시).
- **혼합 배치는 한 번의 쓰기:** `TEventBase` 와 bound 이벤트가 같은 배치를 공유합니다——버퍼 하나, 쓰기 한 번,
  flush 한 번.
- **이름 규칙.** 두 프레임워크 tag 는 프로덕션 코퍼스가 이미 쓰는 tag 이며 영구적인 와이어 형식입니다(alias
  없음). 스트림은 여전히 첫 이벤트의 CLR 짧은 이름으로 명명되지만, `TypePrefix` 에서는 등록된 모든 **타입**
  프리픽스 밖의 이름(`unknown-<id>.jsonl`, 호스트 자체 분류)은 **id 로 접근 가능**하고 타입 열거에는 보이지 않으며,
  `ForeignFormatFiles` 에 세어지지 않고 디렉터리 전체 예외도 일으키지 않습니다. 프리픽스에는 `-` 가 없고,
  같은 짧은 이름은 같은 프리픽스를 공유합니다.
- **`SagaCompleted.Result` 는 `object?` 이며, 이제 영속화됩니다.** 생성된 writer 는 **런타임** 타입으로
  분기하므로, 영속 store 가 보관하는 것은 값이 원래 무엇이었는지 그대로입니다. `null` 은
  `{"Result":null}` 로 쓰이고 `null` 로 읽힙니다. `string` 이나 `bool` 은 자기 자신 그대로 왕복합니다.
  **정수는 `long` 으로 읽힙니다**(`7` 을 쓰고 `7L` 을 읽습니다 — JSON 숫자 타입은 64비트 하나뿐이라 모든
  정수 종류가 거기서 만납니다. 이 확장은 문서화된 동작이며 결함이 아닙니다). any-value 체인에 **JSON
  형태가 없는** 런타임 타입 — 예컨대 평범한 `new object()` — 은 **한 바이트도 쓰기 전에 거부**됩니다.
  종단 배치가 `FormatException`("has no JSON form in the any-value chain; write it with a concrete member
  type or a custom converter")을 던지고 파일도 버전도 남기지 않습니다. 따라서 `null`, `string`, `bool`
  또는 정수 결과를 담은 `MarkComplete(result)` 는 영속화되고 재생됩니다. 그보다 복잡한 것은 구조를
  자신의 기반 타입 비즈니스 이벤트에 넣고 `Result` 는 위 형태들로 유지하세요.
- **등록 규칙.** 바인딩은 options 인스턴스에 있습니다——store 를 생성하기 **전에**
  `options.EventTypeBindings.Add<T>(tag)` 를 호출합니다. 생성자는 이 표를 **동결하고 복사**하므로, 이후의
  `Add` 는 조용히 무시되지 않고 예외가 됩니다. 한 타입에는 tag 가 하나, 한 tag 는 타입 하나만 가리킵니다
  (같은 쌍을 다시 호출하면 no-op 이지만 **동결 전에만** 그렇습니다; tag 는 비어 있지 않고 따옴표·백슬래시·
  제어 문자를 포함하지 않습니다). 프레임워크 이벤트를 포함하는 기저 타입(`IDomainEvent` 자체 등)은 생성 시
  거부됩니다——그러면 그것들이 일반 이벤트이자 bound 이벤트가 되어 버리기 때문입니다. `TEventBase` 에 할당
  가능한 bound 타입도 마찬가지로 거부됩니다. `with` 가 복사하는 것은 **참조**입니다——클론과 원본은 동결
  상태까지 같은 표를 공유하므로, 클론의 `Add` 는 예외가 되고 *다른* store 를 지목합니다. 독립된 표가
  필요하면 `new JsonlEventStoreOptions()` 를 쓰세요.
- **바인딩의 tag 는 `TEventBase` 의 모든 `[PicoDerivedType]` 식별자와 달라야 합니다.** 하나의 tag 에는
  주인이 하나뿐이며, 읽기 경로는 **바인딩을 먼저** 묻습니다. 충돌하면 기저가 스스로 쓴 줄이 bound 타입으로
  읽혀 그 이벤트 타입과 멤버가 사라집니다. 그래서 생성자가 곧바로 `InvalidOperationException`(메시지가 해당
  tag 를 지목)을 던져 고정 전에 거부합니다——조용한 치환은 일어나지 않습니다. (남은 하나의 구멍: 어떤 파생
  타입의 reader 가 tag 를 *알아보고* 도 복호 중 예외를 던지는 경우——그것은 빠진 등록이 아니라 그 타입 자체의
  검증이며——생성 시점의 프로브는 「알 수 없는 tag」와 구분할 수 없습니다.)

### Idle Passivation (opt-in)(유휴 패시베이션, 선택 사항)

액터는 중지될 때까지 메모리에 남아 있습니다. 수명이 긴 애그리거트가 많은
워크로드에서는 유휴 예산으로 재구성 가능한 액터의 구체화된 상태를
회수할 수 있습니다:

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- 후보는 **재구성 가능한 Event Sourcing 액터만** 해당합니다: 구체 타입이
  **재구성 팩토리와 함께** 등록된 `EventSourcedActor`입니다. 비-ES 액터는
  절대 건드리지 않습니다(그 상태는 재구성할 수 없습니다).
- 후보는 초기화되어 있어야 하고, 명령을 처리 중이 아니어야 하며, mailbox가 비어 있어야 하고,
  예산 전체 기간 동안 유휴 상태여야 합니다. 중지는 프레임워크 센티널로
  mailbox를 통과하므로 메시지 경계에서 적용됩니다. 중지 요청은 소유권이
  검사되므로 오래된 요청이 같은 id의 최신 인스턴스를 제거할 수 없습니다.
- `SagaActor`는 예외입니다(`CanBePassivated => false`): 외부 응답을 기다리는 것은
  유휴로 보이며, 회수하면 그 응답을 잃게 됩니다.
- 재활성화는 명시적입니다: `GetAsync<T>(id)`가 스트림을 리플레이합니다(또는
  스냅샷을 복원하고 꼬리를 리플레이합니다). 그 전까지 `Send`/`AskAsync`는
  등록되지 않은 id와 정확히 동일하게 동작합니다(`KeyNotFoundException`) — `AskAsync`는
  타입 매개변수가 없으므로 재구성 팩토리를 선택할 수 없습니다.
- 관측성: `SweepIdleActorsAsync()`, `PassivatedCount`, `ReactivatedCount`,
  그리고 재구성마다 리플레이된 이벤트 수를 담은 로그 라인.

### SagaActor(유한 수명 코디네이터)

`SagaActor extends EventSourcedActor` — 수명이 유한한 애그리게이트 간 작업을 위한 타입입니다. 메일박스가 영원히 동작하는 일반 EventSourcedActor 와 달리, SagaActor 는 프레임워크가 종단 이벤트를 영속화한 후 **자동으로 중지**됩니다.

종단 상태는 **프레임워크가 생성**합니다: `MarkComplete(result)` 는 `SagaCompleted(result)` 이벤트를 비즈니스 이벤트와 같은 배치에 추가하고(원자적 추가 — 완료와 영속화가 어긋나지 않음), 처리되지 않은 비즈니스 예외는 `SagaFailed(reason)`(`"ExceptionType: message"`, 512자로 절단)을 추가하며 호출자에게 `SagaExecutionException(Id, Reason)` 을 던집니다. 하위 클래스는 이 이벤트들을 raise 하거나 handle 하지 않습니다 — `Mutate` 는 비즈니스 이벤트만 봅니다.

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

**라이프사이클:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

비즈니스 실패: `OnMessageAsync`/`ResumeAsync` 가 예외를 던지면 → 프레임워크는 커밋되지 않은 비즈니스 이벤트를 폐기하고, `SagaFailed(reason)` 을 추가하고, 자동 중지하며, Ask 호출자에게 `SagaExecutionException(Id, Reason)` 을 전달합니다. 인프라 실패(스토어 다운)는 **종단이 아닙니다** — 이벤트는 롤백되고 saga 는 Running 상태로 유지됩니다.
`SagaFailed` 자체의 영속화가 실패하면(스토어 계속 다운) 종단 상태가 만들어지지 않고 **원래 예외**가 호출자에게 전파됩니다.

**크래시 복구:** `GetAsync` 가 이벤트를 리플레이 → 프레임워크가 `SagaCompleted`/`SagaFailed` 로부터 `Completed`/`Failed` 를 복원합니다(종단 상태인 saga 는 되살아나지 않음 — `GetAsync` 는 null 반환). 종단 이벤트가 없는 saga 에는 `ResumeAsync()` 가 호출되고, 이로써 종단에 도달하면 프레임워크가 같은 플러시 배치에 `SagaCompleted` 를 영속화합니다. 복구는 명시적 풀 방식이며 백그라운드 마법이 아닙니다.

**명시적 일괄 복구:**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` 는 첫 이벤트 타입 이름으로 saga 를 열거하고, 각각을 `GetAsync` 로 단일 비행 복구한 뒤, 복구 후 분류를 반환합니다: `Completed` / `Failed`(프레임워크가 복원한 reason 포함) / `Running`(외부 입력 대기 중). 이미 종단 상태인 saga 는 결코 되살아나지 않으며, 살아있는 saga 는 그 자리에서 분류됩니다. 멱등하고 안전하게 재시도할 수 있고(id별 단일 비행), 스토어 장애 시 빠른 실패(fail-fast)로 호출자가 전체 배치를 재시도할 수 있습니다.

**Process Manager 패턴:**

동일한 기본 클래스가 process manager 도 커버합니다 — 외부 이벤트는 애플리케이션 수준의 이벤트 핸들러(예: PicoMediator 구독자)가 커맨드로 번역하여 saga 의 메일박스로 보냅니다. 이벤트가 actor 로 직접 들어가는 일은 없으며, saga 는 커맨드만 봅니다.

**편의 API:**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` 는 `CreateAsync` + `AskAsync` 를 한 번의 호출로 수행합니다. 성공 시 `SagaExecution<TResult>(Id, Result)` 를 반환하고, 비즈니스 실패 시 `SagaExecutionException(SagaId, Reason)` 을 던져 호출자가 항상 saga id 를 얻도록 합니다.

### 메시징: Ask vs Send

| 패턴 | 메서드 | 의미 |
|---------|--------|-----------|
| 요청-응답 | `AskAsync<TResult>(id, command)` | 메시지 처리 후 결과 반환 |
| Fire-and-Forget | `Send(id, command)` | 응답 없음; 실패는 UnhandledErrorHandler로 라우팅(logger 없으면 stderr); actor 미등록/mailbox 닫힘(종료 경쟁) 시 예외 |

### OutputChannel

Actor는 `ActorOutputEvent` 메시지를 외부 구독자에게 브로드캐스트합니다:

```csharp
counter.OutputWriter = channel.Writer;
// OnMessageAsync 내부:
WriteOutput("Incremented", data: "Delta=5");
```

### Spawn

Actor는 `System` 참조를 통해 자식 Actor를 생성합니다:

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

## 모듈 통합

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();               // IActorSystem만 등록(저장소는 등록하지 않음)
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory: 인메모리 기본 구현

// 사용자 정의 이벤트 저장소
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// PicoCfg에서
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`AddPicoActor()`는 더 이상 저장소를 등록하지 않지만 등록 시점에 실패하지는 않습니다:
`IActorSystem`을 **해석**할 때 `InvalidOperationException`을 던지고 메시지에서
`AddPicoActorInMemoryStore()`를 지목합니다(등록 순서는 여전히 자유롭고, 명시적
`AddPicoActor(store)`가 계속 우선합니다).

`IActorSystem`은 **Singleton**으로 등록됩니다. `ILoggerFactory`가 등록되어
있으면 로거가 자동으로 주입됩니다.

### 사용자 정의 IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* 동시성 검사 포함 INSERT */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* 버전순 SELECT */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* 첫 번째 이벤트 조회(복구 열거) */ }
}
```

### 이벤트 유출(PicoMediator)

`IDomainEvent : IEvent`——도메인 이벤트는 PicoMediator 1급 알림입니다. persist+mutate 이후 프레임워크가 **컨텍스트 엔벨로프** 형태로 `IDomainEventPublisher` 훅을 통해 발행하며;replay(복구)는 재발행하지 않습니다.

`MediatorDomainEventPublisher`는 즉시 사용 가능한 어댑터:각 이벤트를 `DomainEventEnvelope(actorId, version, event)`로 감싼 후 이벤트별 `Publish<DomainEventEnvelope>`(컴파일 타임 제네릭, AOT 안전), 이벤트별 격리——구독자 하나의 실패가 이후 이벤트를 막지 않습니다.

### 도메인 이벤트 구독(declare-and-subscribe)

이벤트 핸들러는 `IDomainEventSubscriber<TEvent>`를 구현하는 일반 클래스입니다——PicoActor.Gen(PicoActor.Abs에 내장)이 스캔하여 자동 등록, 수동 배선 제로. 핸들러는 소스 애그리거트 컨텍스트(`ActorId`, `Version`)를 담은 타입화된 엔벨로프와 좁은 포트 `ICommandSender`를 받습니다:

handler는 class 또는 record로 선언할 수 있습니다. 접근 가능한(public/internal) 인스턴스 생성자가 없는 구현은 잘못된 등록 코드를 생성하는 대신 빌드 시 **PICA001**로 실패합니다.

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

// 배선:AddPicoMediator가 IMediator를 등록;AddPicoActor()가 ActorSystem 팩토리 내에서
// 자동 감지하여 이벤트 유출을 배선(지연 해석, Scoped 수명 주기와 호환).
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe:구독자 자동 등록
container.AddPicoActor();     // MediatorDomainEventPublisher 자동 배선
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// 사용자 지정 publisher 명시적 배선:AddPicoActor(IPublisher)(인스턴스는 Build() 전에 필요).
```

> **필수 패키지:** `PicoActor`(`PicoActor.Abs`와 `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator` 포함)에 더해 저장소 패키지 하나가 필요합니다 — 인메모리라면 `PicoActor.EventStore.InMemory`(`AddPicoActorInMemoryStore()`로 등록), 영속화라면 `PicoActor.EventStore.Jsonl`(PicoJetson 포함). `IDomainEventSubscriber<TEvent>`를 선언할 때 추가로 필요한 패키지는 없습니다. `PicoMediator`는 `PicoActor`의 종속성으로 함께 제공됩니다(생성된 브리지가 해당 패키지의 `MediatorAutoSubscriptionRegistry`를 호출합니다). 컨테이너 배선(`SvcContainer`, `AddPicoMediator`)을 사용할 때는 `PicoDI`와 `PicoMediator.DI`를 추가합니다.

> **로컬 개발(ProjectReference):** analyzer는 ProjectReference 체인을 통해 전파되지 않습니다——프로젝트 소비자는 `PicoActor.Gen`을 직접 참조해야 합니다(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, `tests/PicoActor.Tests` 미러). NuGet 소비자는 `PicoActor.Abs` 패키지의 `buildTransitive` props를 통해 생성기를 자동으로 받습니다——추가 참조 불필요.

이벤트는 persist+mutate 이후 엔벨로프 형태로 PicoMediator를 통해 유출됩니다;replay는 절대 발행하지 않습니다. 핸들러 실패는 actor에 영향을 주지 않습니다(핸들러별 격리). 이벤트→명령→이벤트 변환 루프는 의도된 사용법입니다——핸들러를 멱등하고 유계로 유지하세요.

> **파괴적 변경:** 직접 `ISubscriber<TEvent>`(PicoMediator) 구독자는 더 이상 PicoActor 도메인 이벤트를 받지 못합니다. `IDomainEventSubscriber<TEvent>`로 마이그레이션하세요;엔벨로프의 `ActorId`/`Version`이 수동으로 내장된 애그리거트 id를 대체합니다. 사용자 지정 publisher(`AddPicoActor(IPublisher)`)는 이제 원시 이벤트 대신 `DomainEventEnvelope` 인스턴스를 받습니다——`Publish<TEvent>` 구현을 그에 맞게 조정하세요(관찰되는 페이로드 형태만 변경;actor 파이프라인은 영향 없음). `IEventStoreEnumerator.ListAggregateIds(string)`는 비동기 `ListAggregateIdsAsync(string)`로 변경되었습니다 — 사용자 지정 열거자는 시그니처를 갱신해야 합니다.

참고:
- `Register<T>`는 actor 타입당 한 번만 호출해야 합니다: 중복 등록은 이전 팩터리를 조용히 교체하지 않고 예외를 던집니다.
- `Register<T>`는 런타임이 처리할 수 없는 타입을 거부합니다: `Actor`를 상속하지 않는 구현, `EventSourcedActor`를 상속하지 않는 `IEventSourcedActor`(영속화는 구체 기반 클래스에만 연결됩니다).
- `StopAsync`/`RequestStop`는 먼저 레지스트리에서 actor를 제거한 뒤 메일박스에 이미 버퍼된 메시지를 모두 처리합니다(정상 종료). 종료 후 보낸 메시지는 `KeyNotFoundException`을 던집니다(mailbox가 이미 닫힌 경우 `InvalidOperationException` — 종료 경쟁 구간).
- **이벤트→명령 변환은 구독자(비즈니스 계층)의 책임**——PicoActor는 발행만;명령은 mailbox로만 actor에 진입합니다.
- 발행은 **persist+mutate 이후**——발행 실패는 actor 상태에 영향을 주지 않습니다(이벤트는 이미 영속화됨).
- 복구는 조용함:replay는 재발행하지 않습니다.
- 프레임워크 이벤트(`SagaCompleted`/`SagaFailed`)는 다른 이벤트와 마찬가지로 형식화 구독 가능(Abs는 net10.0 대상).
- **자동 배선은 임의 scope에서 안전**:PicoDI 2026.8.1(E1)부터 Singleton 팩토리는 컨테이너 내부 루트 scope를 사용——자동 배선된 IMediator는 컨테이너 해제까지 생존합니다.
- **발행 중인 애그리거트에 자신의 발행 경로에서 `AskAsync`를 호출하지 마세요**——소스 mailbox가 이벤트 flush로 바쁘므로 요청이 자기 교착(self-deadlock)됩니다. 읽기 측 투영(별도 actor)을 조회하세요;소스 애그리거트로의 `Send`는 안전합니다(발사 후 망각).

---

## 설계 철학 (克制 / 专注 / 优雅 / 高效)

| 원칙 | 실제 적용 |
|-----------|------------|
| **克制 (Restraint / 절제)** | 분산 합의 없음, 감독 트리 없음——Actor와 Event만. |
| **专注 (Focus / 집중)** | Actor당 단일 스레드. 한 번에 하나의 메시지. |
| **优雅 (Elegance / 우아함)** | Persist-then-Mutate: 영속화 후에만 상태 변경. 롤백 자동. |
| **高效 (Efficiency / 효율)** | AOT 호환, 제로 리플렉션, `net10.0` 추상화. |

---

## 사용 사례

- **AI 에이전트 시스템**——각 AI 에이전트가 대화 상태를 가진 Actor
- **워크플로우 오케스트레이션**——Actor로 장기 실행 비즈니스 프로세스 모델링
- **게임 서버 상태**——Event Sourcing Actor로 플레이어/게임 상태 관리
- **IoT 장치 상태**——정기 스냅샷이 포함된 인메모리 Actor

---

## 패키지

| 패키지 | 타겟 | 설명 |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | 핵심 추상화: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor` — 추가로 구독 타입(`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`)과 내장 `PicoActor.Gen` 분석기(declare-and-subscribe) |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 런타임: `ActorSystem`, `MediatorDomainEventPublisher`(엔벨로프 이벤트 유출), PicoDI 통합(`AddPicoActor`가 IMediator + ICommandSender 자동 연결) |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | 인메모리 저장소(`InMemoryEventStore`, v2026.9에서 `PicoActor` 밖으로 이동) + `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | 영속 JSONL 이벤트 저장소: 추가 전용, 낙관적 동시성, `IEventLogReader`를 통한 꼬리만 재구성, 원자적 스냅샷 sidecar, AOT 안전(PicoJetson) — 내장 `PICOSTORE001` 등록 분석기 포함 |

---

## 비교

| 기능 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| 인메모리 전용 | ✅ | ✅ | ✅ | ❌ |
| AOT / 트리밍 | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 추상화 | ❌ | ✅ | ✅ | ❌ |
| PicoDI 통합 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 분산 / 클러스터링 | ❌ | ✅ | ✅ | ✅ |
| Actor당 단일 스레드 | ✅ | ✅ | ✅ | ❌ |
| 패키지 수 | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## 라이선스

MIT — [LICENSE](LICENSE) 참조.
