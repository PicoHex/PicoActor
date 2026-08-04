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

Event Sourcing Actor는 **Persist-then-Mutate**(영속화 후 변경)를 따릅니다:
`OnMessageAsync → RaiseEvent → IEventStore에 영속화 → Mutate 상태`.
상태는 영속화 성공 후에만 변경됩니다——인메모리 상태는 항상 이벤트 스트림과 일치합니다.

---

## 왜 PicoActor인가

| 관점 | 기존 옵션 | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / 트리밍 | ❌ Akka.NET, Proto.Actor, Orleans는 리플렉션 필요 | ✅ 완전 NativeAOT 지원 |
| Event Sourcing | ❌ Proto.Actor, Orleans는 ES 미내장 | ✅ Persist-then-Mutate, 자동 롤백 |
| 의존성 크기 | ❌ Akka.NET(8+ 패키지), Orleans(10+ 패키지) | ✅ 패키지 2개, Channels 외 제로 의존성 |
| DI 통합 | ❌ Microsoft.Extensions.DI에 종속 | ✅ 네이티브 PicoDI, 제로 리플렉션 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 일부만 지원 | ✅ 추상화 계층 netstandard2.0 타겟 |
| 학습 곡선 | ❌ 가파름——감독 트리, 클러스터링, 리모팅 | ✅ 최소——Actor + Event + Mailbox |

---

## 빠른 시작

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

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

`netstandard2.0` 타겟, 최대 호환성.

| 타입 | 역할 |
|------|------|
| `IActor` | 기본 인터페이스——`Id`(UUID v7) 제공 |
| `IActorSystem` | 런타임 계약——Register, CreateAsync, GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | 명령용 마커 인터페이스 |
| `IDomainEvent` | 도메인 이벤트용 마커 인터페이스 |
| `IEventSourcedActor` | 선택적——Version, ReplayEvents, CommitEvents |
| `IEventStore` | 영속화 계약——AppendAsync(낙관적 동시성), LoadAsync |
| `ICancelable` | 선택적——장기 실행 작업용 CancelCurrentTurn |
| `Actor` | 추상 기본 클래스——메일박스, 소비 루프, SignalReady, StopAsync |
| `EventSourcedActor` | ES 기본——RaiseEvent, Mutate, Persist-then-Mutate 파이프라인 |
| `Envelope` | 내부——ICommand를 선택적 TaskCompletionSource로 래핑 |
| `ActorOutputEvent` | 발신 알림——Type, Data, 선택적 ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | 버전 불일치 시 IEventStore가 발생 |

### PicoActor — 런타임

`net10.0` 타겟, AOT 호환.

| 타입 | 역할 |
|------|------|
| `ActorSystem` | 기본 `IActorSystem`——ConcurrentDictionary 레지스트리, 팩토리 등록, 메시지 라우팅, CancelTurn |
| `InMemoryEventStore` | 락-프리 인메모리 저장소——ConcurrentDictionary 기반 |
| `ActorConfig` | 설정 POCO——PicoCfg에서 바인딩 가능 |
| `ActorSystemOptions` | 옵션 — 필수 EventStore, 선택 Logger;`ActorSystem` 생성자에서 사용 |
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

### 메시징: Ask vs Send

| 패턴 | 메서드 | 의미 |
|---------|--------|-----------|
| 요청-응답 | `AskAsync<TResult>(id, command)` | 메시지 처리 후 결과 반환 |
| Fire-and-Forget | `Send(id, command)` | 응답 없음; 예외는 UnhandledErrorHandler로 라우팅 |

### OutputChannel

Actor는 `ActorOutputEvent` 메시지를 외부 구독자에게 브로드캐스트합니다:

```csharp
counter.OutputWriter = channel.Writer;
// OnMessageAsync 내부:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Actor를 중지하지 않고 장기 실행 작업을 취소합니다:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... _currentTurnCts.Token을 사용한 장기 작업
    }
}
system.CancelTurn(actor.Id);
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
container.AddPicoActor();  // IActorSystem + InMemoryEventStore 등록

// 사용자 정의 이벤트 저장소
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// PicoCfg에서
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

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
}
```

---

## 설계 철학 (克制 / 专注 / 优雅 / 高效)

| 원칙 | 실제 적용 |
|-----------|------------|
| **克制 (Restraint / 절제)** | 분산 합의 없음, 감독 트리 없음——Actor와 Event만. |
| **专注 (Focus / 집중)** | Actor당 단일 스레드. 한 번에 하나의 메시지. |
| **优雅 (Elegance / 우아함)** | Persist-then-Mutate: 영속화 후에만 상태 변경. 롤백 자동. |
| **高效 (Efficiency / 효율)** | AOT 호환, 제로 리플렉션, `netstandard2.0` 추상화. |

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | 핵심 추상화: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 런타임: `ActorSystem`, `InMemoryEventStore`, PicoDI 통합 |

---

## 비교

| 기능 | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| 인메모리 전용 | ✅ | ✅ | ✅ | ❌ |
| AOT / 트리밍 | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0 추상화 | ✅ | ✅ | ✅ | ❌ |
| PicoDI 통합 | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| 분산 / 클러스터링 | ❌ | ✅ | ✅ | ✅ |
| Actor당 단일 스레드 | ✅ | ✅ | ✅ | ❌ |
| 패키지 수 | 2 | 8+ | 3+ | 10+ |

---

## 라이선스

MIT — [LICENSE](LICENSE) 참조.
