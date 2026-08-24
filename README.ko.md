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
| 의존성 크기 | ❌ Akka.NET(8+ 패키지), Orleans(10+ 패키지) | ✅ 패키지 4개 — PicoActor + PicoActor.Abs + PicoMediator.Abs + PicoDI.Abs; 다른 런타임 의존성 없음 |
| DI 통합 | ❌ Microsoft.Extensions.DI에 종속 | ✅ 네이티브 PicoDI, 제로 리플렉션 |
| netstandard2.0 | ⚠️ Akka.NET / Proto.Actor 일부만 지원 | ❌ net10.0 전용(PicoMediator 런타임은 net10.0+ 필요) |
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
| `InMemoryEventStore` | 인메모리 저장소——스트림 단위 락, 낙관적 동시성(ConcurrentDictionary 기반) |
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
| Fire-and-Forget | `Send(id, command)` | 응답 없음; 예외는 UnhandledErrorHandler로 라우팅 |

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

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* 첫 번째 이벤트 조회(복구 열거) */ }
}
```

### 이벤트 유출(PicoMediator)

`IDomainEvent : IEvent`——도메인 이벤트는 PicoMediator 1급 알림입니다. persist+mutate 이후 프레임워크가 **컨텍스트 엔벨로프** 형태로 `IDomainEventPublisher` 훅을 통해 발행하며;replay(복구)는 재발행하지 않습니다.

`MediatorDomainEventPublisher`는 즉시 사용 가능한 어댑터:각 이벤트를 `DomainEventEnvelope(actorId, version, event)`로 감싼 후 이벤트별 `Publish<DomainEventEnvelope>`(컴파일 타임 제네릭, AOT 안전), 이벤트별 격리——구독자 하나의 실패가 이후 이벤트를 막지 않습니다.

### 도메인 이벤트 구독(declare-and-subscribe)

이벤트 핸들러는 `IDomainEventSubscriber<TEvent>`를 구현하는 일반 클래스입니다——PicoActor.Gen(PicoActor.Abs에 내장)이 스캔하여 자동 등록, 수동 배선 제로. 핸들러는 소스 애그리거트 컨텍스트(`ActorId`, `Version`)를 담은 타입화된 엔벨로프와 좁은 포트 `ICommandSender`를 받습니다:

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

> **로컬 개발(ProjectReference):** analyzer는 ProjectReference 체인을 통해 전파되지 않습니다——프로젝트 소비자는 `PicoActor.Gen`을 직접 참조해야 합니다(`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, `tests/PicoActor.Tests` 미러). NuGet 소비자는 `PicoActor.Abs` 패키지의 `buildTransitive` props를 통해 생성기를 자동으로 받습니다——추가 참조 불필요.

이벤트는 persist+mutate 이후 엔벨로프 형태로 PicoMediator를 통해 유출됩니다;replay는 절대 발행하지 않습니다. 핸들러 실패는 actor에 영향을 주지 않습니다(핸들러별 격리). 이벤트→명령→이벤트 변환 루프는 의도된 사용법입니다——핸들러를 멱등하고 유계로 유지하세요.

> **파괴적 변경:** 직접 `ISubscriber<TEvent>`(PicoMediator) 구독자는 더 이상 PicoActor 도메인 이벤트를 받지 못합니다. `IDomainEventSubscriber<TEvent>`로 마이그레이션하세요;엔벨로프의 `ActorId`/`Version`이 수동으로 내장된 애그리거트 id를 대체합니다. 사용자 지정 publisher(`AddPicoActor(IPublisher)`)는 이제 원시 이벤트 대신 `DomainEventEnvelope` 인스턴스를 받습니다——`Publish<TEvent>` 구현을 그에 맞게 조정하세요(관찰되는 페이로드 형태만 변경;actor 파이프라인은 영향 없음).

참고:
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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | 런타임: `ActorSystem`, `InMemoryEventStore`, `MediatorDomainEventPublisher`(엔벨로프 이벤트 유출), PicoDI 통합(`AddPicoActor`가 IMediator + ICommandSender 자동 연결) |

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
| 패키지 수 | 4 | 8+ | 3+ | 10+ |

---

## 라이선스

MIT — [LICENSE](LICENSE) 참조.
