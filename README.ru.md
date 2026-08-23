# PicoActor

AOT-совместимый in-memory Actor-фреймворк с Event Sourcing для .NET.
Лёгкий, без рефлексии, разработан для AI-агентных систем и оркестрации
рабочих процессов. Работает под NativeAOT и trimming.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## Вычислительная модель

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

Каждый Actor владеет **почтовым ящиком** (in-memory `Channel<Envelope>`),
идентификатором **UUID v7** и однопоточным циклом обработки. Команды
доставляются через `Send` (fire-and-forget) или `AskAsync` (запрос-ответ).

PicoActor — **управляемый сообщениями**: каждое взаимодействие — это сообщение. Команды (`ICommand`) — адресные сообщения, доставляемые через почтовый ящик (1:1, ответ опционален); доменные события (`IDomainEvent`) — широковещательные сообщения, публикуемые через PicoMediator (1:N, без ответа). События — тоже сообщения: паттерн взаимодействия между агрегатами один — событие → подписчик → переведённая команда → почтовый ящик. Другого способа взаимодействовать с актором не существует.

Event Sourcing Actors следуют **Persist-then-Mutate** (Сначала сохранить, потом изменить):
`OnMessageAsync → RaiseEvent → Сохранить в IEventStore → Mutate состояние`.
Состояние изменяется только после успешного сохранения — состояние в памяти
всегда согласовано с потоком событий.

---

## Почему PicoActor

| Аспект | Существующие решения | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans требуют рефлексию | ✅ Полная поддержка NativeAOT |
| Event Sourcing | ❌ Proto.Actor, Orleans без встроенного ES | ✅ Persist-then-Mutate, автоматический откат |
| Размер зависимостей | ❌ Akka.NET (8+ пакетов), Orleans (10+ пакетов) | ✅ 4 пакета — PicoActor + PicoActor.Abs + PicoMediator.Abs + PicoDI.Abs; других зависимостей нет |
| Интеграция DI | ❌ Привязан к Microsoft.Extensions.DI | ✅ Нативный PicoDI, разрешение без рефлексии |
| netstandard2.0 | ⚠️ Частичная поддержка в Akka.NET / Proto.Actor | ❌ Только net10.0 (среда PicoMediator требует net10.0+) |
| Кривая обучения | ❌ Крутая — деревья супервизии, кластеризация, remoting | ✅ Минимальная — Actor + Event + Mailbox |

---

## Быстрый старт

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Настройка
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Регистрация фабрик Actor
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Создание, отправка сообщений, остановка, восстановление
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Детали модуля

### PicoActor.Abs — Основные абстракции

Цель `net10.0` (среда PicoMediator и сгенерированный bridge-код требуют net10.0+).

| Тип | Роль |
|------|------|
| `IActor` | Базовый интерфейс — предоставляет `Id` (UUID v7) |
| `IActorSystem` | Контракт времени выполнения — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, RequestStop, StopAllAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | Маркерный интерфейс для команд |
| `IDomainEvent` | Маркерный интерфейс для доменных событий |
| `IEventSourcedActor` | Опциональный — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Контракт хранения — AppendAsync (оптимистичная конкурентность), LoadAsync, PeekFirstAsync |
| `ICancelable` | Опциональный — CancelCurrentTurn для длительных операций |
| `Actor` | Абстрактный базовый класс — почтовый ящик, цикл обработки, SignalReady, StopAsync |
| `EventSourcedActor` | ES-база — RaiseEvent, Mutate, конвейер Persist-then-Mutate |
| `SagaActor` | ES-координатор с конечным жизненным циклом — терминальные события фреймворка (SagaCompleted/SagaFailed), авто-остановка, явное пакетное восстановление через ResumeInterruptedSagasAsync |
| `IDomainEventSubscriber<TEvent>` | Контракт подписчика — типизированный конверт + `ICommandSender`; автоматически регистрируется PicoActor.Gen (declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | Конверт контекста — `ActorId`, `Version`, `Event` (транспорт / типизированная доставка) |
| `ICommandSender` | Узкий порт команд для обработчиков — Send, AskAsync, ExecuteSaga |
| `Envelope` | Внутренний — оборачивает ICommand с опциональным TaskCompletionSource |
| `ActorOutputEvent` | Исходящее уведомление — Type, Data, опциональные ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | Выбрасывается IEventStore при несовпадении версий |

### PicoActor — Среда выполнения

Цель `net10.0`, AOT-совместим.

| Тип | Роль |
|------|------|
| `ActorSystem` | Стандартный `IActorSystem` — реестр ConcurrentDictionary, регистрация фабрик, маршрутизация, CancelTurn |
| `InMemoryEventStore` | Безблокировочное in-memory хранилище — на основе ConcurrentDictionary |
| `ActorConfig` | POCO конфигурации — связывается из PicoCfg |
| `ActorSystemOptions` | Параметры — обязательный EventStore, необязательный Logger, необязательный DomainEventPublisher; используется конструктором `ActorSystem` |
| `MediatorDomainEventPublisher` | `IDomainEventPublisher` по умолчанию — публикует `DomainEventEnvelope` для каждого события с изоляцией по событиям |
| `PicoActorDiExtensions` | Метод расширения `AddPicoActor()` для PicoDI |

### Actor (не-ES)

Наследуйте `Actor` для чисто операционных in-memory акторов.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Event Sourcing Actor

Наследуйте `EventSourcedActor`. Переопределите `OnMessageAsync` для вызова
`RaiseEvent` и `Mutate` для применения изменений состояния.

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

### Конвейер Persist-then-Mutate

```
OnMessageAsync → RaiseEvent (только запись, без изменения состояния)
              → AppendAsync (сохранить в IEventStore)
              → Mutate (применить события к состоянию в памяти)
              → Ответ вызывающей стороне (только после успеха)
```

Если `AppendAsync` завершается с ошибкой, незафиксированные события
отбрасываются, а `Version` откатывается. Actor **не отравляется** —
следующее сообщение обрабатывается нормально.

### Обмен сообщениями: Ask vs Send

| Шаблон | Метод | Семантика |
|---------|--------|-----------|
| Запрос-Ответ | `AskAsync<TResult>(id, command)` | Возвращает результат после обработки сообщения |
| Fire-and-Forget | `Send(id, command)` | Без ответа; исключения направляются в UnhandledErrorHandler |

### OutputChannel

Акторы транслируют сообщения `ActorOutputEvent` внешним подписчикам:

```csharp
counter.OutputWriter = channel.Writer;
// Внутри OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Отмена длительных операций без остановки актора:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... длительная работа с _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
```

### Spawn

Акторы создают дочерние акторы через ссылку `System`:

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

## Интеграция модулей

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Регистрирует IActorSystem + InMemoryEventStore

// Пользовательское хранилище событий
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Из PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` регистрируется как **Singleton**. Если зарегистрирован
`ILoggerFactory`, логгер автоматически внедряется.

### Пользовательский IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT с проверкой конкурентности */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT с сортировкой по версии */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* SELECT первого события (перечисление для восстановления) */ }
}
```

### Вывод событий (PicoMediator)

`IDomainEvent : IEvent` — доменные события являются уведомлениями первого класса для PicoMediator. После persist+mutate фреймворк публикует их как **конверты контекста** через хук `IDomainEventPublisher`; replay (восстановление) не публикует повторно.

`MediatorDomainEventPublisher` — готовый адаптер: оборачивает каждое событие в `DomainEventEnvelope(actorId, version, event)` и публикует через `Publish<DomainEventEnvelope>` (обобщение времени компиляции, безопасно для AOT) с изоляцией по событиям — сбой одного подписчика не блокирует последующие события.

### Подписка на доменные события (declare-and-subscribe)

Обработчики событий — это простые классы, реализующие `IDomainEventSubscriber<TEvent>` — PicoActor.Gen (встроенный в PicoActor.Abs) сканирует и автоматически регистрирует их; ноль ручной настройки. Обработчик получает типизированный конверт с контекстом исходного агрегата (`ActorId`, `Version`) плюс узкий порт `ICommandSender`:

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

// Подключение: AddPicoMediator регистрирует IMediator; AddPicoActor() обнаруживает его в
// фабрике ActorSystem и подключает вывод событий (лениво — совместимо со Scoped).
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe: подписчики регистрируются автоматически
container.AddPicoActor();     // автоматически подключает MediatorDomainEventPublisher
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// Явное подключение для пользовательских publisher: AddPicoActor(IPublisher)
// (экземпляр должен быть доступен до Build()).
```

> **Локальная разработка (ProjectReference):** анализаторы не распространяются по цепочкам `ProjectReference` — проектные потребители должны добавить прямую ссылку на `PicoActor.Gen` (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, зеркало `tests/PicoActor.Tests`). Потребители NuGet получают генератор автоматически через props `buildTransitive` пакета `PicoActor.Abs` — дополнительная ссылка не нужна.

События выходят как конверты через PicoMediator после persist+mutate; replay никогда не публикует. Сбои обработчика никогда не влияют на актора (изоляция по обработчикам). Циклы перевода (событие → команда → событие) являются предназначенным использованием — держите обработчики идемпотентными и ограниченными.

> **Разрушающее изменение:** прямые подписчики `ISubscriber<TEvent>` (PicoMediator) больше не получают доменные события PicoActor. Мигрируйте на `IDomainEventSubscriber<TEvent>`; `ActorId`/`Version` конверта заменяют любой вручную встроенный id агрегата. Пользовательские publisher (`AddPicoActor(IPublisher)`) теперь получают экземпляры `DomainEventEnvelope` вместо сырых событий — адаптируйте реализации `Publish<TEvent>` соответствующим образом (изменилась только наблюдаемая форма полезной нагрузки; конвейер актора не затронут).

Примечания:
- **Перевод событие→команда — обязанность подписчика (бизнес-слоя)** — PicoActor только публикует; команды входят в акторы исключительно через mailbox.
- Публикация происходит **после persist+mutate** — сбой публикации не повреждает состояние актора (события уже долговечны).
- Восстановление молчаливо: replay не публикует повторно.
- События фреймворка (`SagaCompleted`/`SagaFailed`) можно типизированно подписать, как и любые другие события (Abs нацелен на net10.0).
- **Автоподключение безопасно из любого scope**: с PicoDI 2026.8.1 (E1) фабрики синглтонов используют внутренний корневой scope контейнера — автоматически подключённый IMediator живёт до освобождения контейнера.
- **Никогда не вызывайте `AskAsync` к публикующему агрегату из его собственного пути публикации** — исходный почтовый ящик занят сбросом события; запрос самозаблокируется. Запрашивайте read-проекции (отдельные акторы); `Send` к исходному агрегату безопасен (fire-and-forget).

---

## Философия дизайна (克制 / 专注 / 优雅 / 高效)

| Принцип | На практике |
|-----------|------------|
| **克制 (Restraint / Сдержанность)** | Без распределённого консенсуса, без деревьев супервизии — только Actors и Events. |
| **专注 (Focus / Сосредоточенность)** | Однопоточность на актор. Одно сообщение за раз. |
| **优雅 (Elegance / Элегантность)** | Persist-then-Mutate: состояние меняется только после сохранения. Откат автоматический. |
| **高效 (Efficiency / Эффективность)** | AOT-совместимость, ноль рефлексии, абстракции `net10.0`. |

---

## Сценарии использования

- **AI-агентные системы** — каждый AI-агент является актором с состоянием диалога
- **Оркестрация рабочих процессов** — акторы моделируют длительные бизнес-процессы
- **Состояние игрового сервера** — Event Sourcing акторы для состояния игрока/игры
- **Состояние IoT-устройств** — in-memory акторы с периодическими снимками

---

## Пакеты

| Пакет | Цель | Описание |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | Основные абстракции: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor` — плюс типы подписки (`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`) и встроенный анализатор `PicoActor.Gen` (declare-and-subscribe) |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Среда выполнения: `ActorSystem`, `InMemoryEventStore`, `MediatorDomainEventPublisher` (исходящие события-конверты), интеграция PicoDI (`AddPicoActor` автоматически подключает IMediator + ICommandSender) |

---

## Сравнение

| Возможность | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Только in-memory | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Абстракции netstandard2.0 | ❌ | ✅ | ✅ | ❌ |
| Интеграция PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Распределённый / Кластеризация | ❌ | ✅ | ✅ | ✅ |
| Однопоточность на актор | ✅ | ✅ | ✅ | ❌ |
| Пакетов | 4 | 8+ | 3+ | 10+ |

---

## Лицензия

MIT — см. [LICENSE](LICENSE).
