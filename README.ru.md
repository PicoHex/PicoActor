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
| Размер зависимостей | ❌ Akka.NET (8+ пакетов), Orleans (10+ пакетов) | ✅ 2 пакета, ноль зависимостей кроме Channels |
| Интеграция DI | ❌ Привязан к Microsoft.Extensions.DI | ✅ Нативный PicoDI, разрешение без рефлексии |
| netstandard2.0 | ⚠️ Частичная поддержка в Akka.NET / Proto.Actor | ✅ Абстракции нацелены на netstandard2.0 |
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
var system = new ActorSystem(store);

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

`CreateAsync<T>(cmd, id)` создаёт actor с id, предоставленным вызывающим кодом — используйте, когда id должен быть известен до создания actor (детерминированная id / восстановление саги). Выбрасывает исключение, если id уже зарегистрирован.

---

## Детали модуля

### PicoActor.Abs — Основные абстракции

Цель `netstandard2.0` для максимальной совместимости.

| Тип | Роль |
|------|------|
| `IActor` | Базовый интерфейс — предоставляет `Id` (UUID v7) |
| `IActorSystem` | Контракт времени выполнения — Register, CreateAsync, CreateAsync(id), GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | Маркерный интерфейс для команд |
| `IDomainEvent` | Маркерный интерфейс для доменных событий |
| `IEventSourcedActor` | Опциональный — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Контракт хранения — AppendAsync (оптимистичная конкурентность), LoadAsync |
| `ICancelable` | Опциональный — CancelCurrentTurn для длительных операций |
| `Actor` | Абстрактный базовый класс — почтовый ящик, цикл обработки, SignalReady, StopAsync |
| `EventSourcedActor` | ES-база — RaiseEvent, Mutate, конвейер Persist-then-Mutate |
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
| `ActorSystemOptions` | [Устарело] Опции с IEventStore и ILogger — не используется; используйте конструктор |
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
}
```

---

## Философия дизайна (克制 / 专注 / 优雅 / 高效)

| Принцип | На практике |
|-----------|------------|
| **克制 (Restraint / Сдержанность)** | Без распределённого консенсуса, без деревьев супервизии — только Actors и Events. |
| **专注 (Focus / Сосредоточенность)** | Однопоточность на актор. Одно сообщение за раз. |
| **优雅 (Elegance / Элегантность)** | Persist-then-Mutate: состояние меняется только после сохранения. Откат автоматический. |
| **高效 (Efficiency / Эффективность)** | AOT-совместимость, ноль рефлексии, абстракции `netstandard2.0`. |

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Основные абстракции: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Среда выполнения: `ActorSystem`, `InMemoryEventStore`, интеграция PicoDI |

---

## Сравнение

| Возможность | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Только in-memory | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Абстракции netstandard2.0 | ✅ | ✅ | ✅ | ❌ |
| Интеграция PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Распределённый / Кластеризация | ❌ | ✅ | ✅ | ✅ |
| Однопоточность на актор | ✅ | ✅ | ✅ | ❌ |
| Пакетов | 2 | 8+ | 3+ | 10+ |

---

## Лицензия

MIT — см. [LICENSE](LICENSE).
