<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

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
| Размер зависимостей | ❌ Akka.NET (8+ пакетов), Orleans (10+ пакетов) | ✅ 1 пакет — PicoActor (включает PicoActor.Abs и абстракции PicoDI/PicoLog/PicoMediator); PicoMediator — для подписчиков, PicoDI — для контейнера |
| Интеграция DI | ❌ Привязан к Microsoft.Extensions.DI | ✅ Нативный PicoDI, разрешение без рефлексии |
| netstandard2.0 | ⚠️ Частичная поддержка в Akka.NET / Proto.Actor | ❌ Только net10.0 (среда PicoMediator требует net10.0+) |
| Кривая обучения | ❌ Крутая — деревья супервизии, кластеризация, remoting | ✅ Минимальная — Actor + Event + Mailbox |

---

## Быстрый старт

```bash
dotnet add package PicoActor
dotnet add package PicoActor.EventStore.InMemory   # хранилище теперь отдельный пакет (v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

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
| `Actor` | Абстрактный базовый класс — почтовый ящик, цикл обработки, SignalReady, StopAsync |
| `EventSourcedActor` | ES-база — RaiseEvent, Mutate, конвейер Persist-then-Mutate |
| `SagaActor` | ES-координатор с конечным жизненным циклом — терминальные события фреймворка (SagaCompleted/SagaFailed), авто-остановка, явное пакетное восстановление через ResumeInterruptedSagasAsync |
| `IDomainEventSubscriber<TEvent>` | Контракт подписчика — типизированный конверт + `ICommandSender`; автоматически регистрируется PicoActor.Gen (declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | Конверт контекста — `ActorId`, `Version`, `Event` (транспорт / типизированная доставка) |
| `ICommandSender` | Узкий порт команд для обработчиков — Send, AskAsync, ExecuteSaga |
| `Envelope` | Внутренний — оборачивает ICommand с опциональным TaskCompletionSource |
| `ActorOutputEvent` | Исходящее уведомление — Type, Data, опциональные TurnId |
| `ConcurrencyException` | Выбрасывается IEventStore при несовпадении версий |

### PicoActor — Среда выполнения

Цель `net10.0`, AOT-совместим.

| Тип | Роль |
|------|------|
| `ActorSystem` | Стандартный `IActorSystem` — реестр ConcurrentDictionary, регистрация фабрик, маршрутизация |
| `InMemoryEventStore` | Переехало в **`PicoActor.EventStore.InMemory`** (v2026.9) — хранилище в памяти: блокировка на поток, оптимистичная конкурентность (на основе ConcurrentDictionary); регистрируется через `AddPicoActorInMemoryStore()` |
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
`Mutate` должен быть чистой функцией и никогда не бросать исключений; если это произошло, пакет уже персистентен, и актор **останавливается** (удаляется из реестра), а не продолжает работу с недостоверным состоянием.

### Snapshot Acceleration (opt-in) (Ускорение через снимки, опционально)

Для длинных потоков повторное воспроизведение всех событий при каждой пересборке состояния — это и есть
стоимость восстановления.
Снимок (snapshot) — это непрозрачная полезная нагрузка, которую актор сохраняет каждые N персистентных событий, так что
пересборка загружает его и воспроизводит только хвост. Снимки — **только** ускорение:
поток событий никогда не усекается, на один агрегат хранится не более одного
снимка, а отсутствующий, повреждённый или выходящий за диапазон снимок приводит к
полному воспроизведению.

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

- Триггер: каждые N **персистентных** событий (считаются по событиям, а не по флешам).
- Формат передачи: `IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`;
  `version` равен числу событий, которые покрывает нагрузка (`version == event count`).
- Сбои никогда не влияют на команду: исключение в `TakeSnapshot`, сбой хранилища или
  исключение в `RestoreSnapshot` сообщаются через `OnSnapshotFailed` /
  предупреждение в логе, а восстановление откатывается к воспроизведению всего потока.
- `IsReplaying` объявлен как `protected`: переопределите публикацию read-модели, управляемую `Mutate`,
  с помощью `if (IsReplaying) return;`, чтобы избежать работы O(n) на каждое воспроизведённое событие
  (иначе пересборка будет O(n²)).
- Тег схемы: переопределите `SnapshotSchema` (по умолчанию `null`) строкой версии, например
  `"CounterState/v1"`, и **повышайте её при любом изменении раскладки нагрузки или её
  смысла**. Изменение тега один раз обесценивает все сохранённые снимки — одно полное
  воспроизведение — а обычная скорость возвращается после того, как агрегат запишет ещё
  `SnapshotEveryNEvents` событий. Держите его константой типа: значение уровня экземпляра,
  различающееся у записывающего и восстанавливающего актора, молча превращает каждое
  восстановление в полное воспроизведение.
- При ненулевом теге фреймворк оборачивает нагрузку в конверт и сравнивает теги при
  восстановлении; несовпадение (или нагрузка без конверта, когда актор объявил тег)
  отбрасывает снимок и воспроизводит весь поток. Только `null` отключает защиту — `""` это
  настоящий тег. Тег длиннее 255 байт UTF-8 вообще не пишет снимок (сообщается через
  `OnSnapshotFailed`).
  Хранилище должно по-прежнему трактовать нагрузку как **непрозрачные байты**: не разбирать
  её и не предполагать, что она равна выходу `TakeSnapshot()`.

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader) (интерфейс чтения журнала событий)

Прикладной код пишется против `IActorSystem` и получает актеров; ни один член прикладной
поверхности не возвращает коллекцию событий. `IEventStore` — это SPI персистентности для
**авторов хранилища**: сохранить добавленные события и вернуть их, а `LoadAsync` остаётся
резервным чтением рантайма. Инструменты и проекции читают через `IEventLogReader` —
`GetVersionAsync`, `OpenAsync`, `PeekFirstAsync` — где `OpenAsync` возвращает курсор только
вперёд:

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- Одно наблюдение: `End` — длина потока, полученная в `OpenAsync`, и она не меняется за время
  жизни курсора. Поэтому защита от отката и длина хвоста берутся из одного открытия, а курсор
  никогда не отдаёт больше `End`.
- Определение возможности — одно правило: `eventStore is IEventLogReader`. Хранилище,
  реализующее интерфейс чтения, позволяет пересборке с ускорением снимком читать только хвост
  после снимка (файловое хранилище может классифицировать голову и десериализовать только
  хвост); хранилище без него продолжает работать через `LoadAsync`, который читает и
  десериализует весь поток.
- `InMemoryEventStore` реализует `IEventLogReader`; для любого другого хранилища это
  необязательно и чисто аддитивно.

### JSONL Event Store (opt-in) (хранилище событий JSONL, опционально)

`PicoActor` исполняет акторов; собственного хранилища он не содержит. Его поставляют два пакета:
`PicoActor.EventStore.InMemory` (хранилище в памяти по умолчанию, регистрируется через
`AddPicoActorInMemoryStore()`) и `PicoActor.EventStore.Jsonl` (долговременное — о нём этот
раздел).

`PicoActor.EventStore.Jsonl` хранит один файл JSONL только-дописывания на агрегат
(`<root>/<aggregateId>.jsonl`, один JSON-объект в строке), а снимки — как атомарные sidecar-файлы
в соседнем каталоге `<root>-snapshots`. Он реализует `IEventStore`, `IEventLogReader`,
`IEventStoreSnapshot` и `IEventStoreEnumerator`, безопасен для AOT (генерация исходников
PicoJetson) и работает **в одном процессе**: два экземпляра хранилища над одним каталогом не
разделяют шлюзы, поэтому параллельные писатели падают явно, а не рвут поток.

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

- **Пропущенная регистрация видна на сборке, а не на первом append**: пакет содержит анализатор
  `PICOSTORE001`, который отмечает доменное событие, не перечисленное ни одним
  `[PicoDerivedType]` (проект, использующий только хранилище в памяти, за это не платит).
- **Оборванный хвост и повреждение — разные вещи.** Последняя строка без перевода строки —
  единственная форма, которую оставляет падение процесса — игнорируется, не считается и не
  бросает исключение; следующий append её обрезает. **Завершённая** строка, которую не удаётся
  десериализовать, — это повреждение, и оно бросает исключение (включая последнюю строку), а
  хранилище её не чинит, потому что починка была бы догадкой. Это документированная цена
  `FlushToDisk` по умолчанию `false`: байты доходят до ОС (другой читатель видит событие сразу),
  но не до диска, поэтому сбой питания может оставить завершённую мусорную строку. Установите
  `FlushToDisk = true`, чтобы делать fsync на каждый append.
- **Курсор и перестройка только хвоста.** `GetVersionAsync`, `End` курсора и его позиционирование
  по байтам берутся из одного дешёвого прохода (полные, непустые, завершённые строки — без
  проверки JSON, без десериализатора), поэтому перестройка с ускорением снимком открывается на
  `snapshot.Version + 1` и десериализует только хвост (`N − v` событий). Экономия — на
  десериализации и аллокациях, а не на дисковом I/O: голову всё равно читают один раз, как байты.
  Проба «пропуска» в сериализаторе не нужна — в общем потоке только события, поэтому нечитаемая
  строка это повреждение, а не чуждая запись.
- `MaxAggregateLocks` (по умолчанию `1024`) ограничивает таблицу шлюзов на агрегат; вытесненная
  запись стоит одного повторного прохода при следующем append этого агрегата, но никогда —
  корректности. `Logger` не зависит от `ILoggerFactory` контейнера: передайте тот же logger
  явно, чтобы видеть предупреждения хранилища в своём логе.
- Вне этого релиза: многопроцессные писатели, архивирование/хранение/компакция, чужие
  (не-событийные) строки, шифрование/сжатие и второй кодек для событий фреймворка.

#### Stream naming (`FileNameFormat`)

`JsonlEventStoreOptions.FileNameFormat` выбирает, **как называется файл** потока; формат строк
одинаков в обоих режимах, поэтому меняются только имена — существующий каталог остаётся читаемым.

| Значение | Файл потока | Что это даёт |
|---|---|---|
| `IdOnly` (по умолчанию) | `<id:D>.jsonl` | историческая раскладка, воспроизведённая побайтово — миграция не нужна, существующий каталог не меняется |
| `TypePrefix` | `<type>-<id:D>.jsonl` | тип едет в имени, поэтому `ListAggregateIdsAsync` восстанавливает id только по именам файлов: **ноль открытий файлов** на перечисление |

`{type}` — это `events[0].GetType().Name`, конкретный тип **первого сохранённого события** потока
(у обобщённого типа сохраняется арность с обратным апострофом), и он **фиксируется при создании**:
последующий пакет, первое событие которого имеет другой тип, всё равно дописывается в имя, с которым
поток родился.

- **Переход на `TypePrefix`.** PicoAgent уже называет свои потоки `{type}-{id}.jsonl`, поэтому
  достаточно указать этот store на такой каталог с `FileNameFormat = TypePrefix` — **никакого
  переименования не требуется**: каталог читается как есть, а снимки переносятся без изменений (они
  привязаны к id агрегата, `<id:D>.snap`, и никогда к имени потока). Обратного направления нет:
  префикс — часть имени, `IdOnly`-store не может вывести его из id и должен был бы переименовать
  каждый файл.
- **Смешанный каталог диагностируется, но никогда не сливается.** Записи, допустимые только в
  *другой* раскладке, учитываются (`ForeignFormatFiles`, по **именам** и монотонно: это ворота защиты
  от дубликатов в `IdOnly`) и сообщаются как **одно** предупреждение на проход (обнаружение
  симметрично: `<id>.jsonl` при `TypePrefix` и `<type>-<id>.jsonl` при `IdOnly` оба считаются «другим
  форматом»). Диагностика всего каталога считает **истории**, а не имена: каталог, в котором **ноль**
  историй настроенного формата и **несколько непустых** историй другого, бросает
  `InvalidOperationException` с обоими счётчиками, обоими форматами и выходами — из перечисления, из
  конструктора и из lazy pass одинаково — вместо сообщения о пустой системе; а вот каталог из одних
  остатков нулевой длины (неудачное добавление оставляет такой) — это **пустое** хранилище, и он
  молчит. Поток *другого типа события* — не смесь: это нормальный случай, он не тратит ни счётчик, ни
  строку лога. Добавление для id, у которого уже есть непустой поток в другой раскладке — или два
  разных префикса, — отклоняется до записи хотя бы одного байта.
- **Стоимость.** `TypePrefix` платит один проход по именам файлов при создании (чтобы построить карту
  id → тип), ещё один на каждый неразрешённый поиск и один `stat` на каждое добавление для защиты от
  дубликатов. `IdOnly` не платит **ничего** при создании и ничего за добавление в уже существующий
  поток; его единственный проход по каталогу ленивый — при первом наблюдении потока, которого у него
  нет, и больше никогда. Пустой пакет ничего не пишет и не создаёт ни в одном режиме, но разрешает свой
  путь **точно так же, как пишущее добавление**, и по-прежнему проверяет `expectedVersion`.
- **Кэш промахов обслуживает только ПУТЬ ЧТЕНИЯ.** Промах `TypePrefix` запоминается — ограниченный
  набор на 4 096 id, который при переполнении очищается целиком, а не растёт — и разделяется
  **single-flight**: серия **одновременных опросов ОДНОГО и того же** отсутствующего id делит один
  проход, а **повторы этого id** бесплатны; **другой** отсутствующий id платит собственный проход.
  **Добавление этому знанию не доверяет никогда**: разрешить там «потока нет» означало бы создать
  второе имя рядом с потоком, который внешний писатель создал уже после построения карты. Поэтому
  добавление при промахе **сканирует заново**, а при успехе отбрасывает только **свою собственную**
  запись — никогда целиком и не беря ворот сканирования. Цена названа прямо: *чтение* может ответить
  «пусто» для потока, появившегося рядом, до следующего прохода — а хост, который больше никогда не
  делает проход, видит устаревший ответ, пока **новый экземпляр** не перечитает каталог.
  `NegativeCacheInvalidations` считает записи, которые добавление действительно опровергло (под
  `IdOnly` — тождественно `0`, потому что там промах не запоминается вовсе).
- **Устаревшую карту исправляет СЛЕДУЮЩЕЕ сканирование.** Конкурирующий префикс, появившийся после
  построения карты, не находит ни чтение (попадание в карту не платит прохода), ни добавление: магазин
  не добавляет проход по каталогу на путь добавления, чтобы закрыть это окно, — это вернуло бы ровно то,
  что даёт карта, на единственном пути, который платит на каждое добавление. Следующий проход
  классификации — перечисление или промах неизвестного id — фиксирует конфликт, и с этого момента любое
  добавление для этого id отклоняется с указанием обоих файлов.
- **Сломанный корень или занятое имя называются, а не поглощаются.** Корень, существующий как **файл**,
  бросает `IOException` (там нельзя ни перечислить, ни открыть, ни создать); ИМЯ потока, занятое
  **каталогом**, бросает `InvalidOperationException` с указанием препятствия — и на пути чтения, и на
  пути добавления, в обеих раскладках — вместо «пустого агрегата» или голого нарушения совместного
  доступа. Неудачный lazy pass (неверная конфигурация, нечитаемый корень, корень-файл) **защёлкивается
  (latch)**: то же исключение бросается при каждом последующем наблюдении, а каталог больше никогда не
  сканируется, поэтому единственный способ восстановления — **новый экземпляр** после починки каталога.

#### Framework events (event type bindings) (события фреймворка: привязки типов)

`SagaCompleted`/`SagaFailed` — `sealed`, наследуются от `IDomainEvent`, а не от вашего `TEventBase`, поэтому
в потоке saga лежит тип, которого ваш сериализатор никогда не видел.
`JsonlEventStoreOptions.EventTypeBindings` направляет события фреймворка в тот же store — фреймворк заранее
регистрирует `SagaCompleted → PicoActorSagaCompleted` и `SagaFailed → PicoActorSagaFailed` (`TryAdd`,
поэтому ваш собственный `Add<T>(tag)` побеждает; `FrameworkDefaultCount` — сколько умолчаний реально
применено, `0…2`, `RegisteredCount` — всего).

- **`Add<T>(tag)` только маршрутизирует, ничего не генерирует.** Tag — это `$type` строки и то, что
  вынюхивает читающая сторона (первый член, без учёта регистра, при неузнавании — безусловный откат на
  `TEventBase`), но типу всё равно нужен сгенерированный сериализатор. `[GenerateSerializer(typeof(T))]`
  на **positional record** не работает (конвейер атрибутов PicoSerDe не видит первичный конструктор:
  `CS7036`/`CS8852`); работающая форма — **закрытая точка вызова**, литеральные вызовы в
  `FrameworkEventSerializers.Generate()`. Привязанный тип без точки генерации отклоняется при **первой
  записи** (`InvalidOperationException`, в сообщении — рецепт).
- **Смешанные пакеты — одна запись:** `TEventBase` и привязанные события делят пакет — один буфер, одна
  запись, один flush.
- **Именование.** Два тега фреймворка — это теги, которые уже есть в производственных корпусах, и
  постоянный формат провода (без alias). Поток, как и раньше, называется по короткому CLR-имени первого
  события; при `TypePrefix` имя вне любого зарегистрированного префикса **типа**
  (`unknown-<id>.jsonl`, собственная категоризация хоста) **достижимо по id**, невидимо для перечисления по типу, не попадает в `ForeignFormatFiles` и
  никогда не вызывает исключение по всему каталогу. Префикс не содержит `-`; одинаковые короткие имена
  делят один префикс.
- **`SagaCompleted.Result` — это `object?`:** без потерь туда и обратно проходят только `null` (единственная
  форма, которую производит фреймворк) и `string`; любой другой объект строкифицируется через `ToString()`
  — с потерей, необратимо. Для потока saga персистентность обещает `null`/`string`, и не более.
- **Правила регистрации.** Привязки живут в экземпляре options —
  `options.EventTypeBindings.Add<T>(tag)` **до** конструирования store: конструктор **замораживает и
  копирует** таблицу, поэтому более поздний `Add` выбрасывает исключение, а не игнорируется молча. У типа
  ровно один tag, и tag называет ровно один тип (та же пара повторно — no-op; tag не пустой и не содержит
  кавычки, обратной косой черты или управляющего символа). База, которая уже покрывает события фреймворка
  (сам `IDomainEvent`), отклоняется при конструировании — иначе они были бы одновременно обычными и
  привязанными событиями; то же относится к привязанному типу, приводимому к `TEventBase`.

### Idle Passivation (opt-in) (Пассивация простоя, опционально)

Акторы остаются в памяти до остановки. Для нагрузок со множеством долгоживущих
агрегатов бюджет простоя освобождает материализованное состояние акторов, которые можно
восстановить:

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- Кандидаты — **только восстановимые event sourcing акторы**: `EventSourcedActor`,
  конкретный тип которого был зарегистрирован **с фабрикой
  восстановления**. Акторы, не использующие ES, не затрагиваются (их состояние нельзя восстановить).
- Кандидат должен быть инициализирован, не обрабатывать команду, иметь пустой
  почтовый ящик и простаивать в течение всего бюджета. Остановка проходит через
  почтовый ящик как сентинел фреймворка, поэтому она попадает на границу сообщений; запрос
  остановки проверяется на принадлежность, так что устаревший запрос не может удалить более новый
  экземпляр того же id.
- `SagaActor` исключён (`CanBePassivated => false`): ожидание внешнего
  ответа выглядит как простой, и его освобождение потеряло бы этот ответ.
- Реактивация явная: `GetAsync<T>(id)` воспроизводит поток (или загружает
  снимок и воспроизводит хвост). До этого `Send`/`AskAsync` ведут себя
  точно как с любым незарегистрированным id (`KeyNotFoundException`) — у `AskAsync` нет
  параметра типа, поэтому он не может выбрать фабрику восстановления.
- Наблюдаемость: `SweepIdleActorsAsync()`, `PassivatedCount`, `ReactivatedCount`,
  и строка лога на каждое восстановление с числом воспроизведённых событий.

### SagaActor (Координатор с конечным временем жизни)

`SagaActor extends EventSourcedActor` — для межагрегатных операций с конечным жизненным циклом. В отличие от обычного EventSourcedActor, чей почтовый ящик работает вечно, SagaActor **автоматически останавливается** после того, как фреймворк персистит его терминальное событие.

Терминальное состояние **генерируется фреймворком**: `MarkComplete(result)` добавляет событие `SagaCompleted(result)` в тот же батч, что и ваши бизнес-события (атомарное добавление — завершение и персистентность не могут разойтись); необработанное бизнес-исключение добавляет `SagaFailed(reason)` («ExceptionType: message», обрезается до 512 символов) и пробрасывает `SagaExecutionException(Id, Reason)` вызывающему. Подклассы никогда не порождают и не обрабатывают эти события — `Mutate` видит только бизнес-события.

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

**Жизненный цикл:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

Бизнес-сбой: если `OnMessageAsync`/`ResumeAsync` бросает исключение, фреймворк отбрасывает незафиксированные бизнес-события, добавляет `SagaFailed(reason)`, автоматически останавливается и возвращает сбой `SagaExecutionException(Id, Reason)` вызывающему через Ask. Инфраструктурные сбои (хранилище недоступно) **не** являются терминальными — события откатываются, сага остаётся в состоянии Running.
Если и сохранение `SagaFailed` не удаётся (хранилище всё ещё недоступно), терминальное состояние не создаётся, а вызывающему пробрасывается **исходное** исключение.

**Восстановление после сбоя:** `GetAsync` воспроизводит события → фреймворк восстанавливает `Completed`/`Failed` из `SagaCompleted`/`SagaFailed` (терминальные саги остаются мёртвыми — `GetAsync` возвращает null). Саги без терминального события получают вызов `ResumeAsync()`; если он достигает терминального состояния, фреймворк персистит `SagaCompleted` в том же флеш-батче. Восстановление — явный pull, никакой фоновой магии.

**Явное пакетное восстановление:**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` перечисляет саги по имени типа первого события, восстанавливает каждую через `GetAsync` в режиме single-flight и возвращает пост-восстановительную классификацию: `Completed` / `Failed` (с причиной, восстановленной фреймворком) / `Running` (всё ещё ждёт внешнего ввода). Уже терминальные саги никогда не воскрешаются; живые классифицируются на месте. Идемпотентно и безопасно для повторных попыток (single-flight на id); сбой хранилища приводит к быстрому отказу, чтобы вызывающий мог повторить весь пакет.

**Паттерн process manager:**

тот же базовый класс покрывает и процесс-менеджеры — внешние события переводятся в команды обработчиком событий уровня приложения (например, подписчиком PicoMediator), который отправляет их в почтовый ящик саги. События никогда не попадают напрямую в акторов; сага видит только команды.

**Удобный API:**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` выполняет `CreateAsync` + `AskAsync` одним вызовом. Возвращает `SagaExecution<TResult>(Id, Result)`; при бизнес-сбое бросает `SagaExecutionException(SagaId, Reason)`, так что вызывающий всегда получает id саги.

### Обмен сообщениями: Ask vs Send

| Шаблон | Метод | Семантика |
|---------|--------|-----------|
| Запрос-Ответ | `AskAsync<TResult>(id, command)` | Возвращает результат после обработки сообщения |
| Fire-and-Forget | `Send(id, command)` | Без ответа; сбои направляются в UnhandledErrorHandler (без logger — в stderr); исключение при неизвестном акторе или закрытом mailbox (гонка остановки) |

### OutputChannel

Акторы транслируют сообщения `ActorOutputEvent` внешним подписчикам:

```csharp
counter.OutputWriter = channel.Writer;
// Внутри OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
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
container.AddPicoActor();               // Регистрирует IActorSystem — без хранилища
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory: хранилище в памяти по умолчанию

// Пользовательское хранилище событий
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Из PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` регистрируется как **Singleton**. Если зарегистрирован
`ILoggerFactory`, логгер автоматически внедряется.

`AddPicoActor()` больше не регистрирует хранилище и не падает при регистрации: исключение
`InvalidOperationException` бросает **разрешение** `IActorSystem`, называя
`AddPicoActorInMemoryStore()` (поэтому порядок регистрации остаётся свободным, а явный
`AddPicoActor(store)` по-прежнему выигрывает).

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

Обработчики можно объявлять как class или record. Реализация без доступного (public/internal) конструктора экземпляра падает на сборке с **PICA001** вместо генерации нерабочего кода регистрации.

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

> **Необходимые пакеты:** помимо `PicoActor` (он включает `PicoActor.Abs` и абстракции `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator`) нужен пакет хранилища: `PicoActor.EventStore.InMemory` для работы в памяти (регистрация через `AddPicoActorInMemoryStore()`) или `PicoActor.EventStore.Jsonl` для долговременного хранения (PicoJetson включён). Для обработчиков `IDomainEventSubscriber<TEvent>` дополнительных пакетов не нужно: `PicoMediator` уже является зависимостью `PicoActor` (сгенерированный bridge вызывает его `MediatorAutoSubscriptionRegistry`). `PicoDI` + `PicoMediator.DI` нужны только для контейнера (`SvcContainer`, `AddPicoMediator`).

> **Локальная разработка (ProjectReference):** анализаторы не распространяются по цепочкам `ProjectReference` — проектные потребители должны добавить прямую ссылку на `PicoActor.Gen` (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, зеркало `tests/PicoActor.Tests`). Потребители NuGet получают генератор автоматически через props `buildTransitive` пакета `PicoActor.Abs` — дополнительная ссылка не нужна.

События выходят как конверты через PicoMediator после persist+mutate; replay никогда не публикует. Сбои обработчика никогда не влияют на актора (изоляция по обработчикам). Циклы перевода (событие → команда → событие) являются предназначенным использованием — держите обработчики идемпотентными и ограниченными.

> **Разрушающее изменение:** прямые подписчики `ISubscriber<TEvent>` (PicoMediator) больше не получают доменные события PicoActor. Мигрируйте на `IDomainEventSubscriber<TEvent>`; `ActorId`/`Version` конверта заменяют любой вручную встроенный id агрегата. Пользовательские publisher (`AddPicoActor(IPublisher)`) теперь получают экземпляры `DomainEventEnvelope` вместо сырых событий — адаптируйте реализации `Publish<TEvent>` соответствующим образом (изменилась только наблюдаемая форма полезной нагрузки; конвейер актора не затронут). `IEventStoreEnumerator.ListAggregateIds(string)` стал асинхронным `ListAggregateIdsAsync(string)` — пользовательским перечислителям нужно обновить сигнатуру.

Примечания:
- `Register<T>` вызывается один раз на тип актора: повторная регистрация теперь выбрасывает исключение, а не молча заменяет первую фабрику.
- `Register<T>` отклоняет типы, которые рантайм не может обслуживать: реализации, не наследующие `Actor`, и реализации `IEventSourcedActor`, не наследующие `EventSourcedActor` (персистентность подключается только для конкретных базовых классов).
- `StopAsync`/`RequestStop` сначала удаляют актор из реестра, затем обрабатывают уже буферизованные в mailbox сообщения (graceful stop); отправленные после остановки сообщения завершаются `KeyNotFoundException` (или `InvalidOperationException`, если mailbox уже закрыт — окно гонки остановки).
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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Среда выполнения: `ActorSystem`, `MediatorDomainEventPublisher` (исходящие события-конверты), интеграция PicoDI (`AddPicoActor` автоматически подключает IMediator + ICommandSender) |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | Хранилище в памяти (`InMemoryEventStore`, вынесено из `PicoActor` в v2026.9) + `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | Долговременное хранилище событий JSONL: только-дописывание, оптимистичная конкурентность, перестройка только хвоста через `IEventLogReader`, атомарные sidecar-снимки, безопасность для AOT (PicoJetson) — плюс встроенный анализатор регистрации `PICOSTORE001` |

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
| Пакетов | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## Лицензия

MIT — см. [LICENSE](LICENSE).
