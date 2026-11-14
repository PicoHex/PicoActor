# PicoActor

AOT-kompatibles In-Memory-Actor-Framework mit Event Sourcing für .NET.
Leichtgewichtig, ohne Reflection, entwickelt für KI-Agentensysteme und
Workflow-Orchestrierung. Läuft unter NativeAOT und Trimming.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## Berechnungsmodell

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

Jeder Actor besitzt eine **Mailbox** (In-Memory `Channel<Envelope>`), eine
**UUID v7**-Identität und eine single-threaded Konsumschleife. Befehle werden
über `Send` (Fire-and-Forget) oder `AskAsync` (Request-Reply) zugestellt.

PicoActor ist **nachrichtengesteuert**: Jede Interaktion ist eine Nachricht. Befehle (`ICommand`) sind gezielte Nachrichten, die über die Mailbox zugestellt werden (1:1, Antwort optional); Domain-Ereignisse (`IDomainEvent`) sind Broadcast-Nachrichten, die über PicoMediator veröffentlicht werden (1:N, keine Antwort). Ereignisse sind ebenfalls Nachrichten — das Muster für die Zusammenarbeit zwischen Aggregaten ist eine einzige Schleife: Ereignis → Abonnent → übersetzter Befehl → Mailbox. Es gibt keine andere Möglichkeit, mit einem Actor zu interagieren.

Event-Sourcing-Actors folgen **Persist-then-Mutate** (Erst persistieren, dann mutieren):
`OnMessageAsync → RaiseEvent → In IEventStore persistieren → Zustand mutieren`.
Der Zustand wird nur nach erfolgreicher Persistenz geändert — der In-Memory-Zustand
ist immer konsistent mit dem Event-Stream.

---

## Warum PicoActor

| Aspekt | Bestehende Optionen | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans benötigen Reflection | ✅ Vollständige NativeAOT-Unterstützung |
| Event Sourcing | ❌ Proto.Actor, Orleans ohne integriertes ES | ✅ Persist-then-Mutate, automatischer Rollback |
| Abhängigkeitsgröße | ❌ Akka.NET (8+ Pakete), Orleans (10+ Pakete) | ✅ 1 Paket — PicoActor (enthält PicoActor.Abs sowie die PicoDI/PicoLog/PicoMediator-Abstraktionen inklusive der PicoMediator-Laufzeit, die der generierte Bridge-Code braucht); PicoDI nur für den Container |
| DI-Integration | ❌ An Microsoft.Extensions.DI gebunden | ✅ Natives PicoDI, auflösung ohne Reflection |
| netstandard2.0 | ⚠️ Teilweise Unterstützung in Akka.NET / Proto.Actor | ❌ Nur net10.0 (PicoMediator-Laufzeit erfordert net10.0+) |
| Lernkurve | ❌ Steil — Supervision-Bäume, Clustering, Remoting | ✅ Minimal — Actor + Event + Mailbox |

---

## Schnellstart

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Einrichtung
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Actor-Fabriken registrieren
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Erstellen, Nachrichten senden, stoppen, wiederherstellen
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Moduldetails

### PicoActor.Abs — Kernabstraktionen

Ziel `net10.0` (PicoMediator-Laufzeit und generierter Bridge-Code erfordern net10.0+).

| Typ | Rolle |
|------|------|
| `IActor` | Basisschnittstelle — stellt `Id` (UUID v7) bereit |
| `IActorSystem` | Laufzeitvertrag — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, RequestStop, StopAllAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | Markierungsschnittstelle für Befehle |
| `IDomainEvent` | Markierungsschnittstelle für Domänenereignisse |
| `IEventSourcedActor` | Optional — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Persistenzvertrag — AppendAsync (optimistische Nebenläufigkeit), LoadAsync, PeekFirstAsync |
| `Actor` | Abstrakte Basisklasse — Mailbox, Konsumschleife, SignalReady, StopAsync |
| `EventSourcedActor` | ES-Basis — RaiseEvent, Mutate, Persist-then-Mutate-Pipeline |
| `SagaActor` | ES-Koordinator mit begrenzter Lebensdauer — Framework-Terminalereignisse (SagaCompleted/SagaFailed), Auto-Stopp, explizite Batch-Wiederherstellung über ResumeInterruptedSagasAsync |
| `IDomainEventSubscriber<TEvent>` | Abonnentenvertrag — typisierter Envelope + `ICommandSender`; automatisch registriert durch PicoActor.Gen (declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | Kontext-Envelope — `ActorId`, `Version`, `Event` (Transport / typisierte Zustellung) |
| `ICommandSender` | Schmaler Befehlsport für Handler — Send, AskAsync, ExecuteSaga |
| `Envelope` | Intern — umschließt ICommand mit optionalem TaskCompletionSource |
| `ActorOutputEvent` | Ausgehende Benachrichtigung — Type, Data, optional TurnId |
| `ConcurrencyException` | Von IEventStore bei Versionsdivergenz ausgelöst |

### PicoActor — Laufzeit

Ziel `net10.0`, AOT-kompatibel.

| Typ | Rolle |
|------|------|
| `ActorSystem` | Standard-`IActorSystem` — ConcurrentDictionary-Registry, Fabrikregistrierung, Nachrichtenrouting |
| `InMemoryEventStore` | In-Memory-Speicher mit Sperre pro Stream — optimistische Nebenläufigkeit, ConcurrentDictionary-basiert |
| `ActorConfig` | Konfigurations-POCO — aus PicoCfg bindbar |
| `ActorSystemOptions` | Optionen — EventStore erforderlich, Logger optional, DomainEventPublisher optional; vom `ActorSystem`-Konstruktor verwendet |
| `MediatorDomainEventPublisher` | Standard-`IDomainEventPublisher` — veröffentlicht `DomainEventEnvelope` pro Ereignis mit Isolierung pro Ereignis |
| `PicoActorDiExtensions` | `AddPicoActor()`-Erweiterungsmethode für PicoDI |

### Actor (Nicht-ES)

Erben Sie von `Actor` für rein operative In-Memory-Actors.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Event-Sourcing-Actor

Erben Sie von `EventSourcedActor`. Überschreiben Sie `OnMessageAsync` zum
Aufruf von `RaiseEvent` und `Mutate` zum Anwenden von Zustandsänderungen.

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

### Persist-then-Mutate-Pipeline

```
OnMessageAsync → RaiseEvent (nur Aufzeichnung, keine Zustandsänderung)
              → AppendAsync (in IEventStore persistieren)
              → Mutate (Ereignisse auf In-Memory-Zustand anwenden)
              → Antwort an Aufrufer (nur nach Erfolg)
```

Schlägt `AppendAsync` fehl, werden nicht-committete Ereignisse verworfen und
`Version` wird zurückgesetzt. Der Actor wird **nicht vergiftet** — die nächste
Nachricht wird normal verarbeitet.
`Mutate` muss eine reine Funktion sein und darf nie werfen; wenn doch, ist der Batch bereits dauerhaft und der Actor wird **gestoppt** (aus der Registry entfernt), statt mit nicht vertrauenswürdigem Zustand weiterzulaufen.

### Snapshot Acceleration (opt-in) (Snapshot-Beschleunigung, optional)

Bei langen Streams sind das erneute Abspielen aller Ereignisse bei jedem Neuaufbau die Wiederherstellungskosten.
Ein Snapshot ist eine undurchsichtige Nutzlast, die der Actor alle N persistierten Ereignisse speichert, sodass ein
Neuaufbau ihn wiederherstellt und nur den Rest abspielt. Snapshots sind **ausschließlich** Beschleunigung:
Der Ereignisstrom wird nie abgeschnitten, pro
Aggregat wird höchstens ein Snapshot gehalten, und ein fehlender, beschädigter oder außerhalb des Bereichs liegender Snapshot fällt auf
ein vollständiges Replay zurück.

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

- Auslöser: alle N **persistierten** Ereignisse (pro Ereignis gezählt, nicht pro Flush).
- Wire-Format: `IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`;
  `version` entspricht der Anzahl der Ereignisse, die die Nutzlast abdeckt (`version == event count`).
- Fehler wirken sich nie auf einen Befehl aus: ein werfendes `TakeSnapshot`, ein fehlschlagender Store oder
  ein werfendes `RestoreSnapshot` wird über `OnSnapshotFailed` /
  ein Warn-Log gemeldet, und die Wiederherstellung fällt auf das Abspielen des gesamten Streams zurück.
- `IsReplaying` ist `protected`: überschreiben Sie die `Mutate`-gesteuerte Read-Model-Veröffentlichung
  mit `if (IsReplaying) return;`, um O(n)-Arbeit pro abgespieltem Ereignis zu vermeiden
  (andernfalls ist ein Neuaufbau O(n²)).
- Schema-Tag: überschreiben Sie `SnapshotSchema` (Standard `null`) mit einer Versionszeichenfolge
  wie `"CounterState/v1"` und **erhöhen Sie sie, sobald sich das Layout der Nutzlast oder deren
  Bedeutung ändert**. Eine Änderung macht alle gespeicherten Snapshots einmal ungültig — ein
  vollständiges Replay — und die normale Geschwindigkeit kehrt zurück, sobald das Aggregat
  weitere `SnapshotEveryNEvents` Ereignisse schreibt. Halten Sie es als Typkonstante: ein
  instanzbezogener Wert, der sich zwischen schreibendem und neu aufbauendem Actor unterscheidet,
  degradiert jede Wiederherstellung stillschweigend zu einem vollständigen Replay.
- Bei einem Nicht-`null`-Tag verpackt das Framework die Nutzlast in einen Umschlag und vergleicht
  die Tags bei der Wiederherstellung; eine Abweichung (oder eine Nutzlast ohne Umschlag, obwohl
  der Actor ein Tag deklariert) verwirft den Snapshot und spielt den gesamten Stream ab. Nur
  `null` schaltet den Schutz ab — `""` ist ein echtes Tag. Ein Tag mit mehr als 255 UTF-8-Bytes
  schreibt gar keinen Snapshot (gemeldet über `OnSnapshotFailed`).

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Idle Passivation (opt-in) (Idle-Passivierung, optional)

Actors bleiben im Speicher, bis sie gestoppt werden. Für Workloads mit vielen langlebigen
Aggregaten gewinnt ein Idle-Budget den materialisierten Zustand von Actors zurück, die
neu aufgebaut werden können:

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- Kandidaten sind **nur neu aufbaubare Event-Sourcing-Actors**: ein
  `EventSourcedActor`, dessen konkreter Typ **mit einer Neuaufbau-Factory** registriert wurde.
  Nicht-ES-Actors werden nie berührt (ihr Zustand kann nicht neu aufgebaut werden).
- Ein Kandidat muss initialisiert sein, keinen Befehl verarbeiten, eine leere
  Mailbox haben und über das gesamte Budget hinweg inaktiv sein. Der Stopp wandert durch die
  Mailbox als Framework-Sentinel und trifft daher auf eine Nachrichtengrenze; die Stopp-Anfrage
  wird auf Eigentümerschaft geprüft, sodass eine veraltete Anfrage keine neuere
  Instanz derselben id entfernen kann.
- `SagaActor` ist ausgenommen (`CanBePassivated => false`): das Warten auf eine externe
  Antwort sieht wie Inaktivität aus, und das Zurückgewinnen würde diese Antwort verwerfen.
- Reaktivierung ist explizit: `GetAsync<T>(id)` spielt den Stream erneut ab (oder stellt
  den Snapshot wieder her und spielt den Rest ab). Bis dahin verhalten sich `Send`/`AskAsync`
  genau wie bei jeder nicht registrierten id (`KeyNotFoundException`) — `AskAsync` hat keinen
  Typparameter und kann daher keine Neuaufbau-Factory wählen.
- Beobachtbarkeit: `SweepIdleActorsAsync()`, `PassivatedCount`, `ReactivatedCount`,
  und eine Log-Zeile pro Neuaufbau mit der Anzahl der abgespielten Ereignisse.

### SagaActor (Koordinator mit endlicher Lebensdauer)

`SagaActor extends EventSourcedActor` — für aggregatübergreifende Operationen mit endlichem Lebenszyklus. Anders als ein gewöhnlicher EventSourcedActor, dessen Mailbox ewig läuft, stoppt ein SagaActor sich **selbsttätig**, nachdem das Framework sein Terminal-Ereignis persistiert hat.

Der Terminal-Zustand wird **vom Framework erzeugt**: `MarkComplete(result)` hängt ein `SagaCompleted(result)`-Ereignis an denselben Batch wie Ihre Geschäftsereignisse an (atomarer Anhang — Abschluss und Persistenz können nicht auseinanderlaufen); eine nicht abgefangene Geschäftsausnahme hängt `SagaFailed(reason)` an („ExceptionType: message“, auf 512 Zeichen gekürzt) und propagiert `SagaExecutionException(Id, Reason)` an den Aufrufer. Unterklassen werfen diese Ereignisse weder noch behandeln sie — `Mutate` sieht nur Geschäftsereignisse.

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

**Lebenszyklus:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

Geschäftlicher Fehler: Wirft `OnMessageAsync`/`ResumeAsync`, verwirft das Framework uncommittete Geschäftsereignisse, hängt `SagaFailed(reason)` an, stoppt selbsttätig und lässt den Ask-Aufrufer mit `SagaExecutionException(Id, Reason)` fehlschlagen. Infrastrukturfehler (Store nicht erreichbar) sind **kein** Terminalzustand — Ereignisse werden zurückgerollt, die Saga bleibt Running.
Schlägt das Persistieren von `SagaFailed` selbst fehl (Store weiterhin down), entsteht kein Terminalzustand und die **ursprüngliche** Ausnahme wird an den Aufrufer weitergegeben.

**Crash-Wiederherstellung:** `GetAsync` spielt Ereignisse erneut ab → das Framework stellt `Completed`/`Failed` aus `SagaCompleted`/`SagaFailed` wieder her (terminale Sagas bleiben tot — `GetAsync` gibt null zurück). Sagas ohne Terminal-Ereignis erhalten einen `ResumeAsync()`-Aufruf; erreicht dies den Terminalzustand, persistiert das Framework `SagaCompleted` im selben Flush-Batch. Wiederherstellung ist explizites Pullen, keine Hintergrundmagie.

**Explizite Stapelwiederherstellung:**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` enumeriert Sagas nach dem Typnamen des ersten Ereignisses, stellt jede einzeln über `GetAsync` wieder her (Single-Flight) und liefert die Klassifikation nach der Wiederherstellung: `Completed` / `Failed` (mit dem vom Framework wiederhergestellten Grund) / `Running` (wartet noch auf externe Eingabe). Bereits terminale Sagas werden nie wiederbelebt; lebende werden in-place klassifiziert. Idempotent und sicher wiederholbar (Single-Flight pro ID); ein Store-Fehler schlägt schnell fehl, damit der Aufrufer den ganzen Batch wiederholen kann.

**Process-Manager-Muster:**

dieselbe Basisklasse deckt auch Prozessmanager ab — externe Ereignisse werden von einem Anwendungsebenen-Ereignishandler (z. B. einem PicoMediator-Abonnenten) in Befehle übersetzt und an die Mailbox der Saga gesendet. Ereignisse gelangen nie direkt in Aktoren; die Saga sieht nur Befehle.

**Komfort-API:**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` führt `CreateAsync` + `AskAsync` in einem Aufruf aus. Gibt `SagaExecution<TResult>(Id, Result)` zurück; bei geschäftlichem Fehler wirft es `SagaExecutionException(SagaId, Reason)`, sodass der Aufrufer immer die Saga-ID erhält.

### Nachrichten: Ask vs Send

| Muster | Methode | Semantik |
|---------|--------|-----------|
| Request-Reply | `AskAsync<TResult>(id, command)` | Gibt Ergebnis nach Nachrichtenverarbeitung zurück |
| Fire-and-Forget | `Send(id, command)` | Keine Antwort; Fehler werden an UnhandledErrorHandler geroutet (ohne Logger nach stderr); wirft bei unbekanntem Actor / geschlossener Mailbox (Stop-Race) |

### OutputChannel

Actors senden `ActorOutputEvent`-Nachrichten an externe Abonnenten:

```csharp
counter.OutputWriter = channel.Writer;
// In OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### Spawn

Actors erstellen Kind-Actors über die `System`-Referenz:

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

## Modulintegration

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Registriert IActorSystem + InMemoryEventStore

// Benutzerdefinierter Event-Store
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Aus PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` wird als **Singleton** registriert. Wenn `ILoggerFactory`
registriert ist, wird automatisch ein Logger injiziert.

### Benutzerdefinierter IEventStore

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT mit Nebenläufigkeitsprüfung */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT nach Version sortiert */ }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* SELECT des ersten Ereignisses (Wiederherstellungs-Enumeration) */ }
}
```

### Event-Ausgang (PicoMediator)

`IDomainEvent : IEvent` — Domain-Events sind First-Class-Benachrichtigungen für PicoMediator. Nach persist+mutate veröffentlicht das Framework sie als **Kontext-Envelopes** über den `IDomainEventPublisher`-Hook; Replay (Wiederherstellung) veröffentlicht nicht erneut.

`MediatorDomainEventPublisher` ist der Out-of-the-Box-Adapter: verpackt jedes Event in einen `DomainEventEnvelope(actorId, version, event)` und veröffentlicht ihn per `Publish<DomainEventEnvelope>` (Compile-Time-Generic, AOT-sicher) mit Isolation pro Event — ein fehlgeschlagener Subscriber blockiert keine weiteren Events.

### Abonnieren von Domain-Events (declare-and-subscribe)

Event-Handler sind einfache Klassen, die `IDomainEventSubscriber<TEvent>` implementieren — PicoActor.Gen (in PicoActor.Abs eingebettet) scannt und auto-registriert sie; null manuelle Verdrahtung. Der Handler erhält einen typisierten Envelope mit dem Kontext des Quell-Aggregats (`ActorId`, `Version`) plus einen schmalen `ICommandSender`-Port:

Handler können als class oder record deklariert werden. Eine Implementierung ohne zugänglichen (public/internal) Instanzkonstruktor schlägt beim Build mit **PICA001** fehl, statt fehlerhaften Registrierungscode zu erzeugen.

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

// Verdrahtung: AddPicoMediator registriert IMediator; AddPicoActor() erkennt ihn in der
// ActorSystem-Factory und verdrahtet den Event-Ausgang (lazy — Scoped-kompatibel).
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe: Subscriber automatisch registriert
container.AddPicoActor();     // MediatorDomainEventPublisher automatisch verdrahtet
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// Explizite Verdrahtung für eigene Publisher: AddPicoActor(IPublisher)
// (die Instanz muss vor Build() verfügbar sein).
```

> **Benötigte Pakete:** hinzugefügt werden muss nur `PicoActor` (enthält `PicoActor.Abs` und die Abstraktionen `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator`). Für `IDomainEventSubscriber<TEvent>`-Handler ist kein weiteres Paket nötig — `PicoMediator` ist eine Abhängigkeit von `PicoActor` (der generierte Bridge ruft dessen `MediatorAutoSubscriptionRegistry`). `PicoDI` + `PicoMediator.DI` kommen hinzu, sobald Sie den Container verdrahten (`SvcContainer`, `AddPicoMediator`).

> **Lokale Entwicklung (ProjectReference):** Analyzer propagieren nicht durch `ProjectReference`-Ketten — Projektkonsumenten müssen eine direkte Referenz auf `PicoActor.Gen` hinzufügen (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, Spiegel von `tests/PicoActor.Tests`). NuGet-Konsumenten erhalten den Generator automatisch über die `buildTransitive`-Props von `PicoActor.Abs` — keine zusätzliche Referenz nötig.

Events fließen nach persist+mutate als Envelopes durch PicoMediator; Replay veröffentlicht nie. Handler-Fehler wirken sich nie auf den Actor aus (Isolation pro Handler). Übersetzungsschleifen (Event → Command → Event) sind beabsichtigt; halten Sie Handler idempotent und begrenzt.

> **Breaking Change:** direkte `ISubscriber<TEvent>`-Subscriber (PicoMediator) erhalten keine PicoActor-Domain-Events mehr. Migrieren Sie zu `IDomainEventSubscriber<TEvent>`; `ActorId`/`Version` des Envelopes ersetzen jede manuell eingebettete Aggregat-ID. Eigene Publisher (`AddPicoActor(IPublisher)`) erhalten jetzt `DomainEventEnvelope`-Instanzen statt roher Events — passen Sie `Publish<TEvent>`-Implementierungen entsprechend an (nur die beobachtete Payload-Form änderte sich; die Actor-Pipeline ist nicht betroffen). `IEventStoreEnumerator.ListAggregateIds(string)` heißt jetzt `ListAggregateIdsAsync(string)` (asynchron) — eigene Enumeratoren müssen die Signatur anpassen.

Hinweise:
- `Register<T>` wird genau einmal pro Actor-Typ aufgerufen: eine zweite Registrierung wirft jetzt eine Ausnahme, statt die erste Factory still zu ersetzen.
- `Register<T>` lehnt Typen ab, die die Laufzeit nicht bedienen kann: Implementierungen ohne Ableitung von `Actor` sowie `IEventSourcedActor`-Implementierungen ohne Ableitung von `EventSourcedActor` (Persistenz wird nur für die konkreten Basen verdrahtet).
- `StopAsync`/`RequestStop` entfernen den Actor zuerst aus der Registry und arbeiten dann die bereits in der Mailbox gepufferten Nachrichten ab (graceful stop); danach gesendete Nachrichten schlagen mit `KeyNotFoundException` fehl (bei bereits geschlossener Mailbox mit `InvalidOperationException` — Stop-Race-Fenster).
- **Event→Command-Übersetzung ist Aufgabe des Subscribers (Geschäftsebene)** — PicoActor veröffentlicht nur; Commands gelangen ausschließlich über die Mailbox in Actor.
- Veröffentlichung erfolgt **nach persist+mutate** — ein fehlgeschlagener Publish beeinträchtigt den Actor-Zustand nicht (Events sind bereits dauerhaft).
- Wiederherstellung ist still: Replay veröffentlicht nicht erneut.
- Framework-Events (`SagaCompleted`/`SagaFailed`) sind wie alle anderen Events typisiert abonnierbar (Abs zielt auf net10.0).
- **Auto-Verdrahtung aus jedem Scope sicher**: Seit PicoDI 2026.8.1 (E1) laufen Singleton-Factories gegen den container-internen Root-Scope — der auto-verdrahtete IMediator lebt bis zur Container-Freigabe.
- **Verwenden Sie `AskAsync` niemals auf dem veröffentlichenden Aggregat aus dessen eigenem Veröffentlichungspfad** — die Quell-Mailbox ist mit dem Flush des Events beschäftigt; die Anfrage würde sich selbst blockieren. Fragen Sie Lese-Projektionen ab (separate Actors); `Send` an das Quell-Aggregat ist sicher (Fire-and-Forget).

---

## Designphilosophie (克制 / 专注 / 优雅 / 高效)

| Prinzip | In der Praxis |
|-----------|------------|
| **克制 (Restraint / Zurückhaltung)** | Kein verteilter Konsens, keine Supervision-Bäume — nur Actors und Events. |
| **专注 (Focus / Fokus)** | Single-threaded pro Actor. Eine Nachricht nach der anderen. |
| **优雅 (Elegance / Eleganz)** | Persist-then-Mutate: Zustand ändert sich nur nach Persistenz. Rollback automatisch. |
| **高效 (Efficiency / Effizienz)** | AOT-kompatibel, null Reflection, `net10.0`-Abstraktionen. |

---

## Anwendungsfälle

- **KI-Agentensysteme** — jeder KI-Agent ist ein Actor mit Konversationszustand
- **Workflow-Orchestrierung** — Actors modellieren langlaufende Geschäftsprozesse
- **Spieleserver-Zustand** — Event-Sourcing-Actors für Spieler-/Spielzustand
- **IoT-Gerätezustand** — In-Memory-Actors mit periodischen Snapshots

---

## Pakete

| Paket | Ziel | Beschreibung |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | Kernabstraktionen: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor` — plus Abonnementstypen (`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`) und den eingebetteten `PicoActor.Gen`-Analyzer (declare-and-subscribe) |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Laufzeit: `ActorSystem`, `InMemoryEventStore`, `MediatorDomainEventPublisher` (Envelope-Ereignisausgang), PicoDI-Integration (`AddPicoActor` verdrahtet IMediator + ICommandSender automatisch) |

---

## Vergleich

| Funktion | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Nur In-Memory | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0-Abstraktionen | ❌ | ✅ | ✅ | ❌ |
| PicoDI-Integration | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Verteilt / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded pro Actor | ✅ | ✅ | ✅ | ❌ |
| Pakete | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## Lizenz

MIT — siehe [LICENSE](LICENSE).
