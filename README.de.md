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
| Abhängigkeitsgröße | ❌ Akka.NET (8+ Pakete), Orleans (10+ Pakete) | ✅ 2 Pakete, null Abhängigkeiten außer Channels |
| DI-Integration | ❌ An Microsoft.Extensions.DI gebunden | ✅ Natives PicoDI, auflösung ohne Reflection |
| netstandard2.0 | ⚠️ Teilweise Unterstützung in Akka.NET / Proto.Actor | ✅ Abstraktionen zielen auf netstandard2.0 |
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
var system = new ActorSystem(store);

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

`CreateAsync<T>(cmd, id)` erstellt einen Actor mit einer vom Aufrufer bereitgestellten id — verwenden Sie es, wenn die id bekannt sein muss, bevor der Actor existiert (deterministische id / Saga-Wiederherstellung). Wirft eine Ausnahme, wenn die id bereits registriert ist.

---

## Moduldetails

### PicoActor.Abs — Kernabstraktionen

Ziel `netstandard2.0` für maximale Kompatibilität.

| Typ | Rolle |
|------|------|
| `IActor` | Basisschnittstelle — stellt `Id` (UUID v7) bereit |
| `IActorSystem` | Laufzeitvertrag — Register, CreateAsync, CreateAsync(id), GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | Markierungsschnittstelle für Befehle |
| `IDomainEvent` | Markierungsschnittstelle für Domänenereignisse |
| `IEventSourcedActor` | Optional — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Persistenzvertrag — AppendAsync (optimistische Nebenläufigkeit), LoadAsync |
| `ICancelable` | Optional — CancelCurrentTurn für langlaufende Operationen |
| `Actor` | Abstrakte Basisklasse — Mailbox, Konsumschleife, SignalReady, StopAsync |
| `EventSourcedActor` | ES-Basis — RaiseEvent, Mutate, Persist-then-Mutate-Pipeline |
| `Envelope` | Intern — umschließt ICommand mit optionalem TaskCompletionSource |
| `ActorOutputEvent` | Ausgehende Benachrichtigung — Type, Data, optional ToolCallId/ToolName/TurnId |
| `ConcurrencyException` | Von IEventStore bei Versionsdivergenz ausgelöst |

### PicoActor — Laufzeit

Ziel `net10.0`, AOT-kompatibel.

| Typ | Rolle |
|------|------|
| `ActorSystem` | Standard-`IActorSystem` — ConcurrentDictionary-Registry, Fabrikregistrierung, Nachrichtenrouting, CancelTurn |
| `InMemoryEventStore` | Lock-freier In-Memory-Speicher — ConcurrentDictionary-basiert |
| `ActorConfig` | Konfigurations-POCO — aus PicoCfg bindbar |
| `ActorSystemOptions` | Optionen mit optionalem IEventStore und ILogger |
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

### Nachrichten: Ask vs Send

| Muster | Methode | Semantik |
|---------|--------|-----------|
| Request-Reply | `AskAsync<TResult>(id, command)` | Gibt Ergebnis nach Nachrichtenverarbeitung zurück |
| Fire-and-Forget | `Send(id, command)` | Keine Antwort; Ausnahmen werden an UnhandledErrorHandler geroutet |

### OutputChannel

Actors senden `ActorOutputEvent`-Nachrichten an externe Abonnenten:

```csharp
counter.OutputWriter = channel.Writer;
// In OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Langlaufende Operationen abbrechen, ohne den Actor zu stoppen:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... langlaufende Arbeit mit _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
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
}
```

---

## Designphilosophie (克制 / 专注 / 优雅 / 高效)

| Prinzip | In der Praxis |
|-----------|------------|
| **克制 (Restraint / Zurückhaltung)** | Kein verteilter Konsens, keine Supervision-Bäume — nur Actors und Events. |
| **专注 (Focus / Fokus)** | Single-threaded pro Actor. Eine Nachricht nach der anderen. |
| **优雅 (Elegance / Eleganz)** | Persist-then-Mutate: Zustand ändert sich nur nach Persistenz. Rollback automatisch. |
| **高效 (Efficiency / Effizienz)** | AOT-kompatibel, null Reflection, `netstandard2.0`-Abstraktionen. |

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Kernabstraktionen: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Laufzeit: `ActorSystem`, `InMemoryEventStore`, PicoDI-Integration |

---

## Vergleich

| Funktion | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Nur In-Memory | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| netstandard2.0-Abstraktionen | ✅ | ✅ | ✅ | ❌ |
| PicoDI-Integration | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Verteilt / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded pro Actor | ✅ | ✅ | ✅ | ❌ |
| Pakete | 2 | 8+ | 3+ | 10+ |

---

## Lizenz

MIT — siehe [LICENSE](LICENSE).
