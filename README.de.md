<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

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
dotnet add package PicoActor.EventStore.InMemory   # der Speicher ist ein eigenes Paket (v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

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
| `InMemoryEventStore` | Verschoben nach **`PicoActor.EventStore.InMemory`** (v2026.9) — In-Memory-Speicher: Sperre pro Stream, optimistische Nebenläufigkeit (ConcurrentDictionary-basiert); registrieren mit `AddPicoActorInMemoryStore()` |
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
  Der Store muss die Nutzlast weiterhin als **undurchsichtige Bytes** behandeln: nicht parsen
  und nicht annehmen, dass sie exakt der Ausgabe von `TakeSnapshot()` entspricht.

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader) (Ereignis-Log-Leseschnittstelle)

Anwendungscode wird gegen `IActorSystem` geschrieben und erhält Actors; kein Member der
fachlichen Oberfläche gibt eine Sammlung von Events zurück. `IEventStore` ist die
Persistenz-SPI der **Store-Autoren**: angehängte Events persistieren und wieder liefern, wobei
`LoadAsync` als Fallback-Lesen der Runtime erhalten bleibt. Tools und Projektionen lesen
stattdessen über `IEventLogReader` — `GetVersionAsync`, `OpenAsync`, `PeekFirstAsync` —, wobei
`OpenAsync` einen ausschließlich vorwärts laufenden Cursor zurückgibt:

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- Eine einzige Beobachtung: `End` ist die bei `OpenAsync` ermittelte Stream-Länge und ändert
  sich über die Lebensdauer des Cursors nicht. Rollback-Schutz und Tail-Länge stammen damit aus
  einem einzigen Öffnen, und der Cursor liefert nie mehr als `End`.
- Die Fähigkeitsprüfung ist eine Regel — `eventStore is IEventLogReader`. Ein Store, der die
  Leseschnittstelle implementiert, erlaubt einem snapshot-beschleunigten Rebuild, nur den Tail
  nach dem Snapshot zu lesen (ein Datei-Store kann den Kopf klassifizieren und nur den Tail
  deserialisieren); ein Store ohne sie funktioniert weiter über `LoadAsync`, das den gesamten
  Stream liest und deserialisiert.
- `InMemoryEventStore` implementiert `IEventLogReader`; für jeden anderen Store ist es optional
  und rein additiv.

### JSONL Event Store (opt-in) (JSONL-Ereignisspeicher, optional)

`PicoActor` führt Actors aus; es bringt keinen Speicher mit. Zwei Pakete liefern einen:
`PicoActor.EventStore.InMemory` (der In-Memory-Standard, registriert mit
`AddPicoActorInMemoryStore()`) und `PicoActor.EventStore.Jsonl` (dauerhaft, in diesem Abschnitt
beschrieben).

`PicoActor.EventStore.Jsonl` hält eine reine Append-JSONL-Datei pro Aggregat
(`<root>/<aggregateId>.jsonl`, ein JSON-Objekt pro Zeile), Snapshots als atomare Sidecars im
benachbarten Verzeichnis `<root>-snapshots`. Er implementiert `IEventStore`, `IEventLogReader`,
`IEventStoreSnapshot` und `IEventStoreEnumerator`, ist AOT-sicher (PicoJetson-Quellgenerierung)
und **einprozessig**: zwei Speicher-Instanzen über einem Verzeichnis teilen keine Gates, daher
scheitern gleichzeitige Schreiber laut, statt einen Stream zu zerreißen.

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

- **Eine vergessene Registrierung fällt beim Build auf, nicht beim ersten Append**: das Paket
  enthält den Analyzer `PICOSTORE001`, der jedes Domänen-Ereignis meldet, das kein
  `[PicoDerivedType]` auflistet (ein Projekt, das nur den In-Memory-Speicher nutzt, zahlt dafür
  nichts).
- **Gerissener Tail vs. Korruption.** Die nicht terminierte letzte Zeile — die einzige Form, die
  ein Prozessabsturz hinterlässt — wird ignoriert, nie gezählt und nie geworfen; der nächste
  Append schneidet sie ab. Eine **terminierte** Zeile, die sich nicht deserialisieren lässt, ist
  Korruption und wirft (die letzte eingeschlossen), und der Speicher repariert sie nicht, denn
  Reparieren wäre Raten. Das sind die dokumentierten Kosten dafür, `FlushToDisk` beim Standard
  `false` zu lassen: Die Bytes erreichen das Betriebssystem (ein anderer Leser sieht das Ereignis
  sofort), aber nicht die Platte, sodass ein Stromausfall eine terminierte Müllzeile hinterlassen
  kann. Setzen Sie `FlushToDisk = true`, um jeden Append zu fsyncen.
- **Cursor und Rebuild nur des Tails.** `GetVersionAsync`, das `End` des Cursors und seine
  Byte-Positionierung stammen aus einem einzigen billigen Scan (vollständige, nicht leere,
  terminierte Zeilen — keine JSON-Validierung, kein Deserialisierer). Ein
  snapshot-beschleunigter Rebuild öffnet daher bei `snapshot.Version + 1` und deserialisiert nur
  den Tail (`N − v` Ereignisse). Gespart werden Deserialisierung und Allokation, nicht
  Platten-I/O: der Kopf wird weiterhin einmal als Bytes gelesen. Eine „Skip“-Sonde des
  Serialisierers ist nicht nötig — ein generischer Stream enthält nur Ereignisse, eine unlesbare
  Zeile ist also Korruption und kein fremder Datensatz.
- `MaxAggregateLocks` (Standard `1024`) begrenzt die Gate-Tabelle pro Aggregat; ein verdrängter
  Eintrag kostet einen erneuten Scan beim nächsten Append dieses Aggregats, nie Korrektheit.
  `Logger` ist unabhängig von der DI-`ILoggerFactory`: übergeben Sie denselben Logger explizit,
  um die Warnungen des Speichers in Ihrem Log zu sehen.
- Nicht in dieser Version: Mehrprozess-Schreiber, Archivierung/Aufbewahrung/Kompaktierung, fremde
  (Nicht-Ereignis-)Zeilen, Verschlüsselung/Kompression. Framework-Ereignisse brauchen **keinen zweiten
  Codec**: Bindungen routen sie an Serializer, die im Store-Paket generiert werden (siehe unten
  «Framework events (event type bindings)»).

#### Stream naming (`FileNameFormat`)

`JsonlEventStoreOptions.FileNameFormat` legt fest, wie eine Stream-**Datei benannt** wird; das
Zeilenformat ist in beiden Modi identisch, es unterscheiden sich nur die Namen — ein vorhandenes
Verzeichnis bleibt also lesbar.

| Wert | Stream-Datei | Was es bringt |
|---|---|---|
| `IdOnly` (Standard) | `<id:D>.jsonl` | das historische Layout, byte-genau reproduziert — keine Migration, keine Änderung für ein bestehendes Verzeichnis |
| `TypePrefix` | `<type>-<id:D>.jsonl` | der Typ steht im Namen, daher löst `ListAggregateIdsAsync` die Ids allein aus Dateinamen auf: **null Dateiöffnungen** pro Enumeration |

`{type}` ist `events[0].GetType().Name` — der konkrete Typ des **ersten persistierten Events** eines
Streams (ein generischer Typ behält seine Backtick-Arität) — und er ist **bei der Erstellung
festgelegt**: ein späterer Batch, dessen erstes Event einen anderen Typ hat, hängt weiter an den
Namen, mit dem der Stream geboren wurde.

- **Migration auf `TypePrefix`.** PicoAgent benennt seine Streams bereits `{type}-{id}.jsonl`; zeigt
  dieser Store mit `FileNameFormat = TypePrefix` auf ein solches Verzeichnis, ist **keine einzige
  Umbenennung** nötig — das Verzeichnis ist so lesbar, wie es ist, und Snapshots ziehen unverändert
  mit (sie sind auf die Aggregat-Id geschlüsselt, `<id:D>.snap`, nie auf den Stream-Namen). Die
  Gegenrichtung gibt es nicht: ein Präfix ist Teil des Namens, ein `IdOnly`-Store kann ihn nicht aus
  der Id ableiten und müsste jede Datei umbenennen.
- **Ein gemischtes Verzeichnis wird diagnostiziert, nie verschmolzen.** Einträge, die nur im
  *anderen* Layout gültig sind, werden gezählt (`ForeignFormatFiles`, nach **Namen** und monoton: das ist
  das Gate des `IdOnly`-Duplikat-Schutzes) und als **eine** Warnung pro Scan gemeldet (die Erkennung ist
  symmetrisch: `<id>.jsonl` unter `TypePrefix` und `<type>-<id>.jsonl` unter `IdOnly` gelten beide als
  „anderes Format“). Die Ganzverzeichnis-Diagnose zählt **Historien**, nicht Namen: ein Verzeichnis mit
  **null** Historien im konfigurierten Format und **einigen nicht-leeren** im anderen wirft eine
  `InvalidOperationException`, die beide Zähler, beide Formate und die Auswege nennt — aus Enumeration,
  Konstruktion und Lazy-Pass gleichermaßen — statt ein leeres System zu melden; ein Verzeichnis aus
  lauter Überbleibseln der Länge null (ein fehlgeschlagenes Append hinterlässt eines) ist dagegen ein
  **leerer** Store und bleibt still. Ein Stream eines *anderen Event-Typs* ist keine Mischung: das ist
  der Normalfall, er kostet weder Zähler noch Logzeile. Ein Append für eine Id, die bereits einen
  nicht-leeren Stream im anderen Layout hat — oder zwei verschiedene Präfixe — wird abgelehnt, bevor ein
  Byte geschrieben ist.
- **Kosten.** `TypePrefix` zahlt einen Dateinamen-Durchlauf bei der Konstruktion (für die Id → Typ
  Abbildung), einen pro aufgelöstem Fehlgriff und ein `stat` pro Append für den Duplikat-Schutz.
  `IdOnly` zahlt bei der Konstruktion **nichts** und nichts für ein Append an einen bereits vorhandenen
  Stream; sein einziger Verzeichnis-Durchlauf läuft lazy bei der ersten Beobachtung eines Streams, den es
  nicht hat, und danach nie wieder. Ein leerer Batch schreibt und erzeugt in beiden Modi nichts, löst
  seinen Pfad aber **genau wie ein schreibendes Append** auf und prüft weiterhin `expectedVersion`.
- **Der Fehlgriff-Cache dient nur dem LESEPFAD.** Ein `TypePrefix`-Fehlgriff wird erinnert — ein
  begrenztes Set von 4.096 Ids, das bei Überschreitung geleert statt vergrößert wird — und
  **single-flight** geteilt: ein Schwung **gleichzeitiger Probes EINER** fehlenden Id teilt sich
  einen einzigen Durchlauf, und **Wiederholungen dieser Id** sind gratis; eine **andere** fehlende Id
  zahlt ihren eigenen Durchlauf. Ein **Append vertraut diesem Gedächtnis nie**: dort „kein Stream“
  aufzulösen würde einen zweiten Namen neben einem Stream anlegen, den ein externer Schreiber nach dem
  Aufbau der Abbildung erzeugt hat. Ein Append scannt bei einem Fehlgriff also **erneut** und verwirft
  bei Erfolg nur **seinen eigenen** Eintrag — niemals alles, und ohne das Scan-Gate zu nehmen. Der Preis
  wird benannt: ein *Lesen* darf „leer“ für einen Stream antworten, der daneben entstanden ist, bis zum
  nächsten Durchlauf — und ein Host, der nie wieder einen Durchlauf macht, sieht die veraltete Antwort,
  bis eine **neue Instanz** das Verzeichnis neu einliest. `NegativeCacheInvalidations` zählt die
  Einträge, die ein Append tatsächlich widerlegt hat (unter `IdOnly` identisch `0`, weil dort nie ein
  Fehlgriff erinnert wird).
- **Eine veraltete Abbildung korrigiert der nächste SCAN.** Ein Rivale-Präfix, der nach dem Aufbau der
  Abbildung eintrifft, wird von einem Lesen nicht gefunden (ein Map-Treffer kostet keinen Durchlauf) und
  von einem Append **ebenso wenig**: der Store fügt dem Append-Pfad keinen Verzeichnis-Durchlauf hinzu,
  um dieses Fenster zu schließen — er gäbe damit genau zurück, was die Abbildung einbringt, und zwar auf
  dem einen Pfad, der pro Append zahlt. Der nächste Klassifikations-Durchlauf — eine Enumeration oder der
  Fehlgriff einer unbekannten Id — hält den Konflikt fest, und von da an wird jedes Append für diese Id
  abgelehnt und nennt beide Dateien.
- **Ein kaputtes Root oder ein belegter Name wird benannt, nicht geschluckt.** Ein Root, der als
  **Datei** existiert, wirft `IOException` (dort lässt sich nichts enumerieren, öffnen oder erzeugen);
  ein Stream-NAME, den ein **Verzeichnis** belegt, wirft `InvalidOperationException` mit dem Hindernis im
  Text — im Lesepfad wie im Append-Pfad, in beiden Formaten — statt „leeres Aggregat“ oder einer nackten
  Freigabeverletzung. Ein fehlgeschlagener Lazy-Pass (Fehlkonfiguration, unlesbares Root, Root als Datei)
  wird **gelatcht**: jede spätere Beobachtung wirft dieselbe Ausnahme, und das Verzeichnis wird nie erneut
  gescannt — die einzige Erholung ist eine **neue Instanz**, sobald das Verzeichnis in Ordnung ist.

#### Framework events (event type bindings) (Framework-Ereignisse: Typ-Bindungen)

`SagaCompleted`/`SagaFailed` sind `sealed`, leiten von `IDomainEvent` ab und nicht von Ihrem `TEventBase` —
ein Saga-Stream enthält also einen Typ, den Ihr Serializer nie gesehen hat.
`JsonlEventStoreOptions.EventTypeBindings` leitet Framework-Ereignisse in denselben Store: Das Framework
registriert `SagaCompleted → PicoActorSagaCompleted` und `SagaFailed → PicoActorSagaFailed` vorab (`TryAdd`,
Ihr eigenes `Add<T>(tag)` gewinnt also; `FrameworkDefaultCount` ist die Zahl der tatsächlich angewandten
Defaults, `0…2`, `RegisteredCount` die Gesamtzahl).

- **`Add<T>(tag)` routet nur, es generiert nichts.** Der Tag ist das `$type` der Zeile und das, was die
  Leseseite erschnüffelt (erstes Member, Groß-/Kleinschreibung egal, bei Unbekanntem unbedingter Rückfall auf
  `TEventBase`) — der Typ braucht trotzdem einen generierten Serializer. `[GenerateSerializer(typeof(T))]` auf
  einem **positional record** funktioniert nicht (PicoSerDes Attribut-Pipeline sieht keinen Primärkonstruktor:
  `CS7036`/`CS8852`); die funktionierende Form ist eine **geschlossene Aufrufstelle** — die literalen Aufrufe
  in `FrameworkEventSerializers.Generate()`. Ein gebundener Typ ohne Generierungsstelle wird beim **ersten
  Append** abgelehnt (`InvalidOperationException`, mit dem Ausweg in der Meldung).
- **Gemischte Batches sind ein Schreibvorgang:** `TEventBase` und gebundene Ereignisse teilen sich einen Batch
  — ein Puffer, ein Schreibvorgang, ein Flush.
- **Benennung.** Die beiden Framework-Tags sind die Tags, die Produktionskorpora bereits tragen, und
  dauerhaftes Wire-Format (kein Alias). Ein Stream heißt wie üblich nach dem CLR-Kurznamen seines ersten
  Ereignisses; unter `TypePrefix` ist ein Name außerhalb jedes registrierten **Typ**-Präfixes
  (`unknown-<id>.jsonl`, die eigene Kategorisierung eines Hosts) **per id** erreichbar, in der Typ-Enumeration unsichtbar, zählt nie in
  `ForeignFormatFiles` und löst nie den Verzeichnis-weiten Throw aus. Ein Präfix enthält kein `-`; gleiche
  Kurznamen teilen sich ein Präfix.
- **`SagaCompleted.Result` ist `object?` — und der Serializer kann einen Nicht-null-Wert gar nicht
  schreiben** (der generierte Writer schreibt nur den Membernamen, keinen Wert). `null` — die einzige Form,
  die ein **persistenter Store** heute akzeptiert — geht hin und zurück; alles andere scheitert sofort und
  schreibt kein Byte. `MarkComplete(result)` mit einem Nicht-null-Ergebnis lässt sich nicht persistieren:
  der Terminal-Batch wirft und schreibt nichts. In einem persistenten Store `Result` auf `null` halten.
- **Regeln der Registrierung.** Bindungen liegen auf der options-Instanz —
  `options.EventTypeBindings.Add<T>(tag)` **vor** dem Konstruieren des Stores: Der Konstruktor **friert die
  Tabelle ein und kopiert sie**, ein späteres `Add` wirft also, statt still ignoriert zu werden. Ein Typ hat
  genau einen Tag und ein Tag benennt genau einen Typ (dasselbe Paar erneut ist ein No-op **vor dem
  Einfrieren**; ein Tag ist nicht leer und enthält kein Anführungszeichen, keinen Backslash und kein
  Steuerzeichen). Eine Basis, die die Framework-Ereignisse bereits abdeckt (`IDomainEvent` selbst), wird
  beim Konstruieren abgelehnt — sie wären zugleich gewöhnliche und gebundene Ereignisse — und ebenso ein
  gebundener Typ, der `TEventBase` zuweisbar ist. `with` kopiert die **Referenz**: Klon und Original teilen
  sich eine Tabelle samt Einfrierzustand, ein `Add` auf dem Klon wirft also und nennt den *anderen* Store —
  für eine unabhängige Tabelle `new JsonlEventStoreOptions()` bauen.
- **Der Tag einer Bindung muss sich von jedem `[PicoDerivedType]`-Diskriminator auf `TEventBase`
  unterscheiden.** Ein Tag hat genau einen Besitzer, und der Lesepfad fragt **zuerst die Bindings**: bei
  einer Kollision würden die vom Basis-Typ selbst geschriebenen Zeilen als der gebundene Typ gelesen —
  Ereignistyp und Member gehen verloren. Deshalb wirft der Konstruktor schon `InvalidOperationException`
  (die Meldung nennt den Tag) und lehnt vor dem Einfrieren ab — eine stille Ersetzung ist nicht möglich.
  (Die eine verbleibende Lücke: ein abgeleitetes Ereignis, dessen Reader den Tag *erkennt* und dann wirft —
  seine eigene Validierung, keine fehlende Registrierung —, was die Prüfung im Konstruktor nicht von einem
  unbekannten Tag unterscheiden kann.)

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
container.AddPicoActor();               // Registriert IActorSystem — keinen Speicher
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory: der In-Memory-Standard

// Benutzerdefinierter Event-Store
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Aus PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` wird als **Singleton** registriert. Wenn `ILoggerFactory`
registriert ist, wird automatisch ein Logger injiziert.

`AddPicoActor()` registriert keinen Speicher mehr und scheitert auch nicht beim Registrieren: erst
die **Auflösung** von `IActorSystem` wirft `InvalidOperationException` und nennt
`AddPicoActorInMemoryStore()` (die Registrierungsreihenfolge bleibt damit frei, und ein explizites
`AddPicoActor(store)` gewinnt weiterhin).

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

> **Benötigte Pakete:** zusätzlich zu `PicoActor` (enthält `PicoActor.Abs` und die Abstraktionen `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator`) braucht es ein Speicher-Paket: `PicoActor.EventStore.InMemory` für den In-Memory-Betrieb (registriert mit `AddPicoActorInMemoryStore()`) oder `PicoActor.EventStore.Jsonl` für Dauerhaftigkeit (PicoJetson inklusive). Für `IDomainEventSubscriber<TEvent>`-Handler ist kein weiteres Paket nötig — `PicoMediator` ist eine Abhängigkeit von `PicoActor` (der generierte Bridge ruft dessen `MediatorAutoSubscriptionRegistry`). `PicoDI` + `PicoMediator.DI` kommen hinzu, sobald Sie den Container verdrahten (`SvcContainer`, `AddPicoMediator`).

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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Laufzeit: `ActorSystem`, `MediatorDomainEventPublisher` (Envelope-Ereignisausgang), PicoDI-Integration (`AddPicoActor` verdrahtet IMediator + ICommandSender automatisch) |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | Der In-Memory-Speicher (`InMemoryEventStore`, in v2026.9 aus `PicoActor` ausgelagert) + `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | Dauerhafter JSONL-Ereignisspeicher: reines Append, optimistische Nebenläufigkeit, Rebuild nur des Tails über `IEventLogReader`, atomare Snapshot-Sidecars, AOT-sicher (PicoJetson) — plus der eingebettete Registrierungs-Analyzer `PICOSTORE001` |

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
