# PicoActor

Framework Actor en mémoire compatible AOT avec Event Sourcing pour .NET.
Léger, zéro réflexion, conçu pour les systèmes d'IA agent et l'orchestration
de workflows. Fonctionne sous NativeAOT et trimming.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## Modèle Computationnel

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

Chaque Acteur possède une **boîte aux lettres** (`Channel<Envelope>` en mémoire),
une identité **UUID v7** et une boucle de consommation mono-thread. Les commandes
sont délivrées via `Send` (fire-and-forget) ou `AskAsync` (requête-réponse).

Les Acteurs Event Sourcing suivent **Persist-then-Mutate** (Persister-puis-Muter) :
`OnMessageAsync → RaiseEvent → Persister dans IEventStore → Mutate état`.
L'état n'est modifié qu'après une persistance réussie — l'état en mémoire est
toujours cohérent avec le flux d'événements.

---

## Pourquoi PicoActor

| Préoccupation | Options existantes | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans nécessitent la réflexion | ✅ Support complet NativeAOT |
| Event Sourcing | ❌ Proto.Actor, Orleans sans ES intégré | ✅ Persist-then-Mutate, rollback automatique |
| Taille des dépendances | ❌ Akka.NET (8+ packages), Orleans (10+ packages) | ✅ 2 packages, zéro dépendance au-delà de Channels |
| Intégration DI | ❌ Couplé à Microsoft.Extensions.DI | ✅ PicoDI natif, résolution sans réflexion |
| netstandard2.0 | ⚠️ Support partiel dans Akka.NET / Proto.Actor | ✅ Abstractions ciblant netstandard2.0 |
| Courbe d'apprentissage | ❌ Raide — arbres de supervision, clustering, remoting | ✅ Minimale — Actor + Event + Mailbox |

---

## Démarrage Rapide

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Configuration
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Enregistrer les fabriques d'Acteurs
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Créer, envoyer des messages, arrêter, reconstruire
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Détails du Module

### PicoActor.Abs — Abstractions Fondamentales

Cible `netstandard2.0` pour une compatibilité maximale.

| Type | Rôle |
|------|------|
| `IActor` | Interface de base — fournit `Id` (UUID v7) |
| `IActorSystem` | Contrat d'exécution — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | Interface marqueur pour les commandes |
| `IDomainEvent` | Interface marqueur pour les événements de domaine |
| `IEventSourcedActor` | Interface optionnelle — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Contrat de persistance — AppendAsync (concurrence optimiste), LoadAsync |
| `ICancelable` | Optionnel — CancelCurrentTurn pour les opérations longues |
| `Actor` | Classe de base abstraite — boîte aux lettres, boucle de consommation, SignalReady, StopAsync |
| `EventSourcedActor` | Base ES — RaiseEvent, Mutate, pipeline Persist-then-Mutate |
| `Envelope` | Interne — enveloppe ICommand avec TaskCompletionSource optionnel |
| `ActorOutputEvent` | Notification sortante — Type, Data, ToolCallId/ToolName/TurnId optionnels |
| `ConcurrencyException` | Levée par IEventStore en cas de divergence de version |

### PicoActor — Exécution

Cible `net10.0`, compatible AOT.

| Type | Rôle |
|------|------|
| `ActorSystem` | `IActorSystem` par défaut — registre ConcurrentDictionary, fabriques, routage, CancelTurn |
| `InMemoryEventStore` | Stockage en mémoire sans verrou — basé sur ConcurrentDictionary |
| `ActorConfig` | POCO de configuration — liable depuis PicoCfg |
| `ActorSystemOptions` | Options — EventStore requis, Logger facultatif, DomainEventPublisher facultatif; consommé par le constructeur `ActorSystem` |
| `PicoActorDiExtensions` | Méthode d'extension `AddPicoActor()` pour PicoDI |

### Acteur (Non-ES)

Héritez d'`Actor` pour des acteurs opérationnels purement en mémoire.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Acteur Event Sourcing

Héritez d'`EventSourcedActor`. Surchargez `OnMessageAsync` pour appeler
`RaiseEvent`, et `Mutate` pour appliquer les changements d'état.

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

### Pipeline Persist-then-Mutate

```
OnMessageAsync → RaiseEvent (enregistrement seul, sans changement d'état)
              → AppendAsync (persister dans IEventStore)
              → Mutate (appliquer les événements à l'état en mémoire)
              → Répondre à l'appelant (seulement après succès)
```

Si `AppendAsync` échoue, les événements non validés sont rejetés et `Version`
est restaurée. L'Acteur n'est **pas empoisonné** — le message suivant est
traité normalement.

### Messagerie : Ask vs Send

| Modèle | Méthode | Sémantique |
|---------|--------|-----------|
| Requête-Réponse | `AskAsync<TResult>(id, command)` | Retourne le résultat après traitement du message |
| Fire-and-Forget | `Send(id, command)` | Pas de réponse ; exceptions routées vers UnhandledErrorHandler |

### OutputChannel

Les Acteurs diffusent des messages `ActorOutputEvent` aux abonnés externes :

```csharp
counter.OutputWriter = channel.Writer;
// Dans OnMessageAsync :
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Annule les opérations longues sans arrêter l'Acteur :

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... travail long avec _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
```

### Spawn

Les Acteurs créent des Acteurs enfants via la référence `System` :

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

## Intégration des Modules

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Enregistre IActorSystem + InMemoryEventStore

// Stockage d'événements personnalisé
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Depuis PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` est enregistré en tant que **Singleton**. Si `ILoggerFactory`
est enregistré, un logger est automatiquement injecté.

### IEventStore Personnalisé

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT avec vérification de concurrence */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT ordonné par version */ }
}
```

---

## Philosophie de Conception (克制 / 专注 / 优雅 / 高效)

| Principe | En pratique |
|-----------|------------|
| **克制 (Restraint / Retenue)** | Pas de consensus distribué, pas d'arbres de supervision — juste des Acteurs et des Événements. |
| **专注 (Focus / Concentration)** | Mono-thread par Acteur. Un message à la fois. |
| **优雅 (Elegance / Élégance)** | Persist-then-Mutate : l'état ne change qu'après persistance. Rollback automatique. |
| **高效 (Efficiency / Efficacité)** | Compatible AOT, zéro réflexion, abstractions `netstandard2.0`. |

---

## Cas d'Usage

- **Systèmes d'IA agent** — chaque agent IA est un Acteur avec état de conversation
- **Orchestration de workflows** — les Acteurs modélisent des processus métier longs
- **État de serveur de jeux** — Acteurs Event Sourcing pour l'état joueur/jeu
- **État de dispositifs IoT** — Acteurs en mémoire avec snapshots périodiques

---

## Packages

| Package | Cible | Description |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Abstractions fondamentales : `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Exécution : `ActorSystem`, `InMemoryEventStore`, intégration PicoDI |

---

## Comparaison

| Fonctionnalité | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Mémoire uniquement | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Abstractions netstandard2.0 | ✅ | ✅ | ✅ | ❌ |
| Intégration PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persist-then-Mutate | ✅ | ❌ | ❌ | ❌ |
| Distribué / Clustering | ❌ | ✅ | ✅ | ✅ |
| Mono-thread par Acteur | ✅ | ✅ | ✅ | ❌ |
| Packages | 2 | 8+ | 3+ | 10+ |

---

## Licence

MIT — voir [LICENSE](LICENSE).
