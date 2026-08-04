# PicoActor

Framework de Actores en memoria compatible con AOT y Event Sourcing para .NET.
Ligero, sin reflexión, diseñado para sistemas de IA agente y orquestación
de flujos de trabajo. Funciona bajo NativeAOT y trimming.

[![CI](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml/badge.svg)](https://github.com/PicoHex/PicoActor/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PicoActor)](https://www.nuget.org/packages/PicoActor)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[English](README.md) | [简体中文](README.zh.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Português](README.pt.md) | [繁體中文](README.zh-tw.md) | [한국어](README.ko.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

---

## Modelo Computacional

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

Cada Actor posee un **buzón** (`Channel<Envelope>` en memoria), una identidad
**UUID v7** y un bucle de consumo monohilo. Los comandos se entregan mediante
`Send` (dispara-y-olvida) o `AskAsync` (solicitud-respuesta).

Los Actores con Event Sourcing siguen **Persistir-luego-Mutar**:
`OnMessageAsync → RaiseEvent → Persistir en IEventStore → Mutar estado`.
El estado solo cambia tras una persistencia exitosa — el estado en memoria
siempre es consistente con el flujo de eventos.

---

## ¿Por qué PicoActor?

| Preocupación | Opciones existentes | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans requieren reflexión | ✅ Soporte completo NativeAOT |
| Event Sourcing | ❌ Proto.Actor, Orleans sin ES integrado | ✅ Persistir-luego-Mutar, rollback automático |
| Dependencias | ❌ Akka.NET (8+ paquetes), Orleans (10+ paquetes) | ✅ 2 paquetes, cero dependencias más allá de Channels |
| Integración DI | ❌ Acoplado a Microsoft.Extensions.DI | ✅ PicoDI nativo, resolución sin reflexión |
| netstandard2.0 | ⚠️ Soporte parcial en Akka.NET / Proto.Actor | ✅ Abstracciones target netstandard2.0 |
| Curva de aprendizaje | ❌ Pronunciada — árboles de supervisión, clustering, remoting | ✅ Mínima — Actor + Event + Mailbox |

---

## Inicio Rápido

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Configuración
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Registrar fábricas de Actores
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Crear, enviar mensajes, detener, reconstruir
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Detalles del Módulo

### PicoActor.Abs — Abstracciones Centrales

Target `netstandard2.0` para máxima compatibilidad.

| Tipo | Rol |
|------|------|
| `IActor` | Interfaz base — proporciona `Id` (UUID v7) |
| `IActorSystem` | Contrato de runtime — Register, CreateAsync, GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | Interfaz marcadora para comandos |
| `IDomainEvent` | Interfaz marcadora para eventos de dominio |
| `IEventSourcedActor` | Interfaz opcional — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Contrato de persistencia — AppendAsync (concurrencia optimista), LoadAsync |
| `ICancelable` | Opcional — CancelCurrentTurn para operaciones de larga duración |
| `Actor` | Clase base abstracta — buzón, bucle de consumo, SignalReady, StopAsync |
| `EventSourcedActor` | Base ES — RaiseEvent, Mutate, pipeline Persistir-luego-Mutar |
| `Envelope` | Interno — envuelve ICommand con TaskCompletionSource opcional |
| `ActorOutputEvent` | Notificación saliente — Type, Data, ToolCallId/ToolName/TurnId opcionales |
| `ConcurrencyException` | Lanzada por IEventStore en desajuste de versión |

### PicoActor — Runtime

Target `net10.0`, compatible con AOT.

| Tipo | Rol |
|------|------|
| `ActorSystem` | `IActorSystem` por defecto — registro ConcurrentDictionary, fábricas, enrutamiento, CancelTurn |
| `InMemoryEventStore` | Almacén en memoria sin bloqueos — basado en ConcurrentDictionary |
| `ActorConfig` | POCO de configuración — vinculable desde PicoCfg |
| `ActorSystemOptions` | Opciones — EventStore obligatorio, Logger opcional, DomainEventPublisher opcional; consumido por el constructor de `ActorSystem` |
| `PicoActorDiExtensions` | Método de extensión `AddPicoActor()` para PicoDI |

### Actor (No-ES)

Hereda de `Actor` para actores operacionales puramente en memoria.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Actor con Event Sourcing

Hereda de `EventSourcedActor`. Sobrescribe `OnMessageAsync` para llamar a
`RaiseEvent`, y `Mutate` para aplicar cambios de estado.

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

### Pipeline Persistir-luego-Mutar

```
OnMessageAsync → RaiseEvent (solo registro, sin cambio de estado)
              → AppendAsync (persistir en IEventStore)
              → Mutate (aplicar eventos al estado en memoria)
              → Responder al llamante (solo tras éxito)
```

Si `AppendAsync` falla, los eventos no confirmados se descartan y `Version`
se revierte. El Actor **no se envenena** — el siguiente mensaje se procesa
correctamente.

### Mensajería: Ask vs Send

| Patrón | Método | Semántica |
|---------|--------|-----------|
| Solicitud-Respuesta | `AskAsync<TResult>(id, command)` | Retorna resultado tras procesar el mensaje |
| Dispara-y-Olvida | `Send(id, command)` | Sin respuesta; excepciones enrutadas a UnhandledErrorHandler |

### OutputChannel

Los Actores transmiten mensajes `ActorOutputEvent` a suscriptores externos:

```csharp
counter.OutputWriter = channel.Writer;
// Dentro de OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Cancela operaciones de larga duración sin detener el Actor:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... trabajo largo con _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
```

### Spawn

Los Actores crean Actores hijos mediante la referencia `System`:

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

## Integración de Módulos

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Registra IActorSystem + InMemoryEventStore

// Almacén de eventos personalizado
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// Desde PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` se registra como **Singleton**. Si `ILoggerFactory` está
registrado, se inyecta automáticamente un logger.

### IEventStore Personalizado

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT con control de concurrencia */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT ordenado por versión */ }
}
```

---

## Filosofía de Diseño (克制 / 专注 / 优雅 / 高效)

| Principio | En la práctica |
|-----------|------------|
| **克制 (Restraint / Moderación)** | Sin consenso distribuido, sin árboles de supervisión — solo Actores y Eventos. |
| **专注 (Focus / Enfoque)** | Monohilo por Actor. Un mensaje a la vez. |
| **优雅 (Elegance / Elegancia)** | Persistir-luego-Mutar: el estado cambia solo tras la persistencia. Rollback automático. |
| **高效 (Efficiency / Eficiencia)** | Compatible con AOT, cero reflexión, abstracciones `netstandard2.0`. |

---

## Casos de Uso

- **Sistemas de IA agente** — cada agente IA es un Actor con estado de conversación
- **Orquestación de flujos de trabajo** — Actores modelan procesos de negocio de larga duración
- **Estado de servidor de juegos** — Actores con Event Sourcing para estado de jugador/juego
- **Estado de dispositivos IoT** — Actores en memoria con snapshotting periódico

---

## Paquetes

| Paquete | Target | Descripción |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Abstracciones centrales: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `InMemoryEventStore`, integración PicoDI |

---

## Comparación

| Funcionalidad | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Solo en memoria | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Abstracciones netstandard2.0 | ✅ | ✅ | ✅ | ❌ |
| Integración PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persistir-luego-Mutar | ✅ | ❌ | ❌ | ❌ |
| Distribuido / Clustering | ❌ | ✅ | ✅ | ✅ |
| Monohilo por Actor | ✅ | ✅ | ✅ | ❌ |
| Paquetes | 2 | 8+ | 3+ | 10+ |

---

## Licencia

MIT — ver [LICENSE](LICENSE).
