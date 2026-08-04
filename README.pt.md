# PicoActor

Framework de Atores em memória compatível com AOT e Event Sourcing para .NET.
Leve, zero reflexão, projetado para sistemas de IA agente e orquestração
de workflows. Executa sob NativeAOT e trimming.

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

Cada Ator possui uma **mailbox** (`Channel<Envelope>` em memória), uma
identidade **UUID v7** e um loop de consumo single-threaded. Comandos são
entregues via `Send` (dispara-e-esquece) ou `AskAsync` (requisição-resposta).

Atores com Event Sourcing seguem **Persistir-depois-Mutar**:
`OnMessageAsync → RaiseEvent → Persistir no IEventStore → Mutar estado`.
O estado só é alterado após persistência bem-sucedida — o estado em memória
está sempre consistente com o fluxo de eventos.

---

## Por que PicoActor?

| Preocupação | Opções existentes | PicoActor |
|---------|:----------------:|:--------------:|
| AOT / Trimming | ❌ Akka.NET, Proto.Actor, Orleans exigem reflexão | ✅ Suporte completo NativeAOT |
| Event Sourcing | ❌ Proto.Actor, Orleans sem ES integrado | ✅ Persistir-depois-Mutar, rollback automático |
| Dependências | ❌ Akka.NET (8+ pacotes), Orleans (10+ pacotes) | ✅ 2 pacotes, zero dependências além de Channels |
| Integração DI | ❌ Acoplado ao Microsoft.Extensions.DI | ✅ PicoDI nativo, resolução sem reflexão |
| netstandard2.0 | ⚠️ Suporte parcial no Akka.NET / Proto.Actor | ✅ Abstrações target netstandard2.0 |
| Curva de aprendizado | ❌ Íngreme — árvores de supervisão, clustering, remoting | ✅ Mínima — Actor + Event + Mailbox |

---

## Início Rápido

```bash
dotnet add package PicoActor
```

```csharp
using PicoActor;
using PicoActor.Abs;

// 1. Configuração
var store = new InMemoryEventStore();
var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

// 2. Registrar fábricas de Atores
system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException()
    },
    rebuildFactory: () => new Counter()
);

// 3. Criar, enviar mensagens, parar, reconstruir
var counter = await system.CreateAsync<Counter>(new CreateCounter(42));
system.Send(counter.Id, new Increment(5));
var value = await system.AskAsync<int>(counter.Id, new GetValue());
await system.StopAsync(counter.Id);
var rebuilt = await system.GetAsync<Counter>(counter.Id);
```

---

## Detalhes do Módulo

### PicoActor.Abs — Abstrações Centrais

Target `netstandard2.0` para máxima compatibilidade.

| Tipo | Papel |
|------|------|
| `IActor` | Interface base — fornece `Id` (UUID v7) |
| `IActorSystem` | Contrato de runtime — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, ExecuteSaga |
| `ICommand` | Interface marcadora para comandos |
| `IDomainEvent` | Interface marcadora para eventos de domínio |
| `IEventSourcedActor` | Interface opcional — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Contrato de persistência — AppendAsync (concorrência otimista), LoadAsync |
| `ICancelable` | Opcional — CancelCurrentTurn para operações de longa duração |
| `Actor` | Classe base abstrata — mailbox, loop de consumo, SignalReady, StopAsync |
| `EventSourcedActor` | Base ES — RaiseEvent, Mutate, pipeline Persistir-depois-Mutar |
| `Envelope` | Interno — encapsula ICommand com TaskCompletionSource opcional |
| `ActorOutputEvent` | Notificação de saída — Type, Data, ToolCallId/ToolName/TurnId opcionais |
| `ConcurrencyException` | Lançada pelo IEventStore em caso de divergência de versão |

### PicoActor — Runtime

Target `net10.0`, compatível com AOT.

| Tipo | Papel |
|------|------|
| `ActorSystem` | `IActorSystem` padrão — registro ConcurrentDictionary, fábricas, roteamento, CancelTurn |
| `InMemoryEventStore` | Armazenamento em memória lock-free — baseado em ConcurrentDictionary |
| `ActorConfig` | POCO de configuração — vinculável a partir do PicoCfg |
| `ActorSystemOptions` | Opções — EventStore obrigatório, Logger opcional, DomainEventPublisher opcional; consumido pelo construtor de `ActorSystem` |
| `PicoActorDiExtensions` | Método de extensão `AddPicoActor()` para PicoDI |

### Ator (Não-ES)

Herde de `Actor` para atores operacionais puramente em memória.

```csharp
public sealed class EchoActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => new ValueTask<object?>(command);
}
```

### Ator com Event Sourcing

Herde de `EventSourcedActor`. Sobrescreva `OnMessageAsync` para chamar
`RaiseEvent`, e `Mutate` para aplicar mudanças de estado.

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

### Pipeline Persistir-depois-Mutar

```
OnMessageAsync → RaiseEvent (apenas registro, sem mudança de estado)
              → AppendAsync (persistir no IEventStore)
              → Mutate (aplicar eventos ao estado em memória)
              → Responder ao chamador (somente após sucesso)
```

Se `AppendAsync` falhar, os eventos não confirmados são descartados e
`Version` é revertida. O Ator **não é envenenado** — a próxima mensagem
é processada normalmente.

### Mensageria: Ask vs Send

| Padrão | Método | Semântica |
|---------|--------|-----------|
| Requisição-Resposta | `AskAsync<TResult>(id, command)` | Retorna resultado após processamento da mensagem |
| Dispara-e-Esquece | `Send(id, command)` | Sem resposta; exceções roteadas para UnhandledErrorHandler |

### OutputChannel

Atores transmitem mensagens `ActorOutputEvent` para assinantes externos:

```csharp
counter.OutputWriter = channel.Writer;
// Dentro de OnMessageAsync:
WriteOutput("Incremented", data: "Delta=5");
```

### CancelTurn

Cancela operações de longa duração sem parar o Ator:

```csharp
public sealed class MyActor : Actor, ICancelable
{
    private CancellationTokenSource? _currentTurnCts;
    public void CancelCurrentTurn() => _currentTurnCts?.Cancel();

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        _currentTurnCts = CancellationTokenSource.CreateLinkedTokenSource(StopToken);
        // ... trabalho longo com _currentTurnCts.Token
    }
}
system.CancelTurn(actor.Id);
```

### Spawn

Atores criam atores filhos através da referência `System`:

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

## Integração de Módulos

### PicoDI

```csharp
var container = new SvcContainer();
container.AddPicoActor();  // Registra IActorSystem + InMemoryEventStore

// Armazenamento de eventos personalizado
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// A partir do PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` é registrado como **Singleton**. Se `ILoggerFactory` estiver
registrado, um logger é automaticamente injetado.

### IEventStore Personalizado

```csharp
public sealed class PostgresEventStore : IEventStore
{
    public ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events) { /* INSERT com verificação de concorrência */ }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    { /* SELECT ordenado por versão */ }
}
```

---

## Filosofia de Design (克制 / 专注 / 优雅 / 高效)

| Princípio | Na prática |
|-----------|------------|
| **克制 (Restraint / Moderação)** | Sem consenso distribuído, sem árvores de supervisão — apenas Atores e Eventos. |
| **专注 (Focus / Foco)** | Single-threaded por Ator. Uma mensagem por vez. |
| **优雅 (Elegance / Elegância)** | Persistir-depois-Mutar: estado muda apenas após persistência. Rollback automático. |
| **高效 (Efficiency / Eficiência)** | Compatível com AOT, zero reflexão, abstrações `netstandard2.0`. |

---

## Casos de Uso

- **Sistemas de IA agente** — cada agente IA é um Ator com estado de conversa
- **Orquestração de workflows** — Atores modelam processos de negócio de longa duração
- **Estado de servidor de jogos** — Atores com Event Sourcing para estado de jogador/jogo
- **Estado de dispositivos IoT** — Atores em memória com snapshotting periódico

---

## Pacotes

| Pacote | Target | Descrição |
|---------|--------|-------------|
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `netstandard2.0` | Abstrações centrais: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `InMemoryEventStore`, integração PicoDI |

---

## Comparação

| Funcionalidade | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Apenas em memória | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Abstrações netstandard2.0 | ✅ | ✅ | ✅ | ❌ |
| Integração PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persistir-depois-Mutar | ✅ | ❌ | ❌ | ❌ |
| Distribuído / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded por Ator | ✅ | ✅ | ✅ | ❌ |
| Pacotes | 2 | 8+ | 3+ | 10+ |

---

## Licença

MIT — veja [LICENSE](LICENSE).
