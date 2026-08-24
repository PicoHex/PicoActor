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

O PicoActor é **orientado a mensagens**: toda interação é uma mensagem. Comandos (`ICommand`) são mensagens direcionadas entregues via mailbox (1:1, resposta opcional); eventos de domínio (`IDomainEvent`) são mensagens de difusão publicadas via PicoMediator (1:N, sem resposta). Eventos também são mensagens — o padrão de colaboração entre agregados é um único ciclo: evento → assinante → comando traduzido → mailbox. Não há outra forma de interagir com um ator.

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
| Dependências | ❌ Akka.NET (8+ pacotes), Orleans (10+ pacotes) | ✅ 4 pacotes — PicoActor + PicoActor.Abs + PicoMediator.Abs + PicoDI.Abs; sem outras dependências de runtime |
| Integração DI | ❌ Acoplado ao Microsoft.Extensions.DI | ✅ PicoDI nativo, resolução sem reflexão |
| netstandard2.0 | ⚠️ Suporte parcial no Akka.NET / Proto.Actor | ❌ Apenas net10.0 (o runtime do PicoMediator exige net10.0+) |
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

Target `net10.0` (o runtime do PicoMediator e o código bridge gerado exigem net10.0+).

| Tipo | Papel |
|------|------|
| `IActor` | Interface base — fornece `Id` (UUID v7) |
| `IActorSystem` | Contrato de runtime — Register, CreateAsync, FindAggregateIds, GetAsync, Send, AskAsync, StopAsync, RequestStop, StopAllAsync, ExecuteSaga, ResumeInterruptedSagasAsync |
| `ICommand` | Interface marcadora para comandos |
| `IDomainEvent` | Interface marcadora para eventos de domínio |
| `IEventSourcedActor` | Interface opcional — Version, ReplayEvents, CommitEvents |
| `IEventStore` | Contrato de persistência — AppendAsync (concorrência otimista), LoadAsync, PeekFirstAsync |
| `Actor` | Classe base abstrata — mailbox, loop de consumo, SignalReady, StopAsync |
| `EventSourcedActor` | Base ES — RaiseEvent, Mutate, pipeline Persistir-depois-Mutar |
| `SagaActor` | Coordenador ES de vida finita — eventos terminais do framework (SagaCompleted/SagaFailed), auto-stop, recuperação em lote explícita via ResumeInterruptedSagasAsync |
| `IDomainEventSubscriber<TEvent>` | Contrato de assinante — envelope tipado + `ICommandSender`; auto-registrado pelo PicoActor.Gen (declare-and-subscribe) |
| `DomainEventEnvelope` / `DomainEventEnvelope<TEvent>` | Envelope de contexto — `ActorId`, `Version`, `Event` (transporte / entrega tipada) |
| `ICommandSender` | Porta estreita para handlers — Send, AskAsync, ExecuteSaga |
| `Envelope` | Interno — encapsula ICommand com TaskCompletionSource opcional |
| `ActorOutputEvent` | Notificação de saída — Type, Data, TurnId opcionais |
| `ConcurrencyException` | Lançada pelo IEventStore em caso de divergência de versão |

### PicoActor — Runtime

Target `net10.0`, compatível com AOT.

| Tipo | Papel |
|------|------|
| `ActorSystem` | `IActorSystem` padrão — registro ConcurrentDictionary, fábricas, roteamento |
| `InMemoryEventStore` | Armazenamento em memória com bloqueio por fluxo — simultaneidade otimista, baseado em ConcurrentDictionary |
| `ActorConfig` | POCO de configuração — vinculável a partir do PicoCfg |
| `ActorSystemOptions` | Opções — EventStore obrigatório, Logger opcional, DomainEventPublisher opcional; consumido pelo construtor de `ActorSystem` |
| `MediatorDomainEventPublisher` | `IDomainEventPublisher` padrão — publica `DomainEventEnvelope` por evento com isolamento por evento |
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

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    { /* SELECT do primeiro evento (enumeração de recuperação) */ }
}
```

### Saída de eventos (PicoMediator)

`IDomainEvent : IEvent` — eventos de domínio são notificações de primeira classe para o PicoMediator. Após persist+mutate, o framework os publica como **envelopes de contexto** por meio do hook `IDomainEventPublisher`; o replay (recuperação) não republica.

`MediatorDomainEventPublisher` é o adaptador pronto para uso: envolve cada evento em um `DomainEventEnvelope(actorId, version, event)` e o publica via `Publish<DomainEventEnvelope>` (genérico em tempo de compilação, seguro para AOT) com isolamento por evento — um assinante com falha não bloqueia os eventos seguintes.

### Assinatura de eventos de domínio (declare-and-subscribe)

Handlers de eventos são classes simples que implementam `IDomainEventSubscriber<TEvent>` — o PicoActor.Gen (integrado no PicoActor.Abs) os escaneia e auto-registra; zero fiação manual. O handler recebe um envelope tipado com o contexto do agregado fonte (`ActorId`, `Version`) mais uma porta estreita `ICommandSender`:

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

// Fiação: AddPicoMediator registra IMediator; AddPicoActor() o detecta na
// fábrica do ActorSystem e conecta a saída de eventos (preguiçoso — compatível com Scoped).
var container = new SvcContainer();
container.AddPicoMediator();  // declare-and-subscribe: assinantes registrados automaticamente
container.AddPicoActor();     // conecta automaticamente MediatorDomainEventPublisher
container.Build();
await using var scope = container.CreateScope();
var system = (IActorSystem)scope.GetService(typeof(IActorSystem));

// Fiação explícita para publishers personalizados: AddPicoActor(IPublisher)
// (a instância deve estar disponível antes de Build()).
```

> **Desenvolvimento local (ProjectReference):** analyzers não se propagam por cadeias `ProjectReference` — consumidores por projeto devem adicionar uma referência direta a `PicoActor.Gen` (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, espelho de `tests/PicoActor.Tests`). Consumidores NuGet recebem o gerador automaticamente via os props `buildTransitive` de `PicoActor.Abs` — sem referência adicional.

Os eventos fluem como envelopes através do PicoMediator após persist+mutate; o replay nunca publica. Falhas de handler nunca afetam o ator (isolamento por handler). Loops de tradução (evento → comando → evento) são intencionais; mantenha os handlers idempotentes e limitados.

> **Mudança incompatível:** assinantes diretos `ISubscriber<TEvent>` (PicoMediator) não recebem mais eventos de domínio do PicoActor. Migre para `IDomainEventSubscriber<TEvent>`; o `ActorId`/`Version` do envelope substitui qualquer id de agregado embutido manualmente. Publishers personalizados (`AddPicoActor(IPublisher)`) agora recebem instâncias de `DomainEventEnvelope` em vez de eventos crus — adapte as implementações de `Publish<TEvent>` em consequência (apenas a forma do payload observado mudou; o pipeline do ator não é afetado).

Notas:
- **A tradução evento→comando é responsabilidade do assinante (camada de negócios)** — o PicoActor apenas publica; comandos entram nos atores exclusivamente via mailbox.
- A publicação ocorre **após persist+mutate** — uma falha de publicação não corrompe o estado do ator (os eventos já são duráveis).
- A recuperação é silenciosa: o replay não republica.
- Eventos do framework (`SagaCompleted`/`SagaFailed`) podem ser assinados tipicamente como qualquer outro evento (Abs visa net10.0).
- **Fiação automática segura de qualquer scope**: desde o PicoDI 2026.8.1 (E1), fábricas de singletons usam o scope raiz interno do contêiner — o IMediator auto-conectado vive até a liberação do contêiner.
- **Nunca use `AskAsync` no agregado que publica a partir do próprio caminho de publicação** — o mailbox fonte está ocupado fazendo flush do evento; a requisição se auto-bloquearia. Consulte projeções de leitura (atores separados); `Send` ao agregado fonte é seguro (dispara-e-esquece).

---

## Filosofia de Design (克制 / 专注 / 优雅 / 高效)

| Princípio | Na prática |
|-----------|------------|
| **克制 (Restraint / Moderação)** | Sem consenso distribuído, sem árvores de supervisão — apenas Atores e Eventos. |
| **专注 (Focus / Foco)** | Single-threaded por Ator. Uma mensagem por vez. |
| **优雅 (Elegance / Elegância)** | Persistir-depois-Mutar: estado muda apenas após persistência. Rollback automático. |
| **高效 (Efficiency / Eficiência)** | Compatível com AOT, zero reflexão, abstrações `net10.0`. |

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
| [PicoActor.Abs](https://www.nuget.org/packages/PicoActor.Abs) | `net10.0` | Abstrações centrais: `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor` — mais tipos de assinatura (`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`) e o analisador embutido `PicoActor.Gen` (declare-and-subscribe) |
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `InMemoryEventStore`, `MediatorDomainEventPublisher` (saída de eventos por envelope), integração PicoDI (`AddPicoActor` conecta automaticamente IMediator + ICommandSender) |

---

## Comparação

| Funcionalidade | PicoActor | Akka.NET | Proto.Actor | Orleans |
|---------|:---:|:--:|:--:|:--:|
| Apenas em memória | ✅ | ✅ | ✅ | ❌ |
| AOT / Trimming | ✅ | ❌ | ❌ | ❌ |
| Event Sourcing | ✅ | ✅ | ❌ | ❌ |
| Abstrações netstandard2.0 | ❌ | ✅ | ✅ | ❌ |
| Integração PicoDI | ✅ | ❌ | ❌ | ❌ |
| Persistir-depois-Mutar | ✅ | ❌ | ❌ | ❌ |
| Distribuído / Clustering | ❌ | ✅ | ✅ | ✅ |
| Single-threaded por Ator | ✅ | ✅ | ✅ | ❌ |
| Pacotes | 4 | 8+ | 3+ | 10+ |

---

## Licença

MIT — veja [LICENSE](LICENSE).
