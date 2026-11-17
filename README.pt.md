<p align="center"><img src="assets/picohex.svg" alt="PicoHex" width="120"></p>

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
| Dependências | ❌ Akka.NET (8+ pacotes), Orleans (10+ pacotes) | ✅ 1 pacote — PicoActor (inclui PicoActor.Abs e as abstrações PicoDI/PicoLog/PicoMediator); adicione PicoMediator para assinantes e PicoDI para o contêiner |
| Integração DI | ❌ Acoplado ao Microsoft.Extensions.DI | ✅ PicoDI nativo, resolução sem reflexão |
| netstandard2.0 | ⚠️ Suporte parcial no Akka.NET / Proto.Actor | ❌ Apenas net10.0 (o runtime do PicoMediator exige net10.0+) |
| Curva de aprendizado | ❌ Íngreme — árvores de supervisão, clustering, remoting | ✅ Mínima — Actor + Event + Mailbox |

---

## Início Rápido

```bash
dotnet add package PicoActor
dotnet add package PicoActor.EventStore.InMemory   # o armazenamento é um pacote separado (v2026.9)
```

```csharp
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

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
| `InMemoryEventStore` | Movido para **`PicoActor.EventStore.InMemory`** (v2026.9) — armazenamento em memória: bloqueio por fluxo, simultaneidade otimista (baseado em ConcurrentDictionary); registre com `AddPicoActorInMemoryStore()` |
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
`Mutate` deve ser uma função pura que nunca lança; se lançar, o lote já está durável e o ator é **parado** (removido do registro) em vez de continuar com estado não confiável.

### Snapshot Acceleration (opt-in) (Aceleração por snapshots, opcional)

Em fluxos longos, reproduzir todos os eventos a cada reconstrução é o custo de recuperação.
Um snapshot é um payload opaco que o ator salva a cada N eventos persistidos, de modo que
uma reconstrução o restaura e reproduz apenas a cauda. Snapshots são **somente** aceleração:
o fluxo de eventos nunca é truncado, no máximo um snapshot por
agregado é mantido, e um snapshot ausente, corrompido ou fora de faixa recorre a
uma reprodução completa.

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

- Gatilho: a cada N eventos **persistidos** (contados por evento, não por flush).
- Formato de transmissão: `IEventStoreSnapshot.SaveSnapshotAsync(id, new ActorSnapshot(version, payload))`;
  `version` é igual ao número de eventos que o payload cobre (`version == event count`).
- Falhas nunca afetam um comando: um `TakeSnapshot` que lança, um store que falha, ou
  um `RestoreSnapshot` que lança é reportado por `OnSnapshotFailed` /
  um log de aviso, e a recuperação recorre a reproduzir o fluxo inteiro.
- `IsReplaying` é `protected`: sobrescreva a publicação do read model guiada por `Mutate`
  com `if (IsReplaying) return;` para evitar trabalho O(n) por evento reproduzido
  (caso contrário, uma reconstrução é O(n²)).
- Etiqueta de esquema: sobrescreva `SnapshotSchema` (padrão `null`) com uma string de versão
  como `"CounterState/v1"` e **incremente-a sempre que o layout do payload ou seu significado
  mudar**. Alterá-la invalida uma vez todos os snapshots gravados — uma reprodução completa — e
  a velocidade normal volta quando o agregado persiste mais `SnapshotEveryNEvents` eventos.
  Mantenha-a como constante de tipo: um valor por instância que difira entre o ator que grava e
  o que reconstrói degrada silenciosamente toda recuperação para uma reprodução completa.
- Com uma etiqueta não nula o framework envolve o payload num envelope e compara as etiquetas
  na restauração; uma divergência (ou um payload sem envelope quando o ator declara etiqueta)
  descarta o snapshot e reproduz o fluxo inteiro. Apenas `null` desativa a proteção — `""` é uma
  etiqueta real. Uma etiqueta com mais de 255 bytes UTF-8 não grava snapshot algum
  (reportado por `OnSnapshotFailed`).
  O store deve continuar tratando o payload como **bytes opacos**: não o analise nem presuma
  que seja exatamente a saída de `TakeSnapshot()`.

```
[PAS1][1-byte UTF-8 schema length][UTF-8 schema][your payload]
```

### Event Log Reader (IEventLogReader) (Superfície de leitura do log de eventos)

O código de negócio é escrito contra `IActorSystem` e recebe atores; nenhum membro da superfície
de negócio devolve uma coleção de eventos. `IEventStore` é a SPI de persistência do **autor do
store**: persistir os eventos anexados e devolvê-los, com `LoadAsync` mantido como leitura de
reserva do runtime. Ferramentas e projeções leem através de `IEventLogReader` —
`GetVersionAsync`, `OpenAsync`, `PeekFirstAsync` — e `OpenAsync` devolve um cursor apenas para a
frente:

```csharp
await using var cursor = await reader.OpenAsync(id, fromVersion);  // 1-based, inclusive
ulong end = cursor.End;                    // stream length observed by this open
while (await cursor.MoveNextAsync())
{
    var version = cursor.CurrentVersion;   // strictly ascending
    var @event = cursor.Current;
}
```

- Uma única observação: `End` é o comprimento do fluxo obtido em `OpenAsync` e não muda durante a
  vida do cursor, pelo que a guarda de rollback e o comprimento da cauda vêm de uma só abertura,
  e o cursor nunca entrega mais do que `End`.
- A identificação de capacidade é uma só regra — `eventStore is IEventLogReader`. Um store que
  implementa o leitor permite que uma reconstrução acelerada por snapshot leia apenas a cauda
  após o snapshot (um store de ficheiros pode classificar a cabeça e desserializar apenas a
  cauda); um store que não o implementa continua a funcionar via `LoadAsync`, que lê e
  desserializa o fluxo inteiro.
- `InMemoryEventStore` implementa `IEventLogReader`; para qualquer outro store é opcional e
  puramente aditivo.

### JSONL Event Store (opt-in) (Armazenamento de eventos JSONL, opcional)

`PicoActor` executa atores; não inclui nenhum armazenamento. Dois pacotes o fornecem:
`PicoActor.EventStore.InMemory` (o padrão em memória, registrado com
`AddPicoActorInMemoryStore()`) e `PicoActor.EventStore.Jsonl` (durável, descrito nesta seção).

`PicoActor.EventStore.Jsonl` mantém um arquivo JSONL somente-append por agregado
(`<root>/<aggregateId>.jsonl`, um objeto JSON por linha), com os snapshots como sidecars atômicos
no diretório irmão `<root>-snapshots`. Implementa `IEventStore`, `IEventLogReader`,
`IEventStoreSnapshot` e `IEventStoreEnumerator`, é seguro para AOT (geração de código-fonte do
PicoJetson) e é de **processo único**: duas instâncias do armazenamento sobre um mesmo diretório
não compartilham portões, então escritores concorrentes falham de forma explícita em vez de
rasgar um fluxo.

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

- **Um registro esquecido aparece na compilação, não no primeiro append**: o pacote inclui o
  analisador `PICOSTORE001`, que aponta todo evento de domínio que nenhum `[PicoDerivedType]`
  lista (um projeto que só usa o armazenamento em memória não paga por ele).
- **Cauda rasgada versus corrupção.** A última linha sem terminação — a única forma que um crash do
  processo deixa — é ignorada, nunca contada e nunca lança; o próximo append a trunca. Uma linha
  **terminada** que não desserializa é corrupção e lança (a última inclusive), e o armazenamento não
  a repara, porque reparar é adivinhar. Esse é o custo documentado de deixar `FlushToDisk` no
  padrão `false`: os bytes chegam ao sistema operacional (outro leitor vê o evento imediatamente)
  mas não ao disco, então uma queda de energia pode deixar uma linha de lixo terminada. Defina
  `FlushToDisk = true` para fazer fsync a cada append.
- **Cursor e reconstrução apenas da cauda.** `GetVersionAsync`, o `End` do cursor e seu
  posicionamento em bytes vêm de uma única varredura barata (linhas completas, não vazias e
  terminadas — sem validação de JSON, sem desserializador), então uma reconstrução acelerada por
  snapshot abre em `snapshot.Version + 1` e desserializa apenas a cauda (`N − v` eventos). A
  economia é de desserialização e alocação, não de E/S de disco: a cabeça ainda é lida uma vez como
  bytes. Nenhuma sonda de "salto" do serializador é necessária — um fluxo genérico contém apenas
  eventos, então uma linha ilegível é corrupção, não um registro estranho.
- `MaxAggregateLocks` (padrão `1024`) limita a tabela de portões por agregado; uma entrada
  despejada custa apenas uma nova varredura no próximo append daquele agregado, nunca correção.
  `Logger` é independente do `ILoggerFactory` do DI: passe o mesmo logger explicitamente para ver
  os avisos do armazenamento no seu log.
- Fora desta versão: escritores multiprocesso, arquivamento/retenção/compactação, linhas estranhas
  (não eventos), criptografia/compressão. Eventos do framework **não pedem um segundo codec**: os vínculos
  os encaminham para serializadores gerados no pacote do store (veja «Framework events (event type
  bindings)» abaixo).

#### Stream naming (`FileNameFormat`)

`JsonlEventStoreOptions.FileNameFormat` escolhe como o **arquivo** de um stream é nomeado; o formato
de linha é idêntico nos dois modos, então só os nomes mudam — um diretório existente continua legível.

| Valor | Arquivo do stream | O que se ganha |
|---|---|---|
| `IdOnly` (padrão) | `<id:D>.jsonl` | o layout histórico, reproduzido byte a byte — nenhuma migração, nenhuma mudança em diretório existente |
| `TypePrefix` | `<type>-<id:D>.jsonl` | o tipo vai no nome, então `ListAggregateIdsAsync` resolve os ids só pelos nomes dos arquivos: **zero aberturas de arquivo** por enumeração |

`{type}` é `events[0].GetType().Name` — o tipo concreto do **primeiro evento persistido** do stream
(um tipo genérico mantém sua aridade com acento grave) — e fica **fixado na criação**: um lote
posterior cujo primeiro evento seja de outro tipo continua anexando ao nome com que o stream nasceu.

- **Migrar para `TypePrefix`.** O PicoAgent já nomeia seus streams `{type}-{id}.jsonl`, então apontar
  este store para esse diretório com `FileNameFormat = TypePrefix` **não exige nenhuma renomeação** —
  o diretório é lido como está, e os snapshots seguem inalterados (são indexados pelo id do agregado,
  `<id:D>.snap`, nunca pelo nome do stream). O sentido inverso não é oferecido: o prefixo faz parte do
  nome, um store `IdOnly` não consegue deduzi-lo e teria de renomear cada arquivo.
- **Um diretório misto é diagnosticado, nunca mesclado.** Entradas válidas apenas no *outro* layout são
  contadas (`ForeignFormatFiles`, por **nome** e de forma monotônica: é o portão da guarda de duplicidade
  do `IdOnly`) e reportadas como **um** Warning por varredura (a detecção é simétrica: `<id>.jsonl` em
  `TypePrefix` e `<type>-<id>.jsonl` em `IdOnly` contam ambos como "outro formato"). O diagnóstico do
  diretório inteiro conta **históricos**, não nomes: um diretório com **zero** históricos do formato
  configurado e **alguns** não vazios do outro lança `InvalidOperationException` nomeando as duas
  contagens, os dois formatos e as saídas — da enumeração, da construção e do lazy pass igualmente — em
  vez de reportar um sistema vazio; já um diretório só de restos de comprimento zero (um append que falha
  deixa um) é um armazenamento **vazio** e permanece silencioso. Um stream de *outro tipo de evento* não
  é mistura: é o caso normal e não custa contador nem linha de log. Um append para um id que já tem um
  stream não vazio no outro layout — ou dois prefixos diferentes — é recusado antes de qualquer byte ser
  escrito.
- **Custo.** `TypePrefix` paga uma passada de nomes de arquivo na construção (para aprender o mapa
  id → tipo), uma por resolução perdida, e um `stat` por append para a guarda de duplicidade. `IdOnly`
  não paga **nada** na construção nem para anexar a um stream que já possui; sua única passada de
  diretório é preguiçosa, na primeira observação de um stream que não tem, e nunca mais. Um lote vazio
  não escreve nem cria nada em nenhum dos modos, mas resolve seu caminho **exatamente como um append que
  escreve** e ainda valida `expectedVersion`.
- **O cache de erros serve só ao CAMINHO DE LEITURA.** Um erro de `TypePrefix` é lembrado — um conjunto
  limitado a 4.096 ids, esvaziado em vez de ampliado ao ultrapassá-lo — e compartilhado em
  **single-flight**: uma rajada de **sondagens concorrentes de UM mesmo id** ausente compartilha uma
  única passada, e as **repetições desse id** são grátis; um id ausente **diferente** paga uma passada
  própria. Um **append nunca confia nessa memória**: resolver ali "nenhum stream" criaria um segundo
  nome ao lado de um stream que um escritor externo criou depois de o mapa ser montado, então um append
  **varre de novo** ao errar e, ao acertar, descarta apenas **a sua própria** entrada — nunca em bloco,
  e sem pegar o portão da varredura. O preço é declarado: uma *leitura* pode responder "vazio" para um
  stream que apareceu ao lado até a próxima passada — e um host que nunca mais passa vê a resposta
  obsoleta até uma **nova instância** reler o diretório.
  `NegativeCacheInvalidations` conta as entradas que um append realmente invalidou (identicamente `0` no
  `IdOnly`, que nunca lembra um erro).
- **Um mapa desatualizado é corrigido pela PRÓXIMA varredura.** Um prefixo rival que chega depois de o
  mapa ser montado não é encontrado por uma leitura (um acerto de mapa não paga passada) nem por um
  append: o armazenamento não acrescenta uma passada de diretório ao caminho de append para fechar essa
  janela, porque devolveria exatamente o que o mapa rende, no único caminho que paga a cada append. A
  próxima passada de classificação — uma enumeração, ou o erro de um id desconhecido — registra o
  conflito, e a partir daí todo append para esse id é recusado e nomeia os dois arquivos.
- **Uma raiz quebrada ou um nome ocupado é nomeado, não engolido.** Uma raiz que existe como **arquivo**
  lança `IOException` (ali nada pode ser enumerado, aberto ou criado); um NOME de stream ocupado por um
  **diretório** lança `InvalidOperationException` nomeando o obstáculo — no caminho de leitura e no de
  append, nos dois formatos — em vez de "agregado vazio" ou uma violação de compartilhamento crua. Um
  lazy pass que falha (má configuração, raiz ilegível, raiz que é um arquivo) fica **latcheado**: a mesma
  exceção é relançada em toda observação posterior e o diretório nunca é varrido de novo, então a única
  recuperação é uma **instância nova** depois de o diretório ser corrigido.

#### Framework events (event type bindings) (eventos do framework: vínculos de tipo)

`SagaCompleted`/`SagaFailed` são `sealed`, derivam de `IDomainEvent` e não do seu `TEventBase`, então um
stream de saga guarda um tipo que o seu serializador nunca viu.
`JsonlEventStoreOptions.EventTypeBindings` encaminha os eventos do framework para o mesmo store — o framework
pré-registra `SagaCompleted → PicoActorSagaCompleted` e `SagaFailed → PicoActorSagaFailed` (`TryAdd`, então o
seu próprio `Add<T>(tag)` vence; `FrameworkDefaultCount` é quantos padrões foram realmente aplicados, `0…2`,
e `RegisteredCount` é o total).

- **`Add<T>(tag)` só encaminha, não gera nada.** O tag é o `$type` da linha e o que o lado de leitura fareja
  (primeiro membro, sem diferenciar maiúsculas, com queda incondicional para `TEventBase`), mas o tipo ainda
  precisa de um serializador gerado. `[GenerateSerializer(typeof(T))]` em um **positional record** não
  funciona (o pipeline de atributos do PicoSerDe não enxerga um construtor primário: `CS7036`/`CS8852`); a
  forma que funciona é um **ponto de chamada fechado** — as chamadas literais em
  `FrameworkEventSerializers.Generate()`. Um tipo vinculado sem ponto de geração é recusado no **primeiro
  append** (`InvalidOperationException`, com o remédio na mensagem).
- **Lotes mistos são uma só escrita:** `TEventBase` e eventos vinculados compartilham o lote — um buffer, uma
  escrita, um flush.
- **Nomes.** Os dois tags do framework são os tags que os corpora de produção já carregam, e são formato de
  fio permanente (sem alias). Um stream é nomeado, como sempre, pelo nome curto CLR do seu primeiro evento;
  com `TypePrefix`, um nome fora de todo prefixo de **tipo** registrado
  (`unknown-<id>.jsonl`, categorização própria de um host) é **alcançável por id**, invisível à enumeração por tipo, nunca contado em `ForeignFormatFiles` e nunca
  dispara o throw do diretório inteiro. Um prefixo não contém `-`; nomes curtos iguais compartilham prefixo.
- **`SagaCompleted.Result` é `object?` — e seu serializador não sabe gravar nenhum valor não nulo** (o
  writer gerado escreve só o nome do membro, sem valor). `null` — a única forma que um **store
  persistente** aceita hoje — faz ida e volta; qualquer outra coisa falha na hora e não grava um único byte.
  `MarkComplete(result)` com um resultado não nulo não pode ser persistido: o lote terminal lança e nada é
  gravado. Num store persistente, mantenha `Result` em `null`.
- **Regras de registro.** Os vínculos ficam na instância de opções —
  `options.EventTypeBindings.Add<T>(tag)` **antes** de construir o store: o construtor **congela e copia** a
  tabela, então um `Add` posterior lança em vez de ser ignorado em silêncio. Um tipo tem exatamente um tag e
  um tag nomeia exatamente um tipo (o mesmo par de novo é um no-op **antes da congelação**; um tag não é
  vazio e não contém aspas, barra invertida nem caractere de controle). Uma base que já cobre os eventos do
  framework (`IDomainEvent` em si) é recusada na construção — seriam ao mesmo tempo eventos comuns e
  vinculados — e o mesmo vale para um tipo vinculado atribuível a `TEventBase`. `with` copia a **referência**:
  o clone e o original compartilham uma só tabela, incluindo o estado de congelação, então um `Add` no clone
  lança e nomeia o *outro* store — construa `new JsonlEventStoreOptions()` para uma tabela independente.
- **O tag de um vínculo deve diferir de todo discriminador `[PicoDerivedType]` de `TEventBase`.** O sniff tem
  precedência: uma colisão lê as linhas da base como o tipo vinculado — em silêncio, sem exceção e sem aviso.

### Idle Passivation (opt-in) (Passivação por inatividade, opcional)

Os atores permanecem em memória até serem parados. Para cargas de trabalho com muitos
agregados de vida longa, um orçamento de inatividade recupera o estado materializado
de atores que podem ser reconstruídos:

```csharp
var system = new ActorSystem(new ActorSystemOptions
{
    EventStore = store,
    IdleTimeout = TimeSpan.FromMinutes(5),        // default null = disabled
    IdleSweepInterval = TimeSpan.FromSeconds(30), // default: half the budget, min 1s
});
```

- Candidatos são **apenas atores com event sourcing reconstruíveis**: um
  `EventSourcedActor` cujo tipo concreto foi registrado **com uma fábrica de
  reconstrução**. Atores não-ES nunca são tocados (seu estado não pode ser reconstruído).
- Um candidato deve estar inicializado, não estar processando um comando, ter a caixa de correio vazia
  e estar inativo durante todo o orçamento. A parada viaja pela
  caixa de correio como uma sentinela do framework, então ela cai em um limite de mensagem; a solicitação
  de parada tem verificação de propriedade, então uma solicitação obsoleta não pode remover uma instância
  mais nova do mesmo id.
- `SagaActor` é isento (`CanBePassivated => false`): esperar uma resposta
  externa parece inatividade, e recuperá-lo descartaria essa resposta.
- A reativação é explícita: `GetAsync<T>(id)` reproduz o fluxo (ou restaura
  o snapshot e reproduz a cauda). Até então `Send`/`AskAsync` se comportam
  exatamente como qualquer id não registrado (`KeyNotFoundException`) — `AskAsync` não tem
  parâmetro de tipo, então não pode escolher uma fábrica de reconstrução.
- Observabilidade: `SweepIdleActorsAsync()`, `PassivatedCount`, `ReactivatedCount`,
  e uma linha de log por reconstrução com o número de eventos reproduzidos.

### SagaActor (Coordenador de vida finita)

`SagaActor extends EventSourcedActor` — para operações entre agregados com ciclo de vida finito. Diferente de um EventSourcedActor comum cuja caixa de correio roda para sempre, um SagaActor **para automaticamente** depois que o framework persiste seu evento terminal.

O estado terminal é **gerado pelo framework**: `MarkComplete(result)` acrescenta um evento `SagaCompleted(result)` ao mesmo lote dos seus eventos de negócio (append atômico — conclusão e persistência não podem divergir); uma exceção de negócio não tratada acrescenta `SagaFailed(reason)` (`"ExceptionType: message"`, truncado em 512 caracteres) e propaga `SagaExecutionException(Id, Reason)` ao chamador. Subclasses nunca lançam nem tratam esses eventos — o `Mutate` só vê eventos de negócio.

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

**Ciclo de vida:**

```
CreateAsync(cmd) → mailbox processes cmd → MarkComplete(result)
→ framework appends SagaCompleted(result) in the same flush batch (atomic)
→ ProcessAsync returns → auto-stop (fire-and-forget) → removed from registry
```

Falha de negócio: `OnMessageAsync`/`ResumeAsync` lança exceção → o framework descarta os eventos de negócio não commitados, acrescenta `SagaFailed(reason)`, para automaticamente e retorna a falha `SagaExecutionException(Id, Reason)` ao chamador do Ask. Falhas de infraestrutura (store fora do ar) **não** são terminais — os eventos revertem e a saga permanece Running.
Se o próprio `SagaFailed` não puder ser persistido (store ainda fora), nenhum estado terminal é produzido e a exceção **original** é propagada ao chamador.

**Recuperação de falhas:** `GetAsync` repete os eventos → o framework restaura `Completed`/`Failed` a partir de `SagaCompleted`/`SagaFailed` (sagas terminais permanecem mortas — `GetAsync` retorna null). Sagas sem evento terminal recebem a chamada de `ResumeAsync()` e, se isso atingir o estado terminal, o framework persiste `SagaCompleted` no mesmo lote. A recuperação é pull explícito, não mágica em segundo plano.

**Recuperação explícita em lote:**

```csharp
var results = await system.ResumeInterruptedSagasAsync<OrderSaga>(
    nameof(OrderPlaced));

foreach (var r in results)   // SagaResumeResult(Id, Status, Reason?)
{
    // SagaResumeStatus.Completed | Failed (Reason) | Running
}
```

`ResumeInterruptedSagasAsync<TSaga>(firstEventType, match?)` enumera sagas pelo nome do tipo do primeiro evento, recupera cada uma via `GetAsync` com single-flight e devolve a classificação pós-recuperação: `Completed` / `Failed` (com a razão recuperada pelo framework) / `Running` (ainda aguardando entrada externa). Sagas já terminais nunca ressuscitam; sagas vivas são classificadas in-place. Idempotente e seguro para repetir (single-flight por id); falha do store falha rápido para que o chamador repita o lote inteiro.

**Padrão process manager:**

a mesma classe base cobre process managers — eventos externos são traduzidos a comandos por um handler de eventos no nível da aplicação (ex.: um assinante do PicoMediator), que os envia à caixa da saga. Eventos nunca entram diretamente nos atores; a saga só vê comandos.

**API de conveniência:**

```csharp
var execution = await system.ExecuteSaga<OrderSaga, Guid>(new PlaceOrder(orderId));
// SagaExecution<Guid>(Id, Result) — saga auto-stops, no StopAsync needed
```

`ExecuteSaga<TSaga, TResult>(command)` faz `CreateAsync` + `AskAsync` numa única chamada. Retorna `SagaExecution<TResult>(Id, Result)`; em falha de negócio lança `SagaExecutionException(SagaId, Reason)` para que o chamador sempre obtenha o id da saga.

### Mensageria: Ask vs Send

| Padrão | Método | Semântica |
|---------|--------|-----------|
| Requisição-Resposta | `AskAsync<TResult>(id, command)` | Retorna resultado após processamento da mensagem |
| Dispara-e-Esquece | `Send(id, command)` | Sem resposta; falhas são roteadas para UnhandledErrorHandler (stderr sem logger); lança para ator inexistente ou mailbox fechada (corrida de parada) |

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
container.AddPicoActor();               // Registra IActorSystem — nenhum armazenamento
container.AddPicoActorInMemoryStore();  // PicoActor.EventStore.InMemory: o padrão em memória

// Armazenamento de eventos personalizado
var store = new InMemoryEventStore();
container.AddPicoActor(store);

// A partir do PicoCfg
var cfg = CfgBind.Bind<ActorConfig>(configuration, "Actor");
container.AddPicoActor(cfg);
```

`IActorSystem` é registrado como **Singleton**. Se `ILoggerFactory` estiver
registrado, um logger é automaticamente injetado.

`AddPicoActor()` não registra mais nenhum armazenamento, e não falha no registro: a **resolução**
de `IActorSystem` lança `InvalidOperationException` nomeando `AddPicoActorInMemoryStore()` (assim a
ordem de registro continua livre e um `AddPicoActor(store)` explícito ainda ganha).

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

Handlers podem ser declarados como class ou record. Uma implementação sem construtor de instância acessível (public/internal) falha na compilação com **PICA001** em vez de gerar código de registro quebrado.

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

> **Pacotes necessários:** além de `PicoActor` (inclui `PicoActor.Abs` e as abstrações `PicoDI.Abs`/`PicoLog.Abs`/`PicoMediator.Abs`/`PicoMediator`) é preciso um pacote de armazenamento: `PicoActor.EventStore.InMemory` em memória (registrado com `AddPicoActorInMemoryStore()`) ou `PicoActor.EventStore.Jsonl` para persistência (inclui PicoJetson). Para declarar handlers `IDomainEventSubscriber<TEvent>` não é preciso nenhum pacote extra: `PicoMediator` já é uma dependência de `PicoActor` (o bridge gerado chama o `MediatorAutoSubscriptionRegistry` dele). Adicione `PicoDI` + `PicoMediator.DI` apenas quando usar o contêiner (`SvcContainer`, `AddPicoMediator`).

> **Desenvolvimento local (ProjectReference):** analyzers não se propagam por cadeias `ProjectReference` — consumidores por projeto devem adicionar uma referência direta a `PicoActor.Gen` (`<ProjectReference Include="..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />`, espelho de `tests/PicoActor.Tests`). Consumidores NuGet recebem o gerador automaticamente via os props `buildTransitive` de `PicoActor.Abs` — sem referência adicional.

Os eventos fluem como envelopes através do PicoMediator após persist+mutate; o replay nunca publica. Falhas de handler nunca afetam o ator (isolamento por handler). Loops de tradução (evento → comando → evento) são intencionais; mantenha os handlers idempotentes e limitados.

> **Mudança incompatível:** assinantes diretos `ISubscriber<TEvent>` (PicoMediator) não recebem mais eventos de domínio do PicoActor. Migre para `IDomainEventSubscriber<TEvent>`; o `ActorId`/`Version` do envelope substitui qualquer id de agregado embutido manualmente. Publishers personalizados (`AddPicoActor(IPublisher)`) agora recebem instâncias de `DomainEventEnvelope` em vez de eventos crus — adapte as implementações de `Publish<TEvent>` em consequência (apenas a forma do payload observado mudou; o pipeline do ator não é afetado). `IEventStoreEnumerator.ListAggregateIds(string)` passou a `ListAggregateIdsAsync(string)` (assíncrono) — enumeradores personalizados precisam atualizar a assinatura.

Notas:
- `Register<T>` deve ser chamado uma única vez por tipo de ator: uma segunda chamada lança exceção em vez de substituir silenciosamente a primeira fábrica.
- `Register<T>` rejeita tipos que o runtime não consegue servir: implementações que não derivam de `Actor` e implementações de `IEventSourcedActor` que não derivam de `EventSourcedActor` (a persistência só é ligada para as bases concretas).
- `StopAsync`/`RequestStop` removem primeiro o ator do registro e depois drenam as mensagens já presentes na mailbox (parada graciosa); mensagens enviadas após a parada falham com `KeyNotFoundException` (ou `InvalidOperationException` se a mailbox já estava fechada — janela de corrida de parada).
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
| [PicoActor](https://www.nuget.org/packages/PicoActor) | `net10.0` | Runtime: `ActorSystem`, `MediatorDomainEventPublisher` (saída de eventos por envelope), integração PicoDI (`AddPicoActor` conecta automaticamente IMediator + ICommandSender) |
| [PicoActor.EventStore.InMemory](https://www.nuget.org/packages/PicoActor.EventStore.InMemory) | `net10.0` | O armazenamento em memória (`InMemoryEventStore`, separado de `PicoActor` em v2026.9) + `AddPicoActorInMemoryStore()` |
| [PicoActor.EventStore.Jsonl](https://www.nuget.org/packages/PicoActor.EventStore.Jsonl) | `net10.0` | Armazenamento de eventos JSONL durável: somente-append, simultaneidade otimista, reconstrução apenas da cauda via `IEventLogReader`, sidecars de snapshot atômicos, seguro para AOT (PicoJetson) — mais o analisador `PICOSTORE001` de registro |

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
| Pacotes | 1 (+2 optional) | 8+ | 3+ | 10+ |

---

## Licença

MIT — veja [LICENSE](LICENSE).
