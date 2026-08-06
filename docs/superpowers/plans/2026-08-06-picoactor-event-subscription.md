# PicoActor 事件订阅抽象 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 PicoActor 增加订阅侧抽象(`IDomainEventSubscriber<TEvent>` + 上下文信封 + `ICommandSender` 窄端口),事件经 PicoMediator 以非泛型信封传输,由内嵌的 PicoActor.Gen 源码生成器 declare-and-subscribe 自动注册,业务代码零 PicoMediator 依赖。

**Architecture:** 三层:①PicoActor.Abs 新增订阅者契约(类型化信封交付,逐调用传入 `ICommandSender`);②PicoActor runtime 的 `MediatorDomainEventPublisher` 改为发布非泛型 `DomainEventEnvelope(actorId, version, event)`,`AddPicoActor` 注册 `ICommandSender` 适配器;③新 analyzer 项目 PicoActor.Gen(照抄 PicoMediator.Gen 蓝图,内嵌进 PicoActor.Abs)扫描 `IDomainEventSubscriber<TEvent>` 闭合实现,生成每程序集 configurator 注册进共享的 `MediatorAutoSubscriptionRegistry`,`AddPicoMediator()` 的 `TryApplyConfiguration` 自动应用。破坏性变更:`ISubscriber<TEvent>` 直连订阅者不再收到 PicoActor 事件。

**Tech Stack:** net10.0(Abs/runtime/测试)、netstandard2.0(analyzer)、Roslyn `Microsoft.CodeAnalysis.CSharp` 5.6.0、PicoMediator 2026.8.2(共享注册表 + 传输)、PicoDI 2026.8.2、TUnit 1.63.0。

## Global Constraints

- 目标框架:PicoActor/PicoActor.Abs/PicoActor.Tests 均为 net10.0;PicoActor.Gen 为 netstandard2.0(analyzer 惯例)
- **AOT 硬约束**:运行时路径零反射——类型分发只允许编译期泛型(`is TEvent` / 泛型实例化),禁止 `MakeGenericMethod`/`MakeGenericType`/`dynamic`
- 分层:PicoMediator 不认识 PicoActor 类型;PicoActor 生成代码引用 `PicoMediator.MediatorAutoSubscriptionRegistry` + `PicoDI.Abs`(PicoActor.Abs.csproj 已有 `PrivateAssets="all"`)
- 生成代码规则(与 PicoMediator.Gen 的**有意差异**):handler 注册与 bridge 注册均**不用** `IsRegistered` 去重(append 语义——PicoMediator.Gen 按 service type 去重会让同一事件的第二个 handler 类被跳过,违反"多订阅者同事件"契约);configurator id = `pico-actor::<assemblyName>`(每程序集一个,键不相交,与 `pico-mediator::` 序无关)
- 测试命令:`dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj` 与 `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`(CI 同款)
- 提交信息用仓库现有约定:`feat:` / `refactor:` / `test:` / `docs:` 前缀
- 破坏性变更(用户已确认):`ISubscriber<TEvent>` 直连订阅失效,迁移为 `IDomainEventSubscriber<TEvent>`
- 不引入新的 PicoMediator/PicoDI 运行时依赖;PicoActor.Abs.csproj 只加 PicoActor.Gen 的 analyzer ProjectReference

---

### Task 1: PicoActor.Abs 订阅者抽象类型

**Files:**
- Create: `src/PicoActor.Abs/IDomainEventSubscriber.cs`
- Create: `src/PicoActor.Abs/ICommandSender.cs`
- Test: `tests/PicoActor.Abs.Tests/EventSubscriptionAbstractionTests.cs`

**Interfaces:**
- Consumes: `PicoActor.Abs.IDomainEvent`(既有)、`PicoMediator.Abs.IEvent`(既有)、`PicoActor.Abs.ICommand`(既有)、`PicoActor.Abs.SagaExecution<TResult>`(既有,SagaContracts.cs)
- Produces:
  - `public interface IDomainEventSubscriber<in TEvent> where TEvent : IDomainEvent { ValueTask Handle(DomainEventEnvelope<TEvent> envelope, ICommandSender sender, CancellationToken ct = default); }`
  - `public sealed record DomainEventEnvelope<TEvent>(Guid ActorId, ulong Version, TEvent Event) where TEvent : IDomainEvent;`
  - `public sealed record DomainEventEnvelope(Guid ActorId, ulong Version, IDomainEvent Event) : IEvent;`
  - `public interface ICommandSender { void Send(Guid actorId, ICommand command); ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command); ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command); }`

- [ ] **Step 1: 写失败测试**

Create `tests/PicoActor.Abs.Tests/EventSubscriptionAbstractionTests.cs`:

```csharp
using PicoActor.Abs;
using PicoMediator.Abs;

namespace PicoActor.Abs.Tests;

internal sealed record SampleEvent(int Value) : IDomainEvent;

public sealed class EventSubscriptionAbstractionTests
{
    [Test]
    public async Task TypedEnvelope_CarriesActorContextAndEvent()
    {
        var id = Guid.CreateVersion7();
        var e = new SampleEvent(5);
        var envelope = new DomainEventEnvelope<SampleEvent>(id, 42, e);

        await Assert.That(envelope.ActorId).IsEqualTo(id);
        await Assert.That(envelope.Version).IsEqualTo(42);
        await Assert.That(ReferenceEquals(envelope.Event, e)).IsTrue();
    }

    [Test]
    public async Task TransportEnvelope_IsIEventButNotIDomainEvent()
    {
        var e = new SampleEvent(5);
        var envelope = new DomainEventEnvelope(Guid.CreateVersion7(), 1, e);

        await Assert.That(envelope).IsAssignableTo<IEvent>();
        await Assert.That(envelope is IDomainEvent).IsFalse();
        await Assert.That(ReferenceEquals(envelope.Event, e)).IsTrue();
    }

    [Test]
    public async Task Subscriber_HandlesTypedEnvelope()
    {
        var handler = new SampleSubscriber();
        var envelope = new DomainEventEnvelope<SampleEvent>(Guid.CreateVersion7(), 3, new SampleEvent(9));

        await handler.Handle(envelope, new NoopSender(), CancellationToken.None);

        await Assert.That(ReferenceEquals(handler.Last, envelope)).IsTrue();
        await Assert.That(handler.Last!.Event.Value).IsEqualTo(9);
    }

    private sealed class SampleSubscriber : IDomainEventSubscriber<SampleEvent>
    {
        public DomainEventEnvelope<SampleEvent>? Last { get; private set; }

        public ValueTask Handle(
            DomainEventEnvelope<SampleEvent> envelope,
            ICommandSender sender,
            CancellationToken ct = default
        )
        {
            Last = envelope;
            return default;
        }
    }

    private sealed class NoopSender : ICommandSender
    {
        public void Send(Guid actorId, ICommand command) { }
        public ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command) => default;
        public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command) => default;
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj`
Expected: 编译失败 —— `IDomainEventSubscriber` / `DomainEventEnvelope` / `ICommandSender` 不存在

- [ ] **Step 3: 实现类型**

Create `src/PicoActor.Abs/IDomainEventSubscriber.cs`:

```csharp
namespace PicoActor.Abs;

/// <summary>
/// Domain-event subscriber: receives the typed envelope (event + source aggregate
/// context) and translates it into commands sent to actors via <see cref="ICommandSender"/>.
/// Registered automatically by PicoActor.Gen (declare-and-subscribe) — no manual wiring.
/// </summary>
public interface IDomainEventSubscriber<in TEvent>
    where TEvent : IDomainEvent
{
    ValueTask Handle(
        DomainEventEnvelope<TEvent> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    );
}

/// <summary>Typed delivery envelope. <see cref="Event"/> is guaranteed to be <typeparamref name="TEvent"/>
/// (narrowed by the generated envelope bridge).</summary>
public sealed record DomainEventEnvelope<TEvent>(Guid ActorId, ulong Version, TEvent Event)
    where TEvent : IDomainEvent;

/// <summary>Transport envelope. Implements IEvent but NOT IDomainEvent — it never
/// enters the event store. Published per event by MediatorDomainEventPublisher;
/// the generated bridge narrows <see cref="Event"/> to its concrete type.</summary>
public sealed record DomainEventEnvelope(Guid ActorId, ulong Version, IDomainEvent Event)
    : IEvent;
```

Create `src/PicoActor.Abs/ICommandSender.cs`:

```csharp
namespace PicoActor.Abs;

/// <summary>
/// Narrow command-sending port for event handlers (mirrors IActorSystem signatures,
/// no CancellationToken — consistent with IActorSystem). Implemented by the framework
/// (ActorSystemCommandSender) and registered by AddPicoActor.
/// </summary>
public interface ICommandSender
{
    /// <summary>Fire-and-forget command delivery to an actor's mailbox.</summary>
    void Send(Guid actorId, ICommand command);

    /// <summary>Request-reply command; throws if the actor is not found.</summary>
    ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command);

    /// <summary>Run a saga: create + ask + auto-stop. Business failure throws
    /// <see cref="SagaExecutionException"/>.</summary>
    ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command);
}
```

- [ ] **Step 4: 运行验证通过**

Run: `dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj`
Expected: 3 tests PASS

- [ ] **Step 5: 提交**

```bash
git add src/PicoActor.Abs/IDomainEventSubscriber.cs src/PicoActor.Abs/ICommandSender.cs tests/PicoActor.Abs.Tests/EventSubscriptionAbstractionTests.cs
git commit -m "feat(abs): domain event subscriber abstraction with context envelope and ICommandSender"
```

---

### Task 2: MediatorDomainEventPublisher 发布信封

**Files:**
- Modify: `src/PicoActor/MediatorDomainEventPublisher.cs`(整体替换发布循环)
- Test: `tests/PicoActor.Tests/MediatorDomainEventPublisherTests.cs`(改两个断言)

**Interfaces:**
- Consumes: `PicoActor.Abs.DomainEventEnvelope`(Task 1)、`PicoActor.Abs.IDomainEventPublisher`(既有,契约不变)
- Produces: 发布对象从裸事件变为 `DomainEventEnvelope(actorId, version, event)`——后续所有订阅路径依赖此形状

- [ ] **Step 1: 写失败测试(改断言)**

在 `tests/PicoActor.Tests/MediatorDomainEventPublisherTests.cs` 中替换 `PublishAsync_PublishesEachEvent_WithActorContext` 与 `PublishAsync_OneEventFailure_DoesNotStopOthers` 两个测试的断言部分:

```csharp
    [Test]
    public async Task PublishAsync_PublishesEachEvent_WithActorContext()
    {
        var publisher = new RecordingMediatorPublisher();
        var sut = new MediatorDomainEventPublisher(publisher);

        var events =
            (IReadOnlyList<IDomainEvent>)
                new IDomainEvent[] { new PubEventA(1), new PubEventB("x") };
        await sut.PublishAsync(ActorId, 7, events);

        await Assert.That(publisher.Published.Count).IsEqualTo(2);
        await Assert.That(publisher.Published[0]).IsTypeOf<DomainEventEnvelope>();
        var envelope0 = (DomainEventEnvelope)publisher.Published[0];
        await Assert.That(envelope0.ActorId).IsEqualTo(ActorId);
        await Assert.That(envelope0.Version).IsEqualTo(7);
        await Assert.That(envelope0.Event).IsTypeOf<PubEventA>();
        var envelope1 = (DomainEventEnvelope)publisher.Published[1];
        await Assert.That(envelope1.Event).IsTypeOf<PubEventB>();
    }

    [Test]
    public async Task PublishAsync_OneEventFailure_DoesNotStopOthers()
    {
        var publisher = new RecordingMediatorPublisher { FailOnCall = 2 }; // the 2nd event fails
        var sut = new MediatorDomainEventPublisher(publisher);

        var events =
            (IReadOnlyList<IDomainEvent>)
                new IDomainEvent[] { new PubEventA(1), new PubEventB("x"), new PubEventA(2) };
        await sut.PublishAsync(ActorId, 7, events); // does not throw — per-event isolation

        await Assert.That(publisher.Published.Count).IsEqualTo(2); // events 1 and 3 arrive
        await Assert.That(((DomainEventEnvelope)publisher.Published[0]).Event).IsTypeOf<PubEventA>();
        await Assert.That(((DomainEventEnvelope)publisher.Published[1]).Event).IsTypeOf<PubEventA>();
    }
```

(其余三个测试:`PublishAsync_EmptyBatch_IsNoOp` 不变;两个 ODE 诊断测试不变——错误消息仍引用事件类型名 `PubEventA` 与 "root scope"。)
Expected: 2 个改动的测试 FAIL —— 发布的是裸事件,`IsTypeOf<DomainEventEnvelope>` 不成立

- [ ] **Step 2: 实现信封发布**

替换 `src/PicoActor/MediatorDomainEventPublisher.cs` 的 `PublishAsync` 方法(并更新类 XML 注释为信封语义;**删除** `#pragma warning disable PMGEN001` 块——发布类型现在是具体类型 `DomainEventEnvelope`,不再有基类型发布,无 PMGEN001):

```csharp
    public async ValueTask PublishAsync(
        Guid actorId,
        ulong version,
        IReadOnlyList<IDomainEvent> events
    )
    {
        foreach (var e in events)
        {
            try
            {
                // Concrete-type publish (DomainEventEnvelope) — direct key match,
                // no base-type bridge needed. The generated PicoActor bridge
                // narrows envelope.Event to its concrete type for subscribers.
                await _publisher
                    .Publish(new DomainEventEnvelope(actorId, version, e))
                    .ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                var message =
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): "
                    + $"{ex.Message}. The IMediator is bound to a disposed scope — resolve "
                    + "IActorSystem from the application-level root scope.";
                if (_logger is not null)
                    _logger.Error(message);
                else
                    Console.Error.WriteLine($"[PicoActor] {message}");
            }
            catch (Exception ex)
            {
                _logger?.Error(
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): {ex.Message}"
                );
            }
        }
    }
```

- [ ] **Step 3: 运行验证通过**

Run: `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: `MediatorDomainEventPublisherTests` 5 个测试全 PASS;注意 `MediatorIntegrationTests` 此刻**可以失败**(旧 `ISubscriber` 订阅者已收不到裸事件)——Task 6 重写,本任务不处理

- [ ] **Step 4: 提交**

```bash
git add src/PicoActor/MediatorDomainEventPublisher.cs tests/PicoActor.Tests/MediatorDomainEventPublisherTests.cs
git commit -m "feat: publish domain events as context envelopes via mediator"
```

---

### Task 3: ActorSystemCommandSender + DI 注册

**Files:**
- Create: `src/PicoActor/ActorSystemCommandSender.cs`
- Modify: `src/PicoActor/PicoActorDiExtensions.cs`(主路径 `AddPicoActor(IEventStore?)` 的 IActorSystem 注册之后追加 ICommandSender 注册)
- Test: `tests/PicoActor.Tests/CommandSenderTests.cs`

**Interfaces:**
- Consumes: `PicoActor.Abs.ICommandSender`(Task 1)、`PicoActor.Abs.IActorSystem`(既有)
- Produces: `internal sealed class ActorSystemCommandSender(IActorSystem) : ICommandSender`;DI 中 `ICommandSender` 以 Singleton 注册(工厂内解析 IActorSystem)

- [ ] **Step 1: 写失败测试**

Create `tests/PicoActor.Tests/CommandSenderTests.cs`:

```csharp
using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

internal sealed record Echo(string Msg) : ICommand;

internal sealed record Ping(string Msg) : ICommand;

internal sealed class EchoActor : Actor
{
    private string _last = "";

    public EchoActor(Echo cmd) : base(cmd) => _last = cmd.Msg;
    public EchoActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is Echo e)
        {
            _last = e.Msg;
            return new ValueTask<object?>(_last);
        }
        if (command is Ping p)
        {
            _last = p.Msg;
            return default;
        }
        return default;
    }
}

internal sealed record MiniSagaCmd : ICommand;

internal sealed record MiniSagaDone(string Name) : IDomainEvent;

internal sealed class MiniSaga : SagaActor
{
    public MiniSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is MiniSagaCmd)
        {
            RaiseEvent(new MiniSagaDone("mini"));
            MarkComplete("done");
            return new ValueTask<object?>("done");
        }
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class CommandSenderTests
{
    [Test]
    public async Task Send_DeliversToMailbox()
    {
        var system = NewSystem();
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        var sender = new ActorSystemCommandSender(system);
        sender.Send(actor.Id, new Ping("pong"));

        var last = await system.AskAsync<string>(actor.Id, new Echo("probe"));
        await Assert.That(last).IsEqualTo("pong");
    }

    [Test]
    public async Task AskAsync_ReturnsResult()
    {
        var system = NewSystem();
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        var sender = new ActorSystemCommandSender(system);
        var result = await sender.AskAsync<string>(actor.Id, new Echo("hi"));

        await Assert.That(result).IsEqualTo("hi");
    }

    [Test]
    public async Task ExecuteSaga_RunsSagaToCompletion()
    {
        var system = NewSystem();
        system.Register<MiniSaga>(_ => new MiniSaga(), () => new MiniSaga());

        var sender = new ActorSystemCommandSender(system);
        var execution = await sender.ExecuteSaga<MiniSaga, string>(new MiniSagaCmd());

        await Assert.That(execution.Result).IsEqualTo("done");
        await Assert.That(execution.Id).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task AddPicoActor_RegistersSender_ResolvableFromChildScope()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var childScope = container.CreateScope();

        var sender = (ICommandSender)childScope.GetService(typeof(ICommandSender));
        await Assert.That(sender).IsNotNull();

        var system = (IActorSystem)childScope.GetService(typeof(IActorSystem));
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        sender.Send(actor.Id, new Ping("via-di"));
        var last = await system.AskAsync<string>(actor.Id, new Echo("probe"));
        await Assert.That(last).IsEqualTo("via-di");
    }

    private static IActorSystem NewSystem() =>
        new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
}
```

- [ ] **Step 2: 运行验证失败**

Run: `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: 编译失败 —— `ActorSystemCommandSender` 不存在;`ICommandSender` 未注册(DI 测试运行时失败)

- [ ] **Step 3: 实现**

Create `src/PicoActor/ActorSystemCommandSender.cs`:

```csharp
using PicoActor.Abs;

namespace PicoActor;

/// <summary>IActorSystem adapter for the ICommandSender narrow port.</summary>
internal sealed class ActorSystemCommandSender : ICommandSender
{
    private readonly IActorSystem _system;

    public ActorSystemCommandSender(IActorSystem system)
    {
        ArgumentNullException.ThrowIfNull(system);
        _system = system;
    }

    public void Send(Guid actorId, ICommand command) => _system.Send(actorId, command);

    public ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command) =>
        _system.AskAsync<TResult>(actorId, command);

    public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command) =>
        _system.ExecuteSaga<TSaga, TResult>(command);
}
```

在 `src/PicoActor/PicoActorDiExtensions.cs` 的 `AddPicoActor(IEventStore?)` 主方法中,`IActorSystem` 的 `container.Register(...)` 调用之后追加:

```csharp
        // ICommandSender — narrow port for event handlers (Send/AskAsync/ExecuteSaga).
        // Resolves the singleton IActorSystem lazily so any registration order works.
        container.Register(
            typeof(ICommandSender),
            scope =>
                new ActorSystemCommandSender(
                    (IActorSystem)scope.GetService(typeof(IActorSystem))
                ),
            SvcLifetime.Singleton
        );
```

(`AddPicoActor(ActorConfig?)` 与 `AddPicoActor(IPublisher)` 两个重载都委托/复用主路径——`ActorConfig` 重载调用 store 重载,`IPublisher` 重载先调主路径再重注册 IActorSystem;ICommandSender 只注册一次,其工厂在解析时取最终注册的 IActorSystem,两个重载均自动获得 sender。)

- [ ] **Step 4: 运行验证通过**

Run: `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: `CommandSenderTests` 4 个测试 PASS

- [ ] **Step 5: 提交**

```bash
git add src/PicoActor/ActorSystemCommandSender.cs src/PicoActor/PicoActorDiExtensions.cs tests/PicoActor.Tests/CommandSenderTests.cs
git commit -m "feat: ICommandSender narrow port registered via AddPicoActor"
```

---

### Task 4: PicoActor.Gen 工程脚手架

**Files:**
- Create: `src/PicoActor.Gen/PicoActor.Gen.csproj`
- Create: `src/PicoActor.Gen/buildTransitive/PicoActor.Gen.props`
- Create: `src/PicoActor.Abs/buildTransitive/PicoActor.Abs.props`
- Modify: `src/PicoActor.Abs/PicoActor.Abs.csproj`(内嵌 analyzer + 打包)
- Modify: `Directory.Packages.props`(加 `Microsoft.CodeAnalysis.CSharp`)
- Modify: `PicoActor.slnx`(加项目)

**Interfaces:**
- Produces: analyzer 工程骨架(此时为空 generator 也能编译);PicoActor.Abs 对消费者的 analyzer 注入链路(ProjectReference `OutputItemType="Analyzer"` 本地开发 / `buildTransitive` props + `analyzers/dotnet/cs` 打包路径)

- [ ] **Step 1: 建工程文件**

Create `src/PicoActor.Gen/PicoActor.Gen.csproj`(镜像 PicoMediator.Gen.csproj):

```xml
<Project Sdk="Microsoft.NET.Sdk" TreatAsLocalProperty="PublishAot">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <PublishAot>false</PublishAot>
    <EnforceExtendedAnalyzerRules>true</EnforceExtendedAnalyzerRules>
    <IsRoslynComponent>true</IsRoslynComponent>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <NoWarn>$(NoWarn);RS2008;RS1041;NU5128</NoWarn>
    <IsPackable>false</IsPackable>
    <DevelopmentDependency>false</DevelopmentDependency>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <PackageId>PicoActor.Gen</PackageId>
    <Description>Source generator for PicoActor — declare-and-subscribe domain event handlers</Description>
    <PackageTags>PicoHex;Actor;AOT;Source Generator;Roslyn;Analyzer</PackageTags>
    <PackageReadmeFile>README.md</PackageReadmeFile>
  </PropertyGroup>

  <PropertyGroup>
    <GenerateDependencyFile>false</GenerateDependencyFile>
    <CopyLocalLockFileAssemblies>false</CopyLocalLockFileAssemblies>
  </PropertyGroup>

  <PropertyGroup>
    <MSBuildWarningsAsMessages>MSB3026</MSBuildWarningsAsMessages>
  </PropertyGroup>

  <ItemGroup>
    <None Include="..\..\README.md" Pack="true" PackagePath="" Visible="false" />
  </ItemGroup>

  <ItemGroup>
    <None
      Include="$(OutputPath)/$(AssemblyName).dll"
      Pack="true"
      PackagePath="buildTransitive/PicoActor.Gen/analyzers/dotnet/cs"
      Visible="false"
    />
    <None
      Include="buildTransitive\PicoActor.Gen.props"
      Pack="true"
      PackagePath="buildTransitive/PicoActor.Gen.props"
      Visible="false"
    />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" PrivateAssets="all" />
  </ItemGroup>
</Project>
```

Create `src/PicoActor.Gen/buildTransitive/PicoActor.Gen.props`:

```xml
<Project>
  <ItemGroup>
    <Analyzer Include="$(MSBuildThisFileDirectory)PicoActor.Gen\analyzers\dotnet\cs\PicoActor.Gen.dll" />
  </ItemGroup>
</Project>
```

Create `src/PicoActor.Abs/buildTransitive/PicoActor.Abs.props`:

```xml
<Project>
  <ItemGroup>
    <Analyzer Include="$(MSBuildThisFileDirectory)../analyzers/dotnet/cs/PicoActor.Gen.dll" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: 接线 Abs 工程**

`src/PicoActor.Abs/PicoActor.Abs.csproj` 的 `</Project>` 前追加(镜像 PicoMediator.Abs.csproj:21-38):

```xml
  <ItemGroup>
    <ProjectReference
      Include="..\PicoActor.Gen\PicoActor.Gen.csproj"
      OutputItemType="Analyzer"
      ReferenceOutputAssembly="false"
      PrivateAssets="all"
    />
  </ItemGroup>

  <ItemGroup>
    <None
      Include="..\PicoActor.Gen\$(OutputPath)\PicoActor.Gen.dll"
      Pack="true"
      PackagePath="analyzers/dotnet/cs"
      Visible="false"
    />
    <None
      Include="buildTransitive\PicoActor.Abs.props"
      Pack="true"
      PackagePath="buildTransitive\PicoActor.Abs.props"
      Visible="false"
    />
  </ItemGroup>
```

`Directory.Packages.props` 的 `<ItemGroup>` 内追加:

```xml
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp" Version="5.6.0" />
```

`PicoActor.slnx` 的 `/src/` Folder 内追加:

```xml
    <Project Path="src/PicoActor.Gen/PicoActor.Gen.csproj" />
```

- [ ] **Step 3: 放一个空的 generator 占位(保证编译通过)**

Create `src/PicoActor.Gen/PlaceholderGenerator.cs`(Task 5 删除):

```csharp
using Microsoft.CodeAnalysis;

namespace PicoActor.Gen;

[Generator(LanguageNames.CSharp)]
public sealed class PlaceholderGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context) { }
}
```

- [ ] **Step 4: 构建验证**

Run: `dotnet build PicoActor.slnx`
Expected: BUILD SUCCEEDED(analyzer 已流式注入 PicoActor.Abs → PicoActor → 各测试工程;占位 generator 无输出,各工程照常编译)

- [ ] **Step 5: 提交**

```bash
git add src/PicoActor.Gen src/PicoActor.Abs/buildTransitive src/PicoActor.Abs/PicoActor.Abs.csproj Directory.Packages.props PicoActor.slnx
git commit -m "build(gen): scaffold PicoActor.Gen analyzer embedded in PicoActor.Abs"
```

---

### Task 5: ActorSubscriberGenerator 实现 + 输出测试

**Files:**
- Delete: `src/PicoActor.Gen/PlaceholderGenerator.cs`
- Create: `src/PicoActor.Gen/ActorSubscriberGenerator.cs`
- Modify: `tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj`(加 `Microsoft.CodeAnalysis.CSharp`)
- Create: `tests/PicoActor.Abs.Tests/ActorSubscriberGeneratorOutputTests.cs`

**Interfaces:**
- Consumes: `PicoMediator.MediatorAutoSubscriptionRegistry.Register(string, Action<ISvcContainer>)`、`PicoDI.Abs.SvcDescriptor.Create(Type, Func<ISvcScope,object?>, SvcLifetime)`、`PicoDI.Abs.ISvcScope.TryGetServices(Type, out object?)`
- Produces: `PicoActor.Gen.ActorSubscriberGenerator : IIncrementalGenerator`——扫描 `PicoActor.Abs.IDomainEventSubscriber`1` 闭合实现,生成 `PicoActorSubscriberRegistrations.<Assembly>.g.cs`(每程序集一个 configurator,id = `pico-actor::<assemblyName>`)

**生成代码语义(重要,与 PicoMediator.Gen 的两处有意差异):**
1. **handler 注册无 `IsRegistered` 去重**(append 语义)——否则两个类实现同一 `IDomainEventSubscriber<TEvent>` 时第二个被跳过,违反"多订阅者同事件"契约(spec §7.3)
2. **bridge 按"事件类型"去重生成,不按 handler 类**——每事件类型恰好一个 `PicoActorEnvelopeBridge_<TEvent>`(注册键 `ISubscriber<DomainEventEnvelope>`,append);bridge 内 `TryGetServices` 取该事件类型**全部** handler 并逐个调用,逐 handler try/catch 收集 `AggregateException`(镜像 `Mediator.Publish` 的隔离语义)

- [ ] **Step 1: 加测试工程引用**

`tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj` 的 ItemGroup 内追加:

```xml
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" PrivateAssets="all" />
```

- [ ] **Step 2: 写失败测试(真实生成器输出)**

Create `tests/PicoActor.Abs.Tests/ActorSubscriberGeneratorOutputTests.cs`:

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;
using PicoDI.Abs;
using PicoMediator.Abs;

namespace PicoActor.Abs.Tests;

// Verifies the REAL generator output — not mirrored strings. Guards the
// multi-subscriber invariant: one envelope bridge per DISTINCT event type
// (not per handler class), so two handlers for the same event both receive it.
public sealed class ActorSubscriberGeneratorOutputTests
{
    private const string InputSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;

        public record Paid(int Id) : IDomainEvent;

        public sealed class PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public sealed class PaidAuditHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public record Shipped(Guid Id) : IDomainEvent;

        public sealed class ShippedHandler : IDomainEventSubscriber<Shipped>
        {
            public ValueTask Handle(DomainEventEnvelope<Shipped> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        // A PicoMediator subscriber must NOT be picked up — namespace-locked scan.
        public sealed class PaidMediatorSub : ISubscriber<Paid>
        {
            public ValueTask Handle(Paid e, CancellationToken ct) => ValueTask.CompletedTask;
        }
        """;

    private static GeneratorDriver RunGenerator(string assemblyName)
    {
        var inputTree = CSharpSyntaxTree.ParseText(
            InputSource,
            CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview)
        );
        var compilation = CSharpCompilation.Create(
            assemblyName: assemblyName,
            syntaxTrees: [inputTree],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        var generator = new ActorSubscriberGenerator();
        return CSharpGeneratorDriver
            .Create([generator.AsSourceGenerator()])
            .RunGeneratorsAndUpdateCompilation(compilation, out _, out _);
    }

    private static MetadataReference[] GetMetadataReferences()
    {
        var trustedPlatformAssemblies = (
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
        )!.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        var refs = trustedPlatformAssemblies
            .Select(static p => MetadataReference.CreateFromFile(p))
            .ToList();
        refs.Add(MetadataReference.CreateFromFile(typeof(IDomainEvent).Assembly.Location));
        refs.Add(MetadataReference.CreateFromFile(typeof(IEvent).Assembly.Location));
        refs.Add(MetadataReference.CreateFromFile(typeof(ISvcScope).Assembly.Location));
        return [.. refs];
    }

    private static string? FindGeneratedSource(GeneratorDriver driver, string fileNamePart)
    {
        var runResult = driver.GetRunResult();
        foreach (var result in runResult.Results)
        foreach (var source in result.GeneratedSources)
        if (source.HintName.Contains(fileNamePart, StringComparison.Ordinal))
            return source.SourceText.ToString();
        return null;
    }

    private static int CountOccurrences(string source, string marker) =>
        source.Split(marker, StringSplitOptions.None).Length - 1;

    [Test]
    public async Task ConfiguratorId_IsStablePerAssembly()
    {
        foreach (var asm in new[] { "myapp", "PicoActor.Tests", "Zapp" })
        {
            var driver = RunGenerator(asm);
            var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations");
            await Assert.That(source).IsNotNull();
            await Assert.That(source!).Contains($"pico-actor::{asm}");
        }
    }

    [Test]
    public async Task OneBridgePerEventType_NotPerHandlerClass()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        // 3 handler classes (Paid ×2, Shipped ×1) → 3 handler registrations
        await Assert.That(CountOccurrences(source, "IDomainEventSubscriber<global::Paid>")).IsGreaterThanOrEqualTo(3);

        // exactly 2 bridge classes (Paid, Shipped) and 2 bridge registrations
        await Assert.That(CountOccurrences(source, "internal sealed class PicoActorEnvelopeBridge_")).IsEqualTo(2);
        await Assert.That(CountOccurrences(source, "static scope => new PicoActorEnvelopeBridge_")).IsEqualTo(2);
    }

    [Test]
    public async Task Bridge_NarrowsAndDeliversTypedEnvelope()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        await Assert.That(source).Contains("if (envelope.Event is global::Paid e)");
        await Assert.That(source)
            .Contains("_scope.TryGetServices(typeof(global::PicoActor.Abs.IDomainEventSubscriber<global::Paid>), out var rawHandlers)");
        await Assert.That(source)
            .Contains("new global::PicoActor.Abs.DomainEventEnvelope<global::Paid>(envelope.ActorId, envelope.Version, e)");
        await Assert.That(source).Contains("exceptions ??= []");
        await Assert.That(source).Contains("throw new global::System.AggregateException(exceptions)");
    }

    [Test]
    public async Task Subscriber_InNamespaceLockedToPicoActorAbs()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        // the generator must only react to PicoActor.Abs.IDomainEventSubscriber`1 —
        // a PicoMediator ISubscriber in the input must NOT generate anything extra
        await Assert.That(source).DoesNotContain("ISubscriber<global::Paid>");
    }
}
```

Note: 此测试引用 `PicoActor.Gen`(using PicoActor.Gen)——Abs.Tests 需能引用 analyzer 工程。给 `tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj` 追加(镜像 PicoMediator.Tests.csproj:20-22 的确认模式——`OutputItemType="Analyzer"` 单独使用,默认 `ReferenceOutputAssembly=true`,既注入 analyzer 又提供编译引用):

```xml
    <ProjectReference Include="..\..\src\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" />
```

- [ ] **Step 3: 运行验证失败**

Run: `dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj`
Expected: 编译失败 —— `ActorSubscriberGenerator` 不存在(PlaceholderGenerator 仍为唯一 generator)

- [ ] **Step 4: 实现生成器**

Delete `src/PicoActor.Gen/PlaceholderGenerator.cs`,Create `src/PicoActor.Gen/ActorSubscriberGenerator.cs`:

```csharp
using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace PicoActor.Gen;

/// <summary>
/// Declare-and-subscribe for PicoActor: scans closed, non-abstract
/// IDomainEventSubscriber&lt;TEvent&gt; implementations and emits a per-assembly
/// configurator (id "pico-actor::&lt;assembly&gt;") into MediatorAutoSubscriptionRegistry.
/// The configurator registers each handler (append semantics — multiple handlers
/// per event type are supported) and one envelope bridge per distinct event type.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class ActorSubscriberGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var subscriberDeclarations = context
            .SyntaxProvider.CreateSyntaxProvider(
                predicate: static (node, _) =>
                    node is ClassDeclarationSyntax c && c.BaseList?.Types.Count > 0,
                transform: static (ctx, ct) => GetSubscriberInfos(ctx, ct)
            )
            .Where(static x => x.Length > 0)
            .SelectMany(static (x, _) => x);

        context.RegisterSourceOutput(
            context.CompilationProvider.Combine(subscriberDeclarations.Collect()),
            static (spc, pair) => GenerateRegistrations(spc, pair.Left, pair.Right)
        );
    }

    private sealed record SubscriberInfo(
        string EventTypeFqn,
        string ImplementationType,
        ImmutableArray<string> ConstructorParameterTypes
    );

    private static ImmutableArray<SubscriberInfo> GetSubscriberInfos(
        GeneratorSyntaxContext ctx,
        CancellationToken ct
    )
    {
        if (ctx.Node is not ClassDeclarationSyntax classDecl)
            return [];

        var typeSymbol = ctx.SemanticModel.GetDeclaredSymbol(classDecl, ct) as INamedTypeSymbol;
        var accessibility = typeSymbol?.DeclaredAccessibility;
        if (
            typeSymbol is null
            || typeSymbol.IsAbstract
            || typeSymbol.IsGenericType
            || (accessibility != Accessibility.Public && accessibility != Accessibility.Internal)
        )
            return [];

        var results = ImmutableArray.CreateBuilder<SubscriberInfo>();
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            if (!iface.IsGenericType)
                continue;

            var constructed = iface.ConstructedFrom;
            if (constructed.ContainingNamespace.ToDisplayString() != "PicoActor.Abs")
                continue;
            if (constructed.MetadataName != "IDomainEventSubscriber`1")
                continue;

            var ctor = typeSymbol.InstanceConstructors.FirstOrDefault(c =>
                c.DeclaredAccessibility == Accessibility.Public && !c.IsStatic
            );
            var ctorParams = ctor is null
                ? []
                : ctor
                    .Parameters.Select(p =>
                        p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                    )
                    .ToImmutableArray();

            results.Add(
                new SubscriberInfo(
                    iface.TypeArguments[0].ToDisplayString(
                        SymbolDisplayFormat.FullyQualifiedFormat
                    ),
                    typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    ctorParams
                )
            );
        }

        return results.ToImmutable();
    }

    private static void GenerateRegistrations(
        SourceProductionContext context,
        Compilation compilation,
        ImmutableArray<SubscriberInfo> subscribers
    )
    {
        if (subscribers.IsDefaultOrEmpty)
            return;

        var assemblyName = compilation.AssemblyName ?? "Unknown";
        var safeAssemblyName = SanitizeIdentifier(assemblyName);
        var className = $"PicoActorSubscriberRegistrations_{safeAssemblyName}";
        var configuratorId = $"pico-actor::{assemblyName}";

        var distinctEventTypes = subscribers
            .Select(static s => s.EventTypeFqn)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static x => x, StringComparer.Ordinal)
            .ToArray();

        var sb = new StringBuilder(8192);
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine("using System.Runtime.CompilerServices;");
        sb.AppendLine();
        sb.AppendLine("namespace PicoActor.Generated;");
        sb.AppendLine();

        foreach (var eventType in distinctEventTypes)
        {
            var bridgeName = "PicoActorEnvelopeBridge_" + SanitizeIdentifier(eventType);
            sb.AppendLine(
                $"internal sealed class {bridgeName} : global::PicoMediator.Abs.ISubscriber<global::PicoActor.Abs.DomainEventEnvelope>"
            );
            sb.AppendLine("{");
            sb.AppendLine("    private readonly global::PicoDI.Abs.ISvcScope _scope;");
            sb.AppendLine();
            sb.AppendLine(
                $"    public {bridgeName}(global::PicoDI.Abs.ISvcScope scope) => _scope = scope;"
            );
            sb.AppendLine();
            sb.AppendLine(
                "    public async ValueTask Handle(global::PicoActor.Abs.DomainEventEnvelope envelope, global::System.Threading.CancellationToken ct)"
            );
            sb.AppendLine("    {");
            sb.AppendLine($"        if (envelope.Event is {eventType} e)");
            sb.AppendLine("        {");
            sb.AppendLine(
                $"            if (!_scope.TryGetServices(typeof(global::PicoActor.Abs.IDomainEventSubscriber<{eventType}>), out var rawHandlers))"
            );
            sb.AppendLine("                return;");
            sb.AppendLine(
                "            var sender = (global::PicoActor.Abs.ICommandSender)_scope.GetService(typeof(global::PicoActor.Abs.ICommandSender));"
            );
            sb.AppendLine(
                $"            var typed = new global::PicoActor.Abs.DomainEventEnvelope<{eventType}>(envelope.ActorId, envelope.Version, e);"
            );
            sb.AppendLine(
                "            global::System.Collections.Generic.List<global::System.Exception>? exceptions = null;"
            );
            sb.AppendLine("            foreach (var raw in rawHandlers)");
            sb.AppendLine("            {");
            sb.AppendLine("                try");
            sb.AppendLine("                {");
            sb.AppendLine(
                $"                    await ((global::PicoActor.Abs.IDomainEventSubscriber<{eventType}>)raw).Handle(typed, sender, ct).ConfigureAwait(false);"
            );
            sb.AppendLine("                }");
            sb.AppendLine("                catch (global::System.Exception ex)");
            sb.AppendLine("                {");
            sb.AppendLine("                    exceptions ??= [];");
            sb.AppendLine("                    exceptions.Add(ex);");
            sb.AppendLine("                }");
            sb.AppendLine("            }");
            sb.AppendLine("            if (exceptions is { Count: > 0 })");
            sb.AppendLine(
                "                throw new global::System.AggregateException(exceptions);"
            );
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");
            sb.AppendLine();
        }

        sb.AppendLine($"public static class {className}");
        sb.AppendLine("{");
        sb.AppendLine("    [ModuleInitializer]");
        sb.AppendLine("    internal static void AutoRegister()");
        sb.AppendLine("    {");
        sb.AppendLine(
            "        global::PicoMediator.MediatorAutoSubscriptionRegistry.Register("
        );
        sb.AppendLine(
            $"            \"{EscapeStringLiteral(configuratorId)}\", static container => ConfigureGeneratedHandlers(container));"
        );
        sb.AppendLine("    }");
        sb.AppendLine();
        sb.AppendLine(
            "    internal static void ConfigureGeneratedHandlers(global::PicoDI.Abs.ISvcContainer container)"
        );
        sb.AppendLine("    {");
        foreach (var s in subscribers)
        {
            sb.AppendLine("        container.Register(global::PicoDI.Abs.SvcDescriptor.Create(");
            sb.AppendLine(
                $"            typeof(global::PicoActor.Abs.IDomainEventSubscriber<{s.EventTypeFqn}>),"
            );
            sb.AppendLine($"            {EmitFactory(s)},");
            sb.AppendLine("            global::PicoDI.Abs.SvcLifetime.Transient));");
            sb.AppendLine();
        }
        foreach (var eventType in distinctEventTypes)
        {
            var bridgeName = "PicoActorEnvelopeBridge_" + SanitizeIdentifier(eventType);
            sb.AppendLine("        container.Register(global::PicoDI.Abs.SvcDescriptor.Create(");
            sb.AppendLine(
                "            typeof(global::PicoMediator.Abs.ISubscriber<global::PicoActor.Abs.DomainEventEnvelope>),"
            );
            sb.AppendLine($"            static scope => new {bridgeName}(scope),");
            sb.AppendLine("            global::PicoDI.Abs.SvcLifetime.Transient));");
            sb.AppendLine();
        }
        sb.AppendLine("    }");
        sb.AppendLine("}");
        context.AddSource(
            $"PicoActorSubscriberRegistrations.{safeAssemblyName}.g.cs",
            sb.ToString()
        );
    }

    private static string EmitFactory(SubscriberInfo s)
    {
        if (s.ConstructorParameterTypes.IsEmpty)
            return $"static _ => new {s.ImplementationType}()";

        var args = string.Join(
            ", ",
            s.ConstructorParameterTypes.Select(p => $"({p})scope.GetService(typeof({p}))")
        );
        return $"static scope => new {s.ImplementationType}({args})";
    }

    private static string SanitizeIdentifier(string value) =>
        new(
            value
                .Select(c => c == '.' ? '_' : c)
                .Where(static c => char.IsLetterOrDigit(c) || c == '_')
                .ToArray()
        );

    private static string EscapeStringLiteral(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
```

- [ ] **Step 5: 运行验证通过**

Run: `dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj`
Expected: `ActorSubscriberGeneratorOutputTests` 4 个测试 PASS(生成源码断言逐条命中);既有 Abs.Tests 测试仍 PASS

- [ ] **Step 6: 提交**

```bash
git add src/PicoActor.Gen/ActorSubscriberGenerator.cs tests/PicoActor.Abs.Tests tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj
git rm src/PicoActor.Gen/PlaceholderGenerator.cs
git commit -m "feat(gen): declare-and-subscribe source generator for domain event handlers"
```

---

### Task 6: 集成测试重写(信封断言 + 事件→命令翻译闭环)

**Files:**
- Rewrite: `tests/PicoActor.Tests/MediatorIntegrationTests.cs`

**Interfaces:**
- Consumes: `IDomainEventSubscriber<TEvent>` / `DomainEventEnvelope<TEvent>` / `ICommandSender`(Task 1)、信封发布( Task 2)、`AddPicoActor` 的 sender 注册(Task 3)、PicoActor.Gen 在测试程序集上的自动注册(Task 4+5)、`SagaExecutionException.SagaId`(既有)、`FailSaga`/`FailCmd`(既有,ExecuteSagaApiTests.cs)
- Produces: 端到端契约验证——订阅者收类型化信封(actorId=源聚合 id、version=批后版本)、多订阅者同事件、逐 handler 隔离、无订阅者静默、`ICommandSender.Send/ExecuteSaga` 翻译闭环、replay 静默

- [ ] **Step 1: 重写测试文件(完整替换)**

Replace `tests/PicoActor.Tests/MediatorIntegrationTests.cs` 全部内容为:(原 `EventOutflow_BasePublishBridge_ReachesTypedSubscribers` 测试**删除**——它验证的是基类型发布 bridge 机制,已被具体类型信封直连路由取代,新测试 1-3 覆盖等价语义)

```csharp
using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

/// <summary>Business-event subscriber (declare-and-subscribe via PicoActor.Gen) —
/// receives the typed envelope with the source aggregate context.</summary>
internal sealed class MedIntegrationStartedSub : IDomainEventSubscriber<MedIntegrationStarted>
{
    public static readonly List<DomainEventEnvelope<MedIntegrationStarted>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<MedIntegrationStarted> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

internal sealed class SagaCompletedSub : IDomainEventSubscriber<SagaCompleted>
{
    public static readonly List<DomainEventEnvelope<SagaCompleted>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<SagaCompleted> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

/// <summary>Framework failure-event subscriber — SagaFailed typed subscription.</summary>
public sealed class SagaFailedSub : IDomainEventSubscriber<SagaFailed>
{
    public static readonly List<DomainEventEnvelope<SagaFailed>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<SagaFailed> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

/// <summary>Event→command translation: Send to a target actor.</summary>
internal sealed class TriggerSendHandler : IDomainEventSubscriber<MedIntegrationTrigger>
{
    public static Guid TargetActorId { get; set; }
    public static readonly List<Guid> Sent = [];

    public ValueTask Handle(
        DomainEventEnvelope<MedIntegrationTrigger> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        sender.Send(TargetActorId, new TargetCmd("ping"));
        Sent.Add(envelope.ActorId);
        return default;
    }
}

/// <summary>Event→command translation: ExecuteSaga.</summary>
internal sealed class TriggerSagaHandler : IDomainEventSubscriber<MedIntegrationTrigger>
{
    public static readonly List<SagaExecution<string>> Results = [];

    public async ValueTask Handle(
        DomainEventEnvelope<MedIntegrationTrigger> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Results.Add(
            await sender.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("from-trigger"))
        );
    }
}

internal sealed record MedIntegrationCmd(string Name) : ICommand;

internal sealed record MedIntegrationStarted(string Name) : IDomainEvent;

internal sealed record MedIntegrationTrigger(Guid Id) : IDomainEvent;

internal sealed record MedIntegrationUnobserved(string Name) : IDomainEvent;

internal sealed record TriggerCmd : ICommand;

internal sealed record TargetCmd(string Payload) : ICommand;

internal sealed record UnobservedCmd(string Name) : ICommand;

/// <summary>Raises MedIntegrationTrigger — the event handlers translate to commands.</summary>
internal sealed class MedIntegrationTriggerActor : EventSourcedActor
{
    public MedIntegrationTriggerActor(TriggerCmd cmd) : base(cmd) { }
    public MedIntegrationTriggerActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is TriggerCmd)
            RaiseEvent(new MedIntegrationTrigger(Id));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>Target actor for the event→command Send translation.</summary>
internal sealed class MedIntegrationTargetActor : Actor
{
    public static readonly List<string> ReceivedPayloads = [];

    public MedIntegrationTargetActor(TargetCmd cmd) : base(cmd)
    {
        ReceivedPayloads.Add(cmd.Payload);
    }

    public MedIntegrationTargetActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is TargetCmd c)
        {
            ReceivedPayloads.Add(c.Payload);
            return new ValueTask<object?>(c.Payload);
        }
        return default;
    }
}

/// <summary>Raises an event nobody subscribes to — must be silently dropped.</summary>
internal sealed class MedIntegrationUnobservedActor : EventSourcedActor
{
    public MedIntegrationUnobservedActor(UnobservedCmd cmd) : base(cmd) { }
    public MedIntegrationUnobservedActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is UnobservedCmd c)
            RaiseEvent(new MedIntegrationUnobserved(c.Name));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

internal sealed class MedIntegrationSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public MedIntegrationSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is MedIntegrationCmd c)
        {
            if (_step < 1)
            {
                RaiseEvent(new MedIntegrationStarted(c.Name));
                _name = c.Name;
            }
            MarkComplete(_name); // Unconditional: resumes converge to terminal even when all steps are done
            return new ValueTask<object?>(_name);
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        // Re-dispatch the driving command; step guards skip completed steps and
        // MarkComplete converges the saga to the Completed terminal state.
        await OnMessageAsync(new MedIntegrationCmd(_name));
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is MedIntegrationStarted e)
        {
            _step = 1;
            _name = e.Name;
        }
    }
}

/// <summary>Shared static subscriber lists — tests in this class must run serially.</summary>
[NotInParallel]
public sealed partial class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // Saga completes → events flow out as envelopes → typed subscribers receive
        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("hello")
        );
        await Task.Delay(300); // publish runs inside ProcessAsync's flush; delay is defensive

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        var started = MedIntegrationStartedSub.Received[0];
        await Assert.That(started.ActorId).IsEqualTo(execution.Id); // source aggregate context
        await Assert.That(started.Version).IsEqualTo(2); // batch [Started, SagaCompleted] — version after batch
        await Assert.That(started.Event.Name).IsEqualTo("hello");

        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].ActorId).IsEqualTo(execution.Id);
        await Assert.That(SagaCompletedSub.Received[0].Version).IsEqualTo(2);
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("hello");
    }

    [Test]
    public async Task AutoWiring_RootScopeBinding_SurvivesChildScopeDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // Recommended usage: ActorSystem first resolved from an application-level (root) scope
        await using var rootScope = container.CreateScope();
        var system = (IActorSystem)rootScope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // Short-lived child scopes do not affect root-bound outflow
        await using (var childScope = container.CreateScope()) { }

        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("root")
        );
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].ActorId).IsEqualTo(execution.Id);
        await Assert.That(MedIntegrationStartedSub.Received[0].Event.Name).IsEqualTo("root");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task AutoWiring_ChildScopeFirstResolution_SurvivesDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // E1 (PicoDI 2026.8.1): singleton factories use the container-internal root scope —
        // first resolution from a child scope is safe (pre-E1: outflow died after scope disposal)
        IActorSystem system;
        Guid sagaId;
        await using (var childScope = container.CreateScope())
        {
            system = (IActorSystem)childScope.GetService(typeof(IActorSystem));
            system.Register<MedIntegrationSaga>(
                _ => new MedIntegrationSaga(),
                () => new MedIntegrationSaga()
            );
            var saga = await system.CreateAsync<MedIntegrationSaga>(new MedIntegrationCmd("init"));
            sagaId = saga.Id;
        }

        // Child scope disposed: mediator bound to the root scope — outflow unaffected.
        // (creation command ignored by the parameterless factory; AskAsync("x") produces one event)
        await system.AskAsync<string>(sagaId, new MedIntegrationCmd("x"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].ActorId).IsEqualTo(sagaId);
        await Assert.That(MedIntegrationStartedSub.Received[0].Version).IsEqualTo(2);
        await Assert.That(MedIntegrationStartedSub.Received[0].Event.Name).IsEqualTo("x");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Recovery_CompletedEvents_FlowToSubscribers()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        // Interrupted saga: step 1 persisted, no terminal event. The explicit store is
        // passed to AddPicoActor so the ActorSystem sees the same stream (AddPicoActor()
        // without an argument would create a fresh InMemoryEventStore).
        var store = new InMemoryEventStore();
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new MedIntegrationStarted("recover")]);

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor(store);
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        var results = await system.ResumeInterruptedSagasAsync<MedIntegrationSaga>(
            nameof(MedIntegrationStarted)
        );
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);

        // Events produced by the resume flow out through the mediator:
        // replay is silent (MedIntegrationStarted not republished), the resume-advanced
        // SagaCompleted is published as an envelope
        await Task.Delay(300);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].ActorId).IsEqualTo(sagaId);
        await Assert.That(SagaCompletedSub.Received[0].Version).IsEqualTo(2); // appended after v1 stream
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("recover");
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(0);
    }

    [Test]
    public async Task SagaFailed_FlowsToSubscribers()
    {
        SagaFailedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<FailSaga>(_ => new FailSaga(), () => new FailSaga());

        SagaExecutionException? caught = null;
        try
        {
            await system.ExecuteSaga<FailSaga, string>(new FailCmd());
        }
        catch (SagaExecutionException ex)
        {
            caught = ex;
        }
        await Assert.That(caught).IsNotNull();

        // Failure = terminal state: SagaFailed event flows out to the typed subscriber
        await Task.Delay(300);
        await Assert.That(SagaFailedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaFailedSub.Received[0].ActorId).IsEqualTo(caught!.SagaId);
        await Assert.That(SagaFailedSub.Received[0].Version).IsEqualTo(1);
        await Assert.That(SagaFailedSub.Received[0].Event.Reason).Contains("step failed");
    }

    [Test]
    public async Task Handler_EventToCommand_SendReachesTargetActor()
    {
        TriggerSendHandler.Sent.Clear();
        MedIntegrationTargetActor.ReceivedPayloads.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationTriggerActor>(
            _ => new MedIntegrationTriggerActor(),
            () => new MedIntegrationTriggerActor()
        );
        system.Register<MedIntegrationTargetActor>(
            _ => new MedIntegrationTargetActor(),
            () => new MedIntegrationTargetActor()
        );

        var target = await system.CreateAsync<MedIntegrationTargetActor>(new TargetCmd("init"));
        TriggerSendHandler.TargetActorId = target.Id;
        MedIntegrationTargetActor.ReceivedPayloads.Clear(); // discard creation entries (base ctor dispatches OnMessageAsync, derived ctor adds again)
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        ); // keep the sibling TriggerSagaHandler clean (it also fires for MedIntegrationTrigger)

        // creation command is processed → MedIntegrationTrigger fires → handler Sends
        var trigger = await system.CreateAsync<MedIntegrationTriggerActor>(new TriggerCmd());
        await system.AskAsync<object?>(trigger.Id, new TriggerCmd()); // second trigger
        await Task.Delay(300);

        await Assert.That(TriggerSendHandler.Sent.Count).IsEqualTo(2); // two events, envelope.ActorId = source
        await Assert.That(TriggerSendHandler.Sent[0]).IsEqualTo(trigger.Id);
        await Assert.That(MedIntegrationTargetActor.ReceivedPayloads.Count).IsEqualTo(2); // two "ping" deliveries only
        await Assert.That(MedIntegrationTargetActor.ReceivedPayloads).Contains("ping");
    }

    [Test]
    public async Task Handler_EventToCommand_ExecuteSagaRunsSaga()
    {
        TriggerSagaHandler.Results.Clear();
        SagaCompletedSub.Received.Clear();
        MedIntegrationStartedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationTriggerActor>(
            _ => new MedIntegrationTriggerActor(),
            () => new MedIntegrationTriggerActor()
        );
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // creation command is processed → MedIntegrationTrigger fires → handler ExecuteSaga
        await system.CreateAsync<MedIntegrationTriggerActor>(new TriggerCmd());
        await Task.Delay(300);

        await Assert.That(TriggerSagaHandler.Results.Count).IsEqualTo(1);
        await Assert.That(TriggerSagaHandler.Results[0].Result).IsEqualTo("from-trigger");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("from-trigger");
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task UnobservedEvent_SilentlyDropped()
    {
        MedIntegrationStartedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationUnobservedActor>(
            _ => new MedIntegrationUnobservedActor(),
            () => new MedIntegrationUnobservedActor()
        );

        var actor = await system.CreateAsync<MedIntegrationUnobservedActor>(
            new UnobservedCmd("ghost")
        );
        await system.AskAsync<object?>(actor.Id, new UnobservedCmd("ghost2"));
        await Task.Delay(300);

        // no subscribers for MedIntegrationUnobserved — everything completes without error
        await Assert.That(actor.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(0); // no leakage
    }
}
```

- [ ] **Step 2: 运行验证**

Run: `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: `MediatorIntegrationTests` 8 个测试全 PASS(生成器在测试程序集自动注册了全部订阅者;`ActorSystemPublisherTests` 等其他文件若引用旧订阅形状则在 Task 6 内一并修复——先跑,若有失败按失败内容迁移为信封断言)

- [ ] **Step 3: 全量回归**

Run: `dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj && dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: 全部 PASS

- [ ] **Step 4: 提交**

```bash
git add tests/PicoActor.Tests/MediatorIntegrationTests.cs
git commit -m "test: envelope-based subscriber integration tests with event-to-command translation"
```

---

### Task 7: README 订阅章节更新

**Files:**
- Modify: `README.md`(订阅章节,约 400 行附近)
- Modify: `README.zh.md`(对应章节)

**Interfaces:**
- Consumes: Task 1-5 的最终 API(`IDomainEventSubscriber<TEvent>`、`DomainEventEnvelope<TEvent>`、`ICommandSender`、零配置注册)

- [ ] **Step 1: 更新 README.md 订阅示例**

定位 README.md 中 `OrderPaidSub : ISubscriber<OrderPaid>` 示例(约 400 行)及其后的 "Base-declared subscribers" 说明,替换为:

```markdown
### Subscribing to Domain Events (declare-and-subscribe)

Event handlers are plain classes implementing `IDomainEventSubscriber<TEvent>`
— PicoActor.Gen (embedded in PicoActor.Abs) scans and auto-registers them;
no manual wiring. The handler receives a typed envelope carrying the source
aggregate context (`ActorId`, `Version`) plus a narrow `ICommandSender` port:

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

Events flow out as envelopes through PicoMediator after persist+mutate; replay
never publishes. Handler failures never affect the actor (per-event isolation).
Translation loops (event → command → event) are intended; keep handlers
idempotent and bounded.

> **Breaking change:** direct `ISubscriber<TEvent>` (PicoMediator)
> subscribers no longer receive PicoActor domain events. Migrate to
> `IDomainEventSubscriber<TEvent>`; the envelope's `ActorId`/`Version` replace
> any manually embedded aggregate id.
```

(若 README 中还有旧 `ISubscriber`/`Publish` 描述片段,一并同步替换;保持 14 节结构与既有风格——多语言版本仅做译文同步。)

- [ ] **Step 2: 同步 README.zh.md**

对 `README.zh.md` 的对应章节做同内容中文翻译替换(标题 "订阅领域事件(declare-and-subscribe)"、示例、破坏性变更提示)。

- [ ] **Step 3: 构建 + 测试回归**

Run: `dotnet build PicoActor.slnx && dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj && dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj`
Expected: BUILD SUCCEEDED,全部测试 PASS

- [ ] **Step 4: 提交**

```bash
git add README.md README.zh.md
git commit -m "docs: subscriber API via IDomainEventSubscriber with envelope and ICommandSender"
```

---

## 自审备注(计划级)

- **多订阅者同事件**:handler 注册 append(无 IsRegistered 去重)+ 每事件类型一个 bridge + `TryGetServices` 复数解析——Task 5 与 PicoMediator.Gen 有意差异,GeneratorOutputTests 锁定(spec §7.3)
- **破坏性变更**:旧 `ISubscriber<TEvent>` 订阅在 Task 2 后失效,Task 6 全部迁移;`MediatorIntegrationTests` 之外若有使用旧形状的测试文件,Task 6 Step 2 的失败列表为准
- **版本断言依据**:框架终态事件经 `RaiseEvent` 追加(`SagaActor.cs:99`/`:193`,`Version++`)——批量 [Started, SagaCompleted] 发布 version=2、单独 [SagaFailed] 为 1、恢复追加后为 2,已逐一对应 Task 6 断言
- **遗留(不在本计划)**:其余 7 种语言 README 的译文同步;`docs/superpowers/` 下旧文档(architecture spec、bridge defect note)中的 `ISubscriber` 描述更新;picohex-actor skill 更新;带订阅者的 AOT publish 验证(CI 的 sample publish 不含订阅路径,生成代码的 AOT 兼容性由 GeneratorOutputTests 的编译行为 + 常规构建间接覆盖)
