# PicoActor 事件订阅抽象(带上下文信封 + declare-and-subscribe)

**日期**:2026-08-06
**状态**:已确认(用户逐节拍板:Q1-A 应用代码纯度 / Q2-B 破坏性升级 / Q3-Gen 现在上 / 信封形状=类型化信封 / ICommandSender 含 ExecuteSaga / 命名 IDomainEventSubscriber)
**范围**:PicoActor(事件订阅子系统)

## 1. 背景与动机

现状核查(2026-08-06 报告,已实证)确认两个精确缺口:

- **G1 无订阅侧抽象**:`PicoActor.Abs` 只有发布钩子(`IDomainEventPublisher`),无任何订阅者抽象;业务消费事件被迫依赖 `PicoMediator.Abs.ISubscriber<TEvent>`(类型依赖)+ Mediator/SvcContainer(运行时依赖),无法"纯粹使用 PicoActor"
- **G2 订阅端上下文丢失**:发布端有 `(actorId, version)`(`PublishAsync(actorId, version, events)`),但 `MediatorDomainEventPublisher` 只把它们写进错误日志,转发 `Publish(e)` 时丢弃;`ISubscriber<T>.Handle(e, ct)` 拿不到上下文——handler 连"事件来自哪个聚合"都不知道,事件→命令翻译无从谈起

**设计定位(用户拍板)**:PicoMediator 是基础设施,PicoActor 是技术框架——后者使用前者实现功能。业务代码只依赖 PicoActor;PicoActor 内部依赖 PicoMediator 做传输。**PicoActor 不做兼容包袱,接受破坏性变更。**

框架身份:事件是唯一的信息流出通道,命令是唯一的流入通道——跨聚合协作只有"事件→命令"翻译一种模式(与既有架构 spec 的"eventhandler 是无状态翻译层"一致)。

## 2. 设计原则

1. **actor 是纯执行单元**:只收 command(mailbox)、只发 domain event(RaiseEvent),无其他副作用
2. **event handler 是纯翻译单元**:订阅事件 → 翻译成 command → 经 `ICommandSender` 发给目标 actor;无状态、无注入依赖(端口逐调用传入)、零业务状态
3. **上下文随事件交付**:订阅端必须拿到 `(actorId, version)`——用类型化信封,事件无需自携带聚合 id
4. **最窄接口**:handler 只获得发送命令所需的窄端口(`ICommandSender`),不注入完整 `IActorSystem`(镜像 PicoMediator 的 `IRequester/IPublisher/IMediator` 哲学)
5. **declare-and-subscribe 是唯一注册路径**:PicoActor.Gen 扫描 `IDomainEventSubscriber<TEvent>` 自动注册,不提供显式 DI 注册 API(YAGNI;将来需要时 Gen 生成的正是显式注册调用,增量兼容)
6. **错误隔离契约不变**:订阅者失败永不影响 actor(现有逐事件 catch 保留)

## 3. 应用架构与数据流

```
                 PicoMediator (基础设施: 消息总线, 非 actor)
                          │
  Command ──► 聚合Actor ──► DomainEvent ──► MediatorDomainEventPublisher
              (actor)     (业务事件)         └─► Publish(DomainEventEnvelope)     非泛型信封(传输)
                                                       │
                                                       ▼ (mediator 按具体类型路由)
                                            ISubscriber<DomainEventEnvelope> bridge × N
                                                       │  is TEvent 类型测试
                                                       ▼
                                            handler.Handle(DomainEventEnvelope<TEvent>, ICommandSender, ct)
                                                       │  Send / AskAsync / ExecuteSaga
                                                       ▼
                                              目标 actor 的 mailbox
```

关键点:

- 发布的是**具体类型** `DomainEventEnvelope`(非泛型,`IEvent` 实现)——直接键匹配路由,**不需要**基类型发布 bridge(PMGEN001 类复杂度不涉及)
- 基类型声明订阅者(`ISubscriber<IEvent>`)不收具体类型发布——信封不会泄漏给统一订阅者(PicoMediator 路由契约)
- 框架事件 `SagaCompleted/SagaFailed` 走同一发布路径 → 自动以信封交付(`IDomainEventSubscriber<SagaCompleted>` 可订阅)
- replay 永不发布(恢复静默)——现状不变,订阅者不重放

## 4. 组件详设

### 4.1 PicoActor.Abs · 新文件 `IDomainEventSubscriber.cs`

```csharp
namespace PicoActor.Abs;

/// <summary>订阅者契约:接收类型化事件 + 上下文信封,翻译为命令发给 actor。</summary>
public interface IDomainEventSubscriber<in TEvent> where TEvent : IDomainEvent
{
    ValueTask Handle(DomainEventEnvelope<TEvent> envelope, ICommandSender sender, CancellationToken ct = default);
}

/// <summary>交付给 handler 的类型化信封。Event 已保证是 TEvent(由 bridge 类型测试保证)。</summary>
public sealed record DomainEventEnvelope<TEvent>(Guid ActorId, ulong Version, TEvent Event)
    where TEvent : IDomainEvent;

/// <summary>传输信封。实现 IEvent 但不实现 IDomainEvent —— 永不入事件存储,语义诚实。</summary>
public sealed record DomainEventEnvelope(Guid ActorId, ulong Version, IDomainEvent Event) : IEvent;
```

### 4.2 PicoActor.Abs · 新文件 `ICommandSender.cs`

```csharp
namespace PicoActor.Abs;

/// <summary>handler 的命令发送窄端口。镜像 IActorSystem 对应方法签名(无 ct,与 IActorSystem 一致)。</summary>
public interface ICommandSender
{
    void Send(Guid actorId, ICommand command);
    ValueTask<TResult> AskAsync<TResult>(Guid actorId, ICommand command);
    ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command);
}
```

### 4.3 PicoActor runtime · 改 `MediatorDomainEventPublisher.cs`

- `PublishAsync` 内逐事件包装:`await _publisher.Publish(new DomainEventEnvelope(actorId, version, e))`
- actorId/version 不再只进错误日志——真正进入信封
- 逐事件 try/catch 隔离保留(含 ObjectDisposedException 诊断分支)
- `IDomainEventPublisher` 接口**不变**——信封化是 Mediator 适配器的内部决策,自定义 publisher 不受影响

### 4.4 PicoActor runtime · 新文件 `ActorSystemCommandSender.cs`(内部类)

`ICommandSender` 的 ActorSystem 薄适配器:Send/AskAsync/ExecuteSaga 直接委托给 ActorSystem 实例。

### 4.5 PicoActor runtime · 改 `PicoActorDiExtensions.cs`

`AddPicoActor(...)` 三个重载统一追加注册:`container.Register(typeof(ICommandSender), scope => new ActorSystemCommandSender((IActorSystem)scope.GetService(typeof(IActorSystem))), SvcLifetime.Singleton)`——IActorSystem 注册之后的独立注册,工厂内解析单例 IActorSystem(单例→单例,任意 scope 解析安全)。

### 4.6 新项目 `src/PicoActor.Gen`(analyzer · 内嵌进 PicoActor.Abs)

照抄 PicoMediator.Gen 蓝图(已验证:ProjectReference `OutputItemType="Analyzer"` 内嵌 + `buildTransitive/PicoActor.Gen.props` 注入):

**扫描规则**(镜像 `MediatorGenerator.cs`):

- `typeSymbol.AllInterfaces` 中 `ContainingNamespace == "PicoActor.Abs"` 且 `MetadataName == "IDomainEventSubscriber`1"`
- 仅闭合、非抽象、非 open generic 实现;每个 `IDomainEventSubscriber<TEvent>` 接口生成一条注册

**每程序集生成 configurator**:

- 每程序集一个 configurator,id = `pico-actor::<Assembly>`(注册进 `MediatorAutoSubscriptionRegistry`;`AddPicoMediator()` 的 `TryApplyConfiguration` 自动应用——**零新增 DI 入口**;与 `pico-mediator::` id 序无关,注册键不相交) 
- 每 handler 生成两项注册(均 Transient,照 PicoMediator 声明-订阅模式):
  1. handler 本体:`Register(typeof(IDomainEventSubscriber<TEvent>), ...)` —— 构造依赖从容器解析
  2. bridge:`Register(typeof(ISubscriber<DomainEventEnvelope>), ...)` —— 内部 `if (envelope.Event is TEvent e)` 类型测试(编译期泛型,AOT 安全),命中则构造 `DomainEventEnvelope<TEvent>` 并调用 `handler.Handle(env, sender, ct)`;handler 与 `ICommandSender` 按 PicoMediator 现有生成的 bridge 代码模式从 scope 解析
- 注册键 `ISubscriber<DomainEventEnvelope>` 与 PicoMediator 生成的键不相交 → 无 dedup 冲突;同一 key 多注册 = mediator 1:N 扇出(多订阅者同事件天然支持)

### 4.7 PicoActor.Abs.csproj · 改

- `<ProjectReference Include="..\PicoActor.Gen\PicoActor.Gen.csproj" OutputItemType="Analyzer" ReferenceOutputAssembly="false" />`
- 生成代码引用 `PicoMediator` + `PicoDI.Abs` 的 `PrivateAssets="all"` 已具备(csproj 注释已声明此用途)

## 5. 错误处理

| 场景 | 行为 |
|---|---|
| handler 抛异常 | 传入 mediator Publish → `AggregateException` → `MediatorDomainEventPublisher` 逐事件 catch(现有)→ **actor 不受影响** |
| 一个事件多订阅者,部分失败 | 同上,逐事件隔离;后续事件继续发布 |
| 无订阅者 | mediator 静默丢弃(PUB/SUB 语义,现状) |
| 订阅者缺失(漏注册) | 不可能——Gen 编译期注册,漏一个 handler 类 = 少一个生成条目,但编译期可查(与 PicoMediator 相同的"声明即注册"保证) |
| 事件→命令→事件级联 | **设计意图**;防风暴是文档化纪律(handler 应幂等、有界),非机制——与 skill re-entrancy 警示一致 |

## 6. 破坏性变更与迁移

| 变更 | 迁移 |
|---|---|
| `ISubscriber<TEvent>`(PicoMediator 直连)不再收到 PicoActor 领域事件 | 改为实现 `IDomainEventSubscriber<TEvent>`;需要上下文时读 `envelope.ActorId/Version` |
| `ISubscriber<IDomainEvent>` 统一订阅者同理失效 | 同上(逐事件类型订阅) |
| 自定义 `IDomainEventPublisher` | **不受影响**(接口契约不变;其输出格式自定) |

测试更新:`MediatorIntegrationTests` 等改为信封断言(actorId/version 精确匹配)。

## 7. 测试计划

1. 类型化分发:`IDomainEventSubscriber<CounterIncremented>` 收到信封,`Event` 为精确类型
2. envelope 上下文精确性:actorId 正确(新创建聚合的 id)、version = 批后版本
3. 多订阅者同事件:全部收到,互不干扰
4. 框架事件信封化:`IDomainEventSubscriber<SagaCompleted>` 可订阅 saga 终态
5. 逐事件隔离:一个 handler 抛异常 → 其他 handler 与后续事件正常;actor 状态不受影响
6. 无订阅者静默:不抛、不挂起
7. Gen 输出测试:照 `PicoMediator.Tests/GeneratorOutputTests` 模式(生成源码快照断言)
8. AOT/裁剪:Gen 输出引用均为既有包(PicoMediator/PicoDI.Abs),`IsAotCompatible` 维持
9. `ICommandSender`:Send 到达 mailbox / AskAsync 返回结果 / ExecuteSaga 完整执行(含 saga 终态信封)

## 8. 文件变更清单

| 文件 | 操作 |
|---|---|
| `src/PicoActor.Abs/IDomainEventSubscriber.cs` | 新增(接口 + 两个信封类型) |
| `src/PicoActor.Abs/ICommandSender.cs` | 新增 |
| `src/PicoActor/MediatorDomainEventPublisher.cs` | 改(发布信封) |
| `src/PicoActor/ActorSystemCommandSender.cs` | 新增(内部类) |
| `src/PicoActor/PicoActorDiExtensions.cs` | 改(注册 ICommandSender) |
| `src/PicoActor.Gen/PicoActor.Gen.csproj` | 新增 |
| `src/PicoActor.Gen/ActorSubscriberGenerator.cs` | 新增 |
| `src/PicoActor.Gen/buildTransitive/PicoActor.Gen.props` | 新增 |
| `src/PicoActor.Abs/PicoActor.Abs.csproj` | 改(内嵌 analyzer) |
| `tests/PicoActor.Tests/MediatorIntegrationTests.cs` 等 | 改(信封断言 + 新测试) |
| `README.md`(+多语言) | 改(订阅新用法) |
| 本 spec | 新增 |

## 9. 非目标(Out of Scope)

- 显式 DI 注册 API(`AddPicoActorEventSubscriber<...>`)——Gen 是唯一路径;将来需要时纯增量
- 持久化订阅/重放投递(replay 永不发布,现状)
- 级联风暴机制防护(文档纪律)
- 旧 `ISubscriber<TEvent>` 兼容层(破坏性,已确认)
- 快照/分布式 actor(既有 Non-Scope)
