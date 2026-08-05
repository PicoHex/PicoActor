# PicoMediator 2026.8.1 缺陷报告（PicoActor 迁移实证）

> 日期: 2026-08-04 · 版本: PicoMediator/PicoDI 2026.8.1 · 来源: PicoActor mediator-81 迁移
> 两个缺陷均在 PicoActor 迁移中实证（测试 + 探针），已用 workaround 落地，需 PicoMediator 侧修复。
>
> **状态更新（2026-08-04 晚）**: 缺陷 1 已由 PicoActor 侧根治——`PicoActor.Abs` 目标框架改为 **net10.0**，
> bridge 生成代码可编译，框架事件（`SagaCompleted`/`SagaFailed`）现已**可类型化订阅**（测试实证：
> `SagaCompletedSub` 收到事件）；统一订阅者已迁移为类型化订阅并删除。缺陷 1 的 PicoMediator 侧修复
>（注册设施下沉 Abs / 生成开关）仍建议推进，惠及其他 netstandard2.0 消费集。缺陷 2 在 2026.8.2
> 已由 `85caefd`（deterministic configurator ordering）修复。

---

## 缺陷 1: bridge 生成代码不兼容 netstandard2.0 消费集（框架事件类型化订阅不可达）

### 现象

2026.8.1 的 Gen analyzer（内嵌于 `PicoMediator.Abs` 包，buildTransitive 自动注入）对任何**包含具体 `IEvent` 派生类型**的编译单元生成 `MediatorEventDispatchers.*.g.cs`。生成代码引用：

- `global::PicoMediator.MediatorAutoSubscriptionRegistry`（**主包 PicoMediator, net10.0**）
- `global::PicoDI.Abs.SvcDescriptor / SvcLifetime / ISvcScope`（PicoDI.Abs）
- `[ModuleInitializer]`（netstandard2.0 无此 attribute）

`PicoActor.Abs`（netstandard2.0、零 PicoHex 依赖、定义 `SagaCompleted`/`SagaFailed`）编译失败：

```
MediatorEventDispatchers.PicoActor_Abs.g.cs(13,30): error CS0400: The type or namespace name 'PicoDI'
  could not be found ...
MediatorEventDispatchers.PicoActor_Abs.g.cs(107,6): error CS0246: The type or namespace name
  'ModuleInitializerAttribute' could not be found ...
```

### 影响

- netstandard2.0 消费集**无法生成 bridge** → 跨程序集基类型发布无法路由到具体订阅者
- PicoActor 的具体后果：框架事件 `SagaCompleted`/`SagaFailed`（定义于 `PicoActor.Abs`）在 2026.8.1 下**无法被类型化订阅**（`ISubscriber<SagaCompleted>` 收不到——测试 canary 锁定为 0）
- 框架事件只能走统一订阅者 `ISubscriber<IDomainEvent>`（base-key 直接订阅，见缺陷 2 契约）

### Workaround（PicoActor 已落地——2026-08-04 晚由 TFM 变更根治，workaround 已移除）

**已移除**：`PicoActor.Abs` 改为目标 **net10.0**（`IsAotCompatible`/`IsTrimmable`），并增加
`PicoMediator`（主包）与 `PicoDI.Abs` 引用（`PrivateAssets=all`，仅生成代码内部使用）——
bridge 在 Abs 正常生成，框架事件可类型化订阅。原先的 `BeforeTargets="CoreCompile"`
analyzer 移除 Target 已删除。

保留此节供其他 netstandard2.0 消费集参考：

```xml
<Target Name="ExcludePicoMediatorGenAnalyzer" BeforeTargets="CoreCompile">
  <ItemGroup>
    <Analyzer Remove="@(Analyzer)" Condition="'%(Filename)' == 'PicoMediator.Gen'" />
  </ItemGroup>
</Target>
```

### 修复建议（PicoMediator 侧，选一）

1. `MediatorAutoSubscriptionRegistry` + 注册辅助下沉到 `PicoMediator.Abs`（netstandard2.0 兼容）——生成的 dispatcher 即可在 netstandard2.0 编译
2. 生成器提供 MSBuild 开关（如 `PicoMediatorGenDisableDispatchers`）——消费集自行选择
3. 生成器检测编译单元无法引用所需基础设施时跳过 dispatcher 生成（并报诊断）

---

## 缺陷 2: base-key 订阅者与 bridge 同键共存（IsRegistered 去重跳过用户订阅者）

### 文档契约 vs 实际行为

最新 skill 文档契约: "base-key direct subscribers + all concrete subscribers of the runtime type"——即 `ISubscriber<IEvent>` 统一订阅者应收到基类型发布。

实际机制：

- **bridge 注册无去重**：`container.Register(ISubscriber<Base>, ...)`（生成器注释声称 "disjoint keys"，实际与用户 base-key 订阅者**同键**）
- **用户 base-key 订阅者注册有去重**：`if (!container.IsRegistered(typeof(ISubscriber<Base>)))` → 已存在则跳过
- **configurator 应用顺序 = configuratorId Ordinal 排序**：`pico-mediator::events::...`（bridge）< `pico-mediator::<Assembly>::...`（handlers）——小写 `e` 排在字母前

**推论**：任一程序集的 bridge configurator 先于消费集的 handler configurator 应用 → 消费集的 base-key 统一订阅者被 `IsRegistered` 去重**跳过** → 契约破坏（订阅者注册数为 1：只剩 bridge）。

### PicoActor 实证

- **无 Abs bridge 时（缺陷 1 workaround 后）**：测试集 handler configurator 先应用 → 统一订阅者正常注册 → 基类型发布到达统一订阅者 ✅ 契约成立（`EventOutflow_DeclareAndSubscribe_ReachesSubscriber` 等测试持续 GREEN）
- **计划探针（Concrete=1、Unified=0、注册数=1）**：仅在存在"先排序的 bridge 程序集"时复现——与上述推论一致

### 修复建议（PicoMediator 侧）

bridge 注册不得导致用户 base-key 订阅者被跳过：同键共存/追加语义（bridge 去重逻辑与用户注册互不感知），或独立键机制（如 `ISubscriber<Base>` 的 bridge 内部转发链）。

---

## 附加记录（非缺陷）

- **PMGEN001**（基类型发布无可视具体事件诊断）在 `PicoActor` 主包适配器触发（`Publish<IDomainEvent>` 跨程序集）——`#pragma warning disable PMGEN001` 抑制，语义正确（bridge 在应用集编译时生成）
- **E1（PicoDI 2026.8.1）**：Singleton 工厂使用容器内部根 scope——PicoActor 的 captive dependency（自动接线绑定首解析 scope）已修复，契约测试重写为 `AutoWiring_ChildScopeFirstResolution_SurvivesDisposal`（GREEN）
- **适配器 ODE 诊断**保留为防御（用户自建 publisher 场景），消息含 "root scope" 引导

---

## 最小复现探针（缺陷 1）

```csharp
// netstandard2.0 类库,引用 PicoMediator.Abs 2026.8.1,声明一个具体事件:
public sealed record MyEvent(int Value) : IDomainEvent;   // IDomainEvent : IEvent

// 构建 → MediatorEventDispatchers.*.g.cs 生成 → CS0400 (PicoDI 不存在) / CS0246 (ModuleInitializerAttribute)
```

## 最小复现探针（缺陷 2）

```csharp
// 程序集 A(bridge 源):声明 EventA : IEvent → 生成 pico-mediator::events::A::... configurator
// 程序集 B(消费者):声明 UnifiedSub : ISubscriber<IEvent> → 生成 pico-mediator::B::... configurator
container.AddPicoMediator();   // 按 configuratorId Ordinal 应用: A(events) 先于 B
mediator.Publish((IEvent)new EventA());
// 预期(契约): UnifiedSub.Received == 1
// 实际: UnifiedSub 未注册(IsRegistered 去重跳过)→ UnifiedSub.Received == 0
```
