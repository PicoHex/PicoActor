namespace PicoActor.Abs;

/// <summary>
/// Saga Actor — 有限生命周期的协调 actor(同时覆盖 saga 与 process manager 模式)。
/// 继承 EventSourcedActor;进度由业务事件记录,终态(完成/失败)由框架生成的事件
/// (SagaCompleted/SagaFailed)承载——replay 时由框架恢复,不依赖子类纪律。
///
/// 生命周期:
///   CreateAsync/ExecuteSaga → 命令经 mailbox 驱动(无构造期命令处理)
///   → MarkComplete(result) → 框架同批追加 SagaCompleted(result) 原子落盘
///   → auto-stop(异步,Task.Run + Task.Yield)
///   或 OnMessageAsync/ResumeAsync 抛未捕获业务异常 → 框架追加 SagaFailed(reason)
///   → auto-stop → AskAsync 调用者收 SagaExecutionException(Id, Reason)
///
/// 崩溃恢复:
///   GetAsync 重建 → ReplayEvents(框架恢复终态标志)→ 无终态事件则 ResumeAsync()
///   → 框架 flush 包装消费 resume 期间的 pending(推进到完成时 SagaCompleted 同批落盘)
///   → 到达终态则 auto-stop。恢复是显式拉取(ResumeInterruptedSagasAsync),无后台魔法。
/// </summary>
public abstract class SagaActor : EventSourcedActor
{
    private bool _completed;
    private bool _failed;
    private string? _failedReason;
    private object? _pendingResult;
    private bool _pendingComplete;
    private bool _inCommandContext;

    /// <summary>
    /// [Obsolete] 构造期命令处理已废弃——命令只走 mailbox。
    /// 保留旧语义:命令仍在构造期处理(链 EventSourcedActor(ICommand))——
    /// 与 OnReadyAsync 的恢复判断(Version>0)组合可能误触发 resume,这是废弃 API 的
    /// 已知行为,仅影响迁移期旧代码(编译警告提示迁移);新代码必须用参数less构造。
    /// </summary>
    [Obsolete(
        "Saga creation commands are handled via the mailbox. Use the parameterless constructor."
    )]
    protected SagaActor(ICommand creationCommand)
        : base(creationCommand) { }

    /// <summary>唯一构造路径。命令经 mailbox 驱动。</summary>
    protected SagaActor() { }

    /// <summary>replay 后由框架从事件流恢复。</summary>
    protected internal bool IsCompleted => _completed;

    /// <summary>replay 后由框架从事件流恢复。</summary>
    protected internal bool IsFailed => _failed;

    /// <summary>replay 后由框架从 SagaFailed(reason) 恢复;恢复 API 分类用。</summary>
    protected internal string? FailedReason => _failedReason;

    /// <summary>
    /// 标记完成。仅记录 pending——框架 flush 包装在本次 flush 中追加 SagaCompleted(result)。
    /// 只能在 OnMessageAsync 或 ResumeAsync 内调用;其他位置(Mutate/replay/hook)抛异常。
    /// 同一消息内重复调用幂等忽略。
    /// </summary>
    protected void MarkComplete(object? result = null)
    {
        if (!_inCommandContext)
            throw new InvalidOperationException(
                "MarkComplete must be called from OnMessageAsync or ResumeAsync."
            );
        _pendingComplete = true;
        _pendingResult = result;
    }

    /// <summary>
    /// 恢复后的重评估 hook:可推进、可 AskAsync 查询外部聚合再决策、可 no-op(继续等事件)。
    /// 每个步骤必须幂等(_step &lt; N 守卫或外部查询)——框架保证事件不重复落盘、单飞、串行。
    /// </summary>
    protected abstract ValueTask ResumeAsync();

    protected override bool TryHandleFrameworkEvent(IDomainEvent @event)
    {
        switch (@event)
        {
            case SagaCompleted:
                _completed = true;
                return true;
            case SagaFailed f:
                _failed = true;
                _failedReason = f.Reason;
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// 框架 flush 包装:消费 pending(追加 SagaCompleted 到同一批),然后走基类持久化。
    /// pending 生命周期 = 单次 flush 尝试,无论成败清除。
    /// ProcessAsync 与 OnReadyAsync 两个入口共用。
    /// </summary>
    protected override async ValueTask FlushEventsAsync()
    {
        if (_pendingComplete)
            RaiseEvent(new SagaCompleted(_pendingResult));
        _pendingComplete = false;
        _pendingResult = null;

        await base.FlushEventsAsync().ConfigureAwait(false);
    }

    protected override async ValueTask OnReadyAsync()
    {
        await base.OnReadyAsync().ConfigureAwait(false);

        // 恢复:无终态事件且 Version > 0(SagaActor 构造无命令 → 构造期无事件 → Version>0 ⟺ 重放)
        if (!_completed && !_failed && Version > 0)
        {
            _inCommandContext = true;
            try
            {
                await ResumeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // 恢复路径业务失败 = 终态:记 SagaFailed,不抛(SagaFailed 落盘失败时此处抛出,
                // 由 GetAsync 的 init 失败清理路径处理)
                await FailAsync(ex).ConfigureAwait(false);
            }
            finally
            {
                _inCommandContext = false;
            }
            await FlushEventsAsync().ConfigureAwait(false);
        }

        if (_completed || _failed)
            ScheduleStop();
    }

    /// <summary>
    /// 终态守卫 + 失败处理。终态后到达的命令拒绝处理(Ask fault / Send 静默丢弃,
    /// 子类不被调用)——防止终态后事件污染事件流(auto-stop 是异步的,存在窗口期)。
    /// </summary>
    protected sealed override async ValueTask ProcessAsync(Envelope envelope)
    {
        if (_completed || _failed)
        {
            envelope.Tcs?.TrySetException(
                new InvalidOperationException($"Saga {Id} already terminated.")
            );
            return;
        }

        _inCommandContext = true;
        try
        {
            await base.ProcessAsync(envelope).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // 业务失败 → 终态。SagaFailed 落盘失败(store down)时此处抛出原始异常,
            // RunAsync 会 fault TCS(原始异常)——符合"基础设施失败非终态"语义。
            await FailAsync(ex).ConfigureAwait(false);
            var failure = new SagaExecutionException(Id, MakeReason(ex));
            if (envelope.Tcs is not null)
                envelope.Tcs.TrySetException(failure);
            else
                UnhandledErrorHandler?.Invoke(failure, envelope.Command);
        }
        finally
        {
            _inCommandContext = false;
        }

        if (_completed || _failed)
            ScheduleStop();
    }

    private async ValueTask FailAsync(Exception ex)
    {
        var reason = MakeReason(ex);

        // 丢弃未提交业务事件与 pending(现有原子性语义:失败时事件未落盘)。
        // Version 必须同步回滚——CommitEvents 只清列表,保留 Version 会让后续
        // SagaFailed 的 flush 算出错误的 expectedVersion(ConcurrencyException)。
        Version -= (ulong)((IEventSourcedActor)this).GetUncommittedEvents().Count;
        ((IEventSourcedActor)this).CommitEvents();
        _pendingComplete = false;
        _pendingResult = null;

        RaiseEvent(new SagaFailed(reason));
        await FlushEventsAsync().ConfigureAwait(false); // 可能抛(store down)→ 传播原始异常
    }

    private static string MakeReason(Exception ex)
    {
        var message = ex.Message ?? string.Empty;
        if (message.Length > 512)
            message = message.Substring(0, 512);
        return $"{ex.GetType().Name}: {message}";
    }

    /// <summary>
    /// 异步调度停止。Task.Run + Task.Yield 保证当前 ProcessAsync/OnReadyAsync
    /// 完全返回后再 StopAsync(否则 await _loopTask 自死锁)。
    /// </summary>
    private void ScheduleStop()
    {
        if (System is null)
            return;

        var sys = System;
        var id = Id;

        _ = Task.Run(async () =>
        {
            await Task.Yield();
            try
            {
                await sys.StopAsync(id).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort cleanup — saga terminal events are already persisted.
            }
        });
    }
}
