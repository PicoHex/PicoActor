namespace PicoActor.Abs;

/// <summary>
/// 框架生成的 saga 完成事件。由 MarkComplete(result) 触发,与业务事件同批原子持久化。
/// replay 时框架识别本事件恢复 _completed,不投递给子类 Mutate。
/// </summary>
public sealed record SagaCompleted(object? Result) : IDomainEvent;

/// <summary>
/// 框架生成的 saga 失败事件。由未捕获的业务异常触发(OnMessageAsync 或 ResumeAsync)。
/// Reason 为"异常类型名 + 截断的消息(≤512 字符)",不序列化异常对象(AOT 安全)。
/// </summary>
public sealed record SagaFailed(string Reason) : IDomainEvent;

/// <summary>ExecuteSaga 的执行结果:saga id + 最终结果。</summary>
public sealed record SagaExecution<TResult>(Guid Id, TResult Result);

/// <summary>
/// saga 业务失败异常。由 SagaActor 在业务异常后生成并 fault AskAsync 调用者的 TCS。
/// Reason 与 SagaFailed 事件一致。
/// </summary>
public sealed class SagaExecutionException : Exception
{
    public Guid SagaId { get; }
    public string Reason { get; }

    public SagaExecutionException(Guid sagaId, string reason)
        : base($"Saga {sagaId} failed: {reason}")
    {
        SagaId = sagaId;
        Reason = reason;
    }
}

/// <summary>ResumeInterruptedSagasAsync 的单个恢复结果。</summary>
public sealed record SagaResumeResult(Guid Id, SagaResumeStatus Status, string? Reason = null);

/// <summary>恢复后 saga 的状态分类。</summary>
public enum SagaResumeStatus
{
    Completed,
    Failed,
    Running,
}
