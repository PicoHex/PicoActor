using System.IO;
using PicoActor.Abs;

namespace PicoActor.Tests;

internal sealed record GuardProbeEvent : IDomainEvent;

/// <summary>Mutate 里调 MarkComplete——迁移期旧代码模式,必须 loud 失败。</summary>
internal sealed class MutateMarkCompleteSaga : SagaActor
{
    public MutateMarkCompleteSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        MarkComplete(); // 旧代码:在 Mutate 里标记完成
    }
}

internal sealed record PendingStart : ICommand;

internal sealed record PendingFinish : ICommand;

internal sealed record PendingStep1 : IDomainEvent;

internal sealed record PendingStep2 : IDomainEvent;

/// <summary>第一条命令只推进,第二条命令推进 + MarkComplete——验证 pending 在失败 flush 后被清除。</summary>
internal sealed class PendingProbeSaga : SagaActor
{
    public PendingProbeSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case PendingStart:
                RaiseEvent(new PendingStep1());
                return default;
            case PendingFinish:
                RaiseEvent(new PendingStep2());
                MarkComplete("done");
                return new ValueTask<object?>("done");
            default:
                return default;
        }
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>同一消息内重复 MarkComplete——只应产生一个 SagaCompleted。</summary>
internal sealed record DoubleCompleteCmd : ICommand;

internal sealed class DoubleCompleteSaga : SagaActor
{
    public DoubleCompleteSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is DoubleCompleteCmd)
        {
            RaiseEvent(new PendingStep1());
            MarkComplete("first");
            MarkComplete("second"); // 幂等:不追加第二个完成事件
            return new ValueTask<object?>("first");
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>第 N 次 AppendAsync 抛一次异常的 store——pending 失败清除验证。</summary>
internal sealed class FailOnceStore : IEventStore
{
    private readonly InMemoryEventStore _inner = new();
    private int _callCount;

    public int FailOnCall { get; init; } = 2;

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        if (Interlocked.Increment(ref _callCount) == FailOnCall)
            throw new IOException("store down");
        return _inner.AppendAsync(actorId, expectedVersion, events);
    }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
        _inner.LoadAsync(actorId);
}

public sealed class MarkCompleteGuardTests
{
    // ═══════════════════════════════════════════════════════════
    // I2: MarkComplete 位置约束——Mutate 内调用必须 loud 失败
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task MarkComplete_InMutate_ThrowsInvalidOperation()
    {
        var actor = new MutateMarkCompleteSaga();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        try
        {
            var ex = await Assert
                .That(() =>
                    ((IEventSourcedActor)actor).ReplayEvents(
                        new IDomainEvent[] { new GuardProbeEvent() }
                    )
                )
                .Throws<InvalidOperationException>();

            await Assert.That(ex.Message).Contains("MarkComplete");
        }
        finally
        {
            await actor.StopAsync();
        }
    }

    // ═══════════════════════════════════════════════════════════
    // M1: 失败 flush 后 pending 清除——下次 flush 不追加虚假 SagaCompleted
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task FailedFlush_ClearsPending_NoSpuriousSagaCompletedOnNextFlush()
    {
        var store = new FailOnceStore { FailOnCall = 2 }; // 第 2 次 append(PendingFinish 批)失败
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<PendingProbeSaga>(
            _ => new PendingProbeSaga(),
            () => new PendingProbeSaga()
        );

        var saga = await system.CreateAsync<PendingProbeSaga>(new PendingStart());
        await system.AskAsync<object?>(saga.Id, new PendingStart()); // append #1 OK

        // append #2 失败:[PendingStep2 + SagaCompleted] 整批丢弃,pending 清除
        // → FailAsync 的 SagaFailed flush 不得携带残留的 SagaCompleted
        await Assert
            .That(async () => await system.AskAsync<string>(saga.Id, new PendingFinish()))
            .Throws<SagaExecutionException>();

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<PendingStep1>();
        await Assert.That(events[1]).IsTypeOf<SagaFailed>();
        await Assert.That(events.OfType<SagaCompleted>().Count()).IsEqualTo(0);
    }

    // ═══════════════════════════════════════════════════════════
    // M2: 同一消息内重复 MarkComplete 幂等——只追加一个 SagaCompleted
    // ═══════════════════════════════════════════════════════════

    [Test]
    public async Task MarkComplete_RepeatedInSameMessage_AppendsSingleCompletion()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<DoubleCompleteSaga>(
            _ => new DoubleCompleteSaga(),
            () => new DoubleCompleteSaga()
        );

        var saga = await system.CreateAsync<DoubleCompleteSaga>(new DoubleCompleteCmd());
        await system.AskAsync<string>(saga.Id, new DoubleCompleteCmd());

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.OfType<SagaCompleted>().Count()).IsEqualTo(1);
        await Assert.That(events[^1]).IsTypeOf<SagaCompleted>();
    }
}
