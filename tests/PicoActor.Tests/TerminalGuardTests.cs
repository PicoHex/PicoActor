using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>OnMessageAsync 被调用计数——终态守卫后子类不应被调用。</summary>
internal sealed class GuardProbeSaga : SagaActor
{
    public static int MessageCount;

    public GuardProbeSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        Interlocked.Increment(ref MessageCount);
        if (command is StartSaga s)
        {
            RaiseEvent(new SagaStep1Started(s.Name));
            MarkComplete(s.Name);
            return new ValueTask<object?>(s.Name);
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class TerminalGuardTests
{
    [Test]
    public async Task Command_AfterTerminal_FaultsOrDrops_WithoutProcessing()
    {
        GuardProbeSaga.MessageCount = 0;

        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<GuardProbeSaga>(_ => new GuardProbeSaga(), () => new GuardProbeSaga());

        var saga = await system.CreateAsync<GuardProbeSaga>(new StartSaga("x"));
        await system.AskAsync<string>(saga.Id, new StartSaga("x"));

        // auto-stop 窗口期内再 AskAsync:要么 KeyNotFoundException(已移除),要么 fault(终态守卫),
        // 绝不挂起、绝不再次处理
        await Task.Delay(50);
        Exception? caught = null;
        try
        {
            await system.AskAsync<string>(saga.Id, new StartSaga("again"));
        }
        catch (Exception ex)
        {
            caught = ex;
        }
        await Assert.That(caught).IsNotNull();
        await Assert.That(GuardProbeSaga.MessageCount).IsEqualTo(1);

        // 事件流无终态后事件
        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Count).IsEqualTo(2); // Step1Started + SagaCompleted
    }
}
