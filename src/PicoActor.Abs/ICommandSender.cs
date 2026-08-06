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
