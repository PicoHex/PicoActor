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

    public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command)
        where TSaga : SagaActor => _system.ExecuteSaga<TSaga, TResult>(command);
}
