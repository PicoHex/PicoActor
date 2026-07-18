namespace PicoNode.Actor.Abs;

/// <summary>
/// Optional interface for actors that support cancelling a long-running
/// operation without stopping the actor. ActorSystem.CancelTurn checks
/// for this interface to bypass the mailbox for immediate cancellation.
/// </summary>
public interface ICancelable
{
    /// <summary>Cancel the currently running long-running operation. No-op if nothing is running.</summary>
    void CancelCurrentTurn();
}
