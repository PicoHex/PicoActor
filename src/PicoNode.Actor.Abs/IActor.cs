namespace PicoNode.Actor.Abs;

/// <summary>
/// Base interface for all actors. Provides the actor's UUID v7 identity.
/// </summary>
public interface IActor
{
    /// <summary>Framework-generated UUID v7 identity. Assigned by IActorSystem after construction.</summary>
    Guid Id { get; }
}
