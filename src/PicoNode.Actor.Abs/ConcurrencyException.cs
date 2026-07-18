namespace PicoNode.Actor.Abs;

/// <summary>
/// Thrown by IEventStore when the expected version does not match the stored version,
/// indicating a concurrent modification of the event stream.
/// </summary>
public sealed class ConcurrencyException : Exception
{
    public Guid ActorId { get; }
    public ulong ExpectedVersion { get; }
    public ulong ActualVersion { get; }

    public ConcurrencyException(Guid actorId, ulong expectedVersion, ulong actualVersion)
        : base(
            $"Concurrency conflict on {actorId}: expected version {expectedVersion}, actual {actualVersion}."
        )
    {
        ActorId = actorId;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }
}
