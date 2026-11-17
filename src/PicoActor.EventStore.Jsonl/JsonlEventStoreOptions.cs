namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Tunables of <see cref="JsonlEventStore{TEventBase}"/>.
/// </summary>
public sealed record JsonlEventStoreOptions
{
    /// <summary>
    /// fsync every append. Default <c>false</c>: the bytes are flushed to the OS (so another reader
    /// sees the event immediately) but not to the platter. The documented cost of leaving it off is
    /// that a power loss can leave a TERMINATED garbage line, which the store treats as corruption
    /// and never repairs — an operator removes the line.
    /// </summary>
    public bool FlushToDisk { get; init; }

    /// <summary>
    /// Upper bound of the per-aggregate gate table. Must be greater than zero. An evicted entry costs
    /// one re-scan on that aggregate's next append, never correctness.
    /// </summary>
    public int MaxAggregateLocks { get; init; } = 1024;

    /// <summary>
    /// The store's logger, independent of any DI <c>ILoggerFactory</c>: a host that wants the store's
    /// warnings in its own log passes the same logger explicitly. Null is silent.
    /// </summary>
    public ILogger? Logger { get; init; }
}
