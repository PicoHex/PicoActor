namespace PicoActor.Abs;

/// <summary>
/// Optional event-store capability: enumerate aggregate ids by first-event type name.
/// Used by recovery paths (content adoption, singleton scan); frameworks return
/// an empty list when the store does not support enumeration.
/// </summary>
public interface IEventStoreEnumerator
{
    /// <summary>
    /// List ids of aggregates whose FIRST event type name matches
    /// <paramref name="firstEventType"/>. Empty if no matches.
    /// Async by design: enumeration takes a per-stream gate and scans every stream, so a
    /// synchronous wait would block the caller's thread inside an otherwise awaitable
    /// recovery path.
    /// </summary>
    ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType);
}
