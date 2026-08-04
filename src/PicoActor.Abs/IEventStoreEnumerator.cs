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
    /// </summary>
    IReadOnlyList<Guid> ListAggregateIds(string firstEventType);
}
