namespace PicoActor.Abs;

/// <summary>
/// The READ port of an event stream: a forward-only cursor instead of an eagerly materialized
/// list. Optional companion contract for <see cref="IEventStore"/> — a store that implements it
/// lets the runtime (and tools/projections) read the events from a version onward in order,
/// observing the stream length ONCE, and lets a file store deserialize only the tail it is asked
/// for.
/// <para>
/// Capability discovery is a single runtime rule — <c>eventStore is IEventLogReader</c> — so a
/// store opts in by implementing this interface and needs no configuration: the rebuild path
/// prefers the cursor and falls back to <see cref="IEventStore.LoadAsync"/> when the store is not
/// a reader. Business code should keep using <see cref="IActorSystem"/>; this interface is for
/// store authors and for tools/projections that read a stream directly.
/// </para>
/// <para>
/// <see cref="IEventStore"/> is unchanged: <see cref="IEventStore.AppendAsync"/> stays the write
/// path and <see cref="IEventStore.LoadAsync"/>/<see cref="IEventStore.PeekFirstAsync"/> stay as
/// the fallback. Whole-stream projections legitimately need the list and keep using
/// <see cref="IEventStore.LoadAsync"/>.
/// </para>
/// </summary>
public interface IEventLogReader
{
    /// <summary>
    /// The current event count of an actor's stream (0 when there is no stream). This is the
    /// tools/diagnostics surface; the rebuild path does NOT call it — it takes the length from
    /// the <see cref="IEventCursor.End"/> of the cursor it already opened (one observation).
    /// <para>
    /// The count is a LINE COUNT, taken by the same cheap scan <see cref="IEventCursor.End"/> uses:
    /// complete, non-blank, terminated lines (records), never a JSON validation. It therefore equals
    /// <c>(await LoadAsync(id)).Count</c> for a clean stream, and may EXCEED it for a damaged one: a
    /// terminated line that fails to deserialize is counted here while
    /// <see cref="IEventStore.LoadAsync"/> and the cursor throw for it (a file store does not repair
    /// corruption). An unterminated final line — the crash tail — is neither counted nor thrown.
    /// </para>
    /// </summary>
    ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default);

    /// <summary>
    /// Open a forward-only cursor over the events with version ≥ <paramref name="fromVersion"/>
    /// (1-based, inclusive) and report the stream length as <see cref="IEventCursor.End"/>.
    /// <para>
    /// <paramref name="fromVersion"/> = 0 is invalid and throws
    /// <see cref="System.ArgumentOutOfRangeException"/>. A value PAST the end is legal and yields
    /// an empty cursor whose <see cref="IEventCursor.End"/> is the actual length — the rollback
    /// path depends on it (a snapshot newer than the stream opens at <c>version + 1</c>).
    /// </para>
    /// <para>
    /// The implementation takes a read snapshot at open (the in-memory store copies the stream
    /// under its per-actor gate), so a concurrent append cannot tear the cursor.
    /// </para>
    /// </summary>
    ValueTask<IEventCursor> OpenAsync(
        Guid actorId,
        ulong fromVersion,
        CancellationToken ct = default
    );

    /// <summary>
    /// The first deserializable event (version 1), or null when the stream does not exist or
    /// yields no such event. Same meaning as <see cref="IEventStore.PeekFirstAsync"/>, exposed
    /// here so recovery enumeration that already holds a reader never has to call the write port.
    /// </summary>
    ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId, CancellationToken ct = default);
}
