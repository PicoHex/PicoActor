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

/// <summary>
/// OPTIONAL capability of an <see cref="IEventLogReader"/>: open a cursor at a BYTE POSITION instead of
/// at a version. A store whose stream is a file can seek to where a snapshot says its tail begins, so a
/// rebuild pays for reading the tail instead of numbering every line of a 47 MB stream.
/// <para>
/// Capability discovery is the same one runtime test the read port uses — <c>store is
/// IEventLogOffsetReader</c> — so a store that keeps its events in memory simply does not declare it,
/// and no caller has to ask a question with a configured answer.
/// </para>
/// <para>
/// An offset is ADVISORY. Implementations must refuse — answer <see langword="null"/>, log, never
/// throw — anything they cannot verify against the stream itself: a position past the end, a position
/// that is not a line boundary, or a stream that shrank below the length the snapshot recorded. The
/// caller's answer to <see langword="null"/> is today's path (open at a version and let the store
/// scan), so a refusal costs speed and nothing else.
/// </para>
/// </summary>
public interface IEventLogOffsetReader
{
    /// <summary>
    /// Open a forward-only cursor over the events from <paramref name="byteOffset"/> on, or
    /// <see langword="null"/> when that position cannot be believed (see the type's summary) or the
    /// stream does not exist.
    /// <para>
    /// The cursor numbers nothing before <paramref name="byteOffset"/>, so it cannot report the stream
    /// length: its <see cref="IEventCursor.End"/> is <see cref="ulong.MaxValue"/> — the explicit "not
    /// measured" sentinel — and the caller derives the length, and therefore the rebuilt version, from
    /// the events it reads (<c>firstVersion - 1 + count</c>). That is ONE observation of the stream (the
    /// read itself), which is what makes the derivation safe: a length counted by a separate pass could
    /// describe a stream this cursor never saw.
    /// </para>
    /// <para>
    /// <paramref name="firstVersion"/> is the version the FIRST yielded event has (1-based, normally
    /// <c>snapshot.Version + 1</c>); version 0 is invalid and throws
    /// <see cref="System.ArgumentOutOfRangeException"/>. A negative offset is an argument error too. A
    /// rolled back stream is NOT an error: the cursor simply reaches the end of the file, and the
    /// caller's own checks decide what that means.
    /// </para>
    /// </summary>
    ValueTask<IEventCursor?> OpenAtOffsetAsync(
        Guid actorId,
        long byteOffset,
        ulong firstVersion,
        CancellationToken ct = default
    );
}
