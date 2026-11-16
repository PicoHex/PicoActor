namespace PicoActor.Abs;

/// <summary>
/// Forward-only read position over one actor's event stream, opened by
/// <see cref="IEventLogReader.OpenAsync"/>. A cursor is a READ SNAPSHOT taken at open: it
/// describes the stream as of that moment and never yields more than its <see cref="End"/>.
/// <para>
/// Versions are 1-based and strictly ascending — version ≡ position in the stream — so a cursor
/// opened at <c>V</c> yields exactly the events with version in <c>[V, End]</c> while the stream
/// is unchanged. If the stream is truncated or rolled back while the cursor is open, the cursor
/// MAY yield fewer events (best effort, never more): callers that need a complete tail detect
/// that with <c>End</c> instead of a per-event contract.
/// </para>
/// <para>
/// A cursor owns no domain state and is cheap to dispose; <see cref="System.IAsyncDisposable.DisposeAsync"/>
/// releases the underlying handle (a file handle, a lock, a snapshot array reference) and is
/// idempotent. Disposal does not invalidate an event that was already produced by
/// <see cref="MoveNextAsync"/>.
/// </para>
/// </summary>
public interface IEventCursor : IAsyncDisposable
{
    /// <summary>
    /// The stream length (event count) observed by the <see cref="IEventLogReader.OpenAsync"/>
    /// that created this cursor. It never changes for the cursor's lifetime and the cursor MUST
    /// NOT yield more events than this.
    /// <para>
    /// This is the single-observation anchor of the rebuild path: the rollback guard
    /// (<c>snapshot.Version &gt; End</c>), the "did the drain get the whole tail" check
    /// (<c>snapshot.Version + tail.Count == End</c>) and the reconstructed
    /// <see cref="IEventSourcedActor.Version"/> all come from this one value, so a rebuild never
    /// mixes two different observations of the stream.
    /// </para>
    /// </summary>
    ulong End { get; }

    /// <summary>
    /// The event produced by the last successful <see cref="MoveNextAsync"/> call.
    /// Throws <see cref="System.InvalidOperationException"/> only when no move has ever succeeded:
    /// before the first one, and after the cursor is exhausted.
    /// <para>
    /// A cancelled or failed move KEEPS the last successfully produced event readable — the failure
    /// belongs to the move attempt, not to what was already read (the same rule as disposal).
    /// </para>
    /// </summary>
    IDomainEvent Current { get; }

    /// <summary>
    /// The 1-based version of <see cref="Current"/> (normally equal to the position in the
    /// stream). Throws <see cref="System.InvalidOperationException"/> exactly when
    /// <see cref="Current"/> does.
    /// </summary>
    ulong CurrentVersion { get; }

    /// <summary>
    /// Advance to the next event. Returns <c>false</c> once the cursor is exhausted, and stays
    /// <c>false</c> on every later call. A corrupt event throws the same exception the store's
    /// loader would raise for it — a cursor never silently skips a bad event.
    /// </summary>
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
}
