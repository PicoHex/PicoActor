namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The answer of one pass over a stream, and the whole of it: <paramref name="Count"/> is how many
/// newline-terminated, non-blank lines the stream holds, <paramref name="Offset"/> is the byte at which
/// the requested <c>fromVersion</c> starts (the position a cursor is opened at), and
/// <paramref name="TornTail"/> says whether bytes were left after the last newline — a shape a crash
/// leaves, reported and never counted.
/// <para>
/// The three travel together because they come from ONE pass: counting, positioning and the torn-tail
/// verdict are the same walk over the same bytes, so a caller can never be handed a count that belongs to
/// a different read of the file than its offset.
/// </para>
/// </summary>
/// <param name="Count">Terminated, non-blank line count — the stream's version.</param>
/// <param name="Offset">First byte of the <c>fromVersion</c>-th event; past the end, the last line start.</param>
/// <param name="TornTail">True when bytes follow the final newline (they are NOT counted).</param>
internal readonly record struct LineScan(ulong Count, long Offset, bool TornTail);
