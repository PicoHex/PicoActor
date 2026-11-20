using System.Buffers;

namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The ONE line rule of this store, shared by the count scan, the cursor positioning and the loader
/// so the three can never disagree: a line ends at a newline (and only at a newline), a blank line is
/// one whose bytes are all ASCII space, tab or carriage return, and the bytes after the last newline
/// are a TORN TAIL — the only shape a process crash leaves, never an event.
/// </summary>
internal static class JsonlLines
{
    internal const byte NewLine = (byte)'\n';

    /// <summary>The blank-byte set of the line rule: ASCII space (the first member of the set).</summary>
    internal const byte Space = (byte)' ';

    /// <summary>Tab — blank, but never a terminator.</summary>
    internal const byte Tab = (byte)'\t';

    /// <summary>
    /// Carriage return — blank, and DELIBERATELY not a terminator: this store splits on <c>\n</c> alone,
    /// so a CRLF leaves its <c>\r</c> at the end of the line's bytes (where it changes nothing) instead of
    /// creating an empty line nobody wrote.
    /// </summary>
    internal const byte CarriageReturn = (byte)'\r';

    /// <summary>
    /// The read segment of <see cref="Scan"/>: the bytes a stream is walked in. One buffer per call is
    /// amortized over a real stream, and 64 KiB stays in cache between the scan's two passes over it
    /// (newline boundaries, then the blank verdict).
    /// </summary>
    internal const int SegmentBytes = 64 * 1024;

    /// <summary>True when the line carries no content (spaces, tabs and carriage returns only).</summary>
    internal static bool IsBlank(ReadOnlySpan<byte> line)
    {
        foreach (var value in line)
        {
            if (!IsBlankByte(value))
                return false;
        }

        return true;
    }

    /// <summary>The byte form of the blank-line rule, for the scan that classifies without copying.</summary>
    internal static bool IsBlankByte(byte value) => value is Space or Tab or CarriageReturn;

    /// <summary>
    /// Count complete, non-blank, newline-terminated lines and record the byte offset at which the
    /// <paramref name="fromVersion"/>-th event starts. No JSON validation: the count is a LINE count
    /// (the documented relaxation of "version == the loader's count").
    /// <para>
    /// The stream is read from its CURRENT position, in <see cref="SegmentBytes"/> segments, and is left
    /// where the pass ended — the caller positions a cursor, it does not get a rewind.
    /// </para>
    /// <para>
    /// The loop does NOT step byte by byte (that was 455 MB/s on a 47 MB production stream): it finds the
    /// terminator with a vectorized <see cref="MemoryExtensions.IndexOf{T}(ReadOnlySpan{T}, T)"/> and asks
    /// "does this line carry content" with a vectorized
    /// <see cref="MemoryExtensions.IndexOfAnyExcept{T}(ReadOnlySpan{T}, T, T, T)"/> — one search per LINE
    /// for the boundary, and the content search only for a line that does not begin with content, instead
    /// of one branch per BYTE. The RULES are untouched, which is the whole point of the
    /// differential suite (<c>ScanEquivalenceTests</c>): only <c>\n</c> terminates, a line counts iff it
    /// holds a byte outside <c>{ ' ', '\t', '\r' }</c>, and the bytes after the last newline are a torn
    /// tail that is reported and never counted. Because the searches are byte-exact, a U+2028, a U+0085 or
    /// a lone <c>\r</c> inside a line is content — the scan never decodes UTF-8 to decide where a line is.
    /// </para>
    /// </summary>
    /// <param name="stream">The stream to walk; only <c>Read</c> is used.</param>
    /// <param name="fromVersion">1-based version whose first byte the offset must report.</param>
    internal static LineScan Scan(Stream stream, ulong fromVersion)
    {
        var skip = fromVersion <= 1 ? 0UL : fromVersion - 1;

        // Rented, not allocated: a probe loop calls this once per aggregate per call and most streams are a
        // few hundred bytes, so a fresh 64 KiB array per call would be the scan's largest cost. Safe because
        // every read below is bounded by SegmentBytes and only the bytes ACTUALLY read are looked at, so no
        // byte left by a previous renter is ever inside the span being scanned.
        var buffer = ArrayPool<byte>.Shared.Rent(SegmentBytes);

        // The verdict belongs to the LINE, not to the segment: it starts blank at the start of the stream
        // and survives a line split across two reads.
        var lineHasContent = false;
        var count = 0UL;

        // -1 means "not seen yet": the offset is recorded the first time the count reaches `skip`, and if
        // it never does, the answer is the last line start (past the end / torn tail), set below.
        var offset = skip == 0 ? 0L : -1L;
        var lineStart = 0L;
        var absolute = 0L;

        try
        {
            int read;
            while ((read = stream.Read(buffer, 0, SegmentBytes)) > 0)
            {
                var segment = buffer.AsSpan(0, read);
                var position = 0;

                while (position < read)
                {
                    var newline = segment[position..].IndexOf(NewLine);
                    var end = newline < 0 ? read : position + newline;

                    // An EMPTY part (two newlines in a row) contains no byte outside the blank set, so it
                    // stays blank — which is the rule for an empty line, reached without a special case.
                    // A line already known to carry content is not re-judged: its content may be in an
                    // earlier segment, and rescanning it would cost the tail of every long line.
                    if (!lineHasContent)
                    {
                        // The FIRST byte settles the ordinary line — every real event starts with '{' — so
                        // the vectorized search is what a whitespace-LED line pays, not every line. One
                        // byte outside the blank set is already the answer, and a part with no byte at all
                        // is the empty line above, so this is the same verdict, only reached sooner.
                        if (end > position && !IsBlankByte(segment[position]))
                            lineHasContent = true;
                        else if (
                            segment[position..end].IndexOfAnyExcept(Space, Tab, CarriageReturn) >= 0
                        )
                            lineHasContent = true;
                    }

                    // No terminator in the rest of the segment: the line continues in the next one.
                    if (newline < 0)
                        break;

                    if (lineHasContent)
                    {
                        count++;
                        if (offset < 0 && count == skip)
                            offset = absolute + end + 1;
                    }

                    position = end + 1;
                    lineStart = absolute + end + 1;
                    lineHasContent = false;
                }

                absolute += read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        // Bytes after the last newline: a fragment a crash left, never an event.
        var tornTail = lineStart < absolute;

        // Past the end (or a torn tail): the remaining bytes are where a next event would start, and the
        // caller's capacity is 0 there, so the cursor yields nothing either way.
        if (offset < 0)
            offset = lineStart;

        return new LineScan(count, offset, tornTail);
    }
}
