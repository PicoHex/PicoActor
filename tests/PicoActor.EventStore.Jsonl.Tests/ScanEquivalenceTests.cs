namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The scan's acceptance bar (spec I1, "Testing"): a FAST line count is only worth having if it is the
/// SAME line count, so the oracle is the byte-by-byte loop itself, copied verbatim into
/// <see cref="ReferenceScan"/> — not a re-description of the rules, which would drift with the
/// implementation it is supposed to judge.
/// <para>
/// The three things a scan produces are checked, and nothing else needs to be: <c>Count</c> (how many
/// newline-terminated non-blank lines), <c>Offset</c> (the byte at which <c>fromVersion</c> starts, which
/// is what the cursor is positioned on) and <c>TornTail</c> (bytes after the last newline: reported,
/// never counted). A pathological corpus disagrees on one of them long before it disagrees on anything an
/// operator would notice, so the corpus is deliberately hostile: blank lines, whitespace-only lines, CRLF,
/// trailing spaces, lines longer than the 64 KiB segment, non-ASCII UTF-8 that must never be DECODED
/// (U+2028, U+0085 and a lone <c>\r</c> are not line terminators here), a torn tail, an empty file and a
/// single unterminated line.
/// </para>
/// <para>
/// Two levels, because each can see something the other cannot:
/// </para>
/// <list type="number">
/// <item>
/// <see cref="StoreScan_EqualsTheReference_ForEveryFromVersion"/> goes through the store's public read
/// path (<c>GetVersionAsync</c> = the count, <c>OpenAsync(...).End</c> = the same count,
/// <see cref="JsonlEventStore{TEventBase}.TornTailLines"/> + the warning = the report) on bytes written
/// straight to disk, so garbage lines are legal input — nothing is deserialized.
/// </item>
/// <item>
/// <c>LinesScan_EqualsTheReference_ForEveryFromVersion</c> calls the scan itself, where the returned
/// <c>Offset</c> and <c>TornTail</c> are visible instead of inferred, and re-reads every corpus through a
/// stream that hands out 1, 7 or 64 KiB per <c>Read</c>: a line split across a segment boundary by a short
/// read is exactly where a cleverer loop goes wrong.
/// </item>
/// </list>
/// </summary>
public sealed class ScanEquivalenceTests
{
    /// <summary>
    /// Every corpus, each with a name, so a failure says WHICH shape disagreed instead of pointing at an
    /// index in an anonymous array.
    /// </summary>
    private static IEnumerable<(string Name, byte[] Bytes)> Corpora()
    {
        yield return ("empty file", []);
        yield return ("lone newline", Bytes("\n"));
        yield return ("blank lines only", Bytes("\n\n\n\n"));
        yield return ("whitespace lines only", Bytes("  \t\r\n \t\n  \r\r\n"));
        yield return ("no newline at all", Bytes("abc"));
        yield return ("one terminated line", Bytes("abc\n"));
        yield return ("crlf", Bytes("a\r\nb\r\n"));
        yield return ("crlf blank lines", Bytes("\r\n\r\n\r\n"));
        yield return ("trailing spaces", Bytes("abc   \ndef\t\t\n"));
        yield return ("torn tail after content", Bytes("a\nb\n   "));
        yield return ("torn tail whitespace only", Bytes("   "));
        yield return ("torn tail after a blank line", Bytes("a\n   \n{}"));

        // Non-ASCII must be COUNTED, never decoded: the multibyte terminators a StreamReader would honour
        // (U+2028 = E2 80 A8, U+0085 = C2 85) plus 0x0B/0x0C are content here, and a lone '\r' neither
        // ends a line nor makes one blank.
        yield return ("non-ascii utf-8", Utf8("h\u00e9llo\n\u65e5\u672c\u8a9e\n\u03a9mega"));
        yield return (
            "multibyte terminators that are not newlines",
            [
                0xE2,
                0x80,
                0xA8,
                0x0B,
                0x0C,
                0xC2,
                0x85,
                (byte)'\r',
                (byte)'\n',
                (byte)'x',
                (byte)'\n',
            ]
        );

        // The 64 KiB segment: a newline ON the boundary, a line that SPANS it, and a whitespace-only line
        // that spans it (the blank verdict has to survive the seam, and a line already known to carry
        // content must not be rescanned on the far side of it).
        yield return (
            "newline at the segment boundary",
            Concat(Repeat((byte)'a', 65_535), Bytes("\nb\n"))
        );
        yield return (
            "long line spanning the boundary",
            Concat(Repeat((byte)'a', 65_536), Bytes("\nb\n"))
        );
        yield return (
            "long blank line spanning the boundary",
            Concat(Repeat((byte)'\r', 65_536), Bytes("\nd\n"))
        );
        yield return ("long torn tail", Concat(Bytes("a\n"), Repeat((byte)'b', 200_000)));

        // Ordinary mixed lines: the every-fromVersion sweep needs a wider count than the shapes above.
        yield return ("mixed ordinary lines", Bytes("1\n\n2\n   \n3\r\n\t\r\n4\n5\n\n\n6\n7\nx"));

        // Fixed seeds, so a failure is reproducible byte for byte, over an alphabet dense in exactly the
        // bytes the rule cares about.
        for (var seed = 0; seed < 4; seed++)
            yield return ($"random seed {seed}", RandomCorpus(seed, length: 4096));
    }

    [Test, Timeout(300_000)]
    public async Task StoreScan_EqualsTheReference_ForEveryFromVersion(
        CancellationToken cancellationToken = default
    )
    {
        foreach (var (name, bytes) in Corpora())
        {
            using var temp = new TempRoot();
            var logger = new RecordingLogger();
            var store = new JsonlEventStore<ConformanceEvent>(
                temp.Root,
                new JsonlEventStoreOptions { Logger = logger }
            );
            var id = Guid.NewGuid();
            await File.WriteAllBytesAsync(StreamPath(temp.Root, id), bytes, cancellationToken);

            // The count of the WHOLE stream has one answer (fromVersion 1), so the oracle is asked once.
            var whole = ReferenceScan.Scan(bytes, fromVersion: 1);
            await Assert
                .That(await store.GetVersionAsync(id, cancellationToken))
                .IsEqualTo(whole.Count)
                .Because($"{name}: the count of a whole-file scan");

            // Every scan below reports a torn tail or does not; the store's counter and its warning are
            // that report's operator-visible half, so they are accounted one call at a time.
            var tornTails = whole.TornTail ? 1 : 0;

            foreach (var fromVersion in RepresentedFromVersions(whole.Count))
            {
                // TWO scans per iteration (the count and the cursor), so a torn tail here is reported
                // twice — the accounting stays per CALL, not per corpus.
                var expected = ReferenceScan.Scan(bytes, fromVersion);
                if (expected.TornTail)
                    tornTails += 2;

                await Assert
                    .That(await store.GetVersionAsync(id, cancellationToken))
                    .IsEqualTo(expected.Count)
                    .Because($"{name}: GetVersionAsync with fromVersion {fromVersion}");

                // OpenAsync runs the SAME pass and hands its count out as End. Disposing without moving
                // reads no line, so nothing but the scan can raise the torn-tail report here.
                var cursor = await store.OpenAsync(id, fromVersion, cancellationToken);
                var end = cursor.End;
                await cursor.DisposeAsync();
                await Assert
                    .That(end)
                    .IsEqualTo(expected.Count)
                    .Because($"{name}: OpenAsync(fromVersion {fromVersion}).End");
            }

            await Assert
                .That(store.TornTailLines)
                .IsEqualTo((long)tornTails)
                .Because($"{name}: one torn-tail report per scan that saw one");
            await Assert
                .That(UnterminatedWarnings(logger))
                .IsEqualTo(tornTails)
                .Because($"{name}: one 'unterminated' warning per scan that saw one");
        }
    }

    /// <summary>
    /// The same comparison at the scan itself, where <c>Offset</c> and <c>TornTail</c> are read directly
    /// instead of inferred from a cursor — and repeated over three read chunkings, because a segment seam
    /// is the one place a scan that looks at more than one byte at a time can go wrong: 1 byte per
    /// <c>Read</c> splits EVERY line, 7 bytes splits most of them, 64 KiB reproduces production.
    /// </summary>
    [Test, Timeout(300_000)]
    public async Task LinesScan_EqualsTheReference_ForEveryFromVersion_AndEveryReadChunking(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        foreach (var chunk in (int[])[1, 7, JsonlLines.SegmentBytes])
        {
            foreach (var (name, bytes) in Corpora())
            {
                var whole = ReferenceScan.Scan(bytes, fromVersion: 1);
                for (var fromVersion = 1UL; fromVersion <= whole.Count + 2; fromVersion++)
                {
                    var expected = ReferenceScan.Scan(bytes, fromVersion);
                    var actual = JsonlLines.Scan(new ChunkedStream(bytes, chunk), fromVersion);
                    var reason = $"{name}: fromVersion {fromVersion}, {chunk}-byte reads";

                    await Assert
                        .That(actual.Count)
                        .IsEqualTo(expected.Count)
                        .Because($"{reason}, count");
                    await Assert
                        .That(actual.Offset)
                        .IsEqualTo(expected.Offset)
                        .Because($"{reason}, offset");
                    await Assert
                        .That(actual.TornTail)
                        .IsEqualTo(expected.TornTail)
                        .Because($"{reason}, torn tail");
                }
            }
        }
    }

    /// <summary>
    /// A stream that does not exist has no lines: the count is 0 and the cursor is empty, which is the
    /// answer every scan shape above must also give for a zero-byte file.
    /// </summary>
    [Test, Timeout(30_000)]
    public async Task AbsentStream_CountsZero(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, new JsonlEventStoreOptions());

        await Assert.That(await store.GetVersionAsync(Guid.NewGuid())).IsEqualTo(0UL);
        await using var cursor = await store.OpenAsync(Guid.NewGuid(), 1);
        await Assert.That(cursor.End).IsEqualTo(0UL);
        await Assert.That(await cursor.MoveNextAsync(default)).IsFalse();
    }

    /// <summary>
    /// The offset exists for ONE reason: it is the byte the cursor is positioned on. So the witness is
    /// behavioural — a stream whose lines are real events, with blank and whitespace-only lines written
    /// BETWEEN them, must yield exactly the events from <c>fromVersion</c> onward and nothing past its end.
    /// A cursor positioned one line early, one line late, or on the wrong side of a CRLF fails here.
    /// </summary>
    [Test, Timeout(60_000)]
    public async Task CursorStartsAtTheFromVersionLine(
        CancellationToken cancellationToken = default
    )
    {
        using var seedRoot = new TempRoot();
        using var root = new TempRoot();
        var id = Guid.NewGuid();

        // The event bytes are the SERIALIZER's (each append is one line): a hand-written JSON literal
        // would test the reader's tolerance rather than the scan.
        var seed = new JsonlEventStore<ConformanceEvent>(
            seedRoot.Root,
            new JsonlEventStoreOptions()
        );
        for (var i = 1; i <= 5; i++)
            await seed.AppendAsync(id, (ulong)(i - 1), [new SSnapshotAdded(i)]);

        var lines = Encoding
            .UTF8.GetString(
                await File.ReadAllBytesAsync(StreamPath(seedRoot.Root, id), cancellationToken)
            )
            .Split('\n')
            .Where(line => line.Length > 0)
            .ToArray();
        await Assert.That(lines.Length).IsEqualTo(5);

        // Noise between the events: an empty line, a whitespace-only line, a CRLF-terminated blank line and
        // a trailing-space line. None of them is an event, so the count must step over all four.
        var noise = new[] { "\n", "   \n", "\r\n", " \t\r\n" };
        var composed = new StringBuilder();
        for (var i = 0; i < lines.Length; i++)
        {
            composed.Append(lines[i]).Append('\n');
            if (i < noise.Length)
                composed.Append(noise[i]);
        }

        await File.WriteAllTextAsync(
            StreamPath(root.Root, id),
            composed.ToString(),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(root.Root, new JsonlEventStoreOptions());
        var count = await store.GetVersionAsync(id, cancellationToken);
        await Assert.That(count).IsEqualTo(5UL);

        for (var fromVersion = 1UL; fromVersion <= count + 2; fromVersion++)
        {
            var expected = new List<int>();
            for (var version = fromVersion; version <= count; version++)
                expected.Add((int)version);

            var actual = new List<int>();
            await using var cursor = await store.OpenAsync(id, fromVersion, cancellationToken);
            while (await cursor.MoveNextAsync(cancellationToken))
                actual.Add(((SSnapshotAdded)cursor.Current).Delta);

            await Assert
                .That(string.Join(",", actual))
                .IsEqualTo(string.Join(",", expected))
                .Because($"OpenAsync(fromVersion {fromVersion}) must start at that event");
            await Assert.That(cursor.End).IsEqualTo(count);
        }
    }

    private static string StreamPath(string root, Guid id) =>
        Path.Combine(root, id.ToString("D") + ".jsonl");

    private static int UnterminatedWarnings(RecordingLogger logger) =>
        logger.Messages.Count(message =>
            message.Contains("unterminated", StringComparison.OrdinalIgnoreCase)
        );

    /// <summary>
    /// Every <c>fromVersion</c> for a stream short enough to sweep exhaustively (<c>1..n+2</c>, the two
    /// past-the-end values included); past that, the values that can still tell two implementations apart:
    /// the first, the middle, the last, and the three that land on or past the end.
    /// </summary>
    private static IEnumerable<ulong> RepresentedFromVersions(ulong count)
    {
        if (count <= 40)
        {
            for (var fromVersion = 1UL; fromVersion <= count + 2; fromVersion++)
                yield return fromVersion;
            yield break;
        }

        yield return 1;
        yield return 2;
        yield return 3;
        yield return count / 2;
        yield return count - 1;
        yield return count;
        yield return count + 1;
        yield return count + 2;
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    private static byte[] Repeat(byte value, int count)
    {
        var bytes = new byte[count];
        Array.Fill(bytes, value);
        return bytes;
    }

    private static byte[] Concat(byte[] first, byte[] second)
    {
        var bytes = new byte[first.Length + second.Length];
        first.CopyTo(bytes, 0);
        second.CopyTo(bytes, first.Length);
        return bytes;
    }

    /// <summary>
    /// A deterministic pseudo-random corpus (<c>seed</c> alone fixes it, so any failure reproduces) drawn
    /// from an alphabet dense in the bytes the rule distinguishes: terminators, blank bytes, content, and
    /// two bytes of a multibyte sequence so that a decoder-based implementation cannot pass.
    /// </summary>
    private static byte[] RandomCorpus(int seed, int length)
    {
        byte[] alphabet = [0x0A, 0x0A, (byte)' ', (byte)'\t', (byte)'\r', (byte)'a', 0xC3, 0xA9];
        var state = (uint)(0x9E3779B9 ^ (seed * 0x85EBCA6B));
        var bytes = new byte[length];
        for (var i = 0; i < length; i++)
        {
            // xorshift32: fixed, short, and good enough that the alphabet does not repeat in a period.
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            bytes[i] = alphabet[state % (uint)alphabet.Length];
        }

        return bytes;
    }

    /// <summary>
    /// A read-only stream that answers each <c>Read</c> with at most <c>chunk</c> bytes: the seam control
    /// for a scan that reads in segments.
    /// </summary>
    private sealed class ChunkedStream(byte[] content, int chunk) : Stream
    {
        private int _position;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => content.Length;

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var take = Math.Min(Math.Min(count, chunk), content.Length - _position);
            if (take <= 0)
                return 0;

            Array.Copy(content, _position, buffer, offset, take);
            _position += take;
            return take;
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }
}

/// <summary>
/// The ORACLE: today's scan, byte by byte, copied out of <c>JsonlEventStore.ScanStream</c> verbatim so it
/// keeps answering the way the shipped store did before any of this was touched. It is deliberately NOT
/// refactored toward the new implementation: sharing a helper with the code under test would let one
/// mistake satisfy both sides of the comparison.
/// </summary>
internal static class ReferenceScan
{
    internal static (ulong Count, long Offset, bool TornTail) Scan(
        byte[] content,
        ulong fromVersion
    )
    {
        using var stream = new MemoryStream(content, writable: false);
        return Scan(stream, fromVersion);
    }

    internal static (ulong Count, long Offset, bool TornTail) Scan(Stream stream, ulong fromVersion)
    {
        var skip = fromVersion <= 1 ? 0UL : fromVersion - 1;
        var buffer = new byte[64 * 1024];
        var lineBlank = true;
        var count = 0UL;
        var offset = skip == 0 ? 0L : -1L;
        var lineStart = 0L;
        var absolute = 0L;

        int read;
        while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
        {
            for (var i = 0; i < read; i++)
            {
                var value = buffer[i];
                if (value != (byte)'\n')
                {
                    if (!(value is (byte)' ' or (byte)'\t' or (byte)'\r'))
                        lineBlank = false;
                    continue;
                }

                if (!lineBlank)
                {
                    count++;
                    if (offset < 0 && count == skip)
                        offset = absolute + i + 1;
                }

                lineStart = absolute + i + 1;
                lineBlank = true;
            }

            absolute += read;
        }

        var tornTail = lineStart < absolute;

        // Past the end (or a torn tail): the remaining bytes are where a next event would start, and the
        // caller's capacity is 0 there, so the cursor yields nothing either way.
        if (offset < 0)
            offset = lineStart;

        return (count, offset, tornTail);
    }
}
