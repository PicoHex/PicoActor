namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The scan's PRICE, measured instead of argued (spec D5): the SAME bytes, counted twice — once by
/// <see cref="ReferenceScan"/> (today's per-byte loop, the oracle of the differential suite) and once by
/// <see cref="JsonlLines.Scan"/> (the vectorized one) — on a 50 000-line / 1.7 MB synthetic stream.
/// <para>
/// This is a BENCHMARK, not a gate, so it is <see cref="ExplicitAttribute"/>: the ratio is a property of the
/// machine (SIMD width, cache, JIT) and a build that fails because the box was busy is a build people learn
/// to ignore. What IS asserted is the one thing a fast scan must never get wrong: both counts, and both
/// offsets, agree exactly — a 4× scan that counts a different number of lines is not a faster scan.
/// </para>
/// <code>
/// dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj -c Release -f net10.0 -- --treenode-filter "/*/*/ScanPerf/*"
/// </code>
/// </summary>
public sealed class ScanPerf
{
    /// <summary>50 000 lines — a stream a busy aggregate reaches, and enough to swamp per-call noise.</summary>
    private const int Lines = 50_000;

    /// <summary>~34 bytes of ordinary event JSON per line: 50 000 of them is ~1.7 MB.</summary>
    private const string Line = "{\"$type\":\"ss-added\",\"Delta\":123456}\n";

    private const int WarmupRuns = 2;
    private const int MeasuredRuns = 11;

    [Test, Explicit, Category("Performance"), Timeout(600_000)]
    public void VectorizedScan_VersusTheByteLoop(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        var bytes = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(Line, Lines)));
        var mebibytes = bytes.Length / (1024.0 * 1024.0);

        // The oracle and the scan must agree BEFORE anything is timed: same count, same offset, same
        // torn-tail verdict (there is none — every line is terminated).
        foreach (
            var fromVersion in (ulong[])[1, 2, (ulong)(Lines / 2), (ulong)Lines, (ulong)Lines + 1]
        )
        {
            var expected = ReferenceScan.Scan(bytes, fromVersion);
            using var stream = new MemoryStream(bytes, writable: false);
            var actual = JsonlLines.Scan(stream, fromVersion);

            Expect(actual.Count, expected.Count, $"count at fromVersion {fromVersion}");
            Expect(actual.Offset, expected.Offset, $"offset at fromVersion {fromVersion}");
            Expect(actual.TornTail, expected.TornTail, $"torn tail at fromVersion {fromVersion}");
        }

        Expect(
            (int)JsonlLines.Scan(new MemoryStream(bytes, writable: false), 1).Count,
            Lines,
            "lines"
        );

        var old = Measure(() => ReferenceScan.Scan(new MemoryStream(bytes, writable: false), 1));
        var neu = Measure(() => JsonlLines.Scan(new MemoryStream(bytes, writable: false), 1));

        Report("byte loop (reference)", old, mebibytes);
        Report("vectorized scan", neu, mebibytes);
        Console.WriteLine(
            $"speedup: {old[MeasuredRuns / 2] / neu[MeasuredRuns / 2]:F2}x "
                + $"(median {old[MeasuredRuns / 2]:F2} ms -> {neu[MeasuredRuns / 2]:F2} ms "
                + $"over {Lines} lines / {mebibytes:F2} MiB)"
        );
    }

    /// <summary>
    /// One <c>Scan</c> per sample, timed — the shape every caller uses (<c>GetVersionAsync</c>,
    /// <c>OpenAsync</c>, the append's length witness). The stream is built INSIDE the timed region because
    /// a <see cref="MemoryStream"/> construction is what the store pays too: it wraps a <see cref="FileStream"/>
    /// over the same bytes.
    /// </summary>
    private static double[] Measure(Func<object> scan)
    {
        for (var i = 0; i < WarmupRuns; i++)
            _ = scan();

        var samples = new double[MeasuredRuns];
        for (var i = 0; i < samples.Length; i++)
        {
            var stopwatch = Stopwatch.StartNew();
            _ = scan();
            stopwatch.Stop();
            samples[i] = stopwatch.Elapsed.TotalMilliseconds;
        }

        Array.Sort(samples);
        return samples;
    }

    private static void Report(string label, double[] sortedSamples, double mebibytes) =>
        Console.WriteLine(
            $"{label}: median {sortedSamples[MeasuredRuns / 2]:F2} ms "
                + $"({mebibytes / (sortedSamples[MeasuredRuns / 2] / 1000.0):F0} MiB/s, "
                + $"min {sortedSamples[0]:F2}, max {sortedSamples[^1]:F2})"
        );

    /// <summary>
    /// The corpus check: a duration means nothing if the call under it did not do the work. Thrown, not
    /// asserted — a benchmark reports, and the only failure worth raising is "the corpus is wrong".
    /// </summary>
    private static void Expect(int actual, int expected, string what)
    {
        if (actual != expected)
            throw new InvalidOperationException($"{what}: expected {expected}, got {actual}.");
    }

    private static void Expect(ulong actual, ulong expected, string what)
    {
        if (actual != expected)
            throw new InvalidOperationException($"{what}: expected {expected}, got {actual}.");
    }

    private static void Expect(long actual, long expected, string what)
    {
        if (actual != expected)
            throw new InvalidOperationException($"{what}: expected {expected}, got {actual}.");
    }

    private static void Expect(bool actual, bool expected, string what)
    {
        if (actual != expected)
            throw new InvalidOperationException($"{what}: expected {expected}, got {actual}.");
    }
}
