namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The naming format's PRICE, measured instead of argued: what an enumeration costs on the SAME stream
/// count in either layout — <c>&lt;id:D&gt;.jsonl</c> must OPEN every stream to learn its type, while
/// <c>&lt;type&gt;-&lt;id:D&gt;.jsonl</c> classifies the directory entries and opens nothing — plus the
/// one cost the prefixed layout ADDS: a brand-new id is a map miss, and a miss is a filename pass.
/// <para>
/// This is a BENCHMARK, not a gate, so it is <see cref="ExplicitAttribute"/>: too slow and far too
/// machine-dependent for CI, and it runs only when it is asked for by name:
/// </para>
/// <code>
/// dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj -c Release -- --treenode-filter "/*/*/FileNameFormatPerf/*"
/// </code>
/// <para>
/// Nothing is written to docs and nothing is asserted about a DURATION: a benchmark that fails a build
/// because the machine was busy is a benchmark an operator learns to ignore. The COUNTS are asserted —
/// an enumeration that measured beautifully while returning the wrong ids would be worthless — and the
/// measured value is consumed for the same reason, so no run can be optimized away.
/// </para>
/// <para>
/// Seeding goes through <see cref="JsonlEventStore{TEventBase}.AppendAsync"/> on purpose, never a
/// hand-rolled line: the bytes on disk are then exactly what the serializer writes, three events per
/// stream. (A stream file carries no header — the <c>PASJ</c> magic belongs to the snapshot container,
/// not to a JSONL stream.) The prefixed seed is inherently O(N&#178;): under
/// <see cref="FileNameFormat.TypePrefix"/> a brand-new id MISSES the resolution map, and every miss is
/// one filename pass over the root. That is precisely what item 3 measures; the seed itself is timed
/// and reported separately, never inside a measurement.
/// </para>
/// </summary>
public sealed class FileNameFormatPerf
{
    /// <summary>Streams per layout: big enough that 4,600 opens dominate the enumeration's own cost.</summary>
    private const int StreamsPerDirectory = 4600;

    /// <summary>Events per stream file — one batch, so one write and three lines.</summary>
    private const int EventsPerStream = 3;

    /// <summary>First appends in a brand-new directory: the "new id" path, measured as ONE total.</summary>
    private const int NewIdAppends = 2000;

    private const int WarmupRuns = 1;
    private const int MeasuredRuns = 5;

    // Disjoint id ranges, one per corpus, so a report's file names can never be confused for another
    // corpus's (the two layouts also own separate directories — a mixed root is a diagnosed error).
    private const int IdOnlySeedBase = 10_000;
    private const int TypePrefixSeedBase = 100_000;
    private const int NewIdSeedBase = 1_000_000;

    [Test, Explicit, Category("Performance"), Timeout(1_800_000)]
    public async Task ListAndFirstAppend(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var idOnlyRoot = new TempRoot();
        using var typePrefixRoot = new TempRoot();
        using var newIdRoot = new TempRoot();

        var seedStopwatch = Stopwatch.StartNew();
        await SeedAsync(idOnlyRoot.Root, FileNameFormat.IdOnly, IdOnlySeedBase);
        var idOnlySeedMs = seedStopwatch.Elapsed.TotalMilliseconds;
        seedStopwatch.Restart();
        await SeedAsync(typePrefixRoot.Root, FileNameFormat.TypePrefix, TypePrefixSeedBase);
        var typePrefixSeedMs = seedStopwatch.Elapsed.TotalMilliseconds;

        Console.WriteLine(
            $"seed: {StreamsPerDirectory} streams x {EventsPerStream} events — "
                + $"IdOnly {idOnlySeedMs:F0} ms, TypePrefix {typePrefixSeedMs:F0} ms "
                + "(not part of any measurement below)"
        );

        // Two COLD instances over their own seeded directory: the IdOnly one pays nothing at
        // construction, the TypePrefix one pays its single filename pass to learn the map — a cost that
        // belongs to startup and is therefore outside the enumeration timed below, which re-walks the
        // directory either way (IdOnly opens each stream, TypePrefix reads each name).
        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        var typePrefix = new JsonlEventStore<ConformanceEvent>(
            typePrefixRoot.Root,
            PrefixedOptions()
        );

        Report(
            "ListAggregateIdsAsync — IdOnly (<id>.jsonl)",
            await MeasureAsync(async () =>
            {
                var ids = await idOnly.ListAggregateIdsAsync(nameof(SSnapshotAdded));
                Expect(ids.Count, StreamsPerDirectory, "IdOnly enumeration");
            })
        );

        Report(
            "ListAggregateIdsAsync — TypePrefix (<type>-<id>.jsonl)",
            await MeasureAsync(async () =>
            {
                var ids = await typePrefix.ListAggregateIdsAsync(nameof(SSnapshotAdded));
                Expect(ids.Count, StreamsPerDirectory, "TypePrefix enumeration");
            })
        );

        // The cost the prefixed layout ADDS, as ONE total: 2,000 FIRST appends into an empty directory.
        // The directory is emptied before every run, so each run is the same cold start — a run that
        // inherited the previous run's files would measure 2,000 EXISTING streams instead.
        Report(
            $"first append x{NewIdAppends} — fresh TypePrefix directory",
            await MeasureAsync(async () =>
            {
                TestRoots.TryDelete(newIdRoot.Root);

                var writer = new JsonlEventStore<ConformanceEvent>(
                    newIdRoot.Root,
                    PrefixedOptions()
                );
                for (var i = 0; i < NewIdAppends; i++)
                    await writer.AppendAsync(DeterministicId(NewIdSeedBase + i), 0, EventsFor(i));

                Expect(
                    Directory.GetFiles(newIdRoot.Root).Length,
                    NewIdAppends,
                    "new-id first appends"
                );
            })
        );
    }

    // ── Seeding ────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Write <see cref="StreamsPerDirectory"/> streams of <see cref="EventsPerStream"/> events each, in
    /// <paramref name="format"/>, through the store itself — so the corpus is the store's own output and
    /// not an approximation of it.
    /// </summary>
    private static async Task SeedAsync(string root, FileNameFormat format, int idBase)
    {
        var store = new JsonlEventStore<ConformanceEvent>(
            root,
            new JsonlEventStoreOptions { FileNameFormat = format }
        );

        for (var i = 0; i < StreamsPerDirectory; i++)
            await store.AppendAsync(DeterministicId(idBase + i), 0, EventsFor(i));
    }

    private static IReadOnlyList<IDomainEvent> EventsFor(int seed)
    {
        var events = new IDomainEvent[EventsPerStream];
        for (var i = 0; i < EventsPerStream; i++)
            events[i] = new SSnapshotAdded(seed + i);

        return events;
    }

    /// <summary>
    /// A REPRODUCIBLE id per index — the same corpus on every machine and every run, so two reports are
    /// comparable and a slow number can be chased into a file name. The index is the first GUID field,
    /// which keeps every id distinct without a random source anywhere in the harness.
    /// </summary>
    private static Guid DeterministicId(int index) => new(index, 0x5EED, 0, 1, 2, 3, 4, 5, 6, 7, 8);

    // ── Measurement ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="WarmupRuns"/> untimed run (JIT, tiered compilation, the file-system cache) then
    /// <see cref="MeasuredRuns"/> timed ones, returned SORTED so the caller's median is a middle index.
    /// A full collection precedes each timed run: a run that inherited the previous run's garbage would
    /// report a pause the machine, not the code, decided.
    /// </summary>
    private static async Task<double[]> MeasureAsync(Func<Task> action)
    {
        for (var i = 0; i < WarmupRuns; i++)
            await action();

        var samples = new double[MeasuredRuns];
        for (var i = 0; i < MeasuredRuns; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var stopwatch = Stopwatch.StartNew();
            await action();
            stopwatch.Stop();

            samples[i] = stopwatch.Elapsed.TotalMilliseconds;
        }

        Array.Sort(samples);
        return samples;
    }

    /// <summary>One line per measurement: <c>label: median X ms (min Y, max Z)</c>, plain for a human or a log.</summary>
    private static void Report(string label, double[] sortedSamples) =>
        Console.WriteLine(
            $"{label}: median {sortedSamples[sortedSamples.Length / 2]:F1} ms "
                + $"(min {sortedSamples[0]:F1}, max {sortedSamples[^1]:F1})"
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

    private static JsonlEventStoreOptions PrefixedOptions() =>
        new() { FileNameFormat = FileNameFormat.TypePrefix };
}
