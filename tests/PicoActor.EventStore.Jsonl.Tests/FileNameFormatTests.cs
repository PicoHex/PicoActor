namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The naming surface: <see cref="FileNameFormat"/> (an opt-in stream FILE name), the stream-name
/// classifier every later scan is built on, and the diagnostic counters those scans feed.
/// <para>
/// The classifier is the load-bearing part, and its two rules must agree with the files on disk:
/// <c>&lt;id:D&gt;.jsonl</c> (<see cref="FileNameFormat.IdOnly"/>, today's layout) and
/// <c>&lt;type&gt;-&lt;id:D&gt;.jsonl</c> (<see cref="FileNameFormat.TypePrefix"/>). Parsing strips
/// the stream suffix FIRST and then splits the stem at its FIRST <c>'-'</c>: a GUID contains four
/// hyphens, so a <c>LastIndexOf</c> split would return the last twelve hex digits as the "id", and a
/// short foreign name (<c>x.txt</c>) would make a bare <c>name[..^6]</c> throw.
/// </para>
/// <para>
/// Format detection is SYMMETRIC: a name that is valid under the OTHER rule is
/// <see cref="StreamNameKind.OtherFormat"/> in either direction. An <c>IdOnly</c> store pointed at a
/// <c>{type}-{id}</c> directory is a whole-directory misconfiguration that must be reported as such,
/// never silently enumerated as nothing.
/// </para>
/// </summary>
public sealed class FileNameFormatTests
{
    // ── The parser matrix ───────────────────────────────────────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Parser_ConfiguredMatch_TypePrefix(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var id = Guid.NewGuid();

        // The configured rule for TypePrefix: "<first event type name>-<id:D>".
        var kind = JsonlNames.Classify(
            $"{nameof(SSnapshotAdded)}-{id:D}.jsonl",
            FileNameFormat.TypePrefix,
            out var type,
            out var parsedId
        );

        await Assert.That(kind).IsEqualTo(StreamNameKind.ConfiguredMatch);
        await Assert.That(type).IsEqualTo(nameof(SSnapshotAdded));
        await Assert.That(parsedId).IsEqualTo(id);

        // The split is at the FIRST '-', and the four hyphens INSIDE the GUID are what makes that the
        // only correct choice: the id must come back whole, not truncated to its last hex group.
        var hyphenated = Guid.Parse("11111111-2222-3333-4444-555555555555");
        var viaFirstHyphen = JsonlNames.Classify(
            $"T-{hyphenated:D}.jsonl",
            FileNameFormat.TypePrefix,
            out var hyphenatedType,
            out var hyphenatedId
        );

        await Assert.That(viaFirstHyphen).IsEqualTo(StreamNameKind.ConfiguredMatch);
        await Assert.That(hyphenatedType).IsEqualTo("T");
        await Assert.That(hyphenatedId).IsEqualTo(hyphenated);

        // A hyphen INSIDE the "type" part is not a stream of this format: C# identifiers cannot contain
        // one, so such a name is a skipped entry rather than a stream with a silently invented id.
        var insideType = JsonlNames.Classify(
            $"Type-With-Hyphens-{hyphenated:D}.jsonl",
            FileNameFormat.TypePrefix,
            out _,
            out _
        );

        await Assert.That(insideType).IsEqualTo(StreamNameKind.UnparseableId);
    }

    [Test, Timeout(15000)]
    public async Task Parser_OtherFormat_UnderIdOnly(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var id = Guid.NewGuid();

        // The TypePrefix layout seen by an IdOnly store: the other format, counted and warned about —
        // never a stream of this store.
        var kind = JsonlNames.Classify(
            $"{nameof(SSnapshotAdded)}-{id:D}.jsonl",
            FileNameFormat.IdOnly,
            out _,
            out _
        );

        await Assert.That(kind).IsEqualTo(StreamNameKind.OtherFormat);

        // The configured rule still wins on its own layout.
        var configured = JsonlNames.Classify(
            $"{id:D}.jsonl",
            FileNameFormat.IdOnly,
            out _,
            out var parsedId
        );

        await Assert.That(configured).IsEqualTo(StreamNameKind.ConfiguredMatch);
        await Assert.That(parsedId).IsEqualTo(id);
    }

    [Test, Timeout(15000)]
    public async Task Parser_BareGuid_UnderTypePrefix(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var id = Guid.NewGuid();

        // SYMMETRIC detection: a bare "<id:D>.jsonl" is the OTHER format's rule, so under TypePrefix it
        // is OtherFormat (a mixed directory), NOT a foreign file. Reporting it as Foreign would let an
        // IdOnly directory be enumerated as "no streams" with no diagnostic at all.
        var kind = JsonlNames.Classify($"{id:D}.jsonl", FileNameFormat.TypePrefix, out _, out _);

        await Assert.That(kind).IsEqualTo(StreamNameKind.OtherFormat);
    }

    [Test, Timeout(15000)]
    public async Task Parser_ShortForeignName_NoThrow(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        // "x.txt" is SHORTER than the ".jsonl" suffix: slicing the stem before the suffix guard would
        // throw ArgumentOutOfRangeException inside a boot-time enumeration.
        var underTypePrefix = JsonlNames.Classify("x.txt", FileNameFormat.TypePrefix, out _, out _);
        var underIdOnly = JsonlNames.Classify("x.txt", FileNameFormat.IdOnly, out _, out _);

        await Assert.That(underTypePrefix).IsEqualTo(StreamNameKind.Foreign);
        await Assert.That(underIdOnly).IsEqualTo(StreamNameKind.Foreign);
    }

    [Test, Timeout(15000)]
    public async Task Parser_Readme_IsForeign(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        // A store directory is a shared directory: a README is a foreign ENTRY in both formats.
        await Assert
            .That(JsonlNames.Classify("README.md", FileNameFormat.IdOnly, out _, out _))
            .IsEqualTo(StreamNameKind.Foreign);
        await Assert
            .That(JsonlNames.Classify("README.md", FileNameFormat.TypePrefix, out _, out _))
            .IsEqualTo(StreamNameKind.Foreign);
    }

    [Test, Timeout(15000)]
    public async Task Parser_UnparseableId(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        // The configured SHAPE with an id that is not a D GUID: a skipped file name, not a foreign one.
        var underTypePrefix = JsonlNames.Classify(
            "SSnapshotAdded-notaguid.jsonl",
            FileNameFormat.TypePrefix,
            out _,
            out _
        );
        var underIdOnly = JsonlNames.Classify(
            "SSnapshotAdded-notaguid.jsonl",
            FileNameFormat.IdOnly,
            out _,
            out _
        );

        await Assert.That(underTypePrefix).IsEqualTo(StreamNameKind.UnparseableId);
        await Assert.That(underIdOnly).IsEqualTo(StreamNameKind.UnparseableId);
    }

    // ── The option ──────────────────────────────────────────────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task UndefinedFormat_Throws(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;

        // The default preserves v2026.9.0 byte-for-byte: names, enumeration, counts.
        await Assert
            .That(new JsonlEventStoreOptions().FileNameFormat)
            .IsEqualTo(FileNameFormat.IdOnly);

        // A config typo must NOT silently fall back to IdOnly: the store would read "no streams" and
        // the operator would never learn why.
        var outOfRange = await Assert
            .That(() => new JsonlEventStoreOptions { FileNameFormat = (FileNameFormat)42 })
            .Throws<ArgumentOutOfRangeException>();

        await Assert
            .That(outOfRange!.ParamName)
            .IsEqualTo(nameof(JsonlEventStoreOptions.FileNameFormat));
    }

    // ── G1a: IdOnly is untouched ────────────────────────────────────────────────────────────────

    [Test, Timeout(30000)]
    public async Task G1a_IdOnly_NoScanAtConstruction_And_AppendToExisting(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        // Pre-write ONE stream in the v2026.9.0 layout (a throwaway instance, so the store under test is
        // constructed over an existing directory that already holds a stream).
        var writer = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await writer.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        // Construction performs NO directory scan: an IdOnly store costs nothing until it is used.
        await Assert.That(store.DirectoryScans).IsEqualTo(0L);
        await Assert.That(store.ForeignFormatFiles).IsEqualTo(0L);
        await Assert.That(store.FilesOpened).IsEqualTo(0L);

        // The path is derived from the id alone — that is the whole IdOnly rule.
        var expected = Path.Combine(temp.Root, id.ToString("D") + ".jsonl");
        await Assert.That(store.IdOnlyPathFor(id)).IsEqualTo(expected);
        await Assert.That(File.Exists(expected)).IsTrue();

        // An append to an EXISTING stream resolves without any scan.
        await Assert.That(await store.AppendAsync(id, 1, [new SSnapshotAdded(2)])).IsEqualTo(2UL);
        await Assert.That(store.DirectoryScans).IsEqualTo(0L);

        // ... and the result on disk is unchanged from v2026.9.0: one "<id:D>.jsonl", no second file.
        var names = Directory.GetFiles(temp.Root).Select(Path.GetFileName).ToArray();
        await Assert.That(names.Length).IsEqualTo(1);
        await Assert.That(names[0]).IsEqualTo(id.ToString("D") + ".jsonl");

        // Counts are unchanged: the id is enumerated exactly once and nothing was skipped.
        var ids = await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));
        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(id);
        await Assert.That(store.SkippedFileNames).IsEqualTo(0L);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(2UL);

        var loaded = await store.LoadAsync(id);
        await Assert.That(loaded.Count).IsEqualTo(2);
        await Assert.That(loaded[1]).IsEqualTo(new SSnapshotAdded(2));

        // Still no scan — the enumeration above is a PEEK pass, not a classification pass.
        await Assert.That(store.DirectoryScans).IsEqualTo(0L);
        await Assert.That(store.ForeignFormatFiles).IsEqualTo(0L);

        // Positive control for the counter: an IdOnly enumeration opens the stream (exactly the cost
        // TypePrefix removes in 1b), so the counter is alive rather than frozen at zero.
        await Assert.That(store.FilesOpened).IsGreaterThanOrEqualTo(1L);
    }
}
