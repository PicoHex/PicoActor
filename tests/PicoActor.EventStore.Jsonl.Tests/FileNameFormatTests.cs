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

    // ── G2: TypePrefix enumerates the NAMES, with zero opens ────────────────────────────────────

    /// <summary>
    /// The whole point of the format: the type is carried by the FILE NAME, so an enumeration resolves
    /// the ids without opening a single stream. The IdOnly half of the test is the positive control —
    /// the counter is alive, it just stops moving when there is nothing to open.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G2_TypePrefix_EnumeratesByFileName_WithZeroOpens(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var idOnlyRoot = new TempRoot();
        using var typePrefixRoot = new TempRoot();

        // Two INDEPENDENT directories: the same id must never exist in both layouts (that is the
        // mixed-directory condition diagnosed in 1c), so each format owns its own root.
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var events = new IDomainEvent[]
        {
            new SSnapshotAdded(1),
            new SSnapshotAdded(2),
            new SSnapshotAdded(3),
        };

        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        var typePrefix = new JsonlEventStore<ConformanceEvent>(typePrefixRoot.Root, TypePrefix());
        for (var i = 0; i < ids.Length; i++)
        {
            await idOnly.AppendAsync(ids[i], 0, [events[i]]);
            await typePrefix.AppendAsync(ids[i], 0, [events[i]]);
        }

        // IdOnly: the type is not in the name, so the FIRST event must be read — one open per stream.
        var openedBeforeIdOnly = idOnly.FilesOpened;
        var idOnlyIds = await idOnly.ListAggregateIdsAsync(nameof(SSnapshotAdded));
        await Assert.That(idOnly.FilesOpened - openedBeforeIdOnly).IsEqualTo(3L);
        await Assert.That(SameIds(idOnlyIds, ids)).IsTrue();

        // TypePrefix: a COLD instance whose construction pass learns the map from the names — and the
        // enumeration that follows opens NOTHING.
        var reader = new JsonlEventStore<ConformanceEvent>(typePrefixRoot.Root, TypePrefix());
        await Assert.That(reader.FilesOpened).IsEqualTo(0L);

        var openedBeforePrefix = reader.FilesOpened;
        var prefixIds = await reader.ListAggregateIdsAsync(nameof(SSnapshotAdded));
        await Assert.That(reader.FilesOpened - openedBeforePrefix).IsEqualTo(0L);

        // The ids come back exactly: an empty set would satisfy "zero opens" too, so the set is
        // asserted on its own — and against the IdOnly answer, which must be the same set.
        await Assert.That(prefixIds.Count).IsEqualTo(3);
        await Assert.That(SameIds(prefixIds, ids)).IsTrue();
        await Assert.That(SameIds(prefixIds, idOnlyIds)).IsTrue();

        // ... and the names on disk are the TypePrefix rule, not the bare "<guid>.jsonl" of IdOnly.
        var names = Directory.GetFiles(typePrefixRoot.Root).Select(Path.GetFileName).ToArray();
        await Assert.That(names.Length).IsEqualTo(3);
        foreach (var id in ids)
            await Assert.That(names).Contains($"{nameof(SSnapshotAdded)}-{id:D}.jsonl");
    }

    // ── G3: the read path is format-independent ────────────────────────────────────────────────

    /// <summary>
    /// The format changes ONLY which path a member resolves. Version, cursor position and peek answer
    /// the same for the same events in either layout — plus the one documented deviation: an id whose
    /// first line is corrupt is still enumerated under <see cref="FileNameFormat.TypePrefix"/> (its
    /// NAME is valid, and no file is opened) and not under <see cref="FileNameFormat.IdOnly"/>.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G3_BothFormats_SameReadSemantics(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var idOnlyRoot = new TempRoot();
        using var typePrefixRoot = new TempRoot();
        var id = Guid.NewGuid();
        IReadOnlyList<IDomainEvent> appended =
        [
            new SSnapshotAdded(1),
            new SSnapshotAdded(2),
            new SSnapshotAdded(3),
            new SSnapshotAdded(4),
            new SSnapshotAdded(5),
            new SSnapshotAdded(6),
        ];

        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        var typePrefix = new JsonlEventStore<ConformanceEvent>(typePrefixRoot.Root, TypePrefix());
        await idOnly.AppendAsync(id, 0, appended);
        await typePrefix.AppendAsync(id, 0, appended);

        // The version: a line count in both layouts.
        var idOnlyVersion = await idOnly.GetVersionAsync(id);
        var prefixVersion = await typePrefix.GetVersionAsync(id);
        await Assert.That(idOnlyVersion).IsEqualTo(6UL);
        await Assert.That(prefixVersion).IsEqualTo(idOnlyVersion);

        // The tail rebuild: OpenAsync(End - 3) yields the same three events with the same End. The
        // cursor positions off a byte OFFSET the scan recorded, never a per-line re-read.
        var (idOnlyEnd, idOnlyTail) = await DrainAsync(idOnly, id, idOnlyVersion - 2);
        var (prefixEnd, prefixTail) = await DrainAsync(typePrefix, id, prefixVersion - 2);
        await Assert.That(prefixEnd).IsEqualTo(idOnlyEnd);
        await Assert.That(idOnlyTail.Count).IsEqualTo(3);
        await Assert.That(prefixTail.Count).IsEqualTo(idOnlyTail.Count);
        await Assert.That(prefixTail.SequenceEqual(idOnlyTail)).IsTrue();

        // The peek: the first event, identical in both.
        await Assert
            .That(await typePrefix.PeekFirstAsync(id))
            .IsEqualTo(await idOnly.PeekFirstAsync(id));

        // An unknown id is an empty answer in both — and version 0 stays an ARGUMENT error in both,
        // because the 1-based version check happens BEFORE any path is resolved.
        var unknown = Guid.NewGuid();
        await Assert
            .That(async () => await idOnly.OpenAsync(unknown, 0))
            .Throws<ArgumentOutOfRangeException>();
        await Assert
            .That(async () => await typePrefix.OpenAsync(unknown, 0))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(await idOnly.GetVersionAsync(unknown)).IsEqualTo(0UL);
        await Assert.That(await typePrefix.GetVersionAsync(unknown)).IsEqualTo(0UL);
        await Assert.That(await typePrefix.PeekFirstAsync(unknown)).IsNull();
        await Assert.That(await typePrefix.LoadAsync(unknown)).IsEmpty();

        // The documented deviation: a corrupt FIRST line. TypePrefix knows the id from the NAME (zero
        // opens); IdOnly cannot classify the aggregate and must not guess.
        using var corruptIdOnlyRoot = new TempRoot();
        using var corruptPrefixRoot = new TempRoot();
        var corruptId = Guid.NewGuid();
        const string corrupt = "~~~~ not an event ~~~\n";
        await File.WriteAllTextAsync(
            Path.Combine(corruptIdOnlyRoot.Root, corruptId.ToString("D") + ".jsonl"),
            corrupt,
            cancellationToken
        );
        await File.WriteAllTextAsync(
            Path.Combine(corruptPrefixRoot.Root, $"{nameof(SSnapshotAdded)}-{corruptId:D}.jsonl"),
            corrupt,
            cancellationToken
        );

        var corruptPrefix = new JsonlEventStore<ConformanceEvent>(
            corruptPrefixRoot.Root,
            TypePrefix()
        );
        var corruptIdOnly = new JsonlEventStore<ConformanceEvent>(corruptIdOnlyRoot.Root);

        var openedBeforeCorrupt = corruptPrefix.FilesOpened;
        await Assert
            .That(await corruptPrefix.ListAggregateIdsAsync(nameof(SSnapshotAdded)))
            .Contains(corruptId);
        await Assert.That(corruptPrefix.FilesOpened - openedBeforeCorrupt).IsEqualTo(0L);
        await Assert
            .That(await corruptIdOnly.ListAggregateIdsAsync(nameof(SSnapshotAdded)))
            .IsEmpty();
    }

    // ── G4: snapshots are keyed by id, never by the stream's name ───────────────────────────────

    /// <summary>
    /// A snapshot's key is the AGGREGATE ID: <c>&lt;id&gt;.snap</c> in both formats. That is what lets a
    /// snapshot survive its stream (archived, deleted, or never written at all) and what keeps the
    /// <c>PASJ</c> container format-independent.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G4_Snapshot_Works_WithoutStream_And_AcrossInstances(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var snapshot = new ActorSnapshot(7, payload);

        var writer = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await Assert.That(await writer.LoadSnapshotAsync(id)).IsNull();

        // No stream for this id — the snapshot is written anyway, and no stream file appears.
        await writer.SaveSnapshotAsync(id, snapshot);
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);

        var snapshotPath = Path.Combine(temp.Root + "-snapshots", id.ToString("D") + ".snap");
        await Assert.That(File.Exists(snapshotPath)).IsTrue();
        var bytes = await File.ReadAllBytesAsync(snapshotPath, cancellationToken);

        // The container is unchanged: PASJ magic, little-endian header, raw payload — byte for byte.
        await Assert.That(Encoding.ASCII.GetString(bytes, 0, 4)).IsEqualTo("PASJ");
        await Assert
            .That(bytes.SequenceEqual(JsonlSnapshotFile.Encode(snapshot.Version, payload)))
            .IsTrue();

        // A NEW instance (not the writer) reads it back — and needs no stream map to do it.
        var reader = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await Assert.That(reader.FilesOpened).IsEqualTo(0L);
        var loaded = await reader.LoadSnapshotAsync(id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Version).IsEqualTo(7UL);
        await Assert.That(loaded.Payload.SequenceEqual(payload)).IsTrue();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────────

    private static JsonlEventStoreOptions TypePrefix() =>
        new() { FileNameFormat = FileNameFormat.TypePrefix };

    /// <summary>Set equality, order-independent: an enumeration may yield ids in directory order.</summary>
    private static bool SameIds(IReadOnlyList<Guid> actual, IReadOnlyList<Guid> expected) =>
        actual.Count == expected.Count
        && actual.OrderBy(id => id).SequenceEqual(expected.OrderBy(id => id));

    /// <summary>Open a cursor at <paramref name="fromVersion"/> and drain it, keeping <c>End</c>.</summary>
    private static async Task<(ulong End, List<IDomainEvent> Events)> DrainAsync(
        JsonlEventStore<ConformanceEvent> store,
        Guid id,
        ulong fromVersion
    )
    {
        var cursor = await store.OpenAsync(id, fromVersion);
        var events = new List<IDomainEvent>();
        while (await cursor.MoveNextAsync())
            events.Add(cursor.Current);

        var end = cursor.End;
        await cursor.DisposeAsync();
        return (end, events);
    }
}
