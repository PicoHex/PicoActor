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

        // The enumeration IS a classification pass in BOTH formats — that pass is where a foreign name,
        // an unparseable id, the other layout and a whole-directory misconfiguration are diagnosed — so
        // exactly ONE pass was paid here, and it had nothing to diagnose. Neither append paid the IdOnly
        // LAZY pass either: both resolved a stream that already existed, which is not a missing-stream
        // observation. That is the IdOnly cost model in one assertion: nothing at construction, nothing
        // per append, one pass at the first enumeration — or at the first miss, whichever comes first.
        await Assert.That(store.DirectoryScans).IsEqualTo(1L);
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

    // ── G2b: a stream of another type is silent, an unparseable id is reported ────────────────

    /// <summary>
    /// The two buckets a <see cref="FileNameFormat.TypePrefix"/> pass must NOT confuse. A stream of
    /// ANOTHER type is the normal case for a store directory — many aggregate types live side by side —
    /// so it is neither counted nor logged. A name in the configured SHAPE whose id does not parse is a
    /// skipped file: counted once, warned about once, and unrelated to the other layout.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G2b_Buckets_OtherType_And_UnparseableId(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var id = Guid.NewGuid();
        var otherTypeId = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions
            {
                FileNameFormat = FileNameFormat.TypePrefix,
                Logger = logger,
            }
        );
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // 1. A configured-format stream of ANOTHER type: a valid name, a type this query did not ask for.
        var otherTypeName = $"OtherType-{otherTypeId:D}.jsonl";
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, otherTypeName),
            "{\"$type\":\"ss-added\",\"Delta\":9}\n",
            cancellationToken
        );

        // 2. The configured SHAPE with an id that is not a D GUID: a skipped file name.
        const string unparseableName = "SSnapshotAdded-notaguid.jsonl";
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, unparseableName),
            "{\"$type\":\"ss-added\",\"Delta\":8}\n",
            cancellationToken
        );

        var skippedBefore = store.SkippedFileNames;
        var foreignBefore = store.ForeignFormatFiles;

        var ids = await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));

        // The configured stream is the only id: the other type belongs to its own query, and the
        // unparseable name names nothing at all.
        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(id);

        // ONE skipped name — the unparseable one. A stream of another type is NOT a skipped file, and
        // it is not the other LAYOUT either: it is the configured format, correctly ignored.
        await Assert.That(store.SkippedFileNames - skippedBefore).IsEqualTo(1L);
        await Assert.That(store.ForeignFormatFiles - foreignBefore).IsEqualTo(0L);

        // The other-type name appears NOWHERE in the log (a directory of one format is not a problem),
        // and the unparseable one appears exactly once — one warning per skipped NAME, not per pass.
        await Assert
            .That(
                logger.Messages.Any(message =>
                    message.Contains(otherTypeName, StringComparison.Ordinal)
                )
            )
            .IsFalse();
        await Assert
            .That(
                logger.Messages.Count(message =>
                    message.Contains(unparseableName, StringComparison.Ordinal)
                )
            )
            .IsEqualTo(1);
    }

    // ── G5: a mixed directory is diagnosed, never merged ──────────────────────────────────────

    /// <summary>
    /// The same aggregate present in BOTH layouts, non-empty in the other one: an append must refuse and
    /// name both paths. Continuing would write a second history for one id, and whichever file a later
    /// reader opened would define the truth.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G5a_OtherFormatNonEmpty_AppendThrows(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // The other layout's file for the same aggregate. Its content is never parsed here — the NAME is
        // the whole fact this guard acts on.
        var streamPath = Path.Combine(temp.Root, $"{nameof(SSnapshotAdded)}-{id:D}.jsonl");
        var otherPath = Path.Combine(temp.Root, id.ToString("D") + ".jsonl");
        await File.WriteAllTextAsync(
            otherPath,
            "{\"$type\":\"ss-added\",\"Delta\":9}\n",
            cancellationToken
        );

        // ONE enumeration is what makes the store treat this directory as mixed.
        var foreignBefore = store.ForeignFormatFiles;
        await Assert.That(await store.ListAggregateIdsAsync(nameof(SSnapshotAdded))).Contains(id);
        await Assert.That(store.ForeignFormatFiles - foreignBefore).IsEqualTo(1L);

        var error = await CaptureAppendErrorAsync(store, id, 1, new SSnapshotAdded(2));

        await Assert.That(error).IsNotNull();
        await Assert.That(error is InvalidOperationException).IsTrue();

        // Both paths and the id are named: an operator acts on the message without guessing which file is
        // which, and a support ticket carries the aggregate.
        await Assert
            .That(error!.Message.Contains(id.ToString("D"), StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(error.Message.Contains(Path.GetFileName(streamPath), StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(error.Message.Contains(Path.GetFileName(otherPath), StringComparison.Ordinal))
            .IsTrue();

        // Refused BEFORE a byte was written: the configured stream is untouched.
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(1UL);
    }

    /// <summary>
    /// The same shape with a ZERO-LENGTH other-format file. rev 5 lets a failed append leave an empty file
    /// behind, so an empty file is not a stream and must never block a good append — and it must not be
    /// "repaired" either: its bytes stay exactly as they were.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G5b_OtherFormatZeroLength_AppendSucceeds(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        var otherPath = Path.Combine(temp.Root, id.ToString("D") + ".jsonl");
        await File.WriteAllBytesAsync(otherPath, [], cancellationToken);

        var foreignBefore = store.ForeignFormatFiles;
        await Assert.That(await store.ListAggregateIdsAsync(nameof(SSnapshotAdded))).Contains(id);
        await Assert.That(store.ForeignFormatFiles - foreignBefore).IsEqualTo(1L);

        // The guard is `FileInfo { Exists: true, Length: > 0 }`, never `File.Exists`: the empty file is
        // ignored and the append goes through.
        await Assert.That(await store.AppendAsync(id, 1, [new SSnapshotAdded(2)])).IsEqualTo(2UL);

        // ... and the empty file is still there, still empty — the append never touches the other layout.
        await Assert
            .That((await File.ReadAllBytesAsync(otherPath, cancellationToken)).Length)
            .IsEqualTo(0);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(2UL);
    }

    /// <summary>
    /// A short foreign name is not a crash and not a stream: <c>x.txt</c> is SHORTER than the
    /// <c>.jsonl</c> suffix, so a scan that sliced first would throw ArgumentOutOfRangeException while
    /// booting. It is counted as skipped, warned about, and the enumeration still answers.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G5c_ShortForeignName_CountedSkipped_NoThrow(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions
            {
                FileNameFormat = FileNameFormat.TypePrefix,
                Logger = logger,
            }
        );
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, "x.txt"),
            "not a stream",
            cancellationToken
        );

        var skippedBefore = store.SkippedFileNames;
        var ids = await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));

        await Assert.That(ids).Contains(id);
        await Assert.That(store.SkippedFileNames - skippedBefore).IsEqualTo(1L);
        await Assert
            .That(
                logger.Messages.Any(message => message.Contains("x.txt", StringComparison.Ordinal))
            )
            .IsTrue();
    }

    /// <summary>
    /// ONE warning per SCAN for the other layout, not one per file: a half-migrated directory holds
    /// thousands of them, and a warning storm is how an operator learns to ignore warnings. The message
    /// has to say what the operator's choice is, too.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G5d_ScanReportsOneWarning(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // Two other-format files: two counted entries, but ONE report for the pass.
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, $"{nameof(SSnapshotAdded)}-{Guid.NewGuid():D}.jsonl"),
            "{}\n",
            cancellationToken
        );
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, $"Other-{Guid.NewGuid():D}.jsonl"),
            "{}\n",
            cancellationToken
        );

        // IdOnly performs no scan at construction, but the append above was this store's FIRST
        // observation of a stream it did not have — the one lazy pass it ever pays, and it found nothing
        // to diagnose (the directory was empty when it ran). The enumeration below is therefore the
        // SECOND and last pass, which is what its delta asserts.
        await Assert.That(store.DirectoryScans).IsEqualTo(1L);
        var scansBefore = store.DirectoryScans;
        var foreignBefore = store.ForeignFormatFiles;
        var ids = await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));

        await Assert.That(ids).Contains(id);
        await Assert.That(store.DirectoryScans - scansBefore).IsEqualTo(1L);
        await Assert.That(store.ForeignFormatFiles - foreignBefore).IsEqualTo(2L);
        await Assert
            .That(
                logger.Messages.Count(message =>
                    message.Contains("use the other naming format", StringComparison.Ordinal)
                )
            )
            .IsEqualTo(1);
    }

    /// <summary>
    /// ONE id, several prefixes: <c>A-&lt;id&gt;.jsonl</c> and <c>B-&lt;id&gt;.jsonl</c> are two histories
    /// of the same aggregate, and a stream's prefix is fixed when it is created. An append cannot pick
    /// one, so it refuses and lists the candidates — in BOTH directions, because the prefix that is
    /// "foreign" depends only on which layout the store was configured with.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G5e_DuplicatePrefixes_Throws(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var id = Guid.NewGuid();

        // ── IdOnly: the prefixed names are the OTHER layout, so the guard runs once a scan has seen one
        //    (the monotonic ForeignFormatFiles gate) and gathers the candidates from one pass.
        using var idOnlyRoot = new TempRoot();
        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        await idOnly.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        var candidateA = Path.Combine(idOnlyRoot.Root, $"A-{id:D}.jsonl");
        var candidateB = Path.Combine(idOnlyRoot.Root, $"B-{id:D}.jsonl");
        await File.WriteAllTextAsync(
            candidateA,
            "{\"$type\":\"ss-added\",\"Delta\":7}\n",
            cancellationToken
        );
        await File.WriteAllTextAsync(
            candidateB,
            "{\"$type\":\"ss-added\",\"Delta\":8}\n",
            cancellationToken
        );

        await Assert.That(await idOnly.ListAggregateIdsAsync(nameof(SSnapshotAdded))).Contains(id);
        await Assert.That(idOnly.ForeignFormatFiles).IsEqualTo(2L);

        var idOnlyError = await CaptureAppendErrorAsync(idOnly, id, 1, new SSnapshotAdded(2));
        await Assert.That(idOnlyError is InvalidOperationException).IsTrue();
        await Assert
            .That(
                idOnlyError!.Message.Contains(
                    Path.GetFileName(candidateA),
                    StringComparison.Ordinal
                )
            )
            .IsTrue();
        await Assert
            .That(
                idOnlyError.Message.Contains(Path.GetFileName(candidateB), StringComparison.Ordinal)
            )
            .IsTrue();

        // Nothing was written: the append was refused before the stream was opened.
        await Assert.That(await idOnly.GetVersionAsync(id)).IsEqualTo(1UL);

        // ── TypePrefix: both names are configured-format streams, and the classification pass is what
        //    notices that ONE id has TWO of them.
        using var prefixRoot = new TempRoot();
        var prefixId = Guid.NewGuid();
        var prefixA = Path.Combine(prefixRoot.Root, $"A-{prefixId:D}.jsonl");
        var prefixB = Path.Combine(prefixRoot.Root, $"B-{prefixId:D}.jsonl");
        await File.WriteAllTextAsync(
            prefixA,
            "{\"$type\":\"ss-added\",\"Delta\":7}\n",
            cancellationToken
        );
        await File.WriteAllTextAsync(
            prefixB,
            "{\"$type\":\"ss-added\",\"Delta\":8}\n",
            cancellationToken
        );

        var prefix = new JsonlEventStore<ConformanceEvent>(prefixRoot.Root, TypePrefix());
        var prefixError = await CaptureAppendErrorAsync(prefix, prefixId, 0, new SSnapshotAdded(2));

        await Assert.That(prefixError is InvalidOperationException).IsTrue();
        await Assert
            .That(
                prefixError!.Message.Contains(Path.GetFileName(prefixA), StringComparison.Ordinal)
            )
            .IsTrue();
        await Assert
            .That(prefixError.Message.Contains(Path.GetFileName(prefixB), StringComparison.Ordinal))
            .IsTrue();

        // No third name appeared: the store never invents a prefix for a stream it refused to extend.
        await Assert.That(Directory.GetFiles(prefixRoot.Root).Length).IsEqualTo(2);
    }

    /// <summary>
    /// A whole directory in the OTHER layout is a misconfiguration, and the loud answer is the only one
    /// that reaches an operator: an empty enumeration would look like "no aggregates yet" and every
    /// stream would be invisible. The message names both formats, both counts and the two ways out.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G6_TypePrefixOnlyDirectory_WithIdOnlyOptions_Throws(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var typePrefixOnly = new TempRoot();
        var id = Guid.NewGuid();
        await File.WriteAllTextAsync(
            Path.Combine(typePrefixOnly.Root, $"{nameof(SSnapshotAdded)}-{id:D}.jsonl"),
            "{\"$type\":\"ss-added\",\"Delta\":1}\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(typePrefixOnly.Root);
        await Assert.That(store.DirectoryScans).IsEqualTo(0L);

        var error = await CaptureAsync(async () =>
        {
            await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));
        });

        await Assert.That(error is InvalidOperationException).IsTrue();
        var message = error!.Message;

        // Both counts...
        await Assert.That(message.Contains("configured: 0", StringComparison.Ordinal)).IsTrue();
        await Assert.That(message.Contains("other-format: 1", StringComparison.Ordinal)).IsTrue();

        // ... and both formats, spelled so the operator knows which one to migrate to.
        await Assert
            .That(message.Contains("IdOnly (<id>.jsonl)", StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(message.Contains("TypePrefix (<type>-<id>.jsonl)", StringComparison.Ordinal))
            .IsTrue();

        // ... plus the remedy.
        await Assert
            .That(message.Contains("Migrate the directory or set", StringComparison.Ordinal))
            .IsTrue();

        // The rule holds for ANY scan, including the one a TypePrefix store performs at CONSTRUCTION to
        // learn its map: the mirror case must not start up quietly either.
        using var idOnlyOnly = new TempRoot();
        await File.WriteAllTextAsync(
            Path.Combine(idOnlyOnly.Root, id.ToString("D") + ".jsonl"),
            "{\"$type\":\"ss-added\",\"Delta\":1}\n",
            cancellationToken
        );
        await Assert
            .That(() => new JsonlEventStore<ConformanceEvent>(idOnlyOnly.Root, TypePrefix()))
            .Throws<InvalidOperationException>();
    }

    /// <summary>
    /// The format decides what a renamed EVENT TYPE costs. IdOnly classifies an aggregate by its first
    /// event's CONTENT, so a stream whose type was renamed is still found; TypePrefix classifies by the
    /// NAME, so the same stream answers a different query. That is why the spec allows a TypePrefix
    /// adopter to skip the rename migration — and why an IdOnly adopter still owes one.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G7_TypeRename_IsFormatDependent(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var prefixRoot = new TempRoot();
        using var idOnlyRoot = new TempRoot();
        var id = Guid.NewGuid();

        // The SAME content in both layouts: one SSnapshotAdded, appended by the store itself so the line
        // is exactly what the serializer writes. The TypePrefix file is then RENAMED to the retired name.
        var prefixWriter = new JsonlEventStore<ConformanceEvent>(prefixRoot.Root, TypePrefix());
        await prefixWriter.AppendAsync(id, 0, [new SSnapshotAdded(1)]);
        File.Move(
            Path.Combine(prefixRoot.Root, $"{nameof(SSnapshotAdded)}-{id:D}.jsonl"),
            Path.Combine(prefixRoot.Root, $"RetiredType-{id:D}.jsonl")
        );

        var idOnlyWriter = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        await idOnlyWriter.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // IdOnly goes by CONTENT: the first event is an SSnapshotAdded whatever the file is called.
        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        await Assert.That(await idOnly.ListAggregateIdsAsync(nameof(SSnapshotAdded))).Contains(id);

        // TypePrefix goes by NAME: the retired prefix is a valid stream of ANOTHER type — enumerated by
        // the query that asks for it, invisible to the one that does not, and never a diagnostic.
        var prefix = new JsonlEventStore<ConformanceEvent>(prefixRoot.Root, TypePrefix());
        await Assert.That(await prefix.ListAggregateIdsAsync(nameof(SSnapshotAdded))).IsEmpty();
        await Assert.That(await prefix.ListAggregateIdsAsync("RetiredType")).Contains(id);
        await Assert.That(prefix.SkippedFileNames).IsEqualTo(0L);

        // The prefix is fixed at CREATION: an append still targets the retired name and never invents
        // "SSnapshotAdded-<id>.jsonl" beside it.
        await Assert.That(await prefix.AppendAsync(id, 1, [new SSnapshotAdded(2)])).IsEqualTo(2UL);

        var names = Directory.GetFiles(prefixRoot.Root).Select(Path.GetFileName).ToArray();
        await Assert.That(names.Length).IsEqualTo(1);
        await Assert.That(names[0]).IsEqualTo($"RetiredType-{id:D}.jsonl");
        await Assert.That(await prefix.GetVersionAsync(id)).IsEqualTo(2UL);
    }

    // ── G8: a host that never enumerates still gets the diagnosis ──────────────────────────────

    /// <summary>
    /// The one loud outcome available to a host that only loads and appends BY ID. IdOnly derives the
    /// path from the id alone, so without the lazy pass such a host sees "no stream" in a directory that
    /// is entirely in the OTHER layout and appends a SECOND history beside the file it cannot see. The
    /// pass runs at the first missing stream — a LOAD counts, not only an append — exactly once; a pass
    /// that threw is LATCHED, so the misconfiguration is raised again instead of decaying into an empty
    /// aggregate, and the directory is never re-scanned per operation.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G8_NoEnumerationHost_LazyPass_ThrowsOnMisconfiguration(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var existingId = Guid.NewGuid();
        var missingId = Guid.NewGuid();

        // A directory in the OTHER layout, written by the store itself so it is a real TypePrefix root
        // rather than a hand-made approximation of one.
        var writer = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await writer.AppendAsync(existingId, 0, [new SSnapshotAdded(1)]);

        // The host under test NEVER enumerates: no ListAggregateIdsAsync in this test on it.
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );

        // Nothing at construction: an IdOnly store does not look at its directory until it has to.
        await Assert.That(store.DirectoryScans).IsEqualTo(0L);

        // The FIRST observation is a LOAD of a stream that is not there — not an enumeration and not an
        // append. That is enough: the lazy pass runs here, counts itself BEFORE it throws, and throws.
        var first = await CaptureAsync(async () =>
        {
            await store.LoadAsync(existingId);
        });

        await Assert.That(first is InvalidOperationException).IsTrue();
        await Assert.That(store.DirectoryScans).IsEqualTo(1L);

        // ... and it says what is wrong — both counts — instead of answering "no events".
        await Assert
            .That(first!.Message.Contains("configured: 0", StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(first.Message.Contains("other-format: 1", StringComparison.Ordinal))
            .IsTrue();
        await Assert.That(logger.Contains("use the other naming format")).IsTrue();

        // A SECOND observation, through a DIFFERENT member, raises the same diagnosis: the latch holds
        // the FAILURE, not a silent "already scanned". The directory is not scanned a second time.
        var second = await CaptureAsync(async () =>
        {
            await store.GetVersionAsync(missingId);
        });

        await Assert.That(second is InvalidOperationException).IsTrue();
        await Assert
            .That(second!.Message.Contains("configured: 0", StringComparison.Ordinal))
            .IsTrue();
        await Assert.That(store.DirectoryScans).IsEqualTo(1L);

        // The APPEND path owes the same diagnosis: a fresh host whose first observation is a NEW id.
        var appender = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var appendError = await CaptureAppendErrorAsync(
            appender,
            missingId,
            0,
            new SSnapshotAdded(2)
        );

        await Assert.That(appendError is InvalidOperationException).IsTrue();
        await Assert.That(appender.DirectoryScans).IsEqualTo(1L);

        // ... and it refused BEFORE the write: no second stream was forked beside the file it cannot see,
        // and the other layout's history is exactly as it was.
        await Assert
            .That(File.Exists(Path.Combine(temp.Root, missingId.ToString("D") + ".jsonl")))
            .IsFalse();
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(1);
        await Assert.That(await writer.GetVersionAsync(existingId)).IsEqualTo(1UL);
    }

    // ── G9: an empty batch validates the version and creates nothing ────────────────────────────

    /// <summary>
    /// An empty batch is neither a bypass nor a write. It resolves its path the same way a writing append
    /// does (map, then the TypePrefix root pass), it validates the HELD stream's
    /// <c>expectedVersion</c>, and then it does nothing at all — no duplicate-guard pass, no file, no
    /// directory creation. The version check is the point: a producer that "appends nothing" against a
    /// stale version is refused exactly like one that appends an event.
    /// </summary>
    [Test, Timeout(30000)]
    public async Task G9_TypePrefix_EmptyBatch_ValidatesVersion_AndCreatesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var streamName = $"{nameof(SSnapshotAdded)}-{id:D}.jsonl";

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await store.AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2), new SSnapshotAdded(3)]
        );

        var names = Directory.GetFiles(temp.Root).Select(Path.GetFileName).ToArray();
        await Assert.That(names.Length).IsEqualTo(1);
        await Assert.That(names[0]).IsEqualTo(streamName);
        var scansBefore = store.DirectoryScans;

        // A STALE expectedVersion is refused: the empty batch still observes the stream's version.
        var stale = await CaptureAsync(async () =>
        {
            await store.AppendAsync(id, 0, []);
        });

        await Assert.That(stale is ConcurrencyException).IsTrue();
        var conflict = (ConcurrencyException)stale!;
        await Assert.That(conflict.ActorId).IsEqualTo(id);
        await Assert.That(conflict.ExpectedVersion).IsEqualTo(0UL);
        await Assert.That(conflict.ActualVersion).IsEqualTo(3UL);

        // A CURRENT one is a NO-OP that answers the version it observed, in the same format.
        await Assert.That(await store.AppendAsync(id, 3, [])).IsEqualTo(3UL);

        // No path resolution, no duplicate guard, no new file: the map hit answered both calls, and
        // nothing on disk moved — not the stream, and not the OTHER layout's name for the same id.
        await Assert.That(store.DirectoryScans).IsEqualTo(scansBefore);
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(1);
        await Assert
            .That(File.Exists(Path.Combine(temp.Root, id.ToString("D") + ".jsonl")))
            .IsFalse();
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(3UL);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────────

    private static JsonlEventStoreOptions TypePrefix() =>
        new() { FileNameFormat = FileNameFormat.TypePrefix };

    /// <summary>
    /// Run <paramref name="action"/> and hand back what it threw — null when it did not throw. The guards
    /// under test are asserted on their TYPE and their MESSAGE, so the exception is the value under test;
    /// a bare <c>Throws</c> would discard the text an operator has to read.
    /// </summary>
    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>An append that must be REFUSED with an <see cref="InvalidOperationException"/>.</summary>
    private static Task<Exception?> CaptureAppendErrorAsync(
        JsonlEventStore<ConformanceEvent> store,
        Guid id,
        ulong expectedVersion,
        IDomainEvent domainEvent
    ) =>
        CaptureAsync(async () =>
        {
            await store.AppendAsync(id, expectedVersion, [domainEvent]);
        });

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
