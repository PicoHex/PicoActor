namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Durable, append-only event store: one file per aggregate (<c>&lt;root&gt;/&lt;id:D&gt;.jsonl</c>),
/// one JSON object per line, snapshots as sidecars in <c>&lt;root&gt;-snapshots</c>.
/// <para>
/// Serialization is PicoJetson source generation over the CONSUMER's base type: every concrete event
/// must be registered with <c>[PicoDerivedType]</c> on <typeparamref name="TEventBase"/> in the
/// assembly that declares it. The generator ignores type parameters, so this package cannot register
/// anything on the consumer's behalf — an unregistered event fails on the first write, loudly.
/// </para>
/// <para>
/// Single-process: one store instance per directory. Two instances in one process do not share gates,
/// and a concurrent write to the same stream fails with an <see cref="IOException"/> (file sharing) —
/// that is the documented outcome, not something retried.
/// </para>
/// </summary>
public sealed class JsonlEventStore<TEventBase>
    : IEventStore,
        IEventLogReader,
        IEventStoreEnumerator,
        IEventStoreSnapshot
    where TEventBase : class, IDomainEvent
{
    // The name grammar lives in JsonlNames: one spelling of the suffix for the store and the
    // classifier both.
    private const string StreamSuffix = JsonlNames.StreamSuffix;
    private const string SnapshotSuffix = ".snap";
    private const int ScanBufferBytes = 64 * 1024;

    private readonly string _root;
    private readonly string _snapshotRoot;
    private readonly JsonlEventStoreOptions _options;
    private readonly ILogger? _logger;
    private readonly JsonlLockTable _locks;

    // The id → type map TypePrefix resolution needs, because IEventStore carries only the aggregate id.
    // Never consulted under IdOnly, where the name IS the id.
    private readonly ConcurrentDictionary<Guid, string> _typeById = new();

    private long _deserializedEvents;
    private long _tornTailLines;
    private long _skippedFileNames;
    private long _filesOpened;
    private long _directoryScans;
    private long _foreignFormatFiles;

    /// <summary>
    /// Create a store rooted at <paramref name="rootDirectory"/>. The directory is created on the first
    /// WRITE, never here — a store that only reads must not litter the file system.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="rootDirectory"/> is null, empty or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="JsonlEventStoreOptions.MaxAggregateLocks"/> is not positive.</exception>
    public JsonlEventStore(string rootDirectory, JsonlEventStoreOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);

        options ??= new JsonlEventStoreOptions();
        if (options.MaxAggregateLocks <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(JsonlEventStoreOptions.MaxAggregateLocks),
                options.MaxAggregateLocks,
                $"{nameof(JsonlEventStoreOptions.MaxAggregateLocks)} must be greater than zero."
            );

        // The trailing separator is normalized BEFORE deriving anything from the root, so "data/" and
        // "data" name the same stream and the same -snapshots sibling.
        _root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(rootDirectory));
        _snapshotRoot = _root + "-snapshots";
        _locks = new JsonlLockTable(options.MaxAggregateLocks);
        _options = options;
        _logger = options.Logger;

        // A TypePrefix store learns its map from ONE filename pass. IdOnly pays nothing here: the id
        // alone is the name, so there is nothing to learn.
        if (_options.FileNameFormat == FileNameFormat.TypePrefix)
            ScanTypeMap();
    }

    /// <summary>Events this store has DESERIALIZED (a load or a cursor move), not lines it has read.</summary>
    internal long DeserializedEventCount => Interlocked.Read(ref _deserializedEvents);

    /// <summary>Unterminated final lines seen and ignored — one per crash-torn append.</summary>
    internal long TornTailLines => Interlocked.Read(ref _tornTailLines);

    /// <summary>Directory entries skipped by <see cref="ListAggregateIdsAsync"/> because they are not streams.</summary>
    internal long SkippedFileNames => Interlocked.Read(ref _skippedFileNames);

    /// <summary>
    /// Stream files this store has OPENED for reading (a load, a cursor move, a count, a peek). Written
    /// in one place — the read path — so the counter measures what an enumeration actually costs: under
    /// <see cref="FileNameFormat.IdOnly"/> one open per stream, under
    /// <see cref="FileNameFormat.TypePrefix"/> none at all.
    /// </summary>
    internal long FilesOpened => Interlocked.Read(ref _filesOpened);

    /// <summary>
    /// Directory CLASSIFICATION passes this store has performed. A single-format directory on the
    /// historical layout pays none: <see cref="FileNameFormat.IdOnly"/> derives every path from the id
    /// and defers the one pass to the first observation of a MISSING stream. A
    /// <see cref="FileNameFormat.TypePrefix"/> store pays one at construction (to learn the
    /// <c>id → type</c> map) and one per resolved miss.
    /// </summary>
    internal long DirectoryScans => Interlocked.Read(ref _directoryScans);

    /// <summary>
    /// Directory entries this store has classified as a stream in the OTHER naming format. Non-zero in a
    /// mixed directory; non-zero with ZERO configured-format streams means the store is pointed at a
    /// directory of the other layout, which is a misconfiguration and not an empty store.
    /// </summary>
    internal long ForeignFormatFiles => Interlocked.Read(ref _foreignFormatFiles);

    /// <summary>Live per-aggregate gate entries.</summary>
    internal int LockEntryCount => _locks.Count;

    /// <inheritdoc/>
    /// <remarks>
    /// The ORDER is the contract: read-only witness, version, torn-tail truncate, ONE write, flush,
    /// cache update. A rejected append never changes the stream's content (a missing stream may be left
    /// as a zero-length file, which enumeration skips), and the cache is what makes the steady state
    /// O(1) per append instead of a rescan.
    /// </remarks>
    public async ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        ArgumentNullException.ThrowIfNull(events);

        var (path, newType) = AppendPathFor(actorId, events);
        var entry = _locks.Acquire(actorId);
        await entry.Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            // 1. An empty batch is an append that writes nothing, NOT a bypass: the read-only witness
            //    and the version check run before it becomes a no-op.
            if (events.Count == 0)
                return EmptyAppend(actorId, expectedVersion, path, entry);

            // 2. Every element must be the declared base type, and the batch is encoded as ONE buffer
            //    before anything reaches the disk — a serialization failure creates no file at all.
            //
            //    A non-empty batch always resolves a path: an existing stream, or a NEW name formed
            //    from events[0]'s type, which is known before the path ever is. Re-stating it as
            //    non-null here keeps every later use (the encode message, the handle, the scans, the
            //    reports) honest without scattering null-forgiving operators.
            var streamPath = path!;
            var payload = Encode(events, streamPath);

            Directory.CreateDirectory(_root);

            await using var stream = new FileStream(
                streamPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read
            );

            // 4. Length witness: a length the cache did not count at means somebody else wrote here.
            //    Refuse unconditionally and re-seed — never merge two histories.
            var length = stream.Length;
            var scanReportedTornTail = false;
            if (!entry.Seeded)
            {
                var seed = ScanStream(stream, fromVersion: 1, streamPath);
                entry.Seed(seed.Count, length);
                scanReportedTornTail = seed.TornTail;
            }
            else if (entry.Length != length)
            {
                var observed = ScanStream(stream, fromVersion: 1, streamPath);
                entry.Seed(observed.Count, length);
                throw new ConcurrencyException(actorId, expectedVersion, entry.Count);
            }

            // 5. Optimistic concurrency against the counted version.
            if (entry.Count != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, entry.Count);

            // 6. Torn-tail repair: truncate the unterminated fragment, never terminate it. Terminating
            //    would publish half an event as if it had been committed. The report is part of the
            //    repair, not of the SCAN: this branch is reached with an already-SEEDED cache (a
            //    refused append seeded the length of the torn file), so no scan here saw the fragment
            //    and a silent truncation would drop bytes with no warning and no counter — the one
            //    thing this store's corruption policy never does. When a scan in THIS call already
            //    reported it, that report stands and this one is not duplicated.
            if (length > 0 && !EndsWithNewline(stream, length))
            {
                if (!scanReportedTornTail)
                    ReportTornTail(streamPath);

                stream.SetLength(FindLastNewline(stream, length));
            }

            // 7. ONE write over the encoded bytes, then a flush the append contract can wait for.
            stream.Seek(0, SeekOrigin.End);
            await stream.WriteAsync(payload, CancellationToken.None).ConfigureAwait(false);
            await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            if (_options.FlushToDisk)
                stream.Flush(flushToDisk: true);

            // 8. The cache describes the file as it now is.
            var version = entry.Count + (ulong)events.Count;
            entry.Seed(version, stream.Length);

            // The prefix is fixed at CREATION: the map learns it once the bytes are on disk, so an id
            // that is later recreated keeps pointing at the file its first event named.
            if (newType is not null)
                _typeById.TryAdd(actorId, newType);

            return version;
        }
        catch (ConcurrencyException)
        {
            // Content untouched: a witness mismatch has already re-seeded the cache, and a plain
            // version mismatch leaves a valid observation alone.
            throw;
        }
        catch
        {
            // A failed write may have flushed partial bytes, so the cache no longer describes the file.
            entry.Invalidate();
            throw;
        }
        finally
        {
            entry.Gate.Release();
            _locks.Release(entry);
        }
    }

    /// <summary>
    /// The empty-batch path: a READ-ONLY witness (no file is created, no torn tail is repaired), then
    /// the version check, then nothing. <c>InMemoryEventStore</c> behaves the same way.
    /// </summary>
    private ulong EmptyAppend(
        Guid actorId,
        ulong expectedVersion,
        string? path,
        JsonlLockEntry entry
    )
    {
        var (count, length) = Inspect(path);

        if (!entry.Seeded)
            entry.Seed(count, length);
        else if (entry.Length != length)
        {
            entry.Seed(count, length);
            throw new ConcurrencyException(actorId, expectedVersion, count);
        }

        if (entry.Count != expectedVersion)
            throw new ConcurrencyException(actorId, expectedVersion, entry.Count);

        return entry.Count;
    }

    /// <summary>Observe a stream WITHOUT creating or repairing anything: (count, length), 0/0 when absent.</summary>
    private (ulong Count, long Length) Inspect(string? path)
    {
        // A null path is "this format has no stream for the id" — the same observation as a missing
        // file, and it must not create one.
        if (path is null || !File.Exists(path))
            return (0UL, 0L);

        using var stream = OpenRead(path);
        var length = stream.Length;
        return (ScanStream(stream, fromVersion: 1, path).Count, length);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Fully materialized on purpose: the contract permits a lazy view, and this store chooses eager so
    /// a caller may index the list after the store's handle is closed. A rebuild that must not pay for
    /// the head uses <see cref="OpenAsync"/> instead.
    /// </remarks>
    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        var path = PathFor(actorId);
        if (path is null || !File.Exists(path))
            return Array.Empty<IDomainEvent>();

        var events = new List<IDomainEvent>();
        await using var stream = OpenRead(path);
        var scanner = new JsonlLineScanner(stream);

        while (await scanner.MoveNextAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (!scanner.CurrentTerminated)
            {
                ReportTornTail(path);
                break;
            }

            var line = scanner.Current;
            if (JsonlLines.IsBlank(line))
                continue;

            events.Add(DeserializeLine(line, path));
        }

        return events;
    }

    /// <inheritdoc/>
    public async ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    {
        var path = PathFor(actorId);
        if (path is null || !File.Exists(path))
            return null;

        await using var stream = OpenRead(path);
        var scanner = new JsonlLineScanner(stream);

        while (await scanner.MoveNextAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (!scanner.CurrentTerminated)
                return null;

            var line = scanner.Current;
            if (JsonlLines.IsBlank(line))
                continue;

            try
            {
                return DeserializeLine(line, path);
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException)
            {
                // The IEventStore contract for this member is "null when the stream yields no
                // deserializable event": an unclassifiable FIRST line does not abort boot. The loader
                // is where corruption is loud.
                _logger?.Warning(
                    $"The first line of '{path}' is not a deserializable event: {ex.Message}"
                );
                return null;
            }
        }

        return null;
    }

    /// <inheritdoc/>
    ValueTask<IDomainEvent?> IEventLogReader.PeekFirstAsync(Guid actorId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return PeekFirstAsync(actorId);
    }

    /// <inheritdoc/>
    public ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var path = PathFor(actorId);
        if (path is null || !File.Exists(path))
            return new ValueTask<ulong>(0UL);

        using var stream = OpenRead(path);
        return new ValueTask<ulong>(ScanStream(stream, fromVersion: 1, path).Count);
    }

    /// <inheritdoc/>
    public ValueTask<IEventCursor> OpenAsync(
        Guid actorId,
        ulong fromVersion,
        CancellationToken ct = default
    )
    {
        // Version 0 does not exist: versions are 1-based.
        if (fromVersion == 0)
            throw new ArgumentOutOfRangeException(
                nameof(fromVersion),
                fromVersion,
                "fromVersion is 1-based."
            );

        ct.ThrowIfCancellationRequested();

        // The path is resolved AFTER the version check: fromVersion == 0 is an argument error in both
        // formats, whether or not the aggregate exists.
        var path = PathFor(actorId);
        if (path is null || !File.Exists(path))
            return new ValueTask<IEventCursor>(JsonlCursor.Empty(fromVersion));

        var stream = OpenRead(path);
        try
        {
            // ONE pass records both the length (End) and where fromVersion starts, so a tail rebuild
            // reads the head as bytes only — it is never deserialized.
            var scan = ScanStream(stream, fromVersion, path);
            stream.Position = scan.Offset;

            // Clamped BEFORE the subtraction: past the end is a legal empty cursor (the rollback path
            // opens at snapshot.Version + 1) whose End is still the real length.
            var capacity = fromVersion > scan.Count ? 0 : scan.Count - fromVersion + 1;

            return new ValueTask<IEventCursor>(
                new JsonlCursor(
                    stream,
                    scan.Count,
                    capacity,
                    fromVersion,
                    line => DeserializeLine(line, path),
                    () => ReportTornTail(path)
                )
            );
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// NEVER throws for a snapshot it cannot use — a missing file, a bad magic, a truncated header, a
    /// version of 0 and a payload-length mismatch all mean "no snapshot". The event stream is
    /// authoritative, and the only other answer available here is a full replay. The payload is a FRESH
    /// array on every call (the contract permits handing out the stored one; copying means callers need
    /// no ownership rules). The version is NOT validated against the stream: a snapshot newer than the
    /// events is returned unchanged so the runtime's rollback guard can discard it.
    /// </remarks>
    public ValueTask<ActorSnapshot?> LoadSnapshotAsync(Guid actorId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var path = SnapshotPathFor(actorId);
        if (!File.Exists(path))
            return new ValueTask<ActorSnapshot?>((ActorSnapshot?)null);

        byte[] file;
        try
        {
            file = File.ReadAllBytes(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Same answer as an unreadable snapshot: accelerate nothing, replay the stream.
            _logger?.Warning($"Ignoring unreadable snapshot '{path}': {ex.Message}");
            return new ValueTask<ActorSnapshot?>((ActorSnapshot?)null);
        }

        return new ValueTask<ActorSnapshot?>(
            JsonlSnapshotFile.TryDecode(file, out var version, out var payload)
                ? new ActorSnapshot(version, payload)
                : null
        );
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The payload is COPIED before this <see cref="ValueTask"/> completes: a producer is allowed to
    /// serialize into a buffer it reuses, and keeping the reference would let the next snapshot rewrite
    /// the file this one just wrote.
    /// </remarks>
    public ValueTask SaveSnapshotAsync(
        Guid actorId,
        ActorSnapshot snapshot,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ct.ThrowIfCancellationRequested();

        var bytes = JsonlSnapshotFile.Encode(snapshot.Version, snapshot.Payload);
        JsonlSnapshotFile.WriteAtomic(SnapshotPathFor(actorId), bytes);
        return default;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Under <see cref="FileNameFormat.IdOnly"/> the type is not in the name, so every stream is OPENED
    /// once to read its first event. Under <see cref="FileNameFormat.TypePrefix"/> the type IS the name:
    /// the ids come from <see cref="JsonlNames.Classify"/> alone, so the enumeration opens NOTHING.
    /// <para>
    /// One deviation follows from that, and it is deliberate: a stream whose first LINE is corrupt is
    /// enumerated under TypePrefix (its name is a valid stream name) and not under IdOnly (which has no
    /// other way to learn the type). The runtime peeks every id it is given, so recovery still filters
    /// it out.
    /// </para>
    /// </remarks>
    public async ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType)
    {
        ArgumentNullException.ThrowIfNull(firstEventType);

        var result = new List<Guid>();
        if (!Directory.Exists(_root))
            return result;

        if (_options.FileNameFormat == FileNameFormat.TypePrefix)
            return ListTypePrefixedIds(firstEventType);

        foreach (var file in Directory.EnumerateFiles(_root))
        {
            var fileName = Path.GetFileName(file);
            var name = fileName.EndsWith(StreamSuffix, StringComparison.Ordinal)
                ? fileName[..^StreamSuffix.Length]
                : null;

            if (name is null || !Guid.TryParseExact(name, "D", out var id))
            {
                SkipFileName(file);
                continue;
            }

            // An empty stream has no first event and a corrupt first line yields no id — enumeration
            // never aborts boot, and it never guesses.
            var first = await PeekFirstAsync(id).ConfigureAwait(false);
            if (first is not null && first.GetType().Name == firstEventType)
                result.Add(id);
        }

        return result;
    }

    /// <summary>
    /// The <see cref="FileNameFormat.TypePrefix"/> enumeration: classify every DIRECTORY ENTRY and keep
    /// the ones whose name is <c>&lt;firstEventType&gt;-&lt;id:D&gt;</c>. No file is opened — the length
    /// that separates a stream from a zero-length leftover comes from the directory metadata.
    /// </summary>
    private List<Guid> ListTypePrefixedIds(string firstEventType)
    {
        var result = new List<Guid>();

        // A classification pass is a scan, and the increment comes FIRST: a pass that throws further
        // down still counts as a pass.
        Interlocked.Increment(ref _directoryScans);

        foreach (var file in new DirectoryInfo(_root).EnumerateFiles())
        {
            var kind = JsonlNames.Classify(
                file.Name,
                FileNameFormat.TypePrefix,
                out var type,
                out var id
            );

            if (kind != StreamNameKind.ConfiguredMatch)
            {
                // 1c splits this into the five buckets (other format / foreign / unparseable id). For
                // now every non-match is a skipped file name, exactly like the IdOnly scan.
                SkipFileName(file.FullName);
                continue;
            }

            // A zero-length file is not a stream — rev 5 lets a failed append leave one, and IdOnly
            // skips it too.
            if (file.Length == 0)
                continue;

            // A stream of ANOTHER type is the configured format, but not this query's answer: no
            // count, no log, no id.
            if (!string.Equals(type, firstEventType, StringComparison.Ordinal))
                continue;

            result.Add(id);

            // The names are in hand, so the resolution map is completed for free: one pass amortizes
            // across every id it finds.
            _typeById.TryAdd(id, type);
        }

        return result;
    }

    // ── Internals ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The ONE place a read handle is opened, and therefore the one place
    /// <see cref="FilesOpened"/> moves — a write handle is not an open for this counter's purpose.
    /// </summary>
    private FileStream OpenRead(string path)
    {
        Interlocked.Increment(ref _filesOpened);
        return new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
    }

    /// <summary>
    /// The path of <paramref name="actorId"/>'s stream, or <see langword="null"/> when THIS format has
    /// no stream for it — the answer every member turns into its "unknown aggregate" branch (an empty
    /// list, version 0, an empty cursor, null).
    /// <para>
    /// <see cref="FileNameFormat.IdOnly"/> derives the name from the id and never looks at the
    /// directory. <see cref="FileNameFormat.TypePrefix"/> resolves through the map; a MISS costs one
    /// filename pass, which also completes the map for every parseable name it finds — a cold start
    /// costs one pass, not one pass per id.
    /// </para>
    /// </summary>
    internal string? PathFor(Guid actorId)
    {
        if (_options.FileNameFormat != FileNameFormat.TypePrefix)
            return IdOnlyPathFor(actorId);

        if (_typeById.TryGetValue(actorId, out var type))
            return PrefixedPathFor(actorId, type);

        ScanTypeMap();
        return _typeById.TryGetValue(actorId, out type) ? PrefixedPathFor(actorId, type) : null;
    }

    /// <summary>
    /// The path a NEW stream takes under <see cref="FileNameFormat.TypePrefix"/>:
    /// <c>&lt;typeName&gt;-&lt;id:D&gt;.jsonl</c>. The caller supplies the type because only the BATCH
    /// knows it, so a store can never form the meaningless <c>-&lt;guid&gt;.jsonl</c> an empty prefix
    /// would produce.
    /// </summary>
    internal string PathForNew(Guid actorId, string typeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(typeName);
        return PrefixedPathFor(actorId, typeName);
    }

    private string PrefixedPathFor(Guid actorId, string typeName) =>
        Path.Combine(_root, typeName + "-" + actorId.ToString("D") + StreamSuffix);

    /// <summary>
    /// ONE filename pass over the root: fills the <c>id → type</c> map from every PARSEABLE, NON-EMPTY
    /// name. It reads directory metadata only (no file is opened, so it costs one
    /// <see cref="DirectoryScans"/> and no <see cref="FilesOpened"/>), and a root that does not exist
    /// yields an empty map — a store that only reads must not create the directory, and a missing
    /// directory is an empty store, not a failure.
    /// </summary>
    private void ScanTypeMap()
    {
        // Before classifying: a pass that throws still counts as a pass.
        Interlocked.Increment(ref _directoryScans);

        var root = new DirectoryInfo(_root);
        if (!root.Exists)
            return;

        foreach (var file in root.EnumerateFiles())
        {
            var kind = JsonlNames.Classify(
                file.Name,
                FileNameFormat.TypePrefix,
                out var type,
                out var id
            );

            // Only a stream of the CONFIGURED format names a prefix; a name in the other format has no
            // prefix this store may use.
            if (kind != StreamNameKind.ConfiguredMatch)
            {
                // Recorded, not acted on: this pass is where the store LEARNS that the directory is
                // mixed. 1c owns the per-scan Warning and the whole-directory throw that read it.
                if (kind == StreamNameKind.OtherFormat)
                    Interlocked.Increment(ref _foreignFormatFiles);

                continue;
            }

            // A zero-length file is not a stream: a failed append leaves one, and it carries no type.
            if (file.Length == 0)
                continue;

            // First name wins. The prefix is fixed when the stream is created; two coexisting prefixes
            // for one id are the append guard's business (1c), never a merge here.
            _typeById.TryAdd(id, type);
        }
    }

    /// <summary>
    /// The path an append writes to, plus the type prefix to REGISTER once the write succeeded (null
    /// when nothing has to be registered, and for every IdOnly store). An empty batch resolves its path
    /// the same way — map, then root pass — but writes nothing.
    /// </summary>
    private (string? Path, string? NewType) AppendPathFor(
        Guid actorId,
        IReadOnlyList<IDomainEvent> events
    )
    {
        var existing = PathFor(actorId);
        if (existing is not null || events.Count == 0)
            return (existing, null);

        // A NEW aggregate under TypePrefix: the prefix is the batch's first event type — with its
        // backtick arity, exactly as Type.Name reports it, and never FullName.
        var typeName = events[0].GetType().Name;
        return (PathForNew(actorId, typeName), typeName);
    }

    /// <summary>
    /// The path under <see cref="FileNameFormat.IdOnly"/>: the id alone, no directory knowledge. Also
    /// the path a <see cref="FileNameFormat.TypePrefix"/> store must check when it wants to know whether
    /// the OTHER format already holds this aggregate.
    /// </summary>
    internal string IdOnlyPathFor(Guid actorId) =>
        Path.Combine(_root, actorId.ToString("D") + StreamSuffix);

    private string SnapshotPathFor(Guid actorId) =>
        Path.Combine(_snapshotRoot, actorId.ToString("D") + SnapshotSuffix);

    /// <summary>Encode the batch as ONE buffer: <c>json</c> + newline per event, never split.</summary>
    private static byte[] Encode(IReadOnlyList<IDomainEvent> events, string path)
    {
        using var buffer = new MemoryStream();
        for (var i = 0; i < events.Count; i++)
        {
            if (events[i] is not TEventBase domainEvent)
            {
                throw new ArgumentException(
                    $"Event at index {i} is {events[i]?.GetType().FullName ?? "null"}, which is not a "
                        + $"{typeof(TEventBase).FullName}. A stream's event type is fixed by the store's "
                        + "generic argument — register every concrete event with [PicoDerivedType] on "
                        + $"{typeof(TEventBase).FullName}.",
                    nameof(events)
                );
            }

            byte[] json;
            try
            {
                json = JsonSerializer.SerializeToUtf8Bytes(domainEvent);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw Wrap(ex, path, "serialize");
            }

            buffer.Write(json);
            buffer.WriteByte(JsonlLines.NewLine);
        }

        return buffer.ToArray();
    }

    /// <summary>
    /// Deserialize one line, wrapping the three documented PicoJetson failures so the message names the
    /// base type and the ONE registration form. The alternative is a bare "Unknown type discriminator"
    /// from a generator the consumer never sees.
    /// </summary>
    private IDomainEvent DeserializeLine(byte[] line, string path)
    {
        try
        {
            if (JsonSerializer.Deserialize<TEventBase>(line) is { } domainEvent)
            {
                Interlocked.Increment(ref _deserializedEvents);
                return domainEvent;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Wrap(ex, path, "deserialize");
        }

        throw Wrap(new FormatException("the line deserialized to null"), path, "deserialize");
    }

    private static Exception Wrap(Exception inner, string path, string action)
    {
        var message =
            $"Cannot {action} '{path}' as {typeof(TEventBase).FullName}: {inner.Message} "
            + "Every concrete event must be registered with [PicoDerivedType(typeof(T), \"tag\")] on "
            + $"{typeof(TEventBase).FullName} in the assembly that declares it — that is where the "
            + "PicoJetson serializer is generated.";

        // The exception TYPE is preserved: the docs distinguish the three failures — FormatException
        // for a bad/unknown discriminator or an unregistered derived type, InvalidOperationException
        // when the base itself was never generated.
        return inner is InvalidOperationException
            ? new InvalidOperationException(message, inner)
            : new FormatException(message, inner);
    }

    private static bool EndsWithNewline(FileStream stream, long length)
    {
        stream.Seek(length - 1, SeekOrigin.Begin);
        return stream.ReadByte() == JsonlLines.NewLine;
    }

    /// <summary>Offset just after the last newline (0 when the file has none) — the truncation point.</summary>
    private static long FindLastNewline(FileStream stream, long length)
    {
        var buffer = new byte[8192];
        var position = length;
        while (position > 0)
        {
            var chunk = (int)Math.Min(buffer.Length, position);
            position -= chunk;
            stream.Seek(position, SeekOrigin.Begin);
            stream.ReadExactly(buffer, 0, chunk);
            for (var i = chunk - 1; i >= 0; i--)
            {
                if (buffer[i] == JsonlLines.NewLine)
                    return position + i + 1;
            }
        }

        return 0;
    }

    /// <summary>
    /// The single pass over a stream: the event count, the byte offset at which
    /// <c>fromVersion</c> starts, and whether the pass reported a torn tail — the append's repair step
    /// needs to know, so one fragment is never reported twice for one append.
    /// </summary>
    private readonly record struct Scan(ulong Count, long Offset, bool TornTail);

    /// <summary>
    /// Count complete, non-blank, newline-terminated lines and record the byte offset at which the
    /// <paramref name="fromVersion"/>-th event starts. No JSON validation: the count is a LINE count
    /// (the documented relaxation of "version == the loader's count").
    /// </summary>
    private Scan ScanStream(Stream stream, ulong fromVersion, string path)
    {
        var skip = fromVersion <= 1 ? 0UL : fromVersion - 1;
        var buffer = new byte[ScanBufferBytes];
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
                if (value != JsonlLines.NewLine)
                {
                    if (!JsonlLines.IsBlankByte(value))
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
        if (tornTail)
            ReportTornTail(path);

        // Past the end (or a torn tail): the remaining bytes are where a next event would start, and the
        // caller's capacity is 0 there, so the cursor yields nothing either way.
        if (offset < 0)
            offset = lineStart;

        return new Scan(count, offset, tornTail);
    }

    private void ReportTornTail(string path)
    {
        Interlocked.Increment(ref _tornTailLines);
        _logger?.Warning(
            $"Ignoring the unterminated final line of '{path}': a torn append is a crash artefact, "
                + "never an event. The next append truncates it."
        );
    }

    private void SkipFileName(string file)
    {
        Interlocked.Increment(ref _skippedFileNames);
        _logger?.Warning($"Skipping '{file}' during enumeration: a stream is named <guid>.jsonl.");
    }
}
