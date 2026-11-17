namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Durable, append-only event store: one file per aggregate, one JSON object per line, snapshots as
/// sidecars in <c>&lt;root&gt;-snapshots</c>. <c>&lt;root&gt;/&lt;id:D&gt;.jsonl</c> is the DEFAULT
/// layout; <see cref="FileNameFormat.TypePrefix"/> names the same content
/// <c>&lt;root&gt;/&lt;type&gt;-&lt;id:D&gt;.jsonl</c>, which is what lets an enumeration read the ids off
/// the names instead of opening every stream.
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

    // The three states of the ONE lazy directory pass an IdOnly store performs. Three, not a bool: a
    // pass that threw must be RAISED AGAIN on every later observation instead of being remembered as
    // "already scanned" — that would turn a misconfigured root into a silently empty store.
    private const int LazyScanNotRun = 0;
    private const int LazyScanDone = 1;
    private const int LazyScanFailed = 2;

    private readonly string _root;
    private readonly string _snapshotRoot;
    private readonly JsonlEventStoreOptions _options;
    private readonly ILogger? _logger;
    private readonly JsonlLockTable _locks;

    // The id → type map TypePrefix resolution needs, because IEventStore carries only the aggregate id.
    // Never consulted under IdOnly, where the name IS the id.
    private readonly ConcurrentDictionary<Guid, string> _typeById = new();

    // Ids a classification pass saw under TWO different prefixes. The prefix is fixed at creation, so two
    // names for one id are two histories — an append must refuse rather than merge them into whichever
    // name the map happened to learn first.
    private readonly ConcurrentDictionary<Guid, string> _conflictingPrefixes = new();

    private long _deserializedEvents;
    private long _tornTailLines;
    private long _skippedFileNames;
    private long _filesOpened;
    private long _directoryScans;
    private long _foreignFormatFiles;

    // One lock, one state and one latched failure: the whole cost of the IdOnly lazy pass, which is
    // read on every observation of a stream that is not there and written at most once per instance.
    private readonly object _lazyScanGate = new();
    private int _lazyScanState = LazyScanNotRun;
    private Exception? _lazyScanFailure;

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

    /// <summary>
    /// Directory entries a classification pass skipped because they are not streams of the configured
    /// format: a foreign entry (a README, a lease, a backup) or the configured shape with an unparseable
    /// id. One per FILE, each with its own warning.
    /// </summary>
    internal long SkippedFileNames => Interlocked.Read(ref _skippedFileNames);

    /// <summary>
    /// Stream files this store has OPENED for reading (a load, a cursor move, a count, a peek). Written
    /// in one place — the read path — so the counter measures what an enumeration actually costs: under
    /// <see cref="FileNameFormat.IdOnly"/> one open per stream, under
    /// <see cref="FileNameFormat.TypePrefix"/> none at all.
    /// </summary>
    internal long FilesOpened => Interlocked.Read(ref _filesOpened);

    /// <summary>
    /// Directory CLASSIFICATION passes this store has performed. EVERY enumeration pays one in both
    /// formats, because the pass is where a foreign name, an unparseable id, the other layout and a
    /// whole-directory misconfiguration are diagnosed. A <see cref="FileNameFormat.TypePrefix"/> store
    /// pays one more at construction (to learn the <c>id → type</c> map) and one per resolved miss.
    /// <para>
    /// <see cref="FileNameFormat.IdOnly"/> pays exactly ONE more, and only lazily: at the FIRST
    /// observation of a stream it does not have (a load, a version, a cursor, a peek, an empty batch, or
    /// an append of a new id). A store that only ever touches streams it already has never looks at the
    /// directory at all.
    /// </para>
    /// </summary>
    internal long DirectoryScans => Interlocked.Read(ref _directoryScans);

    /// <summary>
    /// Directory entries this store has classified as a stream in the OTHER naming format, summed over
    /// every pass — MONOTONIC, because it is the gate the <see cref="FileNameFormat.IdOnly"/> duplicate
    /// guard reads: once any scan has seen another layout in this directory, appends pay for one check
    /// per id; a pure single-format directory never does. Non-zero with ZERO configured-format names means
    /// the store is pointed at a directory of the other layout — a misconfiguration, not an empty store.
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

        // The gate comes FIRST: the path is resolved INSIDE it, because resolving a new id's path is
        // what claims that id's prefix — and, under IdOnly, what pays the one lazy pass.
        var entry = _locks.Acquire(actorId);
        await entry.Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            // 1. An empty batch is an append that writes nothing, NOT a bypass: it resolves its path
            //    the same way a writing append does (map, then the TypePrefix root pass), and the
            //    version check that follows validates a HELD stream's expectedVersion in both formats.
            //    Only the duplicate guard and the write are skipped — there is nothing to write and
            //    nothing to create.
            if (events.Count == 0)
                return EmptyAppend(actorId, expectedVersion, PathFor(actorId), entry);

            // 2. A non-empty batch knows its TYPE before its path: the prefix is events[0]'s concrete
            //    type name (its backtick arity kept, exactly as Type.Name reports it — never FullName),
            //    so an id can never be given the meaningless "-<guid>.jsonl" an EMPTY prefix would form.
            var typeName = events[0].GetType().Name;

            // 3. Resolving the path is also the observation of a MISSING stream, which is the one
            //    moment an IdOnly store pays its lazy pass: a host that only appends by id must still
            //    learn that its whole directory is in the other layout before it forks a second stream.
            //    A TypePrefix miss has already paid for its own pass inside PathFor.
            var existing = PathFor(actorId);
            if (existing is null || !File.Exists(existing))
                ObserveMissingStream();

            // 4. A NEW id takes the name its first event gives it; an EXISTING stream keeps the prefix
            //    it was created with, whatever this batch's first event happens to be called.
            var streamPath = existing ?? PathForNew(actorId, typeName);

            // 5. Two histories for ONE id are never merged. Under TypePrefix the other layout's name is
            //    derived from the id (one stat); under IdOnly the check costs a directory pass and is
            //    gated on a scan having seen another layout here. Either way it runs BEFORE the encode,
            //    so a refused append has touched no file.
            GuardAmbiguousStream(actorId, streamPath);

            // 6. Every element must be the declared base type, and the batch is encoded as ONE buffer
            //    before anything reaches the disk — a serialization failure creates no file at all.
            var payload = Encode(events, streamPath);

            Directory.CreateDirectory(_root);

            await using var stream = new FileStream(
                streamPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read
            );

            // 7. Length witness: a length the cache did not count at means somebody else wrote here.
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

            // 8. Optimistic concurrency against the counted version.
            if (entry.Count != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, entry.Count);

            // 9. Torn-tail repair: truncate the unterminated fragment, never terminate it. Terminating
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

            // 10. ONE write over the encoded bytes, then a flush the append contract can wait for.
            stream.Seek(0, SeekOrigin.End);
            await stream.WriteAsync(payload, CancellationToken.None).ConfigureAwait(false);
            await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            if (_options.FlushToDisk)
                stream.Flush(flushToDisk: true);

            // 11. The cache describes the file as it now is.
            var version = entry.Count + (ulong)events.Count;
            entry.Seed(version, stream.Length);

            // The prefix is fixed at CREATION: the map learns it once the bytes are on disk, so an id
            // that is later recreated keeps pointing at the file its first event named.
            if (existing is null)
                _typeById.TryAdd(actorId, typeName);

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

    /// <summary>
    /// Observe a stream WITHOUT creating or repairing anything: (count, length), 0/0 when absent. An
    /// absent stream is an OBSERVATION like any other, so under <see cref="FileNameFormat.IdOnly"/> this
    /// is also where the single lazy pass runs — an EMPTY batch for an id the store has never written
    /// diagnoses a misconfigured root instead of reporting version 0.
    /// </summary>
    private (ulong Count, long Length) Inspect(string? path)
    {
        // A null path is "this format has no stream for the id" — the same observation as a missing
        // file, and it must not create one.
        if (path is null || !File.Exists(path))
        {
            ObserveMissingStream();
            return (0UL, 0L);
        }

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
        var path = ExistingPathFor(actorId);
        if (path is null)
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
        var path = ExistingPathFor(actorId);
        if (path is null)
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

        var path = ExistingPathFor(actorId);
        if (path is null)
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
        var path = ExistingPathFor(actorId);
        if (path is null)
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

        // The IdOnly pass CLASSIFIES every entry before it reads any: the ids still come from the first
        // EVENT (this name carries no type), but the pass itself is what reports a name in the other
        // layout, a foreign entry and — when there is nothing else — a whole-directory
        // misconfiguration.
        // No query type is handed to the pass: under IdOnly the NAME carries no type, so there is no
        // "configured, another type" bucket to compute — the first EVENT decides, below.
        foreach (var entry in ScanRoot())
        {
            // A zero-length file is not a stream: rev 5 lets a failed append leave one.
            if (entry.File.Length == 0)
                continue;

            // Here the type lives in the CONTENT, so reading is the only way to classify the aggregate
            // — and a corrupt first line yields no id. Enumeration never aborts boot, never guesses.
            var first = await PeekFirstAsync(entry.Id).ConfigureAwait(false);
            if (first is not null && first.GetType().Name == firstEventType)
                result.Add(entry.Id);
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

        foreach (var entry in ScanRoot(firstEventType))
        {
            // A zero-length file is not a stream — rev 5 lets a failed append leave one, and IdOnly
            // skips it too.
            if (entry.File.Length == 0)
                continue;

            // The names are in hand, so the resolution map is completed for free — for EVERY id the
            // pass sees, other types included: one pass amortizes across all of them.
            LearnPrefix(entry.Id, entry.Type);

            // A stream of ANOTHER type is the configured format, but not this query's answer: no
            // count, no log, no id.
            if (entry.Kind != StreamNameKind.ConfiguredMatch)
                continue;

            result.Add(entry.Id);
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
    /// The path of a stream that EXISTS, or <see langword="null"/> when this format has none for
    /// <paramref name="actorId"/> — and the one place a read of a MISSING stream becomes a diagnosis.
    /// <para>
    /// Under <see cref="FileNameFormat.IdOnly"/> the name is derived from the id alone, so this is the
    /// first moment the store learns it has no stream: exactly when the single lazy pass runs
    /// (<see cref="ObserveMissingStream"/>). Under <see cref="FileNameFormat.TypePrefix"/> a miss has
    /// already paid its own pass inside <see cref="PathFor"/>, and the lazy pass is an IdOnly cost.
    /// </para>
    /// </summary>
    private string? ExistingPathFor(Guid actorId)
    {
        var path = PathFor(actorId);
        if (path is not null && File.Exists(path))
            return path;

        ObserveMissingStream();
        return null;
    }

    /// <summary>
    /// The ONE lazy directory pass of an <see cref="FileNameFormat.IdOnly"/> store: run the first time it
    /// observes a stream it does not have, and never again.
    /// <para>
    /// Why it exists: IdOnly derives every path from the id, so a host that only loads and appends BY ID
    /// would never enumerate — and would never learn that its whole directory is in the OTHER layout.
    /// It would write a second stream beside the one it cannot see. One pass at the first miss is what
    /// turns that into the loud whole-directory error, and it costs nothing for a store that only ever
    /// touches streams it already has.
    /// </para>
    /// <para>
    /// A FAILED pass is LATCHED, not swallowed: the misconfiguration is raised again on the next
    /// observation, because a store pointed at the wrong directory must not start answering "empty
    /// aggregate" after one loud error. The latch is also why <see cref="DirectoryScans"/> stops at one —
    /// the directory is never re-scanned per operation.
    /// </para>
    /// </summary>
    private void ObserveMissingStream()
    {
        // TypePrefix resolves a miss with its own pass (construction, or PathFor), so the lazy pass is an
        // IdOnly cost: that is the format whose paths are derived without ever looking at the directory.
        if (_options.FileNameFormat != FileNameFormat.IdOnly)
            return;

        var state = Volatile.Read(ref _lazyScanState);
        if (state == LazyScanNotRun)
            state = RunLazyScan();

        if (state == LazyScanFailed)
            throw new InvalidOperationException(_lazyScanFailure!.Message, _lazyScanFailure);
    }

    /// <summary>
    /// Run the lazy pass at most ONCE per instance, whatever the concurrency: the winner classifies the
    /// directory (which counts the pass BEFORE it can throw) and publishes the outcome, the losers read
    /// it. A pass that threw publishes the FAILURE, so every later observation raises it again.
    /// </summary>
    private int RunLazyScan()
    {
        lock (_lazyScanGate)
        {
            var state = Volatile.Read(ref _lazyScanState);
            if (state != LazyScanNotRun)
                return state;

            try
            {
                ScanRoot();
                state = LazyScanDone;
            }
            catch (InvalidOperationException ex)
            {
                // The whole-directory misconfiguration: remembered, never treated as "already scanned".
                _lazyScanFailure = ex;
                state = LazyScanFailed;
            }

            Volatile.Write(ref _lazyScanState, state);
            return state;
        }
    }

    // ── Classification: the ONE pass every scan is built on ──────────────────────────────────

    /// <summary>
    /// A stream file a classification pass recognised under the CONFIGURED format, with its name already
    /// parsed. The length is directory METADATA — no file is opened to learn whether an entry is a stream
    /// or a zero-length leftover.
    /// </summary>
    private readonly record struct StreamEntry(
        FileInfo File,
        StreamNameKind Kind,
        string Type,
        Guid Id
    );

    /// <summary>
    /// ONE pass over the root's entries, and the only place a directory is classified. Every entry lands
    /// in exactly one of the <see cref="StreamNameKind"/> buckets, and the buckets are what turn a mixed
    /// directory into a diagnosis instead of an empty enumeration:
    /// <list type="bullet">
    /// <item><see cref="StreamNameKind.ConfiguredMatch"/> — returned to the caller.</item>
    /// <item><see cref="StreamNameKind.ConfiguredOtherType"/> — also returned, marked, when
    /// <c>firstEventType</c> names the query: the configured format, another type. NO counter moves and
    /// NOTHING is logged — one store directory normally holds many stream types.</item>
    /// <item><see cref="StreamNameKind.OtherFormat"/> — added to <see cref="ForeignFormatFiles"/>
    /// (monotonic) and reported as ONE warning for the whole pass, not one per file: a half-migrated
    /// directory would otherwise emit thousands of lines.</item>
    /// <item><see cref="StreamNameKind.UnparseableId"/> and <see cref="StreamNameKind.Foreign"/> — one
    /// skipped name each, counted and reported individually: an unreadable name is a problem with that
    /// NAME, not with the directory.</item>
    /// </list>
    /// <para>
    /// A pass that finds ZERO configured names and SOME other-format names THROWS: the store is pointed at
    /// a directory of the other layout, and the honest answer is an error naming both formats and both
    /// counts — not "this store is empty", which is how a whole history disappears from view.
    /// </para>
    /// </summary>
    /// <param name="firstEventType">
    /// The type the caller queries, or <see langword="null"/> when the pass has no query (learning the
    /// <c>id → type</c> map): with a query, a configured name of another type is marked
    /// <see cref="StreamNameKind.ConfiguredOtherType"/> instead of
    /// <see cref="StreamNameKind.ConfiguredMatch"/>.
    /// </param>
    private List<StreamEntry> ScanRoot(string? firstEventType = null)
    {
        // The pass counts as a pass BEFORE anything can throw at it, so a scan that diagnoses a
        // misconfiguration still shows up in DirectoryScans.
        Interlocked.Increment(ref _directoryScans);

        var root = new DirectoryInfo(_root);
        if (!root.Exists)
            return [];

        var streams = new List<StreamEntry>();
        long configured = 0;
        long other = 0;

        foreach (var file in root.EnumerateFiles())
        {
            var kind = JsonlNames.Classify(
                file.Name,
                _options.FileNameFormat,
                out var type,
                out var id
            );

            switch (kind)
            {
                case StreamNameKind.ConfiguredMatch:
                    // Counted by NAME, length aside: the layout of the names is what the whole-directory
                    // rule diagnoses, and a zero-length file still speaks for that layout.
                    configured++;
                    streams.Add(
                        new StreamEntry(
                            file,
                            firstEventType is not null
                            && type.Length > 0
                            && !string.Equals(type, firstEventType, StringComparison.Ordinal)
                                ? StreamNameKind.ConfiguredOtherType
                                : StreamNameKind.ConfiguredMatch,
                            type,
                            id
                        )
                    );
                    break;

                case StreamNameKind.OtherFormat:
                    other++;
                    break;

                default:
                    // UnparseableId and Foreign: one name, one count, one warning each.
                    SkipFileName(file.FullName);
                    break;
            }
        }

        if (other > 0)
        {
            Interlocked.Add(ref _foreignFormatFiles, other);
            _logger?.Warning(
                $"{other} file(s) in '{_root}' use the other naming format "
                    + $"({JsonlNames.Describe(JsonlNames.Opposite(_options.FileNameFormat))}); this store "
                    + $"reads {JsonlNames.Describe(_options.FileNameFormat)} names only and ignores the "
                    + "rest of the directory."
            );
        }

        if (configured == 0 && other > 0)
            throw MisconfiguredDirectory(configured, other);

        return streams;
    }

    /// <summary>
    /// The loud outcome for a store pointed at a directory of the OTHER layout. Both formats are named,
    /// both counts are stated and the remedy is spelled out: without it the store starts, enumerates
    /// nothing, and the operator sees an empty system instead of a broken one.
    /// </summary>
    private InvalidOperationException MisconfiguredDirectory(long configured, long other) =>
        new(
            $"Misconfigured store root '{_root}': configured: {configured}, other-format: {other}. This "
                + $"store reads {JsonlNames.Describe(_options.FileNameFormat)} names and found none, while "
                + $"{other} file(s) are named for "
                + $"{JsonlNames.Describe(JsonlNames.Opposite(_options.FileNameFormat))}. Enumerating it "
                + "would report an empty system instead of a misconfigured one. Migrate the directory or "
                + $"set '{nameof(JsonlEventStoreOptions)}.{nameof(JsonlEventStoreOptions.FileNameFormat)}'."
        );

    /// <summary>
    /// Record one configured-format name in the <c>id → type</c> map. The FIRST name wins — the prefix is
    /// fixed when the stream is created — and a SECOND, different one is remembered as a CONFLICT: two
    /// coexisting prefixes for one id are two histories, so an append for that id must refuse instead of
    /// extending whichever name the map happened to learn first.
    /// </summary>
    private void LearnPrefix(Guid id, string type)
    {
        if (_typeById.TryAdd(id, type))
            return;

        if (!_typeById.TryGetValue(id, out var known))
            return;

        if (!string.Equals(known, type, StringComparison.Ordinal))
            _conflictingPrefixes.TryAdd(id, PrefixedPathFor(id, type));
    }

    /// <summary>
    /// The duplicate guard: one aggregate, ONE layout and ONE prefix. It runs in
    /// <c>AppendAsync</c> before the encode, so a refused append has touched no file at all.
    /// </summary>
    private void GuardAmbiguousStream(Guid actorId, string streamPath)
    {
        // 1. Two PREFIXES for one id, seen by a classification pass. Nothing was written, so both
        //    histories are intact and an operator can still decide which one survives.
        if (_conflictingPrefixes.TryGetValue(actorId, out var conflicting))
        {
            throw new InvalidOperationException(
                $"Aggregate '{actorId:D}' has TWO stream names in '{_root}': '{streamPath}' and "
                    + $"'{conflicting}'. A stream's prefix is fixed when it is created, so the store "
                    + "cannot tell which history an append belongs to. Archive one of the files, then "
                    + "retry the append."
            );
        }

        if (_options.FileNameFormat == FileNameFormat.TypePrefix)
        {
            // 2. The OTHER layout's name IS derivable from the id, so this check is one stat and it is
            //    never gated: a mixed directory cannot grow a second history through this store.
            var otherPath = IdOnlyPathFor(actorId);
            if (new FileInfo(otherPath) is { Exists: true, Length: > 0 })
                throw MixedLayout(actorId, streamPath, otherPath);

            return;
        }

        // 3. Under IdOnly the other layout's prefix lives in the NAME and is not derivable from the id,
        //    so the check costs one directory pass — and a single-format directory must never pay it.
        //    The gate is ForeignFormatFiles, MONOTONIC: once a pass has seen another layout here,
        //    appends check.
        if (Interlocked.Read(ref _foreignFormatFiles) == 0)
            return;

        var candidates = OtherLayoutPathsFor(actorId);

        if (candidates.Count > 1)
        {
            throw new InvalidOperationException(
                $"Aggregate '{actorId:D}' has {candidates.Count} stream names in the other naming format: "
                    + string.Join(", ", candidates.Select(path => "'" + path + "'"))
                    + ". The prefix is fixed when a stream is created, so the store cannot tell which "
                    + "history an append belongs to. Archive all but one of the files, then retry the "
                    + "append."
            );
        }

        // A zero-length file is NOT a stream (rev 5 lets a failed append leave one behind), so it never
        // blocks an append; a non-empty one is a second history for this id.
        if (candidates.Count == 1 && new FileInfo(candidates[0]) is { Exists: true, Length: > 0 })
            throw MixedLayout(actorId, streamPath, candidates[0]);
    }

    /// <summary>
    /// Every name in the OTHER layout that <paramref name="actorId"/> may already have here — empty for a
    /// single-format directory. The caller owns the gate on this directory pass.
    /// </summary>
    private List<string> OtherLayoutPathsFor(Guid actorId)
    {
        var matches = new List<string>();

        var root = new DirectoryInfo(_root);
        if (!root.Exists)
            return matches;

        foreach (var file in root.EnumerateFiles())
        {
            if (
                JsonlNames.TryParseOtherFormat(file.Name, _options.FileNameFormat, out var id)
                && id == actorId
            )
                matches.Add(file.FullName);
        }

        return matches;
    }

    /// <summary>
    /// A stream of one id living in BOTH layouts. The two files hold two histories and this store never
    /// merges them: the append is refused before a byte is written, and both paths are named.
    /// </summary>
    private static InvalidOperationException MixedLayout(
        Guid actorId,
        string streamPath,
        string otherPath
    ) =>
        new(
            $"Aggregate '{actorId:D}' already has a non-empty stream in the OTHER naming format: "
                + $"'{otherPath}'. This append targets '{streamPath}'. The two layouts are never merged, "
                + "so one of the two histories would be lost. Migrate the directory or set "
                + $"'{nameof(JsonlEventStoreOptions)}.{nameof(JsonlEventStoreOptions.FileNameFormat)}'."
        );

    /// <summary>
    /// ONE filename pass over the root: fills the <c>id → type</c> map from every PARSEABLE, NON-EMPTY
    /// name. It reads directory metadata only (no file is opened, so it costs one
    /// <see cref="DirectoryScans"/> and no <see cref="FilesOpened"/>), and a root that does not exist
    /// yields an empty map — a store that only reads must not create the directory, and a missing
    /// directory is an empty store, not a failure.
    /// </summary>
    private void ScanTypeMap()
    {
        foreach (var entry in ScanRoot())
        {
            // A zero-length file is not a stream: a failed append leaves one, and it carries no type.
            if (entry.File.Length == 0)
                continue;

            LearnPrefix(entry.Id, entry.Type);
        }
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

        // The message names the configuration instead of one fixed layout: this store may be reading
        // "<id>.jsonl" OR "<type>-<id>.jsonl", so claiming a stream is "always <guid>.jsonl" would
        // misdirect the operator who just added the file this warning is about.
        _logger?.Warning(
            $"Skipping '{file}' during enumeration: it is not a stream name in the configured format "
                + $"({JsonlNames.Describe(_options.FileNameFormat)})."
        );
    }
}
