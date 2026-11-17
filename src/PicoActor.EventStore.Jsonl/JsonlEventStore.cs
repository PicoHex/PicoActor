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
/// <para>
/// THE EXCEPTION SURFACE. Beyond its own argument errors, this store raises
/// <see cref="InvalidOperationException"/> for the three shapes it must never merge or silently ignore:
/// </para>
/// <list type="bullet">
/// <item>
/// A directory that is ENTIRELY in the other naming format — no stream of the configured layout in it.
/// Every path that classifies the directory raises it: <see cref="ListAggregateIdsAsync"/>, the
/// construction of a <see cref="FileNameFormat.TypePrefix"/> store, and the FIRST observation of a
/// stream an <see cref="FileNameFormat.IdOnly"/> store does not have (<see cref="LoadAsync"/>,
/// <see cref="GetVersionAsync"/>, <see cref="OpenAsync"/>, <see cref="PeekFirstAsync"/>, an empty batch
/// and an append of a new id). Enumerating such a directory would report an empty system, which is how a
/// whole history disappears from view.
/// </item>
/// <item>
/// One aggregate id with a NON-EMPTY stream in BOTH layouts: two histories, and no append can tell which
/// one it belongs to.
/// </item>
/// <item>
/// One aggregate id under SEVERAL prefixes of the configured layout (a
/// <see cref="FileNameFormat.TypePrefix"/> directory): the prefix is fixed at creation, so the same
/// ambiguity applies.
/// </item>
/// <item>
/// A stream NAME occupied by a DIRECTORY: nothing can be opened or created at that path, and the honest
/// answer is neither "this aggregate is empty" (the read path) nor a bare sharing violation (the append
/// path) but the sentence that names what is in the way. Every member that resolves that id raises it,
/// in both formats, and it is the one case whose remedy is a file-system action of the operator's.
/// </item>
/// </list>
/// <para>
/// The remedy is always stated in the message: migrate the directory to one layout, move the files that
/// do not belong to this store out of it, set
/// <see cref="JsonlEventStoreOptions.FileNameFormat"/> to the layout the directory was WRITTEN with, or
/// move the directory that occupies a stream's name. Nothing is deleted, moved or merged on the caller's
/// behalf.
/// </para>
/// <para>
/// A root that cannot be READ is the fourth: the first pass that looks at it — the enumeration, the
/// construction of a <see cref="FileNameFormat.TypePrefix"/> store, or the lazy pass a first absent
/// stream triggers — raises <see cref="IOException"/> instead of answering "no events" (including when
/// the root exists as a FILE), and an <see cref="UnauthorizedAccessException"/> stays an
/// <see cref="UnauthorizedAccessException"/>. A lazy pass LATCHES it, so it costs one failed attempt,
/// never one per call.
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

    // Ids a classification pass saw under several different prefixes, with EVERY rival name found. The
    // prefix is fixed at creation, so two names for one id are two histories — an append must refuse
    // rather than merge them into whichever name the map happened to learn first. ALL of the rivals are
    // kept: a record that held one per id would drop the third name, and the operator would archive the
    // two files the message named and be refused again by the one it did not.
    private readonly ConcurrentDictionary<Guid, List<string>> _conflictingPrefixes = new();

    private long _deserializedEvents;
    private long _tornTailLines;
    private long _skippedFileNames;
    private long _filesOpened;
    private long _directoryScans;
    private long _foreignFormatFiles;

    // The ids a TypePrefix MISS already failed to resolve, so a host that probes streams it does not
    // have pays ONE directory pass per unknown id instead of one per probe.
    // IT SERVES THE READ PATH ONLY. An append that trusted a remembered absence would resolve the id to
    // "no stream" and fork a second name beside the one that is there (an external writer's file this
    // instance never scanned), so the append re-scans on a miss and never consults this cache.
    // CONCURRENT, not a HashSet under the scan gate: the ONE mutation the append hot path performs —
    // dropping the id its own append falsified — must not serialize on the gate a directory pass holds.
    // BOUNDED: past the capacity the whole set is emptied, because the pass it saves is worth less than
    // an unbounded allocation in the one path a probe loop drives.
    private const int MissingTypeCapacity = 4096;
    private readonly ConcurrentDictionary<Guid, byte> _missingTypes = new();
    private long _negativeCacheInvalidations;

    // One lock, one state and one latched failure: the whole cost of the IdOnly lazy pass, which is
    // read on every observation of a stream that is not there and written at most once per instance.
    // The SAME lock serializes a TypePrefix MISS — the other pass that must run at most once for a whole
    // burst of concurrent readers. No instance is both formats, so the two never contend for it.
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
    /// pays one more at construction (to learn the <c>id → type</c> map) and one per resolved miss — and
    /// the TWO paths pay it differently, which is the whole point of the negative cache: a READ remembers
    /// the miss, so probes of ONE absent id cost a pass once (single-flight inside the gate; a DIFFERENT
    /// absent id pays a pass of its own), while
    /// an APPEND never trusts that memory and re-scans, because a remembered absence is not evidence that
    /// the stream is still absent.
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
    /// per id; a pure single-format directory never does.
    /// <para>
    /// It counts NAMES, not histories — that is what the gate above needs, since every foreign entry is
    /// worth reporting. The whole-directory diagnosis is the OTHER question and uses the other count
    /// (<c>otherNonEmpty</c>: the foreign entries that are not ZERO-LENGTH): a directory whose only
    /// foreign names are failed-append leftovers holds no history at all, so it is an empty store, not a
    /// misconfigured one. Non-zero here with ZERO configured-format names is therefore ambiguous on its
    /// own; the throw is decided by histories.
    /// </para>
    /// </summary>
    internal long ForeignFormatFiles => Interlocked.Read(ref _foreignFormatFiles);

    /// <summary>Live per-aggregate gate entries.</summary>
    internal int LockEntryCount => _locks.Count;

    /// <summary>
    /// Remembered misses this store has dropped because a successful append of THAT id falsified them.
    /// PER ID and never wholesale: an append that learns a new prefix says nothing about another id's
    /// absence, and the drop is a <see cref="ConcurrentDictionary{TKey,TValue}.TryRemove"/> that takes no
    /// global lock — the append hot path must not serialize on the scan gate for a cache it never reads.
    /// Under <see cref="FileNameFormat.IdOnly"/> no miss is ever REMEMBERED (the name is the id, so a
    /// read never looks at the directory) and none is ever dropped, so the counter is identically zero
    /// there.
    /// </summary>
    internal long NegativeCacheInvalidations => Interlocked.Read(ref _negativeCacheInvalidations);

    /// <inheritdoc/>
    /// <remarks>
    /// The ORDER is the contract: read-only witness, version, torn-tail truncate, ONE write, flush,
    /// cache update. A rejected append never changes the stream's content (a missing stream may be left
    /// as a zero-length file, which enumeration skips), and the cache is what makes the steady state
    /// O(1) per append instead of a rescan.
    /// <para>
    /// An append is also where a misconfigured root is diagnosed, because a host that only appends by id
    /// never enumerates: a directory that is entirely in the other naming format raises
    /// <see cref="InvalidOperationException"/> BEFORE a byte is written (otherwise the store would fork a
    /// second history beside the file it cannot see). The same exception refuses the two ambiguous
    /// shapes — an aggregate with a non-empty stream in both layouts, and an aggregate with several
    /// prefixes — and a stream name occupied by a directory. The remedy is named in the message: migrate
    /// the directory, move the files that do not belong to this store out of it, or set
    /// <see cref="JsonlEventStoreOptions.FileNameFormat"/>.
    /// </para>
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
            //    Its path is resolved by the APPEND path's resolver (and so is a writing append's,
            //    below): a remembered miss is not an answer for an append, which re-scans instead of
            //    forking a second name beside the stream it never saw.
            if (events.Count == 0)
                return EmptyAppend(actorId, expectedVersion, AppendingPathFor(actorId), entry);

            // 2. A non-empty batch knows its TYPE before its path: the prefix is events[0]'s concrete
            //    type name (its backtick arity kept, exactly as Type.Name reports it — never FullName),
            //    so an id can never be given the meaningless "-<guid>.jsonl" an EMPTY prefix would form.
            var typeName = events[0].GetType().Name;

            // 3. Resolving the path is also the observation of a MISSING stream, which is the one
            //    moment an IdOnly store pays its lazy pass: a host that only appends by id must still
            //    learn that its whole directory is in the other layout before it forks a second stream.
            //    A TypePrefix miss has already paid for its own pass inside AppendingPathFor — and an
            //    append does NOT take the read path's remembered absence for an answer: it re-scans,
            //    because the miss may have been recorded before another process created this id's stream.
            //    That pass is the price of a MISS, not of an append: an id the map KNOWS costs no pass
            //    here at all (B1c pins it), and a REFUSED append pays exactly the one pass it spent
            //    resolving the id (B9 pins it).
            var existing = AppendingPathFor(actorId);
            if (existing is null || !File.Exists(existing))
            {
                // A name occupied by a DIRECTORY is not an absent stream: appending would fail on the
                // directory handle, so the refusal names the real obstacle before anything is created.
                ThrowIfOccupiedByDirectory(existing);
                ObserveMissingStream();
            }

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
            // that is later recreated keeps pointing at the file its first event named. It learns
            // through the SAME primitive a classification pass uses, so a rival prefix that appeared
            // while this append was encoding is REGISTERED as a conflict instead of being dropped —
            // a silent TryAdd would leave two names for one id with the guard none the wiser.
            if (existing is null)
            {
                LearnPrefix(actorId, typeName);

                // ... and the read path's memory of this id is now false: drop THAT entry, and only it.
                // No gate, no wholesale clear — an append of a new id falsifies no other id's absence.
                ForgetMissingType(actorId);
            }

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
            ThrowIfOccupiedByDirectory(path);
            ObserveMissingStream();
            return (0UL, 0L);
        }

        FileStream stream;
        try
        {
            stream = OpenRead(path);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // The file was there for the check above and gone for the open: an empty batch interrupted by
            // a deletion is a MISSING stream, not a failure — the answer is the same one the check above
            // gives, and an observation under IdOnly is where a misconfigured root is diagnosed.
            ObserveMissingStream();
            return (0UL, 0L);
        }

        using (stream)
        {
            var length = stream.Length;
            return (ScanStream(stream, fromVersion: 1, path).Count, length);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Fully materialized on purpose: the contract permits a lazy view, and this store chooses eager so
    /// a caller may index the list after the store's handle is closed. A rebuild that must not pay for
    /// the head uses <see cref="OpenAsync"/> instead.
    /// <para>
    /// A load is also an OBSERVATION of a stream that is not there, so under
    /// <see cref="FileNameFormat.IdOnly"/> it is the first call that can diagnose a misconfigured root:
    /// a directory entirely in the other naming format raises <see cref="InvalidOperationException"/>
    /// instead of answering "no events" (and an unreadable root raises <see cref="IOException"/>). The
    /// failure is latched — one directory pass in total, then the same exception on every later call.
    /// </para>
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
    /// <remarks>
    /// "No first event" has three causes and this member returns null for all of them: an absent stream,
    /// an empty file and a first line that is not a deserializable event. An absent stream is an
    /// OBSERVATION, so under <see cref="FileNameFormat.IdOnly"/> a peek of an id the store does not have
    /// can raise the same <see cref="InvalidOperationException"/> a misconfigured root raises everywhere
    /// else — once, latched — and an unreadable root raises <see cref="IOException"/>.
    /// </remarks>
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
    /// <remarks>
    /// Version 0 means "no events in this layout", not "the store is fine": an absent stream is an
    /// observation, so under <see cref="FileNameFormat.IdOnly"/> this call can raise the
    /// misconfigured-root <see cref="InvalidOperationException"/> (latched) and an unreadable root raises
    /// <see cref="IOException"/>. An argument error is never hidden behind an observation.
    /// </remarks>
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
    /// <remarks>
    /// The version argument is validated FIRST: <c>fromVersion == 0</c> is an
    /// <see cref="ArgumentOutOfRangeException"/> whether or not the aggregate exists. Past that, an absent
    /// stream yields an empty cursor — but it is an observation, so under
    /// <see cref="FileNameFormat.IdOnly"/> it can raise the misconfigured-root
    /// <see cref="InvalidOperationException"/> (latched) and an unreadable root raises
    /// <see cref="IOException"/>.
    /// </remarks>
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
    /// the ids come from <c>JsonlNames.Classify</c> alone, so the enumeration opens NOTHING.
    /// <para>
    /// One deviation follows from that, and it is deliberate: a stream whose first LINE is corrupt is
    /// enumerated under TypePrefix (its name is a valid stream name) and not under IdOnly (which has no
    /// other way to learn the type). The runtime peeks every id it is given, so recovery still filters
    /// it out.
    /// </para>
    /// <para>
    /// The result is a SNAPSHOT of the moment it was taken: a stream created afterwards is missing from
    /// it, and a stream deleted afterwards may still be in it (the classification pass reads the length
    /// from directory metadata and never opens the files). Callers that must act on a current view read
    /// again, per aggregate, through <see cref="LoadAsync"/> or <see cref="GetVersionAsync"/>.
    /// </para>
    /// <para>
    /// This member owns the loudest case of the store's exception surface: a directory that is ENTIRELY
    /// in the other naming format raises <see cref="InvalidOperationException"/> naming both layouts and
    /// both counts, because enumerating it would report an empty system instead of a misconfigured one.
    /// So does the directory of a <see cref="FileNameFormat.TypePrefix"/> store whose construction
    /// already refused it, and so does the lazy pass a first absent stream triggers. An unreadable root
    /// raises <see cref="IOException"/>. A ZERO-LENGTH file of the other layout is a leftover, not a
    /// history, and does not make a directory misconfigured. The remedy is in the message: migrate the
    /// directory, move the files that do not belong to this store out of it, or set
    /// <see cref="JsonlEventStoreOptions.FileNameFormat"/> to the layout the directory was written with.
    /// </para>
    /// </remarks>
    public async ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType)
    {
        ArgumentNullException.ThrowIfNull(firstEventType);

        var result = new List<Guid>();
        if (!Directory.Exists(_root))
        {
            // A missing root is an empty store. A root that exists as a FILE is not — it is the same
            // misconfiguration <c>ScanRoot</c> raises, so an enumeration answers the same way the first
            // lazy pass would instead of reporting "no aggregates yet".
            if (File.Exists(_root))
                throw RootIsAFile(_root);

            return result;
        }

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
        // Counted AFTER the handle exists: the counter measures what a read COSTS, and an open another
        // process refused to share cost this store nothing but an exception.
        var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete
        );
        Interlocked.Increment(ref _filesOpened);
        return stream;
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
    /// <para>
    /// PRIVATE on purpose: there are exactly two policies — the read one this method names, and
    /// <see cref="AppendingPathFor"/> — and each caller states which it wants. A resolver exposed "just
    /// in case" is a resolver a later caller reaches for from the WRONG path, and on this path that
    /// mistake is a forked history, not a slow read.
    /// </para>
    /// </summary>
    private string? ReadingPathFor(Guid actorId) => PathFor(actorId, trustRememberedMiss: true);

    /// <summary>
    /// The APPEND path's answer: identical to <see cref="ReadingPathFor"/> except in ONE respect — a
    /// remembered miss is not trusted (see <see cref="ResolveTypeMiss"/>).
    /// </summary>
    private string? AppendingPathFor(Guid actorId) => PathFor(actorId, trustRememberedMiss: false);

    private string? PathFor(Guid actorId, bool trustRememberedMiss)
    {
        if (_options.FileNameFormat != FileNameFormat.TypePrefix)
            return IdOnlyPathFor(actorId);

        if (_typeById.TryGetValue(actorId, out var type))
            return KnownPathFor(actorId, type);

        return ResolveTypeMiss(actorId, trustRememberedMiss);
    }

    /// <summary>
    /// The path a RESOLVED prefix names, plus the one warning the read path owes: an id that two prefixes
    /// claim is still readable (a read is not a guard), but an operator who only ever loads the stream
    /// must not have to wait for a refused append to learn that a second history exists beside it.
    /// </summary>
    private string KnownPathFor(Guid actorId, string type)
    {
        if (_conflictingPrefixes.TryGetValue(actorId, out var conflicting))
        {
            string[] others;
            lock (conflicting)
                others = [.. conflicting];

            _logger?.Warning(
                $"Reading aggregate '{actorId:D}' through prefix '{type}': "
                    + $"{others.Length + 1} stream names in '{_root}' claim this id — "
                    + string.Join(", ", others.Select(path => "'" + path + "'"))
                    + ". An append will be refused until one of them is archived."
            );
        }

        return PrefixedPathFor(actorId, type);
    }

    /// <summary>
    /// Resolve a <see cref="FileNameFormat.TypePrefix"/> MISS — one directory pass per DISTINCT unknown id
    /// (single-flight only merges concurrent probes of the SAME id; repeats of that id are free, a
    /// different id pays its own pass), because re-reading the directory per probe of an id the store has
    /// already learned it does not hold is not worth a pass.
    /// <para>
    /// SINGLE-FLIGHT, on the gate a directory pass already holds: the winner scans, a reader that arrived
    /// while it was scanning answers from the double-check inside the gate, and the ids the map is known
    /// not to hold are remembered so the NEXT probe of them costs no pass at all.
    /// </para>
    /// <para>
    /// THE NEGATIVE CACHE IS THE READ PATH'S, and <paramref name="trustRememberedMiss"/> is the whole
    /// difference between the two callers. A read may answer "no stream" from memory: an absence is a fact
    /// about the map THIS instance saw, and a stream another process created beside it is found by the
    /// next pass, not by a probe. An append may NOT — resolving to "no stream" there would create a
    /// SECOND name for an id that already has one, and the file this instance never scanned would become
    /// the other half of a fork. So an append re-scans even when the miss is remembered, and it never
    /// WRITES the cache either: the entry it would record is the one that same append falsifies a moment
    /// later.
    /// </para>
    /// </summary>
    private string? ResolveTypeMiss(Guid actorId, bool trustRememberedMiss)
    {
        lock (_lazyScanGate)
        {
            if (_typeById.TryGetValue(actorId, out var known))
                return KnownPathFor(actorId, known);

            // The map is consulted FIRST, for both callers: a resolved prefix is not an absence, so a
            // stale negative entry can never shadow a stream this instance knows.
            if (trustRememberedMiss && _missingTypes.ContainsKey(actorId))
                return null;

            ScanTypeMap();

            if (_typeById.TryGetValue(actorId, out var type))
                return KnownPathFor(actorId, type);

            if (trustRememberedMiss)
                RememberMissingType(actorId);

            return null;
        }
    }

    /// <summary>
    /// Remember ONE absence for the read path, bounded: past the capacity the set is emptied rather than
    /// grown. IT IS THE ONLY ADDER, and it runs under the scan gate; the single REMOVER
    /// (<see cref="ForgetMissingType"/>) runs outside the gate on purpose.
    /// </summary>
    private void RememberMissingType(Guid actorId)
    {
        if (_missingTypes.Count >= MissingTypeCapacity)
            _missingTypes.Clear();

        _missingTypes.TryAdd(actorId, 0);
    }

    /// <summary>
    /// Drop the ONE remembered miss a successful append of that id has falsified. PER ID, and outside the
    /// scan gate on purpose: an append that learns a new prefix falsifies no other entry, so a wholesale
    /// clear would drop absences that still hold AND serialize every first append of every id against the
    /// one gate a directory pass holds. Under <see cref="FileNameFormat.IdOnly"/> nothing is remembered at
    /// all, so this is never reached there and the counter stays identically zero.
    /// </summary>
    private void ForgetMissingType(Guid actorId)
    {
        if (_missingTypes.TryRemove(actorId, out _))
            Interlocked.Increment(ref _negativeCacheInvalidations);
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
    /// already paid its own pass inside <see cref="ReadingPathFor"/>, and the lazy pass is an IdOnly cost.
    /// </para>
    /// </summary>
    private string? ExistingPathFor(Guid actorId)
    {
        var path = ReadingPathFor(actorId);
        if (path is not null && File.Exists(path))
            return path;

        ThrowIfOccupiedByDirectory(path);
        ObserveMissingStream();
        return null;
    }

    /// <summary>
    /// A stream NAME occupied by a DIRECTORY: the store can neither open it nor create it, and the honest
    /// answer is not "this aggregate is empty" (the read path) or a bare sharing violation (the append
    /// path) but the one sentence that says what is in the way. A null path is just a miss.
    /// </summary>
    private static void ThrowIfOccupiedByDirectory(string? path)
    {
        if (path is null || !Directory.Exists(path))
            return;

        throw new InvalidOperationException(
            $"The stream path '{path}' is a directory: the name is occupied by a directory, so no stream "
                + "file can be read or created there. Move or rename that directory, then retry."
        );
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
    /// <exception cref="IOException">
    /// The directory could not be read. LATCHED: the same failure is raised by every later observation
    /// instead of decaying into "this aggregate is empty".
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The directory is entirely in the OTHER naming format — see
    /// <see cref="MisconfiguredDirectory"/>.
    /// </exception>
    private void ObserveMissingStream()
    {
        // TypePrefix resolves a miss with its own pass (construction, or the path resolver), so the lazy
        // pass is an IdOnly cost: that is the format whose paths are derived without ever looking at the
        // directory.
        if (_options.FileNameFormat != FileNameFormat.IdOnly)
            return;

        var state = Volatile.Read(ref _lazyScanState);
        if (state == LazyScanNotRun)
            state = RunLazyScan();

        // The ORIGINAL exception is re-raised, stack trace and all: an IOException stays an IOException
        // and a misconfiguration keeps the message the operator has to act on.
        if (state == LazyScanFailed)
            ExceptionDispatchInfo.Capture(_lazyScanFailure!).Throw();
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
            catch (Exception ex)
                when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
            {
                // The whole-directory misconfiguration AND an unreadable directory are remembered, never
                // treated as "already scanned": a store whose root cannot be read must not start
                // answering "empty aggregate" after one loud failure, and a miss that is retried per
                // observation would pay the failing pass forever.
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
    /// A pass that finds ZERO configured HISTORIES and SOME OTHER-LAYOUT HISTORIES throws: the store is
    /// pointed at a directory of the other layout, and the honest answer is an error naming both formats
    /// and both counts — not "this store is empty", which is how a whole history disappears from view.
    /// Both counts are of NON-EMPTY files (<c>otherNonEmpty</c> for the foreign side): a zero-length file
    /// is a failed append's leftover, not a history.
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
        {
            // A root that does not exist is an EMPTY store — a store that only reads must not create it.
            // A root that exists as a FILE is not: nothing can be enumerated, opened or written there, so
            // the pass raises the IO failure the enumeration would have raised. The lazy pass LATCHES it,
            // which is what keeps a misconfigured root from costing one failing pass per observation.
            if (File.Exists(_root))
                throw RootIsAFile(_root);

            return [];
        }

        var streams = new List<StreamEntry>();
        long configured = 0;
        long other = 0;
        long otherNonEmpty = 0;

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
                    // Counted by HISTORY, not by name: a ZERO-LENGTH file is a leftover a failed append
                    // leaves behind (rev 5), and it speaks for no history at all. Counting it would let
                    // one leftover raise "configured" to 1 and swallow the whole-directory diagnosis —
                    // a directory of the other layout plus a leftover would enumerate an empty system
                    // in silence, which is the one outcome this rule exists to prevent.
                    if (file.Length > 0)
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
                    // Counted twice, and the two counts answer two different questions. By NAME for the
                    // warning and the monotonic duplicate gate (every foreign entry is worth reporting),
                    // by HISTORY for the whole-directory diagnosis below: a zero-length leftover in the
                    // other layout holds no history at all, so a directory of leftovers is an EMPTY store.
                    other++;
                    if (file.Length > 0)
                        otherNonEmpty++;
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

        // HISTORIES, not names: a directory holding nothing but zero-length files of the other layout is
        // not misconfigured, it is empty — and treating it as misconfigured would throw, latch and raise
        // on every later observation of a store that has no history to lose.
        if (configured == 0 && otherNonEmpty > 0)
            throw MisconfiguredDirectory(configured, otherNonEmpty);

        return streams;
    }

    /// <summary>
    /// The root of a store that cannot have streams: the path exists, and it is a FILE. The IO failure is
    /// the honest answer — the enumeration, the opens and the creates all fail on it — and it is the one
    /// a caller can act on, unlike the empty enumeration a silent "not a directory" would produce.
    /// </summary>
    private static IOException RootIsAFile(string root) =>
        new(
            $"The store root '{root}' is a file, not a directory: no stream can be enumerated, opened or "
                + "created there. Point the store at a directory."
        );

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
                + "would report an empty system instead of a misconfigured one. Migrate the directory, or "
                + "move the files that do not belong to this store out of it, or set "
                + $"'{nameof(JsonlEventStoreOptions)}.{nameof(JsonlEventStoreOptions.FileNameFormat)} = "
                + $"FileNameFormat.{JsonlNames.Opposite(_options.FileNameFormat)}'."
        );

    /// <summary>
    /// Record one configured-format name in the <c>id → type</c> map. The FIRST name wins — the prefix is
    /// fixed when the stream is created — and a SECOND, different one is remembered as a CONFLICT: two
    /// coexisting prefixes for one id are two histories, so an append for that id must refuse instead of
    /// extending whichever name the map happened to learn first.
    /// </summary>
    internal void LearnPrefix(Guid id, string type)
    {
        if (_typeById.TryAdd(id, type))
        {
            // The map GREW. Nothing is dropped from the negative cache here: a resolved prefix is checked
            // BEFORE the cache, so an entry for THIS id can never shadow it, and an entry for ANOTHER id
            // is still a fact about the map this instance saw (the documented staleness). Dropping here
            // would cost the scan gate on every learned prefix — the append hot path most of all.
            return;
        }

        if (!_typeById.TryGetValue(id, out var known))
            return;

        if (!string.Equals(known, type, StringComparison.Ordinal))
            RecordConflictingPrefix(id, PrefixedPathFor(id, type));
    }

    /// <summary>
    /// Add ONE rival name to an id's conflict list. A list, not a single path: the refusal has to name
    /// every file, and it is guarded because a pass may run while another thread resolves a path — the
    /// only mutation is an append, and a path is never removed.
    /// </summary>
    private void RecordConflictingPrefix(Guid id, string path)
    {
        var paths = _conflictingPrefixes.GetOrAdd(id, static _ => []);
        lock (paths)
        {
            if (!paths.Contains(path, StringComparer.Ordinal))
                paths.Add(path);
        }
    }

    /// <summary>
    /// The duplicate guard: one aggregate, ONE layout and ONE prefix. It runs in
    /// <c>AppendAsync</c> before the encode, so a refused append has touched no file at all.
    /// </summary>
    private void GuardAmbiguousStream(Guid actorId, string streamPath)
    {
        // 1. SEVERAL prefix names for one id, seen by a classification pass. Nothing was written, so
        //    every history is intact and an operator can still decide which one survives.
        if (_conflictingPrefixes.TryGetValue(actorId, out var conflicting))
            throw DuplicatePrefixNames(actorId, streamPath, conflicting);

        if (_options.FileNameFormat == FileNameFormat.TypePrefix)
        {
            // 2. The OTHER layout's name IS derivable from the id, so this check is one stat and it is
            //    never gated: a mixed directory cannot grow a second history through this store.
            var otherPath = IdOnlyPathFor(actorId);
            if (new FileInfo(otherPath) is { Exists: true, Length: > 0 })
                throw MixedLayout(actorId, streamPath, otherPath, _options.FileNameFormat);

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

        // The pass returned NON-EMPTY names only, so the single candidate is a second history for this
        // id and the append is refused.
        if (candidates.Count == 1)
            throw MixedLayout(actorId, streamPath, candidates[0], _options.FileNameFormat);
    }

    /// <summary>
    /// ONE id with several stream NAMES in the configured layout: the refusal names the path an append
    /// resolves to and EVERY other name found, so one message carries the whole cleanup.
    /// </summary>
    private InvalidOperationException DuplicatePrefixNames(
        Guid actorId,
        string streamPath,
        List<string> conflicting
    )
    {
        string[] others;
        lock (conflicting)
            others = [.. conflicting];

        var names = string.Join(
            ", ",
            new[] { streamPath }.Concat(others).Select(path => "'" + path + "'")
        );

        return new(
            $"Aggregate '{actorId:D}' has {others.Length + 1} stream names in '{_root}': {names}. A "
                + "stream's prefix is fixed when it is created, so the store cannot tell which history "
                + "an append belongs to. Archive all but one of the files, then retry the append."
        );
    }

    /// <summary>
    /// Every name in the OTHER layout that <paramref name="actorId"/> may already have here — empty for a
    /// single-format directory, and never a leftover: a ZERO-LENGTH file is not a stream (rev 5 lets a
    /// failed append leave one), so it is no rival and does not count towards the multi-candidate
    /// refusal. The caller owns the gate on this directory pass.
    /// </summary>
    private List<string> OtherLayoutPathsFor(Guid actorId)
    {
        // This is a directory pass like any other, and it is the only one an append in a MIXED directory
        // pays: leaving it uncounted would hide the exact cost the guard exists to make visible.
        Interlocked.Increment(ref _directoryScans);

        var matches = new List<string>();

        var root = new DirectoryInfo(_root);
        if (!root.Exists)
            return matches;

        foreach (var file in root.EnumerateFiles())
        {
            if (
                JsonlNames.TryParseOtherFormat(file.Name, _options.FileNameFormat, out var id)
                && id == actorId
                // A zero-length file is a leftover, not a candidate: two of them for one id are zero
                // histories, and refusing the append over them would block a stream with no rival.
                && file.Length > 0
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
        string otherPath,
        FileNameFormat configured
    ) =>
        new(
            $"Aggregate '{actorId:D}' already has a non-empty stream in the OTHER naming format: "
                + $"'{otherPath}'. This append targets '{streamPath}'. The two layouts are never merged, "
                + "so one of the two histories would be lost. Migrate the directory, move the files that "
                + "do not belong to this store out of it, or set "
                + $"'{nameof(JsonlEventStoreOptions)}.{nameof(JsonlEventStoreOptions.FileNameFormat)} = "
                + $"FileNameFormat.{JsonlNames.Opposite(configured)}'."
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
