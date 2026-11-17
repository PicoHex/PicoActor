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

        var path = PathFor(actorId);
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
            var payload = Encode(events, path);

            Directory.CreateDirectory(_root);

            await using var stream = new FileStream(
                path,
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
                var seed = ScanStream(stream, fromVersion: 1, path);
                entry.Seed(seed.Count, length);
                scanReportedTornTail = seed.TornTail;
            }
            else if (entry.Length != length)
            {
                var observed = ScanStream(stream, fromVersion: 1, path);
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
                    ReportTornTail(path);

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
        string path,
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
    private (ulong Count, long Length) Inspect(string path)
    {
        if (!File.Exists(path))
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
        if (!File.Exists(path))
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
        if (!File.Exists(path))
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
        if (!File.Exists(path))
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

        var path = PathFor(actorId);
        if (!File.Exists(path))
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
    public async ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType)
    {
        ArgumentNullException.ThrowIfNull(firstEventType);

        var result = new List<Guid>();
        if (!Directory.Exists(_root))
            return result;

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

    private string PathFor(Guid actorId) => IdOnlyPathFor(actorId);

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
