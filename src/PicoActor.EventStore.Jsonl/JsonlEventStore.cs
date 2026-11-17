namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Tunables of <see cref="JsonlEventStore{TEventBase}"/>.
/// </summary>
public sealed record JsonlEventStoreOptions
{
    /// <summary>
    /// fsync every append. Default <c>false</c>: the bytes are flushed to the OS (so another reader
    /// sees the event immediately) but not to the platter. The documented cost of leaving it off is
    /// that a power loss can leave a TERMINATED garbage line, which the store treats as corruption
    /// and never repairs — an operator removes the line.
    /// </summary>
    public bool FlushToDisk { get; init; }

    /// <summary>
    /// Upper bound of the per-aggregate gate table. Must be greater than zero. An evicted entry costs
    /// one re-scan on that aggregate's next append, never correctness.
    /// </summary>
    public int MaxAggregateLocks { get; init; } = 1024;

    /// <summary>
    /// The store's logger, independent of any DI <c>ILoggerFactory</c>: a host that wants the store's
    /// warnings in its own log passes the same logger explicitly. Null is silent.
    /// </summary>
    public ILogger? Logger { get; init; }
}

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
        IEventStoreEnumerator
    where TEventBase : class, IDomainEvent
{
    private const string StreamSuffix = ".jsonl";
    private const int ScanBufferBytes = 64 * 1024;

    private readonly string _root;
    private readonly JsonlEventStoreOptions _options;
    private readonly ILogger? _logger;
    private readonly JsonlLockTable _locks = new();

    private long _deserializedEvents;
    private long _tornTailLines;
    private long _skippedFileNames;

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
                nameof(options),
                options.MaxAggregateLocks,
                $"{nameof(JsonlEventStoreOptions.MaxAggregateLocks)} must be greater than zero."
            );

        // The trailing separator is normalized BEFORE deriving anything from the root, so "data/" and
        // "data" name the same stream and the same -snapshots sibling.
        _root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(rootDirectory));
        _options = options;
        _logger = options.Logger;
    }

    /// <summary>Events this store has DESERIALIZED (a load or a cursor move), not lines it has read.</summary>
    internal long DeserializedEventCount => Interlocked.Read(ref _deserializedEvents);

    /// <summary>Unterminated final lines seen and ignored — one per crash-torn append.</summary>
    internal long TornTailLines => Interlocked.Read(ref _tornTailLines);

    /// <summary>Directory entries skipped by <see cref="ListAggregateIdsAsync"/> because they are not streams.</summary>
    internal long SkippedFileNames => Interlocked.Read(ref _skippedFileNames);

    /// <summary>Live per-aggregate gate entries.</summary>
    internal int LockEntryCount => _locks.Count;

    /// <summary>The directory the streams live in (the snapshots sit in its sibling).</summary>
    internal string Root => _root;

    /// <inheritdoc/>
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
            Directory.CreateDirectory(_root);

            await using var stream = new FileStream(
                path,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read
            );

            var current = ScanStream(stream, fromVersion: 1, path).Count;
            if (current != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, current);

            if (events.Count == 0)
                return current;

            var payload = Encode(events);
            stream.Seek(0, SeekOrigin.End);
            await stream.WriteAsync(payload, CancellationToken.None).ConfigureAwait(false);
            await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            if (_options.FlushToDisk)
                stream.Flush(flushToDisk: true);

            return current + (ulong)events.Count;
        }
        finally
        {
            entry.Gate.Release();
            _locks.Release(actorId, entry);
        }
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

    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

    private string PathFor(Guid actorId) =>
        Path.Combine(_root, actorId.ToString("D") + StreamSuffix);

    /// <summary>Encode the batch as ONE buffer: <c>json</c> + newline per event, never split.</summary>
    private static byte[] Encode(IReadOnlyList<IDomainEvent> events)
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

            var json = JsonSerializer.SerializeToUtf8Bytes(domainEvent);
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
            throw Wrap(ex, path);
        }

        throw Wrap(new FormatException("the line deserialized to null"), path);
    }

    private static Exception Wrap(Exception inner, string path)
    {
        var message =
            $"Cannot deserialize a line of '{path}' as {typeof(TEventBase).FullName}: {inner.Message} "
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

    private readonly record struct Scan(ulong Count, long Offset);

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

        if (lineStart < absolute)
            ReportTornTail(path);

        // Past the end (or a torn tail): the remaining bytes are where a next event would start, and the
        // caller's capacity is 0 there, so the cursor yields nothing either way.
        if (offset < 0)
            offset = lineStart;

        return new Scan(count, offset);
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
