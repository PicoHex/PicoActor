namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// A cursor over one JSONL stream: the length observed at open (<see cref="End"/>), a byte position
/// recorded by the same scan, and a yield budget (<c>cap = End - fromVersion + 1</c>) that is what
/// binds the cursor — never the end of the file. Lines before the position were never read, so a
/// rebuild from a snapshot pays for deserialization of the TAIL only (the head is still paged in once
/// as bytes, which is the documented cost).
/// <para>
/// A terminated line that will not deserialize THROWS: the store does not skip corruption. The final
/// unterminated line is the torn tail a crash leaves — it is reported and ends the cursor.
/// </para>
/// </summary>
internal sealed class JsonlCursor : IEventCursor
{
    private readonly FileStream? _stream;
    private readonly JsonlLineScanner? _scanner;
    private readonly ulong _capacity;
    private readonly ulong _firstVersion;
    private readonly Func<byte[], IDomainEvent>? _deserialize;
    private readonly Action? _reportTornTail;

    private ulong _yielded;
    private bool _exhausted;
    private IDomainEvent? _current;
    private ulong _currentVersion;

    internal JsonlCursor(
        FileStream? stream,
        ulong end,
        ulong capacity,
        ulong firstVersion,
        Func<byte[], IDomainEvent>? deserialize,
        Action? reportTornTail
    )
    {
        _stream = stream;
        _scanner = stream is null ? null : new JsonlLineScanner(stream);
        _capacity = capacity;
        _firstVersion = firstVersion;
        _deserialize = deserialize;
        _reportTornTail = reportTornTail;
        End = end;
        _exhausted = _scanner is null || capacity == 0;
    }

    /// <summary>A cursor over a stream that does not exist: zero length, nothing to yield.</summary>
    internal static JsonlCursor Empty(ulong firstVersion) =>
        new(null, 0, 0, firstVersion, null, null);

    /// <inheritdoc/>
    public ulong End { get; }

    /// <inheritdoc/>
    public IDomainEvent Current => _current ?? throw NoCurrent();

    /// <inheritdoc/>
    public ulong CurrentVersion => _current is null ? throw NoCurrent() : _currentVersion;

    /// <inheritdoc/>
    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // A cancelled or failed move keeps the last produced event readable: it belongs to the move
        // attempt, not to what was already read. Only exhaustion clears it.
        if (_exhausted || _scanner is null || _deserialize is null)
        {
            _current = null;
            return false;
        }

        while (_yielded < _capacity && await _scanner.MoveNextAsync(ct).ConfigureAwait(false))
        {
            if (!_scanner.CurrentTerminated)
            {
                _reportTornTail?.Invoke();
                break;
            }

            var line = _scanner.Current;
            if (JsonlLines.IsBlank(line))
                continue;

            var domainEvent = _deserialize(line);
            _yielded++;
            _current = domainEvent;
            _currentVersion = _firstVersion + _yielded - 1;
            return true;
        }

        _current = null;
        _exhausted = true;
        return false;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        _exhausted = true;

        if (_stream is not null)
            await _stream.DisposeAsync().ConfigureAwait(false);
    }

    private static InvalidOperationException NoCurrent() =>
        new("The cursor has no current event: MoveNextAsync must return true first.");
}
