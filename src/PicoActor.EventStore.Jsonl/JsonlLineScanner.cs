namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Pull-based line reader over an open stream: splits on a newline and NOTHING ELSE (a <c>StreamReader</c>
/// would also break on a lone carriage return, which this store's rule does not), and reports the final
/// line as UNTERMINATED so the caller can treat it as the torn tail rather than as an event.
/// <para>
/// The stream is owned by the caller: this type only advances through it.
/// </para>
/// </summary>
internal sealed class JsonlLineScanner
{
    private readonly Stream _stream;
    private readonly byte[] _buffer = new byte[64 * 1024];
    private readonly List<byte> _line = new(256);
    private int _offset;
    private int _count;
    private bool _endOfFile;

    internal JsonlLineScanner(Stream stream) => _stream = stream;

    /// <summary>The line produced by the last successful <see cref="MoveNextAsync"/>.</summary>
    internal byte[] Current { get; private set; } = [];

    /// <summary>False only for the final line of a file that does not end in a newline (the torn tail).</summary>
    internal bool CurrentTerminated { get; private set; }

    /// <summary>Advance to the next line; false once the stream is exhausted.</summary>
    internal async ValueTask<bool> MoveNextAsync(CancellationToken ct)
    {
        _line.Clear();

        while (true)
        {
            if (_offset < _count)
            {
                var newline = Array.IndexOf(_buffer, JsonlLines.NewLine, _offset, _count - _offset);
                if (newline >= 0)
                {
                    _line.AddRange(_buffer.AsSpan(_offset, newline - _offset));
                    _offset = newline + 1;
                    Current = _line.ToArray();
                    CurrentTerminated = true;
                    return true;
                }

                _line.AddRange(_buffer.AsSpan(_offset, _count - _offset));
                _offset = _count;
            }

            if (_endOfFile)
            {
                if (_line.Count == 0)
                    return false;

                Current = _line.ToArray();
                CurrentTerminated = false;
                return true;
            }

            _count = await _stream.ReadAsync(_buffer, ct).ConfigureAwait(false);
            _offset = 0;
            if (_count > 0)
                continue;

            _count = 0;
            _endOfFile = true;
        }
    }
}
