namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// On-disk shape of one aggregate snapshot:
/// <code>
/// "PASJ" | u64 version | u32 payloadLength | u32 reserved(0) | u64 streamLength | u64 streamOffset | payload
/// </code>
/// (the actor's own <c>PAS1</c> envelope lives inside the payload, not here).
/// <para>
/// THE TWO LENGTH FIELDS ARE THE REBUILD FAST PATH (spec 2026-11-19, D3). <c>streamOffset</c> is the
/// byte position at which the first event AFTER <c>version</c> starts, and <c>streamLength</c> is the
/// stream's byte length as of that append; both are 0 = "unknown". A rebuild seeks to the offset instead
/// of counting every line, and the recorded LENGTH is what exposes a truncated (rolled back) stream.
/// </para>
/// <para>
/// THE HEADER IS FORWARD-ONLY, and the <c>reserved</c> u32 is why. An older reader validates
/// <c>file.Length - 16 == payloadLength</c>; a 36-byte header leaves 20 extra bytes on disk, so that
/// check FAILS and the older reader answers "no snapshot" — a full replay, which is the documented, safe
/// degradation (a snapshot is an accelerator). Writing a second, 16-byte layout for those readers would
/// mean two formats that can drift, so there is exactly one.
/// </para>
/// <para>
/// A 16-byte header (magic + version + payloadLength + payload, what an older store wrote) still
/// decodes: <see cref="TryDecode"/> reports both offset fields as 0, i.e. "unknown, scan".
/// </para>
/// <para>
/// Multi-byte fields go through <see cref="BinaryPrimitives"/> and never through
/// <c>BitConverter</c>: the format is little-endian ON EVERY HOST, so a file written on one machine is
/// readable on the next regardless of its native order.
/// </para>
/// <para>
/// The write is atomic: a temporary sibling in the SAME directory, flushed to disk, then renamed over
/// the target. A reader therefore sees the previous snapshot or the new one, never a half-written
/// payload — a snapshot is an accelerator, so its durability is independent of
/// <see cref="JsonlEventStoreOptions.FlushToDisk"/> and always flush-to-disk.
/// </para>
/// </summary>
internal static class JsonlSnapshotFile
{
    /// <summary>The container magic: "Polymorphic Actor Store JSON".</summary>
    internal static ReadOnlySpan<byte> Magic => "PASJ"u8;

    /// <summary>
    /// Bytes before the payload: 4 magic + 8 version + 4 payload length + 4 reserved + 8 stream length +
    /// 8 stream offset. The field offsets below are that list, written once.
    /// </summary>
    internal const int HeaderBytes = 36;

    /// <summary>Bytes before the payload of the ORIGINAL layout (magic + version + length), still read.</summary>
    internal const int LegacyHeaderBytes = 16;

    private const int VersionOffset = 4;
    private const int PayloadLengthOffset = 12;
    private const int ReservedOffset = 16;
    private const int StreamLengthOffset = 20;
    private const int StreamOffsetOffset = 28;

    /// <summary>
    /// Encode one snapshot file. <paramref name="streamLength"/> and <paramref name="streamOffset"/> are
    /// the CAPTURED positions the append path recorded (0 = unknown); nothing here measures the stream.
    /// </summary>
    internal static byte[] Encode(
        ulong version,
        ReadOnlySpan<byte> payload,
        long streamLength,
        long streamOffset
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(streamLength);
        ArgumentOutOfRangeException.ThrowIfNegative(streamOffset);

        var bytes = new byte[HeaderBytes + payload.Length];
        Magic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(VersionOffset, 8), version);
        BinaryPrimitives.WriteUInt32LittleEndian(
            bytes.AsSpan(PayloadLengthOffset, 4),
            (uint)payload.Length
        );

        // Reserved stays ZERO, and it is load-bearing: it is what an older 16-byte reader walks into, so
        // its length check fails instead of reading a payload of the wrong length (see the summary).
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(ReservedOffset, 4), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes.AsSpan(StreamLengthOffset, 8),
            (ulong)streamLength
        );
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes.AsSpan(StreamOffsetOffset, 8),
            (ulong)streamOffset
        );
        payload.CopyTo(bytes.AsSpan(HeaderBytes));
        return bytes;
    }

    /// <summary>
    /// Decode a snapshot file. False — "no snapshot", never an exception — for anything a recovery
    /// could not act on: a short header, a bad magic, a version of 0 (covers no events) or a
    /// <c>payloadLength</c> that disagrees with the bytes on disk (a torn write).
    /// <para>
    /// Both a 36-byte (current) and a 16-byte (original) header decode; the original one reports
    /// <paramref name="streamLength"/> and <paramref name="streamOffset"/> as 0 — "unknown" — which is
    /// what makes its recoveries scan. A calibration that contradicts itself (a tail starting past the
    /// end of its own stream) is reported as 0/0 too, never half-believed.
    /// </para>
    /// </summary>
    internal static bool TryDecode(
        ReadOnlySpan<byte> file,
        out ulong version,
        out byte[] payload,
        out long streamLength,
        out long streamOffset
    )
    {
        version = 0;
        payload = [];
        streamLength = 0;
        streamOffset = 0;

        if (file.Length < LegacyHeaderBytes || !file[..4].SequenceEqual(Magic))
            return false;

        version = BinaryPrimitives.ReadUInt64LittleEndian(file[VersionOffset..][..8]);
        var length = BinaryPrimitives.ReadUInt32LittleEndian(file[PayloadLengthOffset..][..4]);

        if (version == 0)
            return false;

        // The CURRENT layout first: its header is longer, so "the length field equals what is left on
        // disk" can be true of only one of the two layouts — a 16-byte file leaves at least 20 bytes
        // unaccounted for by this test, and a 36-byte one leaves exactly the payload.
        if (file.Length >= HeaderBytes && file.Length - HeaderBytes == length)
        {
            payload = file[HeaderBytes..].ToArray();

            var coveredLength = (long)
                BinaryPrimitives.ReadUInt64LittleEndian(file[StreamLengthOffset..][..8]);
            var firstEventOffset = (long)
                BinaryPrimitives.ReadUInt64LittleEndian(file[StreamOffsetOffset..][..8]);

            // A pair that contradicts itself describes nothing a rebuild can use: the position of a tail
            // cannot be past the end of the stream it is the tail of. Both are dropped together
            // (0 = unknown), never half-believed.
            if (coveredLength >= 0 && firstEventOffset >= 0 && firstEventOffset <= coveredLength)
            {
                streamLength = coveredLength;
                streamOffset = firstEventOffset;
            }

            return true;
        }

        // The ORIGINAL layout: no calibration to report, so a rebuild scans. Written by a previous
        // release, and still first-class here.
        if (file.Length - LegacyHeaderBytes == length)
        {
            payload = file[LegacyHeaderBytes..].ToArray();
            return true;
        }

        return false;
    }

    /// <summary>Write <paramref name="bytes"/> through a temp file + flush + rename.</summary>
    internal static void WriteAtomic(string path, ReadOnlySpan<byte> bytes)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var temp = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (
                var stream = new FileStream(
                    temp,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 16 * 1024,
                    FileOptions.WriteThrough
                )
            )
            {
                stream.Write(bytes);
                stream.Flush(flushToDisk: true);
            }

            File.Move(temp, path, overwrite: true);
        }
        catch
        {
            // A failed write may leave the temp file behind, and a leftover .tmp beside the real
            // snapshots would be one more file per failure, forever. Best-effort removal, then the
            // ORIGINAL exception — the caller reports a snapshot failure, never a command failure.
            TryDelete(temp);
            throw;
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Nothing above this call can see THIS failure, and it must not replace the original one.
            // It goes to the debug console because the store's logger belongs to the store, and the
            // leftover file is worth seeing in a diagnosis of a repeated snapshot failure.
            System.Diagnostics.Debug.WriteLine(
                $"Could not remove the snapshot temp file {path}: {ex.Message}"
            );
        }
    }
}
