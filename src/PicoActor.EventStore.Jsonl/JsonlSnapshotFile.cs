namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The fast-path calibration a snapshot header carries: the stream positions a rebuild may SEEK to
/// instead of counting lines, plus the bounded content witness that decides whether they still describe
/// THIS stream — and, when they do not, the one-line reason they were dropped.
/// <para>
/// The geometry and the witness answer different questions, and the fast path needs both. The positions
/// can only prove "this byte is the start of a line of this file" and "the file did not shrink"; they are
/// blind to the covered PREFIX being replaced by a DIFFERENT history of the same length or longer, which
/// rebuilds a state the stream never had. The witness is what sees that (spec 2026-11-19, I2).
/// </para>
/// </summary>
/// <param name="StreamLength">Stream byte length the snapshot covers; 0 = unknown.</param>
/// <param name="StreamOffset">Byte position at which the first event AFTER the snapshot's version
/// starts; 0 = unknown.</param>
/// <param name="CoveredPrefixWitness">XOR of the FNV-1a hashes of the first min(64, offset) bytes and of
/// the min(4096, offset) bytes before <paramref name="StreamOffset"/>; 0 = unknown.</param>
/// <param name="UnusableReason">
/// Null when the pair is coherent. Non-null — with every other field zeroed — when the header contradicts
/// itself, and this is the sentence the operator is owed: a calibration that describes nothing must not
/// be dropped in silence.
/// </param>
internal readonly record struct SnapshotCalibration(
    long StreamLength,
    long StreamOffset,
    ulong CoveredPrefixWitness,
    string? UnusableReason = null
)
{
    /// <summary>No calibration at all: a legacy 16-byte header, or a snapshot nothing recorded a position for.</summary>
    internal static readonly SnapshotCalibration Unknown = new(0, 0, 0, null);

    /// <summary>True when a rebuild may try the seek; the store still verifies it against the stream.</summary>
    internal bool IsCalibrated => StreamOffset > 0 && StreamLength > 0;
}

/// <summary>
/// On-disk shape of one aggregate snapshot:
/// <code>
/// "PASJ" | u64 version | u32 payloadLength | u32 reserved(0) | u64 streamLength | u64 streamOffset
///        | u64 coveredPrefixWitness | payload
/// </code>
/// (the actor's own <c>PAS1</c> envelope lives inside the payload, not here).
/// <para>
/// THE THREE CALIBRATION FIELDS ARE THE REBUILD FAST PATH (spec 2026-11-19, D3). <c>streamOffset</c> is the
/// byte position at which the first event AFTER <c>version</c> starts, <c>streamLength</c> is the stream's
/// byte length as of that append, and <c>coveredPrefixWitness</c> is the bounded content witness of the
/// bytes BEFORE that position; all three are 0 = "unknown". A rebuild seeks to the offset instead of
/// counting every line, and the pair of positions is verified against the stream: the recorded LENGTH
/// exposes a truncated (rolled back) stream, the WITNESS exposes a prefix that was rewritten into a
/// different history of the same geometry. A calibration that passes only the geometric tests can
/// describe a stream that never existed — the witness is a NECESSARY part of the fast path, not an extra.
/// </para>
/// <para>
/// THE HEADER IS FORWARD-ONLY, and the <c>reserved</c> u32 is why. An older reader validates
/// <c>file.Length - 16 == payloadLength</c>; a 44-byte header leaves 28 extra bytes on disk, so that
/// check FAILS and the older reader answers "no snapshot" — a full replay, which is the documented, safe
/// degradation (a snapshot is an accelerator). Writing a second, 16-byte layout for those readers would
/// mean two formats that can drift, so there is exactly one.
/// </para>
/// <para>
/// A 16-byte header (magic + version + payloadLength + payload, what an older store wrote) still
/// decodes: <see cref="TryDecode"/> reports <see cref="SnapshotCalibration.Unknown"/> — "no calibration,
/// scan" — and that is not a fault, so it raises no warning.
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
    /// 8 stream offset + 8 covered-prefix witness. The field offsets below are that list, written once.
    /// </summary>
    internal const int HeaderBytes = 44;

    /// <summary>Bytes before the payload of the ORIGINAL layout (magic + version + length), still read.</summary>
    internal const int LegacyHeaderBytes = 16;

    private const int VersionOffset = 4;
    private const int PayloadLengthOffset = 12;
    private const int ReservedOffset = 16;
    private const int StreamLengthOffset = 20;
    private const int StreamOffsetOffset = 28;
    private const int CoveredPrefixWitnessOffset = 36;

    /// <summary>
    /// Encode one snapshot file. <paramref name="calibration"/> is the CAPTURED positions and witness the
    /// append path recorded (0 = unknown); nothing here measures the stream.
    /// </summary>
    internal static byte[] Encode(
        ulong version,
        ReadOnlySpan<byte> payload,
        SnapshotCalibration calibration
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(calibration.StreamLength);
        ArgumentOutOfRangeException.ThrowIfNegative(calibration.StreamOffset);

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
            (ulong)calibration.StreamLength
        );
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes.AsSpan(StreamOffsetOffset, 8),
            (ulong)calibration.StreamOffset
        );
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes.AsSpan(CoveredPrefixWitnessOffset, 8),
            calibration.CoveredPrefixWitness
        );
        payload.CopyTo(bytes.AsSpan(HeaderBytes));
        return bytes;
    }

    /// <summary>
    /// Decode a snapshot file. False — "no snapshot", never an exception — for anything a recovery
    /// could not act on: a short header, a bad magic, a version of 0 (covers no events) or a
    /// <c>payloadLength</c> that disagrees with the bytes on disk (a torn write).
    /// <para>
    /// Both a 44-byte (current) and a 16-byte (original) header decode; the original one reports
    /// <see cref="SnapshotCalibration.Unknown"/> — "unknown" — which is what makes its recoveries scan.
    /// A pair of positions that contradicts itself is reported as unknown too, never half-believed, but
    /// WITH <see cref="SnapshotCalibration.UnusableReason"/> set: the caller owns the logger, so the
    /// reason travels out of the decoder as a sentence instead of a silently zeroed pair.
    /// </para>
    /// </summary>
    internal static bool TryDecode(
        ReadOnlySpan<byte> file,
        out ulong version,
        out byte[] payload,
        out SnapshotCalibration calibration
    )
    {
        version = 0;
        payload = [];
        calibration = SnapshotCalibration.Unknown;

        if (file.Length < LegacyHeaderBytes || !file[..4].SequenceEqual(Magic))
            return false;

        version = BinaryPrimitives.ReadUInt64LittleEndian(file[VersionOffset..][..8]);
        var length = BinaryPrimitives.ReadUInt32LittleEndian(file[PayloadLengthOffset..][..4]);

        if (version == 0)
            return false;

        // The CURRENT layout first: its header is longer, so "the length field equals what is left on
        // disk" can be true of only one of the two layouts — a 16-byte file leaves at least 28 bytes
        // unaccounted for by this test, and a 44-byte one leaves exactly the payload.
        if (file.Length >= HeaderBytes && file.Length - HeaderBytes == length)
        {
            payload = file[HeaderBytes..].ToArray();

            var coveredLength = (long)
                BinaryPrimitives.ReadUInt64LittleEndian(file[StreamLengthOffset..][..8]);
            var firstEventOffset = (long)
                BinaryPrimitives.ReadUInt64LittleEndian(file[StreamOffsetOffset..][..8]);
            var witness = BinaryPrimitives.ReadUInt64LittleEndian(
                file[CoveredPrefixWitnessOffset..][..8]
            );

            // A pair that contradicts itself describes nothing a rebuild can use: the position of a tail
            // cannot be past the end of the stream it is the tail of. Both are dropped together
            // (0 = unknown), never half-believed — and the reason is carried out, because the operator
            // reads the fast path's degradation in the log, not in a header dump.
            calibration = ReadCalibration(coveredLength, firstEventOffset, witness);
            return true;
        }

        // The ORIGINAL layout: no calibration to report, so a rebuild scans. Written by a previous
        // release, and still first-class here — and not a fault, so it carries no reason.
        if (file.Length - LegacyHeaderBytes == length)
        {
            payload = file[LegacyHeaderBytes..].ToArray();
            return true;
        }

        return false;
    }

    /// <summary>
    /// The calibration a header describes, or the reason it describes NONE. Half-calibrations are the
    /// interesting case: a position with no covered length cannot be checked for a shrink, and a covered
    /// length with no position has nothing to seek to — each is reported as unknown WITH its own reason
    /// rather than passed through as a number a rebuild would act on.
    /// </summary>
    private static SnapshotCalibration ReadCalibration(
        long coveredLength,
        long firstEventOffset,
        ulong witness
    )
    {
        // A negative field is the high bit of a value no append can produce: a corrupt header.
        if (coveredLength < 0 || firstEventOffset < 0)
            return new SnapshotCalibration(
                0,
                0,
                0,
                $"its positions do not fit in a stream length ({coveredLength}/{firstEventOffset})"
            );

        if (firstEventOffset == 0 && coveredLength == 0)
            return SnapshotCalibration.Unknown;

        if (firstEventOffset > 0 && coveredLength == 0)
            return new SnapshotCalibration(
                0,
                0,
                0,
                $"it records the stream position {firstEventOffset} but no covered stream length"
            );

        if (firstEventOffset == 0 && coveredLength > 0)
            return new SnapshotCalibration(
                0,
                0,
                0,
                $"it records the {coveredLength} bytes it covers but no stream position"
            );

        if (firstEventOffset > coveredLength)
            return new SnapshotCalibration(
                0,
                0,
                0,
                $"the stream position {firstEventOffset} is past the {coveredLength} bytes it covers"
            );

        return new SnapshotCalibration(coveredLength, firstEventOffset, witness, null);
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
