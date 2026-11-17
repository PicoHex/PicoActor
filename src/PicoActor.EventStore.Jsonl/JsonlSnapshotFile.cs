namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// On-disk shape of one aggregate snapshot: <c>PASJ</c> + little-endian <c>u64</c> version +
/// little-endian <c>u32</c> payload length + the opaque payload (the actor's own <c>PAS1</c> envelope
/// lives inside it, not here).
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

    /// <summary>Bytes before the payload: 4 magic + 8 version + 4 length.</summary>
    internal const int HeaderBytes = 16;

    /// <summary>Encode one snapshot file.</summary>
    internal static byte[] Encode(ulong version, ReadOnlySpan<byte> payload)
    {
        var bytes = new byte[HeaderBytes + payload.Length];
        Magic.CopyTo(bytes);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(4, 8), version);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(12, 4), (uint)payload.Length);
        payload.CopyTo(bytes.AsSpan(HeaderBytes));
        return bytes;
    }

    /// <summary>
    /// Decode a snapshot file. False — "no snapshot", never an exception — for anything a recovery
    /// could not act on: a short header, a bad magic, a version of 0 (covers no events) or a
    /// <c>payloadLength</c> that disagrees with the bytes on disk (a torn write).
    /// </summary>
    internal static bool TryDecode(ReadOnlySpan<byte> file, out ulong version, out byte[] payload)
    {
        version = 0;
        payload = [];

        if (file.Length < HeaderBytes || !file[..4].SequenceEqual(Magic))
            return false;

        version = BinaryPrimitives.ReadUInt64LittleEndian(file[4..12]);
        var length = BinaryPrimitives.ReadUInt32LittleEndian(file[12..16]);

        if (version == 0 || file.Length - HeaderBytes != length)
            return false;

        payload = file[HeaderBytes..].ToArray();
        return true;
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
