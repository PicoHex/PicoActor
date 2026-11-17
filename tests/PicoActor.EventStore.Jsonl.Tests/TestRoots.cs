namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// Per-test-root scratch directories. Every store test owns a FRESH directory (one store instance per
/// directory is the documented concurrency boundary, so a shared root would let two suites trip over
/// each other), and the whole run's scratch tree is removed at the start of the next run — deleting
/// the root the previous run left behind, rather than trying to delete a directory a parallel test may
/// still hold open.
/// </summary>
internal static class TestRoots
{
    private static readonly string Base = Path.Combine(
        Path.GetTempPath(),
        "picoactor-jsonl-tests-" + Environment.ProcessId
    );

    private static int _cleaned;

    internal static string Create()
    {
        if (Interlocked.Exchange(ref _cleaned, 1) == 0)
            TryDelete(Base);

        var root = Path.Combine(Base, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
    }

    internal static void TryDelete(string directory)
    {
        try
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
            // Best effort: a leftover scratch directory is not a test failure.
        }
        catch (UnauthorizedAccessException)
        {
            // Best effort, same reason.
        }
    }
}

/// <summary>Scratch root that deletes itself, for the tests that can own their directory.</summary>
internal sealed class TempRoot : IDisposable
{
    internal string Root { get; } = TestRoots.Create();

    public void Dispose() => TestRoots.TryDelete(Root);
}
