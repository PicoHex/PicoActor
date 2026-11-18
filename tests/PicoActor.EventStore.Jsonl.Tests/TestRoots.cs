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
    // A Lazy, not a "first caller cleans up" flag: the cleanup deletes the whole Base tree, which
    // RACES with a parallel test's just-created root — the first caller's recursive delete removed a
    // sibling's directory between its CreateDirectory and its first file operation, and that sibling
    // then failed with DirectoryNotFoundException (Bnd9, win-arm64). Lazy (ExecutionAndPublication)
    // runs the delete-and-create exactly once and completes it before any caller gets a root, so no
    // caller can observe a Base that is being removed underneath it.
    private static readonly Lazy<string> Base = new(() =>
    {
        var basePath = Path.Combine(
            Path.GetTempPath(),
            "picoactor-jsonl-tests-" + Environment.ProcessId
        );
        TryDelete(basePath);
        Directory.CreateDirectory(basePath);
        return basePath;
    });

    internal static string Create()
    {
        var root = Path.Combine(Base.Value, Guid.NewGuid().ToString("N"));
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
            // Best effort: a leftover scratch directory is not a test failure. DirectoryNotFoundException
            // is an IOException, so a sibling deleting the same path between the Exists check and the
            // delete lands here too — "not there any more" is the outcome we wanted anyway.
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
