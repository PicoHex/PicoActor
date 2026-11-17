namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The JSONL store satisfies the cursor read port exactly as the bundled in-memory store does: the
/// same <see cref="EventLogReaderConformanceBase"/> suite, over a temp directory instead of a
/// dictionary. Both ports are the same instance — conformance is a property of the pair.
/// </summary>
[InheritsTests]
public sealed class JsonlConformanceTests : EventLogReaderConformanceBase
{
    private string _root = string.Empty;

    protected override (IEventStore Store, IEventLogReader Reader) Create()
    {
        _root = TestRoots.Create();
        var store = new JsonlEventStore<ConformanceEvent>(_root);
        return (store, store);
    }

    /// <summary>
    /// The blank-line fixture: three blank shapes (spaces, a tab, a bare CR) are woven BETWEEN the
    /// stored events — and repeated after the last one — behind the store's back. They are all
    /// TERMINATED lines, so the file is intact; the store must ignore them everywhere (the count,
    /// the cursor position, the loader).
    /// <para>
    /// The interleaving is the point: blanks only at the tail never exercise the cursor's
    /// positioning, which counts EVENTS, not lines. With a block between every pair, a reader that
    /// counted lines would start one event too late per block while still ending at the right count.
    /// </para>
    /// </summary>
    protected override Task InjectBlankLinesAsync(Guid actorId)
    {
        var path = Path.Combine(_root, actorId.ToString("D") + ".jsonl");
        var events = File.ReadAllLines(path);
        // Trailing ASCII spaces, a lone tab and a bare CR — each one '\n'-terminated.
        const string blanks = "   \n\t\n\r\n";

        File.WriteAllText(path, string.Join("\n" + blanks, events) + "\n" + blanks);
        return Task.CompletedTask;
    }
}
