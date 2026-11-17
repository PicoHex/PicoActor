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
    /// The blank-line fixture: three blank shapes (spaces, a tab, a bare CR) are appended to the
    /// stream file behind the store's back. They are TERMINATED lines, so the file is intact — the
    /// store must ignore them everywhere (the count, the cursor position, the loader).
    /// </summary>
    protected override Task InjectBlankLinesAsync(Guid actorId)
    {
        File.AppendAllText(Path.Combine(_root, actorId.ToString("D") + ".jsonl"), "   \n\t\n\r\n");
        return Task.CompletedTask;
    }
}
