namespace PicoActor.EventStore.InMemory.Tests;

/// <summary>The bundled in-memory store satisfies the cursor read port.</summary>
[InheritsTests]
public sealed class EventLogReaderConformanceTests : EventLogReaderConformanceBase
{
    protected override (IEventStore Store, IEventLogReader Reader) Create()
    {
        var store = new InMemoryEventStore();
        return (store, store);
    }
}
