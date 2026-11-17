using PicoDI.Abs;

namespace PicoActor.EventStore.InMemory;

/// <summary>
/// PicoDI registration for the in-memory store — the one convenience the core deliberately does
/// not ship: <c>PicoActor</c> registers no concrete store, so a host that wants the in-memory one
/// asks for it here.
/// </summary>
public static class InMemoryStoreDiExtensions
{
    /// <summary>
    /// Registers <see cref="InMemoryEventStore"/> as the container's <see cref="IEventStore"/>
    /// singleton.
    /// <para>
    /// Order against <c>AddPicoActor()</c> is free in both directions: the store is resolved
    /// inside the <see cref="IActorSystem"/> factory, never captured at registration time. Calling
    /// this AFTER <c>AddPicoActor(store)</c> replaces that explicit instance — the last
    /// registration wins, so register the store you actually mean.
    /// </para>
    /// </summary>
    public static ISvcContainer AddPicoActorInMemoryStore(this ISvcContainer container)
    {
        ArgumentNullException.ThrowIfNull(container);

        container.Register(
            typeof(IEventStore),
            _ => new InMemoryEventStore(),
            SvcLifetime.Singleton
        );

        return container;
    }
}
