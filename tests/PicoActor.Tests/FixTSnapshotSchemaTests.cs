namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// T — snapshot payload schema envelope (PAS1).
//
// A snapshot payload is opaque bytes: nothing in it says which STATE SHAPE produced
// it. After a shape change the old bytes still deserialize (same length, same types)
// and the aggregate comes back WRONG with no exception anywhere. EventSourcedActor
// therefore stamps a tag the actor declares (SnapshotSchema) into a framework-owned
// envelope and refuses a payload whose tag does not match, degrading to a full replay
// — a snapshot is acceleration only, so refusing one always costs speed, never truth.
// ═══════════════════════════════════════════════════════════

public sealed class FixTSnapshotSchemaTests
{
    internal sealed record SchemaCreate(int Start) : ICommand;

    internal sealed record SchemaAdd(int Delta) : ICommand;

    internal sealed record SchemaGet : ICommand;

    internal sealed record SchemaValueSet(int Value) : IDomainEvent;

    internal sealed record SchemaValueAdded(int Delta) : IDomainEvent;

    /// <summary>
    /// Counter aggregate whose payload is an <see cref="int"/>. The tag is settable so a
    /// rebuild instance can declare a DIFFERENT one; <see cref="MutateCalls"/> counts replay
    /// work (a rejected snapshot replays the whole stream).
    /// </summary>
    internal sealed class SchemaCounter : EventSourcedActor
    {
        private int _value;

        public SchemaCounter(SchemaCreate cmd)
            : base(cmd) { }

        public SchemaCounter() { }

        /// <summary>The declared tag; null = no envelope at all.</summary>
        public string? Tag { get; init; }

        /// <summary>Optional sink for <see cref="OnSnapshotFailed"/> reports (null = not observed).</summary>
        public SnapshotFailureSink? Failures { get; init; }

        /// <summary>Make reading <see cref="SnapshotSchema"/> throw.</summary>
        public bool ThrowOnSchema { get; init; }

        /// <summary>Make <see cref="TakeSnapshot"/> throw.</summary>
        public bool ThrowOnTake { get; init; }

        /// <summary>Number of Mutate calls on THIS instance (live and replay alike).</summary>
        public int MutateCalls { get; private set; }

        protected override string? SnapshotSchema =>
            ThrowOnSchema ? throw new InvalidOperationException("schema boom") : Tag;

        protected override ValueTask<object?> OnMessageAsync(ICommand command)
        {
            switch (command)
            {
                case SchemaCreate c:
                    RaiseEvent(new SchemaValueSet(c.Start));
                    return default;
                case SchemaAdd a:
                    RaiseEvent(new SchemaValueAdded(a.Delta));
                    return default;
                case SchemaGet:
                    return new ValueTask<object?>(_value);
                default:
                    return default;
            }
        }

        protected override void Mutate(IDomainEvent e)
        {
            MutateCalls++;
            if (e is SchemaValueSet s)
                _value = s.Value;
            else if (e is SchemaValueAdded a)
                _value += a.Delta;
        }

        protected override void OnSnapshotFailed(Exception ex) => Failures?.Add(ex);

        protected override byte[]? TakeSnapshot() =>
            ThrowOnTake
                ? throw new InvalidOperationException("take boom")
                : BitConverter.GetBytes(_value);

        protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload) =>
            _value = BitConverter.ToInt32(payload);
    }

    private static ActorSystem NewSystem(InMemoryEventStore store, int? snapshotEvery = null) =>
        new(new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = snapshotEvery });

    /// <summary>Same tag on the create and the rebuild factory (the normal, type-level case).</summary>
    private static void RegisterSameTag(
        ActorSystem system,
        string? tag,
        SnapshotFailureSink? failures = null,
        bool throwOnSchema = false,
        bool throwOnTake = false
    ) =>
        system.Register<SchemaCounter>(
            c => new SchemaCounter((SchemaCreate)c)
            {
                Tag = tag,
                Failures = failures,
                ThrowOnSchema = throwOnSchema,
                ThrowOnTake = throwOnTake,
            },
            () =>
                new SchemaCounter
                {
                    Tag = tag,
                    Failures = failures,
                    ThrowOnSchema = throwOnSchema,
                    ThrowOnTake = throwOnTake,
                }
        );

    /// <summary>
    /// Different tag per factory. Literal rebuild tag: null here really means null, never
    /// falls back to the create tag.
    /// </summary>
    private static void RegisterTagChange(
        ActorSystem system,
        string? createTag,
        string? rebuildTag,
        SnapshotFailureSink? failures = null
    ) =>
        system.Register<SchemaCounter>(
            c => new SchemaCounter((SchemaCreate)c) { Tag = createTag, Failures = failures },
            () => new SchemaCounter { Tag = rebuildTag, Failures = failures }
        );

    [Test]
    [Timeout(15000)]
    public async Task Envelope_IsWritten_WithUtf8ByteLength(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, "订单-v1");
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 4; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // 5 events -> v5, value 4

        var snap = await store.LoadSnapshotAsync(actor.Id);
        await Assert.That(snap).IsNotNull();
        var tag = "订单-v1";
        var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
        await Assert.That(snap!.Payload.Length).IsEqualTo(5 + tagBytes.Length + 4);
        await Assert.That(snap.Payload[0..4].SequenceEqual("PAS1"u8.ToArray())).IsTrue();
        await Assert.That(snap.Payload[4]).IsEqualTo((byte)tagBytes.Length); // 9 bytes; do not hard-code
        await Assert.That(snap.Payload[5..(5 + tagBytes.Length)].SequenceEqual(tagBytes)).IsTrue();
        await Assert.That(BitConverter.ToInt32(snap.Payload, 5 + tagBytes.Length)).IsEqualTo(4);
    }

    [Test]
    [Timeout(15000)]
    public async Task NullSchema_WritesRawPayload_Unchanged(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, null);
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 4; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
        var snap = await store.LoadSnapshotAsync(actor.Id);
        await Assert.That(snap!.Payload.Length).IsEqualTo(4);
        await Assert.That(BitConverter.ToInt32(snap.Payload)).IsEqualTo(4);
    }

    [Test]
    [Timeout(15000)]
    public async Task Envelope_IsApplied_ReplayingOnlyTheTail(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, "v1");
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // 12 events, snapshot at v10 (value 9)
        await system.StopAsync(actor.Id);
        var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
    }

    [Test]
    [Timeout(15000)]
    public async Task LegacyRawPayload_WithNullSchema_IsApplied(
        CancellationToken cancellationToken = default
    ) // regression pin: green on base
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, null);
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
        await system.StopAsync(actor.Id);
        // v10's state value is 9 (value = version - 1); overwrite with a raw (unenveloped) payload
        await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, BitConverter.GetBytes(9)));
        var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
    }

    [Test]
    [Timeout(15000)]
    public async Task EmptyTag_IsARealTag_WrittenWithZeroLength(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, "");
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
        var snap = await store.LoadSnapshotAsync(actor.Id);
        await Assert.That(snap!.Payload.Length).IsEqualTo(5 + 4);
        await Assert.That(snap.Payload[4]).IsEqualTo((byte)0);
        await system.StopAsync(actor.Id);
        var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
    }

    [Test]
    [Timeout(15000)]
    public async Task Envelope_IsRestoredTwiceFromTheSameStoredSnapshot(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, "v1");
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // snapshot at v10
        await system.StopAsync(actor.Id);
        // Copy: the store hands out the same instance, so a mutating restore would be invisible
        // in a comparison against a live view.
        var before = (await store.LoadSnapshotAsync(actor.Id))!.Payload.ToArray();

        var first = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(first!.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(first.Id, new SchemaGet())).IsEqualTo(11);
        await system.StopAsync(actor.Id);
        var second = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(second!.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(second.Id, new SchemaGet())).IsEqualTo(11);
        await Assert
            .That((await store.LoadSnapshotAsync(actor.Id))!.Payload.SequenceEqual(before))
            .IsTrue();
    }

    [Test]
    [Timeout(15000)]
    public async Task TruncatedEnvelope_IsRejected_WithInvalidOperationException(
        CancellationToken cancellationToken = default
    )
    {
        // Deterministic: call the public interface directly on an unattached instance.
        var probe = new SchemaCounter();
        var truncated = new byte[] { 0x50, 0x41, 0x53, 0x31, 0x09, 0x01, 0x02 }; // magic + schemaLen 9, 2 bytes left
        var ex = await Assert
            .That(() => ((ISnapshotableActor)probe).RestoreSnapshot(10, truncated))
            .Throws<InvalidOperationException>();
        await Assert.That(ex!.Message).Contains("truncated");

        // End-to-end: the stored truncated envelope degrades to a full replay.
        var store = new InMemoryEventStore();
        var system = NewSystem(store, snapshotEvery: 5);
        RegisterSameTag(system, "v1");
        var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
        await system.StopAsync(actor.Id);
        await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, truncated));
        var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
        await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);
        await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
    }
}
