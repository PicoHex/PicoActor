namespace PicoActor.Tests;

internal sealed record PublishRecord(
    Guid ActorId,
    ulong Version,
    IReadOnlyList<IDomainEvent> Events
);

internal sealed class RecordingPublisher : IDomainEventPublisher
{
    public List<PublishRecord> Published { get; } = [];
    public bool ThrowOnPublish { get; set; }

    public ValueTask PublishAsync(Guid actorId, ulong version, IReadOnlyList<IDomainEvent> events)
    {
        if (ThrowOnPublish)
            throw new InvalidOperationException("publisher failure");
        Published.Add(new PublishRecord(actorId, version, events.ToList()));
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Tests for the IDomainEventPublisher hook: events are published after
/// persist+mutate, replay stays silent, publisher failures never break the actor.
/// </summary>
public sealed class ActorSystemPublisherTests
{
    private static void RegisterCounter(ActorSystem system)
    {
        system.Register<Counter>(
            cmd =>
                cmd switch
                {
                    CreateCounter c => new Counter(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new Counter()
        );
    }

    [Test]
    public async Task Create_EventSourcedActor_PublishesConstructionEvents()
    {
        var publisher = new RecordingPublisher();
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = new InMemoryEventStore(),
                DomainEventPublisher = publisher,
            }
        );
        RegisterCounter(system);

        var counter = await system.CreateAsync<Counter>(new CreateCounter(7));

        await Assert.That(publisher.Published.Count).IsEqualTo(1);
        await Assert.That(publisher.Published[0].ActorId).IsEqualTo(counter.Id);
        await Assert.That(publisher.Published[0].Version).IsEqualTo(1UL);
        await Assert.That(publisher.Published[0].Events[0]).IsTypeOf<CounterCreated>();
    }

    [Test]
    public async Task Message_RaisedEvents_PublishedAfterPersist()
    {
        var publisher = new RecordingPublisher();
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = new InMemoryEventStore(),
                DomainEventPublisher = publisher,
            }
        );
        RegisterCounter(system);

        var counter = await system.CreateAsync<Counter>(new CreateCounter(0));
        await system.AskAsync<object?>(counter.Id, new Increment(5));

        await Assert.That(publisher.Published.Count).IsEqualTo(2);
        var second = publisher.Published[1];
        await Assert.That(second.ActorId).IsEqualTo(counter.Id);
        await Assert.That(second.Version).IsEqualTo(2UL);
        await Assert.That(second.Events[0]).IsTypeOf<CounterIncremented>();
    }

    [Test]
    public async Task PublisherFailure_DoesNotBreakActor()
    {
        var publisher = new RecordingPublisher { ThrowOnPublish = true };
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = new InMemoryEventStore(),
                DomainEventPublisher = publisher,
            }
        );
        RegisterCounter(system);

        // Create must not throw even though the publisher blows up
        var counter = await system.CreateAsync<Counter>(new CreateCounter(5));

        // Actor remains fully functional
        var value = await system.AskAsync<int>(counter.Id, new GetValue());
        await Assert.That(value).IsEqualTo(5);
    }

    [Test]
    public async Task Rebuild_DoesNotPublish()
    {
        var store = new InMemoryEventStore();
        var publisherA = new RecordingPublisher();
        var systemA = new ActorSystem(
            new ActorSystemOptions { EventStore = store, DomainEventPublisher = publisherA }
        );
        RegisterCounter(systemA);
        var counter = await systemA.CreateAsync<Counter>(new CreateCounter(7));

        // Fresh system over the same store: rebuild replays events — replay must
        // be silent by construction (no publish on the recovery path).
        var publisherB = new RecordingPublisher();
        var systemB = new ActorSystem(
            new ActorSystemOptions { EventStore = store, DomainEventPublisher = publisherB }
        );
        RegisterCounter(systemB);

        var rebuilt = await systemB.GetAsync<Counter>(counter.Id);
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(publisherB.Published.Count).IsEqualTo(0);
    }

    [Test]
    public async Task SagaResume_PublishesNewEvents()
    {
        var store = new InMemoryEventStore();

        // Seed a saga interrupted after step 1 (crash simulation)
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new SagaStep1Started("partial")]);

        // Rebuild on a fresh system: ReplayEvents restores step 1, ResumeAsync
        // raises NEW events (step2 + completed) which must be published — the
        // replayed event itself must NOT be republished.
        var publisher = new RecordingPublisher();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = store, DomainEventPublisher = publisher }
        );
        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var rebuilt = await system.GetAsync<TestSaga>(sagaId);
        await Assert.That(rebuilt).IsNotNull();

        // GetAsync returns before OnReadyAsync (resume → flush → publish) runs on
        // the loop thread — wait for the publish (the saga auto-stops right after).
        for (var i = 0; i < 50 && publisher.Published.Count == 0; i++)
            await Task.Delay(20);

        await Assert.That(publisher.Published.Count).IsEqualTo(1);
        var record = publisher.Published[0];
        await Assert.That(record.ActorId).IsEqualTo(sagaId);
        // Replayed event excluded — only resume-produced events
        await Assert.That(record.Events.Count).IsEqualTo(2);
        await Assert.That(record.Events[0]).IsTypeOf<SagaStep2Done>();
        await Assert.That(record.Events[1]).IsTypeOf<PicoActor.Abs.SagaCompleted>();
    }
}
