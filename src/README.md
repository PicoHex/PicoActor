# src

PicoActor source packages.

| Package | Target | Description |
|---------|--------|-------------|
| [PicoActor.Abs](PicoActor.Abs) | `net10.0` | Core abstractions — `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor`, `SagaActor`, snapshot/passivation contracts (`IEventStoreSnapshot`, `ActorSnapshot`, `ISnapshotableActor`, `IActorPassivationHost`), subscription types (`IDomainEventSubscriber<TEvent>`, `DomainEventEnvelope`, `ICommandSender`) + embedded `PicoActor.Gen` analyzer |
| [PicoActor](PicoActor) | `net10.0` | Runtime — `ActorSystem` (snapshot acceleration + idle passivation options), `InMemoryEventStore`, `MediatorDomainEventPublisher`, PicoDI integration |

## Build

```bash
dotnet build -c Release
```
