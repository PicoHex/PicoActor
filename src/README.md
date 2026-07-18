# src

PicoActor source packages.

| Package | Target | Description |
|---------|--------|-------------|
| [PicoActor.Abs](PicoActor.Abs) | `netstandard2.0` | Core abstractions — `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoActor](PicoActor) | `net10.0` | Runtime — `ActorSystem`, `InMemoryEventStore`, PicoDI integration |

## Build

```bash
dotnet build -c Release
```
