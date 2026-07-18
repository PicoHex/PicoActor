# src

PicoNode.Actor source packages.

| Package | Target | Description |
|---------|--------|-------------|
| [PicoNode.Actor.Abs](PicoNode.Actor.Abs) | `netstandard2.0` | Core abstractions — `IActor`, `IActorSystem`, `ICommand`, `IDomainEvent`, `IEventStore`, `Actor`, `EventSourcedActor` |
| [PicoNode.Actor](PicoNode.Actor) | `net10.0` | Runtime — `ActorSystem`, `InMemoryEventStore`, PicoDI integration |

## Build

```bash
dotnet build -c Release
```
