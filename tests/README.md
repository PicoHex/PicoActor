# tests

TUnit test suites for PicoNode.Actor.

| Project | Coverage |
|---------|----------|
| [PicoNode.Actor.Abs.Tests](PicoNode.Actor.Abs.Tests) | `Actor` exception propagation, `UnhandledErrorHandler` hook |
| [PicoNode.Actor.Tests](PicoNode.Actor.Tests) | `ActorSystem` concurrency, `GetAsync` race, `EventSourcedActor` recovery, append failure rollback |

## Run

```bash
dotnet test -c Release
```
