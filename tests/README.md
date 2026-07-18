# tests

TUnit test suites for PicoActor.

| Project | Coverage |
|---------|----------|
| [PicoActor.Abs.Tests](PicoActor.Abs.Tests) | `Actor` exception propagation, `UnhandledErrorHandler` hook |
| [PicoActor.Tests](PicoActor.Tests) | `ActorSystem` concurrency, `GetAsync` race, `EventSourcedActor` recovery, append failure rollback |

## Run

```bash
dotnet test -c Release
```
