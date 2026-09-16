# tests

TUnit test suites for PicoActor.

| Project | Coverage |
|---------|----------|
| [PicoActor.Abs.Tests](PicoActor.Abs.Tests) | `Actor` exception propagation, `UnhandledErrorHandler` hook |
| [PicoActor.Tests](PicoActor.Tests) | `ActorSystem` concurrency, `GetAsync` race, `EventSourcedActor` recovery, append failure rollback |

## Run

Run each project individually (this is what CI does — `--project` gives one
stable test binary per run):

```bash
# CI parity
 dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release
 dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release

# Full-fidelity fallback (closest to the TUnit runner; add -p:PublishAot=false
# because the shared AOT tier sets PublishAot=true)
 dotnet run --project tests/PicoActor.Tests/PicoActor.Tests.csproj -p:PublishAot=false
```

> **Known MTP instability (measured):** solution-level `dotnet test PicoActor.slnx`
> can discover **0 tests** and exit with code 5 (MTP "zero tests ran") — exit code
> 5, not 0, so it never reports a false success. Stale `testhost.exe` processes
> are the usual cause: `taskkill //F //IM testhost.exe` and re-run, or use the
> per-project form above, which has been stable.
