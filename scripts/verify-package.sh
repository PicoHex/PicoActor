#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Consumer smoke test for the PACKAGED surface (C1 regression guard).
#
# Packs PicoActor.Abs + PicoActor.EventStore.InMemory + PicoActor into a local feed,
# then restores them into three throwaway console projects:
#
#   probe "minimal"    — an actor type is created at run time, no domain event; the
#                        store comes from the `PicoActor.EventStore.InMemory` PACKAGE
#                        (PicoActor alone carries no concrete store).
#                        Guards the RUNTIME surface: the PicoActor.Abs module
#                        initializer calls PicoMediator.MediatorAutoSubscriptionRegistry,
#                        so a missing PicoMediator dependency surfaces here as
#                        TypeInitializationException -> FileNotFoundException.
#   probe "with-event" — additionally declares one IDomainEvent, which makes the
#                        PicoMediator generator emit a dispatcher into the consumer
#                        assembly. Guards the COMPILE surface: a missing
#                        PicoMediator reference surfaces here as CS0234.
#   probe "no-store"   — NEGATIVE probe: references `PicoActor` + `PicoDI` and NOT the
#                        store package, so `AddPicoActor()` resolves with no IEventStore.
#                        The run MUST fail, and the failure MUST name
#                        AddPicoActorInMemoryStore — that is the publish-face proof of
#                        the C2 decision (the core package carries no concrete store,
#                        and the missing-store case is a resolve-time throw).
#
# The nuspec dependency closure of all three packages is printed first: the package
# must declare what the shipped DLL actually references.
#
# Usage: scripts/verify-package.sh            (exit 0 = package surface is sound)
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FEED="${REPO_ROOT}/artifacts/nupkg"
VERSION="${PICOACTOR_SMOKE_VERSION:-0.0.0-smoke}"

die() {
    echo "verify-package: $*" >&2
    exit 2
}

command -v dotnet >/dev/null || die "dotnet not found"
command -v unzip >/dev/null || die "unzip not found"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

to_win_path() {
    if command -v cygpath >/dev/null; then cygpath -w "$1"; else printf '%s' "$1"; fi
}

FEED_WIN="$(to_win_path "${FEED}")"

# ═══════════════════════════════════════════════════════════════
# 1. Pack the three packages into a clean local feed
# ═══════════════════════════════════════════════════════════════
echo "=== packing PicoActor.Abs + PicoActor.EventStore.InMemory + PicoActor ${VERSION} -> ${FEED} ==="
rm -rf "${FEED}"
mkdir -p "${FEED}"

pack() {
    local project="$1"
    echo "--- dotnet pack ${project} ---"
    dotnet pack "${REPO_ROOT}/${project}" -c Release -p:Version="${VERSION}" -o "${FEED}" ||
        die "dotnet pack ${project} failed"
}

pack "src/PicoActor.Abs/PicoActor.Abs.csproj"
pack "src/PicoActor.EventStore.InMemory/PicoActor.EventStore.InMemory.csproj"
pack "src/PicoActor/PicoActor.csproj"

ls -1 "${FEED}"/*.nupkg || die "no packages produced in ${FEED}"

# ═══════════════════════════════════════════════════════════════
# 2. Print the declared dependency closure (the package contract)
# ═══════════════════════════════════════════════════════════════
echo
echo "=== nuspec dependency closure ==="
PACKAGES=(
    "${FEED}/PicoActor.Abs.${VERSION}.nupkg"
    "${FEED}/PicoActor.EventStore.InMemory.${VERSION}.nupkg"
    "${FEED}/PicoActor.${VERSION}.nupkg"
)
for pkg in "${PACKAGES[@]}"; do
    [ -f "${pkg}" ] || die "missing ${pkg}"
    echo "--- $(basename "${pkg}") ---"
    unzip -p "${pkg}" '*.nuspec' | sed -n 's/.*<dependency \(.*\)\/>.*/  \1/p'
done

# ═══════════════════════════════════════════════════════════════
# 3. Probe projects — the store comes from its own package
# ═══════════════════════════════════════════════════════════════

# NuGet extracts a package once per id/version into the global packages folder and
# then reuses it forever — a re-run would silently test the PREVIOUS build of the
# same version. Purge only this script's synthetic version (never a released one).
GLOBAL_PACKAGES="${NUGET_PACKAGES:-${HOME}/.nuget/packages}"
for id in picoactor picoactor.abs picoactor.eventstore.inmemory; do
    rm -rf "${GLOBAL_PACKAGES}/${id}/${VERSION}"
done
# $3 is the space-separated package list, so a probe can prove what it needs and a
# negative probe can prove what it must NOT have (the store package). An entry may
# name its own version as `id:version` — a third-party package such as PicoDI floats
# on its band and has no smoke version of ours.
write_probe() {
    local name="$1" source="$2" packages="$3"
    local dir="${WORK}/${name}"
    local refs="" spec id ver
    mkdir -p "${dir}"

    for spec in ${packages}; do
        id="${spec%%:*}"
        ver="${VERSION}"
        case "${spec}" in
            *:*) ver="${spec#*:}" ;;
        esac
        refs="${refs}    <PackageReference Include='${id}' Version='${ver}' />"$'\n'
    done

    cat >"${dir}/Smoke.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
${refs}  </ItemGroup>
</Project>
EOF

    cat >"${dir}/nuget.config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-smoke" value="${FEED_WIN}" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOF

    printf '%s\n' "${source}" >"${dir}/Program.cs"
    printf '%s' "${dir}"
}

# 3a. Actor-only consumer: touches the abstractions at run time.
read -r -d '' MINIMAL_SOURCE <<'CSHARP'
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
system.Register<SmokeActor>(_ => new SmokeActor());
var actor = await system.CreateAsync<SmokeActor>(new SmokeCreate());
await system.StopAllAsync();

Console.WriteLine($"SMOKE minimal OK actorId={actor.Id}");

public sealed record SmokeCreate : ICommand;

public sealed class SmokeActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
        => ValueTask.FromResult<object?>(null);
}
CSHARP

# 3b. Actor + one IDomainEvent declaration: the consumer assembly now hosts
#     PicoMediator-generated dispatcher code.
read -r -d '' WITH_EVENT_SOURCE <<'CSHARP'
using PicoActor;
using PicoActor.Abs;
using PicoActor.EventStore.InMemory;

var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
system.Register<SmokeActor>(_ => new SmokeActor());
var actor = await system.CreateAsync<SmokeActor>(new SmokeCreate());
await system.StopAllAsync();

Console.WriteLine($"SMOKE with-event OK actorId={actor.Id}");

public sealed record SmokeCreate : ICommand;

public sealed record SmokeRaised(Guid Id) : IDomainEvent;

public sealed class SmokeActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command) => ValueTask.FromResult<object?>(null);
}
CSHARP

# The negative probe: `PicoActor` + `PicoDI`, deliberately WITHOUT the store package.
# AddPicoActor() registers no IEventStore, so resolving IActorSystem must throw and the
# message must point the consumer at the store package's extension.
read -r -d '' NO_STORE_SOURCE <<'CSHARP'
using PicoActor;
using PicoActor.Abs;
using PicoDI;

var container = new SvcContainer(autoConfigureFromGenerator: false);
container.AddPicoActor();
container.Build();
await using var scope = container.CreateScope();
_ = scope.GetService(typeof(IActorSystem)); // must throw: no IEventStore is registered

Console.WriteLine("SMOKE no-store UNEXPECTED: resolving IActorSystem without a store succeeded");
CSHARP

overall=0

run_probe() {
    local name="$1" source="$2" packages="$3"
    local dir rc
    dir="$(write_probe "${name}" "${source}" "${packages}")"
    echo
    echo "=== probe '${name}': package consumer (${packages}) ==="
    (
        cd "${dir}" || exit 2
        dotnet run -v minimal
    )
    rc=$?
    if [ "${rc}" -eq 0 ]; then
        echo "probe '${name}': PASS"
    else
        echo "probe '${name}': FAIL (exit ${rc})"
        overall=1
    fi
    return 0
}

# Expects a NON-zero exit AND $4 in the output: a probe that "passes" by throwing
# something unrelated, or that succeeds, is a failure of the negative probe.
run_negative_probe() {
    local name="$1" source="$2" packages="$3" expected="$4"
    local dir rc log
    dir="$(write_probe "${name}" "${source}" "${packages}")"
    log="${WORK}/${name}.log"
    echo
    echo "=== probe '${name}' (negative): package consumer (${packages}) ==="
    (
        cd "${dir}" || exit 2
        dotnet run -v minimal
    ) >"${log}" 2>&1
    rc=$?
    cat "${log}"
    if [ "${rc}" -eq 0 ]; then
        echo "probe '${name}': FAIL (exit 0 — resolving IActorSystem without a store must throw)"
        overall=1
    elif ! grep -q "${expected}" "${log}"; then
        echo "probe '${name}': FAIL (exit ${rc}, but the output does not name ${expected})"
        overall=1
    else
        echo "probe '${name}': PASS (exit ${rc}, message names ${expected})"
    fi
    return 0
}

run_probe "minimal" "${MINIMAL_SOURCE}" "PicoActor PicoActor.EventStore.InMemory"
run_probe "with-event" "${WITH_EVENT_SOURCE}" "PicoActor PicoActor.EventStore.InMemory"
run_negative_probe "no-store" "${NO_STORE_SOURCE}" "PicoActor PicoDI:2026.11.*" "AddPicoActorInMemoryStore"

echo
if [ "${overall}" -eq 0 ]; then
    echo "=== verify-package: PASS — packaged surface is consumable, and the store is a separate package ==="
else
    echo "=== verify-package: FAIL — packaged surface is not consumable as published ===" >&2
fi
exit "${overall}"
