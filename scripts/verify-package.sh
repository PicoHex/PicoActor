#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Consumer smoke test for the PACKAGED surface (C1 regression guard).
#
# Packs PicoActor.Abs + PicoActor into a local feed, then restores them into two
# throwaway console projects that reference ONLY the `PicoActor` package:
#
#   probe "minimal"    — an actor type is created at run time, no domain event.
#                        Guards the RUNTIME surface: the PicoActor.Abs module
#                        initializer calls PicoMediator.MediatorAutoSubscriptionRegistry,
#                        so a missing PicoMediator dependency surfaces here as
#                        TypeInitializationException -> FileNotFoundException.
#   probe "with-event" — additionally declares one IDomainEvent, which makes the
#                        PicoMediator generator emit a dispatcher into the consumer
#                        assembly. Guards the COMPILE surface: a missing
#                        PicoMediator reference surfaces here as CS0234.
#
# The nuspec dependency closure of both packages is printed first: the package
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
# 1. Pack both packages into a clean local feed
# ═══════════════════════════════════════════════════════════════
echo "=== packing PicoActor.Abs + PicoActor ${VERSION} -> ${FEED} ==="
rm -rf "${FEED}"
mkdir -p "${FEED}"

pack() {
    local project="$1"
    echo "--- dotnet pack ${project} ---"
    dotnet pack "${REPO_ROOT}/${project}" -c Release -p:Version="${VERSION}" -o "${FEED}" ||
        die "dotnet pack ${project} failed"
}

pack "src/PicoActor.Abs/PicoActor.Abs.csproj"
pack "src/PicoActor/PicoActor.csproj"

ls -1 "${FEED}"/*.nupkg || die "no packages produced in ${FEED}"

# ═══════════════════════════════════════════════════════════════
# 2. Print the declared dependency closure (the package contract)
# ═══════════════════════════════════════════════════════════════
echo
echo "=== nuspec dependency closure ==="
for pkg in "${FEED}/PicoActor.Abs.${VERSION}.nupkg" "${FEED}/PicoActor.${VERSION}.nupkg"; do
    [ -f "${pkg}" ] || die "missing ${pkg}"
    echo "--- $(basename "${pkg}") ---"
    unzip -p "${pkg}" '*.nuspec' | sed -n 's/.*<dependency \(.*\)\/>.*/  \1/p'
done

# ═══════════════════════════════════════════════════════════════
# 3. Probe projects — reference ONLY the PicoActor package
# ═══════════════════════════════════════════════════════════════

# NuGet extracts a package once per id/version into the global packages folder and
# then reuses it forever — a re-run would silently test the PREVIOUS build of the
# same version. Purge only this script's synthetic version (never a released one).
GLOBAL_PACKAGES="${NUGET_PACKAGES:-${HOME}/.nuget/packages}"
for id in picoactor picoactor.abs; do
    rm -rf "${GLOBAL_PACKAGES}/${id}/${VERSION}"
done
write_probe() {
    local name="$1" source="$2"
    local dir="${WORK}/${name}"
    mkdir -p "${dir}"

    cat >"${dir}/Smoke.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="PicoActor" Version="${VERSION}" />
  </ItemGroup>
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

overall=0

run_probe() {
    local name="$1" source="$2"
    local dir rc
    dir="$(write_probe "${name}" "${source}")"
    echo
    echo "=== probe '${name}': package consumer (PicoActor only) ==="
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

run_probe "minimal" "${MINIMAL_SOURCE}"
run_probe "with-event" "${WITH_EVENT_SOURCE}"

echo
if [ "${overall}" -eq 0 ]; then
    echo "=== verify-package: PASS — packaged surface is consumable with PicoActor alone ==="
else
    echo "=== verify-package: FAIL — packaged surface is not consumable with PicoActor alone ===" >&2
fi
exit "${overall}"
