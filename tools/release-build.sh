#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# tools/release-build.sh — the ONLY sanctioned way to build release products.
#
# Owner rule: `main` is the only release branch. This script proves the build is
# from main BEFORE it spends a second compiling, then writes every product
# OUTSIDE the working tree, so the repository can never ship a stale binary that
# merely looks fresh.
#
# Why this guard exists
#   Someone once shipped from a side branch. The binaries were 69 commits behind
#   main, yet their timestamps were new. A new timestamp is not new content, and
#   nothing downstream could tell the difference — so the check cannot live in a
#   convention or a runbook. It lives here, in the exit code.
#
# Contract (exit codes)
#   0  built from main, products written to OUT, fingerprints printed
#   2  refused: HEAD is not main (detached HEAD included)
#   3  refused: working tree is dirty
#   4  refused: OUT resolves inside the repository
#   5  build/pack failed, or produced nothing (tool output passed through verbatim)
#   6  the build left the working tree dirty
#
# Build shape for THIS repo (measured, mirrors .github/workflows/release.yml):
#   · solution file: PicoActor.slnx (there is no .sln)
#   · Directory.Build.props/.targets: shared TFM (net10.0) + AOT tiering
#   · packable projects: src/PicoActor.Abs, src/PicoActor
#       - Abs embeds the PicoActor.Gen analyzer (Gen itself is IsPackable=false)
#       - PicoActor consumes Abs from a staged feed (UseProjectReferences=false),
#         so its nuspec carries a real PicoActor.Abs package dependency
#     => the release products are two .nupkg + two .snupkg, NOT a standalone
#        exe/dll. There is therefore nothing to `dotnet publish` here.
#   · no RuntimeIdentifier is declared anywhere: products are framework-dependent
#     (net10.0). The host RID is printed for the record only, never stamped.
#
# In-repo writes: only the SDK's gitignored bin/obj intermediates (the same files
# any `dotnet build` here produces). Every package lands in OUT, and the script
# asserts `git status --porcelain` is still empty before it exits.
#
# Usage:
#   bash tools/release-build.sh
#   OUT=/some/path/outside/repo bash tools/release-build.sh
#   VERSION=2026.6.0 bash tools/release-build.sh      # package version override
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || exit 5

die() {
    local code="$1"
    shift
    printf 'release-build: %s\n' "$*" >&2
    exit "$code"
}

note() { printf 'release-build: %s\n' "$*"; }

command -v git >/dev/null 2>&1 || die 5 "git not found"
command -v dotnet >/dev/null 2>&1 || die 5 "dotnet not found"
command -v sha256sum >/dev/null 2>&1 || die 5 "sha256sum not found"

cd "${REPO_ROOT}" || die 5 "cannot enter ${REPO_ROOT}"

# ─────────────────────────────────────────────────────────────────────────────
# R1 — main is the only release branch
# ─────────────────────────────────────────────────────────────────────────────
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" ||
    die 2 "cannot determine HEAD (not a git work tree?)"

if [ "${branch}" != "main" ]; then
    if [ "${branch}" = "HEAD" ]; then
        die 2 "refusing to build from a detached HEAD (work tree is at $(git rev-parse --short HEAD)); main is the only release branch: 'git checkout main' first."
    fi
    die 2 "refusing to build from branch '${branch}'; main is the only release branch: 'git checkout main' first."
fi

# ─────────────────────────────────────────────────────────────────────────────
# R2 — no dirty tree: uncommitted work cannot be what ships
# ─────────────────────────────────────────────────────────────────────────────
dirty="$(git status --porcelain)"
if [ -n "${dirty}" ]; then
    {
        printf 'release-build: refusing to build from a dirty working tree on main.\n'
        printf 'release-build: commit or stash first; dirty path(s) (max 3 shown):\n'
        printf '%s\n' "${dirty}" | head -n 3 | while IFS= read -r line; do
            printf '  %s\n' "${line}"
        done
    } >&2
    exit 3
fi

# ─────────────────────────────────────────────────────────────────────────────
# R4 (part 1) — products go OUTSIDE the repository
# ─────────────────────────────────────────────────────────────────────────────
head_sha="$(git rev-parse HEAD)"
short_sha="$(git rev-parse --short HEAD)"
build_time="$(date -u +%FT%TZ)"

OUT="${OUT:-${TMPDIR:-/tmp}/rel-${short_sha}}"

# Containment is checked twice: lexically BEFORE anything is created (a refused
# OUT must leave the tree untouched), then again on the resolved path (so a
# symlink pointing into the repo cannot smuggle products in).
refuse_inside() {
    die 4 "OUT resolves inside the repository ($1); release products must never land in the working tree — point OUT at TMPDIR or an external path."
}

case "${OUT}" in
    /*) ;;
    *) OUT="${PWD}/${OUT}" ;;
esac

repo_logical="$(pwd -L)"
repo_physical="$(pwd -P)"
for root in "${repo_logical}" "${repo_physical}"; do
    case "${OUT}" in
        "${root}" | "${root}"/*) refuse_inside "${OUT}" ;;
    esac
done

mkdir -p "${OUT}" || die 4 "cannot create OUT directory: ${OUT}"
out_abs="$(cd "${OUT}" && pwd -P)" || die 4 "cannot resolve OUT: ${OUT}"
for root in "${repo_logical}" "${repo_physical}"; do
    case "${out_abs}" in
        "${root}" | "${root}"/*) refuse_inside "${out_abs}" ;;
    esac
done

# dotnet is a native process here: hand it a native path, not a Cygwin one.
out_native="${out_abs}"
if command -v cygpath >/dev/null 2>&1; then
    out_native="$(cygpath -w "${out_abs}")"
fi

# ─────────────────────────────────────────────────────────────────────────────
# R3 — print the identity of what is being built
# ─────────────────────────────────────────────────────────────────────────────
tfms="$(grep -ho '<TargetFramework>[^<]*' "${REPO_ROOT}"/src/*/*.csproj 2>/dev/null |
    sed 's/<TargetFramework>//' | sort -u | paste -sd, -)"
[ -n "${tfms}" ] || tfms="(unknown)"

host_rid="$(dotnet --info 2>/dev/null | awk '/^ *RID:/ {print $2; exit}')"
[ -n "${host_rid}" ] || host_rid="(unknown)"

props_version="$(grep -o '<Version Condition[^>]*>[^<]*' "${REPO_ROOT}/Directory.Build.props" 2>/dev/null |
    sed 's/.*>//' | head -n 1)"
version="${VERSION:-${props_version:-0.0.0-dev}}"

echo "=== release identity ========================================================"
printf '  branch      : %s\n' "${branch}"
printf '  HEAD        : %s\n' "${head_sha}"
printf '  HEAD short  : %s\n' "${short_sha}"
printf '  built at    : %s\n' "${build_time}"
printf '  target fwks : %s\n' "${tfms}"
printf '  RID         : none (framework-dependent; host RID %s, not stamped)\n' "${host_rid}"
printf '  version     : %s\n' "${version}"
printf '  OUT         : %s\n' "${out_abs}"
echo "============================================================================"

# ─────────────────────────────────────────────────────────────────────────────
# Build — solution first (this is also the 0/0 gate the release checklist cites)
# ─────────────────────────────────────────────────────────────────────────────
echo "--- dotnet build PicoActor.slnx -c Release ---"
# -nodeReuse:false: a reused MSBuild node left over from a previous run can die
# mid-build (MSB4166 "child node exited prematurely"), which would look like a
# broken release. A release build starts from fresh nodes every time.
if ! dotnet build "${REPO_ROOT}/PicoActor.slnx" -c Release --nologo -nodeReuse:false; then
    die 5 "dotnet build failed (see output above); nothing was released"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Pack — same order and flags as .github/workflows/release.yml, but the staged
# feed is OUT, never artifacts/nupkg inside the tree.
# ─────────────────────────────────────────────────────────────────────────────
packables=()
while IFS= read -r proj; do
    [ -n "${proj}" ] || continue
    if grep -q '<IsPackable>true</IsPackable>' "${proj}"; then
        packables+=("${proj}")
    fi
done < <(find "${REPO_ROOT}/src" -name '*.csproj' -not -path '*/obj/*' -not -path '*/bin/*' | sort)

[ "${#packables[@]}" -gt 0 ] || die 5 "no packable project (IsPackable=true) under src/ — this repo has no publishable product"

# Abs must be packed first: the other packages restore it from the staged feed.
ordered=()
for proj in "${packables[@]}"; do
    case "${proj}" in
        */PicoActor.Abs/*) ordered+=("${proj}") ;;
    esac
done
for proj in "${packables[@]}"; do
    case "${proj}" in
        */PicoActor.Abs/*) ;;
        *) ordered+=("${proj}") ;;
    esac
done

for proj in "${ordered[@]}"; do
    rel="${proj#"${REPO_ROOT}"/}"
    echo "--- dotnet pack ${rel} ---"
    extra=()
    case "${proj}" in
        */PicoActor.Abs/*) ;;
        # Later packages resolve the earlier ones from the staged feed in OUT.
        *) extra+=("-p:RestoreAdditionalProjectSources=${out_native}") ;;
    esac
    if ! dotnet pack "${proj}" -c Release \
        -p:Version="${version}" \
        -p:UseProjectReferences=false \
        -o "${out_native}" \
        --nologo -nodeReuse:false "${extra[@]}"; then
        die 5 "dotnet pack failed for ${rel}; nothing was released"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
# R5 — product fingerprints: per-file size + sha256, plus a digest for the set
# ─────────────────────────────────────────────────────────────────────────────
products=()
while IFS= read -r file; do
    [ -n "${file}" ] && products+=("${file}")
done < <(find "${out_abs}" -maxdepth 1 -type f | sort)

[ "${#products[@]}" -gt 0 ] || die 5 "no product was produced in ${out_abs}"

manifest=""
echo "=== products (size + sha256) ==============================================="
for file in "${products[@]}"; do
    size="$(stat -c %s "${file}")"
    sha="$(sha256sum "${file}" | awk '{print $1}')"
    printf '  %12s B  %s  %s\n' "${size}" "${sha}" "$(basename "${file}")"
    manifest+="${sha}  $(basename "${file}")"$'\n'
done
set_digest="$(printf '%s' "${manifest}" | sha256sum | awk '{print $1}')"
printf '  product-set digest (sha256 over the "sha256  name" lines): %s\n' "${set_digest}"

main_product="${out_abs}/PicoActor.${version}.nupkg"
if [ -f "${main_product}" ]; then
    printf '  main product: %s\n' "${main_product}"
    printf '  main sha256 : %s\n' "$(sha256sum "${main_product}" | awk '{print $1}')"
else
    note "warning: expected main product not found: ${main_product}"
fi
echo "============================================================================"

# ─────────────────────────────────────────────────────────────────────────────
# R4 (part 2) — the working tree must be exactly as clean as it was on entry
# ─────────────────────────────────────────────────────────────────────────────
after="$(git status --porcelain)"
if [ -n "${after}" ]; then
    {
        printf 'release-build: the build left the working tree dirty — products escaped into the repo:\n'
        printf '%s\n' "${after}" | head -n 3
    } >&2
    exit 6
fi

echo "release-build: OK — built from main at ${short_sha}, products in ${out_abs}, working tree clean"
exit 0
