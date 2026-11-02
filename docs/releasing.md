# Releasing PicoActor

One page. If a step is not on this page, it is not a release step.

## Rule: `main` is the only release branch

Release products are built from `main` and nowhere else. No side branch, no
hotfix branch, no topic branch, no detached HEAD. If a fix must ship, it lands on
`main` first; the release then follows `main`.

## Rule: products come from `tools/release-build.sh` and only from there

```bash
bash tools/release-build.sh
# or, to choose the output directory (must be outside the repository):
OUT=/some/external/path bash tools/release-build.sh
```

The script enforces the branch rule for you and fails loudly instead of guessing:

| exit | meaning |
|---|---|
| `0` | built from `main`, products written outside the repo, fingerprints printed |
| `2` | refused: HEAD is not `main` (detached HEAD included) |
| `3` | refused: working tree is dirty |
| `4` | refused: `OUT` resolves inside the repository |
| `5` | build/pack failed (or nothing was produced) |
| `6` | the build left the working tree dirty |

What it does: verifies the branch, verifies the tree is clean, prints the
identity (full `HEAD` sha, branch, UTC build time, target frameworks, `RID`,
package version), runs `dotnet build PicoActor.slnx -c Release`, packs
`PicoActor.Abs` and then `PicoActor` into `OUT` with the same
`UseProjectReferences=false` staged-feed order as the release workflow, and
finally prints a size + `sha256` fingerprint for every product plus a digest for
the whole product set. The products are two `.nupkg` and two `.snupkg` files:
this is a library repository, so there is no exe or standalone dll to publish.

Nothing is written inside the checkout — `git status --porcelain` is empty when
the script exits, which is also the script's last check.

## Before you release: three self-checks

1. **This repo's own suite passes.**
   ```bash
   dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release
   dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release
   ```
2. **`dotnet build` is 0 warnings / 0 errors.**
   ```bash
   dotnet build PicoActor.slnx -c Release
   ```
   The build must print `0 Warning(s)` / `0 Error(s)`. Anything else is not
   releasable.
3. **Record the product fingerprints.** Copy the `main sha256` and the
   product-set digest printed by `tools/release-build.sh` into the release notes.
   A release without a fingerprint cannot be verified later, and "it was rebuilt"
   is not evidence.

Then tag from `main` and let the release workflow publish
(`scripts/release.ps1` decides the next version from the API baseline and
creates the tag).

## Why this rule exists

Someone once shipped from a side branch. The binaries were **69 commits behind
`main`**, but their timestamps were fresh — a new timestamp is not new content —
and consumers had no way to tell they were running stale code. The branch check
lives in `tools/release-build.sh` because a rule that only lives in a runbook is
a rule that fails on the day someone is in a hurry.
