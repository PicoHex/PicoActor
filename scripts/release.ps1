<#
.SYNOPSIS
PicoHex release helper - decide the next version, refresh API baselines, tag.

.DESCRIPTION
Implements the PicoHex version rule:

    <four-digit year>.<x>.<y>

      x bumps when the public API changed (y resets to 0)
      y bumps when it did not
      the year segment is the current year and does not by itself reset x/y
      (pass -ResetOnYearChange for repos that prefer per-year counters)

The public API of every packable project is compared against the committed
baseline in <RepoRoot>/api/<PackageId>.public.txt. A baseline always holds the
surface of the LAST RELEASE, so it is refreshed as part of the release commit.

The tag is the release trigger: the release workflow runs the tests, packs every
package at the tag version, and publishes to NuGet.

.PARAMETER RepoRoot
Repository to release. Defaults to the current directory.

.PARAMETER DryRun
Report the API delta and the next version without writing, committing, or tagging.

.PARAMETER Bootstrap
Create the api/ baselines. Use with -FromTag <tag> (preferred) or -AssumeCurrent.

.PARAMETER FromTag
Bootstrap source revision, e.g. v2026.9.0. Builds that tag in a temporary worktree.

.PARAMETER AssumeCurrent
Bootstrap from the current HEAD instead of a tag. The API delta since the last
release cannot be recovered this way; the next release compares against HEAD.

.PARAMETER BaselineDir
Baseline directory relative to RepoRoot. Default: api

.PARAMETER Push
After tagging, push the branch and the tag (this triggers the release workflow).

.PARAMETER ResetOnYearChange
Reset x and y when the year changes instead of carrying them forward.

.EXAMPLE
# once per repository: seed the baselines from the last release
./scripts/release.ps1 -Bootstrap -FromTag v2026.9.0

.EXAMPLE
# inspect the decision without writing anything
./scripts/release.ps1 -DryRun

.EXAMPLE
# cut the release: refresh baselines, commit, tag, push
./scripts/release.ps1 -Push
#>

[CmdletBinding()]
param(
    [string]$RepoRoot = (Get-Location).Path,
    [switch]$DryRun,
    [switch]$Bootstrap,
    [string]$FromTag,
    [switch]$AssumeCurrent,
    [string]$BaselineDir = "api",
    [switch]$Push,
    [switch]$ResetOnYearChange
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$message) { Write-Host "== $message" -ForegroundColor Cyan }
function Write-Ok([string]$message) { Write-Host "   $message" -ForegroundColor Green }
function Write-Note([string]$message) { Write-Host "   $message" -ForegroundColor Yellow }
function Fail([string]$message) { Write-Host "error: $message" -ForegroundColor Red; exit 2 }

function Invoke-Native([string]$command, [string[]]$arguments) {
    # Native stderr output must not become a terminating error under
    # $ErrorActionPreference='Stop' (git writes progress lines to stderr).
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $command @arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{ Output = $output; ExitCode = $exitCode }
}

function Invoke-Git([string[]]$arguments) {
    $result = Invoke-Native "git" (@("-C", $RepoRoot) + $arguments)
    if ($result.ExitCode -ne 0) { throw "git $($arguments -join ' ') failed: $($result.Output)" }
    return $result.Output
}

function Invoke-GitRaw([string[]]$arguments) {
    return Invoke-Native "git" (@("-C", $RepoRoot) + $arguments)
}

function Invoke-Tool([string[]]$arguments) {
    $result = Invoke-Native "dotnet" (@($script:ToolDll) + $arguments)
    $script:ToolExit = $result.ExitCode
    return $result.Output
}

function Get-PackableProjects([string]$root) {
    $srcRoot = Join-Path $root "src"
    if (-not (Test-Path $srcRoot)) { $srcRoot = $root }
    $projects = @()
    foreach ($file in Get-ChildItem -Path $srcRoot -Recurse -Filter *.csproj -File) {
        if ($file.FullName -match "[\\/](obj|bin)[\\/]") { continue }
        [xml]$xml = Get-Content -LiteralPath $file.FullName -Raw
        $propertyGroups = @($xml.Project.PropertyGroup)
        $isPackable = $propertyGroups | ForEach-Object { $_.IsPackable } | Where-Object { $_ } | Select-Object -First 1
        if ("$isPackable".Trim().ToLowerInvariant() -ne "true") { continue }
        $packageId = $propertyGroups | ForEach-Object { $_.PackageId } | Where-Object { $_ } | Select-Object -First 1
        $assemblyName = $propertyGroups | ForEach-Object { $_.AssemblyName } | Where-Object { $_ } | Select-Object -First 1
        if (-not $packageId) { $packageId = $assemblyName }
        if (-not $packageId) { $packageId = [System.IO.Path]::GetFileNameWithoutExtension($file.Name) }
        if (-not $assemblyName) { $assemblyName = $packageId }
        $targetFrameworks = $propertyGroups | ForEach-Object { $_.TargetFrameworks } | Where-Object { $_ } | Select-Object -First 1
        if (-not $targetFrameworks) {
            $targetFrameworks = $propertyGroups | ForEach-Object { $_.TargetFramework } | Where-Object { $_ } | Select-Object -First 1
        }
        if (-not $targetFrameworks) { $targetFrameworks = "net10.0" }
        $projects += [pscustomobject]@{
            Path            = $file.FullName
            Directory       = $file.DirectoryName
            PackageId       = "$packageId".Trim()
            AssemblyName    = "$assemblyName".Trim()
            TargetFramework = ("$targetFrameworks".Trim() -split ";")[0]
        }
    }
    $duplicateIds = $projects | Group-Object -Property PackageId | Where-Object { $_.Count -gt 1 }
    if ($duplicateIds) {
        Fail "duplicate PackageId across packable projects: $(($duplicateIds | ForEach-Object { $_.Name }) -join ', ')"
    }
    return , $projects
}

function Invoke-Build([string]$root) {
    Write-Step "Building packable projects (Release)"
    $projects = Get-PackableProjects $root
    if ($projects.Count -eq 0) { Fail "no packable projects found under $root" }
    foreach ($project in $projects) {
        $result = Invoke-Native "dotnet" @("build", $project.Path, "-c", "Release", "--nologo")
        if ($result.ExitCode -ne 0) {
            Write-Host ($result.Output -join [Environment]::NewLine)
            throw "build failed: $($project.Path)"
        }
        Write-Ok "built $($project.PackageId)"
    }
}

function Get-Surface([string]$root, [string]$outputDirectory) {
    $surfaces = @{}
    $projects = Get-PackableProjects $root
    foreach ($project in $projects) {
        $assembly = Get-ChildItem -Path (Join-Path $project.Directory "bin/Release") -Recurse -Filter "$($project.AssemblyName).dll" -File -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -notmatch "[\\/](ref|obj)[\\/]" } |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if (-not $assembly) {
            Fail "built assembly not found for $($project.PackageId) (bin/Release/**/$($project.AssemblyName).dll)"
        }
        $surfaceFile = Join-Path $outputDirectory "$($project.PackageId).public.txt"
        $null = Invoke-Tool @("dump", $assembly.FullName, "--out", $surfaceFile)
        if ($script:ToolExit -ne 0) { Fail "api dump failed for $($project.PackageId)" }
        $surfaces[$project.PackageId] = $surfaceFile
    }
    return $surfaces
}

function Get-LastTag {
    $result = Invoke-GitRaw @("tag", "--list", "v*", "--sort=-v:refname")
    if ($result.ExitCode -ne 0 -or -not $result.Output) { return "" }
    return @($result.Output)[0].Trim()
}

# -- locate and build the api-surface tool ------------------------------------

$toolDirectory = Join-Path $PSScriptRoot "api-surface"
if (-not (Test-Path $toolDirectory)) { $toolDirectory = Join-Path $RepoRoot "scripts/api-surface" }
if (-not (Test-Path (Join-Path $toolDirectory "ApiSurface.csproj"))) {
    Fail "api-surface tool not found (looked in '$PSScriptRoot/api-surface' and '$RepoRoot/scripts/api-surface')"
}
$script:ToolDll = Join-Path $toolDirectory "bin/Release/net10.0/picohex-api-surface.dll"
$script:ToolExit = 0

Write-Step "Building api-surface tool"
$toolBuild = Invoke-Native "dotnet" @("build", (Join-Path $toolDirectory "ApiSurface.csproj"), "-c", "Release", "-v", "quiet", "--nologo")
if ($toolBuild.ExitCode -ne 0) {
    Write-Host ($toolBuild.Output -join [Environment]::NewLine)
    Fail "failed to build the api-surface tool"
}

if ([System.IO.Path]::IsPathRooted($BaselineDir)) { $baselineRoot = $BaselineDir }
else { $baselineRoot = Join-Path $RepoRoot $BaselineDir }

$workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("picohex-release-" + [Guid]::NewGuid().ToString("N").Substring(0, 8))
$null = New-Item -ItemType Directory -Path $workRoot -Force

try {
    # -- bootstrap mode -------------------------------------------------------
    if ($Bootstrap) {
        if (-not $FromTag -and -not $AssumeCurrent) { Fail "bootstrap needs -FromTag <tag> (preferred) or -AssumeCurrent" }
        $null = New-Item -ItemType Directory -Path $baselineRoot -Force

        if ($FromTag) {
            Write-Step "Bootstrapping baselines from tag $FromTag (temporary worktree)"
            $worktree = Join-Path $workRoot "tag"
            $null = Invoke-Git @("worktree", "add", "--detach", $worktree, $FromTag)
            try {
                Invoke-Build $worktree
                $surfaces = Get-Surface $worktree $baselineRoot
            }
            finally {
                $null = Invoke-GitRaw @("worktree", "remove", "--force", $worktree)
            }
        }
        else {
            Write-Note "-AssumeCurrent: the API delta since the last release cannot be recovered"
            Invoke-Build $RepoRoot
            $surfaces = Get-Surface $RepoRoot $baselineRoot
        }

        foreach ($packageId in ($surfaces.Keys | Sort-Object)) {
            $count = (Get-Content -LiteralPath $surfaces[$packageId] | Measure-Object -Line).Lines
            Write-Ok "$packageId`: $count API lines -> $BaselineDir/$packageId.public.txt"
        }
        Write-Host ""
        Write-Ok "Baselines written. Commit them, then release without -Bootstrap."
        exit 0
    }

    # -- release mode ---------------------------------------------------------
    if (-not (Test-Path $baselineRoot)) {
        Fail "no baselines in $BaselineDir/. Run: ./scripts/release.ps1 -Bootstrap -FromTag <last-tag>"
    }

    $dirtyResult = Invoke-GitRaw @("status", "--porcelain")
    if ($dirtyResult.Output -and -not $DryRun) { Fail "working tree is not clean; commit or stash before releasing" }

    Invoke-Build $RepoRoot

    Write-Step "Comparing the public API against the $BaselineDir baselines"
    $currentSurfaces = Get-Surface $RepoRoot $workRoot
    $changed = $false
    foreach ($packageId in ($currentSurfaces.Keys | Sort-Object)) {
        $baseline = Join-Path $baselineRoot "$packageId.public.txt"
        if (-not (Test-Path $baseline)) {
            Write-Note "$packageId`: no baseline (new package) -> API changed"
            $changed = $true
            continue
        }
        $diff = Invoke-Tool @("diff", $baseline, $currentSurfaces[$packageId])
        $summary = @($diff | Where-Object { $_ -match "^added=\d+ removed=\d+$" })[-1]
        if (-not ($summary -match "added=(\d+) removed=(\d+)")) { Fail "unexpected diff output for $packageId" }
        $added = [int]$Matches[1]
        $removed = [int]$Matches[2]
        if ($added -gt 0 -or $removed -gt 0) {
            $changed = $true
            Write-Note "$packageId`: API changed (+$added / -$removed)"
            $diff | Where-Object { $_ -match "^[+-] " } | ForEach-Object { Write-Host "     $_" -ForegroundColor DarkGray }
        }
        else {
            Write-Ok "$packageId`: API unchanged"
        }
    }

    $lastTag = Get-LastTag
    $apiChanged = "false"
    if ($changed) { $apiChanged = "true" }
    $versionArgs = @("next-version", "--api-changed", $apiChanged, "--year", (Get-Date).Year)
    if ($lastTag) { $versionArgs += @("--last", $lastTag) }
    if ($ResetOnYearChange) { $versionArgs += "--reset-on-year-change" }
    $nextVersion = @(Invoke-Tool $versionArgs)[-1].Trim()
    if ($script:ToolExit -ne 0) { Fail "next-version failed" }

    Write-Host ""
    Write-Step "Decision"
    if ($lastTag) { Write-Host "   last release : $lastTag" } else { Write-Host "   last release : (none)" }
    Write-Host "   API changed  : $apiChanged"
    Write-Host "   next version : v$nextVersion" -ForegroundColor Green

    if ($DryRun) {
        Write-Host ""
        Write-Ok "Dry run - nothing written, no tag created."
        exit 0
    }

    Write-Step "Refreshing baselines and tagging"
    foreach ($packageId in $currentSurfaces.Keys) {
        Copy-Item -LiteralPath $currentSurfaces[$packageId] -Destination (Join-Path $baselineRoot "$packageId.public.txt") -Force
    }
    foreach ($file in Get-ChildItem -Path $baselineRoot -Filter *.public.txt -File) {
        # BaseName strips only the last extension (PicoTui.public), so strip the
        # full .public.txt suffix explicitly.
        $baselinePackage = $file.Name.Substring(0, $file.Name.Length - ".public.txt".Length)
        if (-not $currentSurfaces.ContainsKey($baselinePackage)) {
            Write-Note "removing stale baseline for dropped package $baselinePackage"
            Remove-Item -LiteralPath $file.FullName -Force
        }
    }

    $null = Invoke-Git @("add", "--", $BaselineDir)
    # An unchanged API (y bump) refreshes the baselines to identical content:
    # only commit when something is actually staged, otherwise the no-op commit
    # fails and the tag is never created.
    $staged = Invoke-GitRaw @("diff", "--cached", "--quiet")
    if ($staged.ExitCode -ne 0) {
        $null = Invoke-Git @("commit", "-m", "chore(release): v$nextVersion")
        Write-Ok "committed baseline refresh"
    }
    else {
        Write-Note "baselines unchanged - tagging the current commit"
    }

    $null = Invoke-Git @("tag", "-a", "v$nextVersion", "-m", "v$nextVersion")
    Write-Ok "created tag v$nextVersion"

    if ($Push) {
        $branchResult = Invoke-GitRaw @("rev-parse", "--abbrev-ref", "HEAD")
        $branch = @($branchResult.Output)[0].Trim()
        $null = Invoke-Git @("push", "origin", $branch)
        $null = Invoke-Git @("push", "origin", "v$nextVersion")
        Write-Ok "pushed $branch and v$nextVersion - the release workflow will publish"
    }
    else {
        Write-Host ""
        Write-Host "   Next: git push origin <branch> && git push origin v$nextVersion"
        Write-Host "   The release workflow publishes every package at v$nextVersion."
    }
}
finally {
    Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
}
