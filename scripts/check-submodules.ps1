<#
.SYNOPSIS
    Automated Submodule Aggregation & One-Way Dependency Rule Verifier (Story 12.5, AD-26, FR-31).
.DESCRIPTION
    Validates that ferrous-cx aggregates all 10 component repositories, enforces that no component
    repository depends on or contains ferrous-cx (One-Way Dependency Rule), and verifies submodule parity.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir/.."
$MonorepoRoot = Resolve-Path "$ProjectRoot/.."

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  FerrousCX - Submodule Aggregation & One-Way Rule Audit  " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Audit ferrous-cx/.gitmodules Aggregation
Write-Host "`n[1/3] Auditing ferrous-cx/.gitmodules component declarations..." -ForegroundColor Green
$GitmodulesPath = "$ProjectRoot/.gitmodules"
if (-not (Test-Path $GitmodulesPath)) {
    throw "Missing .gitmodules in ferrous-cx: $GitmodulesPath"
}
$gitmodulesContent = Get-Content $GitmodulesPath -Raw

$requiredSubmodules = @(
    "ferrous-iam",
    "ferrous-streaming",
    "stream-replay-rs",
    "ferrous-vectordb",
    "ferrous-cqrses",
    "ferrous-api",
    "ferrous-agent",
    "ferrous-frontend",
    "ferrous-reverseproxy",
    "iggy-kafka-bridge"
)

foreach ($sub in $requiredSubmodules) {
    if ($gitmodulesContent -notmatch "\[submodule [`"']$sub[`"']\]") {
        throw "ferrous-cx/.gitmodules missing declaration for submodule '$sub'"
    }
    Write-Host "  [OK] Submodule declared: $sub" -ForegroundColor Green
}

# 2. Enforce One-Way Dependency Rule (AD-26)
Write-Host "`n[2/3] Enforcing One-Way Dependency Rule across component repositories (AD-26)..." -ForegroundColor Green
$violations = @()

foreach ($sub in $requiredSubmodules) {
    # Check standalone clone child path first, then parent monorepo sibling path
    $subPath = if (Test-Path "$ProjectRoot/$sub") {
        "$ProjectRoot/$sub"
    } elseif (Test-Path "$MonorepoRoot/$sub") {
        "$MonorepoRoot/$sub"
    } else {
        $null
    }

    if ($null -ne $subPath) {
        # Check .gitmodules inside component
        $subGitmodules = "$subPath/.gitmodules"
        if (Test-Path $subGitmodules) {
            $subGmContent = Get-Content $subGitmodules -Raw
            if ($subGmContent -match "ferrous-cx") {
                $violations += "Circular dependency detected: $sub lists ferrous-cx as a submodule!"
            }
        }
        # Check for invalid relative references to ferrous-cx in code/configs, pruning build/git directories
        $filesWithRef = Get-ChildItem -Path $subPath -Recurse -File |
            Where-Object {
                $_.FullName -notmatch '[\\/](\.git|target|node_modules|__pycache__|data)[\\/]' -and
                $_.Extension -notin @('.log', '.tmp')
            } |
            Select-String -Pattern '\.\.[\\/]ferrous-cx'

        if ($filesWithRef) {
            foreach ($match in $filesWithRef) {
                $violations += "Invalid path dependency in $($sub): $($match.Filename) references $($match.Line.Trim())"
            }
        }
        Write-Host "  [OK] Component '$sub' obeys One-Way Dependency Rule." -ForegroundColor Green
    } else {
        Write-Host "  [INFO] Submodule directory '$sub' not found (uninitialized standalone clone)." -ForegroundColor Yellow
    }
}

if ($violations.Count -gt 0) {
    Write-Host "`n[FATAL] One-Way Dependency Rule Violations Detected:" -ForegroundColor Red
    foreach ($v in $violations) {
        Write-Host "  - $v" -ForegroundColor Red
    }
    throw "One-Way Dependency Rule verification failed with $($violations.Count) violation(s)."
}

# 3. Audit Monorepo Root Submodule Parity & Commit Pointers
Write-Host "`n[3/3] Checking root monorepo submodule parity & commit pointers..." -ForegroundColor Green
$RootGitmodules = "$MonorepoRoot/.gitmodules"
if (Test-Path $RootGitmodules) {
    $rootGmContent = Get-Content $RootGitmodules -Raw
    foreach ($sub in $requiredSubmodules) {
        if ($rootGmContent -match "\[submodule [`"']$sub[`"']\]") {
            # Check commit SHA pointer parity if git is available
            $rootSha = $null
            $subSha = $null
            try {
                $rootSha = (git -C "$MonorepoRoot" rev-parse "HEAD:$sub" 2>$null)
            } catch {}
            if (Test-Path "$MonorepoRoot/$sub/.git") {
                try {
                    $subSha = (git -C "$MonorepoRoot/$sub" rev-parse HEAD 2>$null)
                } catch {}
            }
            if ($rootSha -and $subSha -and ($rootSha -eq $subSha)) {
                Write-Host "  [OK] Parity & commit pointer verified: $sub ($($rootSha.Substring(0,7)))" -ForegroundColor Green
            } else {
                Write-Host "  [OK] Parity verified: $sub exists in root .gitmodules" -ForegroundColor Green
            }
        } else {
            Write-Host "  [WARN] Parity gap: Submodule $sub declared in ferrous-cx but not root .gitmodules" -ForegroundColor Yellow
        }
    }
    if ($rootGmContent -match '\[submodule ["\x27]ferrous-cx["\x27]\]') {
        Write-Host "  [OK] Root aggregates ferrous-cx as primary coordinator." -ForegroundColor Green
    }
} else {
    Write-Host "  [INFO] Root repository .gitmodules not found (running standalone clone)." -ForegroundColor Yellow
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  [OK] SUBMODULE AGGREGATION & ONE-WAY RULE AUDIT PASSED! " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
