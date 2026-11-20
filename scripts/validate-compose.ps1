<#
.SYNOPSIS
    Automated Docker Compose validation script for FerrousCX master topology (Story 12.1, Story 18.3).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir/.."

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  FerrousCX - Compose Topology & Healthcheck Graph Audit  " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Validate .env.example
Write-Host "`n[1/4] Checking .env.example & BRIDGE_IMAGE pinning (AD-29)..." -ForegroundColor Green
$EnvExample = Get-Content "$ProjectRoot/.env.example" -Raw
if ($EnvExample -notmatch "BRIDGE_IMAGE=iggy-kafka-bridge:[a-f0-9]{7}") {
    throw "BRIDGE_IMAGE in .env.example is not pinned to a submodule short commit SHA (must match iggy-kafka-bridge:<sha>)"
}
if ($EnvExample -match "BRIDGE_IMAGE=.*:latest") {
    throw "BRIDGE_IMAGE must never use ':latest' tag (AD-29 violation)"
}
if ($EnvExample -match "PORT_FRONTEND=3000") {
    throw "PORT_FRONTEND in .env.example must be 5173 (not 3000) to prevent port collision with Apache Iggy HTTP API (AD-26)"
}
Write-Host "  [OK] BRIDGE_IMAGE correctly pinned to immutable short SHA." -ForegroundColor Green

# 2. Validate Compose Syntax via docker compose config
Write-Host "`n[2/4] Validating Docker Compose syntax (AD-26)..." -ForegroundColor Green
$ComposeContent = Get-Content "$ProjectRoot/docker-compose.yml" -Raw

if ($ComposeContent -notmatch "(?m)^name:\s*ferrous-cx") {
    throw "docker-compose.yml is missing top-level 'name: ferrous-cx'"
}

Push-Location "$ProjectRoot"
try {
    $configYaml = docker compose config
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
Write-Host "  [OK] docker-compose.yml passed Compose v2 validation." -ForegroundColor Green

# 3. Validate Day-0 Hardened Runtime Defense Constraints (AD-24)
Write-Host "`n[3/4] Auditing Day-0 kernel hardening constraints (AD-24)..." -ForegroundColor Green

$requiredServices = @("order-projection", "order-saga", "ferrous-api", "ferrous-agent", "ferrous-frontend", "ferrous-reverseproxy")
foreach ($svc in $requiredServices) {
    $svcMatch = [regex]::Match($ComposeContent, "(?ms)^\s{2}${svc}:.*?(?=^\s{2}[a-zA-Z]|\Z)")
    if (-not $svcMatch.Success) {
        throw "Service '$svc' not found in manifest"
    }
    $block = $svcMatch.Value
    if ($block -notmatch "read_only:\s*true") {
        throw "Service $svc is missing 'read_only: true'"
    }
    if ($block -notmatch "cap_drop:\s*-\s*ALL") {
        throw "Service $svc is missing 'cap_drop: [ALL]'"
    }
    if ($block -notmatch "no-new-privileges:true") {
        throw "Service $svc is missing 'security_opt: [no-new-privileges:true]'"
    }
    if ($block -notmatch "tmpfs:") {
        throw "Service $svc is missing 'tmpfs:' declaration"
    }
    Write-Host "  [OK] Service '$svc' enforces strict Day-0 runtime containment." -ForegroundColor Green
}

# Verify order-saga explicit healthcheck probe
$orderSagaMatch = [regex]::Match($ComposeContent, "(?ms)^\s{2}order-saga:.*?(?=^\s{2}[a-zA-Z]|\Z)")
if ($orderSagaMatch.Value -notmatch 'test:\s*\["CMD",\s*"/order_saga",\s*"--healthcheck"\]') {
    throw "order-saga must define healthcheck test: ['CMD', '/order_saga', '--healthcheck']"
}
Write-Host "  [OK] Service 'order-saga' defines explicit heartbeat healthcheck probe." -ForegroundColor Green

# 4. Validate Healthcheck Graph Dependencies (Scoped per service block)
Write-Host "`n[4/4] Auditing Healthcheck Graph Dependency Sequence..." -ForegroundColor Green

function Assert-ServiceDependsOn([string]$svc, [string]$dep) {
    $svcMatch = [regex]::Match($ComposeContent, "(?ms)^\s{2}${svc}:.*?(?=^\s{2}[a-zA-Z]|\Z)")
    if (-not $svcMatch.Success) {
        throw "Service '$svc' not found in manifest"
    }
    if ($svcMatch.Value -notmatch "(?ms)${dep}:\s*condition:\s*service_healthy") {
        throw "$svc must depend on $dep (service_healthy)"
    }
}

Assert-ServiceDependsOn "ferrous-reverseproxy" "ferrous-frontend"
Assert-ServiceDependsOn "ferrous-reverseproxy" "ferrous-iam"
Assert-ServiceDependsOn "ferrous-frontend" "ferrous-iam"
Assert-ServiceDependsOn "ferrous-frontend" "ferrous-api"
Assert-ServiceDependsOn "ferrous-api" "iggy-broker"
Assert-ServiceDependsOn "ferrous-api" "sierradb"
Assert-ServiceDependsOn "ferrous-api" "ferrous-iam"
Assert-ServiceDependsOn "order-projection" "iggy-broker"
Assert-ServiceDependsOn "order-projection" "sierradb"
Assert-ServiceDependsOn "order-saga" "iggy-broker"
Assert-ServiceDependsOn "order-saga" "sierradb"
Assert-ServiceDependsOn "order-saga" "ferrous-api"
Assert-ServiceDependsOn "ferrous-agent" "ferrous-api"

Write-Host "  [OK] Healthcheck graph enforces deterministic startup sequence." -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  [OK] COMPOSE TOPOLOGY AUDIT COMPLETED CLEANLY!          " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
