<#
.SYNOPSIS
    Automated Docker Compose validation script for FerrousCX master topology (Story 12.1).
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
Write-Host "  [OK] BRIDGE_IMAGE correctly pinned to immutable short SHA." -ForegroundColor Green

# 2. Validate Compose Syntax via docker compose config
Write-Host "`n[2/4] Validating Docker Compose syntax & include directives (AD-26)..." -ForegroundColor Green
$dockerComposeText = Get-Content "$ProjectRoot/docker/docker-compose.yml" -Raw
$rootComposeText = Get-Content "$ProjectRoot/docker-compose.yml" -Raw

if ($dockerComposeText -notmatch "(?m)^name:\s*ferrous-cx") {
    throw "docker/docker-compose.yml is missing top-level 'name: ferrous-cx'"
}
if ($rootComposeText -notmatch "(?m)^name:\s*ferrous-cx") {
    throw "Root docker-compose.yml is missing top-level 'name: ferrous-cx'"
}

Push-Location "$ProjectRoot/docker"
try {
    $configYaml = docker compose config
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
Write-Host "  [OK] docker/docker-compose.yml passed Compose v2 validation." -ForegroundColor Green

Push-Location "$ProjectRoot"
try {
    $rootConfig = docker compose config
    if ($LASTEXITCODE -ne 0) {
        throw "Root docker-compose.yml config failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
Write-Host "  [OK] Root docker-compose.yml wrapper passed Compose v2 validation." -ForegroundColor Green

# 3. Validate Day-0 Hardened Runtime Defense Constraints (AD-24)
Write-Host "`n[3/4] Auditing Day-0 kernel hardening constraints (AD-24)..." -ForegroundColor Green
$ComposeContent = Get-Content "$ProjectRoot/docker/docker-compose.yml" -Raw

$requiredServices = @("command-handler", "ferrous-api", "ferrous-agent", "ferrous-frontend", "ferrous-reverseproxy")
foreach ($svc in $requiredServices) {
    if ($ComposeContent -notmatch "(?ms)${svc}:.*?read_only:\s*true") {
        throw "Service $svc is missing 'read_only: true'"
    }
    if ($ComposeContent -notmatch "(?ms)${svc}:.*?cap_drop:\s*-\s*ALL") {
        throw "Service $svc is missing 'cap_drop: [ALL]'"
    }
    if ($ComposeContent -notmatch "(?ms)${svc}:.*?no-new-privileges:true") {
        throw "Service $svc is missing 'security_opt: [no-new-privileges:true]'"
    }
    if ($ComposeContent -notmatch "(?ms)${svc}:.*?tmpfs:") {
        throw "Service $svc is missing 'tmpfs:' declaration"
    }
    Write-Host "  [OK] Service '$svc' enforces strict Day-0 runtime containment." -ForegroundColor Green
}

# 4. Validate Healthcheck Graph Dependencies
Write-Host "`n[4/4] Auditing Healthcheck Graph Dependency Sequence..." -ForegroundColor Green
if ($ComposeContent -notmatch "(?ms)ferrous-reverseproxy:.*?ferrous-frontend:\s*condition:\s*service_healthy") {
    throw "ferrous-reverseproxy must depend on ferrous-frontend (service_healthy)"
}
if ($ComposeContent -notmatch "(?ms)ferrous-reverseproxy:.*?ferrous-iam:\s*condition:\s*service_healthy") {
    throw "ferrous-reverseproxy must depend on ferrous-iam (service_healthy)"
}
if ($ComposeContent -notmatch "(?ms)ferrous-frontend:.*?ferrous-iam:\s*condition:\s*service_healthy") {
    throw "ferrous-frontend must depend on ferrous-iam (service_healthy)"
}
if ($ComposeContent -notmatch "(?ms)ferrous-api:.*?iggy-broker:\s*condition:\s*service_healthy") {
    throw "ferrous-api must depend on iggy-broker (service_healthy)"
}
if ($ComposeContent -notmatch "(?ms)ferrous-api:.*?ferrous-iam:\s*condition:\s*service_healthy") {
    throw "ferrous-api must depend on ferrous-iam (service_healthy)"
}
if ($ComposeContent -notmatch "(?ms)command-handler:.*?iggy-broker:\s*condition:\s*service_healthy") {
    throw "command-handler must depend on iggy-broker (service_healthy)"
}
Write-Host "  [OK] Healthcheck graph enforces deterministic startup sequence." -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  [OK] COMPOSE TOPOLOGY AUDIT COMPLETED CLEANLY!          " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
