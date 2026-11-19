<#
.SYNOPSIS
    Automated OpenTelemetry 3-Tier developer runtime & Jaeger validator (Story 12.2).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir/.."

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  FerrousCX - 3-Tier OpenTelemetry & Jaeger Topology Audit " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Validate .env.example templates (both root and docker/)
Write-Host "`n[1/5] Checking .env.example Tier 1/2/3 configuration (AD-23)..." -ForegroundColor Green
$envFiles = @("$ProjectRoot/.env.example", "$ProjectRoot/docker/.env.example")
foreach ($file in $envFiles) {
    if (-not (Test-Path $file)) {
        throw "Missing environment template: $file"
    }
    $EnvExample = Get-Content $file -Raw
    if ($EnvExample -notmatch "(?m)^OTEL_SDK_DISABLED=true") {
        throw "Missing Tier 1 switch 'OTEL_SDK_DISABLED=true' in $file"
    }
    if ($EnvExample -notmatch "(?m)^OTEL_TRACES_EXPORTER=console") {
        throw "Missing Tier 2 switch 'OTEL_TRACES_EXPORTER=console' in $file"
    }
    if ($EnvExample -notmatch "(?m)^OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317") {
        throw "Missing Tier 3 switch 'OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317' in $file"
    }
    if ($EnvExample -notmatch "(?m)^PORT_OTLP_GRPC=4317") {
        throw "Missing PORT_OTLP_GRPC in $file"
    }
    if ($EnvExample -notmatch "(?m)^PORT_OTLP_HTTP=4318") {
        throw "Missing PORT_OTLP_HTTP in $file"
    }
    if ($EnvExample -notmatch "(?m)^PORT_JAEGER_UI=16686") {
        throw "Missing PORT_JAEGER_UI in $file"
    }
    Write-Host "  [OK] Validated 3-Tier telemetry settings and ports in $file" -ForegroundColor Green
}

# 2. Validate Jaeger Service Definition in Compose
Write-Host "`n[2/5] Validating Jaeger container definition & ephemeral memory config..." -ForegroundColor Green
$ComposeContent = Get-Content "$ProjectRoot/docker-compose.yml" -Raw

if ($ComposeContent -match "(?ms)^\s{2}jaeger:.*?(?=^[a-zA-Z]|^\s{2}[a-zA-Z]|\Z)") {
    $jaegerBlock = $Matches[0]
} else {
    throw "Service 'jaeger' not found in manifest"
}

if ($jaegerBlock -notmatch "image:\s*jaegertracing/jaeger:2\.3\.0") {
    throw "Jaeger image must be pinned to jaegertracing/jaeger:2.3.0"
}
if ($jaegerBlock -notmatch ":4317") {
    throw "Jaeger must expose port 4317 for OTLP gRPC ingest"
}
if ($jaegerBlock -notmatch ":4318") {
    throw "Jaeger must expose port 4318 for OTLP HTTP ingest"
}
if ($jaegerBlock -notmatch ":16686") {
    throw "Jaeger must expose port 16686 for Web UI"
}
if ($jaegerBlock -notmatch "METRICS_STORAGE_TYPE=memory" -or $jaegerBlock -notmatch "SPAN_STORAGE_TYPE=memory") {
    throw "Jaeger must enforce ephemeral in-memory storage configuration"
}
if ($jaegerBlock -match "^\s+volumes:") {
    throw "Jaeger must have ZERO persistent volumes mounted (ephemeral in-memory requirement)"
}
if ($jaegerBlock -notmatch "memory:\s*512M") {
    throw "Jaeger must specify container memory limit (512M)"
}
Write-Host "  [OK] Jaeger container configured with OTLP gRPC/HTTP, Web UI, and ephemeral RAM storage." -ForegroundColor Green

# 3. Validate Day-0 Security Containment on Jaeger
Write-Host "`n[3/5] Validating Day-0 runtime hardening on Jaeger container (AD-24)..." -ForegroundColor Green
if ($jaegerBlock -notmatch "read_only:\s*true") {
    throw "Jaeger container is missing 'read_only: true'"
}
if ($jaegerBlock -notmatch "cap_drop:\s*-\s*ALL") {
    throw "Jaeger container is missing 'cap_drop: [ALL]'"
}
if ($jaegerBlock -notmatch "no-new-privileges:true") {
    throw "Jaeger container is missing 'security_opt: [no-new-privileges:true]'"
}
if ($jaegerBlock -notmatch "(?s)tmpfs:.*?- /tmp:noexec,nosuid,nodev,size=64M") {
    throw "Jaeger container is missing strict tmpfs declaration"
}
if ($jaegerBlock -notmatch "(?s)healthcheck:.*?http://localhost:16686/") {
    throw "Jaeger container is missing healthcheck probing http://localhost:16686/"
}
Write-Host "  [OK] Jaeger container enforces strict Day-0 runtime containment & healthcheck." -ForegroundColor Green

# 4. Validate Application Service Telemetry Propagation (Isolated Block Regex)
Write-Host "`n[4/5] Validating OTel environment propagation across application services..." -ForegroundColor Green
$otelServices = @("ferrous-api", "ferrous-agent", "ferrous-reverseproxy")
foreach ($svc in $otelServices) {
    if ($ComposeContent -match "(?ms)^\s{2}${svc}:.*?(?=^\s{2}[a-zA-Z]|\Z)") {
        $svcBlock = $Matches[0]
        if ($svcBlock -notmatch "OTEL_SDK_DISABLED=") {
            throw "Service '$svc' is missing OTEL_SDK_DISABLED propagation"
        }
        if ($svcBlock -notmatch "OTEL_TRACES_EXPORTER=") {
            throw "Service '$svc' is missing OTEL_TRACES_EXPORTER propagation"
        }
        if ($svcBlock -notmatch "OTEL_EXPORTER_OTLP_ENDPOINT=") {
            throw "Service '$svc' is missing OTEL_EXPORTER_OTLP_ENDPOINT propagation"
        }
        Write-Host "  [OK] Service '$svc' propagates 3-tier OpenTelemetry environment parameters." -ForegroundColor Green
    } else {
        throw "Service '$svc' not found in manifest"
    }
}

# 5. Validate Compose Syntax via docker compose config
Write-Host "`n[5/5] Validating Docker Compose syntax with Jaeger..." -ForegroundColor Green
Push-Location "$ProjectRoot"
try {
    $null = docker compose config
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed for docker-compose.yml"
    }
} finally {
    Pop-Location
}
Write-Host "  [OK] Compose topology with Jaeger passes validation cleanly." -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  [OK] OPENTELEMETRY & JAEGER AUDIT COMPLETED CLEANLY!    " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
