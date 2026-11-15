<#
.SYNOPSIS
    Cross-Platform Kanidm IAM Dev TLS Cert & Persona Provisioner (Story 13.5).
.DESCRIPTION
    Delegates to `cargo run --bin setup-iam` for pure-Rust cert generation and IAM persona auditing.
#>
[CmdletBinding()]
param(
    [switch]$GenerateCerts,
    [switch]$SeedRbac,
    [switch]$SeedM2m,
    [switch]$All,
    [switch]$Force,
    [switch]$DryRun,
    [string]$CertsDir = "",
    [string]$ConfigDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path (Join-Path $ScriptDir "..")

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Error "[ERROR] Cargo toolchain is required to run setup-iam."
    exit 1
}

$cargoArgs = @("run", "--bin", "setup-iam", "--")

if ($GenerateCerts) { $cargoArgs += "--generate-certs" }
if ($SeedRbac)      { $cargoArgs += "--seed-rbac" }
if ($SeedM2m)       { $cargoArgs += "--seed-m2m" }
if ($All)           { $cargoArgs += "--all" }
if ($Force)         { $cargoArgs += "--force" }
if ($DryRun)        { $cargoArgs += "--dry-run" }
if ($CertsDir)      { $cargoArgs += @("--certs-dir", $CertsDir) }
if ($ConfigDir)     { $cargoArgs += @("--config-dir", $ConfigDir) }

Push-Location $ProjectRoot
try {
    & cargo @cargoArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}
