<#
.SYNOPSIS
    Automated Apache Iggy Stream, Topic & Consumer Group Provisioner (Story 12.4, Story 13.3).
.DESCRIPTION
    Delegates to the pure-Rust cross-platform Cargo CLI tool (setup-topics).
#>
[CmdletBinding()]
param(
    [Alias("url")]
    [string]$IggyUrl = $env:IGGY_HTTP_URL,
    [switch]$DryRun,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir/.."

if (Get-Command cargo -ErrorAction SilentlyContinue) {
    Push-Location $ProjectRoot
    try {
        $cargoArgs = @("run", "--bin", "setup-topics", "--")
        if ($IggyUrl) { $cargoArgs += @("--url", $IggyUrl) }
        if ($DryRun) { $cargoArgs += "--dry-run" }
        if ($RemainingArgs) { $cargoArgs += $RemainingArgs }
        & cargo @cargoArgs
        exit $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
} else {
    Write-Error "Cargo toolchain is required to run setup-topics."
    exit 1
}
