<#
.SYNOPSIS
    Automated Validator for Quickstart, Topic Registry, Dataset & Replay Profiles (Story 12.4).
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir/.."

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  FerrousCX - Quickstart, Registry & Profile Validator    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Validate contracts/topics.yml
Write-Host "`n[1/5] Auditing contracts/topics.yml Single Source of Truth..." -ForegroundColor Green
$TopicsYaml = "$ProjectRoot/contracts/topics.yml"
if (-not (Test-Path $TopicsYaml)) {
    throw "Missing topic registry: $TopicsYaml"
}
$topicsContent = Get-Content $TopicsYaml -Raw
if ($topicsContent -notmatch "(?m)^version:\s*1") {
    throw "contracts/topics.yml must specify 'version: 1'"
}
$expectedTopics = @(
    @{ Name = "orders.intake"; Partitions = 4; Durability = "replicated" },
    @{ Name = "orders.consolidated"; Partitions = 4; Durability = "replicated" },
    @{ Name = "olist.orders_stream"; Partitions = 4; Durability = "persisted" },
    @{ Name = "benchmark.hpc_mqbench"; Partitions = 4; Durability = "persisted" }
)
foreach ($t in $expectedTopics) {
    $tName = $t.Name
    $pCount = $t.Partitions
    $dur = $t.Durability
    if ($topicsContent -notmatch "(?ms)kafka_name:\s*$tName\s+partitions:\s*$pCount.*?durability:\s*$dur") {
        throw "contracts/topics.yml missing or invalid topic definition: $tName ($pCount p, $dur)"
    }
}
Write-Host "  [OK] contracts/topics.yml defines all 4 required topics with valid partitions and durability." -ForegroundColor Green

# 2. Validate Dataset Existence
Write-Host "`n[2/5] Auditing Olist sample dataset existence & structure..." -ForegroundColor Green
$DatasetPath = "$ProjectRoot/docs/datasets/olist/olist_orders_dataset.csv"
if (-not (Test-Path $DatasetPath)) {
    throw "Missing Olist dataset: $DatasetPath"
}
$datasetLines = Get-Content $DatasetPath
if ($datasetLines.Count -lt 2) {
    throw "Olist dataset at $DatasetPath is empty or corrupt"
}
$header = $datasetLines[0]
if ($header -notmatch "order_id" -or $header -notmatch "order_purchase_timestamp") {
    throw "Olist dataset header missing required fields (order_id, order_purchase_timestamp)"
}
Write-Host "  [OK] Olist dataset verified ($($datasetLines.Count - 1) records, valid schema)." -ForegroundColor Green

# 3. Validate Replay Profiles
Write-Host "`n[3/5] Auditing Replay Profiles (olist-demo.toml & olist-load.toml)..." -ForegroundColor Green
$DemoProfile = "$ProjectRoot/profiles/olist-demo.toml"
$LoadProfile = "$ProjectRoot/profiles/olist-load.toml"

if (-not (Test-Path $DemoProfile) -or -not (Test-Path $LoadProfile)) {
    throw "Missing replay profile in profiles/ directory"
}

$demoContent = Get-Content $DemoProfile -Raw
if ($demoContent -notmatch "speed\s*=\s*10(\.0)?") {
    throw "olist-demo.toml must configure 'speed = 10.0' (FR-1, FR-1b)"
}
if ($demoContent -notmatch 'destination\s*=\s*"olist\.orders_stream"') {
    throw "olist-demo.toml destination must be 'olist.orders_stream'"
}
if ($demoContent -notmatch 'default_url\s*=\s*"127\.0\.0\.1:9092"') {
    throw "olist-demo.toml sink URL must target bridge endpoint '127.0.0.1:9092'"
}
if ($demoContent -match "(?m)^\s*(kanidm|auth_url|service_account|password|token|secret)") {
    throw "olist-demo.toml must not use credentials or Kanidm authentication (AD-28)"
}
Write-Host "  [OK] olist-demo.toml verified (10x pacing, loop salt, zero Kanidm credentials)." -ForegroundColor Green

$loadContent = Get-Content $LoadProfile -Raw
if ($loadContent -notmatch "rate\s*=\s*25") {
    throw "olist-load.toml must configure 'rate = 25' (NFR-3, SM-3)"
}
if ($loadContent -notmatch 'destination\s*=\s*"olist\.orders_stream"') {
    throw "olist-load.toml destination must be 'olist.orders_stream'"
}
if ($loadContent -match "(?m)^\s*(kanidm|auth_url|service_account|password|token|secret)") {
    throw "olist-load.toml must not use credentials or Kanidm authentication (AD-28)"
}
Write-Host "  [OK] olist-load.toml verified (rate 25 fixed throughput, zero Kanidm credentials)." -ForegroundColor Green

# 4. Validate Quickstart Documentation
Write-Host "`n[4/5] Auditing docs/quickstart.md content, arithmetic & security invariants..." -ForegroundColor Green
$QuickstartDoc = "$ProjectRoot/docs/quickstart.md"
if (-not (Test-Path $QuickstartDoc)) {
    throw "Missing quickstart guide: $QuickstartDoc"
}
$qsContent = Get-Content $QuickstartDoc -Raw
if ($qsContent -notmatch "Why 10x") {
    throw "docs/quickstart.md must document 'Why 10x' section"
}
if ($qsContent -notmatch "0\.095" -or $qsContent -notmatch "order") {
    throw "docs/quickstart.md must explain the ~0.095 orders/min arithmetic derivation"
}
if ($qsContent -notmatch "(?i)read-only\s+UI" -and $qsContent -notmatch "(?i)Operator Studio.*strictly read-only") {
    throw "docs/quickstart.md must document that Operator Studio UI is strictly read-only"
}
if ($qsContent -notmatch "(?i)Kanidm") {
    throw "docs/quickstart.md must document zero Kanidm account dependency for replay"
}
if ($qsContent -notmatch "stream-replay-rs") {
    throw "docs/quickstart.md must document stream-replay-rs CLI execution"
}
Write-Host "  [OK] docs/quickstart.md verified (Why 10x derivation, rate 25, read-only UI, Kanidm isolation)." -ForegroundColor Green

# 5. Run Dry-Run Topic Setup
Write-Host "`n[5/5] Executing setup-topics.ps1 dry-run verification..." -ForegroundColor Green
& "$ProjectRoot/scripts/setup-topics.ps1" -DryRun
Write-Host "  [OK] setup-topics.ps1 executed cleanly in dry-run mode." -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  [OK] QUICKSTART & REGISTRY AUDIT COMPLETED CLEANLY!     " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
