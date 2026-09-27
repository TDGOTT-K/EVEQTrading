param(
    [string]$RemoteHost = $env:EVEQ_REMOTE_HOST,
    [string]$SshKeyPath = $env:EVEQ_SSH_KEY_PATH,
    [string]$RemoteReportScript = $(if ($env:EVEQ_REMOTE_REPORT_SCRIPT) { $env:EVEQ_REMOTE_REPORT_SCRIPT } else { "/srv/eveqtrading/app/EVEQTrading/deploy/scripts/export_market_shortlist_report.sh" }),
    [string]$RemoteAppDir = $(if ($env:EVEQ_REMOTE_APP_DIR) { $env:EVEQ_REMOTE_APP_DIR } else { "/srv/eveqtrading/app/EVEQTrading" }),
    [string]$RemoteVenvDir = $(if ($env:EVEQ_REMOTE_VENV_DIR) { $env:EVEQ_REMOTE_VENV_DIR } else { "/srv/eveqtrading/app/EVEQTrading/.venv" }),
    [string]$RemotePythonBin = $(if ($env:EVEQ_REMOTE_PYTHON_BIN) { $env:EVEQ_REMOTE_PYTHON_BIN } else { "python3" }),
    [string]$RemoteDbPath = $(if ($env:EVEQ_REMOTE_DB_PATH) { $env:EVEQ_REMOTE_DB_PATH } else { "/srv/eveqtrading/data/eveq.db" }),
    [string]$RemoteRunsRoot = $(if ($env:EVEQ_REMOTE_RUNS_ROOT) { $env:EVEQ_REMOTE_RUNS_ROOT } else { "/srv/eveqtrading/runs" }),
    [string]$RemoteLogDir = $(if ($env:EVEQ_REMOTE_LOG_DIR) { $env:EVEQ_REMOTE_LOG_DIR } else { "/srv/eveqtrading/logs" }),
    [string]$RemoteExportRoot = $(if ($env:EVEQ_REMOTE_EXPORT_ROOT) { $env:EVEQ_REMOTE_EXPORT_ROOT } else { "/srv/eveqtrading/export" }),
    [double]$TotalBudgetIsk = $(if ($env:EVEQ_TOTAL_BUDGET_ISK) { [double]$env:EVEQ_TOTAL_BUDGET_ISK } else { 50000000000 }),
    [ValidateSet("conservative", "standard", "aggressive")]
    [string]$RiskProfile = $(if ($env:EVEQ_RISK_PROFILE) { $env:EVEQ_RISK_PROFILE } else { "standard" }),
    [int]$TopOpportunities = $(if ($env:EVEQ_TOP_OPPORTUNITIES) { [int]$env:EVEQ_TOP_OPPORTUNITIES } else { 50 }),
    [int]$TopNearMisses = $(if ($env:EVEQ_TOP_NEAR_MISSES) { [int]$env:EVEQ_TOP_NEAR_MISSES } else { 10 }),
    [int]$MaxRows = $(if ($env:EVEQ_MAX_ROWS) { [int]$env:EVEQ_MAX_ROWS } else { 20 }),
    [string]$ScanRunId = $env:EVEQ_SCAN_RUN_ID,
    [string]$PreferLatestSelectedText = $(if ($null -ne $env:EVEQ_PREFER_LATEST_SELECTED -and $env:EVEQ_PREFER_LATEST_SELECTED -ne "") { $env:EVEQ_PREFER_LATEST_SELECTED } else { "1" }),
    [string]$LocalOutputRoot = $(if ($env:EVEQ_LOCAL_REPORT_ROOT) { $env:EVEQ_LOCAL_REPORT_ROOT } else { (Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path "runs\\server-reports") }),
    [string]$SshBin = $(if ($env:EVEQ_SSH_BIN) { $env:EVEQ_SSH_BIN } else { "ssh" }),
    [string]$ScpBin = $(if ($env:EVEQ_SCP_BIN) { $env:EVEQ_SCP_BIN } else { "scp" })
)

$ErrorActionPreference = "Stop"

function Require-Value {
    param(
        [string]$Name,
        [object]$Value
    )
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        throw "Missing required parameter or environment variable: $Name"
    }
}

function Quote-Bash {
    param([string]$Value)
    $singleQuoteReplacement = [string]::Concat("'", '"', "'", '"', "'")
    return "'" + ($Value -replace "'", $singleQuoteReplacement) + "'"
}

function Parse-BoolText {
    param(
        [string]$Value,
        [bool]$Default = $false
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Default
    }
    switch ($Value.Trim().ToLowerInvariant()) {
        "1" { return $true }
        "true" { return $true }
        "yes" { return $true }
        "y" { return $true }
        "on" { return $true }
        "0" { return $false }
        "false" { return $false }
        "no" { return $false }
        "n" { return $false }
        "off" { return $false }
        default { return $Default }
    }
}

Require-Value -Name "RemoteHost" -Value $RemoteHost
Require-Value -Name "SshKeyPath" -Value $SshKeyPath
Require-Value -Name "RemoteReportScript" -Value $RemoteReportScript
Require-Value -Name "RemoteAppDir" -Value $RemoteAppDir
Require-Value -Name "RemoteDbPath" -Value $RemoteDbPath
Require-Value -Name "RemoteExportRoot" -Value $RemoteExportRoot

$PreferLatestSelected = Parse-BoolText -Value $PreferLatestSelectedText -Default $true

$remoteReportDir = "$RemoteExportRoot/market-shortlist"
$stableMarkdownRemotePath = "$remoteReportDir/market_shortlist.md"
$stableJsonRemotePath = "$remoteReportDir/market_shortlist.json"
$stableManifestRemotePath = "$remoteReportDir/market_shortlist_manifest.json"

$stagingDir = Join-Path $env:TEMP ("eveq-market-shortlist-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $stagingDir | Out-Null
New-Item -ItemType Directory -Force -Path $LocalOutputRoot | Out-Null

$sshOptions = @(
    "-i", $SshKeyPath,
    "-o", "IdentitiesOnly=yes",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30"
)

$remoteEnv = @(
    "EVEQ_APP_DIR=" + (Quote-Bash $RemoteAppDir),
    "EVEQ_VENV_DIR=" + (Quote-Bash $RemoteVenvDir),
    "EVEQ_PYTHON_BIN=" + (Quote-Bash $RemotePythonBin),
    "EVEQ_DB_PATH=" + (Quote-Bash $RemoteDbPath),
    "EVEQ_RUNS_ROOT=" + (Quote-Bash $RemoteRunsRoot),
    "EVEQ_LOG_DIR=" + (Quote-Bash $RemoteLogDir),
    "EVEQ_EXPORT_ROOT=" + (Quote-Bash $RemoteExportRoot),
    "EVEQ_TOTAL_BUDGET_ISK=" + (Quote-Bash ([string]$TotalBudgetIsk)),
    "EVEQ_RISK_PROFILE=" + (Quote-Bash $RiskProfile),
    "EVEQ_TOP_OPPORTUNITIES=" + (Quote-Bash ([string]$TopOpportunities)),
    "EVEQ_TOP_NEAR_MISSES=" + (Quote-Bash ([string]$TopNearMisses)),
    "EVEQ_MAX_ROWS=" + (Quote-Bash ([string]$MaxRows)),
    "EVEQ_PREFER_LATEST_SELECTED=" + (Quote-Bash ($(if ($PreferLatestSelected) { "1" } else { "0" })))
)
if (-not [string]::IsNullOrWhiteSpace($ScanRunId)) {
    $remoteEnv += "EVEQ_SCAN_RUN_ID=" + (Quote-Bash $ScanRunId)
}
$remoteCommand = ($remoteEnv -join " ") + " bash " + (Quote-Bash $RemoteReportScript)

& $SshBin @sshOptions $RemoteHost $remoteCommand
if ($LASTEXITCODE -ne 0) {
    throw "Remote market shortlist export failed with exit code $LASTEXITCODE"
}

$manifestLocalPath = Join-Path $stagingDir "market_shortlist_manifest.json"
$markdownLocalPath = Join-Path $stagingDir "market_shortlist.md"
$jsonLocalPath = Join-Path $stagingDir "market_shortlist.json"

& $ScpBin @sshOptions "$RemoteHost`:$stableManifestRemotePath" $manifestLocalPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to download manifest from remote host"
}
& $ScpBin @sshOptions "$RemoteHost`:$stableMarkdownRemotePath" $markdownLocalPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to download markdown report from remote host"
}
& $ScpBin @sshOptions "$RemoteHost`:$stableJsonRemotePath" $jsonLocalPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to download json report from remote host"
}

$manifest = Get-Content -Path $manifestLocalPath -Raw | ConvertFrom-Json
$scanRunIdForPath = [string]$manifest.report.scan_summary.scan_run_id
if ([string]::IsNullOrWhiteSpace($scanRunIdForPath)) {
    $scanRunIdForPath = "unknown-scan"
}
$exportedAt = [string]$manifest.export_generated_at_utc
if ([string]::IsNullOrWhiteSpace($exportedAt)) {
    $exportedAt = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
} else {
    $exportedAt = $exportedAt.Replace(":", "").Replace("-", "")
}

$safeScanRunId = ($scanRunIdForPath -replace '[^A-Za-z0-9._-]', "_")
$localReportDir = Join-Path $LocalOutputRoot ("market-shortlist-" + $exportedAt + "-" + $safeScanRunId)
New-Item -ItemType Directory -Force -Path $localReportDir | Out-Null

$finalManifestPath = Join-Path $localReportDir "market_shortlist_manifest.json"
$finalMarkdownPath = Join-Path $localReportDir "market_shortlist.md"
$finalJsonPath = Join-Path $localReportDir "market_shortlist.json"

Copy-Item -LiteralPath $manifestLocalPath -Destination $finalManifestPath -Force
Copy-Item -LiteralPath $markdownLocalPath -Destination $finalMarkdownPath -Force
Copy-Item -LiteralPath $jsonLocalPath -Destination $finalJsonPath -Force
Remove-Item -LiteralPath $stagingDir -Recurse -Force

Write-Output ("Local report dir: " + $localReportDir)
Write-Output ("Markdown: " + $finalMarkdownPath)
Write-Output ("JSON: " + $finalJsonPath)
Write-Output ("Manifest: " + $finalManifestPath)
