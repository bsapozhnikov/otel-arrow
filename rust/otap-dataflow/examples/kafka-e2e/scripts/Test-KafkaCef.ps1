[CmdletBinding()]
param(
    [ValidateRange(1, 300)]
    [int]$TimeoutSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ComposeArgs = & (Join-Path $PSScriptRoot 'Setup-KafkaE2E.ps1') -Scenario syslog
$runningServices = @(docker compose @ComposeArgs ps --status running --services 2>&1)
if ($LASTEXITCODE -ne 0) {
    $runningServices | Write-Host
    throw "Failed to inspect the Kafka E2E services."
}

$requiredServices = @('df-engine', 'kafka', 'rsyslog')
foreach ($service in $requiredServices) {
    if (-not ($runningServices | Where-Object { $_ -eq $service })) {
        throw "$service is not running. Start the syslog scenario before running this test."
    }
}

$marker = "kafka-cef-e2e-$([Guid]::NewGuid().ToString('N'))"
& (Join-Path $PSScriptRoot 'Send-SyslogCef.ps1') `
    -Target Rsyslog `
    -Format Cef `
    -Message $marker

$expectedPatterns = @(
    "cef.name=$marker"
    'cef.device_vendor=Security'
    'cef.device_product=threatmanager'
    'cef.device_version=1.0'
    'cef.device_event_class_id=100'
    'cef.severity=10'
    'src=10.0.0.1'
    'dst=2.1.2.2'
    'spt=1232'
    'input.format=cef'
)
$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)

do {
    $logs = docker compose @ComposeArgs logs --no-color df-engine 2>&1
    if ($LASTEXITCODE -ne 0) {
        $logs | Write-Host
        throw "Failed to read the dataflow engine logs."
    }

    $logText = $logs -join "`n"
    $missingPatterns = @(
        $expectedPatterns | Where-Object {
            $logText -notmatch [regex]::Escape($_)
        }
    )
    if ($missingPatterns.Count -eq 0) {
        Write-Host "PASS: Kafka receiver decoded CEF message $marker"
        return
    }

    Start-Sleep -Seconds 1
} while ([DateTime]::UtcNow -lt $deadline)

$logs | Select-Object -Last 100 | Write-Host
throw "CEF validation timed out. Missing: $($missingPatterns -join ', ')"
