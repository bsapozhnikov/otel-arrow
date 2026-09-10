[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('auth', 'syslog', 'syslog-la')]
    [string]$Scenario
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$exampleDirectory = Split-Path -Parent $PSScriptRoot
$composeFiles = @(
    (Join-Path $exampleDirectory 'compose.yaml')
    (Join-Path $exampleDirectory 'compose.dataflow.yaml')
)

if ($Scenario -eq 'syslog-la') {
    $composeFiles += Join-Path $exampleDirectory 'compose.azure.yaml'
}

$Env:KAFKA_SCENARIO = $Scenario

foreach ($composeFile in $composeFiles) {
    '-f'
    $composeFile
}
