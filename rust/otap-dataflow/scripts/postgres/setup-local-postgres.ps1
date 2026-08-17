<#
.SYNOPSIS
Starts and seeds a local PostgreSQL container for receiver development.

.DESCRIPTION
Local development only. Reuses the named container when it already exists,
waits for PostgreSQL to become ready, and resets the application_logs table.
Runs attached by default so Ctrl+C stops PostgreSQL. Use -Detached to leave it
running after setup. Docker Compose is the only prerequisite.
#>
[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int]$HostPort = 54321,

    [switch]$Detached,

    [switch]$Stop
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$database = 'telemetry'
$user = 'otel'
$password = 'localdev'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'

function Invoke-Docker {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    $output = & docker @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }

    return $output
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    Invoke-Docker (@('compose', '--file', $composeFile) + $Arguments)
}

function Invoke-PostgresSqlFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $sql = [System.IO.File]::ReadAllText($Path)
    Invoke-Compose @(
        'exec',
        '--no-TTY',
        '--env', "PGPASSWORD=$password",
        'postgres',
        'psql',
        '--username', $user,
        '--dbname', $database,
        '--set', 'ON_ERROR_STOP=1',
        '--command', $Sql
    ) | Out-Null
}

function Set-ReceiverConnectionString {
    param(
        [Parameter(Mandatory)]
        [string]$ConnectionString
    )

    $env:POSTGRES_CONNECTION_STRING = $ConnectionString

    $workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $envFile = Join-Path $workspaceRoot 'target\local-postgres.env'
    $null = New-Item (Split-Path $envFile -Parent) -ItemType Directory -Force
    [System.IO.File]::WriteAllText(
        $envFile,
        "POSTGRES_CONNECTION_STRING=`"$ConnectionString`"",
        [System.Text.UTF8Encoding]::new($false)
    )

    return $envFile
}

if (-not (Get-Command docker -CommandType Application -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI was not found on PATH.'
}

Invoke-Docker @('info', '--format', '{{.ServerVersion}}') | Out-Null
Invoke-Docker @('compose', 'version', '--short') | Out-Null

if ($Stop) {
    if ($Detached) {
        throw '-Stop and -Detached cannot be used together.'
    }

    Invoke-Compose @('down') | Out-Null
    Write-Host 'PostgreSQL container stopped and removed. The data volume was preserved.'
    return
}

$env:POSTGRES_HOST_PORT = $HostPort
Invoke-Compose @('up', '--detach', '--wait', 'postgres') | Out-Null

Invoke-PostgresSqlFile -Path (Join-Path $PSScriptRoot 'seed.sql')

$connectionString =
    "host=127.0.0.1 port=$HostPort dbname=$database user=$user password=$password sslmode=disable"

$envFile = Set-ReceiverConnectionString -ConnectionString $connectionString

Write-Host ''
Write-Host "PostgreSQL is running and seeded on 127.0.0.1:$HostPort."
Write-Host 'POSTGRES_CONNECTION_STRING is set for this PowerShell process.'
Write-Host "CodeLLDB environment written to $envFile."

if (-not $Detached) {
    Write-Host 'Attaching to PostgreSQL. Press Ctrl+C to stop it.'
    & docker compose --file $composeFile up postgres
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up postgres failed with exit code $LASTEXITCODE."
    }
}
