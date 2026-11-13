<#
.SYNOPSIS
    Quickstart scenario G (US6, SC-008, SC-009, T109): one default run of server/benchmark-notification.ps1 with 100
    subscribers in which Notification is stopped for the whole Load Client run and started when it exits; then the
    drain, the evidence export, and ReconcileMain --mode=fixed, into us6-unchanged/<RunName>/.

.DESCRIPTION
    The harness's -Disruption notification-stopped stops Notification once the subscribers are ready (they register
    through it) and starts it again after the Load Client exits. The service passwords come from the four
    *_DB_PASSWORD variables, copied from the user environment when the process has none; KAFKA_HOME must be set or
    passed. The harness transcript goes to <RunName>.harness.txt.

.EXAMPLE
    load-client/reports/notification-service/us6-unchanged/run-scenario-g.ps1 -NginxBin C:\...\nginx.exe
#>
param(
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [string]$RunName = 'g-notification-stopped',
    [string]$Seed = '20261111',
    [string]$KafkaHome,
    [ValidateRange(0, 64)][double]$IdleCpuCores = 2.0,
    [string[]]$BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR', 'Arknights')
)

$ErrorActionPreference = 'Stop'
# powershell.exe -File passes "-BlockingProcess a,b" as one string.
$BlockingProcess = @($BlockingProcess | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../../..')).Path
$harness = Join-Path $repo 'server/benchmark-notification.ps1'
foreach ($name in @('TRANSACTION', 'INVENTORY', 'ANALYTICS', 'NOTIFICATION')) {
    $variable = "${name}_DB_PASSWORD"
    if (-not [Environment]::GetEnvironmentVariable($variable, 'Process')) {
        [Environment]::SetEnvironmentVariable($variable, [Environment]::GetEnvironmentVariable($variable, 'User'), 'Process')
    }
}
if ($KafkaHome) { $env:KAFKA_HOME = $KafkaHome }

$parameters = @{
    Workload = 'default'; Subscribers = 100; Seed = $Seed; RunName = "us6-unchanged/$RunName"
    Disruption = 'notification-stopped'; DrainSeconds = 300; NginxBin = $NginxBin
    IdleCpuCores = $IdleCpuCores; BlockingProcess = $BlockingProcess
}
$started = Get-Date
Write-Host "=== scenario G -> us6-unchanged/$RunName ($($started.ToString('s'))); blocking: $($BlockingProcess -join ', ')"
$global:LASTEXITCODE = $null
try {
    & $harness @parameters *> (Join-Path $PSScriptRoot "$RunName.harness.txt")
    $exit = $LASTEXITCODE
} catch {
    $exit = "error: $_"
}
Write-Host "=== finished with exit code $exit after $([Math]::Round(((Get-Date) - $started).TotalMinutes, 1)) min"
