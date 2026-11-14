<#
.SYNOPSIS
    Runs a list of server/benchmark-notification.ps1 runs one after another (007 phase 10: CP2, scaling, isolation,
    CP4), each after its own fresh baseline, and logs each run's exit code.

.DESCRIPTION
    Each -Runs entry is "<RunName>=<Workload>=<Subscribers>[=<Fault>]", for example "scaling/10=default=10" or
    "isolation/reject-1=default=100=reject". Every run uses -Seed 20261111, -SkusPerSubscriber 20,
    -FaultSubscriber sub-0001, and -DrainSeconds 300. The service passwords come from the four *_DB_PASSWORD
    variables, copied from the user environment when the process has none; KAFKA_HOME must be set or passed. Each
    harness transcript goes to <RunName>.harness.txt beside its run folder.

.EXAMPLE
    load-client/reports/notification-service/run-evidence.ps1 -NginxBin C:\...\nginx.exe -Runs cp2/default-1=default=100
#>
param(
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [Parameter(Mandatory = $true)][string[]]$Runs,
    [string]$KafkaHome,
    [ValidateRange(0, 64)][double]$IdleCpuCores = 2.0,
    [string[]]$BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR', 'Arknights')
)

$ErrorActionPreference = 'Stop'
# powershell.exe -File passes "-Runs a,b" as one string.
$Runs = @($Runs | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$BlockingProcess = @($BlockingProcess | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$harness = Join-Path $repo 'server/benchmark-notification.ps1'
foreach ($name in @('TRANSACTION', 'INVENTORY', 'ANALYTICS', 'NOTIFICATION')) {
    $variable = "${name}_DB_PASSWORD"
    if (-not [Environment]::GetEnvironmentVariable($variable, 'Process')) {
        [Environment]::SetEnvironmentVariable($variable, [Environment]::GetEnvironmentVariable($variable, 'User'), 'Process')
    }
}
if ($KafkaHome) { $env:KAFKA_HOME = $KafkaHome }

Write-Host "=== $($Runs.Count) run(s); idle gate: $IdleCpuCores cores; blocking: $($BlockingProcess -join ', ')"
foreach ($entry in $Runs) {
    $parts = $entry -split '='
    if ($parts.Count -lt 3 -or $parts.Count -gt 4) { throw "Bad -Runs entry '$entry': expected RunName=Workload=Subscribers[=Fault]" }
    $runName = $parts[0]; $workload = $parts[1]; $subscribers = [int]$parts[2]
    $fault = if ($parts.Count -eq 4) { $parts[3] } else { 'none' }
    $parameters = @{
        Workload = $workload; Subscribers = $subscribers; Seed = '20261111'; SkusPerSubscriber = 20
        Fault = $fault; FaultSubscriber = 'sub-0001'; RunName = $runName; DrainSeconds = 300; NginxBin = $NginxBin
        IdleCpuCores = $IdleCpuCores; BlockingProcess = $BlockingProcess
    }
    $transcript = Join-Path $PSScriptRoot "$runName.harness.txt"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $transcript) | Out-Null
    $started = Get-Date
    Write-Host "=== $runName ($workload, $subscribers subscribers, fault $fault) started $($started.ToString('s'))"
    $global:LASTEXITCODE = $null
    try {
        & $harness @parameters *> $transcript
        $exit = $LASTEXITCODE
    } catch {
        $exit = "error: $_"
    }
    Write-Host "=== $runName finished with exit code $exit after $([Math]::Round(((Get-Date) - $started).TotalMinutes, 1)) min"
}
Write-Host '=== all runs finished'
