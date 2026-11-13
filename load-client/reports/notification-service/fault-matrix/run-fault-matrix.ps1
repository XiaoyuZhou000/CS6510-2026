<#
.SYNOPSIS
    Quickstart scenario F (US5, SC-005, T103): one default run of server/benchmark-notification.ps1 with 100
    subscribers per fault of the matrix, each into fault-matrix/<item>/.

.DESCRIPTION
    Items, injected 20 s into the load with the harness's -Disruption:
      1  notification-selection-crash  notification.selection.afterCommit:halt through ops/faults, then a restart
      2  notification-send-crash       notification.delivery.afterSend:halt through ops/faults, then a restart
      3  transaction-restart           Transaction stopped and started again (006 procedure)
      4  broker-outage                 deploy/kafka/stop-broker.ps1, 30 s, start-broker.ps1
      5  subscriber-outage             SubscriberMain stopped for 30 s, then restarted with --resume
      6  malformed-sale                PublishRawRecord sends a malformed record to the sale topic
    The runs are sequential and each waits for an idle machine. Each harness transcript goes to <item>.harness.txt.
    The service passwords come from the four *_DB_PASSWORD variables, copied from the user environment when the
    process has none; KAFKA_HOME must be set or passed.

.EXAMPLE
    load-client/reports/notification-service/fault-matrix/run-fault-matrix.ps1 -NginxBin C:\...\nginx.exe -Items 1,2
#>
param(
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [string[]]$Items = @('1', '2', '3', '4', '5', '6'),
    [string]$Seed = '20261111',
    [string]$KafkaHome,
    [ValidateRange(0, 64)][double]$IdleCpuCores = 2.0,
    [string[]]$BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR', 'Arknights')
)

$ErrorActionPreference = 'Stop'
# powershell.exe -File passes "-Items 1,2" as one string.
$Items = @($Items | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../../..')).Path
$harness = Join-Path $repo 'server/benchmark-notification.ps1'
$matrix = [ordered]@{
    '1' = @('1-selection-crash', 'notification-selection-crash')
    '2' = @('2-send-crash', 'notification-send-crash')
    '3' = @('3-transaction-restart', 'transaction-restart')
    '4' = @('4-broker-outage', 'broker-outage')
    '5' = @('5-subscriber-outage', 'subscriber-outage')
    '6' = @('6-malformed-sale', 'malformed-sale')
}
foreach ($name in @('TRANSACTION', 'INVENTORY', 'ANALYTICS', 'NOTIFICATION')) {
    $variable = "${name}_DB_PASSWORD"
    if (-not [Environment]::GetEnvironmentVariable($variable, 'Process')) {
        [Environment]::SetEnvironmentVariable($variable, [Environment]::GetEnvironmentVariable($variable, 'User'), 'Process')
    }
}
if ($KafkaHome) { $env:KAFKA_HOME = $KafkaHome }

foreach ($item in $Items) {
    if (-not $matrix.Contains($item)) { throw "Unknown item $item; use 1-6" }
    $folder, $kind = $matrix[$item]
    $parameters = @{
        Workload = 'default'; Subscribers = 100; Seed = $Seed; RunName = "fault-matrix/$folder"; Disruption = $kind
        DisruptAfterSeconds = 20; OutageSeconds = 30; DrainSeconds = 300; NginxBin = $NginxBin
        IdleCpuCores = $IdleCpuCores; BlockingProcess = $BlockingProcess
    }
    $started = Get-Date
    Write-Host "=== scenario F item ${item}: $kind -> fault-matrix/$folder ($($started.ToString('s')))"
    $global:LASTEXITCODE = $null
    try {
        & $harness @parameters *> (Join-Path $PSScriptRoot "$folder.harness.txt")
        $exit = $LASTEXITCODE
    } catch {
        $exit = "error: $_"
    }
    Write-Host "=== item $item finished with exit code $exit after $([Math]::Round(((Get-Date) - $started).TotalMinutes, 1)) min"
}
