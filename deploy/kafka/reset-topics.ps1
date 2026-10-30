<#
.SYNOPSIS
    Empties the broker for a reproducible baseline (research D13, FR-059).

.DESCRIPTION
    Kafka on Windows cannot delete a topic: renaming the open partition directory fails and takes the
    log directory offline. The reset therefore replaces the broker's log instead:
    1. Refuses to run while anything answers on the service ports 8081-8083, so no running service
       loses records or offsets it is working on.
    2. Stops the broker (stop-broker.ps1) and deletes <KAFKA_DATA_DIR>\kraft-logs, which holds every
       topic, every committed consumer offset, and the cluster metadata.
    3. Starts the broker (start-broker.ps1), which reformats the log with the same cluster ID and
       creates the four topics through create-topics.ps1.
    4. Verifies that every end offset is 0 and that none of the three consumer groups exists.

.EXAMPLE
    ./deploy/kafka/reset-topics.ps1
#>
param(
    [string]$KafkaHome,
    [string]$DataDir,
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')
$KafkaHome = Resolve-KafkaHome $KafkaHome
$DataDir = Resolve-KafkaDataDir -KafkaHome $KafkaHome -Explicit $DataDir

$answering = @(Test-ServicePortAnswers -Ports 8081, 8082, 8083)
if ($answering.Count -gt 0) {
    throw "Refusing to reset the broker while ports $($answering -join ', ') answer; stop the services first"
}

& (Join-Path $PSScriptRoot 'stop-broker.ps1')
$logDir = Join-Path $DataDir 'kraft-logs'
if (Test-Path $logDir) { Remove-Item -Recurse -Force -LiteralPath $logDir }

& (Join-Path $PSScriptRoot 'start-broker.ps1') -KafkaHome $KafkaHome -DataDir $DataDir -TimeoutSeconds $TimeoutSeconds
if ($LASTEXITCODE -ne 0) { throw 'start-broker.ps1 failed after the reset' }

$problems = @()
$ends = Get-KafkaEndOffsets -KafkaHome $KafkaHome
foreach ($spec in (Get-KafkaTopicSpecs)) {
    $offsets = @($ends[$spec.Name])
    if ($offsets.Count -ne $spec.Partitions) {
        $problems += "$($spec.Name): end offsets for $($offsets.Count) of $($spec.Partitions) partitions"
    }
    foreach ($offset in $offsets) {
        if ($offset.Offset -ne 0) { $problems += "$($spec.Name)-$($offset.Partition): end offset $($offset.Offset)" }
    }
}
$lags = Get-KafkaGroupLags -KafkaHome $KafkaHome
foreach ($group in (Get-KafkaGroups)) {
    if ($lags[$group].State -ne 'absent') { $problems += "group ${group}: still exists after the reset" }
}

if ($problems.Count -gt 0) {
    $problems | ForEach-Object { Write-Host "RESET FAILED $_" }
    exit 1
}
Write-Host 'Broker reset: four empty topics, and no consumer group or committed offset.'
