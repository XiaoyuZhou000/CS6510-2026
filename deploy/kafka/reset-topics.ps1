<#
.SYNOPSIS
    Empties the broker for a reproducible baseline (research D13, FR-059): deletes and recreates the
    four topics of the event catalog.

.DESCRIPTION
    1. Refuses to run while anything answers on the service ports 8081-8083, so no running service
       loses records or offsets it is working on.
    2. Deletes the four topics, waits until the broker no longer lists them, and recreates them
       through create-topics.ps1, which also verifies their partitions and configuration.
    3. Verifies that every end offset is 0 and that none of the three consumer groups keeps a
       committed offset.
    The container is never restarted; db/reset-baseline.ps1 calls this script.

.EXAMPLE
    ./deploy/kafka/reset-topics.ps1
#>
param(
    [string]$TopicPrefix = '',
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')

$answering = @(Test-ServicePortAnswers -Ports 8081, 8082, 8083)
if ($answering.Count -gt 0) {
    throw "Refusing to reset topics while ports $($answering -join ', ') answer; stop the services first"
}

Wait-KafkaBroker -TimeoutSeconds $TimeoutSeconds
$specs = Get-KafkaTopicSpecs -Prefix $TopicPrefix

foreach ($spec in $specs) {
    if ($null -eq (Get-KafkaTopicDescription -Name $spec.Name)) { continue }
    $result = Invoke-KafkaTool 'kafka-topics.sh' @('--delete', '--topic', $spec.Name)
    if ($result.ExitCode -ne 0) { throw "Deleting $($spec.Name) failed: $($result.Lines -join ' ')" }
}

# Topic deletion is asynchronous; recreating a name the broker still lists would be a no-op.
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while ($true) {
    $listed = (Invoke-KafkaTool 'kafka-topics.sh' @('--list')).Lines
    $remaining = @($specs | Where-Object { $listed -contains $_.Name })
    if ($remaining.Count -eq 0) { break }
    if ((Get-Date) -ge $deadline) { throw "Topics still listed after deletion: $(($remaining | ForEach-Object Name) -join ', ')" }
    Start-Sleep -Milliseconds 500
}

& (Join-Path $PSScriptRoot 'create-topics.ps1') -TopicPrefix $TopicPrefix -TimeoutSeconds $TimeoutSeconds
if ($LASTEXITCODE -ne 0) { throw 'create-topics.ps1 reported a mismatch after recreation' }

$problems = @()
foreach ($spec in $specs) {
    $offsets = @(Get-KafkaEndOffsets -Topic $spec.Name)
    if ($offsets.Count -ne $spec.Partitions) {
        $problems += "$($spec.Name): end offsets for $($offsets.Count) of $($spec.Partitions) partitions"
    }
    foreach ($offset in $offsets) {
        if ($offset.Offset -ne 0) { $problems += "$($spec.Name)-$($offset.Partition): end offset $($offset.Offset)" }
    }
}
# A group's committed offsets are removed with its topics' deletion; a group that survives must
# hold no committed offset on the recreated topics.
foreach ($group in (Get-KafkaGroups)) {
    $lag = Get-KafkaGroupLag -Group $group
    if ($lag.State -eq 'absent') { continue }
    $committed = @($lag.Partitions | Where-Object { $null -ne $_.CurrentOffset })
    if ($committed.Count -gt 0) {
        $problems += "group ${group}: committed offsets remain on $($committed.Count) partitions"
    }
}

if ($problems.Count -gt 0) {
    $problems | ForEach-Object { Write-Host "RESET FAILED $_" }
    exit 1
}
Write-Host 'Topics reset: every end offset is 0 and no consumer group holds a committed offset.'
