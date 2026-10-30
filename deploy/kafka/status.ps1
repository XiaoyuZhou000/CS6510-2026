<#
.SYNOPSIS
    Prints the broker's operational state (FR-046): reachability, each topic's partitions and end
    offsets, and the lag of the three consumer groups.

.DESCRIPTION
    Uses three Kafka tool runs (topics, end offsets, and consumer groups --all-groups). A group that
    does not exist yet is reported as absent, not as an error. With -Json one JSON document is
    written to stdout for the benchmark harness:
      { reachable, topics: [{ name, exists, partitions, endOffsets: [..], totalEndOffset }],
        groups: [{ group, state, lag, uncommitted, assigned,
                   partitions: [{ topic, partition, currentOffset, endOffset, lag, consumerId }] }] }
    assigned counts the partitions a live member holds; consumerId is null for the others.
    An unreachable broker gives reachable=false and exit code 1.

.EXAMPLE
    ./deploy/kafka/status.ps1
    ./deploy/kafka/status.ps1 -Json
#>
param(
    [string]$TopicPrefix = '',
    [string]$KafkaHome,
    [switch]$Json
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')
$KafkaHome = Resolve-KafkaHome $KafkaHome

$reachable = $false
try { $reachable = Test-KafkaBroker -KafkaHome $KafkaHome } catch { $reachable = $false }

$topics = @()
$groups = @()
if ($reachable) {
    $described = Get-KafkaTopics -KafkaHome $KafkaHome
    $ends = Get-KafkaEndOffsets -KafkaHome $KafkaHome
    foreach ($spec in (Get-KafkaTopicSpecs -Prefix $TopicPrefix)) {
        $description = $described[$spec.Name]
        if ($null -eq $description) {
            $topics += [pscustomobject]@{ name = $spec.Name; exists = $false; partitions = 0; endOffsets = @(); totalEndOffset = 0 }
            continue
        }
        $offsets = @($ends[$spec.Name] | ForEach-Object { $_.Offset })
        $sum = [long]0
        foreach ($offset in $offsets) { $sum += $offset }
        $topics += [pscustomobject]@{ name = $spec.Name; exists = $true; partitions = $description.Partitions
            endOffsets = $offsets; totalEndOffset = $sum }
    }
    $lags = Get-KafkaGroupLags -KafkaHome $KafkaHome
    foreach ($group in (Get-KafkaGroups)) {
        $lag = $lags[$group]
        $partitions = @($lag.Partitions | ForEach-Object {
            [pscustomobject]@{ topic = $_.Topic; partition = $_.Partition; currentOffset = $_.CurrentOffset
                endOffset = $_.EndOffset; lag = $_.Lag; consumerId = $_.ConsumerId } })
        $groups += [pscustomobject]@{ group = $group; state = $lag.State; lag = $lag.Lag; uncommitted = $lag.Uncommitted
            assigned = $lag.Assigned; partitions = $partitions }
    }
}

if ($Json) {
    [pscustomobject]@{ reachable = $reachable; topics = $topics; groups = $groups } | ConvertTo-Json -Depth 5 -Compress
} else {
    if (-not $reachable) {
        Write-Host "broker: unreachable on $($script:KafkaBootstrap)"
    } else {
        $process = Get-KafkaBrokerProcess
        Write-Host "broker: reachable on $($script:KafkaBootstrap) (pid $($process.Id))"
        foreach ($topic in $topics) {
            if (-not $topic.exists) { Write-Host ("topic {0}: missing" -f $topic.name); continue }
            Write-Host ("topic {0}: {1} partitions, end offsets [{2}], total {3}" -f $topic.name, $topic.partitions,
                ($topic.endOffsets -join ','), $topic.totalEndOffset)
        }
        foreach ($group in $groups) {
            if ($group.state -eq 'absent') { Write-Host ("group {0}: absent" -f $group.group); continue }
            Write-Host ("group {0}: {1}, lag {2}, partitions without a committed offset {3}" -f $group.group,
                $group.state, $group.lag, $group.uncommitted)
        }
    }
}
if (-not $reachable) { exit 1 }
