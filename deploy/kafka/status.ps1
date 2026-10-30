<#
.SYNOPSIS
    Prints the broker's operational state (FR-046): reachability, each topic's partitions and end
    offsets, and the lag of the three consumer groups.

.DESCRIPTION
    Reads kafka-topics.sh, kafka-get-offsets.sh, and kafka-consumer-groups.sh --describe inside the
    cs6510-kafka container. A group that does not exist yet is reported as absent, not as an error.
    With -Json one JSON document is written to stdout for the benchmark harness:
      { reachable, topics: [{ name, partitions, endOffsets: [..], totalEndOffset }],
        groups: [{ group, state, lag, uncommitted }] }
    An unreachable broker gives reachable=false and exit code 1.

.EXAMPLE
    ./deploy/kafka/status.ps1
    ./deploy/kafka/status.ps1 -Json
#>
param(
    [string]$TopicPrefix = '',
    [switch]$Json
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')

$reachable = $false
try { $reachable = Test-KafkaBroker } catch { $reachable = $false }

$topics = @()
$groups = @()
if ($reachable) {
    foreach ($spec in (Get-KafkaTopicSpecs -Prefix $TopicPrefix)) {
        $description = Get-KafkaTopicDescription -Name $spec.Name
        if ($null -eq $description) {
            $topics += [pscustomobject]@{ name = $spec.Name; exists = $false; partitions = 0; endOffsets = @(); totalEndOffset = 0 }
            continue
        }
        $offsets = @(Get-KafkaEndOffsets -Topic $spec.Name | ForEach-Object { $_.Offset })
        $sum = [long]0
        foreach ($offset in $offsets) { $sum += $offset }
        $topics += [pscustomobject]@{ name = $spec.Name; exists = $true; partitions = $description.Partitions
            endOffsets = $offsets; totalEndOffset = $sum }
    }
    foreach ($group in (Get-KafkaGroups)) {
        $lag = Get-KafkaGroupLag -Group $group
        $groups += [pscustomobject]@{ group = $group; state = $lag.State; lag = $lag.Lag; uncommitted = $lag.Uncommitted }
    }
}

if ($Json) {
    [pscustomobject]@{ reachable = $reachable; topics = $topics; groups = $groups } | ConvertTo-Json -Depth 5 -Compress
} else {
    if (-not $reachable) {
        Write-Host 'broker: unreachable (container cs6510-kafka, host listener 127.0.0.1:19092)'
    } else {
        Write-Host 'broker: reachable (container cs6510-kafka, host listener 127.0.0.1:19092)'
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
