<#
.SYNOPSIS
    Creates the four topics of the event catalog idempotently and verifies them (research D4,
    specs/006-event-based-architecture/contracts/event-catalog.md "Topics").

.DESCRIPTION
    Waits for the broker on 127.0.0.1:19092, then creates any topic that is missing:
      selfcheckout.catalog.v1           1 partition,   cleanup.policy=compact, retention.ms=-1
      selfcheckout.checkout.v1          16 partitions, cleanup.policy=delete,  retention.ms=604800000
      selfcheckout.checkout-outcome.v1  16 partitions, cleanup.policy=delete,  retention.ms=604800000
      selfcheckout.scan-accepted.v1     1 partition,   cleanup.policy=delete,  retention.ms=604800000
    Every topic also pins segment.bytes=1073741824 and segment.ms=Long.MAX_VALUE, so its segments
    never roll on Windows (deploy/kafka/server.properties). An existing topic is left as it is.
    Afterwards every topic's partition count, replication factor, and configuration are compared
    with the catalog, and the script exits nonzero on any mismatch.

.PARAMETER TopicPrefix
    Prepended to every topic name, as KAFKA_TOPIC_PREFIX is by the services (tests use a per-run prefix).

.EXAMPLE
    ./deploy/kafka/create-topics.ps1
#>
param(
    [string]$TopicPrefix = '',
    [string]$KafkaHome,
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')
$KafkaHome = Resolve-KafkaHome $KafkaHome

Wait-KafkaBroker -TimeoutSeconds $TimeoutSeconds -KafkaHome $KafkaHome

$specs = Get-KafkaTopicSpecs -Prefix $TopicPrefix
$existing = Get-KafkaTopics -KafkaHome $KafkaHome
foreach ($spec in $specs) {
    if ($existing.ContainsKey($spec.Name)) { continue }
    $arguments = @('--create', '--if-not-exists', '--topic', $spec.Name,
        '--partitions', "$($spec.Partitions)", '--replication-factor', '1')
    foreach ($key in $spec.Configs.Keys) { $arguments += @('--config', "$key=$($spec.Configs[$key])") }
    $result = Invoke-KafkaTool 'kafka-topics' $arguments -KafkaHome $KafkaHome
    if ($result.ExitCode -ne 0) {
        throw "Creating $($spec.Name) failed: $($result.Lines -join ' ')"
    }
}

$actualTopics = Get-KafkaTopics -KafkaHome $KafkaHome
$mismatches = @()
foreach ($spec in $specs) {
    $actual = $actualTopics[$spec.Name]
    if ($null -eq $actual) { $mismatches += "$($spec.Name): missing after creation"; continue }
    if ($actual.Partitions -ne $spec.Partitions) {
        $mismatches += "$($spec.Name): $($actual.Partitions) partitions, expected $($spec.Partitions)"
    }
    if ($actual.ReplicationFactor -ne 1) {
        $mismatches += "$($spec.Name): replication factor $($actual.ReplicationFactor), expected 1"
    }
    foreach ($key in $spec.Configs.Keys) {
        $value = $actual.Configs[$key]
        if ($value -ne $spec.Configs[$key]) {
            $mismatches += "$($spec.Name): $key=$value, expected $($spec.Configs[$key])"
        }
    }
    Write-Host ("{0}: {1} partitions, cleanup.policy={2}, retention.ms={3}" -f $spec.Name, $actual.Partitions,
        $actual.Configs['cleanup.policy'], $actual.Configs['retention.ms'])
}

if ($mismatches.Count -gt 0) {
    $mismatches | ForEach-Object { Write-Host "MISMATCH $_" }
    Write-Host 'An existing topic is never altered; reset the broker with deploy/kafka/reset-topics.ps1.'
    exit 1
}
Write-Host 'All topics match the event catalog.'
