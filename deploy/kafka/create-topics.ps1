<#
.SYNOPSIS
    Creates the four topics of the event catalog idempotently and verifies them (research D4,
    specs/006-event-based-architecture/contracts/event-catalog.md "Topics").

.DESCRIPTION
    Waits for the broker in container cs6510-kafka, then creates through kafka-topics.sh any topic
    that is missing:
      selfcheckout.catalog.v1           1 partition,   cleanup.policy=compact, retention.ms=-1
      selfcheckout.checkout.v1          16 partitions, cleanup.policy=delete,  retention.ms=604800000
      selfcheckout.checkout-outcome.v1  16 partitions, cleanup.policy=delete,  retention.ms=604800000
      selfcheckout.scan-accepted.v1     1 partition,   cleanup.policy=delete,  retention.ms=604800000
    An existing topic is left as it is. Afterwards every topic's partition count, replication factor,
    and configuration are compared with the catalog, and the script exits nonzero on any mismatch.

.PARAMETER TopicPrefix
    Prepended to every topic name, as KAFKA_TOPIC_PREFIX is by the services (tests use a per-run prefix).

.EXAMPLE
    docker compose -f deploy/kafka/compose.yaml up -d
    ./deploy/kafka/create-topics.ps1
#>
param(
    [string]$TopicPrefix = '',
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')

Wait-KafkaBroker -TimeoutSeconds $TimeoutSeconds

$specs = Get-KafkaTopicSpecs -Prefix $TopicPrefix
foreach ($spec in $specs) {
    $arguments = @('--create', '--if-not-exists', '--topic', $spec.Name,
        '--partitions', "$($spec.Partitions)", '--replication-factor', '1')
    foreach ($key in $spec.Configs.Keys) { $arguments += @('--config', "$key=$($spec.Configs[$key])") }
    $result = Invoke-KafkaTool 'kafka-topics.sh' $arguments
    if ($result.ExitCode -ne 0) {
        throw "Creating $($spec.Name) failed: $($result.Lines -join ' ')"
    }
}

$mismatches = @()
foreach ($spec in $specs) {
    $actual = Get-KafkaTopicDescription -Name $spec.Name
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
    $configs = ($spec.Configs.Keys | ForEach-Object { "$_=$($actual.Configs[$_])" }) -join ','
    Write-Host ("{0}: {1} partitions, {2}" -f $spec.Name, $actual.Partitions, $configs)
}

if ($mismatches.Count -gt 0) {
    $mismatches | ForEach-Object { Write-Host "MISMATCH $_" }
    Write-Host 'An existing topic is never altered; reset it with deploy/kafka/reset-topics.ps1.'
    exit 1
}
Write-Host 'All topics match the event catalog.'
