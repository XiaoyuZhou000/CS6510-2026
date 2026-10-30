<#
.SYNOPSIS
    Shared helpers for the broker scripts in deploy/kafka/ (research D1, D13). Dot-source it:
        . (Join-Path $PSScriptRoot 'kafka-tools.ps1')

.DESCRIPTION
    The broker is the Apache Kafka 4.1.0 binary release, unpacked anywhere and named by KAFKA_HOME,
    run natively by start-broker.ps1. Its Windows .bat wrappers are not used: they fail with "input
    line too long" when they build the classpath, so the tools run as their Java main classes.
    - Resolve-KafkaHome, Resolve-KafkaDataDir, and Resolve-KafkaJava find the release, the broker's
      state directory (KAFKA_DATA_DIR, default <KAFKA_HOME>\cs6510-data), and a Java 17+ java.exe.
      The state directory holds kraft-logs\ (log.dirs), app-logs\ (broker logs and console
      output), and server.properties (the effective configuration, rewritten at every start).
    - Get-KafkaTopicSpecs lists the four topics of the event catalog, honoring a topic prefix.
    - Invoke-KafkaTool runs one Kafka command-line tool against 127.0.0.1:19092.
    - Get-KafkaBrokerProcess, Wait-KafkaBroker, Get-KafkaTopics, Get-KafkaEndOffsets,
      and Get-KafkaGroupLags read the broker's state; Test-ServicePortAnswers guards the resets.
#>

# Windows PowerShell 5.1 pipes text to native commands as ASCII unless told otherwise.
$OutputEncoding = New-Object System.Text.UTF8Encoding $false

$script:KafkaBootstrap = '127.0.0.1:19092'
$script:KafkaPort = 19092
$script:KafkaClusterId = 'Y3M2NTEwLWthZmthLTAwMQ'
$script:KafkaGroups = @('inventory-checkout', 'transaction-checkout-outcome', 'analytics-scan-ingest')
$script:KafkaToolClasses = @{
    'kafka-storage'         = 'kafka.tools.StorageTool'
    'kafka-topics'          = 'org.apache.kafka.tools.TopicCommand'
    'kafka-get-offsets'     = 'org.apache.kafka.tools.GetOffsetShell'
    'kafka-consumer-groups' = 'org.apache.kafka.tools.consumer.group.ConsumerGroupCommand'
}

function Resolve-KafkaHome {
    param([string]$Explicit)
    $kafkaHome = $Explicit
    if (-not $kafkaHome) { $kafkaHome = $env:KAFKA_HOME }
    if (-not $kafkaHome) {
        throw 'Set KAFKA_HOME (or pass -KafkaHome) to the unpacked kafka_2.13-4.1.0 release; see deploy/kafka/README.md'
    }
    $kafkaHome = [IO.Path]::GetFullPath($kafkaHome)
    if (-not (Test-Path (Join-Path $kafkaHome 'libs\kafka-clients-4.1.0.jar'))) {
        throw "KAFKA_HOME does not hold the Kafka 4.1.0 release (libs\kafka-clients-4.1.0.jar missing): $kafkaHome"
    }
    return $kafkaHome
}

function Resolve-KafkaDataDir {
    param([string]$KafkaHome, [string]$Explicit)
    $dir = $Explicit
    if (-not $dir) { $dir = $env:KAFKA_DATA_DIR }
    if (-not $dir) { $dir = Join-Path $KafkaHome 'cs6510-data' }
    return [IO.Path]::GetFullPath($dir)
}

# The JDK's own java.exe: launcher shims such as Oracle's javapath start the JVM as a child
# process, which stopping the shim would leave running.
function Resolve-KafkaJava {
    if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
        return (Join-Path $env:JAVA_HOME 'bin\java.exe')
    }
    $launcher = (Get-Command java -ErrorAction Stop).Source
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $javaHome = (& $launcher -XshowSettings:properties -version 2>&1 | ForEach-Object { "$_" } |
            Where-Object { $_ -match '^\s*java\.home = ' } | Select-Object -First 1) -replace '^\s*java\.home = ', ''
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($javaHome -and (Test-Path (Join-Path $javaHome.Trim() 'bin\java.exe'))) { return (Join-Path $javaHome.Trim() 'bin\java.exe') }
    return $launcher
}

function Get-KafkaTopicSpecs {
    param([string]$Prefix = '')
    $week = '604800000'
    # Every topic pins the never-roll segment settings of server.properties (Windows, see there).
    $noRoll = [ordered]@{ 'segment.bytes' = '1073741824'; 'segment.ms' = '9223372036854775807' }
    $specs = @(
        @{ Name = "${Prefix}selfcheckout.catalog.v1"; Partitions = 1; Policy = 'compact'; Retention = '-1' }
        @{ Name = "${Prefix}selfcheckout.checkout.v1"; Partitions = 16; Policy = 'delete'; Retention = $week }
        @{ Name = "${Prefix}selfcheckout.checkout-outcome.v1"; Partitions = 16; Policy = 'delete'; Retention = $week }
        @{ Name = "${Prefix}selfcheckout.scan-accepted.v1"; Partitions = 1; Policy = 'delete'; Retention = $week }
    )
    return @($specs | ForEach-Object {
        $configs = [ordered]@{ 'cleanup.policy' = $_.Policy; 'retention.ms' = $_.Retention }
        foreach ($key in $noRoll.Keys) { $configs[$key] = $noRoll[$key] }
        [pscustomobject]@{ Name = $_.Name; Partitions = $_.Partitions; Configs = $configs }
    })
}

function Get-KafkaGroups { return $script:KafkaGroups }

function Invoke-KafkaTool {
    param(
        [Parameter(Mandatory = $true)][string]$Tool,
        [string[]]$Arguments = @(),
        [string]$KafkaHome,
        [switch]$NoBootstrap
    )
    $kafkaHome = Resolve-KafkaHome $KafkaHome
    $class = $script:KafkaToolClasses[$Tool]
    if (-not $class) { throw "Unknown Kafka tool: $Tool" }
    $all = @("-Dlog4j2.configurationFile=$(Join-Path $kafkaHome 'config\tools-log4j2.yaml')",
        '-cp', (Join-Path $kafkaHome 'libs\*'), $class)
    if (-not $NoBootstrap) { $all += @('--bootstrap-server', $script:KafkaBootstrap) }
    $all += $Arguments
    # Native stderr would become a terminating error under -ErrorAction Stop in Windows PowerShell 5.1.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $lines = @(& (Resolve-KafkaJava) @all 2>&1 | ForEach-Object { "$_" })
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{ ExitCode = $code; Lines = $lines }
}

# The java.exe that listens on the broker port, or $null.
function Get-KafkaBrokerProcess {
    $listener = Get-NetTCPConnection -LocalPort $script:KafkaPort -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $listener) { return $null }
    return Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
}

function Test-KafkaBroker {
    param([string]$KafkaHome)
    if ($null -eq (Get-KafkaBrokerProcess)) { return $false }
    $result = Invoke-KafkaTool 'kafka-topics' @('--list') -KafkaHome $KafkaHome
    return ($result.ExitCode -eq 0)
}

function Wait-KafkaBroker {
    param([int]$TimeoutSeconds = 120, [string]$KafkaHome)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        if (Test-KafkaBroker -KafkaHome $KafkaHome) { return }
        if ((Get-Date) -ge $deadline) {
            throw "Broker on $($script:KafkaBootstrap) did not answer within $TimeoutSeconds s; start it with deploy/kafka/start-broker.ps1"
        }
        Start-Sleep -Seconds 2
    }
}

# Every non-internal topic, as a hashtable of name to partition count, replication factor, and
# the configuration that kafka-topics --describe prints (topic overrides and static broker settings).
function Get-KafkaTopics {
    param([string]$KafkaHome)
    $result = Invoke-KafkaTool 'kafka-topics' @('--describe', '--exclude-internal') -KafkaHome $KafkaHome
    if ($result.ExitCode -ne 0) { throw "Could not describe topics: $($result.Lines -join ' ')" }
    $topics = @{}
    foreach ($header in ($result.Lines | Where-Object { $_ -match '^Topic:\s*\S+\s.*PartitionCount:' })) {
        $name = ($header -replace '^Topic:\s*(\S+)\s.*$', '$1')
        $configs = @{}
        if ($header -match 'Configs:\s*(\S.*)$') {
            foreach ($pair in ($Matches[1].Trim() -split ',')) {
                $parts = $pair -split '=', 2
                if ($parts.Count -eq 2) { $configs[$parts[0].Trim()] = $parts[1].Trim() }
            }
        }
        $partitions = 0
        if ($header -match 'PartitionCount:\s*(\d+)') { $partitions = [int]$Matches[1] }
        $replication = 0
        if ($header -match 'ReplicationFactor:\s*(\d+)') { $replication = [int]$Matches[1] }
        $topics[$name] = [pscustomobject]@{ Name = $name; Partitions = $partitions; ReplicationFactor = $replication; Configs = $configs }
    }
    return $topics
}

# Latest offset of every partition of every non-internal topic, as a hashtable of topic name to
# objects with Partition and Offset, sorted by partition.
function Get-KafkaEndOffsets {
    param([string]$KafkaHome)
    $result = Invoke-KafkaTool 'kafka-get-offsets' @('--time', '-1', '--exclude-internal-topics') -KafkaHome $KafkaHome
    if ($result.ExitCode -ne 0) { throw "Could not read end offsets: $($result.Lines -join ' ')" }
    $byTopic = @{}
    foreach ($line in $result.Lines) {
        if ($line -match '^(\S+):(\d+):(\d+)\s*$') {
            if (-not $byTopic.ContainsKey($Matches[1])) { $byTopic[$Matches[1]] = @() }
            $byTopic[$Matches[1]] += [pscustomobject]@{ Partition = [int]$Matches[2]; Offset = [long]$Matches[3] }
        }
    }
    $sorted = @{}
    foreach ($topic in $byTopic.Keys) { $sorted[$topic] = @($byTopic[$topic] | Sort-Object Partition) }
    return $sorted
}

# Lag of each requested consumer group from one kafka-consumer-groups --describe --all-groups. A
# group that the broker does not know is absent, not an error. Lag is the sum over partitions with a
# committed offset; partitions without one are counted in Uncommitted. Assigned counts the partitions
# a live member holds.
function Get-KafkaGroupLags {
    param([string[]]$Groups = $script:KafkaGroups, [string]$KafkaHome)
    $result = Invoke-KafkaTool 'kafka-consumer-groups' @('--describe', '--all-groups') -KafkaHome $KafkaHome
    if ($result.ExitCode -ne 0) { throw "Could not describe consumer groups: $($result.Lines -join ' ')" }
    $lags = @{}
    foreach ($group in $Groups) {
        $partitions = @()
        foreach ($line in $result.Lines) {
            $columns = @($line.Trim() -split '\s+')
            if ($columns.Count -ge 6 -and $columns[0] -eq $group -and $columns[2] -match '^\d+$') {
                $current = $null
                if ($columns[3] -match '^\d+$') { $current = [long]$columns[3] }
                $end = $null
                if ($columns[4] -match '^\d+$') { $end = [long]$columns[4] }
                $lag = $null
                if ($columns[5] -match '^\d+$') { $lag = [long]$columns[5] }
                # CONSUMER-ID is '-' while no live member holds the partition.
                $consumer = $null
                if ($columns.Count -ge 7 -and $columns[6] -ne '-') { $consumer = $columns[6] }
                $partitions += [pscustomobject]@{ Topic = $columns[1]; Partition = [int]$columns[2]
                    CurrentOffset = $current; EndOffset = $end; Lag = $lag; ConsumerId = $consumer }
            }
        }
        $empty = @($result.Lines | Where-Object { $_ -match "^Consumer group '$([regex]::Escape($group))' has no active members" }).Count -gt 0
        if ($partitions.Count -eq 0 -and -not $empty) {
            $lags[$group] = [pscustomobject]@{ Group = $group; State = 'absent'; Lag = $null; Uncommitted = 0; Assigned = 0
                Partitions = @() }
            continue
        }
        $total = [long]0
        $uncommitted = 0
        $assigned = 0
        foreach ($p in $partitions) {
            if ($null -eq $p.Lag) { $uncommitted++ } else { $total += $p.Lag }
            if ($null -ne $p.ConsumerId) { $assigned++ }
        }
        $state = 'stable'
        if ($empty) { $state = 'empty' }
        $lags[$group] = [pscustomobject]@{ Group = $group; State = $state; Lag = $total; Uncommitted = $uncommitted
            Assigned = $assigned; Partitions = $partitions }
    }
    return $lags
}

# The ports among $Ports on which anything accepts a TCP connection.
function Test-ServicePortAnswers {
    param([int[]]$Ports = @(8081, 8082, 8083))
    $answering = @()
    foreach ($port in $Ports) {
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $pending = $client.BeginConnect('127.0.0.1', $port, $null, $null)
            if ($pending.AsyncWaitHandle.WaitOne(500) -and $client.Connected) { $answering += $port }
        } catch {
        } finally {
            $client.Close()
        }
    }
    return $answering
}
