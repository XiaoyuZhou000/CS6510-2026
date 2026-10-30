<#
.SYNOPSIS
    Shared helpers for the broker scripts in deploy/kafka/ (research D1, D13). Dot-source it:
        . (Join-Path $PSScriptRoot 'kafka-tools.ps1')

.DESCRIPTION
    - Get-KafkaTopicSpecs lists the four topics of the event catalog with their partitions and
      configuration, honoring a topic prefix.
    - Invoke-KafkaTool runs one Kafka command-line tool inside the cs6510-kafka container.
    - Wait-KafkaBroker, Get-KafkaTopicDescription, Get-KafkaEndOffsets, and Get-KafkaGroupLag read
      the broker's state; Test-ServicePortAnswers guards the reset scripts.
#>

# Windows PowerShell 5.1 pipes text to native commands as ASCII unless told otherwise.
$OutputEncoding = New-Object System.Text.UTF8Encoding $false

$script:KafkaContainer = 'cs6510-kafka'
# The tools run inside the container, where the host listener's port is the broker itself.
$script:KafkaInternalBootstrap = 'localhost:19092'
$script:KafkaGroups = @('inventory-checkout', 'transaction-checkout-outcome', 'analytics-scan-ingest')

function Get-KafkaTopicSpecs {
    param([string]$Prefix = '')
    $week = '604800000'
    return @(
        [pscustomobject]@{ Name = "${Prefix}selfcheckout.catalog.v1"; Partitions = 1
            Configs = [ordered]@{ 'cleanup.policy' = 'compact'; 'retention.ms' = '-1' } }
        [pscustomobject]@{ Name = "${Prefix}selfcheckout.checkout.v1"; Partitions = 16
            Configs = [ordered]@{ 'cleanup.policy' = 'delete'; 'retention.ms' = $week } }
        [pscustomobject]@{ Name = "${Prefix}selfcheckout.checkout-outcome.v1"; Partitions = 16
            Configs = [ordered]@{ 'cleanup.policy' = 'delete'; 'retention.ms' = $week } }
        [pscustomobject]@{ Name = "${Prefix}selfcheckout.scan-accepted.v1"; Partitions = 1
            Configs = [ordered]@{ 'cleanup.policy' = 'delete'; 'retention.ms' = $week } }
    )
}

function Get-KafkaGroups { return $script:KafkaGroups }

function Invoke-KafkaTool {
    param(
        [Parameter(Mandatory = $true)][string]$Tool,
        [string[]]$Arguments = @()
    )
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'docker is not on PATH; install and start Docker Desktop (deploy/kafka/README.md)'
    }
    $all = @('exec', $script:KafkaContainer, "/opt/kafka/bin/$Tool", '--bootstrap-server', $script:KafkaInternalBootstrap) + $Arguments
    # Native stderr would become a terminating error under -ErrorAction Stop in Windows PowerShell 5.1.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $lines = @(& docker @all 2>&1 | ForEach-Object { "$_" })
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{ ExitCode = $code; Lines = $lines }
}

function Test-KafkaBroker {
    $result = Invoke-KafkaTool 'kafka-topics.sh' @('--list')
    return ($result.ExitCode -eq 0)
}

function Wait-KafkaBroker {
    param([int]$TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        if (Test-KafkaBroker) { return }
        if ((Get-Date) -ge $deadline) {
            throw "Broker in container $($script:KafkaContainer) did not answer within $TimeoutSeconds s; start it with: docker compose -f deploy/kafka/compose.yaml up -d"
        }
        Start-Sleep -Seconds 2
    }
}

# Returns $null for a missing topic, otherwise its partition count, replication factor, and the
# topic-level configuration overrides that kafka-topics.sh --describe prints.
function Get-KafkaTopicDescription {
    param([Parameter(Mandatory = $true)][string]$Name)
    $result = Invoke-KafkaTool 'kafka-topics.sh' @('--describe', '--topic', $Name)
    $header = $result.Lines | Where-Object { $_ -match "^Topic:\s*$([regex]::Escape($Name))\s.*PartitionCount:" } | Select-Object -First 1
    if ($result.ExitCode -ne 0 -or -not $header) { return $null }
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
    return [pscustomobject]@{ Name = $Name; Partitions = $partitions; ReplicationFactor = $replication; Configs = $configs }
}

# Latest offset of every partition of a topic, as objects with Partition and Offset.
function Get-KafkaEndOffsets {
    param([Parameter(Mandatory = $true)][string]$Topic)
    $result = Invoke-KafkaTool 'kafka-get-offsets.sh' @('--topic', $Topic, '--time', '-1')
    if ($result.ExitCode -ne 0) { throw "Could not read end offsets of ${Topic}: $($result.Lines -join ' ')" }
    $offsets = @()
    foreach ($line in $result.Lines) {
        if ($line -match "^$([regex]::Escape($Topic)):(\d+):(\d+)\s*$") {
            $offsets += [pscustomobject]@{ Partition = [int]$Matches[1]; Offset = [long]$Matches[2] }
        }
    }
    return @($offsets | Sort-Object Partition)
}

# A group that the broker does not know is Absent, not an error. Lag is the sum over partitions
# with a committed offset; partitions without one are counted in Uncommitted.
function Get-KafkaGroupLag {
    param([Parameter(Mandatory = $true)][string]$Group)
    $result = Invoke-KafkaTool 'kafka-consumer-groups.sh' @('--describe', '--group', $Group)
    $text = $result.Lines -join "`n"
    if ($text -match 'does not exist') {
        return [pscustomobject]@{ Group = $Group; State = 'absent'; Lag = $null; Uncommitted = 0; Partitions = @() }
    }
    if ($result.ExitCode -ne 0) { throw "Could not describe group ${Group}: $text" }
    $partitions = @()
    foreach ($line in $result.Lines) {
        $columns = @($line.Trim() -split '\s+')
        if ($columns.Count -ge 6 -and $columns[0] -eq $Group -and $columns[2] -match '^\d+$') {
            $current = $null
            if ($columns[3] -match '^\d+$') { $current = [long]$columns[3] }
            $end = $null
            if ($columns[4] -match '^\d+$') { $end = [long]$columns[4] }
            $lag = $null
            if ($columns[5] -match '^\d+$') { $lag = [long]$columns[5] }
            $partitions += [pscustomobject]@{ Topic = $columns[1]; Partition = [int]$columns[2]
                CurrentOffset = $current; EndOffset = $end; Lag = $lag }
        }
    }
    $total = [long]0
    $uncommitted = 0
    foreach ($p in $partitions) {
        if ($null -eq $p.Lag) { $uncommitted++ } else { $total += $p.Lag }
    }
    $state = 'stable'
    if ($text -match 'has no active members') { $state = 'empty' }
    return [pscustomobject]@{ Group = $Group; State = $state; Lag = $total; Uncommitted = $uncommitted; Partitions = $partitions }
}

# True when anything accepts a TCP connection on one of the given local ports.
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
