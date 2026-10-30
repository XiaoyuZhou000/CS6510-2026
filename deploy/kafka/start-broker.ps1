<#
.SYNOPSIS
    Starts the broker natively on Windows (research D1) and creates the event catalog's topics.

.DESCRIPTION
    1. Writes the effective configuration <KAFKA_DATA_DIR>\server.properties from
       deploy/kafka/server.properties plus log.dirs=<KAFKA_DATA_DIR>\kraft-logs, and
       log.flush.interval.messages=1 when KAFKA_FLUSH_EVERY_MESSAGE=true (D15).
    2. Formats kraft-logs\ with the fixed cluster ID the first time, or after a reset.
    3. Starts kafka.Kafka as a hidden background JVM, with console output in app-logs\, and waits
       until it answers on 127.0.0.1:19092.
    4. Runs create-topics.ps1 unless -NoTopics.
    If the broker already runs, only step 4 is performed.

.EXAMPLE
    $env:KAFKA_HOME = 'C:\tools\kafka_2.13-4.1.0'
    ./deploy/kafka/start-broker.ps1
#>
param(
    [string]$KafkaHome,
    [string]$DataDir,
    [int]$TimeoutSeconds = 120,
    [switch]$NoTopics
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')

$KafkaHome = Resolve-KafkaHome $KafkaHome
$DataDir = Resolve-KafkaDataDir -KafkaHome $KafkaHome -Explicit $DataDir

if ($null -ne (Get-KafkaBrokerProcess)) {
    Write-Host "Broker already running on $($script:KafkaBootstrap)."
} else {
    $busy = @(Get-NetTCPConnection -LocalPort 19093 -State Listen -ErrorAction SilentlyContinue)
    if ($busy.Count -gt 0) { throw "Port 19093 (controller listener) is held by process $($busy[0].OwningProcess)" }

    $logDir = Join-Path $DataDir 'kraft-logs'
    $appLogs = Join-Path $DataDir 'app-logs'
    New-Item -ItemType Directory -Force -Path $appLogs | Out-Null
    $config = Join-Path $DataDir 'server.properties'
    $lines = @(Get-Content (Join-Path $PSScriptRoot 'server.properties'))
    $lines += '', '# Added by start-broker.ps1', "log.dirs=$($logDir -replace '\\', '/')"
    if ($env:KAFKA_FLUSH_EVERY_MESSAGE -eq 'true') { $lines += 'log.flush.interval.messages=1' }
    [IO.File]::WriteAllLines($config, $lines, (New-Object System.Text.UTF8Encoding $false))

    if (-not (Test-Path (Join-Path $logDir 'meta.properties'))) {
        $format = Invoke-KafkaTool 'kafka-storage' @('format', '--cluster-id', $script:KafkaClusterId, '--config', $config) -KafkaHome $KafkaHome -NoBootstrap
        if ($format.ExitCode -ne 0) { throw "Formatting $logDir failed: $($format.Lines -join ' ')" }
        Write-Host "Formatted $logDir"
    }

    $heap = $env:KAFKA_HEAP_OPTS
    if (-not $heap) { $heap = '-Xmx1G -Xms1G' }
    $arguments = "$heap -Dlog4j2.configurationFile=`"$(Join-Path $KafkaHome 'config\log4j2.yaml')`" " +
        "-Dkafka.logs.dir=`"$appLogs`" -cp `"$(Join-Path $KafkaHome 'libs\*')`" kafka.Kafka `"$config`""
    $process = Start-Process -FilePath (Resolve-KafkaJava) -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $appLogs 'broker.out') -RedirectStandardError (Join-Path $appLogs 'broker.err')
    Set-Content -Path (Join-Path $DataDir 'broker.pid') -Value $process.Id -Encoding ascii

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while (-not (Test-KafkaBroker -KafkaHome $KafkaHome)) {
        if ($process.HasExited) {
            $tail = Get-Content (Join-Path $appLogs 'broker.out') -Tail 20 -ErrorAction SilentlyContinue
            throw "Broker exited during startup:`n$($tail -join "`n")"
        }
        if ((Get-Date) -ge $deadline) { throw "Broker did not answer on $($script:KafkaBootstrap) within $TimeoutSeconds s" }
        Start-Sleep -Seconds 1
    }
    $flush = 'broker default (page cache)'
    if ($env:KAFKA_FLUSH_EVERY_MESSAGE -eq 'true') { $flush = 'every message' }
    Write-Host "Broker started (pid $($process.Id), flush: $flush, data: $DataDir)."
}

if (-not $NoTopics) {
    & (Join-Path $PSScriptRoot 'create-topics.ps1') -KafkaHome $KafkaHome -TimeoutSeconds $TimeoutSeconds
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
