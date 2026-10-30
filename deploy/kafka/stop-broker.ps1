<#
.SYNOPSIS
    Stops the native broker (research D1).

.DESCRIPTION
    Ends the java.exe that listens on 127.0.0.1:19092 and waits until the port is free. Windows has
    no signal that asks a hidden JVM to shut down cleanly, so this is always a forced stop, the same
    as a crash: every record the broker acknowledged is already in its log files, which the
    operating system keeps, and the next start recovers the unflushed segment tails. Producers keep
    every unconfirmed event in their outboxes (D5).

.EXAMPLE
    ./deploy/kafka/stop-broker.ps1
#>
param([int]$TimeoutSeconds = 30)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'kafka-tools.ps1')

$process = Get-KafkaBrokerProcess
if ($null -eq $process) {
    Write-Host 'Broker is not running.'
    exit 0
}
if ($process.ProcessName -ne 'java') {
    throw "Port $($script:KafkaPort) is held by $($process.ProcessName) (pid $($process.Id)), not a broker JVM"
}
Stop-Process -Id $process.Id -Force
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while ($null -ne (Get-KafkaBrokerProcess)) {
    if ((Get-Date) -ge $deadline) { throw "Broker port $($script:KafkaPort) still held after $TimeoutSeconds s" }
    Start-Sleep -Milliseconds 250
}
Write-Host "Broker stopped (pid $($process.Id))."
