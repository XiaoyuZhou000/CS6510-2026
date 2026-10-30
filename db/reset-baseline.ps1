<#
.SYNOPSIS
    Resets the broker and all three service schemas to the benchmark baseline (research D13, FR-059).

.DESCRIPTION
    1. Refuses to run while anything answers on the service ports 8081-8083: a running service
       could publish a stale outbox row into the fresh topics, or lose the offsets it works on.
    2. Runs deploy/kafka/reset-topics.ps1, which stops the broker, deletes its log directory,
       starts it again, recreates the four topics, and verifies that they are empty and that no
       consumer group remains.
    3. Runs db/reset-all.ps1, whose schema.sql runs also recreate, empty, each service's
       outbox_event, dead_letter_event, and (Analytics) consumer_offset.
    4. Checks and prints the baseline: 2,000 products, 2,000 stock rows, 20,000,000 units; zero
       transactions, checkout attempts, checkout operations, accepted scans, outbox rows, parked
       events, and stored consumer offsets; Analytics last_position 0; every topic present with its
       partitions and a total end offset of 0. Exits non-zero on any mismatch or failed step.

    Service passwords come from TRANSACTION_DB_PASSWORD, INVENTORY_DB_PASSWORD, and
    ANALYTICS_DB_PASSWORD; a missing one is prompted for once, before the broker is touched, and
    kept only in this process. The broker scripts need KAFKA_HOME (or -KafkaHome).

.EXAMPLE
    ./db/reset-baseline.ps1
    ./db/reset-baseline.ps1 -KafkaHome C:\tools\kafka_2.13-4.1.0
#>
param(
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe,
    [string]$KafkaHome,
    [string]$DataDir
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')
$kafkaDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'deploy/kafka'
. (Join-Path $kafkaDir 'kafka-tools.ps1')

$answering = @(Test-ServicePortAnswers -Ports 8081, 8082, 8083)
if ($answering.Count -gt 0) {
    Write-Host "reset-baseline refused: ports $($answering -join ', ') answer; stop the services first." -ForegroundColor Red
    exit 1
}

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$KafkaHome = Resolve-KafkaHome $KafkaHome
$credentials = @{}
$prompted = @()
foreach ($service in $script:ServiceNames) {
    $variable = "$($service.ToUpperInvariant())_DB_PASSWORD"
    $credentials[$service] = Get-ServiceDbCredential -Service $service
    if (-not [Environment]::GetEnvironmentVariable($variable)) {
        [Environment]::SetEnvironmentVariable($variable, $credentials[$service].Password, 'Process')
        $prompted += $variable
    }
}

try {
    Write-Host "Resetting the broker..." -ForegroundColor Cyan
    $resetTopicArgs = @{ KafkaHome = $KafkaHome }
    if ($DataDir) { $resetTopicArgs.DataDir = $DataDir }
    # reset-topics.ps1 ends without an explicit exit on success, so clear the exit code first.
    $global:LASTEXITCODE = 0
    & (Join-Path $kafkaDir 'reset-topics.ps1') @resetTopicArgs
    if ($LASTEXITCODE -ne 0) { throw 'deploy/kafka/reset-topics.ps1 failed' }

    Write-Host "Resetting the service schemas..." -ForegroundColor Cyan
    & (Join-Path $PSScriptRoot 'reset-all.ps1') -MySqlHost $MySqlHost -Port $Port -MySqlExe $mysql
    if ($LASTEXITCODE -ne 0) { throw 'db/reset-all.ps1 failed' }

    $inventory = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.inventory `
        -Query ("SELECT (SELECT COUNT(*) FROM catalog_item), (SELECT COUNT(*) FROM inventory), " +
            "(SELECT COALESCE(SUM(stock_quantity), 0) FROM inventory), (SELECT COUNT(*) FROM checkout_operation), " +
            "(SELECT COUNT(*) FROM outbox_event), (SELECT COUNT(*) FROM dead_letter_event)")
    $transaction = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.transaction `
        -Query ("SELECT (SELECT COUNT(*) FROM ``transaction``), (SELECT COUNT(*) FROM checkout_attempt), " +
            "(SELECT COUNT(*) FROM accepted_scan), (SELECT COUNT(*) FROM outbox_event), (SELECT COUNT(*) FROM dead_letter_event)")
    $analytics = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.analytics `
        -Query ("SELECT (SELECT COUNT(*) FROM accepted_scan), (SELECT last_position FROM scan_sequence WHERE sequence_key = 1), " +
            "(SELECT COUNT(*) FROM dead_letter_event), (SELECT COUNT(*) FROM consumer_offset)")

    $baseline = [ordered]@{
        'products'                          = @([long]$inventory[0], 2000)
        'stock rows'                        = @([long]$inventory[1], 2000)
        'stock units'                       = @([long]$inventory[2], 20000000)
        'Inventory checkout operations'     = @([long]$inventory[3], 0)
        'Inventory outbox rows'             = @([long]$inventory[4], 0)
        'Inventory parked events'           = @([long]$inventory[5], 0)
        'transactions'                      = @([long]$transaction[0], 0)
        'Transaction checkout attempts'     = @([long]$transaction[1], 0)
        'Transaction accepted scans'        = @([long]$transaction[2], 0)
        'Transaction outbox rows'           = @([long]$transaction[3], 0)
        'Transaction parked events'         = @([long]$transaction[4], 0)
        'Analytics accepted scans'          = @([long]$analytics[0], 0)
        'Analytics last_position'           = @([long]$analytics[1], 0)
        'Analytics parked events'           = @([long]$analytics[2], 0)
        'Analytics stored consumer offsets' = @([long]$analytics[3], 0)
    }
    $described = Get-KafkaTopics -KafkaHome $KafkaHome
    $ends = Get-KafkaEndOffsets -KafkaHome $KafkaHome
    foreach ($spec in (Get-KafkaTopicSpecs)) {
        $partitions = 0
        if ($described.ContainsKey($spec.Name)) { $partitions = $described[$spec.Name].Partitions }
        $total = [long]0
        foreach ($offset in @($ends[$spec.Name])) { if ($null -ne $offset) { $total += $offset.Offset } }
        $baseline["$($spec.Name) partitions"] = @([long]$partitions, [long]$spec.Partitions)
        $baseline["$($spec.Name) end offsets"] = @($total, 0)
    }

    $failed = $false
    Write-Host "Baseline:" -ForegroundColor Cyan
    foreach ($entry in $baseline.GetEnumerator()) {
        $actual = $entry.Value[0]
        $expected = $entry.Value[1]
        if ($actual -eq $expected) {
            Write-Host ("  {0,-48} {1}" -f $entry.Key, $actual)
        } else {
            Write-Host ("  {0,-48} {1} (expected {2})" -f $entry.Key, $actual, $expected) -ForegroundColor Red
            $failed = $true
        }
    }
    if ($failed) {
        Write-Host "reset-baseline: the baseline is wrong." -ForegroundColor Red
        exit 1
    }
    Write-Host "The broker and all three service schemas are at the baseline." -ForegroundColor Green
    exit 0
} catch {
    Write-Host "reset-baseline failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    foreach ($variable in $prompted) { [Environment]::SetEnvironmentVariable($variable, $null, 'Process') }
}
