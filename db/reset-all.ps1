<#
.SYNOPSIS
    Resets all four service schemas to the benchmark baseline (R13, 007 T013).

.DESCRIPTION
    Runs db/reset-service.ps1 for Inventory, then Transaction, then Analytics,
    each with its own credentials, then Notification (whose schema.sql runs with
    the admin credentials, DB_USER and DB_PASSWORD), and stops at the first
    failure. Then checks and prints the baseline: 2,000 catalog rows, 2,000
    inventory rows, 20,000,000 units, zero transactions, zero accepted_scan rows,
    last_position 0, and Notification at subscription_clock.version 0 with no
    other rows. Exits non-zero if any reset fails or the baseline differs.

    A missing <SERVICE>_DB_PASSWORD is prompted for once and kept only in this
    process for the four resets.
#>
param(
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
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
    foreach ($service in @('inventory', 'transaction', 'analytics', 'notification')) {
        & (Join-Path $PSScriptRoot 'reset-service.ps1') -Service $service -MySqlHost $MySqlHost -Port $Port -MySqlExe $mysql
        if ($LASTEXITCODE -ne 0) {
            Write-Host "reset-all stopped: the $service reset failed." -ForegroundColor Red
            exit 1
        }
    }

    $inventory = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.inventory `
        -Query "SELECT (SELECT COUNT(*) FROM catalog_item), (SELECT COUNT(*) FROM inventory), (SELECT COALESCE(SUM(stock_quantity), 0) FROM inventory)"
    $transaction = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.transaction `
        -Query "SELECT (SELECT COUNT(*) FROM ``transaction``), (SELECT COUNT(*) FROM accepted_scan)"
    $analytics = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.analytics `
        -Query "SELECT (SELECT COUNT(*) FROM accepted_scan), (SELECT last_position FROM scan_sequence WHERE sequence_key = 1)"
    $notification = Get-MySqlRow -MySql $mysql -MySqlHost $MySqlHost -Port $Port -Credential $credentials.notification `
        -Query ("SELECT (SELECT version FROM subscription_clock WHERE id = 1), (SELECT COUNT(*) FROM subscription_clock), " +
            "(SELECT COUNT(*) FROM subscriber) + (SELECT COUNT(*) FROM subscription) + (SELECT COUNT(*) FROM subscription_change) + " +
            "(SELECT COUNT(*) FROM sale_selection) + (SELECT COUNT(*) FROM notification) + (SELECT COUNT(*) FROM dead_letter_event)")

    $baseline = [ordered]@{
        'catalog rows'                     = @([long]$inventory[0], 2000)
        'inventory rows'                   = @([long]$inventory[1], 2000)
        'stock units'                      = @([long]$inventory[2], 20000000)
        'transactions'                     = @([long]$transaction[0], 0)
        'Transaction accepted_scan rows'   = @([long]$transaction[1], 0)
        'Analytics accepted_scan rows'     = @([long]$analytics[0], 0)
        'Analytics last_position'          = @([long]$analytics[1], 0)
        'Notification subscription version' = @([long]$notification[0], 0)
        'Notification clock rows'          = @([long]$notification[1], 1)
        'Notification other rows'          = @([long]$notification[2], 0)
    }
    $failed = $false
    Write-Host "Baseline:" -ForegroundColor Cyan
    foreach ($entry in $baseline.GetEnumerator()) {
        $actual = $entry.Value[0]
        $expected = $entry.Value[1]
        if ($actual -eq $expected) {
            Write-Host ("  {0,-34} {1}" -f $entry.Key, $actual)
        } else {
            Write-Host ("  {0,-34} {1} (expected {2})" -f $entry.Key, $actual, $expected) -ForegroundColor Red
            $failed = $true
        }
    }
    if ($failed) {
        Write-Host "reset-all: the baseline is wrong." -ForegroundColor Red
        exit 1
    }
    Write-Host "All four service schemas are at the baseline." -ForegroundColor Green
    exit 0
} catch {
    Write-Host "reset-all failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    foreach ($variable in $prompted) { [Environment]::SetEnvironmentVariable($variable, $null, 'Process') }
}
