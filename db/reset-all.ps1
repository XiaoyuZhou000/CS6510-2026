<#
.SYNOPSIS
    Resets all three service schemas to the benchmark baseline (R13).

.DESCRIPTION
    Runs db/reset-service.ps1 for Inventory, then Transaction, then Analytics,
    each with its own credentials, and stops at the first failure. Then checks
    and prints the baseline: 2,000 catalog rows, 2,000 inventory rows,
    20,000,000 units, zero transactions, zero accepted_scan rows, and
    last_position 0. Exits non-zero if any reset fails or the baseline differs.

    A missing <SERVICE>_DB_PASSWORD is prompted for once and kept only in this
    process for the three resets.
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
    foreach ($service in @('inventory', 'transaction', 'analytics')) {
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

    $baseline = [ordered]@{
        'catalog rows'                     = @([long]$inventory[0], 2000)
        'inventory rows'                   = @([long]$inventory[1], 2000)
        'stock units'                      = @([long]$inventory[2], 20000000)
        'transactions'                     = @([long]$transaction[0], 0)
        'Transaction accepted_scan rows'   = @([long]$transaction[1], 0)
        'Analytics accepted_scan rows'     = @([long]$analytics[0], 0)
        'Analytics last_position'          = @([long]$analytics[1], 0)
    }
    $failed = $false
    Write-Host "Baseline:" -ForegroundColor Cyan
    foreach ($entry in $baseline.GetEnumerator()) {
        $actual = $entry.Value[0]
        $expected = $entry.Value[1]
        if ($actual -eq $expected) {
            Write-Host ("  {0,-32} {1}" -f $entry.Key, $actual)
        } else {
            Write-Host ("  {0,-32} {1} (expected {2})" -f $entry.Key, $actual, $expected) -ForegroundColor Red
            $failed = $true
        }
    }
    if ($failed) {
        Write-Host "reset-all: the baseline is wrong." -ForegroundColor Red
        exit 1
    }
    Write-Host "All three service schemas are at the baseline." -ForegroundColor Green
    exit 0
} catch {
    Write-Host "reset-all failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    foreach ($variable in $prompted) { [Environment]::SetEnvironmentVariable($variable, $null, 'Process') }
}
