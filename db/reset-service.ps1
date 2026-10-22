<#
.SYNOPSIS
    Resets one service's own schema with that service's own credentials (R13).

.DESCRIPTION
    Runs db/<service>/schema.sql, plus db/inventory/seed.sql for Inventory,
    against cs6510_<service> (or <SERVICE>_DB_NAME), connecting only as
    <SERVICE>_DB_USER (default cs6510_<service>_svc). The password comes from
    <SERVICE>_DB_PASSWORD or a secure prompt and reaches mysql through MYSQL_PWD.
    Prints the resulting row counts. No other service's schema is touched.

.EXAMPLE
    ./reset-service.ps1 -Service inventory
    ./reset-service.ps1 -Service transaction -Port 3307
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet('transaction', 'inventory', 'analytics')][string]$Service,
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$credential = Get-ServiceDbCredential -Service $Service
$scripts = @(Join-Path $PSScriptRoot "$Service/schema.sql")
if ($Service -eq 'inventory') { $scripts += Join-Path $PSScriptRoot 'inventory/seed.sql' }

$counts = @{
    transaction = "SELECT (SELECT COUNT(*) FROM ``transaction``) AS transactions, " +
                  "(SELECT COUNT(*) FROM accepted_scan) AS accepted_scans, " +
                  "(SELECT COUNT(*) FROM checkout_attempt) AS checkout_attempts, " +
                  "(SELECT COUNT(*) FROM product_replica) AS replica_products, " +
                  "(SELECT COALESCE(catalog_version, 'NULL') FROM product_replica_meta WHERE meta_key = 1) AS replica_version"
    inventory   = "SELECT (SELECT COUNT(*) FROM catalog_item) AS catalog_items, " +
                  "(SELECT COUNT(*) FROM inventory) AS inventory_rows, " +
                  "(SELECT COALESCE(SUM(stock_quantity), 0) FROM inventory) AS total_units, " +
                  "(SELECT COUNT(*) FROM checkout_operation) AS checkout_operations, " +
                  "(SELECT catalog_version FROM catalog_meta WHERE meta_key = 1) AS catalog_version"
    analytics   = "SELECT (SELECT COUNT(*) FROM accepted_scan) AS accepted_scans, " +
                  "(SELECT last_position FROM scan_sequence WHERE sequence_key = 1) AS last_position, " +
                  "(SELECT COUNT(*) FROM product_name) AS product_names, " +
                  "(SELECT COUNT(*) FROM popular_window) AS popular_windows, " +
                  "(SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1) AS last_window_end"
}

try {
    Write-Host "Resetting $Service schema '$($credential.Database)' on ${MySqlHost}:$Port as '$($credential.User)'..." -ForegroundColor Cyan
    foreach ($file in $scripts) {
        if (-not (Test-Path $file)) { throw "SQL script not found: $file" }
        Write-Host "  running $file" -ForegroundColor DarkGray
        Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $credential.User `
            -Password $credential.Password -Database $credential.Database `
            -InputText (Get-Content -Raw -Encoding UTF8 $file) | Out-Null
    }
    Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $credential.User `
        -Password $credential.Password -Database $credential.Database -Query $counts[$Service] -Table
} catch {
    Write-Host "Reset of $Service failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host "$Service schema reset." -ForegroundColor Green
exit 0
