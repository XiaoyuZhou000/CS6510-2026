<#
.SYNOPSIS
    Resets one service's own schema with that service's own credentials (R13).

.DESCRIPTION
    Runs db/<service>/schema.sql, plus db/inventory/seed.sql for Inventory,
    against cs6510_<service> (or <SERVICE>_DB_NAME), connecting only as
    <SERVICE>_DB_USER (default cs6510_<service>_svc). The password comes from
    <SERVICE>_DB_PASSWORD or a secure prompt and reaches mysql through MYSQL_PWD.
    Prints the resulting row counts. No other service's schema is touched.

    Notification's user may only read and write rows (007 T012), so its
    schema.sql runs with the admin credentials (DB_USER, default root, and
    DB_PASSWORD) against cs6510_notification; its row counts are then read as
    the Notification user, which checks that user's access too.

.EXAMPLE
    ./reset-service.ps1 -Service inventory
    ./reset-service.ps1 -Service transaction -Port 3307
    ./reset-service.ps1 -Service notification
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet('transaction', 'inventory', 'analytics', 'notification')][string]$Service,
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$credential = Get-ServiceDbCredential -Service $Service
$schemaCredential = $credential
if ($Service -eq 'notification') { $schemaCredential = Get-AdminDbCredential -Database $credential.Database }
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
    notification = "SELECT (SELECT version FROM subscription_clock WHERE id = 1) AS subscription_version, " +
                  "(SELECT COUNT(*) FROM subscriber) AS subscribers, " +
                  "(SELECT COUNT(*) FROM subscription) AS subscriptions, " +
                  "(SELECT COUNT(*) FROM subscription_change) AS subscription_changes, " +
                  "(SELECT COUNT(*) FROM sale_selection) AS sale_selections, " +
                  "(SELECT COUNT(*) FROM notification) AS notifications, " +
                  "(SELECT COUNT(*) FROM dead_letter_event) AS parked_events"
}

try {
    Write-Host "Resetting $Service schema '$($credential.Database)' on ${MySqlHost}:$Port as '$($schemaCredential.User)'..." -ForegroundColor Cyan
    foreach ($file in $scripts) {
        if (-not (Test-Path $file)) { throw "SQL script not found: $file" }
        Write-Host "  running $file" -ForegroundColor DarkGray
        Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $schemaCredential.User `
            -Password $schemaCredential.Password -Database $schemaCredential.Database `
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
