<#
.SYNOPSIS
    Validates every service's invariants and reconciles them across services (R13).

.DESCRIPTION
    Run after pending work drains. Runs db/transaction/validate.sql,
    db/inventory/validate.sql, and db/analytics/validate.sql, each with that
    service's own credentials, then exports aggregates with `mysql --batch` and
    reconciles them here:
      - per SKU, Inventory's sold units (10,000 minus stock) equal the
        quantities of COMPLETED transactions' accepted_scan rows;
      - Inventory's APPLIED operation keys equal Transaction's COMPLETED attempt keys;
      - Analytics' event count and per-source-transaction counts equal
        Transaction's delivered accepted_scan rows, and no row is undelivered.
    -SkipAnalytics omits the Analytics checks and the delivery checks.

    Prints PASS and the FR-054 counts: COMPLETED transactions, background
    completions (client_outcome PENDING_ERROR), and completions without a
    recorded response (NONE). With -TotalTransactions it also prints the
    remaining difference: COMPLETED transactions minus that client count.
    Exits non-zero on any violation.
#>
param(
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe,
    [switch]$SkipAnalytics,
    [long]$TotalTransactions = -1
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$services = @('transaction', 'inventory')
if (-not $SkipAnalytics) { $services += 'analytics' }
$credentials = @{}
foreach ($service in $services) { $credentials[$service] = Get-ServiceDbCredential -Service $service }

$violations = New-Object System.Collections.Generic.List[string]

function Invoke-ServiceQuery([string]$Service, [string]$Query) {
    $c = $credentials[$Service]
    # The leading comma keeps a one-row result an array instead of unrolling it to a string.
    return ,@(Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $c.User -Password $c.Password `
        -Database $c.Database -Query $Query -Batch)
}

# Reads "key<TAB>count" rows into an ordinal (case-sensitive) dictionary.
function Read-Counts([string]$Service, [string]$Query) {
    $map = New-Object 'System.Collections.Generic.Dictionary[string,long]' ([StringComparer]::Ordinal)
    foreach ($line in (Invoke-ServiceQuery $Service $Query)) {
        if ($line -eq '') { continue }
        $fields = $line -split "`t"
        $map[$fields[0]] = [long]$fields[1]
    }
    return ,$map
}

function Read-Keys([string]$Service, [string]$Query) {
    $set = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)
    foreach ($line in (Invoke-ServiceQuery $Service $Query)) {
        if ($line -ne '') { [void]$set.Add($line) }
    }
    return ,$set
}

# Records one violation per mismatching key, printing at most five examples.
function Compare-Counts([string]$Name, $Left, $Right) {
    $keys = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)
    foreach ($key in $Left.Keys) { [void]$keys.Add($key) }
    foreach ($key in $Right.Keys) { [void]$keys.Add($key) }
    $mismatches = 0
    foreach ($key in $keys) {
        $l = 0; $r = 0
        if ($Left.ContainsKey($key)) { $l = $Left[$key] }
        if ($Right.ContainsKey($key)) { $r = $Right[$key] }
        if ($l -ne $r) {
            $mismatches++
            if ($mismatches -le 5) { Write-Host "    $Name ${key}: $l vs $r" -ForegroundColor Red }
        }
    }
    if ($mismatches -gt 0) { $violations.Add("${Name}: $mismatches mismatching key(s)") }
}

foreach ($service in $services) {
    $c = $credentials[$service]
    $file = Join-Path $PSScriptRoot "$service/validate.sql"
    Write-Host "Validating $service ($($c.Database) as $($c.User))..." -ForegroundColor Cyan
    try {
        Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $c.User -Password $c.Password `
            -Database $c.Database -InputText (Get-Content -Raw -Encoding UTF8 $file) -Table
    } catch {
        $violations.Add("$service validate.sql failed")
    }
}

Write-Host "Reconciling across services..." -ForegroundColor Cyan
try {
    $soldByTransaction = Read-Counts 'transaction' ("SELECT s.sku, COUNT(*) FROM accepted_scan s " +
        "JOIN ``transaction`` t ON t.transaction_id = s.transaction_id WHERE t.status = 'COMPLETED' GROUP BY s.sku")
    $soldByInventory = Read-Counts 'inventory' ("SELECT sku, 10000 - stock_quantity FROM inventory " +
        "WHERE stock_quantity <> 10000")
    Compare-Counts 'per-SKU units sold (Transaction vs Inventory)' $soldByTransaction $soldByInventory

    $completedAttempts = Read-Keys 'transaction' ("SELECT CONCAT(transaction_id, '#', attempt_number) " +
        "FROM checkout_attempt WHERE state = 'COMPLETED'")
    $appliedOperations = Read-Keys 'inventory' ("SELECT CONCAT(transaction_id, '#', attempt_number) " +
        "FROM checkout_operation WHERE state = 'APPLIED'")
    $onlyCompleted = @($completedAttempts | Where-Object { -not $appliedOperations.Contains($_) })
    $onlyApplied = @($appliedOperations | Where-Object { -not $completedAttempts.Contains($_) })
    foreach ($key in ($onlyCompleted | Select-Object -First 5)) { Write-Host "    COMPLETED attempt without APPLIED operation: $key" -ForegroundColor Red }
    foreach ($key in ($onlyApplied | Select-Object -First 5)) { Write-Host "    APPLIED operation without COMPLETED attempt: $key" -ForegroundColor Red }
    if ($onlyCompleted.Count + $onlyApplied.Count -gt 0) {
        $violations.Add("attempt keys: $($onlyCompleted.Count) COMPLETED-only, $($onlyApplied.Count) APPLIED-only")
    }

    if (-not $SkipAnalytics) {
        $undelivered = [long]((Invoke-ServiceQuery 'transaction' "SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL")[0])
        if ($undelivered -ne 0) { $violations.Add("undelivered accepted_scan rows: $undelivered") }
        $delivered = Read-Counts 'transaction' ("SELECT transaction_id, COUNT(*) FROM accepted_scan " +
            "WHERE delivered_at IS NOT NULL GROUP BY transaction_id")
        $stored = Read-Counts 'analytics' "SELECT source_transaction_id, COUNT(*) FROM accepted_scan GROUP BY source_transaction_id"
        Compare-Counts 'per-transaction events (Transaction delivered vs Analytics stored)' $delivered $stored
        $deliveredTotal = [long]((Invoke-ServiceQuery 'transaction' "SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NOT NULL")[0])
        $storedTotal = [long]((Invoke-ServiceQuery 'analytics' "SELECT COUNT(*) FROM accepted_scan")[0])
        if ($deliveredTotal -ne $storedTotal) {
            $violations.Add("event count: Transaction delivered $deliveredTotal, Analytics stored $storedTotal")
        }
    }

    $fr054 = (Invoke-ServiceQuery 'transaction' ("SELECT (SELECT COUNT(*) FROM ``transaction`` WHERE status = 'COMPLETED'), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'NONE')"))[0] -split "`t"
} catch {
    $violations.Add("reconciliation query failed: $($_.Exception.Message)")
    $fr054 = $null
}

if ($null -ne $fr054) {
    Write-Host "FR-054 counts:" -ForegroundColor Cyan
    Write-Host "  completed_transactions=$($fr054[0])"
    Write-Host "  background_completions=$($fr054[1])"
    Write-Host "  completions_without_response=$($fr054[2])"
    if ($TotalTransactions -ge 0) {
        Write-Host "  client_total_transactions=$TotalTransactions"
        Write-Host "  remaining_difference=$([long]$fr054[0] - $TotalTransactions)"
    }
}

if ($violations.Count -gt 0) {
    Write-Host "FAIL: $($violations.Count) violation(s)" -ForegroundColor Red
    foreach ($violation in $violations) { Write-Host "  - $violation" -ForegroundColor Red }
    exit 1
}
$scope = 'transaction, inventory, analytics'
if ($SkipAnalytics) { $scope = 'transaction, inventory (Analytics skipped)' }
Write-Host "PASS ($scope)" -ForegroundColor Green
exit 0
