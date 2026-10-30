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
        Transaction's published accepted_scan rows, and no row is unpublished.
    Event checks (feature 006, research D13):
      - no outbox_event row is unpublished (published_at IS NULL) in Transaction
        or Inventory;
      - no dead_letter_event row exists in Transaction, Inventory, or Analytics
        (up to five parked rows are listed);
      - with -KafkaStatusJson (the output of deploy/kafka/status.ps1 -Json, as a
        file path or as the JSON text), the broker is reachable and every consumer
        group has zero lag. A group reported absent passes only when its topic's
        end offsets are all 0 (before US5 the scan group does not exist and the
        scan topic is empty), and a group with partitions that have no committed
        offset passes only when those partitions can all be empty.
    -SkipAnalytics omits the Analytics checks, including its dead_letter_event,
    and the publication checks of accepted_scan.

    Prints PASS and the FR-061 counts (feature 005's FR-054 counts): COMPLETED
    transactions, background completions (client_outcome PENDING_ERROR),
    completions without a recorded response (NONE), and parked events; with
    -KafkaStatusJson also the final consumer lag. With -TotalTransactions it also
    prints the remaining difference: COMPLETED transactions minus that client
    count. Exits non-zero on any violation.
#>
param(
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe,
    [switch]$SkipAnalytics,
    [long]$TotalTransactions = -1,
    [string]$KafkaStatusJson
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
        $unpublished = [long]((Invoke-ServiceQuery 'transaction' "SELECT COUNT(*) FROM accepted_scan WHERE published_at IS NULL")[0])
        if ($unpublished -ne 0) { $violations.Add("unpublished accepted_scan rows: $unpublished") }
        $published = Read-Counts 'transaction' ("SELECT transaction_id, COUNT(*) FROM accepted_scan " +
            "WHERE published_at IS NOT NULL GROUP BY transaction_id")
        $stored = Read-Counts 'analytics' "SELECT source_transaction_id, COUNT(*) FROM accepted_scan GROUP BY source_transaction_id"
        Compare-Counts 'per-transaction events (Transaction published vs Analytics stored)' $published $stored
        $publishedTotal = [long]((Invoke-ServiceQuery 'transaction' "SELECT COUNT(*) FROM accepted_scan WHERE published_at IS NOT NULL")[0])
        $storedTotal = [long]((Invoke-ServiceQuery 'analytics' "SELECT COUNT(*) FROM accepted_scan")[0])
        if ($publishedTotal -ne $storedTotal) {
            $violations.Add("event count: Transaction published $publishedTotal, Analytics stored $storedTotal")
        }
    }

    $fr054 = (Invoke-ServiceQuery 'transaction' ("SELECT (SELECT COUNT(*) FROM ``transaction`` WHERE status = 'COMPLETED'), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'COMPLETED' AND client_outcome = 'NONE')"))[0] -split "`t"
} catch {
    $violations.Add("reconciliation query failed: $($_.Exception.Message)")
    $fr054 = $null
}

# Event checks: every outbox row published, no parked event (research D13).
Write-Host "Checking outboxes and parked events..." -ForegroundColor Cyan
$parkedTotal = $null
try {
    foreach ($service in @('transaction', 'inventory')) {
        $count = [long]((Invoke-ServiceQuery $service "SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL")[0])
        Write-Host "  unpublished_outbox_$service=$count"
        if ($count -ne 0) { $violations.Add("unpublished $service outbox_event rows: $count") }
    }
    $parkedTotal = [long]0
    foreach ($service in $services) {
        $count = [long]((Invoke-ServiceQuery $service "SELECT COUNT(*) FROM dead_letter_event")[0])
        Write-Host "  parked_events_$service=$count"
        $parkedTotal += $count
        if ($count -eq 0) { continue }
        $violations.Add("parked $service dead_letter_event rows: $count")
        $examples = Invoke-ServiceQuery $service ("SELECT reason, topic, partition_no, record_offset, " +
            "COALESCE(event_type, '-'), COALESCE(event_id, '-'), park_count FROM dead_letter_event ORDER BY dead_letter_id LIMIT 5")
        foreach ($line in $examples) {
            $f = $line -split "`t"
            Write-Host ("    {0} parked {1}: {2}-{3}@{4} type {5} event {6} (parked {7}x)" -f $service, $f[0], $f[1], $f[2], $f[3],
                $f[4], $f[5], $f[6]) -ForegroundColor Red
        }
    }
} catch {
    $violations.Add("event check query failed: $($_.Exception.Message)")
    $parkedTotal = $null
}

# Consumer lag from deploy/kafka/status.ps1 -Json, when given.
$finalLag = $null
if ($KafkaStatusJson) {
    Write-Host "Checking consumer lag..." -ForegroundColor Cyan
    # The topic each consumer group reads (contracts/event-catalog.md "Topics").
    $groupTopics = [ordered]@{
        'inventory-checkout'           = 'selfcheckout.checkout.v1'
        'transaction-checkout-outcome' = 'selfcheckout.checkout-outcome.v1'
        'analytics-scan-ingest'        = 'selfcheckout.scan-accepted.v1'
    }
    $status = $null
    try {
        $text = $KafkaStatusJson
        $isFile = $false
        # JSON text is not a valid path; Test-Path throws on some of its characters.
        try { $isFile = Test-Path -LiteralPath $KafkaStatusJson -PathType Leaf } catch { $isFile = $false }
        if ($isFile) { $text = Get-Content -Raw -LiteralPath $KafkaStatusJson }
        $status = $text | ConvertFrom-Json
    } catch {
        $violations.Add("-KafkaStatusJson could not be read: $($_.Exception.Message)")
    }
    if ($null -ne $status -and -not $status.reachable) {
        $violations.Add('the broker was unreachable in -KafkaStatusJson')
    } elseif ($null -ne $status) {
        $finalLag = [long]0
        foreach ($entry in $groupTopics.GetEnumerator()) {
            $name = $entry.Key
            if ($SkipAnalytics -and $name -eq 'analytics-scan-ingest') { continue }
            $group = @($status.groups | Where-Object { $_.group -eq $name }) | Select-Object -First 1
            # status.ps1 names topics with the optional test prefix in front.
            $topic = @($status.topics | Where-Object { "$($_.name)".EndsWith($entry.Value) }) | Select-Object -First 1
            if ($null -eq $topic -or -not $topic.exists) {
                $violations.Add("topic $($entry.Value) of group $name is missing")
                continue
            }
            $ends = @($topic.endOffsets | ForEach-Object { [long]$_ })
            if ($null -eq $group -or "$($group.state)" -eq 'absent') {
                $total = [long]$topic.totalEndOffset
                Write-Host "  ${name}: absent; $($topic.name) holds $total record(s)"
                if ($total -ne 0) {
                    $finalLag += $total
                    $violations.Add("group $name does not exist while $($topic.name) holds $total record(s)")
                }
                continue
            }
            $lag = [long]0
            if ($null -ne $group.lag) { $lag = [long]$group.lag }
            $uncommitted = [int]$group.uncommitted
            $finalLag += $lag
            Write-Host "  ${name}: $($group.state), lag $lag, partitions without a committed offset $uncommitted"
            if ($lag -ne 0) { $violations.Add("group $name has lag $lag") }
            if ($uncommitted -eq 0) { continue }
            # A partition without a committed offset hides its records from the lag. Per-partition
            # offsets settle it when the status carries them; otherwise the counts must allow every
            # such partition to be empty.
            $partitionsProperty = $group.PSObject.Properties['partitions']
            if ($partitionsProperty -and $null -ne $partitionsProperty.Value) {
                $hidden = [long]0
                foreach ($p in @($partitionsProperty.Value)) {
                    if ($null -eq $p.currentOffset -and $null -ne $p.endOffset) { $hidden += [long]$p.endOffset }
                }
                $finalLag += $hidden
                if ($hidden -ne 0) { $violations.Add("group ${name}: $hidden record(s) in partitions without a committed offset") }
            } else {
                $nonEmpty = @($ends | Where-Object { $_ -gt 0 }).Count
                $committed = [int]$topic.partitions - $uncommitted
                if ($nonEmpty -gt $committed) {
                    $violations.Add("group ${name}: $nonEmpty partition(s) of $($topic.name) hold records but only $committed have a committed offset")
                }
            }
        }
    }
}

if ($null -ne $fr054) {
    Write-Host "FR-061 counts:" -ForegroundColor Cyan
    Write-Host "  completed_transactions=$($fr054[0])"
    Write-Host "  background_completions=$($fr054[1])"
    Write-Host "  completions_without_response=$($fr054[2])"
    if ($null -ne $parkedTotal) { Write-Host "  parked_events=$parkedTotal" }
    if ($null -ne $finalLag) { Write-Host "  final_consumer_lag=$finalLag" }
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
