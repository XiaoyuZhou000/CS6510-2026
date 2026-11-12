<#
.SYNOPSIS
    Exports the 007 reconciliation evidence of one run, each source with its owner's credentials (FR-024,
    research N11, contracts/subscriber-client-and-reconciliation.md "Evidence export").

.DESCRIPTION
    Writes tab-separated files with a header row to <RunDir>/exports/:
      Transaction (TRANSACTION_DB_PASSWORD), db/transaction/export-sales.sql and export-sale-events.sql:
        sales.tsv         transaction_id, completed_at_ms, sku, quantity (COMPLETED x COUNT(accepted_scan))
        sale-events.tsv   event_id, aggregate_key, published_at_ms, sale_id, completed_at_ms, lines
                          (the SaleCompleted outbox rows, decoded by subscriber-client DecodeSaleEvents)
      Notification (NOTIFICATION_DB_PASSWORD), db/notification/export-evidence.sql:
        selections.tsv, notifications.tsv, history.tsv, parked-events.tsv
    Values are mysql --batch text (NULL for SQL NULL); times are epoch milliseconds. No service reads another's
    data, and passwords travel only through MYSQL_PWD (db/mysql-tools.ps1).

    The sale-event decoding needs subscriber-client/build.sh to have run. The script never changes any data.

.EXAMPLE
    db/export-notification-evidence.ps1 -RunDir load-client/reports/notification-service/us3-smoke/d-default-1
#>
param(
    [Parameter(Mandatory = $true)][string]$RunDir,
    [string]$MySqlHost = '127.0.0.1',
    [int]$Port = 3307,
    [string]$MySqlExe,
    [string]$JavaExe = 'java'
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$repo = Split-Path -Parent $PSScriptRoot
if (-not [IO.Path]::IsPathRooted($RunDir)) { $RunDir = Join-Path (Get-Location) $RunDir }
$RunDir = [IO.Path]::GetFullPath($RunDir)
$exports = Join-Path $RunDir 'exports'
New-Item -ItemType Directory -Force -Path $exports | Out-Null

$clientOut = Join-Path $repo 'subscriber-client/out'
if (-not (Test-Path (Join-Path $clientOut 'DecodeSaleEvents.class'))) {
    throw 'subscriber-client is not built; run subscriber-client/build.sh first'
}
$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$utf8 = New-Object System.Text.UTF8Encoding $false

# mysql --batch prints no header for an empty result, so every file's header is fixed here; it must match the
# column order of the SQL.
$headers = @{
    'sales.tsv'           = 'transaction_id', 'completed_at_ms', 'sku', 'quantity'
    'sale-events-raw.tsv' = 'event_id', 'aggregate_key', 'published_at_ms', 'envelope_hex'
    'selections.tsv'      = 'sale_id', 'sale_event_id', 'completed_at_ms', 'lines', 'subscription_version',
                            'recipient_count', 'notification_count', 'selected_at_ms'
    'notifications.tsv'   = 'notification_id', 'sale_id', 'sku', 'subscriber_id', 'quantity', 'completed_at_ms',
                            'state', 'attempt_count', 'charged_failures', 'uncharged_failures', 'operator_retries',
                            'created_at_ms', 'delivered_at_ms', 'parked_at_ms'
    'history.tsv'         = 'version', 'subscriber_id', 'change_type', 'added_skus', 'removed_skus',
                            'resulting_skus', 'committed_at_ms'
    'parked-events.tsv'   = 'dead_letter_id', 'event_id', 'topic', 'partition_no', 'record_offset', 'event_type',
                            'reason', 'park_count', 'first_parked_at_ms'
}

function Export-Query($Credential, [string]$Sql, [string]$Name) {
    if (-not $headers.ContainsKey($Name)) { throw "No header defined for $Name" }
    $lines = @(Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $Credential.User `
        -Password $Credential.Password -Database $Credential.Database -InputText $Sql -Batch)
    $text = New-Object System.Text.StringBuilder
    [void]$text.Append(($headers[$Name] -join "`t")).Append("`n")
    foreach ($line in $lines) { if ("$line" -ne '') { [void]$text.Append($line).Append("`n") } }
    [IO.File]::WriteAllText((Join-Path $exports $Name), $text.ToString(), $utf8)
    Write-Host ("  {0,-20} {1,8} rows" -f $Name, $lines.Count)
    return $lines.Count
}

# Transaction's records, with Transaction's credentials.
$transaction = Get-ServiceDbCredential -Service 'transaction'
Write-Host "Exporting Transaction evidence ($($transaction.Database) as $($transaction.User))..." -ForegroundColor Cyan
$null = Export-Query $transaction (Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'transaction/export-sales.sql')) 'sales.tsv'
$null = Export-Query $transaction (Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'transaction/export-sale-events.sql')) 'sale-events-raw.tsv'

$raw = Join-Path $exports 'sale-events-raw.tsv'
$classpath = @($clientOut, (Join-Path $repo 'server/out/contracts.jar'), (Join-Path $repo 'server/lib/messaging/*')) -join ';'
# Start-Process, because Windows PowerShell 5.1 turns a redirected native stderr line into a terminating error.
$decodeArgs = @('-cp', $classpath, 'DecodeSaleEvents', "--in=$raw", "--out=$(Join-Path $exports 'sale-events.tsv')") |
    ForEach-Object { '"' + $_ + '"' }
$decodeLog = Join-Path $exports 'decode-sale-events.txt'
$decode = Start-Process -FilePath $JavaExe -ArgumentList ($decodeArgs -join ' ') -WorkingDirectory $exports -WindowStyle Hidden `
    -Wait -PassThru -RedirectStandardOutput $decodeLog -RedirectStandardError (Join-Path $exports 'decode-sale-events-stderr.txt')
Get-Content $decodeLog | Write-Host
if ($decode.ExitCode -ne 0) { throw "DecodeSaleEvents failed with exit code $($decode.ExitCode); see $exports" }
Remove-Item $raw

# Notification's records, with Notification's credentials: one statement per "-- @file <name>" section.
$notification = Get-ServiceDbCredential -Service 'notification'
Write-Host "Exporting Notification evidence ($($notification.Database) as $($notification.User))..." -ForegroundColor Cyan
$sections = [ordered]@{}
$current = $null
foreach ($line in (Get-Content -Encoding UTF8 (Join-Path $PSScriptRoot 'notification/export-evidence.sql'))) {
    if ($line -match '^--\s*@file\s+(\S+)\s*$') {
        $current = $Matches[1]
        $sections[$current] = New-Object System.Text.StringBuilder
    } elseif ($null -ne $current) {
        [void]$sections[$current].AppendLine($line)
    }
}
foreach ($name in @('selections.tsv', 'notifications.tsv', 'history.tsv', 'parked-events.tsv')) {
    if (-not $sections.Contains($name)) { throw "db/notification/export-evidence.sql has no section for $name" }
    $null = Export-Query $notification $sections[$name].ToString() $name
}
Write-Host "Evidence exported to $exports" -ForegroundColor Green
