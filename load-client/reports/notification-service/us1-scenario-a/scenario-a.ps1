<#
.SYNOPSIS
    Quickstart scenario A (US1, SC-001, T034): SaleCompleted events with Notification Service absent.

.DESCRIPTION
    Resets the baseline and starts Inventory, Transaction, Analytics, and the gateway, never Notification. Then:
      1. completes 3xA + 1xB;
      2. scans R twice with R's stock at 1, so Complete is rejected (409 INSUFFICIENT_STOCK); raises the stock
         to 2 through Inventory's own credentials, and completes again (attempt 2);
      3. sends Complete again to the first basket (a repeated Complete);
      4. leaves a basket with scans only;
      5. restarts Transaction with checkout.afterRecordCompletion:delay:12000 (longer than the 8 s response
         budget), so Complete answers 500 and the checkout completes in the background; then restarts it
         without the fault, which also checks that a Transaction restart publishes no extra event.
    It then exports the evidence, counts the sale-completed topic's records, and reconciles in fixed mode with
    an empty snapshot. Everything goes to this script's folder.

.EXAMPLE
    load-client/reports/notification-service/us1-scenario-a/scenario-a.ps1 -NginxBin C:\...\nginx.exe
#>
param(
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [string]$KafkaHome,
    [string]$JavaExe,
    [string]$SkuA = 'SKU-000001',
    [string]$SkuB = 'SKU-000002',
    [string]$SkuR = 'SKU-000010'
)

$ErrorActionPreference = 'Stop'
$out = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $out '../../../..')).Path
$server = Join-Path $repo 'server'
. (Join-Path $repo 'db/mysql-tools.ps1')
. (Join-Path $repo 'deploy/kafka/kafka-tools.ps1')
. (Join-Path $server 'benchmark-common.ps1')

$mysql = Resolve-MySqlExe
$KafkaHome = Resolve-KafkaHome $KafkaHome
$JavaExe = Resolve-BenchJava $JavaExe
$savedEnv = @{}
$running = @{}
$credentials = @{}
foreach ($service in $script:ServiceNames) { $credentials[$service] = Get-ServiceDbCredential -Service $service }
Set-ProcessEnv 'KAFKA_HOME' $KafkaHome
$classpath = @((Join-Path $repo 'subscriber-client/out'), (Join-Path $server 'out/contracts.jar'),
    (Join-Path $server 'lib/messaging/*')) -join ';'
$gateway = 'http://127.0.0.1:8080'
$utf8 = New-Object System.Text.UTF8Encoding $false

function Get-ServiceRows([string]$Service, [string]$Query) {
    $c = $credentials[$Service]
    return @(Invoke-MySql -MySql $mysql -User $c.User -Password $c.Password -Database $c.Database -Query $Query -Batch)
}

# Status code and parsed body; an error status is returned, not thrown.
function Invoke-Json([string]$Method, [string]$Path, [string]$Body) {
    $params = @{ Uri = "$gateway$Path"; Method = $Method; UseBasicParsing = $true; TimeoutSec = 30 }
    if ($Method -eq 'POST') { $params.Body = $Body; $params.ContentType = 'application/json' }
    try {
        $response = Invoke-WebRequest @params
        return [pscustomobject]@{ Status = [int]$response.StatusCode; Body = ($response.Content | ConvertFrom-Json) }
    } catch {
        $failed = $_.Exception.Response
        if ($null -eq $failed) { throw }
        # Windows PowerShell 5.1 has usually read the error body already; it is in ErrorDetails.
        $text = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else {
            (New-Object IO.StreamReader($failed.GetResponseStream())).ReadToEnd() }
        $parsed = if ($text) { try { $text | ConvertFrom-Json } catch { $text } } else { $null }
        return [pscustomobject]@{ Status = [int]$failed.StatusCode; Body = $parsed }
    }
}

function New-Basket([string]$Station, [string[]]$Skus) {
    $start = Invoke-Json 'POST' '/transactions' "{`"stationId`":`"$Station`"}"
    if ($start.Status -ne 201) { throw "Start answered $($start.Status)" }
    $id = $start.Body.transactionId
    foreach ($sku in $Skus) {
        $scan = Invoke-Json 'POST' "/transactions/$id/items" "{`"sku`":`"$sku`"}"
        if ($scan.Status -ge 300) { throw "Scan of $sku answered $($scan.Status)" }
    }
    return $id
}

function Complete-Basket([string]$Id, [string]$What) {
    $answer = Invoke-Json 'POST' "/transactions/$Id/complete" ''
    $code = ''
    if ($answer.Body -is [psobject] -and $answer.Body.PSObject.Properties['code']) { $code = $answer.Body.code }
    elseif ($answer.Body -is [psobject] -and $answer.Body.PSObject.Properties['error']) { $code = $answer.Body.error }
    elseif ($answer.Body -is [string]) { $code = $answer.Body }
    Write-Host ("  {0,-34} {1} -> {2} {3}" -f $What, $Id, $answer.Status, $code)
    $script:steps += [pscustomobject]@{ step = $What; transactionId = $Id; status = $answer.Status; body = $answer.Body }
    return $answer
}

function Wait-Completed([string]$Id) {
    Wait-Until "transaction $Id COMPLETED" 90 { (Invoke-Json 'GET' "/transactions/$Id" '').Body.status -eq 'COMPLETED' }
}

function Set-Stock([string]$Sku, [int]$Quantity) {
    $null = Get-ServiceRows 'inventory' "UPDATE inventory SET stock_quantity = $Quantity WHERE sku = '$Sku'"
    Write-Host "  stock of $Sku set to $Quantity (Inventory's credentials)"
}

function Start-Transaction {
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(200, 404) }
    $catalogVersion = (Get-ServiceRows 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    Wait-Until 'Transaction product replica at the catalog version' 60 {
        (Get-ServiceRows 'transaction' "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalogVersion
    }
}

# Keeps a stopped Transaction's logs under a suffix, since a new start writes the same file names.
function Stop-Transaction([string]$Suffix) {
    Stop-ProcessTree $running['transaction'].Id
    $running.Remove('transaction')
    Start-Sleep -Seconds 2
    foreach ($stream in @('stdout', 'stderr')) {
        $log = Join-Path $out "transaction-$stream.log"
        if (Test-Path $log) { Move-Item $log (Join-Path $out "transaction-$stream.$Suffix.log") -Force }
    }
}

$prefix = $null
$steps = @()
$baskets = [ordered]@{}
try {
    if (Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 8084 -State Listen -ErrorAction SilentlyContinue) {
        throw 'Benchmark ports are in use'
    }
    if (Test-Path (Join-Path $out 'exports')) { throw "Remove $out/exports first" }
    $runDir = Join-Path $out 'subscribers'
    & (Join-Path $repo 'db/reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome -RunDir $runDir *> (Join-Path $out 'reset-baseline.txt')
    if ($LASTEXITCODE -ne 0) { throw "reset-baseline failed; see $out/reset-baseline.txt" }

    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-Transaction
    Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    $prefix = Start-Gateway $NginxBin $repo
    Wait-Until 'gateway' 30 { Test-Http "$gateway/items" @(200) }
    if (Get-NetTCPConnection -LocalPort 8084 -State Listen -ErrorAction SilentlyContinue) { throw 'Notification must not be running' }
    Write-Host 'Inventory, Transaction, Analytics, and the gateway are up; Notification is absent' -ForegroundColor Cyan

    # 1. A multi-unit, multi-SKU basket.
    $baskets.multiUnit = New-Basket 'station-a1' @($SkuA, $SkuA, $SkuA, $SkuB)
    $first = Complete-Basket $baskets.multiUnit '3xA + 1xB'
    if ($first.Status -ne 200) { throw 'the first basket did not complete' }

    # 2. Rejected for insufficient stock, then completed on attempt 2.
    Set-Stock $SkuR 1
    $baskets.rejectedThenCompleted = New-Basket 'station-a2' @($SkuR, $SkuR)
    $rejected = Complete-Basket $baskets.rejectedThenCompleted '2xR with stock 1 (attempt 1)'
    if ($rejected.Status -ne 409) { throw "expected 409 INSUFFICIENT_STOCK, got $($rejected.Status)" }
    Set-Stock $SkuR 2
    $second = Complete-Basket $baskets.rejectedThenCompleted '2xR with stock 2 (attempt 2)'
    if ($second.Status -ne 200) { throw 'attempt 2 did not complete' }

    # 3. A repeated Complete of a completed basket.
    $repeat = Complete-Basket $baskets.multiUnit 'repeated Complete of basket 1'
    if ($repeat.Status -lt 400) { throw 'a repeated Complete was accepted' }

    # 4. Scans only.
    $baskets.scansOnly = New-Basket 'station-a4' @($SkuA, $SkuB)
    Write-Host "  scans only                         $($baskets.scansOnly)"

    # 5. A background completion after a 500.
    Stop-Transaction 'run1'
    Set-ProcessEnv 'JAVA_TOOL_OPTIONS' '-Dselfcheckout.faults=checkout.afterRecordCompletion:delay:12000'
    Start-Transaction
    [Environment]::SetEnvironmentVariable('JAVA_TOOL_OPTIONS', $savedEnv['JAVA_TOOL_OPTIONS'], 'Process')
    $baskets.background = New-Basket 'station-a5' @($SkuB, $SkuB, $SkuA)
    $background = Complete-Basket $baskets.background 'Complete with the 12 s hook delay'
    if ($background.Status -ne 500) { throw "expected 500, got $($background.Status)" }
    Wait-Completed $baskets.background
    Write-Host "  $($baskets.background) completed in the background"
    Stop-Transaction 'fault'
    Start-Transaction
    Start-Sleep -Seconds 10   # time for a restarted relay to publish anything it (wrongly) thought was unpublished
} catch {
    Write-Host "FAILED: $_" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace
    throw
} finally {
    foreach ($name in @($running.Keys)) { Stop-ProcessTree $running[$name].Id }
    Stop-Gateway $NginxBin $prefix
}

# The evidence, with each service's own credentials.
$attempts = Get-ServiceRows 'transaction' ('SELECT transaction_id, attempt_number, state, COALESCE(rejection_reason, ''''), ' +
    'client_outcome FROM checkout_attempt ORDER BY transaction_id, attempt_number')
$transactions = Get-ServiceRows 'transaction' 'SELECT transaction_id, status FROM `transaction` ORDER BY transaction_id'
$offsets = Get-KafkaEndOffsets -KafkaHome $KafkaHome
$topicRecords = 0
foreach ($p in @($offsets['selfcheckout.sale-completed.v1'])) { if ($p) { $topicRecords += $p.Offset } }

New-Item -ItemType Directory -Force (Join-Path $runDir 'receipts') | Out-Null
[IO.File]::WriteAllText((Join-Path $runDir 'snapshot.json'), ("{`n  `"seed`": 0,`n  `"settings`": {`n    `"subscribers`": 0,`n" +
    "    `"skusPerSubscriber`": 0,`n    `"skuDistribution`": `"uniform`",`n    `"idPrefix`": `"sub`",`n    `"assignment`": `"file`"`n  },`n" +
    "  `"catalog`": {`n    `"size`": 0,`n    `"sha256`": `"`"`n  },`n  `"subscribers`": []`n}`n"), $utf8)
[IO.File]::WriteAllText((Join-Path $runDir 'ready.json'), ("{`n  `"readyAt`": `"$([DateTime]::UtcNow.ToString('o'))`",`n" +
    "  `"subscriptionVersion`": 0,`n  `"subscribers`": {},`n  `"channels`": {}`n}`n"), $utf8)

& (Join-Path $repo 'db/export-notification-evidence.ps1') -RunDir $out -MySqlExe $mysql -JavaExe $JavaExe *> (Join-Path $out 'export.txt')
$reconcile = Start-Process -FilePath $JavaExe -ArgumentList ((@('-cp', $classpath, 'ReconcileMain', "--run-dir=$out",
        '--mode=fixed') | ForEach-Object { '"' + $_ + '"' }) -join ' ') -WorkingDirectory $out -WindowStyle Hidden `
    -Wait -PassThru -RedirectStandardOutput (Join-Path $out 'reconcile.txt') -RedirectStandardError (Join-Path $out 'reconcile-stderr.txt')

$log = [ordered]@{
    baskets = $baskets
    steps = $steps
    transactions = $transactions
    attempts = $attempts
    saleCompletedTopicRecords = $topicRecords
    reconcileExitCode = $reconcile.ExitCode
}
$log | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $out 'scenario-a-log.json')
Write-Host "sale-completed topic records: $topicRecords"
$attempts | ForEach-Object { Write-Host "  attempt $_" }
Get-Content (Join-Path $out 'reconcile.txt') | Write-Host
exit 0
