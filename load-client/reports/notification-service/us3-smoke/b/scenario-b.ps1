<#
.SYNOPSIS
    Quickstart scenario B (US2, T080): notify once per sold SKU, with an explicit assignment file.

.DESCRIPTION
    Resets the baseline, starts the four services and the gateway, then runs SubscriberMain with
    b.json (S1 = {A, B}, S2 = {B}). It completes 3xA + 1xB + 2xC and then a sale of C only, waits for
    the receipts, stops everything, exports the evidence, and reconciles in fixed mode. Every file
    goes to this script's folder. Service passwords come from the four *_DB_PASSWORD variables.

.EXAMPLE
    load-client/reports/notification-service/us3-smoke/b/scenario-b.ps1 -NginxBin C:\...\nginx.exe
#>
param(
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [string]$KafkaHome,
    [string]$JavaExe,
    [string]$SkuA = 'SKU-000001',
    [string]$SkuB = 'SKU-000002',
    [string]$SkuC = 'SKU-000003'
)

$ErrorActionPreference = 'Stop'
$out = $PSScriptRoot
$repo = (Resolve-Path (Join-Path $out '../../../../..')).Path
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

function Get-ServiceRows([string]$Service, [string]$Query) {
    $c = $credentials[$Service]
    return @(Invoke-MySql -MySql $mysql -User $c.User -Password $c.Password -Database $c.Database -Query $Query -Batch)
}

function Invoke-Json([string]$Method, [string]$Path, [string]$Body) {
    $params = @{ Uri = "$gateway$Path"; Method = $Method; UseBasicParsing = $true; TimeoutSec = 15 }
    if ($Method -eq 'POST') { $params.Body = $Body; $params.ContentType = 'application/json' }
    $response = Invoke-WebRequest @params
    return [pscustomobject]@{ Status = [int]$response.StatusCode; Body = ($response.Content | ConvertFrom-Json) }
}

function Complete-Basket([string]$Station, [string[]]$Skus) {
    $id = (Invoke-Json 'POST' '/transactions' "{`"stationId`":`"$Station`"}").Body.transactionId
    foreach ($sku in $Skus) { $null = Invoke-Json 'POST' "/transactions/$id/items" "{`"sku`":`"$sku`"}" }
    $complete = Invoke-Json 'POST' "/transactions/$id/complete" ''
    Wait-Until "transaction $id COMPLETED" 60 { (Invoke-Json 'GET' "/transactions/$id" '').Body.status -eq 'COMPLETED' }
    Write-Host "  $id completed (Complete answered $($complete.Status)): $($Skus -join ', ')"
    return $id
}

$prefix = $null
$subscriber = $null
$log = [ordered]@{}
try {
    if (Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 8084 -State Listen -ErrorAction SilentlyContinue) {
        throw 'Benchmark ports are in use'
    }
    foreach ($name in @('subscribers', 'exports')) { $p = Join-Path $out $name; if (Test-Path $p) { throw "Remove $p first" } }
    $runDir = Join-Path $out 'subscribers'
    & (Join-Path $repo 'db/reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome -RunDir $runDir *> (Join-Path $out 'reset-baseline.txt')
    if ($LASTEXITCODE -ne 0) { throw "reset-baseline failed; see $out/reset-baseline.txt" }

    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    $catalogVersion = (Get-ServiceRows 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    Wait-Until 'Transaction product replica at the catalog version' 60 {
        (Get-ServiceRows 'transaction' "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalogVersion
    }
    Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    Start-ReadyService 'notification' 'Notification catalog loaded' {
        try { (Invoke-RestMethod 'http://127.0.0.1:8084/notifications/ops/status' -TimeoutSec 3).catalogLoaded -eq $true } catch { $false }
    }
    $prefix = Start-Gateway $NginxBin $repo
    Wait-Until 'gateway' 30 { Test-Http "$gateway/notifications/ops/status" @(200) }
    $log.statusBefore = Invoke-RestMethod "$gateway/notifications/ops/status"

    # S1 = {A, B}, S2 = {B}.
    $assignments = Join-Path $out 'b.json'
    $json = "{`n  `"S1`": [`"$SkuA`", `"$SkuB`"],`n  `"S2`": [`"$SkuB`"]`n}`n"
    [IO.File]::WriteAllText($assignments, $json, (New-Object System.Text.UTF8Encoding $false))
    $subscriberArgs = @('-cp', $classpath, 'SubscriberMain', "--assignments=$assignments", "--out=$runDir",
        "--base-url=$gateway", "--bootstrap=$($script:KafkaBootstrap)") | ForEach-Object { '"' + $_ + '"' }
    $subscriber = Start-Process -FilePath $JavaExe -ArgumentList ($subscriberArgs -join ' ') -WorkingDirectory $out `
        -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $out 'subscriber-stdout.log') `
        -RedirectStandardError (Join-Path $out 'subscriber-stderr.log')
    $null = $subscriber.Handle
    $running['subscriber'] = $subscriber
    Wait-Until 'ready.json' 120 { Test-Path (Join-Path $runDir 'ready.json') }
    Write-Host 'Subscribers ready' -ForegroundColor Cyan

    $mixed = Complete-Basket 'station-b1' @($SkuA, $SkuA, $SkuA, $SkuB, $SkuC, $SkuC)
    $cOnly = Complete-Basket 'station-b2' @($SkuC)
    $log.sales = [ordered]@{ mixed = $mixed; cOnly = $cOnly }

    Wait-Until 'three receipts' 60 {
        $status = try { Get-Content -Raw (Join-Path $runDir 'status.json') | ConvertFrom-Json } catch { $null }
        $null -ne $status -and [long]$status.totals.received -ge 3
    }
    Start-Sleep -Seconds 5   # anything unexpected would arrive in this window
    $log.statusAfter = Invoke-RestMethod "$gateway/notifications/ops/status"
} finally {
    if ($subscriber -and -not $subscriber.HasExited) {
        New-Item -ItemType File -Force (Join-Path $out 'subscribers/stop') | Out-Null
        if (-not $subscriber.WaitForExit(15000)) { Stop-ProcessTree $subscriber.Id }
    }
    $running.Remove('subscriber')
    foreach ($name in @($running.Keys)) { Stop-ProcessTree $running[$name].Id }
    Stop-Gateway $NginxBin $prefix
    foreach ($entry in $savedEnv.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process') }
}

& (Join-Path $repo 'db/export-notification-evidence.ps1') -RunDir $out -MySqlExe $mysql -JavaExe $JavaExe
$selections = Get-ServiceRows 'notification' ('SELECT sale_id, subscription_version, recipient_count, notification_count ' +
    'FROM sale_selection ORDER BY sale_id')
$log.selections = $selections
$log | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $out 'scenario-b-log.json')

$reconcile = Start-Process -FilePath $JavaExe -ArgumentList ((@('-cp', $classpath, 'ReconcileMain', "--run-dir=$out",
        '--mode=fixed') | ForEach-Object { '"' + $_ + '"' }) -join ' ') -WorkingDirectory $out -WindowStyle Hidden `
    -Wait -PassThru -RedirectStandardOutput (Join-Path $out 'reconcile-stdout.txt') `
    -RedirectStandardError (Join-Path $out 'reconcile-stderr.txt')
Get-Content (Join-Path $out 'reconcile-stdout.txt') | Write-Host
Write-Host "ReconcileMain exit code: $($reconcile.ExitCode)"
exit $reconcile.ExitCode
