<#
.SYNOPSIS
    Quickstart scenario E (US4, SC-004, T093): subscription changes in their own scenario, verified against the
    recorded history with ReconcileMain --mode=history.

.DESCRIPTION
    Resets the baseline, starts the four services (Notification with NOTIFICATION_TEST_FAULTS=1) and the gateway,
    and runs subscriber-client ChangeScenarioMain with --faults-endpoint. With -WithLoad it also runs the unchanged
    default Load Client in parallel: the driver waits for <run>/subscribers/load.started before its burst of changes,
    and for load.done after it. Then it stops everything, exports the evidence, and reconciles in history mode.

    Everything goes to <this folder>/<RunName>/. Service passwords come from the four *_DB_PASSWORD variables, and
    KAFKA_HOME must be set (or passed).

.EXAMPLE
    load-client/reports/notification-service/change-scenario/scenario-e.ps1 -RunName e-alone -NginxBin C:\...\nginx.exe
    load-client/reports/notification-service/change-scenario/scenario-e.ps1 -RunName e-with-load -WithLoad -NginxBin C:\...\nginx.exe
#>
param(
    [Parameter(Mandatory = $true)][string]$RunName,
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [switch]$WithLoad,
    [string]$Seed = '20261112',
    [ValidateRange(3, 999)][int]$ChangeSubscribers = 10,
    [string]$KafkaHome,
    [string]$JavaExe
)

$ErrorActionPreference = 'Stop'
if ($RunName -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') { throw '-RunName must be one folder name' }
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../../..')).Path
$server = Join-Path $repo 'server'
$clientDir = Join-Path $repo 'load-client'
. (Join-Path $repo 'db/mysql-tools.ps1')
. (Join-Path $repo 'deploy/kafka/kafka-tools.ps1')
. (Join-Path $server 'benchmark-common.ps1')

$out = Join-Path $PSScriptRoot $RunName
if ((Test-Path $out) -and (Get-ChildItem $out -Force | Select-Object -First 1)) { throw "Run directory already has evidence: $out" }
New-Item -ItemType Directory -Force -Path $out | Out-Null
$subscriberDir = Join-Path $out 'subscribers'
$mysql = Resolve-MySqlExe
$KafkaHome = Resolve-KafkaHome $KafkaHome
$JavaExe = Resolve-BenchJava $JavaExe
$savedEnv = @{}
$running = @{}
foreach ($service in $script:ServiceNames) {
    $credential = Get-ServiceDbCredential -Service $service
    Set-ProcessEnv "$($service.ToUpperInvariant())_DB_PASSWORD" $credential.Password
}
$inventory = Get-ServiceDbCredential -Service 'inventory'
$transaction = Get-ServiceDbCredential -Service 'transaction'
Set-ProcessEnv 'KAFKA_HOME' $KafkaHome
$classpath = @((Join-Path $repo 'subscriber-client/out'), (Join-Path $server 'out/contracts.jar'),
    (Join-Path $server 'lib/messaging/*')) -join ';'
$gateway = 'http://127.0.0.1:8080'

function Get-Value($Credential, [string]$Query) {
    return @(Invoke-MySql -MySql $mysql -User $Credential.User -Password $Credential.Password -Database $Credential.Database `
        -Query $Query -Batch)[0]
}

function Start-Java([string]$Name, [string[]]$Arguments, [string]$WorkingDirectory) {
    $quoted = ($Arguments | ForEach-Object { '"' + $_ + '"' }) -join ' '
    $process = Start-Process -FilePath $JavaExe -ArgumentList $quoted -WorkingDirectory $WorkingDirectory -WindowStyle Hidden `
        -PassThru -RedirectStandardOutput (Join-Path $out "$Name-stdout.txt") -RedirectStandardError (Join-Path $out "$Name-stderr.txt")
    $null = $process.Handle
    return $process
}

$prefix = $null
$driver = $null
$client = $null
$result = [ordered]@{ run = $RunName; withLoad = [bool]$WithLoad; seed = $Seed; startedAt = (Get-Date).ToUniversalTime().ToString('o')
    deploymentCommit = (& git -C $repo rev-parse HEAD) }
try {
    if (Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 8084 -State Listen -ErrorAction SilentlyContinue) {
        throw 'Benchmark ports are in use'
    }
    & (Join-Path $repo 'db/reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome -RunDir $subscriberDir *> (Join-Path $out 'reset-baseline.txt')
    if ($LASTEXITCODE -ne 0) { throw "reset-baseline failed; see $out/reset-baseline.txt" }

    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    $catalogVersion = Get-Value $inventory 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1'
    Wait-Until 'Transaction product replica at the catalog version' 60 {
        (Get-Value $transaction "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1") -eq $catalogVersion
    }
    Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    Set-ProcessEnv 'NOTIFICATION_TEST_FAULTS' '1'
    Start-ReadyService 'notification' 'Notification catalog loaded' {
        try { (Invoke-RestMethod 'http://127.0.0.1:8084/notifications/ops/status' -TimeoutSec 3).catalogLoaded -eq $true } catch { $false }
    }
    [Environment]::SetEnvironmentVariable('NOTIFICATION_TEST_FAULTS', $savedEnv['NOTIFICATION_TEST_FAULTS'], 'Process')
    $prefix = Start-Gateway $NginxBin $repo
    Wait-Until 'gateway' 30 { Test-Http "$gateway/notifications/ops/status" @(200) }

    $driverArgs = @('-cp', $classpath, 'ChangeScenarioMain', "--out=$subscriberDir", "--seed=$Seed",
        "--subscribers=$ChangeSubscribers", "--base-url=$gateway", "--bootstrap=$($script:KafkaBootstrap)",
        '--faults-endpoint=http://127.0.0.1:8084/notifications/ops/faults')
    if ($WithLoad) { $driverArgs += '--with-load' }
    $driver = Start-Java 'change-scenario' $driverArgs $out
    $running['change-scenario'] = $driver

    if ($WithLoad) {
        Wait-Until 'the driver waits for the load' 300 {
            (Test-Path (Join-Path $out 'change-scenario-stdout.txt')) -and
                (Select-String -Path (Join-Path $out 'change-scenario-stdout.txt') -Pattern 'load\.started' -Quiet)
        }
        $reports = Join-Path $clientDir 'reports'
        $before = @(Get-ChildItem $reports -Filter 'report-*.json' | ForEach-Object Name)
        $client = Start-Process -FilePath $JavaExe -ArgumentList @('-cp', 'out', 'Main') -WorkingDirectory $clientDir `
            -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $out 'client-stdout.txt') `
            -RedirectStandardError (Join-Path $out 'client-stderr.txt')
        $null = $client.Handle
        New-Item -ItemType File -Path (Join-Path $subscriberDir 'load.started') | Out-Null
        $client.WaitForExit()
        $result.clientExitCode = $client.ExitCode
        New-Item -ItemType File -Path (Join-Path $subscriberDir 'load.done') | Out-Null
        $new = @(Get-ChildItem $reports -Filter 'report-*.json' | Where-Object { $before -notcontains $_.Name })
        if ($new.Count -eq 1) { Move-Item $new[0].FullName (Join-Path $out 'load-client-report.json') }
    }
    $running.Remove('change-scenario')
    if (-not $driver.WaitForExit(600000)) { throw 'ChangeScenarioMain did not finish within 10 minutes' }
    $result.driverExitCode = $driver.ExitCode
    $result.status = Invoke-RestMethod "$gateway/notifications/ops/status"

    # After a restart the index is rebuilt from committed data: its version must equal the clock (FR-008).
    Stop-ProcessTree $running['notification'].Id
    $running.Remove('notification')
    Start-Sleep -Seconds 2
    foreach ($stream in @('stdout', 'stderr')) {
        $log = Join-Path $out "notification-$stream.log"
        if (Test-Path $log) { Move-Item $log (Join-Path $out "notification-$stream.before-restart.log") -Force }
    }
    Start-ReadyService 'notification' 'Notification catalog loaded after the restart' {
        try { (Invoke-RestMethod 'http://127.0.0.1:8084/notifications/ops/status' -TimeoutSec 3).catalogLoaded -eq $true } catch { $false }
    }
    $restarted = Invoke-RestMethod 'http://127.0.0.1:8084/notifications/ops/status'
    $result.afterRestart = [ordered]@{ subscriptionVersion = $restarted.subscriptionVersion; indexVersion = $restarted.indexVersion
        indexEqualsClock = ($restarted.indexVersion -eq $restarted.subscriptionVersion) }
} finally {
    if ($driver -and -not $driver.HasExited) { Stop-ProcessTree $driver.Id }
    if ($client -and -not $client.HasExited) { Stop-ProcessTree $client.Id }
    foreach ($name in @($running.Keys)) { Stop-ProcessTree $running[$name].Id }
    Stop-Gateway $NginxBin $prefix
    foreach ($entry in $savedEnv.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process') }
}

& (Join-Path $repo 'db/export-notification-evidence.ps1') -RunDir $out -MySqlExe $mysql -JavaExe $JavaExe *> (Join-Path $out 'export.txt')
$reconcile = Start-Java 'reconcile' @('-cp', $classpath, 'ReconcileMain', "--run-dir=$out", '--mode=history',
    "--report-out=$(Join-Path $out 'notification-report.json')") $out
$reconcile.WaitForExit()
$result.reconcileExitCode = $reconcile.ExitCode
$result | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $out 'scenario-e.json')
Get-Content (Join-Path $out 'reconcile-stdout.txt') | Write-Host
Write-Host "ChangeScenarioMain exit $($result.driverExitCode), ReconcileMain exit $($result.reconcileExitCode)"
if ($result.driverExitCode -eq 0 -and $result.reconcileExitCode -eq 0) { exit 0 }
exit 1
