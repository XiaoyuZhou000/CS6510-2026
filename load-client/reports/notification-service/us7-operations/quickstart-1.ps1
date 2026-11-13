# Quickstart section 1 of 007 (T117): reset-baseline -RunDir, the four services and Nginx, then the expected checks.
# ASCII only; the repository path is a parameter. Writes a transcript to -Out and tears everything down.
param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][string]$NginxBin,
    [Parameter(Mandatory = $true)][string]$KafkaHome,
    [Parameter(Mandatory = $true)][string]$Out
)
$ErrorActionPreference = 'Stop'
$repo = $Repo
$server = Join-Path $repo 'server'
. (Join-Path $repo 'db/mysql-tools.ps1')
. (Join-Path $server 'benchmark-common.ps1')
$script:savedEnv = @{}
$script:running = @{}
$out = $Out
New-Item -ItemType Directory -Force -Path $out | Out-Null
$JavaExe = Resolve-BenchJava ''
foreach ($name in @('TRANSACTION', 'INVENTORY', 'ANALYTICS', 'NOTIFICATION')) {
    $variable = "${name}_DB_PASSWORD"
    if (-not [Environment]::GetEnvironmentVariable($variable, 'Process')) {
        [Environment]::SetEnvironmentVariable($variable, [Environment]::GetEnvironmentVariable($variable, 'User'), 'Process')
    }
}
$env:KAFKA_HOME = $KafkaHome
$failures = New-Object System.Collections.Generic.List[string]
function Check([string]$What, [bool]$Ok, [string]$Detail) {
    $mark = if ($Ok) { 'PASS' } else { 'FAIL' }
    if (-not $Ok) { $failures.Add($What) }
    Write-Host ("[{0}] {1}: {2}" -f $mark, $What, $Detail)
}
function Invoke-Route([string]$Method, [string]$Path, [string]$Body) {
    $request = [System.Net.HttpWebRequest]::Create("http://127.0.0.1:8080$Path")
    $request.Method = $Method
    $request.Timeout = 15000
    # A [string] parameter turns $null into '', so the method decides whether a body is sent.
    if ($Method -eq 'POST') {
        $request.ContentType = 'application/json'
        $bytes = [Text.Encoding]::UTF8.GetBytes($Body)
        $request.ContentLength = $bytes.Length
        $stream = $request.GetRequestStream(); $stream.Write($bytes, 0, $bytes.Length); $stream.Close()
    }
    try { $response = $request.GetResponse() } catch [System.Net.WebException] { $response = $_.Exception.Response }
    $reader = New-Object IO.StreamReader($response.GetResponseStream())
    $text = $reader.ReadToEnd(); $reader.Close()
    $status = [int]$response.StatusCode; $response.Close()
    return [pscustomobject]@{ Status = $status; Body = $text }
}

$runDir = Join-Path $out 'runs/smoke'
New-Item -ItemType Directory -Force -Path (Join-Path $runDir 'subscribers/receipts') | Out-Null
Set-Content -Path (Join-Path $runDir 'subscribers/receipts/sub-0001.jsonl') -Value '{}'
$nginxPrefix = $null
try {
    $mysql = Resolve-MySqlExe -Explicit ''
    Write-Host "=== db/reset-baseline.ps1 -RunDir runs/smoke"
    & (Join-Path $repo 'db/reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome -RunDir $runDir *> (Join-Path $out 'reset-baseline.txt')
    Check 'reset-baseline exit code' ($LASTEXITCODE -eq 0) "$LASTEXITCODE (output in reset-baseline.txt)"
    Check 'run directory removed' (-not (Test-Path $runDir)) $runDir

    Write-Host "=== start Inventory, Transaction, Analytics, Notification, then Nginx"
    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    Start-ReadyService 'notification' 'Notification status' { Test-Http 'http://127.0.0.1:8084/notifications/ops/status' @(200) }
    $nginxPrefix = Start-Gateway $NginxBin $repo
    Wait-Until 'gateway /items' 30 { Test-Http 'http://127.0.0.1:8080/items' @(200) }

    Write-Host "=== expected results"
    $deadline = (Get-Date).AddSeconds(60)
    do {
        $ops = Invoke-Route 'GET' '/notifications/ops/status' $null
        $status = $ops.Body | ConvertFrom-Json
        if ($status.catalogLoaded) { break }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    Check 'GET :8080/notifications/ops/status' ($ops.Status -eq 200 -and $status.catalogLoaded -eq $true -and [long]$status.subscriptionVersion -eq 0) `
        "status $($ops.Status), catalogLoaded=$($status.catalogLoaded), subscriptionVersion=$($status.subscriptionVersion), activeSubscribers=$($status.activeSubscribers)"

    $kafka = & (Join-Path $repo 'deploy/kafka/status.ps1') -Json -KafkaHome $KafkaHome | Out-String | ConvertFrom-Json
    $topics = @($kafka.topics | Where-Object { $_.exists } | ForEach-Object { "$($_.name) ($($_.partitions))" })
    Check 'deploy/kafka/status.ps1 lists five topics' ($kafka.reachable -and $topics.Count -eq 5) ($topics -join ', ')
    $groups = @($kafka.groups | ForEach-Object { "$($_.group)=$($_.state)/lag $($_.lag)" })
    Check 'status.ps1 reports the four groups' ($groups.Count -eq 4) ($groups -join ', ')

    $items = Invoke-Route 'GET' '/items' $null
    Check 'GET /items' ($items.Status -eq 200 -and @(($items.Body | ConvertFrom-Json).items).Count -eq 2000) "status $($items.Status)"
    $low = Invoke-Route 'GET' '/inventory/low-stock' $null
    Check 'GET /inventory/low-stock' ($low.Status -eq 200) "status $($low.Status), $($low.Body)"
    $popular = Invoke-Route 'GET' '/analytics/popular-items' $null
    Check 'GET /analytics/popular-items' ($popular.Status -eq 200) "status $($popular.Status)"
    $missing = Invoke-Route 'GET' '/transactions/quickstart-missing' $null
    Check 'GET /transactions/{unknown}' ($missing.Status -eq 404) "status $($missing.Status)"
    $start = Invoke-Route 'POST' '/transactions' '{"stationId":"quickstart-1"}'
    $id = ($start.Body | ConvertFrom-Json).transactionId
    Check 'POST /transactions' ($start.Status -eq 201 -and $id) "status $($start.Status)"
    $scan = Invoke-Route 'POST' "/transactions/$id/items" '{"sku":"SKU-000001"}'
    Check 'POST /transactions/{id}/items' ($scan.Status -eq 200) "status $($scan.Status)"
    $lookup = Invoke-Route 'GET' "/transactions/$id" $null
    Check 'GET /transactions/{id}' ($lookup.Status -eq 200) "status $($lookup.Status)"
    $complete = Invoke-Route 'POST' "/transactions/$id/complete" ''
    Check 'POST /transactions/{id}/complete' ($complete.Status -eq 200) "status $($complete.Status)"
    $deadline = (Get-Date).AddSeconds(30)
    do {
        $status = (Invoke-Route 'GET' '/notifications/ops/status' $null).Body | ConvertFrom-Json
        if ([long]$status.salesSelected -ge 1) { break }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    Check 'Notification selected the smoke sale' ([long]$status.salesSelected -eq 1) "salesSelected=$($status.salesSelected), notifications pending=$($status.notifications.pending)"
} catch {
    $failures.Add("error: $_")
    Write-Host "ERROR: $_"
} finally {
    foreach ($process in $script:running.Values) { Stop-ProcessTree $process.Id }
    Stop-Gateway $NginxBin $nginxPrefix
    Write-Host 'deployment stopped'
}
if ($failures.Count -eq 0) { Write-Host 'QUICKSTART SECTION 1: PASS' } else { Write-Host "QUICKSTART SECTION 1: FAIL ($($failures -join '; '))"; exit 1 }
