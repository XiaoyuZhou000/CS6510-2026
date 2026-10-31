# Shared deployment helpers for the phase 11 drivers, from the T139 scenario script.
$ErrorActionPreference = 'Stop'
$repo = 'E:\Wen\学习\NEU\CS6510\CS6510-2026'
$server = Join-Path $repo 'server'
$kafkaDir = Join-Path $repo 'deploy\kafka'
. (Join-Path $repo 'db\mysql-tools.ps1')
foreach ($name in 'TRANSACTION_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'ANALYTICS_DB_PASSWORD') {
    Set-Item "env:$name" ([Environment]::GetEnvironmentVariable($name, 'User'))
}
$env:KAFKA_HOME = 'C:\Users\18422\AppData\Local\Temp\cs6510-kafka\kafka_2.13-4.1.0'
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21'
$java = 'C:\Program Files\Java\jdk-21\bin\java.exe'
$nginx = 'C:\Users\18422\AppData\Local\Temp\cs6510-nginx\nginx-1.31.6\nginx.exe'
$mysql = Resolve-MySqlExe
if (-not $out) { throw 'set $out before dot-sourcing lib-deploy.ps1' }
New-Item -ItemType Directory -Force $out | Out-Null
$steps = Join-Path $out 'steps.md'
$cred = @{}
foreach ($s in 'transaction', 'inventory', 'analytics') { $cred[$s] = Get-ServiceDbCredential -Service $s }
$procs = @{}
$attempt = 0

function Note([string]$Text) { Add-Content -Path $steps -Value $Text -Encoding UTF8; Write-Host $Text }
# Always an array: Get-MySqlRow returns a bare string for one column, where [0] would be its first character.
function Q([string]$Service, [string]$Sql) { return ,[string[]](Get-MySqlRow -MySql $mysql -Credential $cred[$Service] -Query $Sql) }
function Stop-Tree([int]$ProcessId) {
    Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue |
        ForEach-Object { Stop-Tree ([int]$_.ProcessId) }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}
function Http([string]$Method, [string]$Path, [string]$Body, [int]$Port = 8080) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $status = 0; $content = ''
    try {
        $req = @{ Uri = "http://127.0.0.1:$Port$Path"; Method = $Method; UseBasicParsing = $true; TimeoutSec = 20 }
        if ($Body) { $req.Body = $Body; $req.ContentType = 'application/json' }
        $r = Invoke-WebRequest @req
        $status = [int]$r.StatusCode; $content = $r.Content
    } catch {
        $resp = $_.Exception.Response
        if ($null -ne $resp) {
            $status = [int]$resp.StatusCode
            $reader = New-Object IO.StreamReader($resp.GetResponseStream()); $content = $reader.ReadToEnd()
        } else { $content = $_.Exception.Message }
    }
    $watch.Stop()
    return [pscustomobject]@{ Status = $status; Ms = [int]$watch.ElapsedMilliseconds; Body = $content }
}
function WaitFor([string]$What, [int]$Seconds, [scriptblock]$Condition) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try { if (& $Condition) { return } } catch { }
        Start-Sleep -Milliseconds 250
    }
    throw "Timed out after $Seconds s: $What"
}
function Start-Svc([string]$Service) {
    $script:attempt++
    $title = $Service.Substring(0, 1).ToUpperInvariant() + $Service.Substring(1)
    $cp = "out/$Service.jar;out/common.jar;out/contracts.jar;lib/messaging/*;lib/mysql-connector-j-9.0.0.jar"
    $p = Start-Process -FilePath $java -ArgumentList @('-cp', $cp, "$Service.${title}Main") -WorkingDirectory $server `
        -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $out "$Service-$attempt-stdout.log") `
        -RedirectStandardError (Join-Path $out "$Service-$attempt-stderr.log")
    $script:procs[$Service] = $p
}
function Ready([string]$Service) {
    switch ($Service) {
        'inventory' { WaitFor 'Inventory /items' 60 { (Http GET '/items' $null 8082).Status -eq 200 } }
        'transaction' { WaitFor 'Transaction HTTP' 60 { (Http GET '/transactions/probe' $null 8081).Status -eq 404 } }
        'analytics' { WaitFor 'Analytics HTTP' 60 { (Http GET '/analytics/popular-items' $null 8083).Status -eq 200 } }
    }
}
function Json([string]$Text) { return ($Text | ConvertFrom-Json) }
function Units { return [long](Q 'inventory' 'SELECT SUM(stock_quantity) FROM inventory')[0] }
function Stock([string]$Sku) { return [long](Q 'inventory' "SELECT stock_quantity FROM inventory WHERE sku = '$Sku'")[0] }
function Scan([string]$Id, [string]$Sku) { return Http POST "/transactions/$Id/items" ('{"sku":"' + $Sku + '"}') }
function StartTx([string]$Station) { return Http POST '/transactions' ('{"stationId":"' + $Station + '"}') }
function Attempt([string]$Id) { return (Q 'transaction' "SELECT state, client_outcome FROM checkout_attempt WHERE transaction_id = '$Id' ORDER BY attempt_number DESC LIMIT 1") }
function Delivery {
    $t = Q 'transaction' 'SELECT COUNT(*), SUM(published_at IS NULL) FROM accepted_scan'
    $a = Q 'analytics' 'SELECT COUNT(*), COUNT(DISTINCT source_transaction_id, source_scan_sequence) FROM accepted_scan'
    return [pscustomobject]@{ Accepted = [long]$t[0]; Unpublished = [long]$t[1]; Stored = [long]$a[0]; Distinct = [long]$a[1] }
}
function Parked {
    $sum = 0
    foreach ($s in 'transaction', 'inventory', 'analytics') { $sum += [long](Q $s 'SELECT COUNT(*) FROM dead_letter_event')[0] }
    return $sum
}


function Start-Broker {
    & (Join-Path $kafkaDir 'start-broker.ps1') *> (Join-Path $out ('start-broker-' + (Get-Date -Format 'HHmmss') + '.txt'))
}
function Stop-Broker {
    & (Join-Path $kafkaDir 'stop-broker.ps1') *> (Join-Path $out ('stop-broker-' + (Get-Date -Format 'HHmmss') + '.txt'))
}
function Start-Deployment {
    $script:nginxPrefix = Join-Path $env:TEMP ('cs6510-p11-nginx-' + [Guid]::NewGuid().ToString('N'))
    $sw = [Diagnostics.Stopwatch]::StartNew()
    foreach ($s in 'inventory', 'transaction', 'analytics') { Start-Svc $s; Ready $s }
    WaitFor 'replica at the catalog version' 60 {
        (Q 'transaction' 'SELECT COALESCE(catalog_version, ''none'') FROM product_replica_meta WHERE meta_key = 1')[0] -eq
            (Q 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    }
    Note "Services ready and replica at Inventory's catalog_version after $([int]$sw.Elapsed.TotalSeconds) s from the first start"
    New-Item -ItemType Directory -Force -Path (Join-Path $nginxPrefix 'conf'), (Join-Path $nginxPrefix 'logs'), (Join-Path $nginxPrefix 'temp') | Out-Null
    Copy-Item (Join-Path $repo 'deploy\nginx.conf') (Join-Path $nginxPrefix 'conf\nginx.conf')
    $script:procs['nginx'] = Start-Process -FilePath $nginx -ArgumentList @('-p', "$nginxPrefix/", '-c', 'conf/nginx.conf') -WorkingDirectory $nginxPrefix -WindowStyle Hidden -PassThru
    WaitFor 'gateway' 30 { (Http GET '/items').Status -eq 200 }
}
function Stop-Deployment {
    if ($script:nginxPrefix -and (Test-Path $script:nginxPrefix)) {
        $ErrorActionPreference = 'Continue'
        & $nginx -p "$($script:nginxPrefix)/" -c conf/nginx.conf -s stop 2>&1 | Out-Null
        $ErrorActionPreference = 'Stop'
    }
    foreach ($k in @($script:procs.Keys)) { Stop-Tree $script:procs[$k].Id }
    $script:procs = @{}
    Start-Sleep -Seconds 2
}
function Reset-Baseline {
    $global:LASTEXITCODE = 0
    $file = Join-Path $out ('reset-baseline-' + (Get-Date -Format 'HHmmss') + '.txt')
    & (Join-Path $repo 'db\reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $env:KAFKA_HOME *> $file
    if ($LASTEXITCODE -ne 0) { throw "reset-baseline failed (exit $LASTEXITCODE), see $file" }
    return $file
}

$bash = 'C:\Program Files\Git\bin\bash.exe'
function Run-Bash([string]$Command, [string]$Log) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    Push-Location $server
    $unix = $server.Replace([char]92, [char]47)
    & $bash -lc "cd '$unix' && $Command" *> $Log
    $code = $LASTEXITCODE
    Pop-Location
    $ErrorActionPreference = $previous
    return $code
}
