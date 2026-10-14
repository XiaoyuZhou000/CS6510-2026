param(
  [Parameter(Mandatory=$true)][string]$MySqlExe,
  [Parameter(Mandatory=$true)][string]$NginxBin,
  [Parameter(Mandatory=$true)][string]$JavaExe
)
# Creates/resets only cs6510_materializer_bench, then runs the unchanged load client.
# Requires DB_USER and DB_PASSWORD in the environment and prebuilt service/client JARs.
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path '../').Path
$server = (Resolve-Path '.').Path
$client = Join-Path $repo 'load-client'
$results = Join-Path $repo 'load-client/reports/sql-materializer'
New-Item -ItemType Directory -Force -Path $results | Out-Null
$mysql = $MySqlExe
$nginx = $NginxBin
$java = $JavaExe
$benchDb = 'cs6510_materializer_bench'
$prefix = Join-Path $env:TEMP ('cs6510-materializer-nginx-' + $PID)
New-Item -ItemType Directory -Force -Path (Join-Path $prefix 'conf'),(Join-Path $prefix 'logs'),(Join-Path $prefix 'temp') | Out-Null
$config = (Get-Content (Join-Path $repo 'deploy/nginx.conf') -Raw).
    Replace('8081','18081').Replace('8082','18082').Replace('8083','18083').Replace('8080','18080')
[IO.File]::WriteAllText((Join-Path $prefix 'conf/nginx.conf'),$config)
$env:MYSQL_PWD = $env:DB_PASSWORD
$env:DB_HOST = '127.0.0.1'
$env:DB_PORT = '3307'
$env:DB_NAME = $benchDb
$env:DB_POOL_SIZE = '10'
$cp = 'out/transaction.jar;out/common.jar;lib/mysql-connector-j-9.0.0.jar'
$running = @()
function Sql([string]$query) {
  $out = & $mysql -h 127.0.0.1 -P 3307 -u $env:DB_USER -N -B -e $query $benchDb
  if ($LASTEXITCODE -ne 0) { throw "SQL query failed" }
  return $out
}
function Stop-Services {
  foreach ($p in $running) {
    if ($null -ne $p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
  }
  $script:running = @()
}
function Start-Services {
  $script:running = @()
  foreach ($domain in @('transaction','inventory','analytics')) {
    $port = switch ($domain) { 'transaction' {18081} 'inventory' {18082} 'analytics' {18083} }
    $cp = "out/$domain.jar;out/common.jar;lib/mysql-connector-j-9.0.0.jar"
    $proc = Start-Process -FilePath $java -ArgumentList @('-cp',$cp,("$domain." + (Get-Culture).TextInfo.ToTitleCase($domain) + 'Main'),$port) -WorkingDirectory $server -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $results "$domain-stdout.log") -RedirectStandardError (Join-Path $results "$domain-stderr.log")
    $script:running += $proc
  }
  for ($i=0; $i -lt 30; $i++) {
    try {
      $response = Invoke-WebRequest -Uri 'http://127.0.0.1:18080/items' -TimeoutSec 2
      if ($response.StatusCode -eq 200) { return }
    } catch {}
    Start-Sleep -Milliseconds 500
  }
  throw 'Services did not become ready'
}
function Reset-Bench {
  Stop-Services
  $init = (Get-Content (Join-Path $repo 'db/init.sql') -Raw).Replace('cs6510_selfcheckout',$benchDb)
  $init | & $mysql -h 127.0.0.1 -P 3307 -u $env:DB_USER | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Benchmark database init failed' }
}
function Sample([string]$phase) {
  $status = Sql "SELECT (SELECT COUNT(*) FROM accepted_scan),(SELECT last_position FROM scan_sequence WHERE sequence_key=1),(SELECT COUNT(*) FROM accepted_scan WHERE position IS NULL),(SELECT COALESCE(TIMESTAMPDIFF(MICROSECOND,MIN(accepted_at),NOW(3))/1000,0) FROM accepted_scan WHERE position IS NULL)"
  $locks = Sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'"
  $lockTime = Sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time'"
  "$phase,$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()),$($status -replace '\s+',','),$(($locks -split '\s+')[-1]),$(($lockTime -split '\s+')[-1])" | Add-Content (Join-Path $results 'samples.csv')
}
function Run-Phase([string]$phase,[int]$stations,[int]$seconds) {
  Write-Output "Starting $phase"
  Reset-Bench
  Start-Services
  Sample $phase
  $dir = Join-Path $results $phase
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  $args = @('-cp','out','Main',"--baseUrl=http://127.0.0.1:18080","--stations=$stations","--duration=$seconds","--reportDir=$dir")
  $clientProc = Start-Process -FilePath $java -ArgumentList $args -WorkingDirectory $client -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $results "$phase-client.txt") -RedirectStandardError (Join-Path $results "$phase-client-errors.txt")
  try {
    while (-not $clientProc.HasExited) {
      Start-Sleep -Seconds 2
      Sample $phase
      $clientProc.Refresh()
    }
    if ($clientProc.ExitCode -ne 0) { throw "Load client failed in $phase" }
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
      Sample $phase
      $pending = [int](Sql "SELECT COUNT(*) FROM accepted_scan WHERE position IS NULL")
      if ($pending -eq 0) { break }
      Start-Sleep -Seconds 2
    }
    Sample $phase
    $validation = (Get-Content (Join-Path $repo 'db/validate-invariants.sql') -Raw).Replace('cs6510_selfcheckout',$benchDb)
    $validation | & $mysql -h 127.0.0.1 -P 3307 -u $env:DB_USER > (Join-Path $results "$phase-invariants.txt")
    if ($LASTEXITCODE -ne 0) { throw "Invariant validation failed in $phase" }
    Write-Output "Completed $phase"
  } finally {
    if (-not $clientProc.HasExited) { Stop-Process -Id $clientProc.Id -Force }
    Stop-Services
  }
}
try {
  'phase,utc_ms,raw_events,materialized_events,pending_events,oldest_pending_ms,row_lock_waits,row_lock_time_ms' | Set-Content (Join-Path $results 'samples.csv')
  & $nginx -p "$prefix/" -c conf/nginx.conf -t
  if ($LASTEXITCODE -ne 0) { throw 'Nginx config invalid' }
  Start-Process -FilePath $nginx -ArgumentList @('-p',"$prefix/",'-c','conf/nginx.conf') -WorkingDirectory $prefix -WindowStyle Hidden
  Start-Sleep -Seconds 1
  Run-Phase 'default' 10 60
  Run-Phase 'stress' 100 120
} finally {
  Stop-Services
  & $nginx -p "$prefix/" -c conf/nginx.conf -s stop 2>$null
  Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
}

