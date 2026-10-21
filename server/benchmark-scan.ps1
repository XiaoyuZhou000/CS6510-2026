param(
  [Parameter(Mandatory=$true)][string]$MySqlExe,
  [Parameter(Mandatory=$true)][string]$NginxBin,
  [Parameter(Mandatory=$true)][string]$JavaExe,
  [Parameter(Mandatory=$true)][ValidateSet("baseline","retained")][string]$Variant,
  [Parameter(Mandatory=$true)][ValidateSet("default","stress")][string]$Workload,
  [ValidateRange(0,150)][int]$TransactionPoolSize = 0,
  [ValidateRange(0,150)][int]$TransactionPoolMaxSize = 0,
  [string]$OutputLabel = ""
)
# Creates/resets only cs6510_materializer_bench, then runs the unchanged load client.
# Requires DB_USER and DB_PASSWORD in the environment and prebuilt service/client JARs.
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path '../').Path
$server = (Resolve-Path '.').Path
$client = Join-Path $repo 'load-client'
$results = if ($OutputLabel) { Join-Path $repo "load-client/reports/transaction-optimization/$OutputLabel" } else { Join-Path $repo "load-client/reports/scan-optimization/$Variant" }
New-Item -ItemType Directory -Force -Path $results | Out-Null
$runResults = Join-Path $results $Workload
New-Item -ItemType Directory -Force -Path $runResults | Out-Null
$mysql = $MySqlExe
$nginx = $NginxBin
$java = $JavaExe
$benchDb = 'cs6510_materializer_bench'
$prefix = Join-Path $env:TEMP ('cs6510-materializer-nginx-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path (Join-Path $prefix 'conf'),(Join-Path $prefix 'logs'),(Join-Path $prefix 'temp') | Out-Null
$config = (Get-Content (Join-Path $repo 'deploy/nginx.conf') -Raw).
    Replace('8081','18081').Replace('8082','18082').Replace('8083','18083').Replace('8080','18080')
[IO.File]::WriteAllText((Join-Path $prefix 'conf/nginx.conf'),$config)
$env:MYSQL_PWD = $env:DB_PASSWORD
$env:DB_HOST = '127.0.0.1'
$env:DB_PORT = '3307'
$env:DB_NAME = $benchDb
$env:DB_POOL_SIZE = '10'
$env:SCAN_PROFILE = 'true'
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
    $env:DB_POOL_SIZE = if ($domain -eq 'transaction') { if ($TransactionPoolSize -gt 0) { [string]$TransactionPoolSize } elseif ($Variant -eq 'retained') { '32' } else { '10' } } else { '10' }
    if ($domain -eq 'transaction' -and $TransactionPoolMaxSize -gt 0) { $env:DB_POOL_MAX_SIZE = [string]$TransactionPoolMaxSize } else { Remove-Item Env:DB_POOL_MAX_SIZE -ErrorAction SilentlyContinue }
    $cp = "out/$domain.jar;out/common.jar;lib/mysql-connector-j-9.0.0.jar"
    $proc = Start-Process -FilePath $java -ArgumentList @('-cp',$cp,("$domain." + (Get-Culture).TextInfo.ToTitleCase($domain) + 'Main'),$port) -WorkingDirectory $server -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runResults "$domain-stdout.log") -RedirectStandardError (Join-Path $runResults "$domain-stderr.log")
    $script:running += $proc
  }
  Start-Sleep -Milliseconds 500
  for ($i=0; $i -lt 30; $i++) {
    foreach ($proc in $script:running) {
      $proc.Refresh()
      if ($proc.HasExited) { throw "Benchmark service exited during startup: PID $($proc.Id)" }
    }
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
  $other = Sql "SELECT COALESCE(MAX(CASE WHEN VARIABLE_NAME='Threads_running' THEN VARIABLE_VALUE END),0),COALESCE(MAX(CASE WHEN VARIABLE_NAME='Threads_connected' THEN VARIABLE_VALUE END),0),COALESCE(MAX(CASE WHEN VARIABLE_NAME='Com_commit' THEN VARIABLE_VALUE END),0),COALESCE(MAX(CASE WHEN VARIABLE_NAME='Innodb_os_log_fsyncs' THEN VARIABLE_VALUE END),0),COALESCE(MAX(CASE WHEN VARIABLE_NAME='Innodb_log_waits' THEN VARIABLE_VALUE END),0) FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Threads_running','Threads_connected','Com_commit','Innodb_os_log_fsyncs','Innodb_log_waits')"
  "$phase,$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()),$($status -replace '\s+',','),$(($locks -split '\s+')[-1]),$(($lockTime -split '\s+')[-1]),$($other -replace '\s+',',')" | Add-Content (Join-Path $runResults 'samples.csv')
}
function Run-Phase([string]$phase,[int]$stations,[int]$seconds) {
  Write-Output "Starting $phase"
  Reset-Bench
  Start-Services
  Sample $phase
  $dir = Join-Path $results $phase
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  $args = @('-cp','out','Main',"--baseUrl=http://127.0.0.1:18080","--stations=$stations","--duration=$seconds","--reportDir=$dir")
  $clientProc = Start-Process -FilePath $java -ArgumentList $args -WorkingDirectory $client -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runResults "$phase-client.txt") -RedirectStandardError (Join-Path $runResults "$phase-client-errors.txt")
  # Windows PowerShell 5.1 reports a null ExitCode unless the process handle is opened before exit.
  $null = $clientProc.Handle
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
    $validation | & $mysql -h 127.0.0.1 -P 3307 -u $env:DB_USER > (Join-Path $runResults "$phase-invariants.txt")
    if ($LASTEXITCODE -ne 0) { throw "Invariant validation failed in $phase" }
    Write-Output "Completed $phase"
  } finally {
    if (-not $clientProc.HasExited) { Stop-Process -Id $clientProc.Id -Force }
    Stop-Services
  }
}
try {
  $occupied = Get-NetTCPConnection -LocalPort 18080,18081,18082,18083 -State Listen -ErrorAction SilentlyContinue
  if ($occupied) { throw 'Disposable benchmark ports 18080-18083 are occupied' }
  'phase,utc_ms,raw_events,materialized_events,pending_events,oldest_pending_ms,row_lock_waits,row_lock_time_ms,threads_running,threads_connected,commits,log_fsyncs,log_waits' | Set-Content (Join-Path $runResults 'samples.csv')
  & $nginx -p "$prefix/" -c conf/nginx.conf -t
  if ($LASTEXITCODE -ne 0) { throw 'Nginx config invalid' }
  Start-Process -FilePath $nginx -ArgumentList @('-p',"$prefix/",'-c','conf/nginx.conf') -WorkingDirectory $prefix -WindowStyle Hidden
  Start-Sleep -Seconds 1
  if ($Workload -eq 'default') { Run-Phase 'default' 10 60 }
  else { Run-Phase 'stress' 100 120 }
} finally {
  Stop-Services
  & $nginx -p "$prefix/" -c conf/nginx.conf -s stop 2>$null
  Get-CimInstance Win32_Process -Filter "name='nginx.exe'" | Where-Object { $_.CommandLine -like "*$prefix*" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
  Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
  Remove-Item Env:SCAN_PROFILE -ErrorAction SilentlyContinue
  Remove-Item Env:DB_POOL_MAX_SIZE -ErrorAction SilentlyContinue
}











