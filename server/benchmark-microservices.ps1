<#
.SYNOPSIS
    Benchmark harness for the microservices deployment (R21, R22;
    contracts/service-deployment.md "Benchmark harness and checkpoints").

.DESCRIPTION
    1. Checks that ports 8080-8083, 9092, and 9093 are free, and builds load-client/ if needed.
    2. Runs db/reset-all.ps1.
    3. Starts Inventory, then Transaction, then Analytics (unless -SkipAnalytics) with
       PERF_METRICS=true, then Nginx with deploy/nginx.conf unchanged.
    4. From load-client/, runs exactly `java -cp out Main` (default),
       `java -cp out Main --stations=100 --duration=120` (stress), or
       `java -cp out Main --duration=20` (smoke).
    5. Every 2 s samples undelivered rows, pending attempts, unpublished boundaries, and
       SHOW GLOBAL STATUS (Innodb_row_lock_waits, Innodb_row_lock_time, Com_commit, Innodb_log_waits).
    6. Waits for zero pending work.
    7. Runs db/validate-invariants.ps1.
    8. Records innodb_flush_log_at_trx_commit, sync_binlog, and log_bin.
    9. Stops every process it started.

    Output goes to load-client/reports/microservices-architecture/<label>/: the client report
    (moved byte-identical out of load-client/reports/), samples.csv, the service and gateway logs,
    the validator output, and evidence.json. evidence.json sets comparable=true only when all three
    services ran, the exact Load Client command of the default or stress workload was used, and the
    validator passed; only comparable runs may decide a CP3 variant or enter the final comparison.

    Service passwords come from TRANSACTION_DB_PASSWORD, INVENTORY_DB_PASSWORD, and
    ANALYTICS_DB_PASSWORD, or a secure prompt. -ExtraEnv adds environment variables to every
    service, for example @{ TRANSACTION_CACHE_MAX_ENTRIES = '0' } for a CP3 variant.

.EXAMPLE
    ./benchmark-microservices.ps1 -Workload default -OutputLabel cp1-default-1 -NginxBin C:\nginx\nginx.exe
#>
param(
    [ValidateSet('default', 'stress', 'smoke')][string]$Workload,
    [string]$OutputLabel,
    [string]$MySqlExe,
    [string]$NginxBin,
    [string]$JavaExe,
    [switch]$SkipAnalytics,
    [hashtable]$ExtraEnv = @{}
)

$ErrorActionPreference = 'Stop'
if (-not $Workload) { throw '-Workload default|stress|smoke is required' }
if (-not $OutputLabel -or $OutputLabel -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') {
    throw '-OutputLabel is required and may contain only letters, digits, dot, underscore, and hyphen'
}

$server = $PSScriptRoot
$repo = Split-Path -Parent $server
$clientDir = Join-Path $repo 'load-client'
$dbDir = Join-Path $repo 'db'
. (Join-Path $dbDir 'mysql-tools.ps1')

$out = Join-Path $repo "load-client/reports/microservices-architecture/$OutputLabel"
if ((Test-Path $out) -and (Get-ChildItem $out -Force | Select-Object -First 1)) {
    throw "Output directory already has evidence; choose a new label: $out"
}
New-Item -ItemType Directory -Force -Path $out | Out-Null

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
if (-not $JavaExe) {
    # Resolve the JDK's own java.exe: launcher shims such as Oracle's javapath start the JVM as a
    # child process, which stopping the shim would leave running.
    $launcher = (Get-Command java -ErrorAction Stop).Source
    # The settings print on stderr; Windows PowerShell 5.1 would turn that into a terminating error.
    $ErrorActionPreference = 'Continue'
    $javaHome = (& $launcher -XshowSettings:properties -version 2>&1 | ForEach-Object { "$_" } |
        Where-Object { $_ -match '^\s*java\.home = ' } | Select-Object -First 1) -replace '^\s*java\.home = ', ''
    $ErrorActionPreference = 'Stop'
    $JavaExe = $launcher
    if ($javaHome -and (Test-Path (Join-Path $javaHome.Trim() 'bin\java.exe'))) { $JavaExe = Join-Path $javaHome.Trim() 'bin\java.exe' }
}
if (-not $NginxBin) {
    $onPath = Get-Command nginx -ErrorAction SilentlyContinue
    if (-not $onPath) { throw 'Pass -NginxBin <nginx.exe>; nginx is not on PATH' }
    $NginxBin = $onPath.Source
}
foreach ($tool in @($JavaExe, $NginxBin)) { if (-not (Test-Path $tool)) { throw "Not found: $tool" } }

$clientArgs = switch ($Workload) {
    'default' { @('-cp', 'out', 'Main') }
    'stress'  { @('-cp', 'out', 'Main', '--stations=100', '--duration=120') }
    'smoke'   { @('-cp', 'out', 'Main', '--duration=20') }
}
$clientCommand = 'java ' + ($clientArgs -join ' ')
$exactCommand = $Workload -ne 'smoke'
$services = @('inventory', 'transaction')
if (-not $SkipAnalytics) { $services += 'analytics' }
$httpPorts = @{ transaction = 8081; inventory = 8082; analytics = 8083 }
$intervalSeconds = 10
if ($ExtraEnv.ContainsKey('PERF_METRICS_INTERVAL_SECONDS')) { $intervalSeconds = [int]$ExtraEnv['PERF_METRICS_INTERVAL_SECONDS'] }

# Credentials for every service, kept in this process so the services and resets inherit them.
$credentials = @{}
$savedEnv = @{}
function Set-ProcessEnv([string]$Name, [string]$Value) {
    if (-not $script:savedEnv.ContainsKey($Name)) {
        $script:savedEnv[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process')
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}
foreach ($service in $script:ServiceNames) {
    $credentials[$service] = Get-ServiceDbCredential -Service $service
    Set-ProcessEnv "$($service.ToUpperInvariant())_DB_PASSWORD" $credentials[$service].Password
}

$running = @{}
$nginxPrefix = $null
$samplesFile = Join-Path $out 'samples.csv'

function Get-Prop($Object, [string]$Name) {
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Test-Http([string]$Url, [int[]]$Accept) {
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
        return $Accept -contains [int]$response.StatusCode
    } catch {
        $response = $_.Exception.Response
        if ($null -ne $response) { return $Accept -contains [int]$response.StatusCode }
        return $false
    }
}

# Stops a process and every process it started, children first.
function Stop-ProcessTree([int]$ProcessId) {
    Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue |
        ForEach-Object { Stop-ProcessTree ([int]$_.ProcessId) }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Wait-Until([string]$What, [int]$Seconds, [scriptblock]$Condition) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        foreach ($name in @($script:running.Keys)) {
            $process = $script:running[$name]
            $process.Refresh()
            if ($process.HasExited) { throw "$name exited (code $($process.ExitCode)) while waiting for: $What" }
        }
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out after $Seconds s waiting for: $What"
}

function Start-BenchService([string]$Service) {
    $title = $Service.Substring(0, 1).ToUpperInvariant() + $Service.Substring(1)
    $cp = "out/$Service.jar;out/common.jar;out/contracts.jar;lib/grpc/*;lib/mysql-connector-j-9.0.0.jar"
    $process = Start-Process -FilePath $JavaExe -ArgumentList @('-cp', $cp, "$Service.${title}Main") `
        -WorkingDirectory $server -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out "$Service-stdout.log") `
        -RedirectStandardError (Join-Path $out "$Service-stderr.log")
    $null = $process.Handle
    $script:running[$Service] = $process
}

function Get-ServiceRow([string]$Service, [string]$Query) {
    return Get-MySqlRow -MySql $mysql -Credential $credentials[$Service] -Query $Query
}

function Get-Sample([string]$Phase) {
    $c = $credentials.transaction
    $lines = @(Invoke-MySql -MySql $mysql -User $c.User -Password $c.Password -Database $c.Database -Batch -Query (
        "SELECT (SELECT COUNT(*) FROM accepted_scan WHERE delivered_at IS NULL), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'); " +
        "SHOW GLOBAL STATUS WHERE Variable_name IN " +
        "('Innodb_row_lock_waits', 'Innodb_row_lock_time', 'Com_commit', 'Innodb_log_waits')"))
    $first = $lines[0] -split "`t"
    $status = @{}
    foreach ($line in ($lines | Select-Object -Skip 1)) {
        $fields = $line -split "`t"
        $status[$fields[0]] = [long]$fields[1]
    }
    $unpublished = ''
    if (-not $SkipAnalytics) {
        $unpublished = [long](Get-ServiceRow 'analytics' (
            "SELECT GREATEST(0, (CASE WHEN s.last_position < 1000 THEN 0 " +
            "ELSE 1 + FLOOR((s.last_position - 1000) / 500) END) - " +
            "(CASE WHEN c.last_window_end = 0 THEN 0 ELSE 1 + (c.last_window_end - 1000) DIV 500 END)) " +
            "FROM scan_sequence s, analytics_checkpoint c WHERE s.sequence_key = 1 AND c.checkpoint_key = 1"))[0]
    }
    $sample = [pscustomobject]@{
        phase = $Phase
        utc_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        undelivered = [long]$first[0]
        pending_attempts = [long]$first[1]
        unpublished_boundaries = $unpublished
        row_lock_waits = $status['Innodb_row_lock_waits']
        row_lock_time_ms = $status['Innodb_row_lock_time']
        commits = $status['Com_commit']
        log_waits = $status['Innodb_log_waits']
    }
    ($sample | ConvertTo-Csv -NoTypeInformation | Select-Object -Skip 1) | Add-Content -Encoding ASCII $samplesFile
    return $sample
}

function Test-PendingWorkZero($Sample) {
    if ($Sample.pending_attempts -ne 0) { return $false }
    if ($SkipAnalytics) { return $true }
    return $Sample.undelivered -eq 0 -and $Sample.unpublished_boundaries -eq 0
}

function Get-LastMetrics([string]$Service) {
    $file = Join-Path $out "$Service-stdout.log"
    if (-not (Test-Path $file)) { return $null }
    $line = Get-Content $file | Where-Object { $_.StartsWith('{"service":') } | Select-Object -Last 1
    if (-not $line) { return $null }
    return $line | ConvertFrom-Json
}

function Get-TimerP99($Metrics, [string]$Name) { return Get-Prop (Get-Prop (Get-Prop $Metrics 'timers') $Name) 'p99_ms' }
function Get-Counter($Metrics, [string]$Name) { return Get-Prop (Get-Prop $Metrics 'counters') $Name }
function Get-Gauge($Metrics, [string]$Name) { return Get-Prop (Get-Prop $Metrics 'gauges') $Name }

$result = [ordered]@{ clientOk = $false; drained = $false; validatorPassed = $false }
$client = $null
$samples = New-Object System.Collections.Generic.List[object]
$drainedAt = $null
$clientReportPath = $null
$validatorExit = $null
try {
    # 1. Ports and artifacts.
    $occupied = Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 9092, 9093 -State Listen -ErrorAction SilentlyContinue
    if ($occupied) { throw "Benchmark ports are in use: $(($occupied.LocalPort | Sort-Object -Unique) -join ', ')" }
    foreach ($artifact in @('out/common.jar', 'out/contracts.jar', 'out/transaction.jar', 'out/inventory.jar', 'out/analytics.jar', 'lib/grpc')) {
        if (-not (Test-Path (Join-Path $server $artifact))) { throw "Missing $artifact; run server/build.sh first" }
    }
    if (-not (Test-Path (Join-Path $clientDir 'out/Main.class'))) {
        $javac = Join-Path (Split-Path -Parent $JavaExe) 'javac.exe'
        if (-not (Test-Path $javac)) { $javac = 'javac' }
        New-Item -ItemType Directory -Force -Path (Join-Path $clientDir 'out') | Out-Null
        & $javac -d (Join-Path $clientDir 'out') (Get-ChildItem (Join-Path $clientDir 'src') -Filter *.java | ForEach-Object FullName)
        if ($LASTEXITCODE -ne 0) { throw 'Load client build failed' }
    }
    'phase,utc_ms,undelivered,pending_attempts,unpublished_boundaries,row_lock_waits,row_lock_time_ms,commits,log_waits' |
        Set-Content -Encoding ASCII $samplesFile

    # 2. Reset every schema with its own credentials.
    & (Join-Path $dbDir 'reset-all.ps1') -MySqlExe $mysql *> (Join-Path $out 'reset-all.txt')
    if ($LASTEXITCODE -ne 0) { throw "db/reset-all.ps1 failed; see $out/reset-all.txt" }

    # 3. Services, then the gateway.
    Set-ProcessEnv 'PERF_METRICS' 'true'
    foreach ($entry in $ExtraEnv.GetEnumerator()) { Set-ProcessEnv ([string]$entry.Key) ([string]$entry.Value) }
    Start-BenchService 'inventory'
    Wait-Until 'Inventory /items' 60 { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-BenchService 'transaction'
    Wait-Until 'Transaction HTTP' 60 { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    $catalogVersion = (Get-ServiceRow 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    Wait-Until 'Transaction product replica' 60 {
        (Get-ServiceRow 'transaction' "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalogVersion
    }
    if (-not $SkipAnalytics) {
        Start-BenchService 'analytics'
        Wait-Until 'Analytics popular items' 60 { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    }
    $nginxPrefix = Join-Path $env:TEMP ('cs6510-microservices-nginx-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path (Join-Path $nginxPrefix 'conf'), (Join-Path $nginxPrefix 'logs'), (Join-Path $nginxPrefix 'temp') | Out-Null
    Copy-Item (Join-Path $repo 'deploy/nginx.conf') (Join-Path $nginxPrefix 'conf/nginx.conf')
    # nginx -t reports on stderr, which Windows PowerShell 5.1 turns into errors when redirected.
    $nginxTest = Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$nginxPrefix/", '-c', 'conf/nginx.conf', '-t') `
        -WorkingDirectory $nginxPrefix -WindowStyle Hidden -Wait -PassThru `
        -RedirectStandardOutput (Join-Path $out 'nginx-config-test.out.txt') `
        -RedirectStandardError (Join-Path $out 'nginx-config-test.txt')
    if ($nginxTest.ExitCode -ne 0) { throw "Nginx configuration test failed; see $out/nginx-config-test.txt" }
    Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$nginxPrefix/", '-c', 'conf/nginx.conf') -WorkingDirectory $nginxPrefix -WindowStyle Hidden
    Wait-Until 'gateway /items' 30 { Test-Http 'http://127.0.0.1:8080/items' @(200) }

    # 4-5. The unchanged Load Client, sampled every 2 s.
    $reportsDir = Join-Path $clientDir 'reports'
    New-Item -ItemType Directory -Force -Path $reportsDir | Out-Null
    $before = @(Get-ChildItem $reportsDir -Filter 'report-*.json' | ForEach-Object Name)
    $samples.Add((Get-Sample 'start'))
    $client = Start-Process -FilePath $JavaExe -ArgumentList $clientArgs -WorkingDirectory $clientDir -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out 'client-stdout.txt') -RedirectStandardError (Join-Path $out 'client-stderr.txt')
    # Windows PowerShell 5.1 reports a null ExitCode unless the process handle is opened before exit.
    $null = $client.Handle
    while (-not $client.HasExited) {
        Start-Sleep -Seconds 2
        $samples.Add((Get-Sample 'load'))
        $client.Refresh()
    }
    $client.WaitForExit()
    $result.clientOk = $client.ExitCode -eq 0
    if (-not $result.clientOk) { Write-Host "Load client exited with code $($client.ExitCode)" -ForegroundColor Red }

    # 6. Drain.
    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $deadline) {
        $sample = Get-Sample 'drain'
        $samples.Add($sample)
        if (Test-PendingWorkZero $sample) { $result.drained = $true; $drainedAt = Get-Date; break }
        Start-Sleep -Seconds 2
    }
    $samples.Add((Get-Sample 'final'))
    if (-not $result.drained) { Write-Host 'Pending work did not drain within 180 s' -ForegroundColor Red }

    $newReports = @(Get-ChildItem $reportsDir -Filter 'report-*.json' | Where-Object { $before -notcontains $_.Name })
    if ($newReports.Count -eq 1) {
        $clientReportPath = Join-Path $out $newReports[0].Name
        Move-Item $newReports[0].FullName $clientReportPath
    } else {
        Write-Host "Expected one new client report, found $($newReports.Count)" -ForegroundColor Red
        $result.clientOk = $false
    }
    $report = $null
    if ($clientReportPath) { $report = Get-Content -Raw $clientReportPath | ConvertFrom-Json }

    # 7. Invariants.
    $validatorArgs = @{ MySqlExe = $mysql }
    if ($SkipAnalytics) { $validatorArgs.SkipAnalytics = $true }
    if ($report) { $validatorArgs.TotalTransactions = [long]$report.totalTransactions }
    & (Join-Path $dbDir 'validate-invariants.ps1') @validatorArgs *> (Join-Path $out 'validator.txt')
    $validatorExit = $LASTEXITCODE
    $result.validatorPassed = $validatorExit -eq 0

    # 8. Durability settings, as configured on the server.
    $durability = Get-ServiceRow 'transaction' 'SELECT @@GLOBAL.innodb_flush_log_at_trx_commit, @@GLOBAL.sync_binlog, @@GLOBAL.log_bin'

    # Each service prints a periodic summary; wait for one taken after the drain.
    if ($drainedAt) {
        $wait = [Math]::Max(0, $intervalSeconds + 2 - ((Get-Date) - $drainedAt).TotalSeconds)
        Start-Sleep -Seconds ([int][Math]::Ceiling($wait))
    }
} finally {
    # 9. Stop everything this run started.
    if ($client -and -not $client.HasExited) { Stop-ProcessTree $client.Id }
    foreach ($process in $running.Values) { Stop-ProcessTree $process.Id }
    if ($nginxPrefix) {
        try {
            Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$nginxPrefix/", '-c', 'conf/nginx.conf', '-s', 'stop') `
                -WorkingDirectory $nginxPrefix -WindowStyle Hidden -Wait | Out-Null
        } catch { }
        Get-CimInstance Win32_Process -Filter "name='nginx.exe'" | Where-Object { $_.CommandLine -like "*$nginxPrefix*" } |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        if (Test-Path (Join-Path $nginxPrefix 'logs')) {
            Copy-Item (Join-Path $nginxPrefix 'logs/*') $out -ErrorAction SilentlyContinue
        }
    }
    foreach ($entry in $savedEnv.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

# Evidence.
$metrics = [ordered]@{}
foreach ($service in $services) { $metrics[$service] = Get-LastMetrics $service }
$validatorText = ''
if (Test-Path (Join-Path $out 'validator.txt')) { $validatorText = Get-Content -Raw (Join-Path $out 'validator.txt') }
function Get-ValidatorCount([string]$Name) {
    if ($script:validatorText -match "(?m)^\s*$Name=(-?\d+)") { return [long]$Matches[1] }
    return $null
}
$operations = [ordered]@{}
if ($report) {
    foreach ($operation in $report.operations) {
        $operations[$operation.operation] = [ordered]@{
            successCount = $operation.successCount; errorCount = $operation.errorCount
            p95Ms = $operation.p95Ms; p99Ms = $operation.p99Ms
        }
    }
}
$first = $samples | Select-Object -First 1
$last = $samples | Select-Object -Last 1
function Get-Delta([string]$Field) {
    if ($null -eq $first -or $null -eq $last) { return $null }
    return $last.$Field - $first.$Field
}
function Get-Peak([string]$Field) {
    # Compare as text: in PowerShell 0 -ne '' is false, which would drop zero samples.
    $values = @($samples | Where-Object { "$($_.$Field)" -ne '' } | ForEach-Object { [long]$_.$Field })
    if ($values.Count -eq 0) { return $null }
    return ($values | Measure-Object -Maximum).Maximum
}
$poolEvidence = [ordered]@{}
foreach ($service in $services) {
    $poolEvidence[$service] = [ordered]@{
        borrowWaitP99Ms = Get-TimerP99 $metrics[$service] "$service.pool.borrowWait"
        holdTimeP99Ms = Get-TimerP99 $metrics[$service] "$service.pool.holdTime"
        peakOccupancy = Get-Gauge $metrics[$service] "$service.pool.occupancyHighWater"
    }
}
$jars = [ordered]@{}
foreach ($jar in @('transaction', 'inventory', 'analytics', 'common', 'contracts')) {
    $path = Join-Path $server "out/$jar.jar"
    if (Test-Path $path) { $jars["$jar.jar"] = (Get-FileHash -Algorithm SHA256 $path).Hash.ToLowerInvariant() }
}
$commit = (& git -C $repo rev-parse HEAD).Trim()
$dirty = [bool](& git -C $repo status --porcelain --untracked-files=no)
$comparable = (-not $SkipAnalytics) -and $exactCommand -and $result.clientOk -and $result.drained -and $result.validatorPassed
$evidence = [ordered]@{
    label = $OutputLabel
    workload = $Workload
    command = $clientCommand
    exactCommand = $exactCommand
    gitCommit = $commit
    gitDirty = $dirty
    jarSha256 = $jars
    extraEnv = $ExtraEnv
    skipAnalytics = [bool]$SkipAnalytics
    client = [ordered]@{
        report = if ($clientReportPath) { Split-Path -Leaf $clientReportPath } else { $null }
        reportSha256 = if ($clientReportPath) { (Get-FileHash -Algorithm SHA256 $clientReportPath).Hash.ToLowerInvariant() } else { $null }
        totalTransactions = Get-Prop $report 'totalTransactions'
        totalItemsScanned = Get-Prop $report 'totalItemsScanned'
        transactionsPerSecond = Get-Prop $report 'transactionsPerSecond'
        wallClockSeconds = Get-Prop $report 'wallClockSeconds'
        operations = $operations
    }
    durability = if ($durability) { [ordered]@{
        innodb_flush_log_at_trx_commit = $durability[0]; sync_binlog = $durability[1]; log_bin = $durability[2] } } else { $null }
    samples = [ordered]@{
        count = $samples.Count
        peakUndelivered = Get-Peak 'undelivered'
        peakPendingAttempts = Get-Peak 'pending_attempts'
        peakUnpublishedBoundaries = Get-Peak 'unpublished_boundaries'
        rowLockWaitsDelta = Get-Delta 'row_lock_waits'
        rowLockTimeMsDelta = Get-Delta 'row_lock_time_ms'
        commitsDelta = Get-Delta 'commits'
        logWaitsDelta = Get-Delta 'log_waits'
    }
    pools = $poolEvidence
    recovery = [ordered]@{
        scanQueries = Get-Counter $metrics.transaction 'transaction.recovery.scanQueries'
        scanRows = Get-Counter $metrics.transaction 'transaction.recovery.scanRows'
        waitDbChecks = Get-Counter $metrics.transaction 'transaction.wait.dbChecks'
    }
    locks = [ordered]@{
        waitP99Ms = Get-TimerP99 $metrics.transaction 'transaction.lock.wait'
        holdP99Ms = Get-TimerP99 $metrics.transaction 'transaction.lock.hold'
        contended = Get-Counter $metrics.transaction 'transaction.lock.contended'
        collisions = Get-Counter $metrics.transaction 'transaction.lock.collisions'
        inventoryApplyLockWaitP99Ms = Get-TimerP99 $metrics.inventory 'inventory.apply.lockWait'
    }
    idlePolls = [ordered]@{
        relay = Get-Counter $metrics.transaction 'transaction.relay.idlePolls'
        analyticsPoller = if ($metrics.Contains('analytics')) { Get-Counter $metrics.analytics 'analytics.poller.idlePolls' } else { $null }
    }
    validator = [ordered]@{ passed = $result.validatorPassed; exitCode = $validatorExit; output = 'validator.txt' }
    fr054 = [ordered]@{
        completedTransactions = Get-ValidatorCount 'completed_transactions'
        backgroundCompletions = Get-ValidatorCount 'background_completions'
        completionsWithoutResponse = Get-ValidatorCount 'completions_without_response'
        remainingDifference = Get-ValidatorCount 'remaining_difference'
    }
    serviceMetrics = $metrics
    clientOk = $result.clientOk
    drained = $result.drained
    comparable = $comparable
}
[IO.File]::WriteAllText((Join-Path $out 'evidence.json'), ($evidence | ConvertTo-Json -Depth 12), (New-Object System.Text.UTF8Encoding $false))

Write-Host ("{0}: totalTransactions={1} validator={2} drained={3} comparable={4} -> {5}" -f $OutputLabel,
    (Get-Prop $report 'totalTransactions'), $result.validatorPassed, $result.drained, $comparable, $out)
if ($result.clientOk -and $result.drained -and $result.validatorPassed) { exit 0 }
exit 1
