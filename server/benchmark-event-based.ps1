<#
.SYNOPSIS
    Benchmark harness for the event-based deployment (research D13, D15, D16;
    contracts/broker-and-deployment.md "Benchmark harness").

.DESCRIPTION
    Replaces benchmark-microservices.ps1, keeping its steps and adding the broker:
    1. Checks that ports 8080-8083 (and the former gRPC port 9092, until T148) are free,
       that the service artifacts exist, and that the broker answers with its four topics; builds
       load-client/ if needed.
    2. Applies -ExtraEnv to this process, so the broker restarted by the reset also sees it (for
       example KAFKA_FLUSH_EVERY_MESSAGE), then runs db/reset-baseline.ps1.
    3. Records the broker: the Kafka version from KAFKA_HOME's libs/kafka_<scala>-<version>.jar, the
       flush setting (KAFKA_FLUSH_EVERY_MESSAGE and the effective server.properties), and the JDK
       running the broker JVM.
    4. Starts Inventory, then Transaction, then Analytics (unless -SkipAnalytics) with
       PERF_METRICS=true, retrying a start that fails with a BindException, then Nginx with
       deploy/nginx.conf unchanged. Readiness is each service's health, Transaction's product
       replica matching Inventory's catalog_version (replayed from the catalog topic, D9), and every
       partition of each group in $ConsumerGroups held by a live member (SC-009).
    5. From load-client/, runs exactly `java -cp out Main` (default),
       `java -cp out Main --stations=100 --duration=120` (stress), or
       `java -cp out Main --duration=20` (smoke).
    6. Every 2 s samples feature 005's values (unpublished scans, pending attempts, unpublished
       boundaries, SHOW GLOBAL STATUS) plus unpublished outbox rows and parked events per service,
       into samples.csv. Every 10 s it samples consumer lag per group with
       deploy/kafka/status.ps1 -Json in a background process (one call takes about 4 s), into
       lag-samples.csv.
    7. Waits up to 180 s for zero pending work: no pending attempt, no unpublished outbox row or
       scan, no unpublished boundary, and zero lag for every group (an absent group counts only
       while its topic is empty). Parked events are reported separately.
    8. Saves the final broker status as kafka-status-final.json and runs db/validate-invariants.ps1
       with it.
    9. Records innodb_flush_log_at_trx_commit, sync_binlog, and log_bin, then stops every process it
       started. The broker keeps running.

    Output goes to load-client/reports/event-based-architecture/<label>/: the client report (moved
    byte-identical out of load-client/reports/), samples.csv, lag-samples.csv, the reset, service,
    and gateway logs, the validator output, and evidence.json. evidence.json identifies the run,
    because the unchanged client report cannot (FR-062): the git commit, the broker version and
    flush setting, the broker's JDK, and every configuration variable set for the services. Its
    fr061 block holds the durable COMPLETED count, background completions, completions without a
    recorded response, the remaining difference from totalTransactions, parked events, and final
    consumer lag. comparable=true requires all three services, the exact default or stress
    command, a passing validator, a drained run, zero parked events, zero final lag, and, when the
    idle gate is on, an idle machine.

    Idle gate: with -IdleCpuCores or -BlockingProcess, the run first waits for an idle machine
    using benchmark-compare.ps1's gate, and afterwards measures the background CPU averaged over
    the run; a higher average, or a blocking process seen, makes the run not comparable.
    benchmark-compare.ps1 gates its runs itself and leaves these parameters unset.

    Service passwords come from TRANSACTION_DB_PASSWORD, INVENTORY_DB_PASSWORD, and
    ANALYTICS_DB_PASSWORD, or a secure prompt. The broker scripts need KAFKA_HOME (or -KafkaHome).
    -ExtraEnv adds environment variables to every service and to the broker restart, for example
    @{ CHECKOUT_CONSUMER_THREADS = '4' } or @{ KAFKA_FLUSH_EVERY_MESSAGE = 'true' } for a CP3 variant.

.EXAMPLE
    ./benchmark-event-based.ps1 -Workload default -OutputLabel cp1-default-1 -NginxBin C:\nginx\nginx.exe
    ./benchmark-event-based.ps1 -Workload stress -OutputLabel cp4-stress-1 -IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR
#>
param(
    [ValidateSet('default', 'stress', 'smoke')][string]$Workload,
    [string]$OutputLabel,
    [string]$MySqlExe,
    [string]$NginxBin,
    [string]$JavaExe,
    [string]$KafkaHome,
    [switch]$SkipAnalytics,
    [hashtable]$ExtraEnv = @{},
    [ValidateRange(0, 64)][double]$IdleCpuCores = 0,
    [string[]]$BlockingProcess = @()
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
$kafkaDir = Join-Path $repo 'deploy/kafka'
. (Join-Path $dbDir 'mysql-tools.ps1')
. (Join-Path $kafkaDir 'kafka-tools.ps1')

$out = Join-Path $repo "load-client/reports/event-based-architecture/$OutputLabel"
if ((Test-Path $out) -and (Get-ChildItem $out -Force | Select-Object -First 1)) {
    throw "Output directory already has evidence; choose a new label: $out"
}
New-Item -ItemType Directory -Force -Path $out | Out-Null

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$KafkaHome = Resolve-KafkaHome $KafkaHome
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
$intervalSeconds = 10
if ($ExtraEnv.ContainsKey('PERF_METRICS_INTERVAL_SECONDS')) { $intervalSeconds = [int]$ExtraEnv['PERF_METRICS_INTERVAL_SECONDS'] }
# Consumer groups whose partition assignment the readiness step waits for (T112, T126, SC-009); without
# Analytics its scan group has no member to wait for.
$ConsumerGroups = @('inventory-checkout', 'transaction-checkout-outcome')
if (-not $SkipAnalytics) { $ConsumerGroups += 'analytics-scan-ingest' }
# The topic each consumer group reads (contracts/event-catalog.md "Topics").
$groupTopics = [ordered]@{
    'inventory-checkout'           = 'selfcheckout.checkout.v1'
    'transaction-checkout-outcome' = 'selfcheckout.checkout-outcome.v1'
    'analytics-scan-ingest'        = 'selfcheckout.scan-accepted.v1'
}
$lagSampleSeconds = 10
$gateOn = $IdleCpuCores -gt 0 -or $BlockingProcess.Count -gt 0

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
Set-ProcessEnv 'KAFKA_HOME' $KafkaHome

$running = @{}
$nginxPrefix = $null
$samplesFile = Join-Path $out 'samples.csv'
$lagSamplesFile = Join-Path $out 'lag-samples.csv'
$statusScript = Join-Path $kafkaDir 'status.ps1'
$statusProbeFile = Join-Path $out 'kafka-status-probe.json'
$finalStatusFile = Join-Path $out 'kafka-status-final.json'
$statusProbe = $null
$nextProbeAt = Get-Date
$lagSamples = New-Object System.Collections.Generic.List[object]

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
    $cp = "out/$Service.jar;out/common.jar;out/contracts.jar"
    # The gRPC runtime stays on the classpath until T148 removes lib/grpc/.
    if (Test-Path (Join-Path $server 'lib/grpc')) { $cp += ';lib/grpc/*' }
    $cp += ';lib/messaging/*;lib/mysql-connector-j-9.0.0.jar'
    $process = Start-Process -FilePath $JavaExe -ArgumentList @('-cp', $cp, "$Service.${title}Main") `
        -WorkingDirectory $server -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out "$Service-stdout.log") `
        -RedirectStandardError (Join-Path $out "$Service-stderr.log")
    $null = $process.Handle
    $script:running[$Service] = $process
}

<#
    Starts a service and waits for its readiness condition. On this shared machine an outbound
    connection can briefly hold a service port, so a start that fails with a BindException is
    repeated (its logs are kept with an attempt suffix).
#>
function Start-ReadyService([string]$Service, [string]$What, [scriptblock]$Condition, [int]$Attempts = 3) {
    for ($attempt = 1; ; $attempt++) {
        Start-BenchService $Service
        try {
            Wait-Until $What 60 $Condition
            return
        } catch {
            $process = $script:running[$Service]
            $stderr = Join-Path $out "$Service-stderr.log"
            $bind = $process.HasExited -and (Test-Path $stderr) -and (Select-String -Path $stderr -Pattern 'BindException' -Quiet)
            if (-not $bind -or $attempt -ge $Attempts) { throw }
            $script:running.Remove($Service)
            foreach ($stream in @('stdout', 'stderr')) {
                $log = Join-Path $out "$Service-$stream.log"
                if (Test-Path $log) { Move-Item $log (Join-Path $out "$Service-$stream.attempt$attempt.log") }
            }
            Write-Host "$Service could not bind its port (attempt $attempt); retrying" -ForegroundColor Yellow
            Start-Sleep -Seconds 3
        }
    }
}

function Get-ServiceRow([string]$Service, [string]$Query) {
    return Get-MySqlRow -MySql $mysql -Credential $credentials[$Service] -Query $Query
}

function Get-Sample([string]$Phase) {
    $c = $credentials.transaction
    $lines = @(Invoke-MySql -MySql $mysql -User $c.User -Password $c.Password -Database $c.Database -Batch -Query (
        "SELECT (SELECT COUNT(*) FROM accepted_scan WHERE published_at IS NULL), " +
        "(SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'), " +
        "(SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL), " +
        "(SELECT COUNT(*) FROM dead_letter_event); " +
        "SHOW GLOBAL STATUS WHERE Variable_name IN " +
        "('Innodb_row_lock_waits', 'Innodb_row_lock_time', 'Com_commit', 'Innodb_log_waits')"))
    $first = $lines[0] -split "`t"
    $status = @{}
    foreach ($line in ($lines | Select-Object -Skip 1)) {
        $fields = $line -split "`t"
        $status[$fields[0]] = [long]$fields[1]
    }
    $inventory = Get-ServiceRow 'inventory' ("SELECT (SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL), " +
        "(SELECT COUNT(*) FROM dead_letter_event)")
    $unpublished = ''
    $parkedAnalytics = ''
    if (-not $SkipAnalytics) {
        $analytics = Get-ServiceRow 'analytics' (
            "SELECT GREATEST(0, (CASE WHEN s.last_position < 1000 THEN 0 " +
            "ELSE 1 + FLOOR((s.last_position - 1000) / 500) END) - " +
            "(CASE WHEN c.last_window_end = 0 THEN 0 ELSE 1 + (c.last_window_end - 1000) DIV 500 END)), " +
            "(SELECT COUNT(*) FROM dead_letter_event) " +
            "FROM scan_sequence s, analytics_checkpoint c WHERE s.sequence_key = 1 AND c.checkpoint_key = 1")
        $unpublished = [long]$analytics[0]
        $parkedAnalytics = [long]$analytics[1]
    }
    $sample = [pscustomobject][ordered]@{
        phase = $Phase
        utc_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        unpublished_scans = [long]$first[0]
        pending_attempts = [long]$first[1]
        unpublished_boundaries = $unpublished
        unpublished_outbox_transaction = [long]$first[2]
        unpublished_outbox_inventory = [long]$inventory[0]
        parked_transaction = [long]$first[3]
        parked_inventory = [long]$inventory[1]
        parked_analytics = $parkedAnalytics
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
    if ($Sample.unpublished_outbox_transaction -ne 0 -or $Sample.unpublished_outbox_inventory -ne 0) { return $false }
    if ($SkipAnalytics) { return $true }
    return $Sample.unpublished_scans -eq 0 -and $Sample.unpublished_boundaries -eq 0
}

function Get-ParkedTotal($Sample) {
    if ($null -eq $Sample) { return $null }
    $total = [long]$Sample.parked_transaction + [long]$Sample.parked_inventory
    if ("$($Sample.parked_analytics)" -ne '') { $total += [long]$Sample.parked_analytics }
    return $total
}

# Column names for lag-samples.csv: one state, lag, and uncommitted column per group, and one total
# end offset column per topic.
function Get-ColumnName([string]$Name) { return ($Name -replace '[^A-Za-z0-9]+', '_').Trim('_') }

function Get-LagSummary($Status) {
    $summary = [pscustomobject]@{ zero = $false; total = $null; uncertain = @() }
    if ($null -eq $Status -or -not $Status.reachable) { return $summary }
    $total = [long]0
    foreach ($entry in $script:groupTopics.GetEnumerator()) {
        if ($SkipAnalytics -and $entry.Key -eq 'analytics-scan-ingest') { continue }
        $group = @($Status.groups | Where-Object { $_.group -eq $entry.Key }) | Select-Object -First 1
        $topic = @($Status.topics | Where-Object { "$($_.name)".EndsWith($entry.Value) }) | Select-Object -First 1
        if ($null -eq $topic -or -not $topic.exists) { $summary.uncertain += "$($entry.Value) missing"; continue }
        if ($null -eq $group -or "$($group.state)" -eq 'absent') { $total += [long]$topic.totalEndOffset; continue }
        if ($null -ne $group.lag) { $total += [long]$group.lag }
        # A partition without a committed offset hides its records from the lag (see validate-invariants.ps1).
        $nonEmpty = @($topic.endOffsets | Where-Object { [long]$_ -gt 0 }).Count
        if ($nonEmpty -gt [int]$topic.partitions - [int]$group.uncommitted) { $summary.uncertain += "$($entry.Key) uncommitted partitions" }
    }
    $summary.total = $total
    $summary.zero = ($total -eq 0) -and ($summary.uncertain.Count -eq 0)
    return $summary
}

# True when every group in $ConsumerGroups has a live member on each partition of its topic (SC-009).
function Test-GroupsAssigned($Status) {
    if ($null -eq $Status -or -not $Status.reachable) { return $false }
    foreach ($name in $script:ConsumerGroups) {
        $group = @($Status.groups | Where-Object { $_.group -eq $name }) | Select-Object -First 1
        $topic = @($Status.topics | Where-Object { "$($_.name)".EndsWith($script:groupTopics[$name]) }) | Select-Object -First 1
        if ($null -eq $group -or $null -eq $topic -or -not $topic.exists) { return $false }
        if ([int](Get-Prop $group 'assigned') -lt [int]$topic.partitions) { return $false }
    }
    return $true
}

function Add-LagSample([string]$Phase, [datetime]$Started, [string]$Text) {
    $status = $null
    try { if ($Text) { $status = $Text | ConvertFrom-Json } } catch { $status = $null }
    $row = [ordered]@{
        phase = $Phase
        utc_ms = ([DateTimeOffset]$Started.ToUniversalTime()).ToUnixTimeMilliseconds()
        duration_ms = [long]((Get-Date) - $Started).TotalMilliseconds
        reachable = if ($null -eq $status) { 'error' } else { [bool]$status.reachable }
    }
    foreach ($name in $script:groupTopics.Keys) {
        $group = $null
        if ($status) { $group = @($status.groups | Where-Object { $_.group -eq $name }) | Select-Object -First 1 }
        $column = Get-ColumnName $name
        $row["${column}_state"] = Get-Prop $group 'state'
        $row["${column}_lag"] = Get-Prop $group 'lag'
        $row["${column}_uncommitted"] = Get-Prop $group 'uncommitted'
    }
    foreach ($topicName in (@($script:groupTopics.Values) + 'selfcheckout.catalog.v1')) {
        $topic = $null
        if ($status) { $topic = @($status.topics | Where-Object { "$($_.name)".EndsWith($topicName) }) | Select-Object -First 1 }
        $row["$(Get-ColumnName $topicName)_end"] = Get-Prop $topic 'totalEndOffset'
    }
    $sample = [pscustomobject]$row
    if (-not (Test-Path $script:lagSamplesFile)) {
        ($sample | ConvertTo-Csv -NoTypeInformation | Select-Object -First 1) | Set-Content -Encoding ASCII $script:lagSamplesFile
    }
    ($sample | ConvertTo-Csv -NoTypeInformation | Select-Object -Skip 1) | Add-Content -Encoding ASCII $script:lagSamplesFile
    $script:lagSamples.Add($sample)
    return $status
}

# Runs deploy/kafka/status.ps1 -Json in its own powershell.exe, so the 2 s samples go on meanwhile.
function Start-StatusProbe([string]$Phase) {
    # Start-Process joins arguments with spaces and does not quote them, so quote each one.
    $arguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $script:statusScript, '-Json', '-KafkaHome', $KafkaHome) |
        ForEach-Object { '"' + $_ + '"' }
    $process = Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList ($arguments -join ' ') `
        -WindowStyle Hidden -PassThru -RedirectStandardOutput $script:statusProbeFile `
        -RedirectStandardError (Join-Path $out 'kafka-status-probe-stderr.txt')
    $null = $process.Handle
    $script:statusProbe = [pscustomobject]@{ process = $process; phase = $Phase; started = Get-Date }
}

# Collects a finished probe; returns its parsed status, or $null if none finished.
function Receive-StatusProbe([switch]$Wait) {
    if ($null -eq $script:statusProbe) { return $null }
    $process = $script:statusProbe.process
    if ($Wait) { $process.WaitForExit() } else { $process.Refresh() }
    if (-not $process.HasExited) { return $null }
    $text = ''
    if (Test-Path $script:statusProbeFile) { $text = Get-Content -Raw $script:statusProbeFile }
    $probe = $script:statusProbe
    $script:statusProbe = $null
    $script:lastStatusText = $text
    return Add-LagSample $probe.phase $probe.started $text
}

# Called with every 2 s sample: collects a finished probe and starts the next one every 10 s.
function Update-StatusProbe([string]$Phase) {
    $null = Receive-StatusProbe
    if ($null -eq $script:statusProbe -and (Get-Date) -ge $script:nextProbeAt) {
        Start-StatusProbe $Phase
        $script:nextProbeAt = (Get-Date).AddSeconds($script:lagSampleSeconds)
    }
}

# One status call now, after any probe in flight.
function Get-StatusNow([string]$Phase) {
    $null = Receive-StatusProbe -Wait
    Start-StatusProbe $Phase
    $status = Receive-StatusProbe -Wait
    $script:nextProbeAt = (Get-Date).AddSeconds($script:lagSampleSeconds)
    return $status
}

function Get-BrokerEvidence {
    $jar = Get-ChildItem (Join-Path $KafkaHome 'libs') -Filter 'kafka_*.jar' -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^kafka_(\d+\.\d+)-(\d+\.\d+\.\d+)\.jar$' } | Select-Object -First 1
    $version = $null; $scala = $null
    if ($jar -and $jar.Name -match '^kafka_(\d+\.\d+)-(\d+\.\d+\.\d+)\.jar$') { $scala = $Matches[1]; $version = $Matches[2] }
    $dataDir = Resolve-KafkaDataDir -KafkaHome $KafkaHome
    $config = Join-Path $dataDir 'server.properties'
    $effectiveFlush = $null
    if (Test-Path $config) {
        $effectiveFlush = [bool](Get-Content $config | Where-Object { $_ -match '^\s*log\.flush\.interval\.messages\s*=\s*1\s*$' })
    }
    $process = Get-KafkaBrokerProcess
    $jdk = [ordered]@{ executable = $null; version = $null; vendor = $null }
    $heap = $null; $brokerPid = $null
    if ($process) {
        $brokerPid = $process.Id
        $cim = Get-CimInstance Win32_Process -Filter "ProcessId=$($process.Id)" -ErrorAction SilentlyContinue
        if ($cim) {
            $jdk.executable = $cim.ExecutablePath
            if ($cim.CommandLine -match '(-Xmx\S+)') { $heap = $Matches[1] }
        }
        if ($jdk.executable) {
            $release = Join-Path (Split-Path -Parent (Split-Path -Parent $jdk.executable)) 'release'
            if (Test-Path $release) {
                foreach ($line in (Get-Content $release)) {
                    if ($line -match '^JAVA_VERSION="?([^"]+)"?') { $jdk.version = $Matches[1] }
                    if ($line -match '^IMPLEMENTOR="?([^"]+)"?') { $jdk.vendor = $Matches[1] }
                }
            }
        }
    }
    return [ordered]@{
        version = $version
        scalaVersion = $scala
        jar = if ($jar) { $jar.Name } else { $null }
        kafkaHome = $KafkaHome
        dataDir = $dataDir
        bootstrap = $script:KafkaBootstrap
        flushEveryMessageEnv = [Environment]::GetEnvironmentVariable('KAFKA_FLUSH_EVERY_MESSAGE', 'Process')
        flushEveryMessage = $effectiveFlush
        pid = $brokerPid
        heap = $heap
        jdk = $jdk
    }
}

# Every configuration variable set for the services (and the broker restart), except passwords.
function Get-ConfigurationEnv {
    $pattern = '^((TRANSACTION|INVENTORY|ANALYTICS|CHECKOUT|OUTBOX|CONSUMER|SCAN|KAFKA|CATALOG)_|PERF_METRICS|STATUS_INTERVAL_SECONDS$|DB_(HOST|PORT|SERVER_PREPARED_STATEMENTS)$)'
    $variables = [Environment]::GetEnvironmentVariables('Process')
    $configuration = [ordered]@{}
    foreach ($name in ($variables.Keys | ForEach-Object { [string]$_ } | Sort-Object)) {
        if ($name -match $pattern -and $name -notmatch 'PASSWORD') { $configuration[$name] = [string]$variables[$name] }
    }
    return $configuration
}

<#
    benchmark-compare.ps1's idle-machine gate. Dot-sourcing that script binds its parameters as
    variables, so it is loaded inside this function's scope, never into the harness's.
#>
function Invoke-IdleGate([string]$Action, $Before, [datetime]$Started) {
    $cores = $IdleCpuCores
    if ($cores -le 0) { $cores = 64 }
    . (Join-Path $PSScriptRoot 'benchmark-compare.ps1') -IdleCpuCores $cores -BlockingProcess $BlockingProcess
    switch ($Action) {
        'wait' { Wait-IdleMachine (Join-Path $out 'idle-gate.log') }
        'snapshot' { return Get-BackgroundCpuSnapshot }
        'measure' { return Measure-BackgroundLoad $Before (Get-BackgroundCpuSnapshot) ((Get-Date) - $Started).TotalSeconds $BlockingProcess }
    }
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
$broker = $null
$brokerPidAtEnd = $null
$configuration = $null
$finalStatus = $null
$lastStatusText = ''
$idle = $null
$gateBefore = $null
$gateStarted = $null
try {
    if ($gateOn) {
        Invoke-IdleGate 'wait'
        $gateBefore = Invoke-IdleGate 'snapshot'
        $gateStarted = Get-Date
    }

    # 1. Ports, artifacts, and the broker. 9092 was Inventory's gRPC port, removed by T111; T148 drops the check.
    $occupied = Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 9092 -State Listen -ErrorAction SilentlyContinue
    if ($occupied) { throw "Benchmark ports are in use: $(($occupied.LocalPort | Sort-Object -Unique) -join ', ')" }
    foreach ($artifact in @('out/common.jar', 'out/contracts.jar', 'out/transaction.jar', 'out/inventory.jar', 'out/analytics.jar', 'lib/messaging')) {
        if (-not (Test-Path (Join-Path $server $artifact))) { throw "Missing $artifact; run server/build.sh first" }
    }
    if (-not (Test-KafkaBroker -KafkaHome $KafkaHome)) {
        throw "The broker does not answer on $($script:KafkaBootstrap); start it with deploy/kafka/start-broker.ps1"
    }
    $described = Get-KafkaTopics -KafkaHome $KafkaHome
    foreach ($spec in (Get-KafkaTopicSpecs)) {
        if (-not $described.ContainsKey($spec.Name)) { throw "Topic $($spec.Name) is missing; run deploy/kafka/create-topics.ps1" }
        if ($described[$spec.Name].Partitions -ne $spec.Partitions) {
            throw "Topic $($spec.Name) has $($described[$spec.Name].Partitions) partitions, expected $($spec.Partitions)"
        }
    }
    if (-not (Test-Path (Join-Path $clientDir 'out/Main.class'))) {
        $javac = Join-Path (Split-Path -Parent $JavaExe) 'javac.exe'
        if (-not (Test-Path $javac)) { $javac = 'javac' }
        New-Item -ItemType Directory -Force -Path (Join-Path $clientDir 'out') | Out-Null
        & $javac -d (Join-Path $clientDir 'out') (Get-ChildItem (Join-Path $clientDir 'src') -Filter *.java | ForEach-Object FullName)
        if ($LASTEXITCODE -ne 0) { throw 'Load client build failed' }
    }
    'phase,utc_ms,unpublished_scans,pending_attempts,unpublished_boundaries,unpublished_outbox_transaction,unpublished_outbox_inventory,parked_transaction,parked_inventory,parked_analytics,row_lock_waits,row_lock_time_ms,commits,log_waits' |
        Set-Content -Encoding ASCII $samplesFile

    # 2. The run's environment first, so the broker restarted by the reset sees it; then the baseline.
    Set-ProcessEnv 'PERF_METRICS' 'true'
    foreach ($entry in $ExtraEnv.GetEnumerator()) { Set-ProcessEnv ([string]$entry.Key) ([string]$entry.Value) }
    & (Join-Path $dbDir 'reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome *> (Join-Path $out 'reset-baseline.txt')
    if ($LASTEXITCODE -ne 0) { throw "db/reset-baseline.ps1 failed; see $out/reset-baseline.txt" }

    # 3. The broker as restarted by the reset, and the configuration the services get.
    $broker = Get-BrokerEvidence
    $configuration = Get-ConfigurationEnv

    # 4. Services, then the gateway.
    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    # The reset emptied Transaction's replica, so it matches only once the catalog events were replayed (D9).
    $catalogVersion = (Get-ServiceRow 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    Wait-Until 'Transaction product replica at the catalog version' 60 {
        (Get-ServiceRow 'transaction' "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalogVersion
    }
    if (-not $SkipAnalytics) {
        Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    }
    # After every consumer started, so analytics-scan-ingest has its member too (T126).
    Wait-Until "partition assignment of $($ConsumerGroups -join ', ')" 90 { Test-GroupsAssigned (Get-StatusNow 'ready') }
    $nginxPrefix = Join-Path $env:TEMP ('cs6510-event-based-nginx-' + [Guid]::NewGuid().ToString('N'))
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

    # 5-6. The unchanged Load Client, sampled every 2 s, with consumer lag every 10 s.
    $reportsDir = Join-Path $clientDir 'reports'
    New-Item -ItemType Directory -Force -Path $reportsDir | Out-Null
    $before = @(Get-ChildItem $reportsDir -Filter 'report-*.json' | ForEach-Object Name)
    $samples.Add((Get-Sample 'start'))
    $null = Get-StatusNow 'start'
    $client = Start-Process -FilePath $JavaExe -ArgumentList $clientArgs -WorkingDirectory $clientDir -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out 'client-stdout.txt') -RedirectStandardError (Join-Path $out 'client-stderr.txt')
    # Windows PowerShell 5.1 reports a null ExitCode unless the process handle is opened before exit.
    $null = $client.Handle
    while (-not $client.HasExited) {
        Start-Sleep -Seconds 2
        $samples.Add((Get-Sample 'load'))
        Update-StatusProbe 'load'
        $client.Refresh()
    }
    $client.WaitForExit()
    $result.clientOk = $client.ExitCode -eq 0
    if (-not $result.clientOk) { Write-Host "Load client exited with code $($client.ExitCode)" -ForegroundColor Red }

    # 7. Drain: the database counts first, then one status call to confirm zero lag.
    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $deadline) {
        $sample = Get-Sample 'drain'
        $samples.Add($sample)
        if (Test-PendingWorkZero $sample) {
            if ((Get-LagSummary (Get-StatusNow 'drain')).zero) { $result.drained = $true; $drainedAt = Get-Date; break }
        } else {
            Update-StatusProbe 'drain'
        }
        Start-Sleep -Seconds 2
    }
    $samples.Add((Get-Sample 'final'))
    $finalStatus = Get-StatusNow 'final'
    [IO.File]::WriteAllText($finalStatusFile, "$lastStatusText", (New-Object System.Text.UTF8Encoding $false))
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

    # 8. Invariants, including outboxes, parked events, and the final lag.
    $validatorArgs = @{ MySqlExe = $mysql }
    if ($SkipAnalytics) { $validatorArgs.SkipAnalytics = $true }
    if ($report) { $validatorArgs.TotalTransactions = [long]$report.totalTransactions }
    if ($finalStatus) { $validatorArgs.KafkaStatusJson = $finalStatusFile }
    & (Join-Path $dbDir 'validate-invariants.ps1') @validatorArgs *> (Join-Path $out 'validator.txt')
    $validatorExit = $LASTEXITCODE
    $result.validatorPassed = $validatorExit -eq 0

    # 9. Durability settings, as configured on the server, and whether the broker stayed up.
    $durability = Get-ServiceRow 'transaction' 'SELECT @@GLOBAL.innodb_flush_log_at_trx_commit, @@GLOBAL.sync_binlog, @@GLOBAL.log_bin'
    $endBroker = Get-KafkaBrokerProcess
    if ($endBroker) { $brokerPidAtEnd = $endBroker.Id }

    # Each service prints a periodic summary; wait for one taken after the drain.
    if ($drainedAt) {
        $wait = [Math]::Max(0, $intervalSeconds + 2 - ((Get-Date) - $drainedAt).TotalSeconds)
        Start-Sleep -Seconds ([int][Math]::Ceiling($wait))
    }
} finally {
    # Stop everything this run started; the broker keeps running.
    if ($statusProbe -and -not $statusProbe.process.HasExited) { Stop-ProcessTree $statusProbe.process.Id }
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
    if ($gateOn -and $gateBefore) {
        $load = Invoke-IdleGate 'measure' $gateBefore $gateStarted
        $idle = [ordered]@{
            idleCpuCores = $IdleCpuCores
            blockingProcesses = $BlockingProcess
            backgroundCpuCores = $load.cores
            blockingSeen = $load.blocking
            heaviest = $load.heaviest
            passed = ($IdleCpuCores -le 0 -or $load.cores -le $IdleCpuCores) -and $load.blocking.Count -eq 0
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
function Get-Peak($Rows, [string]$Field) {
    # Compare as text: in PowerShell 0 -ne '' is false, which would drop zero samples.
    $values = @($Rows | Where-Object { "$($_.$Field)" -ne '' } | ForEach-Object { [long]$_.$Field })
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
$peakLag = [ordered]@{}
foreach ($name in $groupTopics.Keys) { $peakLag[$name] = Get-Peak $lagSamples "$(Get-ColumnName $name)_lag" }
$jars = [ordered]@{}
foreach ($jar in @('transaction', 'inventory', 'analytics', 'common', 'contracts')) {
    $path = Join-Path $server "out/$jar.jar"
    if (Test-Path $path) { $jars["$jar.jar"] = (Get-FileHash -Algorithm SHA256 $path).Hash.ToLowerInvariant() }
}
$commit = (& git -C $repo rev-parse HEAD).Trim()
$dirty = [bool](& git -C $repo status --porcelain --untracked-files=no)
$parkedEvents = Get-ParkedTotal $last
$lagSummary = Get-LagSummary $finalStatus
$finalLag = $lagSummary.total
$brokerStayedUp = $null
if ($broker -and $broker.pid) { $brokerStayedUp = $brokerPidAtEnd -eq $broker.pid }
$comparable = (-not $SkipAnalytics) -and $exactCommand -and $result.clientOk -and $result.drained -and $result.validatorPassed -and
    ($parkedEvents -eq 0) -and ($null -ne $finalLag) -and ($finalLag -eq 0) -and $lagSummary.zero -and ($brokerStayedUp -eq $true)
if ($gateOn) { $comparable = $comparable -and $null -ne $idle -and $idle.passed }
$evidence = [ordered]@{
    harness = 'benchmark-event-based'
    label = $OutputLabel
    workload = $Workload
    command = $clientCommand
    exactCommand = $exactCommand
    gitCommit = $commit
    gitDirty = $dirty
    jarSha256 = $jars
    extraEnv = $ExtraEnv
    configuration = $configuration
    skipAnalytics = [bool]$SkipAnalytics
    broker = $broker
    brokerStayedUp = $brokerStayedUp
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
        peakUnpublishedScans = Get-Peak $samples 'unpublished_scans'
        peakPendingAttempts = Get-Peak $samples 'pending_attempts'
        peakUnpublishedBoundaries = Get-Peak $samples 'unpublished_boundaries'
        peakUnpublishedOutboxTransaction = Get-Peak $samples 'unpublished_outbox_transaction'
        peakUnpublishedOutboxInventory = Get-Peak $samples 'unpublished_outbox_inventory'
        rowLockWaitsDelta = Get-Delta 'row_lock_waits'
        rowLockTimeMsDelta = Get-Delta 'row_lock_time_ms'
        commitsDelta = Get-Delta 'commits'
        logWaitsDelta = Get-Delta 'log_waits'
    }
    lag = [ordered]@{
        sampleCount = $lagSamples.Count
        sampleSeconds = $lagSampleSeconds
        peakByGroup = $peakLag
        final = $finalLag
        finalZero = $lagSummary.zero
        finalUncertain = $lagSummary.uncertain
        finalStatus = if (Test-Path $finalStatusFile) { 'kafka-status-final.json' } else { $null }
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
    idleGate = $idle
    validator = [ordered]@{ passed = $result.validatorPassed; exitCode = $validatorExit; output = 'validator.txt' }
    fr061 = [ordered]@{
        completedTransactions = Get-ValidatorCount 'completed_transactions'
        backgroundCompletions = Get-ValidatorCount 'background_completions'
        completionsWithoutResponse = Get-ValidatorCount 'completions_without_response'
        remainingDifference = Get-ValidatorCount 'remaining_difference'
        parkedEvents = $parkedEvents
        finalConsumerLag = $finalLag
        comparable = $comparable
    }
    serviceMetrics = $metrics
    clientOk = $result.clientOk
    drained = $result.drained
    comparable = $comparable
}
[IO.File]::WriteAllText((Join-Path $out 'evidence.json'), ($evidence | ConvertTo-Json -Depth 12), (New-Object System.Text.UTF8Encoding $false))

Write-Host ("{0}: totalTransactions={1} validator={2} drained={3} parked={4} finalLag={5} comparable={6} -> {7}" -f $OutputLabel,
    (Get-Prop $report 'totalTransactions'), $result.validatorPassed, $result.drained, $parkedEvents, $finalLag, $comparable, $out)
if ($result.clientOk -and $result.drained -and $result.validatorPassed) { exit 0 }
exit 1
