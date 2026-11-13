<#
.SYNOPSIS
    Main load test with fixed subscriptions for the Notification Service (007 US3, FR-019-FR-026, SC-002, SC-007,
    contracts/deployment-and-evidence.md "Harness").

.DESCRIPTION
    1. Checks that ports 8080-8084 are free, the artifacts exist (server jars, subscriber-client/out, load-client/out),
       and the broker answers with its five topics; with -IdleCpuCores or -BlockingProcess, waits for an idle machine.
    2. Runs db/reset-baseline.ps1 -RunDir <run>/subscribers.
    3. Starts Inventory, Transaction, Analytics, and Notification (PERF_METRICS=true, BindException retries), waits for
       Transaction's product replica and Notification's catalog, then for every partition of the four consumer groups
       to have a live member, then starts Nginx with deploy/nginx.conf.
    4. Starts SubscriberMain (--subscribers, --seed, --skus-per-subscriber) with <run>/subscribers as its run
       directory, and waits for ready.json.
    5. (-Fault reject|stall arrives with T120; until then only -Fault none is accepted.)
       -Disruption injects one fault of quickstart scenario F (T103), -DisruptAfterSeconds into the load:
         notification-selection-crash  ops/faults notification.selection.afterCommit:halt, then restart Notification;
         notification-send-crash       ops/faults notification.delivery.afterSend:halt, then restart Notification;
         transaction-restart           stop Transaction (a forced kill, as on Windows every stop is) and start it;
         broker-outage                 deploy/kafka/stop-broker.ps1, -OutageSeconds, start-broker.ps1 -NoTopics;
         subscriber-outage             stop SubscriberMain (stop file), -OutageSeconds, restart it with --resume;
         malformed-sale                support.PublishRawRecord sends a malformed record to the sale topic.
       -Disruption notification-stopped (quickstart scenario G, T109) instead stops Notification once the subscribers
       are ready, runs the whole load without it, and starts it again when the Load Client exits.
       The Notification crashes set NOTIFICATION_TEST_FAULTS=1 for the run. Each step is logged in run.json.
    6. Runs the unchanged Load Client, `java -cp out Main` (default) or `java -cp out Main --stations=100
       --duration=120` (stress), sampling every 2 s into backlog.tsv: Transaction's unpublished SaleCompleted rows
       and pending checkout attempts, Notification's sale-consumer lag and pending notifications (ops/status), and the
       subscribers' channel lag (status.json).
    7. Waits at most -DrainSeconds for the drain (FR-025): two consecutive samples with all of them zero.
    8. Saves the broker status, stops the subscriber application (stop file), the services, and Nginx, then runs
       db/validate-invariants.ps1, db/export-notification-evidence.ps1, and ReconcileMain --mode=fixed.
    9. Writes run.json, reconciliation.json, notification-report.json and .md, and moves the Load Client's report,
       byte-identical, to load-client-report.json.

    Everything goes to load-client/reports/notification-service/<RunName>/. Service passwords come from the four
    *_DB_PASSWORD user environment variables (or a secure prompt) and are never printed. The broker keeps running.

.EXAMPLE
    ./benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default -RunName d-default `
        -NginxBin C:\Users\18422\cs6510-tools\nginx-1.31.6\nginx.exe -IdleCpuCores 2.0 `
        -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights
#>
param(
    [ValidateRange(1, 9999)][int]$Subscribers = 100,
    [string]$Seed,
    [ValidateRange(1, 2000)][int]$SkusPerSubscriber = 20,
    [ValidateSet('default', 'stress')][string]$Workload = 'default',
    [ValidateSet('none', 'reject', 'stall')][string]$Fault = 'none',
    [string]$FaultSubscriber = 'sub-0001',
    [string]$RunName,
    [ValidateRange(10, 3600)][int]$DrainSeconds = 120,
    [string]$MySqlExe,
    [string]$NginxBin,
    [string]$JavaExe,
    [string]$KafkaHome,
    [hashtable]$ExtraEnv = @{},
    [ValidateRange(0, 64)][double]$IdleCpuCores = 0,
    [string[]]$BlockingProcess = @(),
    [ValidateSet('none', 'notification-selection-crash', 'notification-send-crash', 'transaction-restart',
        'broker-outage', 'subscriber-outage', 'malformed-sale', 'notification-stopped')][string]$Disruption = 'none',
    [ValidateRange(1, 600)][int]$DisruptAfterSeconds = 20,
    [ValidateRange(1, 600)][int]$OutageSeconds = 30
)

$ErrorActionPreference = 'Stop'
if ($Fault -ne 'none') { throw "-Fault $Fault is not implemented yet (T120); use -Fault none" }
# Nested names such as scaling/10 or isolation/reject-1 follow the evidence layout.
if (-not $RunName -or $RunName -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$' -or $RunName -match '(^|/)\.\.?(/|$)') {
    throw '-RunName is required: folder names of letters, digits, dot, underscore, and hyphen, separated by /'
}
if ($Seed -and $Seed -notmatch '^-?\d+$') { throw '-Seed must be an integer' }

$server = $PSScriptRoot
$repo = Split-Path -Parent $server
$clientDir = Join-Path $repo 'load-client'
$subscriberClient = Join-Path $repo 'subscriber-client'
$dbDir = Join-Path $repo 'db'
$kafkaDir = Join-Path $repo 'deploy/kafka'
. (Join-Path $dbDir 'mysql-tools.ps1')
. (Join-Path $kafkaDir 'kafka-tools.ps1')
. (Join-Path $server 'benchmark-common.ps1')

$out = Join-Path $repo "load-client/reports/notification-service/$RunName"
if ((Test-Path $out) -and (Get-ChildItem $out -Force | Select-Object -First 1)) {
    throw "Run directory already has evidence; choose a new -RunName: $out"
}
New-Item -ItemType Directory -Force -Path $out | Out-Null
$subscriberDir = Join-Path $out 'subscribers'

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
$KafkaHome = Resolve-KafkaHome $KafkaHome
$JavaExe = Resolve-BenchJava $JavaExe
if (-not $NginxBin) {
    $onPath = Get-Command nginx -ErrorAction SilentlyContinue
    if (-not $onPath) { throw 'Pass -NginxBin <nginx.exe>; nginx is not on PATH' }
    $NginxBin = $onPath.Source
}
foreach ($tool in @($JavaExe, $NginxBin)) { if (-not (Test-Path $tool)) { throw "Not found: $tool" } }

$stations = 10; $durationSeconds = 60
$clientArgs = @('-cp', 'out', 'Main')
if ($Workload -eq 'stress') { $stations = 100; $durationSeconds = 120; $clientArgs += @('--stations=100', '--duration=120') }
$clientCommand = 'java ' + ($clientArgs -join ' ')
$services = @('inventory', 'transaction', 'analytics', 'notification')
# Each consumer group and the topic it reads; readiness waits until every partition has a live member.
$groupTopics = [ordered]@{
    'inventory-checkout'           = 'selfcheckout.checkout.v1'
    'transaction-checkout-outcome' = 'selfcheckout.checkout-outcome.v1'
    'analytics-scan-ingest'        = 'selfcheckout.scan-accepted.v1'
    'notification-sale-completed'  = 'selfcheckout.sale-completed.v1'
}
$gateOn = $IdleCpuCores -gt 0 -or $BlockingProcess.Count -gt 0
$startedAt = [DateTimeOffset]::UtcNow

$credentials = @{}
$savedEnv = @{}
foreach ($service in $script:ServiceNames) {
    $credentials[$service] = Get-ServiceDbCredential -Service $service
    Set-ProcessEnv "$($service.ToUpperInvariant())_DB_PASSWORD" $credentials[$service].Password
}
Set-ProcessEnv 'KAFKA_HOME' $KafkaHome

$running = @{}
$nginxPrefix = $null
$subscriber = $null
$client = $null
$backlogFile = Join-Path $out 'backlog.tsv'
$classpath = @((Join-Path $subscriberClient 'out'), (Join-Path $server 'out/contracts.jar'),
    (Join-Path $server 'lib/messaging/*')) -join ';'

function Get-ServiceRow([string]$Service, [string]$Query) {
    return Get-MySqlRow -MySql $mysql -Credential $credentials[$Service] -Query $Query
}

function Get-OpsStatus {
    try { return Invoke-RestMethod -Uri 'http://127.0.0.1:8084/notifications/ops/status' -TimeoutSec 3 } catch { return $null }
}

$disruptionEvents = New-Object System.Collections.Generic.List[object]
function Add-DisruptionEvent([string]$Step) {
    $disruptionEvents.Add([ordered]@{ utcMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds(); step = $Step })
    Write-Host "[disruption] $Step"
}

# Keeps a stopped process's logs under a numbered name, so the restarted process starts fresh ones.
function Save-ServiceLogs([string]$Service, [string]$Suffix) {
    foreach ($stream in @('stdout', 'stderr')) {
        $log = Join-Path $out "$Service-$stream.log"
        if (Test-Path $log) { Move-Item $log (Join-Path $out "$Service-$stream.$Suffix.log") }
    }
}

function Start-SubscriberApp([string[]]$Arguments, [string]$LogSuffix) {
    $process = Start-Process -FilePath $JavaExe -ArgumentList (($Arguments | ForEach-Object { '"' + $_ + '"' }) -join ' ') `
        -WorkingDirectory $out -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out "subscriber-stdout$LogSuffix.log") `
        -RedirectStandardError (Join-Path $out "subscriber-stderr$LogSuffix.log")
    $null = $process.Handle
    return $process
}

# One scenario F fault (T103), injected during the load and recovered from before it returns.
function Invoke-Disruption([string]$Kind) {
    switch ($Kind) {
        { $_ -in @('notification-selection-crash', 'notification-send-crash') } {
            $hook = if ($Kind -eq 'notification-selection-crash') { 'notification.selection.afterCommit' } else { 'notification.delivery.afterSend' }
            $process = $script:running['notification']
            $body = @{ faults = "${hook}:halt" } | ConvertTo-Json -Compress
            # Under load the halt can fire before Notification answers this request; the wait below proves it fired.
            try {
                $null = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8084/notifications/ops/faults' -ContentType 'application/json' -Body $body -TimeoutSec 5
            } catch [System.Net.WebException] {
                Write-Host "[disruption] ops/faults reply lost ($($_.Exception.Status)); waiting for the halt"
            }
            Add-DisruptionEvent "fault ${hook}:halt set on Notification (pid $($process.Id))"
            # Not Wait-Until, which fails as soon as a service it watches has exited.
            $haltDeadline = (Get-Date).AddSeconds(60)
            while (-not $process.HasExited -and (Get-Date) -lt $haltDeadline) {
                Start-Sleep -Milliseconds 200
                $process.Refresh()
            }
            if (-not $process.HasExited) { throw "Notification did not halt at $hook within 60 s" }
            $process.WaitForExit()
            Add-DisruptionEvent "Notification exited with code $($process.ExitCode)"
            $script:running.Remove('notification')
            Save-ServiceLogs 'notification' 'crash1'
            Start-ReadyService 'notification' 'Notification catalog loaded' {
                $ops = Get-OpsStatus
                $null -ne $ops -and $ops.catalogLoaded -eq $true
            }
            Add-DisruptionEvent "Notification restarted without the fault (pid $($script:running['notification'].Id))"
        }
        'transaction-restart' {
            $process = $script:running['transaction']
            Stop-ProcessTree $process.Id
            $process.WaitForExit()
            Add-DisruptionEvent "Transaction stopped (pid $($process.Id))"
            $script:running.Remove('transaction')
            Save-ServiceLogs 'transaction' 'restart1'
            Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
            Add-DisruptionEvent "Transaction restarted (pid $($script:running['transaction'].Id))"
        }
        'broker-outage' {
            & (Join-Path $kafkaDir 'stop-broker.ps1') *> (Join-Path $out 'broker-stop.txt')
            Add-DisruptionEvent 'broker stopped'
            Start-Sleep -Seconds $OutageSeconds
            & (Join-Path $kafkaDir 'start-broker.ps1') -KafkaHome $KafkaHome -NoTopics *> (Join-Path $out 'broker-start.txt')
            if ($LASTEXITCODE -ne 0) { throw "start-broker.ps1 failed; see $out/broker-start.txt" }
            Add-DisruptionEvent "broker started after $OutageSeconds s"
        }
        'subscriber-outage' {
            $process = $script:subscriber
            New-Item -ItemType File -Force -Path (Join-Path $subscriberDir 'stop') | Out-Null
            if (-not $process.WaitForExit(30000)) { throw 'SubscriberMain did not stop within 30 s' }
            $process.WaitForExit()
            $script:running.Remove('subscriber')
            Add-DisruptionEvent "SubscriberMain stopped with code $($process.ExitCode)"
            Start-Sleep -Seconds $OutageSeconds
            Remove-Item (Join-Path $subscriberDir 'stop') -Force -ErrorAction SilentlyContinue
            $resumed = Start-SubscriberApp ($subscriberArgs + '--resume') '.resume1'
            $script:subscriber = $resumed
            $script:running['subscriber'] = $resumed
            Wait-Until 'SubscriberMain resumed' 60 {
                (Test-Path (Join-Path $out 'subscriber-stdout.resume1.log')) -and
                (Select-String -Path (Join-Path $out 'subscriber-stdout.resume1.log') -Pattern 'resumed' -Quiet)
            }
            Add-DisruptionEvent "SubscriberMain resumed after $OutageSeconds s (pid $($resumed.Id))"
        }
        'malformed-sale' {
            $prefix = [Environment]::GetEnvironmentVariable('KAFKA_TOPIC_PREFIX', 'Process')
            $topic = "${prefix}selfcheckout.sale-completed.v1"
            $publishArgs = @('-cp', ((Join-Path $server 'out/test'), (Join-Path $server 'out/common.jar'),
                (Join-Path $server 'lib/messaging/*') -join ';'), 'support.PublishRawRecord', $topic, 'scenario-f-malformed',
                'malformed SaleCompleted of quickstart scenario F item 6') | ForEach-Object { '"' + $_ + '"' }
            $publish = Start-Process -FilePath $JavaExe -ArgumentList ($publishArgs -join ' ') -WorkingDirectory $server `
                -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput (Join-Path $out 'publish-raw.txt') `
                -RedirectStandardError (Join-Path $out 'publish-raw-stderr.txt')
            if ($publish.ExitCode -ne 0) { throw "PublishRawRecord failed; see $out/publish-raw-stderr.txt" }
            Add-DisruptionEvent "malformed record published to $topic with key scenario-f-malformed"
        }
    }
}

function Get-SubscriberStatus {
    $file = Join-Path $subscriberDir 'status.json'
    if (-not (Test-Path $file)) { return $null }
    try { return Get-Content -Raw $file | ConvertFrom-Json } catch { return $null }
}

# True when every partition of each consumer group's topic is held by a live member.
function Test-GroupsAssigned {
    try {
        $lags = Get-KafkaGroupLags -Groups @($groupTopics.Keys) -KafkaHome $KafkaHome
        $topics = Get-KafkaTopics -KafkaHome $KafkaHome
    } catch { return $false }
    foreach ($entry in $groupTopics.GetEnumerator()) {
        $group = $lags[$entry.Key]
        $topic = $topics[$entry.Value]
        if ($null -eq $group -or $null -eq $topic -or $group.State -eq 'absent') { return $false }
        if ([int]$group.Assigned -lt [int]$topic.Partitions) { return $false }
    }
    return $true
}

# One backlog sample (research N11 "Drain"); returns it with drained = all five values known and zero.
function Add-BacklogSample([string]$Phase) {
    $row = Get-ServiceRow 'transaction' ("SELECT (SELECT COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted' " +
        "AND published_at IS NULL), (SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING')")
    $ops = Get-OpsStatus
    $status = Get-SubscriberStatus
    $sample = [ordered]@{
        utcMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        phase = $Phase
        saleEventsUnpublished = [long]$row[0]
        saleConsumerLag = if ($ops) { [long]$ops.saleConsumerLag } else { '' }
        notificationsPending = if ($ops) { [long]$ops.notifications.pending } else { '' }
        channelLag = if ($status -and $status.totals.lagKnown) { [long]$status.totals.lag } else { '' }
        pendingAttempts = [long]$row[1]
    }
    $line = ($sample.Values | ForEach-Object { "$_" }) -join "`t"
    [IO.File]::AppendAllText($backlogFile, $line + "`n", (New-Object System.Text.UTF8Encoding $false))
    $zero = $true
    foreach ($field in @('saleEventsUnpublished', 'saleConsumerLag', 'notificationsPending', 'channelLag', 'pendingAttempts')) {
        if ("$($sample[$field])" -ne '0') { $zero = $false }
    }
    $sample['drained'] = $zero
    return $sample
}

$result = [ordered]@{ clientOk = $false; subscriberReady = $false; drained = $false; validatorPassed = $false
    exported = $false; reconciliationPassed = $false }
$drainElapsedSeconds = $null
$clientReportPath = $null
$report = $null
$idle = $null
$gateBefore = $null
$gateStarted = $null
$subscriberExit = $null
$validatorExit = $null
$reconcileExit = $null
try {
    if ($gateOn) {
        Invoke-IdleGate 'wait'
        $gateBefore = Invoke-IdleGate 'snapshot'
        $gateStarted = Get-Date
    }

    # 1. Ports, artifacts, and the broker.
    $occupied = Get-NetTCPConnection -LocalPort 8080, 8081, 8082, 8083, 8084 -State Listen -ErrorAction SilentlyContinue
    if ($occupied) { throw "Benchmark ports are in use: $(($occupied.LocalPort | Sort-Object -Unique) -join ', ')" }
    foreach ($artifact in @('out/common.jar', 'out/contracts.jar', 'out/transaction.jar', 'out/inventory.jar',
            'out/analytics.jar', 'out/notification.jar', 'lib/messaging')) {
        if (-not (Test-Path (Join-Path $server $artifact))) { throw "Missing $artifact; run server/build.sh all first" }
    }
    foreach ($main in @('SubscriberMain', 'ReconcileMain', 'DecodeSaleEvents')) {
        if (-not (Test-Path (Join-Path $subscriberClient "out/$main.class"))) { throw 'subscriber-client is not built; run subscriber-client/build.sh' }
    }
    if (-not (Test-Path (Join-Path $clientDir 'out/Main.class'))) { throw 'load-client is not built; run load-client/build.sh' }
    if ($Disruption -eq 'malformed-sale' -and -not (Test-Path (Join-Path $server 'out/test/support/PublishRawRecord.class'))) {
        throw '-Disruption malformed-sale needs support.PublishRawRecord; run server/build-tests.sh'
    }
    if (-not (Test-KafkaBroker -KafkaHome $KafkaHome)) {
        throw "The broker does not answer on $($script:KafkaBootstrap); start it with deploy/kafka/start-broker.ps1"
    }
    $described = Get-KafkaTopics -KafkaHome $KafkaHome
    foreach ($spec in (Get-KafkaTopicSpecs)) {
        if (-not $described.ContainsKey($spec.Name)) { throw "Topic $($spec.Name) is missing; run deploy/kafka/create-topics.ps1" }
    }

    # 2. The run's environment, then the baseline (which also removes the subscriber directory).
    Set-ProcessEnv 'PERF_METRICS' 'true'
    foreach ($entry in $ExtraEnv.GetEnumerator()) { Set-ProcessEnv ([string]$entry.Key) ([string]$entry.Value) }
    if ($Disruption -like 'notification-*') { Set-ProcessEnv 'NOTIFICATION_TEST_FAULTS' '1' }
    & (Join-Path $dbDir 'reset-baseline.ps1') -MySqlExe $mysql -KafkaHome $KafkaHome -RunDir $subscriberDir *> (Join-Path $out 'reset-baseline.txt')
    if ($LASTEXITCODE -ne 0) { throw "db/reset-baseline.ps1 failed; see $out/reset-baseline.txt" }

    # 3. Services, consumer groups, then the gateway.
    Start-ReadyService 'inventory' 'Inventory /items' { Test-Http 'http://127.0.0.1:8082/items' @(200) }
    Start-ReadyService 'transaction' 'Transaction HTTP' { Test-Http 'http://127.0.0.1:8081/transactions/readiness-probe' @(404) }
    $catalogVersion = (Get-ServiceRow 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
    Wait-Until 'Transaction product replica at the catalog version' 60 {
        (Get-ServiceRow 'transaction' "SELECT COALESCE(catalog_version, '') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalogVersion
    }
    Start-ReadyService 'analytics' 'Analytics popular items' { Test-Http 'http://127.0.0.1:8083/analytics/popular-items' @(200) }
    Start-ReadyService 'notification' 'Notification catalog loaded' {
        $ops = Get-OpsStatus
        $null -ne $ops -and $ops.catalogLoaded -eq $true
    }
    Wait-Until "partition assignment of $(@($groupTopics.Keys) -join ', ')" 90 { Test-GroupsAssigned }
    $nginxPrefix = Start-Gateway $NginxBin $repo
    Wait-Until 'gateway /items and /notifications/' 30 {
        (Test-Http 'http://127.0.0.1:8080/items' @(200)) -and (Test-Http 'http://127.0.0.1:8080/notifications/ops/status' @(200))
    }

    # 4. The subscriber test application, until ready.json.
    $subscriberArgs = @('-cp', $classpath, 'SubscriberMain', "--subscribers=$Subscribers", "--skus-per-subscriber=$SkusPerSubscriber",
        "--out=$subscriberDir", '--base-url=http://127.0.0.1:8080', "--bootstrap=$($script:KafkaBootstrap)")
    if ($Seed) { $subscriberArgs += "--seed=$Seed" }
    $prefix = [Environment]::GetEnvironmentVariable('KAFKA_TOPIC_PREFIX', 'Process')
    if ($prefix) { $subscriberArgs += "--topic-prefix=$prefix" }
    # Start-Process joins arguments with spaces and does not quote them, so quote each one.
    $subscriber = Start-Process -FilePath $JavaExe -ArgumentList (($subscriberArgs | ForEach-Object { '"' + $_ + '"' }) -join ' ') `
        -WorkingDirectory $out -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out 'subscriber-stdout.log') -RedirectStandardError (Join-Path $out 'subscriber-stderr.log')
    $null = $subscriber.Handle
    $running['subscriber'] = $subscriber
    Wait-Until 'subscriber ready.json' 180 { Test-Path (Join-Path $subscriberDir 'ready.json') }
    $result.subscriberReady = $true

    # 6. The unchanged Load Client, with backlog samples every 2 s.
    ("utcMs`tphase`tsaleEventsUnpublished`tsaleConsumerLag`tnotificationsPending`tchannelLag`tpendingAttempts`n") |
        ForEach-Object { [IO.File]::WriteAllText($backlogFile, $_, (New-Object System.Text.UTF8Encoding $false)) }
    $reportsDir = Join-Path $clientDir 'reports'
    New-Item -ItemType Directory -Force -Path $reportsDir | Out-Null
    $before = @(Get-ChildItem $reportsDir -Filter 'report-*.json' | ForEach-Object Name)
    $null = Add-BacklogSample 'start'
    if ($Disruption -eq 'notification-stopped') {
        # Scenario G (T109): the whole load runs without Notification. Every stop is a forced kill on Windows; before
        # the load Notification has no sale to work on.
        $process = $running['notification']
        $running.Remove('notification')
        Stop-ProcessTree $process.Id
        $process.WaitForExit()
        Save-ServiceLogs 'notification' 'before-load'
        Add-DisruptionEvent "Notification stopped before the load (pid $($process.Id))"
    }
    $client = Start-Process -FilePath $JavaExe -ArgumentList $clientArgs -WorkingDirectory $clientDir -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out 'client-stdout.txt') -RedirectStandardError (Join-Path $out 'client-stderr.txt')
    $null = $client.Handle
    $loadStarted = Get-Date
    $disrupted = $Disruption -in @('none', 'notification-stopped')
    while (-not $client.HasExited) {
        Start-Sleep -Seconds 2
        $null = Add-BacklogSample 'load'
        if (-not $disrupted -and ((Get-Date) - $loadStarted).TotalSeconds -ge $DisruptAfterSeconds) {
            $disrupted = $true
            $null = Add-BacklogSample 'disruption'
            Invoke-Disruption $Disruption
            $null = Add-BacklogSample 'disruption'
        }
        $client.Refresh()
    }
    if (-not $disrupted) { Write-Host "The load ended before -DisruptAfterSeconds; nothing was injected" -ForegroundColor Red }
    $client.WaitForExit()
    $clientEnded = Get-Date
    if ($Disruption -eq 'notification-stopped') {
        $null = Add-BacklogSample 'disruption'
        Start-ReadyService 'notification' 'Notification catalog loaded' {
            $ops = Get-OpsStatus
            $null -ne $ops -and $ops.catalogLoaded -eq $true
        }
        Add-DisruptionEvent "Notification started after the load (pid $($running['notification'].Id))"
    }
    $result.clientOk = $client.ExitCode -eq 0
    if (-not $result.clientOk) { Write-Host "Load client exited with code $($client.ExitCode)" -ForegroundColor Red }

    # 7. Drain: two consecutive all-zero samples, within -DrainSeconds.
    $deadline = $clientEnded.AddSeconds($DrainSeconds)
    $zeroInRow = 0
    while ((Get-Date) -lt $deadline) {
        $sample = Add-BacklogSample 'drain'
        if ($sample.drained) { $zeroInRow++ } else { $zeroInRow = 0 }
        if ($zeroInRow -ge 2) { $result.drained = $true; $drainElapsedSeconds = [Math]::Round(((Get-Date) - $clientEnded).TotalSeconds, 1); break }
        Start-Sleep -Seconds 2
    }
    if (-not $result.drained) { Write-Host "The run did not drain within $DrainSeconds s" -ForegroundColor Red }

    $newReports = @(Get-ChildItem $reportsDir -Filter 'report-*.json' | Where-Object { $before -notcontains $_.Name })
    if ($newReports.Count -eq 1) {
        $clientReportPath = Join-Path $out 'load-client-report.json'
        Move-Item $newReports[0].FullName $clientReportPath
        $report = Get-Content -Raw $clientReportPath | ConvertFrom-Json
    } else {
        Write-Host "Expected one new Load Client report, found $($newReports.Count)" -ForegroundColor Red
        $result.clientOk = $false
    }

    # 8. Broker status while the consumers are still up, then stop everything this run started.
    $statusText = & (Join-Path $kafkaDir 'status.ps1') -Json -KafkaHome $KafkaHome | Out-String
    [IO.File]::WriteAllText((Join-Path $out 'kafka-status-final.json'), $statusText, (New-Object System.Text.UTF8Encoding $false))
} finally {
    if ($client -and -not $client.HasExited) { Stop-ProcessTree $client.Id }
    if ($subscriber) {
        $running.Remove('subscriber')
        if (-not $subscriber.HasExited) {
            New-Item -ItemType File -Force -Path (Join-Path $subscriberDir 'stop') | Out-Null
            if (-not $subscriber.WaitForExit(30000)) { Stop-ProcessTree $subscriber.Id }
        }
        $subscriber.WaitForExit()
        $subscriberExit = $subscriber.ExitCode
    }
    foreach ($process in $running.Values) { Stop-ProcessTree $process.Id }
    $running.Clear()
    Stop-Gateway $NginxBin $nginxPrefix
    if ($gateOn -and $gateBefore) {
        $load = Invoke-IdleGate 'measure' $gateBefore $gateStarted
        $idle = [ordered]@{
            idleCpuCores = $IdleCpuCores; blockingProcesses = $BlockingProcess; backgroundCpuCores = $load.cores
            blockingSeen = $load.blocking; heaviest = $load.heaviest
            passed = ($IdleCpuCores -le 0 -or $load.cores -le $IdleCpuCores) -and $load.blocking.Count -eq 0
        }
    }
}

# 8 (continued). Invariants, evidence export, and reconciliation; the services are stopped.
try {
    if ($result.subscriberReady) {
        $validatorArgs = @{ MySqlExe = $mysql; KafkaStatusJson = (Join-Path $out 'kafka-status-final.json') }
        if ($report) { $validatorArgs.TotalTransactions = [long]$report.totalTransactions }
        & (Join-Path $dbDir 'validate-invariants.ps1') @validatorArgs *> (Join-Path $out 'validator.txt')
        $validatorExit = $LASTEXITCODE
        $result.validatorPassed = $validatorExit -eq 0

        & (Join-Path $dbDir 'export-notification-evidence.ps1') -RunDir $out -MySqlExe $mysql -JavaExe $JavaExe *> (Join-Path $out 'export.txt')
        $result.exported = $true
    }
} catch {
    Write-Host "Evidence step failed: $_" -ForegroundColor Red
}

# 9. run.json, the reconciliation, and the notification report.
$validatorText = ''
if (Test-Path (Join-Path $out 'validator.txt')) { $validatorText = Get-Content -Raw (Join-Path $out 'validator.txt') }
$background = $null
if ($validatorText -match '(?m)^\s*background_completions=(-?\d+)') { $background = [long]$Matches[1] }
$commit = (& git -C $repo rev-parse HEAD).Trim()
$dirty = [bool](& git -C $repo status --porcelain --untracked-files=no)
$runInfo = [ordered]@{
    harness = 'benchmark-notification'
    id = $RunName
    startedAt = $startedAt.ToString('o')
    deploymentCommit = $commit
    gitDirty = $dirty
    workload = $Workload
    command = $clientCommand
    loadClient = [ordered]@{ stations = $stations; durationSeconds = $durationSeconds }
    subscribers = $Subscribers
    seed = if ($Seed) { [long]$Seed } else { $null }
    skusPerSubscriber = $SkusPerSubscriber
    faults = 'none'
    faultSubscriber = $null
    disruption = if ($Disruption -eq 'none') { $null } else {
        [ordered]@{ kind = $Disruption; afterSeconds = $DisruptAfterSeconds; outageSeconds = $OutageSeconds
            # ToArray, not @(): Windows PowerShell 5.1 fails on @(<generic list>) inside a hashtable literal.
            injected = $disruptionEvents.Count -gt 0; events = $disruptionEvents.ToArray() }
    }
    backgroundCompletions = $background
    drainSeconds = $drainElapsedSeconds
    drainDeadlineSeconds = $DrainSeconds
    extraEnv = $ExtraEnv
    idleGate = $idle
    subscriberExitCode = $subscriberExit
    validatorExitCode = $validatorExit
    result = $result
}
[IO.File]::WriteAllText((Join-Path $out 'run.json'), ($runInfo | ConvertTo-Json -Depth 8), (New-Object System.Text.UTF8Encoding $false))
if ($result.exported) {
    # Start-Process, because Windows PowerShell 5.1 turns a redirected native stderr line into a terminating error.
    $reconcileArgs = @('-cp', $classpath, 'ReconcileMain', "--run-dir=$out", '--mode=fixed',
        "--report-out=$(Join-Path $out 'notification-report.json')") | ForEach-Object { '"' + $_ + '"' }
    $reconcile = Start-Process -FilePath $JavaExe -ArgumentList ($reconcileArgs -join ' ') -WorkingDirectory $out `
        -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput (Join-Path $out 'reconcile.txt') `
        -RedirectStandardError (Join-Path $out 'reconcile-stderr.txt')
    $reconcileExit = $reconcile.ExitCode
    $result.reconciliationPassed = $reconcileExit -eq 0
    Get-Content (Join-Path $out 'reconcile.txt') | Write-Host
    $runInfo.result = $result
    [IO.File]::WriteAllText((Join-Path $out 'run.json'), ($runInfo | ConvertTo-Json -Depth 8), (New-Object System.Text.UTF8Encoding $false))
}
foreach ($entry in $savedEnv.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process') }

Write-Host ("{0}: totalTransactions={1} ready={2} drained={3} ({4} s) validator={5} reconciliation={6} -> {7}" -f $RunName,
    (Get-Prop $report 'totalTransactions'), $result.subscriberReady, $result.drained, $drainElapsedSeconds, $result.validatorPassed,
    $result.reconciliationPassed, $out)
if ($result.clientOk -and $result.subscriberReady -and $result.drained -and $result.validatorPassed -and $result.reconciliationPassed) { exit 0 }
exit 1
