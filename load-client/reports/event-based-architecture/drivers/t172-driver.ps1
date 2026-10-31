# T172: quickstart sections 2 and 5 (scenarios A-H) on fresh baselines, against the phase 11 build.
param([string[]]$Parts = @('1', '2', '3'))
$out = Join-Path $PSScriptRoot ('t172-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Force $out | Out-Null
. (Join-Path $PSScriptRoot 'lib-deploy.ps1')
$clientDir = Join-Path $repo 'load-client'

function Outbox([string]$Service) { return [long](Q $Service 'SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL')[0] }
function Pending { return [long](Q 'transaction' "SELECT COUNT(*) FROM checkout_attempt WHERE state = 'PENDING'")[0] }
function Settled {
    $d = Delivery
    return (Pending) -eq 0 -and (Outbox 'transaction') -eq 0 -and (Outbox 'inventory') -eq 0 -and $d.Unpublished -eq 0 -and $d.Stored -eq $d.Accepted
}
function Admin([string]$Database, [string]$Sql) {
    $null = Invoke-MySql -MySql $mysql -User $env:DB_USER -Password $env:DB_PASSWORD -Database $Database -Query $Sql
}
function StatusLine([string]$Service, [string]$Pattern, [int]$Seconds = 70) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        $file = Get-ChildItem $out -Filter "$Service-*-stdout.log" | Sort-Object LastWriteTime | Select-Object -Last 1
        if ($file) {
            $hit = Select-String -Path $file.FullName -Pattern $Pattern | Select-Object -Last 1
            if ($hit) { return $hit.Line }
        }
        Start-Sleep -Seconds 2
    }
    return "(no status line matching '$Pattern' within $Seconds s)"
}
function Since { return [math]::Round(((Get-Date) - $script:clientStarted).TotalSeconds, 1) }
function Start-Client([string]$Tag) {
    $script:clientReportsBefore = @(Get-ChildItem (Join-Path $clientDir 'reports') -Filter 'report-*.json' | ForEach-Object Name)
    $script:clientStarted = Get-Date
    return Start-Process -FilePath $java -ArgumentList @('-cp', 'out', 'Main') -WorkingDirectory $clientDir -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out "client-$Tag-stdout.txt") -RedirectStandardError (Join-Path $out "client-$Tag-stderr.txt")
}
function At([double]$Seconds) {
    $wait = $Seconds - ((Get-Date) - $script:clientStarted).TotalSeconds
    if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]($wait * 1000)) }
}
function Finish-Client($Process, [string]$Tag) {
    $Process.WaitForExit()
    $new = @(Get-ChildItem (Join-Path $clientDir 'reports') -Filter 'report-*.json' | Where-Object { $script:clientReportsBefore -notcontains $_.Name })
    $report = $null
    foreach ($f in $new) {
        $target = Join-Path $out "client-$Tag-$($f.Name)"
        Move-Item $f.FullName $target
        $report = Get-Content $target -Raw | ConvertFrom-Json
    }
    if (-not $report) { throw "no client report for $Tag" }
    $ops = @{}
    foreach ($o in $report.operations) { $ops[$o.operation] = $o }
    Note ("Client ({0}): totalTransactions {1}; Start {2} ok / {3} errors; Scan {4} / {5}; Complete {6} / {7}; p99 Start {8:N1}, Scan {9:N1}, Complete {10:N1} ms" -f $Tag,
        $report.totalTransactions, $ops.START_TRANSACTION.successCount, $ops.START_TRANSACTION.errorCount, $ops.SCAN_ITEM.successCount,
        $ops.SCAN_ITEM.errorCount, $ops.COMPLETE_TRANSACTION.successCount, $ops.COMPLETE_TRANSACTION.errorCount,
        $ops.START_TRANSACTION.p99Ms, $ops.SCAN_ITEM.p99Ms, $ops.COMPLETE_TRANSACTION.p99Ms)
    return $report
}
function Validate([string]$Tag, $Report) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    WaitFor "$Tag pending work to drain" 240 { Settled }
    Note "All work drained $([math]::Round($sw.Elapsed.TotalSeconds, 1)) s after the client ended"
    $status = Join-Path $out "kafka-status-$Tag.json"
    WaitFor "$Tag consumer lag 0" 120 {
        & (Join-Path $kafkaDir 'status.ps1') -Json *> $status
        $j = Get-Content $status -Raw | ConvertFrom-Json
        @($j.groups | Where-Object { $_.lag -ne 0 }).Count -eq 0
    }
    $file = Join-Path $out "validator-$Tag.txt"
    $global:LASTEXITCODE = 0
    & (Join-Path $repo 'db\validate-invariants.ps1') -MySqlExe $mysql -KafkaStatusJson $status -TotalTransactions $Report.totalTransactions *> $file
    $code = $LASTEXITCODE
    Note "validate-invariants exit ${code}:"
    Get-Content $file | Where-Object { $_ -match '=|PASS|FAIL|lag' -and $_ -notmatch '^\s*[+|]' } | ForEach-Object { Note "    $($_.Trim())" }
    $d = Delivery
    Note "Delivery: accepted $($d.Accepted), unpublished $($d.Unpublished), Analytics stored $($d.Stored), distinct identities $($d.Distinct); parked $(Parked)"
    $dup = [long](Q 'inventory' "SELECT COUNT(*) FROM (SELECT transaction_id FROM checkout_operation WHERE state = 'APPLIED' GROUP BY transaction_id HAVING COUNT(*) > 1) x")[0]
    $completedUnits = [long](Q 'transaction' "SELECT COALESCE(SUM(item_count), 0) FROM ``transaction`` WHERE status = 'COMPLETED'")[0]
    Note "Transactions with more than one applied deduction: $dup; units $($script:units0) -> $(Units) (completed units $completedUnits)"
}

try {
    Note "# T172 quickstart validation ($(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))"
    if ($Parts -contains '1') {
        Note ''
        Note '## Part 1: section 2, then scenarios A, G, B, D, H'
        $reset = Reset-Baseline
        Get-Content $reset | Where-Object { $_ -match '\s\d+\s*$|baseline\.' } | ForEach-Object { Note "    $($_.Trim())" }
        Start-Deployment
        $script:units0 = Units
        $catalog = (Q 'inventory' 'SELECT catalog_version FROM catalog_meta WHERE meta_key = 1')[0]
        Note "Inventory catalog_version $catalog"
        Note "Transaction status: $(StatusLine 'transaction' 'status service=transaction')"
        Note "Inventory status: $(StatusLine 'inventory' 'status service=inventory')"
        Note "Analytics status: $(StatusLine 'analytics' 'status service=analytics')"
        & (Join-Path $kafkaDir 'status.ps1') *> (Join-Path $out 'kafka-status-part1.txt')
        Get-Content (Join-Path $out 'kafka-status-part1.txt') | ForEach-Object { Note "    $_" }

        Note ''
        Note '### Scenario A'
        $a1 = Stock 'SKU-000001'; $b1 = Stock 'SKU-000002'
        $t = Json (StartTx 'scenario-a').Body
        foreach ($sku in 'SKU-000001', 'SKU-000001', 'SKU-000002') { $null = Scan $t.transactionId $sku }
        $c = Http POST "/transactions/$($t.transactionId)/complete" ''
        Note "A1 Complete: $($c.Status) in $($c.Ms) ms, $($c.Body); SKU-000001 $a1 -> $(Stock 'SKU-000001'), SKU-000002 $b1 -> $(Stock 'SKU-000002')"
        Admin 'cs6510_inventory' "UPDATE inventory SET stock_quantity = 1 WHERE sku = 'SKU-000003'"
        $t2 = Json (StartTx 'scenario-a').Body
        foreach ($sku in 'SKU-000003', 'SKU-000003') { $null = Scan $t2.transactionId $sku }
        $c2 = Http POST "/transactions/$($t2.transactionId)/complete" ''
        $l2 = Json (Http GET "/transactions/$($t2.transactionId)").Body
        Note "A2 Complete: $($c2.Status) in $($c2.Ms) ms, $($c2.Body); SKU-000003 1 -> $(Stock 'SKU-000003'); lookup $($l2.status), itemCount $($l2.itemCount)"
        WaitFor 'outboxes to drain' 30 { (Outbox 'transaction') -eq 0 -and (Outbox 'inventory') -eq 0 }
        Note "Outboxes: Transaction $((Q 'transaction' 'SELECT COUNT(*) FROM outbox_event')[0]) rows, Inventory $((Q 'inventory' 'SELECT COUNT(*) FROM outbox_event')[0]) rows (catalog included), 0 unpublished; parked $(Parked)"

        Note ''
        Note '### Scenario G'
        $pub = Start-Process -FilePath $java -ArgumentList @('-cp', 'out/test;out/common.jar;out/contracts.jar;lib/messaging/*', 'support.PublishRawRecord',
            'selfcheckout.checkout.v1', 'poison-key', 'not-an-event-envelope') -WorkingDirectory $server -WindowStyle Hidden -Wait -PassThru `
            -RedirectStandardOutput (Join-Path $out 'publish-stdout.txt') -RedirectStandardError (Join-Path $out 'publish-stderr.txt')
        $published = Get-Date
        if ($pub.ExitCode -ne 0) { throw 'PublishRawRecord failed' }
        WaitFor 'the poison record to be parked' 30 { [long](Q 'inventory' 'SELECT COUNT(*) FROM dead_letter_event')[0] -ge 1 }
        $ms = [int]((Get-Date) - $published).TotalMilliseconds
        $p = Q 'inventory' 'SELECT reason, event_id IS NULL, topic, park_count FROM dead_letter_event'
        Note "Parked $ms ms after the publisher exited: reason $($p[0]), null event_id $($p[1]), topic $($p[2]), park_count $($p[3])"
        $g = Json (StartTx 'scenario-g').Body
        $null = Scan $g.transactionId 'SKU-000004'
        Note "Checkout right afterwards: Complete $((Http POST "/transactions/$($g.transactionId)/complete" '').Status)"
        Note "Inventory status: $(StatusLine 'inventory' 'status service=inventory.*parked_events=1')"
        Admin 'cs6510_inventory' 'DELETE FROM dead_letter_event'
        Note 'The parked poison row was then removed with the admin account, so the later scenarios start with zero parked events.'

        Note ''
        Note '### Scenario B'
        $w = Json (StartTx 'warm').Body
        $null = Scan $w.transactionId 'SKU-000001'
        Note "Warm-up Complete with the broker up: $((Http POST "/transactions/$($w.transactionId)/complete" '').Status)"
        Stop-Broker
        Note "Broker stopped (port 19092 answering: $((Test-NetConnection 127.0.0.1 -Port 19092 -InformationLevel Quiet -WarningAction SilentlyContinue)))"
        $s = StartTx 'outage-1'
        $b = Json $s.Body
        Note "Start: $($s.Status) in $($s.Ms) ms"
        foreach ($sku in 'SKU-000005', 'SKU-000006', 'SKU-000005') {
            $r = Scan $b.transactionId $sku
            $v = Json $r.Body
            Note "Scan ${sku}: $($r.Status) in $($r.Ms) ms, '$($v.name)' $($v.unitPrice), itemCount $($v.itemCount), runningTotal $($v.runningTotal)"
        }
        $l = Http GET "/transactions/$($b.transactionId)"
        $lv = Json $l.Body
        Note "Lookup: $($l.Status) in $($l.Ms) ms, $($lv.status), $($lv.itemCount) items, $($lv.runningTotal)"
        foreach ($path in '/items', '/inventory/low-stock', '/analytics/popular-items') { $r = Http GET $path; Note "GET ${path}: $($r.Status) in $($r.Ms) ms" }
        $s5 = Stock 'SKU-000005'
        $c = Http POST "/transactions/$($b.transactionId)/complete" ''
        $a = Attempt $b.transactionId
        Note "Complete: $($c.Status) in $($c.Ms) ms, $($c.Body); attempt $($a[0]) / $($a[1]); SKU-000005 stock $s5 -> $(Stock 'SKU-000005')"
        Stop-Tree $procs['transaction'].Id
        Start-Svc 'transaction'
        Ready 'transaction'
        $lb = Json (Http GET "/transactions/$($b.transactionId)").Body
        Note "Transaction killed and restarted with the broker down: lookup $($lb.status), $($lb.itemCount) items, $($lb.runningTotal), same start time $($lb.startedAt -eq $lv.startedAt)"
        $s3 = Json (StartTx 'outage-3').Body
        $r = Scan $s3.transactionId 'SKU-000007'
        Note "Start and Scan after the restart: $($r.Status) in $($r.Ms) ms"
        $d = Delivery
        Note "Before the broker returns: accepted $($d.Accepted), unpublished $($d.Unpublished)"
        $back = Get-Date
        & (Join-Path $kafkaDir 'start-broker.ps1') -NoTopics *> (Join-Path $out 'start-broker-b.txt')
        Note "Broker started again in $([int]((Get-Date) - $back).TotalSeconds) s"
        WaitFor 'the outage attempt to settle' 180 { (Attempt $b.transactionId)[0] -ne 'PENDING' }
        $a = Attempt $b.transactionId
        Note "Attempt settled $([math]::Round(((Get-Date) - $back).TotalSeconds, 1)) s after the broker start began: $($a[0]) / $($a[1]); transaction $((Json (Http GET "/transactions/$($b.transactionId)").Body).status); SKU-000005 $s5 -> $(Stock 'SKU-000005'), SKU-000006 -> $(Stock 'SKU-000006')"
        Note "Repeated Complete: $((Http POST "/transactions/$($b.transactionId)/complete" '').Status)"
        WaitFor 'every scan counted' 180 { $x = Delivery; $x.Unpublished -eq 0 -and $x.Stored -eq $x.Accepted }
        $d = Delivery
        Note "Delivery: accepted $($d.Accepted), unpublished $($d.Unpublished), stored $($d.Stored), distinct $($d.Distinct); parked $(Parked)"

        Note ''
        Note '### Scenario D'
        Stop-Tree $procs['inventory'].Id
        $t = Json (StartTx 'inventory-down').Body
        foreach ($sku in 'SKU-000008', 'SKU-000009', 'SKU-000008') { $r = Scan $t.transactionId $sku; Note "Scan ${sku}: $($r.Status) in $($r.Ms) ms" }
        $s8 = Stock 'SKU-000008'; $s9 = Stock 'SKU-000009'
        $c = Http POST "/transactions/$($t.transactionId)/complete" ''
        $a = Attempt $t.transactionId
        Note "Complete: $($c.Status) in $($c.Ms) ms; attempt $($a[0]) / $($a[1])"
        foreach ($path in '/items', '/inventory/low-stock', '/analytics/popular-items') { $r = Http GET $path; Note "GET ${path} with Inventory down: $($r.Status)" }
        $back = Get-Date
        Start-Svc 'inventory'
        Ready 'inventory'
        WaitFor 'the attempt to settle' 180 { (Attempt $t.transactionId)[0] -ne 'PENDING' }
        $a = Attempt $t.transactionId
        Note "Inventory restarted; settled $([math]::Round(((Get-Date) - $back).TotalSeconds, 1)) s after its start: $($a[0]) / $($a[1]); SKU-000008 $s8 -> $(Stock 'SKU-000008'), SKU-000009 $s9 -> $(Stock 'SKU-000009')"
        WaitFor 'part 1 to settle' 120 { Settled }
        $d = Delivery
        $completed = Q 'transaction' "SELECT COUNT(*), COALESCE(SUM(item_count), 0) FROM ``transaction`` WHERE status = 'COMPLETED'"
        $applied = Q 'inventory' "SELECT COUNT(*) FROM checkout_operation WHERE state = 'APPLIED'"
        Note "End of A, G, B, D: accepted $($d.Accepted), unpublished $($d.Unpublished), stored $($d.Stored), distinct $($d.Distinct); parked $(Parked); pending $(Pending); $($completed[0]) completed transactions with $($completed[1]) units, $($applied[0]) applied operations; units $($script:units0) -> $(Units) (SKU-000003 was set to 1 by hand in A2)"

        Note ''
        Note '### Scenario H'
        Stop-Deployment
        $global:LASTEXITCODE = 0
        & (Join-Path $repo 'db\reset-service.ps1') -Service transaction -MySqlExe $mysql *> (Join-Path $out 'reset-transaction.txt')
        if ($LASTEXITCODE -ne 0) { throw 'reset-service transaction failed' }
        $rep = Q 'transaction' "SELECT COALESCE(catalog_version, 'NULL'), (SELECT COUNT(*) FROM product_replica) FROM product_replica_meta WHERE meta_key = 1"
        Note "Transaction reset with Inventory stopped: replica version $($rep[0]), $($rep[1]) products"
        $started = Get-Date
        Start-Svc 'transaction'
        Ready 'transaction'
        WaitFor 'the replica to be rebuilt' 60 { (Q 'transaction' "SELECT COALESCE(catalog_version, 'none') FROM product_replica_meta WHERE meta_key = 1")[0] -eq $catalog }
        $rep = Q 'transaction' 'SELECT catalog_version, (SELECT COUNT(*) FROM product_replica) FROM product_replica_meta WHERE meta_key = 1'
        Note "Rebuilt from the compacted topic within $([math]::Round(((Get-Date) - $started).TotalSeconds, 1)) s of the process start, Inventory down: version $($rep[0]) (Inventory's $catalog), $($rep[1]) products"
        $h = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8081/transactions' -ContentType 'application/json' -Body '{"stationId":"scenario-h"}'
        $hs = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/transactions/$($h.transactionId)/items" -ContentType 'application/json' -Body '{"sku":"SKU-000007"}'
        Note "Scan SKU-000007 afterwards: '$($hs.name)' $($hs.unitPrice)"
        $log = Get-ChildItem $out -Filter 'transaction-*-std*.log' | Sort-Object LastWriteTime | Select-Object -Last 2 | ForEach-Object { Get-Content $_.FullName }
        Note "Replica log line: $(($log | Where-Object { $_ -match 'replica now at' }) -join ' / ')"
        Note "Log lines naming gRPC or an Inventory address: $(@($log | Where-Object { $_ -match '(?i)grpc|:8082|inventory_service' }).Count)"
        Stop-Deployment
    }
    if ($Parts -contains '2') {
        Note ''
        Note '## Part 2: scenario E (Analytics outage under the default load)'
        $null = Reset-Baseline
        Start-Deployment
        $script:units0 = Units
        $client = Start-Client 'e'
        At 20
        $before = Q 'analytics' 'SELECT s.last_position, c.last_window_end FROM scan_sequence s JOIN analytics_checkpoint c ON c.checkpoint_key = 1 WHERE s.sequence_key = 1'
        Stop-Tree $procs['analytics'].Id
        $procs.Remove('analytics')
        Note "Analytics killed at $(Since) s: last_position $($before[0]), window $($before[1])"
        At 35
        & (Join-Path $kafkaDir 'status.ps1') -Json *> (Join-Path $out 'kafka-status-e-mid.json')
        $topic = (Get-Content (Join-Path $out 'kafka-status-e-mid.json') -Raw | ConvertFrom-Json).topics | Where-Object { $_.name -eq 'selfcheckout.scan-accepted.v1' }
        $mid = Q 'analytics' 'SELECT last_window_end FROM analytics_checkpoint WHERE checkpoint_key = 1'
        Start-Svc 'analytics'
        Ready 'analytics'
        Note "Analytics restarted at $(Since) s; the scan topic had $($topic.totalEndOffset) records while its window stayed at $($mid[0])"
        $report = Finish-Client $client 'e'
        Validate 'e' $report
        $an = Q 'analytics' 'SELECT s.last_position, c.last_window_end, (SELECT MAX(next_offset) FROM consumer_offset) FROM scan_sequence s JOIN analytics_checkpoint c ON c.checkpoint_key = 1 WHERE s.sequence_key = 1'
        Note "Analytics after the run: last_position $($an[0]), checkpoint $($an[1]), stored next offset $($an[2])"
        Stop-Deployment
    }
    if ($Parts -contains '3') {
        Note ''
        Note '## Part 3: scenario C (broker restarts under the default load)'
        $null = Reset-Baseline
        Start-Deployment
        $script:units0 = Units
        $client = Start-Client 'c'
        At 20
        Stop-Broker
        Note "Broker stopped at $(Since) s"
        & (Join-Path $kafkaDir 'start-broker.ps1') -NoTopics *> (Join-Path $out 'start-broker-c1.txt')
        Note "Broker started again at $(Since) s"
        At 40
        Stop-Broker
        Note "Broker stopped at $(Since) s"
        Start-Sleep -Seconds 10
        & (Join-Path $kafkaDir 'start-broker.ps1') -NoTopics *> (Join-Path $out 'start-broker-c2.txt')
        Note "Broker started again at $(Since) s, after 10 s down"
        $report = Finish-Client $client 'c'
        Validate 'c' $report
        $bg = Q 'transaction' "SELECT SUM(state = 'COMPLETED'), SUM(state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'), SUM(state = 'COMPLETED' AND client_outcome = 'NONE'), SUM(state <> 'COMPLETED') FROM checkout_attempt"
        Note "After settling: COMPLETED attempts $($bg[0]), background completions $($bg[1]), without a recorded response $($bg[2]), other attempts $($bg[3])"
        Stop-Deployment
    }
} catch {
    Note "FAILED: $($_.Exception.Message) at line $($_.InvocationInfo.ScriptLineNumber)"
} finally {
    Stop-Deployment
    if (-not (Test-NetConnection 127.0.0.1 -Port 19092 -InformationLevel Quiet -WarningAction SilentlyContinue)) {
        & (Join-Path $kafkaDir 'start-broker.ps1') -NoTopics *> (Join-Path $out 'start-broker-final.txt')
        Note 'Broker restarted in cleanup'
    }
    Note "== driver finished $(Get-Date -Format 'HH:mm:ss') out=$out"
}
