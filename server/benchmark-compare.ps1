<#
.SYNOPSIS
    Interleaved A/B benchmark comparison with the R22 decision rule.

.DESCRIPTION
    Runs variants A and B as alternating pairs (A B, then B A, then A B, ...) to cancel linear drift.
    Every run starts from its own full reset. Variant runs use benchmark-microservices.ps1 with the
    variant's -ExtraEnv; with -BaselineWorktree, variant A instead runs that worktree's
    server/benchmark-scan.ps1 with the T001 arguments. Runs are labelled <label>-a-<n> and
    <label>-b-<n>. A variant run that is not comparable aborts the comparison, and a baseline run must
    pass its own harness's checks.

    Decision rule (R22, streamlined 2026-10-22): the script runs exactly -Pairs pairs, never more, and
    classifies the result by delta, B's totalTransactions median against A's in percent. Differences
    within +/-3% are practically equivalent.
      - One pair (screening): promising at delta >= +5%, regression at delta <= -3%, otherwise
        no-demonstrated-improvement.
      - More pairs (confirmation): improvement at delta >= +3% with B ahead in a majority of the
        pairs (two of three), regression at delta <= -3%, otherwise no-demonstrated-improvement.
      - A Scan or Complete p99 median more than 10% above A's is a latency regression, which stops
        B from being retained. Resource use is reported only, and the Mann-Whitney U value is kept as
        description; no significance is claimed.

    Idle-machine gate: every run needs an otherwise idle machine. Before each run the script waits
    until no -BlockingProcess is running, no other program holds a benchmark port, and the CPU used
    by processes outside the benchmark stays at or below -IdleCpuCores for three consecutive 10 s
    samples. A run whose services fail to start is set aside and repeated like a loaded one. After the run it measures that
    background CPU again, averaged over the run; a run with a higher average, or one during which a
    blocking process ran, is moved to compare/<label>/invalidated/ and repeated under the same label,
    up to -MaxRunAttempts times. Waits and invalidations are logged to compare/idle-gate.log, and
    compare.json records each run's background CPU and every invalidated attempt.

    Resume: rerunning an interrupted comparison (one without compare.json) with the same label keeps
    its complete pairs and repeats the rest. The runs of an incomplete pair, including one cut off
    mid-run, are moved to compare/<label>/interrupted/. compare.json records how many pairs were
    resumed, so a comparison that spans a restart is visible.

    Output: load-client/reports/microservices-architecture/compare/<label>/compare.json, plus a
    Markdown row for checkpoints.md, printed and saved as compare.md.

    Dot-source this file (. ./benchmark-compare.ps1) to load its statistics functions without
    running a comparison.

.EXAMPLE
    ./benchmark-compare.ps1 -Workload stress -Label cp3-screen-tx48-80-stress -VariantB @{ TRANSACTION_DB_POOL_SIZE = '48'; TRANSACTION_DB_POOL_MAX_SIZE = '80' } -NginxBin C:\nginx\nginx.exe
    ./benchmark-compare.ps1 -Workload stress -Label cp4-final-stress -BaselineWorktree ..\..\cs6510-cp0 -Pairs 3 -NginxBin C:\nginx\nginx.exe
#>
param(
    [ValidateSet('default', 'stress')][string]$Workload,
    [string]$Label,
    [hashtable]$VariantA = @{},
    [hashtable]$VariantB = @{},
    [string]$BaselineWorktree,
    [ValidateRange(1, 20)][int]$Pairs = 1,
    [string]$MySqlExe,
    [string]$NginxBin,
    [string]$JavaExe,
    [ValidateRange(0.1, 64)][double]$IdleCpuCores = 1.0,
    [string[]]$BlockingProcess = @(),
    [ValidateRange(1, 20)][int]$MaxRunAttempts = 5
)

# R22 thresholds, in percent of variant A's median.
$script:EquivalencePercent = 3
$script:PromisingPercent = 5
$script:LatencyRegressionPercent = 10
$script:ResourceMetrics = @('borrowWaitP99Ms', 'inventoryLockWaitP99Ms', 'rowLockWaitsDelta', 'commitsPerTransaction')

function Get-Median([double[]]$Values) {
    if ($Values.Count -eq 0) { return $null }
    $sorted = @($Values | Sort-Object)
    $middle = [int][Math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) { return [double]$sorted[$middle] }
    return ([double]$sorted[$middle - 1] + [double]$sorted[$middle]) / 2
}

function Get-Range([double[]]$Values) {
    if ($Values.Count -eq 0) { return $null }
    $measure = $Values | Measure-Object -Minimum -Maximum
    return [pscustomobject]@{ min = [double]$measure.Minimum; max = [double]$measure.Maximum }
}

# U counts the pairs in which the loser beats the winner (ties count one half). It is recorded as a
# description of the overlap only; no decision uses it.
function Get-MannWhitneyU([double[]]$Winner, [double[]]$Loser) {
    $u = 0.0
    foreach ($l in $Loser) {
        foreach ($w in $Winner) {
            if ($l -gt $w) { $u += 1 } elseif ($l -eq $w) { $u += 0.5 }
        }
    }
    return $u
}

# Counts runs through the pipeline: in Windows PowerShell 5.1, @($list).Count throws "Argument types do not
# match" for a one-element generic List.
function Get-RunCount($Runs) { return @($Runs | ForEach-Object { $_ }).Count }

function Get-VariantSummary($Runs, [string]$Metric) {
    $values = @($Runs | ForEach-Object { $_.$Metric } | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($values.Count -ne (Get-RunCount $Runs) -or $values.Count -eq 0) { return $null }
    return [pscustomobject]@{ median = Get-Median $values; range = Get-Range $values; values = $values }
}

<#
    Classifies completed runs by the R22 rule. Runs are paired by position: the n-th A run and the
    n-th B run belong to pair n.
#>
function Get-Decision($RunsA, $RunsB) {
    $a = Get-VariantSummary $RunsA 'totalTransactions'
    $b = Get-VariantSummary $RunsB 'totalTransactions'
    $n = Get-RunCount $RunsA
    $listA = @($RunsA | ForEach-Object { $_ })
    $listB = @($RunsB | ForEach-Object { $_ })
    $change = 0.0
    if ($a.median -gt 0) { $change = [Math]::Round(($b.median - $a.median) / $a.median * 100, 2) }
    $perPair = @()
    $ahead = 0
    for ($i = 0; $i -lt $n; $i++) {
        $ta = [double]$listA[$i].totalTransactions
        $tb = [double]$listB[$i].totalTransactions
        if ($tb -gt $ta) { $ahead++ }
        $pairChange = $null
        if ($ta -gt 0) { $pairChange = [Math]::Round(($tb - $ta) / $ta * 100, 2) }
        $perPair += [pscustomobject][ordered]@{ pair = $i + 1; A = $ta; B = $tb; changePercent = $pairChange }
    }
    $latencyRegressions = @()
    foreach ($metric in @('scanP99Ms', 'completeP99Ms')) {
        $ma = Get-VariantSummary $RunsA $metric
        $mb = Get-VariantSummary $RunsB $metric
        if ($null -eq $ma -or $null -eq $mb -or $ma.median -le 0) { continue }
        $rise = ($mb.median - $ma.median) / $ma.median * 100
        if ($rise -gt $script:LatencyRegressionPercent) { $latencyRegressions += ('{0} +{1:0.0}%' -f $metric, $rise) }
    }
    if ($change -le -$script:EquivalencePercent) { $classification = 'regression' }
    elseif ($n -eq 1) {
        $classification = 'no-demonstrated-improvement'
        if ($change -ge $script:PromisingPercent) { $classification = 'promising' }
    } else {
        $classification = 'no-demonstrated-improvement'
        if ($change -ge $script:EquivalencePercent -and 2 * $ahead -gt $n) { $classification = 'improvement' }
    }
    $winner = 'A'
    if ($classification -eq 'improvement' -and $latencyRegressions.Count -eq 0) { $winner = 'B' }
    return [pscustomobject][ordered]@{
        pairs = $n
        changePercent = $change
        equivalent = [Math]::Abs($change) -lt $script:EquivalencePercent
        pairsAheadB = $ahead
        perPair = $perPair
        classification = $classification
        latencyRegressions = $latencyRegressions
        winner = $winner
        uDescriptive = Get-MannWhitneyU $b.values $a.values
    }
}

function Read-MicroservicesRun([string]$EvidencePath) {
    $evidence = Get-Content -Raw $EvidencePath | ConvertFrom-Json
    $operations = $evidence.client.operations
    $commitsPerTransaction = $null
    $completed = $evidence.fr054.completedTransactions
    if ($null -ne $evidence.samples.commitsDelta -and $completed -gt 0) {
        $commitsPerTransaction = [double]$evidence.samples.commitsDelta / $completed
    }
    return [pscustomobject]@{
        source = 'benchmark-microservices'
        comparable = [bool]$evidence.comparable
        totalTransactions = $evidence.client.totalTransactions
        scanP99Ms = $operations.SCAN_ITEM.p99Ms
        completeP99Ms = $operations.COMPLETE_TRANSACTION.p99Ms
        borrowWaitP99Ms = $evidence.pools.transaction.borrowWaitP99Ms
        inventoryLockWaitP99Ms = $evidence.locks.inventoryApplyLockWaitP99Ms
        rowLockWaitsDelta = $evidence.samples.rowLockWaitsDelta
        commitsPerTransaction = $commitsPerTransaction
    }
}

# Reads a benchmark-scan.ps1 (T001 harness) run directory: client report, samples, and profiler log.
function Read-BaselineRun([string]$RunDir) {
    $report = Get-ChildItem $RunDir -Filter 'report-*.json' | Select-Object -First 1
    if (-not $report) { throw "No client report in $RunDir" }
    $client = Get-Content -Raw $report.FullName | ConvertFrom-Json
    $operations = @{}
    foreach ($operation in $client.operations) { $operations[$operation.operation] = $operation }
    $samples = @(Import-Csv (Join-Path $RunDir 'samples.csv'))
    $rowLockWaitsDelta = $null; $commitsPerTransaction = $null
    if ($samples.Count -ge 2) {
        $rowLockWaitsDelta = [long]$samples[-1].row_lock_waits - [long]$samples[0].row_lock_waits
        if ($client.totalTransactions -gt 0) {
            $commitsPerTransaction = ([long]$samples[-1].commits - [long]$samples[0].commits) / [double]$client.totalTransactions
        }
    }
    $borrowWait = $null
    $profile = Join-Path $RunDir 'transaction-stderr.log'
    if (Test-Path $profile) {
        $line = Get-Content $profile | Where-Object { $_ -match '^\[scan-profile\] POOL_ACQUIRE ' } | Select-Object -Last 1
        if ($line -match 'p99_ms=([0-9.]+)') { $borrowWait = [double]$Matches[1] }
    }
    return [pscustomobject]@{
        source = 'benchmark-scan (baseline worktree)'
        comparable = $true
        totalTransactions = $client.totalTransactions
        scanP99Ms = $operations['SCAN_ITEM'].p99Ms
        completeP99Ms = $operations['COMPLETE_TRANSACTION'].p99Ms
        borrowWaitP99Ms = $borrowWait
        inventoryLockWaitP99Ms = $null
        rowLockWaitsDelta = $rowLockWaitsDelta
        commitsPerTransaction = $commitsPerTransaction
    }
}

# Processes that belong to the benchmark itself (services, gateway, database, harness shells) and
# the kernel's own accounting entries; every other process counts as background load.
$script:BenchmarkProcessNames = @('Idle', 'System', 'Secure System', 'java', 'javaw', 'nginx', 'mysqld', 'mysql',
    'powershell', 'pwsh', 'conhost', 'bash', 'sh', 'git')

# Cumulative CPU seconds of every background process, keyed by PID and start time so a reused PID
# is not mistaken for the same process.
function Get-BackgroundCpuSnapshot {
    $snapshot = @{}
    foreach ($process in Get-Process) {
        if ($script:BenchmarkProcessNames -contains $process.ProcessName) { continue }
        $cpu = $null; $start = $null
        try { $cpu = $process.CPU; $start = $process.StartTime } catch { }
        if ($null -eq $cpu) { continue }
        $ticks = 0
        if ($start) { $ticks = $start.Ticks }
        $snapshot['{0}@{1}' -f $process.Id, $ticks] = [pscustomobject]@{ name = $process.ProcessName; cpu = [double]$cpu }
    }
    return $snapshot
}

<#
    The average number of cores that background processes used between two snapshots, plus the
    blocking processes seen in either snapshot. A process that started after $Before counts with all
    of its CPU time; one that both started and exited in between is missed.
#>
function Measure-BackgroundLoad($Before, $After, [double]$Seconds, [string[]]$Blocking) {
    $used = 0.0
    $byName = @{}
    foreach ($key in $After.Keys) {
        $now = $After[$key]
        $delta = $now.cpu
        if ($Before.ContainsKey($key)) { $delta = $now.cpu - $Before[$key].cpu }
        if ($delta -le 0) { continue }
        $used += $delta
        if (-not $byName.ContainsKey($now.name)) { $byName[$now.name] = 0.0 }
        $byName[$now.name] += $delta
    }
    $names = @(@($Before.Values) + @($After.Values) | ForEach-Object { $_.name } | Sort-Object -Unique)
    $seen = @($Blocking | Where-Object { $names -contains $_ })
    $span = [Math]::Max($Seconds, 0.001)
    $heaviest = @($byName.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 3 |
        ForEach-Object { '{0}={1:0.00}' -f $_.Key, ($_.Value / $span) })
    return [pscustomobject]@{ cores = [Math]::Round($used / $span, 3); blocking = $seen; heaviest = $heaviest }
}

function Write-IdleGateLog([string]$Path, [string]$Text) {
    $line = "$(Get-Date -Format s) $Text"
    Write-Host "[idle-gate] $line"
    # A reader may hold the log open briefly; retry rather than fail the comparison.
    for ($try = 0; $try -lt 20; $try++) {
        try { [IO.File]::AppendAllText($Path, $line + "`r`n", (New-Object System.Text.UTF8Encoding $false)); return }
        catch { Start-Sleep -Milliseconds 250 }
    }
}

# The services' HTTP and gRPC ports, and the baseline harness's disposable gateway ports. Windows may
# hand these out as client ports when its dynamic range starts low, and a service then cannot bind.
$script:BenchmarkPorts = @(8080, 8081, 8082, 8083, 9092, 9093, 18080, 18081, 18082, 18083)

# Connections on a benchmark port that would stop a service from binding it, as "port (process)".
# Between runs no benchmark process is running, so every such connection belongs to another program.
function Get-ForeignBenchmarkPortUsers {
    return @(Get-NetTCPConnection -LocalPort $script:BenchmarkPorts -ErrorAction SilentlyContinue |
        Where-Object { @('TimeWait', 'Closed') -notcontains [string]$_.State } | ForEach-Object {
            $name = (Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue).ProcessName
            "$($_.LocalPort) ($name)"
        } | Sort-Object -Unique)
}

# Blocks until no blocking process runs, no other program holds a benchmark port, and background
# CPU stays low for three 10 s samples.
function Wait-IdleMachine([string]$LogPath) {
    $quiet = 0
    $waiting = $null
    while ($quiet -lt 3) {
        $running = @()
        if ($BlockingProcess.Count -gt 0) {
            $running = @(Get-Process -Name $BlockingProcess -ErrorAction SilentlyContinue | ForEach-Object ProcessName | Sort-Object -Unique)
        }
        if ($running.Count -gt 0) {
            $quiet = 0
            $reason = "blocking process running: $($running -join ', ')"
            if ($waiting -ne $reason) { Write-IdleGateLog $LogPath "waiting, $reason"; $waiting = $reason }
            Start-Sleep -Seconds 30
            continue
        }
        $ports = @(Get-ForeignBenchmarkPortUsers)
        if ($ports.Count -gt 0) {
            $quiet = 0
            $reason = "benchmark port in use: $($ports -join ', ')"
            if ($waiting -ne $reason) { Write-IdleGateLog $LogPath "waiting, $reason"; $waiting = $reason }
            Start-Sleep -Seconds 30
            continue
        }
        $before = Get-BackgroundCpuSnapshot
        $started = Get-Date
        Start-Sleep -Seconds 10
        $load = Measure-BackgroundLoad $before (Get-BackgroundCpuSnapshot) ((Get-Date) - $started).TotalSeconds $BlockingProcess
        if ($load.cores -le $IdleCpuCores) { $quiet++; continue }
        $quiet = 0
        $reason = "background CPU above $IdleCpuCores cores"
        if ($waiting -ne $reason) {
            Write-IdleGateLog $LogPath "waiting, $reason ($($load.cores); $($load.heaviest -join ', '))"
            $waiting = $reason
        }
    }
    if ($waiting) { Write-IdleGateLog $LogPath 'machine idle again; resuming' }
}

function Format-Summary($Summary) {
    if ($null -eq $Summary) { return 'n/a' }
    return ('{0} ({1}-{2})' -f $Summary.median, $Summary.range.min, $Summary.range.max)
}

function Invoke-Comparison {
    $ErrorActionPreference = 'Stop'
    if (-not $Workload) { throw '-Workload default|stress is required' }
    if (-not $Label -or $Label -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') { throw '-Label is required (letters, digits, . _ -)' }
    $server = $PSScriptRoot
    $repo = Split-Path -Parent $server
    $reports = Join-Path $repo 'load-client/reports/microservices-architecture'
    $compareDir = Join-Path $reports "compare/$Label"
    if (Test-Path (Join-Path $compareDir 'compare.json')) { throw "Comparison already recorded: $compareDir" }
    New-Item -ItemType Directory -Force -Path $compareDir | Out-Null
    . (Join-Path $repo 'db/mysql-tools.ps1')
    $mysql = Resolve-MySqlExe -Explicit $MySqlExe
    if (-not $JavaExe) {
        # The JDK's own java.exe: the baseline harness stops services by PID, and a launcher shim
        # such as Oracle's javapath would leave the JVM it started running.
        $launcher = (Get-Command java -ErrorAction Stop).Source
        $ErrorActionPreference = 'Continue'
        $javaHome = (& $launcher -XshowSettings:properties -version 2>&1 | ForEach-Object { "$_" } |
            Where-Object { $_ -match '^\s*java\.home = ' } | Select-Object -First 1) -replace '^\s*java\.home = ', ''
        $ErrorActionPreference = 'Stop'
        $script:JavaExe = $launcher
        if ($javaHome -and (Test-Path (Join-Path $javaHome.Trim() 'bin\java.exe'))) {
            $script:JavaExe = Join-Path $javaHome.Trim() 'bin\java.exe'
        }
    }

    $runsA = New-Object System.Collections.Generic.List[object]
    $runsB = New-Object System.Collections.Generic.List[object]
    $order = New-Object System.Collections.Generic.List[string]
    $invalidated = New-Object System.Collections.Generic.List[object]
    $gateLog = Join-Path $reports 'compare/idle-gate.log'

    # Runs one variant once under the idle-machine gate, repeating an attempt that background load
    # invalidated.
    function Invoke-Run([string]$Variant, [int]$Number) {
        $runLabel = "$Label-$($Variant.ToLowerInvariant())-$Number"
        $order.Add($runLabel)
        for ($attempt = 1; ; $attempt++) {
            Wait-IdleMachine $gateLog
            Write-Host "=== $runLabel (attempt $attempt) ===" -ForegroundColor Cyan
            $before = Get-BackgroundCpuSnapshot
            $started = Get-Date
            $run = $null; $failure = $null
            try { $run = Invoke-MeasuredRun $Variant $runLabel } catch { $failure = $_ }
            $load = Measure-BackgroundLoad $before (Get-BackgroundCpuSnapshot) ((Get-Date) - $started).TotalSeconds $BlockingProcess
            # A failure while the services start (for example, another program took a port) happens
            # before any load, so the attempt is set aside and repeated like a loaded one.
            $startupFailure = $failure -and "$($failure.Exception.Message)" -match 'while waiting for|Benchmark ports are in use'
            if ($load.cores -le $IdleCpuCores -and $load.blocking.Count -eq 0 -and -not $startupFailure) {
                if ($failure) { throw $failure }
                $run | Add-Member -NotePropertyName backgroundCpuCores -NotePropertyValue $load.cores
                # Kept beside the run so a resumed comparison still reports it.
                [IO.File]::WriteAllText((Join-Path $reports "$runLabel/idle-gate.json"),
                    ([ordered]@{ backgroundCpuCores = $load.cores; heaviest = $load.heaviest } | ConvertTo-Json),
                    (New-Object System.Text.UTF8Encoding $false))
                break
            }
            $reason = "background CPU $($load.cores) cores ($($load.heaviest -join ', '))"
            if ($load.blocking.Count -gt 0) { $reason = "blocking process ran: $($load.blocking -join ', '); $reason" }
            if ($startupFailure) { $reason = "startup failed: $($failure.Exception.Message); $reason" }
            $moved = Join-Path $compareDir "invalidated/$runLabel-attempt$attempt"
            New-Item -ItemType Directory -Force -Path (Split-Path $moved) | Out-Null
            if (Test-Path (Join-Path $reports $runLabel)) { Move-Item (Join-Path $reports $runLabel) $moved }
            if ($BaselineWorktree) {
                $baselineRun = Join-Path (Resolve-Path $BaselineWorktree).Path "load-client/reports/transaction-optimization/$runLabel"
                if (Test-Path $baselineRun) { Move-Item $baselineRun "$moved-baseline" }
            }
            $invalidated.Add([pscustomobject][ordered]@{ label = $runLabel; attempt = $attempt; reason = $reason
                failed = [bool]$failure; movedTo = "invalidated/$runLabel-attempt$attempt" })
            Write-IdleGateLog $gateLog "invalidated $runLabel attempt ${attempt}: $reason"
            if ($attempt -ge $MaxRunAttempts) { throw "Run $runLabel was invalidated $attempt times; the comparison is aborted" }
        }
        $run | Add-Member -NotePropertyName label -NotePropertyValue $runLabel
        if ($Variant -eq 'A') { $runsA.Add($run) } else { $runsB.Add($run) }
    }

    function Invoke-MeasuredRun([string]$Variant, [string]$runLabel) {
        if ($Variant -eq 'A' -and $BaselineWorktree) {
            $baselineServer = Join-Path (Resolve-Path $BaselineWorktree).Path 'server'
            if (-not $env:DB_USER) { throw 'The baseline harness needs DB_USER and DB_PASSWORD' }
            # A separate powershell.exe runs the baseline harness as it ran for T001: its native tools
            # write stderr to files instead of PowerShell error records, which its own 'Stop' preference
            # would turn into failures here, and its environment variables cannot leak into CP4 runs.
            $harnessLog = Join-Path $compareDir "$runLabel-harness"
            # Start-Process joins arguments with spaces and does not quote them, so quote each one.
            $arguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $baselineServer 'benchmark-scan.ps1'),
                '-MySqlExe', $mysql, '-NginxBin', $NginxBin, '-JavaExe', $JavaExe, '-Variant', 'retained', '-Workload', $Workload,
                '-TransactionPoolSize', '32', '-TransactionPoolMaxSize', '64', '-OutputLabel', $runLabel) | ForEach-Object { '"' + $_ + '"' }
            $harness = Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -WorkingDirectory $baselineServer -WindowStyle Hidden -PassThru `
                -ArgumentList ($arguments -join ' ') -RedirectStandardOutput "$harnessLog-stdout.txt" -RedirectStandardError "$harnessLog-stderr.txt"
            # Windows PowerShell 5.1 reports a null ExitCode unless the process handle is opened before exit.
            $null = $harness.Handle
            $harness.WaitForExit()
            if ($harness.ExitCode -ne 0) { throw "Baseline harness for $runLabel exited with code $($harness.ExitCode); see $harnessLog-stderr.txt" }
            $baselineDir = Join-Path (Resolve-Path $BaselineWorktree).Path "load-client/reports/transaction-optimization/$runLabel/$Workload"
            $invariants = Join-Path $baselineDir "$Workload-invariants.txt"
            if (-not (Test-Path $invariants) -or -not (Select-String -Path $invariants -Pattern 'PASS' -Quiet)) {
                throw "Baseline run $runLabel did not pass its invariant check"
            }
            $copy = Join-Path $reports $runLabel
            New-Item -ItemType Directory -Force -Path $copy | Out-Null
            Copy-Item (Join-Path $baselineDir '*') $copy
            $run = Read-BaselineRun $copy
        } else {
            $extra = $VariantA
            if ($Variant -eq 'B') { $extra = $VariantB }
            $arguments = @{ Workload = $Workload; OutputLabel = $runLabel; MySqlExe = $mysql; JavaExe = $JavaExe; ExtraEnv = $extra }
            if ($NginxBin) { $arguments.NginxBin = $NginxBin }
            & (Join-Path $server 'benchmark-microservices.ps1') @arguments | ForEach-Object { Write-Host $_ }
            $evidencePath = Join-Path $reports "$runLabel/evidence.json"
            if (-not (Test-Path $evidencePath)) { throw "Run $runLabel left no evidence.json" }
            $run = Read-MicroservicesRun $evidencePath
            if (-not $run.comparable) { throw "Run $runLabel is not comparable; the comparison is aborted" }
        }
        return $run
    }

    # A finished run of an earlier, interrupted invocation: its evidence (or, for the baseline, its
    # copied report and passing invariants) is complete and the run passed its checks.
    function Test-CompletedRun([string]$Variant, [string]$RunLabel) {
        $dir = Join-Path $reports $RunLabel
        if ($Variant -eq 'A' -and $BaselineWorktree) {
            $invariants = Join-Path $dir "$Workload-invariants.txt"
            return (Test-Path (Join-Path $dir 'samples.csv')) -and (Test-Path $invariants) -and
                (Select-String -Path $invariants -Pattern 'PASS' -Quiet) -and
                [bool](Get-ChildItem $dir -Filter 'report-*.json' -ErrorAction SilentlyContinue)
        }
        $evidence = Join-Path $dir 'evidence.json'
        if (-not (Test-Path $evidence)) { return $false }
        return (Read-MicroservicesRun $evidence).comparable
    }

    function Add-ResumedRun([string]$Variant, [int]$Number) {
        $runLabel = "$Label-$($Variant.ToLowerInvariant())-$Number"
        $dir = Join-Path $reports $runLabel
        if ($Variant -eq 'A' -and $BaselineWorktree) { $run = Read-BaselineRun $dir }
        else { $run = Read-MicroservicesRun (Join-Path $dir 'evidence.json') }
        $cores = $null
        $gate = Join-Path $dir 'idle-gate.json'
        if (Test-Path $gate) { $cores = (Get-Content -Raw $gate | ConvertFrom-Json).backgroundCpuCores }
        $run | Add-Member -NotePropertyName backgroundCpuCores -NotePropertyValue $cores
        $run | Add-Member -NotePropertyName resumed -NotePropertyValue $true
        $run | Add-Member -NotePropertyName label -NotePropertyValue $runLabel
        $order.Add($runLabel)
        if ($Variant -eq 'A') { $runsA.Add($run) } else { $runsB.Add($run) }
    }

    # Resume: keep the leading complete pairs of an interrupted invocation, in their original order,
    # and move every other run of this label aside so it is repeated.
    $resumedPairs = 0
    for ($pair = 1; $pair -le $Pairs; $pair++) {
        if (-not ((Test-CompletedRun 'A' "$Label-a-$pair") -and (Test-CompletedRun 'B' "$Label-b-$pair"))) { break }
        if ($pair % 2 -eq 1) { Add-ResumedRun 'A' $pair; Add-ResumedRun 'B' $pair }
        else { Add-ResumedRun 'B' $pair; Add-ResumedRun 'A' $pair }
        $resumedPairs = $pair
    }
    $pattern = '^' + [regex]::Escape($Label) + '-[ab]-(\d+)$'
    $leftovers = @(Get-ChildItem $reports -Directory | Where-Object {
        $_.Name -match $pattern -and [int]$Matches[1] -gt $resumedPairs } |
        ForEach-Object { [pscustomobject]@{ item = $_; suffix = '' } })
    if ($BaselineWorktree) {
        $baselineReports = Join-Path (Resolve-Path $BaselineWorktree).Path 'load-client/reports/transaction-optimization'
        if (Test-Path $baselineReports) {
            $leftovers += @(Get-ChildItem $baselineReports -Directory | Where-Object {
                $_.Name -match $pattern -and [int]$Matches[1] -gt $resumedPairs } |
                ForEach-Object { [pscustomobject]@{ item = $_; suffix = '-baseline' } })
        }
    }
    foreach ($leftover in $leftovers) {
        $name = $leftover.item.Name
        $target = Join-Path $compareDir "interrupted/$name-$(Get-Date -Format yyyyMMddHHmmss)$($leftover.suffix)"
        New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
        Move-Item $leftover.item.FullName $target
        Write-IdleGateLog $gateLog "moved interrupted run $name$($leftover.suffix) to interrupted/"
    }
    if ($resumedPairs -gt 0) { Write-IdleGateLog $gateLog "resumed $Label with $resumedPairs complete pairs" }

    for ($pair = $resumedPairs + 1; $pair -le $Pairs; $pair++) {
        if ($pair % 2 -eq 1) { Invoke-Run 'A' $pair; Invoke-Run 'B' $pair }
        else { Invoke-Run 'B' $pair; Invoke-Run 'A' $pair }
    }
    $decision = Get-Decision $runsA $runsB

    $metrics = @('totalTransactions', 'scanP99Ms', 'completeP99Ms') + $script:ResourceMetrics
    $summary = [ordered]@{}
    foreach ($metric in $metrics) {
        $summary[$metric] = [ordered]@{ A = Get-VariantSummary $runsA $metric; B = Get-VariantSummary $runsB $metric }
    }
    $variantADescription = if ($BaselineWorktree) { "baseline worktree $BaselineWorktree (benchmark-scan.ps1, T001 arguments)" } else { $VariantA }
    $result = [ordered]@{
        label = $Label
        workload = $Workload
        variants = [ordered]@{ A = $variantADescription; B = $VariantB }
        runOrder = $order
        idleGate = [ordered]@{ idleCpuCores = $IdleCpuCores; blockingProcesses = $BlockingProcess; invalidatedRuns = $invalidated }
        resumedPairs = $resumedPairs
        runs = [ordered]@{ A = $runsA; B = $runsB }
        summary = $summary
        decision = $decision
    }
    [IO.File]::WriteAllText((Join-Path $compareDir 'compare.json'), ($result | ConvertTo-Json -Depth 10),
        (New-Object System.Text.UTF8Encoding $false))

    $tx = $summary.totalTransactions
    $describeA = if ($BaselineWorktree) { 'CP0 baseline' } else { ($VariantA.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ', ' }
    $describeB = ($VariantB.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ', '
    if (-not $describeA) { $describeA = 'defaults' }
    if (-not $describeB) { $describeB = 'defaults' }
    $latency = 'none'
    if ($decision.latencyRegressions.Count -gt 0) { $latency = $decision.latencyRegressions -join ', ' }
    $row = '| `{0}` | {1} | A: {2}; B: {3} | {4} | {5} | {6} | {7:+0.00;-0.00;0.00}% | {8} of {4} | {9} | {10} |' -f $Label, $Workload,
        $describeA, $describeB, $decision.pairs, (Format-Summary $tx.A), (Format-Summary $tx.B), $decision.changePercent,
        $decision.pairsAheadB, $decision.classification, $latency
    Set-Content -Encoding UTF8 -Path (Join-Path $compareDir 'compare.md') -Value @(
        '| Label | Workload | Variants | Pairs | A median (range) | B median (range) | Delta | B ahead | Classification | p99 regression |',
        '|---|---|---|---:|---:|---:|---:|---:|---|---|',
        $row)
    Write-Host $row
    Write-Host "Recorded $compareDir/compare.json" -ForegroundColor Green
}

if ($MyInvocation.InvocationName -ne '.') { Invoke-Comparison }
