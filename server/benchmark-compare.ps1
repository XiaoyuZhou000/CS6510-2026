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

    Decision rule:
      1. Clear: after -Pairs, the totalTransactions medians differ by at least 3% and the ranges do
         not overlap. The faster variant wins.
      2. Close: otherwise run up to -MaxPairs, deciding on totalTransactions only once the one-sided
         Mann-Whitney U is at or below the 5% critical value for that number of pairs
         (n=3: 0, n=4: 1, n=5: 4).
      3. Tie: otherwise compare Scan p99, then Complete p99, then resource use (pool borrow-wait p99,
         inventory.apply.lockWait p99, Innodb_row_lock_waits delta, Com_commit per completed
         transaction), deciding on a metric only when its ranges do not overlap. If none decides,
         keep variant A, the incumbent.

    Output: load-client/reports/microservices-architecture/compare/<label>/compare.json, plus a
    Markdown row for checkpoints.md, printed and saved as compare.md.

    Dot-source this file (. ./benchmark-compare.ps1) to load its statistics functions without
    running a comparison.

.EXAMPLE
    ./benchmark-compare.ps1 -Workload default -Label cp3-cache -VariantA @{} -VariantB @{ TRANSACTION_CACHE_MAX_ENTRIES = '0' } -NginxBin C:\nginx\nginx.exe
    ./benchmark-compare.ps1 -Workload stress -Label cp4-stress -BaselineWorktree ..\..\cs6510-cp0 -Pairs 3 -MaxPairs 3 -NginxBin C:\nginx\nginx.exe
#>
param(
    [ValidateSet('default', 'stress')][string]$Workload,
    [string]$Label,
    [hashtable]$VariantA = @{},
    [hashtable]$VariantB = @{},
    [string]$BaselineWorktree,
    [ValidateRange(1, 20)][int]$Pairs = 3,
    [ValidateRange(1, 20)][int]$MaxPairs = 5,
    [string]$MySqlExe,
    [string]$NginxBin,
    [string]$JavaExe
)

# One-sided 5% critical values of U for n against n (equal group sizes).
$script:MannWhitneyCritical = @{ 3 = 0; 4 = 1; 5 = 4; 6 = 7; 7 = 11; 8 = 15 }
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

function Test-RangesOverlap($A, $B) {
    return -not ($A.max -lt $B.min -or $B.max -lt $A.min)
}

# U counts the pairs in which the loser beats the winner (ties count one half); a small U supports
# "winner > loser".
function Get-MannWhitneyU([double[]]$Winner, [double[]]$Loser) {
    $u = 0.0
    foreach ($l in $Loser) {
        foreach ($w in $Winner) {
            if ($l -gt $w) { $u += 1 } elseif ($l -eq $w) { $u += 0.5 }
        }
    }
    return $u
}

function Get-VariantSummary($Runs, [string]$Metric) {
    $values = @($Runs | ForEach-Object { $_.$Metric } | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($values.Count -ne @($Runs).Count -or $values.Count -eq 0) { return $null }
    return [pscustomobject]@{ median = Get-Median $values; range = Get-Range $values; values = $values }
}

<#
    Applies the decision rule to completed runs. $Stage is 'pairs' after the initial -Pairs or
    'extended' once more pairs ran. Returns $null while a close result can still be extended.
#>
function Get-Decision($RunsA, $RunsB, [string]$Stage, [bool]$CanExtend) {
    $a = Get-VariantSummary $RunsA 'totalTransactions'
    $b = Get-VariantSummary $RunsB 'totalTransactions'
    $n = @($RunsA).Count
    $faster = 'A'; $slower = 'B'; $fastSummary = $a; $slowSummary = $b
    if ($b.median -gt $a.median) { $faster = 'B'; $slower = 'A'; $fastSummary = $b; $slowSummary = $a }
    $difference = 0.0
    if ($a.median -gt 0) { $difference = [Math]::Abs($b.median - $a.median) / $a.median * 100 }
    $u = Get-MannWhitneyU $fastSummary.values $slowSummary.values
    $critical = $null
    if ($script:MannWhitneyCritical.ContainsKey($n)) { $critical = $script:MannWhitneyCritical[$n] }
    function New-Decision([string]$Winner, [string]$Rule, $DecidingMetric) {
        return [pscustomobject][ordered]@{
            pairs = $n; medianDifferencePercent = [Math]::Round($difference, 2); u = $u; uCritical = $critical
            winner = $Winner; rule = $Rule; decidingMetric = $DecidingMetric
        }
    }

    if ($Stage -eq 'pairs') {
        if ($difference -ge 3 -and -not (Test-RangesOverlap $a.range $b.range)) {
            return New-Decision $faster 'clear' 'totalTransactions'
        }
        # Close: extend toward -MaxPairs before any significance test.
        if ($CanExtend) { return $null }
    }
    if ($null -ne $critical -and $u -le $critical -and $fastSummary.median -ne $slowSummary.median) {
        return New-Decision $faster 'mann-whitney' 'totalTransactions'
    }
    if ($CanExtend) { return $null }

    # Tie: latency first, then resource use; lower is better, and only non-overlapping ranges decide.
    foreach ($metric in @('scanP99Ms', 'completeP99Ms') + $script:ResourceMetrics) {
        $ma = Get-VariantSummary $RunsA $metric
        $mb = Get-VariantSummary $RunsB $metric
        if ($null -eq $ma -or $null -eq $mb) { continue }
        if (-not (Test-RangesOverlap $ma.range $mb.range)) {
            $winner = 'A'
            if ($mb.median -lt $ma.median) { $winner = 'B' }
            return New-Decision $winner 'tie-break' $metric
        }
    }
    return New-Decision 'A' 'no-decision-keep-incumbent' $null
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

function Format-Summary($Summary) {
    if ($null -eq $Summary) { return 'n/a' }
    return ('{0} ({1}-{2})' -f $Summary.median, $Summary.range.min, $Summary.range.max)
}

function Invoke-Comparison {
    $ErrorActionPreference = 'Stop'
    if (-not $Workload) { throw '-Workload default|stress is required' }
    if (-not $Label -or $Label -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') { throw '-Label is required (letters, digits, . _ -)' }
    if ($MaxPairs -lt $Pairs) { throw '-MaxPairs must be at least -Pairs' }
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

    function Invoke-Run([string]$Variant, [int]$Number) {
        $runLabel = "$Label-$($Variant.ToLowerInvariant())-$Number"
        $order.Add($runLabel)
        Write-Host "=== $runLabel ===" -ForegroundColor Cyan
        if ($Variant -eq 'A' -and $BaselineWorktree) {
            $baselineServer = Join-Path (Resolve-Path $BaselineWorktree).Path 'server'
            if (-not $env:DB_USER) { throw 'The baseline harness needs DB_USER and DB_PASSWORD' }
            Push-Location $baselineServer
            try {
                & (Join-Path $baselineServer 'benchmark-scan.ps1') -MySqlExe $mysql -NginxBin $NginxBin -JavaExe $JavaExe `
                    -Variant retained -Workload $Workload -TransactionPoolSize 32 -TransactionPoolMaxSize 64 -OutputLabel $runLabel
            } finally {
                Pop-Location
            }
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
            & (Join-Path $server 'benchmark-microservices.ps1') @arguments
            $evidencePath = Join-Path $reports "$runLabel/evidence.json"
            if (-not (Test-Path $evidencePath)) { throw "Run $runLabel left no evidence.json" }
            $run = Read-MicroservicesRun $evidencePath
            if (-not $run.comparable) { throw "Run $runLabel is not comparable; the comparison is aborted" }
        }
        $run | Add-Member -NotePropertyName label -NotePropertyValue $runLabel
        if ($Variant -eq 'A') { $runsA.Add($run) } else { $runsB.Add($run) }
    }

    $decision = $null
    for ($pair = 1; $pair -le $MaxPairs; $pair++) {
        if ($pair % 2 -eq 1) { Invoke-Run 'A' $pair; Invoke-Run 'B' $pair }
        else { Invoke-Run 'B' $pair; Invoke-Run 'A' $pair }
        if ($pair -lt $Pairs) { continue }
        $stage = 'extended'
        if ($pair -eq $Pairs) { $stage = 'pairs' }
        $decision = Get-Decision $runsA $runsB $stage ($pair -lt $MaxPairs)
        if ($null -ne $decision) { break }
    }

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
    $row = '| `{0}` | {1} | A: {2}; B: {3} | {4} | {5} | {6} | {7} | {8} ({9}) | {10} |' -f $Label, $Workload, $describeA, $describeB,
        $decision.pairs, (Format-Summary $tx.A), (Format-Summary $tx.B), $decision.u, $decision.winner, $decision.rule, $decision.decidingMetric
    Set-Content -Encoding UTF8 -Path (Join-Path $compareDir 'compare.md') -Value @(
        '| Label | Workload | Variants | Pairs | A median (range) | B median (range) | U | Decision | Deciding metric |',
        '|---|---|---|---:|---:|---:|---:|---|---|',
        $row)
    Write-Host $row
    Write-Host "Recorded $compareDir/compare.json" -ForegroundColor Green
}

if ($MyInvocation.InvocationName -ne '.') { Invoke-Comparison }
