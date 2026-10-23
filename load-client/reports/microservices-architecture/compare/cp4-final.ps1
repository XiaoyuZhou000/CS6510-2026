# CP0-versus-CP4 final comparison driver (T152; research R22, streamlined 2026-10-22).
#
# Prerequisites (done once, before this script): the CP0 worktree of commit 26c89af exists outside
# the repository at ..\cs6510-cp0, its server/benchmark-scan.ps1 carries the one-line T001 harness
# fix (the current server/benchmark-scan.ps1 differs from 26c89af by exactly that line), and it is
# built with server/build.sh and load-client/build.sh. DB_USER and DB_PASSWORD are set for the
# baseline harness; the service passwords come from the user environment.
#
# Runs three interleaved pairs per workload, default first, then stress. Variant A is CP0 through the
# worktree's own harness; variant B is CP4, the current build with its defaults. The official
# reports are designated in advance: cp4-final-default-b-1 and cp4-final-stress-b-1, the first CP4
# run of each workload. A completed comparison (compare.json) is reused, so the script can be rerun
# after an interruption.
$ErrorActionPreference = 'Stop'
$repo = 'E:\Wen\学习\NEU\CS6510\CS6510-2026'
$worktree = 'E:\Wen\学习\NEU\CS6510\cs6510-cp0'
$server = Join-Path $repo 'server'
$reports = Join-Path $repo 'load-client/reports/microservices-architecture'
foreach ($name in 'TRANSACTION_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'ANALYTICS_DB_PASSWORD') {
    [Environment]::SetEnvironmentVariable($name, [Environment]::GetEnvironmentVariable($name, 'User'), 'Process')
}
foreach ($name in 'DB_USER', 'DB_PASSWORD') {
    if (-not [Environment]::GetEnvironmentVariable($name, 'Process')) {
        [Environment]::SetEnvironmentVariable($name, [Environment]::GetEnvironmentVariable($name, 'Machine'), 'Process')
    }
}
foreach ($artifact in 'server/out/transaction.jar', 'server/out/inventory.jar', 'server/out/analytics.jar', 'load-client/out/Main.class') {
    if (-not (Test-Path (Join-Path $worktree $artifact))) { throw "The CP0 worktree is not built: missing $artifact" }
}
$common = @{
    MySqlExe = 'C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe'
    NginxBin = 'C:\Users\18422\AppData\Local\Temp\cs6510-nginx\nginx-1.31.6\nginx.exe'
    JavaExe  = 'C:\Program Files\Java\jdk-21\bin\java.exe'
    # Same gate as CP3 (tasks.md T136 revision): 2.0 of 24 logical cores, games blocked by name.
    IdleCpuCores = 2.0
    BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR')
    BaselineWorktree = $worktree
    Pairs = 3
}
$log = Join-Path $reports 'compare/cp4-final.log'
function Log([string]$Text) {
    $line = "$(Get-Date -Format s) $Text"
    Write-Host $line
    for ($try = 0; $try -lt 20; $try++) {
        try { [IO.File]::AppendAllText($log, $line + "`r`n", (New-Object Text.UTF8Encoding $false)); return } catch { Start-Sleep -Milliseconds 250 }
    }
}

foreach ($workload in 'default', 'stress') {
    $label = "cp4-final-$workload"
    $file = Join-Path $reports "compare/$label/compare.json"
    if (-not (Test-Path $file)) {
        Log "start $label (3 pairs; official report $label-b-1)"
        try {
            & (Join-Path $server 'benchmark-compare.ps1') -Workload $workload -Label $label -VariantA @{} -VariantB @{} @common *> (Join-Path $reports "compare/$label.console.txt")
        } catch {
            Log "FAILED $label : $($_.Exception.Message)"
            break
        }
    }
    if (-not (Test-Path $file)) { Log "FAILED $label : no compare.json"; break }
    $result = Get-Content -Raw $file | ConvertFrom-Json
    $d = $result.decision
    Log ("done {0}: CP0={1} CP4={2} change={3}% CP4 ahead={4}/{5}" -f $label, $result.summary.totalTransactions.A.median,
        $result.summary.totalTransactions.B.median, $d.changePercent, $d.pairsAheadB, $d.pairs)
}
Log 'cp4 final finished'
