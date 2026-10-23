# CP3 tuning A/B campaign driver (T123). Screens each variant with one pair per workload, drops it if
# it loses by more than 5% on both, otherwise compares it with -Pairs 3 -MaxPairs 5 per workload.
param([string[]]$Only)
$ErrorActionPreference = 'Stop'
$repo = 'E:\Wen\学习\NEU\CS6510\CS6510-2026'
$server = Join-Path $repo 'server'
$reports = Join-Path $repo 'load-client/reports/microservices-architecture'
foreach ($name in 'TRANSACTION_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'ANALYTICS_DB_PASSWORD') {
    [Environment]::SetEnvironmentVariable($name, [Environment]::GetEnvironmentVariable($name, 'User'), 'Process')
}
$common = @{
    MySqlExe = 'C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe'
    NginxBin = 'C:\Users\18422\AppData\Local\Temp\cs6510-nginx\nginx-1.31.6\nginx.exe'
    JavaExe  = 'C:\Program Files\Java\jdk-21\bin\java.exe'
    IdleCpuCores = 1.0
    BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR')
}
$variants = [ordered]@{
    'ps'       = @{ DB_SERVER_PREPARED_STATEMENTS = 'true' }
    'inv10-32' = @{ INVENTORY_DB_POOL_SIZE = '10'; INVENTORY_DB_POOL_MAX_SIZE = '32' }
    'inv24-64' = @{ INVENTORY_DB_POOL_SIZE = '24'; INVENTORY_DB_POOL_MAX_SIZE = '64' }
    'tx48-80'  = @{ TRANSACTION_DB_POOL_SIZE = '48'; TRANSACTION_DB_POOL_MAX_SIZE = '80' }
    'cacheoff' = @{ TRANSACTION_CACHE_MAX_ENTRIES = '0' }
    'coalesce100' = @{ OUTBOX_COALESCE_MS = '100' }
}
$log = Join-Path $reports 'compare/cp3-campaign.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
function Log([string]$Text) {
    $line = "$(Get-Date -Format s) $Text"
    Write-Host $line
    # A reader may hold the log open briefly; retry rather than abort the campaign.
    for ($try = 0; $try -lt 20; $try++) {
        try { [IO.File]::AppendAllText($log, $line + "`r`n", (New-Object Text.UTF8Encoding $false)); return } catch { Start-Sleep -Milliseconds 250 }
    }
}

function Compare-Variant([string]$Label, [string]$Workload, [hashtable]$Extra, [int]$Pairs, [int]$MaxPairs) {
    $file = Join-Path $reports "compare/$Label/compare.json"
    if (-not (Test-Path $file)) {
        Log "start $Label"
        try {
            & (Join-Path $server 'benchmark-compare.ps1') -Workload $Workload -Label $Label -VariantA @{} -VariantB $Extra `
                -Pairs $Pairs -MaxPairs $MaxPairs @common *> (Join-Path $reports "compare/$Label.console.txt")
        } catch {
            Log "FAILED $Label : $($_.Exception.Message)"
            return $null
        }
    }
    if (-not (Test-Path $file)) { Log "FAILED $Label : no compare.json"; return $null }
    $result = Get-Content -Raw $file | ConvertFrom-Json
    $a = $result.summary.totalTransactions.A.median; $b = $result.summary.totalTransactions.B.median
    $change = [Math]::Round(($b - $a) / $a * 100, 2)
    Log ("done {0}: A={1} B={2} change={3}% winner={4} rule={5} metric={6}" -f $Label, $a, $b, $change,
        $result.decision.winner, $result.decision.rule, $result.decision.decidingMetric)
    return [pscustomobject]@{ change = $change; winner = $result.decision.winner }
}

foreach ($entry in $variants.GetEnumerator()) {
    if ($Only -and $Only -notcontains $entry.Key) { continue }
    $screen = @{}
    foreach ($workload in 'default', 'stress') {
        $screen[$workload] = Compare-Variant "cp3-screen-$($entry.Key)-$workload" $workload $entry.Value 1 1
    }
    if ($null -eq $screen.default -or $null -eq $screen.stress) { Log "REJECT $($entry.Key): a screening run failed"; continue }
    if ($screen.default.change -lt -5 -and $screen.stress.change -lt -5) {
        Log "DROP $($entry.Key): loses by more than 5% on both workloads at screening"
        continue
    }
    foreach ($workload in 'default', 'stress') {
        $null = Compare-Variant "cp3-$($entry.Key)-$workload" $workload $entry.Value 3 5
    }
}
Log 'campaign finished'
