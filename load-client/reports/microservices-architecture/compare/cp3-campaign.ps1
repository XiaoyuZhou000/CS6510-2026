# CP3 tuning campaign driver (T123), streamlined policy of 2026-10-22 (research R22).
#
# For each remaining variant: screen it with one interleaved pair on each relevant workload, confirm
# it with three fresh pairs only where the screen is promising (delta >= +5%), and give a confirmed
# variant a default-workload regression check when the default was not already measured. A variant
# is kept only with a confirmed improvement, no regression on any measured workload, and no p99
# regression. Comparisons already recorded (compare.json) are reused, so the driver can be rerun
# after an interruption. The variants measured under the earlier five-pair rule (server prepared
# statements, Inventory pool 10/32) are recorded by cp3-campaign-v1.ps1 and not rerun.
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
    # 2.0 of 24 logical cores: 1.0 rejected runs for ordinary desktop load (about 1.1-1.5 cores of
    # chat, browser, and helper apps), while the games that invalidated earlier runs are blocked by name.
    IdleCpuCores = 2.0
    BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR')
}
# Pool sizes only matter under stress: at 10 stations peak occupancy stays below every initial pool.
$variants = [ordered]@{
    'inv24-64'    = @{ env = @{ INVENTORY_DB_POOL_SIZE = '24'; INVENTORY_DB_POOL_MAX_SIZE = '64' }; workloads = @('stress') }
    'tx48-80'     = @{ env = @{ TRANSACTION_DB_POOL_SIZE = '48'; TRANSACTION_DB_POOL_MAX_SIZE = '80' }; workloads = @('stress') }
    'coalesce100' = @{ env = @{ OUTBOX_COALESCE_MS = '100' }; workloads = @('default', 'stress') }
}
$log = Join-Path $reports 'compare/cp3-campaign.log'
$decisionsFile = Join-Path $reports 'compare/cp3-decisions.json'
function Log([string]$Text) {
    $line = "$(Get-Date -Format s) $Text"
    Write-Host $line
    # A reader may hold the log open briefly; retry rather than abort the campaign.
    for ($try = 0; $try -lt 20; $try++) {
        try { [IO.File]::AppendAllText($log, $line + "`r`n", (New-Object Text.UTF8Encoding $false)); return } catch { Start-Sleep -Milliseconds 250 }
    }
}

# Runs (or reuses) one comparison and returns its decision, or $null when a run failed.
function Compare-Variant([string]$Label, [string]$Workload, [hashtable]$Extra, [int]$Pairs) {
    $file = Join-Path $reports "compare/$Label/compare.json"
    $fresh = $false
    if (-not (Test-Path $file)) {
        Log "start $Label ($Pairs pair(s))"
        try {
            & (Join-Path $server 'benchmark-compare.ps1') -Workload $Workload -Label $Label -VariantA @{} -VariantB $Extra `
                -Pairs $Pairs @common *> (Join-Path $reports "compare/$Label.console.txt")
        } catch {
            Log "FAILED $Label : $($_.Exception.Message)"
            return $null
        }
        if (-not (Test-Path $file)) { Log "FAILED $Label : no compare.json"; return $null }
        $fresh = $true
    }
    $result = Get-Content -Raw $file | ConvertFrom-Json
    $d = $result.decision
    $latency = 'none'
    if (@($d.latencyRegressions).Count -gt 0) { $latency = @($d.latencyRegressions) -join ', ' }
    if ($fresh) {
        Log ("done {0}: A={1} B={2} change={3}% class={4} ahead={5}/{6} p99={7}" -f $Label, $result.summary.totalTransactions.A.median,
            $result.summary.totalTransactions.B.median, $d.changePercent, $d.classification, $d.pairsAheadB, $d.pairs, $latency)
    }
    return $d
}

$decisions = [ordered]@{}
$paused = $false
if (Test-Path $decisionsFile) {
    $previous = Get-Content -Raw $decisionsFile | ConvertFrom-Json
    foreach ($p in $previous.PSObject.Properties) { $decisions[$p.Name] = $p.Value }
}
foreach ($entry in $variants.GetEnumerator()) {
    $name = $entry.Key
    if ($decisions.Contains($name)) { continue }
    $settings = $entry.Value.env
    $screens = [ordered]@{}
    $failed = $false
    foreach ($workload in $entry.Value.workloads) {
        $screens[$workload] = Compare-Variant "cp3-screen-$name-$workload" $workload $settings 1
        if ($null -eq $screens[$workload]) { $failed = $true }
    }
    # A failed run is not a measurement: stop here so a rerun of the driver repeats it.
    if ($failed) { Log "PAUSED $name : a screening comparison failed; rerun the driver after fixing the cause"; $paused = $true; break }
    $measured = @($screens.Values)
    $promising = @($screens.Keys | Where-Object { $screens[$_].classification -eq 'promising' })
    $verdict = $null
    if ($promising.Count -eq 0) {
        $verdict = [ordered]@{ keep = $false; reason = 'no screen reached +5%: no demonstrated improvement' }
    } else {
        $confirmed = @()
        foreach ($workload in $promising) {
            $confirmation = Compare-Variant "cp3-$name-$workload" $workload $settings 3
            if ($null -eq $confirmation) { $failed = $true; break }
            $measured += $confirmation
            if ($confirmation.classification -eq 'improvement') { $confirmed += $workload }
        }
        if ($failed) { Log "PAUSED $name : a confirmation comparison failed; rerun the driver after fixing the cause"; $paused = $true; break }
        if ($confirmed.Count -eq 0) {
            $verdict = [ordered]@{ keep = $false; reason = "promising on $($promising -join ', ') but not confirmed in three fresh pairs" }
        } elseif ($entry.Value.workloads -notcontains 'default') {
            $check = Compare-Variant "cp3-$name-default-check" 'default' $settings 1
            if ($null -eq $check) { Log "PAUSED $name : the default regression check failed; rerun the driver after fixing the cause"; $paused = $true; break }
            $measured += $check
        }
    }
    if ($null -eq $verdict) {
        $regressions = @($measured | Where-Object { $_.classification -eq 'regression' -or @($_.latencyRegressions).Count -gt 0 })
        if ($regressions.Count -gt 0) {
            $verdict = [ordered]@{ keep = $false; reason = 'confirmed, but a measured workload shows a totalTransactions or p99 regression' }
        } else {
            $verdict = [ordered]@{ keep = $true; reason = "confirmed improvement on $($confirmed -join ', ') with no regression" }
        }
    }
    $verdict.decidedAt = (Get-Date -Format s)
    $decisions[$name] = $verdict
    [IO.File]::WriteAllText($decisionsFile, ($decisions | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding $false))
    if ($verdict.keep) { Log "KEEP $name : $($verdict.reason)" } else { Log "REJECT $name : $($verdict.reason)" }
}
if (-not $paused) { Log 'campaign finished' }
