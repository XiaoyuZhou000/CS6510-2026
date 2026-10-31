param([string[]]$Only = @())
# CP4 (T164-T166): the designated official runs, then the CP0-versus-CP4 pairs.
$ErrorActionPreference = 'Continue'
$repo = 'E:\Wen\学习\NEU\CS6510\CS6510-2026'
$env:KAFKA_HOME = 'C:\Users\18422\AppData\Local\Temp\cs6510-kafka\kafka_2.13-4.1.0'
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21'
foreach ($name in 'TRANSACTION_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'ANALYTICS_DB_PASSWORD') {
    $value = [Environment]::GetEnvironmentVariable($name, 'User')
    if (-not $value) { $value = [Environment]::GetEnvironmentVariable($name, 'Machine') }
    if (-not $value) { throw "$name is not set" }
    Set-Item -Path "Env:$name" -Value $value
}
$common = @{
    NginxBin = 'C:\Users\18422\AppData\Local\Temp\cs6510-nginx\nginx-1.31.6\nginx.exe'
    MySqlExe = 'C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe'
    JavaExe = 'C:\Program Files\Java\jdk-21\bin\java.exe'
    IdleCpuCores = 2.0
    BlockingProcess = @('WardogsClient-Win64-Shipping', 'ROTTR', 'Arknights')
    Pairs = 1
}
$gate = @{ NginxBin = $common.NginxBin; MySqlExe = $common.MySqlExe; JavaExe = $common.JavaExe; IdleCpuCores = 2.0; BlockingProcess = $common.BlockingProcess }
Set-Location (Join-Path $repo 'server')
foreach ($run in @(@{ Workload = 'default'; Label = 'cp4-default-1' }, @{ Workload = 'stress'; Label = 'cp4-stress-1' })) {
    if ($Only.Count -gt 0 -and $Only -notcontains $run.Label) { continue }
    if (Test-Path (Join-Path $repo "load-client/reports/event-based-architecture/$($run.Label)/evidence.json")) { Write-Output "== $($run.Label): already done"; continue }
    Write-Output "== $($run.Label) start $(Get-Date -Format s)"
    & ./benchmark-event-based.ps1 -Workload $run.Workload -OutputLabel $run.Label @gate
    Write-Output "== $($run.Label) end $(Get-Date -Format s) ok=$?"
}
foreach ($workload in 'default', 'stress') {
    $label = "cp4-vs-cp0-$workload"
    if ($Only.Count -gt 0 -and $Only -notcontains $label) { continue }
    if (Test-Path (Join-Path $repo "load-client/reports/event-based-architecture/compare/$label/compare.json")) { Write-Output "== ${label}: already complete"; continue }
    Write-Output "== $label start $(Get-Date -Format s)"
    & ./benchmark-compare.ps1 -Workload $workload -Label $label -BaselineCommit 8891c57 @common
    Write-Output "== $label end $(Get-Date -Format s) exit=$LASTEXITCODE ok=$?"
}
Write-Output "== CP4 runner finished $(Get-Date -Format s)"
