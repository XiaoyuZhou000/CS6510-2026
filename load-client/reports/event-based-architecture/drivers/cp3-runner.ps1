param([string[]]$Only = @())
# CP3 screening pairs (T163), run one after another. Each comparison resumes if rerun with the same label.
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
$plan = @(
    @{ Label = 'cp3-flush-default';    Workload = 'default'; A = @{ KAFKA_FLUSH_EVERY_MESSAGE = 'false' }; B = @{ KAFKA_FLUSH_EVERY_MESSAGE = 'true' } },
    @{ Label = 'cp3-flush-stress';     Workload = 'stress';  A = @{ KAFKA_FLUSH_EVERY_MESSAGE = 'false' }; B = @{ KAFKA_FLUSH_EVERY_MESSAGE = 'true' } },
    @{ Label = 'cp3-threads-default';  Workload = 'default'; A = @{ CHECKOUT_CONSUMER_THREADS = '8' }; B = @{ CHECKOUT_CONSUMER_THREADS = '4' } },
    @{ Label = 'cp3-threads-stress';   Workload = 'stress';  A = @{ CHECKOUT_CONSUMER_THREADS = '8' }; B = @{ CHECKOUT_CONSUMER_THREADS = '4' } },
    @{ Label = 'cp3-linger-default';   Workload = 'default'; A = @{ SCAN_PRODUCER_LINGER_MS = '5' }; B = @{ SCAN_PRODUCER_LINGER_MS = '20' } },
    @{ Label = 'cp3-coalesce-default'; Workload = 'default'; A = @{ OUTBOX_COALESCE_MS = '0' }; B = @{ OUTBOX_COALESCE_MS = '100' } }
)
Set-Location (Join-Path $repo 'server')
foreach ($item in $plan) {
    if ($Only.Count -gt 0 -and $Only -notcontains $item.Label) { continue }
    $done = Join-Path $repo "load-client/reports/event-based-architecture/compare/$($item.Label)/compare.json"
    if (Test-Path $done) { Write-Output "== $($item.Label): already complete"; continue }
    Write-Output "== $($item.Label) start $(Get-Date -Format s)"
    & ./benchmark-compare.ps1 -Workload $item.Workload -Label $item.Label -VariantA $item.A -VariantB $item.B @common
    Write-Output "== $($item.Label) end $(Get-Date -Format s) exit=$LASTEXITCODE ok=$?"
}
Write-Output "== CP3 runner finished $(Get-Date -Format s)"
