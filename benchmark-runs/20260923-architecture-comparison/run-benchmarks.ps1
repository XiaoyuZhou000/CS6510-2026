param(
    [int]$Trials = 3
)

$ErrorActionPreference = "Stop"

$repo = "E:\Wen\学习\NEU\CS6510\CS6510-2026"
$bench = Join-Path $repo "benchmark-runs\20260923-architecture-comparison"
$mysql = "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe"
$schema = Join-Path $repo "db\init.sql"
$serverPort = 18080
$databasePort = 3310

$architectures = @(
    [pscustomobject]@{
        Name = "monolithic"
        Root = Join-Path $bench "monolithic"
        MainClass = "Main"
    },
    [pscustomobject]@{
        Name = "layered"
        Root = $repo
        MainClass = "api.Main"
    }
)

$workloads = @(
    [pscustomobject]@{ Name = "default"; Stations = 10; Duration = 60 },
    [pscustomobject]@{ Name = "stress"; Stations = 100; Duration = 120 }
)

function Reset-Database {
    Get-Content -Raw -Encoding UTF8 $schema |
        & $mysql -h 127.0.0.1 -P $databasePort -u root --default-character-set=utf8mb4
    if ($LASTEXITCODE -ne 0) {
        throw "Database reset failed with exit code $LASTEXITCODE"
    }
}

function Start-BenchmarkServer($architecture, $logBase) {
    $psi = [Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = (Get-Command java).Source
    $psi.WorkingDirectory = Join-Path $architecture.Root "server"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.Environment["DB_PASSWORD"] = ""
    $psi.Environment["DB_POOL_SIZE"] = "10"
    foreach ($argument in @(
        "-cp",
        "out/main;lib/*",
        $architecture.MainClass,
        "$serverPort",
        "127.0.0.1",
        "$databasePort",
        "cs6510_selfcheckout",
        "root"
    )) {
        $psi.ArgumentList.Add($argument)
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $psi
    if (-not $process.Start()) {
        throw "Failed to start $($architecture.Name) server"
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        if ($process.HasExited) {
            $stdout = $stdoutTask.GetAwaiter().GetResult()
            $stderr = $stderrTask.GetAwaiter().GetResult()
            Set-Content -LiteralPath "$logBase.stdout.log" -Value $stdout
            Set-Content -LiteralPath "$logBase.stderr.log" -Value $stderr
            throw "$($architecture.Name) server exited during startup with code $($process.ExitCode)"
        }
        try {
            Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri "http://127.0.0.1:$serverPort/items" | Out-Null
            return [pscustomobject]@{
                Process = $process
                StdoutTask = $stdoutTask
                StderrTask = $stderrTask
                LogBase = $logBase
            }
        } catch {
            Start-Sleep -Milliseconds 250
        }
    } while ([DateTime]::UtcNow -lt $deadline)

    $process.Kill($true)
    throw "$($architecture.Name) server did not become ready within 30 seconds"
}

function Stop-BenchmarkServer($handle) {
    if (-not $handle.Process.HasExited) {
        $handle.Process.Kill($true)
        $handle.Process.WaitForExit()
    }
    Set-Content -LiteralPath "$($handle.LogBase).stdout.log" -Value $handle.StdoutTask.GetAwaiter().GetResult()
    Set-Content -LiteralPath "$($handle.LogBase).stderr.log" -Value $handle.StderrTask.GetAwaiter().GetResult()
    $handle.Process.Dispose()
}

foreach ($architecture in $architectures) {
    foreach ($workload in $workloads) {
        $reportDirectory = Join-Path $bench "reports\$($architecture.Name)\$($workload.Name)"
        $logDirectory = Join-Path $bench "logs\$($architecture.Name)\$($workload.Name)"
        New-Item -ItemType Directory -Force -Path $reportDirectory, $logDirectory | Out-Null

        for ($trial = 1; $trial -le $Trials; $trial++) {
            Write-Host "[$($architecture.Name)] [$($workload.Name)] trial $trial/${Trials}: reset"
            Reset-Database
            $logBase = Join-Path $logDirectory ("trial-{0}" -f $trial)
            $server = Start-BenchmarkServer $architecture $logBase
            try {
                Write-Host "[$($architecture.Name)] [$($workload.Name)] trial $trial/${Trials}: load"
                $clientRoot = Join-Path $architecture.Root "load-client"
                & java -cp (Join-Path $clientRoot "out") Main `
                    "--baseUrl=http://127.0.0.1:$serverPort" `
                    "--stations=$($workload.Stations)" `
                    "--duration=$($workload.Duration)" `
                    "--reportDir=$reportDirectory"
                if ($LASTEXITCODE -ne 0) {
                    throw "Load client failed with exit code $LASTEXITCODE"
                }
            } finally {
                Stop-BenchmarkServer $server
            }
        }
    }
}

Write-Host "All benchmark trials completed."
