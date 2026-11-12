<#
.SYNOPSIS
    Helpers shared by the benchmark harnesses (benchmark-event-based.ps1 and benchmark-notification.ps1). Dot-source it:
        . (Join-Path $PSScriptRoot 'benchmark-common.ps1')

.DESCRIPTION
    Dot-sourcing runs this file in the harness's own scope, so the functions read the harness's variables:
    $script:savedEnv and $script:running (hashtables the harness creates), $JavaExe, $server, $out, $IdleCpuCores,
    and $BlockingProcess.
#>

# Sets a process environment variable, remembering the first previous value so the harness can restore it.
function Set-ProcessEnv([string]$Name, [string]$Value) {
    if (-not $script:savedEnv.ContainsKey($Name)) {
        $script:savedEnv[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process')
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

function Get-Prop($Object, [string]$Name) {
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Test-Http([string]$Url, [int[]]$Accept) {
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
        return $Accept -contains [int]$response.StatusCode
    } catch {
        $response = $_.Exception.Response
        if ($null -ne $response) { return $Accept -contains [int]$response.StatusCode }
        return $false
    }
}

# Stops a process and every process it started, children first.
function Stop-ProcessTree([int]$ProcessId) {
    Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue |
        ForEach-Object { Stop-ProcessTree ([int]$_.ProcessId) }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Wait-Until([string]$What, [int]$Seconds, [scriptblock]$Condition) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        foreach ($name in @($script:running.Keys)) {
            $process = $script:running[$name]
            $process.Refresh()
            if ($process.HasExited) { throw "$name exited (code $($process.ExitCode)) while waiting for: $What" }
        }
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out after $Seconds s waiting for: $What"
}

function Start-BenchService([string]$Service) {
    $title = $Service.Substring(0, 1).ToUpperInvariant() + $Service.Substring(1)
    $cp = "out/$Service.jar;out/common.jar;out/contracts.jar;lib/messaging/*;lib/mysql-connector-j-9.0.0.jar"
    $process = Start-Process -FilePath $JavaExe -ArgumentList @('-cp', $cp, "$Service.${title}Main") `
        -WorkingDirectory $server -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $out "$Service-stdout.log") `
        -RedirectStandardError (Join-Path $out "$Service-stderr.log")
    $null = $process.Handle
    $script:running[$Service] = $process
}

<#
    Starts a service and waits for its readiness condition. On this shared machine an outbound
    connection can briefly hold a service port, so a start that fails with a BindException is
    repeated (its logs are kept with an attempt suffix).
#>
function Start-ReadyService([string]$Service, [string]$What, [scriptblock]$Condition, [int]$Attempts = 3) {
    for ($attempt = 1; ; $attempt++) {
        Start-BenchService $Service
        try {
            Wait-Until $What 60 $Condition
            return
        } catch {
            $process = $script:running[$Service]
            $stderr = Join-Path $out "$Service-stderr.log"
            $bind = $process.HasExited -and (Test-Path $stderr) -and (Select-String -Path $stderr -Pattern 'BindException' -Quiet)
            if (-not $bind -or $attempt -ge $Attempts) { throw }
            $script:running.Remove($Service)
            foreach ($stream in @('stdout', 'stderr')) {
                $log = Join-Path $out "$Service-$stream.log"
                if (Test-Path $log) { Move-Item $log (Join-Path $out "$Service-$stream.attempt$attempt.log") }
            }
            Write-Host "$Service could not bind its port (attempt $attempt); retrying" -ForegroundColor Yellow
            Start-Sleep -Seconds 3
        }
    }
}

<#
    The JDK's own java.exe: launcher shims such as Oracle's javapath start the JVM as a child process,
    which stopping the shim would leave running.
#>
function Resolve-BenchJava([string]$Explicit) {
    if ($Explicit) { return $Explicit }
    $launcher = (Get-Command java -ErrorAction Stop).Source
    # The settings print on stderr; Windows PowerShell 5.1 would turn that into a terminating error.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $javaHome = (& $launcher -XshowSettings:properties -version 2>&1 | ForEach-Object { "$_" } |
        Where-Object { $_ -match '^\s*java\.home = ' } | Select-Object -First 1) -replace '^\s*java\.home = ', ''
    $ErrorActionPreference = $previous
    if ($javaHome -and (Test-Path (Join-Path $javaHome.Trim() 'bin\java.exe'))) { return Join-Path $javaHome.Trim() 'bin\java.exe' }
    return $launcher
}

<#
    Starts Nginx with deploy/nginx.conf unchanged, in its own prefix under %TEMP% (only its logs and
    config live there), and returns the prefix for Stop-Gateway. The configuration test output goes to
    $out/nginx-config-test*.txt.
#>
function Start-Gateway([string]$NginxBin, [string]$Repo) {
    $prefix = Join-Path $env:TEMP ('cs6510-bench-nginx-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path (Join-Path $prefix 'conf'), (Join-Path $prefix 'logs'), (Join-Path $prefix 'temp') | Out-Null
    Copy-Item (Join-Path $Repo 'deploy/nginx.conf') (Join-Path $prefix 'conf/nginx.conf')
    # nginx -t reports on stderr, which Windows PowerShell 5.1 turns into errors when redirected.
    $test = Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$prefix/", '-c', 'conf/nginx.conf', '-t') `
        -WorkingDirectory $prefix -WindowStyle Hidden -Wait -PassThru `
        -RedirectStandardOutput (Join-Path $out 'nginx-config-test.out.txt') `
        -RedirectStandardError (Join-Path $out 'nginx-config-test.txt')
    if ($test.ExitCode -ne 0) { throw "Nginx configuration test failed; see $out/nginx-config-test.txt" }
    Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$prefix/", '-c', 'conf/nginx.conf') -WorkingDirectory $prefix -WindowStyle Hidden
    return $prefix
}

# Stops the Nginx started by Start-Gateway and copies its logs into $out.
function Stop-Gateway([string]$NginxBin, [string]$Prefix) {
    if (-not $Prefix) { return }
    try {
        Start-Process -FilePath $NginxBin -ArgumentList @('-p', "$Prefix/", '-c', 'conf/nginx.conf', '-s', 'stop') `
            -WorkingDirectory $Prefix -WindowStyle Hidden -Wait | Out-Null
    } catch { }
    Get-CimInstance Win32_Process -Filter "name='nginx.exe'" | Where-Object { $_.CommandLine -like "*$Prefix*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    if (Test-Path (Join-Path $Prefix 'logs')) { Copy-Item (Join-Path $Prefix 'logs/*') $out -ErrorAction SilentlyContinue }
}

<#
    benchmark-compare.ps1's idle-machine gate. Dot-sourcing that script binds its parameters as
    variables, so it is loaded inside this function's scope, never into the harness's.
#>
function Invoke-IdleGate([string]$Action, $Before, [datetime]$Started) {
    $cores = $IdleCpuCores
    if ($cores -le 0) { $cores = 64 }
    . (Join-Path $PSScriptRoot 'benchmark-compare.ps1') -IdleCpuCores $cores -BlockingProcess $BlockingProcess
    switch ($Action) {
        'wait' { Wait-IdleMachine (Join-Path $out 'idle-gate.log') }
        'snapshot' { return Get-BackgroundCpuSnapshot }
        'measure' { return Measure-BackgroundLoad $Before (Get-BackgroundCpuSnapshot) ((Get-Date) - $Started).TotalSeconds $BlockingProcess }
    }
}
