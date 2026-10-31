# T171 (release suite) plus quickstart sections 1-4 on a fresh baseline.
$out = Join-Path $PSScriptRoot ('t171-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Force $out | Out-Null
. (Join-Path $PSScriptRoot 'lib-deploy.ps1')
try {
    Note "# T171 and quickstart 1-4 ($(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))"
    Note "## 1. Build"
    Note "jars: $((Get-ChildItem (Join-Path $server 'out') -Filter *.jar | ForEach-Object Name) -join ', ')"
    Note "lib/grpc exists: $(Test-Path (Join-Path $server 'lib\grpc'))"
    Note "messaging runtime: $((Get-ChildItem (Join-Path $server 'lib\messaging') -Filter 'kafka-clients-*.jar' | ForEach-Object Name) -join ', ')"
    Note "## 2. Reset and start"
    $reset = Reset-Baseline
    Note "reset-baseline output (tail):"
    Get-Content $reset -Tail 25 | ForEach-Object { Note "    $_" }
    Start-Deployment
    Start-Sleep -Seconds 5
    $status = Join-Path $out 'kafka-status.json'
    & (Join-Path $kafkaDir 'status.ps1') -Json *> $status
    Note "status.ps1 -Json saved to kafka-status.json"
    Note "## 3. Contract smoke"
    $env:SERVER_BASE_URL = 'http://localhost:8080'
    $code = Run-Bash './run-tests.sh contract' (Join-Path $out 'contract.log')
    Note "contract exit $code; $((Select-String -Path (Join-Path $out 'contract.log') -Pattern '^Summary').Line)"
    Note "## 4. Release suite (T171)"
    $env:KAFKA_TEST_BOOTSTRAP = '127.0.0.1:19092'
    $env:KAFKA_TEST_BROKER_CONTROL = '1'
    $env:STRICT_LIVE_GATES = '1'
    $env:NGINX_BIN = $nginx
    $started = Get-Date
    $code = Run-Bash './run-tests.sh' (Join-Path $out 'release-suite.log')
    Note "release suite exit $code after $([int]((Get-Date) - $started).TotalMinutes) min"
    Select-String -Path (Join-Path $out 'release-suite.log') -Pattern '^Summary|^Direct restoration|^Live tests skipped' | ForEach-Object { Note "    $($_.Line)" }
} catch {
    Note "FAILED: $($_.Exception.Message)"
} finally {
    Stop-Deployment
    Note "deployment stopped $(Get-Date -Format 'HH:mm:ss')"
    Note "== driver finished"
}
