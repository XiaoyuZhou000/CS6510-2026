<#
.SYNOPSIS
    One-time, idempotent creation of the four service schemas and users (R3, 007 N13).

.DESCRIPTION
    Creates cs6510_transaction, cs6510_inventory, cs6510_analytics, and
    cs6510_notification (utf8mb4, utf8mb4_0900_ai_ci) and the users
    cs6510_<service>_svc for both 'localhost' and '127.0.0.1'. The 006 users are
    granted ALL PRIVILEGES on their own schema only; the Notification user gets
    only SELECT, INSERT, UPDATE, and DELETE on cs6510_notification. Running it
    again keeps the schemas and their data, resets the users' passwords, and
    narrows their grants back to their own schema.

    Passwords:
      - the MySQL admin password comes from MYSQL_ADMIN_PASSWORD, or a secure prompt;
      - each 006 service password comes from TRANSACTION_DB_PASSWORD,
        INVENTORY_DB_PASSWORD, or ANALYTICS_DB_PASSWORD, or a secure prompt;
      - the Notification password comes only from NOTIFICATION_DB_PASSWORD, which the
        user sets as a user environment variable; the script stops if it is unset.
    They reach mysql only through stdin and MYSQL_PWD, never a command line, and are
    never printed.

.EXAMPLE
    ./bootstrap.ps1
    ./bootstrap.ps1 -AdminUser root -Port 3307 -MySqlExe "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe"
#>
param(
    [string]$AdminUser = "root",
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3307,
    [string]$MySqlExe
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'mysql-tools.ps1')

$template = Join-Path $PSScriptRoot 'bootstrap.sql'
if (-not (Test-Path $template)) { throw "bootstrap.sql not found at $template" }
$mysql = Resolve-MySqlExe -Explicit $MySqlExe
Write-Host "Using mysql client: $mysql" -ForegroundColor DarkGray

$adminPassword = $env:MYSQL_ADMIN_PASSWORD
if ($null -eq $adminPassword -or $adminPassword -eq '') {
    $adminPassword = Read-SecretText -Prompt "MySQL admin password for '$AdminUser'"
}

# A password inside a single-quoted SQL literal: escape backslashes, then quotes.
function ConvertTo-SqlLiteralText([string]$Value) {
    return $Value.Replace('\', '\\').Replace("'", "''")
}

$sql = Get-Content -Raw -Encoding UTF8 $template
foreach ($service in $script:ServiceNames) {
    $variable = "$($service.ToUpperInvariant())_DB_PASSWORD"
    $password = Get-EnvironmentText $variable
    if (($null -eq $password -or $password -eq '') -and $service -eq 'notification') {
        throw ("$variable is not set. Set it as a user environment variable (for example with " +
            "[Environment]::SetEnvironmentVariable('$variable', <password>, 'User')) and run bootstrap.ps1 again.")
    }
    if ($null -eq $password -or $password -eq '') {
        $password = Read-SecretText -Prompt "New password for 'cs6510_${service}_svc' ($variable)"
    }
    if ($password -eq '') { throw "$variable must not be empty" }
    $sql = $sql.Replace("{{$variable}}", (ConvertTo-SqlLiteralText $password))
}
if ($sql.Contains('{{')) { throw "bootstrap.sql has an unreplaced placeholder" }

Write-Host "Bootstrapping service schemas and users on ${MySqlHost}:$Port as '$AdminUser'..." -ForegroundColor Cyan
Invoke-MySql -MySql $mysql -MySqlHost $MySqlHost -Port $Port -User $AdminUser -Password $adminPassword `
    -InputText $sql -Table
Write-Host "Service schemas and users are ready. Next: db/reset-all.ps1" -ForegroundColor Green
exit 0
