<#
.SYNOPSIS
    Reinitialize the self-checkout MySQL database (drop, recreate, reseed).

.DESCRIPTION
    Runs db/init.sql against a locally installed MySQL server. Run this before
    every load-test to guarantee a clean starting state (2000 items @ 10000
    stock each). Creates/replaces the `cs6510_selfcheckout` database.

    The `mysql` client is located automatically:
      1. if `mysql` is already on PATH, that is used;
      2. otherwise the newest "C:\Program Files\MySQL\MySQL Server *\bin\mysql.exe"
         is used;
      3. or pass an explicit path with -MySqlExe.

.PARAMETER MySqlUser
    MySQL user (default: root).

.PARAMETER MySqlHost
    MySQL host (default: 127.0.0.1).

.PARAMETER Port
    MySQL port (default: 3306).

.PARAMETER Password
    MySQL password. If omitted, you are prompted securely (no echo).

.PARAMETER MySqlExe
    Full path to mysql.exe, overriding auto-detection.

.EXAMPLE
    ./init-db.ps1
    ./init-db.ps1 -MySqlUser root -Port 3307
    ./init-db.ps1 -MySqlExe "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe"
#>
param(
    [string]$MySqlUser = "root",
    [string]$MySqlHost = "127.0.0.1",
    [int]$Port = 3306,
    [string]$Password,
    [string]$MySqlExe
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$sqlFile = Join-Path $scriptDir "init.sql"

if (-not (Test-Path $sqlFile)) {
    Write-Error "init.sql not found at $sqlFile"
    exit 1
}

# ---------------------------------------------------------------------------
# Locate mysql.exe
# ---------------------------------------------------------------------------
function Resolve-MySqlExe {
    param([string]$Explicit)

    if ($Explicit) {
        if (Test-Path $Explicit) { return $Explicit }
        throw "mysql.exe not found at the path given via -MySqlExe: $Explicit"
    }

    # 1. Already on PATH?
    $onPath = Get-Command mysql -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }

    # 2. Standard MySQL Server install locations (newest version first).
    $candidates = @(
        "C:\Program Files\MySQL\MySQL Server *\bin\mysql.exe",
        "C:\Program Files (x86)\MySQL\MySQL Server *\bin\mysql.exe"
    )
    foreach ($pattern in $candidates) {
        $found = Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue |
                 Sort-Object FullName -Descending
        if ($found) { return $found[0].FullName }
    }

    throw "Could not find mysql.exe. Add MySQL's bin directory to PATH, or pass -MySqlExe 'C:\path\to\mysql.exe'."
}

$mysql = Resolve-MySqlExe -Explicit $MySqlExe
Write-Host "Using mysql client: $mysql" -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# Password (prompt securely if not supplied)
# ---------------------------------------------------------------------------
if (-not $Password) {
    $secure = Read-Host -Prompt "MySQL password for '$MySqlUser'" -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $Password = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

# ---------------------------------------------------------------------------
# Run init.sql
# ---------------------------------------------------------------------------
Write-Host "Reinitializing 'cs6510_selfcheckout' on $MySqlHost`:$Port as '$MySqlUser'..." -ForegroundColor Cyan
Write-Host "Using $sqlFile" -ForegroundColor DarkGray

# Password is passed via MYSQL_PWD so it never appears in the process command
# line / argument list.
$env:MYSQL_PWD = $Password
try {
    Get-Content -Raw -Encoding UTF8 $sqlFile |
        & $mysql -h $MySqlHost -P $Port -u $MySqlUser --default-character-set=utf8mb4
    $code = $LASTEXITCODE
} finally {
    Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
}

if ($code -ne 0) {
    Write-Error "Database initialization failed (exit code $code)."
    exit $code
}

Write-Host "Database 'cs6510_selfcheckout' reinitialized successfully." -ForegroundColor Green
