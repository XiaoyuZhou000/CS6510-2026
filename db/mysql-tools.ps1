<#
.SYNOPSIS
    Shared helpers for the per-service database scripts (R3, R13). Dot-source it:
        . (Join-Path $PSScriptRoot 'mysql-tools.ps1')

.DESCRIPTION
    - Resolve-MySqlExe finds the mysql client the same way the retired db/init-db.ps1 did.
    - Get-ServiceDbCredential reads <SERVICE>_DB_NAME, <SERVICE>_DB_USER, and
      <SERVICE>_DB_PASSWORD, prompting securely for a missing password.
    - Invoke-MySql runs SQL text on stdin or one query, passing the password only
      through MYSQL_PWD, never as a command-line argument.
#>

# Windows PowerShell 5.1 pipes text to native commands as ASCII unless told otherwise.
$OutputEncoding = New-Object System.Text.UTF8Encoding $false

$script:ServiceNames = @('transaction', 'inventory', 'analytics')

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

function Read-SecretText {
    param([Parameter(Mandatory = $true)][string]$Prompt)
    $secure = Read-Host -Prompt $Prompt -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Get-ServiceDbCredential {
    param([Parameter(Mandatory = $true)][ValidateSet('transaction', 'inventory', 'analytics')][string]$Service)
    $prefix = $Service.ToUpperInvariant()
    $database = [Environment]::GetEnvironmentVariable("${prefix}_DB_NAME")
    if (-not $database) { $database = "cs6510_$Service" }
    $user = [Environment]::GetEnvironmentVariable("${prefix}_DB_USER")
    if (-not $user) { $user = "cs6510_${Service}_svc" }
    $password = [Environment]::GetEnvironmentVariable("${prefix}_DB_PASSWORD")
    if ($null -eq $password -or $password -eq '') {
        $password = Read-SecretText -Prompt "MySQL password for '$user' (${prefix}_DB_PASSWORD)"
    }
    return [pscustomobject]@{ Service = $Service; Database = $database; User = $user; Password = $password }
}

<#
    Runs the mysql client and returns its stdout lines. -InputText goes to stdin;
    -Query is passed with -e. -Batch adds -N -B (tab-separated, no headers) and
    -Table adds -t. Throws when the client exits non-zero.
#>
function Invoke-MySql {
    param(
        [Parameter(Mandatory = $true)][string]$MySql,
        [string]$MySqlHost = '127.0.0.1',
        [int]$Port = 3307,
        [Parameter(Mandatory = $true)][string]$User,
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Password,
        [string]$Database,
        [string]$InputText,
        [string]$Query,
        [switch]$Batch,
        [switch]$Table
    )
    $arguments = @('-h', $MySqlHost, '-P', [string]$Port, '-u', $User, '--default-character-set=utf8mb4')
    if ($Database) { $arguments += @('-D', $Database) }
    if ($Batch) { $arguments += @('-N', '-B') }
    if ($Table) { $arguments += '-t' }
    if ($Query) { $arguments += @('-e', $Query) }

    $previous = $env:MYSQL_PWD
    $env:MYSQL_PWD = $Password
    try {
        if ($PSBoundParameters.ContainsKey('InputText')) {
            $output = $InputText | & $MySql @arguments
        } else {
            $output = & $MySql @arguments
        }
        $code = $LASTEXITCODE
    } finally {
        if ($null -eq $previous) { Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue }
        else { $env:MYSQL_PWD = $previous }
    }
    if ($code -ne 0) { throw "mysql exited with code $code (user '$User', database '$Database')" }
    return $output
}

# Runs a query that returns exactly one row and gives back its tab-separated fields.
function Get-MySqlRow {
    param(
        [Parameter(Mandatory = $true)][string]$MySql,
        [string]$MySqlHost = '127.0.0.1',
        [int]$Port = 3307,
        [Parameter(Mandatory = $true)]$Credential,
        [Parameter(Mandatory = $true)][string]$Query
    )
    $lines = @(Invoke-MySql -MySql $MySql -MySqlHost $MySqlHost -Port $Port -User $Credential.User `
        -Password $Credential.Password -Database $Credential.Database -Query $Query -Batch)
    if ($lines.Count -ne 1) { throw "Expected one row from: $Query" }
    return ,($lines[0] -split "`t")
}
