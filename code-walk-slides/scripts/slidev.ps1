param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dev', 'build', 'export-pdf', 'export-pptx')]
    [string]$Command
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$originalLocation = Get-Location
$driveLetter = @('Z', 'Y', 'X', 'W', 'V') |
    Where-Object { -not (Test-Path "$($_):\") } |
    Select-Object -First 1

if (-not $driveLetter) {
    throw 'No temporary drive letter is available for the Slidev Unicode-path workaround.'
}

$drive = "${driveLetter}:"
& subst.exe $drive $projectRoot
if ($LASTEXITCODE -ne 0) {
    throw "Failed to map $drive to $projectRoot"
}

try {
    Set-Location "$drive\"
    $slidev = '.\node_modules\.bin\slidev.cmd'

    switch ($Command) {
        'dev' {
            & $slidev 'slides.md' '--open'
        }
        'build' {
            & $slidev 'build' 'slides.md'
        }
        'export-pdf' {
            & $slidev 'export' 'slides.md' '--format' 'pdf' '--output' 'pipeline-code-walk.pdf'
        }
        'export-pptx' {
            & $slidev 'export' 'slides.md' '--format' 'pptx' '--output' 'pipeline-code-walk.pptx'
        }
    }

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Set-Location $originalLocation
    & subst.exe $drive '/D' | Out-Null
}
