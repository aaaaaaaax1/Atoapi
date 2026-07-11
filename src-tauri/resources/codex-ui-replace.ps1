param(
    [Parameter(Mandatory = $true)]
    [string]$Target,
    [Parameter(Mandatory = $true)]
    [string]$Source,
    [Parameter(Mandatory = $true)]
    [string]$ExpectedSha256,
    [Parameter(Mandatory = $true)]
    [string]$Result,
    [Parameter(Mandatory = $true)]
    [string]$AllowedSourceRoot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Result {
    param([string]$Value)
    [System.IO.File]::WriteAllText($Result, $Value, [System.Text.UTF8Encoding]::new($false))
}

$targetPath = [System.IO.Path]::GetFullPath($Target)
$sourcePath = [System.IO.Path]::GetFullPath($Source)
$sourceRoot = [System.IO.Path]::GetFullPath($AllowedSourceRoot).TrimEnd("\") + "\"
$windowsApps = [System.IO.Path]::GetFullPath((Join-Path $env:ProgramFiles "WindowsApps")).TrimEnd("\") + "\"

try {
    if (-not $targetPath.StartsWith($windowsApps, [System.StringComparison]::OrdinalIgnoreCase) -or
        -not $targetPath.EndsWith("\app\resources\app.asar", [System.StringComparison]::OrdinalIgnoreCase) -or
        $targetPath.IndexOf("\OpenAI.Codex_", [System.StringComparison]::OrdinalIgnoreCase) -lt 0) {
        throw "Refusing to modify an unexpected Codex target: $targetPath"
    }
    if (-not $sourcePath.StartsWith($sourceRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to use a source outside the Atoapi patch directory: $sourcePath"
    }
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        throw "Patch source does not exist: $sourcePath"
    }
    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourcePath).Hash
    if ($sourceHash -ne $ExpectedSha256) {
        throw "Patch source hash changed before elevation."
    }

    $originalAcl = Get-Acl -LiteralPath $targetPath
    $replaceError = $null
    try {
        & takeown.exe /F $targetPath /A | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "takeown failed with exit code $LASTEXITCODE"
        }
        & icacls.exe $targetPath /grant "*S-1-5-32-544:(F)" /C | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "icacls grant failed with exit code $LASTEXITCODE"
        }
        Copy-Item -LiteralPath $sourcePath -Destination $targetPath -Force
        $targetHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $targetPath).Hash
        if ($targetHash -ne $ExpectedSha256) {
            throw "Codex app.asar verification failed after replacement."
        }
    } catch {
        $replaceError = $_
    } finally {
        try {
            Set-Acl -LiteralPath $targetPath -AclObject $originalAcl
        } catch {
            if ($null -eq $replaceError) {
                $replaceError = $_
            }
        }
    }
    if ($null -ne $replaceError) {
        throw $replaceError
    }
    Write-Result "ok"
} catch {
    Write-Result ("error: " + $_.Exception.Message)
    exit 1
}
