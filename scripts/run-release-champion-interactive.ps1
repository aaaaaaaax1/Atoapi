[CmdletBinding()]
param(
    [ValidateSet('tool-tail-maturity', 'full-replay', 'dynamic-tail-mix')]
    [string]$Scenario = 'tool-tail-maturity',

    [ValidateRange(1, 18)]
    [int]$Pairs = 1,

    [ValidateRange(0, 2500000)]
    [int]$SeedContextChars = 0,

    [ValidateRange(0, 1000000)]
    [int]$MinimumSeedInputTokens = 0,

    [string]$Model = 'gpt-5.6-terra',

    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$KeyRealmHash = '4574f5c28bcca32c7845a8625bed88d421bcdf03b48a4550d5109d3e2e25b407',

    [string]$ProviderId = 'agent-codex-provider-2',

    [string]$KeyId = '',

    [string]$ConfigDir = (Join-Path $env:APPDATA 'Atoapi'),

    [string]$ChampionExe = 'G:\Atoapi\releases\v1.4.33-exact-sent-waterline-maturity-20260807\Atoapi.exe',

    [string]$CandidateExe = 'G:\Atoapi\src-tauri\target\release\atoapi.exe',

    [string]$Output = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$verifier = Join-Path $PSScriptRoot 'verify-release-champion.mjs'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$isDynamicTailMix = $Scenario -eq 'dynamic-tail-mix'

# The dynamic-tail fixture is only meaningful at the requested half-million
# token class. These defaults remain overrideable, but invoking that scenario
# no longer silently degenerates into the historical three-turn short probe.
if ($isDynamicTailMix) {
    if ($SeedContextChars -eq 0) {
        $SeedContextChars = 2350000
    }
    if ($MinimumSeedInputTokens -eq 0) {
        $MinimumSeedInputTokens = 450000
    }
}

$turns = if ($isDynamicTailMix) { 11 } else { 3 }
$toolChars = if ($isDynamicTailMix) { 131072 } else { 40960 }
$toolCalls = if ($isDynamicTailMix) { 2 } else { 1 }

foreach ($item in @(
    @{ Label = 'Atoapi config directory'; Path = $ConfigDir; Type = 'Container' },
    @{ Label = 'v1.4.33 champion executable'; Path = $ChampionExe; Type = 'Leaf' },
    @{ Label = 'candidate executable'; Path = $CandidateExe; Type = 'Leaf' },
    @{ Label = 'release champion verifier'; Path = $verifier; Type = 'Leaf' }
)) {
    if (-not (Test-Path -LiteralPath $item.Path -PathType $item.Type)) {
        throw "$($item.Label) is missing: $($item.Path)"
    }
}

if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    throw 'Node.js is required to run scripts/verify-release-champion.mjs.'
}

if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $repoRoot "output\candidate-v1433-v1433-$Scenario-interactive-$stamp.json"
}

$outputDirectory = Split-Path -Parent $Output
if (-not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

Write-Host 'Runs a bounded, isolated A/B seed. The running 18883 instance is never stopped or signaled.'
Write-Host "Windows identity: $(whoami)"
Write-Host "Scenario: $Scenario; pairs: $Pairs; turns: $turns; model: $Model"
Write-Host "Seed context chars: $SeedContextChars; minimum seed input tokens: $MinimumSeedInputTokens"
Write-Host "Provider: $ProviderId; Key realm: $($KeyRealmHash.Substring(0, 12))..."
if (-not [string]::IsNullOrWhiteSpace($KeyId)) {
    Write-Host "Pinned Key: $KeyId"
}
Write-Host "Report: $Output"
Write-Host 'Run this from the normal interactive Windows account that can use the saved Atoapi Provider, not from the Codex sandbox terminal.'

$arguments = @(
    $verifier,
    '--live',
    '--champion-exe', $ChampionExe,
    '--candidate-exe', $CandidateExe,
    '--source-config-dir', $ConfigDir,
    '--model', $Model,
    '--key-realm-hash', $KeyRealmHash,
    '--provider-id', $ProviderId,
    '--scenario', $Scenario,
    '--pairs', "$Pairs",
    '--turns', "$turns",
    '--max-output-tokens', '16',
    '--stable-instruction-chars', '16384',
    '--seed-context-chars', "$SeedContextChars",
    '--minimum-seed-input-tokens', "$MinimumSeedInputTokens",
    '--tool-chars', "$toolChars",
    '--tool-calls', "$toolCalls",
    '--tool-output-shape', 'natural',
    '--fixture-profile', 'natural',
    '--max-local-proxy-overhead-regression-ms', '500',
    '--isolate-upstream-cache',
    '--prompt-cache-key-prefix', "atoapi-release-champion-$stamp",
    '--output', $Output
)

if (-not [string]::IsNullOrWhiteSpace($KeyId)) {
    $arguments += @('--key-id', $KeyId.Trim())
}

& node @arguments
exit $LASTEXITCODE
