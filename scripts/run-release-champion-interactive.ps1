[CmdletBinding()]
param(
    [ValidateSet('tool-tail-maturity', 'tool-burst', 'full-replay', 'dynamic-tail-mix')]
    [string]$Scenario = 'tool-tail-maturity',

    [ValidateRange(1, 18)]
    [int]$Pairs = 1,

    # Keep the full dynamic mix at 11 turns by default, while allowing a
    # bounded seed -> changing tail -> direct-follow-up control when an older
    # champion cannot carry the full history through the selected upstream.
    [ValidateRange(0, 60)]
    [int]$Turns = 0,

    # Warm-up pairs exercise each isolated runtime's cold-start path but are
    # excluded from scored hit-rate aggregates. Keep them symmetric so the
    # candidate cannot gain or lose by receiving an extra cold start.
    [ValidateRange(0, 17)]
    [int]$WarmupPairs = 0,

    # Ordering changes only which isolated arm sends first.  It never changes
    # the selected Provider, mapped Key, request body, or upstream call count.
    [ValidateSet('champion', 'candidate')]
    [string]$FirstArm = 'champion',

    [ValidateRange(0, 1)]
    [int]$PairOffset = 0,

    # Verifier-only cooldown between complete pairs. This is useful when a
    # selected upstream reports capacity failure after a long uninterrupted
    # dynamic control; it never changes either arm's body, Key, placement, or
    # per-inbound request count.
    [ValidateRange(0, 60000)]
    [int]$PairDelayMs = 0,

    # The selected upstream can legitimately spend several minutes waiting
    # for headers on a 1M+ token seed. This verifier-only deadline is kept
    # separate from Atoapi runtime behavior and never changes live traffic.
    [ValidateRange(30000, 600000)]
    [int]$ResponseTimeoutMs = 180000,

    # Test-only horizon delay. It runs only after both seed SSE streams have
    # completed and before the first changing tail, so a 24h retention field
    # can be measured across the ordinary cache window without changing live
    # desktop traffic.
    [ValidateRange(0, 3600000)]
    [int]$SeedToReuseDelayMs = 0,

    # Verifier-only pacing within a crossover pair. These delays do not alter
    # either arm's request body or cache policy; they only give a capacity-
    # sensitive upstream a bounded recovery window between streamed turns.
    [ValidateRange(0, 5000)]
    [int]$TurnDelayMs = 0,

    [ValidateRange(0, 5000)]
    [int]$InterArmDelayMs = 0,

    # Shared crossover keeps both isolated binaries on one generated cache
    # placement and rotates their turn order. This is the default for a live
    # promotion comparison because distinct local placement values do not
    # prove that a selected upstream will isolate its cache. Pass
    # -SharedCacheCrossover:$false only for a diagnostic isolated-lane run;
    # the verifier will retain it as non-promotable evidence.
    [switch]$SharedCacheCrossover = $true,

    # Diagnostic-only: reverse persistent isolated process startup without
    # changing the current Provider, Key, cache placement, or turn sequence.
    [ValidateSet('champion', 'candidate')]
    [string]$PersistentRuntimeStartOrder = 'champion',

    [ValidateRange(0, 2500000)]
    [int]$SeedContextChars = 0,

    [ValidateRange(0, 1000000)]
    [int]$MinimumSeedInputTokens = 0,

    [ValidateRange(0, 1000000)]
    [int]$MinimumPeakInputTokens = 0,

    [ValidateRange(0, 1000000)]
    [int]$MaximumPeakInputTokens = 0,

    [ValidateSet('mixed', 'natural-dense')]
    [string]$DynamicTailProfile = 'mixed',

    # Use text tails for a protocol-only probe when a provider rejects
    # replayed function-call history. The verifier rejects this outside the
    # dynamic-tail scenario.
    [ValidateSet('tool', 'text')]
    [string]$DynamicTailMode = 'tool',

    # Select the verifier fixture protocol. Function is the historical wire;
    # custom exercises Responses custom_tool_call/custom_tool_call_output.
    [ValidateSet('function', 'custom')]
    [string]$ToolProtocol = 'function',

    # Narrow verifier-only fixture: exercise local previous_response_id
    # continuation with regenerated closed tool call ids. It never changes
    # normal desktop traffic and is invalid outside the three-turn dynamic
    # tool scenario.
    [switch]$ExerciseLocalPreviousResponseIdRebind,

    # Narrow verifier-only fixture: exercise a local previous_response_id
    # continuation while preserving every completed tool call id. This is
    # intentionally separate from the rebind fixture and never changes normal
    # desktop traffic.
    [switch]$ExerciseLocalPreviousResponseIdFullReplay,

    [ValidateSet('natural', 'natural-dense', 'legacy-repeated')]
    [string]$FixtureProfile = 'natural',

    # Verifier-only tool-history shape selection. Keep the default natural so
    # existing calibration calls remain byte-for-byte compatible; callers may
    # explicitly select natural-dense when isolating that one upstream vector.
    [ValidateSet('natural', 'natural-dense', 'legacy-repeated')]
    [string]$ToolOutputShape = 'natural',

    # The historical 4KiB tool-tail evidence declares the fixture tool schema
    # from the seed onward. Keep that wire explicit for compatibility probes;
    # turning it off is a different fixture and must be requested deliberately.
    [switch]$IncludeToolSchema = $true,

    # End-to-end TTFT includes the selected upstream.  Keep it diagnostic by
    # default; the verifier still gates complete local pre-upstream overhead.
    [switch]$RequireTtftNoRegression,

    [ValidateRange(0, 10000)]
    [int]$MaxInputTokenDelta = 128,

    # The frozen stable prefix must be large enough to exercise maturity
    # policies that deliberately decline tiny contexts. Keep the historical
    # default intact, while allowing an explicit high-waterline probe.
    [ValidateRange(1024, 2500000)]
    [int]$StableInstructionChars = 16384,

    # A candidate-only guard is an optimization under test, not an assumed
    # result. This lets a live validation fail closed when its target path
    # never actually ran.
    [ValidateRange(0, 60)]
    [int]$RequireCandidateGuardedRequests = 0,

    # Target-only evidence gate for the candidate's 4-8KiB exact tool-tail
    # maturity branch. Generic prefix waits must never stand in for it.
    [switch]$RequireCandidateExactMediumToolTailMaturityWait,

    # Target-only evidence gate for the candidate's exact large-message text
    # tail lag branch. A generic prefix wait must never qualify this path.
    [switch]$RequireCandidateExactLargeMessageTailLag,

    # Target-only evidence gate for the late direct-child branch after a
    # proven shallow provider-waterline rollback.  This remains an isolated
    # candidate experiment: a generic prefix wait or ordinary waterline
    # settle must never qualify it for a promotion result.
    [switch]$RequireCandidateLateShallowProviderWaterlineRollbackWait,

    [ValidateRange(1024, 512000)]
    [int]$ToolChars = 0,

    [ValidateRange(1, 8)]
    [int]$ToolCalls = 0,

    # Test-only override applied to both isolated arms.  This removes the
    # package-version default User-Agent as a cache-lane variable without
    # changing the live Provider configuration.
    [string]$UpstreamUserAgent = '',

    # Same-binary diagnostic only: compare the selected Provider's untouched
    # versioned default User-Agent with one explicit stable User-Agent. This
    # is intentionally non-promotable and must run with isolated cache lanes.
    [switch]$DiagnosticUserAgentSplit,

    [string]$ChampionUpstreamUserAgent = '',

    [string]$CandidateUpstreamUserAgent = '',

    # By default the historical runner injects one generated client-owned
    # prompt_cache_key into both arms.  Native-policy comparisons must be able
    # to omit that test key so the candidate's own cache placement policy is
    # observable while retaining the same live-scope resolver and guards.
    [switch]$NoClientPromptCacheKey = $true,

    # Isolated candidate-only experiment: inject Atoapi's generated native
    # prompt_cache_key after the selected provider/model/Key scope has already
    # accepted that field. It never changes the live desktop policy.
    [switch]$CandidatePromptCacheKey,

    # Isolated candidate-only subvariant: derive the generated prompt-cache key
    # from a trusted explicit thread so session/conversation churn can reuse
    # one placement. It still uses the native PCK capability gate.
    [switch]$CandidateThreadStablePromptCacheKeyBridge,

    # Isolated candidate-only experiment: replay a narrowly recognized
    # upstream load-balancer placement cookie. Normal desktop traffic remains
    # disabled in the executable and this switch never alters live config.
    [switch]$CandidateUpstreamAffinity,

    # Isolated candidate-only experiment: inject the compatibility-verified
    # native prompt_cache_options field. Normal desktop traffic never receives
    # this field until a dynamic A/B produces an exact-scope promotion result.
    [switch]$CandidatePromptCacheOptions,

    # Subvariant of CandidatePromptCacheOptions: the disposable candidate uses
    # ttl=24h rather than the ordinary implicit/30m value. It is wire-witnessed
    # and never changes the normal desktop process.
    [switch]$CandidatePromptCacheOptions24h,

    # Require the isolated 24h options candidate to exercise the bounded
    # static-sibling settle hint at least once. This is an A/B evidence gate;
    # it never enables the behavior in the live desktop process.
    [switch]$RequireCandidateOptions24hSiblingSettle,

    # Isolated candidate-only experiment: inject the compatibility-verified
    # prompt_cache_retention field. It never changes the saved Provider setting
    # or normal desktop traffic until an exact-scope promotion result exists.
    [switch]$CandidatePromptCacheRetention,

    # Isolated candidate-only transport experiment: force HTTP/1.1 to test
    # whether an upstream HTTP/2 waterline rollback is multiplexing-related.
    # Normal desktop traffic keeps the established negotiated HTTP/2 behavior.
    [switch]$CandidateHttp1,

    # Isolated candidate-only maturity experiment: after a proven shallow
    # provider waterline rollback, spend at most the existing 500ms foreground
    # settle window before the next exact safe successor.
    # Production remains unchanged until a positive dynamic A/B is verified.
    [switch]$CandidateProviderWaterlineRecoveryWait,

    # These values are refreshed from the latest successful live Codex request
    # before every run.  Supplying one is allowed only as an assertion: a stale
    # manual value fails closed instead of silently testing a previous upstream.
    [string]$Model = '',

    # The verifier must replay the current Codex request's effective
    # reasoning setting when the selected upstream rejects the implicit
    # default. This remains identical for champion and candidate.
    [ValidateSet('none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra')]
    [string]$ReasoningEffort = '',

    [string]$KeyRealmHash = '',

    [string]$ProviderId = '',

    [ValidateSet('codex-agent', 'active-provider')]
    [string]$ProviderScope = 'codex-agent',

    [string]$KeyId = '',

    [ValidateRange(30, 3600)]
    [int]$LiveRecordMaxAgeSeconds = 600,

    [switch]$ResolveLiveScopeOnly,

    # Machine-readable, secret-free variant for isolated verifier scripts.
    # It emits the complete hashed realm so the verifier can bind the exact
    # currently selected Key without exposing the Key id or secret.
    [switch]$ResolveLiveScopeJson,

    [switch]$SelfTest,

    [string]$ConfigDir = (Join-Path $env:APPDATA 'Atoapi'),

    [string]$ChampionExe = 'G:\Atoapi\releases\v1.5.0-thread-stable-pck-bridge-20260818\Atoapi.exe',

    [string]$CandidateExe = 'G:\Atoapi\src-tauri\target\release\atoapi.exe',

    [string]$Output = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$verifier = Join-Path $PSScriptRoot 'verify-release-champion.mjs'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$isDynamicTailMix = $Scenario -eq 'dynamic-tail-mix'
$isExerciseLocalPreviousResponseIdRebind = $ExerciseLocalPreviousResponseIdRebind.IsPresent
$isExerciseLocalPreviousResponseIdFullReplay = $ExerciseLocalPreviousResponseIdFullReplay.IsPresent
$isExactMediumToolTailMaturity = $RequireCandidateExactMediumToolTailMaturityWait
$isExactLargeMessageTailLag = $RequireCandidateExactLargeMessageTailLag
$isLateShallowProviderWaterlineRollback = $RequireCandidateLateShallowProviderWaterlineRollbackWait

if ($ToolProtocol -eq 'custom') {
    if ($Scenario -eq 'dynamic-tail-mix' -and $DynamicTailMode -eq 'text') {
        throw 'ToolProtocol=custom requires DynamicTailMode=tool so the request contains custom tool items.'
    }
    if (-not $IncludeToolSchema) {
        throw 'ToolProtocol=custom requires IncludeToolSchema.'
    }
    if ($Scenario -notin @('dynamic-tail-mix', 'tool-burst', 'tool-tail-maturity')) {
        throw 'ToolProtocol=custom requires a tool-history scenario.'
    }
}

if ($isExerciseLocalPreviousResponseIdRebind -and $isExerciseLocalPreviousResponseIdFullReplay) {
    throw 'ExerciseLocalPreviousResponseIdRebind and ExerciseLocalPreviousResponseIdFullReplay are mutually exclusive.'
}

if ($isExerciseLocalPreviousResponseIdRebind -or $isExerciseLocalPreviousResponseIdFullReplay) {
    if ($Scenario -ne 'dynamic-tail-mix') {
        throw 'The local previous_response_id fixture requires -Scenario dynamic-tail-mix.'
    }
    # Keep the historical two-pair default, but allow an explicit warm-up
    # pair before the two scored crossover pairs.  Shared upstream lanes can
    # otherwise make pair zero cold for one arm and warm for the other; the
    # verifier must be able to exclude that pair without weakening the
    # local-rebind witness on the scored pairs.
    if ($PSBoundParameters.ContainsKey('Pairs') -and $Pairs -lt 2) {
        throw 'The local previous_response_id fixture requires -Pairs at least 2.'
    }
    if (-not $PSBoundParameters.ContainsKey('Pairs')) {
        $Pairs = 2
    }
    if ($PSBoundParameters.ContainsKey('Turns') -and $Turns -ne 3) {
        throw 'The local previous_response_id fixture requires exactly -Turns 3.'
    }
    $Turns = 3
    if ($DynamicTailMode -ne 'tool' -or -not $IncludeToolSchema) {
        throw 'The local previous_response_id fixture requires DynamicTailMode=tool and IncludeToolSchema.'
    }
    if ($DiagnosticUserAgentSplit -or $CandidateUpstreamAffinity -or
        $CandidatePromptCacheKey -or $CandidateThreadStablePromptCacheKeyBridge -or $CandidatePromptCacheOptions -or
        $CandidatePromptCacheRetention -or $CandidateHttp1 -or
        $CandidateProviderWaterlineRecoveryWait) {
        throw 'The local previous_response_id fixture cannot be combined with another candidate-only or diagnostic treatment.'
    }
    if ($isExerciseLocalPreviousResponseIdFullReplay) {
        if ($PSBoundParameters.ContainsKey('ToolChars') -and $ToolChars -lt 32768) {
            throw 'ExerciseLocalPreviousResponseIdFullReplay requires ToolChars of at least 32768 so turn 1 has a material tool tail.'
        }
        if (-not $PSBoundParameters.ContainsKey('SeedContextChars')) {
            $SeedContextChars = 65536
        }
        if (-not $PSBoundParameters.ContainsKey('MinimumPeakInputTokens')) {
            $MinimumPeakInputTokens = 16384
        } elseif ($MinimumPeakInputTokens -lt 16384) {
            throw 'ExerciseLocalPreviousResponseIdFullReplay requires MinimumPeakInputTokens of at least 16384.'
        }
    }
}

if ($isExactMediumToolTailMaturity) {
    if ($Scenario -ne 'tool-tail-maturity') {
        throw 'RequireCandidateExactMediumToolTailMaturityWait requires -Scenario tool-tail-maturity.'
    }
    if ($PSBoundParameters.ContainsKey('Pairs') -and $Pairs -ne 2) {
        throw 'Exact medium tool-tail maturity requires exactly -Pairs 2 for the reversed leader crossover.'
    }
    $Pairs = 2
    if ($PSBoundParameters.ContainsKey('Turns') -and $Turns -ne 4) {
        throw 'Exact medium tool-tail maturity requires exactly -Turns 4.'
    }
    if ($TurnDelayMs -ne 0 -or $InterArmDelayMs -ne 0) {
        throw 'Exact medium tool-tail maturity requires zero TurnDelayMs and InterArmDelayMs.'
    }
    if (-not $IncludeToolSchema) {
        throw 'Exact medium tool-tail maturity requires IncludeToolSchema.'
    }
    $RequireCandidateGuardedRequests = [Math]::Max($RequireCandidateGuardedRequests, 1)
}

if ($isExactLargeMessageTailLag) {
    if ($Scenario -ne 'dynamic-tail-mix') {
        throw 'RequireCandidateExactLargeMessageTailLag requires -Scenario dynamic-tail-mix.'
    }
    if ($PSBoundParameters.ContainsKey('Turns') -and $Turns -ne 3) {
        throw 'Exact large message tail lag requires exactly -Turns 3.'
    }
    $Turns = 3
    if ($MinimumSeedInputTokens -lt 262144 -or $MinimumPeakInputTokens -lt 262144) {
        throw 'Exact large message tail lag requires MinimumSeedInputTokens and MinimumPeakInputTokens of at least 262144.'
    }
    if ($DynamicTailProfile -ne 'mixed' -or $DynamicTailMode -ne 'text' -or $FixtureProfile -ne 'natural') {
        throw 'Exact large message tail lag requires DynamicTailProfile=mixed, DynamicTailMode=text, and FixtureProfile=natural.'
    }
    if ($ToolChars -ne 131072 -or $ToolCalls -ne 2 -or -not $IncludeToolSchema) {
        throw 'Exact large message tail lag requires ToolChars=131072, ToolCalls=2, and IncludeToolSchema.'
    }
    if ($TurnDelayMs -ne 0 -or $InterArmDelayMs -ne 0) {
        throw 'Exact large message tail lag requires zero TurnDelayMs and InterArmDelayMs.'
    }
    $RequireCandidateGuardedRequests = [Math]::Max($RequireCandidateGuardedRequests, 1)
}

if ($isLateShallowProviderWaterlineRollback) {
    if (-not $CandidateProviderWaterlineRecoveryWait) {
        throw 'RequireCandidateLateShallowProviderWaterlineRollbackWait requires CandidateProviderWaterlineRecoveryWait so the observation belongs to the isolated candidate treatment.'
    }
    if ($Scenario -ne 'dynamic-tail-mix') {
        throw 'RequireCandidateLateShallowProviderWaterlineRollbackWait requires -Scenario dynamic-tail-mix.'
    }
    if ($PSBoundParameters.ContainsKey('Pairs') -and $Pairs -ne 2) {
        throw 'Late shallow provider-waterline rollback requires exactly -Pairs 2 for the reversed leader crossover.'
    }
    $Pairs = 2
    if ($PSBoundParameters.ContainsKey('Turns') -and $Turns -ne 4) {
        throw 'Late shallow provider-waterline rollback requires exactly -Turns 4.'
    }
    $Turns = 4
    if ($MinimumSeedInputTokens -lt 32768 -or $MinimumPeakInputTokens -lt 32768) {
        throw 'Late shallow provider-waterline rollback requires MinimumSeedInputTokens and MinimumPeakInputTokens of at least 32768.'
    }
    if ($DynamicTailProfile -ne 'mixed' -or $DynamicTailMode -ne 'text' -or $FixtureProfile -ne 'natural') {
        throw 'Late shallow provider-waterline rollback requires DynamicTailProfile=mixed, DynamicTailMode=text, and FixtureProfile=natural.'
    }
    if ($TurnDelayMs -lt 750 -or $InterArmDelayMs -ne 0) {
        throw 'Late shallow provider-waterline rollback requires TurnDelayMs of at least 750 and InterArmDelayMs of exactly 0.'
    }
}

if ($CandidateUpstreamAffinity) {
    $CandidateExe = Join-Path $repoRoot 'src-tauri\target\release\atoapi.exe'
    if (-not (Test-Path -LiteralPath $CandidateExe -PathType Leaf)) {
        throw "candidate upstream-affinity executable is missing: $CandidateExe"
    }
}

if ($NoClientPromptCacheKey -or $CandidatePromptCacheKey -or $CandidateThreadStablePromptCacheKeyBridge -or $CandidatePromptCacheOptions -or $CandidatePromptCacheRetention -or $CandidateHttp1 -or $CandidateProviderWaterlineRecoveryWait) {
    $CandidateExe = Join-Path $repoRoot 'src-tauri\target\release\atoapi.exe'
    if (-not (Test-Path -LiteralPath $CandidateExe -PathType Leaf)) {
        throw "candidate cache-control executable is missing: $CandidateExe"
    }
}

# Thread-stable bridge is a PCK subvariant, not a second independent
# candidate treatment.  It intentionally requires CandidatePromptCacheKey
# below, so count the pair as one experiment in the mutual-exclusion gate.
$candidateCacheTreatmentCount = @(
    $CandidateUpstreamAffinity,
    ($CandidatePromptCacheKey -and -not $CandidateThreadStablePromptCacheKeyBridge),
    $CandidateThreadStablePromptCacheKeyBridge,
    $CandidatePromptCacheOptions,
    $CandidatePromptCacheRetention,
    $CandidateHttp1,
    $CandidateProviderWaterlineRecoveryWait
) | Where-Object { $_ } | Measure-Object | Select-Object -ExpandProperty Count
if ($candidateCacheTreatmentCount -gt 1) {
    throw 'Only one candidate-only cache experiment may run in one A/B.'
}

if (($CandidatePromptCacheKey -or $CandidateThreadStablePromptCacheKeyBridge) -and -not $NoClientPromptCacheKey) {
    throw 'CandidatePromptCacheKey requires NoClientPromptCacheKey so a shared client key cannot confound the native-policy candidate.'
}

if ($CandidateThreadStablePromptCacheKeyBridge -and -not $CandidatePromptCacheKey) {
    throw 'CandidateThreadStablePromptCacheKeyBridge requires CandidatePromptCacheKey so the native PCK capability gate remains explicit.'
}

if ($CandidatePromptCacheOptions24h -and -not $CandidatePromptCacheOptions) {
    throw 'CandidatePromptCacheOptions24h requires CandidatePromptCacheOptions so the exact field and ttl variant are both explicit.'
}

if ($RequireCandidateOptions24hSiblingSettle -and -not $CandidatePromptCacheOptions24h) {
    throw 'RequireCandidateOptions24hSiblingSettle requires CandidatePromptCacheOptions24h so the observed settle reason belongs to the isolated ttl=24h treatment.'
}

if ($DiagnosticUserAgentSplit) {
    if ($SharedCacheCrossover) {
        throw 'DiagnosticUserAgentSplit requires SharedCacheCrossover:$false so intentionally different User-Agents cannot share one cache lane.'
    }
    if (-not [string]::IsNullOrWhiteSpace($UpstreamUserAgent)) {
        throw 'DiagnosticUserAgentSplit requires arm-specific User-Agent input; do not supply UpstreamUserAgent.'
    }
    $overrideCount = @($ChampionUpstreamUserAgent, $CandidateUpstreamUserAgent | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    if ($overrideCount -ne 1) {
        throw 'DiagnosticUserAgentSplit requires exactly one of ChampionUpstreamUserAgent or CandidateUpstreamUserAgent.'
    }
    if ((@($CandidateUpstreamAffinity, $CandidatePromptCacheKey, $CandidateThreadStablePromptCacheKeyBridge, $CandidatePromptCacheOptions, $CandidatePromptCacheRetention, $CandidateHttp1, $CandidateProviderWaterlineRecoveryWait) | Where-Object { $_ }).Count -gt 0) {
        throw 'DiagnosticUserAgentSplit cannot be combined with another candidate-only cache experiment.'
    }
}

if ($WarmupPairs -ge $Pairs) {
    throw 'WarmupPairs must be smaller than Pairs so at least one scored pair remains.'
}

function Get-TomlScalar {
    param(
        [Parameter(Mandatory = $true)][string]$Block,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $escapedName = [regex]::Escape($Name)
    $match = [regex]::Match(
        $Block,
        "(?m)^[ \t]*$escapedName[ \t]*=[ \t]*(?<value>.*?)[ \t]*\r?$"
    )
    if (-not $match.Success) {
        return ''
    }

    $value = $match.Groups['value'].Value.Trim()
    if ($value.Length -ge 2 -and (
        ($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))
    )) {
        return $value.Substring(1, $value.Length - 2)
    }
    return $value
}

function Get-TomlBoolean {
    param(
        [Parameter(Mandatory = $true)][string]$Block,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $value = (Get-TomlScalar -Block $Block -Name $Name).Trim().ToLowerInvariant()
    if ($value -eq 'true') { return $true }
    if ($value -eq 'false') { return $false }
    return $null
}

function Get-TomlArrayTableSlices {
    param(
        [Parameter(Mandatory = $true)][string]$Toml,
        [Parameter(Mandatory = $true)][string]$TableName
    )

    $headerPattern = "(?m)^[ \t]*\[\[$([regex]::Escape($TableName))\]\][ \t]*(?:#.*)?\r?$"
    $headers = [regex]::Matches($Toml, $headerPattern)
    $allHeaders = Get-TomlHeaderMatches -Toml $Toml
    $slices = @()
    foreach ($header in $headers) {
        $end = Get-NextTomlHeaderIndex -Headers $allHeaders -AfterIndex $header.Index -Fallback $Toml.Length
        $bodyStart = $header.Index + $header.Length
        $slices += [pscustomobject]@{
            Start = $header.Index
            End = $end
            Body = $Toml.Substring($bodyStart, $end - $bodyStart)
        }
    }
    return $slices
}

function Get-TomlHeaderMatches {
    param([Parameter(Mandatory = $true)][string]$Toml)

    return [regex]::Matches(
        $Toml,
        '(?m)^[ \t]*(?:\[\[[^\]\r\n]+\]\]|\[(?!\[)[^\]\r\n]+\])[ \t]*(?:#.*)?\r?$'
    )
}

function Get-NextTomlHeaderIndex {
    param(
        [Parameter(Mandatory = $true)]$Headers,
        [Parameter(Mandatory = $true)][int]$AfterIndex,
        [Parameter(Mandatory = $true)][int]$Fallback
    )

    foreach ($header in $Headers) {
        if ($header.Index -gt $AfterIndex) {
            return $header.Index
        }
    }
    return $Fallback
}

function Get-ProviderKeyPoolContext {
    param(
        [Parameter(Mandatory = $true)][string]$Toml,
        [Parameter(Mandatory = $true)][string]$ProviderId
    )

    $poolPattern = '(?m)^[ \t]*\[\[provider_key_pools\]\][ \t]*(?:#.*)?\r?$'
    $poolHeaders = [regex]::Matches($Toml, $poolPattern)
    $keyHeaders = [regex]::Matches(
        $Toml,
        '(?m)^[ \t]*\[\[provider_key_pools\.keys\]\][ \t]*(?:#.*)?\r?$'
    )
    $allHeaders = Get-TomlHeaderMatches -Toml $Toml
    $matches = @()
    foreach ($header in $poolHeaders) {
        $directEnd = Get-NextTomlHeaderIndex -Headers $allHeaders -AfterIndex $header.Index -Fallback $Toml.Length
        $bodyStart = $header.Index + $header.Length
        $poolBody = $Toml.Substring($bodyStart, $directEnd - $bodyStart)
        if ((Get-TomlScalar -Block $poolBody -Name 'provider_id') -ne $ProviderId) {
            continue
        }

        $poolEnd = $Toml.Length
        foreach ($nextPool in $poolHeaders) {
            if ($nextPool.Index -gt $header.Index) {
                $poolEnd = $nextPool.Index
                break
            }
        }
        $keys = @()
        foreach ($keyHeader in $keyHeaders) {
            if ($keyHeader.Index -le $header.Index -or $keyHeader.Index -ge $poolEnd) {
                continue
            }
            $keyEnd = Get-NextTomlHeaderIndex -Headers $allHeaders -AfterIndex $keyHeader.Index -Fallback $Toml.Length
            if ($keyEnd -gt $poolEnd) {
                $keyEnd = $poolEnd
            }
            $keyBodyStart = $keyHeader.Index + $keyHeader.Length
            $keys += $Toml.Substring($keyBodyStart, $keyEnd - $keyBodyStart)
        }

        $matches += [pscustomobject]@{
            Pool = $poolBody
            Keys = $keys
        }
    }
    if ($matches.Count -eq 0) {
        return $null
    }
    if ($matches.Count -ne 1) {
        throw "Provider '$ProviderId' has an ambiguous Provider Key pool; live verification is refused."
    }
    return $matches[0]
}

function Get-TomlObjectValue {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) {
        return ''
    }
    return [string]$property.Value
}

function Get-ObjectBoolean {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) {
        return $null
    }
    if ($property.Value -is [bool]) {
        return [bool]$property.Value
    }
    $text = ([string]$property.Value).Trim().ToLowerInvariant()
    if ($text -eq 'true') { return $true }
    if ($text -eq 'false') { return $false }
    return $null
}

function Get-Sha256Bytes {
    param([Parameter(Mandatory = $true)][byte[]]$Bytes)

    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([System.BitConverter]::ToString($sha256.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant()
    } finally {
        $sha256.Dispose()
    }
}

function Get-Sha256Text {
    param([Parameter(Mandatory = $true)][string]$Text)

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    try {
        return Get-Sha256Bytes -Bytes $bytes
    } finally {
        [System.Array]::Clear($bytes, 0, $bytes.Length)
    }
}

function Get-Sha256Parts {
    param([Parameter(Mandatory = $true)][string[]]$Parts)

    $stream = New-Object System.IO.MemoryStream
    try {
        foreach ($part in $Parts) {
            $bytes = [System.Text.Encoding]::UTF8.GetBytes([string]$part)
            try {
                $stream.Write($bytes, 0, $bytes.Length)
                $stream.WriteByte(0)
            } finally {
                [System.Array]::Clear($bytes, 0, $bytes.Length)
            }
        }
        $payload = $stream.ToArray()
        try {
            return Get-Sha256Bytes -Bytes $payload
        } finally {
            [System.Array]::Clear($payload, 0, $payload.Length)
        }
    } finally {
        $stream.Dispose()
    }
}

function Get-NormalizedDeployment {
    param([Parameter(Mandatory = $true)][string]$BaseUrl)

    $trimmed = $BaseUrl.Trim().TrimEnd('/')
    try {
        $uri = [Uri]$trimmed
        if (-not $uri.IsAbsoluteUri -or
            -not [string]::IsNullOrWhiteSpace($uri.UserInfo) -or
            -not [string]::IsNullOrWhiteSpace($uri.Query) -or
            -not [string]::IsNullOrWhiteSpace($uri.Fragment)) {
            throw 'unsupported URL shape'
        }
        return $uri.AbsoluteUri.TrimEnd('/')
    } catch {
        throw 'The selected Provider base_url cannot be safely normalized for live Key-realm verification.'
    }
}

function Initialize-AtoapiLiveScopeDpapi {
    if ('AtoapiLiveScopeDpapi' -as [type]) {
        return
    }

    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class AtoapiLiveScopeDpapi
{
    [StructLayout(LayoutKind.Sequential)]
    private struct DATA_BLOB
    {
        public int cbData;
        public IntPtr pbData;
    }

    [DllImport("Crypt32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CryptUnprotectData(
        ref DATA_BLOB pDataIn,
        IntPtr ppszDataDescr,
        IntPtr pOptionalEntropy,
        IntPtr pvReserved,
        IntPtr pPromptStruct,
        int dwFlags,
        ref DATA_BLOB pDataOut);

    [DllImport("Kernel32.dll", SetLastError = true)]
    private static extern IntPtr LocalFree(IntPtr hMem);

    public static byte[] Unprotect(byte[] encrypted)
    {
        if (encrypted == null || encrypted.Length == 0)
        {
            throw new ArgumentException("DPAPI payload is empty", "encrypted");
        }

        IntPtr inputPointer = Marshal.AllocHGlobal(encrypted.Length);
        try
        {
            Marshal.Copy(encrypted, 0, inputPointer, encrypted.Length);
            DATA_BLOB input = new DATA_BLOB();
            input.cbData = encrypted.Length;
            input.pbData = inputPointer;
            DATA_BLOB output = new DATA_BLOB();
            if (!CryptUnprotectData(
                ref input,
                IntPtr.Zero,
                IntPtr.Zero,
                IntPtr.Zero,
                IntPtr.Zero,
                0,
                ref output))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }

            try
            {
                byte[] plain = new byte[output.cbData];
                Marshal.Copy(output.pbData, plain, 0, plain.Length);
                return plain;
            }
            finally
            {
                if (output.pbData != IntPtr.Zero)
                {
                    LocalFree(output.pbData);
                }
            }
        }
        finally
        {
            Marshal.FreeHGlobal(inputPointer);
        }
    }
}
'@
}

function Get-SecretDigest {
    param([Parameter(Mandatory = $true)][string]$EncryptedValue)

    $plainBytes = $null
    $secret = $null
    try {
        if ($EncryptedValue.StartsWith('dpapi:', [System.StringComparison]::Ordinal)) {
            Initialize-AtoapiLiveScopeDpapi
            $protectedBytes = [Convert]::FromBase64String($EncryptedValue.Substring(6))
            try {
                $plainBytes = [AtoapiLiveScopeDpapi]::Unprotect($protectedBytes)
            } finally {
                [System.Array]::Clear($protectedBytes, 0, $protectedBytes.Length)
            }
        } else {
            $plainBytes = [System.Text.Encoding]::UTF8.GetBytes($EncryptedValue)
        }
        $secret = [System.Text.UTF8Encoding]::new($false, $true).GetString($plainBytes).Trim()
        if ([string]::IsNullOrWhiteSpace($secret)) {
            throw 'saved Provider Key decrypted to an empty value'
        }
        return Get-Sha256Text -Text $secret
    } finally {
        if ($null -ne $plainBytes) {
            [System.Array]::Clear($plainBytes, 0, $plainBytes.Length)
        }
        $secret = $null
    }
}

function Test-ProviderKeyUsable {
    param([Parameter(Mandatory = $true)][string]$KeyBlock)

    if ((Get-TomlBoolean -Block $KeyBlock -Name 'enabled') -ne $true) {
        return $false
    }
    $disabledUntil = Get-TomlScalar -Block $KeyBlock -Name 'disabled_until'
    if ([string]::IsNullOrWhiteSpace($disabledUntil)) {
        return $true
    }
    try {
        $until = [DateTimeOffset]::Parse(
            $disabledUntil,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind
        )
        return $until -le [DateTimeOffset]::UtcNow
    } catch {
        throw 'A saved Provider Key has an invalid disabled_until value; live verification is refused.'
    }
}

function Get-ProviderKeyRealm {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$KeyId,
        [Parameter(Mandatory = $true)][string]$EncryptedKey,
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Channel,
        [Parameter(Mandatory = $true)][string]$Model
    )

    $secretDigest = Get-SecretDigest -EncryptedValue $EncryptedKey
    $keyRecord = Get-Sha256Parts -Parts @(
        'key-record-v2',
        $(if ([string]::IsNullOrWhiteSpace($KeyId)) { 'default' } else { $KeyId }),
        $secretDigest
    )
    return Get-Sha256Parts -Parts @(
        'cache-realm-v2',
        (Get-NormalizedDeployment -BaseUrl $BaseUrl),
        $Channel,
        $Model,
        $keyRecord
    )
}

function Get-SelectedProviderKeyReferenceV2 {
    param(
        [Parameter(Mandatory = $true)][string]$LocalKey,
        [Parameter(Mandatory = $true)][string]$WorkspaceFingerprint,
        [Parameter(Mandatory = $true)][string]$ProviderId,
        [Parameter(Mandatory = $true)][string]$KeyId
    )

    $localKey = $LocalKey.Trim()
    $workspaceFingerprint = $WorkspaceFingerprint.Trim()
    $providerId = $ProviderId.Trim()
    $keyId = $KeyId.Trim()
    if ([string]::IsNullOrWhiteSpace($localKey) -or
        [string]::IsNullOrWhiteSpace($workspaceFingerprint) -or
        [string]::IsNullOrWhiteSpace($providerId) -or
        [string]::IsNullOrWhiteSpace($keyId)) {
        return ''
    }
    return Get-Sha256Parts -Parts @(
        'selected-provider-key-ref-v2',
        $localKey,
        $workspaceFingerprint,
        $providerId,
        $keyId
    )
}

function Get-SelectedProviderKeyReference {
    param(
        [Parameter(Mandatory = $true)][string]$LocalKey,
        [Parameter(Mandatory = $true)][string]$WorkspaceFingerprint,
        [Parameter(Mandatory = $true)][string]$ProviderId,
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Channel,
        [Parameter(Mandatory = $true)][string]$Model,
        [Parameter(Mandatory = $true)][string]$KeyId,
        [Parameter(Mandatory = $true)][string]$EncryptedKey
    )

    $localKey = $LocalKey.Trim()
    $workspaceFingerprint = $WorkspaceFingerprint.Trim()
    $providerId = $ProviderId.Trim()
    $channel = $Channel.Trim()
    $model = $Model.Trim()
    $keyId = $KeyId.Trim()
    if ([string]::IsNullOrWhiteSpace($localKey) -or
        [string]::IsNullOrWhiteSpace($workspaceFingerprint) -or
        [string]::IsNullOrWhiteSpace($providerId) -or
        [string]::IsNullOrWhiteSpace($channel) -or
        [string]::IsNullOrWhiteSpace($model) -or
        [string]::IsNullOrWhiteSpace($keyId) -or
        [string]::IsNullOrWhiteSpace($EncryptedKey)) {
        return ''
    }
    return Get-Sha256Parts -Parts @(
        'selected-provider-key-ref-v3',
        $localKey,
        $workspaceFingerprint,
        $providerId,
        (Get-NormalizedDeployment -BaseUrl $BaseUrl),
        $channel,
        $model,
        $keyId,
        (Get-Sha256Text -Text $EncryptedKey)
    )
}

function Get-LatestCodexMainRecord {
    param([Parameter(Mandatory = $true)]$Metrics)

    $records = @(
        @($Metrics.recent_requests) +
        @($Metrics.recent_failed_requests)
    ) | Where-Object { $null -ne $_ }
    $candidates = foreach ($record in $records) {
        if ((Get-TomlObjectValue -Object $record -Name 'agent_id') -ne 'codex') {
            continue
        }
        if ((Get-TomlObjectValue -Object $record -Name 'upstream_call_source') -ne 'main') {
            continue
        }
        $at = Get-TomlObjectValue -Object $record -Name 'at'
        if ([string]::IsNullOrWhiteSpace($at)) {
            throw 'A Codex main request has no timestamp; live verification will not fall back to an older scope.'
        }
        try {
            [pscustomobject]@{
                At = [DateTimeOffset]::Parse(
                    $at,
                    [System.Globalization.CultureInfo]::InvariantCulture,
                    [System.Globalization.DateTimeStyles]::RoundtripKind
                )
                Status = [int](Get-TomlObjectValue -Object $record -Name 'status')
                ProviderId = Get-TomlObjectValue -Object $record -Name 'provider_id'
                Model = Get-TomlObjectValue -Object $record -Name 'model'
                EffectiveReasoningEffort = Get-TomlObjectValue -Object $record -Name 'effective_reasoning_effort'
                Realm = Get-TomlObjectValue -Object $record -Name 'shadow_affinity_realm_id'
                SelectedKeyRef = Get-TomlObjectValue -Object $record -Name 'selected_provider_key_ref'
                ClientChannel = Get-TomlObjectValue -Object $record -Name 'client_channel'
                Channel = Get-TomlObjectValue -Object $record -Name 'upstream_channel'
                CallKind = Get-TomlObjectValue -Object $record -Name 'upstream_call_kind'
                SseCompleted = Get-ObjectBoolean -Object $record -Name 'sse_completed_event_seen'
            }
        } catch {
            throw 'A Codex main request has an invalid timestamp; live verification will not fall back to an older scope.'
        }
    }
    return $candidates | Sort-Object At -Descending | Select-Object -First 1
}

function Resolve-LiveCodexScope {
    param(
        [Parameter(Mandatory = $true)][string]$ConfigDir,
        [Parameter(Mandatory = $true)][int]$MaximumAgeSeconds,
        [AllowEmptyString()][string]$RequestedKeyId = ''
    )

    $configPath = Join-Path $ConfigDir 'config.toml'
    $toml = Get-Content -LiteralPath $configPath -Raw
    $localKey = (Get-TomlScalar -Block $toml -Name 'local_key').Trim()
    if ([string]::IsNullOrWhiteSpace($localKey)) {
        throw 'The saved config has no local_key; live metrics verification cannot authenticate safely.'
    }
    $workspaceFingerprint = Get-TomlScalar -Block $toml -Name 'workspace_fingerprint'
    $metrics = Invoke-RestMethod -Uri 'http://127.0.0.1:18883/admin/metrics' -Method Get -Headers @{ Authorization = "Bearer $localKey" } -TimeoutSec 8
    $live = Get-LatestCodexMainRecord -Metrics $metrics
    if ($null -eq $live) {
        throw 'No recent terminal Codex main request is available; live comparison is refused.'
    }
    if ($live.Status -lt 200 -or $live.Status -ge 300) {
        throw 'The latest Codex main request failed; live verification will not fall back to an older Key or upstream.'
    }
    if ($live.ClientChannel -ne 'responses' -or $live.Channel -ne 'responses') {
        throw 'The latest Codex request is not a Responses-to-Responses route; live verification is refused.'
    }
    if ($live.CallKind -ne 'stream' -or $live.SseCompleted -ne $true) {
        throw 'The latest Codex main request is not a completed streaming SSE response; live verification is refused.'
    }
    if ([string]::IsNullOrWhiteSpace($live.ProviderId) -or
        [string]::IsNullOrWhiteSpace($live.Model) -or
        $live.Realm -notmatch '^[0-9a-f]{64}$') {
        throw 'The latest completed Codex request has incomplete provider/model/realm evidence; live verification is refused.'
    }
    $reasoningEffort = $live.EffectiveReasoningEffort.Trim().ToLowerInvariant()
    if (-not [string]::IsNullOrWhiteSpace($reasoningEffort) -and $reasoningEffort -notin @('none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra')) {
        throw 'The latest completed Codex request has an invalid effective reasoning effort; live verification is refused.'
    }
    $ageSeconds = ([DateTimeOffset]::UtcNow - $live.At).TotalSeconds
    if ($ageSeconds -gt $MaximumAgeSeconds) {
        throw "The newest Codex upstream record is $([math]::Floor($ageSeconds)) seconds old; send or wait for a live Codex request before verification."
    }

    $codexRoutes = @(Get-TomlArrayTableSlices -Toml $toml -TableName 'agent_injections' |
        Where-Object {
            (Get-TomlScalar -Block $_.Body -Name 'id') -eq 'codex' -and
            (Get-TomlBoolean -Block $_.Body -Name 'enabled') -eq $true
        })
    if ($codexRoutes.Count -ne 1) {
        throw 'The saved config must have exactly one enabled Codex injection before live verification.'
    }
    $route = $codexRoutes[0].Body
    $configuredProviderId = Get-TomlScalar -Block $route -Name 'provider_id'
    if ($configuredProviderId -ne $live.ProviderId) {
        throw "Latest live Codex provider '$($live.ProviderId)' does not match the currently saved Codex injection '$configuredProviderId'; verification is refused."
    }
    $configuredModel = Get-TomlScalar -Block $route -Name 'model_id'
    if (-not [string]::IsNullOrWhiteSpace($configuredModel) -and $configuredModel -ne $live.Model) {
        throw "Latest live Codex model '$($live.Model)' does not match the injection model '$configuredModel'; verification is refused."
    }

    $providerBlocks = @(Get-TomlArrayTableSlices -Toml $toml -TableName 'providers' |
        Where-Object { (Get-TomlScalar -Block $_.Body -Name 'id') -eq $live.ProviderId })
    if ($providerBlocks.Count -ne 1) {
        throw "The current live Provider '$($live.ProviderId)' is not uniquely present in config.toml."
    }
    $provider = $providerBlocks[0].Body
    if ((Get-TomlBoolean -Block $provider -Name 'enabled') -ne $true) {
        throw "The current live Provider '$($live.ProviderId)' is disabled in config.toml."
    }
    $baseUrl = Get-TomlScalar -Block $provider -Name 'base_url'
    if ([string]::IsNullOrWhiteSpace($baseUrl)) {
        throw "The current live Provider '$($live.ProviderId)' has no base_url."
    }
    $configuredChannel = Get-TomlScalar -Block $provider -Name 'channel'
    if ($configuredChannel -ne 'responses' -or $configuredChannel -ne $live.Channel) {
        throw "The current live Provider '$($live.ProviderId)' channel does not match the completed Codex Responses route; verification is refused."
    }

    $pinnedKeyId = ''
    $unreadableKeyCount = 0
    $poolContext = Get-ProviderKeyPoolContext -Toml $toml -ProviderId $live.ProviderId
    if ($null -ne $poolContext -and (Get-TomlBoolean -Block $poolContext.Pool -Name 'enabled') -eq $true) {
        $matchedKeyIds = @()
        $selectedKeyRef = $live.SelectedKeyRef.Trim()
        foreach ($keyBlock in $poolContext.Keys) {
            $keyId = Get-TomlScalar -Block $keyBlock -Name 'id'
            $encryptedKey = Get-TomlScalar -Block $keyBlock -Name 'key_encrypted'
            if (-not [string]::IsNullOrWhiteSpace($RequestedKeyId) -and $keyId -ne $RequestedKeyId) {
                continue
            }
            if ([string]::IsNullOrWhiteSpace($keyId) -or
                [string]::IsNullOrWhiteSpace($encryptedKey) -or
                -not (Test-ProviderKeyUsable -KeyBlock $keyBlock)) {
                continue
            }
            if (-not [string]::IsNullOrWhiteSpace($selectedKeyRef)) {
                # v3 binds local/workspace/provider plus the exact saved
                # encrypted Key record and live deployment/channel/model. It
                # can therefore map a desktop-user DPAPI Key without this
                # runner attempting CryptUnprotectData under a sandbox token.
                $candidateV3Reference = Get-SelectedProviderKeyReference `
                    -LocalKey $localKey `
                    -WorkspaceFingerprint $workspaceFingerprint `
                    -ProviderId $live.ProviderId `
                    -BaseUrl $baseUrl `
                    -Channel $live.Channel `
                    -Model $live.Model `
                    -KeyId $keyId `
                    -EncryptedKey $encryptedKey
                if ($candidateV3Reference -eq $selectedKeyRef) {
                    $matchedKeyIds += $keyId
                    continue
                }

                # Metrics written by the prior v2 diagnostic reference remain
                # readable. v2 did not bind encrypted material, so it retains
                # the old same-principal realm recomputation and can fail when
                # the current terminal identity cannot unlock DPAPI.
                $candidateV2Reference = Get-SelectedProviderKeyReferenceV2 -LocalKey $localKey -WorkspaceFingerprint $workspaceFingerprint -ProviderId $live.ProviderId -KeyId $keyId
                if ($candidateV2Reference -ne $selectedKeyRef) {
                    continue
                }
                try {
                    $candidateRealm = Get-ProviderKeyRealm -KeyId $keyId -EncryptedKey $encryptedKey -BaseUrl $baseUrl -Channel $live.Channel -Model $live.Model
                } catch {
                    throw "The Key selected by the latest live Codex v2 reference could not be decrypted for realm verification; run the v1.4.38 candidate once to emit a v3 reference before verification."
                }
                if ($candidateRealm -ne $live.Realm) {
                    throw "The Key selected by the latest live Codex v2 reference no longer matches its live realm; verification is refused."
                }
                $matchedKeyIds += $keyId
                continue
            }
            try {
                $candidateRealm = Get-ProviderKeyRealm -KeyId $keyId -EncryptedKey $encryptedKey -BaseUrl $baseUrl -Channel $live.Channel -Model $live.Model
            } catch {
                if (-not [string]::IsNullOrWhiteSpace($RequestedKeyId) -and $keyId -eq $RequestedKeyId) {
                    throw "The explicitly pinned live Key '$RequestedKeyId' could not be decrypted; verification is refused."
                }
                $unreadableKeyCount++
                continue
            }
            if ($candidateRealm -eq $live.Realm) {
                $matchedKeyIds += $keyId
            }
        }
        if ($matchedKeyIds.Count -ne 1) {
            if (-not [string]::IsNullOrWhiteSpace($selectedKeyRef)) {
                throw "The latest live Codex selected-Key reference could not be mapped to exactly one currently usable saved Key for Provider '$($live.ProviderId)'; verification is refused."
            }
            throw "The live Key realm could not be mapped to exactly one currently usable saved Key for Provider '$($live.ProviderId)'; verification is refused."
        }
        $pinnedKeyId = $matchedKeyIds[0]
    } else {
        $directKey = Get-TomlScalar -Block $provider -Name 'api_key_encrypted'
        if ([string]::IsNullOrWhiteSpace($directKey)) {
            $directKey = Get-TomlScalar -Block $provider -Name 'api_key'
        }
        if ([string]::IsNullOrWhiteSpace($directKey)) {
            throw "The current live Provider '$($live.ProviderId)' has no direct saved Key and no enabled Key pool; verification is refused."
        }
        $directRealm = Get-ProviderKeyRealm -KeyId '' -EncryptedKey $directKey -BaseUrl $baseUrl -Channel $live.Channel -Model $live.Model
        if ($directRealm -ne $live.Realm) {
            throw "The current direct Provider Key does not match the latest live Codex realm; verification is refused."
        }
    }

    return [pscustomobject]@{
        ProviderId = $live.ProviderId
        Model = $live.Model
        ReasoningEffort = $reasoningEffort
        KeyRealmHash = $live.Realm
        KeyId = $pinnedKeyId
        KeyPoolPinned = -not [string]::IsNullOrWhiteSpace($pinnedKeyId)
        UnreadableKeyCount = $unreadableKeyCount
        ObservedAt = $live.At
    }
}

function Assert-LiveScopeSelfTest {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )

    if (-not $Condition) {
        throw "live-scope self-test failed: $Message"
    }
}

function Invoke-LiveScopeSelfTest {
    $selectedReference = Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://provider-a.example/v1/' `
        -Channel 'responses' `
        -Model 'model-a' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:opaque-encrypted-key-a'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -match '^[0-9a-f]{64}$') -Message 'selected Key reference must be opaque SHA-256 material'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -eq 'df14495c4f3e6b1fc68cd3a3e7f564bec796df46eb2795a955bda5a1d237d4be') -Message 'selected Key reference must retain the Rust v3 hash contract'
    Assert-LiveScopeSelfTest -Condition (-not $selectedReference.Contains('key-a')) -Message 'selected Key reference must not expose the editor Key id'
    Assert-LiveScopeSelfTest -Condition (-not $selectedReference.Contains('opaque-encrypted-key-a')) -Message 'selected Key reference must not expose encrypted Key material'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -eq (Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://provider-a.example/v1' `
        -Channel 'responses' `
        -Model 'model-a' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:opaque-encrypted-key-a')) -Message 'selected Key reference must normalize deployment and remain deterministic'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -ne (Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://provider-a.example/v1' `
        -Channel 'responses' `
        -Model 'model-a' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:rotated-encrypted-key-a')) -Message 'selected Key reference must bind encrypted Key material'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -ne (Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://other-provider.example/v1' `
        -Channel 'responses' `
        -Model 'model-a' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:opaque-encrypted-key-a')) -Message 'selected Key reference must bind deployment'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -ne (Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://provider-a.example/v1' `
        -Channel 'chat' `
        -Model 'model-a' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:opaque-encrypted-key-a')) -Message 'selected Key reference must bind upstream channel'
    Assert-LiveScopeSelfTest -Condition ($selectedReference -ne (Get-SelectedProviderKeyReference `
        -LocalKey 'local-key-a' `
        -WorkspaceFingerprint 'workspace-a' `
        -ProviderId 'provider-a' `
        -BaseUrl 'https://provider-a.example/v1' `
        -Channel 'responses' `
        -Model 'model-b' `
        -KeyId 'key-a' `
        -EncryptedKey 'dpapi:opaque-encrypted-key-a')) -Message 'selected Key reference must bind upstream model'
    $legacySelectedReference = Get-SelectedProviderKeyReferenceV2 -LocalKey 'local-key-a' -WorkspaceFingerprint 'workspace-a' -ProviderId 'provider-a' -KeyId 'key-a'
    Assert-LiveScopeSelfTest -Condition ($legacySelectedReference -eq 'a350d3915563ac40e0942485cb8b06161b6e1156caa83dfb608a0334f4720300') -Message 'selected Key v2 compatibility contract must remain readable'

    $referenceRealm = Get-ProviderKeyRealm -KeyId 'key-a' -EncryptedKey 'secret-a' -BaseUrl 'https://provider-a.example/v1' -Channel 'responses' -Model 'model-a'
    $rotatedReferenceRealm = Get-ProviderKeyRealm -KeyId 'key-a' -EncryptedKey 'rotated-secret-a' -BaseUrl 'https://provider-a.example/v1' -Channel 'responses' -Model 'model-a'
    Assert-LiveScopeSelfTest -Condition ($referenceRealm -match '^[0-9a-f]{64}$') -Message 'selected Key realm must be SHA-256 material'
    Assert-LiveScopeSelfTest -Condition ($referenceRealm -ne $rotatedReferenceRealm) -Message 'the selected Key realm must change when a retained Key id receives a new secret'

    $boundaryToml = @'
[[provider_key_pools]]
provider_id = "provider-a"
enabled = true

[[provider_key_pools.keys]]
id = "key-a"
enabled = true

[[provider_channel_modes]]
key_encrypted = "must-not-leak-into-key-block"
'@
    $context = Get-ProviderKeyPoolContext -Toml $boundaryToml -ProviderId 'provider-a'
    Assert-LiveScopeSelfTest -Condition ($null -ne $context) -Message 'selected pool must be found'
    Assert-LiveScopeSelfTest -Condition ($context.Keys.Count -eq 1) -Message 'key slice must stop at the next arbitrary TOML table'
    Assert-LiveScopeSelfTest -Condition (
        [string]::IsNullOrWhiteSpace((Get-TomlScalar -Block $context.Keys[0] -Name 'key_encrypted'))
    ) -Message 'key slice must not consume fields from a following table'

    $commentedHeaderToml = @'
[[providers]] # selected route
id = "provider-a"

[provider_metadata] # must remain outside the Provider table
id = "must-not-leak-into-provider"
'@
    $commentedSlices = @(Get-TomlArrayTableSlices -Toml $commentedHeaderToml -TableName 'providers')
    Assert-LiveScopeSelfTest -Condition ($commentedSlices.Count -eq 1) -Message 'commented array-table header must be recognized'
    Assert-LiveScopeSelfTest -Condition (
        (Get-TomlScalar -Block $commentedSlices[0].Body -Name 'id') -eq 'provider-a'
    ) -Message 'commented table header must still form a slice boundary'

    $duplicatePoolToml = @'
[[provider_key_pools]]
provider_id = "provider-a"
enabled = true

[[provider_key_pools]]
provider_id = "provider-a"
enabled = true
'@
    $duplicateRejected = $false
    try {
        $null = Get-ProviderKeyPoolContext -Toml $duplicatePoolToml -ProviderId 'provider-a'
    } catch {
        $duplicateRejected = $true
    }
    Assert-LiveScopeSelfTest -Condition $duplicateRejected -Message 'ambiguous Provider Key pools must fail closed'

    $providerToml = @'
[[providers]]
id = "provider-a"
enabled = true

[provider_metadata]
id = "must-not-leak-into-provider"
'@
    $providerSlices = @(Get-TomlArrayTableSlices -Toml $providerToml -TableName 'providers')
    Assert-LiveScopeSelfTest -Condition ($providerSlices.Count -eq 1) -Message 'Provider table must be found'
    Assert-LiveScopeSelfTest -Condition (
        (Get-TomlScalar -Block $providerSlices[0].Body -Name 'id') -eq 'provider-a'
    ) -Message 'Provider slice must stop at a following single-bracket table'

    $realm = ('a' * 64) -join ''
    $selectionMetrics = [pscustomobject]@{
        recent_requests = @(
            [pscustomobject]@{
                at = '2026-08-10T00:00:00Z'
                agent_id = 'codex'
                upstream_call_source = 'main'
                status = 200
                provider_id = 'provider-a'
                model = 'model-a'
                shadow_affinity_realm_id = $realm
                client_channel = 'responses'
                upstream_channel = 'responses'
                upstream_call_kind = 'stream'
                sse_completed_event_seen = $true
            },
            [pscustomobject]@{
                at = '2026-08-10T00:00:02Z'
                agent_id = 'other'
                upstream_call_source = 'main'
                status = 200
            }
        )
        recent_failed_requests = @(
            [pscustomobject]@{
                at = '2026-08-10T00:00:01Z'
                agent_id = 'codex'
                upstream_call_source = 'main'
                status = 502
                provider_id = 'provider-a'
                model = 'model-a'
                shadow_affinity_realm_id = $realm
                client_channel = 'responses'
                upstream_channel = 'responses'
                upstream_call_kind = 'stream'
                sse_completed_event_seen = $false
            }
        )
    }
    $latest = Get-LatestCodexMainRecord -Metrics $selectionMetrics
    Assert-LiveScopeSelfTest -Condition ($latest.Status -eq 502) -Message 'a newer failed Codex request must not fall back to an older success'

    $malformedMetrics = [pscustomobject]@{
        recent_requests = @(
            $selectionMetrics.recent_requests[0],
            [pscustomobject]@{
                agent_id = 'codex'
                upstream_call_source = 'main'
                status = 200
            }
        )
        recent_failed_requests = @()
    }
    $malformedRejected = $false
    try {
        $null = Get-LatestCodexMainRecord -Metrics $malformedMetrics
    } catch {
        $malformedRejected = $true
    }
    Assert-LiveScopeSelfTest -Condition $malformedRejected -Message 'a malformed Codex main record must not fall back to an older scope'
}

if ($SelfTest) {
    Invoke-LiveScopeSelfTest
    Write-Output 'live-scope resolver self-test: passed'
    exit 0
}

# The dynamic-tail fixture is only meaningful at the requested half-million
# token class. The natural seed remains deliberately distinct from the dense
# tail: changing the seed to CJK-dense at the same character count would exceed
# the currently observed provider body ceiling sooner, not make the result more
# comparable.
if ($isDynamicTailMix) {
    if ($SeedContextChars -eq 0) {
        $SeedContextChars = if ($DynamicTailProfile -eq 'natural-dense') { 1500000 } else { 2350000 }
    }
    if ($MinimumSeedInputTokens -eq 0) {
        $MinimumSeedInputTokens = if ($DynamicTailProfile -eq 'natural-dense') { 250000 } else { 450000 }
    }
    if ($MinimumPeakInputTokens -eq 0 -and $DynamicTailProfile -eq 'natural-dense') {
        $MinimumPeakInputTokens = 450000
    }
    if ($MaximumPeakInputTokens -eq 0 -and $DynamicTailProfile -eq 'natural-dense') {
        $MaximumPeakInputTokens = 500000
    }
}

$defaultTurns = if ($isExactMediumToolTailMaturity) { 4 } elseif ($isDynamicTailMix) { 11 } else { 3 }
$turns = if ($Turns -gt 0) { $Turns } else { $defaultTurns }
if ($isDynamicTailMix -and (
    $turns -lt 3 -or (
        $turns % 2 -eq 0 -and -not $isLateShallowProviderWaterlineRollback
    )
)) {
    throw 'dynamic-tail-mix requires an odd -Turns value of at least 3, except the late shallow provider-waterline rollback probe which requires exactly 4 turns.'
}

if ($SeedToReuseDelayMs -gt 0) {
    if ($Scenario -ne 'dynamic-tail-mix' -or $turns -lt 3) {
        throw 'SeedToReuseDelayMs requires dynamic-tail-mix with at least three turns (seed, changing tail, direct successor).'
    }
    if (($Pairs - $WarmupPairs) -lt 2) {
        throw 'SeedToReuseDelayMs requires at least two scored pairs for reversed prime order.'
    }
    if (-not $SharedCacheCrossover) {
        throw 'SeedToReuseDelayMs requires SharedCacheCrossover so both arms share the same upstream cache lane.'
    }
    if (-not $CandidatePromptCacheRetention) {
        throw 'SeedToReuseDelayMs requires CandidatePromptCacheRetention as the only candidate treatment.'
    }
    if ($TurnDelayMs -ne 0 -or $InterArmDelayMs -ne 0) {
        throw 'SeedToReuseDelayMs requires TurnDelayMs=0 and InterArmDelayMs=0 so the horizon is the only injected pacing.'
    }
}

# Keep the default dense-tail probe below the current provider2 body ceiling;
# a larger value is explicit and must be justified by a fresh capacity result.
$toolChars = if ($ToolChars -gt 0) { $ToolChars } elseif ($isExactMediumToolTailMaturity) { 6144 } elseif ($isDynamicTailMix -and $DynamicTailProfile -eq 'natural-dense') { 80000 } elseif ($isDynamicTailMix) { 131072 } else { 40960 }
$toolCalls = if ($ToolCalls -gt 0) { $ToolCalls } elseif ($isDynamicTailMix) { 2 } else { 1 }

if (($isExerciseLocalPreviousResponseIdRebind -or $isExerciseLocalPreviousResponseIdFullReplay) -and $toolCalls -lt 1) {
    throw 'The local previous_response_id fixture requires at least one tool call.'
}

if ($isExactMediumToolTailMaturity) {
    if ($turns -ne 4) {
        throw 'Exact medium tool-tail maturity requires exactly -Turns 4.'
    }
    if ($toolChars -lt 4096 -or $toolChars -gt 8191 -or $toolCalls -ne 1) {
        throw 'Exact medium tool-tail maturity requires ToolChars from 4096 through 8191 and ToolCalls 1.'
    }
    if ($MinimumPeakInputTokens -eq 0) {
        $MinimumPeakInputTokens = 16384
    }
    if ($MinimumPeakInputTokens -lt 16384) {
        throw 'Exact medium tool-tail maturity requires MinimumPeakInputTokens of at least 16384.'
    }
}

foreach ($item in @(
    @{ Label = 'Atoapi config directory'; Path = $ConfigDir; Type = 'Container' },
    @{ Label = 'v1.4.33 hit-rate comparator executable'; Path = $ChampionExe; Type = 'Leaf' },
    @{ Label = 'v1.5.0 development-base executable'; Path = $CandidateExe; Type = 'Leaf' },
    @{ Label = 'release champion verifier'; Path = $verifier; Type = 'Leaf' }
)) {
    if (-not (Test-Path -LiteralPath $item.Path -PathType $item.Type)) {
        throw "$($item.Label) is missing: $($item.Path)"
    }
}

if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    throw 'Node.js is required to run scripts/verify-release-champion.mjs.'
}

if ($ProviderScope -ne 'codex-agent') {
    throw 'This interactive runner always follows the latest actual Codex upstream. Use provider-scope codex-agent; active-provider is intentionally not a substitute.'
}

$liveScope = Resolve-LiveCodexScope -ConfigDir $ConfigDir -MaximumAgeSeconds $LiveRecordMaxAgeSeconds -RequestedKeyId $KeyId
if ($PSBoundParameters.ContainsKey('Model') -and $Model -ne $liveScope.Model) {
    throw "Explicit Model '$Model' disagrees with the latest live Codex model '$($liveScope.Model)'; verification is refused."
}
if ($PSBoundParameters.ContainsKey('ReasoningEffort') -and $ReasoningEffort -ne $liveScope.ReasoningEffort) {
    throw "Explicit ReasoningEffort '$ReasoningEffort' disagrees with the latest live Codex effective reasoning effort '$($liveScope.ReasoningEffort)'; verification is refused."
}
if ($PSBoundParameters.ContainsKey('ProviderId') -and $ProviderId -ne $liveScope.ProviderId) {
    throw "Explicit ProviderId '$ProviderId' disagrees with the latest live Codex Provider '$($liveScope.ProviderId)'; verification is refused."
}
if ($PSBoundParameters.ContainsKey('KeyRealmHash') -and $KeyRealmHash -ne $liveScope.KeyRealmHash) {
    throw 'Explicit KeyRealmHash disagrees with the latest live Codex Key realm; verification is refused.'
}
if (-not [string]::IsNullOrWhiteSpace($KeyId) -and $KeyId -ne $liveScope.KeyId) {
    throw 'Explicit KeyId disagrees with the Key mapped from the latest live Codex realm; verification is refused.'
}

$Model = $liveScope.Model
$ReasoningEffort = $liveScope.ReasoningEffort
$ProviderId = $liveScope.ProviderId
$KeyRealmHash = $liveScope.KeyRealmHash
$KeyId = $liveScope.KeyId

if ($ResolveLiveScopeOnly -or $ResolveLiveScopeJson) {
    if ($ResolveLiveScopeJson) {
        [pscustomobject]@{
            provider_id = $ProviderId
            model_id = $Model
            reasoning_effort = $ReasoningEffort
            key_realm_hash = $KeyRealmHash
            observed_at = $liveScope.ObservedAt.UtcDateTime.ToString('o')
            key_pool_pinned = [bool]$liveScope.KeyPoolPinned
        } | ConvertTo-Json -Compress
        exit 0
    }
    Write-Host 'Live Codex scope resolved and configuration matched.'
    Write-Host "Provider: $ProviderId; model: $Model; Key realm: $($KeyRealmHash.Substring(0, 12))..."
    Write-Host "Observed at: $($liveScope.ObservedAt.UtcDateTime.ToString('o')); multi-Key pin: $($liveScope.KeyPoolPinned)"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($Output)) {
    $reportStem = if ($DiagnosticUserAgentSplit) {
        "diagnostic-user-agent-split-v1500-$Scenario-interactive-$stamp"
    } elseif ($isExerciseLocalPreviousResponseIdFullReplay) {
        "diagnostic-v1500-vs-champion-v1433-$Scenario-local-prid-full-replay-interactive-$stamp"
    } elseif ($ToolProtocol -eq 'custom') {
        "development-v1500-vs-champion-v1433-$Scenario-custom-tool-interactive-$stamp"
    } elseif ($isExerciseLocalPreviousResponseIdRebind) {
        "development-v1500-vs-champion-v1433-$Scenario-local-prid-rebind-interactive-$stamp"
    } else {
        "development-v1500-vs-champion-v1433-$Scenario-interactive-$stamp"
    }
    $Output = Join-Path $repoRoot "output\$reportStem.json"
}

# The embedded v1.5.0 desktop runner includes the capacity-aware dynamic
# profile: it hard-codes one scored pair and therefore inherits this script's
# 2.35M-character / 450k-token mixed default.  The currently selected live
# upstream has explicitly rejected that seed before either arm can produce a
# cache observation.  Detect only that fixed legacy caller shape and turn it
# into a symmetric, capacity-reachable dynamic A/B.  Direct invocations keep
# their explicit parameters unchanged, including the 500k-class profile.
$desktopReleaseOutputDirectory = Join-Path (Join-Path $ConfigDir 'release') 'release-champion'
$isLegacyDesktopRunner = $isDynamicTailMix `
    -and $ToolProtocol -eq 'function' `
    -and $Pairs -eq 1 `
    -and $WarmupPairs -eq 0 `
    -and -not $PSBoundParameters.ContainsKey('SeedContextChars') `
    -and -not $PSBoundParameters.ContainsKey('MinimumSeedInputTokens') `
    -and -not $PSBoundParameters.ContainsKey('MinimumPeakInputTokens') `
    -and -not $PSBoundParameters.ContainsKey('MaximumPeakInputTokens') `
    -and -not $PSBoundParameters.ContainsKey('DynamicTailProfile') `
    -and -not $PSBoundParameters.ContainsKey('SeedToReuseDelayMs') `
    -and -not $PSBoundParameters.ContainsKey('ToolChars') `
    -and [string]::Equals(
        [IO.Path]::GetFullPath((Split-Path -Parent $Output)),
        [IO.Path]::GetFullPath($desktopReleaseOutputDirectory),
        [StringComparison]::OrdinalIgnoreCase
    ) `
    -and (Split-Path -Leaf $Output).StartsWith('development-v1438-vs-champion-v1433-', [StringComparison]::OrdinalIgnoreCase)

if ($isLegacyDesktopRunner) {
    $Pairs = 3
    $WarmupPairs = 1
    $PairDelayMs = [Math]::Max($PairDelayMs, 1000)
    $DynamicTailProfile = 'natural-dense'
    $SeedContextChars = 900000
    $MinimumSeedInputTokens = 120000
    $MinimumPeakInputTokens = 0
    $MaximumPeakInputTokens = 0
    # The actual selected upstream accepted both large seed wires but rejected
    # the first replayed function-call-output tail with a 400.  Keep that as
    # a separate protocol-compatibility regression, and use changing text
    # tails for this cache A/B so both arms can complete the same dynamic
    # conversation instead of treating an upstream schema rejection as hit
    # evidence.
    $DynamicTailMode = 'text'
    $IncludeToolSchema = $false
    $toolChars = 80000
    $toolCalls = 2

    # Keep the fixed desktop route on the normal current candidate. The PCK
    # path remains an explicit isolated switch because its dynamic A/B result
    # must be positive and reproducible before it can influence a baseline.
    # Reverse the legacy first-run defaults so this ordinary replay supplies a
    # complementary ordering sample without altering either request body.
    $FirstArm = 'candidate'
    $PairOffset = 1
    $PersistentRuntimeStartOrder = 'candidate'
}

$outputDirectory = Split-Path -Parent $Output
if (-not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

Write-Host 'Runs a bounded, isolated A/B seed. The running 18883 instance is never stopped or signaled.'
Write-Host "Windows identity: $(whoami)"
Write-Host "Scenario: $Scenario; pairs: $Pairs; turns: $turns; model: $Model"
Write-Host "Effective reasoning effort: $(if ([string]::IsNullOrWhiteSpace($ReasoningEffort)) { 'unset' } else { $ReasoningEffort })"
if ($isLegacyDesktopRunner) {
    Write-Host 'Desktop runner compatibility profile: capacity-reachable dynamic crossover (not a 500k-class claim)'
}
Write-Host "Warm-up pairs (excluded from scoring): $WarmupPairs"
Write-Host "Pair cooldown: $PairDelayMs ms"
Write-Host "Response deadline: $ResponseTimeoutMs ms"
Write-Host "Turn delay: $TurnDelayMs ms; inter-arm delay: $InterArmDelayMs ms"
Write-Host "Seed-to-reuse horizon delay: $SeedToReuseDelayMs ms"
Write-Host "Seed context chars: $SeedContextChars; minimum seed input tokens: $MinimumSeedInputTokens; peak gate: $MinimumPeakInputTokens-$MaximumPeakInputTokens"
Write-Host "Stable instruction chars: $StableInstructionChars; candidate guarded-request minimum: $RequireCandidateGuardedRequests"
Write-Host "Require candidate exact medium tool-tail maturity wait: $isExactMediumToolTailMaturity"
Write-Host "Require candidate exact large message tail lag: $isExactLargeMessageTailLag"
Write-Host "Require candidate late shallow provider-waterline rollback wait: $isLateShallowProviderWaterlineRollback"
Write-Host "Dynamic tail profile: $DynamicTailProfile; mode: $DynamicTailMode; tool chars: $toolChars; strict end-to-end TTFT gate: $RequireTtftNoRegression; local pre-upstream gate: 0ms; input-token delta: $MaxInputTokenDelta"
Write-Host "Fixture profile: $FixtureProfile; tool output shape: $ToolOutputShape; include tool schema: $IncludeToolSchema"
Write-Host "Tool protocol: $ToolProtocol"
Write-Host "Exercise local previous_response_id rebind: $isExerciseLocalPreviousResponseIdRebind"
Write-Host "Exercise local previous_response_id unchanged FullReplay: $isExerciseLocalPreviousResponseIdFullReplay"
Write-Host "Provider: $ProviderId; Key realm: $($KeyRealmHash.Substring(0, 12))..."
Write-Host "Provider scope: $ProviderScope"
Write-Host "Pinned current multi-Key realm: $($liveScope.KeyPoolPinned)"
Write-Host "Cache comparison mode: $(if ($SharedCacheCrossover) { 'shared turn crossover' } else { 'isolated per-arm lanes' })"
Write-Host "Client prompt_cache_key: $(if ($NoClientPromptCacheKey) { 'not injected (native policy)' } else { 'generated common test key' })"
Write-Host "Candidate isolated upstream affinity: $CandidateUpstreamAffinity"
Write-Host "Candidate isolated generated prompt_cache_key: $CandidatePromptCacheKey"
Write-Host "Candidate thread-stable prompt_cache_key bridge: $CandidateThreadStablePromptCacheKeyBridge"
Write-Host "Candidate isolated prompt_cache_options: $CandidatePromptCacheOptions"
Write-Host "Candidate isolated prompt_cache_options ttl=24h: $CandidatePromptCacheOptions24h"
Write-Host "Require candidate options-24h sibling settle: $RequireCandidateOptions24hSiblingSettle"
Write-Host "Candidate isolated prompt_cache_retention: $CandidatePromptCacheRetention"
Write-Host "Candidate isolated upstream HTTP/1.1: $CandidateHttp1"
Write-Host "Candidate isolated provider-waterline recovery wait: $CandidateProviderWaterlineRecoveryWait"
Write-Host "Diagnostic same-binary User-Agent split: $DiagnosticUserAgentSplit"
if ($SharedCacheCrossover) {
    Write-Host "Persistent runtime start order: $PersistentRuntimeStartOrder"
}
if (-not [string]::IsNullOrWhiteSpace($UpstreamUserAgent)) {
    Write-Host 'Test upstream User-Agent: fixed across both isolated arms'
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
    '--provider-scope', $ProviderScope,
    '--provider-id', $ProviderId,
    '--live-codex-metrics-url', 'http://127.0.0.1:18883/admin/metrics',
    '--live-codex-max-age-seconds', "$LiveRecordMaxAgeSeconds",
    '--scenario', $Scenario,
    '--pairs', "$Pairs",
    '--warmup-pairs', "$WarmupPairs",
    '--first-arm', $FirstArm,
    '--pair-offset', "$PairOffset",
    '--pair-delay-ms', "$PairDelayMs",
    '--response-timeout-ms', "$ResponseTimeoutMs",
    '--seed-to-reuse-delay-ms', "$SeedToReuseDelayMs",
    '--turn-delay-ms', "$TurnDelayMs",
    '--inter-arm-delay-ms', "$InterArmDelayMs",
    '--turns', "$turns",
    '--max-output-tokens', '16',
    '--stable-instruction-chars', "$StableInstructionChars",
    '--seed-context-chars', "$SeedContextChars",
    '--minimum-seed-input-tokens', "$MinimumSeedInputTokens",
    '--minimum-peak-input-tokens', "$MinimumPeakInputTokens",
    '--maximum-peak-input-tokens', "$MaximumPeakInputTokens",
    '--max-input-token-delta', "$MaxInputTokenDelta",
    '--require-candidate-guarded-requests', "$RequireCandidateGuardedRequests",
    '--tool-chars', "$toolChars",
    '--tool-calls', "$toolCalls",
    '--tool-output-shape', $ToolOutputShape,
    '--tool-protocol', $ToolProtocol,
    '--include-tool-schema', "$IncludeToolSchema".ToLowerInvariant(),
    '--dynamic-tail-profile', $DynamicTailProfile,
    '--dynamic-tail-mode', $DynamicTailMode,
    '--fixture-profile', $FixtureProfile,
    '--max-local-proxy-overhead-regression-ms', '0',
    '--output', $Output
)

if ($isExerciseLocalPreviousResponseIdRebind) {
    $arguments += '--exercise-local-previous-response-id-rebind'
}

if ($isExerciseLocalPreviousResponseIdFullReplay) {
    $arguments += '--exercise-local-previous-response-id-full-replay'
}

if (-not [string]::IsNullOrWhiteSpace($ReasoningEffort)) {
    $arguments += @('--reasoning-effort', $ReasoningEffort)
}

if (-not $NoClientPromptCacheKey) {
    $arguments += @('--prompt-cache-key-prefix', "atoapi-release-champion-$stamp")
}

if ($CandidateUpstreamAffinity) {
    $arguments += '--candidate-upstream-affinity'
}

if ($CandidatePromptCacheKey) {
    $arguments += @('--candidate-cache-control-field', 'prompt-cache-key')
}

if ($CandidateThreadStablePromptCacheKeyBridge) {
    $arguments += '--candidate-thread-stable-pck-bridge'
}

if ($CandidatePromptCacheOptions) {
    $arguments += @('--candidate-cache-control-field', 'prompt-cache-options')
}

if ($CandidatePromptCacheOptions24h) {
    $arguments += '--candidate-cache-options-24h'
}

if ($RequireCandidateOptions24hSiblingSettle) {
    $arguments += '--require-candidate-options24h-sibling-settle'
}

if ($CandidatePromptCacheRetention) {
    $arguments += @('--candidate-cache-control-field', 'prompt-cache-retention')
}

if ($CandidateHttp1) {
    $arguments += '--candidate-http1'
}

if ($CandidateProviderWaterlineRecoveryWait) {
    $arguments += '--candidate-provider-waterline-recovery-wait'
}

if ($isExactMediumToolTailMaturity) {
    $arguments += '--require-candidate-exact-medium-tool-tail-maturity-wait'
}

if ($isExactLargeMessageTailLag) {
    $arguments += '--require-candidate-exact-large-message-tail-lag'
}

if ($isLateShallowProviderWaterlineRollback) {
    $arguments += '--require-candidate-late-shallow-provider-waterline-rollback-wait'
}


if ($DiagnosticUserAgentSplit) {
    $arguments += '--diagnostic-user-agent-split'
}

if (-not [string]::IsNullOrWhiteSpace($ChampionUpstreamUserAgent)) {
    $arguments += @('--champion-upstream-user-agent', $ChampionUpstreamUserAgent.Trim())
}

if (-not [string]::IsNullOrWhiteSpace($CandidateUpstreamUserAgent)) {
    $arguments += @('--candidate-upstream-user-agent', $CandidateUpstreamUserAgent.Trim())
}

if ($SharedCacheCrossover) {
    # The verifier rejects this mode without persistent isolated runtimes,
    # because every matching turn must alternate the first sender.
    $arguments += @(
        '--reuse-runtime-per-arm',
        '--shared-cache-crossover',
        '--persistent-runtime-start-order', $PersistentRuntimeStartOrder
    )
} else {
    $arguments += '--isolate-upstream-cache'
}

if ($RequireTtftNoRegression) {
    $arguments += '--require-ttft-no-regression'
} else {
    $arguments += '--allow-upstream-ttft-regression'
}

if (-not [string]::IsNullOrWhiteSpace($KeyId)) {
    $arguments += @('--key-id', $KeyId.Trim())
}

if (-not [string]::IsNullOrWhiteSpace($UpstreamUserAgent)) {
    $arguments += @('--upstream-user-agent', $UpstreamUserAgent.Trim())
}

& node @arguments
exit $LASTEXITCODE
