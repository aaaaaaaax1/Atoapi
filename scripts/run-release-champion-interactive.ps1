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

    [ValidateRange(1024, 512000)]
    [int]$ToolChars = 0,

    [ValidateRange(1, 8)]
    [int]$ToolCalls = 0,

    # Test-only override applied to both isolated arms.  This removes the
    # package-version default User-Agent as a cache-lane variable without
    # changing the live Provider configuration.
    [string]$UpstreamUserAgent = '',

    # By default the historical runner injects one generated client-owned
    # prompt_cache_key into both arms.  Native-policy comparisons must be able
    # to omit that test key so the candidate's own cache placement policy is
    # observable while retaining the same live-scope resolver and guards.
    [switch]$NoClientPromptCacheKey,

    # These values are refreshed from the latest successful live Codex request
    # before every run.  Supplying one is allowed only as an assertion: a stale
    # manual value fails closed instead of silently testing a previous upstream.
    [string]$Model = '',

    [string]$KeyRealmHash = '',

    [string]$ProviderId = '',

    [ValidateSet('codex-agent', 'active-provider')]
    [string]$ProviderScope = 'codex-agent',

    [string]$KeyId = '',

    [ValidateRange(30, 3600)]
    [int]$LiveRecordMaxAgeSeconds = 600,

    [switch]$ResolveLiveScopeOnly,

    [switch]$SelfTest,

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
    $localKey = Get-TomlScalar -Block $toml -Name 'local_key'
    $workspaceFingerprint = Get-TomlScalar -Block $toml -Name 'workspace_fingerprint'
    $metrics = Invoke-RestMethod -Uri 'http://127.0.0.1:18883/admin/metrics' -Method Get -TimeoutSec 8
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

$defaultTurns = if ($isDynamicTailMix) { 11 } else { 3 }
$turns = if ($Turns -gt 0) { $Turns } else { $defaultTurns }
if ($isDynamicTailMix -and ($turns -lt 3 -or $turns % 2 -eq 0)) {
    throw 'dynamic-tail-mix requires an odd -Turns value of at least 3 (seed, changing tail, direct follow-up).'
}
# Keep the default dense-tail probe below the current provider2 body ceiling;
# a larger value is explicit and must be justified by a fresh capacity result.
$toolChars = if ($ToolChars -gt 0) { $ToolChars } elseif ($isDynamicTailMix -and $DynamicTailProfile -eq 'natural-dense') { 80000 } elseif ($isDynamicTailMix) { 131072 } else { 40960 }
$toolCalls = if ($ToolCalls -gt 0) { $ToolCalls } elseif ($isDynamicTailMix) { 2 } else { 1 }

foreach ($item in @(
    @{ Label = 'Atoapi config directory'; Path = $ConfigDir; Type = 'Container' },
    @{ Label = 'v1.4.33 hit-rate comparator executable'; Path = $ChampionExe; Type = 'Leaf' },
    @{ Label = 'v1.4.38 development-base executable'; Path = $CandidateExe; Type = 'Leaf' },
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
$ProviderId = $liveScope.ProviderId
$KeyRealmHash = $liveScope.KeyRealmHash
$KeyId = $liveScope.KeyId

if ($ResolveLiveScopeOnly) {
    Write-Host 'Live Codex scope resolved and configuration matched.'
    Write-Host "Provider: $ProviderId; model: $Model; Key realm: $($KeyRealmHash.Substring(0, 12))..."
    Write-Host "Observed at: $($liveScope.ObservedAt.UtcDateTime.ToString('o')); multi-Key pin: $($liveScope.KeyPoolPinned)"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $repoRoot "output\development-v1438-vs-champion-v1433-$Scenario-interactive-$stamp.json"
}

$outputDirectory = Split-Path -Parent $Output
if (-not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

Write-Host 'Runs a bounded, isolated A/B seed. The running 18883 instance is never stopped or signaled.'
Write-Host "Windows identity: $(whoami)"
Write-Host "Scenario: $Scenario; pairs: $Pairs; turns: $turns; model: $Model"
Write-Host "Warm-up pairs (excluded from scoring): $WarmupPairs"
Write-Host "Pair cooldown: $PairDelayMs ms"
Write-Host "Turn delay: $TurnDelayMs ms; inter-arm delay: $InterArmDelayMs ms"
Write-Host "Seed context chars: $SeedContextChars; minimum seed input tokens: $MinimumSeedInputTokens; peak gate: $MinimumPeakInputTokens-$MaximumPeakInputTokens"
Write-Host "Dynamic tail profile: $DynamicTailProfile; mode: $DynamicTailMode; tool chars: $toolChars; strict end-to-end TTFT gate: $RequireTtftNoRegression; local pre-upstream gate: 0ms; input-token delta: $MaxInputTokenDelta"
Write-Host "Fixture profile: $FixtureProfile; tool output shape: $ToolOutputShape; include tool schema: $IncludeToolSchema"
Write-Host "Provider: $ProviderId; Key realm: $($KeyRealmHash.Substring(0, 12))..."
Write-Host "Provider scope: $ProviderScope"
Write-Host "Pinned current multi-Key realm: $($liveScope.KeyPoolPinned)"
Write-Host "Cache comparison mode: $(if ($SharedCacheCrossover) { 'shared turn crossover' } else { 'isolated per-arm lanes' })"
Write-Host "Client prompt_cache_key: $(if ($NoClientPromptCacheKey) { 'not injected (native policy)' } else { 'generated common test key' })"
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
    '--turn-delay-ms', "$TurnDelayMs",
    '--inter-arm-delay-ms', "$InterArmDelayMs",
    '--turns', "$turns",
    '--max-output-tokens', '16',
    '--stable-instruction-chars', '16384',
    '--seed-context-chars', "$SeedContextChars",
    '--minimum-seed-input-tokens', "$MinimumSeedInputTokens",
    '--minimum-peak-input-tokens', "$MinimumPeakInputTokens",
    '--maximum-peak-input-tokens', "$MaximumPeakInputTokens",
    '--max-input-token-delta', "$MaxInputTokenDelta",
    '--tool-chars', "$toolChars",
    '--tool-calls', "$toolCalls",
    '--tool-output-shape', $ToolOutputShape,
    '--include-tool-schema', "$IncludeToolSchema".ToLowerInvariant(),
    '--dynamic-tail-profile', $DynamicTailProfile,
    '--dynamic-tail-mode', $DynamicTailMode,
    '--fixture-profile', $FixtureProfile,
    '--max-local-proxy-overhead-regression-ms', '0',
    '--output', $Output
)

if (-not $NoClientPromptCacheKey) {
    $arguments += @('--prompt-cache-key-prefix', "atoapi-release-champion-$stamp")
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
