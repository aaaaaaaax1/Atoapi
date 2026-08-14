use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
};
use uuid::Uuid;

use crate::crypto::{decrypt_secret, encrypt_secret};

fn default_proxy_auto_start() -> bool {
    true
}

fn default_proxy_mode_host() -> String {
    "127.0.0.1".to_string()
}

fn default_proxy_mode_port() -> u16 {
    18884
}

fn default_prompt_cache_retention_enabled() -> bool {
    true
}

fn default_request_body_gzip_enabled() -> bool {
    true
}

fn default_use_system_proxy() -> bool {
    true
}

fn default_compact_compatibility_mode() -> CompactCompatibilityMode {
    CompactCompatibilityMode::CcSwitchFast
}

fn default_provider_channel_mode() -> ProviderChannelMode {
    ProviderChannelMode::Auto
}

fn default_key_failure_threshold() -> u32 {
    3
}

fn default_key_recovery_minutes() -> u32 {
    5
}

fn default_key_priority() -> u32 {
    5
}

fn default_new_key_pool_strategy() -> KeyLoadBalanceStrategy {
    KeyLoadBalanceStrategy::Sequential
}

// v1.4.12 exposed this as a built-in default rather than a user-authored
// provider override.  Leaving it persisted pins later releases to an old
// upstream User-Agent/cache lane.  Migrate only this exact historical value;
// all other custom User-Agents remain user-owned and untouched.
const LEGACY_STATIC_DEFAULT_USER_AGENT: &str = "Atoapi/1.4.12";

fn key_failure_exhausts_account_capacity(message: &str) -> bool {
    let normalized = message.to_lowercase();
    [
        "insufficient_quota",
        "quota exceeded",
        "quota exhausted",
        "quota is exhausted",
        "exceeded your current quota",
        "out of credits",
        "credits exhausted",
        "credit balance",
        "insufficient balance",
        "balance is exhausted",
        "billing hard limit",
        "billing limit",
        "payment required",
        "http 402",
        "余额不足",
        "余额耗尽",
        "余额",
        "额度不足",
        "额度耗尽",
        "欠费",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Chat,
    Responses,
    Anthropic,
}

impl Channel {
    pub fn endpoint_path(&self) -> &'static str {
        match self {
            Channel::Chat => "/chat/completions",
            Channel::Responses => "/responses",
            Channel::Anthropic => "/messages",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Channel::Chat => "chat",
            Channel::Responses => "responses",
            Channel::Anthropic => "anthropic",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_proxy_auto_start")]
    pub proxy_auto_start: bool,
    #[serde(default = "default_proxy_mode_host")]
    pub proxy_mode_host: String,
    #[serde(default = "default_proxy_mode_port")]
    pub proxy_mode_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_proxy_url: Option<String>,
    pub local_key: String,
    pub default_channel: Channel,
    #[serde(default)]
    pub active_provider_id: Option<String>,
    pub workspace_fingerprint: String,
    pub providers: Vec<ProviderConfig>,
    pub route_profiles: Vec<RouteProfile>,
    pub cache: CacheConfig,
    #[serde(default = "default_agent_injections")]
    pub agent_injections: Vec<AgentInjectionConfig>,
    #[serde(default)]
    pub agent_provider_orders: Vec<AgentProviderOrderConfig>,
    #[serde(default)]
    pub provider_key_pools: Vec<ProviderKeyPoolConfig>,
    #[serde(default)]
    pub provider_compact_modes: Vec<ProviderCompactModeConfig>,
    /// Per-upstream Codex auto-compaction override. It is applied only when
    /// that upstream is the active bound Codex route; absent means preserve
    /// Codex's own model/default behavior.
    #[serde(default)]
    pub provider_auto_compaction_limits: Vec<ProviderAutoCompactionConfig>,
    #[serde(default)]
    pub provider_channel_modes: Vec<ProviderChannelModeConfig>,
    /// Retired v1.4.4 session-reuse certificates are read once so loading an
    /// older configuration can rewrite it without preserving a feature that
    /// no longer exists in the request path.
    #[serde(default, rename = "provider_response_session_reuse", skip_serializing)]
    _legacy_provider_response_session_reuse: Option<toml::Value>,
    #[serde(default)]
    pub provider_cache_capabilities: Vec<ProviderCacheCapabilityConfig>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models_url: Option<String>,
    #[serde(default)]
    pub is_full_url: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_user_agent: Option<String>,
    pub channel: Channel,
    #[serde(default = "default_prompt_cache_retention_enabled")]
    pub prompt_cache_retention_enabled: bool,
    #[serde(default = "default_request_body_gzip_enabled")]
    pub request_body_gzip_enabled: bool,
    #[serde(default = "default_use_system_proxy")]
    pub use_system_proxy: bool,
    pub api_key_encrypted: Option<String>,
    pub models: Vec<ModelConfig>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInput {
    pub id: Option<String>,
    /// Ephemeral UI ownership hint used by the control plane before this input
    /// is persisted as a provider record.
    #[serde(default)]
    pub owner_agent_id: Option<String>,
    pub name: String,
    pub base_url: String,
    pub models_url: Option<String>,
    #[serde(default)]
    pub is_full_url: bool,
    pub custom_user_agent: Option<String>,
    #[serde(default = "default_provider_channel_mode")]
    pub channel_mode: ProviderChannelMode,
    pub channel: Channel,
    #[serde(default = "default_prompt_cache_retention_enabled")]
    pub prompt_cache_retention_enabled: bool,
    #[serde(default = "default_request_body_gzip_enabled")]
    pub request_body_gzip_enabled: bool,
    #[serde(default = "default_use_system_proxy")]
    pub use_system_proxy: bool,
    #[serde(default)]
    pub non_sse_compact_compat_enabled: bool,
    /// Positive token limit asks the bound Codex client to compact before
    /// reaching that budget. `None` preserves the client/model default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_compact_token_limit: Option<u32>,
    /// Keeps older API callers that do not know this field from clearing a
    /// saved per-provider override. Current UI sends this for both set and
    /// explicit clear operations.
    #[serde(default)]
    pub auto_compact_token_limit_configured: bool,
    pub api_key: Option<String>,
    #[serde(default)]
    pub key_pool: Option<ProviderKeyPoolInput>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CompactCompatibilityMode {
    CcSwitchFast,
    NonSseValidation,
}

impl Default for CompactCompatibilityMode {
    fn default() -> Self {
        Self::CcSwitchFast
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCompactModeConfig {
    pub provider_id: String,
    #[serde(default = "default_compact_compatibility_mode")]
    pub mode: CompactCompatibilityMode,
    pub updated_at: DateTime<Utc>,
}

/// Stored separately from `ProviderConfig` so older provider records retain a
/// fully compatible shape and an unset override can be removed cleanly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAutoCompactionConfig {
    pub provider_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_limit: Option<u32>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderChannelMode {
    Auto,
    Manual,
}

impl Default for ProviderChannelMode {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderChannelModeConfig {
    pub provider_id: String,
    #[serde(default = "default_provider_channel_mode")]
    pub mode: ProviderChannelMode,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderCacheCapabilityField {
    PromptCacheKey,
    PromptCacheRetention,
    PromptCacheOptions,
    PromptCacheBreakpoint,
}

impl ProviderCacheCapabilityField {
    pub fn json_name(self) -> &'static str {
        match self {
            Self::PromptCacheKey => "prompt_cache_key",
            Self::PromptCacheRetention => "prompt_cache_retention",
            Self::PromptCacheOptions => "prompt_cache_options",
            Self::PromptCacheBreakpoint => "prompt_cache_breakpoint",
        }
    }

    pub const ALL: [Self; 4] = [
        Self::PromptCacheKey,
        Self::PromptCacheRetention,
        Self::PromptCacheOptions,
        Self::PromptCacheBreakpoint,
    ];
}

const PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderCacheCapabilityStatus {
    Unverified,
    Verified,
    Unsupported,
    Error,
}

impl Default for ProviderCacheCapabilityStatus {
    fn default() -> Self {
        Self::Unverified
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderCacheEffectStatus {
    Unverified,
    Promoted,
    NoBenefit,
    Error,
}

impl Default for ProviderCacheEffectStatus {
    fn default() -> Self {
        Self::Unverified
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCacheCapabilityConfig {
    pub provider_id: String,
    pub model_id: String,
    pub channel: Channel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// Opaque exact realm and request-shape identifier for measured cache
    /// effect evidence. Capability acceptance remains key-scoped, while a
    /// promoted control may only affect the wire shape that proved it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_scope_id: Option<String>,
    pub field: ProviderCacheCapabilityField,
    #[serde(default)]
    pub evidence_version: u32,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub status: ProviderCacheCapabilityStatus,
    #[serde(default)]
    pub effect_status: ProviderCacheEffectStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_ttft_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_ttft_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

fn provider_cache_capability_evidence_is_current(item: &ProviderCacheCapabilityConfig) -> bool {
    item.field != ProviderCacheCapabilityField::PromptCacheBreakpoint
        || item.evidence_version >= PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION
}

/// A promoted cache control must name the exact key realm, stream/store shape,
/// and final-wire breakpoint placement used by its validation. v1 records did
/// not bind that last dimension and can never authorize ordinary traffic.
fn provider_cache_effect_scope_is_current_v2(
    scope: Option<&str>,
    field: ProviderCacheCapabilityField,
) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    let Some((base, breakpoint)) = scope.rsplit_once(":bp=") else {
        return false;
    };
    if breakpoint.is_empty() || breakpoint.chars().any(char::is_whitespace) {
        return false;
    }
    let mut parts = base.split(':');
    let version = parts.next();
    let realm = parts.next();
    let stream = parts.next();
    let store = parts.next();
    if parts.next().is_some()
        || version != Some("cache-effect-v2")
        || realm.is_none_or(str::is_empty)
        || !matches!(stream, Some("stream" | "sync" | "stream-absent"))
        || !matches!(store, Some("store" | "no-store" | "store-absent"))
    {
        return false;
    }
    field != ProviderCacheCapabilityField::PromptCacheBreakpoint || breakpoint != "none"
}

/// A generated `prompt_cache_key` has a different safety contract from every
/// other cache control: its stable value is bound to the exact selected-Key
/// realm and trusted session identity. Earlier records may have measured a
/// caller, cohort, or different session key, so they cannot authorize this
/// strategy.
fn generated_prompt_cache_key_effect_scope_is_current_v4(scope: Option<&str>) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    let mut parts = scope.split(':');
    let version = parts.next();
    let realm = parts.next();
    let stream = parts.next();
    let store = parts.next();
    let strategy = parts.next();
    let session_scope = parts.next();
    parts.next().is_none()
        && version == Some("cache-effect-v4")
        && realm.is_some_and(|value| !value.is_empty())
        && matches!(stream, Some("stream" | "sync" | "stream-absent"))
        && matches!(store, Some("store" | "no-store" | "store-absent"))
        && strategy == Some("pk=realm-session-v1")
        && session_scope
            .and_then(|value| value.strip_prefix("sid="))
            .is_some_and(|value| {
                value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
}

fn provider_cache_effect_scope_is_current(
    scope: Option<&str>,
    field: ProviderCacheCapabilityField,
) -> bool {
    if field == ProviderCacheCapabilityField::PromptCacheKey {
        generated_prompt_cache_key_effect_scope_is_current_v4(scope)
    } else {
        provider_cache_effect_scope_is_current_v2(scope, field)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCacheCapabilityProbeFieldResult {
    pub field: ProviderCacheCapabilityField,
    pub status: ProviderCacheCapabilityStatus,
    pub enabled: bool,
    pub effect_status: ProviderCacheEffectStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCacheCapabilityProbeResult {
    pub provider_id: String,
    pub model_id: String,
    pub channel: Channel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub baseline_status: Option<u16>,
    pub fields: Vec<ProviderCacheCapabilityProbeFieldResult>,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCacheCapabilityProbeInput {
    pub provider_id: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCacheCapabilityProbeTarget {
    provider_id: String,
    base_url: String,
    is_full_url: bool,
    channel: Channel,
    channel_mode: ProviderChannelMode,
    provider_updated_at: DateTime<Utc>,
    key_pool_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum KeyLoadBalanceStrategy {
    RoundRobin,
    Priority,
    LeastUsed,
    Random,
    Sequential,
}

impl Default for KeyLoadBalanceStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKeyStatus {
    Unknown,
    Healthy,
    Unhealthy,
}

impl Default for ProviderKeyStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyPoolConfig {
    pub provider_id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub strategy: KeyLoadBalanceStrategy,
    #[serde(default = "default_key_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_key_recovery_minutes")]
    pub recovery_minutes: u32,
    #[serde(default)]
    pub next_index: usize,
    #[serde(default)]
    pub keys: Vec<ProviderKeyConfig>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default)]
    pub key_encrypted: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_key_priority")]
    pub priority: u32,
    #[serde(default)]
    pub status: ProviderKeyStatus,
    #[serde(default)]
    pub total_requests: u64,
    #[serde(default)]
    pub successes: u64,
    #[serde(default)]
    pub failures: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyPoolInput {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_new_key_pool_strategy")]
    pub strategy: KeyLoadBalanceStrategy,
    #[serde(default = "default_key_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_key_recovery_minutes")]
    pub recovery_minutes: u32,
    #[serde(default)]
    pub keys: Vec<ProviderKeyInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyInput {
    pub id: Option<String>,
    pub alias: Option<String>,
    pub key: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_key_priority")]
    pub priority: u32,
    #[serde(default)]
    pub status: ProviderKeyStatus,
    #[serde(default)]
    pub total_requests: u64,
    #[serde(default)]
    pub successes: u64,
    #[serde(default)]
    pub failures: u64,
    #[serde(default)]
    pub last_checked_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub disabled_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_model_id: Option<String>,
    pub display_name: String,
    pub context_window: Option<u32>,
    pub output_window: Option<u32>,
    #[serde(default)]
    pub reasoning_effort_override_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_reasoning_efforts: Vec<String>,
    pub supports_tools: bool,
    pub supports_streaming: bool,
    pub enabled: bool,
}

pub const REASONING_EFFORT_VALUES: [&str; 8] = [
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
];

pub fn normalize_reasoning_effort(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    REASONING_EFFORT_VALUES
        .contains(&normalized.as_str())
        .then_some(normalized)
}

pub fn normalize_reasoning_efforts(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        let Some(value) = normalize_reasoning_effort(value) else {
            continue;
        };
        if !normalized.contains(&value) {
            normalized.push(value);
        }
    }
    normalized.sort_by_key(|value| {
        REASONING_EFFORT_VALUES
            .iter()
            .position(|candidate| candidate == value)
            .unwrap_or(REASONING_EFFORT_VALUES.len())
    });
    normalized
}

pub fn codex_model_alias(model_id: &str) -> Option<String> {
    let trimmed = model_id.trim();
    let alias = trimmed.rsplit('/').next().unwrap_or(trimmed).trim();
    if alias.is_empty() || alias == trimmed {
        return None;
    }
    Some(alias.to_ascii_lowercase())
}

pub fn model_request_alias(model: &ModelConfig) -> Option<String> {
    model
        .request_model_id
        .as_deref()
        .map(str::trim)
        .filter(|alias| !alias.is_empty() && *alias != model.id.trim())
        .map(ToOwned::to_owned)
}

pub fn provider_model_cache_key(provider: &ProviderConfig, requested_model: &str) -> String {
    let requested = requested_model.trim();
    if requested.is_empty() {
        return String::new();
    }
    let requested_lower = requested.to_ascii_lowercase();
    if let Some(model) = provider.models.iter().find(|model| {
        model.enabled
            && (model.id == requested
                || model_request_alias(model)
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(requested))
                || codex_model_alias(&model.id).is_some_and(|alias| alias == requested_lower))
    }) {
        return model.id.clone();
    }

    let mut enabled_models = provider.models.iter().filter(|model| model.enabled);
    let Some(only_model) = enabled_models.next() else {
        return requested.to_string();
    };
    if enabled_models.next().is_none() {
        only_model.id.clone()
    } else {
        requested.to_string()
    }
}

pub fn codex_model_display_name(model_id: &str) -> String {
    model_id
        .split('-')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            if part.eq_ignore_ascii_case("gpt") {
                "GPT".to_string()
            } else {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => {
                        format!(
                            "{}{}",
                            first.to_uppercase(),
                            chars.as_str().to_ascii_lowercase()
                        )
                    }
                    None => String::new(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteProfile {
    pub name: String,
    pub client_channel: Channel,
    pub upstream_channel: Channel,
    pub provider_id: Option<String>,
    pub model_alias: Option<String>,
    pub long_context_threshold: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CacheMode {
    PassiveWarm,
    SessionPrewarm,
    PrefixPrewarm,
}

fn default_cache_mode() -> CacheMode {
    CacheMode::PrefixPrewarm
}

fn default_background_prewarm_enabled() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_mode")]
    pub mode: CacheMode,
    pub enabled: bool,
    pub exact_enabled: bool,
    pub semantic_enabled: bool,
    pub semantic_threshold: f32,
    pub max_age_seconds: u64,
    pub max_entries: usize,
    pub persist_encrypted: bool,
    pub prewarm_enabled: bool,
    #[serde(default = "default_background_prewarm_enabled")]
    pub background_prewarm_enabled: bool,
}

impl CacheConfig {
    pub fn smart_max_hit() -> Self {
        Self {
            mode: CacheMode::PrefixPrewarm,
            enabled: true,
            exact_enabled: true,
            semantic_enabled: true,
            semantic_threshold: 0.985,
            max_age_seconds: 86_400,
            max_entries: 300_000,
            persist_encrypted: true,
            prewarm_enabled: true,
            background_prewarm_enabled: false,
        }
    }

    pub fn normalize_fast_forwarding_hit_policy(&mut self) {
        if self.enabled {
            self.exact_enabled = true;
            self.semantic_enabled = true;
            if matches!(self.mode, CacheMode::PassiveWarm) {
                self.prewarm_enabled = false;
            }
        } else {
            self.exact_enabled = false;
            self.semantic_enabled = false;
            self.prewarm_enabled = false;
        }
        // Active companion prewarm is intentionally kept off the foreground
        // path: hit-rate learning happens from real requests and cache writes,
        // while current requests continue forwarding immediately.
        self.background_prewarm_enabled = false;
        self.semantic_threshold = 0.985;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentInjectionKind {
    ClaudeCode,
    Codex,
    ClaudeDesktop,
    Gemini,
    #[serde(alias = "opencode")]
    OpenCode,
    #[serde(alias = "openclaw")]
    OpenClaw,
    Hermes,
    ProxyMode,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentInjectionConfig {
    pub id: String,
    pub label: String,
    pub kind: AgentInjectionKind,
    pub enabled: bool,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub target_path: Option<PathBuf>,
    #[serde(default)]
    pub last_injected_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hidden_provider_ids: Vec<String>,
}

/// Presentation-only provider order. Keeping it outside the Agent injection
/// record avoids changing routing semantics and preserves each Agent's list
/// independently from the global provider collection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentProviderOrderConfig {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicProvider {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub models_url: Option<String>,
    pub is_full_url: bool,
    pub custom_user_agent: Option<String>,
    pub channel_mode: ProviderChannelMode,
    pub channel: Channel,
    pub prompt_cache_retention_enabled: bool,
    pub request_body_gzip_enabled: bool,
    pub use_system_proxy: bool,
    pub non_sse_compact_compat_enabled: bool,
    pub auto_compact_token_limit: Option<u32>,
    #[serde(default)]
    pub cache_capabilities: Vec<ProviderCacheCapabilityConfig>,
    pub has_api_key: bool,
    pub key_pool: Option<PublicProviderKeyPool>,
    pub models: Vec<ModelConfig>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicProviderKeyPool {
    pub enabled: bool,
    pub strategy: KeyLoadBalanceStrategy,
    pub failure_threshold: u32,
    pub recovery_minutes: u32,
    pub available_keys: usize,
    pub keys: Vec<PublicProviderKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicProviderKey {
    pub id: String,
    pub alias: Option<String>,
    pub preview: String,
    #[serde(default)]
    pub has_saved_secret: bool,
    pub enabled: bool,
    pub priority: u32,
    pub status: ProviderKeyStatus,
    pub total_requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub disabled_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SelectedProviderKey {
    pub secret: String,
    pub key_id: Option<String>,
    /// SHA-256 of the saved `key_encrypted` record, never of the decrypted
    /// upstream secret.  This lets local diagnostics identify a rotated
    /// desktop-user DPAPI record without exposing or decrypting it again.
    pub encrypted_material_digest: Option<String>,
}

pub(crate) fn encrypted_provider_key_material_digest(encrypted: &str) -> Option<String> {
    (!encrypted.trim().is_empty()).then(|| {
        let digest = Sha256::digest(encrypted.as_bytes());
        format!("{digest:x}")
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicConfig {
    pub host: String,
    pub port: u16,
    pub proxy_auto_start: bool,
    pub proxy_mode_host: String,
    pub proxy_mode_port: u16,
    pub upstream_proxy_url: Option<String>,
    pub local_key: String,
    pub default_channel: Channel,
    pub active_provider_id: Option<String>,
    pub workspace_fingerprint: String,
    pub providers: Vec<PublicProvider>,
    pub route_profiles: Vec<RouteProfile>,
    pub cache: CacheConfig,
    pub agent_injections: Vec<AgentInjectionConfig>,
    pub agent_provider_orders: Vec<AgentProviderOrderConfig>,
    pub provider_key_pools: Vec<PublicProviderKeyPoolEntry>,
    pub updated_at: DateTime<Utc>,
    pub config_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicProviderKeyPoolEntry {
    pub provider_id: String,
    pub pool: PublicProviderKeyPool,
}

impl Default for AppConfig {
    fn default() -> Self {
        let now = Utc::now();
        let local_key = format!("ato-{}", Uuid::new_v4().simple());

        Self {
            host: "127.0.0.1".to_string(),
            port: 18883,
            proxy_auto_start: default_proxy_auto_start(),
            proxy_mode_host: default_proxy_mode_host(),
            proxy_mode_port: default_proxy_mode_port(),
            upstream_proxy_url: None,
            local_key,
            default_channel: Channel::Anthropic,
            active_provider_id: None,
            workspace_fingerprint: "default-workspace".to_string(),
            providers: Vec::new(),
            route_profiles: vec![
                RouteProfile {
                    name: "anthropic".to_string(),
                    client_channel: Channel::Anthropic,
                    upstream_channel: Channel::Anthropic,
                    provider_id: None,
                    model_alias: None,
                    long_context_threshold: 60_000,
                },
                RouteProfile {
                    name: "chat".to_string(),
                    client_channel: Channel::Chat,
                    upstream_channel: Channel::Chat,
                    provider_id: None,
                    model_alias: None,
                    long_context_threshold: 60_000,
                },
                RouteProfile {
                    name: "responses".to_string(),
                    client_channel: Channel::Responses,
                    upstream_channel: Channel::Responses,
                    provider_id: None,
                    model_alias: None,
                    long_context_threshold: 60_000,
                },
            ],
            cache: CacheConfig::smart_max_hit(),
            agent_injections: default_agent_injections(),
            agent_provider_orders: Vec::new(),
            provider_key_pools: Vec::new(),
            provider_compact_modes: Vec::new(),
            provider_auto_compaction_limits: Vec::new(),
            provider_channel_modes: Vec::new(),
            _legacy_provider_response_session_reuse: None,
            provider_cache_capabilities: Vec::new(),
            updated_at: now,
        }
    }
}

fn proxy_bind_conflicts(host: &str, port: u16, other_host: &str, other_port: u16) -> Result<bool> {
    if port != other_port {
        return Ok(false);
    }
    let ip = host.parse::<IpAddr>()?;
    let other_ip = other_host.parse::<IpAddr>()?;
    Ok(ip == other_ip || ip.is_unspecified() || other_ip.is_unspecified())
}

fn provider_is_private_to_agent_for_migration(provider_id: &str, agent_id: &str) -> bool {
    provider_id.starts_with(&format!(
        "agent-{}-",
        sanitize_agent_provider_id_part_for_migration(agent_id)
    ))
}

fn unique_agent_provider_id_for_migration(
    config: &AppConfig,
    agent_id: &str,
    source_provider_id: &str,
) -> String {
    let base = format!(
        "agent-{}-{}",
        sanitize_agent_provider_id_part_for_migration(agent_id),
        sanitize_agent_provider_id_part_for_migration(source_provider_id),
    );
    let mut candidate = base.clone();
    let mut suffix = 2;
    while config
        .providers
        .iter()
        .any(|provider| provider.id == candidate)
    {
        candidate = format!("{base}-{suffix}");
        suffix += 1;
    }
    candidate
}

fn unique_agent_provider_name_for_migration(config: &AppConfig, desired: &str) -> String {
    let base = desired.trim();
    let base = if base.is_empty() {
        "Agent provider"
    } else {
        base
    };
    let mut candidate = base.to_string();
    let mut suffix = 2;
    while config
        .providers
        .iter()
        .any(|provider| provider.name == candidate)
    {
        candidate = format!("{base} ({suffix})");
        suffix += 1;
    }
    candidate
}

fn sanitize_agent_provider_id_part_for_migration(value: &str) -> String {
    let mut result = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    while result.contains("--") {
        result = result.replace("--", "-");
    }
    let result = result.trim_matches('-').to_string();
    if result.is_empty() {
        "provider".to_string()
    } else {
        result
    }
}

impl AppConfig {
    pub fn upstream_proxy_url_for(&self, use_system_proxy: bool) -> Option<&str> {
        use_system_proxy
            .then_some(self.upstream_proxy_url.as_deref())
            .flatten()
    }

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let mut config: AppConfig = toml::from_str(&raw)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let mut changed = false;
            config.cache.normalize_fast_forwarding_hit_policy();
            if config.normalize_legacy_static_default_user_agents() {
                changed = true;
            }
            // v1.4.25 replaces manually configured balance URLs with bounded
            // built-in protocol profiles. The field is intentionally no
            // longer part of `AppConfig`, so the next canonical save removes
            // only this retired configuration section.
            if raw.contains("provider_balance_probe_configs") {
                changed = true;
            }
            if config
                ._legacy_provider_response_session_reuse
                .take()
                .is_some()
            {
                changed = true;
            }
            if config.normalize_provider_cache_capability_effect_state() {
                changed = true;
            }
            if !raw.contains("proxy_auto_start") {
                config.proxy_auto_start = default_proxy_auto_start();
                changed = true;
            }
            if !raw.contains("proxy_mode_host") {
                config.proxy_mode_host = default_proxy_mode_host();
                changed = true;
            }
            if !raw.contains("proxy_mode_port") {
                config.proxy_mode_port = default_proxy_mode_port();
                changed = true;
            }
            if strip_builtin_demo_provider(&mut config) {
                changed = true;
            }
            let previous_agent_injections = config.agent_injections.clone();
            normalize_agent_injections(&mut config.agent_injections);
            if config.agent_injections != previous_agent_injections {
                config.updated_at = Utc::now();
                changed = true;
            }
            if config.migrate_legacy_agent_provider_bindings() {
                changed = true;
            }
            if config.prune_orphaned_references() {
                changed = true;
            }
            if changed {
                config.save(path)?;
            }
            Ok(config)
        } else {
            let config = AppConfig::default();
            config.save(path)?;
            Ok(config)
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let host_ip = self.host.parse::<IpAddr>()?;
        let proxy_mode_ip = self.proxy_mode_host.parse::<IpAddr>()?;
        if host_ip.is_unspecified() && self.local_key.trim().is_empty() {
            return Err(anyhow!(
                "binding to 0.0.0.0 requires a non-empty local authentication key"
            ));
        }
        if proxy_mode_ip.is_unspecified() && self.local_key.trim().is_empty() {
            return Err(anyhow!(
                "binding proxy mode to 0.0.0.0 requires a non-empty local authentication key"
            ));
        }
        let proxy_mode_conflicts_with_main = proxy_bind_conflicts(
            &self.host,
            self.port,
            &self.proxy_mode_host,
            self.proxy_mode_port,
        )?;
        if proxy_mode_conflicts_with_main {
            return Err(anyhow!(
                "proxy mode address must be different from the main agent proxy address"
            ));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    fn normalize_legacy_static_default_user_agents(&mut self) -> bool {
        let now = Utc::now();
        let mut affected_provider_ids = Vec::new();
        for provider in &mut self.providers {
            let is_legacy_default = provider
                .custom_user_agent
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| value == LEGACY_STATIC_DEFAULT_USER_AGENT);
            if !is_legacy_default {
                continue;
            }
            provider.custom_user_agent = None;
            provider.updated_at = now;
            affected_provider_ids.push(provider.id.clone());
        }
        if affected_provider_ids.is_empty() {
            return false;
        }
        for provider_id in affected_provider_ids {
            // The effective upstream identity changed. Existing per-route
            // capability evidence must be re-observed rather than silently
            // being applied across cache lanes.
            self.clear_cache_capabilities_for_provider(&provider_id);
        }
        self.updated_at = now;
        true
    }

    /// Converts old Agent bindings that still point at a shared provider into
    /// independent per-Agent records. The original provider is kept intact as
    /// a migration source; only the Agent binding and its visible saved list
    /// move to the clone. This keeps an Agent's Key rotation, health, balance,
    /// compaction, channel, and cache-capability state from leaking into any
    /// other Agent.
    fn migrate_legacy_agent_provider_bindings(&mut self) -> bool {
        let now = Utc::now();
        let routes = self
            .agent_injections
            .iter()
            .enumerate()
            .map(|(index, agent)| {
                (
                    index,
                    agent.id.clone(),
                    agent.label.clone(),
                    agent.provider_id.clone(),
                    agent.hidden_provider_ids.clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut registered_agents_by_provider: HashMap<String, HashSet<String>> = HashMap::new();
        for (_, agent_id, _, bound_provider_id, hidden_provider_ids) in &routes {
            let ordered_provider_ids = self
                .agent_provider_orders
                .iter()
                .find(|order| order.agent_id == *agent_id)
                .map(|order| order.provider_ids.as_slice())
                .unwrap_or_default();
            for provider_id in ordered_provider_ids {
                if !hidden_provider_ids.contains(provider_id) {
                    registered_agents_by_provider
                        .entry(provider_id.clone())
                        .or_default()
                        .insert(agent_id.clone());
                }
            }
            if let Some(provider_id) = bound_provider_id {
                registered_agents_by_provider
                    .entry(provider_id.clone())
                    .or_default()
                    .insert(agent_id.clone());
            }
        }
        let mut changed = false;

        for (agent_index, agent_id, agent_label, bound_provider_id, hidden_provider_ids) in routes {
            let mut visible_provider_ids = self
                .agent_provider_orders
                .iter()
                .find(|order| order.agent_id == agent_id)
                .map(|order| order.provider_ids.clone())
                .unwrap_or_default()
                .into_iter()
                .filter(|provider_id| !hidden_provider_ids.contains(provider_id))
                .collect::<Vec<_>>();
            if let Some(provider_id) = bound_provider_id.as_ref() {
                if !visible_provider_ids
                    .iter()
                    .any(|visible_id| visible_id == provider_id)
                {
                    visible_provider_ids.push(provider_id.clone());
                }
            }

            let mut replacements = HashMap::new();
            for source_provider_id in &visible_provider_ids {
                let shared_with_another_agent = registered_agents_by_provider
                    .get(source_provider_id)
                    .is_some_and(|agents| agents.iter().any(|owner| owner != &agent_id));
                if (provider_is_private_to_agent_for_migration(source_provider_id, &agent_id)
                    && !shared_with_another_agent)
                    || replacements.contains_key(source_provider_id)
                {
                    continue;
                }
                if let Some(cloned_provider_id) = self.clone_provider_for_agent_migration(
                    &agent_id,
                    &agent_label,
                    source_provider_id,
                    now,
                ) {
                    replacements.insert(source_provider_id.clone(), cloned_provider_id);
                }
            }
            if replacements.is_empty() {
                continue;
            }

            let replacement_for = |provider_id: &str| {
                replacements
                    .get(provider_id)
                    .cloned()
                    .unwrap_or_else(|| provider_id.to_string())
            };
            let next_bound_provider_id = bound_provider_id.as_deref().map(replacement_for);
            let mut next_order = Vec::new();
            for provider_id in visible_provider_ids {
                let provider_id = replacement_for(&provider_id);
                if !next_order.iter().any(|existing| existing == &provider_id) {
                    next_order.push(provider_id);
                }
            }
            if let Some(provider_id) = next_bound_provider_id.as_ref() {
                if !next_order.iter().any(|existing| existing == provider_id) {
                    next_order.push(provider_id.clone());
                }
            }

            let agent = &mut self.agent_injections[agent_index];
            agent.provider_id = next_bound_provider_id;
            for source_provider_id in replacements.keys() {
                agent
                    .hidden_provider_ids
                    .retain(|hidden_id| hidden_id != source_provider_id);
            }
            agent.last_status = Some("已迁移为当前 Agent 独立上游".to_string());

            if let Some(order) = self
                .agent_provider_orders
                .iter_mut()
                .find(|order| order.agent_id == agent_id)
            {
                order.provider_ids = next_order;
            } else if !next_order.is_empty() {
                self.agent_provider_orders.push(AgentProviderOrderConfig {
                    agent_id: agent_id.clone(),
                    provider_ids: next_order,
                });
            }
            changed = true;
        }

        if changed {
            self.updated_at = now;
        }
        changed
    }

    fn clone_provider_for_agent_migration(
        &mut self,
        agent_id: &str,
        agent_label: &str,
        source_provider_id: &str,
        now: DateTime<Utc>,
    ) -> Option<String> {
        let source_provider = self
            .providers
            .iter()
            .find(|provider| provider.id == source_provider_id)
            .cloned()?;
        let cloned_provider_id =
            unique_agent_provider_id_for_migration(self, agent_id, source_provider_id);
        let mut cloned_provider = source_provider.clone();
        cloned_provider.id = cloned_provider_id.clone();
        cloned_provider.name = unique_agent_provider_name_for_migration(
            self,
            &format!("{} / {}", source_provider.name, agent_label),
        );
        cloned_provider.created_at = now;
        cloned_provider.updated_at = now;
        self.providers.push(cloned_provider);

        if let Some(mut pool) = self
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == source_provider_id)
            .cloned()
        {
            pool.provider_id = cloned_provider_id.clone();
            pool.updated_at = now;
            self.provider_key_pools.push(pool);
        }
        if let Some(mut compact_mode) = self
            .provider_compact_modes
            .iter()
            .find(|mode| mode.provider_id == source_provider_id)
            .cloned()
        {
            compact_mode.provider_id = cloned_provider_id.clone();
            compact_mode.updated_at = now;
            self.provider_compact_modes.push(compact_mode);
        }
        if let Some(mut auto_compaction) = self
            .provider_auto_compaction_limits
            .iter()
            .find(|limit| limit.provider_id == source_provider_id)
            .cloned()
        {
            auto_compaction.provider_id = cloned_provider_id.clone();
            auto_compaction.updated_at = now;
            self.provider_auto_compaction_limits.push(auto_compaction);
        }
        if let Some(mut channel_mode) = self
            .provider_channel_modes
            .iter()
            .find(|mode| mode.provider_id == source_provider_id)
            .cloned()
        {
            channel_mode.provider_id = cloned_provider_id.clone();
            channel_mode.updated_at = now;
            self.provider_channel_modes.push(channel_mode);
        }
        let cache_capabilities = self
            .provider_cache_capabilities
            .iter()
            .filter(|capability| capability.provider_id == source_provider_id)
            .cloned()
            .map(|mut capability| {
                capability.provider_id = cloned_provider_id.clone();
                capability.updated_at = now;
                capability
            })
            .collect::<Vec<_>>();
        self.provider_cache_capabilities.extend(cache_capabilities);

        Some(cloned_provider_id)
    }

    /// Remove route and presentation references that can no longer be selected
    /// because their provider or Agent was deleted. Provider-scoped keys and
    /// capability evidence are deliberately retained: a user may restore a
    /// provider with the same id and should not lose its saved credentials or
    /// verified compatibility state merely because it is temporarily absent.
    fn prune_orphaned_references(&mut self) -> bool {
        let provider_ids = self
            .providers
            .iter()
            .map(|provider| provider.id.clone())
            .collect::<HashSet<_>>();
        let agent_ids = self
            .agent_injections
            .iter()
            .map(|agent| agent.id.clone())
            .collect::<HashSet<_>>();
        let mut changed = false;

        if self
            .active_provider_id
            .as_deref()
            .is_some_and(|provider_id| !provider_ids.contains(provider_id))
        {
            self.active_provider_id = None;
            changed = true;
        }

        for profile in &mut self.route_profiles {
            if profile
                .provider_id
                .as_deref()
                .is_some_and(|provider_id| !provider_ids.contains(provider_id))
            {
                // The alias was scoped to the missing provider.  Leaving it
                // behind would make a future fallback route look intentional.
                profile.provider_id = None;
                profile.model_alias = None;
                changed = true;
            }
        }

        for agent in &mut self.agent_injections {
            let mut hidden_seen = HashSet::new();
            let hidden_before = agent.hidden_provider_ids.len();
            agent.hidden_provider_ids.retain(|provider_id| {
                provider_ids.contains(provider_id) && hidden_seen.insert(provider_id.clone())
            });
            changed |= agent.hidden_provider_ids.len() != hidden_before;

            if agent
                .provider_id
                .as_deref()
                .is_some_and(|provider_id| !provider_ids.contains(provider_id))
            {
                // A missing route was already unable to inject safely.  Make
                // the inactive state explicit rather than retaining a stale
                // binding that could be mistaken for a working route.
                agent.provider_id = None;
                agent.model_id = None;
                agent.enabled = false;
                agent.last_status = None;
                changed = true;
            }
        }

        let previous_orders = self.agent_provider_orders.clone();
        let mut normalized_orders = Vec::with_capacity(previous_orders.len());
        for order in previous_orders.iter() {
            if !agent_ids.contains(&order.agent_id) {
                continue;
            }
            let mut seen = HashSet::new();
            let provider_ids = order
                .provider_ids
                .iter()
                .filter(|provider_id| {
                    provider_ids.contains(*provider_id) && seen.insert((*provider_id).clone())
                })
                .cloned()
                .collect::<Vec<_>>();
            if provider_ids.is_empty() {
                continue;
            }
            if let Some(existing) =
                normalized_orders
                    .iter_mut()
                    .find(|existing: &&mut AgentProviderOrderConfig| {
                        existing.agent_id == order.agent_id
                    })
            {
                for provider_id in provider_ids {
                    if !existing
                        .provider_ids
                        .iter()
                        .any(|existing_id| existing_id == &provider_id)
                    {
                        existing.provider_ids.push(provider_id);
                    }
                }
            } else {
                normalized_orders.push(AgentProviderOrderConfig {
                    agent_id: order.agent_id.clone(),
                    provider_ids,
                });
            }
        }
        if normalized_orders != previous_orders {
            self.agent_provider_orders = normalized_orders;
            changed = true;
        }

        if changed {
            self.updated_at = Utc::now();
        }
        changed
    }

    pub fn public_view(&self, config_path: PathBuf) -> PublicConfig {
        PublicConfig {
            host: self.host.clone(),
            port: self.port,
            proxy_auto_start: self.proxy_auto_start,
            proxy_mode_host: self.proxy_mode_host.clone(),
            proxy_mode_port: self.proxy_mode_port,
            upstream_proxy_url: self.upstream_proxy_url.clone(),
            local_key: self.local_key.clone(),
            default_channel: self.default_channel.clone(),
            active_provider_id: self.active_provider_id.clone(),
            workspace_fingerprint: self.workspace_fingerprint.clone(),
            providers: self
                .providers
                .iter()
                .map(|provider| PublicProvider {
                    id: provider.id.clone(),
                    name: provider.name.clone(),
                    base_url: provider.base_url.clone(),
                    models_url: provider.models_url.clone(),
                    is_full_url: provider.is_full_url,
                    custom_user_agent: provider.custom_user_agent.clone(),
                    channel_mode: self.provider_channel_mode_for_provider(&provider.id),
                    channel: provider.channel.clone(),
                    prompt_cache_retention_enabled: provider.prompt_cache_retention_enabled,
                    request_body_gzip_enabled: provider.request_body_gzip_enabled,
                    use_system_proxy: provider.use_system_proxy,
                    non_sse_compact_compat_enabled: self
                        .non_sse_compact_compat_enabled_for_provider(&provider.id),
                    auto_compact_token_limit: self
                        .auto_compact_token_limit_for_provider(&provider.id),
                    cache_capabilities: self.cache_capabilities_for_provider(&provider.id),
                    has_api_key: provider.api_key_encrypted.is_some(),
                    key_pool: self.public_key_pool_for_provider(&provider.id),
                    models: provider.models.clone(),
                    enabled: provider.enabled,
                    created_at: provider.created_at,
                    updated_at: provider.updated_at,
                })
                .collect(),
            route_profiles: self.route_profiles.clone(),
            cache: self.cache.clone(),
            agent_injections: self
                .agent_injections
                .iter()
                .cloned()
                .map(|mut item| {
                    item.local_key = public_agent_local_key(&self.local_key, &item.id);
                    item
                })
                .collect(),
            agent_provider_orders: self.agent_provider_orders.clone(),
            provider_key_pools: self
                .provider_key_pools
                .iter()
                .map(|pool| PublicProviderKeyPoolEntry {
                    provider_id: pool.provider_id.clone(),
                    pool: public_key_pool(pool),
                })
                .collect(),
            updated_at: self.updated_at,
            config_path,
        }
    }

    pub fn provider_api_key(&self, provider_id: &str) -> Result<Option<String>> {
        let Some(provider) = self.providers.iter().find(|p| p.id == provider_id) else {
            return Ok(None);
        };
        provider
            .api_key_encrypted
            .as_deref()
            .map(decrypt_secret)
            .transpose()
    }

    pub fn select_provider_key_for_request(
        &mut self,
        provider_id: &str,
        preferred_key_id: Option<&str>,
        exclude_key_id: Option<&str>,
    ) -> Result<Option<SelectedProviderKey>> {
        let now = Utc::now();
        if let Some(pool) = self
            .provider_key_pools
            .iter_mut()
            .find(|pool| pool.provider_id == provider_id && pool.enabled)
        {
            let mut candidates = pool
                .keys
                .iter()
                .enumerate()
                .filter(|(_, key)| {
                    key.enabled
                        && key.key_encrypted.is_some()
                        && key.disabled_until.map(|until| until <= now).unwrap_or(true)
                        && exclude_key_id.map(|id| id != key.id).unwrap_or(true)
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if !candidates.is_empty() {
                let preferred_index = preferred_key_id.and_then(|preferred| {
                    candidates
                        .iter()
                        .copied()
                        .find(|index| pool.keys[*index].id == preferred)
                });
                let selected_index = if let Some(index) = preferred_index {
                    index
                } else {
                    match pool.strategy {
                        KeyLoadBalanceStrategy::RoundRobin => {
                            candidates.sort_unstable();
                            let selected = candidates
                                .iter()
                                .copied()
                                .find(|index| *index >= pool.next_index)
                                .unwrap_or(candidates[0]);
                            pool.next_index = (selected + 1) % pool.keys.len().max(1);
                            selected
                        }
                        KeyLoadBalanceStrategy::Priority => candidates
                            .into_iter()
                            .max_by_key(|index| {
                                let key = &pool.keys[*index];
                                (
                                    key.priority,
                                    std::cmp::Reverse(key.total_requests),
                                    key.successes,
                                    // Keep equal-priority, equal-load keys in the editor order.
                                    std::cmp::Reverse(*index),
                                )
                            })
                            .unwrap_or(0),
                        KeyLoadBalanceStrategy::LeastUsed => candidates
                            .into_iter()
                            .min_by_key(|index| {
                                let key = &pool.keys[*index];
                                (key.total_requests, std::cmp::Reverse(key.priority))
                            })
                            .unwrap_or(0),
                        KeyLoadBalanceStrategy::Random => {
                            let seed = now
                                .timestamp_nanos_opt()
                                .unwrap_or_else(|| now.timestamp_micros() * 1000)
                                .unsigned_abs() as usize;
                            candidates[seed % candidates.len()]
                        }
                        KeyLoadBalanceStrategy::Sequential => {
                            candidates.sort_unstable();
                            candidates[0]
                        }
                    }
                };
                let selected = &mut pool.keys[selected_index];
                selected.total_requests = selected.total_requests.saturating_add(1);
                selected.updated_at = now;
                let encrypted_material_digest = selected
                    .key_encrypted
                    .as_deref()
                    .and_then(encrypted_provider_key_material_digest);
                let secret = selected
                    .key_encrypted
                    .as_deref()
                    .map(decrypt_secret)
                    .transpose()?;
                if let Some(secret) = secret {
                    return Ok(Some(SelectedProviderKey {
                        secret,
                        key_id: Some(selected.id.clone()),
                        encrypted_material_digest,
                    }));
                }
            }

            // An enabled pool is an explicit routing boundary: its health and
            // load-balancing rules own key selection. Falling back to the
            // connection-info key here would silently bypass that boundary.
            return Err(anyhow!(
                "provider API key is not configured: provider key pool has no enabled usable key"
            ));
        }

        self.provider_api_key(provider_id).map(|key| {
            key.map(|secret| SelectedProviderKey {
                secret,
                key_id: None,
                encrypted_material_digest: None,
            })
        })
    }

    pub fn mark_provider_key_success(&mut self, provider_id: &str, key_id: Option<&str>) {
        let Some(key_id) = key_id else {
            return;
        };
        let now = Utc::now();
        if let Some(key) = self.provider_key_mut(provider_id, key_id) {
            key.successes = key.successes.saturating_add(1);
            key.status = ProviderKeyStatus::Healthy;
            key.last_checked_at = Some(now);
            key.last_error = None;
            key.disabled_until = None;
            key.updated_at = now;
            self.updated_at = now;
        }
    }

    pub fn mark_provider_key_failure(
        &mut self,
        provider_id: &str,
        key_id: Option<&str>,
        message: &str,
        force_cooldown: bool,
    ) {
        let Some(key_id) = key_id else {
            return;
        };
        let now = Utc::now();
        let pool_settings = self
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .map(|pool| (pool.failure_threshold.max(1), pool.recovery_minutes.max(1)));
        let Some((failure_threshold, recovery_minutes)) = pool_settings else {
            return;
        };
        if let Some(key) = self.provider_key_mut(provider_id, key_id) {
            key.failures = key.failures.saturating_add(1);
            key.status = ProviderKeyStatus::Unhealthy;
            key.last_checked_at = Some(now);
            key.last_error = Some(message.chars().take(180).collect());
            if key_failure_exhausts_account_capacity(message) {
                // Quota/balance exhaustion cannot recover through a wait. Turn
                // only this Key off so the next inbound chooses the next
                // configured Key; the failed inbound is never retried here.
                key.enabled = false;
                key.disabled_until = None;
            } else if force_cooldown || key.failures >= failure_threshold as u64 {
                // Temporary health failures preserve the user's routing switch
                // and use a recoverable circuit-breaker cooldown instead.
                key.disabled_until = Some(now + chrono::Duration::minutes(recovery_minutes as i64));
            }
            key.updated_at = now;
            self.updated_at = now;
        }
    }

    pub fn provider_key_secret(&self, provider_id: &str, key_id: &str) -> Result<Option<String>> {
        self.provider_key(provider_id, key_id)
            .and_then(|key| key.key_encrypted.as_deref())
            .map(decrypt_secret)
            .transpose()
    }

    pub fn compact_compatibility_mode_for_provider(
        &self,
        provider_id: &str,
    ) -> CompactCompatibilityMode {
        self.provider_compact_modes
            .iter()
            .find(|item| item.provider_id == provider_id)
            .map(|item| item.mode.clone())
            .unwrap_or_default()
    }

    pub fn non_sse_compact_compat_enabled_for_provider(&self, provider_id: &str) -> bool {
        self.compact_compatibility_mode_for_provider(provider_id)
            == CompactCompatibilityMode::NonSseValidation
    }

    pub fn auto_compact_token_limit_for_provider(&self, provider_id: &str) -> Option<u32> {
        self.provider_auto_compaction_limits
            .iter()
            .find(|item| item.provider_id == provider_id)
            .and_then(|item| item.token_limit)
            .filter(|limit| *limit > 0)
    }

    pub fn provider_channel_mode_for_provider(&self, provider_id: &str) -> ProviderChannelMode {
        self.provider_channel_modes
            .iter()
            .find(|item| item.provider_id == provider_id)
            .map(|item| item.mode.clone())
            .unwrap_or_default()
    }

    pub fn cache_capabilities_for_provider(
        &self,
        provider_id: &str,
    ) -> Vec<ProviderCacheCapabilityConfig> {
        self.provider_cache_capabilities
            .iter()
            .filter(|item| item.provider_id == provider_id)
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub fn cache_capability_status(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        field: ProviderCacheCapabilityField,
    ) -> ProviderCacheCapabilityStatus {
        self.cache_capability_status_for_key(provider_id, model_id, channel, None, field)
    }

    pub fn cache_capability_status_for_key(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        field: ProviderCacheCapabilityField,
    ) -> ProviderCacheCapabilityStatus {
        self.provider_cache_capabilities
            .iter()
            .find(|item| {
                item.provider_id == provider_id
                    && item.model_id == model_id
                    && &item.channel == channel
                    && item.key_id.as_deref() == key_id
                    && item.field == field
            })
            .map(|item| item.status.clone())
            .unwrap_or_default()
    }

    pub fn cache_capability_for_key(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        field: ProviderCacheCapabilityField,
    ) -> Option<&ProviderCacheCapabilityConfig> {
        self.provider_cache_capabilities.iter().find(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && &item.channel == channel
                && item.key_id.as_deref() == key_id
                && item.field == field
        })
    }

    #[cfg(test)]
    pub fn cache_capability_verified_for(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        field: ProviderCacheCapabilityField,
    ) -> bool {
        self.cache_capability_verified_for_key(provider_id, model_id, channel, None, field)
    }

    #[cfg(test)]
    pub fn cache_capability_verified_for_key(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        field: ProviderCacheCapabilityField,
    ) -> bool {
        self.provider_cache_capabilities.iter().any(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && &item.channel == channel
                && item.key_id.as_deref() == key_id
                && item.field == field
                && provider_cache_capability_evidence_is_current(item)
                && item.enabled
                && item.status == ProviderCacheCapabilityStatus::Verified
        })
    }

    /// A successful capability probe proves only that the exact upstream
    /// accepted a field.  It does not prove that the field improves cache
    /// behavior, so callers must use this only for an explicit controlled
    /// validation run, never for ordinary request routing.
    pub fn cache_capability_probe_accepted_for_key(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        field: ProviderCacheCapabilityField,
    ) -> bool {
        self.provider_cache_capabilities.iter().any(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && &item.channel == channel
                && item.key_id.as_deref() == key_id
                && item.field == field
                && provider_cache_capability_evidence_is_current(item)
                && item.status == ProviderCacheCapabilityStatus::Verified
        })
    }

    /// Same as [`Self::cache_capability_effect_promoted_for_key`], but binds
    /// production use to the exact opaque key realm and request shape that
    /// produced the measured cache evidence.
    pub fn cache_capability_effect_promoted_for_scope(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        effect_scope_id: Option<&str>,
        field: ProviderCacheCapabilityField,
    ) -> bool {
        field != ProviderCacheCapabilityField::PromptCacheKey
            && provider_cache_effect_scope_is_current(effect_scope_id, field)
            && self.provider_cache_capabilities.iter().any(|item| {
                item.provider_id == provider_id
                    && item.model_id == model_id
                    && &item.channel == channel
                    && item.key_id.as_deref() == key_id
                    && item.effect_scope_id.as_deref() == effect_scope_id
                    && item.field == field
                    && provider_cache_capability_evidence_is_current(item)
                    && item.status == ProviderCacheCapabilityStatus::Verified
                    && item.effect_status == ProviderCacheEffectStatus::Promoted
            })
    }

    /// Returns true only for a positive real-effect certificate generated by
    /// the realm/session key strategy used on the production wire.
    pub fn generated_prompt_cache_key_promoted_for_scope(
        &self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        effect_scope_id: Option<&str>,
    ) -> bool {
        generated_prompt_cache_key_effect_scope_is_current_v4(effect_scope_id)
            && self.provider_cache_capabilities.iter().any(|item| {
                item.provider_id == provider_id
                    && item.model_id == model_id
                    && &item.channel == channel
                    && item.key_id.as_deref() == key_id
                    && item.effect_scope_id.as_deref() == effect_scope_id
                    && item.field == ProviderCacheCapabilityField::PromptCacheKey
                    && provider_cache_capability_evidence_is_current(item)
                    && item.status == ProviderCacheCapabilityStatus::Verified
                    && item.effect_status == ProviderCacheEffectStatus::Promoted
            })
    }

    pub fn cache_capability_probe_target(
        &self,
        provider_id: &str,
    ) -> Option<ProviderCacheCapabilityProbeTarget> {
        let provider = self
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)?;
        Some(ProviderCacheCapabilityProbeTarget {
            provider_id: provider.id.clone(),
            base_url: provider.base_url.clone(),
            is_full_url: provider.is_full_url,
            channel: provider.channel.clone(),
            channel_mode: self.provider_channel_mode_for_provider(provider_id),
            provider_updated_at: provider.updated_at,
            key_pool_updated_at: self
                .provider_key_pools
                .iter()
                .find(|pool| pool.provider_id == provider_id)
                .map(|pool| pool.updated_at),
        })
    }

    #[cfg(test)]
    pub fn record_cache_capability_probe(
        &mut self,
        provider_id: &str,
        model_id: &str,
        channel: Channel,
        field: ProviderCacheCapabilityField,
        status: ProviderCacheCapabilityStatus,
        message: Option<String>,
    ) {
        self.record_cache_capability_probe_for_key(
            provider_id,
            model_id,
            channel,
            None,
            field,
            status,
            message,
        );
    }

    pub fn record_cache_capability_probe_for_key(
        &mut self,
        provider_id: &str,
        model_id: &str,
        channel: Channel,
        key_id: Option<&str>,
        field: ProviderCacheCapabilityField,
        status: ProviderCacheCapabilityStatus,
        message: Option<String>,
    ) {
        let now = Utc::now();
        if let Some(item) = self.provider_cache_capabilities.iter_mut().find(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && item.channel == channel
                && item.key_id.as_deref() == key_id
                && item.field == field
        }) {
            if status != ProviderCacheCapabilityStatus::Error {
                item.evidence_version = PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION;
                item.status = status;
                if item.status != ProviderCacheCapabilityStatus::Verified {
                    item.enabled = false;
                    item.effect_status = ProviderCacheEffectStatus::Unverified;
                    item.effect_scope_id = None;
                    item.effect_checked_at = None;
                    item.effect_message = None;
                    item.baseline_cache_read_tokens = None;
                    item.candidate_cache_read_tokens = None;
                    item.baseline_ttft_ms = None;
                    item.candidate_ttft_ms = None;
                } else if item.effect_status != ProviderCacheEffectStatus::Promoted {
                    item.enabled = false;
                }
            }
            item.checked_at = Some(now);
            item.last_error = clean_optional_string(message);
            item.updated_at = now;
        } else {
            self.provider_cache_capabilities
                .push(ProviderCacheCapabilityConfig {
                    provider_id: provider_id.to_string(),
                    model_id: model_id.to_string(),
                    channel,
                    key_id: clean_optional_string(key_id.map(ToOwned::to_owned)),
                    effect_scope_id: None,
                    field,
                    evidence_version: PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION,
                    enabled: false,
                    status,
                    effect_status: ProviderCacheEffectStatus::Unverified,
                    checked_at: Some(now),
                    effect_checked_at: None,
                    effect_message: None,
                    baseline_cache_read_tokens: None,
                    candidate_cache_read_tokens: None,
                    baseline_ttft_ms: None,
                    candidate_ttft_ms: None,
                    last_error: clean_optional_string(message),
                    updated_at: now,
                });
        }
        self.updated_at = now;
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn record_cache_capability_effect_for_key(
        &mut self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        fields: &[ProviderCacheCapabilityField],
        effect_status: ProviderCacheEffectStatus,
        message: Option<String>,
        baseline_cache_read_tokens: Option<u64>,
        candidate_cache_read_tokens: Option<u64>,
        baseline_ttft_ms: Option<u64>,
        candidate_ttft_ms: Option<u64>,
    ) {
        self.record_cache_capability_effect_for_scope(
            provider_id,
            model_id,
            channel,
            key_id,
            None,
            fields,
            effect_status,
            message,
            baseline_cache_read_tokens,
            candidate_cache_read_tokens,
            baseline_ttft_ms,
            candidate_ttft_ms,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_cache_capability_effect_for_scope(
        &mut self,
        provider_id: &str,
        model_id: &str,
        channel: &Channel,
        key_id: Option<&str>,
        effect_scope_id: Option<&str>,
        fields: &[ProviderCacheCapabilityField],
        effect_status: ProviderCacheEffectStatus,
        message: Option<String>,
        baseline_cache_read_tokens: Option<u64>,
        candidate_cache_read_tokens: Option<u64>,
        baseline_ttft_ms: Option<u64>,
        candidate_ttft_ms: Option<u64>,
    ) {
        let now = Utc::now();
        for item in self.provider_cache_capabilities.iter_mut().filter(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && &item.channel == channel
                && item.key_id.as_deref() == key_id
                && fields.contains(&item.field)
                && item.status == ProviderCacheCapabilityStatus::Verified
                && provider_cache_capability_evidence_is_current(item)
        }) {
            let preserve_promoted = effect_status == ProviderCacheEffectStatus::Error
                && item.effect_status == ProviderCacheEffectStatus::Promoted;
            item.effect_checked_at = Some(now);
            if preserve_promoted {
                item.last_error = clean_optional_string(message.clone());
                item.updated_at = now;
                continue;
            }
            let scope_is_current =
                provider_cache_effect_scope_is_current(effect_scope_id, item.field);
            let invalid_promoted_scope =
                effect_status == ProviderCacheEffectStatus::Promoted && !scope_is_current;
            item.effect_status = if invalid_promoted_scope {
                ProviderCacheEffectStatus::Unverified
            } else {
                effect_status
            };
            item.effect_scope_id = scope_is_current
                .then(|| clean_optional_string(effect_scope_id.map(ToOwned::to_owned)))
                .flatten();
            // Every field needs both acceptance and a positive measured
            // effect. Prompt-cache keys additionally require the v3
            // realm/session strategy scope checked above.
            item.enabled =
                item.effect_status == ProviderCacheEffectStatus::Promoted && scope_is_current;
            item.effect_message = (!invalid_promoted_scope)
                .then(|| clean_optional_string(message.clone()))
                .flatten();
            item.baseline_cache_read_tokens = baseline_cache_read_tokens;
            item.candidate_cache_read_tokens = candidate_cache_read_tokens;
            item.baseline_ttft_ms = baseline_ttft_ms;
            item.candidate_ttft_ms = candidate_ttft_ms;
            if invalid_promoted_scope {
                item.last_error = Some(
                    "cache effect promotion requires a v2 exact-scope certificate".to_string(),
                );
            } else if effect_status == ProviderCacheEffectStatus::Error {
                item.last_error = clean_optional_string(message.clone());
            } else {
                item.last_error = None;
            }
            item.updated_at = now;
        }
        self.updated_at = now;
    }

    fn normalize_provider_cache_capability_effect_state(&mut self) -> bool {
        let mut changed = false;
        let now = Utc::now();
        for item in &mut self.provider_cache_capabilities {
            if item.field == ProviderCacheCapabilityField::PromptCacheBreakpoint
                && item.evidence_version < PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION
                && item.status == ProviderCacheCapabilityStatus::Verified
            {
                item.evidence_version = PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION;
                item.status = ProviderCacheCapabilityStatus::Unverified;
                item.enabled = false;
                item.effect_status = ProviderCacheEffectStatus::Unverified;
                item.effect_checked_at = None;
                item.effect_message = None;
                item.baseline_cache_read_tokens = None;
                item.candidate_cache_read_tokens = None;
                item.baseline_ttft_ms = None;
                item.candidate_ttft_ms = None;
                item.last_error = Some(
                    "legacy prompt_cache_breakpoint evidence requires re-verification".to_string(),
                );
                item.updated_at = now;
                changed = true;
            }
            // Older effect records predate the exact key-realm, request-shape
            // and frozen-breakpoint binding. They cannot authorize ordinary
            // traffic: retain field acceptance only and require a new paired
            // baseline/candidate validation.
            if item.effect_status == ProviderCacheEffectStatus::Promoted
                && !provider_cache_effect_scope_is_current(
                    item.effect_scope_id.as_deref(),
                    item.field,
                )
            {
                item.enabled = false;
                item.effect_status = ProviderCacheEffectStatus::Unverified;
                item.effect_checked_at = None;
                item.effect_message = None;
                item.baseline_cache_read_tokens = None;
                item.candidate_cache_read_tokens = None;
                item.baseline_ttft_ms = None;
                item.candidate_ttft_ms = None;
                item.last_error = Some(
                    "legacy cache effect evidence requires v2 exact-scope re-verification"
                        .to_string(),
                );
                item.updated_at = now;
                changed = true;
            }
            let promoted = item.effect_status == ProviderCacheEffectStatus::Promoted
                && item.status == ProviderCacheCapabilityStatus::Verified
                && provider_cache_capability_evidence_is_current(item)
                && provider_cache_effect_scope_is_current(
                    item.effect_scope_id.as_deref(),
                    item.field,
                );
            if item.enabled != promoted {
                item.enabled = promoted;
                changed = true;
            }
        }
        changed
    }

    pub fn clear_cache_capabilities_for_provider(&mut self, provider_id: &str) {
        self.provider_cache_capabilities
            .retain(|item| item.provider_id != provider_id);
        self.updated_at = Utc::now();
    }

    pub fn clear_cache_capabilities_for_model(&mut self, provider_id: &str, model_id: &str) {
        self.provider_cache_capabilities
            .retain(|item| item.provider_id != provider_id || item.model_id != model_id);
        self.updated_at = Utc::now();
    }

    fn provider_key(&self, provider_id: &str, key_id: &str) -> Option<&ProviderKeyConfig> {
        self.provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)?
            .keys
            .iter()
            .find(|key| key.id == key_id)
    }

    fn provider_key_mut(
        &mut self,
        provider_id: &str,
        key_id: &str,
    ) -> Option<&mut ProviderKeyConfig> {
        self.provider_key_pools
            .iter_mut()
            .find(|pool| pool.provider_id == provider_id)?
            .keys
            .iter_mut()
            .find(|key| key.id == key_id)
    }

    pub fn upsert_provider(&mut self, input: ProviderInput) -> Result<String> {
        let now = Utc::now();
        let id = input
            .id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| slugify(&input.name));
        let encrypted_key = input
            .api_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .map(encrypt_secret)
            .transpose()?;
        let supplied_api_key = encrypted_key.is_some();
        let key_pool_connection_changed = input.key_pool.as_ref().is_some_and(|pool| {
            provider_key_pool_connection_changed(
                self.provider_key_pools
                    .iter()
                    .find(|existing| existing.provider_id == id),
                pool,
            )
        });
        let channel_mode_changed =
            self.provider_channel_mode_for_provider(&id) != input.channel_mode;
        let auto_compact_token_limit = input.auto_compact_token_limit.filter(|limit| *limit > 0);
        let auto_compact_token_limit_configured =
            input.auto_compact_token_limit_configured || input.auto_compact_token_limit.is_some();
        let mut invalidate_provider_capabilities = false;

        if let Some(provider) = self.providers.iter_mut().find(|p| p.id == id) {
            invalidate_provider_capabilities = provider.base_url != input.base_url
                || provider.is_full_url != input.is_full_url
                || provider.channel != input.channel
                || provider.custom_user_agent
                    != clean_optional_string(input.custom_user_agent.clone())
                || channel_mode_changed
                || supplied_api_key
                || key_pool_connection_changed;
            provider.name = input.name;
            provider.base_url = input.base_url;
            provider.models_url = clean_optional_string(input.models_url);
            provider.is_full_url = input.is_full_url;
            provider.custom_user_agent = clean_optional_string(input.custom_user_agent);
            provider.channel = input.channel;
            provider.prompt_cache_retention_enabled = input.prompt_cache_retention_enabled;
            provider.request_body_gzip_enabled = input.request_body_gzip_enabled;
            provider.use_system_proxy = input.use_system_proxy;
            provider.enabled = input.enabled;
            provider.updated_at = now;
            if encrypted_key.is_some() {
                provider.api_key_encrypted = encrypted_key;
            }
        } else {
            self.providers.push(ProviderConfig {
                id: id.clone(),
                name: input.name,
                base_url: input.base_url,
                models_url: clean_optional_string(input.models_url),
                is_full_url: input.is_full_url,
                custom_user_agent: clean_optional_string(input.custom_user_agent),
                channel: input.channel,
                prompt_cache_retention_enabled: input.prompt_cache_retention_enabled,
                request_body_gzip_enabled: input.request_body_gzip_enabled,
                use_system_proxy: input.use_system_proxy,
                api_key_encrypted: encrypted_key,
                models: Vec::new(),
                enabled: input.enabled,
                created_at: now,
                updated_at: now,
            });
        }

        if invalidate_provider_capabilities {
            self.clear_cache_capabilities_for_provider(&id);
        }

        if let Some(key_pool) = input.key_pool {
            self.upsert_provider_key_pool(&id, key_pool)?;
        }
        self.upsert_provider_compact_mode(
            &id,
            if input.non_sse_compact_compat_enabled {
                CompactCompatibilityMode::NonSseValidation
            } else {
                CompactCompatibilityMode::CcSwitchFast
            },
        );
        if auto_compact_token_limit_configured {
            self.upsert_provider_auto_compaction_limit(&id, auto_compact_token_limit);
        }
        self.upsert_provider_channel_mode(&id, input.channel_mode);

        self.updated_at = now;
        Ok(id)
    }

    pub fn upsert_provider_compact_mode(
        &mut self,
        provider_id: &str,
        mode: CompactCompatibilityMode,
    ) {
        let now = Utc::now();
        if mode == CompactCompatibilityMode::CcSwitchFast {
            self.provider_compact_modes
                .retain(|item| item.provider_id != provider_id);
            self.updated_at = now;
            return;
        }
        if let Some(item) = self
            .provider_compact_modes
            .iter_mut()
            .find(|item| item.provider_id == provider_id)
        {
            item.mode = mode;
            item.updated_at = now;
        } else {
            self.provider_compact_modes.push(ProviderCompactModeConfig {
                provider_id: provider_id.to_string(),
                mode,
                updated_at: now,
            });
        }
        self.updated_at = now;
    }

    pub fn upsert_provider_auto_compaction_limit(
        &mut self,
        provider_id: &str,
        token_limit: Option<u32>,
    ) {
        let token_limit = token_limit.filter(|limit| *limit > 0);
        let existing = self
            .provider_auto_compaction_limits
            .iter()
            .find(|item| item.provider_id == provider_id)
            .and_then(|item| item.token_limit);
        if existing == token_limit {
            return;
        }
        let now = Utc::now();
        if token_limit.is_none() {
            self.provider_auto_compaction_limits
                .retain(|item| item.provider_id != provider_id);
        } else if let Some(item) = self
            .provider_auto_compaction_limits
            .iter_mut()
            .find(|item| item.provider_id == provider_id)
        {
            item.token_limit = token_limit;
            item.updated_at = now;
        } else {
            self.provider_auto_compaction_limits
                .push(ProviderAutoCompactionConfig {
                    provider_id: provider_id.to_string(),
                    token_limit,
                    updated_at: now,
                });
        }
        self.updated_at = now;
    }

    pub fn upsert_provider_channel_mode(&mut self, provider_id: &str, mode: ProviderChannelMode) {
        let now = Utc::now();
        if mode == ProviderChannelMode::Auto {
            self.provider_channel_modes
                .retain(|item| item.provider_id != provider_id);
            self.updated_at = now;
            return;
        }
        if let Some(item) = self
            .provider_channel_modes
            .iter_mut()
            .find(|item| item.provider_id == provider_id)
        {
            item.mode = mode;
            item.updated_at = now;
        } else {
            self.provider_channel_modes.push(ProviderChannelModeConfig {
                provider_id: provider_id.to_string(),
                mode,
                updated_at: now,
            });
        }
        self.updated_at = now;
    }

    pub fn public_key_pool_for_provider(&self, provider_id: &str) -> Option<PublicProviderKeyPool> {
        self.provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .map(public_key_pool)
    }

    pub fn upsert_provider_key_pool(
        &mut self,
        provider_id: &str,
        input: ProviderKeyPoolInput,
    ) -> Result<()> {
        let now = Utc::now();
        let existing_pool = self
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .cloned();
        let keys = input
            .keys
            .into_iter()
            .map(|item| {
                let id = item
                    .id
                    .clone()
                    .filter(|id| !id.trim().is_empty())
                    .unwrap_or_else(|| format!("key-{}", Uuid::new_v4().simple()));
                let existing = existing_pool
                    .as_ref()
                    .and_then(|pool| pool.keys.iter().find(|key| key.id == id));
                let re_enabled = existing.is_some_and(|key| !key.enabled) && item.enabled;
                let supplied_secret = item.key.as_deref().filter(|key| !key.trim().is_empty());
                let health_reset = re_enabled || supplied_secret.is_some();
                let encrypted = supplied_secret
                    .map(encrypt_secret)
                    .transpose()?
                    .or_else(|| existing.and_then(|key| key.key_encrypted.clone()));
                Ok(ProviderKeyConfig {
                    id,
                    alias: clean_optional_string(item.alias),
                    key_encrypted: encrypted,
                    enabled: item.enabled,
                    priority: item.priority,
                    // Health is owned by the runtime. An editor opened before
                    // an actual request must not overwrite fresh health data
                    // when it saves an unrelated alias/order change.
                    status: if health_reset {
                        ProviderKeyStatus::Unknown
                    } else {
                        existing
                            .map(|key| key.status.clone())
                            .unwrap_or(item.status)
                    },
                    total_requests: if health_reset {
                        0
                    } else {
                        existing
                            .map(|key| key.total_requests)
                            .unwrap_or(item.total_requests)
                    },
                    successes: if health_reset {
                        0
                    } else {
                        existing.map(|key| key.successes).unwrap_or(item.successes)
                    },
                    failures: if health_reset {
                        0
                    } else {
                        existing.map(|key| key.failures).unwrap_or(item.failures)
                    },
                    last_checked_at: if health_reset {
                        None
                    } else {
                        existing
                            .and_then(|key| key.last_checked_at)
                            .or(item.last_checked_at)
                    },
                    last_error: if health_reset {
                        None
                    } else {
                        existing
                            .and_then(|key| key.last_error.clone())
                            .or_else(|| clean_optional_string(item.last_error))
                    },
                    disabled_until: if health_reset {
                        None
                    } else {
                        existing
                            .and_then(|key| key.disabled_until)
                            .or(item.disabled_until)
                    },
                    created_at: existing.map(|key| key.created_at).unwrap_or(now),
                    updated_at: now,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let selection_order_changed = existing_pool.as_ref().is_some_and(|existing| {
            existing.strategy != input.strategy
                || existing.keys.len() != keys.len()
                || existing
                    .keys
                    .iter()
                    .zip(&keys)
                    .any(|(before, after)| before.id != after.id)
        });

        if let Some(pool) = self
            .provider_key_pools
            .iter_mut()
            .find(|pool| pool.provider_id == provider_id)
        {
            pool.enabled = input.enabled;
            pool.strategy = input.strategy;
            pool.failure_threshold = input.failure_threshold.max(1);
            pool.recovery_minutes = input.recovery_minutes.max(1);
            pool.keys = keys;
            if selection_order_changed {
                // The round-robin cursor addresses positions in this vector.
                // A reordered or strategy-changed pool must begin from its
                // newly declared order rather than a stale numeric position.
                pool.next_index = 0;
            } else if !pool.keys.is_empty() {
                pool.next_index %= pool.keys.len();
            } else {
                pool.next_index = 0;
            }
            pool.updated_at = now;
        } else {
            self.provider_key_pools.push(ProviderKeyPoolConfig {
                provider_id: provider_id.to_string(),
                enabled: input.enabled,
                strategy: input.strategy,
                failure_threshold: input.failure_threshold.max(1),
                recovery_minutes: input.recovery_minutes.max(1),
                next_index: 0,
                keys,
                updated_at: now,
            });
        }
        self.updated_at = now;
        Ok(())
    }
}

fn provider_key_pool_connection_changed(
    existing: Option<&ProviderKeyPoolConfig>,
    input: &ProviderKeyPoolInput,
) -> bool {
    let Some(existing) = existing else {
        return input.enabled
            && input.keys.iter().any(|key| {
                key.key
                    .as_deref()
                    .is_some_and(|secret| !secret.trim().is_empty())
            });
    };
    if existing.enabled != input.enabled {
        return true;
    }
    if existing.keys.len() != input.keys.len() {
        return true;
    }
    input.keys.iter().any(|input_key| {
        let Some(key_id) = input_key.id.as_deref().filter(|id| !id.trim().is_empty()) else {
            return true;
        };
        let Some(existing_key) = existing.keys.iter().find(|key| key.id == key_id) else {
            return true;
        };
        input_key
            .key
            .as_deref()
            .is_some_and(|secret| !secret.trim().is_empty())
            || existing_key.enabled != input_key.enabled
    })
}

fn public_key_pool(pool: &ProviderKeyPoolConfig) -> PublicProviderKeyPool {
    let now = Utc::now();
    let keys = pool
        .keys
        .iter()
        .map(|key| PublicProviderKey {
            id: key.id.clone(),
            alias: key.alias.clone(),
            preview: key_preview(key.key_encrypted.as_deref()),
            has_saved_secret: key.key_encrypted.is_some(),
            enabled: key.enabled,
            priority: key.priority,
            status: key.status.clone(),
            total_requests: key.total_requests,
            successes: key.successes,
            failures: key.failures,
            last_checked_at: key.last_checked_at,
            last_error: key.last_error.clone(),
            disabled_until: key.disabled_until,
        })
        .collect::<Vec<_>>();
    let available_keys = pool
        .keys
        .iter()
        .filter(|key| {
            key.enabled
                && key.key_encrypted.is_some()
                && key.disabled_until.map(|until| until <= now).unwrap_or(true)
        })
        .count();
    PublicProviderKeyPool {
        enabled: pool.enabled,
        strategy: pool.strategy.clone(),
        failure_threshold: pool.failure_threshold,
        recovery_minutes: pool.recovery_minutes,
        available_keys,
        keys,
    }
}

fn key_preview(encrypted: Option<&str>) -> String {
    let Some(encrypted) = encrypted else {
        return "未保存".to_string();
    };
    match decrypt_secret(encrypted) {
        Ok(secret) => mask_secret(&secret),
        Err(_) => "解密失败".to_string(),
    }
}

fn mask_secret(secret: &str) -> String {
    let trimmed = secret.trim();
    if trimmed.len() <= 10 {
        return "*".repeat(trimmed.len().max(4));
    }
    let start = &trimmed[..trimmed.len().min(6)];
    let end = &trimmed[trimmed.len().saturating_sub(4)..];
    format!("{start}...{end}")
}

fn strip_builtin_demo_provider(config: &mut AppConfig) -> bool {
    let had_demo = config.providers.iter().any(is_builtin_demo_provider);
    if !had_demo {
        return false;
    }

    config
        .providers
        .retain(|provider| !is_builtin_demo_provider(provider));
    if config.active_provider_id.as_deref() == Some("zai-anthropic") {
        config.active_provider_id = None;
    }
    for profile in config.route_profiles.iter_mut() {
        if profile.provider_id.as_deref() == Some("zai-anthropic") {
            profile.provider_id = None;
        }
    }
    config.updated_at = Utc::now();
    true
}

fn is_builtin_demo_provider(provider: &ProviderConfig) -> bool {
    provider.id == "zai-anthropic"
        && provider.api_key_encrypted.is_none()
        && provider.base_url.trim_end_matches('/') == "https://api.z.ai/api/anthropic"
}

fn clean_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

pub fn normalize_upstream_proxy_url(value: Option<String>) -> Result<Option<String>> {
    let Some(value) = clean_optional_string(value) else {
        return Ok(None);
    };
    let parsed =
        reqwest::Url::parse(&value).with_context(|| "explicit upstream proxy URL is invalid")?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(anyhow!(
            "explicit upstream proxy URL must use http:// or https:// and include a host"
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(anyhow!(
            "proxy credentials must not be embedded in the explicit upstream proxy URL"
        ));
    }
    if !matches!(parsed.path(), "" | "/") || parsed.query().is_some() || parsed.fragment().is_some()
    {
        return Err(anyhow!(
            "explicit upstream proxy URL must not contain a path, query, or fragment"
        ));
    }
    Ok(Some(value))
}

fn public_agent_local_key(local_key: &str, agent_id: &str) -> Option<String> {
    if local_key.trim().is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(local_key.as_bytes());
    hasher.update(b"\0atoapi-agent\0");
    hasher.update(agent_id.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    Some(format!("ato-agent-{}", &digest[..32]))
}
pub fn default_agent_injections() -> Vec<AgentInjectionConfig> {
    vec![
        AgentInjectionConfig {
            id: "claude-code".to_string(),
            label: "Claude Code".to_string(),
            kind: AgentInjectionKind::ClaudeCode,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "claude-desktop".to_string(),
            label: "Claude Desktop".to_string(),
            kind: AgentInjectionKind::ClaudeDesktop,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "codex".to_string(),
            label: "Codex".to_string(),
            kind: AgentInjectionKind::Codex,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "gemini".to_string(),
            label: "Gemini".to_string(),
            kind: AgentInjectionKind::Gemini,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "opencode".to_string(),
            label: "OpenCode".to_string(),
            kind: AgentInjectionKind::OpenCode,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "openclaw".to_string(),
            label: "OpenClaw".to_string(),
            kind: AgentInjectionKind::OpenClaw,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "hermes".to_string(),
            label: "Hermes".to_string(),
            kind: AgentInjectionKind::Hermes,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
        AgentInjectionConfig {
            id: "proxy-mode".to_string(),
            label: "本地代理模式".to_string(),
            kind: AgentInjectionKind::ProxyMode,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        },
    ]
}

pub fn normalize_agent_injections(items: &mut Vec<AgentInjectionConfig>) {
    let defaults = default_agent_injections();
    items.retain(|item| item.kind != AgentInjectionKind::Unknown);
    for default_item in defaults.iter().cloned() {
        if !items.iter().any(|item| item.id == default_item.id) {
            items.push(default_item);
        }
    }
    items.sort_by_key(|item| {
        defaults
            .iter()
            .position(|default_item| default_item.id == item.id)
            .unwrap_or(usize::MAX)
    });
}

pub fn app_config_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ATOAPI_CONFIG_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        return Ok(path);
    }
    let base = dirs::config_dir()
        .or_else(dirs::data_local_dir)
        .ok_or_else(|| anyhow!("failed to locate user config directory"))?;
    let current = base.join("Atoapi");
    let legacy = base.join("AgentProxy");
    if legacy_agentproxy_import_enabled(
        std::env::var("ATOAPI_IMPORT_LEGACY_AGENTPROXY")
            .ok()
            .as_deref(),
    ) && !current.exists()
        && legacy.exists()
    {
        fs::create_dir_all(&current)?;
        copy_if_exists(&legacy.join("config.toml"), &current.join("config.toml"))?;
        copy_if_exists(
            &legacy.join("response-cache.bin"),
            &current.join("response-cache.bin"),
        )?;
    }
    Ok(current)
}

pub fn isolated_test_instance() -> bool {
    std::env::var("ATOAPI_ISOLATED_TEST_INSTANCE")
        .ok()
        .is_some_and(|value| isolated_test_flag_enabled(&value))
}

pub fn isolated_test_listen_port() -> Option<u16> {
    parse_isolated_test_listen_port(
        isolated_test_instance(),
        std::env::var("ATOAPI_TEST_LISTEN_PORT").ok().as_deref(),
    )
}

fn isolated_test_flag_enabled(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "on" | "enabled")
}

fn legacy_agentproxy_import_enabled(value: Option<&str>) -> bool {
    value.is_some_and(isolated_test_flag_enabled)
}

fn parse_isolated_test_listen_port(isolated: bool, value: Option<&str>) -> Option<u16> {
    isolated
        .then_some(value)
        .flatten()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|port| *port > 0 && *port < u16::MAX)
}

pub fn config_path() -> Result<PathBuf> {
    Ok(app_config_dir()?.join("config.toml"))
}

fn copy_if_exists(from: &Path, to: &Path) -> Result<()> {
    if !from.exists() || to.exists() {
        return Ok(());
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(from, to)
        .with_context(|| format!("failed to migrate {} to {}", from.display(), to.display()))?;
    Ok(())
}

fn slugify(input: &str) -> String {
    let mut slug = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while slug.contains("--") {
        slug = slug.replace("--", "-");
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        format!("provider-{}", Uuid::new_v4().simple())
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_upstream_proxy_url_is_optional_and_strictly_validated() {
        assert_eq!(normalize_upstream_proxy_url(None).unwrap(), None);
        assert_eq!(
            normalize_upstream_proxy_url(Some("  http://127.0.0.1:7897  ".to_string()))
                .unwrap()
                .as_deref(),
            Some("http://127.0.0.1:7897")
        );
        assert!(normalize_upstream_proxy_url(Some("socks5://127.0.0.1:7897".to_string())).is_err());
        assert!(normalize_upstream_proxy_url(Some(
            "http://user:secret@127.0.0.1:7897".to_string()
        ))
        .is_err());
        assert!(normalize_upstream_proxy_url(Some(
            "http://127.0.0.1:7897/?token=secret".to_string()
        ))
        .is_err());
        assert!(
            normalize_upstream_proxy_url(Some("http://127.0.0.1:7897/proxy".to_string())).is_err()
        );

        let mut config = AppConfig::default();
        config.upstream_proxy_url = Some("http://127.0.0.1:7897".to_string());
        assert_eq!(config.upstream_proxy_url_for(false), None);
        assert_eq!(
            config.upstream_proxy_url_for(true),
            Some("http://127.0.0.1:7897")
        );
    }

    #[test]
    fn proxy_auto_start_defaults_to_enabled() {
        assert!(AppConfig::default().proxy_auto_start);
    }

    #[test]
    fn background_prewarm_is_forced_disabled() {
        let mut cache = CacheConfig::smart_max_hit();
        assert!(!cache.background_prewarm_enabled);
        cache.background_prewarm_enabled = true;
        cache.normalize_fast_forwarding_hit_policy();
        assert!(!cache.background_prewarm_enabled);
    }

    #[test]
    fn cache_normalization_preserves_user_selected_mode() {
        let mut cache = CacheConfig::smart_max_hit();
        cache.mode = CacheMode::PassiveWarm;
        cache.prewarm_enabled = true;

        cache.normalize_fast_forwarding_hit_policy();

        assert_eq!(cache.mode, CacheMode::PassiveWarm);
        assert!(!cache.prewarm_enabled);
        assert!(cache.exact_enabled);
        assert!(cache.semantic_enabled);
    }

    #[test]
    fn prompt_cache_retention_defaults_to_enabled_for_legacy_providers() {
        let raw = r#"
id = "share"
name = "share"
base_url = "https://share.example/v1"
is_full_url = false
channel = "responses"
models = []
enabled = true
created_at = "2026-06-21T00:00:00Z"
updated_at = "2026-06-21T00:00:00Z"
"#;
        let provider: ProviderConfig = toml::from_str(raw).expect("legacy provider should parse");
        assert!(provider.prompt_cache_retention_enabled);
        assert!(provider.request_body_gzip_enabled);
        assert!(provider.use_system_proxy);
    }

    #[test]
    fn legacy_static_default_user_agent_migrates_without_touching_custom_values() {
        let mut config = AppConfig::default();
        let provider_id = config.upsert_provider(provider_input(None)).unwrap();
        let provider = config
            .providers
            .iter_mut()
            .find(|provider| provider.id == provider_id)
            .unwrap();
        provider.custom_user_agent = Some(LEGACY_STATIC_DEFAULT_USER_AGENT.to_string());
        let mut custom = provider.clone();
        custom.id = "custom-user-agent".to_string();
        custom.custom_user_agent = Some("codex.1.147.26".to_string());
        config.providers.push(custom);
        config.record_cache_capability_probe(
            &provider_id,
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );

        assert!(config.normalize_legacy_static_default_user_agents());
        assert_eq!(
            config
                .providers
                .iter()
                .find(|provider| provider.id == provider_id)
                .and_then(|provider| provider.custom_user_agent.as_deref()),
            None
        );
        assert_eq!(
            config
                .providers
                .iter()
                .find(|provider| provider.id == "custom-user-agent")
                .and_then(|provider| provider.custom_user_agent.as_deref()),
            Some("codex.1.147.26")
        );
        assert!(config.provider_cache_capabilities.is_empty());
        assert!(!config.normalize_legacy_static_default_user_agents());
    }

    #[test]
    fn startup_migrates_legacy_agent_routes_into_isolated_provider_state() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-agent-provider-isolation-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let mut config = AppConfig::default();
        let source_provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 1,
                recovery_minutes: 30,
                keys: vec![key_input("source-key", Some("test-secret"), true, 5)],
            })))
            .unwrap();
        config.mark_provider_key_success(&source_provider_id, Some("source-key"));
        let now = Utc::now();
        config
            .provider_compact_modes
            .push(ProviderCompactModeConfig {
                provider_id: source_provider_id.clone(),
                mode: CompactCompatibilityMode::NonSseValidation,
                updated_at: now,
            });
        config
            .provider_auto_compaction_limits
            .push(ProviderAutoCompactionConfig {
                provider_id: source_provider_id.clone(),
                token_limit: Some(90_000),
                updated_at: now,
            });
        config
            .provider_channel_modes
            .push(ProviderChannelModeConfig {
                provider_id: source_provider_id.clone(),
                mode: ProviderChannelMode::Manual,
                updated_at: now,
            });
        config.record_cache_capability_probe(
            &source_provider_id,
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Verified,
            Some("source-key".to_string()),
        );
        for agent_id in ["codex", "opencode"] {
            let agent = config
                .agent_injections
                .iter_mut()
                .find(|agent| agent.id == agent_id)
                .unwrap();
            agent.enabled = true;
            agent.provider_id = Some(source_provider_id.clone());
        }
        config.agent_provider_orders = ["codex", "opencode"]
            .into_iter()
            .map(|agent_id| AgentProviderOrderConfig {
                agent_id: agent_id.to_string(),
                provider_ids: vec![source_provider_id.clone()],
            })
            .collect();
        config.save(&path).unwrap();

        let mut loaded = AppConfig::load_or_create(&path).unwrap();
        let provider_for = |agent_id: &str| {
            loaded
                .agent_injections
                .iter()
                .find(|agent| agent.id == agent_id)
                .and_then(|agent| agent.provider_id.clone())
                .unwrap()
        };
        let codex_provider_id = provider_for("codex");
        let opencode_provider_id = provider_for("opencode");
        assert_ne!(codex_provider_id, source_provider_id);
        assert_ne!(opencode_provider_id, source_provider_id);
        assert_ne!(codex_provider_id, opencode_provider_id);
        assert!(provider_is_private_to_agent_for_migration(
            &codex_provider_id,
            "codex"
        ));
        assert!(provider_is_private_to_agent_for_migration(
            &opencode_provider_id,
            "opencode"
        ));
        assert!(loaded
            .providers
            .iter()
            .any(|provider| provider.id == source_provider_id));
        assert_eq!(loaded.provider_key_pools.len(), 3);
        assert_eq!(loaded.provider_compact_modes.len(), 3);
        assert_eq!(loaded.provider_auto_compaction_limits.len(), 3);
        assert_eq!(loaded.provider_channel_modes.len(), 3);
        assert_eq!(loaded.provider_cache_capabilities.len(), 3);

        loaded.mark_provider_key_failure(
            &codex_provider_id,
            Some("source-key"),
            "quota exhausted",
            true,
        );
        let key_status = |provider_id: &str| {
            loaded
                .provider_key_pools
                .iter()
                .find(|pool| pool.provider_id == provider_id)
                .and_then(|pool| pool.keys.iter().find(|key| key.id == "source-key"))
                .map(|key| key.status.clone())
        };
        assert_eq!(
            key_status(&codex_provider_id),
            Some(ProviderKeyStatus::Unhealthy)
        );
        assert_eq!(
            key_status(&source_provider_id),
            Some(ProviderKeyStatus::Healthy)
        );
        assert_eq!(
            key_status(&opencode_provider_id),
            Some(ProviderKeyStatus::Healthy)
        );

        let reloaded = AppConfig::load_or_create(&path).unwrap();
        assert_eq!(reloaded.providers.len(), 3, "migration is idempotent");
        assert_eq!(reloaded.provider_key_pools.len(), 3);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn shared_prefixed_legacy_provider_is_still_split_per_agent() {
        let mut config = AppConfig::default();
        let mut input = provider_input(None);
        input.id = Some("agent-codex-manual-shared".to_string());
        let source_provider_id = config.upsert_provider(input).unwrap();
        for agent_id in ["codex", "opencode"] {
            let agent = config
                .agent_injections
                .iter_mut()
                .find(|agent| agent.id == agent_id)
                .unwrap();
            agent.provider_id = Some(source_provider_id.clone());
        }
        config.agent_provider_orders = ["codex", "opencode"]
            .into_iter()
            .map(|agent_id| AgentProviderOrderConfig {
                agent_id: agent_id.to_string(),
                provider_ids: vec![source_provider_id.clone()],
            })
            .collect();

        assert!(config.migrate_legacy_agent_provider_bindings());
        let provider_for = |agent_id: &str| {
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == agent_id)
                .and_then(|agent| agent.provider_id.clone())
                .unwrap()
        };
        let codex_provider_id = provider_for("codex");
        let opencode_provider_id = provider_for("opencode");
        assert_ne!(codex_provider_id, source_provider_id);
        assert_ne!(opencode_provider_id, source_provider_id);
        assert_ne!(codex_provider_id, opencode_provider_id);
        assert!(provider_is_private_to_agent_for_migration(
            &codex_provider_id,
            "codex"
        ));
        assert!(provider_is_private_to_agent_for_migration(
            &opencode_provider_id,
            "opencode"
        ));
        assert!(config
            .agent_provider_orders
            .iter()
            .all(|order| !order.provider_ids.contains(&source_provider_id)));
    }

    #[test]
    fn cache_capability_records_round_trip_and_remain_field_scoped() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ProviderCacheCapabilityStatus::Unsupported,
            Some("field rejected".to_string()),
        );

        let encoded = toml::to_string_pretty(&config).expect("config should serialize");
        let decoded: AppConfig = toml::from_str(&encoded).expect("config should parse");

        assert!(!decoded.cache_capability_verified_for(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
        let compatible = decoded
            .cache_capability_for_key(
                "provider-a",
                "gpt-5.6-luna",
                &Channel::Responses,
                None,
                ProviderCacheCapabilityField::PromptCacheOptions,
            )
            .expect("compatibility record should round-trip");
        assert_eq!(compatible.status, ProviderCacheCapabilityStatus::Verified);
        assert_eq!(
            compatible.effect_status,
            ProviderCacheEffectStatus::Unverified
        );
        assert_eq!(
            decoded.cache_capability_status(
                "provider-a",
                "gpt-5.6-luna",
                &Channel::Responses,
                ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ),
            ProviderCacheCapabilityStatus::Unsupported
        );
        assert_eq!(
            decoded.cache_capability_status(
                "provider-a",
                "other-model",
                &Channel::Responses,
                ProviderCacheCapabilityField::PromptCacheOptions,
            ),
            ProviderCacheCapabilityStatus::Unverified
        );
    }

    #[test]
    fn clearing_one_model_does_not_remove_other_cache_capability_records() {
        let mut config = AppConfig::default();
        for model in ["gpt-5.6-luna", "gpt-5.6-sol"] {
            config.record_cache_capability_probe(
                "provider-a",
                model,
                Channel::Responses,
                ProviderCacheCapabilityField::PromptCacheKey,
                ProviderCacheCapabilityStatus::Verified,
                None,
            );
        }

        config.clear_cache_capabilities_for_model("provider-a", "gpt-5.6-luna");

        assert_eq!(config.provider_cache_capabilities.len(), 1);
        assert_eq!(
            config.provider_cache_capabilities[0].model_id,
            "gpt-5.6-sol"
        );
    }

    #[test]
    fn generic_probe_error_preserves_previous_verified_capability() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some("cache-effect-v2:test-realm:stream:no-store:bp=none"),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            ProviderCacheEffectStatus::Promoted,
            Some("effect verified".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(110),
        );

        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Error,
            Some("opaque HTTP 502".to_string()),
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some("cache-effect-v2:test-realm:stream:no-store:bp=none"),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            ProviderCacheEffectStatus::Error,
            Some("temporary effect HTTP 502".to_string()),
            None,
            None,
            None,
            None,
        );

        assert!(config.cache_capability_verified_for(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
        let record = &config.provider_cache_capabilities[0];
        assert_eq!(record.status, ProviderCacheCapabilityStatus::Verified);
        assert!(record.enabled);
        assert_eq!(record.effect_status, ProviderCacheEffectStatus::Promoted);
        assert_eq!(record.baseline_cache_read_tokens, Some(0));
        assert_eq!(record.candidate_cache_read_tokens, Some(512));
        assert_eq!(
            record.last_error.as_deref(),
            Some("temporary effect HTTP 502")
        );
    }

    #[test]
    fn cache_capability_verification_is_key_scoped() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe_for_key(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            Some("key-a"),
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some("cache-effect-v2:test-realm:stream:no-store:bp=none"),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );

        assert!(config.cache_capability_verified_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
        assert!(!config.cache_capability_verified_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-b"),
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
    }

    #[test]
    fn generated_prompt_cache_key_requires_its_v4_realm_session_certificate() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe_for_key(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            Some("key-a"),
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        let v2_scope = "cache-effect-v2:realm-a:stream:no-store:bp=none";
        let v4_scope = format!(
            "cache-effect-v4:realm-a:stream:no-store:pk=realm-session-v1:sid={}",
            "a".repeat(64)
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some(v2_scope),
            &[ProviderCacheCapabilityField::PromptCacheKey],
            ProviderCacheEffectStatus::Promoted,
            None,
            Some(100),
            Some(200),
            Some(100),
            Some(90),
        );
        assert!(!config.generated_prompt_cache_key_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some(v2_scope),
        ));

        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some(&v4_scope),
            &[ProviderCacheCapabilityField::PromptCacheKey],
            ProviderCacheEffectStatus::Promoted,
            Some("generated key improved real cache reads".to_string()),
            Some(100),
            Some(200),
            Some(100),
            Some(90),
        );
        assert!(config.generated_prompt_cache_key_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some(&v4_scope),
        ));
        assert!(!config.generated_prompt_cache_key_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-b"),
            Some(&v4_scope),
        ));
    }

    #[test]
    fn promoted_cache_control_requires_the_measured_effect_scope() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe_for_key(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            Some("key-a"),
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some("cache-effect-v2:realm-a:stream:no-store:bp=none"),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            ProviderCacheEffectStatus::Promoted,
            Some("measured exact wire shape".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );

        assert!(config.cache_capability_effect_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some("cache-effect-v2:realm-a:stream:no-store:bp=none"),
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
        assert!(!config.cache_capability_effect_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some("cache-effect-v2:realm-a:sync:no-store:bp=none"),
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
        assert!(!config.cache_capability_effect_promoted_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
            Some("cache-effect-v2:realm-a:stream:store:bp=none"),
            ProviderCacheCapabilityField::PromptCacheOptions,
        ));
    }

    #[test]
    fn legacy_enabled_cache_capability_requires_effect_reverification() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.provider_cache_capabilities[0].enabled = true;

        assert!(config.normalize_provider_cache_capability_effect_state());
        let record = &config.provider_cache_capabilities[0];
        assert!(!record.enabled);
        assert_eq!(record.status, ProviderCacheCapabilityStatus::Verified);
        assert_eq!(record.effect_status, ProviderCacheEffectStatus::Unverified);
    }

    #[test]
    fn legacy_unscoped_promoted_controls_require_exact_scope_reverification() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-legacy-breakpoint-evidence-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("config.toml");
        let mut config = AppConfig::default();
        for field in [
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ProviderCacheCapabilityField::PromptCacheOptions,
        ] {
            config.record_cache_capability_probe(
                "provider-a",
                "gpt-5.6-terra",
                Channel::Responses,
                field,
                ProviderCacheCapabilityStatus::Verified,
                None,
            );
            config.record_cache_capability_effect_for_key(
                "provider-a",
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                &[field],
                ProviderCacheEffectStatus::Promoted,
                Some("legacy promoted evidence".to_string()),
                Some(0),
                Some(512),
                Some(100),
                Some(100),
            );
        }
        config.save(&path).unwrap();

        let current = fs::read_to_string(&path).unwrap();
        assert_eq!(current.matches("evidence_version = 2").count(), 2);
        let legacy = current
            .lines()
            .filter(|line| !line.trim_start().starts_with("evidence_version ="))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&path, format!("{legacy}\n")).unwrap();

        let mut loaded = AppConfig::load_or_create(&path).unwrap();
        let breakpoint = loaded
            .cache_capability_for_key(
                "provider-a",
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                ProviderCacheCapabilityField::PromptCacheBreakpoint,
            )
            .unwrap();
        assert_eq!(
            breakpoint.evidence_version,
            PROVIDER_CACHE_CAPABILITY_EVIDENCE_VERSION
        );
        assert_eq!(breakpoint.status, ProviderCacheCapabilityStatus::Unverified);
        assert_eq!(
            breakpoint.effect_status,
            ProviderCacheEffectStatus::Unverified
        );
        assert!(!breakpoint.enabled);
        assert!(breakpoint
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("requires re-verification")));

        let options = loaded
            .cache_capability_for_key(
                "provider-a",
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                ProviderCacheCapabilityField::PromptCacheOptions,
            )
            .unwrap();
        assert_eq!(options.status, ProviderCacheCapabilityStatus::Verified);
        assert_eq!(
            options.effect_status,
            ProviderCacheEffectStatus::Unverified,
            "a promoted result without its key realm and wire shape cannot be reused"
        );
        assert!(!options.enabled);
        assert!(options
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("v2 exact-scope")));

        loaded.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        loaded.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some("cache-effect-v2:test-realm:stream:no-store:bp=v1:test-placement"),
            &[ProviderCacheCapabilityField::PromptCacheBreakpoint],
            ProviderCacheEffectStatus::Promoted,
            Some("re-verified".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        assert!(loaded.cache_capability_verified_for(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
        ));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn provider_cache_model_key_uses_real_model_for_request_alias() {
        let provider = ProviderConfig {
            id: "agent-codex-hb".to_string(),
            name: "hb / Codex".to_string(),
            base_url: "https://hubway.cc/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: true,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
                id: "gpt-5.6-sol".to_string(),
                request_model_id: Some("gpt-5.5".to_string()),
                display_name: "GPT-5.6 Sol".to_string(),
                context_window: Some(256_000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        assert_eq!(
            provider_model_cache_key(&provider, "gpt-5.5"),
            "gpt-5.6-sol"
        );
        assert_eq!(
            provider_model_cache_key(&provider, "gpt-5.6-sol"),
            "gpt-5.6-sol"
        );
    }

    #[test]
    fn missing_proxy_auto_start_loads_as_enabled() {
        let raw = r#"
host = "127.0.0.1"
port = 18883
local_key = "test-local-key"
default_channel = "anthropic"
workspace_fingerprint = "test-workspace"
providers = []
route_profiles = []
agent_injections = []
updated_at = "2026-06-20T00:00:00Z"

[cache]
mode = "prefix-prewarm"
enabled = true
exact_enabled = true
semantic_enabled = true
semantic_threshold = 0.985
max_age_seconds = 86400
max_entries = 300000
persist_encrypted = true
prewarm_enabled = true
"#;
        let config: AppConfig = toml::from_str(raw).expect("legacy config should parse");
        assert!(config.proxy_auto_start);
        assert!(!config.cache.background_prewarm_enabled);
    }

    #[test]
    fn proxy_mode_address_must_not_conflict_with_main_proxy() {
        let mut config = AppConfig::default();
        config.host = "127.0.0.1".to_string();
        config.port = 18883;
        config.proxy_mode_host = "127.0.0.1".to_string();
        config.proxy_mode_port = 18883;

        let dir = std::env::temp_dir().join(format!(
            "atoapi-proxy-mode-conflict-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let err = config.save(&dir.join("config.toml")).unwrap_err();

        assert!(err
            .to_string()
            .contains("proxy mode address must be different"));
        std::fs::remove_dir_all(dir).ok();
    }
    #[test]
    fn unknown_legacy_agent_injection_kind_is_ignored() {
        let raw = r#"
id = "legacy-unknown"
label = "Legacy Unknown"
kind = "legacy-unknown"
enabled = true
"#;
        let item: AgentInjectionConfig =
            toml::from_str(raw).expect("legacy agent kind should parse");
        assert_eq!(item.kind, AgentInjectionKind::Unknown);

        let mut items = vec![item];
        normalize_agent_injections(&mut items);

        assert!(items
            .iter()
            .all(|item| item.kind != AgentInjectionKind::Unknown));
        assert!(items.iter().all(|item| item.id != "legacy-unknown"));
        assert!(items.iter().any(|item| item.id == "gemini"));
        assert!(items.iter().any(|item| item.id == "codex"));
    }

    #[test]
    fn provider_key_pool_encrypts_and_preserves_saved_keys() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-first-secret"), true, 5)],
            })))
            .expect("provider should save");

        let pool = config
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .expect("pool should exist");
        assert_ne!(
            pool.keys[0].key_encrypted.as_deref(),
            Some("sk-first-secret")
        );
        assert_eq!(
            config
                .provider_key_secret(&provider_id, "key-a")
                .expect("secret should decrypt")
                .as_deref(),
            Some("sk-first-secret")
        );
        let public = config
            .public_key_pool_for_provider(&provider_id)
            .expect("public pool should exist");
        assert_eq!(public.available_keys, 1);
        assert_ne!(public.keys[0].preview, "sk-first-secret");
        assert!(public.keys[0].has_saved_secret);

        config
            .upsert_provider_key_pool(
                &provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::RoundRobin,
                    failure_threshold: 3,
                    recovery_minutes: 5,
                    keys: vec![key_input("key-a", None, true, 7)],
                },
            )
            .expect("pool update should preserve saved key");
        assert_eq!(
            config
                .provider_key_secret(&provider_id, "key-a")
                .expect("secret should still decrypt")
                .as_deref(),
            Some("sk-first-secret")
        );
    }

    #[test]
    fn provider_auto_compaction_limit_round_trips_and_can_be_cleared() {
        let mut config = AppConfig::default();
        let mut input = provider_input(None);
        input.auto_compact_token_limit = Some(120_000);
        input.auto_compact_token_limit_configured = true;
        let provider_id = config.upsert_provider(input).unwrap();
        assert_eq!(
            config.auto_compact_token_limit_for_provider(&provider_id),
            Some(120_000)
        );
        assert_eq!(
            config
                .public_view(PathBuf::from("config.toml"))
                .providers
                .iter()
                .find(|provider| provider.id == provider_id)
                .and_then(|provider| provider.auto_compact_token_limit),
            Some(120_000)
        );

        // A caller from an older UI/API does not know this optional field and
        // therefore must not erase the saved override on an unrelated edit.
        config.upsert_provider(provider_input(None)).unwrap();
        assert_eq!(
            config.auto_compact_token_limit_for_provider(&provider_id),
            Some(120_000)
        );

        let mut clear = provider_input(None);
        clear.auto_compact_token_limit = None;
        clear.auto_compact_token_limit_configured = true;
        config.upsert_provider(clear).unwrap();
        assert_eq!(
            config.auto_compact_token_limit_for_provider(&provider_id),
            None
        );
    }

    #[test]
    fn provider_key_pool_round_robin_skips_failed_keys() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 1,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", Some("sk-a"), true, 5),
                    key_input("key-b", Some("sk-b"), true, 5),
                ],
            })))
            .expect("provider should save");

        let first = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("key selection should work")
            .expect("first key should exist");
        assert_eq!(first.key_id.as_deref(), Some("key-a"));
        assert_eq!(first.secret, "sk-a");

        let second = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("key selection should work")
            .expect("second key should exist");
        assert_eq!(second.key_id.as_deref(), Some("key-b"));
        assert_eq!(second.secret, "sk-b");

        config.mark_provider_key_failure(&provider_id, Some("key-a"), "HTTP 429", true);
        let next = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("key selection should work")
            .expect("healthy key should exist");
        assert_eq!(next.key_id.as_deref(), Some("key-b"));
    }

    #[test]
    fn selected_pooled_key_carries_only_its_saved_encrypted_material_digest() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 1,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-first"), true, 5)],
            })))
            .expect("provider should save");
        let saved = config
            .provider_key(&provider_id, "key-a")
            .expect("saved Key must exist")
            .key_encrypted
            .clone()
            .expect("saved Key must keep encrypted material");
        let expected = encrypted_provider_key_material_digest(&saved)
            .expect("non-empty encrypted material must produce a digest");
        let selected = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("selection should work")
            .expect("pooled Key should be selected");
        assert_eq!(selected.key_id.as_deref(), Some("key-a"));
        assert_eq!(
            selected.encrypted_material_digest.as_deref(),
            Some(expected.as_str())
        );
        assert_ne!(
            selected.encrypted_material_digest.as_deref(),
            Some("sk-first")
        );

        config
            .upsert_provider_key_pool(
                &provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 1,
                    recovery_minutes: 5,
                    keys: vec![key_input("key-a", Some("sk-rotated"), true, 5)],
                },
            )
            .expect("rotated Key should save");
        let rotated = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("selection should work after Key rotation")
            .expect("rotated Key should be selected");
        assert_ne!(
            selected.encrypted_material_digest, rotated.encrypted_material_digest,
            "a replaced saved Key record must produce a new diagnostic reference input"
        );
    }

    #[test]
    fn provider_key_health_cooldown_preserves_the_user_enabled_switch() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-a"), true, 5)],
            })))
            .expect("provider should save");

        config.mark_provider_key_failure(&provider_id, Some("key-a"), "HTTP 429", true);
        let failed = config
            .provider_key(&provider_id, "key-a")
            .expect("key should exist");
        assert!(
            failed.enabled,
            "health failure must not change the user switch"
        );
        assert!(
            failed.disabled_until.is_some(),
            "health failure should apply cooldown"
        );
        assert!(config
            .select_provider_key_for_request(&provider_id, None, None)
            .is_err());

        config.mark_provider_key_success(&provider_id, Some("key-a"));
        let recovered = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("recovered key selection should work")
            .expect("recovered key should be selectable");
        assert_eq!(recovered.key_id.as_deref(), Some("key-a"));
    }

    #[test]
    fn quota_failure_disables_key_but_temporary_transport_failure_does_not() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-quota", Some("sk-quota"), true, 5),
                    key_input("key-network", Some("sk-network"), true, 5),
                ],
            })))
            .expect("provider should save");

        config.mark_provider_key_failure(
            &provider_id,
            Some("key-quota"),
            "HTTP 402: insufficient_quota; balance is exhausted",
            true,
        );
        let quota_key = config
            .provider_key(&provider_id, "key-quota")
            .expect("quota key should exist");
        assert!(
            !quota_key.enabled,
            "quota exhaustion should turn the Key off"
        );
        assert_eq!(quota_key.status, ProviderKeyStatus::Unhealthy);
        let next_key = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("next inbound should select a remaining Key")
            .expect("remaining Key should be available");
        assert_eq!(next_key.key_id.as_deref(), Some("key-network"));

        config.mark_provider_key_failure(
            &provider_id,
            Some("key-network"),
            "request failed: connection timed out",
            true,
        );
        let network_key = config
            .provider_key(&provider_id, "key-network")
            .expect("network key should exist");
        assert!(
            network_key.enabled,
            "temporary transport errors must not turn a Key off"
        );
        assert!(
            network_key.disabled_until.is_some(),
            "temporary errors still receive a cooldown"
        );
    }

    #[test]
    fn new_key_pool_input_defaults_to_sequential_order() {
        let input: ProviderKeyPoolInput = serde_json::from_str(r#"{"enabled":true,"keys":[]}"#)
            .expect("new key-pool input should deserialize");
        assert_eq!(input.strategy, KeyLoadBalanceStrategy::Sequential);
    }

    #[test]
    fn re_enabling_a_key_clears_stale_health_cooldown_and_error() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-a"), false, 5)],
            })))
            .expect("provider should save");
        let stale_cooldown = Utc::now() + chrono::Duration::minutes(5);
        {
            let key = config
                .provider_key_mut(&provider_id, "key-a")
                .expect("key should exist");
            key.status = ProviderKeyStatus::Unhealthy;
            key.failures = 3;
            key.last_checked_at = Some(Utc::now());
            key.last_error = Some("old quota failure".to_string());
            key.disabled_until = Some(stale_cooldown);
        }

        let mut re_enabled = key_input("key-a", None, true, 5);
        re_enabled.status = ProviderKeyStatus::Unhealthy;
        re_enabled.failures = 3;
        re_enabled.last_checked_at = Some(Utc::now());
        re_enabled.last_error = Some("old quota failure".to_string());
        re_enabled.disabled_until = Some(stale_cooldown);
        config
            .upsert_provider_key_pool(
                &provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 3,
                    recovery_minutes: 5,
                    keys: vec![re_enabled],
                },
            )
            .expect("key should re-enable");

        let key = config
            .provider_key(&provider_id, "key-a")
            .expect("key should exist");
        assert!(key.enabled);
        assert_eq!(key.status, ProviderKeyStatus::Unknown);
        assert_eq!(key.failures, 0);
        assert!(key.last_checked_at.is_none());
        assert!(key.last_error.is_none());
        assert!(key.disabled_until.is_none());
        assert!(config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("re-enabled key selection should work")
            .is_some());
    }

    #[test]
    fn stale_editor_save_preserves_runtime_key_health() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Sequential,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-a"), true, 5)],
            })))
            .expect("provider should save");

        config.mark_provider_key_failure(&provider_id, Some("key-a"), "HTTP 429", true);
        let stale_editor_draft = ProviderKeyPoolInput {
            enabled: true,
            strategy: KeyLoadBalanceStrategy::Sequential,
            failure_threshold: 3,
            recovery_minutes: 5,
            // This is the stale state a previously opened editor would send
            // while changing only ordering or aliases. Its missing cooldown
            // must not invalidate the provider's cache/session evidence.
            keys: vec![key_input("key-a", None, true, 9)],
        };
        assert!(
            !provider_key_pool_connection_changed(
                config
                    .provider_key_pools
                    .iter()
                    .find(|pool| pool.provider_id == provider_id),
                &stale_editor_draft,
            ),
            "runtime health and ordering are not connection changes"
        );
        config
            .upsert_provider_key_pool(&provider_id, stale_editor_draft)
            .expect("stale editor save should work");

        let key = config
            .provider_key(&provider_id, "key-a")
            .expect("key should exist");
        assert_eq!(key.priority, 9);
        assert_eq!(key.status, ProviderKeyStatus::Unhealthy);
        assert_eq!(key.failures, 1);
        assert!(key.disabled_until.is_some());
    }

    #[test]
    fn enabled_provider_key_pool_never_falls_back_to_connection_key() {
        let mut config = AppConfig::default();
        let mut input = provider_input(Some(ProviderKeyPoolInput {
            enabled: true,
            strategy: KeyLoadBalanceStrategy::RoundRobin,
            failure_threshold: 1,
            recovery_minutes: 5,
            keys: vec![key_input("pool-key", Some("sk-pool"), false, 5)],
        }));
        input.api_key = Some("sk-connection".to_string());
        let provider_id = config.upsert_provider(input).expect("provider should save");

        let err = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect_err(
                "an enabled pool with no eligible key must not fall back to the connection key",
            );
        assert!(err
            .to_string()
            .contains("provider key pool has no enabled usable key"));

        config
            .upsert_provider_key_pool(
                &provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::RoundRobin,
                    failure_threshold: 1,
                    recovery_minutes: 5,
                    keys: vec![key_input("pool-key", Some("sk-pool"), true, 5)],
                },
            )
            .expect("pool should update");
        let selected = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("selection should work")
            .expect("the enabled pool key should be used");
        assert_eq!(selected.key_id.as_deref(), Some("pool-key"));
        assert_eq!(selected.secret, "sk-pool");
    }

    #[test]
    fn disabled_provider_key_pool_uses_connection_key() {
        let mut config = AppConfig::default();
        let mut input = provider_input(Some(ProviderKeyPoolInput {
            enabled: false,
            strategy: KeyLoadBalanceStrategy::RoundRobin,
            failure_threshold: 1,
            recovery_minutes: 5,
            keys: vec![key_input("pool-key", Some("sk-pool"), true, 5)],
        }));
        input.api_key = Some("sk-connection".to_string());
        let provider_id = config.upsert_provider(input).expect("provider should save");

        let selected = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("selection should work")
            .expect("connection key should be available");
        assert_eq!(selected.key_id, None);
        assert_eq!(selected.secret, "sk-connection");
    }

    #[test]
    fn provider_key_pool_reorder_resets_round_robin_and_priority_ties_use_editor_order() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", Some("sk-a"), true, 5),
                    key_input("key-b", Some("sk-b"), true, 5),
                    key_input("key-c", Some("sk-c"), true, 5),
                ],
            })))
            .expect("provider should save");

        config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("first selection should work");
        config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("second selection should work");

        config
            .upsert_provider_key_pool(
                &provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::RoundRobin,
                    failure_threshold: 3,
                    recovery_minutes: 5,
                    keys: vec![
                        key_input("key-c", None, true, 5),
                        key_input("key-a", None, true, 5),
                        key_input("key-b", None, true, 5),
                    ],
                },
            )
            .expect("reordered pool should save");
        assert_eq!(
            config
                .provider_key_pools
                .iter()
                .find(|pool| pool.provider_id == provider_id)
                .expect("pool should exist")
                .next_index,
            0
        );
        let reordered = config
            .select_provider_key_for_request(&provider_id, None, None)
            .expect("reordered selection should work")
            .expect("reordered key should exist");
        assert_eq!(reordered.key_id.as_deref(), Some("key-c"));

        let mut priority_config = AppConfig::default();
        let priority_provider_id = priority_config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Priority,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", Some("sk-a"), true, 5),
                    key_input("key-b", Some("sk-b"), true, 5),
                    key_input("key-c", Some("sk-c"), true, 5),
                ],
            })))
            .expect("priority provider should save");
        let priority_selected = priority_config
            .select_provider_key_for_request(&priority_provider_id, None, None)
            .expect("priority selection should work")
            .expect("priority key should exist");
        assert_eq!(priority_selected.key_id.as_deref(), Some("key-a"));
    }

    #[test]
    fn provider_key_pool_prefers_affinity_key_when_available() {
        let mut config = AppConfig::default();
        let provider_id = config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 1,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", Some("sk-a"), true, 5),
                    key_input("key-b", Some("sk-b"), true, 5),
                ],
            })))
            .expect("provider should save");

        let selected = config
            .select_provider_key_for_request(&provider_id, Some("key-b"), None)
            .expect("key selection should work")
            .expect("preferred key should exist");
        assert_eq!(selected.key_id.as_deref(), Some("key-b"));
        assert_eq!(selected.secret, "sk-b");

        let failover = config
            .select_provider_key_for_request(&provider_id, Some("key-b"), Some("key-b"))
            .expect("key selection should work")
            .expect("fallback key should exist");
        assert_eq!(failover.key_id.as_deref(), Some("key-a"));
    }

    #[test]
    fn retired_response_session_records_are_removed_on_load() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-retired-session-reuse-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("config.toml");
        let current = toml::to_string(&AppConfig::default()).unwrap();
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            &path,
            format!(
                "{current}\n[[provider_response_session_reuse]]\nprovider_id = \"provider-a\"\nmodel_id = \"model-a\"\nenabled = true\nstatus = \"verified\"\n"
            ),
        )
        .unwrap();

        let _loaded = AppConfig::load_or_create(&path).unwrap();

        let persisted = fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains("provider_response_session_reuse"));
        assert!(!persisted.contains("status = \"verified\""));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn isolated_test_instance_requires_explicit_flag_and_valid_port() {
        assert!(isolated_test_flag_enabled("1"));
        assert!(isolated_test_flag_enabled("enabled"));
        assert!(!isolated_test_flag_enabled("0"));
        assert_eq!(
            parse_isolated_test_listen_port(true, Some("18885")),
            Some(18885)
        );
        assert_eq!(parse_isolated_test_listen_port(false, Some("18885")), None);
        assert_eq!(parse_isolated_test_listen_port(true, Some("0")), None);
        assert_eq!(parse_isolated_test_listen_port(true, Some("invalid")), None);
    }

    #[test]
    fn legacy_agentproxy_import_is_opt_in() {
        assert!(!legacy_agentproxy_import_enabled(None));
        assert!(!legacy_agentproxy_import_enabled(Some("0")));
        assert!(legacy_agentproxy_import_enabled(Some("true")));
        assert!(legacy_agentproxy_import_enabled(Some("enabled")));
    }

    #[test]
    fn loading_config_prunes_orphaned_route_references_without_touching_provider_state() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-config-prune-orphans-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("config.toml");
        let mut config = AppConfig::default();
        let live_provider_id = config.upsert_provider(provider_input(None)).unwrap();

        config.active_provider_id = Some("removed-provider".to_string());
        config.route_profiles[0].provider_id = Some("removed-provider".to_string());
        config.route_profiles[0].model_alias = Some("old-route-model".to_string());

        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("removed-provider".to_string());
        codex.model_id = Some("old-route-model".to_string());
        codex.last_status = Some("previously applied".to_string());
        codex.hidden_provider_ids = vec![
            live_provider_id.clone(),
            "removed-provider".to_string(),
            live_provider_id.clone(),
        ];
        config.agent_provider_orders = vec![
            AgentProviderOrderConfig {
                agent_id: "codex".to_string(),
                provider_ids: vec![
                    live_provider_id.clone(),
                    "removed-provider".to_string(),
                    live_provider_id.clone(),
                ],
            },
            AgentProviderOrderConfig {
                agent_id: "removed-agent".to_string(),
                provider_ids: vec![live_provider_id.clone()],
            },
        ];

        config.save(&path).unwrap();

        let loaded = AppConfig::load_or_create(&path).unwrap();
        assert_eq!(loaded.providers.len(), 1);
        assert_eq!(loaded.providers[0].id, live_provider_id);
        assert!(loaded.active_provider_id.is_none());
        assert!(loaded.route_profiles[0].provider_id.is_none());
        assert!(loaded.route_profiles[0].model_alias.is_none());

        let codex = loaded
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        assert!(!codex.enabled);
        assert!(codex.provider_id.is_none());
        assert!(codex.model_id.is_none());
        assert!(codex.last_status.is_none());
        assert_eq!(codex.hidden_provider_ids, vec![live_provider_id.clone()]);
        assert_eq!(
            loaded.agent_provider_orders,
            vec![AgentProviderOrderConfig {
                agent_id: "codex".to_string(),
                provider_ids: vec![live_provider_id.clone()],
            }]
        );

        let persisted = fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains("removed-provider"));
        assert!(!persisted.contains("removed-agent"));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn loading_a_legacy_manual_balance_url_cleans_only_the_retired_section() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-retired-balance-config-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        AppConfig::default().save(&path).unwrap();
        let mut raw = fs::read_to_string(&path).unwrap();
        raw.push_str(
            "\n[[provider_balance_probe_configs]]\nprovider_id = \"legacy\"\nendpoint_url = \"https://legacy.example/balance\"\nupdated_at = \"2026-08-05T00:00:00Z\"\n",
        );
        fs::write(&path, raw).unwrap();

        let loaded = AppConfig::load_or_create(&path).unwrap();
        assert!(loaded.providers.is_empty());
        let persisted = fs::read_to_string(&path).unwrap();
        assert!(
            !persisted.contains("provider_balance_probe_configs"),
            "the deprecated manual endpoint must be removed on the first v1.4.25 load"
        );
        assert!(!persisted.contains("legacy.example/balance"));
        fs::remove_dir_all(dir).ok();
    }

    fn provider_input(key_pool: Option<ProviderKeyPoolInput>) -> ProviderInput {
        ProviderInput {
            id: Some("share".to_string()),
            owner_agent_id: None,
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel_mode: ProviderChannelMode::Auto,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            non_sse_compact_compat_enabled: false,
            auto_compact_token_limit: None,
            auto_compact_token_limit_configured: false,
            api_key: None,
            key_pool,
            enabled: true,
        }
    }

    fn key_input(id: &str, key: Option<&str>, enabled: bool, priority: u32) -> ProviderKeyInput {
        ProviderKeyInput {
            id: Some(id.to_string()),
            alias: None,
            key: key.map(ToOwned::to_owned),
            enabled,
            priority,
            status: ProviderKeyStatus::Unknown,
            total_requests: 0,
            successes: 0,
            failures: 0,
            last_checked_at: None,
            last_error: None,
            disabled_until: None,
        }
    }
}
