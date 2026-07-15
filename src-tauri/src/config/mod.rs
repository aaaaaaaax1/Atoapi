use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
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
    false
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
    pub provider_key_pools: Vec<ProviderKeyPoolConfig>,
    #[serde(default)]
    pub provider_compact_modes: Vec<ProviderCompactModeConfig>,
    #[serde(default)]
    pub provider_channel_modes: Vec<ProviderChannelModeConfig>,
    #[serde(default)]
    pub provider_response_session_reuse: Vec<ProviderResponseSessionReuseConfig>,
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
    #[serde(default)]
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
    #[serde(default)]
    pub use_system_proxy: bool,
    #[serde(default)]
    pub non_sse_compact_compat_enabled: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderResponseSessionReuseStatus {
    Unverified,
    Verified,
    Unsupported,
    Error,
}

impl Default for ProviderResponseSessionReuseStatus {
    fn default() -> Self {
        Self::Unverified
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResponseSessionReuseConfig {
    pub provider_id: String,
    pub model_id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub status: ProviderResponseSessionReuseStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResponseSessionReuseProbeResult {
    pub provider_id: String,
    pub model_id: String,
    pub status: ProviderResponseSessionReuseStatus,
    pub enabled: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_status: Option<u16>,
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
    pub field: ProviderCacheCapabilityField,
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

/// A non-secret snapshot of the provider connection that a manual Responses
/// session-reuse probe actually exercised.  It is intentionally transient:
/// the probe command compares it again before persisting a verified result so
/// an old endpoint or key pool cannot be marked as verified after settings
/// change mid-probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponseSessionReuseProbeTarget {
    provider_id: String,
    base_url: String,
    is_full_url: bool,
    channel: Channel,
    channel_mode: ProviderChannelMode,
    provider_updated_at: DateTime<Utc>,
    key_pool_updated_at: Option<DateTime<Utc>>,
}

/// The mutable capability state that a probe must not overwrite when the user
/// changes the setting while its two management requests are in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponseSessionReuseRecordSnapshot {
    enabled: bool,
    status: ProviderResponseSessionReuseStatus,
    updated_at: DateTime<Utc>,
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
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(default)]
    pub response_session_reuse_models: Vec<ProviderResponseSessionReuseConfig>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicConfig {
    pub host: String,
    pub port: u16,
    pub proxy_auto_start: bool,
    pub proxy_mode_host: String,
    pub proxy_mode_port: u16,
    pub local_key: String,
    pub default_channel: Channel,
    pub active_provider_id: Option<String>,
    pub workspace_fingerprint: String,
    pub providers: Vec<PublicProvider>,
    pub route_profiles: Vec<RouteProfile>,
    pub cache: CacheConfig,
    pub agent_injections: Vec<AgentInjectionConfig>,
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
            provider_key_pools: Vec::new(),
            provider_compact_modes: Vec::new(),
            provider_channel_modes: Vec::new(),
            provider_response_session_reuse: Vec::new(),
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
impl AppConfig {
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let mut config: AppConfig = toml::from_str(&raw)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let mut changed = false;
            config.cache.normalize_fast_forwarding_hit_policy();
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

    pub fn public_view(&self, config_path: PathBuf) -> PublicConfig {
        PublicConfig {
            host: self.host.clone(),
            port: self.port,
            proxy_auto_start: self.proxy_auto_start,
            proxy_mode_host: self.proxy_mode_host.clone(),
            proxy_mode_port: self.proxy_mode_port,
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
                    response_session_reuse_models: self
                        .response_session_reuse_for_provider(&provider.id),
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
                let secret = selected
                    .key_encrypted
                    .as_deref()
                    .map(decrypt_secret)
                    .transpose()?;
                if let Some(secret) = secret {
                    return Ok(Some(SelectedProviderKey {
                        secret,
                        key_id: Some(selected.id.clone()),
                    }));
                }
            }
        }

        self.provider_api_key(provider_id).map(|key| {
            key.map(|secret| SelectedProviderKey {
                secret,
                key_id: None,
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
        force_disable: bool,
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
            if force_disable || key.failures >= failure_threshold as u64 {
                key.enabled = false;
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

    pub fn provider_channel_mode_for_provider(&self, provider_id: &str) -> ProviderChannelMode {
        self.provider_channel_modes
            .iter()
            .find(|item| item.provider_id == provider_id)
            .map(|item| item.mode.clone())
            .unwrap_or_default()
    }

    pub fn response_session_reuse_for_provider(
        &self,
        provider_id: &str,
    ) -> Vec<ProviderResponseSessionReuseConfig> {
        self.provider_response_session_reuse
            .iter()
            .filter(|item| item.provider_id == provider_id)
            .cloned()
            .collect()
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
                && item.enabled
                && item.status == ProviderCacheCapabilityStatus::Verified
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
                item.status = status;
                if item.status != ProviderCacheCapabilityStatus::Verified {
                    item.enabled = false;
                    item.effect_status = ProviderCacheEffectStatus::Unverified;
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
                    field,
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
        let now = Utc::now();
        for item in self.provider_cache_capabilities.iter_mut().filter(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && &item.channel == channel
                && item.key_id.as_deref() == key_id
                && fields.contains(&item.field)
                && item.status == ProviderCacheCapabilityStatus::Verified
        }) {
            let preserve_promoted = effect_status == ProviderCacheEffectStatus::Error
                && item.effect_status == ProviderCacheEffectStatus::Promoted;
            item.effect_checked_at = Some(now);
            if preserve_promoted {
                item.last_error = clean_optional_string(message.clone());
                item.updated_at = now;
                continue;
            }
            item.effect_status = effect_status;
            item.enabled = effect_status == ProviderCacheEffectStatus::Promoted;
            item.effect_message = clean_optional_string(message.clone());
            item.baseline_cache_read_tokens = baseline_cache_read_tokens;
            item.candidate_cache_read_tokens = candidate_cache_read_tokens;
            item.baseline_ttft_ms = baseline_ttft_ms;
            item.candidate_ttft_ms = candidate_ttft_ms;
            if effect_status == ProviderCacheEffectStatus::Error {
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
        for item in &mut self.provider_cache_capabilities {
            let promoted = item.effect_status == ProviderCacheEffectStatus::Promoted
                && item.status == ProviderCacheCapabilityStatus::Verified;
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

    pub fn response_session_reuse_verified_for(&self, provider_id: &str, model_id: &str) -> bool {
        self.provider_response_session_reuse.iter().any(|item| {
            item.provider_id == provider_id
                && item.model_id == model_id
                && item.enabled
                && item.status == ProviderResponseSessionReuseStatus::Verified
        })
    }

    pub fn response_session_reuse_record_snapshot(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<ProviderResponseSessionReuseRecordSnapshot> {
        self.provider_response_session_reuse
            .iter()
            .find(|item| item.provider_id == provider_id && item.model_id == model_id)
            .map(|item| ProviderResponseSessionReuseRecordSnapshot {
                enabled: item.enabled,
                status: item.status.clone(),
                updated_at: item.updated_at,
            })
    }

    pub fn response_session_reuse_probe_target(
        &self,
        provider_id: &str,
    ) -> Option<ProviderResponseSessionReuseProbeTarget> {
        let provider = self
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)?;
        Some(ProviderResponseSessionReuseProbeTarget {
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

    pub fn set_response_session_reuse_enabled(
        &mut self,
        provider_id: &str,
        model_id: &str,
        enabled: bool,
    ) -> Result<()> {
        let item = self
            .provider_response_session_reuse
            .iter_mut()
            .find(|item| item.provider_id == provider_id && item.model_id == model_id)
            .ok_or_else(|| {
                anyhow!("Responses session reuse has not been verified for {model_id}")
            })?;
        if enabled && item.status != ProviderResponseSessionReuseStatus::Verified {
            return Err(anyhow!(
                "Responses session reuse must pass compatibility verification before enabling"
            ));
        }
        item.enabled = enabled;
        item.updated_at = Utc::now();
        self.updated_at = item.updated_at;
        Ok(())
    }

    pub fn record_response_session_reuse_probe(
        &mut self,
        provider_id: &str,
        model_id: &str,
        status: ProviderResponseSessionReuseStatus,
        message: Option<String>,
    ) {
        let now = Utc::now();
        let enabled = status == ProviderResponseSessionReuseStatus::Verified;
        if let Some(item) = self
            .provider_response_session_reuse
            .iter_mut()
            .find(|item| item.provider_id == provider_id && item.model_id == model_id)
        {
            item.enabled = enabled;
            item.status = status;
            item.checked_at = Some(now);
            item.last_error = clean_optional_string(message);
            item.updated_at = now;
        } else {
            self.provider_response_session_reuse
                .push(ProviderResponseSessionReuseConfig {
                    provider_id: provider_id.to_string(),
                    model_id: model_id.to_string(),
                    enabled,
                    status,
                    checked_at: Some(now),
                    last_error: clean_optional_string(message),
                    updated_at: now,
                });
        }
        self.updated_at = now;
    }

    pub fn clear_response_session_reuse_for_provider(&mut self, provider_id: &str) {
        self.provider_response_session_reuse
            .retain(|item| item.provider_id != provider_id);
        self.updated_at = Utc::now();
    }

    pub fn clear_response_session_reuse_for_model(&mut self, provider_id: &str, model_id: &str) {
        self.provider_response_session_reuse
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
            self.clear_response_session_reuse_for_provider(&id);
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
                let encrypted = item
                    .key
                    .as_deref()
                    .filter(|key| !key.trim().is_empty())
                    .map(encrypt_secret)
                    .transpose()?
                    .or_else(|| existing.and_then(|key| key.key_encrypted.clone()));
                Ok(ProviderKeyConfig {
                    id,
                    alias: clean_optional_string(item.alias),
                    key_encrypted: encrypted,
                    enabled: item.enabled,
                    priority: item.priority,
                    status: item.status,
                    total_requests: item.total_requests,
                    successes: item.successes,
                    failures: item.failures,
                    last_checked_at: item.last_checked_at,
                    last_error: clean_optional_string(item.last_error),
                    disabled_until: item.disabled_until,
                    created_at: existing.map(|key| key.created_at).unwrap_or(now),
                    updated_at: now,
                })
            })
            .collect::<Result<Vec<_>>>()?;

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
            if !pool.keys.is_empty() {
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
    if existing.enabled != input.enabled || existing.strategy != input.strategy {
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
            || existing_key.priority != input_key.priority
            || existing_key.disabled_until != input_key.disabled_until
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
    if !current.exists() && legacy.exists() {
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
        assert!(!provider.use_system_proxy);
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
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
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
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
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
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            Some("key-a"),
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
    fn response_session_reuse_requires_model_scoped_verification() {
        let mut config = AppConfig::default();
        config.record_response_session_reuse_probe(
            "provider-a",
            "model-a",
            ProviderResponseSessionReuseStatus::Verified,
            None,
        );

        assert!(config.response_session_reuse_verified_for("provider-a", "model-a"));
        assert!(!config.response_session_reuse_verified_for("provider-a", "model-b"));

        config
            .set_response_session_reuse_enabled("provider-a", "model-a", false)
            .unwrap();
        assert!(!config.response_session_reuse_verified_for("provider-a", "model-a"));

        config
            .set_response_session_reuse_enabled("provider-a", "model-a", true)
            .unwrap();
        assert!(config.response_session_reuse_verified_for("provider-a", "model-a"));

        config.record_response_session_reuse_probe(
            "provider-a",
            "model-a",
            ProviderResponseSessionReuseStatus::Unsupported,
            Some("previous_response_id is not supported".to_string()),
        );
        assert!(!config.response_session_reuse_verified_for("provider-a", "model-a"));
    }

    #[test]
    fn response_session_reuse_snapshot_changes_when_user_disables_it() {
        let mut config = AppConfig::default();
        config.record_response_session_reuse_probe(
            "provider-a",
            "model-a",
            ProviderResponseSessionReuseStatus::Verified,
            None,
        );
        let before = config
            .response_session_reuse_record_snapshot("provider-a", "model-a")
            .expect("verified record should have a snapshot");

        config
            .set_response_session_reuse_enabled("provider-a", "model-a", false)
            .unwrap();

        assert_ne!(
            config.response_session_reuse_record_snapshot("provider-a", "model-a"),
            Some(before)
        );
    }

    #[test]
    fn provider_connection_change_invalidates_session_reuse_verification() {
        let mut config = AppConfig::default();
        config.upsert_provider(provider_input(None)).unwrap();
        config.record_response_session_reuse_probe(
            "share",
            "gpt-5.5",
            ProviderResponseSessionReuseStatus::Verified,
            None,
        );
        config.record_cache_capability_probe(
            "share",
            "gpt-5.5",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        assert!(config.response_session_reuse_verified_for("share", "gpt-5.5"));
        let probe_target = config
            .response_session_reuse_probe_target("share")
            .expect("provider probe target should exist");

        let mut changed = provider_input(None);
        changed.base_url = "https://other.example/v1".to_string();
        config.upsert_provider(changed).unwrap();

        assert!(!config.response_session_reuse_verified_for("share", "gpt-5.5"));
        assert_eq!(
            config.cache_capability_status(
                "share",
                "gpt-5.5",
                &Channel::Responses,
                ProviderCacheCapabilityField::PromptCacheKey,
            ),
            ProviderCacheCapabilityStatus::Unverified
        );
        assert_ne!(
            config.response_session_reuse_probe_target("share").as_ref(),
            Some(&probe_target)
        );
    }

    #[test]
    fn provider_key_pool_secret_change_invalidates_session_reuse_verification() {
        let mut config = AppConfig::default();
        config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-original"), true, 5)],
            })))
            .unwrap();
        config.record_response_session_reuse_probe(
            "share",
            "gpt-5.5",
            ProviderResponseSessionReuseStatus::Verified,
            None,
        );
        assert!(config.response_session_reuse_verified_for("share", "gpt-5.5"));
        let probe_target = config
            .response_session_reuse_probe_target("share")
            .expect("provider probe target should exist");

        config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::RoundRobin,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![key_input("key-a", Some("sk-replaced"), true, 5)],
            })))
            .unwrap();

        assert!(!config.response_session_reuse_verified_for("share", "gpt-5.5"));
        assert_ne!(
            config.response_session_reuse_probe_target("share").as_ref(),
            Some(&probe_target)
        );
    }

    #[test]
    fn provider_key_pool_routing_change_invalidates_session_reuse_verification() {
        let mut config = AppConfig::default();
        config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Priority,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", Some("sk-a"), true, 10),
                    key_input("key-b", Some("sk-b"), true, 5),
                ],
            })))
            .unwrap();
        config.record_response_session_reuse_probe(
            "share",
            "gpt-5.5",
            ProviderResponseSessionReuseStatus::Verified,
            None,
        );
        assert!(config.response_session_reuse_verified_for("share", "gpt-5.5"));

        config
            .upsert_provider(provider_input(Some(ProviderKeyPoolInput {
                enabled: true,
                strategy: KeyLoadBalanceStrategy::Priority,
                failure_threshold: 3,
                recovery_minutes: 5,
                keys: vec![
                    key_input("key-a", None, false, 10),
                    key_input("key-b", None, true, 5),
                ],
            })))
            .unwrap();

        assert!(!config.response_session_reuse_verified_for("share", "gpt-5.5"));
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

    fn provider_input(key_pool: Option<ProviderKeyPoolInput>) -> ProviderInput {
        ProviderInput {
            id: Some("share".to_string()),
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
