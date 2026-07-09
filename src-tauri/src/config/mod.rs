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
    pub display_name: String,
    pub context_window: Option<u32>,
    pub output_window: Option<u32>,
    pub supports_tools: bool,
    pub supports_streaming: bool,
    pub enabled: bool,
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

    pub fn normalize_smart_max_hit(&mut self) {
        self.mode = CacheMode::PrefixPrewarm;
        if self.enabled {
            self.exact_enabled = true;
            self.semantic_enabled = true;
            self.prewarm_enabled = true;
        } else {
            self.exact_enabled = false;
            self.semantic_enabled = false;
            self.prewarm_enabled = false;
        }
        // Active prewarm was removed by product rule: do not spend a second
        // upstream request to improve cosmetic cache hit rate.
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
    pub non_sse_compact_compat_enabled: bool,
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
            config.cache.normalize_smart_max_hit();
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
                    non_sse_compact_compat_enabled: self
                        .non_sse_compact_compat_enabled_for_provider(&provider.id),
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

        if let Some(provider) = self.providers.iter_mut().find(|p| p.id == id) {
            provider.name = input.name;
            provider.base_url = input.base_url;
            provider.models_url = clean_optional_string(input.models_url);
            provider.is_full_url = input.is_full_url;
            provider.custom_user_agent = clean_optional_string(input.custom_user_agent);
            provider.channel = input.channel;
            provider.prompt_cache_retention_enabled = input.prompt_cache_retention_enabled;
            provider.request_body_gzip_enabled = input.request_body_gzip_enabled;
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
                api_key_encrypted: encrypted_key,
                models: Vec::new(),
                enabled: input.enabled,
                created_at: now,
                updated_at: now,
            });
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
        cache.normalize_smart_max_hit();
        assert!(!cache.background_prewarm_enabled);
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
