use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

fn default_prompt_cache_retention_enabled() -> bool {
    true
}

fn default_request_body_gzip_enabled() -> bool {
    false
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
    pub channel: Channel,
    #[serde(default = "default_prompt_cache_retention_enabled")]
    pub prompt_cache_retention_enabled: bool,
    #[serde(default = "default_request_body_gzip_enabled")]
    pub request_body_gzip_enabled: bool,
    pub api_key: Option<String>,
    pub enabled: bool,
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
    ProxyMode,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicProvider {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub models_url: Option<String>,
    pub is_full_url: bool,
    pub custom_user_agent: Option<String>,
    pub channel: Channel,
    pub prompt_cache_retention_enabled: bool,
    pub request_body_gzip_enabled: bool,
    pub has_api_key: bool,
    pub models: Vec<ModelConfig>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicConfig {
    pub host: String,
    pub port: u16,
    pub proxy_auto_start: bool,
    pub local_key: String,
    pub default_channel: Channel,
    pub active_provider_id: Option<String>,
    pub workspace_fingerprint: String,
    pub providers: Vec<PublicProvider>,
    pub route_profiles: Vec<RouteProfile>,
    pub cache: CacheConfig,
    pub agent_injections: Vec<AgentInjectionConfig>,
    pub updated_at: DateTime<Utc>,
    pub config_path: PathBuf,
}

impl Default for AppConfig {
    fn default() -> Self {
        let now = Utc::now();
        let local_key = format!("ato-{}", Uuid::new_v4().simple());

        Self {
            host: "127.0.0.1".to_string(),
            port: 18883,
            proxy_auto_start: default_proxy_auto_start(),
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
            updated_at: now,
        }
    }
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
        if self.host.parse::<IpAddr>()?.is_unspecified() && self.local_key.trim().is_empty() {
            return Err(anyhow!(
                "binding to 0.0.0.0 requires a non-empty local authentication key"
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
                    channel: provider.channel.clone(),
                    prompt_cache_retention_enabled: provider.prompt_cache_retention_enabled,
                    request_body_gzip_enabled: provider.request_body_gzip_enabled,
                    has_api_key: provider.api_key_encrypted.is_some(),
                    models: provider.models.clone(),
                    enabled: provider.enabled,
                    created_at: provider.created_at,
                    updated_at: provider.updated_at,
                })
                .collect(),
            route_profiles: self.route_profiles.clone(),
            cache: self.cache.clone(),
            agent_injections: self.agent_injections.clone(),
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

        self.updated_at = now;
        Ok(id)
    }
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
        },
        AgentInjectionConfig {
            id: "proxy-mode".to_string(),
            label: "代理模式".to_string(),
            kind: AgentInjectionKind::ProxyMode,
            enabled: false,
            provider_id: None,
            model_id: None,
            target_path: None,
            last_injected_at: None,
            last_status: None,
        },
    ]
}

pub fn normalize_agent_injections(items: &mut Vec<AgentInjectionConfig>) {
    for default_item in default_agent_injections() {
        if !items.iter().any(|item| item.id == default_item.id) {
            items.push(default_item);
        }
    }
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
}
