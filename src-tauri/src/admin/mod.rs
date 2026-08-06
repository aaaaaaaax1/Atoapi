use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tauri::State;

use crate::{
    agent_injection::{
        self, AgentInjectionResult, AgentInjectionRouteUpdate, AgentInjectionUpdate,
    },
    codex_ui_patch,
    config::{
        normalize_upstream_proxy_url, AgentInjectionConfig, AppConfig, CacheConfig, Channel,
        ModelConfig, ProviderCacheCapabilityProbeInput, ProviderCacheCapabilityProbeResult,
        ProviderInput, PublicConfig,
    },
    metrics::MetricsSnapshot,
    metrics_history::{
        MetricsTrendQueryInput, MetricsTrendSnapshot, ReleaseChampionQueryInput,
        ReleaseChampionSnapshot,
    },
    proxy::{
        self,
        cache_validation::{
            CacheValidationControlInput, CacheValidationMode, CacheValidationStatus,
        },
    },
    state::{AppState, ProxyStatus},
};

type CommandResult<T> = Result<T, String>;

const ERROR_BODY_MAX_CHARS: usize = 512;
const PROVIDER_NETWORK_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(10);
const KEY_POOL_HEALTH_CONCURRENCY: usize = 4;
const PROVIDER_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
// A stream health probe only needs enough of the first upstream event to
// establish that the route, Key and selected model were accepted. Some
// Responses-compatible gateways put a very large opaque payload in their
// first `response.created` event, so the old 16 KiB all-body cap falsely
// rejected healthy routes before a usable event could be observed.
const PROVIDER_HEALTH_PROBE_STREAM_SCAN_MAX_BYTES: usize = 128 * 1024;
const PROVIDER_HEALTH_PROBE_JSON_MAX_BYTES: usize = 256 * 1024;
const PROVIDER_BALANCE_PROBE_RESPONSE_MAX_BYTES: usize = 16 * 1024;
const PROVIDER_HEALTH_PROBE_PREVIEW_MAX_CHARS: usize = 160;
const KNOWN_COMPAT_SUFFIXES: &[&str] = &[
    "/api/claudecode",
    "/api/anthropic",
    "/apps/anthropic",
    "/api/coding",
    "/claudecode",
    "/anthropic",
    "/step_plan",
    "/coding",
    "/claude",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneralConfigInput {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub proxy_auto_start: Option<bool>,
    #[serde(default)]
    pub upstream_proxy_url: Option<String>,
    pub local_key: String,
    pub default_channel: Channel,
    pub workspace_fingerprint: String,
    pub cache: Option<CacheConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyModeConfigInput {
    pub host: String,
    pub port: u16,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderModelFetchInput {
    pub provider_id: Option<String>,
    pub name: Option<String>,
    pub base_url: String,
    pub models_url: Option<String>,
    #[serde(default)]
    pub is_full_url: bool,
    pub custom_user_agent: Option<String>,
    pub channel: Channel,
    pub api_key: Option<String>,
    #[serde(default)]
    pub use_system_proxy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyTestInput {
    pub provider_id: Option<String>,
    pub key_id: Option<String>,
    pub api_key: Option<String>,
    pub base_url: String,
    pub models_url: Option<String>,
    #[serde(default)]
    pub is_full_url: bool,
    pub custom_user_agent: Option<String>,
    pub channel: Channel,
    #[serde(default)]
    pub use_system_proxy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderKeyTestResult {
    pub provider_id: Option<String>,
    pub key_id: Option<String>,
    pub ok: bool,
    pub message: String,
    pub models_count: usize,
}

/// Explicit management-only probe variants. These use the standard upstream
/// request shapes without entering the normal relay, routing, or cache path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthProbeMode {
    ResponsesStreaming,
    ChatStreaming,
    ChatJson,
    ResponsesJson,
    AnthropicStreaming,
    AnthropicJson,
}

impl ProviderHealthProbeMode {
    fn is_streaming(&self) -> bool {
        matches!(
            self,
            Self::ResponsesStreaming | Self::ChatStreaming | Self::AnthropicStreaming
        )
    }

    fn endpoint_channel(&self) -> Channel {
        match self {
            Self::ResponsesStreaming | Self::ResponsesJson => Channel::Responses,
            Self::ChatStreaming | Self::ChatJson => Channel::Chat,
            Self::AnthropicStreaming | Self::AnthropicJson => Channel::Anthropic,
        }
    }
}

/// Which saved Key set an explicit management-only health probe may use.
/// `Current` mirrors the next normal relay selection on a configuration clone,
/// so it never advances the live Key-pool cursor or counters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthProbeTarget {
    Current,
    AllEnabled,
    Selected,
}

impl Default for ProviderHealthProbeTarget {
    fn default() -> Self {
        // Preserve the previous command contract for callers that have not yet
        // sent an explicit target. The provider-card UI now always sends
        // `current` instead.
        Self::AllEnabled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealthProbeInput {
    pub provider_id: String,
    /// Used only with `selected`; the explicit target owns empty-list meaning.
    #[serde(default)]
    pub key_ids: Vec<String>,
    #[serde(default)]
    pub target: ProviderHealthProbeTarget,
    pub model: String,
    pub mode: ProviderHealthProbeMode,
    #[serde(default)]
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealthProbeResult {
    pub provider_id: String,
    pub model: String,
    pub mode: ProviderHealthProbeMode,
    pub target: ProviderHealthProbeTarget,
    pub elapsed_ms: u64,
    pub results: Vec<ProviderHealthProbeKeyResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealthProbeKeyResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_response_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_version: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderBalanceProbeResult {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// `false` means none of the bounded built-in protocol profiles exposed a
    /// recognizable balance response. It is never a claim of zero balance.
    pub supported: bool,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
    pub message: String,
}

/// Built-in, read-only balance API shapes. These are protocol profiles, not
/// provider-name branches: the selected base URL determines the candidate
/// route and the JSON schema determines whether it is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderBalanceProbeProfile {
    Sub2Usage,
    NewApiTokenUsage,
}

impl ProviderBalanceProbeProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Sub2Usage => "v1_usage",
            Self::NewApiTokenUsage => "new_api_token_usage",
        }
    }
}

#[derive(Debug, Clone)]
struct ProviderBalanceProbeCandidate {
    profile: ProviderBalanceProbeProfile,
    endpoint: String,
}

/// Result of an editor-triggered connection test. Both direct and system
/// proxy paths are measured against the same draft endpoint and credential;
/// the recommendation only updates the editor until the user saves it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConnectionPathTestResult {
    pub provider_id: Option<String>,
    pub key_id: Option<String>,
    pub ok: bool,
    pub recommended_use_system_proxy: bool,
    pub models_count: usize,
    pub message: String,
    pub paths: Vec<ProviderNetworkPathResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderNetworkPathDiagnosticResult {
    pub provider_id: String,
    pub target_url: String,
    pub paths: Vec<ProviderNetworkPathResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderNetworkPathResult {
    pub path: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

struct ProviderNetworkPathAttempt {
    result: ProviderNetworkPathResult,
    has_valid_model_list: bool,
    models_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProviderCloneInput {
    pub agent_id: String,
    pub provider_id: String,
    #[serde(default)]
    pub model_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProviderOrderInput {
    pub agent_id: String,
    #[serde(default)]
    pub provider_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInput {
    pub provider_id: String,
    pub model: ModelConfig,
}

#[tauri::command]
pub async fn get_config(state: State<'_, Arc<AppState>>) -> CommandResult<PublicConfig> {
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn save_config(
    state: State<'_, Arc<AppState>>,
    input: GeneralConfigInput,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        config.host = input.host;
        config.port = input.port;
        if let Some(proxy_auto_start) = input.proxy_auto_start {
            config.proxy_auto_start = proxy_auto_start;
        }
        if let Some(upstream_proxy_url) = input.upstream_proxy_url {
            config.upstream_proxy_url =
                normalize_upstream_proxy_url(Some(upstream_proxy_url)).map_err(to_command_error)?;
        }
        config.local_key = input.local_key;
        config.default_channel = input.default_channel;
        config.workspace_fingerprint = input.workspace_fingerprint;
        if let Some(cache) = input.cache {
            config.cache = cache;
            config.cache.normalize_fast_forwarding_hit_policy();
        }
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn save_proxy_mode_config(
    state: State<'_, Arc<AppState>>,
    input: ProxyModeConfigInput,
) -> CommandResult<PublicConfig> {
    let was_running = state.proxy_mode_status().await.running;
    if was_running {
        state
            .stop_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    }
    let version = {
        let mut config = state.config.write().await;
        config.proxy_mode_host = input.host.trim().to_string();
        config.proxy_mode_port = input.port;
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    if was_running {
        state
            .start_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    }
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn get_proxy_mode_status(state: State<'_, Arc<AppState>>) -> CommandResult<ProxyStatus> {
    Ok(state.proxy_mode_status().await)
}
#[tauri::command]
pub async fn select_provider(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        let provider = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .ok_or_else(|| format!("provider {provider_id} was not found"))?
            .clone();
        if !provider.enabled {
            return Err(format!("provider {provider_id} is disabled"));
        }
        config.active_provider_id = Some(provider.id.clone());
        config.default_channel = provider.channel.clone();
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn clone_provider_for_agent(
    state: State<'_, Arc<AppState>>,
    input: AgentProviderCloneInput,
) -> CommandResult<PublicConfig> {
    let agent_id = input.agent_id.trim().to_string();
    let source_provider_id = input.provider_id.trim().to_string();
    let (should_start_proxy, version) = {
        let mut config = state.config.write().await;
        let mut staged = config.clone();
        clone_provider_for_agent_config(
            &mut staged,
            &agent_id,
            &source_provider_id,
            input.model_id.as_deref(),
        )
        .map_err(to_command_error)?;
        let agent_index = staged
            .agent_injections
            .iter()
            .position(|agent| agent.id == agent_id)
            .ok_or_else(|| format!("agent injection {agent_id} was not found"))?;
        let now = Utc::now();
        staged.updated_at = now;
        let version = state
            .publish_config_snapshot(&staged)
            .map_err(to_command_error)?;
        let should_start_proxy = staged.agent_injections[agent_index].enabled;
        *config = staged;
        (should_start_proxy, version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;

    if should_start_proxy {
        if agent_id == "proxy-mode" {
            state
                .start_proxy_mode_proxy()
                .await
                .map_err(to_command_error)?;
        } else {
            state.start_proxy().await.map_err(to_command_error)?;
        }
    }
    Ok(state.public_config().await)
}

fn clone_provider_for_agent_config(
    config: &mut AppConfig,
    agent_id: &str,
    source_provider_id: &str,
    requested_model_id: Option<&str>,
) -> anyhow::Result<String> {
    let agent_index = config
        .agent_injections
        .iter()
        .position(|agent| agent.id == agent_id)
        .ok_or_else(|| anyhow::anyhow!("agent injection {agent_id} was not found"))?;
    let source_provider = config
        .providers
        .iter()
        .find(|provider| provider.id == source_provider_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("provider {source_provider_id} was not found"))?;

    let target_provider_id =
        if provider_is_registered_to_agent(config, source_provider_id, agent_id) {
            source_provider_id.to_string()
        } else if let Some(existing) = config.providers.iter().find(|provider| {
            provider_clone_matches_source(&provider.id, source_provider_id, agent_id)
                && provider_is_registered_to_agent(config, &provider.id, agent_id)
        }) {
            existing.id.clone()
        } else {
            let cloned_id = unique_agent_provider_id(config, source_provider_id, agent_id);
            let cloned_name = unique_agent_provider_name(
                config,
                &format!(
                    "{} / {}",
                    source_provider.name, config.agent_injections[agent_index].label
                ),
            );
            let now = Utc::now();
            let mut cloned_provider = source_provider.clone();
            cloned_provider.id = cloned_id.clone();
            cloned_provider.name = cloned_name;
            cloned_provider.created_at = now;
            cloned_provider.updated_at = now;
            config.providers.push(cloned_provider);

            if let Some(mut pool) = config
                .provider_key_pools
                .iter()
                .find(|pool| pool.provider_id == source_provider.id)
                .cloned()
            {
                pool.provider_id = cloned_id.clone();
                pool.updated_at = now;
                config.provider_key_pools.push(pool);
            }
            if let Some(mut compact_mode) = config
                .provider_compact_modes
                .iter()
                .find(|item| item.provider_id == source_provider.id)
                .cloned()
            {
                compact_mode.provider_id = cloned_id.clone();
                compact_mode.updated_at = now;
                config.provider_compact_modes.push(compact_mode);
            }
            if let Some(mut auto_compaction) = config
                .provider_auto_compaction_limits
                .iter()
                .find(|item| item.provider_id == source_provider.id)
                .cloned()
            {
                auto_compaction.provider_id = cloned_id.clone();
                auto_compaction.updated_at = now;
                config.provider_auto_compaction_limits.push(auto_compaction);
            }
            if let Some(mut channel_mode) = config
                .provider_channel_modes
                .iter()
                .find(|item| item.provider_id == source_provider.id)
                .cloned()
            {
                channel_mode.provider_id = cloned_id.clone();
                channel_mode.updated_at = now;
                config.provider_channel_modes.push(channel_mode);
            }
            let cache_capabilities = config
                .provider_cache_capabilities
                .iter()
                .filter(|item| item.provider_id == source_provider.id)
                .cloned()
                .map(|mut item| {
                    item.provider_id = cloned_id.clone();
                    item.updated_at = now;
                    item
                })
                .collect::<Vec<_>>();
            config
                .provider_cache_capabilities
                .extend(cache_capabilities);
            cloned_id
        };

    let target_provider = config
        .providers
        .iter()
        .find(|provider| provider.id == target_provider_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("provider {target_provider_id} was not found"))?;
    let selected_model = requested_model_id
        .filter(|model_id| {
            target_provider
                .models
                .iter()
                .any(|model| model.id == *model_id)
        })
        .map(ToOwned::to_owned);

    {
        let agent = &mut config.agent_injections[agent_index];
        agent
            .hidden_provider_ids
            .retain(|provider_id| provider_id != source_provider_id);
        agent.provider_id = Some(target_provider_id.clone());
        agent.model_id = selected_model;
        agent.last_status = Some("已绑定当前 Agent 独立上游".to_string());
    }
    replace_or_append_agent_provider_order(
        config,
        agent_id,
        source_provider_id,
        &target_provider_id,
    );
    Ok(target_provider_id)
}

fn unique_agent_provider_id(config: &AppConfig, source_id: &str, agent_id: &str) -> String {
    let base = format!(
        "agent-{}-{}",
        sanitize_provider_id_part(agent_id),
        sanitize_provider_id_part(source_id)
    );
    let mut candidate = base.clone();
    let mut index = 2;
    while config
        .providers
        .iter()
        .any(|provider| provider.id == candidate)
    {
        candidate = format!("{base}-{index}");
        index += 1;
    }
    candidate
}

fn unique_agent_provider_name(config: &AppConfig, desired: &str) -> String {
    let base = desired.trim();
    let base = if base.is_empty() {
        "Agent provider"
    } else {
        base
    };
    let mut candidate = base.to_string();
    let mut index = 2;
    while config
        .providers
        .iter()
        .any(|provider| provider.name == candidate)
    {
        candidate = format!("{base} ({index})");
        index += 1;
    }
    candidate
}

fn sanitize_provider_id_part(value: &str) -> String {
    let mut out = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "provider".to_string()
    } else {
        out
    }
}

fn agent_provider_prefix(agent_id: &str) -> String {
    format!("agent-{}-", sanitize_provider_id_part(agent_id))
}

fn provider_belongs_to_agent(provider_id: &str, agent_id: &str) -> bool {
    provider_id.starts_with(&agent_provider_prefix(agent_id))
}

fn provider_is_registered_to_agent(config: &AppConfig, provider_id: &str, agent_id: &str) -> bool {
    config.agent_provider_orders.iter().any(|order| {
        order.agent_id == agent_id
            && order
                .provider_ids
                .iter()
                .any(|registered| registered == provider_id)
    }) || (provider_belongs_to_agent(provider_id, agent_id)
        && config
            .agent_injections
            .iter()
            .any(|agent| agent.id == agent_id && agent.provider_id.as_deref() == Some(provider_id)))
}

fn provider_clone_matches_source(provider_id: &str, source_id: &str, agent_id: &str) -> bool {
    let base = format!(
        "{}{}",
        agent_provider_prefix(agent_id),
        sanitize_provider_id_part(source_id)
    );
    provider_id == base
        || provider_id
            .strip_prefix(&format!("{base}-"))
            .is_some_and(|suffix| suffix.chars().all(|ch| ch.is_ascii_digit()))
}

#[tauri::command]
pub async fn add_or_update_provider(
    state: State<'_, Arc<AppState>>,
    mut input: ProviderInput,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        let owner_agent_id = input
            .owner_agent_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        prepare_agent_owned_provider_input(&mut config, &mut input).map_err(to_command_error)?;
        let id = config.upsert_provider(input).map_err(to_command_error)?;
        if let Some(agent_id) = owner_agent_id.as_deref() {
            append_agent_provider_order(&mut config, agent_id, &id);
        }
        if config.active_provider_id.is_none() {
            config.active_provider_id = Some(id.clone());
        }
        refresh_enabled_injections_for_provider(&mut config, &id).map_err(to_command_error)?;
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

fn prepare_agent_owned_provider_input(
    config: &mut AppConfig,
    input: &mut ProviderInput,
) -> anyhow::Result<()> {
    let Some(agent_id) = input
        .owner_agent_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if !config
        .agent_injections
        .iter()
        .any(|agent| agent.id == agent_id)
    {
        anyhow::bail!("agent injection {agent_id} was not found");
    }

    let requested_id = input
        .id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let provider_id = match requested_id {
        Some(provider_id) if provider_is_registered_to_agent(config, provider_id, agent_id) => {
            provider_id.to_string()
        }
        Some(provider_id) => {
            let selected_model_id = config
                .agent_injections
                .iter()
                .find(|agent| agent.id == agent_id)
                .and_then(|agent| {
                    (agent.provider_id.as_deref() == Some(provider_id))
                        .then(|| agent.model_id.clone())
                        .flatten()
                });
            if selected_model_id.is_none()
                && !config.agent_injections.iter().any(|agent| {
                    agent.id == agent_id && agent.provider_id.as_deref() == Some(provider_id)
                })
            {
                anyhow::bail!("provider {provider_id} is not available to agent {agent_id}");
            }
            clone_provider_for_agent_config(
                config,
                agent_id,
                provider_id,
                selected_model_id.as_deref(),
            )?
        }
        None => unique_agent_provider_id(config, &sanitize_provider_id_part(&input.name), agent_id),
    };
    input.id = Some(provider_id);
    input.owner_agent_id = None;
    Ok(())
}

#[tauri::command]
pub async fn reorder_agent_providers(
    state: State<'_, Arc<AppState>>,
    input: AgentProviderOrderInput,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        reorder_agent_providers_config(&mut config, &input).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

fn reorder_agent_providers_config(
    config: &mut AppConfig,
    input: &AgentProviderOrderInput,
) -> anyhow::Result<()> {
    let agent_id = input.agent_id.trim();
    if agent_id.is_empty() {
        anyhow::bail!("agent id is required");
    }
    let expected = visible_provider_ids_for_agent(config, agent_id)?;
    let received = input
        .provider_ids
        .iter()
        .map(|provider_id| provider_id.trim())
        .collect::<Vec<_>>();
    if received.iter().any(|provider_id| provider_id.is_empty()) {
        anyhow::bail!("provider order cannot contain an empty id");
    }
    let expected_set = expected.iter().map(String::as_str).collect::<HashSet<_>>();
    let received_set = received.iter().copied().collect::<HashSet<_>>();
    if received.len() != expected.len()
        || received_set.len() != received.len()
        || expected_set != received_set
    {
        anyhow::bail!("provider order must contain exactly the current agent providers");
    }

    let provider_ids = received.into_iter().map(ToOwned::to_owned).collect();
    if let Some(order) = config
        .agent_provider_orders
        .iter_mut()
        .find(|order| order.agent_id == agent_id)
    {
        order.provider_ids = provider_ids;
    } else {
        config
            .agent_provider_orders
            .push(crate::config::AgentProviderOrderConfig {
                agent_id: agent_id.to_string(),
                provider_ids,
            });
    }
    Ok(())
}

fn visible_provider_ids_for_agent(
    config: &AppConfig,
    agent_id: &str,
) -> anyhow::Result<Vec<String>> {
    let agent = config
        .agent_injections
        .iter()
        .find(|agent| agent.id == agent_id)
        .ok_or_else(|| anyhow::anyhow!("agent injection {agent_id} was not found"))?;
    Ok(config
        .providers
        .iter()
        .filter(|provider| {
            provider_belongs_to_agent(&provider.id, agent_id)
                || agent.provider_id.as_deref() == Some(provider.id.as_str())
        })
        .map(|provider| provider.id.clone())
        .collect())
}

fn append_agent_provider_order(config: &mut AppConfig, agent_id: &str, provider_id: &str) {
    if let Some(order) = config
        .agent_provider_orders
        .iter_mut()
        .find(|order| order.agent_id == agent_id)
    {
        if !order.provider_ids.iter().any(|id| id == provider_id) {
            order.provider_ids.push(provider_id.to_string());
        }
        return;
    }

    let mut provider_ids = bound_provider_ids_for_agent(config, agent_id);
    if !provider_ids.iter().any(|id| id == provider_id) {
        provider_ids.push(provider_id.to_string());
    }
    config
        .agent_provider_orders
        .push(crate::config::AgentProviderOrderConfig {
            agent_id: agent_id.to_string(),
            provider_ids,
        });
}

fn replace_or_append_agent_provider_order(
    config: &mut AppConfig,
    agent_id: &str,
    source_provider_id: &str,
    target_provider_id: &str,
) {
    if let Some(order) = config
        .agent_provider_orders
        .iter_mut()
        .find(|order| order.agent_id == agent_id)
    {
        if let Some(source_index) = order
            .provider_ids
            .iter()
            .position(|provider_id| provider_id == source_provider_id)
        {
            order.provider_ids[source_index] = target_provider_id.to_string();
        } else if !order
            .provider_ids
            .iter()
            .any(|provider_id| provider_id == target_provider_id)
        {
            order.provider_ids.push(target_provider_id.to_string());
        }
        return;
    }

    let mut provider_ids = bound_provider_ids_for_agent(config, agent_id);
    if !provider_ids.iter().any(|id| id == target_provider_id) {
        provider_ids.push(target_provider_id.to_string());
    }
    config
        .agent_provider_orders
        .push(crate::config::AgentProviderOrderConfig {
            agent_id: agent_id.to_string(),
            provider_ids,
        });
}

fn bound_provider_ids_for_agent(config: &AppConfig, agent_id: &str) -> Vec<String> {
    config
        .agent_injections
        .iter()
        .find(|agent| agent.id == agent_id)
        .and_then(|agent| agent.provider_id.clone())
        .into_iter()
        .collect()
}

#[tauri::command]
pub async fn probe_provider_cache_capabilities(
    state: State<'_, Arc<AppState>>,
    input: ProviderCacheCapabilityProbeInput,
) -> CommandResult<ProviderCacheCapabilityProbeResult> {
    proxy::probe_and_record_provider_cache_capabilities(
        &state,
        input.provider_id.trim(),
        input.model_id.trim(),
        input.channel,
    )
    .await
    .map_err(to_command_error)
}

#[tauri::command]
pub async fn delete_provider(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
    agent_id: Option<String>,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        delete_provider_config(&mut config, &provider_id, agent_id.as_deref())
            .map_err(to_command_error)?;
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

fn delete_provider_config(
    config: &mut AppConfig,
    provider_id: &str,
    agent_id: Option<&str>,
) -> anyhow::Result<()> {
    if !config
        .providers
        .iter()
        .any(|provider| provider.id == provider_id)
    {
        return Err(anyhow::anyhow!("provider {provider_id} was not found"));
    }

    if let Some(agent_id) = agent_id {
        let agent_index = config
            .agent_injections
            .iter()
            .position(|agent| agent.id == agent_id)
            .ok_or_else(|| anyhow::anyhow!("agent injection {agent_id} was not found"))?;

        if !provider_is_registered_to_agent(config, provider_id, agent_id) {
            let agent = &mut config.agent_injections[agent_index];
            hide_provider_for_agent(agent, provider_id);
            if agent.provider_id.as_deref() == Some(provider_id) {
                agent.provider_id = None;
                agent.model_id = None;
                agent.enabled = false;
            }
            agent.last_status =
                Some("已从当前 Agent 移除共享上游，其他 Agent 不受影响".to_string());
            return Ok(());
        }

        if config.active_provider_id.as_deref() == Some(provider_id)
            || config
                .route_profiles
                .iter()
                .any(|profile| profile.provider_id.as_deref() == Some(provider_id))
        {
            return Err(anyhow::anyhow!(
                "provider {provider_id} is still referenced by a global route"
            ));
        }

        let other_agent_ids = config
            .agent_injections
            .iter()
            .filter(|agent| {
                agent.id != agent_id && agent.provider_id.as_deref() == Some(provider_id)
            })
            .map(|agent| agent.id.clone())
            .collect::<Vec<_>>();
        for other_agent_id in other_agent_ids {
            let other_model_id = config
                .agent_injections
                .iter()
                .find(|agent| agent.id == other_agent_id)
                .and_then(|agent| agent.model_id.clone());
            clone_provider_for_agent_config(
                config,
                &other_agent_id,
                provider_id,
                other_model_id.as_deref(),
            )?;
        }

        let source_provider_id = config
            .providers
            .iter()
            .filter(|provider| !provider.id.starts_with("agent-"))
            .find(|provider| provider_clone_matches_source(provider_id, &provider.id, agent_id))
            .map(|provider| provider.id.clone());
        remove_provider_records(config, provider_id);
        let agent = &mut config.agent_injections[agent_index];
        if let Some(source_provider_id) = source_provider_id.as_deref() {
            hide_provider_for_agent(agent, source_provider_id);
        }
        if agent.provider_id.as_deref() == Some(provider_id) {
            agent.provider_id = None;
            agent.model_id = None;
            agent.enabled = false;
            agent.last_status =
                Some("已删除当前 Agent 的独立上游，其他 Agent 不受影响".to_string());
        }
        return Ok(());
    }

    let referenced_by_agents = config
        .agent_injections
        .iter()
        .any(|agent| agent.provider_id.as_deref() == Some(provider_id));
    let referenced_by_global_route = config.active_provider_id.as_deref() == Some(provider_id)
        || config
            .route_profiles
            .iter()
            .any(|profile| profile.provider_id.as_deref() == Some(provider_id));
    if referenced_by_agents || referenced_by_global_route {
        return Err(anyhow::anyhow!(
            "provider {provider_id} is still referenced; remove it from the Agent or global route first"
        ));
    }
    remove_provider_records(config, provider_id);
    Ok(())
}

fn remove_provider_records(config: &mut AppConfig, provider_id: &str) {
    config
        .providers
        .retain(|provider| provider.id != provider_id);
    config
        .provider_key_pools
        .retain(|pool| pool.provider_id != provider_id);
    config
        .provider_compact_modes
        .retain(|item| item.provider_id != provider_id);
    config
        .provider_auto_compaction_limits
        .retain(|item| item.provider_id != provider_id);
    config
        .provider_channel_modes
        .retain(|item| item.provider_id != provider_id);
    config
        .provider_cache_capabilities
        .retain(|item| item.provider_id != provider_id);
    for agent in &mut config.agent_injections {
        agent
            .hidden_provider_ids
            .retain(|hidden_id| hidden_id != provider_id);
    }
    for order in &mut config.agent_provider_orders {
        order.provider_ids.retain(|id| id != provider_id);
    }
    config
        .agent_provider_orders
        .retain(|order| !order.provider_ids.is_empty());
}

fn hide_provider_for_agent(agent: &mut AgentInjectionConfig, provider_id: &str) {
    if !agent
        .hidden_provider_ids
        .iter()
        .any(|hidden_id| hidden_id == provider_id)
    {
        agent.hidden_provider_ids.push(provider_id.to_string());
    }
}

#[tauri::command]
pub async fn test_provider_key(
    state: State<'_, Arc<AppState>>,
    input: ProviderKeyTestInput,
) -> CommandResult<ProviderKeyTestResult> {
    let result = match test_provider_key_inner(state.inner(), &input).await {
        Ok(result) => result,
        Err(err) => failed_provider_key_test(&input, err),
    };
    persist_provider_key_test_result(state.inner(), &input, &result)
        .await
        .map_err(to_command_error)?;
    Ok(result)
}

/// Tests the exact Key that the next ordinary inbound for this provider would
/// use. An enabled key pool is authoritative: this command never falls back
/// to the legacy connection-info Key when the pool has no usable Key.
#[tauri::command]
pub async fn test_active_provider_key(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<ProviderKeyTestResult> {
    let input = active_provider_key_test_input(state.inner(), provider_id.trim())
        .await
        .map_err(to_command_error)?;
    let result = match test_provider_key_inner(state.inner(), &input).await {
        Ok(result) => result,
        Err(err) => failed_provider_key_test(&input, err),
    };
    persist_provider_key_test_result(state.inner(), &input, &result)
        .await
        .map_err(to_command_error)?;
    Ok(result)
}

async fn persist_provider_key_test_result(
    state: &AppState,
    input: &ProviderKeyTestInput,
    result: &ProviderKeyTestResult,
) -> Result<()> {
    let (Some(provider_id), Some(key_id)) = (input.provider_id.as_deref(), input.key_id.as_deref())
    else {
        return Ok(());
    };
    let version = {
        let mut config = state.config.write().await;
        if result.ok {
            config.mark_provider_key_success(provider_id, Some(key_id));
        } else {
            config.mark_provider_key_failure(provider_id, Some(key_id), &result.message, true);
        }
        state.publish_config_snapshot(&config)?
    };
    state.wait_for_config_snapshot(version).await?;
    Ok(())
}

async fn active_provider_key_test_input(
    state: &AppState,
    provider_id: &str,
) -> Result<ProviderKeyTestInput> {
    let (input, version) = {
        let mut config = state.config.write().await;
        let input = active_provider_key_test_input_from_config(&mut config, provider_id)?;
        let version = state.publish_config_snapshot(&config)?;
        (input, version)
    };
    state.wait_for_config_snapshot(version).await?;
    Ok(input)
}

fn active_provider_key_test_input_from_config(
    config: &mut AppConfig,
    provider_id: &str,
) -> Result<ProviderKeyTestInput> {
    let provider_id = provider_id.trim();
    if provider_id.is_empty() {
        return Err(anyhow!("provider id is required"));
    }
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .cloned()
        .ok_or_else(|| anyhow!("provider {provider_id} was not found"))?;
    if !provider.enabled {
        return Err(anyhow!("provider {} is disabled", provider.name));
    }
    let selected = config
        .select_provider_key_for_request(provider_id, None, None)?
        .ok_or_else(|| anyhow!("provider API key is not configured"))?;
    Ok(ProviderKeyTestInput {
        provider_id: Some(provider_id.to_string()),
        key_id: selected.key_id,
        // Pass the freshly selected secret directly, so the test cannot
        // resolve a stale connection-info Key after the pool has advanced.
        api_key: Some(selected.secret),
        base_url: provider.base_url,
        models_url: provider.models_url,
        is_full_url: provider.is_full_url,
        custom_user_agent: provider.custom_user_agent,
        channel: provider.channel,
        use_system_proxy: provider.use_system_proxy,
    })
}

/// Tests the editable connection over both direct and system-proxy paths.
/// This command intentionally has no config side effects: the UI applies the
/// fastest successful recommendation to its unsaved draft only.
#[tauri::command]
pub async fn test_provider_connection_paths(
    state: State<'_, Arc<AppState>>,
    input: ProviderKeyTestInput,
) -> CommandResult<ProviderConnectionPathTestResult> {
    test_provider_connection_paths_inner(state.inner(), &input)
        .await
        .map_err(to_command_error)
}

#[tauri::command]
pub async fn test_provider_key_pool(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<Vec<ProviderKeyTestResult>> {
    test_provider_key_pool_inner(state.inner(), &provider_id)
        .await
        .map_err(to_command_error)
}

/// Fetches models with the exact non-mutating Key selection used by the health
/// modal. It intentionally does not fall back to the legacy connection-info
/// Key when a Key pool is enabled.
#[tauri::command]
pub async fn fetch_provider_health_models(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<Vec<ModelConfig>> {
    let input = health_probe_current_key_input_for_provider(state.inner(), provider_id.trim())
        .await
        .map_err(to_command_error)?;
    let secret = input
        .api_key
        .as_deref()
        .ok_or_else(|| "provider API key is not configured".to_string())?;
    let client = state
        .control_plane_upstream_client(input.use_system_proxy)
        .await
        .map_err(to_command_error)?;
    fetch_models_from_upstream_with_options(
        &client,
        &input.base_url,
        input.channel.clone(),
        Some(secret),
        input.is_full_url,
        input.models_url.as_deref(),
        input.custom_user_agent.as_deref(),
    )
    .await
    .map_err(to_command_error)
}

/// Sends one explicit, bounded management probe per selected Key. These calls
/// never enter normal relay routing and never mutate pool cursors, counters,
/// health state, proxy settings, or cache state.
#[tauri::command]
pub async fn probe_provider_health(
    state: State<'_, Arc<AppState>>,
    input: ProviderHealthProbeInput,
) -> CommandResult<ProviderHealthProbeResult> {
    probe_provider_health_inner(state.inner(), input)
        .await
        .map_err(to_command_error)
}

async fn probe_provider_health_inner(
    state: &AppState,
    input: ProviderHealthProbeInput,
) -> Result<ProviderHealthProbeResult> {
    let provider_id = input.provider_id.trim().to_string();
    let model = input.model.trim().to_string();
    if provider_id.is_empty() {
        return Err(anyhow!("provider id is required"));
    }
    if model.is_empty() {
        return Err(anyhow!("请选择要测活的模型"));
    }
    let prompt = normalize_health_probe_prompt(input.prompt)?;
    let target = input.target.clone();
    let key_inputs =
        health_probe_inputs_for_target(state, &provider_id, &target, &input.key_ids).await?;
    let started = Instant::now();
    let mode = input.mode.clone();
    let mut outcomes = stream::iter(key_inputs.into_iter().enumerate().map(
        |(index, key_input)| {
            let state = state;
            let model = model.clone();
            let prompt = prompt.clone();
            let mode = mode.clone();
            async move {
                let result =
                    probe_provider_health_key(&state, key_input, &model, &mode, &prompt).await;
                (index, result)
            }
        },
    ))
    .buffer_unordered(KEY_POOL_HEALTH_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    outcomes.sort_by_key(|(index, _)| *index);
    Ok(ProviderHealthProbeResult {
        provider_id,
        model,
        mode,
        target,
        elapsed_ms: started.elapsed().as_millis() as u64,
        results: outcomes.into_iter().map(|(_, result)| result).collect(),
    })
}

/// Balance probing is capability-driven: the request is sent only to a URL the
/// user explicitly configured for this provider. Unsupported endpoints never
/// alter Key state and are never treated as a zero balance.
#[tauri::command]
pub async fn probe_provider_balance(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<ProviderBalanceProbeResult> {
    probe_provider_balance_inner(state.inner(), provider_id.trim())
        .await
        .map_err(to_command_error)
}

async fn test_provider_key_pool_inner(
    state: &AppState,
    provider_id: &str,
) -> Result<Vec<ProviderKeyTestResult>> {
    let (provider, key_ids) = {
        let config = state.config.read().await;
        let provider = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .cloned()
            .ok_or_else(|| anyhow!("provider {provider_id} was not found"))?;
        let key_ids = config
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .map(|pool| {
                pool.keys
                    .iter()
                    .filter(|key| key.enabled && key.key_encrypted.is_some())
                    .map(|key| key.id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        (provider, key_ids)
    };
    let inputs = key_ids
        .into_iter()
        .map(|key_id| {
            let input = ProviderKeyTestInput {
                provider_id: Some(provider_id.to_string()),
                key_id: Some(key_id.clone()),
                api_key: None,
                base_url: provider.base_url.clone(),
                models_url: provider.models_url.clone(),
                is_full_url: provider.is_full_url,
                custom_user_agent: provider.custom_user_agent.clone(),
                channel: provider.channel.clone(),
                use_system_proxy: provider.use_system_proxy,
            };
            (key_id, input)
        })
        .collect::<Vec<_>>();

    let mut outcomes = stream::iter(inputs.into_iter().enumerate().map(
        |(index, (key_id, input))| async move {
            let result = match test_provider_key_inner(state, &input).await {
                Ok(result) => result,
                Err(err) => failed_provider_key_test(&input, err),
            };
            (index, key_id, result)
        },
    ))
    .buffer_unordered(KEY_POOL_HEALTH_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    outcomes.sort_by_key(|(index, _, _)| *index);
    let outcomes = outcomes
        .into_iter()
        .map(|(_, key_id, result)| (key_id, result))
        .collect::<Vec<_>>();

    if outcomes.is_empty() {
        return Err(anyhow!(
            "provider key pool has no enabled saved key to test"
        ));
    }

    // Health checks run independently, but their resulting state is one
    // coherent config update. This avoids a disk write per key and prevents
    // another request from observing a partially applied batch.
    let version = {
        let mut config = state.config.write().await;
        for (key_id, result) in &outcomes {
            if result.ok {
                config.mark_provider_key_success(provider_id, Some(key_id));
            } else {
                config.mark_provider_key_failure(provider_id, Some(key_id), &result.message, true);
            }
        }
        state.publish_config_snapshot(&config)?
    };
    state.wait_for_config_snapshot(version).await?;

    Ok(outcomes.into_iter().map(|(_, result)| result).collect())
}

async fn health_probe_current_key_input_for_provider(
    state: &AppState,
    provider_id: &str,
) -> Result<ProviderKeyTestInput> {
    let config = state.config.read().await;
    health_probe_current_key_input_from_config(&config, provider_id)
}

fn health_probe_current_key_input_from_config(
    config: &AppConfig,
    provider_id: &str,
) -> Result<ProviderKeyTestInput> {
    let provider_id = provider_id.trim();
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .cloned()
        .ok_or_else(|| anyhow!("provider {provider_id} was not found"))?;
    if !provider.enabled {
        return Err(anyhow!("provider {} is disabled", provider.name));
    }
    // Resolve on a clone so opening the health modal cannot advance a
    // sequential/round-robin Key pool or add ordinary request counters.
    connection_path_test_input_from_config(
        config,
        &ProviderKeyTestInput {
            provider_id: Some(provider_id.to_string()),
            key_id: None,
            api_key: None,
            base_url: provider.base_url,
            models_url: provider.models_url,
            is_full_url: provider.is_full_url,
            custom_user_agent: provider.custom_user_agent,
            channel: provider.channel,
            use_system_proxy: provider.use_system_proxy,
        },
    )
}

async fn health_probe_inputs_for_provider(
    state: &AppState,
    provider_id: &str,
    key_ids: &[String],
) -> Result<Vec<ProviderKeyTestInput>> {
    let config = state.config.read().await;
    health_probe_inputs_from_config(&config, provider_id, key_ids)
}

async fn health_probe_inputs_for_target(
    state: &AppState,
    provider_id: &str,
    target: &ProviderHealthProbeTarget,
    key_ids: &[String],
) -> Result<Vec<ProviderKeyTestInput>> {
    match target {
        ProviderHealthProbeTarget::Current => Ok(vec![
            health_probe_current_key_input_for_provider(state, provider_id).await?,
        ]),
        ProviderHealthProbeTarget::AllEnabled => {
            health_probe_inputs_for_provider(state, provider_id, &[]).await
        }
        ProviderHealthProbeTarget::Selected => {
            if key_ids.iter().all(|key_id| key_id.trim().is_empty()) {
                return Err(anyhow!("select at least one saved Key to probe"));
            }
            health_probe_inputs_for_provider(state, provider_id, key_ids).await
        }
    }
}

fn health_probe_inputs_from_config(
    config: &AppConfig,
    provider_id: &str,
    key_ids: &[String],
) -> Result<Vec<ProviderKeyTestInput>> {
    let provider_id = provider_id.trim();
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .cloned()
        .ok_or_else(|| anyhow!("provider {provider_id} was not found"))?;
    if !provider.enabled {
        return Err(anyhow!("provider {} is disabled", provider.name));
    }
    let requested = key_ids
        .iter()
        .map(|key_id| key_id.trim())
        .filter(|key_id| !key_id.is_empty())
        .collect::<HashSet<_>>();
    let now = Utc::now();
    if let Some(pool) = config
        .provider_key_pools
        .iter()
        .find(|pool| pool.provider_id == provider_id && pool.enabled)
    {
        let candidates = pool
            .keys
            .iter()
            .filter(|key| {
                key.enabled
                    && key.key_encrypted.is_some()
                    && key.disabled_until.map(|until| until <= now).unwrap_or(true)
            })
            .filter(|key| requested.is_empty() || requested.contains(key.id.as_str()))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(anyhow!(
                "provider key pool has no enabled saved key to probe"
            ));
        }
        if !requested.is_empty() && candidates.len() != requested.len() {
            return Err(anyhow!(
                "one or more selected Keys are disabled, cooling down, or unavailable"
            ));
        }
        return candidates
            .into_iter()
            .map(|key| {
                let secret = config
                    .provider_key_secret(provider_id, &key.id)?
                    .ok_or_else(|| anyhow!("selected provider Key is empty"))?;
                Ok(ProviderKeyTestInput {
                    provider_id: Some(provider_id.to_string()),
                    key_id: Some(key.id.clone()),
                    api_key: Some(secret),
                    base_url: provider.base_url.clone(),
                    models_url: provider.models_url.clone(),
                    is_full_url: provider.is_full_url,
                    custom_user_agent: provider.custom_user_agent.clone(),
                    channel: provider.channel.clone(),
                    use_system_proxy: provider.use_system_proxy,
                })
            })
            .collect();
    }
    if !requested.is_empty() {
        return Err(anyhow!("this provider does not have an enabled Key pool"));
    }
    let secret = config
        .provider_api_key(provider_id)?
        .ok_or_else(|| anyhow!("provider API key is not configured"))?;
    Ok(vec![ProviderKeyTestInput {
        provider_id: Some(provider_id.to_string()),
        key_id: None,
        api_key: Some(secret),
        base_url: provider.base_url,
        models_url: provider.models_url,
        is_full_url: provider.is_full_url,
        custom_user_agent: provider.custom_user_agent,
        channel: provider.channel,
        use_system_proxy: provider.use_system_proxy,
    }])
}

fn normalize_health_probe_prompt(prompt: Option<String>) -> Result<String> {
    let prompt = prompt.unwrap_or_else(|| "hi".to_string());
    let prompt = if prompt.trim().is_empty() {
        "hi".to_string()
    } else {
        prompt
    };
    if prompt.chars().count() > 2_000 {
        return Err(anyhow!("测活内容最多 2000 个字符"));
    }
    Ok(prompt)
}

async fn probe_provider_health_key(
    state: &AppState,
    input: ProviderKeyTestInput,
    model: &str,
    mode: &ProviderHealthProbeMode,
    prompt: &str,
) -> ProviderHealthProbeKeyResult {
    let started = Instant::now();
    let key_id = input.key_id.clone();
    let Some(secret) = input.api_key.as_deref() else {
        return failed_health_probe_result(key_id, started, "key_missing");
    };
    let endpoint = health_probe_endpoint_url(&input.base_url, &mode.endpoint_channel());
    let Ok(endpoint) = endpoint else {
        return failed_health_probe_result(key_id, started, "invalid_endpoint");
    };
    let client = match state
        .control_plane_upstream_client(input.use_system_proxy)
        .await
    {
        Ok(client) => client,
        Err(_) => return failed_health_probe_result(key_id, started, "client_unavailable"),
    };
    let request_body = health_probe_request_body(mode, model, prompt);
    // A management probe must negotiate the same upstream protocol identity
    // as a normal relay request. In particular, several compatible gateways
    // require `x-api-key`, SSE `Accept`, the stable User-Agent, or identity
    // encoding before they emit their normal terminal frames.
    let request = client
        .post(endpoint)
        .headers(proxy::build_management_probe_headers(
            secret,
            &input.channel,
            mode.is_streaming(),
            input.custom_user_agent.as_deref(),
        ))
        .json(&request_body);
    let response = match tokio::time::timeout(PROVIDER_HEALTH_PROBE_TIMEOUT, request.send()).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => return failed_health_probe_result(key_id, started, "transport_error"),
        Err(_) => return failed_health_probe_result(key_id, started, "request_timeout"),
    };
    let status = response.status();
    let http_version = Some(format!("{:?}", response.version()));
    if !status.is_success() {
        return ProviderHealthProbeKeyResult {
            key_id,
            ok: false,
            status: Some(status.as_u16()),
            elapsed_ms: started.elapsed().as_millis() as u64,
            first_response_ms: None,
            http_version,
            message: format!("HTTP {}", status.as_u16()),
            response_preview: None,
        };
    }
    match read_health_probe_response(response, mode, started).await {
        Ok(observation) => ProviderHealthProbeKeyResult {
            key_id,
            ok: true,
            status: Some(status.as_u16()),
            elapsed_ms: started.elapsed().as_millis() as u64,
            first_response_ms: observation.first_response_ms,
            http_version,
            message: if mode.is_streaming() {
                "stream_accepted".to_string()
            } else {
                "response_received".to_string()
            },
            response_preview: observation.response_preview,
        },
        Err(code) => ProviderHealthProbeKeyResult {
            key_id,
            ok: false,
            status: Some(status.as_u16()),
            elapsed_ms: started.elapsed().as_millis() as u64,
            first_response_ms: None,
            http_version,
            message: code.to_string(),
            response_preview: None,
        },
    }
}

fn failed_health_probe_result(
    key_id: Option<String>,
    started: Instant,
    message: &str,
) -> ProviderHealthProbeKeyResult {
    ProviderHealthProbeKeyResult {
        key_id,
        ok: false,
        status: None,
        elapsed_ms: started.elapsed().as_millis() as u64,
        first_response_ms: None,
        http_version: None,
        message: message.to_string(),
        response_preview: None,
    }
}

fn health_probe_endpoint_url(base_url: &str, channel: &Channel) -> Result<String> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(anyhow!("base URL is empty"));
    }
    reqwest::Url::parse(trimmed)?;
    let endpoint = channel.endpoint_path();
    if trimmed.ends_with(endpoint)
        || trimmed.ends_with("/chat/completions")
        || trimmed.ends_with("/responses")
        || trimmed.ends_with("/messages")
    {
        Ok(trimmed.to_string())
    } else if trimmed.ends_with("/v1") {
        Ok(format!("{trimmed}{endpoint}"))
    } else {
        Ok(format!("{trimmed}/v1{endpoint}"))
    }
}

fn health_probe_request_body(mode: &ProviderHealthProbeMode, model: &str, prompt: &str) -> Value {
    match mode {
        ProviderHealthProbeMode::ResponsesStreaming => json!({
            "model": model,
            // Match the Codex/New API Responses request shape: a user input
            // item rather than the legacy shorthand string.
            "input": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_output_tokens": 1,
        }),
        ProviderHealthProbeMode::ResponsesJson => json!({
            "model": model,
            "input": [{ "role": "user", "content": prompt }],
            "stream": false,
            "max_output_tokens": 1,
        }),
        ProviderHealthProbeMode::ChatStreaming => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::ChatJson => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": false,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::AnthropicStreaming => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::AnthropicJson => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": false,
            "max_tokens": 1,
        }),
    }
}

struct HealthProbeResponseObservation {
    first_response_ms: Option<u64>,
    response_preview: Option<String>,
}

async fn read_health_probe_response(
    response: reqwest::Response,
    mode: &ProviderHealthProbeMode,
    started: Instant,
) -> Result<HealthProbeResponseObservation> {
    let mode = mode.clone();
    tokio::time::timeout(PROVIDER_HEALTH_PROBE_TIMEOUT, async move {
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        let mut first_response_ms = None;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("response_read_error"))?;
            if first_response_ms.is_none() && !chunk.is_empty() {
                first_response_ms = Some(started.elapsed().as_millis() as u64);
            }
            let max_bytes = if mode.is_streaming() {
                PROVIDER_HEALTH_PROBE_STREAM_SCAN_MAX_BYTES
            } else {
                PROVIDER_HEALTH_PROBE_JSON_MAX_BYTES
            };
            let remaining = max_bytes.saturating_sub(body.len());
            if remaining == 0 {
                return Err(anyhow!("response_too_large"));
            }
            let accepted = chunk.len().min(remaining);
            body.extend_from_slice(&chunk[..accepted]);
            let text = String::from_utf8_lossy(&body);
            if health_probe_stream_failed(&text) {
                return Err(anyhow!("upstream_stream_error"));
            }
            if mode.is_streaming() && health_probe_stream_accepted(&text, &mode) {
                return Ok(HealthProbeResponseObservation {
                    first_response_ms,
                    response_preview: health_probe_preview(&text),
                });
            }
            if chunk.len() > accepted {
                return Err(anyhow!("response_too_large"));
            }
        }
        let text = String::from_utf8_lossy(&body);
        if mode.is_streaming() {
            if health_probe_stream_failed(&text) {
                return Err(anyhow!("upstream_stream_error"));
            }
            if !health_probe_stream_completed(&text, &mode) {
                return Err(anyhow!("stream_incomplete"));
            }
        } else if text.trim().is_empty() || health_probe_json_failed(&text) {
            return Err(anyhow!("invalid_response"));
        }
        Ok(HealthProbeResponseObservation {
            first_response_ms,
            response_preview: health_probe_preview(&text),
        })
    })
    .await
    .map_err(|_| anyhow!("response_timeout"))?
}

fn health_probe_stream_completed(body: &str, mode: &ProviderHealthProbeMode) -> bool {
    match mode {
        ProviderHealthProbeMode::ResponsesStreaming => health_probe_sse_values(body).any(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "response.completed")
                && !health_probe_value_failed(&value)
        }),
        ProviderHealthProbeMode::ChatStreaming => health_probe_sse_lines(body).any(|data| {
            data == "[DONE]"
                || serde_json::from_str::<Value>(data)
                    .ok()
                    .is_some_and(|value| {
                        !health_probe_value_failed(&value)
                            && health_probe_chat_finish_reason(&value)
                    })
        }),
        ProviderHealthProbeMode::AnthropicStreaming => health_probe_sse_values(body).any(|value| {
            !health_probe_value_failed(&value)
                && value
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| {
                        kind == "message_stop"
                            || (kind == "message_delta"
                                && value
                                    .pointer("/delta/stop_reason")
                                    .and_then(Value::as_str)
                                    .is_some_and(|reason| !reason.trim().is_empty()))
                    })
        }),
        _ => false,
    }
}

/// Mirrors the practical New API/Sub2-style channel-test contract: a stream
/// is live once it yields a recognizable, non-error event. We deliberately do
/// not wait for a huge terminal frame here: this is a management-only
/// reachability probe, while normal relay traffic retains its strict terminal
/// handling and error propagation.
fn health_probe_stream_accepted(body: &str, mode: &ProviderHealthProbeMode) -> bool {
    if health_probe_stream_completed(body, mode) {
        return true;
    }
    let Some(data) = health_probe_sse_lines(body).next() else {
        return false;
    };
    if data == "[DONE]" {
        return matches!(mode, ProviderHealthProbeMode::ChatStreaming);
    }
    let normalized = data.trim();
    if normalized.is_empty() || !normalized.starts_with('{') {
        return false;
    }
    if let Ok(value) = serde_json::from_str::<Value>(normalized) {
        if health_probe_value_failed(&value) {
            return false;
        }
        return match mode {
            ProviderHealthProbeMode::ResponsesStreaming => value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.starts_with("response.")),
            ProviderHealthProbeMode::ChatStreaming => {
                value.get("choices").is_some() || value.get("id").is_some()
            }
            ProviderHealthProbeMode::AnthropicStreaming => value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| {
                    kind.starts_with("message_") || kind.starts_with("content_block_")
                }),
            _ => false,
        };
    }
    // A few compatible Responses gateways emit a single very large
    // `response.created` JSON line. Its initial ASCII fields are enough to
    // prove the stream is accepted; explicit error prefixes remain failures.
    let compact = normalized.split_whitespace().collect::<String>();
    if compact.contains("\"type\":\"error\"")
        || compact.contains("\"type\":\"response.failed\"")
        || compact.contains("\"error\":{")
    {
        return false;
    }
    match mode {
        ProviderHealthProbeMode::ResponsesStreaming => compact.contains("\"type\":\"response."),
        ProviderHealthProbeMode::ChatStreaming => {
            compact.contains("\"choices\"") || compact.contains("\"id\"")
        }
        ProviderHealthProbeMode::AnthropicStreaming => {
            compact.contains("\"type\":\"message_") || compact.contains("\"type\":\"content_block_")
        }
        _ => false,
    }
}

fn health_probe_stream_failed(body: &str) -> bool {
    health_probe_sse_values(body).any(|value| health_probe_value_failed(&value))
}

fn health_probe_json_failed(body: &str) -> bool {
    match serde_json::from_str::<Value>(body) {
        Ok(value) => health_probe_value_failed(&value),
        Err(_) => true,
    }
}

fn health_probe_sse_lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("data:").map(str::trim))
        .filter(|data| !data.is_empty())
}

fn health_probe_sse_values(body: &str) -> impl Iterator<Item = Value> + '_ {
    health_probe_sse_lines(body).filter_map(|data| serde_json::from_str::<Value>(data).ok())
}

/// Upstreams commonly include `"error": null` on a successful terminal
/// response. Only a non-null error object, an explicit error event, or an
/// explicit failed status is a probe failure.
fn health_probe_value_failed(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "error" | "response.failed"))
        || value.get("error").is_some_and(|error| !error.is_null())
        || value
            .pointer("/response/error")
            .is_some_and(|error| !error.is_null())
        || value
            .pointer("/response/status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "failed" | "cancelled" | "incomplete"))
}

fn health_probe_chat_finish_reason(value: &Value) -> bool {
    value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| !reason.trim().is_empty())
        || value
            .get("finish_reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| !reason.trim().is_empty())
}

fn health_probe_preview(body: &str) -> Option<String> {
    let mut preview = body
        .lines()
        .find_map(|line| line.trim().strip_prefix("data:").map(str::trim))
        .and_then(|data| serde_json::from_str::<Value>(data).ok())
        .and_then(|value| health_probe_text_from_value(&value))
        .or_else(|| {
            serde_json::from_str::<Value>(body)
                .ok()
                .and_then(|value| health_probe_text_from_value(&value))
        });
    if preview.is_none() {
        preview = body
            .lines()
            .map(str::trim)
            .find(|line| {
                !line.is_empty() && !line.starts_with("event:") && !line.starts_with("data:")
            })
            .map(ToOwned::to_owned);
    }
    preview.and_then(|text| redact_health_probe_preview(&text))
}

fn health_probe_text_from_value(value: &Value) -> Option<String> {
    for pointer in [
        "/response/output/0/content/0/text",
        "/output/0/content/0/text",
        "/output_text",
        "/choices/0/message/content",
        "/choices/0/delta/content",
        "/delta/text",
        "/content/0/text",
        "/content",
    ] {
        if let Some(text) = value.pointer(pointer).and_then(Value::as_str) {
            return Some(text.to_string());
        }
    }
    None
}

fn redact_health_probe_preview(text: &str) -> Option<String> {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut output = String::new();
    for ch in trimmed
        .chars()
        .take(PROVIDER_HEALTH_PROBE_PREVIEW_MAX_CHARS)
    {
        output.push(ch);
    }
    if trimmed.chars().count() > PROVIDER_HEALTH_PROBE_PREVIEW_MAX_CHARS {
        output.push('…');
    }
    Some(output)
}

async fn probe_provider_balance_inner(
    state: &AppState,
    provider_id: &str,
) -> Result<ProviderBalanceProbeResult> {
    let provider_id = provider_id.trim();
    if provider_id.is_empty() {
        return Err(anyhow!("provider id is required"));
    }
    let current_key_input = {
        let config = state.config.read().await;
        health_probe_current_key_input_from_config(&config, provider_id)?
    };
    let started = Instant::now();
    let secret = current_key_input
        .api_key
        .as_deref()
        .ok_or_else(|| anyhow!("provider API key is not configured"))?;
    let client = state
        .control_plane_upstream_client(current_key_input.use_system_proxy)
        .await?;
    let candidates = balance_probe_candidates(&current_key_input.base_url)?;
    let mut last_status = None;
    for candidate in candidates {
        let mut headers = proxy::build_management_probe_headers(
            secret,
            &current_key_input.channel,
            false,
            current_key_input.custom_user_agent.as_deref(),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let request = client.get(&candidate.endpoint).headers(headers);
        let response =
            match tokio::time::timeout(PROVIDER_HEALTH_PROBE_TIMEOUT, request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    return Ok(balance_probe_failure(
                        provider_id,
                        current_key_input.key_id,
                        None,
                        started,
                        "transport_error",
                        false,
                    ));
                }
                Err(_) => {
                    return Ok(balance_probe_failure(
                        provider_id,
                        current_key_input.key_id,
                        None,
                        started,
                        "request_timeout",
                        false,
                    ));
                }
            };
        let status = response.status();
        last_status = Some(status.as_u16());
        if !status.is_success() {
            // A missing endpoint is the expected way to reject a generic
            // profile. Permission, rate-limit, and server failures are real
            // probe failures; do not create extra requests by guessing again.
            if status.as_u16() == 404 {
                continue;
            }
            return Ok(balance_probe_failure(
                provider_id,
                current_key_input.key_id,
                Some(status.as_u16()),
                started,
                &format!("HTTP {}", status.as_u16()),
                true,
            ));
        }
        let body = read_limited_probe_body(response).await?;
        let Some(value) = serde_json::from_slice::<Value>(&body).ok() else {
            // A 200 HTML fallback is not a balance response. Try the next
            // documented generic profile without recording the raw body.
            continue;
        };
        if balance_response_failed(&value) {
            return Ok(balance_probe_failure(
                provider_id,
                current_key_input.key_id,
                Some(status.as_u16()),
                started,
                "upstream_balance_error",
                true,
            ));
        }
        if let Some(balance) = balance_display_for_profile(&value, candidate.profile) {
            return Ok(ProviderBalanceProbeResult {
                provider_id: provider_id.to_string(),
                key_id: current_key_input.key_id,
                supported: true,
                ok: true,
                status: Some(status.as_u16()),
                elapsed_ms: started.elapsed().as_millis() as u64,
                balance: Some(balance),
                message: format!("balance_received:{}", candidate.profile.label()),
            });
        }
    }
    Ok(balance_probe_failure(
        provider_id,
        current_key_input.key_id,
        last_status,
        started,
        "balance_api_not_detected",
        false,
    ))
}

fn balance_probe_failure(
    provider_id: &str,
    key_id: Option<String>,
    status: Option<u16>,
    started: Instant,
    message: &str,
    supported: bool,
) -> ProviderBalanceProbeResult {
    ProviderBalanceProbeResult {
        provider_id: provider_id.to_string(),
        key_id,
        supported,
        ok: false,
        status,
        elapsed_ms: started.elapsed().as_millis() as u64,
        balance: None,
        message: message.to_string(),
    }
}

fn balance_probe_candidates(base_url: &str) -> Result<Vec<ProviderBalanceProbeCandidate>> {
    let api_root = balance_probe_api_root(base_url)?;
    let mut origin = api_root.clone();
    origin.set_path("/");
    origin.set_query(None);
    origin.set_fragment(None);
    let candidates = [
        (
            ProviderBalanceProbeProfile::Sub2Usage,
            api_root.join("usage")?,
        ),
        // New API deployments are split on whether their router preserves the
        // trailing slash. Try the documented slash form first; the no-slash
        // variant remains a bounded compatibility fallback for other hosts.
        (
            ProviderBalanceProbeProfile::NewApiTokenUsage,
            origin.join("api/usage/token/")?,
        ),
        (
            ProviderBalanceProbeProfile::NewApiTokenUsage,
            origin.join("api/usage/token")?,
        ),
    ];
    let mut seen = HashSet::new();
    Ok(candidates
        .into_iter()
        .filter_map(|(profile, endpoint)| {
            let endpoint = endpoint.to_string();
            seen.insert(endpoint.clone())
                .then_some(ProviderBalanceProbeCandidate { profile, endpoint })
        })
        .collect())
}

fn balance_probe_api_root(base_url: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(base_url.trim())?;
    url.set_query(None);
    url.set_fragment(None);
    let mut segments = url
        .path()
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    match segments.last().map(String::as_str) {
        Some("responses" | "messages") => {
            segments.pop();
        }
        Some("completions") if segments.iter().rev().nth(1).map(String::as_str) == Some("chat") => {
            segments.pop();
            segments.pop();
        }
        _ => {}
    }
    if !segments
        .last()
        .is_some_and(|segment| segment.eq_ignore_ascii_case("v1"))
    {
        segments.push("v1".to_string());
    }
    url.set_path(&format!("/{}{}", segments.join("/"), "/"));
    Ok(url)
}

async fn read_limited_probe_body(response: reqwest::Response) -> Result<Vec<u8>> {
    tokio::time::timeout(PROVIDER_HEALTH_PROBE_TIMEOUT, async move {
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("response_read_error"))?;
            let remaining = PROVIDER_BALANCE_PROBE_RESPONSE_MAX_BYTES.saturating_sub(body.len());
            if remaining == 0 || chunk.len() > remaining {
                return Err(anyhow!("response_too_large"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    })
    .await
    .map_err(|_| anyhow!("response_timeout"))?
}

fn balance_response_failed(value: &Value) -> bool {
    value.get("error").is_some_and(|error| !error.is_null())
        || value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
        || value
            .get("code")
            .and_then(Value::as_bool)
            .is_some_and(|code| !code)
}

fn balance_display_for_profile(
    value: &Value,
    profile: ProviderBalanceProbeProfile,
) -> Option<String> {
    match profile {
        // Sub2-style `GET /v1/usage`: direct balance/remaining fields are
        // common, with compatible deployments sometimes nesting them.
        ProviderBalanceProbeProfile::Sub2Usage => balance_display_at(
            value,
            &[
                "/remaining",
                "/balance",
                "/quota/remaining",
                "/data/remaining",
                "/data/balance",
                "/data/quota/remaining",
            ],
        ),
        // New API reports unlimited status separately; numerical values are
        // upstream quota units, so the UI must not invent a currency symbol.
        ProviderBalanceProbeProfile::NewApiTokenUsage => {
            let Some(data) = recognized_new_api_token_usage(value) else {
                return None;
            };
            let numeric_pointers = [
                "/data/total_available",
                "/data/remaining",
                "/data/balance",
                "/total_available",
                "/remaining",
                "/balance",
            ];
            // New API's `unlimited_quota` is authoritative for its
            // token-usage envelope. `total_available` can be a signed usage
            // accounting value (including a negative number) and must not be
            // exposed as a monetary balance when the route explicitly marks
            // the quota unlimited.
            if data
                .get("unlimited_quota")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Some("无限额度".to_string());
            }
            // Finite New API quotas may legitimately be zero or negative;
            // preserve that value so the UI can render the depleted red state.
            balance_display_at(value, &numeric_pointers)
        }
    }
}

/// Avoid treating arbitrary JSON that happens to contain a balance-looking
/// field as a New API token-usage response. Unknown payloads remain
/// `余额不可查`; only a success envelope plus the documented token-usage shape
/// can produce a numeric or unlimited display.
fn recognized_new_api_token_usage(value: &Value) -> Option<&Value> {
    let data = value.get("data")?;
    let has_success_envelope = value.get("code").and_then(Value::as_bool).unwrap_or(false)
        || value
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || data
            .get("object")
            .and_then(Value::as_str)
            .is_some_and(|object| object == "token_usage");
    let has_usage_field = [
        "unlimited_quota",
        "total_available",
        "total_granted",
        "total_used",
        "remaining",
        "balance",
    ]
    .iter()
    .any(|field| data.get(*field).is_some());
    (has_success_envelope && has_usage_field).then_some(data)
}

fn balance_display_at(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer))
        .and_then(balance_scalar_display)
}

fn balance_scalar_display(value: &Value) -> Option<String> {
    match value {
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()
                && trimmed.chars().count() <= 48
                && trimmed.chars().all(|ch| {
                    ch.is_ascii_digit()
                        || matches!(ch, '.' | ',' | '-' | '+' | '$' | '¥' | '€' | '£' | ' ')
                }))
            .then(|| trimmed.to_string())
        }
        _ => None,
    }
}

#[tauri::command]
pub async fn diagnose_provider_network_paths(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<ProviderNetworkPathDiagnosticResult> {
    diagnose_provider_network_paths_inner(&state, &provider_id)
        .await
        .map_err(to_command_error)
}

#[tauri::command]
pub async fn reveal_provider_api_key(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<Option<String>> {
    state
        .config
        .read()
        .await
        .provider_api_key(&provider_id)
        .map_err(to_command_error)
}

#[tauri::command]
pub async fn reveal_provider_key(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
    key_id: String,
) -> CommandResult<Option<String>> {
    state
        .config
        .read()
        .await
        .provider_key_secret(&provider_id, &key_id)
        .map_err(to_command_error)
}

#[tauri::command]
pub async fn fetch_provider_models(
    state: State<'_, Arc<AppState>>,
    input: ProviderModelFetchInput,
) -> CommandResult<Vec<ModelConfig>> {
    let mut base_url = input.base_url.trim().to_string();
    let mut models_url = clean_optional_string(input.models_url);
    let mut is_full_url = input.is_full_url;
    let mut custom_user_agent = clean_optional_string(input.custom_user_agent);
    let mut upstream_secret = input
        .api_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
        .map(ToOwned::to_owned);

    if let Some(provider_id) = input.provider_id.as_deref() {
        let config = state.config.read().await;
        if let Some(provider) = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
        {
            if base_url.is_empty() {
                base_url = provider.base_url.clone();
            }
            if models_url.is_none() {
                models_url = provider.models_url.clone();
            }
            if !is_full_url {
                is_full_url = provider.is_full_url;
            }
            if custom_user_agent.is_none() {
                custom_user_agent = provider.custom_user_agent.clone();
            }
            if upstream_secret.is_none() {
                upstream_secret = config
                    .provider_api_key(provider_id)
                    .map_err(to_command_error)?;
            }
        }
    }

    let client = state
        .control_plane_upstream_client(input.use_system_proxy)
        .await
        .map_err(to_command_error)?;
    let models = fetch_models_from_upstream_with_options(
        &client,
        &base_url,
        input.channel,
        upstream_secret.as_deref(),
        is_full_url,
        models_url.as_deref(),
        custom_user_agent.as_deref(),
    )
    .await
    .map_err(to_command_error)?;

    Ok(models)
}

async fn diagnose_provider_network_paths_inner(
    state: &AppState,
    provider_id: &str,
) -> Result<ProviderNetworkPathDiagnosticResult> {
    let provider_id = provider_id.trim();
    if provider_id.is_empty() {
        return Err(anyhow!("provider id is empty"));
    }

    // Select from a config snapshot so this manual diagnostic never rotates a
    // pool, updates key counters, or otherwise changes the saved provider.
    let (provider, api_key, explicit_proxy_url) = {
        let config = state.config.read().await;
        let provider = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .cloned()
            .ok_or_else(|| anyhow!("provider {provider_id} was not found"))?;
        let mut selection_snapshot = config.clone();
        let selected_key = selection_snapshot
            .select_provider_key_for_request(provider_id, None, None)
            .with_context(|| format!("failed to select provider key for {provider_id}"))?
            .ok_or_else(|| anyhow!("provider API key is not configured"))?;
        if selected_key.secret.trim().is_empty() {
            return Err(anyhow!("provider API key is not configured"));
        }
        (
            provider,
            selected_key.secret,
            config.upstream_proxy_url.clone(),
        )
    };

    let explicit_proxy_client = explicit_proxy_url
        .as_deref()
        .map(|proxy_url| {
            state
                .transport_clients
                .explicit_proxy_client(proxy_url, false)
        })
        .transpose()?;

    let candidates = model_endpoint_candidates(
        &provider.base_url,
        provider.is_full_url,
        provider.models_url.as_deref(),
    )
    .with_context(|| format!("could not derive a models URL for provider {provider_id}"))?;

    let custom_user_agent = provider.custom_user_agent.as_deref();
    let mut last_attempt = None;
    for target_url in candidates {
        // Each candidate is compared over the same endpoint and credentials.
        // Only a valid model list can select it; a status-only 200 is not
        // enough to hide a compatibility fallback.
        let (direct, system_proxy, explicit_proxy) = tokio::join!(
            diagnose_model_endpoint(
                "direct",
                state.upstream_client(false),
                &target_url,
                &provider.channel,
                &api_key,
                custom_user_agent,
            ),
            diagnose_model_endpoint(
                "system-proxy",
                state.upstream_client(true),
                &target_url,
                &provider.channel,
                &api_key,
                custom_user_agent,
            ),
            async {
                match explicit_proxy_client.as_ref() {
                    Some(client) => Some(
                        diagnose_model_endpoint(
                            "explicit-proxy",
                            client,
                            &target_url,
                            &provider.channel,
                            &api_key,
                            custom_user_agent,
                        )
                        .await,
                    ),
                    None => None,
                }
            }
        );
        let has_valid_model_list = direct.has_valid_model_list
            || system_proxy.has_valid_model_list
            || explicit_proxy
                .as_ref()
                .is_some_and(|attempt| attempt.has_valid_model_list);
        let mut paths = vec![direct.result, system_proxy.result];
        if let Some(explicit_proxy) = explicit_proxy {
            paths.push(explicit_proxy.result);
        }
        if has_valid_model_list {
            return Ok(ProviderNetworkPathDiagnosticResult {
                provider_id: provider_id.to_string(),
                target_url,
                paths,
            });
        }
        last_attempt = Some((target_url, paths));
    }

    let (target_url, paths) = last_attempt
        .ok_or_else(|| anyhow!("could not derive a models URL for provider {provider_id}"))?;
    Ok(ProviderNetworkPathDiagnosticResult {
        provider_id: provider_id.to_string(),
        target_url,
        paths,
    })
}

async fn diagnose_model_endpoint(
    path: &'static str,
    client: &reqwest::Client,
    url: &str,
    channel: &Channel,
    api_key: &str,
    custom_user_agent: Option<&str>,
) -> ProviderNetworkPathAttempt {
    let started_at = Instant::now();
    let outcome = tokio::time::timeout(
        PROVIDER_NETWORK_DIAGNOSTIC_TIMEOUT,
        model_list_request(client, url, channel, Some(api_key), custom_user_agent).send(),
    )
    .await;
    let elapsed_ms = started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;

    match outcome {
        Ok(Ok(response)) => {
            let status = response.status();
            let mut result = ProviderNetworkPathResult {
                path: path.to_string(),
                ok: false,
                status: Some(status.as_u16()),
                elapsed_ms,
                http_version: Some(format!("{:?}", response.version())),
                remote_addr: response.remote_addr().map(|address| address.to_string()),
                error: None,
            };
            if !status.is_success() {
                result.error = Some(format!("HTTP {status}"));
                return ProviderNetworkPathAttempt {
                    result,
                    has_valid_model_list: false,
                    models_count: 0,
                };
            }

            let remaining = PROVIDER_NETWORK_DIAGNOSTIC_TIMEOUT
                .checked_sub(started_at.elapsed())
                .unwrap_or_default();
            let body = match tokio::time::timeout(remaining, response.text()).await {
                Ok(Ok(body)) => body,
                Ok(Err(_)) => {
                    result.error = Some("could not read response body".to_string());
                    return ProviderNetworkPathAttempt {
                        result,
                        has_valid_model_list: false,
                        models_count: 0,
                    };
                }
                Err(_) => {
                    result.error = Some(format!(
                        "timed out after {}s",
                        PROVIDER_NETWORK_DIAGNOSTIC_TIMEOUT.as_secs()
                    ));
                    return ProviderNetworkPathAttempt {
                        result,
                        has_valid_model_list: false,
                        models_count: 0,
                    };
                }
            };
            let value = match serde_json::from_str::<Value>(&body) {
                Ok(value) => value,
                Err(_) => {
                    result.error = Some("response body was not valid JSON".to_string());
                    return ProviderNetworkPathAttempt {
                        result,
                        has_valid_model_list: false,
                        models_count: 0,
                    };
                }
            };
            if value.get("success").and_then(Value::as_bool) == Some(false) {
                result.error = Some("upstream reported failure".to_string());
                return ProviderNetworkPathAttempt {
                    result,
                    has_valid_model_list: false,
                    models_count: 0,
                };
            }

            let models_count = parse_models(value).len();
            let has_valid_model_list = models_count > 0;
            result.ok = has_valid_model_list;
            if !has_valid_model_list {
                result.error = Some("response did not contain model records".to_string());
            }
            ProviderNetworkPathAttempt {
                result,
                has_valid_model_list,
                models_count,
            }
        }
        Ok(Err(err)) => ProviderNetworkPathAttempt {
            result: ProviderNetworkPathResult {
                path: path.to_string(),
                ok: false,
                status: None,
                elapsed_ms,
                http_version: None,
                remote_addr: None,
                error: Some(err.to_string()),
            },
            has_valid_model_list: false,
            models_count: 0,
        },
        Err(_) => ProviderNetworkPathAttempt {
            result: ProviderNetworkPathResult {
                path: path.to_string(),
                ok: false,
                status: None,
                elapsed_ms,
                http_version: None,
                remote_addr: None,
                error: Some(format!(
                    "timed out after {}s",
                    PROVIDER_NETWORK_DIAGNOSTIC_TIMEOUT.as_secs()
                )),
            },
            has_valid_model_list: false,
            models_count: 0,
        },
    }
}

async fn test_provider_key_inner(
    state: &AppState,
    input: &ProviderKeyTestInput,
) -> Result<ProviderKeyTestResult> {
    let mut upstream_secret = input
        .api_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
        .map(ToOwned::to_owned);
    if upstream_secret.is_none() {
        if let Some(provider_id) = input.provider_id.as_deref() {
            let config = state.config.read().await;
            upstream_secret =
                configured_provider_test_secret(&config, provider_id, input.key_id.as_deref())?;
        }
    }
    let Some(upstream_secret) = upstream_secret else {
        return Ok(ProviderKeyTestResult {
            provider_id: input.provider_id.clone(),
            key_id: input.key_id.clone(),
            ok: false,
            message: "key is empty".to_string(),
            models_count: 0,
        });
    };
    let client = state
        .control_plane_upstream_client(input.use_system_proxy)
        .await?;
    let models = fetch_models_from_upstream_with_options(
        &client,
        &input.base_url,
        input.channel.clone(),
        Some(upstream_secret.as_str()),
        input.is_full_url,
        input.models_url.as_deref(),
        input.custom_user_agent.as_deref(),
    )
    .await;
    match models {
        Ok(models) => Ok(ProviderKeyTestResult {
            provider_id: input.provider_id.clone(),
            key_id: input.key_id.clone(),
            ok: true,
            message: format!("可用，获取到 {} 个模型", models.len()),
            models_count: models.len(),
        }),
        Err(err) => Ok(ProviderKeyTestResult {
            provider_id: input.provider_id.clone(),
            key_id: input.key_id.clone(),
            ok: false,
            message: err.to_string(),
            models_count: 0,
        }),
    }
}

fn failed_provider_key_test(
    input: &ProviderKeyTestInput,
    error: anyhow::Error,
) -> ProviderKeyTestResult {
    ProviderKeyTestResult {
        provider_id: input.provider_id.clone(),
        key_id: input.key_id.clone(),
        ok: false,
        message: error.to_string(),
        models_count: 0,
    }
}

/// Runs the connection test across the two paths that a saved provider can
/// actually use. This deliberately keeps the edited endpoint and channel
/// supplied by the UI, while an empty secret may be resolved from the saved
/// provider on the backend.
async fn test_provider_connection_paths_inner(
    state: &AppState,
    input: &ProviderKeyTestInput,
) -> Result<ProviderConnectionPathTestResult> {
    let resolved_input = resolve_connection_path_test_input(state, input).await?;
    let input = &resolved_input;
    let mut upstream_secret = input
        .api_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
        .map(ToOwned::to_owned);
    if upstream_secret.is_none() {
        if let Some(provider_id) = input.provider_id.as_deref() {
            let config = state.config.read().await;
            upstream_secret =
                configured_provider_test_secret(&config, provider_id, input.key_id.as_deref())?;
        }
    }
    let Some(upstream_secret) = upstream_secret else {
        return Ok(ProviderConnectionPathTestResult {
            provider_id: input.provider_id.clone(),
            key_id: input.key_id.clone(),
            ok: false,
            recommended_use_system_proxy: input.use_system_proxy,
            models_count: 0,
            message: "key is empty".to_string(),
            paths: Vec::new(),
        });
    };

    let candidates = model_endpoint_candidates(
        &input.base_url,
        input.is_full_url,
        input.models_url.as_deref(),
    )?;
    let direct_client = state.control_plane_upstream_client(false).await?;
    let system_proxy_client = state.control_plane_upstream_client(true).await?;
    let mut last_paths = Vec::new();

    for target_url in candidates {
        let (direct, system_proxy) = tokio::join!(
            diagnose_model_endpoint(
                "direct",
                &direct_client,
                &target_url,
                &input.channel,
                &upstream_secret,
                input.custom_user_agent.as_deref(),
            ),
            diagnose_model_endpoint(
                "system-proxy",
                &system_proxy_client,
                &target_url,
                &input.channel,
                &upstream_secret,
                input.custom_user_agent.as_deref(),
            ),
        );
        let recommended_use_system_proxy = fastest_connection_path(&direct, &system_proxy);
        let models_count = recommended_use_system_proxy
            .map(|use_system_proxy| {
                if use_system_proxy {
                    system_proxy.models_count
                } else {
                    direct.models_count
                }
            })
            .unwrap_or_default();
        let paths = vec![direct.result, system_proxy.result];
        if let Some(recommended_use_system_proxy) = recommended_use_system_proxy {
            let path_name = if recommended_use_system_proxy {
                "system proxy"
            } else {
                "direct"
            };
            return Ok(ProviderConnectionPathTestResult {
                provider_id: input.provider_id.clone(),
                key_id: input.key_id.clone(),
                ok: true,
                recommended_use_system_proxy,
                models_count,
                message: format!("{path_name} path returned a valid model list"),
                paths,
            });
        }
        last_paths = paths;
    }

    Ok(ProviderConnectionPathTestResult {
        provider_id: input.provider_id.clone(),
        key_id: input.key_id.clone(),
        ok: false,
        recommended_use_system_proxy: input.use_system_proxy,
        models_count: 0,
        message: "neither direct nor system-proxy path returned a valid model list".to_string(),
        paths: last_paths,
    })
}

/// Resolves a saved provider's diagnostic credential from the same Key pool
/// selection rules as an ordinary request, without changing the saved pool
/// cursor, counters, health, or proxy setting. A draft-secret or an explicitly
/// selected Key always wins so users can still test an unsaved edit by hand.
async fn resolve_connection_path_test_input(
    state: &AppState,
    input: &ProviderKeyTestInput,
) -> Result<ProviderKeyTestInput> {
    let config = state.config.read().await;
    connection_path_test_input_from_config(&config, input)
}

fn connection_path_test_input_from_config(
    config: &AppConfig,
    input: &ProviderKeyTestInput,
) -> Result<ProviderKeyTestInput> {
    if input
        .api_key
        .as_deref()
        .is_some_and(|key| !key.trim().is_empty())
        || input
            .key_id
            .as_deref()
            .is_some_and(|key_id| !key_id.trim().is_empty())
    {
        return Ok(input.clone());
    }
    let Some(provider_id) = input
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|provider_id| !provider_id.is_empty())
    else {
        return Ok(input.clone());
    };

    // Selection is evaluated on a copy. A manual connection test must never
    // rotate a sequential pool, revive a disabled Key, or mutate the editor.
    let mut selection_snapshot = config.clone();
    let selected = selection_snapshot
        .select_provider_key_for_request(provider_id, None, None)?
        .ok_or_else(|| anyhow!("provider API key is not configured"))?;
    let mut resolved = input.clone();
    resolved.key_id = selected.key_id;
    resolved.api_key = Some(selected.secret);
    Ok(resolved)
}

/// Returns the faster successful path. A tie intentionally preserves direct
/// routing because it has fewer moving parts; an existing saved setting is not
/// mutated until the user explicitly saves the editor.
fn fastest_connection_path(
    direct: &ProviderNetworkPathAttempt,
    system_proxy: &ProviderNetworkPathAttempt,
) -> Option<bool> {
    match (
        direct.has_valid_model_list,
        system_proxy.has_valid_model_list,
    ) {
        (true, true) => Some(system_proxy.result.elapsed_ms < direct.result.elapsed_ms),
        (true, false) => Some(false),
        (false, true) => Some(true),
        (false, false) => None,
    }
}

fn configured_provider_test_secret(
    config: &AppConfig,
    provider_id: &str,
    key_id: Option<&str>,
) -> Result<Option<String>> {
    let secret = if let Some(key_id) = key_id {
        config.provider_key_secret(provider_id, key_id)
    } else {
        config.provider_api_key(provider_id)
    };
    secret.map_err(to_command_error).map_err(anyhow::Error::msg)
}

#[tauri::command]
pub async fn add_or_update_model(
    state: State<'_, Arc<AppState>>,
    input: ModelInput,
) -> CommandResult<PublicConfig> {
    let mut normalized_model = input.model;
    normalized_model.id = normalized_model.id.trim().to_string();
    if normalized_model.id.is_empty() {
        return Err("model id cannot be empty".to_string());
    }
    normalized_model.request_model_id = clean_optional_string(normalized_model.request_model_id)
        .filter(|alias| alias != &normalized_model.id);
    normalized_model.display_name = normalized_model.display_name.trim().to_string();
    if normalized_model.display_name.is_empty() {
        normalized_model.display_name = normalized_model.id.clone();
    }
    normalized_model.reasoning_effort = normalized_model
        .reasoning_effort
        .as_deref()
        .and_then(crate::config::normalize_reasoning_effort);
    normalized_model.supported_reasoning_efforts =
        crate::config::normalize_reasoning_efforts(&normalized_model.supported_reasoning_efforts);
    if normalized_model.reasoning_effort_override_enabled
        && normalized_model.reasoning_effort.is_none()
    {
        return Err("reasoning effort override requires a valid effort".to_string());
    }
    let version = {
        let mut config = state.config.write().await;
        {
            let provider = config
                .providers
                .iter_mut()
                .find(|provider| provider.id == input.provider_id)
                .ok_or_else(|| format!("provider {} was not found", input.provider_id))?;
            if let Some(model) = provider
                .models
                .iter_mut()
                .find(|item| item.id == normalized_model.id)
            {
                *model = normalized_model.clone();
            } else {
                provider.models.push(normalized_model);
            }
            provider.updated_at = Utc::now();
        }
        config.updated_at = Utc::now();
        refresh_enabled_injections_for_provider(&mut config, &input.provider_id)
            .map_err(to_command_error)?;
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

fn refresh_enabled_injections_for_provider(
    config: &mut AppConfig,
    provider_id: &str,
) -> Result<Vec<AgentInjectionResult>> {
    let agent_ids = config
        .agent_injections
        .iter()
        .filter(|agent| agent.enabled && agent.provider_id.as_deref() == Some(provider_id))
        .map(|agent| agent.id.clone())
        .collect::<Vec<_>>();
    let mut results = Vec::new();
    for agent_id in agent_ids {
        results.extend(agent_injection::apply_one_by_id(config, &agent_id)?);
    }
    Ok(results)
}

#[tauri::command]
pub async fn delete_model(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
    model_id: String,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        {
            let provider = config
                .providers
                .iter_mut()
                .find(|provider| provider.id == provider_id)
                .ok_or_else(|| format!("provider {provider_id} was not found"))?;
            provider.models.retain(|model| model.id != model_id);
            provider.updated_at = Utc::now();
        }
        config.clear_cache_capabilities_for_model(&provider_id, &model_id);
        for agent in config
            .agent_injections
            .iter_mut()
            .filter(|agent| agent.provider_id.as_deref() == Some(provider_id.as_str()))
        {
            if agent.model_id.as_deref() == Some(model_id.as_str()) {
                agent.model_id = None;
                agent.last_status = Some(
                    "已移除 Agent 默认模型映射，后续请求将原样使用 Agent 选择的模型".to_string(),
                );
            }
        }
        config.updated_at = Utc::now();
        refresh_enabled_injections_for_provider(&mut config, &provider_id)
            .map_err(to_command_error)?;
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn start_proxy(state: State<'_, Arc<AppState>>) -> CommandResult<ProxyStatus> {
    state.start_proxy().await.map_err(to_command_error)
}

#[tauri::command]
pub async fn stop_proxy(state: State<'_, Arc<AppState>>) -> CommandResult<ProxyStatus> {
    state.stop_proxy().await.map_err(to_command_error)
}

#[tauri::command]
pub async fn get_proxy_status(state: State<'_, Arc<AppState>>) -> CommandResult<ProxyStatus> {
    Ok(state.proxy_status().await)
}

#[tauri::command]
pub async fn get_metrics(state: State<'_, Arc<AppState>>) -> CommandResult<MetricsSnapshot> {
    Ok(state.metrics.snapshot().await)
}

/// Reads the independently persisted, time-bucketed cache trend. This is kept
/// out of `get_metrics` because the live snapshot is refreshed much more often.
#[tauri::command]
pub async fn get_metrics_trend(
    state: State<'_, Arc<AppState>>,
    input: MetricsTrendQueryInput,
) -> CommandResult<MetricsTrendSnapshot> {
    state.metrics.trend(input).map_err(to_command_error)
}

/// Returns the current runtime cohort against the best persisted build with
/// exactly the same key-safe cache scope. Legacy hourly history is never
/// promoted into a version champion.
#[tauri::command]
pub async fn get_release_champion(
    state: State<'_, Arc<AppState>>,
    input: ReleaseChampionQueryInput,
) -> CommandResult<ReleaseChampionSnapshot> {
    state
        .metrics
        .release_champion(input)
        .map_err(to_command_error)
}

#[tauri::command]
pub async fn get_cache_validation_status(
    state: State<'_, Arc<AppState>>,
) -> CommandResult<CacheValidationStatus> {
    Ok(state.cache_validation.lock().await.status(Utc::now()))
}

#[tauri::command]
pub async fn set_cache_validation_mode(
    state: State<'_, Arc<AppState>>,
    input: CacheValidationControlInput,
) -> CommandResult<CacheValidationStatus> {
    let provider_name = if input.mode == CacheValidationMode::Auto {
        None
    } else {
        let provider_id = input
            .provider_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "provider is required for cache validation".to_string())?;
        Some(
            state
                .config
                .read()
                .await
                .providers
                .iter()
                .find(|provider| provider.id == provider_id)
                .map(|provider| provider.name.clone())
                .ok_or_else(|| format!("provider {provider_id} was not found"))?,
        )
    };
    state
        .cache_validation
        .lock()
        .await
        .configure(input, provider_name, Utc::now())
}

#[tauri::command]
pub async fn reload_config(state: State<'_, Arc<AppState>>) -> CommandResult<PublicConfig> {
    state.reload_config().await.map_err(to_command_error)
}

#[tauri::command]
pub async fn save_cache_policy(
    state: State<'_, Arc<AppState>>,
    mut input: CacheConfig,
) -> CommandResult<PublicConfig> {
    let version = {
        let mut config = state.config.write().await;
        input.normalize_fast_forwarding_hit_policy();
        config.cache = input;
        config.updated_at = Utc::now();
        state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn clear_cache(state: State<'_, Arc<AppState>>) -> CommandResult<()> {
    state.cache.clear().await.map_err(to_command_error)
}

#[tauri::command]
pub async fn get_agent_injections(
    state: State<'_, Arc<AppState>>,
) -> CommandResult<Vec<crate::config::AgentInjectionConfig>> {
    let (injections, version) = {
        let mut config = state.config.write().await;
        agent_injection::ensure_defaults(&mut config);
        let version = state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?;
        (config.agent_injections.clone(), version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    Ok(injections)
}

#[tauri::command]
pub async fn set_agent_injection_enabled(
    state: State<'_, Arc<AppState>>,
    input: AgentInjectionUpdate,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let enabled = input.enabled;
    let agent_id = input.id.clone();
    let (mut results, version) = {
        let mut config = state.config.write().await;
        let results = agent_injection::set_enabled(&mut config, &input.id, input.enabled)
            .map_err(to_command_error)?;
        config.updated_at = Utc::now();
        let version = state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?;
        (results, version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    if agent_id == "proxy-mode" {
        if enabled {
            state
                .start_proxy_mode_proxy()
                .await
                .map_err(to_command_error)?;
        } else {
            state
                .stop_proxy_mode_proxy()
                .await
                .map_err(to_command_error)?;
        }
    } else if enabled {
        state.start_proxy().await.map_err(to_command_error)?;
    }
    if agent_id == "codex" && !enabled && codex_ui_patch::has_managed_patch() {
        let patch_status = codex_ui_patch_notice(false, codex_ui_patch::set_enabled(false));
        attach_codex_ui_patch_status(&mut results, false, patch_status);
    }
    Ok(results)
}

#[tauri::command]
pub async fn apply_agent_injection(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let (results, version) = {
        let mut config = state.config.write().await;
        let results =
            agent_injection::apply_one_by_id(&mut config, &id).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        let version = state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?;
        (results, version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    if id == "proxy-mode" {
        state
            .start_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    } else {
        state.start_proxy().await.map_err(to_command_error)?;
    }
    Ok(results)
}

#[tauri::command]
pub async fn apply_enabled_agent_injections(
    state: State<'_, Arc<AppState>>,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let (results, version) = {
        let mut config = state.config.write().await;
        let results = agent_injection::apply_enabled(&mut config).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        let version = state
            .publish_config_snapshot(&config)
            .map_err(to_command_error)?;
        (results, version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    if results.iter().any(|item| item.id == "proxy-mode") {
        state
            .start_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    }
    if results.iter().any(|item| item.id != "proxy-mode") {
        state.start_proxy().await.map_err(to_command_error)?;
    }
    Ok(results)
}

fn codex_ui_patch_notice(enabled: bool, result: anyhow::Result<String>) -> String {
    match result {
        Ok(status) => status,
        Err(error) => {
            let action = if enabled { "显示" } else { "恢复" };
            format!(
                "代理注入已热更新；Codex UI {action}补丁未完成：{error}。这不会影响 Responses 代理注入"
            )
        }
    }
}

fn attach_codex_ui_patch_status(
    results: &mut Vec<AgentInjectionResult>,
    enabled: bool,
    patch_status: String,
) {
    if let Some(result) = results.iter_mut().find(|item| item.id == "codex") {
        result.status = format!("{}；{}", result.status, patch_status);
        return;
    }

    results.push(AgentInjectionResult {
        id: "codex".to_string(),
        label: "Codex".to_string(),
        enabled,
        target_path: None,
        backup_path: None,
        status: format!(
            "Codex 自动注入已{}；{}",
            if enabled { "启用" } else { "关闭" },
            patch_status
        ),
        injected_at: Utc::now().to_rfc3339(),
    });
}

#[tauri::command]
pub async fn update_agent_injection_route(
    state: State<'_, Arc<AppState>>,
    input: AgentInjectionRouteUpdate,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let agent_id = input.id.clone();
    let should_start_proxy = {
        let config = state.config.read().await;
        config
            .agent_injections
            .iter()
            .any(|item| item.id == input.id && item.enabled)
    };
    let (results, version) = {
        let mut config = state.config.write().await;
        let (mut staged, results) =
            stage_agent_injection_route_update(&config, input).map_err(to_command_error)?;
        staged.updated_at = Utc::now();
        let version = state
            .publish_config_snapshot(&staged)
            .map_err(to_command_error)?;
        *config = staged;
        (results, version)
    };
    state
        .wait_for_config_snapshot(version)
        .await
        .map_err(to_command_error)?;
    if should_start_proxy && !results.is_empty() {
        if agent_id == "proxy-mode" {
            state
                .start_proxy_mode_proxy()
                .await
                .map_err(to_command_error)?;
        } else {
            state.start_proxy().await.map_err(to_command_error)?;
        }
    }
    Ok(results)
}

fn stage_agent_injection_route_update(
    config: &AppConfig,
    mut input: AgentInjectionRouteUpdate,
) -> Result<(AppConfig, Vec<AgentInjectionResult>)> {
    let agent_id = input.id.clone();
    let mut staged = config.clone();
    if let Some(provider_id) = input.provider_id.clone() {
        if !provider_is_registered_to_agent(&staged, &provider_id, &agent_id) {
            let private_provider_id = clone_provider_for_agent_config(
                &mut staged,
                &agent_id,
                &provider_id,
                input.model_id.as_deref(),
            )?;
            input.provider_id = Some(private_provider_id);
        }
    }
    let results = agent_injection::update_route(&mut staged, input)?;
    Ok((staged, results))
}

fn model_list_request(
    client: &reqwest::Client,
    url: &str,
    channel: &Channel,
    api_key: Option<&str>,
    custom_user_agent: Option<&str>,
) -> reqwest::RequestBuilder {
    upstream_authorized_request(client.get(url), channel, api_key, custom_user_agent)
}

fn upstream_authorized_request(
    request: reqwest::RequestBuilder,
    channel: &Channel,
    api_key: Option<&str>,
    custom_user_agent: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(api_key) = api_key {
        return request.headers(proxy::build_management_probe_headers(
            api_key,
            channel,
            false,
            custom_user_agent,
        ));
    }
    let user_agent = custom_user_agent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(crate::ATOAPI_USER_AGENT);
    let request = request.header(reqwest::header::USER_AGENT, user_agent);
    if matches!(channel, Channel::Anthropic) {
        request.header("anthropic-version", "2023-06-01")
    } else {
        request
    }
}

async fn fetch_models_from_upstream_with_options(
    client: &reqwest::Client,
    base_url: &str,
    channel: Channel,
    api_key: Option<&str>,
    is_full_url: bool,
    models_url: Option<&str>,
    custom_user_agent: Option<&str>,
) -> Result<Vec<ModelConfig>> {
    let candidates = model_endpoint_candidates(base_url, is_full_url, models_url)?;
    let mut last_error = None;
    for url in candidates {
        match model_list_request(client, &url, &channel, api_key, custom_user_agent)
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let body = response.text().await.unwrap_or_default();
                if !status.is_success() {
                    last_error = Some(format!(
                        "{url} returned HTTP {status}: {}",
                        truncate_body(&body)
                    ));
                    continue;
                }
                let value = serde_json::from_str::<Value>(&body).with_context(|| {
                    format!("{url} returned {content_type} but JSON parsing failed")
                })?;
                if value.get("success").and_then(Value::as_bool) == Some(false) {
                    let message = value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("upstream reported failure");
                    last_error = Some(format!("{url} returned failure: {message}"));
                    continue;
                }
                let models = parse_models(value);
                if !models.is_empty() {
                    return Ok(models);
                }
                last_error = Some(format!("{url} returned no model records"));
            }
            Err(err) => {
                last_error = Some(format!("{url} failed: {err}"));
            }
        }
    }
    Err(anyhow!(
        "could not fetch model list: {}",
        last_error.unwrap_or_else(|| "no candidate model endpoint worked".to_string())
    ))
}

fn model_endpoint_candidates(
    base_url: &str,
    is_full_url: bool,
    models_url_override: Option<&str>,
) -> Result<Vec<String>> {
    if let Some(raw) = models_url_override {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            reqwest::Url::parse(trimmed)?;
            return Ok(vec![trimmed.to_string()]);
        }
    }

    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(anyhow!("base URL is empty"));
    }
    reqwest::Url::parse(trimmed)?;

    let mut candidates = Vec::new();

    if is_full_url {
        if let Some(index) = trimmed.find("/v1/") {
            candidates.push(format!("{}/v1/models", &trimmed[..index]));
        } else if let Some(index) = trimmed.rfind('/') {
            let root = &trimmed[..index];
            if root.contains("://") && root.len() > root.find("://").unwrap_or(0) + 3 {
                push_standard_model_candidates(&mut candidates, root);
            }
        }
        if candidates.is_empty() {
            return Err(anyhow!("could not derive /v1/models from full URL"));
        }
        return Ok(dedupe_preserve_order(candidates));
    }

    let mut matched_full_endpoint = false;
    for suffix in [
        "/v1/chat/completions",
        "/v1/responses",
        "/v1/messages",
        "/chat/completions",
        "/responses",
        "/messages",
    ] {
        if let Some(root) = trimmed.strip_suffix(suffix) {
            push_standard_model_candidates(&mut candidates, root);
            matched_full_endpoint = true;
        }
    }

    if !matched_full_endpoint {
        push_standard_model_candidates(&mut candidates, trimmed);
    }

    if let Some(stripped) = strip_compat_suffix(trimmed) {
        let root = stripped.trim_end_matches('/');
        if !root.is_empty() && root.contains("://") {
            candidates.push(format!("{root}/v1/models"));
            candidates.push(format!("{root}/models"));
        }
    }

    Ok(dedupe_preserve_order(candidates))
}

fn push_standard_model_candidates(candidates: &mut Vec<String>, base_url: &str) {
    let trimmed = base_url.trim_end_matches('/');
    if ends_with_version_segment(trimmed) {
        candidates.push(format!("{trimmed}/models"));
        if !trimmed.ends_with("/v1") {
            candidates.push(format!("{trimmed}/v1/models"));
        }
    } else {
        candidates.push(format!("{trimmed}/v1/models"));
    }
}

fn strip_compat_suffix(base_url: &str) -> Option<&str> {
    KNOWN_COMPAT_SUFFIXES.iter().find_map(|suffix| {
        base_url
            .ends_with(suffix)
            .then(|| &base_url[..base_url.len() - suffix.len()])
    })
}

fn ends_with_version_segment(url: &str) -> bool {
    let last = url.rsplit('/').next().unwrap_or("");
    last.strip_prefix('v').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn dedupe_preserve_order(items: Vec<String>) -> Vec<String> {
    let mut unique = Vec::with_capacity(items.len());
    for item in items {
        if !unique.iter().any(|existing| existing == &item) {
            unique.push(item);
        }
    }
    unique
}

fn truncate_body(body: &str) -> String {
    if body.chars().count() <= ERROR_BODY_MAX_CHARS {
        body.to_string()
    } else {
        let mut truncated = body.chars().take(ERROR_BODY_MAX_CHARS).collect::<String>();
        truncated.push('…');
        truncated
    }
}

fn parse_models(value: Value) -> Vec<ModelConfig> {
    let arrays = [
        value.get("data"),
        value.get("models"),
        value.pointer("/data/models"),
        Some(&value),
    ];
    for candidate in arrays.into_iter().flatten() {
        if let Some(items) = candidate.as_array() {
            let models = items
                .iter()
                .filter_map(parse_model)
                .collect::<Vec<ModelConfig>>();
            if !models.is_empty() {
                return models;
            }
        }
    }
    Vec::new()
}

fn parse_model(value: &Value) -> Option<ModelConfig> {
    let id = value
        .get("id")
        .or_else(|| value.get("name"))
        .or_else(|| value.get("model"))
        .and_then(Value::as_str)?
        .to_string();
    let display_name = value
        .get("display_name")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .unwrap_or(&id)
        .to_string();
    let context_window = value
        .get("context_window")
        .or_else(|| value.get("context_length"))
        .or_else(|| value.get("max_context_length"))
        .or_else(|| value.get("max_tokens"))
        .and_then(Value::as_u64)
        .map(|value| value as u32);
    Some(ModelConfig {
        id,
        request_model_id: None,
        display_name,
        context_window,
        output_window: value
            .get("max_output_tokens")
            .or_else(|| value.get("output_window"))
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        reasoning_effort_override_enabled: false,
        reasoning_effort: None,
        supported_reasoning_efforts: parse_reasoning_efforts(value),
        supports_tools: value
            .get("supports_tools")
            .or_else(|| value.pointer("/capabilities/tools"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        supports_streaming: value
            .get("supports_streaming")
            .or_else(|| value.pointer("/capabilities/streaming"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        enabled: true,
    })
}

fn parse_reasoning_efforts(value: &Value) -> Vec<String> {
    let candidates = [
        value.get("supported_reasoning_efforts"),
        value.get("reasoning_efforts"),
        value
            .get("capabilities")
            .and_then(|capabilities| capabilities.get("reasoning_efforts")),
        value
            .get("reasoning")
            .and_then(|reasoning| reasoning.get("supported_efforts")),
    ];
    let parsed = candidates
        .into_iter()
        .flatten()
        .find_map(|candidate| {
            candidate.as_array().map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default();
    crate::config::normalize_reasoning_efforts(&parsed)
}

fn to_command_error(err: impl std::fmt::Display) -> String {
    err.to_string()
}

fn clean_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_provider(id: &str) -> crate::config::ProviderConfig {
        crate::config::ProviderConfig {
            id: id.to_string(),
            name: id.to_string(),
            base_url: format!("https://{id}.example/v1"),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn agent_owned_edit_clones_a_bound_legacy_provider_without_losing_its_key() {
        let mut config = AppConfig::default();
        let mut shared = test_provider("shared");
        shared.api_key_encrypted = Some("shared-secret".to_string());
        config.providers.push(shared);
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.provider_id = Some("shared".to_string());

        let mut input = ProviderInput {
            id: Some("shared".to_string()),
            owner_agent_id: Some("codex".to_string()),
            name: "Codex private".to_string(),
            base_url: "https://private.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel_mode: crate::config::ProviderChannelMode::Auto,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: true,
            use_system_proxy: true,
            non_sse_compact_compat_enabled: false,
            auto_compact_token_limit: None,
            auto_compact_token_limit_configured: false,
            api_key: None,
            key_pool: None,
            enabled: true,
        };

        prepare_agent_owned_provider_input(&mut config, &mut input).unwrap();
        let private_id = input.id.clone().unwrap();
        assert_eq!(private_id, "agent-codex-shared");
        assert_eq!(input.owner_agent_id, None);
        assert_eq!(
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == "codex")
                .and_then(|agent| agent.provider_id.as_deref()),
            Some(private_id.as_str())
        );

        config.upsert_provider(input).unwrap();
        assert_eq!(
            config
                .providers
                .iter()
                .find(|provider| provider.id == private_id)
                .and_then(|provider| provider.api_key_encrypted.as_deref()),
            Some("shared-secret")
        );
        assert_eq!(
            config
                .providers
                .iter()
                .find(|provider| provider.id == "shared")
                .map(|provider| provider.name.as_str()),
            Some("shared")
        );
    }

    #[test]
    fn manual_agent_like_id_is_not_reused_as_a_private_clone() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        config.providers.push(test_provider("agent-codex-shared"));

        let private_id =
            clone_provider_for_agent_config(&mut config, "codex", "shared", None).unwrap();
        assert_eq!(private_id, "agent-codex-shared-2");
        assert!(provider_is_registered_to_agent(
            &config,
            &private_id,
            "codex"
        ));
        assert!(!provider_is_registered_to_agent(
            &config,
            "agent-codex-shared",
            "codex"
        ));

        let reused = clone_provider_for_agent_config(&mut config, "codex", "shared", None).unwrap();
        assert_eq!(reused, private_id);
        assert_eq!(
            config
                .providers
                .iter()
                .filter(|provider| provider.id.starts_with("agent-codex-shared"))
                .count(),
            2
        );
    }

    #[test]
    fn agent_provider_order_is_isolated_and_requires_the_complete_visible_set() {
        let mut config = AppConfig::default();
        config.providers.extend([
            test_provider("agent-codex-first"),
            test_provider("agent-codex-second"),
            test_provider("agent-opencode-other"),
        ]);
        config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap()
            .provider_id = Some("agent-codex-first".to_string());

        reorder_agent_providers_config(
            &mut config,
            &AgentProviderOrderInput {
                agent_id: "codex".to_string(),
                provider_ids: vec![
                    "agent-codex-second".to_string(),
                    "agent-codex-first".to_string(),
                ],
            },
        )
        .unwrap();
        assert_eq!(
            config
                .agent_provider_orders
                .iter()
                .find(|order| order.agent_id == "codex")
                .map(|order| order.provider_ids.as_slice()),
            Some(
                [
                    "agent-codex-second".to_string(),
                    "agent-codex-first".to_string()
                ]
                .as_slice()
            )
        );
        assert!(reorder_agent_providers_config(
            &mut config,
            &AgentProviderOrderInput {
                agent_id: "codex".to_string(),
                provider_ids: vec![
                    "agent-codex-first".to_string(),
                    "agent-opencode-other".to_string(),
                ],
            },
        )
        .is_err());
    }

    #[test]
    fn first_private_provider_append_preserves_the_current_legacy_provider_position() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap()
            .provider_id = Some("shared".to_string());
        config.providers.push(test_provider("agent-codex-new"));

        append_agent_provider_order(&mut config, "codex", "agent-codex-new");

        assert_eq!(
            config
                .agent_provider_orders
                .iter()
                .find(|order| order.agent_id == "codex")
                .map(|order| order.provider_ids.clone()),
            Some(vec!["shared".to_string(), "agent-codex-new".to_string(),])
        );
    }

    #[test]
    fn provider_connection_test_uses_saved_primary_key_without_a_key_id() {
        let mut config = AppConfig::default();
        let mut provider = test_provider("primary-key-test");
        provider.api_key_encrypted = Some("primary-test-secret".to_string());
        config.providers.push(provider);

        let secret = configured_provider_test_secret(&config, "primary-key-test", None)
            .expect("saved primary key should resolve");

        assert_eq!(secret.as_deref(), Some("primary-test-secret"));
    }

    #[test]
    fn active_provider_test_uses_the_next_eligible_pool_key_and_never_the_connection_key() {
        use crate::config::{
            KeyLoadBalanceStrategy, ProviderKeyInput, ProviderKeyPoolInput, ProviderKeyStatus,
        };

        let provider_id = "active-pool-key-test";
        let mut config = AppConfig::default();
        let mut provider = test_provider(provider_id);
        provider.api_key_encrypted = Some("legacy-connection-key".to_string());
        config.providers.push(provider);
        config
            .upsert_provider_key_pool(
                provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 1,
                    recovery_minutes: 30,
                    keys: vec![
                        ProviderKeyInput {
                            id: Some("disabled-old-key".to_string()),
                            alias: None,
                            key: Some("disabled-secret".to_string()),
                            enabled: false,
                            priority: 5,
                            status: ProviderKeyStatus::Unhealthy,
                            total_requests: 0,
                            successes: 0,
                            failures: 1,
                            last_checked_at: Some(Utc::now()),
                            last_error: Some("quota exhausted".to_string()),
                            disabled_until: None,
                        },
                        ProviderKeyInput {
                            id: Some("current-key".to_string()),
                            alias: None,
                            key: Some("current-secret".to_string()),
                            enabled: true,
                            priority: 5,
                            status: ProviderKeyStatus::Healthy,
                            total_requests: 0,
                            successes: 0,
                            failures: 0,
                            last_checked_at: Some(Utc::now()),
                            last_error: None,
                            disabled_until: None,
                        },
                    ],
                },
            )
            .unwrap();

        let input = active_provider_key_test_input_from_config(&mut config, provider_id).unwrap();
        assert_eq!(input.key_id.as_deref(), Some("current-key"));
        assert_eq!(input.api_key.as_deref(), Some("current-secret"));
        assert_ne!(input.api_key.as_deref(), Some("legacy-connection-key"));

        let pool = config
            .provider_key_pools
            .iter_mut()
            .find(|pool| pool.provider_id == provider_id)
            .unwrap();
        pool.keys
            .iter_mut()
            .find(|key| key.id == "current-key")
            .unwrap()
            .disabled_until = Some(Utc::now() + chrono::Duration::minutes(30));
        let error = active_provider_key_test_input_from_config(&mut config, provider_id)
            .unwrap_err()
            .to_string();
        assert!(error.contains("key pool has no enabled usable key"));
        assert!(!error.contains("legacy-connection-key"));

        let mut connection_test_snapshot = config.clone();
        connection_test_snapshot
            .provider_key_pools
            .iter_mut()
            .find(|pool| pool.provider_id == provider_id)
            .unwrap()
            .keys
            .iter_mut()
            .find(|key| key.id == "current-key")
            .unwrap()
            .disabled_until = None;
        let before = toml::to_string(&connection_test_snapshot).unwrap();
        let selected = connection_path_test_input_from_config(
            &connection_test_snapshot,
            &ProviderKeyTestInput {
                provider_id: Some(provider_id.to_string()),
                key_id: None,
                api_key: None,
                base_url: "https://draft.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                use_system_proxy: false,
            },
        )
        .unwrap();
        assert_eq!(selected.key_id.as_deref(), Some("current-key"));
        assert_eq!(selected.api_key.as_deref(), Some("current-secret"));
        assert_eq!(selected.base_url, "https://draft.example/v1");
        assert_eq!(toml::to_string(&connection_test_snapshot).unwrap(), before);
    }

    #[test]
    fn health_probe_model_selection_uses_a_pool_snapshot_without_advancing_it() {
        use crate::config::{
            KeyLoadBalanceStrategy, ProviderKeyInput, ProviderKeyPoolInput, ProviderKeyStatus,
        };

        let provider_id = "health-model-selection";
        let mut config = AppConfig::default();
        let mut provider = test_provider(provider_id);
        provider.api_key_encrypted = Some("legacy-connection-secret".to_string());
        config.providers.push(provider);
        config
            .upsert_provider_key_pool(
                provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 3,
                    recovery_minutes: 10,
                    keys: vec![
                        ProviderKeyInput {
                            id: Some("saved-current-key".to_string()),
                            alias: Some("主 Key".to_string()),
                            key: Some("pool-current-secret".to_string()),
                            enabled: true,
                            priority: 5,
                            status: ProviderKeyStatus::Healthy,
                            total_requests: 7,
                            successes: 7,
                            failures: 0,
                            last_checked_at: None,
                            last_error: None,
                            disabled_until: None,
                        },
                        ProviderKeyInput {
                            id: Some("saved-next-key".to_string()),
                            alias: Some("next Key".to_string()),
                            key: Some("pool-next-secret".to_string()),
                            enabled: true,
                            priority: 5,
                            status: ProviderKeyStatus::Healthy,
                            total_requests: 8,
                            successes: 8,
                            failures: 0,
                            last_checked_at: None,
                            last_error: None,
                            disabled_until: None,
                        },
                    ],
                },
            )
            .unwrap();
        let before = toml::to_string(&config).unwrap();

        let input = health_probe_current_key_input_from_config(&config, provider_id).unwrap();

        assert_eq!(input.key_id.as_deref(), Some("saved-current-key"));
        assert_eq!(input.api_key.as_deref(), Some("pool-current-secret"));
        let all = health_probe_inputs_from_config(&config, provider_id, &[]).unwrap();
        assert_eq!(
            all.iter()
                .filter_map(|item| item.key_id.as_deref())
                .collect::<Vec<_>>(),
            vec!["saved-current-key", "saved-next-key"],
            "the all-Key action retains saved-list order while the card uses only Current"
        );
        assert_eq!(toml::to_string(&config).unwrap(), before);
    }

    #[test]
    fn health_probe_accepts_a_successful_terminal_with_a_null_error_field() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n"
        );
        assert!(health_probe_stream_completed(
            body,
            &ProviderHealthProbeMode::ResponsesStreaming
        ));
        assert!(
            !health_probe_stream_failed(body),
            "a successful `error: null` terminal is not a stream error"
        );
        assert!(!health_probe_json_failed("{\"error\":null}"));
    }

    #[test]
    fn health_probe_accepts_a_large_responses_first_event_without_waiting_for_terminal() {
        let body = format!(
            "event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_large\",\"metadata\":\"{}\"}}}}\n\n",
            "x".repeat(24_000)
        );
        assert!(health_probe_stream_accepted(
            &body,
            &ProviderHealthProbeMode::ResponsesStreaming
        ));
        assert!(!health_probe_stream_accepted(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"blocked\"}}\n\n",
            &ProviderHealthProbeMode::ResponsesStreaming
        ));
    }

    #[test]
    fn health_probe_responses_use_codex_style_input_items() {
        let body = health_probe_request_body(
            &ProviderHealthProbeMode::ResponsesStreaming,
            "gpt-5.6-terra",
            "hi",
        );
        assert_eq!(
            body.pointer("/input/0/role").and_then(Value::as_str),
            Some("user")
        );
        assert_eq!(
            body.pointer("/input/0/content").and_then(Value::as_str),
            Some("hi")
        );
        assert_eq!(
            body.get("max_output_tokens").and_then(Value::as_u64),
            Some(1)
        );
        assert!(body.get("store").is_none());
        assert!(body.get("tools").is_none());
        assert!(body.get("previous_response_id").is_none());
        assert!(body.get("prompt_cache_key").is_none());
        assert!(body.get("prompt_cache_retention").is_none());
        assert!(body.get("metadata").is_none());
    }

    #[test]
    fn health_probe_adds_anthropic_messages_without_changing_responses_default() {
        assert_eq!(
            health_probe_endpoint_url("https://upstream.example/v1", &Channel::Anthropic).unwrap(),
            "https://upstream.example/v1/messages"
        );
        let body = health_probe_request_body(
            &ProviderHealthProbeMode::AnthropicStreaming,
            "claude-sonnet-4",
            "hi",
        );
        assert_eq!(
            body.pointer("/messages/0/role").and_then(Value::as_str),
            Some("user")
        );
        assert!(health_probe_stream_accepted(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n",
            &ProviderHealthProbeMode::AnthropicStreaming
        ));
        assert!(!health_probe_stream_accepted(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"bad key\"}}\n\n",
            &ProviderHealthProbeMode::AnthropicStreaming
        ));
    }

    #[test]
    fn built_in_balance_profiles_use_only_safe_api_roots_and_recognized_json() {
        let candidates = balance_probe_candidates("https://upstream.example/v1/responses").unwrap();
        assert_eq!(candidates.len(), 3);
        assert_eq!(
            candidates[0].profile,
            ProviderBalanceProbeProfile::Sub2Usage
        );
        assert_eq!(candidates[0].endpoint, "https://upstream.example/v1/usage");
        assert_eq!(
            candidates[1].profile,
            ProviderBalanceProbeProfile::NewApiTokenUsage
        );
        assert_eq!(
            candidates[1].endpoint,
            "https://upstream.example/api/usage/token/"
        );
        assert_eq!(
            candidates[2].endpoint,
            "https://upstream.example/api/usage/token"
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({"balance": 0.74182673, "remaining": 0.74182673}),
                ProviderBalanceProbeProfile::Sub2Usage,
            )
            .as_deref(),
            Some("0.74182673")
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({"data": {"unlimited_quota": true}}),
                ProviderBalanceProbeProfile::NewApiTokenUsage,
            )
            .as_deref(),
            None
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({
                    "code": true,
                    "data": {"object": "token_usage", "unlimited_quota": true}
                }),
                ProviderBalanceProbeProfile::NewApiTokenUsage,
            )
            .as_deref(),
            Some("无限额度")
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({
                    "code": true,
                    "data": {
                        "object": "token_usage",
                        "unlimited_quota": true,
                        "total_available": -9815964
                    }
                }),
                ProviderBalanceProbeProfile::NewApiTokenUsage,
            )
            .as_deref(),
            Some("无限额度")
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({"balance": 0}),
                ProviderBalanceProbeProfile::Sub2Usage,
            )
            .as_deref(),
            Some("0")
        );
        assert_eq!(
            balance_display_for_profile(
                &json!({"balance": "-$10.25"}),
                ProviderBalanceProbeProfile::Sub2Usage,
            )
            .as_deref(),
            Some("-$10.25")
        );
        assert!(balance_display_for_profile(
            &json!({"status": "ok", "data": {"message": "no balance here"}}),
            ProviderBalanceProbeProfile::Sub2Usage,
        )
        .is_none());
        assert!(!balance_response_failed(&json!({"error": null})));
        assert!(balance_response_failed(
            &json!({"error": {"code": "denied"}})
        ));
    }

    #[tokio::test]
    async fn built_in_balance_probe_uses_the_current_key_and_stops_after_sub2_usage() {
        use crate::config::{
            KeyLoadBalanceStrategy, ProviderKeyInput, ProviderKeyPoolInput, ProviderKeyStatus,
        };
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let usage_requests = requests.clone();
        let fallback_requests = requests.clone();
        let upstream = axum::Router::new()
            .route(
                "/v1/usage",
                axum::routing::get(move || {
                    let requests = usage_requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        axum::Json(json!({"balance": 0.74182673, "remaining": 0.74182673}))
                    }
                }),
            )
            .route(
                "/api/usage/token",
                axum::routing::get(move || {
                    let requests = fallback_requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "unexpected fallback",
                        )
                    }
                }),
            );
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

        let provider_id = "built-in-balance";
        let dir = std::env::temp_dir().join(format!(
            "atoapi-built-in-balance-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = AppConfig::default();
        let mut provider = test_provider(provider_id);
        provider.base_url = format!("http://{address}/v1");
        config.providers.push(provider);
        config
            .upsert_provider_key_pool(
                provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 3,
                    recovery_minutes: 10,
                    keys: vec![ProviderKeyInput {
                        id: Some("balance-current-key".to_string()),
                        alias: Some("balance current".to_string()),
                        key: Some("balance-probe-secret".to_string()),
                        enabled: true,
                        priority: 0,
                        status: ProviderKeyStatus::Healthy,
                        total_requests: 4,
                        successes: 4,
                        failures: 0,
                        last_checked_at: None,
                        last_error: None,
                        disabled_until: None,
                    }],
                },
            )
            .unwrap();
        let before = toml::to_string(&config).unwrap();
        let state = AppState::for_test(
            config,
            dir.join("config.toml"),
            crate::cache::CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();

        let result = probe_provider_balance_inner(&state, provider_id)
            .await
            .unwrap();
        assert!(result.supported);
        assert!(result.ok);
        assert_eq!(result.key_id.as_deref(), Some("balance-current-key"));
        assert_eq!(result.balance.as_deref(), Some("0.74182673"));
        assert_eq!(result.message, "balance_received:v1_usage");
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "a recognized Sub2 usage response must stop the bounded profile sequence"
        );
        assert_eq!(
            toml::to_string(&*state.config.read().await).unwrap(),
            before
        );
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("balance-probe-secret"));
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn explicit_health_probe_checks_sse_completion_once_per_key_without_config_mutation() {
        use crate::config::{
            KeyLoadBalanceStrategy, ProviderKeyInput, ProviderKeyPoolInput, ProviderKeyStatus,
        };
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_for_responses = requests.clone();
        let requests_for_chat = requests.clone();
        let upstream = axum::Router::new()
            .route(
                "/v1/responses",
                axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                    let requests = requests_for_responses.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        let model = body.get("model").and_then(Value::as_str).unwrap_or_default();
                        if model == "blocked-model" {
                            return (
                                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                                "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"blocked\"}}\n\n".to_string(),
                            );
                        }
                        if model == "large-first-event" {
                            return (
                                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                                format!(
                                    "event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_large\",\"metadata\":\"{}\"}}}}\n\n",
                                    "x".repeat(24_000)
                                ),
                            );
                        }
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"content\":[{\"text\":\"probe-response\"}]}]}}\n\n".to_string(),
                        )
                    }
                }),
            )
            .route(
                "/v1/chat/completions",
                axum::routing::post(move || {
                    let requests = requests_for_chat.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                            "data: {\"choices\":[{\"delta\":{\"content\":\"probe-response\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n",
                        )
                    }
                }),
            );
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

        let dir = std::env::temp_dir().join(format!(
            "atoapi-health-probe-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let provider_id = "health-probe";
        let mut config = AppConfig::default();
        let mut provider = test_provider(provider_id);
        provider.base_url = format!("http://{address}/v1");
        provider.api_key_encrypted = Some("legacy-connection-secret".to_string());
        config.providers.push(provider);
        config
            .upsert_provider_key_pool(
                provider_id,
                ProviderKeyPoolInput {
                    enabled: true,
                    strategy: KeyLoadBalanceStrategy::Sequential,
                    failure_threshold: 3,
                    recovery_minutes: 10,
                    keys: vec![ProviderKeyInput {
                        id: Some("probe-key".to_string()),
                        alias: Some("测活 Key".to_string()),
                        key: Some("probe-secret".to_string()),
                        enabled: true,
                        priority: 5,
                        status: ProviderKeyStatus::Healthy,
                        total_requests: 9,
                        successes: 9,
                        failures: 0,
                        last_checked_at: None,
                        last_error: None,
                        disabled_until: None,
                    }],
                },
            )
            .unwrap();
        let before = toml::to_string(&config).unwrap();
        let state = AppState::for_test(
            config,
            dir.join("config.toml"),
            crate::cache::CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();

        let responses = probe_provider_health_inner(
            &state,
            ProviderHealthProbeInput {
                provider_id: provider_id.to_string(),
                key_ids: Vec::new(),
                target: ProviderHealthProbeTarget::AllEnabled,
                model: "health-model".to_string(),
                mode: ProviderHealthProbeMode::ResponsesStreaming,
                prompt: Some("private probe prompt".to_string()),
            },
        )
        .await
        .unwrap();
        assert_eq!(responses.results.len(), 1);
        assert!(responses.results[0].ok);
        assert_eq!(responses.results[0].key_id.as_deref(), Some("probe-key"));
        assert_eq!(
            responses.results[0].response_preview.as_deref(),
            Some("probe-response")
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        let serialized = serde_json::to_string(&responses).unwrap();
        assert!(!serialized.contains("probe-secret"));
        assert!(!serialized.contains("private probe prompt"));

        let chat = probe_provider_health_inner(
            &state,
            ProviderHealthProbeInput {
                provider_id: provider_id.to_string(),
                key_ids: Vec::new(),
                target: ProviderHealthProbeTarget::AllEnabled,
                model: "health-model".to_string(),
                mode: ProviderHealthProbeMode::ChatStreaming,
                prompt: None,
            },
        )
        .await
        .unwrap();
        assert!(chat.results[0].ok);
        assert_eq!(requests.load(Ordering::SeqCst), 2);

        let large = probe_provider_health_inner(
            &state,
            ProviderHealthProbeInput {
                provider_id: provider_id.to_string(),
                key_ids: Vec::new(),
                target: ProviderHealthProbeTarget::AllEnabled,
                model: "large-first-event".to_string(),
                mode: ProviderHealthProbeMode::ResponsesStreaming,
                prompt: None,
            },
        )
        .await
        .unwrap();
        assert!(large.results[0].ok);
        assert_eq!(large.results[0].message, "stream_accepted");
        assert_eq!(requests.load(Ordering::SeqCst), 3);

        let blocked = probe_provider_health_inner(
            &state,
            ProviderHealthProbeInput {
                provider_id: provider_id.to_string(),
                key_ids: Vec::new(),
                target: ProviderHealthProbeTarget::AllEnabled,
                model: "blocked-model".to_string(),
                mode: ProviderHealthProbeMode::ResponsesStreaming,
                prompt: None,
            },
        )
        .await
        .unwrap();
        assert!(!blocked.results[0].ok);
        assert_eq!(blocked.results[0].message, "upstream_stream_error");
        assert_eq!(requests.load(Ordering::SeqCst), 4);

        let after = {
            let config = state.config.read().await;
            toml::to_string(&*config).unwrap()
        };
        assert_eq!(
            after, before,
            "explicit probes must not mutate Key routing state"
        );

        let balance = probe_provider_balance_inner(&state, provider_id)
            .await
            .unwrap();
        assert!(!balance.supported);
        assert!(!balance.ok);
        assert_eq!(balance.message, "balance_api_not_detected");
        assert_eq!(
            requests.load(Ordering::SeqCst),
            4,
            "the health-route fixture only counts its three matching POST requests"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_ui_patch_failure_becomes_a_non_blocking_notice() {
        let notice = codex_ui_patch_notice(true, Err(anyhow!("node unavailable")));

        assert!(notice.contains("代理注入已热更新"));
        assert!(notice.contains("不会影响 Responses 代理注入"));
        assert!(notice.contains("node unavailable"));
    }

    #[test]
    fn disabled_codex_injection_still_returns_the_ui_restart_notice() {
        let mut results = Vec::new();

        attach_codex_ui_patch_status(
            &mut results,
            false,
            "Codex UI 恢复补丁需要重启 Codex 后生效".to_string(),
        );

        assert_eq!(results.len(), 1);
        assert!(!results[0].enabled);
        assert!(results[0].status.contains("自动注入已关闭"));
        assert!(results[0].status.contains("需要重启 Codex"));
    }

    #[test]
    fn parses_openai_style_models() {
        let value = json!({
            "data": [
                { "id": "gpt-5.5", "context_window": 800000 },
                { "id": "glm-5" }
            ]
        });
        let models = parse_models(value);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "gpt-5.5");
        assert_eq!(models[0].context_window, Some(800000));
    }

    #[test]
    fn parses_reasoning_capabilities_without_extra_probe() {
        let explicit = parse_model(&json!({
            "id": "provider-model",
            "supported_reasoning_efforts": ["low", "high", "ultra"]
        }))
        .unwrap();
        assert_eq!(
            explicit.supported_reasoning_efforts,
            vec!["low", "high", "ultra"]
        );

        let unspecified = parse_model(&json!({ "id": "gpt-5.6" })).unwrap();
        assert!(unspecified.supported_reasoning_efforts.is_empty());
    }

    #[test]
    fn refreshes_enabled_agent_catalog_after_bound_model_update() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-agent-model-refresh-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let config_path = dir.join("config.toml");
        let mut config = AppConfig::default();
        let mut provider = test_provider("share");
        provider.models = vec![ModelConfig {
            id: "vendor/gpt-custom".to_string(),
            request_model_id: Some("gpt-custom".to_string()),
            display_name: "GPT Custom".to_string(),
            context_window: Some(128_000),
            output_window: None,
            reasoning_effort_override_enabled: false,
            reasoning_effort: None,
            supported_reasoning_efforts: Vec::new(),
            supports_tools: true,
            supports_streaming: true,
            enabled: true,
        }];
        config.providers = vec![provider];
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("vendor/gpt-custom".to_string());
        codex.target_path = Some(config_path.clone());

        refresh_enabled_injections_for_provider(&mut config, "share").unwrap();
        config.providers[0].models[0].context_window = Some(256_000);
        refresh_enabled_injections_for_provider(&mut config, "share").unwrap();

        let parsed: toml::Value =
            toml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        let catalog_path = parsed
            .get("model_catalog_json")
            .and_then(toml::Value::as_str)
            .unwrap();
        let catalog: Value =
            serde_json::from_str(&std::fs::read_to_string(catalog_path).unwrap()).unwrap();
        let custom = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["slug"] == "gpt-custom")
            .unwrap();
        assert_eq!(custom["context_window"], 256_000);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn builds_zai_model_candidates() {
        let candidates =
            model_endpoint_candidates("https://api.z.ai/api/anthropic", false, None).unwrap();
        assert_eq!(
            candidates,
            vec![
                "https://api.z.ai/api/anthropic/v1/models",
                "https://api.z.ai/v1/models",
                "https://api.z.ai/models",
            ]
        );
    }

    #[test]
    fn builds_v1_model_candidates_for_user_gateway() {
        let candidates =
            model_endpoint_candidates("https://520.yunshuzhilian.asia/v1", false, None).unwrap();
        assert_eq!(candidates, vec!["https://520.yunshuzhilian.asia/v1/models"]);
    }

    #[test]
    fn model_url_override_wins() {
        let candidates = model_endpoint_candidates(
            "https://api.example.com/anthropic",
            false,
            Some("https://api.example.com/custom/models"),
        )
        .unwrap();
        assert_eq!(candidates, vec!["https://api.example.com/custom/models"]);
    }

    #[test]
    fn full_url_derives_v1_models() {
        let candidates =
            model_endpoint_candidates("https://proxy.example.com/v1/chat/completions", true, None)
                .unwrap();
        assert_eq!(candidates, vec!["https://proxy.example.com/v1/models"]);
    }

    #[tokio::test]
    async fn provider_network_path_diagnostic_uses_one_saved_endpoint_and_key_without_mutation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen_headers = Arc::new(tokio::sync::Mutex::new(Vec::<(String, String)>::new()));
        let seen_headers_for_app = seen_headers.clone();
        let upstream_app = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let seen_headers = seen_headers_for_app.clone();
                async move {
                    let authorization = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let user_agent = headers
                        .get(axum::http::header::USER_AGENT)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    seen_headers.lock().await.push((authorization, user_agent));
                    axum::Json(json!({ "data": [{ "id": "diagnostic-model" }] }))
                }
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, upstream_app).await.unwrap();
        });

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-network-path-diagnostic-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        let mut config = AppConfig::default();
        let mut provider = test_provider("network-diagnostic");
        provider.base_url = format!("http://{address}/v1");
        provider.custom_user_agent = Some("AtoapiNetworkDiagnosticTest/1.0".to_string());
        provider.api_key_encrypted = Some("diagnostic-secret".to_string());
        config.providers.push(provider);
        let config_before = toml::to_string(&config).unwrap();
        let state = AppState::for_test(
            config,
            config_dir.join("config.toml"),
            crate::cache::CacheStore::load(config_dir.join("cache.bin")).unwrap(),
        )
        .unwrap();

        let result = diagnose_provider_network_paths_inner(&state, "network-diagnostic")
            .await
            .unwrap();

        assert_eq!(result.provider_id, "network-diagnostic");
        assert_eq!(result.target_url, format!("http://{address}/v1/models"));
        assert_eq!(result.paths.len(), 2);
        assert_eq!(result.paths[0].path, "direct");
        assert_eq!(result.paths[1].path, "system-proxy");
        assert!(result.paths.iter().all(|path| path.ok));
        assert!(result
            .paths
            .iter()
            .all(|path| path.status == Some(200) && path.error.is_none()));
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("diagnostic-secret"));

        let connection_test = test_provider_connection_paths_inner(
            &state,
            &ProviderKeyTestInput {
                provider_id: Some("network-diagnostic".to_string()),
                key_id: None,
                api_key: None,
                base_url: format!("http://{address}/v1"),
                models_url: None,
                is_full_url: false,
                custom_user_agent: Some("AtoapiNetworkDiagnosticTest/1.0".to_string()),
                channel: Channel::Responses,
                use_system_proxy: false,
            },
        )
        .await
        .unwrap();
        assert!(connection_test.ok);
        assert_eq!(connection_test.paths.len(), 2);
        assert!(connection_test
            .paths
            .iter()
            .any(|path| path.path == "direct" && path.ok));
        assert!(connection_test
            .paths
            .iter()
            .any(|path| path.path == "system-proxy" && path.ok));
        let selected_path = if connection_test.recommended_use_system_proxy {
            "system-proxy"
        } else {
            "direct"
        };
        assert!(connection_test
            .paths
            .iter()
            .any(|path| path.path == selected_path && path.ok));

        let config_after = {
            let config = state.config.read().await;
            toml::to_string(&*config).unwrap()
        };
        assert_eq!(config_after, config_before);

        let seen_headers = seen_headers.lock().await;
        assert_eq!(seen_headers.len(), 4);
        assert!(seen_headers.iter().all(|(authorization, user_agent)| {
            authorization == "Bearer diagnostic-secret"
                && user_agent == "AtoapiNetworkDiagnosticTest/1.0"
        }));

        std::fs::remove_dir_all(config_dir).ok();
    }

    #[tokio::test]
    async fn provider_network_path_diagnostic_falls_back_to_next_common_models_endpoint() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let first_candidate_hits = Arc::new(AtomicUsize::new(0));
        let fallback_candidate_hits = Arc::new(AtomicUsize::new(0));
        let first_candidate_hits_for_app = first_candidate_hits.clone();
        let fallback_candidate_hits_for_app = fallback_candidate_hits.clone();
        let upstream_app = axum::Router::new()
            .route(
                "/api/anthropic/v1/models",
                axum::routing::get(move || {
                    let hits = first_candidate_hits_for_app.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        axum::Json(json!({ "data": [] }))
                    }
                }),
            )
            .route(
                "/v1/models",
                axum::routing::get(move || {
                    let hits = fallback_candidate_hits_for_app.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        axum::Json(json!({ "data": [{ "id": "fallback-model" }] }))
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(listener, upstream_app).await.unwrap();
        });

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-network-path-diagnostic-fallback-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        let mut config = AppConfig::default();
        let mut provider = test_provider("network-diagnostic-fallback");
        provider.base_url = format!("http://{address}/api/anthropic");
        provider.api_key_encrypted = Some("diagnostic-secret".to_string());
        config.providers.push(provider);
        let state = AppState::for_test(
            config,
            config_dir.join("config.toml"),
            crate::cache::CacheStore::load(config_dir.join("cache.bin")).unwrap(),
        )
        .unwrap();

        let result = diagnose_provider_network_paths_inner(&state, "network-diagnostic-fallback")
            .await
            .unwrap();

        assert_eq!(result.target_url, format!("http://{address}/v1/models"));
        assert!(result.paths.iter().all(|path| path.ok));
        assert_eq!(first_candidate_hits.load(Ordering::SeqCst), 2);
        assert_eq!(fallback_candidate_hits.load(Ordering::SeqCst), 2);

        std::fs::remove_dir_all(config_dir).ok();
    }

    #[test]
    fn fastest_connection_path_prefers_the_fastest_successful_route() {
        let attempt = |path: &str, ok: bool, elapsed_ms: u64| ProviderNetworkPathAttempt {
            result: ProviderNetworkPathResult {
                path: path.to_string(),
                ok,
                status: ok.then_some(200),
                elapsed_ms,
                http_version: None,
                remote_addr: None,
                error: (!ok).then_some("failed".to_string()),
            },
            has_valid_model_list: ok,
            models_count: if ok { 1 } else { 0 },
        };

        let direct = attempt("direct", true, 120);
        let system_proxy = attempt("system-proxy", true, 80);
        assert_eq!(fastest_connection_path(&direct, &system_proxy), Some(true));

        let direct = attempt("direct", true, 80);
        let system_proxy = attempt("system-proxy", true, 80);
        assert_eq!(fastest_connection_path(&direct, &system_proxy), Some(false));

        let direct = attempt("direct", false, 1);
        let system_proxy = attempt("system-proxy", true, 200);
        assert_eq!(fastest_connection_path(&direct, &system_proxy), Some(true));

        let direct = attempt("direct", false, 1);
        let system_proxy = attempt("system-proxy", false, 1);
        assert_eq!(fastest_connection_path(&direct, &system_proxy), None);
    }

    #[test]
    fn deleting_one_agent_clone_keeps_other_agent_and_source_provider() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        let codex_provider =
            clone_provider_for_agent_config(&mut config, "codex", "shared", None).unwrap();
        let opencode_provider =
            clone_provider_for_agent_config(&mut config, "opencode", "shared", None).unwrap();

        assert_ne!(codex_provider, opencode_provider);
        delete_provider_config(&mut config, &codex_provider, Some("codex")).unwrap();

        assert!(config
            .providers
            .iter()
            .any(|provider| provider.id == "shared"));
        assert!(!config
            .providers
            .iter()
            .any(|provider| provider.id == codex_provider));
        assert!(config
            .providers
            .iter()
            .any(|provider| provider.id == opencode_provider));
        assert_eq!(
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == "opencode")
                .and_then(|agent| agent.provider_id.as_deref()),
            Some(opencode_provider.as_str())
        );
        let codex = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        assert_eq!(codex.hidden_provider_ids, vec!["shared"]);
    }

    #[test]
    fn deleting_shared_provider_from_agent_only_detaches_that_agent() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        for agent_id in ["codex", "opencode"] {
            let agent = config
                .agent_injections
                .iter_mut()
                .find(|agent| agent.id == agent_id)
                .unwrap();
            agent.enabled = true;
            agent.provider_id = Some("shared".to_string());
        }

        delete_provider_config(&mut config, "shared", Some("codex")).unwrap();

        assert!(config
            .providers
            .iter()
            .any(|provider| provider.id == "shared"));
        let codex = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        let opencode = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "opencode")
            .unwrap();
        assert!(!codex.enabled);
        assert!(codex.provider_id.is_none());
        assert_eq!(codex.hidden_provider_ids, vec!["shared"]);
        assert_eq!(opencode.provider_id.as_deref(), Some("shared"));
    }

    #[test]
    fn deleting_unbound_shared_provider_from_agent_does_not_touch_other_agents() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("daoge"));
        let opencode = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "opencode")
            .unwrap();
        opencode.enabled = true;
        opencode.provider_id = Some("daoge".to_string());

        delete_provider_config(&mut config, "daoge", Some("codex")).unwrap();

        assert!(config
            .providers
            .iter()
            .any(|provider| provider.id == "daoge"));
        assert_eq!(
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == "opencode")
                .and_then(|agent| agent.provider_id.as_deref()),
            Some("daoge")
        );
        let codex = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        assert_eq!(codex.hidden_provider_ids, vec!["daoge"]);
        let opencode = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "opencode")
            .unwrap();
        assert!(opencode.hidden_provider_ids.is_empty());
    }

    #[test]
    fn selecting_hidden_shared_provider_restores_it_only_for_that_agent() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.hidden_provider_ids.push("shared".to_string());

        let provider_id =
            clone_provider_for_agent_config(&mut config, "codex", "shared", None).unwrap();

        let codex = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        assert!(codex.hidden_provider_ids.is_empty());
        assert_eq!(codex.provider_id.as_deref(), Some(provider_id.as_str()));
    }

    #[test]
    fn failed_agent_route_switch_does_not_leave_private_provider_records() {
        let mut config = AppConfig::default();
        config.providers.push(test_provider("shared"));
        let agent = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        agent.kind = crate::config::AgentInjectionKind::Gemini;
        agent.enabled = true;
        agent.provider_id = Some("shared".to_string());

        let before = toml::to_string(&config).unwrap();
        let result = stage_agent_injection_route_update(
            &config,
            AgentInjectionRouteUpdate {
                id: "codex".to_string(),
                provider_id: Some("shared".to_string()),
                model_id: None,
            },
        );

        assert!(result.is_err());
        assert_eq!(toml::to_string(&config).unwrap(), before);
        assert_eq!(config.providers.len(), 1);
        assert!(config.provider_key_pools.is_empty());
        assert!(config.provider_compact_modes.is_empty());
        assert!(config.provider_channel_modes.is_empty());
        assert!(config.provider_cache_capabilities.is_empty());
        assert!(config.agent_provider_orders.is_empty());
    }
}
