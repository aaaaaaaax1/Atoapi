use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tauri::State;

use crate::{
    agent_injection::{
        self, AgentInjectionResult, AgentInjectionRouteUpdate, AgentInjectionUpdate,
    },
    codex_ui_patch,
    config::{AppConfig, CacheConfig, Channel, ModelConfig, ProviderInput, PublicConfig},
    metrics::MetricsSnapshot,
    state::{AppState, ProxyStatus},
};

type CommandResult<T> = Result<T, String>;

const ERROR_BODY_MAX_CHARS: usize = 512;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProviderCloneInput {
    pub agent_id: String,
    pub provider_id: String,
    #[serde(default)]
    pub model_id: Option<String>,
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
    let mut config = state.config.write().await;
    config.host = input.host;
    config.port = input.port;
    config.local_key = input.local_key;
    config.default_channel = input.default_channel;
    config.workspace_fingerprint = input.workspace_fingerprint;
    if let Some(cache) = input.cache {
        config.cache = cache;
        config.cache.normalize_smart_max_hit();
    }
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
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
    {
        let mut config = state.config.write().await;
        config.proxy_mode_host = input.host.trim().to_string();
        config.proxy_mode_port = input.port;
        config.updated_at = Utc::now();
        config.save(&state.config_path).map_err(to_command_error)?;
    }
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
    let mut config = state.config.write().await;
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .ok_or_else(|| format!("provider {provider_id} was not found"))?
        .clone();
    config.active_provider_id = Some(provider.id.clone());
    config.default_channel = provider.channel.clone();
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn clone_provider_for_agent(
    state: State<'_, Arc<AppState>>,
    input: AgentProviderCloneInput,
) -> CommandResult<PublicConfig> {
    let agent_id = input.agent_id.trim().to_string();
    let source_provider_id = input.provider_id.trim().to_string();
    let should_start_proxy = {
        let mut config = state.config.write().await;
        clone_provider_for_agent_config(
            &mut config,
            &agent_id,
            &source_provider_id,
            input.model_id.as_deref(),
        )
        .map_err(to_command_error)?;
        let agent_index = config
            .agent_injections
            .iter()
            .position(|agent| agent.id == agent_id)
            .ok_or_else(|| format!("agent injection {agent_id} was not found"))?;
        let now = Utc::now();
        config.updated_at = now;
        config.save(&state.config_path).map_err(to_command_error)?;
        config.agent_injections[agent_index].enabled
    };

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
        if provider_belongs_to_agent(source_provider_id, agent_id) {
            source_provider_id.to_string()
        } else if let Some(existing) = config.providers.iter().find(|provider| {
            provider_clone_matches_source(&provider.id, source_provider_id, agent_id)
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
        .map(ToOwned::to_owned)
        .or_else(|| {
            target_provider
                .models
                .iter()
                .find(|model| model.enabled)
                .or_else(|| target_provider.models.first())
                .map(|model| model.id.clone())
        });

    let agent = &mut config.agent_injections[agent_index];
    agent.provider_id = Some(target_provider_id.clone());
    agent.model_id = selected_model;
    agent.last_status = Some("已绑定当前 Agent 独立上游".to_string());
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
    input: ProviderInput,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    let id = config.upsert_provider(input).map_err(to_command_error)?;
    if config.active_provider_id.is_none() {
        config.active_provider_id = Some(id.clone());
    }
    refresh_enabled_injections_for_provider(&mut config, &id).map_err(to_command_error)?;
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn delete_provider(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
    agent_id: Option<String>,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    delete_provider_config(&mut config, &provider_id, agent_id.as_deref())
        .map_err(to_command_error)?;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
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

        if !provider_belongs_to_agent(provider_id, agent_id) {
            if config.agent_injections[agent_index].provider_id.as_deref() == Some(provider_id) {
                let agent = &mut config.agent_injections[agent_index];
                agent.provider_id = None;
                agent.model_id = None;
                agent.enabled = false;
                agent.last_status =
                    Some("已从当前 Agent 移除共享上游，其他 Agent 不受影响".to_string());
                return Ok(());
            }
            return Err(anyhow::anyhow!(
                "shared provider {provider_id} is only a template in this Agent; select or edit it to create an independent copy"
            ));
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

        remove_provider_records(config, provider_id);
        let agent = &mut config.agent_injections[agent_index];
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
        .provider_channel_modes
        .retain(|item| item.provider_id != provider_id);
}

#[tauri::command]
pub async fn test_provider_key(
    state: State<'_, Arc<AppState>>,
    input: ProviderKeyTestInput,
) -> CommandResult<ProviderKeyTestResult> {
    let result = test_provider_key_inner(&state, &input).await;
    if let Some(provider_id) = input.provider_id.as_deref() {
        if let Some(key_id) = input.key_id.as_deref() {
            let mut config = state.config.write().await;
            match &result {
                Ok(result) if result.ok => {
                    config.mark_provider_key_success(provider_id, Some(key_id))
                }
                Ok(result) => config.mark_provider_key_failure(
                    provider_id,
                    Some(key_id),
                    &result.message,
                    true,
                ),
                Err(err) => config.mark_provider_key_failure(
                    provider_id,
                    Some(key_id),
                    &err.to_string(),
                    true,
                ),
            }
            config.save(&state.config_path).map_err(to_command_error)?;
        }
    }
    result.map_err(to_command_error)
}

#[tauri::command]
pub async fn test_provider_key_pool(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<Vec<ProviderKeyTestResult>> {
    let provider = {
        let config = state.config.read().await;
        config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .cloned()
            .ok_or_else(|| format!("provider {provider_id} was not found"))?
    };
    let key_ids = {
        let config = state.config.read().await;
        config
            .provider_key_pools
            .iter()
            .find(|pool| pool.provider_id == provider_id)
            .map(|pool| {
                pool.keys
                    .iter()
                    .map(|key| key.id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let mut results = Vec::new();
    for key_id in key_ids {
        let input = ProviderKeyTestInput {
            provider_id: Some(provider_id.clone()),
            key_id: Some(key_id),
            api_key: None,
            base_url: provider.base_url.clone(),
            models_url: provider.models_url.clone(),
            is_full_url: provider.is_full_url,
            custom_user_agent: provider.custom_user_agent.clone(),
            channel: provider.channel.clone(),
            use_system_proxy: provider.use_system_proxy,
        };
        let result = test_provider_key_inner(&state, &input)
            .await
            .map_err(to_command_error)?;
        {
            let mut config = state.config.write().await;
            if result.ok {
                config.mark_provider_key_success(&provider_id, input.key_id.as_deref());
            } else {
                config.mark_provider_key_failure(
                    &provider_id,
                    input.key_id.as_deref(),
                    &result.message,
                    true,
                );
            }
            config.save(&state.config_path).map_err(to_command_error)?;
        }
        results.push(result);
    }
    Ok(results)
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

    let models = fetch_models_from_upstream_with_options(
        state.upstream_client(input.use_system_proxy),
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

async fn test_provider_key_inner(
    state: &State<'_, Arc<AppState>>,
    input: &ProviderKeyTestInput,
) -> Result<ProviderKeyTestResult> {
    let mut upstream_secret = input
        .api_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
        .map(ToOwned::to_owned);
    if upstream_secret.is_none() {
        if let (Some(provider_id), Some(key_id)) =
            (input.provider_id.as_deref(), input.key_id.as_deref())
        {
            let config = state.config.read().await;
            upstream_secret = config
                .provider_key_secret(provider_id, key_id)
                .map_err(to_command_error)
                .map_err(anyhow::Error::msg)?;
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
    let models = fetch_models_from_upstream_with_options(
        state.upstream_client(input.use_system_proxy),
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
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
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
    let mut config = state.config.write().await;
    let provider = config
        .providers
        .iter_mut()
        .find(|provider| provider.id == provider_id)
        .ok_or_else(|| format!("provider {provider_id} was not found"))?;
    provider.models.retain(|model| model.id != model_id);
    provider.updated_at = Utc::now();
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
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

#[tauri::command]
pub async fn reload_config(state: State<'_, Arc<AppState>>) -> CommandResult<PublicConfig> {
    state.reload_config().await.map_err(to_command_error)
}

#[tauri::command]
pub async fn save_cache_policy(
    state: State<'_, Arc<AppState>>,
    mut input: CacheConfig,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    input.normalize_smart_max_hit();
    config.cache = input;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
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
    let mut config = state.config.write().await;
    agent_injection::ensure_defaults(&mut config);
    config.save(&state.config_path).map_err(to_command_error)?;
    Ok(config.agent_injections.clone())
}

#[tauri::command]
pub async fn set_agent_injection_enabled(
    state: State<'_, Arc<AppState>>,
    input: AgentInjectionUpdate,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let enabled = input.enabled;
    let agent_id = input.id.clone();
    let previous_enabled = {
        let config = state.config.read().await;
        config
            .agent_injections
            .iter()
            .find(|item| item.id == agent_id)
            .map(|item| item.enabled)
            .unwrap_or(false)
    };
    let patch_status = if agent_id == "codex" && previous_enabled != enabled {
        Some(codex_ui_patch::set_enabled(enabled).map_err(to_command_error)?)
    } else {
        None
    };
    let results = match async {
        let mut config = state.config.write().await;
        let results = agent_injection::set_enabled(&mut config, &input.id, input.enabled)
            .map_err(to_command_error)?;
        config.updated_at = Utc::now();
        config.save(&state.config_path).map_err(to_command_error)?;
        Ok::<_, String>(results)
    }
    .await
    {
        Ok(results) => results,
        Err(err) => {
            if agent_id == "codex" && previous_enabled != enabled {
                let _ = codex_ui_patch::set_enabled(previous_enabled);
            }
            return Err(err);
        }
    };
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
    let mut results = results;
    if let (Some(status), Some(result)) = (patch_status, results.first_mut()) {
        result.status = format!("{}；{}", result.status, status);
    }
    Ok(results)
}

#[tauri::command]
pub async fn apply_agent_injection(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let previous_enabled = {
        let config = state.config.read().await;
        config
            .agent_injections
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.enabled)
            .unwrap_or(false)
    };
    let patch_status = if id == "codex" {
        Some(codex_ui_patch::set_enabled(true).map_err(to_command_error)?)
    } else {
        None
    };
    let results = match async {
        let mut config = state.config.write().await;
        let results =
            agent_injection::apply_one_by_id(&mut config, &id).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        config.save(&state.config_path).map_err(to_command_error)?;
        Ok::<_, String>(results)
    }
    .await
    {
        Ok(results) => results,
        Err(err) => {
            if id == "codex" && !previous_enabled {
                let _ = codex_ui_patch::set_enabled(false);
            }
            return Err(err);
        }
    };
    if id == "proxy-mode" {
        state
            .start_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    } else {
        state.start_proxy().await.map_err(to_command_error)?;
    }
    let mut results = results;
    if let (Some(status), Some(result)) = (patch_status, results.first_mut()) {
        result.status = format!("{}；{}", result.status, status);
    }
    Ok(results)
}

#[tauri::command]
pub async fn apply_enabled_agent_injections(
    state: State<'_, Arc<AppState>>,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let codex_enabled = {
        let config = state.config.read().await;
        config
            .agent_injections
            .iter()
            .any(|item| item.id == "codex" && item.enabled)
    };
    let patch_status = if codex_enabled {
        Some(codex_ui_patch::set_enabled(true).map_err(to_command_error)?)
    } else {
        None
    };
    let results = {
        let mut config = state.config.write().await;
        let results = agent_injection::apply_enabled(&mut config).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        config.save(&state.config_path).map_err(to_command_error)?;
        results
    };
    if results.iter().any(|item| item.id == "proxy-mode") {
        state
            .start_proxy_mode_proxy()
            .await
            .map_err(to_command_error)?;
    }
    if results.iter().any(|item| item.id != "proxy-mode") {
        state.start_proxy().await.map_err(to_command_error)?;
    }
    let mut results = results;
    if let Some(status) = patch_status {
        if let Some(result) = results.iter_mut().find(|item| item.id == "codex") {
            result.status = format!("{}；{}", result.status, status);
        }
    }
    Ok(results)
}

#[tauri::command]
pub async fn update_agent_injection_route(
    state: State<'_, Arc<AppState>>,
    mut input: AgentInjectionRouteUpdate,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let agent_id = input.id.clone();
    let should_start_proxy = {
        let config = state.config.read().await;
        config
            .agent_injections
            .iter()
            .any(|item| item.id == input.id && item.enabled)
    };
    let results = {
        let mut config = state.config.write().await;
        if let Some(provider_id) = input.provider_id.clone() {
            if !provider_belongs_to_agent(&provider_id, &agent_id) {
                let private_provider_id = clone_provider_for_agent_config(
                    &mut config,
                    &agent_id,
                    &provider_id,
                    input.model_id.as_deref(),
                )
                .map_err(to_command_error)?;
                input.provider_id = Some(private_provider_id);
            }
        }
        let results =
            agent_injection::update_route(&mut config, input).map_err(to_command_error)?;
        config.updated_at = Utc::now();
        config.save(&state.config_path).map_err(to_command_error)?;
        results
    };
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
        let mut request = client.get(&url);
        if let Some(api_key) = api_key {
            request = request.bearer_auth(api_key);
            if matches!(channel, Channel::Anthropic) {
                request = request
                    .header("x-api-key", api_key)
                    .header("anthropic-version", "2023-06-01");
            }
        }
        if let Some(user_agent) = custom_user_agent {
            request = request.header(reqwest::header::USER_AGENT, user_agent);
        }
        match request.send().await {
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
        assert_eq!(opencode.provider_id.as_deref(), Some("shared"));
    }
}
