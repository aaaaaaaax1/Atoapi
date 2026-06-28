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
    config::{CacheConfig, Channel, ModelConfig, ProviderInput, PublicConfig},
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
    for profile in config.route_profiles.iter_mut() {
        profile.provider_id = Some(provider.id.clone());
        profile.upstream_channel = provider.channel.clone();
    }
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn add_or_update_provider(
    state: State<'_, Arc<AppState>>,
    input: ProviderInput,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    let id = config.upsert_provider(input).map_err(to_command_error)?;
    if config.active_provider_id.is_none() {
        config.active_provider_id = Some(id);
    }
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
}

#[tauri::command]
pub async fn delete_provider(
    state: State<'_, Arc<AppState>>,
    provider_id: String,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    config
        .providers
        .retain(|provider| provider.id != provider_id);
    if config.active_provider_id.as_deref() == Some(provider_id.as_str()) {
        config.active_provider_id = config.providers.first().map(|provider| provider.id.clone());
    }
    for item in &mut config.agent_injections {
        if item.provider_id.as_deref() == Some(provider_id.as_str()) {
            item.provider_id = None;
            item.model_id = None;
        }
    }
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
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
        &state.client,
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

#[tauri::command]
pub async fn add_or_update_model(
    state: State<'_, Arc<AppState>>,
    input: ModelInput,
) -> CommandResult<PublicConfig> {
    let mut config = state.config.write().await;
    let provider = config
        .providers
        .iter_mut()
        .find(|provider| provider.id == input.provider_id)
        .ok_or_else(|| format!("provider {} was not found", input.provider_id))?;
    if let Some(model) = provider
        .models
        .iter_mut()
        .find(|model| model.id == input.model.id)
    {
        *model = input.model;
    } else {
        provider.models.push(input.model);
    }
    provider.updated_at = Utc::now();
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    drop(config);
    Ok(state.public_config().await)
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
    let mut config = state.config.write().await;
    let results = agent_injection::set_enabled(&mut config, &input.id, input.enabled)
        .map_err(to_command_error)?;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    Ok(results)
}

#[tauri::command]
pub async fn apply_agent_injection(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let mut config = state.config.write().await;
    let results = agent_injection::apply_one_by_id(&mut config, &id).map_err(to_command_error)?;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    Ok(results)
}

#[tauri::command]
pub async fn apply_enabled_agent_injections(
    state: State<'_, Arc<AppState>>,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let mut config = state.config.write().await;
    let results = agent_injection::apply_enabled(&mut config).map_err(to_command_error)?;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
    Ok(results)
}

#[tauri::command]
pub async fn update_agent_injection_route(
    state: State<'_, Arc<AppState>>,
    input: AgentInjectionRouteUpdate,
) -> CommandResult<Vec<AgentInjectionResult>> {
    let mut config = state.config.write().await;
    let results = agent_injection::update_route(&mut config, input).map_err(to_command_error)?;
    config.updated_at = Utc::now();
    config.save(&state.config_path).map_err(to_command_error)?;
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
        display_name,
        context_window,
        output_window: value
            .get("max_output_tokens")
            .or_else(|| value.get("output_window"))
            .and_then(Value::as_u64)
            .map(|value| value as u32),
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
}
