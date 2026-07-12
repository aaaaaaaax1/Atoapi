use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};
use toml_edit::{value, DocumentMut};

use crate::config::{
    app_config_dir, codex_model_alias, model_request_alias, normalize_agent_injections,
    AgentInjectionConfig, AgentInjectionKind, AppConfig, ModelConfig, ProviderConfig,
};

const CODEX_PROVIDER_ID: &str = "custom";
const CODEX_MODEL_CATALOG_FILE: &str = "atoapi-model-catalog.json";
const OFFICIAL_CODEX_MODELS_JSON: &str = include_str!("../resources/codex-models.json");
const CLAUDE_DESKTOP_PROFILE_ID: &str = "00000000-0000-4000-8000-000000345600";
const CLAUDE_DESKTOP_PROFILE_NAME: &str = "Atoapi";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInjectionUpdate {
    pub id: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInjectionRouteUpdate {
    pub id: String,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInjectionResult {
    pub id: String,
    pub label: String,
    pub enabled: bool,
    pub target_path: Option<PathBuf>,
    pub backup_path: Option<PathBuf>,
    pub status: String,
    pub injected_at: String,
}

#[derive(Debug, Clone)]
struct InjectionContext {
    anthropic_base_url: String,
    openai_base_url: String,
    codex_base_url: String,
    local_key: String,
    default_channel: String,
    default_model: String,
    default_model_is_explicit: bool,
    model_context_window: Option<u32>,
    codex_models: Vec<ModelConfig>,
}

pub fn ensure_defaults(config: &mut AppConfig) {
    normalize_agent_injections(&mut config.agent_injections);
    ensure_enabled_agents_have_provider(config);
}

fn ensure_enabled_agents_have_provider(config: &mut AppConfig) {
    let Some(default_provider) = default_agent_provider(config).cloned() else {
        return;
    };
    for item in config
        .agent_injections
        .iter_mut()
        .filter(|item| item.enabled)
    {
        if item.provider_id.is_some() {
            continue;
        }
        item.provider_id = Some(default_provider.id.clone());
        item.last_status = Some(format!("已默认绑定 {}", default_provider.name));
    }
}

fn default_agent_provider(config: &AppConfig) -> Option<&ProviderConfig> {
    config
        .active_provider_id
        .as_deref()
        .and_then(|id| {
            config
                .providers
                .iter()
                .find(|provider| provider.id == id && provider.enabled)
        })
        .or_else(|| config.providers.iter().find(|provider| provider.enabled))
        .or_else(|| config.providers.first())
}

pub fn set_enabled(
    config: &mut AppConfig,
    id: &str,
    enabled: bool,
) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let default_provider = if enabled {
        default_agent_provider(config).cloned()
    } else {
        None
    };
    let Some(index) = config
        .agent_injections
        .iter()
        .position(|item| item.id == id)
    else {
        return Err(anyhow!("agent injection {id} was not found"));
    };
    let previous = config.agent_injections[index].clone();
    {
        let item = &mut config.agent_injections[index];
        item.enabled = enabled;
        if enabled && item.provider_id.is_none() {
            if let Some(provider) = default_provider.as_ref() {
                item.provider_id = Some(provider.id.clone());
            }
        }
        if !enabled {
            item.last_status = Some("已关闭自动注入".to_string());
            item.last_injected_at = Some(Utc::now());
            return Ok(Vec::new());
        }
    }
    match apply_one_by_id(config, id) {
        Ok(results) => Ok(results),
        Err(err) => {
            config.agent_injections[index] = previous;
            Err(err)
        }
    }
}
pub fn apply_one_by_id(config: &mut AppConfig, id: &str) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let Some(index) = config
        .agent_injections
        .iter()
        .position(|item| item.id == id)
    else {
        return Err(anyhow!("agent injection {id} was not found"));
    };
    let context = InjectionContext::from_config(config, Some(&config.agent_injections[index]));
    let result = apply_item(&config.agent_injections[index], &context)?;
    {
        let item = &mut config.agent_injections[index];
        item.enabled = true;
        item.target_path = result.target_path.clone();
        item.last_injected_at = Some(Utc::now());
        item.last_status = Some(result.status.clone());
    }
    Ok(vec![result])
}

pub fn apply_enabled(config: &mut AppConfig) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let ids = config
        .agent_injections
        .iter()
        .filter(|item| item.enabled)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let mut results = Vec::new();
    for id in ids {
        let Some(index) = config
            .agent_injections
            .iter()
            .position(|item| item.id == id)
        else {
            continue;
        };
        let context = InjectionContext::from_config(config, Some(&config.agent_injections[index]));
        let result = apply_item(&config.agent_injections[index], &context)?;
        let item = &mut config.agent_injections[index];
        item.target_path = result.target_path.clone();
        item.last_injected_at = Some(Utc::now());
        item.last_status = Some(result.status.clone());
        results.push(result);
    }
    Ok(results)
}

pub fn update_route(
    config: &mut AppConfig,
    input: AgentInjectionRouteUpdate,
) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let Some(index) = config
        .agent_injections
        .iter()
        .position(|item| item.id == input.id)
    else {
        return Err(anyhow!("agent injection {} was not found", input.id));
    };

    let provider_id = clean_optional(input.provider_id);
    let model_id = clean_optional(input.model_id);
    if let Some(provider_id) = provider_id.as_deref() {
        let Some(provider) = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .cloned()
        else {
            return Err(anyhow!("provider {provider_id} was not found"));
        };
        if let Some(model_id) = model_id.as_deref() {
            if !provider.models.iter().any(|model| model.id == model_id) {
                return Err(anyhow!(
                    "model {model_id} was not found in provider {}",
                    provider.name
                ));
            }
        }
    }

    let previous = config.agent_injections[index].clone();
    config.agent_injections[index].provider_id = provider_id;
    config.agent_injections[index].model_id = model_id;

    if config.agent_injections[index].enabled {
        match apply_one_by_id(config, &input.id) {
            Ok(results) => Ok(results),
            Err(err) => {
                config.agent_injections[index] = previous;
                Err(err)
            }
        }
    } else {
        Ok(Vec::new())
    }
}

fn apply_item(
    item: &AgentInjectionConfig,
    context: &InjectionContext,
) -> Result<AgentInjectionResult> {
    let started = Utc::now();
    let (target_path, backup_path, status) = match item.kind {
        AgentInjectionKind::ClaudeCode => {
            let target = item
                .target_path
                .clone()
                .unwrap_or_else(|| home_dir().join(".claude").join("settings.json"));
            let backup = backup_file(&target)?;
            write_claude_code_settings(&target, context)?;
            (
                Some(target),
                backup,
                "Claude Code 已注入本地 Anthropic 中转".to_string(),
            )
        }
        AgentInjectionKind::Codex => {
            let target = item
                .target_path
                .clone()
                .unwrap_or_else(|| home_dir().join(".codex").join("config.toml"));
            let backup = backup_file(&target)?;
            write_codex_config(&target, context)?;
            (
                Some(target),
                backup,
                "Codex 已注入本地 Responses 中转".to_string(),
            )
        }
        AgentInjectionKind::ClaudeDesktop => {
            let paths = claude_desktop_paths();
            let targets = [
                &paths.normal_config_path,
                &paths.threep_config_path,
                &paths.profile_path,
                &paths.meta_path,
            ];
            let backups = targets
                .iter()
                .map(|path| Ok(((*path).to_path_buf(), backup_file(path)?)))
                .collect::<Result<Vec<_>>>()?;
            if let Err(err) = write_claude_desktop(&paths, context) {
                let _ = restore_backups(&backups);
                return Err(err);
            }
            (
                Some(paths.profile_path),
                backups.iter().find_map(|(_, backup)| backup.clone()),
                "Claude Desktop 3P Profile 已注入本地网关".to_string(),
            )
        }
        AgentInjectionKind::Gemini => {
            return Err(anyhow!(
                "Gemini injection requires a native Gemini generateContent endpoint; Atoapi currently exposes OpenAI/Anthropic/Responses proxy endpoints only"
            ));
        }
        AgentInjectionKind::OpenCode => {
            let target = item
                .target_path
                .clone()
                .unwrap_or_else(opencode_config_path);
            let backup = backup_file(&target)?;
            write_opencode_config(&target, context)?;
            (
                Some(target),
                backup,
                "OpenCode injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::OpenClaw => {
            let target = item
                .target_path
                .clone()
                .unwrap_or_else(openclaw_config_path);
            let backup = backup_file(&target)?;
            write_openclaw_config(&target, context)?;
            (
                Some(target),
                backup,
                "OpenClaw injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::Hermes => {
            let target = item.target_path.clone().unwrap_or_else(hermes_config_path);
            let backup = backup_file(&target)?;
            write_hermes_config(&target, context)?;
            (
                Some(target),
                backup,
                "Hermes injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::ProxyMode => {
            let target = item.target_path.clone().unwrap_or_else(|| {
                app_config_dir()
                    .unwrap_or_else(|_| home_dir().join(".atoapi"))
                    .join("atoapi-proxy-mode.json")
            });
            let backup = backup_file(&target)?;
            write_proxy_mode_profile(&target, context)?;
            (Some(target), backup, "本地代理模式配置已生成".to_string())
        }
        AgentInjectionKind::Unknown => {
            return Err(anyhow!(
                "unsupported agent injection kind for {}",
                item.label
            ));
        }
    };

    Ok(AgentInjectionResult {
        id: item.id.clone(),
        label: item.label.clone(),
        enabled: true,
        target_path,
        backup_path,
        status,
        injected_at: started.to_rfc3339(),
    })
}

impl InjectionContext {
    fn from_config(config: &AppConfig, item: Option<&AgentInjectionConfig>) -> Self {
        let use_proxy_mode_address = item
            .map(|item| item.kind == AgentInjectionKind::ProxyMode)
            .unwrap_or(false);
        let source_host = if use_proxy_mode_address {
            config.proxy_mode_host.as_str()
        } else {
            config.host.as_str()
        };
        let source_port = if use_proxy_mode_address {
            config.proxy_mode_port
        } else {
            config.port
        };
        let host = if source_host == "0.0.0.0" {
            "127.0.0.1"
        } else {
            source_host
        };
        let base = format!("http://{}:{}", host, source_port);
        let configured_provider_id = item
            .and_then(|item| item.provider_id.as_deref())
            .or(config.active_provider_id.as_deref());
        let provider = configured_provider_id
            .as_deref()
            .and_then(|id| config.providers.iter().find(|provider| provider.id == id))
            .or_else(|| config.providers.iter().find(|provider| provider.enabled));
        let configured_model_id = item.and_then(|item| item.model_id.as_deref());
        let explicit_model_config = provider.and_then(|provider| {
            configured_model_id.and_then(|model_id| {
                provider
                    .models
                    .iter()
                    .find(|model| injection_model_matches(model, model_id))
            })
        });
        let model_config = explicit_model_config.or_else(|| {
            provider.and_then(|provider| provider.models.iter().find(|model| model.enabled))
        });
        let model = model_config
            .map(|model| model.id.clone())
            .unwrap_or_else(|| "gpt-5.2".to_string());
        let agent_model = model_config
            .and_then(model_request_alias)
            .or_else(|| codex_model_alias(&model))
            .unwrap_or_else(|| model.clone());
        let codex_models = provider
            .map(|provider| {
                provider
                    .models
                    .iter()
                    .filter(|model| model.enabled)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Self {
            anthropic_base_url: base.clone(),
            openai_base_url: format!("{base}/v1"),
            codex_base_url: format!("{base}/codex/v1"),
            local_key: item
                .map(|item| agent_local_key(&config.local_key, &item.id))
                .unwrap_or_else(|| config.local_key.clone()),
            default_channel: config.default_channel.label().to_string(),
            default_model: agent_model,
            default_model_is_explicit: explicit_model_config.is_some(),
            model_context_window: model_config.and_then(|model| model.context_window),
            codex_models,
        }
    }
}

fn injection_model_matches(model: &crate::config::ModelConfig, requested: &str) -> bool {
    let requested = requested.trim();
    if model.id == requested {
        return true;
    }
    if model_request_alias(model)
        .map(|alias| alias == requested || alias.eq_ignore_ascii_case(requested))
        .unwrap_or(false)
    {
        return true;
    }
    let requested_lower = requested.to_ascii_lowercase();
    codex_model_alias(&model.id)
        .map(|alias| alias == requested_lower)
        .unwrap_or(false)
}

pub(crate) fn agent_local_key(local_key: &str, agent_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(local_key.as_bytes());
    hasher.update(b"\0atoapi-agent\0");
    hasher.update(agent_id.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("ato-agent-{}", &digest[..32])
}

fn clean_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn write_claude_code_settings(path: &Path, context: &InjectionContext) -> Result<()> {
    let mut value = read_json_or_empty(path)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("Claude Code settings must be a JSON object"))?;
    let env = object.entry("env".to_string()).or_insert_with(|| json!({}));
    if !env.is_object() {
        *env = json!({});
    }
    let env = env
        .as_object_mut()
        .ok_or_else(|| anyhow!("Claude Code env must be a JSON object"))?;
    env.insert(
        "ANTHROPIC_BASE_URL".to_string(),
        Value::String(context.anthropic_base_url.clone()),
    );
    env.insert(
        "ANTHROPIC_AUTH_TOKEN".to_string(),
        Value::String(context.local_key.clone()),
    );
    env.insert(
        "ANTHROPIC_API_KEY".to_string(),
        Value::String(context.local_key.clone()),
    );
    env.insert(
        "ANTHROPIC_MODEL".to_string(),
        Value::String(context.default_model.clone()),
    );
    write_json_pretty(path, &value)
}

fn write_codex_config(path: &Path, context: &InjectionContext) -> Result<()> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut doc = if text.trim().is_empty() {
        DocumentMut::new()
    } else {
        text.parse::<DocumentMut>()
            .map_err(|err| anyhow!("Codex config.toml parse error: {err}"))?
    };

    doc["model_provider"] = value(CODEX_PROVIDER_ID);
    doc["disable_response_storage"] = value(true);
    let model_catalog_path = write_codex_model_catalog(path, context)?;
    doc["model_catalog_json"] = value(model_catalog_path.to_string_lossy().as_ref());
    if context.default_model_is_explicit {
        doc["model"] = value(context.default_model.as_str());
        if let Some(context_window) = context.model_context_window.filter(|value| *value > 0) {
            doc["model_context_window"] = value(i64::from(context_window));
        } else {
            doc.as_table_mut().remove("model_context_window");
        }
        if let Some(reasoning_effort) = context
            .codex_models
            .iter()
            .find(|model| injection_model_matches(model, &context.default_model))
            .filter(|model| model.reasoning_effort_override_enabled)
            .and_then(|model| model.reasoning_effort.as_deref())
            .and_then(crate::config::normalize_reasoning_effort)
        {
            doc["model_reasoning_effort"] = value(reasoning_effort);
        }
    }

    if !doc.as_table().contains_key("model_providers") {
        doc["model_providers"] = toml_edit::table();
    }
    if let Some(model_providers) = doc["model_providers"].as_table_mut() {
        if !model_providers.contains_key(CODEX_PROVIDER_ID) {
            model_providers[CODEX_PROVIDER_ID] = toml_edit::table();
        }
        let provider = model_providers[CODEX_PROVIDER_ID]
            .as_table_mut()
            .ok_or_else(|| anyhow!("model_providers.atoapi must be a table"))?;
        provider["name"] = value("Atoapi");
        provider["base_url"] = value(context.codex_base_url.as_str());
        provider["wire_api"] = value("responses");
        provider["requires_openai_auth"] = value(true);
        provider["experimental_bearer_token"] = value(context.local_key.as_str());
    }

    write_text(path, &doc.to_string())
}

fn write_codex_model_catalog(config_path: &Path, context: &InjectionContext) -> Result<PathBuf> {
    let mut catalog = serde_json::from_str::<Value>(OFFICIAL_CODEX_MODELS_JSON)
        .context("bundled Codex model catalog is invalid")?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("bundled Codex model catalog has no models array"))?;
    let fallback_template = models
        .iter()
        .find(|model| model.get("slug").and_then(Value::as_str) == Some("gpt-5.2"))
        .or_else(|| models.first())
        .cloned()
        .ok_or_else(|| anyhow!("bundled Codex model catalog is empty"))?;
    let mut priority = models
        .iter()
        .filter_map(|model| model.get("priority").and_then(Value::as_i64))
        .max()
        .unwrap_or(100);

    for model in context.codex_models.iter().filter(|model| model.enabled) {
        let slug = model_request_alias(model)
            .or_else(|| codex_model_alias(&model.id))
            .unwrap_or_else(|| model.id.trim().to_string());
        if slug.is_empty() {
            continue;
        }
        let actual_slug =
            codex_model_alias(&model.id).unwrap_or_else(|| model.id.trim().to_ascii_lowercase());
        let official_template = models
            .iter()
            .find(|entry| {
                entry
                    .get("slug")
                    .and_then(Value::as_str)
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(&actual_slug))
            })
            .cloned();
        let inherits_official_capabilities = official_template.is_some();
        let template = official_template.unwrap_or_else(|| fallback_template.clone());
        priority += 1;
        let catalog_model = codex_catalog_model(
            &template,
            model,
            &slug,
            priority,
            inherits_official_capabilities,
        );
        if let Some(index) = models.iter().position(|entry| {
            entry
                .get("slug")
                .and_then(Value::as_str)
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(&slug))
        }) {
            models[index] = catalog_model;
        } else {
            models.push(catalog_model);
        }
    }

    let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    let catalog_path = parent.join(CODEX_MODEL_CATALOG_FILE);
    let catalog_path = if catalog_path.is_absolute() {
        catalog_path
    } else {
        std::env::current_dir()
            .context("failed to resolve Codex model catalog path")?
            .join(catalog_path)
    };
    write_json_pretty(&catalog_path, &catalog)?;
    Ok(catalog_path)
}

fn codex_catalog_model(
    template: &Value,
    model: &ModelConfig,
    slug: &str,
    priority: i64,
    inherits_official_capabilities: bool,
) -> Value {
    let mut catalog_model = template.clone();
    catalog_model["slug"] = json!(slug);
    let display_name = if model_request_alias(model).is_some() {
        slug
    } else {
        let configured = model.display_name.trim();
        if configured.is_empty() {
            slug
        } else {
            configured
        }
    };
    catalog_model["display_name"] = json!(display_name);
    catalog_model["description"] = json!("Model supplied by the active Atoapi upstream.");
    catalog_model["visibility"] = json!("list");
    catalog_model["supported_in_api"] = json!(true);
    catalog_model["priority"] = json!(priority);
    catalog_model["availability_nux"] = Value::Null;
    catalog_model["upgrade"] = Value::Null;
    catalog_model["auto_review_model_override"] = Value::Null;
    if !inherits_official_capabilities {
        catalog_model["use_responses_lite"] = json!(false);
        catalog_model["multi_agent_version"] = Value::Null;
        catalog_model["additional_speed_tiers"] = json!([]);
        catalog_model["service_tiers"] = json!([]);
        catalog_model["default_service_tier"] = Value::Null;
        catalog_model["comp_hash"] = Value::Null;
        catalog_model["auto_compact_token_limit"] = Value::Null;
    }
    catalog_model["supports_parallel_tool_calls"] = json!(model.supports_tools);
    if let Some(context_window) = model.context_window.filter(|value| *value > 0) {
        catalog_model["context_window"] = json!(context_window);
        catalog_model["max_context_window"] = json!(context_window);
    }

    let supported =
        if inherits_official_capabilities && model.supported_reasoning_efforts.is_empty() {
            catalog_model["supported_reasoning_levels"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        } else {
            model
                .supported_reasoning_efforts
                .iter()
                .filter_map(|effort| crate::config::normalize_reasoning_effort(effort))
                .map(|effort| {
                    json!({
                        "effort": effort,
                        "description": format!("Use {effort} reasoning for this model")
                    })
                })
                .collect::<Vec<_>>()
        };
    let default_effort = model
        .reasoning_effort_override_enabled
        .then_some(model.reasoning_effort.as_deref())
        .flatten()
        .and_then(crate::config::normalize_reasoning_effort)
        .filter(|effort| {
            supported
                .iter()
                .any(|item| item.get("effort").and_then(Value::as_str) == Some(effort.as_str()))
        })
        .or_else(|| {
            inherits_official_capabilities
                .then(|| {
                    catalog_model
                        .get("default_reasoning_level")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .flatten()
        })
        .or_else(|| {
            model
                .reasoning_effort
                .as_deref()
                .and_then(crate::config::normalize_reasoning_effort)
                .filter(|effort| {
                    supported.iter().any(|item| {
                        item.get("effort").and_then(Value::as_str) == Some(effort.as_str())
                    })
                })
        })
        .or_else(|| {
            supported
                .iter()
                .find(|item| item.get("effort").and_then(Value::as_str) == Some("medium"))
                .and_then(|item| item.get("effort"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            supported
                .first()
                .and_then(|item| item.get("effort"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });
    let supports_reasoning = !supported.is_empty();
    catalog_model["supported_reasoning_levels"] = Value::Array(supported);
    catalog_model["default_reasoning_level"] =
        default_effort.map(Value::String).unwrap_or(Value::Null);
    catalog_model["supports_reasoning_summaries"] = json!(supports_reasoning);
    catalog_model
}

fn write_opencode_config(path: &Path, context: &InjectionContext) -> Result<()> {
    let mut value = read_json5_or_empty(path)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenCode config must be a JSON object"))?;
    object
        .entry("$schema".to_string())
        .or_insert_with(|| Value::String("https://opencode.ai/config.json".to_string()));
    let provider = object
        .entry("provider".to_string())
        .or_insert_with(|| json!({}));
    if !provider.is_object() {
        *provider = json!({});
    }
    provider
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenCode provider must be a JSON object"))?
        .insert(
            CODEX_PROVIDER_ID.to_string(),
            opencode_provider_value(context),
        );
    write_json_pretty(path, &value)
}

fn opencode_provider_value(context: &InjectionContext) -> Value {
    json!({
        "npm": "@ai-sdk/openai-compatible",
        "name": "Atoapi",
        "options": {
            "baseURL": context.openai_base_url.clone(),
            "apiKey": context.local_key.clone()
        },
        "models": {
            context.default_model.clone(): {
                "name": context.default_model.clone()
            }
        }
    })
}

fn write_openclaw_config(path: &Path, context: &InjectionContext) -> Result<()> {
    let mut value = read_json5_or_empty(path)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw config must be a JSON object"))?;

    let models = object
        .entry("models".to_string())
        .or_insert_with(|| json!({}));
    if !models.is_object() {
        *models = json!({});
    }
    let models = models
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw models must be a JSON object"))?;
    models.insert("mode".to_string(), Value::String("merge".to_string()));
    let providers = models
        .entry("providers".to_string())
        .or_insert_with(|| json!({}));
    if !providers.is_object() {
        *providers = json!({});
    }
    providers
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw models.providers must be a JSON object"))?
        .insert(
            CODEX_PROVIDER_ID.to_string(),
            openclaw_provider_value(context),
        );

    let agents = object
        .entry("agents".to_string())
        .or_insert_with(|| json!({}));
    if !agents.is_object() {
        *agents = json!({});
    }
    let defaults = agents
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw agents must be a JSON object"))?
        .entry("defaults".to_string())
        .or_insert_with(|| json!({}));
    if !defaults.is_object() {
        *defaults = json!({});
    }
    let model = defaults
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw agents.defaults must be a JSON object"))?
        .entry("model".to_string())
        .or_insert_with(|| json!({}));
    if !model.is_object() {
        *model = json!({});
    }
    model
        .as_object_mut()
        .ok_or_else(|| anyhow!("OpenClaw agents.defaults.model must be a JSON object"))?
        .insert(
            "primary".to_string(),
            Value::String(format!("{}/{}", CODEX_PROVIDER_ID, context.default_model)),
        );

    write_json_pretty(path, &value)
}

fn openclaw_provider_value(context: &InjectionContext) -> Value {
    json!({
        "baseUrl": context.openai_base_url.clone(),
        "apiKey": context.local_key.clone(),
        "api": "openai-compatible",
        "models": [
            {
                "id": context.default_model.clone(),
                "name": context.default_model.clone()
            }
        ]
    })
}

fn write_hermes_config(path: &Path, context: &InjectionContext) -> Result<()> {
    let mut value = read_hermes_yaml_or_empty(path)?;
    let root = value
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("Hermes config must be a YAML mapping"))?;

    let mut provider = hermes_provider_value(context);
    let providers_key = yaml_string("custom_providers");
    let providers = root
        .entry(providers_key)
        .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()));
    if !providers.is_sequence() {
        *providers = serde_yaml::Value::Sequence(Vec::new());
    }
    let providers = providers
        .as_sequence_mut()
        .ok_or_else(|| anyhow!("Hermes custom_providers must be a YAML sequence"))?;
    if let Some(existing) = providers
        .iter_mut()
        .find(|item| item.get("name").and_then(|name| name.as_str()) == Some(CODEX_PROVIDER_ID))
    {
        merge_missing_yaml_fields(&mut provider, existing);
        *existing = provider;
    } else {
        providers.push(provider);
    }

    let model_key = yaml_string("model");
    let model = root
        .entry(model_key)
        .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    if !model.is_mapping() {
        *model = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    }
    let model = model
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("Hermes model must be a YAML mapping"))?;
    model.insert(yaml_string("provider"), yaml_string(CODEX_PROVIDER_ID));
    model.insert(yaml_string("default"), yaml_string(&context.default_model));

    write_hermes_yaml(path, &value)
}

fn hermes_provider_value(context: &InjectionContext) -> serde_yaml::Value {
    let mut models = serde_yaml::Mapping::new();
    models.insert(
        yaml_string(&context.default_model),
        serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
    );

    let mut provider = serde_yaml::Mapping::new();
    provider.insert(yaml_string("name"), yaml_string(CODEX_PROVIDER_ID));
    provider.insert(
        yaml_string("base_url"),
        yaml_string(&context.openai_base_url),
    );
    provider.insert(yaml_string("api_key"), yaml_string(&context.local_key));
    provider.insert(yaml_string("api_mode"), yaml_string("chat_completions"));
    provider.insert(yaml_string("model"), yaml_string(&context.default_model));
    provider.insert(yaml_string("models"), serde_yaml::Value::Mapping(models));
    serde_yaml::Value::Mapping(provider)
}

fn merge_missing_yaml_fields(next: &mut serde_yaml::Value, existing: &serde_yaml::Value) {
    let (Some(next), Some(existing)) = (next.as_mapping_mut(), existing.as_mapping()) else {
        return;
    };
    for (key, value) in existing {
        next.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

fn write_proxy_mode_profile(path: &Path, context: &InjectionContext) -> Result<()> {
    let value = json!({
        "name": "Atoapi 代理模式",
        "updatedAt": Utc::now().to_rfc3339(),
        "localKey": context.local_key.clone(),
        "defaultChannel": context.default_channel.clone(),
        "defaultModel": context.default_model.clone(),
        "env": {
            "ANTHROPIC_BASE_URL": context.anthropic_base_url.clone(),
            "ANTHROPIC_AUTH_TOKEN": context.local_key.clone(),
            "ANTHROPIC_API_KEY": context.local_key.clone(),
            "ANTHROPIC_MODEL": context.default_model.clone(),
            "OPENAI_BASE_URL": context.openai_base_url.clone(),
            "OPENAI_API_KEY": context.local_key.clone(),
            "OPENAI_MODEL": context.default_model.clone(),
            "API_KEY": context.local_key.clone()
        },
        "headers": {
            "Authorization": format!("Bearer {}", context.local_key),
            "x-api-key": context.local_key.clone()
        }
    });
    write_json_pretty(path, &value)
}

#[derive(Debug, Clone)]
struct ClaudeDesktopPaths {
    normal_config_path: PathBuf,
    threep_config_path: PathBuf,
    profile_path: PathBuf,
    meta_path: PathBuf,
}

fn claude_desktop_paths() -> ClaudeDesktopPaths {
    let local_app_data = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join("AppData").join("Local"));
    let normal_dir = pick_windows_claude_dir(&local_app_data, false)
        .unwrap_or_else(|| local_app_data.join("Claude"));
    let threep_dir = pick_windows_claude_dir(&local_app_data, true)
        .unwrap_or_else(|| local_app_data.join("Claude-3p"));
    let config_library = threep_dir.join("configLibrary");
    ClaudeDesktopPaths {
        normal_config_path: normal_dir.join("claude_desktop_config.json"),
        threep_config_path: threep_dir.join("claude_desktop_config.json"),
        profile_path: config_library.join(format!("{CLAUDE_DESKTOP_PROFILE_ID}.json")),
        meta_path: config_library.join("_meta.json"),
    }
}

fn pick_windows_claude_dir(local_app_data: &Path, threep: bool) -> Option<PathBuf> {
    let exact = local_app_data.join(if threep { "Claude-3p" } else { "Claude" });
    if exact.exists() {
        return Some(exact);
    }
    let mut candidates = fs::read_dir(local_app_data)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                return false;
            };
            name.starts_with("Claude") && name.contains("-3p") == threep
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.into_iter().next()
}

fn write_claude_desktop(paths: &ClaudeDesktopPaths, context: &InjectionContext) -> Result<()> {
    write_deployment_mode(&paths.normal_config_path, "3p")?;
    write_deployment_mode(&paths.threep_config_path, "3p")?;

    let profile = json!({
        "coworkEgressAllowedHosts": ["*"],
        "disableDeploymentModeChooser": true,
        "inferenceGatewayApiKey": context.local_key.clone(),
        "inferenceGatewayAuthScheme": "bearer",
        "inferenceGatewayBaseUrl": context.anthropic_base_url.clone(),
        "inferenceProvider": "gateway",
        "inferenceModels": [
            { "name": "claude-sonnet-4-6", "labelOverride": context.default_model.clone(), "supports1m": true },
            { "name": "claude-opus-4-8", "labelOverride": context.default_model.clone(), "supports1m": true },
            { "name": "claude-haiku-4-5", "labelOverride": context.default_model.clone(), "supports1m": true }
        ]
    });
    write_json_pretty(&paths.profile_path, &profile)?;
    write_claude_desktop_meta(&paths.meta_path)
}

fn write_deployment_mode(path: &Path, mode: &str) -> Result<()> {
    let mut value = read_json_or_empty(path)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("Claude Desktop config must be a JSON object"))?;
    object.insert(
        "deploymentMode".to_string(),
        Value::String(mode.to_string()),
    );
    write_json_pretty(path, &value)
}

fn write_claude_desktop_meta(path: &Path) -> Result<()> {
    let mut value = read_json_or_empty(path)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("Claude Desktop meta must be a JSON object"))?;
    let entries = object
        .entry("entries".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !entries.is_array() {
        *entries = Value::Array(Vec::new());
    }
    let entries = entries
        .as_array_mut()
        .ok_or_else(|| anyhow!("Claude Desktop meta entries must be an array"))?;
    entries
        .retain(|entry| entry.get("id").and_then(Value::as_str) != Some(CLAUDE_DESKTOP_PROFILE_ID));
    entries.push(json!({
        "id": CLAUDE_DESKTOP_PROFILE_ID,
        "name": CLAUDE_DESKTOP_PROFILE_NAME,
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": Utc::now().to_rfc3339()
    }));
    object.insert(
        "appliedId".to_string(),
        Value::String(CLAUDE_DESKTOP_PROFILE_ID.to_string()),
    );
    write_json_pretty(path, &value)
}

fn restore_backups(items: &[(PathBuf, Option<PathBuf>)]) -> Result<()> {
    for (original, backup) in items {
        match backup {
            Some(path) => {
                if let Some(parent) = original.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(path, original)?;
            }
            None => {
                if original.exists() {
                    fs::remove_file(original).ok();
                }
            }
        }
    }
    Ok(())
}

fn backup_file(path: &Path) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let backup_dir = app_config_dir()?
        .join("backups")
        .join("injections")
        .join(Utc::now().format("%Y%m%d-%H%M%S%.3f").to_string());
    fs::create_dir_all(&backup_dir)?;
    let file_name = path
        .to_string_lossy()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    let backup = backup_dir.join(file_name);
    fs::copy(path, &backup).with_context(|| format!("failed to back up {}", path.display()))?;
    Ok(Some(backup))
}

fn read_json_or_empty(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(json!({}));
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(json!({}));
    }
    let value = serde_json::from_str::<Value>(&text)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if value.is_object() {
        Ok(value)
    } else {
        Ok(json!({}))
    }
}

fn read_json5_or_empty(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(json!({}));
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(json!({}));
    }
    let value = json5::from_str::<Value>(&text)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if value.is_object() {
        Ok(value)
    } else {
        Ok(json!({}))
    }
}

fn read_hermes_yaml_or_empty(path: &Path) -> Result<serde_yaml::Value> {
    if !path.exists() {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    }
    serde_yaml::from_str::<serde_yaml::Value>(&text)
        .with_context(|| format!("failed to parse {}", path.display()))
}

fn write_json_pretty(path: &Path, value: &Value) -> Result<()> {
    write_text(path, &format!("{}\n", serde_json::to_string_pretty(value)?))
}

fn write_hermes_yaml(path: &Path, value: &serde_yaml::Value) -> Result<()> {
    write_text(path, &serde_yaml::to_string(value)?)
}

fn yaml_string(value: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_string())
}

fn write_text(path: &Path, text: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, text)?;
    fs::rename(&tmp, path)
        .or_else(|_| {
            fs::remove_file(path).ok();
            fs::rename(&tmp, path)
        })
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn opencode_config_path() -> PathBuf {
    home_dir()
        .join(".config")
        .join("opencode")
        .join("opencode.json")
}

fn openclaw_config_path() -> PathBuf {
    home_dir().join(".openclaw").join("openclaw.json")
}

fn hermes_config_path() -> PathBuf {
    hermes_config_dir().join("config.yaml")
}

fn hermes_config_dir() -> PathBuf {
    if let Some(raw) = std::env::var_os("HERMES_HOME") {
        let value = raw.to_string_lossy().trim().to_string();
        if !value.is_empty() {
            return PathBuf::from(value);
        }
    }

    #[cfg(target_os = "windows")]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home_dir().join("AppData").join("Local"))
            .join("hermes")
    }

    #[cfg(not(target_os = "windows"))]
    {
        home_dir().join(".hermes")
    }
}

fn home_dir() -> PathBuf {
    dirs::home_dir()
        .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_injection_preserves_other_tables() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        write_text(
            &path,
            r#"model = "old"

[mcp_servers.context7]
command = "npx"
"#,
        )
        .unwrap();
        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-test".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-test".to_string(),
            default_model_is_explicit: true,
            model_context_window: Some(128_000),
            codex_models: vec![ModelConfig {
                id: "vendor/gpt-custom".to_string(),
                request_model_id: Some("gpt-custom".to_string()),
                display_name: "GPT Custom".to_string(),
                context_window: Some(256_000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: vec![
                    "low".to_string(),
                    "high".to_string(),
                    "ultra".to_string(),
                ],
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
        };

        write_codex_config(&path, &context).unwrap();
        write_codex_config(&path, &context).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        let parsed: toml::Value = toml::from_str(&text).unwrap();

        assert_eq!(
            parsed.get("model_provider").and_then(toml::Value::as_str),
            Some(CODEX_PROVIDER_ID)
        );
        assert_eq!(
            parsed
                .get("model_providers")
                .and_then(|value| value.get(CODEX_PROVIDER_ID))
                .and_then(|value| value.get("base_url"))
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:18883/codex/v1")
        );
        assert_eq!(
            parsed
                .get("model_context_window")
                .and_then(toml::Value::as_integer),
            Some(128_000)
        );
        assert!(parsed.get("mcp_servers").is_some());
        let catalog_path = parsed
            .get("model_catalog_json")
            .and_then(toml::Value::as_str)
            .map(PathBuf::from)
            .expect("Codex injection should write model_catalog_json");
        assert!(catalog_path.is_absolute());
        let catalog: Value =
            serde_json::from_str(&fs::read_to_string(&catalog_path).unwrap()).unwrap();
        let models = catalog["models"].as_array().unwrap();
        let sol = models
            .iter()
            .find(|model| model["slug"] == "gpt-5.6-sol")
            .expect("official GPT-5.6 Sol should be present");
        assert!(sol["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|level| level["effort"] == "ultra"));
        assert!(sol["service_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier["id"] == "priority" && tier["name"] == "Fast"));
        assert!(sol["additional_speed_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier == "fast"));
        let terra = models
            .iter()
            .find(|model| model["slug"] == "gpt-5.6-terra")
            .expect("official GPT-5.6 Terra should be present");
        assert!(terra["service_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier["id"] == "priority" && tier["name"] == "Fast"));
        assert!(terra["additional_speed_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier == "fast"));
        let luna = models
            .iter()
            .find(|model| model["slug"] == "gpt-5.6-luna")
            .expect("official GPT-5.6 Luna should be present");
        assert!(luna["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|level| level["effort"] == "max"));
        assert!(!luna["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|level| level["effort"] == "ultra"));
        assert!(luna["service_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier["id"] == "priority" && tier["name"] == "Fast"));
        assert!(luna["additional_speed_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier == "fast"));
        let custom = models
            .iter()
            .filter(|model| model["slug"] == "gpt-custom")
            .collect::<Vec<_>>();
        assert_eq!(custom.len(), 1);
        assert_eq!(custom[0]["context_window"], 256_000);
        assert!(custom[0]["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|level| level["effort"] == "ultra"));
        assert!(custom[0]["service_tiers"].as_array().unwrap().is_empty());
        assert!(custom[0]["additional_speed_tiers"]
            .as_array()
            .unwrap()
            .is_empty());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_injection_without_selected_model_preserves_existing_model_choice() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
            id: "torch".to_string(),
            name: "torch".to_string(),
            base_url: "https://torch.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
                id: "gpt-5.2".to_string(),
                request_model_id: None,
                display_name: "gpt-5.2".to_string(),
                context_window: Some(400_000),
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
        });
        {
            let codex = config
                .agent_injections
                .iter_mut()
                .find(|item| item.id == "codex")
                .unwrap();
            codex.provider_id = Some("torch".to_string());
            codex.model_id = None;
        }
        let codex = config
            .agent_injections
            .iter()
            .find(|item| item.id == "codex")
            .unwrap();
        let context = InjectionContext::from_config(&config, Some(codex));
        assert!(!context.default_model_is_explicit);

        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-preserve-model-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        write_text(
            &path,
            r#"model = "gpt-5.5"
model_reasoning_effort = "ultra"
model_context_window = 888000
"#,
        )
        .unwrap();

        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(
            parsed.get("model").and_then(toml::Value::as_str),
            Some("gpt-5.5")
        );
        assert_eq!(
            parsed
                .get("model_reasoning_effort")
                .and_then(toml::Value::as_str),
            Some("ultra")
        );
        assert_eq!(
            parsed
                .get("model_context_window")
                .and_then(toml::Value::as_integer),
            Some(888_000)
        );
        assert_eq!(
            parsed.get("model_provider").and_then(toml::Value::as_str),
            Some(CODEX_PROVIDER_ID)
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn proxy_mode_profile_contains_both_base_urls() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-mode-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("profile.json");
        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-test".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-test".to_string(),
            default_model_is_explicit: true,
            model_context_window: Some(128_000),
            codex_models: Vec::new(),
        };

        write_proxy_mode_profile(&path, &context).unwrap();
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            value["env"]["OPENAI_BASE_URL"],
            json!("http://127.0.0.1:18883/v1")
        );
        assert_eq!(
            value["env"]["ANTHROPIC_BASE_URL"],
            json!("http://127.0.0.1:18883")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn ensure_defaults_keeps_proxy_mode_as_visible_injection() {
        let mut config = AppConfig::default();

        ensure_defaults(&mut config);

        assert!(config
            .agent_injections
            .iter()
            .any(|item| item.id == "proxy-mode" && item.kind == AgentInjectionKind::ProxyMode));
    }
    #[test]
    fn injection_context_uses_agent_scoped_local_key() {
        let mut config = AppConfig::default();
        config.local_key = "ato-root-key".to_string();
        config.providers.push(ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(128000),
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
        });
        {
            let item = config
                .agent_injections
                .iter_mut()
                .find(|item| item.id == "claude-code")
                .unwrap();
            item.provider_id = Some("share".to_string());
            item.model_id = Some("gpt-5.5".to_string());
        }
        let item = config
            .agent_injections
            .iter()
            .find(|item| item.id == "claude-code")
            .unwrap();

        let context = InjectionContext::from_config(&config, Some(item));

        assert_ne!(context.local_key, "ato-root-key");
        assert_eq!(
            context.local_key,
            agent_local_key("ato-root-key", "claude-code")
        );
    }

    #[test]
    fn codex_injection_context_uses_agent_scoped_local_key() {
        let mut config = AppConfig::default();
        config.local_key = "ato-root-key".to_string();
        config.providers.push(ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "nc/gpt-5.6-sol".to_string(),
                request_model_id: Some("gpt-5.5".to_string()),
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(372000),
                output_window: None,
                reasoning_effort_override_enabled: true,
                reasoning_effort: Some("ultra".to_string()),
                supported_reasoning_efforts: vec![
                    "low".to_string(),
                    "medium".to_string(),
                    "high".to_string(),
                    "xhigh".to_string(),
                    "max".to_string(),
                    "ultra".to_string(),
                ],
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        {
            let item = config
                .agent_injections
                .iter_mut()
                .find(|item| item.id == "codex")
                .unwrap();
            item.provider_id = Some("share".to_string());
            item.model_id = Some("nc/gpt-5.6-sol".to_string());
        }
        let item = config
            .agent_injections
            .iter()
            .find(|item| item.id == "codex")
            .unwrap();

        let context = InjectionContext::from_config(&config, Some(item));

        assert_ne!(context.local_key, "PROXY_MANAGED");
        assert_eq!(context.local_key, agent_local_key("ato-root-key", "codex"));
        assert_eq!(context.default_model, "gpt-5.5");
        assert_eq!(context.model_context_window, Some(372_000));

        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-mapped-model-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed.get("model").and_then(toml::Value::as_str),
            Some("gpt-5.5")
        );
        assert_eq!(
            parsed
                .get("model_reasoning_effort")
                .and_then(toml::Value::as_str),
            Some("ultra")
        );

        let catalog_path = parsed
            .get("model_catalog_json")
            .and_then(toml::Value::as_str)
            .unwrap();
        let catalog: Value =
            serde_json::from_str(&std::fs::read_to_string(catalog_path).unwrap()).unwrap();
        let mapped = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["slug"] == "gpt-5.5")
            .unwrap();
        assert_eq!(mapped["display_name"], "gpt-5.5");
        assert_eq!(mapped["context_window"], 372_000);
        assert!(mapped["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .any(|level| level["effort"] == "ultra"));
        assert!(mapped["service_tiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tier| tier["id"] == "priority" && tier["name"] == "Fast"));
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn enabled_agent_without_provider_defaults_to_active_provider_without_forcing_model() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
            id: "torch".to_string(),
            name: "torch".to_string(),
            base_url: "https://torch.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(128000),
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
        });
        config.active_provider_id = Some("torch".to_string());
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|item| item.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = None;
        codex.model_id = None;

        ensure_defaults(&mut config);

        let codex = config
            .agent_injections
            .iter()
            .find(|item| item.id == "codex")
            .unwrap();
        assert_eq!(codex.provider_id.as_deref(), Some("torch"));
        assert_eq!(codex.model_id, None);
    }

    #[test]
    fn agent_routes_stay_independent_when_multiple_agents_are_enabled() {
        let mut config = AppConfig::default();
        for id in ["share", "torch"] {
            config.providers.push(ProviderConfig {
                id: id.to_string(),
                name: id.to_string(),
                base_url: format!("https://{id}.example/v1"),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: crate::config::Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
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
            });
        }
        update_route(
            &mut config,
            AgentInjectionRouteUpdate {
                id: "claude-code".to_string(),
                provider_id: Some("share".to_string()),
                model_id: Some("gpt-5.5".to_string()),
            },
        )
        .unwrap();
        update_route(
            &mut config,
            AgentInjectionRouteUpdate {
                id: "codex".to_string(),
                provider_id: Some("torch".to_string()),
                model_id: Some("gpt-5.5".to_string()),
            },
        )
        .unwrap();

        let claude_code = config
            .agent_injections
            .iter()
            .find(|item| item.id == "claude-code")
            .unwrap();
        let codex = config
            .agent_injections
            .iter()
            .find(|item| item.id == "codex")
            .unwrap();
        assert_eq!(claude_code.provider_id.as_deref(), Some("share"));
        assert_eq!(codex.provider_id.as_deref(), Some("torch"));
    }

    #[test]
    fn enabled_route_update_rolls_back_when_apply_fails() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
            id: "new".to_string(),
            name: "new".to_string(),
            base_url: "https://new.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-new".to_string(),
                request_model_id: None,
                display_name: "gpt-new".to_string(),
                context_window: Some(128000),
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
        });
        let gemini = config
            .agent_injections
            .iter_mut()
            .find(|item| item.id == "gemini")
            .unwrap();
        gemini.enabled = true;
        gemini.provider_id = Some("old".to_string());
        gemini.model_id = Some("gpt-old".to_string());

        let result = update_route(
            &mut config,
            AgentInjectionRouteUpdate {
                id: "gemini".to_string(),
                provider_id: Some("new".to_string()),
                model_id: Some("gpt-new".to_string()),
            },
        );

        assert!(result.is_err());
        let gemini = config
            .agent_injections
            .iter()
            .find(|item| item.id == "gemini")
            .unwrap();
        assert_eq!(gemini.provider_id.as_deref(), Some("old"));
        assert_eq!(gemini.model_id.as_deref(), Some("gpt-old"));
    }
    #[test]
    fn claude_code_injection_writes_selected_model() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-claude-code-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("settings.json");
        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-test".to_string(),
            default_channel: "anthropic".to_string(),
            default_model: "gpt-test".to_string(),
            default_model_is_explicit: true,
            model_context_window: Some(128_000),
            codex_models: Vec::new(),
        };

        write_claude_code_settings(&path, &context).unwrap();
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(value["env"]["ANTHROPIC_MODEL"], json!("gpt-test"));
        assert_eq!(
            value["env"]["ANTHROPIC_BASE_URL"],
            json!("http://127.0.0.1:18883")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn opencode_injection_writes_provider() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-opencode-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("opencode.json");
        let context = test_context();

        write_opencode_config(&path, &context).unwrap();

        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            value["provider"][CODEX_PROVIDER_ID]["options"]["baseURL"],
            json!("http://127.0.0.1:18883/v1")
        );
        assert_eq!(
            value["provider"][CODEX_PROVIDER_ID]["options"]["apiKey"],
            json!("ato-test")
        );
        assert!(value["provider"][CODEX_PROVIDER_ID]["models"]["gpt-test"].is_object());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn openclaw_injection_writes_provider_and_default_model() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-openclaw-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("openclaw.json");
        let context = test_context();

        write_openclaw_config(&path, &context).unwrap();

        let value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            value["models"]["providers"][CODEX_PROVIDER_ID]["baseUrl"],
            json!("http://127.0.0.1:18883/v1")
        );
        assert_eq!(
            value["models"]["providers"][CODEX_PROVIDER_ID]["apiKey"],
            json!("ato-test")
        );
        assert_eq!(
            value["agents"]["defaults"]["model"]["primary"],
            json!(format!("{}/{}", CODEX_PROVIDER_ID, "gpt-test"))
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn hermes_injection_writes_provider_and_model() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-hermes-inject-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.yaml");
        let context = test_context();

        write_hermes_config(&path, &context).unwrap();

        let value: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let providers = value
            .get("custom_providers")
            .and_then(serde_yaml::Value::as_sequence)
            .unwrap();
        let provider = providers
            .iter()
            .find(|item| {
                item.get("name").and_then(serde_yaml::Value::as_str) == Some(CODEX_PROVIDER_ID)
            })
            .unwrap();
        assert_eq!(
            provider.get("base_url").and_then(serde_yaml::Value::as_str),
            Some("http://127.0.0.1:18883/v1")
        );
        assert_eq!(
            provider.get("api_key").and_then(serde_yaml::Value::as_str),
            Some("ato-test")
        );
        assert_eq!(
            value
                .get("model")
                .and_then(|model| model.get("provider"))
                .and_then(serde_yaml::Value::as_str),
            Some(CODEX_PROVIDER_ID)
        );
        assert_eq!(
            value
                .get("model")
                .and_then(|model| model.get("default"))
                .and_then(serde_yaml::Value::as_str),
            Some("gpt-test")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn gemini_enable_rolls_back_until_native_endpoint_exists() {
        let mut config = AppConfig::default();
        let result = set_enabled(&mut config, "gemini", true);

        assert!(result.is_err());
        let gemini = config
            .agent_injections
            .iter()
            .find(|item| item.id == "gemini")
            .unwrap();
        assert!(!gemini.enabled);
    }

    fn test_context() -> InjectionContext {
        InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-test".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-test".to_string(),
            default_model_is_explicit: true,
            model_context_window: Some(128_000),
            codex_models: Vec::new(),
        }
    }
}
