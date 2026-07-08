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
    app_config_dir, normalize_agent_injections, AgentInjectionConfig, AgentInjectionKind,
    AppConfig, ProviderConfig,
};

const CODEX_PROVIDER_ID: &str = "atoapi";
const PROXY_TOKEN_PLACEHOLDER: &str = "PROXY_MANAGED";
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
        item.model_id = default_agent_model(&default_provider).map(ToOwned::to_owned);
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

fn default_agent_model(provider: &ProviderConfig) -> Option<&str> {
    provider
        .models
        .iter()
        .find(|model| model.enabled)
        .or_else(|| provider.models.first())
        .map(|model| model.id.as_str())
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
    let Some(item) = config
        .agent_injections
        .iter_mut()
        .find(|item| item.id == id)
    else {
        return Err(anyhow!("agent injection {id} was not found"));
    };
    item.enabled = enabled;
    if enabled && item.provider_id.is_none() {
        if let Some(provider) = default_provider.as_ref() {
            item.provider_id = Some(provider.id.clone());
            item.model_id = default_agent_model(provider).map(ToOwned::to_owned);
        }
    }
    if !enabled {
        item.last_status = Some("已关闭自动注入".to_string());
        item.last_injected_at = Some(Utc::now());
        return Ok(Vec::new());
    }
    apply_one_by_id(config, id)
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

    config.agent_injections[index].provider_id = provider_id;
    config.agent_injections[index].model_id = model_id;

    if config.agent_injections[index].enabled {
        apply_one_by_id(config, &input.id)
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
        AgentInjectionKind::ProxyMode => {
            let target = item.target_path.clone().unwrap_or_else(|| {
                app_config_dir()
                    .unwrap_or_else(|_| home_dir().join(".atoapi"))
                    .join("atoapi-proxy-mode.json")
            });
            let backup = backup_file(&target)?;
            write_proxy_mode_profile(&target, context)?;
            (Some(target), backup, "代理模式配置已生成".to_string())
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
        let host = if config.host == "0.0.0.0" {
            "127.0.0.1"
        } else {
            config.host.as_str()
        };
        let base = format!("http://{}:{}", host, config.port);
        let configured_provider_id = item
            .and_then(|item| item.provider_id.as_deref())
            .or(config.active_provider_id.as_deref());
        let provider = configured_provider_id
            .as_deref()
            .and_then(|id| config.providers.iter().find(|provider| provider.id == id))
            .or_else(|| config.providers.iter().find(|provider| provider.enabled));
        let configured_model_id = item.and_then(|item| item.model_id.as_deref());
        let model = provider
            .and_then(|provider| {
                configured_model_id
                    .and_then(|model_id| provider.models.iter().find(|model| model.id == model_id))
                    .or_else(|| provider.models.iter().find(|model| model.enabled))
            })
            .map(|model| model.id.clone())
            .unwrap_or_else(|| "gpt-5.2".to_string());

        Self {
            anthropic_base_url: base.clone(),
            openai_base_url: format!("{base}/v1"),
            codex_base_url: format!("{base}/codex/v1"),
            local_key: item
                .map(|item| {
                    if item.kind == AgentInjectionKind::Codex {
                        PROXY_TOKEN_PLACEHOLDER.to_string()
                    } else {
                        agent_local_key(&config.local_key, &item.id)
                    }
                })
                .unwrap_or_else(|| config.local_key.clone()),
            default_channel: config.default_channel.label().to_string(),
            default_model: model,
        }
    }
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
    doc["model"] = value(context.default_model.as_str());
    doc["disable_response_storage"] = value(true);

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

fn write_json_pretty(path: &Path, value: &Value) -> Result<()> {
    write_text(path, &format!("{}\n", serde_json::to_string_pretty(value)?))
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
        };

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
        assert!(parsed.get("mcp_servers").is_some());
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
    fn ensure_defaults_removes_proxy_mode_from_agent_injections() {
        let mut config = AppConfig::default();

        ensure_defaults(&mut config);

        assert!(config
            .agent_injections
            .iter()
            .all(|item| item.id != "proxy-mode" && item.kind != AgentInjectionKind::ProxyMode));
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
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                display_name: "gpt-5.5".to_string(),
                context_window: Some(128000),
                output_window: None,
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
    fn enabled_agent_without_provider_defaults_to_active_provider() {
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
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                display_name: "gpt-5.5".to_string(),
                context_window: Some(128000),
                output_window: None,
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
        assert_eq!(codex.model_id.as_deref(), Some("gpt-5.5"));
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
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
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
}
