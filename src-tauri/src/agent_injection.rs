use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    net::IpAddr,
    path::{Component, Path, PathBuf},
};
use toml_edit::{value, DocumentMut};
use uuid::Uuid;

use crate::config::{
    app_config_dir, best_model_match, codex_model_alias, model_request_alias,
    normalize_agent_injections, normalize_reasoning_effort, AgentInjectionConfig,
    AgentInjectionKind, AppConfig, CodexContextSettings, ModelConfig,
};

const CODEX_PROVIDER_ID: &str = "custom";
const CODEX_MODEL_CATALOG_LEGACY_FILE: &str = "atoapi-model-catalog.json";
const CODEX_MODEL_CATALOG_FILE_PREFIX: &str = "atoapi-model-catalog-";
const CODEX_MODEL_CATALOG_ACTIVE_FILE: &str = "atoapi-model-catalog-active.json";
const CODEX_RESTORE_STATE_FILE: &str = "atoapi-codex-restore-state.json";
const CODEX_RESTORE_STATE_VERSION: u32 = 1;
const CODEX_MANAGED_ROOT_FIELDS: [&str; 8] = [
    "model_provider",
    "model_catalog_json",
    "disable_response_storage",
    "model",
    "model_reasoning_effort",
    "model_context_window",
    "model_auto_compact_token_limit",
    "model_auto_compact_token_limit_scope",
];
const CODEX_MANAGED_MODEL_FIELDS: [&str; 3] =
    ["model", "model_reasoning_effort", "model_context_window"];
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
    /// True when this application changed an injected configuration artifact.
    #[serde(default)]
    pub changed: bool,
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
    auto_compact_token_limit: Option<u32>,
    codex_context_settings: Option<CodexContextSettings>,
    codex_models: Vec<ModelConfig>,
    /// Raw model records discovered from the currently selected upstream.
    /// `None` means discovery did not complete, so the last managed catalog is
    /// retained instead of replacing it with an incomplete local list.
    discovered_codex_models: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexRestoreEnvelope {
    schema_version: u32,
    encrypted_payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexRestoreState {
    schema_version: u32,
    target_path: PathBuf,
    target_existed: bool,
    fragment_toml: String,
    /// v1 restore files predate some managed root keys. This explicit marker
    /// lets an upgrade capture only a previously user-owned new field once,
    /// rather than mistaking an Atoapi-written value for the user's original
    /// on every later injection refresh.
    #[serde(default)]
    managed_root_fields: Vec<String>,
    /// Encrypted ownership manifest for catalogs created by Atoapi. A file
    /// name alone is never enough evidence to remove a Codex catalog.
    #[serde(default)]
    catalog_manifest: CodexCatalogManifest,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CodexCatalogManifest {
    #[serde(default)]
    entries: Vec<CodexCatalogManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexCatalogManifestEntry {
    path: PathBuf,
    sha256: String,
}

pub fn ensure_defaults(config: &mut AppConfig) {
    normalize_agent_injections(&mut config.agent_injections);
}

pub fn set_enabled(
    config: &mut AppConfig,
    id: &str,
    enabled: bool,
) -> Result<Vec<AgentInjectionResult>> {
    set_enabled_with_discovered_models(config, id, enabled, None)
}

pub fn set_enabled_with_discovered_models(
    config: &mut AppConfig,
    id: &str,
    enabled: bool,
    discovered_models: Option<&[Value]>,
) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let Some(index) = config
        .agent_injections
        .iter()
        .position(|item| item.id == id)
    else {
        return Err(anyhow!("agent injection {id} was not found"));
    };
    let previous = config.agent_injections[index].clone();
    if !enabled {
        let target_path = remove_item(&previous)?;
        let item = &mut config.agent_injections[index];
        item.enabled = false;
        item.last_status = Some("已关闭自动注入并恢复本机 Agent 路由".to_string());
        item.last_injected_at = Some(Utc::now());
        return Ok(vec![AgentInjectionResult {
            id: item.id.clone(),
            label: item.label.clone(),
            enabled: false,
            target_path,
            backup_path: None,
            status: item.last_status.clone().unwrap_or_default(),
            injected_at: Utc::now().to_rfc3339(),
            changed: true,
        }]);
    }

    ensure_agent_route_is_ready(config, &config.agent_injections[index])?;
    {
        let item = &mut config.agent_injections[index];
        item.enabled = enabled;
    }
    match apply_one_by_id_with_discovered_models(config, id, discovered_models) {
        Ok(results) => Ok(results),
        Err(err) => {
            config.agent_injections[index] = previous;
            Err(err)
        }
    }
}
pub fn apply_one_by_id(config: &mut AppConfig, id: &str) -> Result<Vec<AgentInjectionResult>> {
    apply_one_by_id_with_discovered_models(config, id, None)
}

pub fn apply_one_by_id_with_discovered_models(
    config: &mut AppConfig,
    id: &str,
    discovered_models: Option<&[Value]>,
) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let Some(index) = config
        .agent_injections
        .iter()
        .position(|item| item.id == id)
    else {
        return Err(anyhow!("agent injection {id} was not found"));
    };
    ensure_agent_route_is_ready(config, &config.agent_injections[index])?;
    let mut context = InjectionContext::from_config(config, Some(&config.agent_injections[index]));
    context.discovered_codex_models = discovered_models.map(<[Value]>::to_vec);
    let result = apply_item(&config.agent_injections[index], &context)?;
    {
        let item = &mut config.agent_injections[index];
        item.enabled = true;
        item.target_path = result.target_path.clone();
        if result.changed {
            item.last_injected_at = Some(Utc::now());
            item.last_status = Some(result.status.clone());
        }
    }
    Ok(vec![result])
}

pub fn apply_enabled(config: &mut AppConfig) -> Result<Vec<AgentInjectionResult>> {
    apply_enabled_with_discovered_models(config, None)
}

pub fn apply_enabled_with_discovered_models(
    config: &mut AppConfig,
    discovered_models: Option<&[Value]>,
) -> Result<Vec<AgentInjectionResult>> {
    ensure_defaults(config);
    let before = config.clone();
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
        if let Err(error) = ensure_agent_route_is_ready(config, &config.agent_injections[index]) {
            config.agent_injections[index].last_status = Some(format!("未应用本地注入：{error}"));
            continue;
        }
        let mut context =
            InjectionContext::from_config(config, Some(&config.agent_injections[index]));
        context.discovered_codex_models = discovered_models.map(<[Value]>::to_vec);
        let result = match apply_item(&config.agent_injections[index], &context) {
            Ok(result) => result,
            Err(error) => {
                // A batch apply is one user action: restore every artifact
                // already changed by this batch before returning the error.
                for current_item in config.agent_injections.iter() {
                    if let Some(previous_item) = before
                        .agent_injections
                        .iter()
                        .find(|item| item.id == current_item.id)
                    {
                        if previous_item.enabled {
                            let mut prior_context =
                                InjectionContext::from_config(&before, Some(previous_item));
                            prior_context.discovered_codex_models =
                                discovered_models.map(<[Value]>::to_vec);
                            let _ = apply_item(previous_item, &prior_context);
                        } else {
                            let _ = remove_item(current_item);
                        }
                    }
                }
                *config = before;
                return Err(error);
            }
        };
        let item = &mut config.agent_injections[index];
        item.enabled = true;
        item.target_path = result.target_path.clone();
        if result.changed {
            item.last_injected_at = Some(Utc::now());
            item.last_status = Some(result.status.clone());
        }
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
    let requested_model_id = clean_optional(input.model_id);
    let previous = config.agent_injections[index].clone();
    let model_id = if let Some(provider_id) = provider_id.as_deref() {
        let Some(provider) = config
            .providers
            .iter()
            .find(|provider| provider.id == provider_id)
            .cloned()
        else {
            return Err(anyhow!("provider {provider_id} was not found"));
        };
        if !provider.enabled {
            return Err(anyhow!("provider {provider_id} is disabled"));
        }
        requested_model_id
            .as_deref()
            .map(|model_id| {
                best_model_match(&provider.models, model_id, true)
                    .map(|model| model.id.clone())
                    .ok_or_else(|| {
                        anyhow!(
                            "model {model_id} was not found in provider {}",
                            provider.name
                        )
                    })
            })
            .transpose()?
    } else {
        if config.agent_injections[index].enabled {
            return Err(anyhow!(
                "cannot clear an enabled Agent route; disable the injection first"
            ));
        }
        None
    };
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

fn ensure_agent_route_is_ready(config: &AppConfig, item: &AgentInjectionConfig) -> Result<()> {
    let provider_id = item.provider_id.as_deref().ok_or_else(|| {
        anyhow!(
            "agent_route_unbound: {} has no upstream binding; choose an upstream before enabling injection",
            item.label
        )
    })?;
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .ok_or_else(|| {
            anyhow!(
                "agent_route_unavailable: {} is bound to missing upstream {provider_id}",
                item.label
            )
        })?;
    if !provider.enabled {
        return Err(anyhow!(
            "agent_route_unavailable: {} is bound to disabled upstream {}",
            item.label,
            provider.name
        ));
    }
    Ok(())
}

/// Return the exact file(s) that an Agent integration is allowed to touch.
/// `target_path` is persisted user data, so it must never be treated as an
/// arbitrary filesystem destination. Keeping this list here also makes a
/// future integration change explicit and reviewable.
fn allowed_target_paths(kind: &AgentInjectionKind) -> Result<Vec<PathBuf>> {
    match kind {
        AgentInjectionKind::ClaudeCode => {
            Ok(vec![home_dir().join(".claude").join("settings.json")])
        }
        AgentInjectionKind::Codex => Ok(vec![home_dir().join(".codex").join("config.toml")]),
        AgentInjectionKind::ClaudeDesktop => {
            let paths = claude_desktop_paths();
            Ok(vec![
                paths.normal_config_path,
                paths.threep_config_path,
                paths.profile_path,
                paths.meta_path,
            ])
        }
        AgentInjectionKind::OpenCode => Ok(vec![opencode_config_path()]),
        AgentInjectionKind::OpenClaw => Ok(vec![openclaw_config_path()]),
        AgentInjectionKind::Hermes => Ok(vec![hermes_config_path()]),
        AgentInjectionKind::ProxyMode => Ok(vec![app_config_dir()
            .unwrap_or_else(|_| home_dir().join(".atoapi"))
            .join("atoapi-proxy-mode.json")]),
        AgentInjectionKind::Gemini | AgentInjectionKind::Unknown => {
            Err(anyhow!("unsupported Agent injection target kind: {kind:?}"))
        }
    }
}

fn default_target_path(kind: &AgentInjectionKind) -> Result<PathBuf> {
    let paths = allowed_target_paths(kind)?;
    match kind {
        // The profile path is the artifact reported by the Claude Desktop
        // adapter, although that adapter updates all four controlled files.
        AgentInjectionKind::ClaudeDesktop => paths
            .iter()
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == format!("{CLAUDE_DESKTOP_PROFILE_ID}.json"))
            })
            .cloned()
            .ok_or_else(|| anyhow!("Claude Desktop profile target is unavailable")),
        _ => paths
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Agent injection target is unavailable")),
    }
}

fn resolve_injection_target(item: &AgentInjectionConfig) -> Result<PathBuf> {
    let allowed = allowed_target_paths(&item.kind)?;
    let default = default_target_path(&item.kind)?;
    let requested = item.target_path.as_deref().unwrap_or(&default);
    validate_target_against_allowed(&item.label, requested, &allowed)
}

fn validate_claude_desktop_targets(item: &AgentInjectionConfig) -> Result<ClaudeDesktopPaths> {
    let paths = claude_desktop_paths();
    let allowed = vec![
        paths.normal_config_path.clone(),
        paths.threep_config_path.clone(),
        paths.profile_path.clone(),
        paths.meta_path.clone(),
    ];
    if let Some(requested) = item.target_path.as_deref() {
        // Validate the persisted value even though the Desktop adapter has
        // several coordinated files and therefore does not use it directly.
        validate_target_against_allowed(&item.label, requested, &allowed)?;
    }
    for target in &allowed {
        validate_target_against_allowed(&item.label, target, &allowed)?;
    }
    Ok(paths)
}

fn validate_target_against_allowed(
    label: &str,
    candidate: &Path,
    allowed: &[PathBuf],
) -> Result<PathBuf> {
    let candidate = absolute_lexical_path(candidate)?;
    for expected in allowed {
        let expected = absolute_lexical_path(expected)?;
        if same_filesystem_path(&candidate, &expected) {
            let root = expected
                .parent()
                .ok_or_else(|| anyhow!("{label} target has no parent directory"))?;
            validate_controlled_file_path(&candidate, root)?;
            return Ok(candidate);
        }
    }
    Err(anyhow!(
        "{label} target_path is outside its controlled directory: {}",
        candidate.display()
    ))
}

/// Validate a file that must be a direct child of a controlled directory.
/// Existing and newly-created paths are both checked for symlink/reparse-point
/// components before any read, backup, write, restore, or delete operation.
fn validate_controlled_file_path(path: &Path, root: &Path) -> Result<()> {
    let path = absolute_lexical_path(path)?;
    let root = absolute_lexical_path(root)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("controlled target has no parent directory"))?;
    if !same_filesystem_path(parent, &root) {
        return Err(anyhow!(
            "target {} is not directly inside controlled directory {}",
            path.display(),
            root.display()
        ));
    }
    ensure_no_symlink_components(&path)?;

    // Canonicalize the existing root and parent as an additional boundary
    // check. The symlink walk above also covers a target that does not exist
    // yet, while this catches junctions/reparse points resolved by the OS.
    if root.exists() {
        let canonical_root = fs::canonicalize(&root).with_context(|| {
            format!(
                "failed to canonicalize controlled directory {}",
                root.display()
            )
        })?;
        let canonical_parent = fs::canonicalize(parent).with_context(|| {
            format!(
                "failed to canonicalize target directory {}",
                parent.display()
            )
        })?;
        if !same_filesystem_path(&canonical_root, &canonical_parent) {
            return Err(anyhow!(
                "target {} resolves outside controlled directory {}",
                path.display(),
                root.display()
            ));
        }
    }
    Ok(())
}

fn absolute_lexical_path(path: &Path) -> Result<PathBuf> {
    let input = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut output = PathBuf::new();
    for component in input.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !output.pop() {
                    return Err(anyhow!(
                        "path escapes its filesystem root: {}",
                        path.display()
                    ));
                }
            }
            Component::Prefix(prefix) => output.push(prefix.as_os_str()),
            Component::RootDir | Component::Normal(_) => output.push(component.as_os_str()),
        }
    }
    Ok(output)
}

fn ensure_no_symlink_components(path: &Path) -> Result<()> {
    let absolute = absolute_lexical_path(path)?;
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(anyhow!(
                    "refusing to use symlink target component {}",
                    current.display()
                ));
            }
            #[cfg(windows)]
            Ok(metadata)
                if {
                    use std::os::windows::fs::MetadataExt;
                    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
                    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                } =>
            {
                return Err(anyhow!(
                    "refusing to use reparse-point target component {}",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to inspect target component {}", current.display())
                });
            }
        }
    }
    Ok(())
}

fn same_filesystem_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn remove_item(item: &AgentInjectionConfig) -> Result<Option<PathBuf>> {
    match item.kind {
        AgentInjectionKind::Codex => {
            let target = resolve_injection_target(item)?;
            remove_codex_config_injection(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::ClaudeDesktop => {
            let paths = validate_claude_desktop_targets(item)?;
            let reported_target = paths.profile_path.clone();
            for target in [
                paths.normal_config_path,
                paths.threep_config_path,
                paths.profile_path,
                paths.meta_path,
            ] {
                restore_injection_backup(&target)?;
            }
            Ok(Some(reported_target))
        }
        AgentInjectionKind::ClaudeCode => {
            let target = resolve_injection_target(item)?;
            restore_injection_backup(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::OpenCode => {
            let target = resolve_injection_target(item)?;
            restore_injection_backup(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::OpenClaw => {
            let target = resolve_injection_target(item)?;
            restore_injection_backup(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::Hermes => {
            let target = resolve_injection_target(item)?;
            restore_injection_backup(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::ProxyMode => {
            let target = resolve_injection_target(item)?;
            restore_injection_backup(&target)?;
            Ok(Some(target))
        }
        AgentInjectionKind::Gemini | AgentInjectionKind::Unknown => Ok(item.target_path.clone()),
    }
}

fn remove_codex_config_injection(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(());
    }
    let mut doc = text
        .parse::<DocumentMut>()
        .map_err(|error| anyhow!("Codex config.toml parse error: {error}"))?;
    let managed_provider = codex_document_has_atoapi_provider(&doc);
    let restore_path = codex_restore_state_path(path)?;
    let loaded_restore_state = match load_codex_restore_state(&restore_path, path) {
        Ok(state) => state,
        // A catalog-only cleanup needs an intact encrypted ownership record.
        // If it is absent or corrupt, leave the user's configuration and
        // files alone rather than treating a historical filename as proof.
        Err(_) if !managed_provider => return Ok(()),
        Err(error) => return Err(error),
    };
    let managed_catalog_path = loaded_restore_state
        .as_ref()
        .and_then(|state| managed_codex_catalog_path(&doc, path, &state.catalog_manifest));
    let managed_catalog = managed_catalog_path.is_some();
    if !managed_provider && !managed_catalog {
        return Ok(());
    }
    let mut restore_state = match loaded_restore_state {
        Some(state) => state,
        None => {
            let legacy_fragment = capture_codex_legacy_managed_fragment(&doc);
            capture_codex_restore_state(&restore_path, path, true, &legacy_fragment)?
        }
    };
    let fragment = parse_codex_restore_fragment(&restore_state)?;
    restore_codex_root_fields(&mut doc, &fragment, &CODEX_MANAGED_ROOT_FIELDS);
    // Retire the obsolete flag even if an older restore record captured it.
    doc.as_table_mut().remove("disable_response_storage");
    restore_codex_custom_provider(&mut doc, &fragment);
    write_text(path, &doc.to_string())?;
    cleanup_owned_codex_catalogs_except(path, &mut restore_state.catalog_manifest, None);
    fs::remove_file(restore_path).ok();
    Ok(())
}

fn codex_document_has_atoapi_provider(doc: &DocumentMut) -> bool {
    doc.as_table()
        .get("model_providers")
        .and_then(|item| item.as_table())
        .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
        .and_then(|item| item.as_table())
        .is_some_and(|provider| {
            provider.get("name").and_then(|item| item.as_str()) == Some("Atoapi")
                && provider
                    .get("base_url")
                    .and_then(|item| item.as_str())
                    .is_some_and(|url| {
                        url.starts_with("http://127.0.0.1:") && url.ends_with("/codex/v1")
                    })
        })
}

fn managed_codex_catalog_path(
    doc: &DocumentMut,
    config_path: &Path,
    manifest: &CodexCatalogManifest,
) -> Option<PathBuf> {
    let path = configured_codex_catalog_path(doc, config_path)?;
    catalog_manifest_owns(manifest, config_path, &path).then_some(path)
}

fn configured_codex_catalog_path(doc: &DocumentMut, config_path: &Path) -> Option<PathBuf> {
    let configured = doc
        .as_table()
        .get("model_catalog_json")
        .and_then(|item| item.as_str())?
        .trim();
    if configured.is_empty() {
        return None;
    }
    let path = PathBuf::from(configured);
    Some(if path.is_absolute() {
        path
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    })
}

fn is_managed_codex_catalog_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|file| file.to_str())
        .is_some_and(|file| {
            file == CODEX_MODEL_CATALOG_LEGACY_FILE
                || (file.starts_with(CODEX_MODEL_CATALOG_FILE_PREFIX) && file.ends_with(".json"))
        })
}

fn is_competing_codex_catalog_path(path: &Path) -> bool {
    // CC Switch installs a global Codex catalog override. If Atoapi leaves it
    // active, Codex loads CC Switch's startup snapshot instead of querying the
    // Atoapi provider's live `/models` endpoint. Preserve the original value in
    // the encrypted restore state, but suppress it while Atoapi owns the route.
    let normalized = path.to_string_lossy().to_ascii_lowercase();
    normalized.contains("cc-switch") || normalized.contains("ccswitch")
}

/// A catalog may be deleted only when it is registered in the encrypted
/// restore state and still has the exact content Atoapi recorded.  Older
/// filename-only catalogs are intentionally retained: users may have created
/// a file with the same historical name themselves.
fn catalog_manifest_owns(
    manifest: &CodexCatalogManifest,
    config_path: &Path,
    candidate: &Path,
) -> bool {
    let Ok(candidate) = validate_codex_catalog_path(config_path, candidate) else {
        return false;
    };
    let Ok(current_hash) = sha256_file(&candidate) else {
        return false;
    };
    manifest.entries.iter().any(|entry| {
        let Ok(recorded_path) = validate_codex_catalog_path(config_path, &entry.path) else {
            return false;
        };
        same_filesystem_path(&candidate, &recorded_path) && entry.sha256 == current_hash
    })
}

fn validate_codex_catalog_path(config_path: &Path, path: &Path) -> Result<PathBuf> {
    let config_path = absolute_lexical_path(config_path)?;
    let root = config_path
        .parent()
        .ok_or_else(|| anyhow!("Codex config target has no parent directory"))?;
    let path = absolute_lexical_path(path)?;
    if !is_managed_codex_catalog_path(&path) {
        return Err(anyhow!(
            "catalog {} does not use an Atoapi-owned catalog name",
            path.display()
        ));
    }
    validate_controlled_file_path(&path, root)?;
    Ok(path)
}

fn register_owned_codex_catalog(
    manifest: &mut CodexCatalogManifest,
    config_path: &Path,
    catalog_path: &Path,
) -> Result<()> {
    let catalog_path = validate_codex_catalog_path(config_path, catalog_path)?;
    let sha256 = sha256_file(&catalog_path).with_context(|| {
        format!(
            "failed to fingerprint Atoapi Codex catalog {}",
            catalog_path.display()
        )
    })?;
    manifest.entries.retain(|entry| {
        validate_codex_catalog_path(config_path, &entry.path)
            .map(|path| !same_filesystem_path(&path, &catalog_path))
            .unwrap_or(false)
    });
    manifest.entries.push(CodexCatalogManifestEntry {
        path: catalog_path,
        sha256,
    });
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(not(test))]
fn codex_restore_state_path(_target_path: &Path) -> Result<PathBuf> {
    Ok(app_config_dir()?.join(CODEX_RESTORE_STATE_FILE))
}

#[cfg(test)]
fn codex_restore_state_path(target_path: &Path) -> Result<PathBuf> {
    Ok(target_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(CODEX_RESTORE_STATE_FILE))
}

fn absolute_codex_target_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn same_codex_target(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn capture_codex_restore_fragment(doc: &DocumentMut) -> DocumentMut {
    let mut fragment = DocumentMut::new();
    for field in CODEX_MANAGED_ROOT_FIELDS {
        if let Some(item) = doc.as_table().get(field) {
            fragment.as_table_mut().insert(field, item.clone());
        }
    }
    if let Some(custom) = doc
        .as_table()
        .get("model_providers")
        .and_then(|item| item.as_table())
        .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
    {
        fragment["model_providers"] = toml_edit::table();
        fragment["model_providers"][CODEX_PROVIDER_ID] = custom.clone();
    }
    fragment
}

fn capture_codex_legacy_managed_fragment(doc: &DocumentMut) -> DocumentMut {
    let mut fragment = DocumentMut::new();
    for field in CODEX_MANAGED_MODEL_FIELDS {
        if let Some(item) = doc.as_table().get(field) {
            fragment.as_table_mut().insert(field, item.clone());
        }
    }
    // Older Atoapi injections never wrote these keys, so on a legacy restore
    // path any current value is caller-owned and must survive removal.
    for field in [
        "model_auto_compact_token_limit",
        "model_auto_compact_token_limit_scope",
    ] {
        if let Some(item) = doc.as_table().get(field) {
            fragment.as_table_mut().insert(field, item.clone());
        }
    }
    fragment
}

fn capture_codex_restore_state(
    state_path: &Path,
    target_path: &Path,
    target_existed: bool,
    doc: &DocumentMut,
) -> Result<CodexRestoreState> {
    let state = CodexRestoreState {
        schema_version: CODEX_RESTORE_STATE_VERSION,
        target_path: absolute_codex_target_path(target_path)?,
        target_existed,
        fragment_toml: capture_codex_restore_fragment(doc).to_string(),
        managed_root_fields: CODEX_MANAGED_ROOT_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        catalog_manifest: CodexCatalogManifest::default(),
    };
    save_codex_restore_state(state_path, &state)?;
    Ok(state)
}

fn save_codex_restore_state(path: &Path, state: &CodexRestoreState) -> Result<()> {
    let payload = serde_json::to_string(state)?;
    let encrypted_payload = crate::crypto::encrypt_secret(&payload)?;
    if !encrypted_payload.starts_with("dpapi:") {
        return Err(anyhow!("Codex restore state encryption did not use DPAPI"));
    }
    let envelope = CodexRestoreEnvelope {
        schema_version: CODEX_RESTORE_STATE_VERSION,
        encrypted_payload,
    };
    write_json_pretty(path, &serde_json::to_value(envelope)?)
}

fn load_codex_restore_state(
    state_path: &Path,
    target_path: &Path,
) -> Result<Option<CodexRestoreState>> {
    if !state_path.exists() {
        return Ok(None);
    }
    let envelope: CodexRestoreEnvelope = serde_json::from_str(&fs::read_to_string(state_path)?)
        .context("Codex restore state envelope is invalid")?;
    if envelope.schema_version != CODEX_RESTORE_STATE_VERSION
        || !envelope.encrypted_payload.starts_with("dpapi:")
    {
        return Err(anyhow!(
            "Codex restore state envelope is unsupported or unencrypted"
        ));
    }
    let payload = crate::crypto::decrypt_secret(&envelope.encrypted_payload)?;
    let state: CodexRestoreState =
        serde_json::from_str(&payload).context("Codex restore state payload is invalid")?;
    if state.schema_version != CODEX_RESTORE_STATE_VERSION {
        return Err(anyhow!(
            "Codex restore state payload version is unsupported"
        ));
    }
    let expected = absolute_codex_target_path(target_path)?;
    if !same_codex_target(&state.target_path, &expected) {
        return Err(anyhow!(
            "Codex restore state belongs to a different config path; the current file was left unchanged"
        ));
    }
    Ok(Some(state))
}

fn parse_codex_restore_fragment(state: &CodexRestoreState) -> Result<DocumentMut> {
    if state.fragment_toml.trim().is_empty() {
        return Ok(DocumentMut::new());
    }
    state
        .fragment_toml
        .parse::<DocumentMut>()
        .context("Codex restore state TOML fragment is invalid")
}

fn restore_codex_root_fields(doc: &mut DocumentMut, fragment: &DocumentMut, fields: &[&str]) {
    for field in fields {
        doc.as_table_mut().remove(field);
        if let Some(original) = fragment.as_table().get(field) {
            doc.as_table_mut().insert(field, original.clone());
        }
    }
}

fn migrate_codex_restore_state_for_auto_compaction(
    state_path: &Path,
    state: &mut CodexRestoreState,
    live_doc: &DocumentMut,
) -> Result<()> {
    let mut fragment = parse_codex_restore_fragment(state)?;
    let mut changed = false;
    for field in [
        "model_auto_compact_token_limit",
        "model_auto_compact_token_limit_scope",
    ] {
        if state
            .managed_root_fields
            .iter()
            .any(|managed| managed == field)
        {
            continue;
        }
        if !fragment.as_table().contains_key(field) {
            if let Some(original) = live_doc.as_table().get(field) {
                // Existing restore state means this file was managed by an
                // older Atoapi build. That build did not own this root field,
                // so its pre-upgrade value belongs to the user.
                fragment.as_table_mut().insert(field, original.clone());
            }
        }
        state.managed_root_fields.push(field.to_string());
        changed = true;
    }
    if changed {
        state.fragment_toml = fragment.to_string();
        save_codex_restore_state(state_path, state)?;
    }
    Ok(())
}

fn restore_codex_custom_provider(doc: &mut DocumentMut, fragment: &DocumentMut) {
    let original = fragment
        .as_table()
        .get("model_providers")
        .and_then(|item| item.as_table())
        .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
        .cloned();
    let mut remove_parent = false;
    if let Some(providers) = doc
        .as_table_mut()
        .get_mut("model_providers")
        .and_then(|item| item.as_table_mut())
    {
        providers.remove(CODEX_PROVIDER_ID);
        remove_parent = providers.is_empty();
    }
    if remove_parent {
        doc.as_table_mut().remove("model_providers");
    }
    if let Some(original) = original {
        if !doc.as_table().contains_key("model_providers") {
            doc["model_providers"] = toml_edit::table();
        }
        if let Some(providers) = doc["model_providers"].as_table_mut() {
            providers.insert(CODEX_PROVIDER_ID, original);
        }
    }
}

fn apply_item(
    item: &AgentInjectionConfig,
    context: &InjectionContext,
) -> Result<AgentInjectionResult> {
    let started = Utc::now();
    let mut changed = true;
    let (target_path, backup_path, status) = match item.kind {
        AgentInjectionKind::ClaudeCode => {
            let target = resolve_injection_target(item)?;
            let backup = backup_file(&target)?;
            if let Err(error) = write_claude_code_settings(&target, context) {
                discard_injection_backup(&target).ok();
                return Err(error);
            }
            (
                Some(target),
                backup,
                "Claude Code 已注入本地 Anthropic 中转".to_string(),
            )
        }
        AgentInjectionKind::Codex => {
            let target = resolve_injection_target(item)?;
            changed = write_codex_config(&target, context)?;
            (
                Some(target),
                None,
                "Codex 已注入本地 Responses 中转".to_string(),
            )
        }
        AgentInjectionKind::ClaudeDesktop => {
            let paths = validate_claude_desktop_targets(item)?;
            let targets = [
                &paths.normal_config_path,
                &paths.threep_config_path,
                &paths.profile_path,
                &paths.meta_path,
            ];
            let backups = match targets
                .iter()
                .map(|path| Ok(((*path).to_path_buf(), backup_file(path)?)))
                .collect::<Result<Vec<_>>>()
            {
                Ok(backups) => backups,
                Err(error) => {
                    for path in targets {
                        discard_injection_backup(path).ok();
                    }
                    return Err(error);
                }
            };
            if let Err(err) = write_claude_desktop(&paths, context) {
                let _ = restore_backups(&backups);
                for (path, _) in &backups {
                    discard_injection_backup(path).ok();
                }
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
            let target = resolve_injection_target(item)?;
            let backup = backup_file(&target)?;
            if let Err(error) = write_opencode_config(&target, context) {
                discard_injection_backup(&target).ok();
                return Err(error);
            }
            (
                Some(target),
                backup,
                "OpenCode injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::OpenClaw => {
            let target = resolve_injection_target(item)?;
            let backup = backup_file(&target)?;
            if let Err(error) = write_openclaw_config(&target, context) {
                discard_injection_backup(&target).ok();
                return Err(error);
            }
            (
                Some(target),
                backup,
                "OpenClaw injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::Hermes => {
            let target = resolve_injection_target(item)?;
            let backup = backup_file(&target)?;
            if let Err(error) = write_hermes_config(&target, context) {
                discard_injection_backup(&target).ok();
                return Err(error);
            }
            (
                Some(target),
                backup,
                "Hermes injected with local OpenAI-compatible proxy".to_string(),
            )
        }
        AgentInjectionKind::ProxyMode => {
            let target = resolve_injection_target(item)?;
            let backup = backup_file(&target)?;
            if let Err(error) = write_proxy_mode_profile(&target, context) {
                discard_injection_backup(&target).ok();
                return Err(error);
            }
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
        changed,
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
        let base = local_base_url(source_host, source_port);
        let configured_provider_id = item.and_then(|item| item.provider_id.as_deref());
        let provider = configured_provider_id.as_deref().and_then(|id| {
            config
                .providers
                .iter()
                .find(|provider| provider.id == id && provider.enabled)
        });
        let configured_model_id = item.and_then(|item| item.model_id.as_deref());
        let explicit_model_config = provider.and_then(|provider| {
            configured_model_id
                .and_then(|model_id| best_model_match(&provider.models, model_id, true))
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
            auto_compact_token_limit: provider
                .and_then(|provider| config.auto_compact_token_limit_for_provider(&provider.id)),
            codex_context_settings: config.codex_context_settings.clone(),
            codex_models,
            discovered_codex_models: None,
        }
    }
}

/// Build a URL for an agent configuration from an IP listener. IPv6 literals
/// must be enclosed in brackets in URLs, and wildcard listeners are not valid
/// client destinations, so route those through the local loopback address.
fn local_base_url(host: &str, port: u16) -> String {
    let client_host = match host.parse::<IpAddr>() {
        Ok(ip) if ip.is_unspecified() => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        Ok(ip) => ip,
        Err(_) => return format!("http://{host}:{port}"),
    };
    match client_host {
        IpAddr::V6(ip) => format!("http://[{ip}]:{port}"),
        IpAddr::V4(ip) => format!("http://{ip}:{port}"),
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

/// Codex can issue a short-lived internal request such as `codex-auto-review`
/// after a primary request. Keep only the current configured primary model as
/// a process-local fallback so that helper request never escapes to a third
/// party as an unsupported internal model name.
pub(crate) fn codex_runtime_model_bindings(config: &AppConfig) -> HashMap<String, String> {
    config
        .agent_injections
        .iter()
        .filter(|item| item.enabled && item.kind == AgentInjectionKind::Codex)
        .filter_map(|item| {
            let provider_id = item.provider_id.as_deref()?.trim();
            if provider_id.is_empty() {
                return None;
            }
            let model = configured_codex_primary_model(item)?;
            Some((format!("{}\0{}", item.id, provider_id), model))
        })
        .collect()
}

fn configured_codex_primary_model(item: &AgentInjectionConfig) -> Option<String> {
    let target = resolve_injection_target(item).ok()?;
    let text = fs::read_to_string(target).ok()?;
    let document = text.parse::<DocumentMut>().ok()?;
    document
        .as_table()
        .get("model")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|model| !model.is_empty() && *model != "codex-auto-review")
        .map(ToOwned::to_owned)
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

const CODEX_DEFAULT_REASONING_EFFORTS: [&str; 6] =
    ["low", "medium", "high", "xhigh", "max", "ultra"];
const CODEX_CHINESE_BASE_INSTRUCTIONS: &str = "你是 Codex，一个编码代理。你与用户共享同一个工作区，并持续协作，直到用户的意图被完整、可核验地处理完毕。请使用中文思考和回复；代码、命令、路径、协议字段和上游原文可以保持原样。";

fn codex_reasoning_description(effort: &str) -> &'static str {
    match effort {
        "low" => "更快响应，适合轻量推理",
        "medium" => "兼顾速度和推理深度",
        "high" => "更深入的复杂问题推理",
        "xhigh" => "面向复杂问题的超高推理",
        "max" => "面向最难问题的最大推理深度",
        "ultra" => "最大推理深度，并自动进行任务委派",
        _ => "模型支持的推理档位",
    }
}

fn codex_model_reasoning_efforts(model: Option<&ModelConfig>) -> Vec<String> {
    let mut seen = HashSet::new();
    let configured = model
        .map(|model| {
            model
                .supported_reasoning_efforts
                .iter()
                .filter_map(|effort| normalize_reasoning_effort(effort))
                .filter(|effort| seen.insert(effort.clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if configured.is_empty() {
        CODEX_DEFAULT_REASONING_EFFORTS
            .iter()
            .map(|effort| (*effort).to_string())
            .collect()
    } else {
        configured
    }
}

fn codex_model_default_reasoning_level(
    model: Option<&ModelConfig>,
    supported: &[String],
) -> String {
    if let Some(configured) = model
        .filter(|model| model.reasoning_effort_override_enabled)
        .and_then(|model| model.reasoning_effort.as_deref())
        .and_then(normalize_reasoning_effort)
        .filter(|effort| supported.iter().any(|candidate| candidate == effort))
    {
        return configured;
    }
    for preferred in ["high", "medium", "low"] {
        if supported.iter().any(|effort| effort == preferred) {
            return preferred.to_string();
        }
    }
    supported
        .first()
        .cloned()
        .unwrap_or_else(|| "medium".to_string())
}

fn codex_model_catalog_item(
    id: &str,
    display_name: &str,
    model: Option<&ModelConfig>,
    priority: usize,
) -> Value {
    let supported_efforts = codex_model_reasoning_efforts(model);
    let supported_levels = supported_efforts
        .iter()
        .map(|effort| {
            json!({
                "effort": effort,
                "description": codex_reasoning_description(effort)
            })
        })
        .collect::<Vec<_>>();
    let default_reasoning_level = codex_model_default_reasoning_level(model, &supported_efforts);
    let context_window = model
        .and_then(|model| model.context_window)
        .unwrap_or(128_000);
    let supports_tools = model.map(|model| model.supports_tools).unwrap_or(true);
    json!({
        "additional_speed_tiers": [],
        "availability_nux": Value::Null,
        "base_instructions": CODEX_CHINESE_BASE_INSTRUCTIONS,
        "context_window": context_window,
        "default_reasoning_level": default_reasoning_level,
        "default_reasoning_summary": "none",
        "description": format!("Atoapi 模型 {id}，支持 Atoapi 路由、缓存和多会话隔离。"),
        "display_name": display_name,
        "effective_context_window_percent": 95,
        "experimental_supported_tools": [],
        "input_modalities": ["text"],
        "max_context_window": context_window,
        "model_messages": {"instructions_template": CODEX_CHINESE_BASE_INSTRUCTIONS},
        "prefer_websockets": false,
        "priority": priority,
        "service_tiers": [],
        "shell_type": "shell_command",
        "slug": id,
        "support_verbosity": false,
        "supported_in_api": true,
        "supported_reasoning_levels": supported_levels,
        "supports_image_detail_original": false,
        "supports_parallel_tool_calls": supports_tools,
        "supports_reasoning_effort_updates": true,
        "supports_reasoning_summaries": true,
        "supports_search_tool": false,
        "truncation_policy": {"limit": 10_000, "mode": "bytes"},
        "upgrade": Value::Null,
        "visibility": "list"
    })
}

fn codex_discovered_catalog_item(item: &Value, priority: usize) -> Option<Value> {
    let id = item
        .get("id")
        .or_else(|| item.get("slug"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())?;
    if crate::config::codex_native_visible_model(id).is_none() {
        return None;
    }
    let priority = item
        .get("priority")
        .and_then(Value::as_u64)
        .and_then(|priority| usize::try_from(priority).ok())
        .unwrap_or(priority);
    let display_name = item
        .get("display_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(id);
    let mut supplied_efforts = item
        .get("supported_reasoning_levels")
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|level| level.get("effort").and_then(Value::as_str))
                .filter_map(normalize_reasoning_effort)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if supplied_efforts.is_empty() {
        supplied_efforts = item
            .get("supported_reasoning_efforts")
            .and_then(Value::as_array)
            .map(|efforts| {
                efforts
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(normalize_reasoning_effort)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }
    // Codex exposes reasoning controls from the selected model's declared
    // levels. Always keep Atoapi's complete ladder while retaining any extra
    // levels reported by the upstream. This prevents an upstream that omits
    // xhigh/max/ultra from silently hiding those choices in Codex.
    supplied_efforts.extend(
        CODEX_DEFAULT_REASONING_EFFORTS
            .iter()
            .map(|effort| (*effort).to_string()),
    );
    let supported_efforts = crate::config::normalize_reasoning_efforts(&supplied_efforts);
    let supported_levels = supported_efforts
        .iter()
        .map(|effort| {
            json!({
                "effort": effort,
                "description": codex_reasoning_description(effort)
            })
        })
        .collect::<Vec<_>>();
    let default_reasoning_level = item
        .get("default_reasoning_level")
        .and_then(Value::as_str)
        .and_then(normalize_reasoning_effort)
        .filter(|effort| {
            supported_efforts
                .iter()
                .any(|candidate| candidate == effort)
        })
        .unwrap_or_else(|| {
            if supported_efforts.iter().any(|effort| effort == "high") {
                "high".to_string()
            } else {
                supported_efforts
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "medium".to_string())
            }
        });
    let context_window = item
        .get("context_window")
        .and_then(Value::as_u64)
        .unwrap_or(128_000);
    let supports_parallel_tool_calls = item
        .get("supports_parallel_tool_calls")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    Some(json!({
        "additional_speed_tiers": [],
        "availability_nux": Value::Null,
        "base_instructions": CODEX_CHINESE_BASE_INSTRUCTIONS,
        "context_window": context_window,
        "default_reasoning_level": default_reasoning_level,
        "default_reasoning_summary": "none",
        "description": format!("Atoapi 动态模型 {id}，支持 Atoapi 路由、缓存和多会话隔离。"),
        "display_name": display_name,
        "effective_context_window_percent": 95,
        "experimental_supported_tools": [],
        "input_modalities": item
            .get("input_modalities")
            .cloned()
            .unwrap_or_else(|| json!(["text"])),
        "max_context_window": item
            .get("max_context_window")
            .and_then(Value::as_u64)
            .unwrap_or(context_window),
        "model_messages": {"instructions_template": CODEX_CHINESE_BASE_INSTRUCTIONS},
        "prefer_websockets": false,
        "priority": priority,
        "service_tiers": [],
        "shell_type": "shell_command",
        "slug": id,
        "support_verbosity": false,
        "supported_in_api": true,
        "supported_reasoning_levels": supported_levels,
        "supports_image_detail_original": false,
        "supports_parallel_tool_calls": supports_parallel_tool_calls,
        "supports_reasoning_effort_updates": true,
        "supports_reasoning_summaries": true,
        "supports_search_tool": item
            .get("supports_search_tool")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "truncation_policy": item
            .get("truncation_policy")
            .cloned()
            .unwrap_or_else(|| json!({"limit": 10_000, "mode": "bytes"})),
        "upgrade": Value::Null,
        "visibility": "list"
    }))
}

fn codex_model_catalog_value(
    context: &InjectionContext,
    discovered_models: Option<&[Value]>,
) -> Value {
    let mut seen = HashSet::new();
    let mut models = Vec::new();
    if let Some(discovered_models) = discovered_models {
        for item in discovered_models {
            if let Some(catalog_item) = codex_discovered_catalog_item(item, models.len() + 1) {
                upsert_codex_catalog_model(&mut models, &mut seen, catalog_item);
            }
        }
    }
    for (id, display_name, priority) in crate::config::CODEX_NATIVE_VISIBLE_MODELS {
        let model = best_model_match(&context.codex_models, id, true);
        upsert_codex_catalog_model(
            &mut models,
            &mut seen,
            codex_model_catalog_item(id, display_name, model, priority as usize),
        );
    }
    let explicit_aliases = context
        .codex_models
        .iter()
        .filter(|model| model.enabled)
        .filter_map(model_request_alias)
        .map(|alias| alias.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    for model in context.codex_models.iter().filter(|model| model.enabled) {
        if !explicit_aliases.contains(&model.id.to_ascii_lowercase())
            && crate::config::codex_native_visible_model(&model.id).is_some()
        {
            let display_name = crate::config::codex_native_visible_model(&model.id)
                .map(|(_, display_name, _)| display_name)
                .unwrap_or(&model.display_name);
            let priority = models.len() + 1;
            upsert_codex_catalog_model(
                &mut models,
                &mut seen,
                codex_model_catalog_item(&model.id, display_name, Some(model), priority),
            );
        }
        let mut aliases = Vec::new();
        if let Some(alias) = model_request_alias(model) {
            aliases.push(alias);
        }
        if let Some(alias) = codex_model_alias(&model.id) {
            aliases.push(alias);
        }
        for alias in aliases {
            let Some((_, display_name, _)) = crate::config::codex_native_visible_model(&alias)
            else {
                continue;
            };
            let priority = models.len() + 1;
            upsert_codex_catalog_model(
                &mut models,
                &mut seen,
                codex_model_catalog_item(&alias, display_name, Some(model), priority),
            );
        }
    }
    if let Some((id, display_name, _)) =
        crate::config::codex_native_visible_model(&context.default_model)
    {
        let model = best_model_match(&context.codex_models, &context.default_model, true);
        let priority = models.len() + 1;
        upsert_codex_catalog_model(
            &mut models,
            &mut seen,
            codex_model_catalog_item(id, display_name, model, priority),
        );
    }
    // Apply the agent-wide override after discovery and mapping reconciliation,
    // so every visible model agrees with config.toml. Rebuilding the native
    // entries above also prevents retained catalogs from keeping an old limit
    // after the override is cleared or the route changes.
    if let Some(context_window) = context
        .codex_context_settings
        .as_ref()
        .and_then(|settings| settings.model_context_window)
        .filter(|window| *window > 0)
    {
        for model in &mut models {
            model["context_window"] = json!(context_window);
            model["max_context_window"] = json!(context_window);
        }
    }
    json!({"models": models})
}

fn upsert_codex_catalog_model(
    models: &mut Vec<Value>,
    seen: &mut HashSet<String>,
    mut catalog_item: Value,
) {
    let Some(slug) = catalog_item
        .get("slug")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return;
    };
    if seen.insert(slug.clone()) {
        models.push(catalog_item);
    } else if let Some(position) = models.iter().position(|item| {
        item.get("slug")
            .and_then(Value::as_str)
            .is_some_and(|candidate| candidate == slug)
    }) {
        if let Some(priority) = models[position].get("priority").cloned() {
            catalog_item["priority"] = priority;
        }
        models[position] = catalog_item;
    }
}

fn write_codex_model_catalog(
    target_path: &Path,
    context: &InjectionContext,
) -> Result<(PathBuf, bool)> {
    let catalog_path = target_path
        .parent()
        .ok_or_else(|| anyhow!("Codex config target has no parent directory"))?
        .join(CODEX_MODEL_CATALOG_ACTIVE_FILE);
    // If the selected upstream is temporarily unavailable, retain the last
    // complete catalog. Explicit mappings are still applied below and replace
    // same-name entries from that retained catalog.
    let persisted_models = if context.discovered_codex_models.is_none() {
        fs::read_to_string(&catalog_path)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .and_then(|catalog| catalog.get("models").and_then(Value::as_array).cloned())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let discovered_models = context
        .discovered_codex_models
        .as_deref()
        .or_else(|| (!persisted_models.is_empty()).then_some(persisted_models.as_slice()));
    let rendered = format!(
        "{}\n",
        serde_json::to_string_pretty(&codex_model_catalog_value(context, discovered_models))?
    );
    let changed = fs::read_to_string(&catalog_path)
        .map(|current| current != rendered)
        .unwrap_or(true);
    if changed {
        write_text(&catalog_path, &rendered)?;
    }
    Ok((catalog_path, changed))
}

fn write_codex_config(path: &Path, context: &InjectionContext) -> Result<bool> {
    if let Some(settings) = context.codex_context_settings.as_ref() {
        settings.validate()?;
    }
    let target_existed = path.exists();
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut doc = if text.trim().is_empty() {
        DocumentMut::new()
    } else {
        text.parse::<DocumentMut>()
            .map_err(|err| anyhow!("Codex config.toml parse error: {err}"))?
    };

    let restore_path = codex_restore_state_path(path)?;
    let mut restore_state = if codex_document_has_atoapi_provider(&doc) {
        match load_codex_restore_state(&restore_path, path)? {
            Some(state) => Some(state),
            None => {
                let legacy_fragment = capture_codex_legacy_managed_fragment(&doc);
                Some(capture_codex_restore_state(
                    &restore_path,
                    path,
                    target_existed,
                    &legacy_fragment,
                )?)
            }
        }
    } else {
        if restore_path.exists() {
            load_codex_restore_state(&restore_path, path)?;
        }
        Some(capture_codex_restore_state(
            &restore_path,
            path,
            target_existed,
            &doc,
        )?)
    };
    if let Some(state) = restore_state.as_mut() {
        migrate_codex_restore_state_for_auto_compaction(&restore_path, state, &doc)?;
    }
    let restore_fragment = restore_state
        .as_ref()
        .map(parse_codex_restore_fragment)
        .transpose()?;
    let current_catalog_is_owned = restore_state.as_ref().is_some_and(|state| {
        configured_codex_catalog_path(&doc, path)
            .is_some_and(|catalog| catalog_manifest_owns(&state.catalog_manifest, path, &catalog))
    });
    let current_catalog_is_competing = configured_codex_catalog_path(&doc, path)
        .is_some_and(|catalog| is_competing_codex_catalog_path(&catalog));
    let restore_catalog_is_user_owned = restore_fragment.as_ref().is_some_and(|fragment| {
        configured_codex_catalog_path(fragment, path)
            .is_some_and(|catalog| !is_competing_codex_catalog_path(&catalog))
    });
    doc["model_provider"] = value(CODEX_PROVIDER_ID);
    // New Codex versions reject this retired root key. Storage behavior is
    // handled by the relay request path, not this ignored configuration flag.
    doc.as_table_mut().remove("disable_response_storage");
    // Atoapi must own the catalog when no user catalog exists, when CC Switch
    // is competing, or when a previous Atoapi catalog is already active. The
    // generated catalog carries complete reasoning capabilities and Chinese
    // model instructions. A distinct user-owned catalog remains untouched.
    let managed_catalog_changed = if restore_catalog_is_user_owned
        && !current_catalog_is_owned
        && !current_catalog_is_competing
    {
        None
    } else {
        let (catalog_path, changed) = write_codex_model_catalog(path, context)?;
        if let Some(state) = restore_state.as_mut() {
            register_owned_codex_catalog(&mut state.catalog_manifest, path, &catalog_path)?;
        }
        let catalog_name = catalog_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("Atoapi Codex catalog has no valid file name"))?;
        doc["model_catalog_json"] = value(catalog_name);
        Some(changed)
    };
    if let Some(limit) = context.auto_compact_token_limit.filter(|limit| *limit > 0) {
        doc["model_auto_compact_token_limit"] = value(i64::from(limit));
        // Count only the content that has accumulated after the compacted
        // prefix. Counting the summary prefix itself (`total`) can immediately
        // retrigger compaction on an otherwise unchanged FullReplay turn.
        doc["model_auto_compact_token_limit_scope"] = value("body_after_prefix");
    } else if let Some(fragment) = restore_fragment.as_ref() {
        restore_codex_root_fields(
            &mut doc,
            fragment,
            &[
                "model_auto_compact_token_limit",
                "model_auto_compact_token_limit_scope",
            ],
        );
    }
    let repair_backend_model_id = !context.default_model_is_explicit
        && crate::config::codex_native_visible_model(&context.default_model).is_some()
        && doc
            .get("model")
            .and_then(|model| model.as_str())
            .is_none_or(|model| crate::config::codex_native_visible_model(model).is_none());
    if context.default_model_is_explicit || repair_backend_model_id {
        doc["model"] = value(context.default_model.as_str());
        if let Some(context_window) = context.model_context_window.filter(|value| *value > 0) {
            doc["model_context_window"] = value(i64::from(context_window));
        } else if let Some(fragment) = restore_fragment.as_ref() {
            restore_codex_root_fields(&mut doc, fragment, &["model_context_window"]);
        }
        if let Some(reasoning_effort) =
            best_model_match(&context.codex_models, &context.default_model, true)
                .filter(|model| model.reasoning_effort_override_enabled)
                .and_then(|model| model.reasoning_effort.as_deref())
                .and_then(crate::config::normalize_reasoning_effort)
        {
            doc["model_reasoning_effort"] = value(reasoning_effort);
        } else if let Some(fragment) = restore_fragment.as_ref() {
            restore_codex_root_fields(&mut doc, fragment, &["model_reasoning_effort"]);
        }
    } else if let Some(fragment) = restore_fragment.as_ref() {
        restore_codex_root_fields(&mut doc, fragment, &CODEX_MANAGED_MODEL_FIELDS);
    }
    // Explicit agent-wide limits apply independently of a selected model and
    // survive route switches. Empty fields remove only the managed overrides;
    // the pre-injection user values remain in the restore record for disable.
    if let Some(settings) = context.codex_context_settings.as_ref() {
        for (field, configured) in [
            ("model_context_window", settings.model_context_window),
            (
                "model_auto_compact_token_limit",
                settings.model_auto_compact_token_limit,
            ),
        ] {
            doc.as_table_mut().remove(field);
            if let Some(configured) = configured {
                doc[field] = value(i64::from(configured));
            }
        }
        doc.as_table_mut()
            .remove("model_auto_compact_token_limit_scope");
        if settings.model_auto_compact_token_limit.is_some() {
            doc["model_auto_compact_token_limit_scope"] = value("body_after_prefix");
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

    let rendered = doc.to_string();
    let config_changed = rendered != text;
    if config_changed {
        write_text(path, &rendered)?;
    }
    if let Some(state) = restore_state.as_mut() {
        let before_manifest = serde_json::to_string(&state.catalog_manifest)?;
        let keep = managed_codex_catalog_path(&doc, path, &state.catalog_manifest);
        let cleanup_changed =
            cleanup_owned_codex_catalogs_except(path, &mut state.catalog_manifest, keep.as_deref());
        let manifest_changed = before_manifest != serde_json::to_string(&state.catalog_manifest)?;
        if cleanup_changed || manifest_changed {
            save_codex_restore_state(&restore_path, state)?;
        }
    }
    Ok(config_changed || managed_catalog_changed.unwrap_or(false))
}

/// Removes only Atoapi-owned Codex artifacts that are no longer referenced by
/// the current Codex config.  It never changes Codex's config.toml, its active
/// catalog, or the encrypted restore state for an active Atoapi injection.
///
/// This reconciliation is deliberately separate from injection writes so a
/// stale legacy snapshot can be removed without touching Codex's own catalog.
pub fn cleanup_stale_codex_artifacts(config: &AppConfig) {
    let Some(codex) = config
        .agent_injections
        .iter()
        .find(|item| item.kind == AgentInjectionKind::Codex)
    else {
        return;
    };
    let Ok(target) = resolve_injection_target(codex) else {
        return;
    };
    let _ = cleanup_stale_codex_artifacts_for_target(&target);
}

fn cleanup_stale_codex_artifacts_for_target(config_path: &Path) -> Result<()> {
    let restore_path = codex_restore_state_path(config_path)?;
    let Some(mut restore_state) = load_codex_restore_state(&restore_path, config_path)? else {
        // Without an encrypted ownership manifest, filename-shaped files are
        // user-owned by default and must not be removed.
        return Ok(());
    };
    if !config_path.exists() {
        cleanup_owned_codex_catalogs_except(config_path, &mut restore_state.catalog_manifest, None);
        fs::remove_file(restore_path).ok();
        return Ok(());
    }

    let text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    if text.trim().is_empty() {
        cleanup_owned_codex_catalogs_except(config_path, &mut restore_state.catalog_manifest, None);
        fs::remove_file(restore_path).ok();
        return Ok(());
    }
    let doc = text
        .parse::<DocumentMut>()
        .map_err(|error| anyhow!("Codex config.toml parse error: {error}"))?;
    let active_catalog =
        managed_codex_catalog_path(&doc, config_path, &restore_state.catalog_manifest);
    let managed_injection = codex_document_has_atoapi_provider(&doc) || active_catalog.is_some();

    let manifest_changed = cleanup_owned_codex_catalogs_except(
        config_path,
        &mut restore_state.catalog_manifest,
        active_catalog.as_deref(),
    );
    if !managed_injection {
        fs::remove_file(restore_path).ok();
    } else if manifest_changed {
        save_codex_restore_state(&restore_path, &restore_state)?;
    }
    Ok(())
}

fn cleanup_owned_codex_catalogs_except(
    config_path: &Path,
    manifest: &mut CodexCatalogManifest,
    keep: Option<&Path>,
) -> bool {
    let keep = keep.and_then(|path| validate_codex_catalog_path(config_path, path).ok());
    let mut changed = false;
    let mut retained = Vec::with_capacity(manifest.entries.len());

    for entry in std::mem::take(&mut manifest.entries) {
        let Ok(path) = validate_codex_catalog_path(config_path, &entry.path) else {
            // The manifest may be stale or tampered. Drop its record but never
            // use it as authority to delete a path outside the Codex root.
            changed = true;
            continue;
        };
        if keep
            .as_deref()
            .is_some_and(|keep_path| same_filesystem_path(keep_path, &path))
        {
            retained.push(entry);
            continue;
        }
        let hash_matches = sha256_file(&path)
            .map(|hash| hash == entry.sha256)
            .unwrap_or(false);
        if !hash_matches {
            // Do not remove a file that has changed since Atoapi created it.
            // It may have become user-owned content under a historical name.
            changed = true;
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => changed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => changed = true,
            Err(_) => retained.push(entry),
        }
    }
    manifest.entries = retained;
    changed
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
    if let Some(previous) = injection_backup_for(path)? {
        return Ok(previous);
    }
    let backup_dir = app_config_dir()?
        .join("backups")
        .join("injections")
        .join(Utc::now().format("%Y%m%d-%H%M%S%.3f").to_string());
    let backup = if path.exists() {
        fs::create_dir_all(&backup_dir)?;
        let file_name = path
            .to_string_lossy()
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect::<String>();
        let backup = backup_dir.join(file_name);
        fs::copy(path, &backup).with_context(|| format!("failed to back up {}", path.display()))?;
        Some(backup)
    } else {
        None
    };
    if let Err(error) = record_injection_backup(path, backup.clone()) {
        if let Some(backup) = backup {
            fs::remove_file(backup).ok();
        }
        return Err(error);
    }
    Ok(backup)
}

fn injection_backup_manifest_path() -> Result<PathBuf> {
    Ok(app_config_dir()?
        .join("backups")
        .join("injections")
        .join("manifest.json"))
}

fn load_injection_backup_manifest() -> Result<HashMap<String, Option<PathBuf>>> {
    let path = injection_backup_manifest_path()?;
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn write_injection_backup_manifest(manifest: &HashMap<String, Option<PathBuf>>) -> Result<()> {
    let path = injection_backup_manifest_path()?;
    if manifest.is_empty() {
        fs::remove_file(&path).ok();
        return Ok(());
    }
    let raw = serde_json::to_string_pretty(manifest)?;
    write_text(&path, &format!("{raw}\n"))
}

fn injection_backup_key(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn injection_backup_for(path: &Path) -> Result<Option<Option<PathBuf>>> {
    let manifest = load_injection_backup_manifest()?;
    Ok(manifest.get(&injection_backup_key(path)).cloned())
}

fn record_injection_backup(path: &Path, backup: Option<PathBuf>) -> Result<()> {
    let mut manifest = load_injection_backup_manifest()?;
    manifest.insert(injection_backup_key(path), backup);
    write_injection_backup_manifest(&manifest)
}

fn discard_injection_backup(path: &Path) -> Result<()> {
    let mut manifest = load_injection_backup_manifest()?;
    let Some(backup) = manifest.remove(&injection_backup_key(path)) else {
        return Ok(());
    };
    if let Some(backup) = backup {
        fs::remove_file(backup).ok();
    }
    write_injection_backup_manifest(&manifest)
}

fn restore_injection_backup(path: &Path) -> Result<()> {
    let mut manifest = load_injection_backup_manifest()?;
    let Some(backup) = manifest.get(&injection_backup_key(path)).cloned() else {
        return Ok(());
    };
    let backup_to_delete = backup.clone();
    match backup {
        Some(backup) => {
            if !backup.exists() {
                return Err(anyhow!(
                    "injection backup for {} is missing: {}",
                    path.display(),
                    backup.display()
                ));
            }
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&backup, path).with_context(|| {
                format!(
                    "failed to restore injection backup {} to {}",
                    backup.display(),
                    path.display()
                )
            })?;
        }
        None => {
            if path.exists() {
                fs::remove_file(path)
                    .with_context(|| format!("failed to remove injected {}", path.display()))?;
            }
        }
    }
    manifest.remove(&injection_backup_key(path));
    if let Some(backup) = backup_to_delete {
        fs::remove_file(backup).ok();
    }
    write_injection_backup_manifest(&manifest)
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
    write_text_with_replace(path, text, |temporary, target| {
        fs::rename(temporary, target)
    })
}

fn write_text_with_replace(
    path: &Path,
    text: &str,
    mut replace: impl FnMut(&Path, &Path) -> std::io::Result<()>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("{}.tmp", Uuid::new_v4().simple()));
    fs::write(&tmp, text)?;
    match replace(&tmp, path) {
        Ok(()) => Ok(()),
        Err(first_error) if path.exists() => {
            let previous = path.with_extension(format!("{}.previous", Uuid::new_v4().simple()));
            let result = (|| -> Result<()> {
                replace(path, &previous).with_context(|| {
                    format!("failed to stage {} for replacement", path.display())
                })?;
                if let Err(replace_error) = replace(&tmp, path) {
                    let restore_result = replace(&previous, path);
                    return Err(anyhow!(
                        "failed to replace {} after {first_error}: {replace_error}; restore result: {restore_result:?}",
                        path.display()
                    ));
                }
                fs::remove_file(&previous).ok();
                Ok(())
            })();
            if result.is_err() {
                fs::remove_file(&tmp).ok();
            }
            result
        }
        Err(error) => {
            fs::remove_file(&tmp).ok();
            Err(error).with_context(|| format!("failed to write {}", path.display()))
        }
    }
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
    use crate::config::ProviderConfig;

    #[cfg(windows)]
    fn create_test_directory_link(link: &Path, target: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(unix)]
    fn create_test_directory_link(link: &Path, target: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[test]
    fn local_base_url_formats_ipv6_and_wildcards_for_clients() {
        assert_eq!(local_base_url("::1", 18_883), "http://[::1]:18883");
        assert_eq!(
            local_base_url("2001:db8::10", 18_883),
            "http://[2001:db8::10]:18883"
        );
        assert_eq!(local_base_url("0.0.0.0", 18_883), "http://127.0.0.1:18883");
        assert_eq!(
            local_base_url("127.0.0.1", 18_883),
            "http://127.0.0.1:18883"
        );
    }

    #[test]
    fn controlled_target_rejects_a_persisted_path_outside_its_agent_root() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-injection-target-boundary-{}",
            Uuid::new_v4().simple()
        ));
        let allowed = dir.join(".codex").join("config.toml");
        let outside = dir.join("outside").join("config.toml");
        fs::create_dir_all(allowed.parent().unwrap()).unwrap();
        fs::create_dir_all(outside.parent().unwrap()).unwrap();

        let error = validate_target_against_allowed("Codex", &outside, &[allowed]).unwrap_err();

        assert!(error
            .to_string()
            .contains("outside its controlled directory"));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn remove_item_rejects_an_uncontrolled_persisted_target_before_accessing_it() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-injection-remove-boundary-{}",
            Uuid::new_v4().simple()
        ));
        let target = dir.join("config.toml");
        let item = AgentInjectionConfig {
            id: "codex".to_string(),
            label: "Codex".to_string(),
            kind: AgentInjectionKind::Codex,
            enabled: true,
            provider_id: None,
            model_id: None,
            target_path: Some(target.clone()),
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        };

        let error = remove_item(&item).unwrap_err();

        assert!(error
            .to_string()
            .contains("outside its controlled directory"));
        assert!(!target.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn controlled_target_rejects_symlinked_agent_directory() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-injection-target-symlink-{}",
            Uuid::new_v4().simple()
        ));
        let target = dir.join("real-codex");
        let link = dir.join(".codex");
        fs::create_dir_all(&target).unwrap();

        // Windows can deny symlink creation when Developer Mode / the
        // privilege is unavailable. The production check is still exercised
        // on environments where the OS permits creating a test link.
        if create_test_directory_link(&link, &target).is_ok() {
            let error =
                validate_controlled_file_path(&link.join("config.toml"), &link).unwrap_err();
            assert!(error.to_string().contains("symlink") || error.to_string().contains("reparse"));
        }
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn catalog_manifest_refuses_catalogs_outside_the_codex_root() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-catalog-root-boundary-{}",
            Uuid::new_v4().simple()
        ));
        let config = dir.join("codex").join("config.toml");
        let external = dir.join("external").join("atoapi-model-catalog-user.json");
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        fs::create_dir_all(external.parent().unwrap()).unwrap();
        write_text(&external, "{\"owner\":\"user\"}\n").unwrap();

        let mut manifest = CodexCatalogManifest::default();
        assert!(register_owned_codex_catalog(&mut manifest, &config, &external).is_err());
        assert!(external.exists());
        assert!(manifest.entries.is_empty());
        fs::remove_dir_all(dir).ok();
    }

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
            auto_compact_token_limit: None,
            codex_context_settings: None,
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
            discovered_codex_models: None,
        };

        assert!(write_codex_config(&path, &context).unwrap());
        let first_document: toml::Value =
            toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            first_document
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        assert!(!write_codex_config(&path, &context).unwrap());
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
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        let active_catalogs = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|candidate| is_managed_codex_catalog_path(candidate))
            .collect::<Vec<_>>();
        assert_eq!(active_catalogs.len(), 1);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_injection_preserves_native_model_catalog_without_creating_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-native-catalog-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        let native_catalog = dir.join("native-models.json");
        write_text(&native_catalog, "{\"models\":[]}\n").unwrap();
        write_text(&path, "model_catalog_json = \"native-models.json\"\n").unwrap();

        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-native-catalog".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-5.5".to_string(),
            default_model_is_explicit: false,
            model_context_window: None,
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
        };

        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some("native-models.json")
        );
        assert!(native_catalog.exists());
        let managed_catalogs = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|candidate| is_managed_codex_catalog_path(candidate))
            .collect::<Vec<_>>();
        assert!(managed_catalogs.is_empty());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_injection_suppresses_cc_switch_catalog_while_atoapi_is_active() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-competing-catalog-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        let competing_catalog = dir.join("cc-switch-model-catalog.json");
        write_text(&competing_catalog, "{}\n").unwrap();
        let competing_config = r#"model_provider = "custom"
model_catalog_json = "cc-switch-model-catalog.json"

[model_providers.custom]
name = "Atoapi"
base_url = "http://127.0.0.1:15721/v1"
wire_api = "responses"
"#;
        write_text(&path, competing_config).unwrap();
        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-competing-catalog".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-6.1-sol".to_string(),
            default_model_is_explicit: true,
            model_context_window: None,
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
        };

        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        let catalog: Value = serde_json::from_str(
            &fs::read_to_string(dir.join(CODEX_MODEL_CATALOG_ACTIVE_FILE)).unwrap(),
        )
        .unwrap();
        let model = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["slug"].as_str() == Some("gpt-6.1-sol"))
            .unwrap();
        let efforts = model["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|level| level["effort"].as_str())
            .collect::<Vec<_>>();
        assert!(efforts.contains(&"xhigh"));
        assert!(efforts.contains(&"max"));
        assert!(efforts.contains(&"ultra"));
        assert!(model["base_instructions"]
            .as_str()
            .unwrap()
            .contains("中文"));
        assert_eq!(
            parsed
                .get("model_providers")
                .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
                .and_then(|provider| provider.get("base_url"))
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:18883/codex/v1")
        );
        assert!(competing_catalog.exists());

        // Simulate another launcher overwriting config.toml after startup.
        // The periodic guard must restore Atoapi's route and suppress the
        // competing catalog again without requiring a manual refresh.
        write_text(&path, competing_config).unwrap();
        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        assert_eq!(
            parsed
                .get("model_providers")
                .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
                .and_then(|provider| provider.get("base_url"))
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:18883/codex/v1")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_injection_preserves_unverified_legacy_catalog_from_restore_state() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-legacy-catalog-upgrade-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        let legacy_catalog = dir.join("atoapi-model-catalog.json");
        write_text(&legacy_catalog, "{\"models\":[]}\n").unwrap();
        write_text(
            &path,
            r#"model_provider = "custom"
model_catalog_json = "atoapi-model-catalog.json"

[model_providers.custom]
name = "Atoapi"
base_url = "http://127.0.0.1:18883/codex/v1"
"#,
        )
        .unwrap();
        let legacy_doc: DocumentMut = fs::read_to_string(&path).unwrap().parse().unwrap();
        let restore_path = codex_restore_state_path(&path).unwrap();
        capture_codex_restore_state(&restore_path, &path, true, &legacy_doc).unwrap();

        let context = InjectionContext {
            anthropic_base_url: "http://127.0.0.1:18883".to_string(),
            openai_base_url: "http://127.0.0.1:18883/v1".to_string(),
            codex_base_url: "http://127.0.0.1:18883/codex/v1".to_string(),
            local_key: "ato-legacy-catalog".to_string(),
            default_channel: "responses".to_string(),
            default_model: "gpt-5.5".to_string(),
            default_model_is_explicit: false,
            model_context_window: None,
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
        };

        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some("atoapi-model-catalog.json")
        );
        assert!(legacy_catalog.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn startup_artifact_reconciliation_removes_only_unreferenced_codex_catalogs() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-artifact-cleanup-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        let active_catalog = dir.join("atoapi-model-catalog-active.json");
        let stale_catalog = dir.join("atoapi-model-catalog.json");
        let stale_unique_catalog = dir.join("atoapi-model-catalog-old.json");
        let user_named_catalog = dir.join("atoapi-model-catalog-user-owned.json");
        fs::create_dir_all(&dir).unwrap();
        write_text(&active_catalog, "{}\n").unwrap();
        write_text(&stale_catalog, "{}\n").unwrap();
        write_text(&stale_unique_catalog, "{}\n").unwrap();
        write_text(&user_named_catalog, "{\"owner\":\"user\"}\n").unwrap();

        let mut doc = DocumentMut::new();
        doc["model_provider"] = value(CODEX_PROVIDER_ID);
        doc["model_catalog_json"] = value(active_catalog.to_string_lossy().as_ref());
        doc["model_providers"] = toml_edit::table();
        doc["model_providers"][CODEX_PROVIDER_ID] = toml_edit::table();
        let provider = doc["model_providers"][CODEX_PROVIDER_ID]
            .as_table_mut()
            .unwrap();
        provider["name"] = value("Atoapi");
        provider["base_url"] = value("http://127.0.0.1:18883/codex/v1");
        write_text(&path, &doc.to_string()).unwrap();

        let restore_path = codex_restore_state_path(&path).unwrap();
        capture_codex_restore_state(&restore_path, &path, true, &doc).unwrap();
        let mut restore_state = load_codex_restore_state(&restore_path, &path)
            .unwrap()
            .expect("restore state must exist");
        register_owned_codex_catalog(&mut restore_state.catalog_manifest, &path, &active_catalog)
            .unwrap();
        register_owned_codex_catalog(&mut restore_state.catalog_manifest, &path, &stale_catalog)
            .unwrap();
        register_owned_codex_catalog(
            &mut restore_state.catalog_manifest,
            &path,
            &stale_unique_catalog,
        )
        .unwrap();
        save_codex_restore_state(&restore_path, &restore_state).unwrap();
        cleanup_stale_codex_artifacts_for_target(&path).unwrap();

        assert!(active_catalog.exists());
        assert!(!stale_catalog.exists());
        assert!(!stale_unique_catalog.exists());
        assert!(user_named_catalog.exists());
        assert!(restore_path.exists());

        let later_stale_catalog = dir.join("atoapi-model-catalog-later.json");
        write_text(&later_stale_catalog, "{}\n").unwrap();
        let mut restore_state = load_codex_restore_state(&restore_path, &path)
            .unwrap()
            .expect("restore state must remain present");
        register_owned_codex_catalog(
            &mut restore_state.catalog_manifest,
            &path,
            &later_stale_catalog,
        )
        .unwrap();
        save_codex_restore_state(&restore_path, &restore_state).unwrap();
        write_text(&path, "model_provider = \"native\"\n").unwrap();
        cleanup_stale_codex_artifacts_for_target(&path).unwrap();

        assert!(!active_catalog.exists());
        assert!(!later_stale_catalog.exists());
        assert!(user_named_catalog.exists());
        assert!(!restore_path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn disabled_codex_injection_restores_original_fields_and_preserves_new_content() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-disable-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        fs::create_dir_all(&dir).unwrap();
        write_text(
            &path,
            r#"model = "gpt-original"
model_provider = "native"
model_catalog_json = "native-models.json"
disable_response_storage = false
model_reasoning_effort = "ultra"
model_context_window = 999000

[model_providers.custom]
name = "Native custom"
base_url = "https://native.example/v1"
wire_api = "responses"
experimental_bearer_token = "secret-marker"

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
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
        };
        write_codex_config(&path, &context).unwrap();
        let managed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let managed_catalog_path = managed
            .get("model_catalog_json")
            .and_then(toml::Value::as_str)
            .map(PathBuf::from)
            .unwrap();
        let restore_path = codex_restore_state_path(&path).unwrap();
        let envelope = fs::read_to_string(&restore_path).unwrap();
        assert!(envelope.contains("dpapi:"));
        assert!(!envelope.contains("secret-marker"));
        assert!(!envelope.contains("gpt-original"));
        assert!(!envelope.contains("native.example"));

        let mut injected = fs::read_to_string(&path).unwrap();
        injected.push_str("\n[mcp_servers.added_later]\ncommand = \"node\"\n");
        write_text(&path, &injected).unwrap();

        remove_codex_config_injection(&path).unwrap();

        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed.get("model").and_then(toml::Value::as_str),
            Some("gpt-original")
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
            Some(999_000)
        );
        assert_eq!(
            parsed.get("model_provider").and_then(toml::Value::as_str),
            Some("native")
        );
        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some("native-models.json")
        );
        assert!(parsed.get("disable_response_storage").is_none());
        assert_eq!(
            parsed
                .get("model_providers")
                .and_then(|providers| providers.get(CODEX_PROVIDER_ID))
                .and_then(|provider| provider.get("base_url"))
                .and_then(toml::Value::as_str),
            Some("https://native.example/v1")
        );
        assert!(parsed.get("mcp_servers").is_some());
        assert!(parsed
            .get("mcp_servers")
            .and_then(|servers| servers.get("added_later"))
            .is_some());
        assert!(!managed_catalog_path.exists());
        assert!(!restore_path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_auto_compaction_override_is_restored_when_disabled_or_removed() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-auto-compact-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        fs::create_dir_all(&dir).unwrap();
        write_text(
            &path,
            "model_auto_compact_token_limit = 246000\nmodel_auto_compact_token_limit_scope = \"total\"\n[mcp_servers.context7]\ncommand = \"npx\"\n",
        )
        .unwrap();

        let mut context = test_context();
        context.auto_compact_token_limit = Some(120_000);
        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_auto_compact_token_limit")
                .and_then(toml::Value::as_integer),
            Some(120_000)
        );
        assert_eq!(
            parsed
                .get("model_auto_compact_token_limit_scope")
                .and_then(toml::Value::as_str),
            Some("body_after_prefix")
        );

        context.auto_compact_token_limit = None;
        write_codex_config(&path, &context).unwrap();
        let restored_while_managed: toml::Value =
            toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            restored_while_managed
                .get("model_auto_compact_token_limit")
                .and_then(toml::Value::as_integer),
            Some(246_000)
        );
        assert_eq!(
            restored_while_managed
                .get("model_auto_compact_token_limit_scope")
                .and_then(toml::Value::as_str),
            Some("total")
        );

        context.auto_compact_token_limit = Some(140_000);
        write_codex_config(&path, &context).unwrap();
        remove_codex_config_injection(&path).unwrap();
        let restored_after_removal: toml::Value =
            toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            restored_after_removal
                .get("model_auto_compact_token_limit")
                .and_then(toml::Value::as_integer),
            Some(246_000)
        );
        assert_eq!(
            restored_after_removal
                .get("model_auto_compact_token_limit_scope")
                .and_then(toml::Value::as_str),
            Some("total")
        );
        assert!(restored_after_removal.get("mcp_servers").is_some());

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn old_restore_state_captures_auto_compaction_before_atoapi_manages_it() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-auto-compact-legacy-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        fs::create_dir_all(&dir).unwrap();
        write_text(
            &path,
            r#"model_provider = "custom"
model_auto_compact_token_limit = 246000
model_auto_compact_token_limit_scope = "total"

[model_providers.custom]
name = "Atoapi"
base_url = "http://127.0.0.1:18883/codex/v1"
wire_api = "responses"
"#,
        )
        .unwrap();
        let legacy_state = CodexRestoreState {
            schema_version: CODEX_RESTORE_STATE_VERSION,
            target_path: absolute_codex_target_path(&path).unwrap(),
            target_existed: true,
            fragment_toml: "model = \"user-model\"\n".to_string(),
            managed_root_fields: Vec::new(),
            catalog_manifest: CodexCatalogManifest::default(),
        };
        let restore_path = codex_restore_state_path(&path).unwrap();
        save_codex_restore_state(&restore_path, &legacy_state).unwrap();

        let mut context = test_context();
        context.auto_compact_token_limit = Some(120_000);
        write_codex_config(&path, &context).unwrap();
        context.auto_compact_token_limit = None;
        write_codex_config(&path, &context).unwrap();

        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed
                .get("model_auto_compact_token_limit")
                .and_then(toml::Value::as_integer),
            Some(246_000)
        );
        assert_eq!(
            parsed
                .get("model_auto_compact_token_limit_scope")
                .and_then(toml::Value::as_str),
            Some("total")
        );
        let migrated = load_codex_restore_state(&restore_path, &path)
            .unwrap()
            .expect("restore state must remain present");
        assert!(migrated
            .managed_root_fields
            .iter()
            .any(|field| field == "model_auto_compact_token_limit"));
        assert!(migrated
            .managed_root_fields
            .iter()
            .any(|field| field == "model_auto_compact_token_limit_scope"));

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn invalid_restore_state_leaves_managed_codex_config_unchanged() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-invalid-restore-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        let original = r#"model_provider = "custom"
model_catalog_json = "atoapi-model-catalog.json"

[model_providers.custom]
name = "Atoapi"
base_url = "http://127.0.0.1:18883/codex/v1"
"#;
        write_text(&path, original).unwrap();
        write_text(
            &codex_restore_state_path(&path).unwrap(),
            r#"{"schema_version":1,"encrypted_payload":"plaintext"}
"#,
        )
        .unwrap();

        let error = remove_codex_config_injection(&path).unwrap_err();

        assert!(error.to_string().contains("unencrypted"));
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn legacy_managed_codex_config_migrates_before_disable() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-legacy-disable-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        write_text(
            &path,
            r#"model = "gpt-5.6-luna"
model_reasoning_effort = "ultra"
model_provider = "custom"
model_catalog_json = "atoapi-model-catalog.json"
disable_response_storage = true

[model_providers.custom]
name = "Atoapi"
base_url = "http://127.0.0.1:18883/codex/v1"

[mcp_servers.context7]
command = "npx"
"#,
        )
        .unwrap();

        remove_codex_config_injection(&path).unwrap();

        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            parsed.get("model").and_then(toml::Value::as_str),
            Some("gpt-5.6-luna")
        );
        assert_eq!(
            parsed
                .get("model_reasoning_effort")
                .and_then(toml::Value::as_str),
            Some("ultra")
        );
        assert!(parsed.get("model_provider").is_none());
        assert!(parsed.get("model_catalog_json").is_none());
        assert!(parsed.get("disable_response_storage").is_none());
        assert!(parsed.get("mcp_servers").is_some());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn failed_config_replace_keeps_the_previous_file_intact() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-config-replace-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = dir.join("config.toml");
        write_text(&path, "model = \"before\"\n").unwrap();

        let error = write_text_with_replace(&path, "model = \"after\"\n", |_, _| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated locked config",
            ))
        });

        assert!(error.is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "model = \"before\"\n");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn injection_backup_restore_restores_the_original_file() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-agent-backup-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let config_root = dir.join("config-root");
        let path = dir.join("opencode.json");
        fs::create_dir_all(&dir).unwrap();
        let previous_config_root = std::env::var_os("ATOAPI_CONFIG_DIR");
        std::env::set_var("ATOAPI_CONFIG_DIR", &config_root);
        let original = "{\"provider\":\"native\"}\n";
        write_text(&path, original).unwrap();

        let backup = backup_file(&path).unwrap().unwrap();
        write_text(&path, "{\"provider\":\"atoapi\"}\n").unwrap();
        assert_eq!(backup_file(&path).unwrap(), Some(backup.clone()));

        restore_injection_backup(&path).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        fs::remove_file(backup).ok();
        match previous_config_root {
            Some(value) => std::env::set_var("ATOAPI_CONFIG_DIR", value),
            None => std::env::remove_var("ATOAPI_CONFIG_DIR"),
        }
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn unconfigured_agent_route_keeps_the_agent_model_passthrough() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
            id: "passthrough".to_string(),
            name: "passthrough".to_string(),
            base_url: "https://example.test/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: crate::config::Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: true,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

        update_route(
            &mut config,
            AgentInjectionRouteUpdate {
                id: "codex".to_string(),
                provider_id: Some("passthrough".to_string()),
                model_id: None,
            },
        )
        .unwrap();

        let codex = config
            .agent_injections
            .iter()
            .find(|agent| agent.id == "codex")
            .unwrap();
        assert_eq!(codex.provider_id.as_deref(), Some("passthrough"));
        assert!(codex.model_id.is_none());
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
            r#"model = "gpt-5.6-luna"
model_reasoning_effort = "ultra"
model_context_window = 888000
"#,
        )
        .unwrap();

        write_codex_config(&path, &context).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(
            parsed.get("model").and_then(toml::Value::as_str),
            Some("gpt-5.6-luna")
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

        write_text(
            &path,
            r#"model = "gpt-5.6-luna"
model_reasoning_effort = "max"
"#,
        )
        .unwrap();
        write_codex_config(&path, &context).unwrap();
        let supported: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            supported
                .get("model_reasoning_effort")
                .and_then(toml::Value::as_str),
            Some("max")
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
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
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

        assert_eq!(
            parsed
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn enabled_agent_without_provider_stays_unbound() {
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
        assert_eq!(codex.provider_id, None);
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
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
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

    #[test]
    fn codex_context_settings_apply_without_model_binding_and_clear_to_native_defaults() {
        let dir =
            std::env::temp_dir().join(format!("atoapi-codex-limits-{}", Uuid::new_v4().simple()));
        let path = dir.join("config.toml");
        write_text(
            &path,
            concat!(
                "model = \"official-next-model\"\n",
                "model_context_window = 900000\n",
                "model_auto_compact_token_limit = 600000\n",
                "model_auto_compact_token_limit_scope = \"total\"\n",
                "disable_response_storage = true\n",
                "[features.guardianv2]\nthread_context = true\n",
                "[mcp_servers.local]\ncommand = \"local-executor\"\n",
            ),
        )
        .unwrap();
        let assert_catalog = |window: Option<u64>, mapped_window: Option<u64>| {
            let catalog: Value = serde_json::from_str(
                &fs::read_to_string(dir.join(CODEX_MODEL_CATALOG_ACTIVE_FILE)).unwrap(),
            )
            .unwrap();
            let models = catalog["models"].as_array().unwrap();
            assert_eq!(
                models.len(),
                crate::config::CODEX_NATIVE_VISIBLE_MODELS.len()
            );
            for (slug, _, _) in crate::config::CODEX_NATIVE_VISIBLE_MODELS {
                let model = models
                    .iter()
                    .find(|model| model["slug"].as_str() == Some(slug))
                    .unwrap();
                let expected_window = window.unwrap_or_else(|| {
                    if slug == "gpt-6.1-sol" {
                        mapped_window.unwrap_or(128_000)
                    } else {
                        128_000
                    }
                });
                assert_eq!(
                    model["context_window"].as_u64(),
                    Some(expected_window),
                    "{slug}"
                );
                assert_eq!(
                    model["max_context_window"].as_u64(),
                    Some(expected_window),
                    "{slug}"
                );
                let efforts = model["supported_reasoning_levels"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|level| level["effort"].as_str())
                    .collect::<Vec<_>>();
                for effort in CODEX_DEFAULT_REASONING_EFFORTS {
                    assert!(efforts.contains(&effort), "{slug}: {effort}");
                }
            }
        };
        let mut context = test_context();
        context.default_model_is_explicit = false;
        context.auto_compact_token_limit = Some(111_000);
        context.discovered_codex_models = Some(vec![json!({
            "id": "gpt-6.1-sol",
            "context_window": 128_000,
            "max_context_window": 128_000,
        })]);
        context.codex_context_settings = Some(CodexContextSettings {
            model_context_window: Some(872_000),
            model_auto_compact_token_limit: Some(740_000),
        });
        assert!(write_codex_config(&path, &context).unwrap());
        assert!(!write_codex_config(&path, &context).unwrap());
        let first: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(first["model"].as_str(), Some("official-next-model"));
        assert_eq!(first["model_context_window"].as_integer(), Some(872_000));
        assert_catalog(Some(872_000), None);
        assert_eq!(
            first["model_auto_compact_token_limit"].as_integer(),
            Some(740_000)
        );
        assert!(first.get("disable_response_storage").is_none());
        assert_eq!(
            first
                .get("model_catalog_json")
                .and_then(toml::Value::as_str),
            Some(CODEX_MODEL_CATALOG_ACTIVE_FILE)
        );
        assert_eq!(
            first["features"]["guardianv2"]["thread_context"].as_bool(),
            Some(true)
        );

        // Simulate a different upstream/model mapping during reinjection.
        context.default_model_is_explicit = true;
        context.model_context_window = Some(128_000);
        context.auto_compact_token_limit = Some(90_000);
        context.local_key = "ato-other-route".to_string();
        context.discovered_codex_models = None;
        context.codex_models = vec![ModelConfig {
            id: "deepseek-v4.1-flash".to_string(),
            request_model_id: Some("gpt-6.1-sol".to_string()),
            display_name: "DeepSeek V4.1 Flash".to_string(),
            context_window: Some(256_000),
            output_window: None,
            reasoning_effort_override_enabled: false,
            reasoning_effort: None,
            supported_reasoning_efforts: Vec::new(),
            supports_tools: true,
            supports_streaming: true,
            enabled: true,
        }];
        write_codex_config(&path, &context).unwrap();
        assert_catalog(Some(872_000), Some(256_000));
        let routed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(routed["model_context_window"].as_integer(), Some(872_000));
        assert_eq!(
            routed["model_auto_compact_token_limit"].as_integer(),
            Some(740_000)
        );

        for (window, limit) in [
            (Some(500_000), Some(400_000)),
            (Some(872_000), None),
            (None, Some(740_000)),
            (None, None),
        ] {
            context.codex_context_settings = Some(CodexContextSettings {
                model_context_window: window,
                model_auto_compact_token_limit: limit,
            });
            write_codex_config(&path, &context).unwrap();
            let doc: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            assert_eq!(
                doc.get("model_context_window")
                    .and_then(toml::Value::as_integer),
                window.map(i64::from)
            );
            assert_eq!(
                doc.get("model_auto_compact_token_limit")
                    .and_then(toml::Value::as_integer),
                limit.map(i64::from)
            );
            assert_eq!(
                doc.get("model_auto_compact_token_limit_scope")
                    .and_then(toml::Value::as_str),
                limit.map(|_| "body_after_prefix")
            );
            assert_catalog(window.map(u64::from), Some(256_000));
            assert!(!write_codex_config(&path, &context).unwrap());
        }
        // A route without a mapping must not inherit the last route's window
        // or any global override from the persisted catalog during discovery failure.
        context.codex_models.clear();
        context.default_model_is_explicit = false;
        context.codex_context_settings = Some(CodexContextSettings {
            model_context_window: Some(500_000),
            model_auto_compact_token_limit: Some(400_000),
        });
        assert!(write_codex_config(&path, &context).unwrap());
        assert_catalog(Some(500_000), None);
        assert!(!write_codex_config(&path, &context).unwrap());
        context.codex_context_settings = Some(CodexContextSettings::default());
        assert!(write_codex_config(&path, &context).unwrap());
        assert_catalog(None, None);
        assert!(!write_codex_config(&path, &context).unwrap());
        remove_codex_config_injection(&path).unwrap();
        let restored: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(restored["model_context_window"].as_integer(), Some(900_000));
        assert_eq!(
            restored["model_auto_compact_token_limit"].as_integer(),
            Some(600_000)
        );
        assert_eq!(
            restored["model_auto_compact_token_limit_scope"].as_str(),
            Some("total")
        );
        assert!(restored.get("disable_response_storage").is_none());
        assert!(restored.get("mcp_servers").is_some());
        write_codex_config(&path, &context).unwrap();
        assert!(
            toml::from_str::<toml::Value>(&fs::read_to_string(&path).unwrap())
                .unwrap()
                .get("disable_response_storage")
                .is_none()
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn invalid_codex_context_settings_do_not_modify_artifacts() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-invalid-limits-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("config.toml");
        let original = "model = \"native\"\n";
        write_text(&path, original).unwrap();
        let mut context = test_context();
        context.codex_context_settings = Some(CodexContextSettings {
            model_context_window: Some(100),
            model_auto_compact_token_limit: Some(101),
        });
        assert!(write_codex_config(&path, &context).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert!(!codex_restore_state_path(&path).unwrap().exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_injection_replaces_a_backend_model_id_with_its_request_alias() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-codex-model-alias-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("config.toml");
        write_text(&path, "model = \"deepseek-v4.1-flash\"\n").unwrap();
        let mut context = test_context();
        context.default_model = "gpt-6-sol".to_string();
        context.default_model_is_explicit = false;
        context.codex_models = vec![ModelConfig {
            id: "deepseek-v4.1-flash".to_string(),
            request_model_id: Some("gpt-6-sol".to_string()),
            display_name: "DeepSeek V4.1 Flash".to_string(),
            context_window: Some(272_000),
            output_window: None,
            reasoning_effort_override_enabled: false,
            reasoning_effort: None,
            supported_reasoning_efforts: Vec::new(),
            supports_tools: true,
            supports_streaming: true,
            enabled: true,
        }];

        write_codex_config(&path, &context).unwrap();

        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed["model"].as_str(), Some("gpt-6-sol"));
        let catalog: Value = serde_json::from_str(
            &fs::read_to_string(dir.join(CODEX_MODEL_CATALOG_ACTIVE_FILE)).unwrap(),
        )
        .unwrap();
        let selected = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["slug"].as_str() == Some("gpt-6-sol"))
            .unwrap();
        assert_eq!(selected["display_name"].as_str(), Some("GPT-6-Sol"));
        assert!(!catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"].as_str() == Some("deepseek-v4.1-flash")));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn codex_catalog_shows_only_codex_request_models_with_all_reasoning_levels() {
        let mut context = test_context();
        context.discovered_codex_models = Some(vec![
            json!({
                "id": "gpt-6.1-sol",
                "display_name": "GPT-6.1-Sol",
                "supported_reasoning_efforts": ["low"]
            }),
            json!({
                "id": "gpt-6.1-luna",
                "display_name": "GPT-6.1-Luna",
                "supported_reasoning_levels": [{"effort": "medium"}]
            }),
            json!({
                "id": "gpt-5.6-luna",
                "display_name": "GPT-5.6-Luna"
            }),
            json!({
                "id": "deepseek-v4.1-flash",
                "display_name": "DeepSeek V4.1 Flash"
            }),
        ]);

        let catalog =
            codex_model_catalog_value(&context, context.discovered_codex_models.as_deref());
        let models = catalog["models"].as_array().unwrap();
        let slugs = models
            .iter()
            .filter_map(|model| model["slug"].as_str())
            .collect::<Vec<_>>();

        assert!(slugs.contains(&"gpt-6.1-sol"));
        assert!(slugs.contains(&"gpt-5.6-luna"));
        assert!(!slugs.contains(&"gpt-6.1-luna"));
        assert!(!slugs.contains(&"deepseek-v4.1-flash"));
        assert_eq!(
            slugs.len(),
            crate::config::CODEX_NATIVE_VISIBLE_MODELS.len()
        );
        for (slug, _, _) in crate::config::CODEX_NATIVE_VISIBLE_MODELS {
            let model = models
                .iter()
                .find(|model| model["slug"].as_str() == Some(slug))
                .unwrap();
            let efforts = model["supported_reasoning_levels"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|level| level["effort"].as_str())
                .collect::<Vec<_>>();
            for effort in CODEX_DEFAULT_REASONING_EFFORTS {
                assert!(efforts.contains(&effort));
            }
        }
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
            auto_compact_token_limit: None,
            codex_context_settings: None,
            codex_models: Vec::new(),
            discovered_codex_models: None,
        }
    }
}
