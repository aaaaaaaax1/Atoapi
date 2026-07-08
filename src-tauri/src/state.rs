use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex, RwLock},
};

use crate::{
    agent_injection,
    cache::{cache_path, CacheStore},
    config::{app_config_dir, config_path, AppConfig, PublicConfig},
    metrics::MetricsStore,
    proxy,
};

#[derive(Debug, Clone, Serialize)]
pub struct ProxyStatus {
    pub running: bool,
    pub address: Option<String>,
}

#[derive(Debug)]
struct ProxyServer {
    address: String,
    shutdown: oneshot::Sender<()>,
}

#[derive(Debug)]
pub struct AppState {
    pub config: RwLock<AppConfig>,
    pub config_path: PathBuf,
    pub runtime_state_path: PathBuf,
    pub cache: CacheStore,
    pub metrics: MetricsStore,
    pub client: reqwest::Client,
    pub prefix_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    pub prefix_states: Mutex<HashMap<String, PrefixWarmState>>,
    pub prefix_error_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub prefix_prewarm_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub request_body_gzip_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub compact_chat_compat_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub response_session_error_cooldowns: Mutex<HashMap<String, ResponseSessionCooldownState>>,
    pub response_sessions: Mutex<HashMap<String, ResponseSessionState>>,
    pub provider_route_affinity: Mutex<HashMap<String, String>>,
    pub provider_key_affinity: Mutex<HashMap<String, String>>,
    server: Mutex<Option<ProxyServer>>,
}

#[derive(Debug, Clone)]
pub struct PrefixWarmState {
    pub finished_at: std::time::Instant,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub shortfall_tokens: u64,
    pub seen_bucket_tokens: u64,
    pub avoidable_shortfall_tokens: u64,
    pub avoidable_shortfall_streak: u32,
    pub shortfall_tokens_128: u64,
    pub seen_bucket_tokens_128: u64,
    pub avoidable_shortfall_tokens_128: u64,
    pub small_gap_recovery_streak: u32,
    pub cache_instability_score: u32,
    pub tail_tool_output_chars: u64,
    pub tail_largest_tool_output_chars: u64,
    pub tail_tool_output_noise_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResponseSessionState {
    pub response_id: String,
    pub input: serde_json::Value,
    pub scope_key: Option<String>,
    pub finished_at: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct ResponseSessionCooldownState {
    pub until: std::time::Instant,
    pub failures: u32,
    pub unsupported: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedRuntimeState {
    #[serde(default)]
    prefix_states: HashMap<String, PersistedPrefixWarmState>,
    #[serde(default)]
    response_sessions: HashMap<String, PersistedResponseSessionState>,
    #[serde(default)]
    response_session_error_cooldowns: HashMap<String, PersistedResponseSessionCooldownState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPrefixWarmState {
    saved_at: chrono::DateTime<Utc>,
    input_tokens: u64,
    cache_read_tokens: u64,
    shortfall_tokens: u64,
    seen_bucket_tokens: u64,
    avoidable_shortfall_tokens: u64,
    avoidable_shortfall_streak: u32,
    shortfall_tokens_128: u64,
    seen_bucket_tokens_128: u64,
    avoidable_shortfall_tokens_128: u64,
    #[serde(default)]
    small_gap_recovery_streak: u32,
    #[serde(default)]
    cache_instability_score: u32,
    #[serde(default)]
    tail_tool_output_chars: u64,
    #[serde(default)]
    tail_largest_tool_output_chars: u64,
    #[serde(default)]
    tail_tool_output_noise_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedResponseSessionState {
    saved_at: chrono::DateTime<Utc>,
    response_id: String,
    input: serde_json::Value,
    #[serde(default)]
    scope_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedResponseSessionCooldownState {
    saved_at: chrono::DateTime<Utc>,
    until_at: chrono::DateTime<Utc>,
    failures: u32,
    #[serde(default)]
    unsupported: bool,
}

const RUNTIME_STATE_TTL: StdDuration = StdDuration::from_secs(30 * 60);
const PREFIX_RUNTIME_STATE_TTL: StdDuration = StdDuration::from_secs(20 * 60);

impl AppState {
    pub fn load() -> Result<Self> {
        let config_path = config_path()?;
        let config_dir = app_config_dir()?;
        let mut config = AppConfig::load_or_create(&config_path)?;
        agent_injection::ensure_defaults(&mut config);
        config.save(&config_path)?;
        let cache = CacheStore::load(cache_path(&config_dir))?;
        let runtime_state_path = runtime_state_path(&config_dir);
        let runtime_state = load_runtime_state(&runtime_state_path)?;
        Ok(Self {
            config: RwLock::new(config),
            config_path: config_path.clone(),
            runtime_state_path,
            cache,
            metrics: MetricsStore::new(),
            client: reqwest::Client::builder()
                .user_agent("Atoapi/0.1")
                .build()?,
            prefix_locks: Mutex::new(HashMap::new()),
            prefix_states: Mutex::new(runtime_state.prefix_states),
            prefix_error_cooldowns: Mutex::new(HashMap::new()),
            prefix_prewarm_cooldowns: Mutex::new(HashMap::new()),
            request_body_gzip_cooldowns: Mutex::new(HashMap::new()),
            compact_chat_compat_cooldowns: Mutex::new(HashMap::new()),
            response_session_error_cooldowns: Mutex::new(
                runtime_state.response_session_error_cooldowns,
            ),
            response_sessions: Mutex::new(runtime_state.response_sessions),
            provider_route_affinity: Mutex::new(HashMap::new()),
            provider_key_affinity: Mutex::new(HashMap::new()),
            server: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub fn for_test(config: AppConfig, config_path: PathBuf, cache: CacheStore) -> Result<Self> {
        let runtime_state_path = config_path.with_file_name("runtime-state.json");
        Ok(Self {
            config: RwLock::new(config),
            config_path: config_path.clone(),
            runtime_state_path,
            cache,
            metrics: MetricsStore::new(),
            client: reqwest::Client::builder()
                .user_agent("AtoapiTest/0.1")
                .build()?,
            prefix_locks: Mutex::new(HashMap::new()),
            prefix_states: Mutex::new(HashMap::new()),
            prefix_error_cooldowns: Mutex::new(HashMap::new()),
            prefix_prewarm_cooldowns: Mutex::new(HashMap::new()),
            request_body_gzip_cooldowns: Mutex::new(HashMap::new()),
            compact_chat_compat_cooldowns: Mutex::new(HashMap::new()),
            response_session_error_cooldowns: Mutex::new(HashMap::new()),
            response_sessions: Mutex::new(HashMap::new()),
            provider_route_affinity: Mutex::new(HashMap::new()),
            provider_key_affinity: Mutex::new(HashMap::new()),
            server: Mutex::new(None),
        })
    }

    pub async fn public_config(&self) -> PublicConfig {
        self.config
            .read()
            .await
            .public_view(self.config_path.clone())
    }

    pub async fn reload_config(&self) -> Result<PublicConfig> {
        let mut config = AppConfig::load_or_create(&self.config_path)?;
        agent_injection::ensure_defaults(&mut config);
        config.save(&self.config_path)?;
        let public = config.public_view(self.config_path.clone());
        *self.config.write().await = config;
        Ok(public)
    }

    pub async fn apply_enabled_agent_injections_on_startup(&self) -> Result<()> {
        let mut config = self.config.write().await;
        let has_enabled_agent = config.agent_injections.iter().any(|item| item.enabled);
        if !has_enabled_agent {
            return Ok(());
        }

        agent_injection::apply_enabled(&mut config)?;
        config.updated_at = Utc::now();
        config.save(&self.config_path)?;
        Ok(())
    }

    pub async fn start_proxy(self: &Arc<Self>) -> Result<ProxyStatus> {
        let mut server_guard = self.server.lock().await;
        if let Some(server) = server_guard.as_ref() {
            self.set_proxy_auto_start(true).await?;
            return Ok(ProxyStatus {
                running: true,
                address: Some(server.address.clone()),
            });
        }

        let config = self.config.read().await.clone();
        let bind = format!("{}:{}", config.host, config.port);
        let listener = TcpListener::bind(&bind)
            .await
            .with_context(|| format!("failed to bind local proxy at {bind}"))?;
        let address = listener.local_addr()?.to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let router = proxy::router(self.clone());

        tokio::spawn(async move {
            let result = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;

            if let Err(err) = result {
                eprintln!("local proxy server stopped with error: {err}");
            }
        });

        *server_guard = Some(ProxyServer {
            address: address.clone(),
            shutdown: shutdown_tx,
        });
        self.set_proxy_auto_start(true).await?;
        Ok(ProxyStatus {
            running: true,
            address: Some(address),
        })
    }

    pub async fn stop_proxy(&self) -> Result<ProxyStatus> {
        let mut server_guard = self.server.lock().await;
        if let Some(server) = server_guard.take() {
            let _ = server.shutdown.send(());
        }
        self.set_proxy_auto_start(false).await?;
        Ok(ProxyStatus {
            running: false,
            address: None,
        })
    }

    pub async fn proxy_status(&self) -> ProxyStatus {
        let server_guard = self.server.lock().await;
        ProxyStatus {
            running: server_guard.is_some(),
            address: server_guard.as_ref().map(|server| server.address.clone()),
        }
    }

    async fn set_proxy_auto_start(&self, enabled: bool) -> Result<()> {
        let mut config = self.config.write().await;
        if config.proxy_auto_start != enabled {
            config.proxy_auto_start = enabled;
            config.updated_at = Utc::now();
            config.save(&self.config_path)?;
        }
        Ok(())
    }

    pub async fn persist_runtime_state(&self) -> Result<()> {
        let prefix_states = self.prefix_states.lock().await.clone();
        let response_sessions = self.response_sessions.lock().await.clone();
        let response_session_error_cooldowns =
            self.response_session_error_cooldowns.lock().await.clone();
        save_runtime_state(
            &self.runtime_state_path,
            &prefix_states,
            &response_sessions,
            &response_session_error_cooldowns,
        )
    }
}

pub fn runtime_state_path(config_dir: &Path) -> PathBuf {
    config_dir.join("runtime-state.json")
}

fn load_runtime_state(path: &Path) -> Result<RuntimeStateMaps> {
    if !path.exists() {
        return Ok(RuntimeStateMaps::default());
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let persisted: PersistedRuntimeState = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(persisted.into_runtime())
}

fn save_runtime_state(
    path: &Path,
    prefix_states: &HashMap<String, PrefixWarmState>,
    response_sessions: &HashMap<String, ResponseSessionState>,
    response_session_error_cooldowns: &HashMap<String, ResponseSessionCooldownState>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let persisted = PersistedRuntimeState::from_runtime(
        prefix_states,
        response_sessions,
        response_session_error_cooldowns,
    );
    let raw = serde_json::to_string_pretty(&persisted)?;
    fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Default)]
struct RuntimeStateMaps {
    prefix_states: HashMap<String, PrefixWarmState>,
    response_sessions: HashMap<String, ResponseSessionState>,
    response_session_error_cooldowns: HashMap<String, ResponseSessionCooldownState>,
}

impl PersistedRuntimeState {
    fn from_runtime(
        prefix_states: &HashMap<String, PrefixWarmState>,
        response_sessions: &HashMap<String, ResponseSessionState>,
        response_session_error_cooldowns: &HashMap<String, ResponseSessionCooldownState>,
    ) -> Self {
        let now = Utc::now();
        let prefix_states = prefix_states
            .iter()
            .filter_map(|(key, state)| {
                (state.finished_at.elapsed() <= PREFIX_RUNTIME_STATE_TTL).then(|| {
                    (
                        key.clone(),
                        PersistedPrefixWarmState {
                            saved_at: now,
                            input_tokens: state.input_tokens,
                            cache_read_tokens: state.cache_read_tokens,
                            shortfall_tokens: state.shortfall_tokens,
                            seen_bucket_tokens: state.seen_bucket_tokens,
                            avoidable_shortfall_tokens: state.avoidable_shortfall_tokens,
                            avoidable_shortfall_streak: state.avoidable_shortfall_streak,
                            shortfall_tokens_128: state.shortfall_tokens_128,
                            seen_bucket_tokens_128: state.seen_bucket_tokens_128,
                            avoidable_shortfall_tokens_128: state.avoidable_shortfall_tokens_128,
                            small_gap_recovery_streak: state.small_gap_recovery_streak,
                            cache_instability_score: state.cache_instability_score,
                            tail_tool_output_chars: state.tail_tool_output_chars,
                            tail_largest_tool_output_chars: state.tail_largest_tool_output_chars,
                            tail_tool_output_noise_hint: state.tail_tool_output_noise_hint.clone(),
                        },
                    )
                })
            })
            .collect();
        let response_sessions = response_sessions
            .iter()
            .filter_map(|(key, state)| {
                let age = state.finished_at.elapsed();
                (age <= RUNTIME_STATE_TTL).then(|| {
                    let saved_at = chrono::Duration::from_std(age)
                        .ok()
                        .map(|age| now - age)
                        .unwrap_or(now);
                    (
                        key.clone(),
                        PersistedResponseSessionState {
                            saved_at,
                            response_id: state.response_id.clone(),
                            input: state.input.clone(),
                            scope_key: state.scope_key.clone(),
                        },
                    )
                })
            })
            .collect();
        let response_session_error_cooldowns = response_session_error_cooldowns
            .iter()
            .filter_map(|(key, state)| {
                if !state.unsupported {
                    return None;
                }
                let remaining = state.until.checked_duration_since(Instant::now());
                let until_at = remaining
                    .and_then(|duration| chrono::Duration::from_std(duration).ok())
                    .map(|duration| now + duration)
                    .unwrap_or(now);
                let expired_recently = Instant::now()
                    .checked_duration_since(state.until)
                    .map(|elapsed| elapsed <= RUNTIME_STATE_TTL)
                    .unwrap_or(false);
                (remaining.is_some() || expired_recently).then(|| {
                    (
                        key.clone(),
                        PersistedResponseSessionCooldownState {
                            saved_at: now,
                            until_at,
                            failures: state.failures,
                            unsupported: state.unsupported,
                        },
                    )
                })
            })
            .collect();

        Self {
            prefix_states,
            response_sessions,
            response_session_error_cooldowns,
        }
    }

    fn into_runtime(self) -> RuntimeStateMaps {
        let now = Utc::now();
        let instant_now = Instant::now();
        let prefix_states = self
            .prefix_states
            .into_iter()
            .filter_map(|(key, state)| {
                let age = (now - state.saved_at).to_std().ok()?;
                if age > PREFIX_RUNTIME_STATE_TTL {
                    return None;
                }
                Some((
                    key,
                    PrefixWarmState {
                        finished_at: instant_now.checked_sub(age).unwrap_or(instant_now),
                        input_tokens: state.input_tokens,
                        cache_read_tokens: state.cache_read_tokens,
                        shortfall_tokens: state.shortfall_tokens,
                        seen_bucket_tokens: state.seen_bucket_tokens,
                        avoidable_shortfall_tokens: state.avoidable_shortfall_tokens,
                        avoidable_shortfall_streak: state.avoidable_shortfall_streak,
                        shortfall_tokens_128: state.shortfall_tokens_128,
                        seen_bucket_tokens_128: state.seen_bucket_tokens_128,
                        avoidable_shortfall_tokens_128: state.avoidable_shortfall_tokens_128,
                        small_gap_recovery_streak: state.small_gap_recovery_streak,
                        cache_instability_score: state.cache_instability_score,
                        tail_tool_output_chars: state.tail_tool_output_chars,
                        tail_largest_tool_output_chars: state.tail_largest_tool_output_chars,
                        tail_tool_output_noise_hint: state.tail_tool_output_noise_hint,
                    },
                ))
            })
            .collect();
        let response_sessions = self
            .response_sessions
            .into_iter()
            .filter_map(|(key, state)| {
                let age = (now - state.saved_at).to_std().ok()?;
                if age > RUNTIME_STATE_TTL {
                    return None;
                }
                Some((
                    key,
                    ResponseSessionState {
                        response_id: state.response_id,
                        input: state.input,
                        scope_key: state.scope_key,
                        finished_at: instant_now.checked_sub(age).unwrap_or(instant_now),
                    },
                ))
            })
            .collect();
        let response_session_error_cooldowns = self
            .response_session_error_cooldowns
            .into_iter()
            .filter_map(|(key, state)| {
                let age = (now - state.saved_at).to_std().ok()?;
                if !state.unsupported {
                    return None;
                }
                let active = state.until_at > now;
                if !active && age > RUNTIME_STATE_TTL {
                    return None;
                }
                let until_delta = (state.until_at - now).to_std().ok();
                let until = until_delta
                    .and_then(|duration| instant_now.checked_add(duration))
                    .unwrap_or(instant_now);
                Some((
                    key,
                    ResponseSessionCooldownState {
                        until,
                        failures: state.failures,
                        unsupported: state.unsupported,
                    },
                ))
            })
            .collect();
        RuntimeStateMaps {
            prefix_states,
            response_sessions,
            response_session_error_cooldowns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AgentInjectionKind, CacheConfig, Channel, ModelConfig, ProviderConfig};
    use serde_json::json;
    use std::fs;
    use uuid::Uuid;

    #[tokio::test]
    async fn startup_reapplies_enabled_agent_injection_route() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-startup-agent-inject-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let target_path = dir.join("codex-config.toml");
        let mut config = AppConfig::default();
        config.host = "127.0.0.1".to_string();
        config.port = 18883;
        config.local_key = "ato-root-key".to_string();
        config.cache = CacheConfig::smart_max_hit();
        config.providers.push(ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
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
        config.active_provider_id = Some("share".to_string());
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|item| item.kind == AgentInjectionKind::Codex)
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("gpt-5.5".to_string());
        codex.target_path = Some(target_path.clone());

        let state = AppState::for_test(
            config,
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();

        state
            .apply_enabled_agent_injections_on_startup()
            .await
            .unwrap();

        let value: toml::Value =
            toml::from_str(&fs::read_to_string(&target_path).unwrap()).unwrap();
        assert_eq!(
            value.get("model").and_then(toml::Value::as_str),
            Some("gpt-5.5")
        );
        assert_eq!(
            value.get("model_provider").and_then(toml::Value::as_str),
            Some("atoapi")
        );
        assert_eq!(
            value
                .get("model_providers")
                .and_then(|providers| providers.get("atoapi"))
                .and_then(|provider| provider.get("base_url"))
                .and_then(toml::Value::as_str),
            Some("http://127.0.0.1:18883/codex/v1")
        );
        let config = state.config.read().await;
        let item = config
            .agent_injections
            .iter()
            .find(|item| item.id == "codex")
            .unwrap();
        assert!(item.enabled);
        assert_eq!(item.provider_id.as_deref(), Some("share"));
        assert_eq!(item.model_id.as_deref(), Some("gpt-5.5"));
        assert!(item.last_injected_at.is_some());

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_state_persists_recent_prefix_and_response_session() {
        let dir =
            std::env::temp_dir().join(format!("atoapi-runtime-state-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let state = AppState::for_test(
            AppConfig::default(),
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();
        state.prefix_states.lock().await.insert(
            "prefix-a".to_string(),
            PrefixWarmState {
                finished_at: Instant::now(),
                input_tokens: 170_000,
                cache_read_tokens: 166_912,
                shortfall_tokens: 3_072,
                seen_bucket_tokens: 166_912,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: 3_072,
                seen_bucket_tokens_128: 166_912,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                cache_instability_score: 0,
                tail_tool_output_chars: 0,
                tail_largest_tool_output_chars: 0,
                tail_tool_output_noise_hint: None,
            },
        );
        state.response_sessions.lock().await.insert(
            "session-a".to_string(),
            ResponseSessionState {
                response_id: "resp_123".to_string(),
                input: json!([{ "type": "message", "role": "user", "content": "hello" }]),
                scope_key: Some("scope-a".to_string()),
                finished_at: Instant::now(),
            },
        );
        state.response_session_error_cooldowns.lock().await.insert(
            "responses:share:gpt-5.5".to_string(),
            ResponseSessionCooldownState {
                until: Instant::now() + StdDuration::from_secs(3600),
                failures: 2,
                unsupported: true,
            },
        );

        state.persist_runtime_state().await.unwrap();
        let loaded = load_runtime_state(&state.runtime_state_path).unwrap();

        let prefix = loaded.prefix_states.get("prefix-a").unwrap();
        assert_eq!(prefix.cache_read_tokens, 166_912);
        let session = loaded.response_sessions.get("session-a").unwrap();
        assert_eq!(session.response_id, "resp_123");
        assert_eq!(session.input[0]["role"], "user");
        assert_eq!(session.scope_key.as_deref(), Some("scope-a"));
        let cooldown = loaded
            .response_session_error_cooldowns
            .get("responses:share:gpt-5.5")
            .unwrap();
        assert_eq!(cooldown.failures, 2);
        assert!(cooldown.unsupported);
        assert!(cooldown.until > Instant::now());

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_state_drops_transient_response_session_cooldowns() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-runtime-state-transient-cooldown-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let state = AppState::for_test(
            AppConfig::default(),
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();
        state.response_session_error_cooldowns.lock().await.insert(
            "responses:share:gpt-5.5".to_string(),
            ResponseSessionCooldownState {
                until: Instant::now() + StdDuration::from_secs(3600),
                failures: 2,
                unsupported: false,
            },
        );

        state.persist_runtime_state().await.unwrap();
        let loaded = load_runtime_state(&state.runtime_state_path).unwrap();

        assert!(
            loaded.response_session_error_cooldowns.is_empty(),
            "transient session-delta failures must not survive restart and block exact session retry"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_state_does_not_refresh_expired_prefix_age() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-runtime-state-expired-prefix-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let state = AppState::for_test(
            AppConfig::default(),
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();
        let expired_at = Instant::now()
            .checked_sub(PREFIX_RUNTIME_STATE_TTL + StdDuration::from_secs(1))
            .unwrap_or_else(Instant::now);
        state.prefix_states.lock().await.insert(
            "expired-prefix".to_string(),
            PrefixWarmState {
                finished_at: expired_at,
                input_tokens: 170_000,
                cache_read_tokens: 166_912,
                shortfall_tokens: 3_072,
                seen_bucket_tokens: 166_912,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: 3_072,
                seen_bucket_tokens_128: 166_912,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                cache_instability_score: 0,
                tail_tool_output_chars: 0,
                tail_largest_tool_output_chars: 0,
                tail_tool_output_noise_hint: None,
            },
        );

        state.persist_runtime_state().await.unwrap();
        let loaded = load_runtime_state(&state.runtime_state_path).unwrap();

        assert!(!loaded.prefix_states.contains_key("expired-prefix"));

        fs::remove_dir_all(dir).ok();
    }
}
