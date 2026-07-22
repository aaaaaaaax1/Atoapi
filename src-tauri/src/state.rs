use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration as StdDuration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex, RwLock},
};

use crate::{
    agent_injection,
    cache::{cache_path, CacheStore},
    config::{
        app_config_dir, config_path, isolated_test_listen_port, provider_model_cache_key,
        AppConfig, ProviderConfig, PublicConfig,
    },
    continuation_lineage::ContinuationLineageIndex,
    metrics::MetricsStore,
    metrics_history::metrics_history_path,
    persistence::{WriteBehindCoordinator, WriteOperation},
    proxy::{
        self,
        cache_affinity::ShadowAffinityStore,
        cache_validation::CacheValidationController,
        final_scope_waterline::{FinalScopeObservationRegistry, FinalScopeWaterlineLedger},
        DispatchDrainOutcome, DispatchTracker, TransportClients,
    },
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
    task: tokio::task::JoinHandle<()>,
}

impl ProxyServer {
    async fn shutdown(mut self) {
        let _ = self.shutdown.send(());
        if tokio::time::timeout(StdDuration::from_secs(2), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = self.task.await;
        }
    }
}

#[derive(Debug)]
pub struct AppState {
    pub config: RwLock<AppConfig>,
    pub config_path: PathBuf,
    #[cfg_attr(not(test), allow(dead_code))]
    pub runtime_state_path: PathBuf,
    pub cache: CacheStore,
    pub metrics: MetricsStore,
    pub transport_clients: TransportClients,
    pub prefix_locks: Mutex<PrefixLockRegistry>,
    pub prefix_states: Arc<Mutex<HashMap<String, PrefixWarmState>>>,
    prefix_state_maintenance_operations: AtomicU64,
    prefix_state_maintenance_running: Arc<AtomicBool>,
    /// Observe-only, final-wire-scoped cache evidence. It is deliberately
    /// process-memory-only and is always accessed with `try_lock` by the
    /// request path so it can never delay upstream dispatch.
    pub final_scope_waterlines: StdMutex<FinalScopeWaterlineLedger>,
    pub final_scope_observations: FinalScopeObservationRegistry,
    pub prefix_error_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    #[cfg(test)]
    pub prefix_prewarm_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub request_body_gzip_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub compact_endpoint_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub compact_chat_compat_cooldowns: Mutex<HashMap<String, std::time::Instant>>,
    pub reasoning_effort_rejections: Mutex<HashMap<String, std::time::Instant>>,
    pub response_session_error_cooldowns: Arc<Mutex<HashMap<String, ResponseSessionCooldownState>>>,
    pub continuation_lineage: ContinuationLineageIndex,
    pub provider_route_affinity: Mutex<HashMap<String, String>>,
    pub provider_key_affinity: Mutex<HashMap<String, String>>,
    pub shadow_affinity: Arc<Mutex<ShadowAffinityStore>>,
    pub cache_validation: Mutex<CacheValidationController>,
    pub relay_tasks: DispatchTracker,
    server: Mutex<Option<ProxyServer>>,
    proxy_mode_server: Mutex<Option<ProxyServer>>,
    config_persistence: ConfigWriteCoordinator,
    runtime_state_journal: RuntimeStateJournal,
}

/// Prefix coordination is a latency optimization, never a correctness or
/// dispatch requirement. Keep its registry bounded so a long stream of unique
/// prompts cannot turn the short map lock into an unbounded memory and lookup
/// cost. Active locks are never evicted; when every slot is active, callers
/// receive an ephemeral lock and proceed without cross-prefix coordination.
const PREFIX_LOCK_REGISTRY_LIMIT: usize = 2_048;
const PREFIX_LOCK_EVICTION_SCAN_BUDGET: usize = 32;

#[derive(Debug)]
pub struct PrefixLockRegistry {
    entries: HashMap<Arc<str>, PrefixLockEntry>,
    age_index: BTreeSet<(Instant, Arc<str>)>,
}

#[derive(Debug)]
struct PrefixLockEntry {
    lock: Arc<Mutex<()>>,
    last_used: Instant,
}

impl Default for PrefixLockRegistry {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            age_index: BTreeSet::new(),
        }
    }
}

impl PrefixLockRegistry {
    /// Returns the shared lock for a recent prefix. If every registry slot is
    /// currently in use, return an untracked lock instead of retaining more
    /// request-specific state. Losing coordination in that exceptional case is
    /// safe because callers may never require it to send upstream.
    pub fn acquire(&mut self, key: &str) -> Arc<Mutex<()>> {
        let now = Instant::now();
        if let Some((entry_key, previous_last_used)) = self
            .entries
            .get_key_value(key)
            .map(|(entry_key, entry)| (entry_key.clone(), entry.last_used))
        {
            self.age_index
                .remove(&(previous_last_used, entry_key.clone()));
            let lock = {
                let entry = self
                    .entries
                    .get_mut(entry_key.as_ref())
                    .expect("existing prefix-lock entry must remain present");
                entry.last_used = now;
                entry.lock.clone()
            };
            self.age_index.insert((now, entry_key));
            return lock;
        }

        self.evict_idle_entries_for_new_key();
        if self.entries.len() >= PREFIX_LOCK_REGISTRY_LIMIT {
            return Arc::new(Mutex::new(()));
        }

        let lock = Arc::new(Mutex::new(()));
        let entry_key: Arc<str> = Arc::from(key);
        self.entries.insert(
            entry_key.clone(),
            PrefixLockEntry {
                lock: lock.clone(),
                last_used: now,
            },
        );
        self.age_index.insert((now, entry_key));
        lock
    }

    fn evict_idle_entries_for_new_key(&mut self) {
        while self.entries.len() >= PREFIX_LOCK_REGISTRY_LIMIT {
            let candidate = self
                .age_index
                .iter()
                .take(PREFIX_LOCK_EVICTION_SCAN_BUDGET)
                .find_map(|(last_used, key)| {
                    self.entries
                        .get(key.as_ref())
                        .is_some_and(|entry| Arc::strong_count(&entry.lock) == 1)
                        .then(|| (*last_used, key.clone()))
                });
            let Some((last_used, key)) = candidate else {
                break;
            };
            self.age_index.remove(&(last_used, key.clone()));
            self.entries.remove(key.as_ref());
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }
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
    // Runtime-only evidence: persisted prefix snapshots must re-earn this
    // narrow recovery signal after restart.
    pub recent_clean_tiny_gap_streak: u32,
    pub cache_instability_score: u32,
    pub settle_after_cold_read: bool,
    pub tail_tool_output_chars: u64,
    pub tail_largest_tool_output_chars: u64,
    pub tail_tool_output_noise_hint: Option<String>,
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
    #[serde(default, skip_serializing, rename = "response_sessions")]
    _legacy_response_sessions: HashMap<String, PersistedResponseSessionState>,
    #[serde(default)]
    response_session_error_cooldowns: HashMap<String, PersistedResponseSessionCooldownState>,
    #[serde(default)]
    shadow_affinity: ShadowAffinityStore,
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

#[derive(Debug, Default)]
struct RuntimeStateMaps {
    prefix_states: HashMap<String, PrefixWarmState>,
    response_session_error_cooldowns: HashMap<String, ResponseSessionCooldownState>,
    shadow_affinity: ShadowAffinityStore,
}

fn capture_runtime_state(
    prefix_states: &Mutex<HashMap<String, PrefixWarmState>>,
    response_session_error_cooldowns: &Mutex<HashMap<String, ResponseSessionCooldownState>>,
    shadow_affinity: &Mutex<ShadowAffinityStore>,
) -> RuntimeStateMaps {
    RuntimeStateMaps {
        prefix_states: prefix_states.blocking_lock().clone(),
        response_session_error_cooldowns: response_session_error_cooldowns.blocking_lock().clone(),
        shadow_affinity: shadow_affinity.blocking_lock().clone(),
    }
}

#[derive(Debug, Clone)]
struct ConfigWriteCoordinator {
    state: Arc<std::sync::Mutex<ConfigWriteState>>,
    writer: WriteBehindCoordinator,
}

#[derive(Debug)]
struct ConfigWriteState {
    accepting: bool,
    latest: Option<Arc<AppConfig>>,
}

impl ConfigWriteCoordinator {
    fn new(path: PathBuf, metrics: MetricsStore) -> Self {
        Self::new_with_job(metrics, move |config| config.save(&path))
    }

    fn new_with_job(
        metrics: MetricsStore,
        write_job: impl Fn(Arc<AppConfig>) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        let state = Arc::new(std::sync::Mutex::new(ConfigWriteState {
            accepting: true,
            latest: None,
        }));
        let state_for_writer = state.clone();
        let writer = WriteBehindCoordinator::new("config_save", move |operation| {
            debug_assert_eq!(operation, WriteOperation::Snapshot);
            let config = state_for_writer
                .lock()
                .expect("config persistence snapshot lock must not be poisoned")
                .latest
                .clone()
                .context("config persistence snapshot was not published")?;
            write_job(config)
        });
        writer.attach_error_reporter(metrics);
        Self { state, writer }
    }

    fn publish(&self, config: &AppConfig) -> Result<u64> {
        let mut state = self
            .state
            .lock()
            .expect("config persistence snapshot lock must not be poisoned");
        if !state.accepting {
            return Err(anyhow::anyhow!("config persistence is closed"));
        }
        state.latest = Some(Arc::new(config.clone()));
        // Keep snapshot replacement and generation publication under the same
        // gate so flush can never observe one without the other.
        Ok(self.writer.mark_dirty(WriteOperation::Snapshot))
    }

    async fn wait_for(&self, version: u64) -> Result<()> {
        self.writer.wait_for(version).await
    }

    async fn persist(&self, config: &AppConfig) -> Result<()> {
        let version = self.publish(config)?;
        self.wait_for(version).await
    }

    async fn flush(&self) -> Result<()> {
        match self.writer.flush_latest().await {
            Ok(()) => Ok(()),
            Err(_) => self.writer.retry_latest().await,
        }
    }

    async fn close_and_flush(&self) -> Result<()> {
        self.state
            .lock()
            .expect("config persistence snapshot lock must not be poisoned")
            .accepting = false;
        self.flush().await
    }
}

#[derive(Debug, Clone)]
struct RuntimeStateJournal {
    state: Arc<StdMutex<RuntimeStateJournalState>>,
    writer: WriteBehindCoordinator,
}

#[derive(Debug)]
struct RuntimeStateJournalState {
    accepting: bool,
    close_version: Option<u64>,
}

const RUNTIME_STATE_WRITE_ATTEMPTS: usize = 3;
const RUNTIME_STATE_RETRY_DELAY_MS: u64 = 10;

impl RuntimeStateJournal {
    fn new(
        path: PathBuf,
        prefix_states: Arc<Mutex<HashMap<String, PrefixWarmState>>>,
        response_session_error_cooldowns: Arc<Mutex<HashMap<String, ResponseSessionCooldownState>>>,
        shadow_affinity: Arc<Mutex<ShadowAffinityStore>>,
        metrics: MetricsStore,
    ) -> Self {
        Self::new_with_job(metrics, move || {
            let snapshot = capture_runtime_state(
                &prefix_states,
                &response_session_error_cooldowns,
                &shadow_affinity,
            );
            save_runtime_state(
                &path,
                &snapshot.prefix_states,
                &snapshot.response_session_error_cooldowns,
                &snapshot.shadow_affinity,
            )
        })
    }

    fn new_with_job(
        metrics: MetricsStore,
        write_job: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        let writer = WriteBehindCoordinator::new("runtime_state_save", move |operation| {
            debug_assert_eq!(operation, WriteOperation::Snapshot);
            let mut result = write_job();
            for attempt in 1..RUNTIME_STATE_WRITE_ATTEMPTS {
                if result.is_ok() {
                    return Ok(());
                }
                std::thread::sleep(StdDuration::from_millis(
                    RUNTIME_STATE_RETRY_DELAY_MS * attempt as u64,
                ));
                result = write_job();
            }
            result.with_context(|| {
                format!(
                    "runtime-state persistence failed after {RUNTIME_STATE_WRITE_ATTEMPTS} attempts"
                )
            })
        });
        writer.attach_error_reporter(metrics);
        Self {
            state: Arc::new(StdMutex::new(RuntimeStateJournalState {
                accepting: true,
                close_version: None,
            })),
            writer,
        }
    }

    fn mark_dirty(&self) -> bool {
        let state = self
            .state
            .lock()
            .expect("runtime-state journal gate must not be poisoned");
        if !state.accepting {
            return false;
        }
        self.writer.mark_dirty(WriteOperation::Snapshot);
        true
    }

    #[cfg(test)]
    async fn flush(&self) -> Result<()> {
        let version = {
            let state = self
                .state
                .lock()
                .expect("runtime-state journal gate must not be poisoned");
            if !state.accepting {
                return Err(anyhow::anyhow!("runtime-state journal is closed"));
            }
            self.writer.mark_dirty(WriteOperation::Snapshot)
        };
        self.writer.wait_for(version).await
    }

    async fn close_and_flush(&self) -> Result<()> {
        let version = {
            let mut state = self
                .state
                .lock()
                .expect("runtime-state journal gate must not be poisoned");
            if let Some(version) = state.close_version {
                version
            } else {
                // Close the publication gate before publishing the final
                // internal snapshot. Any mark_dirty call after this point is
                // rejected under the same lock and cannot escape the barrier.
                state.accepting = false;
                let version = self.writer.mark_dirty(WriteOperation::Snapshot);
                state.close_version = Some(version);
                version
            }
        };
        self.writer.wait_for(version).await
    }
}

const RUNTIME_STATE_TTL: StdDuration = StdDuration::from_secs(30 * 60);
const PREFIX_RUNTIME_STATE_TTL: StdDuration = StdDuration::from_secs(20 * 60);
const PREFIX_RUNTIME_STATE_LIMIT: usize = 8_192;
const PREFIX_RUNTIME_STATE_MAINTENANCE_INTERVAL: u64 = 128;

impl AppState {
    pub fn load() -> Result<Self> {
        let config_path = config_path()?;
        let config_dir = app_config_dir()?;
        let mut config = AppConfig::load_or_create(&config_path)?;
        agent_injection::ensure_defaults(&mut config);
        config.save(&config_path)?;
        if let Some(port) = isolated_test_listen_port() {
            config.host = "127.0.0.1".to_string();
            config.port = port;
            config.proxy_mode_host = "127.0.0.1".to_string();
            config.proxy_mode_port = port.saturating_add(1);
        }
        let cache = CacheStore::load(cache_path(&config_dir))?;
        let metrics = MetricsStore::with_history_path(metrics_history_path(&config_dir));
        cache.attach_error_reporter(metrics.clone());
        let config_persistence = ConfigWriteCoordinator::new(config_path.clone(), metrics.clone());
        let runtime_state_path = runtime_state_path(&config_dir);
        let mut runtime_state = load_runtime_state(&runtime_state_path)?;
        migrate_prefix_states_for_config(&mut runtime_state.prefix_states, &config);
        trim_prefix_runtime_states(&mut runtime_state.prefix_states);
        crate::proxy::cache_affinity::prepare_shadow_affinity_store(
            &mut runtime_state.shadow_affinity,
        );
        let prefix_states = Arc::new(Mutex::new(runtime_state.prefix_states));
        let response_session_error_cooldowns =
            Arc::new(Mutex::new(runtime_state.response_session_error_cooldowns));
        let continuation_lineage = ContinuationLineageIndex::default();
        let shadow_affinity = Arc::new(Mutex::new(runtime_state.shadow_affinity));
        let runtime_state_journal = RuntimeStateJournal::new(
            runtime_state_path.clone(),
            prefix_states.clone(),
            response_session_error_cooldowns.clone(),
            shadow_affinity.clone(),
            metrics.clone(),
        );
        Ok(Self {
            config: RwLock::new(config),
            config_path: config_path.clone(),
            runtime_state_path,
            cache,
            metrics,
            transport_clients: TransportClients::new(crate::ATOAPI_USER_AGENT)?,
            prefix_locks: Mutex::new(PrefixLockRegistry::default()),
            prefix_states,
            prefix_state_maintenance_operations: AtomicU64::new(0),
            prefix_state_maintenance_running: Arc::new(AtomicBool::new(false)),
            final_scope_waterlines: StdMutex::new(FinalScopeWaterlineLedger::default()),
            final_scope_observations: FinalScopeObservationRegistry::default(),
            prefix_error_cooldowns: Mutex::new(HashMap::new()),
            #[cfg(test)]
            prefix_prewarm_cooldowns: Mutex::new(HashMap::new()),
            request_body_gzip_cooldowns: Mutex::new(HashMap::new()),
            compact_endpoint_cooldowns: Mutex::new(HashMap::new()),
            compact_chat_compat_cooldowns: Mutex::new(HashMap::new()),
            reasoning_effort_rejections: Mutex::new(HashMap::new()),
            response_session_error_cooldowns,
            continuation_lineage,
            provider_route_affinity: Mutex::new(HashMap::new()),
            provider_key_affinity: Mutex::new(HashMap::new()),
            shadow_affinity,
            cache_validation: Mutex::new(CacheValidationController::default()),
            relay_tasks: DispatchTracker::default(),
            server: Mutex::new(None),
            proxy_mode_server: Mutex::new(None),
            config_persistence,
            runtime_state_journal,
        })
    }

    #[cfg(test)]
    pub fn for_test(config: AppConfig, config_path: PathBuf, cache: CacheStore) -> Result<Self> {
        let runtime_state_path = config_path.with_file_name("runtime-state.json");
        let metrics = MetricsStore::new();
        cache.attach_error_reporter(metrics.clone());
        let config_persistence = ConfigWriteCoordinator::new(config_path.clone(), metrics.clone());
        let prefix_states = Arc::new(Mutex::new(HashMap::new()));
        let response_session_error_cooldowns = Arc::new(Mutex::new(HashMap::new()));
        let continuation_lineage = ContinuationLineageIndex::default();
        let shadow_affinity = Arc::new(Mutex::new(ShadowAffinityStore::default()));
        let runtime_state_journal = RuntimeStateJournal::new(
            runtime_state_path.clone(),
            prefix_states.clone(),
            response_session_error_cooldowns.clone(),
            shadow_affinity.clone(),
            metrics.clone(),
        );
        Ok(Self {
            config: RwLock::new(config),
            config_path: config_path.clone(),
            runtime_state_path,
            cache,
            metrics,
            transport_clients: TransportClients::new("AtoapiTest/0.1")?,
            prefix_locks: Mutex::new(PrefixLockRegistry::default()),
            prefix_states,
            prefix_state_maintenance_operations: AtomicU64::new(0),
            prefix_state_maintenance_running: Arc::new(AtomicBool::new(false)),
            final_scope_waterlines: StdMutex::new(FinalScopeWaterlineLedger::default()),
            final_scope_observations: FinalScopeObservationRegistry::default(),
            prefix_error_cooldowns: Mutex::new(HashMap::new()),
            #[cfg(test)]
            prefix_prewarm_cooldowns: Mutex::new(HashMap::new()),
            request_body_gzip_cooldowns: Mutex::new(HashMap::new()),
            compact_endpoint_cooldowns: Mutex::new(HashMap::new()),
            compact_chat_compat_cooldowns: Mutex::new(HashMap::new()),
            reasoning_effort_rejections: Mutex::new(HashMap::new()),
            response_session_error_cooldowns,
            continuation_lineage,
            provider_route_affinity: Mutex::new(HashMap::new()),
            provider_key_affinity: Mutex::new(HashMap::new()),
            shadow_affinity,
            cache_validation: Mutex::new(CacheValidationController::default()),
            relay_tasks: DispatchTracker::default(),
            server: Mutex::new(None),
            proxy_mode_server: Mutex::new(None),
            config_persistence,
            runtime_state_journal,
        })
    }

    pub async fn public_config(&self) -> PublicConfig {
        self.config
            .read()
            .await
            .public_view(self.config_path.clone())
    }

    pub fn upstream_client(&self, use_system_proxy: bool) -> &reqwest::Client {
        self.transport_clients.client(use_system_proxy)
    }

    pub async fn control_plane_upstream_client(
        &self,
        use_system_proxy: bool,
    ) -> Result<reqwest::Client> {
        let explicit_proxy_url = self
            .config
            .read()
            .await
            .upstream_proxy_url_for(use_system_proxy)
            .map(str::to_owned);
        match explicit_proxy_url {
            Some(proxy_url) => self
                .transport_clients
                .explicit_proxy_client(&proxy_url, false),
            None => Ok(self.transport_clients.client(use_system_proxy).clone()),
        }
    }

    pub async fn reload_config(&self) -> Result<PublicConfig> {
        let mut current = self.config.write().await;
        self.flush_config().await?;
        let mut config = AppConfig::load_or_create(&self.config_path)?;
        agent_injection::ensure_defaults(&mut config);
        self.persist_config_snapshot(&config).await?;
        let public = config.public_view(self.config_path.clone());
        *current = config;
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
        self.persist_config_snapshot(&config).await?;
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

        let task = tokio::spawn(async move {
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
            task,
        });
        self.set_proxy_auto_start(true).await?;
        Ok(ProxyStatus {
            running: true,
            address: Some(address),
        })
    }

    pub async fn start_proxy_mode_proxy(self: &Arc<Self>) -> Result<ProxyStatus> {
        let mut server_guard = self.proxy_mode_server.lock().await;
        if let Some(server) = server_guard.as_ref() {
            return Ok(ProxyStatus {
                running: true,
                address: Some(server.address.clone()),
            });
        }

        let config = self.config.read().await.clone();
        let bind = format!("{}:{}", config.proxy_mode_host, config.proxy_mode_port);
        let listener = TcpListener::bind(&bind)
            .await
            .with_context(|| format!("failed to bind proxy mode at {bind}"))?;
        let address = listener.local_addr()?.to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let router = proxy::router(self.clone());

        let task = tokio::spawn(async move {
            let result = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;

            if let Err(err) = result {
                eprintln!("proxy mode server stopped with error: {err}");
            }
        });

        *server_guard = Some(ProxyServer {
            address: address.clone(),
            shutdown: shutdown_tx,
            task,
        });
        Ok(ProxyStatus {
            running: true,
            address: Some(address),
        })
    }
    pub async fn stop_proxy(&self) -> Result<ProxyStatus> {
        let server = self.server.lock().await.take();
        if let Some(server) = server {
            server.shutdown().await;
        }
        self.set_proxy_auto_start(false).await?;
        Ok(ProxyStatus {
            running: false,
            address: None,
        })
    }

    pub async fn stop_proxy_mode_proxy(&self) -> Result<ProxyStatus> {
        let server = self.proxy_mode_server.lock().await.take();
        if let Some(server) = server {
            server.shutdown().await;
        }
        Ok(ProxyStatus {
            running: false,
            address: None,
        })
    }

    pub async fn shutdown_for_exit(&self) -> Result<()> {
        self.shutdown_for_exit_with_relay_timeout(StdDuration::from_secs(2))
            .await
    }

    async fn shutdown_for_exit_with_relay_timeout(&self, relay_timeout: StdDuration) -> Result<()> {
        let main_server = self.server.lock().await.take();
        let proxy_mode_server = self.proxy_mode_server.lock().await.take();
        tokio::join!(
            async move {
                if let Some(server) = main_server {
                    server.shutdown().await;
                }
            },
            async move {
                if let Some(server) = proxy_mode_server {
                    server.shutdown().await;
                }
            }
        );
        if let DispatchDrainOutcome::Aborted { task_count } =
            self.relay_tasks.close_and_drain(relay_timeout).await
        {
            self.metrics
                .record_error(
                    "shutdown_relay_drain_timeout",
                    &format!("aborted {task_count} active relay owner(s) after shutdown timeout"),
                )
                .await;
        }
        self.metrics.close_and_wait_for_commits().await;
        // Relay draining fences request-owned runtime mutations. Cache/config
        // also close their own publication gates so concurrent admin commands
        // cannot publish after the final flush target is captured.
        let (runtime_state, cache, config, metrics_history) = tokio::join!(
            self.runtime_state_journal.close_and_flush(),
            self.cache.close_and_flush(),
            self.config_persistence.close_and_flush(),
            self.metrics.flush_history()
        );
        runtime_state?;
        cache?;
        config?;
        metrics_history
    }

    pub async fn proxy_status(&self) -> ProxyStatus {
        let server_guard = self.server.lock().await;
        ProxyStatus {
            running: server_guard.is_some(),
            address: server_guard.as_ref().map(|server| server.address.clone()),
        }
    }

    pub async fn proxy_mode_status(&self) -> ProxyStatus {
        let server_guard = self.proxy_mode_server.lock().await;
        ProxyStatus {
            running: server_guard.is_some(),
            address: server_guard.as_ref().map(|server| server.address.clone()),
        }
    }
    async fn set_proxy_auto_start(&self, enabled: bool) -> Result<()> {
        let version = {
            let mut config = self.config.write().await;
            if config.proxy_auto_start == enabled {
                return Ok(());
            }
            config.proxy_auto_start = enabled;
            config.updated_at = Utc::now();
            self.publish_config_snapshot(&config)?
        };
        self.wait_for_config_snapshot(version).await?;
        Ok(())
    }

    pub fn journal_config(&self, config: &AppConfig) {
        let _ = self.publish_config_snapshot(config);
    }

    /// Publish while the caller still owns the config mutation guard so
    /// persistence generations preserve the same order as in-memory changes.
    pub fn publish_config_snapshot(&self, config: &AppConfig) -> Result<u64> {
        self.config_persistence.publish(config)
    }

    pub async fn wait_for_config_snapshot(&self, version: u64) -> Result<()> {
        self.config_persistence.wait_for(version).await
    }

    pub async fn persist_config_snapshot(&self, config: &AppConfig) -> Result<()> {
        self.config_persistence.persist(config).await
    }

    pub(crate) async fn flush_config(&self) -> Result<()> {
        self.config_persistence.flush().await
    }

    #[cfg(test)]
    pub async fn persist_runtime_state(&self) -> Result<()> {
        self.runtime_state_journal.flush().await
    }

    pub fn journal_runtime_state(&self) {
        self.runtime_state_journal.mark_dirty();
    }

    /// Prefix state is a cache-quality hint, not request truth. Maintenance is
    /// therefore detached from settlement and bounded; it must never hold up
    /// an ingress send or grow indefinitely under a stream of novel prompts.
    pub fn schedule_prefix_state_maintenance(&self) {
        let operation = self
            .prefix_state_maintenance_operations
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if operation % PREFIX_RUNTIME_STATE_MAINTENANCE_INTERVAL != 0
            || self
                .prefix_state_maintenance_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }

        let prefix_states = self.prefix_states.clone();
        let running = self.prefix_state_maintenance_running.clone();
        tokio::spawn(async move {
            {
                let mut states = prefix_states.lock().await;
                trim_prefix_runtime_states(&mut states);
            }
            running.store(false, Ordering::Release);
        });
    }
}

pub fn runtime_state_path(config_dir: &Path) -> PathBuf {
    config_dir.join("runtime-state.json")
}

fn load_runtime_state(path: &Path) -> Result<RuntimeStateMaps> {
    if path.exists() {
        match read_runtime_state_file(path) {
            Ok((runtime, _)) => return Ok(runtime),
            Err(err) => {
                eprintln!(
                    "runtime-state recovery: ignoring damaged {}: {err:#}",
                    path.display()
                );
                quarantine_runtime_state_file(path);
            }
        }
    }

    let backup_path = runtime_state_backup_path(path);
    if !backup_path.exists() {
        return Ok(RuntimeStateMaps::default());
    }
    match read_runtime_state_file(&backup_path) {
        Ok((runtime, raw)) => {
            if let Err(err) = write_synced_replacement(path, &raw) {
                eprintln!(
                    "runtime-state recovery: loaded {}, but could not restore {}: {err:#}",
                    backup_path.display(),
                    path.display()
                );
            }
            Ok(runtime)
        }
        Err(err) => {
            eprintln!(
                "runtime-state recovery: ignoring damaged backup {}: {err:#}",
                backup_path.display()
            );
            quarantine_runtime_state_file(&backup_path);
            Ok(RuntimeStateMaps::default())
        }
    }
}

fn read_runtime_state_file(path: &Path) -> Result<(RuntimeStateMaps, Vec<u8>)> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let persisted: PersistedRuntimeState = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok((persisted.into_runtime(), raw))
}

fn runtime_state_backup_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".bak");
    path.with_file_name(name)
}

fn runtime_state_temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(
        ".tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    path.with_file_name(name)
}

fn quarantine_runtime_state_file(path: &Path) {
    if !path.exists() {
        return;
    }
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".corrupt-{}", uuid::Uuid::new_v4().simple()));
    let quarantine_path = path.with_file_name(name);
    if let Err(err) = fs::rename(path, &quarantine_path) {
        eprintln!(
            "runtime-state recovery: could not quarantine {} as {}: {err}",
            path.display(),
            quarantine_path.display()
        );
    }
}

fn migrate_prefix_states_for_config(
    prefix_states: &mut HashMap<String, PrefixWarmState>,
    config: &AppConfig,
) {
    let existing = prefix_states.clone();
    for (key, state) in existing {
        let parts = key.split('\0').collect::<Vec<_>>();
        if parts.len() == 4 && parts[0] != "prefix-alias" {
            let Some((provider_group, model_key)) =
                prefix_state_migration_target(config, parts[0], parts[1])
            else {
                continue;
            };
            let migrated_key = format!(
                "{}\0{}\0{}\0{}",
                provider_group, model_key, parts[2], parts[3]
            );
            insert_stronger_prefix_state(prefix_states, migrated_key.clone(), state.clone());
            insert_stronger_prefix_state(
                prefix_states,
                format!(
                    "prefix-alias\0{}\0{}\0{}\0{}",
                    provider_group, model_key, parts[2], parts[3]
                ),
                state,
            );
        } else if parts.len() == 5 && parts[0] == "prefix-alias" {
            let Some((provider_group, model_key)) =
                prefix_state_migration_target(config, parts[1], parts[2])
            else {
                continue;
            };
            let migrated_key = format!(
                "prefix-alias\0{}\0{}\0{}\0{}",
                provider_group, model_key, parts[3], parts[4]
            );
            insert_stronger_prefix_state(prefix_states, migrated_key, state);
        } else if parts.len() == 5 && parts[0] == "prefix-family" {
            let Some((provider_group, model_key)) =
                prefix_state_migration_target(config, parts[1], parts[2])
            else {
                continue;
            };
            let migrated_key = format!(
                "prefix-family\0{}\0{}\0{}\0{}",
                provider_group, model_key, parts[3], parts[4]
            );
            insert_stronger_prefix_state(prefix_states, migrated_key, state);
        }
    }
}

fn prefix_state_migration_target(
    config: &AppConfig,
    provider_scope: &str,
    model: &str,
) -> Option<(String, String)> {
    let providers = config
        .providers
        .iter()
        .filter(|provider| provider.id == provider_scope)
        .collect::<Vec<_>>();
    let providers = if providers.is_empty() {
        config
            .providers
            .iter()
            .filter(|provider| prefix_provider_group(provider) == provider_scope)
            .collect::<Vec<_>>()
    } else {
        providers
    };

    let mut targets = providers
        .into_iter()
        .map(|provider| {
            (
                prefix_provider_group(provider),
                provider_model_cache_key(provider, model),
            )
        })
        .collect::<Vec<_>>();
    targets.sort();
    targets.dedup();
    (targets.len() == 1).then(|| targets.remove(0))
}

fn prefix_provider_group(provider: &ProviderConfig) -> String {
    let base_url = provider
        .base_url
        .trim()
        .trim_end_matches('/')
        .to_ascii_lowercase();
    if base_url.is_empty() {
        provider.id.clone()
    } else {
        base_url
    }
}

fn insert_stronger_prefix_state(
    prefix_states: &mut HashMap<String, PrefixWarmState>,
    key: String,
    state: PrefixWarmState,
) {
    let should_replace = prefix_states
        .get(&key)
        .map(|current| prefix_state_strength(&state) > prefix_state_strength(current))
        .unwrap_or(true);
    if should_replace {
        prefix_states.insert(key, state);
    }
}

fn prefix_state_strength(state: &PrefixWarmState) -> u64 {
    state
        .seen_bucket_tokens_128
        .max(state.seen_bucket_tokens)
        .max(state.cache_read_tokens)
}

fn trim_prefix_runtime_states(prefix_states: &mut HashMap<String, PrefixWarmState>) {
    prefix_states.retain(|_, state| state.finished_at.elapsed() <= PREFIX_RUNTIME_STATE_TTL);
    if prefix_states.len() <= PREFIX_RUNTIME_STATE_LIMIT {
        return;
    }

    let mut oldest = prefix_states
        .iter()
        .map(|(key, state)| (state.finished_at, key.clone()))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(finished_at, _)| *finished_at);
    let overflow = prefix_states
        .len()
        .saturating_sub(PREFIX_RUNTIME_STATE_LIMIT);
    for (_, key) in oldest.into_iter().take(overflow) {
        prefix_states.remove(&key);
    }
}

fn save_runtime_state(
    path: &Path,
    prefix_states: &HashMap<String, PrefixWarmState>,
    response_session_error_cooldowns: &HashMap<String, ResponseSessionCooldownState>,
    shadow_affinity: &ShadowAffinityStore,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let persisted = PersistedRuntimeState::from_runtime(
        prefix_states,
        response_session_error_cooldowns,
        shadow_affinity,
    );
    let raw = serde_json::to_string_pretty(&persisted)?;
    let backup_path = runtime_state_backup_path(path);
    if let Ok(previous) = fs::read(path) {
        if serde_json::from_slice::<PersistedRuntimeState>(&previous).is_ok() {
            write_synced_replacement(&backup_path, &previous).with_context(|| {
                format!(
                    "failed to preserve the last good runtime state at {}",
                    backup_path.display()
                )
            })?;
        }
    }
    write_synced_replacement(path, raw.as_bytes())
        .with_context(|| format!("failed to replace {}", path.display()))
}

fn write_synced_replacement(path: &Path, raw: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp_path = runtime_state_temp_path(path);
    let result = (|| -> Result<()> {
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;
        temp.write_all(raw)
            .with_context(|| format!("failed to write {}", temp_path.display()))?;
        temp.sync_all()
            .with_context(|| format!("failed to sync {}", temp_path.display()))?;
        drop(temp);
        // The temporary file is in the destination directory, so rename is a
        // same-volume atomic replacement. Rust's Windows implementation uses
        // replacement semantics when the destination already exists; on a
        // sharing violation we fail without deleting the last good file and
        // let the bounded journal retry handle the transient error.
        fs::rename(&temp_path, path).with_context(|| {
            format!(
                "failed to atomically replace {} with {}",
                path.display(),
                temp_path.display()
            )
        })?;
        sync_runtime_state_parent(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

#[cfg(unix)]
fn sync_runtime_state_parent(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync {}", parent.display()))
}

#[cfg(not(unix))]
fn sync_runtime_state_parent(_parent: &Path) -> Result<()> {
    Ok(())
}

impl PersistedRuntimeState {
    fn from_runtime(
        prefix_states: &HashMap<String, PrefixWarmState>,
        response_session_error_cooldowns: &HashMap<String, ResponseSessionCooldownState>,
        shadow_affinity: &ShadowAffinityStore,
    ) -> Self {
        let now = Utc::now();
        let mut prefix_states = prefix_states.clone();
        trim_prefix_runtime_states(&mut prefix_states);
        let prefix_states = prefix_states
            .iter()
            .map(|(key, state)| {
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
            _legacy_response_sessions: HashMap::new(),
            response_session_error_cooldowns,
            shadow_affinity: shadow_affinity.clone(),
        }
    }

    fn into_runtime(self) -> RuntimeStateMaps {
        let now = Utc::now();
        let instant_now = Instant::now();
        let mut prefix_states = self
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
                        recent_clean_tiny_gap_streak: 0,
                        cache_instability_score: state.cache_instability_score,
                        settle_after_cold_read: false,
                        tail_tool_output_chars: state.tail_tool_output_chars,
                        tail_largest_tool_output_chars: state.tail_largest_tool_output_chars,
                        tail_tool_output_noise_hint: state.tail_tool_output_noise_hint,
                    },
                ))
            })
            .collect();
        trim_prefix_runtime_states(&mut prefix_states);
        // Legacy plaintext response-session snapshots are deliberately not
        // admitted into the active continuation map.  A restart therefore
        // falls back to the Agent's complete request instead of trusting a
        // stale or cross-key response reference.
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
                let until_delta = (state.until_at - now).to_std().ok().map(|duration| {
                    if state.unsupported {
                        duration.min(StdDuration::from_secs(5 * 60))
                    } else {
                        duration
                    }
                });
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
        let mut shadow_affinity = self.shadow_affinity;
        crate::proxy::cache_affinity::evict_assignments(&mut shadow_affinity, now);
        RuntimeStateMaps {
            prefix_states,
            response_session_error_cooldowns,
            shadow_affinity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AgentInjectionKind, CacheConfig, Channel, ModelConfig, ProviderConfig};
    use crate::proxy::cache_affinity::{
        PostBurstEvidence, PostBurstWindow, ShadowAffinityArm, ShadowAffinityAssignment,
        ShadowCacheCandidateVariant, ShadowCacheLane, SHADOW_POLICY_EPOCH,
    };
    use serde_json::json;
    use std::{
        fs,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        sync::Mutex as StdMutex,
    };
    use uuid::Uuid;

    fn prefix_state_at(finished_at: Instant) -> PrefixWarmState {
        PrefixWarmState {
            finished_at,
            input_tokens: 131_072,
            cache_read_tokens: 130_560,
            shortfall_tokens: 512,
            seen_bucket_tokens: 130_560,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 512,
            seen_bucket_tokens_128: 130_560,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 0,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        }
    }

    #[tokio::test]
    async fn exit_shutdown_releases_both_ports_without_disabling_auto_start() {
        let dir =
            std::env::temp_dir().join(format!("atoapi-exit-shutdown-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let mut config = AppConfig::default();
        config.host = "127.0.0.1".to_string();
        config.port = 0;
        config.proxy_mode_host = "127.0.0.1".to_string();
        config.proxy_mode_port = 0;
        config.proxy_auto_start = true;
        config.cache.enabled = true;
        config.cache.persist_encrypted = true;
        let state = Arc::new(
            AppState::for_test(
                config,
                dir.join("config.toml"),
                CacheStore::load(dir.join("cache.bin")).unwrap(),
            )
            .unwrap(),
        );

        let main = state.start_proxy().await.unwrap();
        let proxy_mode = state.start_proxy_mode_proxy().await.unwrap();
        let main_address: SocketAddr = main.address.unwrap().parse().unwrap();
        let proxy_mode_address: SocketAddr = proxy_mode.address.unwrap().parse().unwrap();
        assert!(tokio::net::TcpStream::connect(main_address).await.is_ok());
        assert!(tokio::net::TcpStream::connect(proxy_mode_address)
            .await
            .is_ok());

        let cache_config = state.config.read().await.cache.clone();
        state
            .cache
            .insert_many(
                vec![crate::cache::CacheEntry {
                    key: "shutdown-cache-entry".to_string(),
                    semantic_text: None,
                    semantic_shape: None,
                    semantic_vector: Vec::new(),
                    content_type: "application/json".to_string(),
                    status: 200,
                    body: br#"{"ok":true}"#.to_vec(),
                    created_at: Utc::now(),
                    expires_at: Utc::now() + chrono::Duration::minutes(5),
                    provider_id: "provider".to_string(),
                    model: "model".to_string(),
                    workspace_fingerprint: Some("workspace".to_string()),
                }],
                &cache_config,
            )
            .await
            .unwrap();
        state.journal_runtime_state();
        let relay_settled = Arc::new(AtomicBool::new(false));
        let relay_settled_for_task = relay_settled.clone();
        let state_for_relay = state.clone();
        state
            .relay_tasks
            .spawn(async move {
                tokio::time::sleep(StdDuration::from_millis(25)).await;
                relay_settled_for_task.store(true, Ordering::Release);
                state_for_relay.journal_runtime_state();
            })
            .unwrap();

        state.shutdown_for_exit().await.unwrap();

        assert!(!state.proxy_status().await.running);
        assert!(!state.proxy_mode_status().await.running);
        assert!(state.config.read().await.proxy_auto_start);
        assert!(relay_settled.load(Ordering::Acquire));
        assert!(state.runtime_state_path.exists());
        assert!(dir.join("cache.bin").exists());
        let main_rebind = tokio::net::TcpListener::bind(main_address).await.unwrap();
        let proxy_mode_rebind = tokio::net::TcpListener::bind(proxy_mode_address)
            .await
            .unwrap();
        drop(main_rebind);
        drop(proxy_mode_rebind);
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn exit_timeout_aborts_relay_before_final_persistence_barrier() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-exit-abort-fence-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.persist_encrypted = true;
        let cache_path = dir.join("cache.bin");
        let state = Arc::new(
            AppState::for_test(
                config,
                dir.join("config.toml"),
                CacheStore::load(cache_path.clone()).unwrap(),
            )
            .unwrap(),
        );
        let late_mutation = Arc::new(AtomicBool::new(false));
        let late_mutation_for_relay = late_mutation.clone();
        let state_for_relay = state.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        state
            .relay_tasks
            .spawn(async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                let cache_config = state_for_relay.config.read().await.cache.clone();
                state_for_relay
                    .cache
                    .insert_many(
                        vec![crate::cache::CacheEntry {
                            key: "late-cache-entry".to_string(),
                            semantic_text: None,
                            semantic_shape: None,
                            semantic_vector: Vec::new(),
                            content_type: "application/json".to_string(),
                            status: 200,
                            body: br#"{"late":true}"#.to_vec(),
                            created_at: Utc::now(),
                            expires_at: Utc::now() + chrono::Duration::minutes(5),
                            provider_id: "provider".to_string(),
                            model: "model".to_string(),
                            workspace_fingerprint: Some("workspace".to_string()),
                        }],
                        &cache_config,
                    )
                    .await
                    .unwrap();
                state_for_relay.journal_runtime_state();
                late_mutation_for_relay.store(true, Ordering::Release);
            })
            .unwrap();
        started_rx.await.unwrap();

        state
            .shutdown_for_exit_with_relay_timeout(StdDuration::from_millis(10))
            .await
            .unwrap();

        assert!(!late_mutation.load(Ordering::Acquire));
        assert!(release_tx.send(()).is_err());
        assert!(state.relay_tasks.spawn(async {}).is_err());
        let cache_config = state.config.read().await.cache.clone();
        assert!(state
            .cache
            .lookup_exact("late-cache-entry", &cache_config)
            .await
            .is_none());
        assert!(!cache_path.exists());
        assert!(state.runtime_state_path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_snapshot_does_not_touch_the_memory_only_lineage_index() {
        let prefix_states = Arc::new(Mutex::new(HashMap::new()));
        let response_session_error_cooldowns = Arc::new(Mutex::new(HashMap::new()));
        let shadow_affinity = Arc::new(Mutex::new(ShadowAffinityStore::default()));

        let prefix_states_for_capture = prefix_states.clone();
        let cooldowns_for_capture = response_session_error_cooldowns.clone();
        let shadow_affinity_for_capture = shadow_affinity.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            capture_runtime_state(
                &prefix_states_for_capture,
                &cooldowns_for_capture,
                &shadow_affinity_for_capture,
            )
        })
        .await
        .unwrap();

        assert!(snapshot.prefix_states.is_empty());
    }

    #[test]
    #[ignore = "manual FastRelayCore full-capacity runtime snapshot baseline"]
    fn fastrelay_full_capacity_runtime_snapshot_baseline() {
        use std::hint::black_box;

        const FULL_SHADOW_ASSIGNMENTS: usize = 4_096;
        const FULL_POST_BURST_EVIDENCE: usize = 1_536;
        let prefix_states = Mutex::new(HashMap::new());
        let response_session_error_cooldowns = Mutex::new(HashMap::new());
        let shadow_affinity = Mutex::new(ShadowAffinityStore::default());
        let now = Utc::now();

        {
            let mut states = prefix_states.blocking_lock();
            for index in 0..PREFIX_RUNTIME_STATE_LIMIT {
                states.insert(
                    format!("snapshot-prefix-{index}"),
                    prefix_state_at(Instant::now()),
                );
            }
        }
        {
            let mut affinity = shadow_affinity.blocking_lock();
            for index in 0..FULL_SHADOW_ASSIGNMENTS {
                let conversation_id = format!("snapshot-conversation-{index}");
                affinity.assignments.insert(
                    conversation_id.clone(),
                    ShadowAffinityAssignment {
                        conversation_id,
                        cohort_id: format!("snapshot-cohort-{index}"),
                        realm_id: "snapshot-realm".to_string(),
                        policy_epoch: SHADOW_POLICY_EPOCH,
                        lane: ShadowCacheLane::Steady,
                        arm: ShadowAffinityArm::Baseline,
                        shard: 0,
                        anchor_epoch: 0,
                        created_at: now,
                        last_seen_at: now,
                        observations: 1,
                        successful_observations: 1,
                        usage_observations: 1,
                        inconclusive_observations: 0,
                        input_tokens: 131_072,
                        cache_read_tokens: 130_560,
                        active_cache_route_state: Default::default(),
                        active_cache_route_baseline: Default::default(),
                        active_cache_route_candidate: Default::default(),
                        active_cache_route_reason: None,
                        active_cache_route_legacy_seed_consumed: false,
                        active_cache_route_valid_until: None,
                    },
                );
            }
            for index in 0..FULL_POST_BURST_EVIDENCE {
                affinity.post_burst.evidence.push_back(PostBurstEvidence {
                    window_id: index as u64,
                    conversation_id: format!("snapshot-evidence-conversation-{index}"),
                    observed_at: now,
                    followup_index: 1,
                    realm_id: "snapshot-realm".to_string(),
                    lane: ShadowCacheLane::Steady,
                    candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                    arm: ShadowAffinityArm::Baseline,
                    policy_epoch: SHADOW_POLICY_EPOCH,
                    anchor_epoch: 0,
                    success: true,
                    status: 200,
                    has_usage: true,
                    input_tokens: 131_072,
                    cache_read_tokens: 130_560,
                    cache_ratio_bps: 9_960,
                    avoidable_gap_tokens: 0,
                    provider_unstable_gap_tokens: 0,
                    ttft_ms: 100,
                    attempt_count: 1,
                    candidate_applied: false,
                });
            }
        }

        let mut samples = Vec::new();
        for _ in 0..21 {
            let started = Instant::now();
            let snapshot = capture_runtime_state(
                &prefix_states,
                &response_session_error_cooldowns,
                &shadow_affinity,
            );
            black_box(snapshot);
            samples.push(started.elapsed().as_micros());
        }
        samples.sort_unstable();
        let p95_index = ((samples.len() - 1) * 95).div_ceil(100);
        let p95_us = samples[p95_index];
        println!(
            "fastrelay_runtime_snapshot prefixes={PREFIX_RUNTIME_STATE_LIMIT} assignments={FULL_SHADOW_ASSIGNMENTS} evidence={FULL_POST_BURST_EVIDENCE} p95_us={p95_us} samples_us={samples:?}",
        );
        assert!(
            p95_us <= 10_000,
            "full-capacity runtime snapshot p95 ({p95_us}us) exceeded the 10ms background-writer budget"
        );
    }

    #[tokio::test]
    async fn runtime_state_close_fences_late_dirty_publications() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let attempts_for_job = attempts.clone();
        let started_for_job = started.clone();
        let release_for_job = release.clone();
        let journal = RuntimeStateJournal::new_with_job(MetricsStore::new(), move || {
            attempts_for_job.fetch_add(1, Ordering::SeqCst);
            started_for_job.store(true, Ordering::Release);
            while !release_for_job.load(Ordering::Acquire) {
                std::thread::sleep(StdDuration::from_millis(1));
            }
            Ok(())
        });

        let closing_journal = journal.clone();
        let closing = tokio::spawn(async move { closing_journal.close_and_flush().await });
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        assert!(!journal.mark_dirty());
        assert!(journal.flush().await.is_err());
        release.store(true, Ordering::Release);
        closing.await.unwrap().unwrap();
        journal.writer.flush_latest().await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn runtime_state_journal_retries_transient_failure_and_stops_at_bound() {
        let transient_attempts = Arc::new(AtomicUsize::new(0));
        let transient_attempts_for_job = transient_attempts.clone();
        let transient = RuntimeStateJournal::new_with_job(MetricsStore::new(), move || {
            let attempt = transient_attempts_for_job.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(anyhow::anyhow!("transient write failure"))
            } else {
                Ok(())
            }
        });
        assert!(transient.mark_dirty());
        transient.writer.flush_latest().await.unwrap();
        assert_eq!(transient_attempts.load(Ordering::SeqCst), 3);

        let permanent_attempts = Arc::new(AtomicUsize::new(0));
        let permanent_attempts_for_job = permanent_attempts.clone();
        let permanent = RuntimeStateJournal::new_with_job(MetricsStore::new(), move || {
            permanent_attempts_for_job.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("permanent write failure"))
        });
        let error = permanent.flush().await.unwrap_err().to_string();
        assert!(error.contains("failed after 3 attempts"));
        assert_eq!(permanent_attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn runtime_state_atomic_replace_keeps_previous_good_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-runtime-state-atomic-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime-state.json");
        let key = "responses:share:gpt-5.5".to_string();
        let mut cooldowns = HashMap::from([(
            key.clone(),
            ResponseSessionCooldownState {
                until: Instant::now() + StdDuration::from_secs(300),
                failures: 1,
                unsupported: true,
            },
        )]);

        save_runtime_state(
            &path,
            &HashMap::new(),
            &cooldowns,
            &ShadowAffinityStore::default(),
        )
        .unwrap();
        cooldowns.get_mut(&key).unwrap().failures = 2;
        save_runtime_state(
            &path,
            &HashMap::new(),
            &cooldowns,
            &ShadowAffinityStore::default(),
        )
        .unwrap();
        cooldowns.get_mut(&key).unwrap().failures = 3;
        save_runtime_state(
            &path,
            &HashMap::new(),
            &cooldowns,
            &ShadowAffinityStore::default(),
        )
        .unwrap();

        let (current, _) = read_runtime_state_file(&path).unwrap();
        let (previous, _) = read_runtime_state_file(&runtime_state_backup_path(&path)).unwrap();
        assert_eq!(
            current
                .response_session_error_cooldowns
                .get(&key)
                .unwrap()
                .failures,
            3
        );
        assert_eq!(
            previous
                .response_session_error_cooldowns
                .get(&key)
                .unwrap()
                .failures,
            2
        );
        assert!(!fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp-")
        }));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn runtime_state_recovers_valid_backup_and_quarantines_damaged_primary() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-runtime-state-recovery-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime-state.json");
        let key = "responses:share:gpt-5.5".to_string();
        let cooldowns = HashMap::from([(
            key.clone(),
            ResponseSessionCooldownState {
                until: Instant::now() + StdDuration::from_secs(300),
                failures: 7,
                unsupported: true,
            },
        )]);
        save_runtime_state(
            &path,
            &HashMap::new(),
            &cooldowns,
            &ShadowAffinityStore::default(),
        )
        .unwrap();
        fs::copy(&path, runtime_state_backup_path(&path)).unwrap();
        fs::write(&path, b"{damaged-json").unwrap();

        let recovered = load_runtime_state(&path).unwrap();

        assert_eq!(
            recovered
                .response_session_error_cooldowns
                .get(&key)
                .unwrap()
                .failures,
            7
        );
        assert!(read_runtime_state_file(&path).is_ok());
        assert!(fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("runtime-state.json.corrupt-")
        }));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn damaged_runtime_state_without_backup_starts_with_empty_state() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-runtime-state-empty-recovery-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime-state.json");
        fs::write(&path, b"not-json").unwrap();

        let recovered = load_runtime_state(&path).unwrap();

        assert!(recovered.prefix_states.is_empty());
        assert!(recovered.response_session_error_cooldowns.is_empty());
        assert!(!path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn config_journal_coalesces_latest_snapshot_without_blocking_publisher() {
        let saved_ports = Arc::new(StdMutex::new(Vec::new()));
        let saved_ports_for_job = saved_ports.clone();
        let coordinator =
            ConfigWriteCoordinator::new_with_job(MetricsStore::new(), move |config| {
                std::thread::sleep(StdDuration::from_millis(100));
                saved_ports_for_job.lock().unwrap().push(config.port);
                Ok(())
            });
        let mut config = AppConfig::default();
        let started = Instant::now();
        config.port = 18_881;
        coordinator.publish(&config).unwrap();
        config.port = 18_882;
        coordinator.publish(&config).unwrap();
        assert!(started.elapsed() < StdDuration::from_millis(20));

        coordinator.flush().await.unwrap();
        assert_eq!(*saved_ports.lock().unwrap(), vec![18_882]);
    }

    #[tokio::test]
    async fn published_config_snapshot_releases_app_config_lock_before_slow_writer_finishes() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-config-publish-unlocks-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let writer_started = Arc::new(AtomicBool::new(false));
        let writer_started_for_job = writer_started.clone();
        let release_writer = Arc::new(AtomicBool::new(false));
        let release_writer_for_job = release_writer.clone();
        let writer_finished = Arc::new(AtomicBool::new(false));
        let writer_finished_for_job = writer_finished.clone();
        let saved_ports = Arc::new(StdMutex::new(Vec::new()));
        let saved_ports_for_job = saved_ports.clone();

        let mut state = AppState::for_test(
            AppConfig::default(),
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();
        state.config_persistence =
            ConfigWriteCoordinator::new_with_job(state.metrics.clone(), move |config| {
                writer_started_for_job.store(true, Ordering::Release);
                let deadline = Instant::now() + StdDuration::from_secs(2);
                while !release_writer_for_job.load(Ordering::Acquire) && Instant::now() < deadline {
                    std::thread::sleep(StdDuration::from_millis(1));
                }
                saved_ports_for_job.lock().unwrap().push(config.port);
                writer_finished_for_job.store(true, Ordering::Release);
                Ok(())
            });
        let state = Arc::new(state);

        let version = {
            let mut config = state.config.write().await;
            config.port = 18_884;
            state.publish_config_snapshot(&config).unwrap()
        };
        tokio::time::timeout(StdDuration::from_secs(1), async {
            while !writer_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow config writer should start");
        assert!(!writer_finished.load(Ordering::Acquire));

        let config = tokio::time::timeout(StdDuration::from_millis(100), state.config.read())
            .await
            .expect("reader should not wait for config persistence");
        assert_eq!(config.port, 18_884);
        drop(config);

        let mut wait = Box::pin(state.wait_for_config_snapshot(version));
        assert!(
            tokio::time::timeout(StdDuration::from_millis(25), &mut wait)
                .await
                .is_err()
        );
        release_writer.store(true, Ordering::Release);
        tokio::time::timeout(StdDuration::from_secs(1), wait)
            .await
            .expect("config persistence should finish after release")
            .unwrap();

        assert!(writer_finished.load(Ordering::Acquire));
        assert_eq!(*saved_ports.lock().unwrap(), vec![18_884]);
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn config_journal_persists_newer_snapshot_after_inflight_older_write() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_job = attempts.clone();
        let first_started = Arc::new(AtomicBool::new(false));
        let first_started_for_job = first_started.clone();
        let release_first = Arc::new(AtomicBool::new(false));
        let release_first_for_job = release_first.clone();
        let saved_ports = Arc::new(StdMutex::new(Vec::new()));
        let saved_ports_for_job = saved_ports.clone();
        let coordinator =
            ConfigWriteCoordinator::new_with_job(MetricsStore::new(), move |config| {
                if attempts_for_job.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_started_for_job.store(true, Ordering::Release);
                    while !release_first_for_job.load(Ordering::Acquire) {
                        std::thread::sleep(StdDuration::from_millis(1));
                    }
                }
                saved_ports_for_job.lock().unwrap().push(config.port);
                Ok(())
            });
        let mut config = AppConfig::default();
        config.port = 18_881;
        let first_version = coordinator.publish(&config).unwrap();
        while !first_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        config.port = 18_882;
        let second_version = coordinator.publish(&config).unwrap();
        assert!(second_version > first_version);
        release_first.store(true, Ordering::Release);
        coordinator.wait_for(second_version).await.unwrap();

        assert_eq!(*saved_ports.lock().unwrap(), vec![18_881, 18_882]);
    }

    #[tokio::test]
    async fn config_journal_close_is_a_final_publication_fence() {
        let saved_ports = Arc::new(StdMutex::new(Vec::new()));
        let saved_ports_for_job = saved_ports.clone();
        let coordinator =
            ConfigWriteCoordinator::new_with_job(MetricsStore::new(), move |config| {
                saved_ports_for_job.lock().unwrap().push(config.port);
                Ok(())
            });
        let mut config = AppConfig::default();
        config.port = 18_881;
        let version = coordinator.publish(&config).unwrap();
        coordinator.close_and_flush().await.unwrap();
        coordinator.wait_for(version).await.unwrap();

        config.port = 18_882;
        assert!(coordinator.publish(&config).is_err());
        assert_eq!(*saved_ports.lock().unwrap(), vec![18_881]);
    }

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
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
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
            Some("custom")
        );
        assert_eq!(
            value
                .get("model_providers")
                .and_then(|providers| providers.get("custom"))
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
    async fn runtime_state_persists_prefix_but_not_plaintext_response_session() {
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
                recent_clean_tiny_gap_streak: 2,
                cache_instability_score: 0,
                settle_after_cold_read: false,
                tail_tool_output_chars: 0,
                tail_largest_tool_output_chars: 0,
                tail_tool_output_noise_hint: None,
            },
        );
        state
            .continuation_lineage
            .seed_for_test(
                "session-a",
                crate::continuation_lineage::ResponseSessionState {
                    generation: 1,
                    parent_generation: None,
                    response_id: "resp_sensitive_123".to_string(),
                    input: json!([{
                        "type": "function_call_output",
                        "call_id": "call-sensitive",
                        "output": {
                            "file": "SENSITIVE_SOURCE_MARKER",
                            "stderr": "SENSITIVE_ERROR_MARKER",
                            "exit_code": 1
                        }
                    }]),
                    output_items: Vec::new(),
                    finished_at: Instant::now(),
                },
            )
            .await;
        state.response_session_error_cooldowns.lock().await.insert(
            "responses:share:gpt-5.5".to_string(),
            ResponseSessionCooldownState {
                until: Instant::now() + StdDuration::from_secs(3600),
                failures: 2,
                unsupported: true,
            },
        );
        let shadow_now = Utc::now();
        let mut shadow_affinity = state.shadow_affinity.lock().await;
        shadow_affinity.assignments.insert(
            "conversation-a".to_string(),
            ShadowAffinityAssignment {
                conversation_id: "conversation-a".to_string(),
                cohort_id: "cohort-a".to_string(),
                realm_id: "realm-a".to_string(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                lane: ShadowCacheLane::Steady,
                arm: ShadowAffinityArm::Baseline,
                shard: 0,
                anchor_epoch: 2,
                created_at: shadow_now,
                last_seen_at: shadow_now,
                observations: 4,
                successful_observations: 3,
                usage_observations: 3,
                inconclusive_observations: 1,
                input_tokens: 12_000,
                cache_read_tokens: 10_000,
                active_cache_route_state: Default::default(),
                active_cache_route_baseline: Default::default(),
                active_cache_route_candidate: Default::default(),
                active_cache_route_reason: None,
                active_cache_route_legacy_seed_consumed: false,
                active_cache_route_valid_until: None,
            },
        );
        shadow_affinity.post_burst.next_window_id = 7;
        shadow_affinity.post_burst.windows.insert(
            "conversation-a".to_string(),
            PostBurstWindow {
                window_id: 7,
                conversation_id: "conversation-a".to_string(),
                opened_at: shadow_now,
                expires_at: shadow_now + chrono::Duration::hours(1),
                remaining_requests: 2,
                captured_requests: 1,
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant:
                    crate::proxy::cache_affinity::ShadowCacheCandidateVariant::CohortKey,
                realm_id: "realm-a".to_string(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 0,
            },
        );
        shadow_affinity
            .post_burst
            .evidence
            .push_back(PostBurstEvidence {
                window_id: 6,
                conversation_id: "conversation-a".to_string(),
                observed_at: shadow_now,
                followup_index: 1,
                realm_id: "realm-a".to_string(),
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant:
                    crate::proxy::cache_affinity::ShadowCacheCandidateVariant::CohortKey,
                arm: ShadowAffinityArm::Baseline,
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 2,
                success: true,
                status: 200,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 90_000,
                cache_ratio_bps: 9_000,
                avoidable_gap_tokens: 1_024,
                provider_unstable_gap_tokens: 0,
                ttft_ms: 1_200,
                attempt_count: 1,
                candidate_applied: false,
            });
        drop(shadow_affinity);

        state.persist_runtime_state().await.unwrap();
        let raw = fs::read_to_string(&state.runtime_state_path).unwrap();
        assert!(!raw.contains("SENSITIVE_SOURCE_MARKER"));
        assert!(!raw.contains("SENSITIVE_ERROR_MARKER"));
        assert!(!raw.contains("resp_sensitive_123"));
        let loaded = load_runtime_state(&state.runtime_state_path).unwrap();

        let prefix = loaded.prefix_states.get("prefix-a").unwrap();
        assert_eq!(prefix.cache_read_tokens, 166_912);
        assert_eq!(prefix.recent_clean_tiny_gap_streak, 0);
        let cooldown = loaded
            .response_session_error_cooldowns
            .get("responses:share:gpt-5.5")
            .unwrap();
        assert_eq!(cooldown.failures, 2);
        assert!(cooldown.unsupported);
        assert!(cooldown.until > Instant::now());
        let shadow = loaded
            .shadow_affinity
            .assignments
            .get("conversation-a")
            .unwrap();
        assert_eq!(shadow.cohort_id, "cohort-a");
        assert_eq!(
            shadow.arm,
            crate::proxy::cache_affinity::ShadowAffinityArm::Baseline
        );
        assert_eq!(shadow.anchor_epoch, 2);
        assert_eq!(shadow.usage_observations, 3);
        assert_eq!(loaded.shadow_affinity.post_burst.next_window_id, 7);
        assert_eq!(loaded.shadow_affinity.post_burst.windows.len(), 1);
        assert_eq!(loaded.shadow_affinity.post_burst.evidence.len(), 1);
        let evidence = loaded.shadow_affinity.post_burst.evidence.front().unwrap();
        assert_eq!(evidence.cache_ratio_bps, 9_000);
        assert_eq!(evidence.attempt_count, 1);

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn runtime_state_migrates_old_agent_prefix_keys_to_same_upstream_group() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
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
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(256000),
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
        let mut prefix_states = HashMap::new();
        prefix_states.insert(
            "agent-codex-hb\0codex-auto-review\0responses\0fingerprint-a".to_string(),
            PrefixWarmState {
                finished_at: Instant::now(),
                input_tokens: 220_000,
                cache_read_tokens: 215_040,
                shortfall_tokens: 4_096,
                seen_bucket_tokens: 215_040,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: 4_096,
                seen_bucket_tokens_128: 215_040,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                recent_clean_tiny_gap_streak: 0,
                cache_instability_score: 0,
                settle_after_cold_read: false,
                tail_tool_output_chars: 0,
                tail_largest_tool_output_chars: 0,
                tail_tool_output_noise_hint: None,
            },
        );

        migrate_prefix_states_for_config(&mut prefix_states, &config);

        let migrated_key = "https://hubway.cc/v1\0gpt-5.6-sol\0responses\0fingerprint-a";
        let migrated_alias =
            "prefix-alias\0https://hubway.cc/v1\0gpt-5.6-sol\0responses\0fingerprint-a";
        assert_eq!(
            prefix_states
                .get(migrated_key)
                .map(|state| state.cache_read_tokens),
            Some(215_040)
        );
        assert_eq!(
            prefix_states
                .get(migrated_alias)
                .map(|state| state.seen_bucket_tokens),
            Some(215_040)
        );
    }

    #[test]
    fn runtime_state_migrates_existing_upstream_group_alias_to_real_model() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
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
                display_name: "gpt-5.6-sol".to_string(),
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
        });
        let state = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 174_257,
            cache_read_tokens: 164_736,
            shortfall_tokens: 9_344,
            seen_bucket_tokens: 164_736,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 9_472,
            seen_bucket_tokens_128: 164_736,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 0,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };
        let mut prefix_states = HashMap::from([
            (
                "https://hubway.cc/v1\0gpt-5.5\0responses\0fingerprint-a".to_string(),
                state.clone(),
            ),
            (
                "prefix-alias\0https://hubway.cc/v1\0gpt-5.5\0responses\0fingerprint-a".to_string(),
                state,
            ),
        ]);

        migrate_prefix_states_for_config(&mut prefix_states, &config);

        assert_eq!(
            prefix_states
                .get("https://hubway.cc/v1\0gpt-5.6-sol\0responses\0fingerprint-a")
                .map(|state| state.seen_bucket_tokens_128),
            Some(164_736)
        );
        assert_eq!(
            prefix_states
                .get("prefix-alias\0https://hubway.cc/v1\0gpt-5.6-sol\0responses\0fingerprint-a")
                .map(|state| state.cache_read_tokens),
            Some(164_736)
        );
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
                recent_clean_tiny_gap_streak: 0,
                cache_instability_score: 0,
                settle_after_cold_read: false,
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

    #[test]
    fn prefix_lock_registry_reuses_active_locks_and_evicts_only_idle_entries() {
        let mut registry = PrefixLockRegistry::default();
        let held = registry.acquire("held-prefix");
        let same = registry.acquire("held-prefix");
        assert!(std::sync::Arc::ptr_eq(&held, &same));

        for index in 0..(PREFIX_LOCK_REGISTRY_LIMIT - 1) {
            let _ = registry.acquire(&format!("idle-prefix-{index}"));
        }
        assert_eq!(registry.len(), PREFIX_LOCK_REGISTRY_LIMIT);

        let incoming = registry.acquire("incoming-prefix");

        assert_eq!(registry.len(), PREFIX_LOCK_REGISTRY_LIMIT);
        assert!(registry.contains("held-prefix"));
        assert!(registry.contains("incoming-prefix"));
        assert!(!std::sync::Arc::ptr_eq(&held, &incoming));
    }

    #[test]
    fn prefix_lock_registry_does_not_grow_when_every_slot_is_active() {
        let mut registry = PrefixLockRegistry::default();
        let active = (0..PREFIX_LOCK_REGISTRY_LIMIT)
            .map(|index| registry.acquire(&format!("active-prefix-{index}")))
            .collect::<Vec<_>>();

        let overflow = registry.acquire("overflow-prefix");

        assert_eq!(registry.len(), PREFIX_LOCK_REGISTRY_LIMIT);
        assert!(!registry.contains("overflow-prefix"));
        assert_eq!(std::sync::Arc::strong_count(&overflow), 1);

        drop(active);
        let tracked = registry.acquire("overflow-prefix");
        assert!(registry.contains("overflow-prefix"));
        assert!(!std::sync::Arc::ptr_eq(&overflow, &tracked));
    }

    #[test]
    fn prefix_lock_registry_bounds_a_long_unique_prefix_stream() {
        let mut registry = PrefixLockRegistry::default();
        let total = PREFIX_LOCK_REGISTRY_LIMIT * 3;
        for index in 0..total {
            let _ = registry.acquire(&format!("unique-prefix-{index}"));
            assert!(registry.len() <= PREFIX_LOCK_REGISTRY_LIMIT);
        }

        assert_eq!(registry.len(), PREFIX_LOCK_REGISTRY_LIMIT);
        assert!(!registry.contains("unique-prefix-0"));
        assert!(registry.contains(&format!("unique-prefix-{}", total - 1)));
    }

    #[test]
    fn prefix_runtime_state_trim_removes_expired_and_keeps_the_freshest_entries() {
        let now = Instant::now();
        let mut states = HashMap::new();
        states.insert(
            "expired-prefix".to_string(),
            prefix_state_at(
                now.checked_sub(PREFIX_RUNTIME_STATE_TTL + StdDuration::from_secs(1))
                    .unwrap_or(now),
            ),
        );
        let total = PREFIX_RUNTIME_STATE_LIMIT + 96;
        for index in 0..total {
            let age_ms = (total - index) as u64;
            states.insert(
                format!("prefix-{index}"),
                prefix_state_at(
                    now.checked_sub(StdDuration::from_millis(age_ms))
                        .unwrap_or(now),
                ),
            );
        }

        trim_prefix_runtime_states(&mut states);

        assert_eq!(states.len(), PREFIX_RUNTIME_STATE_LIMIT);
        assert!(!states.contains_key("expired-prefix"));
        assert!(!states.contains_key("prefix-0"));
        assert!(states.contains_key(&format!("prefix-{}", total - 1)));
    }
}
