use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard},
};
use tokio::sync::RwLock;

use crate::{
    config::CacheConfig,
    crypto,
    metrics::MetricsStore,
    persistence::{WriteBehindCoordinator, WriteOperation},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub key: String,
    pub semantic_text: Option<String>,
    #[serde(default)]
    pub semantic_shape: Option<String>,
    #[serde(default)]
    pub semantic_vector: Vec<(u64, f32)>,
    pub content_type: String,
    pub status: u16,
    pub body: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub provider_id: String,
    pub model: String,
    #[serde(default)]
    pub workspace_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheLookup {
    pub entry: CacheEntry,
    pub status: CacheLookupStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheLookupStatus {
    Exact,
    Semantic,
}

#[derive(Debug, Clone)]
pub struct CacheStore {
    entries: Arc<RwLock<HashMap<String, Arc<CacheEntry>>>>,
    path: PathBuf,
    persistence: CacheWriteCoordinator,
    #[cfg(test)]
    publication_hook: Option<CachePublicationHook>,
}

#[cfg(test)]
#[derive(Clone)]
struct CachePublicationHook(Arc<dyn Fn(WriteOperation) + Send + Sync>);

#[cfg(test)]
impl std::fmt::Debug for CachePublicationHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CachePublicationHook(..)")
    }
}

#[derive(Debug, Clone)]
struct CacheWriteCoordinator {
    state: Arc<StdMutex<CacheWriteState>>,
    writer: WriteBehindCoordinator,
}

#[derive(Debug)]
struct CacheWriteState {
    accepting: bool,
    latest_snapshot: Arc<Vec<Arc<CacheEntry>>>,
}

#[derive(Debug, Clone)]
enum CacheWriteRequest {
    Snapshot(Arc<Vec<Arc<CacheEntry>>>),
    Delete,
}

impl CacheWriteRequest {
    #[cfg(test)]
    fn operation(&self) -> WriteOperation {
        match self {
            Self::Snapshot(_) => WriteOperation::Snapshot,
            Self::Delete => WriteOperation::Delete,
        }
    }
}

struct CacheMutationGate<'a> {
    coordinator: &'a CacheWriteCoordinator,
    state: StdMutexGuard<'a, CacheWriteState>,
}

impl CacheMutationGate<'_> {
    fn publish_snapshot(&mut self, snapshot: Vec<Arc<CacheEntry>>) -> u64 {
        self.state.latest_snapshot = Arc::new(snapshot);
        self.coordinator.writer.mark_dirty(WriteOperation::Snapshot)
    }

    fn publish_delete(&mut self) -> u64 {
        // Release cached response bodies immediately. An older snapshot worker
        // that has not captured its payload can safely write this empty state
        // before the newer Delete operation removes the file.
        self.state.latest_snapshot = Arc::new(Vec::new());
        self.coordinator.writer.mark_dirty(WriteOperation::Delete)
    }
}

impl CacheWriteCoordinator {
    fn new(path: PathBuf) -> Self {
        Self::new_with_job(move |request| match request {
            CacheWriteRequest::Snapshot(snapshot) => {
                write_cache_snapshot(&path, snapshot.as_ref().clone())
            }
            CacheWriteRequest::Delete => match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err.into()),
            },
        })
    }

    fn new_with_job(
        write_job: impl Fn(CacheWriteRequest) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        let state = Arc::new(StdMutex::new(CacheWriteState {
            accepting: true,
            latest_snapshot: Arc::new(Vec::new()),
        }));
        let state_for_writer = state.clone();
        let writer = WriteBehindCoordinator::new("cache_persist", move |operation| {
            let request = match operation {
                WriteOperation::Snapshot => {
                    let snapshot = state_for_writer
                        .lock()
                        .expect("cache persistence snapshot lock must not be poisoned")
                        .latest_snapshot
                        .clone();
                    CacheWriteRequest::Snapshot(snapshot)
                }
                WriteOperation::Delete => CacheWriteRequest::Delete,
            };
            write_job(request)
        });
        Self { state, writer }
    }

    fn begin_mutation(&self) -> Result<CacheMutationGate<'_>> {
        let state = self
            .state
            .lock()
            .expect("cache persistence snapshot lock must not be poisoned");
        if !state.accepting {
            return Err(anyhow::anyhow!("response cache is closed"));
        }
        Ok(CacheMutationGate {
            coordinator: self,
            state,
        })
    }

    fn attach_error_reporter(&self, metrics: MetricsStore) {
        self.writer.attach_error_reporter(metrics);
    }

    async fn wait_for(&self, version: u64) -> Result<()> {
        self.writer.wait_for(version).await
    }

    async fn flush(&self) -> Result<()> {
        match self.writer.flush_latest().await {
            Ok(()) => Ok(()),
            Err(_) => self.writer.retry_latest().await,
        }
    }

    async fn close_and_flush(&self) -> Result<()> {
        self.close();
        self.flush().await
    }

    fn close(&self) {
        self.state
            .lock()
            .expect("cache persistence snapshot lock must not be poisoned")
            .accepting = false;
    }

    #[cfg(test)]
    fn is_accepting(&self) -> bool {
        self.state
            .lock()
            .expect("cache persistence snapshot lock must not be poisoned")
            .accepting
    }
}

impl CacheStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let entries = Arc::new(RwLock::new(HashMap::new()));
        let store = Self {
            persistence: CacheWriteCoordinator::new(path.clone()),
            entries,
            path,
            #[cfg(test)]
            publication_hook: None,
        };
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn load_with_persistence_job(
        path: PathBuf,
        write_job: impl Fn(WriteOperation) -> Result<()> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::load_with_snapshot_persistence_job(path, move |request| {
            write_job(request.operation())
        })
    }

    #[cfg(test)]
    fn load_with_snapshot_persistence_job(
        path: PathBuf,
        write_job: impl Fn(CacheWriteRequest) -> Result<()> + Send + Sync + 'static,
    ) -> Result<Self> {
        Ok(Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            path,
            persistence: CacheWriteCoordinator::new_with_job(write_job),
            publication_hook: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn load_with_persistence_job_and_publication_hook(
        path: PathBuf,
        write_job: impl Fn(WriteOperation) -> Result<()> + Send + Sync + 'static,
        publication_hook: impl Fn(WriteOperation) + Send + Sync + 'static,
    ) -> Result<Self> {
        let mut store = Self::load_with_persistence_job(path, write_job)?;
        store.publication_hook = Some(CachePublicationHook(Arc::new(publication_hook)));
        Ok(store)
    }

    #[cfg(test)]
    fn before_publication(&self, operation: WriteOperation) {
        if let Some(hook) = &self.publication_hook {
            (hook.0)(operation);
        }
    }

    pub fn attach_error_reporter(&self, metrics: MetricsStore) {
        self.persistence.attach_error_reporter(metrics);
    }

    pub async fn load_from_disk(&self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }
        let raw = fs::read(&self.path)?;
        let plain = crypto::decrypt_cache_bytes(&raw)?;
        let entries: Vec<CacheEntry> = serde_json::from_slice(&plain)?;
        let now = Utc::now();
        let mut guard = self.entries.write().await;
        guard.clear();
        for mut entry in entries.into_iter().filter(|entry| entry.expires_at > now) {
            ensure_semantic_vector(&mut entry);
            guard.insert(entry.key.clone(), Arc::new(entry));
        }
        Ok(())
    }

    pub async fn lookup(
        &self,
        key: &str,
        semantic_text: Option<&str>,
        semantic_shape: Option<&str>,
        provider_id: &str,
        model: &str,
        workspace_fingerprint: &str,
        config: &CacheConfig,
    ) -> Option<CacheLookup> {
        if !config.enabled {
            return None;
        }
        if config.exact_enabled {
            if let Some(hit) = self.lookup_exact(key, config).await {
                return Some(hit);
            }
        }

        if config.semantic_enabled {
            if let (Some(query_text), Some(query_shape)) = (semantic_text, semantic_shape) {
                let now = Utc::now();
                let guard = self.entries.read().await;
                let query_vector = hashed_ngram_embedding(query_text);
                let mut best: Option<(f32, CacheEntry)> = None;
                for entry in guard.values().filter(|entry| entry.expires_at > now) {
                    if entry.provider_id != provider_id
                        || entry.model != model
                        || entry.workspace_fingerprint.as_deref() != Some(workspace_fingerprint)
                        || entry.semantic_shape.as_deref() != Some(query_shape)
                    {
                        continue;
                    }
                    let Some(entry_text) = entry.semantic_text.as_deref() else {
                        continue;
                    };
                    let entry_vector = if entry.semantic_vector.is_empty() {
                        hashed_ngram_embedding(entry_text)
                    } else {
                        entry.semantic_vector.clone()
                    };
                    let score = cosine_similarity(&query_vector, &entry_vector);
                    if score >= config.semantic_threshold
                        && best.as_ref().map(|(best, _)| score > *best).unwrap_or(true)
                    {
                        best = Some((score, entry.as_ref().clone()));
                    }
                }
                if let Some((_, entry)) = best {
                    return Some(CacheLookup {
                        entry,
                        status: CacheLookupStatus::Semantic,
                    });
                }
            }
        }

        None
    }

    pub async fn lookup_exact(&self, key: &str, config: &CacheConfig) -> Option<CacheLookup> {
        if !config.enabled || !config.exact_enabled {
            return None;
        }
        let now = Utc::now();
        let guard = self.entries.read().await;
        guard
            .get(key)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| CacheLookup {
                entry: entry.as_ref().clone(),
                status: CacheLookupStatus::Exact,
            })
    }

    #[cfg(test)]
    pub async fn insert(&self, entry: CacheEntry, config: &CacheConfig) -> Result<()> {
        self.insert_many(vec![entry], config).await
    }

    pub async fn insert_many(
        &self,
        mut entries: Vec<CacheEntry>,
        config: &CacheConfig,
    ) -> Result<()> {
        if !config.enabled {
            return Ok(());
        }
        for entry in &mut entries {
            ensure_semantic_vector(entry);
        }
        let mut guard = self.entries.write().await;
        let mut persistence_gate = self.persistence.begin_mutation()?;
        for entry in entries {
            guard.insert(entry.key.clone(), Arc::new(entry));
        }
        if guard.len() > config.max_entries {
            let mut by_age = guard
                .values()
                .map(|entry| (entry.created_at, entry.key.clone()))
                .collect::<Vec<_>>();
            by_age.sort_by_key(|(created_at, _)| *created_at);
            for (_, key) in by_age.into_iter().take(guard.len() - config.max_entries) {
                guard.remove(&key);
            }
        }
        if config.persist_encrypted {
            let snapshot = guard.values().cloned().collect::<Vec<_>>();
            // Publish the matching immutable snapshot while both the entry map
            // and close gate are held. The writer never reads later live state.
            #[cfg(test)]
            self.before_publication(WriteOperation::Snapshot);
            persistence_gate.publish_snapshot(snapshot);
        }
        drop(persistence_gate);
        drop(guard);
        Ok(())
    }

    pub async fn clear(&self) -> Result<()> {
        let version = {
            let mut entries = self.entries.write().await;
            let mut persistence_gate = self.persistence.begin_mutation()?;
            entries.clear();
            #[cfg(test)]
            self.before_publication(WriteOperation::Delete);
            persistence_gate.publish_delete()
        };
        self.persistence
            .wait_for(version)
            .await
            .context("failed to clear persisted response cache")
    }

    #[cfg(test)]
    pub async fn flush(&self) -> Result<()> {
        self.persistence.flush().await
    }

    pub async fn close_and_flush(&self) -> Result<()> {
        self.persistence.close_and_flush().await
    }
}

fn write_cache_snapshot(path: &Path, entries: Vec<Arc<CacheEntry>>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let entries = entries.iter().map(Arc::as_ref).collect::<Vec<_>>();
    let raw = serde_json::to_vec(&entries)?;
    let encrypted = crypto::encrypt_cache_bytes(&raw)?;
    fs::write(path, encrypted)?;
    Ok(())
}

pub fn cache_path(config_dir: &Path) -> PathBuf {
    config_dir.join("response-cache.bin")
}

pub fn cache_key(
    request: &Value,
    provider_id: &str,
    model: &str,
    workspace_fingerprint: &str,
) -> String {
    let canonical = canonical_json(request);
    let mut hasher = Sha256::new();
    hasher.update(provider_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(workspace_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn near_exact_cache_key(
    request: &Value,
    provider_id: &str,
    model: &str,
    workspace_fingerprint: &str,
) -> String {
    cache_key(
        &normalize_for_near_exact(request),
        provider_id,
        model,
        workspace_fingerprint,
    )
}

pub fn session_cache_key(
    request: &Value,
    provider_id: &str,
    model: &str,
    workspace_fingerprint: &str,
) -> String {
    cache_key(
        &normalize_for_session(request, "$"),
        provider_id,
        model,
        workspace_fingerprint,
    )
}

pub fn session_near_exact_cache_key(
    request: &Value,
    provider_id: &str,
    model: &str,
    workspace_fingerprint: &str,
) -> String {
    cache_key(
        &normalize_for_near_exact(&normalize_for_session(request, "$")),
        provider_id,
        model,
        workspace_fingerprint,
    )
}

pub fn semantic_text(request: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_text(request, &mut parts);
    let text = parts.join("\n").trim().to_string();
    if text.len() < 32 {
        None
    } else {
        Some(text)
    }
}

pub fn semantic_shape(request: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_shape(request, "$", &mut parts);
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("|"))
    }
}

pub fn is_cache_eligible(request: &Value) -> bool {
    let temperature = request
        .get("temperature")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let has_no_store = request
        .get("metadata")
        .and_then(|metadata| metadata.get("cache"))
        .and_then(Value::as_str)
        .map(|cache| cache.eq_ignore_ascii_case("no-store"))
        .unwrap_or(false);
    temperature <= 0.3 && !has_no_store
}

pub fn is_fuzzy_cache_safe(request: &Value) -> bool {
    if has_tool_or_function_context(request) {
        return false;
    }
    let Some(text) = semantic_text(request) else {
        return false;
    };
    let lower = text.to_lowercase();
    let risk_text = format!(
        " {} ",
        lower.split_whitespace().collect::<Vec<_>>().join(" ")
    );
    let high_risk_markers = [
        "```",
        "diff --git",
        "*** begin patch",
        "@@",
        "stack trace",
        "traceback",
        "exception",
        "function ",
        "class ",
        "import ",
        "export ",
        "package ",
        "use ",
        "fn ",
        "def ",
        "select ",
        "insert into",
        "curl ",
        "</",
        " do not ",
        " don't ",
        " must not ",
        " never ",
        " without ",
        " only ",
        " latest ",
        " today",
        " yesterday",
        " tomorrow",
        " current ",
        " now ",
    ];
    if high_risk_markers
        .iter()
        .any(|marker| risk_text.contains(marker) || lower.contains(marker))
    {
        return false;
    }
    let cjk_risk_markers = [
        "不要",
        "不能",
        "禁止",
        "不得",
        "必须",
        "务必",
        "只允许",
        "仅",
        "最新",
        "今天",
        "昨天",
        "明天",
        "现在",
        "实时",
        "当前",
    ];
    if cjk_risk_markers.iter().any(|marker| text.contains(marker)) {
        return false;
    }
    if text.lines().any(|line| line.len() > 240) {
        return false;
    }

    let total = text.chars().count().max(1);
    let syntax = text
        .chars()
        .filter(|ch| {
            matches!(
                ch,
                '{' | '}' | '[' | ']' | ';' | '<' | '>' | '=' | '\\' | '|'
            )
        })
        .count();
    syntax * 100 / total < 8
}

fn has_tool_or_function_context(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.contains_key("tools") || map.contains_key("tool_choice") {
                return true;
            }
            if matches!(
                map.get("type").and_then(Value::as_str),
                Some("function_call" | "function_call_output")
            ) {
                return true;
            }
            map.values().any(has_tool_or_function_context)
        }
        Value::Array(items) => items.iter().any(has_tool_or_function_context),
        _ => false,
    }
}

fn normalize_for_session(value: &Value, path: &str) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| normalize_for_session(item, &format!("{path}[{index}]")))
                .collect(),
        ),
        Value::Object(map) => {
            let mut normalized = serde_json::Map::new();
            for (key, value) in map {
                if should_skip_for_session_key(path, key)
                    || should_skip_agent_noise_for_session_key(path, key, map)
                {
                    continue;
                }
                normalized.insert(
                    key.clone(),
                    normalize_for_session(value, &format!("{path}.{key}")),
                );
            }
            Value::Object(normalized)
        }
        Value::String(text) => normalize_string_for_session_key(path, text),
        _ => value.clone(),
    }
}

fn normalize_string_for_session_key(path: &str, text: &str) -> Value {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".arguments") {
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            return Value::String(canonical_json(&value));
        }
    }
    Value::String(text.to_string())
}

fn should_skip_for_session_key(path: &str, key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let request_root = path == "$" || path == "$.request";
    matches!(
        lower.as_str(),
        "prompt_cache_key"
            | "cache_control"
            | "request_id"
            | "client_request_id"
            | "trace_id"
            | "span_id"
            | "event_id"
            | "run_id"
            | "session_id"
            | "conversation_id"
            | "thread_id"
            | "workspace_id"
            | "project_id"
            | "nonce"
            | "timestamp"
            | "created_at"
            | "updated_at"
            | "traceparent"
    ) || (request_root
        && matches!(
            lower.as_str(),
            "metadata" | "stream_options" | "user" | "store" | "service_tier"
        ))
}

fn should_skip_agent_noise_for_session_key(
    path: &str,
    key: &str,
    parent: &serde_json::Map<String, Value>,
) -> bool {
    let lower = key.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "call_id"
            | "tool_call_id"
            | "tool_use_id"
            | "item_id"
            | "output_index"
            | "content_index"
            | "expires_at"
            | "completed_at"
    ) {
        return true;
    }

    if lower == "id" && is_agent_generated_item_path(path) && parent_has_semantic_payload(parent) {
        return true;
    }

    false
}

fn is_agent_generated_item_path(path: &str) -> bool {
    path.contains(".input[")
        || path.contains(".messages[")
        || path.contains(".content[")
        || path.contains(".tool_calls[")
}

fn parent_has_semantic_payload(parent: &serde_json::Map<String, Value>) -> bool {
    parent.contains_key("content")
        || parent.contains_key("text")
        || parent.contains_key("arguments")
        || parent.contains_key("input")
        || parent.contains_key("output")
        || parent.contains_key("name")
        || parent.contains_key("role")
        || parent.contains_key("type")
        || parent.contains_key("function")
}

fn normalize_for_near_exact(value: &Value) -> Value {
    match value {
        Value::String(text) => Value::String(normalize_string_for_near_exact(text)),
        Value::Array(items) => Value::Array(items.iter().map(normalize_for_near_exact).collect()),
        Value::Object(map) => {
            let mut normalized = serde_json::Map::new();
            for (key, value) in map {
                normalized.insert(key.clone(), normalize_for_near_exact(value));
            }
            Value::Object(normalized)
        }
        _ => value.clone(),
    }
}

fn normalize_string_for_near_exact(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || is_cjk(ch) {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn collect_shape(value: &Value, path: &str, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let joined_keys = keys
                .iter()
                .map(|key| key.as_str())
                .collect::<Vec<_>>()
                .join(",");
            parts.push(format!("{path}:object:{joined_keys}"));
            if let Some(role) = map.get("role").and_then(Value::as_str) {
                parts.push(format!("{path}.role={role}"));
            }
            for key in keys {
                if matches!(
                    key.as_str(),
                    "content" | "text" | "input" | "message" | "system" | "instructions" | "prompt"
                ) {
                    parts.push(format!("{path}.{key}:text"));
                } else {
                    collect_shape(&map[key], &format!("{path}.{key}"), parts);
                }
            }
        }
        Value::Array(items) => {
            parts.push(format!("{path}:array:{}", items.len()));
            for (index, item) in items.iter().enumerate() {
                collect_shape(item, &format!("{path}[{index}]"), parts);
            }
        }
        Value::String(_) => parts.push(format!("{path}:string")),
        Value::Number(_) => parts.push(format!("{path}:number")),
        Value::Bool(_) => parts.push(format!("{path}:bool")),
        Value::Null => parts.push(format!("{path}:null")),
    }
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let body = keys
                .into_iter()
                .map(|key| format!("{:?}:{}", key, canonical_json(&map[key])))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(items) => {
            let body = items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{body}]")
        }
        _ => value.to_string(),
    }
}

fn collect_text(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            if text.len() > 2 {
                parts.push(text.clone());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text(item, parts);
            }
        }
        Value::Object(map) => {
            for key in ["input", "messages", "content", "system", "prompt"] {
                if let Some(value) = map.get(key) {
                    collect_text(value, parts);
                }
            }
        }
        _ => {}
    }
}

fn ensure_semantic_vector(entry: &mut CacheEntry) {
    if entry.semantic_vector.is_empty() {
        if let Some(text) = entry.semantic_text.as_deref() {
            entry.semantic_vector = hashed_ngram_embedding(text);
        }
    }
}

fn hashed_ngram_embedding(text: &str) -> Vec<(u64, f32)> {
    let normalized = normalize_for_embedding(text);
    let chars = normalized.chars().collect::<Vec<_>>();
    let mut vector = HashMap::new();
    if chars.is_empty() {
        return Vec::new();
    }

    for gram in 2..=4 {
        if chars.len() < gram {
            continue;
        }
        for window in chars.windows(gram) {
            let token = window.iter().collect::<String>();
            let hash = stable_hash(&token);
            *vector.entry(hash).or_insert(0.0) += 1.0 / gram as f32;
        }
    }

    for token in normalized
        .split_whitespace()
        .filter(|token| !token.is_empty())
    {
        let hash = stable_hash(token);
        *vector.entry(hash).or_insert(0.0) += 1.5;
    }

    let mut features = vector.into_iter().collect::<Vec<_>>();
    features.sort_by_key(|(key, _)| *key);
    features
}

fn normalize_for_embedding(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || is_cjk(ch) {
                ch
            } else if ch.is_whitespace() {
                ' '
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x4E00..=0x9FFF | 0x3400..=0x4DBF | 0x3040..=0x30FF | 0xAC00..=0xD7AF
    )
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn cosine_similarity(left: &[(u64, f32)], right: &[(u64, f32)]) -> f32 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let mut left_index = 0;
    let mut right_index = 0;
    let mut dot = 0.0;
    while left_index < left.len() && right_index < right.len() {
        let (left_key, left_value) = left[left_index];
        let (right_key, right_value) = right[right_index];
        if left_key == right_key {
            dot += left_value * right_value;
            left_index += 1;
            right_index += 1;
        } else if left_key < right_key {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    let left_norm = left
        .iter()
        .map(|(_, value)| value * value)
        .sum::<f32>()
        .sqrt();
    let right_norm = right
        .iter()
        .map(|(_, value)| value * value)
        .sum::<f32>()
        .sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm * right_norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheMode;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Barrier, Mutex as StdMutex,
    };

    #[test]
    fn cache_key_is_stable_for_reordered_objects() {
        let left = json!({ "model": "m", "temperature": 0, "messages": [{ "role": "user", "content": "hello" }] });
        let right = json!({ "messages": [{ "content": "hello", "role": "user" }], "temperature": 0, "model": "m" });
        assert_eq!(
            cache_key(&left, "p", "m", "w"),
            cache_key(&right, "p", "m", "w")
        );
    }

    #[test]
    fn near_exact_key_ignores_case_spacing_and_punctuation_only() {
        let left = json!({
            "model": "m",
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Hello, 缓存 命中率！" }]
        });
        let near = json!({
            "temperature": 0,
            "model": "m",
            "messages": [{ "content": "  hello 缓存 命中率  ", "role": "user" }]
        });
        let changed = json!({
            "model": "m",
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Hello, 缓存 未命中率！" }]
        });

        assert_ne!(
            cache_key(&left, "p", "m", "w"),
            cache_key(&near, "p", "m", "w")
        );
        assert_eq!(
            near_exact_cache_key(&left, "p", "m", "w"),
            near_exact_cache_key(&near, "p", "m", "w")
        );
        assert_ne!(
            near_exact_cache_key(&left, "p", "m", "w"),
            near_exact_cache_key(&changed, "p", "m", "w")
        );
    }

    #[test]
    fn session_key_ignores_agent_trace_and_request_metadata() {
        let stable = json!({
            "client_channel": "chat",
            "upstream_channel": "chat",
            "client_stream": false,
            "request": {
                "temperature": 0,
                "messages": [{ "role": "user", "content": "repeatable cached prompt" }]
            }
        });
        let volatile = json!({
            "client_channel": "chat",
            "upstream_channel": "chat",
            "client_stream": false,
            "request": {
                "temperature": 0,
                "request_id": "req-1",
                "trace_id": "trace-1",
                "session_id": "session-1",
                "metadata": {
                    "user": "local-agent",
                    "timestamp": "2026-06-18T00:00:00Z"
                },
                "messages": [{ "role": "user", "content": "repeatable cached prompt" }]
            }
        });

        assert_ne!(
            cache_key(&stable, "p", "m", "w"),
            cache_key(&volatile, "p", "m", "w")
        );
        assert_eq!(
            session_cache_key(&stable, "p", "m", "w"),
            session_cache_key(&volatile, "p", "m", "w")
        );
    }

    #[test]
    fn session_key_ignores_tool_call_noise_but_keeps_arguments() {
        let stable = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"path\":\"README.md\"}",
                    "output_index": 0,
                    "content_index": 0
                }, {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "project notes"
                }]
            }
        });
        let noisy = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "id": "fc_2",
                    "call_id": "call_2",
                    "name": "read_file",
                    "arguments": "{\"path\":\"README.md\"}",
                    "output_index": 9,
                    "content_index": 3
                }, {
                    "type": "function_call_output",
                    "call_id": "call_2",
                    "output": "project notes"
                }]
            }
        });
        let changed = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "id": "fc_3",
                    "call_id": "call_3",
                    "name": "read_file",
                    "arguments": "{\"path\":\"src/main.rs\"}"
                }, {
                    "type": "function_call_output",
                    "call_id": "call_3",
                    "output": "project notes"
                }]
            }
        });

        assert_ne!(
            cache_key(&stable, "p", "m", "w"),
            cache_key(&noisy, "p", "m", "w")
        );
        assert_eq!(
            session_cache_key(&stable, "p", "m", "w"),
            session_cache_key(&noisy, "p", "m", "w")
        );
        assert_ne!(
            session_cache_key(&stable, "p", "m", "w"),
            session_cache_key(&changed, "p", "m", "w")
        );
    }

    #[test]
    fn session_key_normalizes_tool_argument_json_order() {
        let left = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "name": "read_file",
                    "arguments": "{\"path\":\"README.md\",\"encoding\":\"utf-8\"}"
                }]
            }
        });
        let reordered = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "name": "read_file",
                    "arguments": "{\"encoding\":\"utf-8\",\"path\":\"README.md\"}"
                }]
            }
        });
        let changed = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "input": [{
                    "type": "function_call",
                    "name": "read_file",
                    "arguments": "{\"encoding\":\"utf-8\",\"path\":\"src/main.rs\"}"
                }]
            }
        });

        assert_ne!(
            cache_key(&left, "p", "m", "w"),
            cache_key(&reordered, "p", "m", "w")
        );
        assert_eq!(
            session_cache_key(&left, "p", "m", "w"),
            session_cache_key(&reordered, "p", "m", "w")
        );
        assert_ne!(
            session_cache_key(&left, "p", "m", "w"),
            session_cache_key(&changed, "p", "m", "w")
        );
    }

    #[test]
    fn session_key_keeps_previous_response_id() {
        let left = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "previous_response_id": "resp_a",
                "input": "continue"
            }
        });
        let right = json!({
            "client_channel": "responses",
            "upstream_channel": "responses",
            "client_stream": true,
            "request": {
                "temperature": 0,
                "previous_response_id": "resp_b",
                "input": "continue"
            }
        });

        assert_ne!(
            session_cache_key(&left, "p", "m", "w"),
            session_cache_key(&right, "p", "m", "w")
        );
    }

    #[test]
    fn tools_can_use_exact_or_session_cache() {
        let request = json!({ "temperature": 0.1, "tools": [{ "name": "x" }] });
        assert!(is_cache_eligible(&request));
        assert!(!is_fuzzy_cache_safe(&request));
    }

    #[test]
    fn responses_function_calls_can_use_exact_or_session_but_not_fuzzy_cache() {
        let request = json!({
            "temperature": 0,
            "input": [{
                "type": "function_call",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            }]
        });

        assert!(is_cache_eligible(&request));
        assert!(!is_fuzzy_cache_safe(&request));
    }

    #[test]
    fn low_temperature_text_request_is_eligible() {
        let request =
            json!({ "temperature": 0.2, "messages": [{ "role": "user", "content": "hello" }] });
        assert!(is_cache_eligible(&request));
    }

    #[test]
    fn dynamic_or_code_context_requests_can_use_exact_but_not_fuzzy_cache() {
        let latest = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Summarize the latest provider status today." }]
        });
        let code_like = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Explain this patch:\n```diff\n@@ -1 +1\n-import old\n+import new\n```" }]
        });

        assert!(is_cache_eligible(&latest));
        assert!(is_cache_eligible(&code_like));
        assert!(!is_fuzzy_cache_safe(&latest));
        assert!(!is_fuzzy_cache_safe(&code_like));
    }

    #[test]
    fn fuzzy_cache_rejects_negated_or_time_sensitive_prompts() {
        let stable = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Summarize the stable project cache report in one paragraph." }]
        });
        let negated = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Do not summarize the stable project cache report in one paragraph." }]
        });
        let current = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "Summarize the latest project cache report today." }]
        });

        assert!(is_fuzzy_cache_safe(&stable));
        assert!(!is_fuzzy_cache_safe(&negated));
        assert!(!is_fuzzy_cache_safe(&current));
    }

    #[test]
    fn semantic_shape_ignores_text_but_keeps_roles() {
        let left = json!({ "messages": [{ "role": "user", "content": "hello" }] });
        let right = json!({ "messages": [{ "role": "user", "content": "hola" }] });
        let different = json!({ "messages": [{ "role": "assistant", "content": "hello" }] });
        assert_eq!(semantic_shape(&left), semantic_shape(&right));
        assert_ne!(semantic_shape(&left), semantic_shape(&different));
    }

    #[test]
    fn local_embedding_similarity_handles_chinese_and_english() {
        let near = cosine_similarity(
            &hashed_ngram_embedding("请总结这个项目的缓存命中率和首字延迟表现。"),
            &hashed_ngram_embedding("  请总结这个项目的缓存命中率和首字延迟表现  "),
        );
        let far = cosine_similarity(
            &hashed_ngram_embedding("请总结这个项目的缓存命中率和首字延迟表现"),
            &hashed_ngram_embedding("生成一张蓝色跑车在海边日落的写实图片"),
        );
        assert!(near > 0.985, "near score was {near}");
        assert!(far < 0.5, "far score was {far}");
    }

    #[tokio::test]
    async fn semantic_lookup_is_scoped_to_provider_model_workspace_and_shape() {
        let path = std::env::temp_dir().join(format!(
            "atoapi-cache-test-{}.bin",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let store = CacheStore::load(path).unwrap();
        let config = CacheConfig {
            mode: CacheMode::PassiveWarm,
            enabled: true,
            exact_enabled: false,
            semantic_enabled: true,
            semantic_threshold: 0.5,
            max_age_seconds: 3600,
            max_entries: 10,
            persist_encrypted: false,
            prewarm_enabled: false,
            background_prewarm_enabled: false,
        };
        let request = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "repeatable workspace prompt with many stable words" }]
        });
        let entry = CacheEntry {
            key: "entry".to_string(),
            semantic_text: semantic_text(&request),
            semantic_shape: semantic_shape(&request),
            semantic_vector: Vec::new(),
            content_type: "application/json".to_string(),
            status: 200,
            body: b"cached".to_vec(),
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(60),
            provider_id: "provider-a".to_string(),
            model: "model-a".to_string(),
            workspace_fingerprint: Some("workspace-a".to_string()),
        };
        store.insert(entry, &config).await.unwrap();

        let query_text = semantic_text(&request);
        let query_shape = semantic_shape(&request);
        assert!(store
            .lookup(
                "miss",
                query_text.as_deref(),
                query_shape.as_deref(),
                "provider-b",
                "model-a",
                "workspace-a",
                &config,
            )
            .await
            .is_none());
        assert!(store
            .lookup(
                "miss",
                query_text.as_deref(),
                query_shape.as_deref(),
                "provider-a",
                "model-b",
                "workspace-a",
                &config,
            )
            .await
            .is_none());
        assert!(store
            .lookup(
                "miss",
                query_text.as_deref(),
                query_shape.as_deref(),
                "provider-a",
                "model-a",
                "workspace-b",
                &config,
            )
            .await
            .is_none());
        assert!(store
            .lookup(
                "miss",
                query_text.as_deref(),
                query_shape.as_deref(),
                "provider-a",
                "model-a",
                "workspace-a",
                &config,
            )
            .await
            .is_some());
    }

    #[tokio::test]
    async fn semantic_lookup_hits_near_duplicate_multilingual_prompt() {
        let path = std::env::temp_dir().join(format!(
            "atoapi-semantic-hit-{}.bin",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let store = CacheStore::load(path).unwrap();
        let config = CacheConfig {
            mode: CacheMode::PassiveWarm,
            enabled: true,
            exact_enabled: false,
            semantic_enabled: true,
            semantic_threshold: 0.985,
            max_age_seconds: 3600,
            max_entries: 10,
            persist_encrypted: false,
            prewarm_enabled: false,
            background_prewarm_enabled: false,
        };
        let stored = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "请总结这个项目的缓存命中率和首字延迟表现。" }]
        });
        let query = json!({
            "temperature": 0,
            "messages": [{ "role": "user", "content": "  请总结这个项目的缓存命中率和首字延迟表现  " }]
        });
        store
            .insert(
                CacheEntry {
                    key: cache_key(&stored, "provider-a", "model-a", "workspace-a"),
                    semantic_text: semantic_text(&stored),
                    semantic_shape: semantic_shape(&stored),
                    semantic_vector: Vec::new(),
                    content_type: "application/json".to_string(),
                    status: 200,
                    body: b"semantic".to_vec(),
                    created_at: Utc::now(),
                    expires_at: Utc::now() + chrono::Duration::seconds(60),
                    provider_id: "provider-a".to_string(),
                    model: "model-a".to_string(),
                    workspace_fingerprint: Some("workspace-a".to_string()),
                },
                &config,
            )
            .await
            .unwrap();

        let hit = store
            .lookup(
                "miss",
                semantic_text(&query).as_deref(),
                semantic_shape(&query).as_deref(),
                "provider-a",
                "model-a",
                "workspace-a",
                &config,
            )
            .await;

        assert!(matches!(
            hit.map(|lookup| lookup.status),
            Some(CacheLookupStatus::Semantic)
        ));
    }

    #[tokio::test]
    async fn warm_replay_300k_hits_at_least_99_percent_with_fast_ttft() {
        let path = std::env::temp_dir().join(format!(
            "atoapi-warm-replay-{}.bin",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let store = CacheStore::load(path).unwrap();
        let config = CacheConfig {
            mode: CacheMode::PassiveWarm,
            enabled: true,
            exact_enabled: true,
            semantic_enabled: false,
            semantic_threshold: 0.985,
            max_age_seconds: 3600,
            max_entries: 320_000,
            persist_encrypted: false,
            prewarm_enabled: false,
            background_prewarm_enabled: false,
        };
        let provider_id = "provider-a";
        let model = "model-a";
        let workspace = "workspace-a";
        let total = 300_000usize;
        let mut keys = Vec::with_capacity(total);

        for index in 0..total {
            let request = json!({
                "temperature": 0,
                "messages": [{
                    "role": "user",
                    "content": format!("stable cached prompt number {index} with deterministic local replay body")
                }]
            });
            let key = cache_key(&request, provider_id, model, workspace);
            keys.push(key.clone());
            store
                .insert(
                    CacheEntry {
                        key,
                        semantic_text: None,
                        semantic_shape: None,
                        semantic_vector: Vec::new(),
                        content_type: "application/json".to_string(),
                        status: 200,
                        body: br#"{"ok":true}"#.to_vec(),
                        created_at: Utc::now(),
                        expires_at: Utc::now() + chrono::Duration::seconds(60),
                        provider_id: provider_id.to_string(),
                        model: model.to_string(),
                        workspace_fingerprint: Some(workspace.to_string()),
                    },
                    &config,
                )
                .await
                .unwrap();
        }

        let mut hits = 0usize;
        let mut samples = Vec::with_capacity(keys.len());
        for key in keys {
            let started = std::time::Instant::now();
            let hit = store
                .lookup(
                    key.as_str(),
                    None,
                    None,
                    provider_id,
                    model,
                    workspace,
                    &config,
                )
                .await;
            samples.push(started.elapsed().as_millis() as u64);
            if hit.is_some() {
                hits += 1;
            }
        }

        samples.sort_unstable();
        let hit_rate = hits as f64 / total as f64;
        let p95_ms = samples[(samples.len() as f64 * 0.95).floor() as usize];

        assert!(
            hit_rate >= 0.99,
            "warm replay hit rate was {hit_rate:.4}, expected >= 0.99"
        );
        assert!(
            p95_ms < 50,
            "warm replay cache-hit TTFT p95 was {p95_ms}ms, expected < 50ms"
        );
    }

    #[tokio::test]
    async fn insert_many_flushes_one_reloadable_snapshot_and_clear_orders_delete() {
        let path = std::env::temp_dir().join(format!(
            "atoapi-write-behind-cache-{}.bin",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let store = CacheStore::load(path.clone()).unwrap();
        let config = CacheConfig {
            mode: CacheMode::PassiveWarm,
            enabled: true,
            exact_enabled: true,
            semantic_enabled: false,
            semantic_threshold: 0.985,
            max_age_seconds: 3600,
            max_entries: 10,
            persist_encrypted: true,
            prewarm_enabled: false,
            background_prewarm_enabled: false,
        };
        let now = Utc::now();
        let make_entry = |key: &str| CacheEntry {
            key: key.to_string(),
            semantic_text: None,
            semantic_shape: None,
            semantic_vector: Vec::new(),
            content_type: "application/json".to_string(),
            status: 200,
            body: format!("{{\"key\":\"{key}\"}}").into_bytes(),
            created_at: now,
            expires_at: now + chrono::Duration::minutes(5),
            provider_id: "provider".to_string(),
            model: "model".to_string(),
            workspace_fingerprint: Some("workspace".to_string()),
        };

        store
            .insert_many(vec![make_entry("first"), make_entry("second")], &config)
            .await
            .unwrap();
        store.flush().await.unwrap();
        assert!(path.exists());

        let reloaded = CacheStore::load(path.clone()).unwrap();
        reloaded.load_from_disk().await.unwrap();
        assert!(reloaded.lookup_exact("first", &config).await.is_some());
        assert!(reloaded.lookup_exact("second", &config).await.is_some());

        store
            .insert_many(vec![make_entry("pending")], &config)
            .await
            .unwrap();
        store.clear().await.unwrap();
        store.flush().await.unwrap();
        assert!(!path.exists());
    }

    fn concurrent_persistence_config() -> CacheConfig {
        CacheConfig {
            mode: CacheMode::PassiveWarm,
            enabled: true,
            exact_enabled: true,
            semantic_enabled: false,
            semantic_threshold: 0.985,
            max_age_seconds: 3600,
            max_entries: 10,
            persist_encrypted: true,
            prewarm_enabled: false,
            background_prewarm_enabled: false,
        }
    }

    fn concurrent_cache_entry(key: &str) -> CacheEntry {
        CacheEntry {
            key: key.to_string(),
            semantic_text: None,
            semantic_shape: None,
            semantic_vector: Vec::new(),
            content_type: "application/json".to_string(),
            status: 200,
            body: format!("{{\"key\":\"{key}\"}}").into_bytes(),
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            provider_id: "provider".to_string(),
            model: "model".to_string(),
            workspace_fingerprint: Some("workspace".to_string()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn insert_publication_holds_mutation_order_against_concurrent_clear() {
        let operations = Arc::new(StdMutex::new(Vec::new()));
        let operations_for_job = operations.clone();
        let publication_entered = Arc::new(Barrier::new(2));
        let publication_entered_for_hook = publication_entered.clone();
        let publication_release = Arc::new(Barrier::new(2));
        let publication_release_for_hook = publication_release.clone();
        let store = CacheStore::load_with_persistence_job_and_publication_hook(
            PathBuf::from("unused-cache-path"),
            move |operation| {
                operations_for_job.lock().unwrap().push(operation);
                Ok(())
            },
            move |operation| {
                if operation == WriteOperation::Snapshot {
                    publication_entered_for_hook.wait();
                    publication_release_for_hook.wait();
                }
            },
        )
        .unwrap();
        let config = concurrent_persistence_config();
        let insert_store = store.clone();
        let insert_config = config.clone();
        let insert = tokio::spawn(async move {
            insert_store
                .insert_many(
                    vec![concurrent_cache_entry("insert-before-clear")],
                    &insert_config,
                )
                .await
                .unwrap();
        });

        publication_entered.wait();
        let clear_completed = Arc::new(AtomicBool::new(false));
        let clear_completed_for_task = clear_completed.clone();
        let clear_store = store.clone();
        let clear = tokio::spawn(async move {
            clear_store.clear().await.unwrap();
            clear_completed_for_task.store(true, Ordering::Release);
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!clear_completed.load(Ordering::Acquire));
        publication_release.wait();

        insert.await.unwrap();
        clear.await.unwrap();
        store.flush().await.unwrap();
        assert_eq!(
            operations.lock().unwrap().last(),
            Some(&WriteOperation::Delete)
        );
        assert!(store
            .lookup_exact("insert-before-clear", &config)
            .await
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn clear_publication_holds_mutation_order_against_concurrent_insert() {
        let operations = Arc::new(StdMutex::new(Vec::new()));
        let operations_for_job = operations.clone();
        let publication_entered = Arc::new(Barrier::new(2));
        let publication_entered_for_hook = publication_entered.clone();
        let publication_release = Arc::new(Barrier::new(2));
        let publication_release_for_hook = publication_release.clone();
        let store = CacheStore::load_with_persistence_job_and_publication_hook(
            PathBuf::from("unused-cache-path"),
            move |operation| {
                operations_for_job.lock().unwrap().push(operation);
                Ok(())
            },
            move |operation| {
                if operation == WriteOperation::Delete {
                    publication_entered_for_hook.wait();
                    publication_release_for_hook.wait();
                }
            },
        )
        .unwrap();
        let config = concurrent_persistence_config();
        let clear_store = store.clone();
        let clear = tokio::spawn(async move {
            clear_store.clear().await.unwrap();
        });

        publication_entered.wait();
        let insert_completed = Arc::new(AtomicBool::new(false));
        let insert_completed_for_task = insert_completed.clone();
        let insert_store = store.clone();
        let insert_config = config.clone();
        let insert = tokio::spawn(async move {
            insert_store
                .insert_many(
                    vec![concurrent_cache_entry("insert-after-clear")],
                    &insert_config,
                )
                .await
                .unwrap();
            insert_completed_for_task.store(true, Ordering::Release);
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!insert_completed.load(Ordering::Acquire));
        publication_release.wait();

        clear.await.unwrap();
        insert.await.unwrap();
        store.flush().await.unwrap();
        assert_eq!(
            operations.lock().unwrap().last(),
            Some(&WriteOperation::Snapshot)
        );
        assert!(store
            .lookup_exact("insert-after-clear", &config)
            .await
            .is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_snapshot_excludes_entries_inserted_with_persistence_disabled() {
        let path = std::env::temp_dir().join(format!(
            "atoapi-immutable-cache-snapshot-{}.bin",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let writer_entered = Arc::new(Barrier::new(2));
        let writer_entered_for_job = writer_entered.clone();
        let writer_release = Arc::new(Barrier::new(2));
        let writer_release_for_job = writer_release.clone();
        let path_for_job = path.clone();
        let store = CacheStore::load_with_snapshot_persistence_job(path.clone(), move |request| {
            match request {
                CacheWriteRequest::Snapshot(snapshot) => {
                    writer_entered_for_job.wait();
                    writer_release_for_job.wait();
                    write_cache_snapshot(&path_for_job, snapshot.as_ref().clone())
                }
                CacheWriteRequest::Delete => Ok(()),
            }
        })
        .unwrap();
        let persisted = concurrent_persistence_config();
        let mut memory_only = persisted.clone();
        memory_only.persist_encrypted = false;

        store
            .insert_many(
                vec![concurrent_cache_entry("published-before-disable")],
                &persisted,
            )
            .await
            .unwrap();
        writer_entered.wait();
        store
            .insert_many(
                vec![concurrent_cache_entry("memory-only-after-disable")],
                &memory_only,
            )
            .await
            .unwrap();
        writer_release.wait();
        store.flush().await.unwrap();

        let reloaded = CacheStore::load(path.clone()).unwrap();
        reloaded.load_from_disk().await.unwrap();
        assert!(reloaded
            .lookup_exact("published-before-disable", &persisted)
            .await
            .is_some());
        assert!(reloaded
            .lookup_exact("memory-only-after-disable", &persisted)
            .await
            .is_none());
        fs::remove_file(path).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_gate_rejects_clear_and_insert_while_final_snapshot_is_inflight() {
        let writer_entered = Arc::new(Barrier::new(2));
        let writer_entered_for_job = writer_entered.clone();
        let writer_release = Arc::new(Barrier::new(2));
        let writer_release_for_job = writer_release.clone();
        let store = CacheStore::load_with_snapshot_persistence_job(
            PathBuf::from("unused-cache-path"),
            move |request| {
                if matches!(request, CacheWriteRequest::Snapshot(_)) {
                    writer_entered_for_job.wait();
                    writer_release_for_job.wait();
                }
                Ok(())
            },
        )
        .unwrap();
        let config = concurrent_persistence_config();
        store
            .insert_many(vec![concurrent_cache_entry("before-close")], &config)
            .await
            .unwrap();
        writer_entered.wait();

        let closing_store = store.clone();
        let close = tokio::spawn(async move { closing_store.close_and_flush().await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while store.persistence.is_accepting() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cache close gate should stop accepting mutations");

        assert!(store.clear().await.is_err());
        assert!(store
            .insert_many(vec![concurrent_cache_entry("after-close")], &config)
            .await
            .is_err());
        assert!(store.lookup_exact("before-close", &config).await.is_some());
        assert!(store.lookup_exact("after-close", &config).await.is_none());

        writer_release.wait();
        close.await.unwrap().unwrap();
    }
}
