use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{config::CacheConfig, crypto};

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
    entries: Arc<RwLock<HashMap<String, CacheEntry>>>,
    path: PathBuf,
}

impl CacheStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let store = Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            path,
        };
        Ok(store)
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
            guard.insert(entry.key.clone(), entry);
        }
        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        let entries = self.entries.read().await;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_vec(&entries.values().cloned().collect::<Vec<_>>())?;
        let encrypted = crypto::encrypt_cache_bytes(&raw)?;
        fs::write(&self.path, encrypted)?;
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
                        best = Some((score, entry.clone()));
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
                entry: entry.clone(),
                status: CacheLookupStatus::Exact,
            })
    }

    pub async fn insert(&self, mut entry: CacheEntry, config: &CacheConfig) -> Result<()> {
        if !config.enabled {
            return Ok(());
        }
        ensure_semantic_vector(&mut entry);
        let mut guard = self.entries.write().await;
        guard.insert(entry.key.clone(), entry);
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
        drop(guard);
        if config.persist_encrypted {
            self.persist()
                .await
                .context("failed to persist response cache")?;
        }
        Ok(())
    }

    pub async fn clear(&self) -> Result<()> {
        self.entries.write().await.clear();
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }
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
}
