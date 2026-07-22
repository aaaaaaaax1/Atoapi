use crate::config::{AppConfig, SelectedProviderKey};
#[cfg(test)]
use crate::proxy::json_canonical::canonical_json_string;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::Map;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{self, Write};

use super::RouteDecision;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct AffinityIdentity {
    pub realm_id: String,
    pub cohort_id: String,
    pub stable_prefix_digest: String,
    pub trusted_conversation_id: Option<String>,
    pub trusted_identity_source: Option<String>,
}

pub(super) fn derive(
    config: &AppConfig,
    decision: &RouteDecision,
    client_request: &Value,
    upstream_request: &Value,
    agent_id: Option<&str>,
    selected_key: &SelectedProviderKey,
) -> AffinityIdentity {
    let provider_deployment = normalized_deployment(&decision.provider.base_url);
    let channel = decision.upstream_channel.label();
    let agent_scope = agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let realm_id = realm_id(decision, selected_key);
    let stable_prefix_digest = stable_prefix_digest(upstream_request);
    let cohort_id = hash_parts(&[
        "cache-cohort-v1",
        &config.workspace_fingerprint,
        agent_scope,
        &provider_deployment,
        channel,
        &decision.model,
        &realm_id,
        &stable_prefix_digest,
    ]);
    let (trusted_conversation_id, trusted_identity_source) =
        trusted_conversation_identity(client_request, config, decision, agent_scope);

    AffinityIdentity {
        realm_id,
        cohort_id,
        stable_prefix_digest,
        trusted_conversation_id,
        trusted_identity_source,
    }
}

fn normalized_deployment(base_url: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    reqwest::Url::parse(trimmed)
        .map(|url| url.to_string().trim_end_matches('/').to_string())
        .unwrap_or_else(|_| trimmed.to_string())
}

pub(super) fn key_realm_id(selected_key: &SelectedProviderKey) -> String {
    let key_id = selected_key
        .key_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let secret_digest = if selected_key.secret.trim().is_empty() {
        "empty".to_string()
    } else {
        hash_text(selected_key.secret.trim())
    };
    hash_parts(&["key-record-v2", key_id, &secret_digest])
}

pub(super) fn realm_id(decision: &RouteDecision, selected_key: &SelectedProviderKey) -> String {
    let provider_deployment = normalized_deployment(&decision.provider.base_url);
    let channel = decision.upstream_channel.label();
    let key_record = key_realm_id(selected_key);
    hash_parts(&[
        "cache-realm-v2",
        &provider_deployment,
        channel,
        &decision.model,
        &key_record,
    ])
}

fn trusted_conversation_identity(
    request: &Value,
    config: &AppConfig,
    decision: &RouteDecision,
    agent_scope: &str,
) -> (Option<String>, Option<String>) {
    const DIRECT_KEYS: [(&str, &str); 3] = [
        ("thread-id", "thread_id"),
        ("conversation-id", "conversation_id"),
        ("session-id", "session_id"),
    ];
    for (source, key) in DIRECT_KEYS {
        if let Some(value) = request.get(key).and_then(trusted_value) {
            return (
                Some(hash_parts(&[
                    "trusted-conversation-v1",
                    &config.workspace_fingerprint,
                    agent_scope,
                    &decision.provider.id,
                    &decision.model,
                    source,
                    &value,
                ])),
                Some(source.to_string()),
            );
        }
    }
    for container in ["metadata", "context", "client_context"] {
        let Some(object) = request.get(container) else {
            continue;
        };
        for (source, key) in DIRECT_KEYS {
            if let Some(value) = object.get(key).and_then(trusted_value) {
                return (
                    Some(hash_parts(&[
                        "trusted-conversation-v1",
                        &config.workspace_fingerprint,
                        agent_scope,
                        &decision.provider.id,
                        &decision.model,
                        source,
                        &value,
                    ])),
                    Some(format!("{container}.{source}")),
                );
            }
        }
    }
    (None, None)
}

fn trusted_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty() && value.len() <= 512).then(|| value.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Hash the exact same canonical stable-prefix representation that earlier
/// releases used for cohort assignment, without cloning or concatenating the
/// complete instructions/tool schema into a temporary `Value` and `String`.
///
/// This is deliberately wire-neutral: the digest is an internal affinity
/// identity only, and its bytes must remain stable across this optimization so
/// existing cohort affinity and cache behavior survive an upgrade.
fn stable_prefix_digest(request: &Value) -> String {
    let mut hasher = Sha256::new();
    write_stable_prefix_canonical_json(&mut hasher, request);
    format!("{:x}", hasher.finalize())
}

enum StablePrefixEntry<'a> {
    Direct(&'a Value),
    FilteredArray(Vec<&'a Value>),
}

fn stable_prefix_entries(request: &Value) -> Vec<(&'static str, StablePrefixEntry<'_>)> {
    let Some(object) = request.as_object() else {
        return Vec::new();
    };

    let mut entries = Vec::with_capacity(10);
    for key in [
        "instructions",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "text",
        "response_format",
        "system",
        "system_prompt",
    ] {
        if let Some(value) = object.get(key) {
            entries.push((key, StablePrefixEntry::Direct(value)));
        }
    }
    for key in ["messages", "input"] {
        let Some(items) = object.get(key).and_then(Value::as_array) else {
            continue;
        };
        let stable_items = items
            .iter()
            .filter(|item| {
                item.get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| matches!(role, "system" | "developer"))
            })
            .collect::<Vec<_>>();
        if !stable_items.is_empty() {
            entries.push((key, StablePrefixEntry::FilteredArray(stable_items)));
        }
    }
    entries.sort_by_key(|(key, _)| *key);
    entries
}

fn write_stable_prefix_canonical_json(hasher: &mut Sha256, request: &Value) {
    hasher.update(b"{");
    for (index, (key, entry)) in stable_prefix_entries(request).into_iter().enumerate() {
        if index > 0 {
            hasher.update(b",");
        }
        write_json_string(hasher, key);
        hasher.update(b":");
        match entry {
            StablePrefixEntry::Direct(value) => write_canonical_json_value(hasher, value),
            StablePrefixEntry::FilteredArray(values) => {
                hasher.update(b"[");
                for (index, value) in values.into_iter().enumerate() {
                    if index > 0 {
                        hasher.update(b",");
                    }
                    write_canonical_json_value(hasher, value);
                }
                hasher.update(b"]");
            }
        }
    }
    hasher.update(b"}");
}

fn write_canonical_json_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update(b"null"),
        Value::Bool(value) => hasher.update(value.to_string().as_bytes()),
        Value::Number(value) => hasher.update(value.to_string().as_bytes()),
        Value::String(value) => write_json_string(hasher, value),
        Value::Array(values) => {
            hasher.update(b"[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    hasher.update(b",");
                }
                write_canonical_json_value(hasher, value);
            }
            hasher.update(b"]");
        }
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            hasher.update(b"{");
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    hasher.update(b",");
                }
                write_json_string(hasher, key);
                hasher.update(b":");
                write_canonical_json_value(hasher, value);
            }
            hasher.update(b"}");
        }
    }
}

fn write_json_string(hasher: &mut Sha256, value: &str) {
    let mut writer = DigestWriter(hasher);
    serde_json::to_writer(&mut writer, value)
        .expect("serializing a JSON string into the affinity digest should not fail");
}

struct DigestWriter<'a>(&'a mut Sha256);

impl Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
fn stable_prefix_material(request: &Value) -> Value {
    let mut stable = Map::new();
    let Some(object) = request.as_object() else {
        return Value::Object(stable);
    };
    for key in [
        "instructions",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "text",
        "response_format",
        "system",
        "system_prompt",
    ] {
        if let Some(value) = object.get(key) {
            stable.insert(key.to_string(), value.clone());
        }
    }
    if let Some(messages) = object.get("messages").and_then(Value::as_array) {
        let stable_messages = messages
            .iter()
            .filter(|message| {
                message
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| matches!(role, "system" | "developer"))
            })
            .cloned()
            .collect::<Vec<_>>();
        if !stable_messages.is_empty() {
            stable.insert("messages".to_string(), Value::Array(stable_messages));
        }
    }
    if let Some(input) = object.get("input").and_then(Value::as_array) {
        let stable_input = input
            .iter()
            .filter(|item| {
                item.get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| matches!(role, "system" | "developer"))
            })
            .cloned()
            .collect::<Vec<_>>();
        if !stable_input.is_empty() {
            stable.insert("input".to_string(), Value::Array(stable_input));
        }
    }
    Value::Object(stable)
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

fn hash_text(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    format!("{:x}", digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Channel, ProviderConfig};
    use crate::proxy::cache_affinity::{self, ShadowAffinityStore};
    use chrono::Utc;
    use serde_json::json;

    fn context() -> (AppConfig, RouteDecision, SelectedProviderKey) {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace-a".to_string();
        let provider = ProviderConfig {
            id: "provider-a".to_string(),
            name: "Provider A".to_string(),
            base_url: "https://example.test/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            api_key_encrypted: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        (
            config,
            RouteDecision {
                provider,
                upstream_channel: Channel::Responses,
                model: "gpt-5.5".to_string(),
            },
            SelectedProviderKey {
                secret: "secret-value".to_string(),
                key_id: Some("key-record-a".to_string()),
            },
        )
    }

    #[test]
    fn stable_prefix_cohort_ignores_conversation_tail() {
        let (config, decision, key) = context();
        let first = json!({
            "thread_id": "thread-a",
            "instructions": "stable",
            "tools": [{"type":"function","name":"read_file"}],
            "input": [{"role":"user","content":"one"}]
        });
        let second = json!({
            "thread_id": "thread-b",
            "instructions": "stable",
            "tools": [{"type":"function","name":"read_file"}],
            "input": [{"role":"user","content":"two"}]
        });
        let left = derive(&config, &decision, &first, &first, Some("codex"), &key);
        let right = derive(&config, &decision, &second, &second, Some("codex"), &key);
        assert_eq!(left.cohort_id, right.cohort_id);
        assert_ne!(left.trusted_conversation_id, right.trusted_conversation_id);
    }

    #[test]
    fn stable_schema_changes_cohort_without_mutating_request() {
        let (config, decision, key) = context();
        let first = json!({
            "thread_id": "thread-a",
            "instructions": "stable",
            "tools": [{"type":"function","name":"read_file"}],
            "input": [{"role":"user","content":"one"}]
        });
        let original = first.clone();
        let second = json!({
            "thread_id": "thread-a",
            "instructions": "changed",
            "tools": [{"type":"function","name":"read_file"}],
            "input": [{"role":"user","content":"one"}]
        });
        let left = derive(&config, &decision, &first, &first, Some("codex"), &key);
        let right = derive(&config, &decision, &second, &second, Some("codex"), &key);
        assert_ne!(left.cohort_id, right.cohort_id);
        assert_eq!(first, original);
        assert!(!left.realm_id.contains("secret-value"));
        assert!(!left.realm_id.contains("key-record-a"));
    }

    #[test]
    fn streamed_stable_prefix_digest_matches_legacy_canonical_bytes() {
        let request = json!({
            "instructions": "line one\\nquoted \\\"text\\\"",
            "tools": [{
                "name": "write_file",
                "type": "function",
                "parameters": {"z": [3, {"b": true, "a": null}], "a": 1}
            }],
            "messages": [
                {"role": "user", "content": "tail that must not enter the stable prefix"},
                {"role": "developer", "content": [{"type": "input_text", "text": "developer prefix"}]},
                {"role": "system", "content": "system prefix"}
            ],
            "input": [
                {"role": "assistant", "content": "another tail"},
                {"role": "developer", "content": "input prefix"}
            ],
            "response_format": {"type": "json_schema", "json_schema": {"name": "x", "schema": {"b": 2, "a": 1}}}
        });

        let legacy = hash_text(&canonical_json_string(&stable_prefix_material(&request)));

        assert_eq!(stable_prefix_digest(&request), legacy);
    }

    #[test]
    fn prompt_cache_key_is_not_trusted_conversation_identity() {
        let (config, decision, key) = context();
        let request = json!({
            "prompt_cache_key": "client-key",
            "instructions": "stable",
            "input": [{"role":"user","content":"one"}]
        });
        let identity = derive(&config, &decision, &request, &request, Some("codex"), &key);
        assert!(identity.trusted_conversation_id.is_none());
        assert!(identity.trusted_identity_source.is_none());
    }

    #[test]
    fn realm_changes_for_selected_key_record() {
        let (config, decision, key) = context();
        let request = json!({"instructions":"stable"});
        let first = derive(&config, &decision, &request, &request, Some("codex"), &key);
        let second = derive(
            &config,
            &decision,
            &request,
            &request,
            Some("codex"),
            &SelectedProviderKey {
                secret: "other-secret".to_string(),
                key_id: Some("key-record-b".to_string()),
            },
        );
        assert_ne!(first.realm_id, second.realm_id);
    }

    #[test]
    #[ignore = "manual FastRelayCore large stable-prefix shadow-policy baseline"]
    fn fastrelay_shadow_policy_large_stable_prefix_baseline() {
        use std::{hint::black_box, time::Instant};

        for (target_bytes, p95_budget_us) in [(300_000usize, 5_000u128), (2_000_000, 20_000)] {
            let (config, decision, key) = context();
            let request = json!({
                "thread_id": format!("large-shadow-policy-{target_bytes}"),
                "instructions": "x".repeat(target_bytes),
                "tools": [{"type": "function", "name": "read_file"}],
                "input": [{"role": "user", "content": "new tail"}]
            });

            // Prime allocator and canonicalization paths before sampling.
            for _ in 0..3 {
                let identity = derive(&config, &decision, &request, &request, Some("codex"), &key);
                let mut store = ShadowAffinityStore::default();
                black_box(cache_affinity::compute_shadow_affinity(
                    &mut store,
                    &identity,
                    None,
                    Utc::now(),
                    0,
                    0,
                ));
            }

            let mut samples = Vec::new();
            for _ in 0..21 {
                let started = Instant::now();
                let identity = derive(&config, &decision, &request, &request, Some("codex"), &key);
                let mut store = ShadowAffinityStore::default();
                black_box(cache_affinity::compute_shadow_affinity(
                    &mut store,
                    &identity,
                    None,
                    Utc::now(),
                    0,
                    0,
                ));
                samples.push(started.elapsed().as_micros());
            }
            samples.sort_unstable();
            let p95_index = ((samples.len() - 1) * 95).div_ceil(100);
            let p95_us = samples[p95_index];
            println!(
                "fastrelay_shadow_policy body={target_bytes} p95_us={p95_us} samples_us={samples:?}"
            );
            assert!(
                p95_us <= p95_budget_us,
                "FastRelayCore shadow policy p95 ({p95_us}us) exceeded the {p95_budget_us}us budget for {target_bytes} bytes"
            );
        }
    }
}
