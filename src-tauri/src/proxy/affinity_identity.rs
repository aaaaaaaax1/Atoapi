use crate::{
    config::{AppConfig, SelectedProviderKey},
    proxy::json_canonical::canonical_json_string,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

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
    let stable_material = stable_prefix_material(upstream_request);
    let stable_prefix_digest = hash_text(&canonical_json_string(&stable_material));
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
}
