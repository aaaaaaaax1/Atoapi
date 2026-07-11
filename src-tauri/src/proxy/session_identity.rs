use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{AppConfig, Channel};

use super::{
    canonicalize_object_keys, canonicalize_responses_instruction_shape, provider_prefix_model_key,
    provider_prefix_provider_group, serialize_responses_body_for_provider_prefix,
    stabilize_responses_provider_prefix, strip_provider_cache_key_fields,
    strip_responses_dynamic_provider_cache_tail, trim_response_session_input_to_anchor,
    RouteDecision,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionIdentity {
    pub anchor_key: String,
    pub scope_key: String,
    pub provider_cache_key: String,
    pub control_fingerprint: String,
    pub source: &'static str,
}

impl SessionIdentity {
    pub fn derive(
        config: &AppConfig,
        decision: &RouteDecision,
        client_request: &Value,
        upstream_request: &Value,
    ) -> Option<Self> {
        if !matches!(decision.upstream_channel, Channel::Responses) {
            return None;
        }

        let explicit = explicit_identity(client_request);
        let anchor_material = fallback_anchor_material(upstream_request);
        let scope_material = fallback_scope_material(upstream_request);
        let identity_material = explicit
            .as_ref()
            .map(|identity| format!("{}\0{}", identity.kind, identity.value))
            .unwrap_or_else(|| anchor_material.clone());
        let source = explicit
            .as_ref()
            .map(|identity| identity.kind)
            .unwrap_or("content-anchor");
        let model = provider_prefix_model_key(decision);
        let provider_group = provider_prefix_provider_group(decision);

        Some(Self {
            anchor_key: hash_parts(&[
                "session-anchor-v2",
                &config.workspace_fingerprint,
                &decision.provider.id,
                &model,
                &identity_material,
            ]),
            scope_key: hash_parts(&[
                "session-scope-v2",
                &config.workspace_fingerprint,
                &decision.provider.id,
                &model,
                &scope_material,
            ]),
            provider_cache_key: hash_parts(&[
                "provider-cache-v2",
                &config.workspace_fingerprint,
                &provider_group,
                &model,
                &identity_material,
            ]),
            control_fingerprint: hash_parts(&[
                "prefix-control-v2",
                &config.workspace_fingerprint,
                &decision.provider.id,
                &model,
                &identity_material,
            ]),
            source,
        })
    }
}

#[derive(Debug, Clone)]
struct ExplicitIdentity {
    kind: &'static str,
    value: String,
}

fn explicit_identity(request: &Value) -> Option<ExplicitIdentity> {
    const KEYS: [(&str, &str); 4] = [
        ("thread-id", "thread_id"),
        ("conversation-id", "conversation_id"),
        ("session-id", "session_id"),
        ("client-prompt-cache-key", "prompt_cache_key"),
    ];

    for (kind, key) in KEYS {
        if let Some(value) = request.get(key).and_then(non_empty_identity_value) {
            return Some(ExplicitIdentity { kind, value });
        }
    }
    for container in ["metadata", "context", "client_context"] {
        let Some(object) = request.get(container) else {
            continue;
        };
        for (kind, key) in KEYS {
            if let Some(value) = object.get(key).and_then(non_empty_identity_value) {
                return Some(ExplicitIdentity { kind, value });
            }
        }
    }
    None
}

fn non_empty_identity_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty() && value.len() <= 512).then(|| value.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn fallback_anchor_material(request: &Value) -> String {
    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_dynamic_invocation_fields(&mut material);
    trim_response_session_input_to_anchor(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.session_identity_anchor");
    serialize_responses_body_for_provider_prefix(&material)
}

fn fallback_scope_material(request: &Value) -> String {
    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_dynamic_invocation_fields(&mut material);
    strip_responses_dynamic_provider_cache_tail(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.session_identity_scope");
    serialize_responses_body_for_provider_prefix(&material)
}

pub(super) fn strip_dynamic_invocation_fields(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for key in [
        "model",
        "reasoning",
        "reasoning_effort",
        "text",
        "response_format",
        "temperature",
        "top_p",
        "max_output_tokens",
        "max_tokens",
        "include",
        "stream",
        "store",
        "service_tier",
        "truncation",
        "previous_response_id",
        "metadata",
        "user",
    ] {
        object.remove(key);
    }
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ModelConfig, ProviderConfig};
    use chrono::Utc;
    use serde_json::json;

    fn context() -> (AppConfig, RouteDecision) {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "agent-codex-upstream".to_string(),
            name: "upstream / Codex".to_string(),
            base_url: "https://example.test/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            api_key_encrypted: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: true,
            use_system_proxy: false,
            models: vec![ModelConfig {
                id: "gpt-real".to_string(),
                request_model_id: None,
                display_name: "gpt-real".to_string(),
                context_window: None,
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
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-real".to_string(),
        };
        (config, decision)
    }

    #[test]
    fn explicit_thread_identity_survives_reasoning_and_internal_role_changes() {
        let (config, decision) = context();
        let main = json!({
            "thread_id": "thread-1",
            "model": "gpt-real",
            "reasoning": { "effort": "max" },
            "input": [{ "role": "user", "content": "same history" }]
        });
        let review = json!({
            "thread_id": "thread-1",
            "model": "codex-auto-review",
            "reasoning": { "effort": "low" },
            "max_output_tokens": 64,
            "input": [{ "role": "user", "content": "same history" }]
        });

        let main_identity = SessionIdentity::derive(&config, &decision, &main, &main).unwrap();
        let review_identity =
            SessionIdentity::derive(&config, &decision, &review, &review).unwrap();

        assert_eq!(main_identity.anchor_key, review_identity.anchor_key);
        assert_eq!(
            main_identity.provider_cache_key,
            review_identity.provider_cache_key
        );
        assert_eq!(
            main_identity.control_fingerprint,
            review_identity.control_fingerprint
        );
        assert_eq!(main_identity.source, "thread-id");
    }

    #[test]
    fn explicit_thread_identity_keeps_different_threads_isolated() {
        let (config, decision) = context();
        let left = json!({ "thread_id": "left", "input": ["same"] });
        let right = json!({ "thread_id": "right", "input": ["same"] });

        let left = SessionIdentity::derive(&config, &decision, &left, &left).unwrap();
        let right = SessionIdentity::derive(&config, &decision, &right, &right).unwrap();

        assert_ne!(left.anchor_key, right.anchor_key);
        assert_ne!(left.provider_cache_key, right.provider_cache_key);
    }

    #[test]
    fn fallback_identity_ignores_dynamic_reasoning_fields() {
        let (config, decision) = context();
        let main = json!({
            "instructions": "stable",
            "reasoning": { "effort": "max" },
            "input": [{ "role": "user", "content": "anchor" }, { "role": "user", "content": "tail" }]
        });
        let review = json!({
            "instructions": "stable",
            "reasoning": { "effort": "low" },
            "max_output_tokens": 64,
            "input": [{ "role": "user", "content": "anchor" }, { "role": "user", "content": "other tail" }]
        });

        let main = SessionIdentity::derive(&config, &decision, &main, &main).unwrap();
        let review = SessionIdentity::derive(&config, &decision, &review, &review).unwrap();

        assert_eq!(main.anchor_key, review.anchor_key);
        assert_eq!(main.provider_cache_key, review.provider_cache_key);
        assert_eq!(main.source, "content-anchor");
    }

    #[test]
    fn codex_main_and_auto_review_share_identity_without_explicit_thread_id() {
        let (config, decision) = context();
        let main = json!({
            "model": "gpt-real",
            "instructions": "stable Codex instructions",
            "tools": [{ "type": "function", "name": "read_file" }],
            "reasoning": { "effort": "max" },
            "input": [
                { "type": "message", "role": "user", "content": "inspect the current project" },
                { "type": "function_call_output", "call_id": "call-main", "output": "main result" }
            ]
        });
        let auto_review = json!({
            "model": "codex-auto-review",
            "instructions": "stable Codex instructions",
            "tools": [{ "type": "function", "name": "read_file" }],
            "reasoning": { "effort": "low" },
            "max_output_tokens": 128,
            "metadata": { "invocation": "auto-review" },
            "input": [
                { "type": "message", "role": "user", "content": "inspect the current project" },
                { "type": "function_call_output", "call_id": "call-review", "output": "review result" }
            ]
        });

        let main = SessionIdentity::derive(&config, &decision, &main, &main).unwrap();
        let auto_review =
            SessionIdentity::derive(&config, &decision, &auto_review, &auto_review).unwrap();

        assert_eq!(main.anchor_key, auto_review.anchor_key);
        assert_eq!(main.scope_key, auto_review.scope_key);
        assert_eq!(main.provider_cache_key, auto_review.provider_cache_key);
        assert_eq!(main.control_fingerprint, auto_review.control_fingerprint);
    }

    #[test]
    fn fallback_identity_is_stable_for_appended_turns_and_isolates_other_chats() {
        let (config, decision) = context();
        let first = json!({
            "instructions": "stable",
            "input": [
                { "type": "message", "role": "user", "content": "project alpha anchor" }
            ]
        });
        let appended = json!({
            "instructions": "stable",
            "previous_response_id": "resp-old",
            "input": [
                { "type": "message", "role": "user", "content": "project alpha anchor" },
                { "type": "message", "role": "assistant", "content": "done" },
                { "type": "message", "role": "user", "content": "continue" }
            ]
        });
        let other_chat = json!({
            "instructions": "stable",
            "input": [
                { "type": "message", "role": "user", "content": "project beta anchor" }
            ]
        });

        let first = SessionIdentity::derive(&config, &decision, &first, &first).unwrap();
        let appended = SessionIdentity::derive(&config, &decision, &appended, &appended).unwrap();
        let other_chat =
            SessionIdentity::derive(&config, &decision, &other_chat, &other_chat).unwrap();

        assert_eq!(first.anchor_key, appended.anchor_key);
        assert_eq!(first.provider_cache_key, appended.provider_cache_key);
        assert_ne!(first.anchor_key, other_chat.anchor_key);
        assert_ne!(first.provider_cache_key, other_chat.provider_cache_key);
    }
}
