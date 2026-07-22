#[cfg(test)]
use crate::config::AppConfig;
use crate::config::{
    Channel, ProviderResponseSessionReuseCapability, ResponseSessionReuseStreamShape,
    SelectedProviderKey, RESPONSE_SESSION_REUSE_EVIDENCE_VERSION,
};
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest, Sha256};

use super::{action_scope::CompositeActionScope, affinity_identity, upstream_url, RouteDecision};

#[cfg(test)]
const CONTINUATION_SCOPE_VERSION: &str = "continuation-scope-v1";
#[cfg(test)]
const DELTA_ALGORITHM_VERSION: &str = "exact-lineage-v1";

// Keep this list closed: a field must have explicit Responses semantics and be
// safe for the delta builder to rewrite as a lineage member or carry forward
// unchanged. New or provider-only fields stay on FullReplay until verified.
const NATIVE_RESPONSES_VERIFIED_DELTA_TOP_LEVEL_FIELDS: &[&str] = &[
    "background",
    "client_metadata",
    "conversation_id",
    "include",
    "input",
    "instructions",
    "max_output_tokens",
    "metadata",
    "model",
    "parallel_tool_calls",
    "previous_response_id",
    "prompt_cache_breakpoint",
    "prompt_cache_key",
    "prompt_cache_options",
    "prompt_cache_retention",
    "reasoning",
    "response_format",
    "safety_identifier",
    "service_tier",
    "store",
    "stream",
    "session_id",
    "temperature",
    "text",
    "tool_choice",
    "tools",
    "top_p",
    "thread_id",
    "truncation",
    "user",
];

pub(super) fn native_responses_verified_delta_schema_is_known(request: &Value) -> bool {
    request.as_object().is_some_and(|object| {
        object
            .keys()
            .all(|key| NATIVE_RESPONSES_VERIFIED_DELTA_TOP_LEVEL_FIELDS.contains(&key.as_str()))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContinuationScope {
    pub anchor_key: String,
}

impl ContinuationScope {
    pub(super) fn from_action_scope(scope: &CompositeActionScope) -> Self {
        Self {
            anchor_key: scope.anchor_key.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn derive(
        config: &AppConfig,
        decision: &RouteDecision,
        client_request: &Value,
        agent_id: Option<&str>,
        selected_key: &SelectedProviderKey,
    ) -> Option<Self> {
        if !matches!(decision.upstream_channel, Channel::Responses) {
            return None;
        }
        let workspace = config.workspace_fingerprint.trim();
        if workspace.is_empty() {
            return None;
        }
        let agent_id = agent_id.map(str::trim).filter(|value| !value.is_empty())?;
        let identity = TrustedConversationIdentity::derive(client_request)?;
        let realm_id = affinity_identity::realm_id(decision, selected_key);
        let identity_material = identity.material();
        let evidence_version = RESPONSE_SESSION_REUSE_EVIDENCE_VERSION.to_string();
        let anchor_key = hash_parts(&[
            CONTINUATION_SCOPE_VERSION,
            DELTA_ALGORITHM_VERSION,
            &evidence_version,
            workspace,
            agent_id,
            &decision.provider.id,
            &realm_id,
            &identity_material,
        ]);

        Some(Self { anchor_key })
    }
}

pub(super) fn response_session_capability(
    decision: &RouteDecision,
    selected_key: &SelectedProviderKey,
    stream_shape: ResponseSessionReuseStreamShape,
) -> ProviderResponseSessionReuseCapability {
    ProviderResponseSessionReuseCapability {
        endpoint: normalized_endpoint(&upstream_url(
            &decision.provider.base_url,
            &Channel::Responses,
        )),
        channel: Channel::Responses,
        key_realm_id: affinity_identity::key_realm_id(selected_key),
        stream_shape,
        evidence_version: RESPONSE_SESSION_REUSE_EVIDENCE_VERSION,
    }
}

fn normalized_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    reqwest::Url::parse(trimmed)
        .map(|url| url.to_string().trim_end_matches('/').to_string())
        .unwrap_or_else(|_| trimmed.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
struct TrustedConversationIdentity {
    thread_id: Option<String>,
    conversation_id: Option<String>,
    session_id: Option<String>,
}

#[cfg(test)]
impl TrustedConversationIdentity {
    fn derive(request: &Value) -> Option<Self> {
        let identity = Self {
            thread_id: identity_value(request, "thread_id"),
            conversation_id: identity_value(request, "conversation_id"),
            session_id: identity_value(request, "session_id"),
        };
        (identity.thread_id.is_some()
            || identity.conversation_id.is_some()
            || identity.session_id.is_some())
        .then_some(identity)
    }

    fn material(&self) -> String {
        [
            ("thread", self.thread_id.as_deref()),
            ("conversation", self.conversation_id.as_deref()),
            ("session", self.session_id.as_deref()),
        ]
        .into_iter()
        .filter_map(|(kind, value)| value.map(|value| format!("{kind}\0{value}")))
        .collect::<Vec<_>>()
        .join("\0")
    }
}

#[cfg(test)]
fn identity_value(request: &Value, key: &str) -> Option<String> {
    request
        .get(key)
        .and_then(bounded_identity_value)
        .or_else(|| {
            ["metadata", "context", "client_context"]
                .into_iter()
                .find_map(|container| {
                    request
                        .get(container)
                        .and_then(|value| value.get(key))
                        .and_then(bounded_identity_value)
                })
        })
}

#[cfg(test)]
fn bounded_identity_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty() && value.len() <= 512).then(|| value.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
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

    fn context() -> (AppConfig, RouteDecision, SelectedProviderKey) {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace-a".to_string();
        let provider = ProviderConfig {
            id: "provider-a".to_string(),
            name: "Provider A".to_string(),
            base_url: "https://Example.test/V1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            api_key_encrypted: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
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
        (
            config,
            RouteDecision {
                provider,
                upstream_channel: Channel::Responses,
                model: "gpt-real".to_string(),
            },
            SelectedProviderKey {
                secret: "secret-a".to_string(),
                key_id: Some("key-a".to_string()),
            },
        )
    }

    #[test]
    fn scope_requires_authenticated_agent_and_explicit_conversation_identity() {
        let (config, decision, key) = context();
        let request = json!({"prompt_cache_key":"not-a-conversation"});

        assert!(
            ContinuationScope::derive(&config, &decision, &request, Some("codex"), &key).is_none()
        );
        assert!(ContinuationScope::derive(
            &config,
            &decision,
            &json!({"thread_id":"thread-a"}),
            None,
            &key
        )
        .is_none());
    }

    #[test]
    fn scope_binds_thread_and_session_together() {
        let (config, decision, key) = context();
        let first = ContinuationScope::derive(
            &config,
            &decision,
            &json!({"thread_id":"thread-a","session_id":"session-a"}),
            Some("codex"),
            &key,
        )
        .unwrap();
        let changed_session = ContinuationScope::derive(
            &config,
            &decision,
            &json!({"thread_id":"thread-a","session_id":"session-b"}),
            Some("codex"),
            &key,
        )
        .unwrap();

        assert_ne!(first.anchor_key, changed_session.anchor_key);
    }

    #[test]
    fn scope_changes_for_agent_endpoint_model_channel_or_key_material() {
        let (config, decision, key) = context();
        let request = json!({"thread_id":"thread-a","session_id":"session-a"});
        let baseline =
            ContinuationScope::derive(&config, &decision, &request, Some("codex"), &key).unwrap();

        let other_agent =
            ContinuationScope::derive(&config, &decision, &request, Some("zcode"), &key).unwrap();
        assert_ne!(baseline.anchor_key, other_agent.anchor_key);

        let mut other_endpoint = decision.clone();
        other_endpoint.provider.base_url = "https://example.test/v2".to_string();
        assert_ne!(
            baseline.anchor_key,
            ContinuationScope::derive(&config, &other_endpoint, &request, Some("codex"), &key,)
                .unwrap()
                .anchor_key
        );

        let mut other_model = decision.clone();
        other_model.model = "gpt-other".to_string();
        assert_ne!(
            baseline.anchor_key,
            ContinuationScope::derive(&config, &other_model, &request, Some("codex"), &key,)
                .unwrap()
                .anchor_key
        );

        let replaced_secret = SelectedProviderKey {
            secret: "secret-b".to_string(),
            key_id: key.key_id.clone(),
        };
        assert_ne!(
            baseline.anchor_key,
            ContinuationScope::derive(
                &config,
                &decision,
                &request,
                Some("codex"),
                &replaced_secret,
            )
            .unwrap()
            .anchor_key
        );

        let mut chat = decision.clone();
        chat.upstream_channel = Channel::Chat;
        assert!(ContinuationScope::derive(&config, &chat, &request, Some("codex"), &key).is_none());
    }

    #[test]
    fn native_responses_verified_delta_schema_rejects_vendor_extension() {
        let request = json!({
            "model": "gpt-5.5",
            "input": [{"role": "user", "content": "continue"}],
            "vendor_extension": {"enabled": true}
        });

        assert!(!native_responses_verified_delta_schema_is_known(&request));
    }

    #[test]
    fn native_responses_verified_delta_schema_accepts_common_complete_request() {
        let request = json!({
            "model": "gpt-5.5",
            "input": [{"role": "user", "content": "continue"}],
            "instructions": "Be concise.",
            "stream": true,
            "store": false,
            "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
            "tool_choice": "auto",
            "parallel_tool_calls": true,
            "reasoning": {"effort": "medium"},
            "max_output_tokens": 1024,
            "temperature": 0.2,
            "top_p": 0.9,
            "truncation": "auto",
            "metadata": {"tenant": "test"},
            "include": ["reasoning.encrypted_content"],
            "previous_response_id": "resp_previous",
            "prompt_cache_key": "cache-key",
            "prompt_cache_retention": "24h",
            "prompt_cache_options": {"mode": "implicit"},
            "prompt_cache_breakpoint": {"mode": "explicit"},
            "background": false,
            "safety_identifier": "safety-user",
            "service_tier": "auto",
            "text": {"format": {"type": "text"}},
            "response_format": {"type": "text"},
            "user": "user-123"
        });

        assert!(native_responses_verified_delta_schema_is_known(&request));
    }
}
