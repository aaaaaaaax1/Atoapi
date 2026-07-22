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
    #[cfg(test)]
    pub fn derive(
        config: &AppConfig,
        decision: &RouteDecision,
        client_request: &Value,
        upstream_request: &Value,
    ) -> Option<Self> {
        Self::derive_for_agent(config, decision, client_request, upstream_request, None)
    }

    pub fn derive_for_agent(
        config: &AppConfig,
        decision: &RouteDecision,
        client_request: &Value,
        upstream_request: &Value,
        agent_id: Option<&str>,
    ) -> Option<Self> {
        if !matches!(decision.upstream_channel, Channel::Responses) {
            return None;
        }

        let explicit = explicit_identity(client_request);
        let (identity_material, source) = identity_material_or_fallback(explicit.as_ref(), || {
            fallback_anchor_material(upstream_request)
        });
        // A trusted composite identity must constrain every scope family. Content
        // anchoring remains the fallback only when the request has no such identity.
        let scope_material = explicit
            .as_ref()
            .map(|identity| format!("{}\0{}", identity.source, identity.material))
            .unwrap_or_else(|| fallback_scope_material(upstream_request));
        // Upstream placement and local prefix-control keys are cache hints,
        // never continuation credentials. Keep their legacy primary
        // conversation identity so a stricter v1.3.5 scope (which deliberately
        // binds every supplied dimension) does not cold-start an otherwise
        // unchanged upstream cache or reset its local waterline on upgrade.
        // `anchor_key` and `scope_key` above remain the authoritative, full
        // composite isolation keys.
        let cache_control_material = legacy_primary_provider_cache_identity(client_request)
            .unwrap_or_else(|| identity_material.clone());
        let model = provider_prefix_model_key(decision);
        let provider_group = provider_prefix_provider_group(decision);
        let agent_scope = agent_identity_scope(agent_id);

        Some(Self {
            anchor_key: hash_parts(&[
                "session-anchor-v2",
                &config.workspace_fingerprint,
                &agent_scope,
                &decision.provider.id,
                &model,
                &identity_material,
            ]),
            scope_key: hash_parts(&[
                "session-scope-v2",
                &config.workspace_fingerprint,
                &agent_scope,
                &decision.provider.id,
                &model,
                &scope_material,
            ]),
            provider_cache_key: hash_parts(&[
                "provider-cache-v2",
                &config.workspace_fingerprint,
                &agent_scope,
                &provider_group,
                &model,
                &cache_control_material,
            ]),
            control_fingerprint: hash_parts(&[
                "prefix-control-v2",
                &config.workspace_fingerprint,
                &agent_scope,
                &decision.provider.id,
                &model,
                &cache_control_material,
            ]),
            source,
        })
    }
}

#[derive(Debug, Clone)]
struct ExplicitIdentity {
    source: &'static str,
    material: String,
}

fn identity_material_or_fallback(
    explicit: Option<&ExplicitIdentity>,
    fallback: impl FnOnce() -> String,
) -> (String, &'static str) {
    match explicit {
        Some(identity) => (
            format!("{}\0{}", identity.source, identity.material),
            identity.source,
        ),
        None => (fallback(), "content-anchor"),
    }
}

fn explicit_identity(request: &Value) -> Option<ExplicitIdentity> {
    const KEYS: [(&str, &str); 3] = [
        ("thread-id", "thread_id"),
        ("conversation-id", "conversation_id"),
        ("session-id", "session_id"),
    ];

    // Identity dimensions can be split across request envelopes. Collect every
    // allowed location in a deterministic order instead of allowing the first
    // non-empty container to hide the rest.
    let containers = [
        ("direct-composite", Some(request)),
        ("metadata-composite", request.get("metadata")),
        ("context-composite", request.get("context")),
        ("client-context-composite", request.get("client_context")),
    ];
    let mut values_by_dimension = [Vec::new(), Vec::new(), Vec::new()];
    let mut sources = Vec::new();

    for (source, value) in containers {
        let Some(object) = value.and_then(Value::as_object) else {
            continue;
        };
        let mut contributed = false;
        for (index, (_, key)) in KEYS.iter().enumerate() {
            if let Some(value) = object.get(*key).and_then(non_empty_identity_value) {
                values_by_dimension[index].push(value);
                contributed = true;
            }
        }
        if contributed {
            sources.push(source);
        }
    }

    let mut material = Vec::new();
    let mut has_conflict = false;
    for ((kind, _), values) in KEYS.into_iter().zip(values_by_dimension) {
        let mut values = values;
        values.sort();
        values.dedup();
        match values.as_slice() {
            [] => {}
            [value] => material.push(format!("{kind}\0{value}")),
            values => {
                has_conflict = true;
                material.extend(
                    values
                        .iter()
                        .map(|value| format!("conflict:{kind}\0{value}")),
                );
            }
        }
    }

    let source = if has_conflict {
        "conflicted-composite"
    } else {
        match sources.as_slice() {
            [source] => *source,
            _ => "multi-container-composite",
        }
    };
    (!material.is_empty()).then(|| ExplicitIdentity {
        source,
        material: material.join("\0"),
    })
}

/// Retains the primary identity selection used by the released v1.3.4 cache
/// key. It intentionally excludes `prompt_cache_key`: client placement hints
/// must never become a conversation identity. This compatibility material is
/// used only to choose the provider cache shard; strict session isolation uses
/// the composite identity above.
fn legacy_primary_provider_cache_identity(request: &Value) -> Option<String> {
    const KEYS: [(&str, &str); 3] = [
        ("thread-id", "thread_id"),
        ("conversation-id", "conversation_id"),
        ("session-id", "session_id"),
    ];
    for object in [
        Some(request),
        request.get("metadata"),
        request.get("context"),
        request.get("client_context"),
    ] {
        let Some(object) = object.and_then(Value::as_object) else {
            continue;
        };
        for (kind, key) in KEYS {
            if let Some(value) = object.get(key).and_then(non_empty_identity_value) {
                return Some(format!("{kind}\0{value}"));
            }
        }
    }
    None
}

fn agent_identity_scope(agent_id: Option<&str>) -> String {
    agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("agent:{value}"))
        .unwrap_or_else(|| "agent:default".to_string())
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
        assert_eq!(main_identity.source, "direct-composite");
    }

    #[test]
    fn explicit_identity_does_not_evaluate_fallback_anchor() {
        let explicit = ExplicitIdentity {
            source: "direct-composite",
            material: "thread-id\0thread-1".to_string(),
        };

        let (material, source) = identity_material_or_fallback(Some(&explicit), || {
            panic!("explicit identities must not build fallback anchor material")
        });

        assert_eq!(material, "direct-composite\0thread-id\0thread-1");
        assert_eq!(source, "direct-composite");
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
    fn explicit_thread_identity_is_also_scoped_by_agent() {
        let (config, decision) = context();
        let request = json!({ "thread_id": "same-thread", "input": ["same"] });

        let codex = SessionIdentity::derive_for_agent(
            &config,
            &decision,
            &request,
            &request,
            Some("codex"),
        )
        .unwrap();
        let zcode = SessionIdentity::derive_for_agent(
            &config,
            &decision,
            &request,
            &request,
            Some("zcode"),
        )
        .unwrap();

        assert_ne!(codex.anchor_key, zcode.anchor_key);
        assert_ne!(codex.scope_key, zcode.scope_key);
        assert_ne!(codex.provider_cache_key, zcode.provider_cache_key);
    }

    #[test]
    fn explicit_identity_binds_all_present_conversation_dimensions() {
        let (config, decision) = context();
        let base = json!({
            "thread_id": "thread-a",
            "conversation_id": "conversation-a",
            "session_id": "session-a",
            "input": ["same"]
        });
        let baseline = SessionIdentity::derive(&config, &decision, &base, &base).unwrap();
        for (field, value) in [
            ("thread_id", "thread-b"),
            ("conversation_id", "conversation-b"),
            ("session_id", "session-b"),
        ] {
            let mut changed_body = base.clone();
            changed_body[field] = json!(value);
            let changed =
                SessionIdentity::derive(&config, &decision, &changed_body, &changed_body).unwrap();

            assert_ne!(baseline.anchor_key, changed.anchor_key, "{field}");
            assert_ne!(baseline.scope_key, changed.scope_key, "{field}");
            if field == "thread_id" {
                assert_ne!(
                    baseline.provider_cache_key, changed.provider_cache_key,
                    "{field}"
                );
            } else {
                assert_eq!(
                    baseline.provider_cache_key, changed.provider_cache_key,
                    "provider cache placement follows the stable primary thread"
                );
                assert_eq!(
                    baseline.control_fingerprint, changed.control_fingerprint,
                    "prefix control follows the stable primary thread"
                );
            }
            if field == "thread_id" {
                assert_ne!(
                    baseline.control_fingerprint, changed.control_fingerprint,
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn explicit_identity_merges_conversation_dimensions_across_allowed_containers() {
        let (config, decision) = context();
        let base = json!({
            "thread_id": "thread-a",
            "metadata": {
                "conversation_id": "conversation-a",
                "session_id": "session-a"
            },
            "input": ["same"]
        });
        let mut changed = base.clone();
        changed["metadata"]["session_id"] = json!("session-b");

        let base = SessionIdentity::derive(&config, &decision, &base, &base).unwrap();
        let changed = SessionIdentity::derive(&config, &decision, &changed, &changed).unwrap();

        assert_eq!(base.source, "multi-container-composite");
        assert_ne!(base.anchor_key, changed.anchor_key);
        assert_ne!(base.scope_key, changed.scope_key);
        assert_eq!(base.provider_cache_key, changed.provider_cache_key);
        assert_eq!(base.control_fingerprint, changed.control_fingerprint);
    }

    #[test]
    fn conflicting_explicit_identity_is_not_an_unambiguous_conversation_scope() {
        let (config, decision) = context();
        let left = json!({
            "thread_id": "thread-a",
            "metadata": { "thread_id": "thread-b", "session_id": "session-a" },
            "input": ["same"]
        });
        let right = json!({
            "thread_id": "thread-a",
            "metadata": { "thread_id": "thread-c", "session_id": "session-a" },
            "input": ["same"]
        });

        let left = SessionIdentity::derive(&config, &decision, &left, &left).unwrap();
        let right = SessionIdentity::derive(&config, &decision, &right, &right).unwrap();

        assert_eq!(left.source, "conflicted-composite");
        assert!(!super::super::session_identity_source_is_trusted(
            left.source
        ));
        assert_ne!(left.anchor_key, right.anchor_key);
        assert_ne!(left.scope_key, right.scope_key);
        assert_eq!(left.provider_cache_key, right.provider_cache_key);
        assert_eq!(left.control_fingerprint, right.control_fingerprint);
    }

    #[test]
    fn provider_cache_key_preserves_v1_3_4_primary_thread_identity() {
        let (config, decision) = context();
        let request = json!({
            "thread_id": "thread-a",
            "metadata": {
                "thread_id": "thread-a",
                "session_id": "session-a"
            },
            "input": ["same"]
        });
        let identity = SessionIdentity::derive_for_agent(
            &config,
            &decision,
            &request,
            &request,
            Some("codex"),
        )
        .unwrap();
        let expected = hash_parts(&[
            "provider-cache-v2",
            &config.workspace_fingerprint,
            "agent:codex",
            &provider_prefix_provider_group(&decision),
            &provider_prefix_model_key(&decision),
            "thread-id\0thread-a",
        ]);

        assert_eq!(identity.provider_cache_key, expected);
        let expected_control = hash_parts(&[
            "prefix-control-v2",
            &config.workspace_fingerprint,
            "agent:codex",
            &decision.provider.id,
            &provider_prefix_model_key(&decision),
            "thread-id\0thread-a",
        ]);
        assert_eq!(identity.control_fingerprint, expected_control);
        assert_eq!(identity.source, "multi-container-composite");
    }

    #[test]
    fn provider_cache_key_stays_compatible_for_all_normal_v1_3_4_identity_locations() {
        let (config, decision) = context();
        let fixtures = [
            json!({
                "thread_id": "direct-thread",
                "metadata": { "session_id": "direct-session" },
                "input": ["same"]
            }),
            json!({
                "metadata": {
                    "thread_id": "metadata-thread",
                    "session_id": "metadata-session"
                },
                "input": ["same"]
            }),
            json!({
                "context": { "conversation_id": "context-conversation" },
                "input": ["same"]
            }),
            json!({
                "client_context": { "session_id": "client-session" },
                "input": ["same"]
            }),
        ];

        for request in fixtures {
            let current = SessionIdentity::derive_for_agent(
                &config,
                &decision,
                &request,
                &request,
                Some("codex"),
            )
            .unwrap();
            assert_eq!(
                current.provider_cache_key,
                v1_3_4_provider_cache_key_for_test(&config, &decision, &request, Some("codex")),
                "normal conversation identities must retain the cache key produced by v1.3.4"
            );
            assert_eq!(
                current.control_fingerprint,
                v1_3_4_prefix_control_fingerprint_for_test(
                    &config,
                    &decision,
                    &request,
                    Some("codex"),
                ),
                "normal conversation identities must retain the prefix control key produced by v1.3.4"
            );
        }
    }

    fn v1_3_4_provider_cache_key_for_test(
        config: &AppConfig,
        decision: &RouteDecision,
        request: &Value,
        agent_id: Option<&str>,
    ) -> String {
        let identity_material = v1_3_4_primary_identity_material_for_test(request)
            .unwrap_or_else(|| fallback_anchor_material(request));
        let model = provider_prefix_model_key(decision);
        let provider_group = provider_prefix_provider_group(decision);
        let agent_scope = agent_identity_scope(agent_id);
        hash_parts(&[
            "provider-cache-v2",
            &config.workspace_fingerprint,
            &agent_scope,
            &provider_group,
            &model,
            &identity_material,
        ])
    }

    fn v1_3_4_primary_identity_material_for_test(request: &Value) -> Option<String> {
        const KEYS: [(&str, &str); 3] = [
            ("thread-id", "thread_id"),
            ("conversation-id", "conversation_id"),
            ("session-id", "session_id"),
        ];
        [
            Some(request),
            request.get("metadata"),
            request.get("context"),
            request.get("client_context"),
        ]
        .into_iter()
        .find_map(|candidate| {
            let object = candidate.and_then(Value::as_object)?;
            KEYS.into_iter().find_map(|(kind, key)| {
                object
                    .get(key)
                    .and_then(non_empty_identity_value)
                    .map(|value| format!("{kind}\0{value}"))
            })
        })
    }

    fn v1_3_4_prefix_control_fingerprint_for_test(
        config: &AppConfig,
        decision: &RouteDecision,
        request: &Value,
        agent_id: Option<&str>,
    ) -> String {
        let identity_material = v1_3_4_primary_identity_material_for_test(request)
            .unwrap_or_else(|| fallback_anchor_material(request));
        let model = provider_prefix_model_key(decision);
        let agent_scope = agent_identity_scope(agent_id);
        hash_parts(&[
            "prefix-control-v2",
            &config.workspace_fingerprint,
            &agent_scope,
            &decision.provider.id,
            &model,
            &identity_material,
        ])
    }

    #[test]
    fn prompt_cache_key_is_a_placement_hint_not_a_session_identity() {
        let (config, decision) = context();
        let left = json!({
            "prompt_cache_key": "placement-a",
            "instructions": "stable",
            "input": [{"role": "user", "content": "same"}]
        });
        let mut right = left.clone();
        right["prompt_cache_key"] = json!("placement-b");

        let left = SessionIdentity::derive(&config, &decision, &left, &left).unwrap();
        let right = SessionIdentity::derive(&config, &decision, &right, &right).unwrap();

        assert_eq!(left.source, "content-anchor");
        assert_eq!(left.anchor_key, right.anchor_key);
        assert_eq!(left.scope_key, right.scope_key);
        assert_eq!(left.provider_cache_key, right.provider_cache_key);
        assert_eq!(left.control_fingerprint, right.control_fingerprint);
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
