use crate::{
    config::{Channel, ProviderCacheCapabilityField},
    metrics::ResponsesWirePrefixFingerprints,
};

use super::{action_scope::CompositeActionScope, generation_envelope::GenerationEnvelope};

/// A deep, observe-only cache-control seam. It owns no mutable state, waits,
/// locks, persistence, or network client. Future phases may use its receipts
/// only after shadow evidence proves that doing so is safe.
pub(super) struct CacheControlCore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CacheContextMode {
    FullReplay,
    VerifiedNativeDelta,
    ExternalContinuation,
}

impl CacheContextMode {
    const fn label(self) -> &'static str {
        match self {
            Self::FullReplay => "full_replay",
            Self::VerifiedNativeDelta => "verified_native_delta",
            Self::ExternalContinuation => "external_continuation",
        }
    }
}

pub(super) struct CacheControlPlanInput<'a> {
    pub(super) action_scope: Option<&'a CompositeActionScope>,
    pub(super) active_channel: &'a Channel,
    pub(super) context_mode: CacheContextMode,
    pub(super) lineage_epoch: Option<u64>,
}

/// Immutable planning evidence created after actual routing and Key selection,
/// but before the final body is frozen.
#[derive(Debug, Clone)]
pub(super) struct CacheControlPlan {
    scope: ActionScopeReceipt,
    context_mode: CacheContextMode,
    lineage_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActionScopeReceipt {
    pub(super) version: u8,
    pub(super) scope_digest: Option<String>,
    pub(super) channel: String,
    pub(super) key_realm_digest: Option<String>,
    pub(super) trusted_identity_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SemanticLineageDigest {
    pub(super) version: u8,
    pub(super) context_mode: String,
    pub(super) lineage_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedWireDigest {
    pub(super) version: u8,
    pub(super) wire_bytes: u64,
    pub(super) canonical_member_count: u64,
    pub(super) responses_static_projection_digest: Option<String>,
    pub(super) outbound_prefix_fingerprints: Option<ResponsesWirePrefixFingerprints>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinalCacheControls {
    pub(super) present_field_mask: u8,
    pub(super) breakpoint_present: bool,
}

impl FinalCacheControls {
    pub(super) fn present_fields(&self) -> Vec<ProviderCacheCapabilityField> {
        [
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityField::PromptCacheRetention,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
        ]
        .into_iter()
        .enumerate()
        .filter_map(|(bit, field)| ((self.present_field_mask & (1 << bit)) != 0).then_some(field))
        .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinalWireReceipt {
    pub(super) scope: ActionScopeReceipt,
    pub(super) semantic: SemanticLineageDigest,
    pub(super) wire: PreparedWireDigest,
    pub(super) cache_controls: FinalCacheControls,
}

impl CacheControlCore {
    pub(super) fn plan(input: CacheControlPlanInput<'_>) -> CacheControlPlan {
        CacheControlPlan {
            scope: ActionScopeReceipt {
                version: 1,
                scope_digest: input.action_scope.map(|scope| scope.anchor_key.clone()),
                channel: input.active_channel.label().to_string(),
                key_realm_digest: input.action_scope.map(|scope| scope.key_realm_id.clone()),
                trusted_identity_source: input
                    .action_scope
                    .map(|scope| scope.identity_source.to_string()),
            },
            context_mode: input.context_mode,
            lineage_epoch: input.lineage_epoch,
        }
    }
}

impl CacheControlPlan {
    /// Generates only immutable, request-local evidence from the already
    /// frozen body and wire. No cache decision observes this receipt yet.
    pub(super) fn seal(self, envelope: &GenerationEnvelope) -> FinalWireReceipt {
        let plan = envelope.request_plan();
        let prepared = plan.wire();
        let body = envelope.body();
        // Breakpoint presence is captured from the exact frozen JSON bytes by
        // the prepared wire. This avoids a recursive Value walk over a large
        // Agent input before dispatch.
        let breakpoint_present = prepared.prompt_cache_breakpoint_present();
        let cache_controls = FinalCacheControls {
            present_field_mask: cache_control_mask(body, breakpoint_present),
            breakpoint_present,
        };
        // This receipt intentionally carries only the strict lineage tuple.
        // Exact request bytes are already owned immutably by GenerationEnvelope;
        // hashing the entire large input here added hot-path latency without a
        // production consumer or an additional safety guarantee.
        let semantic = SemanticLineageDigest {
            version: 2,
            context_mode: self.context_mode.label().to_string(),
            lineage_epoch: self.lineage_epoch,
        };
        let wire = PreparedWireDigest {
            version: 2,
            wire_bytes: prepared.len() as u64,
            canonical_member_count: body.as_object().map(|map| map.len() as u64).unwrap_or(0),
            responses_static_projection_digest: prepared
                .responses_static_projection_digest()
                .map(ToOwned::to_owned),
            outbound_prefix_fingerprints: prepared.outbound_prefix_fingerprints().cloned(),
        };

        FinalWireReceipt {
            scope: self.scope,
            semantic,
            wire,
            cache_controls,
        }
    }
}

fn cache_control_mask(body: &serde_json::Value, breakpoint_present: bool) -> u8 {
    let Some(object) = body.as_object() else {
        return 0;
    };
    let mut mask = 0;
    for (bit, key) in [
        (0, "prompt_cache_key"),
        (1, "prompt_cache_retention"),
        (2, "prompt_cache_options"),
    ] {
        if object.contains_key(key) {
            mask |= 1 << bit;
        }
    }
    if breakpoint_present {
        mask |= 1 << 3;
    }
    mask
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::{
        config::{ModelConfig, ProviderConfig},
        proxy::{
            action_scope::{ActionScopeInput, CompositeActionScope},
            generation_envelope::GenerationEnvelope,
            prepared_wire_request::PreparedResponseBody,
        },
    };

    fn provider() -> ProviderConfig {
        ProviderConfig {
            id: "provider-a".to_string(),
            name: "Provider A".to_string(),
            base_url: "https://example.test/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
                id: "gpt-test".to_string(),
                request_model_id: None,
                display_name: "gpt-test".to_string(),
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
        }
    }

    fn action_scope() -> CompositeActionScope {
        CompositeActionScope::derive(ActionScopeInput {
            workspace_fingerprint: "workspace-a",
            agent_id: Some("codex"),
            provider_id: "provider-a",
            endpoint: "https://example.test/v1/responses",
            resolved_model: "gpt-test",
            channel: &Channel::Responses,
            key_realm_id: "key-realm-a",
            thread_id: Some("thread-a"),
            conversation_id: None,
            session_id: Some("session-a"),
            adapter_attested: true,
            identity_source: "adapter-header",
        })
        .unwrap()
    }

    fn receipt(body: serde_json::Value, context_mode: CacheContextMode) -> FinalWireReceipt {
        let envelope = GenerationEnvelope::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            PreparedResponseBody::plain(body),
        );
        CacheControlCore::plan(CacheControlPlanInput {
            action_scope: Some(&action_scope()),
            active_channel: &Channel::Responses,
            context_mode,
            lineage_epoch: Some(3),
        })
        .seal(&envelope)
    }

    #[test]
    fn plan_and_seal_are_observe_only_and_frozen_wire_bound() {
        let body = json!({
            "model": "gpt-test",
            "input": [{"type": "function_call_output", "call_id": "call-a", "output": {"stderr": "error", "stdout": "你好"}}],
            "x_unknown": {"nullable": null}
        });
        let first = receipt(body.clone(), CacheContextMode::FullReplay);
        let second = receipt(body, CacheContextMode::FullReplay);

        assert_eq!(first, second);
        assert_eq!(first.scope.channel, "responses");
        assert_eq!(first.semantic.context_mode, "full_replay");
        assert_eq!(first.semantic.version, 2);
        assert_eq!(first.wire.version, 2);
        assert_eq!(first.wire.wire_bytes > 0, true);
        assert_eq!(first.cache_controls.present_field_mask, 0);
    }

    #[test]
    fn cache_controls_split_static_evidence_without_hashing_the_large_input() {
        let base = json!({"model":"gpt-test","input":[{"type":"message","content":"stable"}]});
        let mut cache_changed = base.clone();
        cache_changed["prompt_cache_key"] = json!("placement-b");
        let mut semantic_changed = base.clone();
        semantic_changed["input"][0]["call_id"] = json!("call-b");

        let baseline = receipt(base, CacheContextMode::FullReplay);
        let cache_receipt = receipt(cache_changed, CacheContextMode::FullReplay);
        let semantic_receipt = receipt(semantic_changed, CacheContextMode::FullReplay);

        assert_ne!(
            baseline.cache_controls.present_field_mask,
            cache_receipt.cache_controls.present_field_mask
        );
        assert_ne!(
            baseline.wire.responses_static_projection_digest,
            cache_receipt.wire.responses_static_projection_digest
        );
        assert_eq!(baseline.semantic, semantic_receipt.semantic);
        assert_eq!(
            baseline.wire.responses_static_projection_digest,
            semantic_receipt.wire.responses_static_projection_digest,
            "input bytes are frozen by GenerationEnvelope but are not rehashed on the send path"
        );
        assert_ne!(baseline.wire.wire_bytes, semantic_receipt.wire.wire_bytes);
    }

    #[test]
    fn frozen_cache_control_receipt_reuses_exact_wire_breakpoint_presence() {
        let nested = receipt(
            json!({
                "model":"gpt-test",
                "input":[{
                    "type":"message",
                    "content":[{
                        "type":"input_text",
                        "text":"stable",
                        "prompt_cache_breakpoint":{"mode":"explicit"}
                    }]
                }]
            }),
            CacheContextMode::FullReplay,
        );

        assert!(nested.cache_controls.breakpoint_present);
        assert_eq!(nested.cache_controls.present_field_mask, 1 << 3);
        assert_eq!(
            nested.cache_controls.present_fields(),
            vec![ProviderCacheCapabilityField::PromptCacheBreakpoint]
        );
    }

    #[test]
    fn final_receipt_keeps_static_projection_stable_across_appended_tail() {
        let first = json!({
            "model":"gpt-test",
            "instructions":"stable instructions",
            "tools":[{"type":"function","name":"read_file"}],
            "input":[{"type":"message","role":"user","content":"anchor"}],
            "stream":true
        });
        let appended = json!({
            "model":"gpt-test",
            "instructions":"stable instructions",
            "tools":[{"type":"function","name":"read_file"}],
            "input":[
                {"type":"message","role":"user","content":"anchor"},
                {"type":"message","role":"user","content":"continue"}
            ],
            "stream":true
        });

        let first = receipt(first, CacheContextMode::FullReplay);
        let appended = receipt(appended, CacheContextMode::FullReplay);
        assert_eq!(
            first.wire.responses_static_projection_digest,
            appended.wire.responses_static_projection_digest
        );
        assert_ne!(first.wire.wire_bytes, appended.wire.wire_bytes);
        assert_eq!(first.semantic, appended.semantic);
    }

    #[test]
    fn receipt_detects_draft_and_context_mode_changes_without_reencoding_the_full_body() {
        let initial = json!({
            "model": "gpt-test",
            "input": [{"type": "message", "content": "stable"}],
            "stream": false
        });
        let mut prepared_body = PreparedResponseBody::responses(initial);
        prepared_body.set_root("stream", json!(true));
        let envelope = GenerationEnvelope::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            prepared_body,
        );
        let full_replay = CacheControlCore::plan(CacheControlPlanInput {
            action_scope: Some(&action_scope()),
            active_channel: &Channel::Responses,
            context_mode: CacheContextMode::FullReplay,
            lineage_epoch: Some(1),
        })
        .seal(&envelope);
        let delta = CacheControlCore::plan(CacheControlPlanInput {
            action_scope: Some(&action_scope()),
            active_channel: &Channel::Responses,
            context_mode: CacheContextMode::VerifiedNativeDelta,
            lineage_epoch: Some(1),
        })
        .seal(&envelope);

        assert_eq!(full_replay.wire, delta.wire);
        assert_ne!(full_replay.semantic, delta.semantic);
    }
}
