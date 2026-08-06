use serde_json::Value;

use crate::config::{Channel, ProviderConfig};

use super::{
    cache_control_core::{CacheControlPlan, FinalWireReceipt},
    generation_envelope::{FrozenGenerationDispatch, GenerationEnvelope},
    prepared_wire_request::PreparedResponseBody,
    request_plan::RequestPlan,
    upstream_affinity::UpstreamAffinityScope,
};

/// The only production seam allowed to turn a prepared Responses body into
/// bytes that may cross the upstream transport boundary.
///
/// Routing, Key selection, compatibility conversion, and cache-control
/// decisions must all have finished before this module is constructed. It
/// binds the frozen semantic body, its exact serialized wire, and the
/// redacted receipt derived from that same wire. Callers cannot seal a receipt
/// from one body and later dispatch bytes from another one.
#[must_use = "dispatch the frozen plan once or deliberately drop it"]
pub(super) struct FinalResponsesWirePlan {
    envelope: GenerationEnvelope,
    receipt: FinalWireReceipt,
}

impl FinalResponsesWirePlan {
    pub(super) fn freeze(
        provider: &ProviderConfig,
        url: impl Into<String>,
        channel: Channel,
        body: PreparedResponseBody,
        cache_control_plan: CacheControlPlan,
        explicit_proxy_url: Option<String>,
    ) -> Self {
        let envelope = GenerationEnvelope::freeze(provider, url, channel, body)
            .with_explicit_proxy_url(explicit_proxy_url);
        let receipt = cache_control_plan.seal(&envelope);
        Self { envelope, receipt }
    }

    pub(super) fn body(&self) -> &Value {
        self.envelope.body()
    }

    pub(super) fn request_plan(&self) -> &RequestPlan {
        self.envelope.request_plan()
    }

    pub(super) fn receipt(&self) -> &FinalWireReceipt {
        &self.receipt
    }

    pub(super) fn with_gzip_enabled(mut self, enabled: bool) -> Self {
        self.envelope = self.envelope.with_gzip_enabled(enabled);
        self
    }

    pub(super) fn with_upstream_affinity_scope(
        mut self,
        scope: Option<UpstreamAffinityScope>,
    ) -> Self {
        self.envelope = self.envelope.with_upstream_affinity_scope(scope);
        self
    }

    pub(super) fn into_dispatch(self) -> FrozenGenerationDispatch {
        self.envelope.into_dispatch()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::{
        config::ModelConfig,
        proxy::{
            action_scope::{ActionScopeInput, CompositeActionScope},
            cache_control_core::{CacheContextMode, CacheControlCore, CacheControlPlanInput},
            upstream_affinity::UpstreamAffinityScope,
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

    #[test]
    fn freezes_controls_receipt_and_one_shot_dispatch_from_one_final_body() {
        let scope = CompositeActionScope::derive(ActionScopeInput {
            workspace_fingerprint: "workspace-a",
            agent_id: Some("codex"),
            provider_id: "provider-a",
            endpoint: "https://example.test/v1/responses",
            resolved_model: "gpt-test",
            channel: &Channel::Responses,
            key_realm_id: "key-realm-a",
            thread_id: Some("thread-a"),
            conversation_id: None,
            session_id: None,
            adapter_attested: true,
            identity_source: "adapter-header",
        })
        .expect("attested test action scope");
        let cache_plan = CacheControlCore::plan(CacheControlPlanInput {
            action_scope: Some(&scope),
            active_channel: &Channel::Responses,
            context_mode: CacheContextMode::FullReplay,
            lineage_epoch: Some(7),
        });
        let mut body = PreparedResponseBody::responses_pending(json!({
            "model": "gpt-test",
            "input": [{"type":"message","role":"user","content":"stable"}],
            "stream": true
        }));
        assert!(body.set_root("prompt_cache_key", json!("stable-key")));
        assert!(body.set_root("prompt_cache_retention", json!("24h")));

        let plan = FinalResponsesWirePlan::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            body,
            cache_plan,
            None,
        );

        assert_eq!(plan.body()["prompt_cache_key"], "stable-key");
        assert_eq!(
            plan.receipt().wire.wire_bytes,
            plan.request_plan().body_len() as u64
        );
        assert_eq!(
            plan.receipt().wire.atoapi_mutated_static_categories,
            vec!["cache_control".to_string()],
            "the fixed diagnostic records Atoapi-owned cache-control roots without retaining values"
        );

        let mut dispatch = plan.with_gzip_enabled(true).into_dispatch();
        assert!(dispatch.take_one_shot_plan().request_body_gzip_enabled());
    }

    #[test]
    fn affinity_scope_crosses_the_frozen_plan_without_touching_the_responses_wire() {
        let body = PreparedResponseBody::responses_pending(json!({
            "model": "gpt-test",
            "input": [{"type":"message","role":"user","content":"stable"}],
            "stream": true
        }));
        let cache_plan = CacheControlCore::plan(CacheControlPlanInput {
            action_scope: None,
            active_channel: &Channel::Responses,
            context_mode: CacheContextMode::FullReplay,
            lineage_epoch: None,
        });
        let scope = UpstreamAffinityScope::for_test("opaque-trusted-session-scope");
        let plan = FinalResponsesWirePlan::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            body,
            cache_plan,
            None,
        )
        .with_upstream_affinity_scope(Some(scope));

        let expected_wire = plan.request_plan().wire().body().to_vec();
        let mut dispatch = plan.into_dispatch();
        let one_shot = dispatch.take_one_shot_plan();
        assert_eq!(one_shot.wire().body().as_ref(), expected_wire.as_slice());
        assert!(one_shot.upstream_affinity_scope().is_some());
        assert!(
            !format!("{one_shot:?}").contains("opaque-trusted-session-scope"),
            "opaque transport scope must not leak through Debug output"
        );
    }
}
