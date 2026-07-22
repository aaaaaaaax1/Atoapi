use serde_json::Value;

use crate::config::{Channel, ProviderConfig};

use super::{
    prepared_wire_request::PreparedResponseBody,
    request_plan::{OneShotRequestPlan, RequestPlan},
};

/// Binds the final semantic request body to the single frozen wire request.
///
/// This is intentionally created only after routing, the actual Key selection,
/// compatibility conversion, and every cache-field mutation are complete. It
/// owns both values so no later code can change the body without rebuilding
/// the wire request that will cross the one-shot transport seam.
#[must_use = "dispatch the envelope through its one-shot plan"]
pub(super) struct GenerationEnvelope {
    final_body: Value,
    request_plan: RequestPlan,
}

/// Settlement can inspect the exact frozen semantic body, while transport can
/// consume its one-shot plan exactly once. The body has no mutable accessor.
pub(super) struct FrozenGenerationDispatch {
    final_body: Value,
    request_plan: Option<OneShotRequestPlan>,
}

impl GenerationEnvelope {
    pub(super) fn freeze(
        provider: &ProviderConfig,
        url: impl Into<String>,
        channel: Channel,
        final_body: PreparedResponseBody,
    ) -> Self {
        let (final_body, wire) = final_body.into_prepared_wire(&channel);
        let request_plan = RequestPlan::from_prepared(provider, url, wire);
        Self {
            final_body,
            request_plan,
        }
    }

    pub(super) fn body(&self) -> &Value {
        &self.final_body
    }

    pub(super) fn request_plan(&self) -> &RequestPlan {
        &self.request_plan
    }

    pub(super) fn with_gzip_enabled(mut self, enabled: bool) -> Self {
        self.request_plan = self.request_plan.with_gzip_enabled(enabled);
        self
    }

    pub(super) fn with_explicit_proxy_url(mut self, proxy_url: Option<String>) -> Self {
        self.request_plan = self.request_plan.with_explicit_proxy_url(proxy_url);
        self
    }

    /// Preserves the frozen body for settlement and makes the one-shot plan
    /// available through a consuming accessor.
    pub(super) fn into_dispatch(self) -> FrozenGenerationDispatch {
        FrozenGenerationDispatch {
            final_body: self.final_body,
            request_plan: Some(self.request_plan.into_one_shot()),
        }
    }
}

impl FrozenGenerationDispatch {
    pub(super) fn body(&self) -> &Value {
        &self.final_body
    }

    pub(super) fn take_one_shot_plan(&mut self) -> OneShotRequestPlan {
        self.request_plan
            .take()
            .expect("a frozen generation plan can only be dispatched once")
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::config::ModelConfig;
    use crate::proxy::prepared_wire_request::{
        serialize_responses_body_bytes_for_provider_prefix, PreparedResponseBody,
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
    fn full_replay_freeze_preserves_every_final_body_field() {
        let final_body = json!({
            "model": "gpt-test",
            "instructions": "keep all fields",
            "input": [{
                "type": "function_call_output",
                "call_id": "call_123",
                "output": {"stdout": "你好", "stderr": "", "exit_code": 0}
            }],
            "reasoning": {"effort": "high", "summary": "auto"},
            "x_unknown_extension": {"phase": null, "error": {"code": "example"}}
        });
        let expected = serialize_responses_body_bytes_for_provider_prefix(&final_body);

        let envelope = GenerationEnvelope::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            PreparedResponseBody::plain(final_body.clone()),
        );

        assert_eq!(envelope.body(), &final_body);
        assert_eq!(
            envelope.request_plan().wire().body().as_ref(),
            expected.as_slice()
        );
        let parsed: Value = serde_json::from_slice(envelope.request_plan().wire().body()).unwrap();
        assert_eq!(parsed, final_body);
    }

    #[test]
    fn draft_freeze_keeps_final_body_and_freezes_the_updated_wire_once() {
        let initial = json!({
            "model": "gpt-test",
            "prompt_cache_key": "before",
            "input": [{"type": "message", "role": "user", "content": "long stable context"}],
            "stream": false
        });
        let mut final_body = initial.clone();
        final_body["prompt_cache_key"] = json!("after");
        final_body["stream"] = json!(true);
        final_body["x_unknown_extension"] = json!({"call_id": "call_unchanged"});
        let expected = serialize_responses_body_bytes_for_provider_prefix(&final_body);
        let mut prepared_body = PreparedResponseBody::responses(initial);
        prepared_body.set_root("prompt_cache_key", json!("after"));
        prepared_body.set_root("stream", json!(true));
        prepared_body.set_root("x_unknown_extension", json!({"call_id": "call_unchanged"}));

        let envelope = GenerationEnvelope::freeze(
            &provider(),
            "https://example.test/v1/responses",
            Channel::Responses,
            prepared_body,
        );

        assert_eq!(envelope.body(), &final_body);
        assert_eq!(
            envelope.request_plan().wire().body().as_ref(),
            expected.as_slice()
        );
        let parsed: Value = serde_json::from_slice(envelope.request_plan().wire().body()).unwrap();
        assert_eq!(parsed, final_body);

        let mut dispatch = envelope.with_gzip_enabled(true).into_dispatch();
        assert_eq!(dispatch.body(), &final_body);
        let plan = dispatch.take_one_shot_plan();
        assert!(plan.request_body_gzip_enabled());
        assert_eq!(plan.wire().body().as_ref(), expected.as_slice());
    }
}
