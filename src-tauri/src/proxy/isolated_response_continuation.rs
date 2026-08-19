//! A deliberately isolated, read-only probe for native Responses continuation.
//!
//! This module only describes the probe wire and sanitises its observation.  It
//! is not used by the normal generation path.  The caller must gate execution
//! behind the exact `ATOAPI_ISOLATED_TEST_INSTANCE=1` opt-in.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::sse::SseFrameDecoder;

pub(crate) const REQUEST_KIND: &str = "isolated-response-continuation-probe";
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ProbeInput {
    pub provider_id: String,
    pub model_id: String,
    #[serde(default)]
    pub key_id: Option<String>,
    #[serde(default)]
    pub key_realm_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProbeIdentity {
    pub provider_id: String,
    pub model_id: String,
    pub channel: &'static str,
    pub key_binding_kind: &'static str,
    pub key_binding_fingerprint: String,
    pub key_id_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id_fingerprint: Option<String>,
    pub key_realm_hash: String,
    pub endpoint_fingerprint: String,
    pub user_agent_fingerprint: String,
    pub transport_policy_fingerprint: String,
    /// Hashes the complete non-secret continuation lane: configured provider
    /// identity, resolved Responses endpoint, selected Key realm, effective
    /// User-Agent, and transport policy. A successful isolated probe must not
    /// be reused across a different cache lane.
    pub continuation_realm_hash: String,
    pub same_user_agent: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ResponseObservation {
    pub attempted: bool,
    pub http_status: Option<u16>,
    pub semantic_status: &'static str,
    /// A deliberately small, content-free rejection category. It lets the
    /// isolated verifier distinguish a rejected native continuation parameter
    /// from a generic upstream rejection without retaining or returning an
    /// upstream error body.
    pub rejection_category: &'static str,
    pub accepted: bool,
    pub previous_response_id_sent: bool,
    pub response_id_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id_fingerprint: Option<String>,
    pub sse: bool,
    pub usage_present: bool,
}

impl ResponseObservation {
    pub(crate) fn not_attempted(previous_response_id_sent: bool) -> Self {
        Self {
            attempted: false,
            http_status: None,
            semantic_status: "not_attempted",
            rejection_category: "not_attempted",
            accepted: false,
            previous_response_id_sent,
            response_id_present: false,
            response_id_fingerprint: None,
            sse: false,
            usage_present: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProbeResult {
    pub schema: &'static str,
    pub ok: bool,
    pub isolated: bool,
    pub live_18883_touched: bool,
    pub identity: ProbeIdentity,
    pub seed: ResponseObservation,
    pub continuation: ResponseObservation,
    pub upstream_request_count: u8,
    pub expected_upstream_request_count: u8,
    pub no_raw_content: bool,
}

/// The raw response id is intentionally kept out of the serialisable result.
/// It exists only transiently between the two probe requests.
#[derive(Debug)]
pub(crate) struct ParsedResponse {
    pub observation: ResponseObservation,
    pub response_id: Option<String>,
}

pub(crate) fn exact_isolated_flag(value: Option<&str>) -> bool {
    value == Some("1")
}

pub(crate) fn validate_input(input: &ProbeInput) -> Result<(), &'static str> {
    if input.provider_id.trim().is_empty() {
        return Err("provider_id is required");
    }
    if input.model_id.trim().is_empty() {
        return Err("model_id is required");
    }
    if input
        .key_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("key_id must not be blank");
    }
    if input
        .key_realm_hash
        .as_deref()
        .is_some_and(|value| !is_sha256_hex(value.trim()))
    {
        return Err("key_realm_hash must be a 64-character lowercase SHA-256 value");
    }
    Ok(())
}

pub(crate) fn classify_error(error: &anyhow::Error) -> &'static str {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("live key realm") || text.contains("key realm") {
        "key_realm_binding_mismatch"
    } else if text.contains("provider api key") || text.contains("provider key") {
        "key_selection_failed"
    } else if text.contains("seed request") {
        "seed_transport_failed"
    } else if text.contains("continuation request") {
        "continuation_transport_failed"
    } else if text.contains("provider was not found") || text.contains("provider") {
        "provider_scope_invalid"
    } else {
        "probe_internal_error"
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn user_agent_for_provider(configured: Option<&str>) -> String {
    // The probe must use precisely the User-Agent that reaches an ordinary
    // upstream request. Third-party cache lanes can partition on this header,
    // so a probe-only default cannot establish transferable capability
    // evidence. `effective_upstream_user_agent` also rejects an invalid custom
    // value exactly as the production wire does.
    super::effective_upstream_user_agent(configured)
        .to_str()
        .unwrap_or(crate::ATOAPI_USER_AGENT)
        .to_string()
}

pub(crate) fn seed_body(model: &str, nonce: &str) -> Value {
    serde_json::json!({
        "model": model,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": format!("Atoapi isolated continuation seed {nonce}. Reply only ACK.")
            }]
        }],
        "store": true,
        "stream": true,
        "max_output_tokens": 32
    })
}

pub(crate) fn continuation_body(model: &str, previous_response_id: &str, nonce: &str) -> Value {
    serde_json::json!({
        "model": model,
        "previous_response_id": previous_response_id,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": format!("Atoapi isolated continuation delta {nonce}. Reply only ACK.")
            }]
        }],
        "store": true,
        "stream": true,
        "max_output_tokens": 32
    })
}

pub(crate) fn fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn transport_policy_fingerprint(
    use_system_proxy: bool,
    request_body_gzip_enabled: bool,
    is_full_url: bool,
) -> String {
    fingerprint(&format!(
        "system-proxy={use_system_proxy}\0request-gzip={request_body_gzip_enabled}\0full-url={is_full_url}"
    ))
}

pub(crate) fn continuation_realm_hash(
    provider_id: &str,
    model_id: &str,
    key_realm_hash: &str,
    resolved_endpoint: &str,
    user_agent: &str,
    transport_policy_fingerprint: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"atoapi-isolated-response-continuation-realm-v1\0");
    for value in [
        provider_id,
        model_id,
        key_realm_hash,
        resolved_endpoint,
        user_agent,
        transport_policy_fingerprint,
    ] {
        hasher.update(value.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

pub(crate) fn inspect_response(
    status: u16,
    content_type: &str,
    bytes: &[u8],
    previous_response_id_sent: bool,
) -> ParsedResponse {
    let is_sse = content_type
        .to_ascii_lowercase()
        .contains("text/event-stream")
        || bytes.windows(5).any(|window| window == b"data:");
    let values = if is_sse {
        let mut decoder = SseFrameDecoder::default();
        let mut frames = decoder.push(bytes);
        frames.extend(decoder.finish());
        frames
            .into_iter()
            .filter_map(|frame| {
                let data = frame.data.trim();
                (!data.is_empty() && data != "[DONE]")
                    .then(|| serde_json::from_str::<Value>(data).ok())
                    .flatten()
            })
            .collect::<Vec<_>>()
    } else {
        serde_json::from_slice::<Value>(bytes)
            .ok()
            .into_iter()
            .collect::<Vec<_>>()
    };

    let response_id = values.iter().find_map(response_id_from_value);
    let usage_present = values.iter().any(value_has_usage);
    let semantic_failure = values.iter().any(value_is_failure);
    let http_success = (200..300).contains(&status);
    let accepted = http_success && !semantic_failure;
    let semantic_status = if accepted { "accepted" } else { "rejected" };
    let rejection_category = if accepted {
        "none"
    } else {
        rejection_category(&values, previous_response_id_sent)
    };

    ParsedResponse {
        observation: ResponseObservation {
            attempted: true,
            http_status: Some(status),
            semantic_status,
            rejection_category,
            accepted,
            previous_response_id_sent,
            response_id_present: response_id.is_some(),
            response_id_fingerprint: response_id.as_deref().map(fingerprint),
            sse: is_sse,
            usage_present,
        },
        response_id,
    }
}

fn rejection_category(values: &[Value], previous_response_id_sent: bool) -> &'static str {
    if previous_response_id_sent
        && values.iter().any(|value| {
            value_contains_case_insensitive(value, "previous_response_id")
                || value_contains_case_insensitive(value, "previous response id")
        })
    {
        "previous_response_id_rejected"
    } else {
        "request_rejected"
    }
}

fn value_contains_case_insensitive(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.to_ascii_lowercase().contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|entry| value_contains_case_insensitive(entry, needle)),
        Value::Object(values) => values.iter().any(|(key, entry)| {
            key.to_ascii_lowercase().contains(needle)
                || value_contains_case_insensitive(entry, needle)
        }),
        _ => false,
    }
}

fn response_id_from_value(value: &Value) -> Option<String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/response/id").and_then(Value::as_str))
        .filter(|id| !id.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn value_has_usage(value: &Value) -> bool {
    value.get("usage").is_some_and(is_non_null_object)
        || value
            .pointer("/response/usage")
            .is_some_and(is_non_null_object)
}

fn is_non_null_object(value: &Value) -> bool {
    value.is_object() && !value.as_object().is_some_and(Map::is_empty)
}

fn value_is_failure(value: &Value) -> bool {
    let type_failure = value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.ends_with(".failed") || kind.ends_with(".error"));
    let status_failure = value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "failed" | "error" | "cancelled"));
    let response_status_failure = value
        .pointer("/response/status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "failed" | "error" | "cancelled"));
    let error_value = value.get("error").is_some_and(|error| !error.is_null());
    let nested_error = value
        .pointer("/response/error")
        .is_some_and(|error| !error.is_null());
    type_failure || status_failure || response_status_failure || error_value || nested_error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_isolated_flag_is_fail_closed() {
        assert!(exact_isolated_flag(Some("1")));
        assert!(!exact_isolated_flag(Some("true")));
        assert!(!exact_isolated_flag(Some("1\n")));
        assert!(!exact_isolated_flag(None));
    }

    #[test]
    fn response_parser_extracts_sse_id_and_usage_without_returning_content() {
        let raw = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_secret_id\",\"usage\":{\"input_tokens\":3}}}\n\n",
            "data: [DONE]\n\n"
        );
        let parsed = inspect_response(200, "text/event-stream", raw.as_bytes(), true);
        assert!(parsed.observation.accepted);
        assert_eq!(parsed.observation.rejection_category, "none");
        assert!(parsed.observation.response_id_present);
        assert!(parsed.observation.usage_present);
        assert!(parsed.observation.sse);
        assert_eq!(parsed.response_id.as_deref(), Some("resp_secret_id"));
        let output = serde_json::to_string(&parsed.observation).unwrap();
        assert!(!output.contains("resp_secret_id"));
    }

    #[test]
    fn semantic_failure_rejects_http_200_response() {
        let raw = serde_json::json!({
            "type": "response.failed",
            "response": {
                "status": "failed",
                "error": {"message": "previous_response_id rejected: secret upstream body"}
            }
        });
        let parsed = inspect_response(200, "application/json", raw.to_string().as_bytes(), true);
        assert!(!parsed.observation.accepted);
        assert_eq!(parsed.observation.semantic_status, "rejected");
        assert_eq!(
            parsed.observation.rejection_category,
            "previous_response_id_rejected"
        );
        assert!(parsed.response_id.is_none());
        assert!(!serde_json::to_string(&parsed.observation)
            .unwrap()
            .contains("secret upstream body"));
    }

    #[test]
    fn generic_rejection_never_echoes_body_or_claims_previous_response_rejection() {
        let raw = serde_json::json!({
            "error": {"message": "raw upstream policy rejection"}
        });
        let parsed = inspect_response(400, "application/json", raw.to_string().as_bytes(), true);
        assert!(!parsed.observation.accepted);
        assert_eq!(parsed.observation.rejection_category, "request_rejected");
        assert!(!serde_json::to_string(&parsed.observation)
            .unwrap()
            .contains("raw upstream policy rejection"));
    }

    #[test]
    fn continuation_wire_is_responses_only_and_carries_one_delta() {
        let seed = seed_body("gpt-test", "nonce");
        let continuation = continuation_body("gpt-test", "resp_seed", "nonce");

        assert_eq!(seed.get("stream"), Some(&Value::Bool(true)));
        assert_eq!(seed.get("store"), Some(&Value::Bool(true)));
        assert!(seed.get("previous_response_id").is_none());
        assert!(seed.get("stream_options").is_none());
        assert_eq!(
            continuation
                .get("previous_response_id")
                .and_then(Value::as_str),
            Some("resp_seed")
        );
        assert_eq!(
            continuation
                .get("input")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert!(continuation.get("stream_options").is_none());
        assert!(continuation.get("prompt_cache_key").is_none());
        assert!(continuation.get("prompt_cache_retention").is_none());
    }

    #[test]
    fn probe_user_agent_matches_production_resolution() {
        assert_eq!(user_agent_for_provider(None), crate::ATOAPI_USER_AGENT);
        assert_eq!(
            user_agent_for_provider(Some("Atoapi-Test/1")),
            "Atoapi-Test/1"
        );
        assert_eq!(
            user_agent_for_provider(Some("invalid\nheader")),
            crate::ATOAPI_USER_AGENT
        );
    }

    #[test]
    fn continuation_realm_hash_changes_with_each_cache_lane_component() {
        let policy = transport_policy_fingerprint(false, true, false);
        let baseline = continuation_realm_hash(
            "provider-a",
            "gpt-test",
            &"a".repeat(64),
            "https://api.example/v1/responses",
            "Atoapi/1",
            &policy,
        );
        assert_eq!(baseline.len(), 64);
        assert_ne!(
            baseline,
            continuation_realm_hash(
                "provider-b",
                "gpt-test",
                &"a".repeat(64),
                "https://api.example/v1/responses",
                "Atoapi/1",
                &policy,
            )
        );
        assert_ne!(
            baseline,
            continuation_realm_hash(
                "provider-a",
                "gpt-test",
                &"a".repeat(64),
                "https://api.example/v1/responses",
                "Atoapi/2",
                &policy,
            )
        );
        assert_ne!(policy, transport_policy_fingerprint(true, true, false));
    }
}
