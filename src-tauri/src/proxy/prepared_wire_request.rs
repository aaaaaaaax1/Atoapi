use bytes::Bytes;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::{
    ops::Range,
    sync::{Arc, OnceLock},
    time::Instant,
};

use crate::{
    config::{Channel, PromptCacheOptionsTtl},
    metrics::ResponsesWirePrefixFingerprints,
    state::{PromptCacheOptionsSiblingProof, PromptCacheOptionsSiblingVariant},
};

use super::{cache_capability, maybe_responses_wire_prefix_fingerprints, request_body_stream_flag};

pub(super) const RESPONSES_WIRE_ORDERED_KEYS: [&str; 23] = [
    "model",
    "prompt_cache_key",
    "prompt_cache_retention",
    "prompt_cache_options",
    "instructions",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "input",
    "reasoning",
    "text",
    "response_format",
    "temperature",
    "top_p",
    "max_output_tokens",
    "include",
    "stream",
    "store",
    "service_tier",
    "truncation",
    "previous_response_id",
    "metadata",
    "user",
];

/// Evidence sealed from the exact final request bytes.  A cache controller
/// must never upgrade a marker merely because an earlier mutable JSON value
/// happened to contain a similarly named member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ProtocolBreakpointProvenance {
    /// The frozen wire has no protocol-level cache breakpoint.
    Absent,
    /// The frozen Responses wire has one exact, legal Atoapi marker.
    AtoapiInjected { placement_digest: String },
    /// A marker exists but it is foreign, duplicated, malformed, or cannot
    /// be proven to be a single legal Atoapi placement.
    AmbiguousOrForeign,
}

impl ProtocolBreakpointProvenance {
    pub(super) const fn is_present(&self) -> bool {
        !matches!(self, Self::Absent)
    }

    pub(super) const fn is_atoapi_injected(&self) -> bool {
        matches!(self, Self::AtoapiInjected { .. })
    }

    pub(super) fn placement_digest(&self) -> Option<&str> {
        match self {
            Self::AtoapiInjected { placement_digest } => Some(placement_digest),
            Self::Absent | Self::AmbiguousOrForeign => None,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct PreparedWireMember {
    key: String,
    range: Range<usize>,
}

/// Retains the first canonical Responses encoding so late, explicitly tracked
/// mutations only re-encode the members that actually changed.
///
/// The draft owns encoded bytes, not cloned JSON values. In particular, a
/// large `input` is serialized once and then copied from its byte range when a
/// cache metadata field changes later in request preparation.
#[cfg(test)]
#[derive(Debug)]
struct PreparedWireDraft {
    body: Bytes,
    members: Vec<PreparedWireMember>,
    responses_static_projection_digest: Option<String>,
    encode_ms: u64,
}

#[cfg(test)]
impl PreparedWireDraft {
    fn from_responses_value(body: &Value) -> Option<Self> {
        let map = body.as_object()?;
        let encode_started = Instant::now();
        let mut output = Vec::new();
        let mut members = Vec::with_capacity(map.len());
        let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
        output.push(b'{');
        let mut first = true;
        for_each_responses_wire_member(map, |key, value| {
            let range = write_draft_json_member(&mut output, &mut first, key, value);
            if let Some(projection) = static_projection.as_mut() {
                projection.observe_member(key, &output[range.clone()]);
            }
            members.push(PreparedWireMember {
                key: key.to_string(),
                range,
            });
        });
        output.push(b'}');
        let responses_static_projection_digest =
            static_projection.and_then(ResponsesStaticProjectionHasher::finish);

        Some(Self {
            body: Bytes::from(output),
            members,
            responses_static_projection_digest,
            encode_ms: encode_started.elapsed().as_millis() as u64,
        })
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.body.len()
    }

    #[cfg(test)]
    pub(super) fn body_ptr(&self) -> *const u8 {
        self.body.as_ptr()
    }

    fn freeze_tracked(
        self,
        final_body: &Value,
        changed_fields: &[String],
        atoapi_injected_protocol_breakpoint: bool,
    ) -> PreparedWireRequest {
        let Some(map) = final_body.as_object() else {
            return PreparedWireRequest::from_value(&Channel::Responses, final_body);
        };
        let same_keys = map.len() == self.members.len()
            && self
                .members
                .iter()
                .all(|member| map.contains_key(&member.key));
        if changed_fields.is_empty() && same_keys {
            return PreparedWireRequest::from_encoded(
                &Channel::Responses,
                final_body,
                self.body,
                self.responses_static_projection_digest,
                atoapi_injected_protocol_breakpoint,
                self.encode_ms,
            );
        }

        let freeze_started = Instant::now();
        let mut output = Vec::with_capacity(self.body.len());
        let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
        output.push(b'{');
        let mut first = true;
        for_each_responses_wire_member(map, |key, value| {
            let changed = changed_fields.iter().any(|field| field == key);
            let retained = (!changed).then(|| {
                self.members
                    .iter()
                    .find(|member| member.key == key)
                    .map(|member| &self.body[member.range.clone()])
            });
            let range = if let Some(Some(bytes)) = retained {
                write_prepared_member_bytes(&mut output, &mut first, bytes)
            } else {
                write_draft_json_member(&mut output, &mut first, key, value)
            };
            if let Some(projection) = static_projection.as_mut() {
                projection.observe_member(key, &output[range]);
            }
        });
        output.push(b'}');
        let responses_static_projection_digest =
            static_projection.and_then(ResponsesStaticProjectionHasher::finish);

        PreparedWireRequest::from_encoded(
            &Channel::Responses,
            final_body,
            Bytes::from(output),
            responses_static_projection_digest,
            atoapi_injected_protocol_breakpoint,
            self.encode_ms
                .saturating_add(freeze_started.elapsed().as_millis() as u64),
        )
    }
}

/// Owns a final request body together with the optional canonical Responses
/// draft it was derived from. Late mutations are intentionally limited to
/// named top-level roots. Anything that cannot be expressed that way marks the
/// body for a safe full freeze instead of risking stale wire bytes.
#[must_use]
#[derive(Debug)]
pub(super) struct PreparedResponseBody {
    body: Value,
    #[cfg(test)]
    wire_draft: Option<PreparedWireDraft>,
    changed_fields: Vec<String>,
    requires_full_freeze: bool,
    // This is an in-memory witness, not a wire field. It is set only by the
    // cache-control mutator after it inserts the marker, and is invalidated by
    // every later input mutation before the body is frozen.
    atoapi_injected_protocol_breakpoint: bool,
}

impl PreparedResponseBody {
    /// Own an unencoded semantic body. Freezing serializes it exactly once.
    ///
    /// This constructor is used when no retained Responses encoding exists,
    /// for example after a compatibility channel conversion.
    pub(super) fn plain(body: Value) -> Self {
        Self {
            body,
            #[cfg(test)]
            wire_draft: None,
            changed_fields: Vec::new(),
            requires_full_freeze: false,
            atoapi_injected_protocol_breakpoint: false,
        }
    }

    /// Own a Responses body and its first canonical encoding as one value.
    ///
    /// The draft is derived only after this method has taken ownership of the
    /// semantic body. Callers can therefore never attach bytes encoded from a
    /// different body, while late root mutations can still retain a large
    /// unchanged `input` member without cloning or re-serializing it.
    #[cfg(test)]
    pub(super) fn responses(mut body: Value) -> Self {
        // A native Responses request is otherwise intentionally passed through
        // unchanged. Canonicalize only protocol-static JSON objects so equal
        // tool/schema/control payloads cannot split an upstream prefix merely
        // because the caller constructed nested maps in a different key order.
        // `input`, arrays, strings, and unknown extension roots stay untouched.
        canonicalize_responses_static_roots(&mut body);
        let wire_draft = PreparedWireDraft::from_responses_value(&body);
        Self {
            body,
            wire_draft,
            changed_fields: Vec::new(),
            requires_full_freeze: false,
            atoapi_injected_protocol_breakpoint: false,
        }
    }

    /// Own a native Responses body while its final cache-control shape is
    /// still being decided. Static roots are canonicalized immediately, but
    /// no draft bytes are retained; the first wire is therefore serialized
    /// only after every ordinary cache-control mutation has settled.
    pub(super) fn responses_pending(mut body: Value) -> Self {
        canonicalize_responses_static_roots(&mut body);
        Self::plain(body)
    }

    pub(super) fn body(&self) -> &Value {
        &self.body
    }

    /// Size of the retained canonical Responses encoding, when the body was
    /// an object and could be drafted.
    #[cfg(test)]
    pub(super) fn initial_wire_len(&self) -> Option<usize> {
        self.wire_draft.as_ref().map(PreparedWireDraft::len)
    }

    #[cfg(test)]
    pub(super) fn initial_wire_ptr(&self) -> Option<*const u8> {
        self.wire_draft.as_ref().map(PreparedWireDraft::body_ptr)
    }

    pub(super) fn set_root(&mut self, key: &str, mut value: Value) -> bool {
        let Some(object) = self.body.as_object_mut() else {
            self.requires_full_freeze = true;
            return false;
        };
        canonicalize_responses_static_root(key, &mut value);
        if object.get(key) == Some(&value) {
            return false;
        }
        object.insert(key.to_string(), value);
        self.mark_changed_root(key);
        true
    }

    pub(super) fn remove_root(&mut self, key: &str) -> bool {
        let Some(object) = self.body.as_object_mut() else {
            self.requires_full_freeze = true;
            return false;
        };
        let changed = object.remove(key).is_some();
        if changed {
            self.mark_changed_root(key);
        }
        changed
    }

    /// Gives a mutation access to exactly one existing root. The caller must
    /// report whether that root changed; it cannot modify sibling roots.
    pub(super) fn mutate_root_if<R>(
        &mut self,
        key: &str,
        mutation: impl FnOnce(&mut Value) -> R,
        changed: impl FnOnce(&R) -> bool,
    ) -> Option<R> {
        let value = self.body.as_object_mut()?.get_mut(key)?;
        let result = mutation(value);
        if changed(&result) {
            self.mark_changed_root(key);
        }
        Some(result)
    }

    /// Whether a root still has the exact semantic value owned when this
    /// prepared body was created. This is used to carry an already-computed
    /// predecessor proof through late top-level mutations without rescanning a
    /// large Agent input. Unknown whole-body mutation always fails closed.
    pub(super) fn preserves_initial_root(&self, key: &str) -> bool {
        !self.requires_full_freeze && !self.changed_fields.iter().any(|field| field == key)
    }

    /// Generic whole-body mutation is deliberately a full-freeze escape hatch.
    /// It exists for compatibility paths that have not been reduced to named
    /// roots yet, and must never retain draft member bytes.
    #[allow(dead_code)] // Compatibility escape hatch; tests cover its full-freeze contract.
    pub(super) fn mutate_unknown<R>(&mut self, mutation: impl FnOnce(&mut Value) -> R) -> R {
        self.requires_full_freeze = true;
        self.atoapi_injected_protocol_breakpoint = false;
        mutation(&mut self.body)
    }

    /// Records a marker inserted by Atoapi itself. The witness is carried to
    /// the immutable wire and never serializes into the user request.
    pub(super) fn mark_atoapi_protocol_breakpoint_injected(&mut self) {
        self.atoapi_injected_protocol_breakpoint = true;
    }

    pub(super) fn into_prepared_wire(self, channel: &Channel) -> (Value, PreparedWireRequest) {
        #[cfg(test)]
        {
            let Self {
                body,
                wire_draft,
                changed_fields,
                requires_full_freeze,
                atoapi_injected_protocol_breakpoint,
            } = self;
            let mut wire = match (channel, wire_draft, requires_full_freeze) {
                (Channel::Responses, Some(draft), false) => draft.freeze_tracked(
                    &body,
                    &changed_fields,
                    atoapi_injected_protocol_breakpoint,
                ),
                _ => PreparedWireRequest::from_value_with_protocol_breakpoint_witness(
                    channel,
                    &body,
                    atoapi_injected_protocol_breakpoint,
                ),
            };
            // The categories are an in-memory, fixed-vocabulary diagnostic.
            // They never alter the final body; retaining them for pending
            // production bodies makes an empty category list meaningful.
            wire.set_atoapi_mutated_roots(&changed_fields);
            (body, wire)
        }

        #[cfg(not(test))]
        {
            let Self {
                body,
                changed_fields,
                requires_full_freeze: _,
                atoapi_injected_protocol_breakpoint,
            } = self;
            let mut wire = PreparedWireRequest::from_value_with_protocol_breakpoint_witness(
                channel,
                &body,
                atoapi_injected_protocol_breakpoint,
            );
            // Production uses `responses_pending`, so the final frozen wire
            // must carry the same redacted mutation categories as the test
            // draft path. Only fixed category labels are retained.
            wire.set_atoapi_mutated_roots(&changed_fields);
            (body, wire)
        }
    }

    #[cfg(test)]
    pub(super) fn requires_full_freeze(&self) -> bool {
        self.requires_full_freeze
    }

    fn mark_changed_root(&mut self, key: &str) {
        if key == "input" {
            self.atoapi_injected_protocol_breakpoint = false;
        }
        if !self.changed_fields.iter().any(|field| field == key) {
            self.changed_fields.push(key.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn unknown_mutation_forces_a_full_freeze_instead_of_reusing_stale_members() {
        let initial = json!({
            "model": "gpt-test",
            "input": [{
                "type": "function_call_output",
                "call_id": "call-a",
                "output": {"stdout": "ok", "stderr": "before", "exit_code": 0}
            }],
            "x_future_extension": {"revision": 1},
            "stream": true
        });
        reset_draft_member_encodings();
        let mut body = PreparedResponseBody::responses(initial);
        body.mutate_unknown(|value| {
            value["input"][0]["output"]["stderr"] = json!("after");
            value["x_future_extension"]["revision"] = json!(2);
        });

        assert!(body.requires_full_freeze());
        let (final_body, wire) = body.into_prepared_wire(&Channel::Responses);
        let parsed: Value = serde_json::from_slice(wire.body()).unwrap();

        assert_eq!(parsed, final_body);
        assert_eq!(
            wire.body().as_ref(),
            serialize_responses_body_bytes_for_provider_prefix(&final_body).as_slice()
        );
        assert_eq!(draft_member_encoding_count("input"), 1);
    }

    #[test]
    fn predecessor_root_preservation_fails_closed_only_when_input_can_change() {
        let initial = json!({
            "model": "gpt-test",
            "input": [{"type":"message","role":"user","content":"stable"}],
            "stream": true
        });
        let mut body = PreparedResponseBody::responses(initial.clone());
        assert!(body.preserves_initial_root("input"));

        body.set_root("stream", json!(false));
        assert!(body.preserves_initial_root("input"));

        body.set_root(
            "input",
            json!([{"type":"message","role":"user","content":"changed"}]),
        );
        assert!(!body.preserves_initial_root("input"));

        let mut unknown = PreparedResponseBody::plain(initial);
        unknown.mutate_unknown(|value| value["model"] = json!("gpt-other"));
        assert!(!unknown.preserves_initial_root("input"));
    }

    #[test]
    fn final_static_projection_is_stable_for_appended_input_and_splits_static_wire_changes() {
        let baseline = json!({
            "model": "gpt-test",
            "prompt_cache_key": "cache-a",
            "instructions": "stable instructions",
            "tools": [{"type":"function","name":"read_file"}],
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "reasoning": {"effort":"max"},
            "stream": true
        });
        let mut appended = baseline.clone();
        appended["input"] = json!([
            {"type":"message","role":"user","content":"anchor"},
            {"type":"message","role":"user","content":"next"}
        ]);

        let baseline_wire = PreparedWireRequest::from_value(&Channel::Responses, &baseline);
        let appended_wire = PreparedWireRequest::from_value(&Channel::Responses, &appended);
        assert_eq!(
            baseline_wire.responses_static_projection_digest(),
            appended_wire.responses_static_projection_digest()
        );
        assert_ne!(baseline_wire.body(), appended_wire.body());

        for (field, value) in [
            ("model", json!("gpt-other")),
            ("prompt_cache_key", json!("cache-b")),
            ("instructions", json!("changed instructions")),
            ("tools", json!([{"type":"function","name":"write_file"}])),
        ] {
            let mut changed = baseline.clone();
            changed[field] = value;
            let changed_wire = PreparedWireRequest::from_value(&Channel::Responses, &changed);
            assert_ne!(
                baseline_wire.responses_static_projection_digest(),
                changed_wire.responses_static_projection_digest(),
                "{field} must split the final static wire projection"
            );
        }
    }

    #[test]
    fn final_static_projection_covers_post_input_and_unknown_members_exactly() {
        let baseline = json!({
            "model": "gpt-test",
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "reasoning": {"effort":"max"},
            "text": {"format":{"type":"text"}},
            "response_format": {"type":"json_object"},
            "temperature": 0.2,
            "top_p": 0.9,
            "max_output_tokens": 1024,
            "include": ["reasoning.encrypted_content"],
            "stream": true,
            "store": false,
            "service_tier": "default",
            "truncation": "disabled",
            "previous_response_id": "resp-a",
            "metadata": {"tenant":"a"},
            "user": "user-a",
            "x_future_extension": {"revision":1}
        });
        let baseline_wire = PreparedWireRequest::from_value(&Channel::Responses, &baseline);
        assert_eq!(
            baseline_wire.body().as_ref(),
            serialize_responses_body_bytes_for_provider_prefix(&baseline).as_slice(),
            "collecting the static projection must not change canonical wire bytes"
        );
        let baseline_digest = baseline_wire
            .responses_static_projection_digest()
            .expect("array input should produce a static projection");

        for (field, value) in [
            ("reasoning", json!({"effort":"high"})),
            ("text", json!({"format":{"type":"json_schema"}})),
            ("response_format", json!({"type":"text"})),
            ("temperature", json!(0.4)),
            ("top_p", json!(0.8)),
            ("max_output_tokens", json!(2048)),
            ("include", json!([])),
            ("stream", json!(false)),
            ("store", json!(true)),
            ("service_tier", json!("priority")),
            ("truncation", json!("auto")),
            ("previous_response_id", json!("resp-b")),
            ("metadata", json!({"tenant":"b"})),
            ("user", json!("user-b")),
            ("x_future_extension", json!({"revision":2})),
        ] {
            let mut changed = baseline.clone();
            changed[field] = value;
            let changed_wire = PreparedWireRequest::from_value(&Channel::Responses, &changed);
            assert_ne!(
                Some(baseline_digest),
                changed_wire.responses_static_projection_digest(),
                "{field} must split the final static wire projection"
            );
        }

        let mut projection_value = baseline.clone();
        projection_value.as_object_mut().unwrap().remove("input");
        let projection_bytes =
            serialize_responses_body_bytes_for_provider_prefix(&projection_value);
        let mut expected = Sha256::new();
        expected.update(b"responses-static-wire-projection-v2\0");
        expected.update(projection_bytes);
        assert_eq!(baseline_digest, format!("{:x}", expected.finalize()));
    }

    #[test]
    fn cache_maturity_projection_ignores_delivery_metadata_but_keeps_prompt_semantics_strict() {
        let baseline = json!({
            "model": "gpt-test",
            "prompt_cache_key": "cache-a",
            "instructions": "stable instructions",
            "tools": [{"type":"function","name":"read_file"}],
            "reasoning": {"effort":"high"},
            "truncation": "auto",
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "stream": true,
            "store": false,
            "metadata": {"attempt":"one"},
            "user": "caller-a",
            "previous_response_id": "resp-a"
        });
        let baseline_wire = PreparedWireRequest::from_value(&Channel::Responses, &baseline);
        let full_wire = baseline_wire
            .responses_static_projection_digest()
            .expect("array input has an exact wire projection");
        let maturity_wire = baseline_wire
            .responses_cache_maturity_static_projection_digest()
            .expect("array input has a cache-maturity projection");

        for (field, value) in [
            ("stream", json!(false)),
            ("store", json!(true)),
            ("metadata", json!({"attempt":"two"})),
            ("user", json!("caller-b")),
            ("previous_response_id", json!("resp-b")),
        ] {
            let mut changed = baseline.clone();
            changed[field] = value;
            let changed_wire = PreparedWireRequest::from_value(&Channel::Responses, &changed);
            assert_ne!(
                changed_wire.responses_static_projection_digest(),
                Some(full_wire),
                "{field} remains part of the exact final wire"
            );
            assert_eq!(
                changed_wire.responses_cache_maturity_static_projection_digest(),
                Some(maturity_wire),
                "{field} is delivery metadata and must not split a stable prefix maturity receipt"
            );
        }

        for (field, value) in [
            ("model", json!("gpt-other")),
            ("prompt_cache_key", json!("cache-b")),
            ("instructions", json!("changed instructions")),
            ("tools", json!([{"type":"function","name":"write_file"}])),
            ("reasoning", json!({"effort":"max"})),
            ("truncation", json!("disabled")),
        ] {
            let mut changed = baseline.clone();
            changed[field] = value;
            let changed_wire = PreparedWireRequest::from_value(&Channel::Responses, &changed);
            assert_ne!(
                changed_wire.responses_cache_maturity_static_projection_digest(),
                Some(maturity_wire),
                "{field} changes prompt semantics and must keep its own maturity scope"
            );
        }
    }

    #[test]
    fn native_responses_static_roots_are_canonical_without_touching_input() {
        let input = json!([{
            "type": "message",
            "role": "user",
            "opaque_json": "{\"second\":2,\"first\":1}",
            "content": [{"type":"input_text","text":"caller-owned input"}]
        }, {
            "type": "function_call_output",
            "call_id": "call_input_order",
            "output": "caller-owned tool output"
        }]);
        let left = json!({
            "model": "gpt-test",
            "tools": [{
                "type": "function",
                "name": "read_file",
                "parameters": {
                    "type": "object",
                    "required": ["path"],
                    "properties": {"path": {"type":"string", "description":"path"}}
                }
            }],
            "reasoning": {"summary":"auto", "effort":"high"},
            "metadata": {"z":"last", "a":"first"},
            "input": input.clone(),
            "stream": true
        });
        let right = json!({
            "model": "gpt-test",
            "tools": [{
                "name": "read_file",
                "parameters": {
                    "properties": {"path": {"description":"path", "type":"string"}},
                    "required": ["path"],
                    "type": "object"
                },
                "type": "function"
            }],
            "reasoning": {"effort":"high", "summary":"auto"},
            "metadata": {"a":"first", "z":"last"},
            "input": input.clone(),
            "stream": true
        });

        let (left_body, left_wire) =
            PreparedResponseBody::responses(left.clone()).into_prepared_wire(&Channel::Responses);
        let (_right_body, right_wire) =
            PreparedResponseBody::responses(right).into_prepared_wire(&Channel::Responses);

        assert_eq!(left_wire.body(), right_wire.body());
        assert_eq!(
            left_wire.responses_static_projection_digest(),
            right_wire.responses_static_projection_digest()
        );
        assert_eq!(
            serde_json::to_vec(&left_body["input"]).unwrap(),
            serde_json::to_vec(&input).unwrap(),
            "canonicalizing static roots must not rewrite caller input"
        );
        assert_eq!(
            left_body["input"][0]["opaque_json"],
            "{\"second\":2,\"first\":1}"
        );
        assert_eq!(left_body["input"][1]["type"], "function_call_output");
        assert_eq!(left_body["input"][1]["output"], "caller-owned tool output");
    }

    #[test]
    fn tracked_final_mutation_recomputes_static_projection_without_reencoding_input() {
        let initial = json!({
            "model": "gpt-test",
            "prompt_cache_key": "cache-a",
            "instructions": "stable",
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "stream": true
        });
        let initial_digest = PreparedWireRequest::from_value(&Channel::Responses, &initial)
            .responses_static_projection_digest()
            .map(str::to_owned);
        reset_draft_member_encodings();
        let mut body = PreparedResponseBody::responses(initial);
        assert!(body.set_root("prompt_cache_key", json!("cache-b")));

        let (_, wire) = body.into_prepared_wire(&Channel::Responses);
        assert_ne!(
            initial_digest.as_deref(),
            wire.responses_static_projection_digest()
        );
        assert_eq!(draft_member_encoding_count("input"), 1);
        assert_eq!(draft_member_encoding_count("prompt_cache_key"), 2);
    }

    #[test]
    fn options_sibling_proof_allows_only_the_recognized_options_root_to_differ() {
        let base = json!({
            "model": "gpt-test",
            "instructions": "same static instruction",
            "tools": [{"type":"function","name":"write_file"}],
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "stream": true
        });
        let absent = PreparedWireRequest::from_value(&Channel::Responses, &base);
        let absent_proof = responses_prompt_cache_options_sibling_proof(
            &base,
            absent.responses_cache_maturity_static_projection_digest(),
        )
        .expect("a wire without Options has a strict sibling proof");

        let mut options_24h = base.clone();
        options_24h["prompt_cache_options"] = json!({"mode":"implicit","ttl":"24h"});
        let options = PreparedWireRequest::from_value(&Channel::Responses, &options_24h);
        let options_proof = options
            .prompt_cache_options_sibling_proof()
            .cloned()
            .expect("a strict implicit 24h Options shape has a sibling proof");

        assert_eq!(
            absent_proof.options_neutral_static_projection_digest,
            options_proof.options_neutral_static_projection_digest,
            "the allowed Options root is the only neutralized static member"
        );
        assert_ne!(absent_proof.variant, options_proof.variant);
        assert_ne!(
            absent_proof.cache_maturity_static_projection_digest,
            options_proof.cache_maturity_static_projection_digest,
            "the normal maturity namespace still distinguishes the actual wire"
        );

        let mut semantic_change = options_24h.clone();
        semantic_change["tools"] = json!([{"type":"function","name":"delete_file"}]);
        let changed = PreparedWireRequest::from_value(&Channel::Responses, &semantic_change);
        assert_ne!(
            options_proof.options_neutral_static_projection_digest,
            changed
                .prompt_cache_options_sibling_proof()
                .expect("a changed but valid Options wire still has a proof")
                .options_neutral_static_projection_digest,
            "any other static root drift must reject sibling reuse"
        );

        let mut invalid = base;
        invalid["prompt_cache_options"] = json!({"mode":"implicit","ttl":"24h","unexpected":true});
        assert!(
            PreparedWireRequest::from_value(&Channel::Responses, &invalid)
                .prompt_cache_options_sibling_proof()
                .is_none(),
            "ambiguous Options shapes cannot supply sibling evidence"
        );
    }

    #[test]
    fn absent_options_skip_the_sibling_hash_on_normal_responses_wires() {
        let body = json!({
            "model": "gpt-test",
            "instructions": "same static instruction",
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "stream": true
        });
        let wire = PreparedWireRequest::from_value(&Channel::Responses, &body);
        assert!(
            wire.prompt_cache_options_sibling_proof().is_none(),
            "normal absent-Options traffic must not pay for the isolated sibling digest"
        );
    }

    #[test]
    fn isolated_ab_keeps_an_absent_options_sibling_witness() {
        let body = json!({
            "model": "gpt-test",
            "instructions": "same static instruction",
            "input": [{"type":"message","role":"user","content":"anchor"}],
            "stream": true
        });

        assert!(
            should_capture_prompt_cache_options_sibling_proof(&body, true),
            "both isolated arms need the absent-Options witness before the candidate adds 24h Options"
        );
        assert!(
            !should_capture_prompt_cache_options_sibling_proof(&body, false),
            "normal absent-Options traffic remains outside the isolated sibling experiment"
        );
    }

    #[test]
    fn pending_responses_freezes_cache_controls_before_the_first_wire_draft() {
        let initial = json!({
            "model": "gpt-test",
            "input": [{"type":"message","role":"user","content":"stable"}],
            "stream": true
        });
        let mut body = PreparedResponseBody::responses_pending(initial);
        assert!(body.set_root("prompt_cache_retention", json!("24h")));
        let (final_body, wire) = body.into_prepared_wire(&Channel::Responses);
        let parsed: Value = serde_json::from_slice(wire.body()).unwrap();

        assert_eq!(parsed, final_body);
        assert_eq!(parsed["prompt_cache_retention"], "24h");
        assert_eq!(
            wire.atoapi_mutated_static_categories(),
            ["cache_control".to_string()],
            "the final production-equivalent wire retains only the fixed category"
        );
        assert!(wire.responses_static_projection_digest().is_some());
    }

    #[test]
    fn stable_cache_controls_keep_root_and_followup_static_projections_equal() {
        let freeze = |input: Value| {
            let mut body = PreparedResponseBody::responses_pending(json!({
                "model": "gpt-test",
                "input": input,
                "stream": true
            }));
            assert!(body.set_root("prompt_cache_key", json!("stable-route-key")));
            assert!(body.set_root("prompt_cache_retention", json!("24h")));
            body.into_prepared_wire(&Channel::Responses).1
        };

        let root = freeze(json!([{
            "type":"message",
            "role":"user",
            "content":"root request"
        }]));
        let followup = freeze(json!([
            {"type":"message","role":"user","content":"root request"},
            {"type":"message","role":"assistant","content":"prior answer"},
            {"type":"message","role":"user","content":"followup request"}
        ]));

        assert_eq!(
            root.responses_static_projection_digest(),
            followup.responses_static_projection_digest(),
            "a stable cache-control policy must not split the root and follow-up cache prefix"
        );
        assert_eq!(
            root.atoapi_mutated_static_categories(),
            ["cache_control".to_string()]
        );
        assert_eq!(
            followup.atoapi_mutated_static_categories(),
            ["cache_control".to_string()]
        );
    }

    #[test]
    fn final_wire_reports_only_fixed_redacted_categories_for_late_mutations() {
        let initial = json!({
            "model": "gpt-test",
            "prompt_cache_key": "before-secret",
            "stream": true,
            "input": [{"type":"message","role":"user","content":"before-context"}],
            "x_private_extension": {"credential":"before-extension-secret"}
        });
        let mut body = PreparedResponseBody::responses(initial);
        assert!(body.set_root("prompt_cache_key", json!("after-secret")));
        assert!(body.set_root("stream", json!(false)));
        assert!(body.set_root(
            "input",
            json!([{"type":"message","role":"user","content":"after-context"}]),
        ));
        assert!(body.set_root(
            "x_private_extension",
            json!({"credential":"after-extension-secret"}),
        ));

        let (_, wire) = body.into_prepared_wire(&Channel::Responses);
        let expected = vec![
            "cache_control".to_string(),
            "extension".to_string(),
            "transport_controls".to_string(),
        ];
        assert_eq!(wire.atoapi_mutated_static_categories(), expected.as_slice());
        let categories = wire.atoapi_mutated_static_categories().join(",");
        assert!(!categories.contains("input"));
        assert!(!categories.contains("x_private_extension"));
        assert!(!categories.contains("after-secret"));
        assert!(!categories.contains("after-context"));
        assert!(!categories.contains("after-extension-secret"));
    }

    #[test]
    fn pending_final_wire_keeps_redacted_mutation_categories() {
        let mut body = PreparedResponseBody::responses_pending(json!({
            "model": "gpt-test",
            "stream": true,
            "input": [{"type":"message","role":"user","content":"context"}]
        }));
        assert!(body.set_root("instructions", json!("stable instructions")));
        assert!(body.set_root("x_vendor_extension", json!({"opaque": true})));

        let (_, wire) = body.into_prepared_wire(&Channel::Responses);
        assert_eq!(
            wire.atoapi_mutated_static_categories(),
            ["extension", "instructions"]
        );
    }

    #[test]
    fn static_projection_requires_a_responses_input_member() {
        let without_input = PreparedWireRequest::from_value(
            &Channel::Responses,
            &json!({"model":"gpt-test","stream":true}),
        );
        let chat = PreparedWireRequest::from_value(
            &Channel::Chat,
            &json!({"model":"gpt-test","messages":[]}),
        );
        let scalar_input = PreparedWireRequest::from_value(
            &Channel::Responses,
            &json!({"model":"gpt-test","input":"not-an-array"}),
        );

        assert!(without_input.responses_static_projection_digest().is_none());
        assert!(chat.responses_static_projection_digest().is_none());
        assert!(scalar_input.responses_static_projection_digest().is_none());
    }

    #[test]
    fn final_wire_detects_only_protocol_breakpoint_positions() {
        let protocol_breakpoint = PreparedWireRequest::from_value(
            &Channel::Responses,
            &json!({
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
        );
        let string_only = PreparedWireRequest::from_value(
            &Channel::Responses,
            &json!({
                "model":"gpt-test",
                "input":[{
                    "type":"message",
                    "content":"literal: \"prompt_cache_breakpoint\": is not a member"
                }]
            }),
        );

        let tool_output_property = PreparedWireRequest::from_value(
            &Channel::Responses,
            &json!({
                "model":"gpt-test",
                "input":[{
                    "type":"function_call_output",
                    "call_id":"call-a",
                    "output":{"prompt_cache_breakpoint":{"mode":"data"}}
                }]
            }),
        );

        assert!(protocol_breakpoint.prompt_cache_breakpoint_present());
        assert!(!string_only.prompt_cache_breakpoint_present());
        assert!(!tool_output_property.prompt_cache_breakpoint_present());
    }
}

fn strict_prompt_cache_options_ttl(body: &Value) -> Option<PromptCacheOptionsTtl> {
    let options = body.get("prompt_cache_options")?.as_object()?;
    if options.len() != 2 || options.get("mode")?.as_str()? != "implicit" {
        return None;
    }
    match options.get("ttl")?.as_str()? {
        "30m" => Some(PromptCacheOptionsTtl::ThirtyMinutes),
        "24h" => Some(PromptCacheOptionsTtl::TwentyFourHours),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct PreparedWireRequest {
    channel: Channel,
    body: Bytes,
    gzip_body: Arc<OnceLock<Bytes>>,
    stream: bool,
    encode_ms: u64,
    /// Exact digest of the canonical Responses object formed by removing only
    /// the final top-level `input` member. Every other final wire member,
    /// including unknown extensions, participates with its already-serialized
    /// bytes. Appending Agent input therefore stays stable without treating an
    /// unclassified static field as equivalent.
    responses_static_projection_digest: Option<String>,
    /// A narrower, cache-maturity-safe witness for native Responses. It ignores
    /// per-attempt delivery/telemetry roots while retaining every model-visible
    /// static root (including unknown extensions). It is shared by the strict
    /// native-delta proof and the FullReplay prefix-maturity guard, is never
    /// sent upstream, and never contains raw request text.
    responses_cache_maturity_static_projection_digest: Option<String>,
    /// Fixed category names for top-level roots Atoapi itself changed after
    /// the initial Responses wire draft. No values, request text, tool output,
    /// keys, or hashes are retained here.
    atoapi_mutated_static_categories: Vec<String>,
    /// Strict Options value captured from the exact body at the same boundary
    /// that creates the immutable wire bytes.
    prompt_cache_options_ttl: Option<PromptCacheOptionsTtl>,
    /// A process-only witness for the isolated Options sibling-settle probe.
    /// It is present only for an array-input Responses wire whose Options root
    /// is absent or exactly `{mode: implicit, ttl: 30m|24h}`.
    prompt_cache_options_sibling_proof: Option<PromptCacheOptionsSiblingProof>,
    protocol_breakpoint_provenance: ProtocolBreakpointProvenance,
    outbound_prefix_fingerprints: Option<ResponsesWirePrefixFingerprints>,
}

impl PreparedWireRequest {
    pub(super) fn from_value(channel: &Channel, body: &Value) -> Self {
        Self::from_value_with_protocol_breakpoint_witness(channel, body, false)
    }

    fn from_value_with_protocol_breakpoint_witness(
        channel: &Channel,
        body: &Value,
        atoapi_injected_protocol_breakpoint: bool,
    ) -> Self {
        let encode_started = Instant::now();
        let (encoded, responses_static_projection_digest) = if matches!(channel, Channel::Responses)
        {
            serialize_responses_body_with_static_projection(body)
        } else {
            (
                serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec()),
                None,
            )
        };
        Self::from_encoded(
            channel,
            body,
            Bytes::from(encoded),
            responses_static_projection_digest,
            atoapi_injected_protocol_breakpoint,
            encode_started.elapsed().as_millis() as u64,
        )
    }

    fn from_encoded(
        channel: &Channel,
        body: &Value,
        encoded: Bytes,
        responses_static_projection_digest: Option<String>,
        atoapi_injected_protocol_breakpoint: bool,
        encode_ms: u64,
    ) -> Self {
        let finalize_started = Instant::now();
        let outbound_prefix_fingerprints = maybe_responses_wire_prefix_fingerprints(channel, body);
        let stream = request_body_stream_flag(body);
        let responses_cache_maturity_static_projection_digest =
            responses_cache_maturity_static_projection_digest(body);
        Self {
            channel: channel.clone(),
            body: encoded,
            gzip_body: Arc::new(OnceLock::new()),
            stream,
            encode_ms: encode_ms.saturating_add(finalize_started.elapsed().as_millis() as u64),
            responses_static_projection_digest,
            // The sibling witness is only consumed by the isolated Options
            // treatment. Normal Responses traffic has no Options root, so
            // avoid hashing the entire static wire on that hot path. In an
            // isolated A/B process both arms must retain the absent-variant
            // witness: the 24h switch belongs only to the candidate arm, but
            // the champion arm is the sibling needed for a strict settle.
            prompt_cache_options_sibling_proof: if should_capture_prompt_cache_options_sibling_proof(
                body,
                crate::config::isolated_test_instance(),
            ) {
                responses_prompt_cache_options_sibling_proof(
                    body,
                    responses_cache_maturity_static_projection_digest.as_deref(),
                )
            } else {
                None
            },
            responses_cache_maturity_static_projection_digest,
            atoapi_mutated_static_categories: Vec::new(),
            prompt_cache_options_ttl: strict_prompt_cache_options_ttl(body),
            protocol_breakpoint_provenance: protocol_breakpoint_provenance(
                channel,
                body,
                atoapi_injected_protocol_breakpoint,
            ),
            outbound_prefix_fingerprints,
        }
    }

    pub(super) fn body(&self) -> &Bytes {
        &self.body
    }

    pub(super) fn channel(&self) -> &Channel {
        &self.channel
    }

    pub(super) fn len(&self) -> usize {
        self.body.len()
    }

    pub(super) fn is_stream(&self) -> bool {
        self.stream
    }

    pub(super) fn encode_ms(&self) -> u64 {
        self.encode_ms
    }

    pub(super) const fn prompt_cache_options_ttl(&self) -> Option<PromptCacheOptionsTtl> {
        self.prompt_cache_options_ttl
    }

    pub(super) fn responses_static_projection_digest(&self) -> Option<&str> {
        self.responses_static_projection_digest.as_deref()
    }

    pub(super) fn responses_cache_maturity_static_projection_digest(&self) -> Option<&str> {
        self.responses_cache_maturity_static_projection_digest
            .as_deref()
    }

    pub(super) fn prompt_cache_options_sibling_proof(
        &self,
    ) -> Option<&PromptCacheOptionsSiblingProof> {
        self.prompt_cache_options_sibling_proof.as_ref()
    }

    pub(super) fn atoapi_mutated_static_categories(&self) -> &[String] {
        &self.atoapi_mutated_static_categories
    }

    #[cfg(test)]
    pub(super) fn prompt_cache_breakpoint_present(&self) -> bool {
        self.protocol_breakpoint_provenance.is_present()
    }

    pub(super) fn protocol_breakpoint_provenance(&self) -> &ProtocolBreakpointProvenance {
        &self.protocol_breakpoint_provenance
    }

    pub(super) fn outbound_prefix_fingerprints(&self) -> Option<&ResponsesWirePrefixFingerprints> {
        self.outbound_prefix_fingerprints.as_ref()
    }

    pub(super) fn cached_gzip_body(&self) -> Option<Bytes> {
        self.gzip_body.get().cloned()
    }

    pub(super) fn cache_gzip_body(&self, body: Bytes) -> Bytes {
        if self.gzip_body.set(body.clone()).is_ok() {
            body
        } else {
            self.gzip_body
                .get()
                .cloned()
                .expect("a failed gzip cache set must leave an initialized value")
        }
    }

    fn set_atoapi_mutated_roots(&mut self, roots: &[String]) {
        self.atoapi_mutated_static_categories = atoapi_static_mutation_categories(roots);
    }
}

fn atoapi_static_mutation_categories(roots: &[String]) -> Vec<String> {
    let mut categories = BTreeSet::new();
    for root in roots {
        // `input` is deliberately excluded: it is not part of the static
        // projection and may contain sensitive conversation material. Unknown
        // roots are coarsened so an untrusted caller cannot make a field name
        // part of diagnostics.
        let category = if root == "input" {
            None
        } else {
            known_responses_static_root_category(root).or(Some("extension"))
        };
        if let Some(category) = category {
            categories.insert(category);
        }
    }
    categories.into_iter().map(str::to_owned).collect()
}

fn known_responses_static_root_category(root: &str) -> Option<&'static str> {
    match root {
        "model" => Some("model"),
        "prompt_cache_key"
        | "prompt_cache_retention"
        | "prompt_cache_options"
        | "prompt_cache_breakpoint" => Some("cache_control"),
        "instructions" => Some("instructions"),
        "tools" => Some("tools"),
        "tool_choice" | "parallel_tool_calls" => Some("tool_settings"),
        "reasoning" | "text" | "response_format" | "temperature" | "top_p"
        | "max_output_tokens" | "include" => Some("generation_controls"),
        "stream" | "store" | "service_tier" | "truncation" | "previous_response_id" => {
            Some("transport_controls")
        }
        "metadata" | "user" => Some("metadata"),
        _ => None,
    }
}

fn canonicalize_responses_static_roots(body: &mut Value) {
    let Some(object) = body.as_object_mut() else {
        return;
    };
    for (key, value) in object.iter_mut() {
        canonicalize_responses_static_root(key, value);
    }
}

fn canonicalize_responses_static_root(key: &str, value: &mut Value) {
    if known_responses_static_root_category(key).is_some() {
        canonicalize_json_object_keys(value);
    }
}

fn canonicalize_json_object_keys(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut canonical = Map::new();
            for key in keys {
                if let Some(mut child) = map.remove(&key) {
                    canonicalize_json_object_keys(&mut child);
                    canonical.insert(key, child);
                }
            }
            *map = canonical;
        }
        Value::Array(items) => {
            // Array order is part of the Responses protocol and must remain
            // byte-for-byte caller-owned; only objects inside each element are
            // canonicalized.
            for item in items {
                canonicalize_json_object_keys(item);
            }
        }
        _ => {}
    }
}

fn protocol_breakpoint_provenance(
    channel: &Channel,
    body: &Value,
    atoapi_injected_protocol_breakpoint: bool,
) -> ProtocolBreakpointProvenance {
    let present = cache_capability::contains_protocol_cache_breakpoint(body, channel);
    if !present && !atoapi_injected_protocol_breakpoint {
        return ProtocolBreakpointProvenance::Absent;
    }

    // Only the mutation witness plus a single legal final Responses marker
    // proves ownership. A marker supplied by a caller may look structurally
    // identical, so it remains foreign unless this request inserted it.
    if matches!(channel, Channel::Responses) && atoapi_injected_protocol_breakpoint {
        if let Some(placement_digest) =
            cache_capability::responses_protocol_breakpoint_placement_digest(body)
        {
            return ProtocolBreakpointProvenance::AtoapiInjected { placement_digest };
        }
    }

    ProtocolBreakpointProvenance::AmbiguousOrForeign
}

pub(super) fn serialize_responses_body_bytes_for_provider_prefix(body: &Value) -> Vec<u8> {
    let Some(map) = body.as_object() else {
        return serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec());
    };

    let mut output = Vec::new();
    output.push(b'{');
    let mut first = true;
    for_each_responses_wire_member(map, |key, value| {
        write_json_member(&mut output, &mut first, key, value);
    });
    output.push(b'}');
    output
}

fn serialize_responses_body_with_static_projection(body: &Value) -> (Vec<u8>, Option<String>) {
    let Some(map) = body.as_object() else {
        return (
            serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec()),
            None,
        );
    };

    let mut output = Vec::new();
    let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
    output.push(b'{');
    let mut first = true;
    for_each_responses_wire_member(map, |key, value| {
        let range = write_json_member(&mut output, &mut first, key, value);
        if let Some(projection) = static_projection.as_mut() {
            projection.observe_member(key, &output[range]);
        }
    });
    output.push(b'}');
    let static_projection_digest =
        static_projection.and_then(ResponsesStaticProjectionHasher::finish);
    (output, static_projection_digest)
}

/// Hashes the static semantics that can safely share native Responses prefix
/// maturity. `input` is the conversation payload and is excluded; per-request
/// delivery and telemetry values are excluded so a normal Codex turn's
/// metadata does not split its stable prefix. Every other root, including
/// unknown extensions, remains a strict equality boundary.
///
/// This is not an upstream request rewrite: the exact final wire is still sent
/// unchanged and still owns the final-scope waterline. It only decides whether
/// a previously observed *same semantic prefix* may consume the bounded local
/// maturity wait.
pub(super) fn responses_cache_maturity_static_projection_digest(body: &Value) -> Option<String> {
    let map = body.as_object()?;
    map.get("input").is_some_and(Value::is_array).then(|| {
        let mut hasher = Sha256::new();
        hasher.update(b"responses-native-continuation-static-v1\0{");
        let mut first = true;
        for_each_responses_wire_member(map, |key, value| {
            if !continuation_static_member(key) {
                return;
            }
            if !first {
                hasher.update(b",");
            }
            first = false;
            let mut encoded = Vec::new();
            let mut member_first = true;
            let _ = write_json_member(&mut encoded, &mut member_first, key, value);
            hasher.update(encoded);
        });
        hasher.update(b"}");
        format!("{:x}", hasher.finalize())
    })
}

fn should_capture_prompt_cache_options_sibling_proof(
    body: &Value,
    isolated_test_instance: bool,
) -> bool {
    body.get("prompt_cache_options").is_some() || isolated_test_instance
}

/// Build the strict Options-sibling witness used only by the isolated
/// pre-dispatch settle experiment.  Unlike the broader maturity projection,
/// this hashes every final static Responses root except `input` and the
/// strictly-recognised `prompt_cache_options` root.  Therefore a matching
/// witness proves that the two frozen wires differ only by the allowed
/// Options variant (absent, implicit 30m, or implicit 24h).
pub(super) fn responses_prompt_cache_options_sibling_proof(
    body: &Value,
    cache_maturity_static_projection_digest: Option<&str>,
) -> Option<PromptCacheOptionsSiblingProof> {
    let map = body.as_object()?;
    if !map.get("input").is_some_and(Value::is_array) {
        return None;
    }
    let variant = match map.get("prompt_cache_options") {
        None => PromptCacheOptionsSiblingVariant::Absent,
        Some(_) => match strict_prompt_cache_options_ttl(body)? {
            PromptCacheOptionsTtl::ThirtyMinutes => {
                PromptCacheOptionsSiblingVariant::ImplicitThirtyMinutes
            }
            PromptCacheOptionsTtl::TwentyFourHours => {
                PromptCacheOptionsSiblingVariant::ImplicitTwentyFourHours
            }
        },
    };
    let cache_maturity_static_projection_digest =
        cache_maturity_static_projection_digest?.to_owned();

    let mut hasher = Sha256::new();
    hasher.update(b"responses-options-sibling-static-v1\0{");
    let mut first = true;
    for_each_responses_wire_member(map, |key, value| {
        if key == "input" || key == "prompt_cache_options" {
            return;
        }
        if !first {
            hasher.update(b",");
        }
        first = false;
        let mut encoded = Vec::new();
        let mut member_first = true;
        let _ = write_json_member(&mut encoded, &mut member_first, key, value);
        hasher.update(encoded);
    });
    hasher.update(b"}");

    Some(PromptCacheOptionsSiblingProof {
        cache_maturity_static_projection_digest,
        options_neutral_static_projection_digest: format!("{:x}", hasher.finalize()),
        variant,
    })
}

fn continuation_static_member(key: &str) -> bool {
    !matches!(
        key,
        "input" | "previous_response_id" | "metadata" | "user" | "stream" | "store"
    )
}

struct ResponsesStaticProjectionHasher {
    hasher: Sha256,
    first: bool,
    saw_input: bool,
}

impl ResponsesStaticProjectionHasher {
    fn for_body(map: &Map<String, Value>) -> Option<Self> {
        map.get("input").is_some_and(Value::is_array).then(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"responses-static-wire-projection-v2\0");
            hasher.update(b"{");
            Self {
                hasher,
                first: true,
                saw_input: false,
            }
        })
    }

    fn observe_member(&mut self, key: &str, encoded_member: &[u8]) {
        if key == "input" {
            self.saw_input = true;
            return;
        }
        if self.first {
            self.first = false;
        } else {
            self.hasher.update(b",");
        }
        self.hasher.update(encoded_member);
    }

    fn finish(mut self) -> Option<String> {
        if !self.saw_input {
            return None;
        }
        self.hasher.update(b"}");
        Some(format!("{:x}", self.hasher.finalize()))
    }
}

fn for_each_responses_wire_member<'a>(
    map: &'a Map<String, Value>,
    mut visit: impl FnMut(&'a str, &'a Value),
) {
    for key in RESPONSES_WIRE_ORDERED_KEYS {
        if let Some(value) = map.get(key) {
            visit(key, value);
        }
    }

    let mut remaining = map
        .keys()
        .filter(|key| !RESPONSES_WIRE_ORDERED_KEYS.contains(&key.as_str()))
        .collect::<Vec<_>>();
    remaining.sort();
    for key in remaining {
        if let Some(value) = map.get(key) {
            visit(key, value);
        }
    }
}

fn write_json_member(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &str,
    value: &Value,
) -> Range<usize> {
    if *first {
        *first = false;
    } else {
        output.push(b',');
    }
    let start = output.len();
    serde_json::to_writer(&mut *output, key)
        .expect("serializing a JSON object key into memory must succeed");
    output.push(b':');
    serde_json::to_writer(&mut *output, value)
        .expect("serializing a JSON value into memory must succeed");
    start..output.len()
}

#[cfg(test)]
fn write_draft_json_member(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &str,
    value: &Value,
) -> Range<usize> {
    record_draft_member_encoding(key);
    write_json_member(output, first, key, value)
}

#[cfg(test)]
fn write_prepared_member_bytes(
    output: &mut Vec<u8>,
    first: &mut bool,
    bytes: &[u8],
) -> Range<usize> {
    if *first {
        *first = false;
    } else {
        output.push(b',');
    }
    let start = output.len();
    output.extend_from_slice(bytes);
    start..output.len()
}

#[cfg(test)]
std::thread_local! {
    static DRAFT_MEMBER_ENCODINGS: std::cell::RefCell<std::collections::HashMap<String, u64>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

#[cfg(test)]
fn record_draft_member_encoding(key: &str) {
    DRAFT_MEMBER_ENCODINGS.with(|counts| {
        let mut counts = counts.borrow_mut();
        *counts.entry(key.to_string()).or_default() += 1;
    });
}

#[cfg(test)]
pub(super) fn reset_draft_member_encodings() {
    DRAFT_MEMBER_ENCODINGS.with(|counts| counts.borrow_mut().clear());
}

#[cfg(test)]
pub(super) fn draft_member_encoding_count(key: &str) -> u64 {
    DRAFT_MEMBER_ENCODINGS.with(|counts| counts.borrow().get(key).copied().unwrap_or(0))
}
