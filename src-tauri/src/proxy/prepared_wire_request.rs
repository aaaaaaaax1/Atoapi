use bytes::Bytes;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    ops::Range,
    sync::{Arc, OnceLock},
    time::Instant,
};

use crate::{config::Channel, metrics::ResponsesWirePrefixFingerprints};

use super::{maybe_responses_wire_prefix_fingerprints, request_body_stream_flag};

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

const PROMPT_CACHE_BREAKPOINT_MEMBER: &[u8] = b"\"prompt_cache_breakpoint\":";

struct BreakpointDetectingWriter<'a> {
    output: &'a mut Vec<u8>,
    matched: usize,
    found: bool,
}

impl<'a> BreakpointDetectingWriter<'a> {
    fn new(output: &'a mut Vec<u8>) -> Self {
        Self {
            output,
            matched: 0,
            found: false,
        }
    }

    fn found(&self) -> bool {
        self.found
    }
}

impl std::io::Write for BreakpointDetectingWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if !self.found {
            for byte in buffer {
                if *byte == PROMPT_CACHE_BREAKPOINT_MEMBER[self.matched] {
                    self.matched += 1;
                    if self.matched == PROMPT_CACHE_BREAKPOINT_MEMBER.len() {
                        self.found = true;
                        break;
                    }
                } else {
                    self.matched = usize::from(*byte == PROMPT_CACHE_BREAKPOINT_MEMBER[0]);
                }
            }
        }
        self.output.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedWireMember {
    key: String,
    range: Range<usize>,
    breakpoint_present: bool,
}

/// Retains the first canonical Responses encoding so late, explicitly tracked
/// mutations only re-encode the members that actually changed.
///
/// The draft owns encoded bytes, not cloned JSON values. In particular, a
/// large `input` is serialized once and then copied from its byte range when a
/// cache metadata field changes later in request preparation.
#[derive(Debug)]
struct PreparedWireDraft {
    body: Bytes,
    members: Vec<PreparedWireMember>,
    responses_static_projection_digest: Option<String>,
    prompt_cache_breakpoint_present: bool,
    encode_ms: u64,
}

impl PreparedWireDraft {
    fn from_responses_value(body: &Value) -> Option<Self> {
        let map = body.as_object()?;
        let encode_started = Instant::now();
        let mut output = Vec::new();
        let mut members = Vec::with_capacity(map.len());
        let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
        let mut prompt_cache_breakpoint_present = false;
        output.push(b'{');
        let mut first = true;
        for_each_responses_wire_member(map, |key, value| {
            let (range, member_breakpoint_present) =
                write_draft_json_member(&mut output, &mut first, key, value);
            prompt_cache_breakpoint_present |= member_breakpoint_present;
            if let Some(projection) = static_projection.as_mut() {
                projection.observe_member(key, &output[range.clone()]);
            }
            members.push(PreparedWireMember {
                key: key.to_string(),
                range,
                breakpoint_present: member_breakpoint_present,
            });
        });
        output.push(b'}');
        let responses_static_projection_digest =
            static_projection.and_then(ResponsesStaticProjectionHasher::finish);

        Some(Self {
            body: Bytes::from(output),
            members,
            responses_static_projection_digest,
            prompt_cache_breakpoint_present,
            encode_ms: encode_started.elapsed().as_millis() as u64,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.body.len()
    }

    #[cfg(test)]
    pub(super) fn body_ptr(&self) -> *const u8 {
        self.body.as_ptr()
    }

    fn freeze_tracked(self, final_body: &Value, changed_fields: &[String]) -> PreparedWireRequest {
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
                self.prompt_cache_breakpoint_present,
                self.encode_ms,
            );
        }

        let freeze_started = Instant::now();
        let mut output = Vec::with_capacity(self.body.len());
        let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
        let mut prompt_cache_breakpoint_present = false;
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
            let (range, member_breakpoint_present) = if let Some(Some(bytes)) = retained {
                let member = self
                    .members
                    .iter()
                    .find(|member| member.key == key)
                    .expect("a retained member must have draft metadata");
                (
                    write_prepared_member_bytes(&mut output, &mut first, bytes),
                    member.breakpoint_present,
                )
            } else {
                write_draft_json_member(&mut output, &mut first, key, value)
            };
            prompt_cache_breakpoint_present |= member_breakpoint_present;
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
            prompt_cache_breakpoint_present,
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
    wire_draft: Option<PreparedWireDraft>,
    changed_fields: Vec<String>,
    requires_full_freeze: bool,
}

impl PreparedResponseBody {
    /// Own an unencoded semantic body. Freezing serializes it exactly once.
    ///
    /// This constructor is used when no retained Responses encoding exists,
    /// for example after a compatibility channel conversion.
    pub(super) fn plain(body: Value) -> Self {
        Self {
            body,
            wire_draft: None,
            changed_fields: Vec::new(),
            requires_full_freeze: false,
        }
    }

    /// Own a Responses body and its first canonical encoding as one value.
    ///
    /// The draft is derived only after this method has taken ownership of the
    /// semantic body. Callers can therefore never attach bytes encoded from a
    /// different body, while late root mutations can still retain a large
    /// unchanged `input` member without cloning or re-serializing it.
    pub(super) fn responses(body: Value) -> Self {
        let wire_draft = PreparedWireDraft::from_responses_value(&body);
        Self {
            body,
            wire_draft,
            changed_fields: Vec::new(),
            requires_full_freeze: false,
        }
    }

    pub(super) fn body(&self) -> &Value {
        &self.body
    }

    /// Size of the retained canonical Responses encoding, when the body was
    /// an object and could be drafted.
    pub(super) fn initial_wire_len(&self) -> Option<usize> {
        self.wire_draft.as_ref().map(PreparedWireDraft::len)
    }

    #[cfg(test)]
    pub(super) fn initial_wire_ptr(&self) -> Option<*const u8> {
        self.wire_draft.as_ref().map(PreparedWireDraft::body_ptr)
    }

    pub(super) fn set_root(&mut self, key: &str, value: Value) -> bool {
        let Some(object) = self.body.as_object_mut() else {
            self.requires_full_freeze = true;
            return false;
        };
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

    pub(super) fn root_keys(&self) -> Vec<String> {
        self.body
            .as_object()
            .map(|object| object.keys().cloned().collect())
            .unwrap_or_default()
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
        mutation(&mut self.body)
    }

    pub(super) fn into_prepared_wire(self, channel: &Channel) -> (Value, PreparedWireRequest) {
        let Self {
            body,
            wire_draft,
            changed_fields,
            requires_full_freeze,
        } = self;
        let wire = match (channel, wire_draft, requires_full_freeze) {
            (Channel::Responses, Some(draft), false) => {
                draft.freeze_tracked(&body, &changed_fields)
            }
            _ => PreparedWireRequest::from_value(channel, &body),
        };
        (body, wire)
    }

    #[cfg(test)]
    pub(super) fn requires_full_freeze(&self) -> bool {
        self.requires_full_freeze
    }

    fn mark_changed_root(&mut self, key: &str) {
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
    fn final_wire_detects_breakpoint_member_without_recursive_value_walk() {
        let nested = PreparedWireRequest::from_value(
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

        assert!(nested.prompt_cache_breakpoint_present());
        assert!(!string_only.prompt_cache_breakpoint_present());
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
    prompt_cache_breakpoint_present: bool,
    outbound_prefix_fingerprints: Option<ResponsesWirePrefixFingerprints>,
}

impl PreparedWireRequest {
    pub(super) fn from_value(channel: &Channel, body: &Value) -> Self {
        let encode_started = Instant::now();
        let (encoded, responses_static_projection_digest, prompt_cache_breakpoint_present) =
            if matches!(channel, Channel::Responses) {
                serialize_responses_body_with_static_projection(body)
            } else {
                (
                    serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec()),
                    None,
                    false,
                )
            };
        Self::from_encoded(
            channel,
            body,
            Bytes::from(encoded),
            responses_static_projection_digest,
            prompt_cache_breakpoint_present,
            encode_started.elapsed().as_millis() as u64,
        )
    }

    fn from_encoded(
        channel: &Channel,
        body: &Value,
        encoded: Bytes,
        responses_static_projection_digest: Option<String>,
        prompt_cache_breakpoint_present: bool,
        encode_ms: u64,
    ) -> Self {
        let finalize_started = Instant::now();
        let outbound_prefix_fingerprints = maybe_responses_wire_prefix_fingerprints(channel, body);
        let stream = request_body_stream_flag(body);
        Self {
            channel: channel.clone(),
            body: encoded,
            gzip_body: Arc::new(OnceLock::new()),
            stream,
            encode_ms: encode_ms.saturating_add(finalize_started.elapsed().as_millis() as u64),
            responses_static_projection_digest,
            prompt_cache_breakpoint_present,
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

    pub(super) fn responses_static_projection_digest(&self) -> Option<&str> {
        self.responses_static_projection_digest.as_deref()
    }

    pub(super) fn prompt_cache_breakpoint_present(&self) -> bool {
        self.prompt_cache_breakpoint_present
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

fn serialize_responses_body_with_static_projection(
    body: &Value,
) -> (Vec<u8>, Option<String>, bool) {
    let Some(map) = body.as_object() else {
        return (
            serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec()),
            None,
            false,
        );
    };

    let mut output = Vec::new();
    let mut static_projection = ResponsesStaticProjectionHasher::for_body(map);
    let mut prompt_cache_breakpoint_present = false;
    output.push(b'{');
    let mut first = true;
    for_each_responses_wire_member(map, |key, value| {
        let (range, member_breakpoint_present) =
            write_json_member_with_breakpoint(&mut output, &mut first, key, value);
        prompt_cache_breakpoint_present |= member_breakpoint_present;
        if let Some(projection) = static_projection.as_mut() {
            projection.observe_member(key, &output[range]);
        }
    });
    output.push(b'}');
    let static_projection_digest =
        static_projection.and_then(ResponsesStaticProjectionHasher::finish);
    (
        output,
        static_projection_digest,
        prompt_cache_breakpoint_present,
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

fn write_json_member_with_breakpoint(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &str,
    value: &Value,
) -> (Range<usize>, bool) {
    if *first {
        *first = false;
    } else {
        output.push(b',');
    }
    let start = output.len();
    serde_json::to_writer(&mut *output, key)
        .expect("serializing a JSON object key into memory must succeed");
    output.push(b':');
    let mut writer = BreakpointDetectingWriter::new(output);
    serde_json::to_writer(&mut writer, value)
        .expect("serializing a JSON value into memory must succeed");
    let breakpoint_present = key == "prompt_cache_breakpoint" || writer.found();
    let end = writer.output.len();
    (start..end, breakpoint_present)
}

fn write_draft_json_member(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &str,
    value: &Value,
) -> (Range<usize>, bool) {
    record_draft_member_encoding(key);
    write_json_member_with_breakpoint(output, first, key, value)
}

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

#[cfg(not(test))]
fn record_draft_member_encoding(_key: &str) {}

#[cfg(test)]
pub(super) fn reset_draft_member_encodings() {
    DRAFT_MEMBER_ENCODINGS.with(|counts| counts.borrow_mut().clear());
}

#[cfg(test)]
pub(super) fn draft_member_encoding_count(key: &str) -> u64 {
    DRAFT_MEMBER_ENCODINGS.with(|counts| counts.borrow().get(key).copied().unwrap_or(0))
}
