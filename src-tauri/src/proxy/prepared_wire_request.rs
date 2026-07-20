use bytes::Bytes;
use serde_json::{Map, Value};
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
#[derive(Debug)]
pub(super) struct PreparedWireDraft {
    body: Bytes,
    members: Vec<PreparedWireMember>,
    encode_ms: u64,
}

impl PreparedWireDraft {
    pub(super) fn from_responses_value(body: &Value) -> Option<Self> {
        let map = body.as_object()?;
        let encode_started = Instant::now();
        let mut output = Vec::new();
        let mut members = Vec::with_capacity(map.len());
        output.push(b'{');
        let mut first = true;
        for_each_responses_wire_member(map, |key, value| {
            let range = write_draft_json_member(&mut output, &mut first, key, value);
            members.push(PreparedWireMember {
                key: key.to_string(),
                range,
            });
        });
        output.push(b'}');

        Some(Self {
            body: Bytes::from(output),
            members,
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

    pub(super) fn freeze(
        self,
        final_body: &Value,
        changed_fields: &[String],
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
                self.encode_ms,
            );
        }

        let freeze_started = Instant::now();
        let mut output = Vec::with_capacity(self.body.len());
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
            if let Some(Some(bytes)) = retained {
                write_prepared_member_bytes(&mut output, &mut first, bytes);
            } else {
                write_draft_json_member(&mut output, &mut first, key, value);
            }
        });
        output.push(b'}');

        PreparedWireRequest::from_encoded(
            &Channel::Responses,
            final_body,
            Bytes::from(output),
            self.encode_ms
                .saturating_add(freeze_started.elapsed().as_millis() as u64),
        )
    }
}

#[derive(Debug)]
pub(super) struct PreparedWireRequest {
    channel: Channel,
    body: Bytes,
    gzip_body: Arc<OnceLock<Bytes>>,
    stream: bool,
    encode_ms: u64,
    outbound_prefix_fingerprints: Option<ResponsesWirePrefixFingerprints>,
}

impl PreparedWireRequest {
    pub(super) fn from_value(channel: &Channel, body: &Value) -> Self {
        let encode_started = Instant::now();
        let encoded = if matches!(channel, Channel::Responses) {
            serialize_responses_body_bytes_for_provider_prefix(body)
        } else {
            serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec())
        };
        Self::from_encoded(
            channel,
            body,
            Bytes::from(encoded),
            encode_started.elapsed().as_millis() as u64,
        )
    }

    fn from_encoded(channel: &Channel, body: &Value, encoded: Bytes, encode_ms: u64) -> Self {
        let finalize_started = Instant::now();
        let outbound_prefix_fingerprints = maybe_responses_wire_prefix_fingerprints(channel, body);
        let stream = request_body_stream_flag(body);
        Self {
            channel: channel.clone(),
            body: encoded,
            gzip_body: Arc::new(OnceLock::new()),
            stream,
            encode_ms: encode_ms.saturating_add(finalize_started.elapsed().as_millis() as u64),
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

fn write_draft_json_member(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &str,
    value: &Value,
) -> Range<usize> {
    record_draft_member_encoding(key);
    write_json_member(output, first, key, value)
}

fn write_prepared_member_bytes(output: &mut Vec<u8>, first: &mut bool, bytes: &[u8]) {
    if *first {
        *first = false;
    } else {
        output.push(b',');
    }
    output.extend_from_slice(bytes);
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
