use serde_json::Value;

use crate::{
    config::{Channel, ProviderCacheCapabilityField},
    metrics::UsageRecord,
};
use std::collections::HashSet;

use super::{
    cache_capability_rejection_fields_from_value, provider_usage_from_value,
    response_id_from_value, sse,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct StreamObservation {
    pub model_output_started: bool,
    pub completed_event_seen: bool,
    pub responses_completed_event_seen: bool,
    pub message_stop_event_seen: bool,
    pub done_marker_seen: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct StreamSummary {
    pub usage: UsageRecord,
    pub response_id: Option<String>,
    pub output_items: Vec<Value>,
    pub completed_event_seen: bool,
    pub responses_completed_event_seen: bool,
    pub message_stop_event_seen: bool,
    pub done_marker_seen: bool,
    pub error_event_seen: bool,
    pub error_summary: Option<String>,
    pub cache_capability_rejection_fields: HashSet<ProviderCacheCapabilityField>,
    pub compaction_output_seen: bool,
    pub model_output_seen: bool,
    pub frame_overflowed: bool,
    responses_completed_sequence: Option<u64>,
    message_stop_sequence: Option<u64>,
    done_marker_sequence: Option<u64>,
    error_event_sequence: Option<u64>,
    frame_overflow_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum TerminalCompatibility {
    #[default]
    Strict,
    ResponsesDoneAtEof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamEnd {
    CleanEof,
    TransportError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalFailure {
    ErrorEvent,
    FrameTooLarge,
    IncompleteEof,
    TransportErrorBeforeTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TerminalVerdict {
    pub success: bool,
    pub failure: Option<TerminalFailure>,
    pub trailing_transport_anomaly: bool,
    pub trailing_protocol_anomaly: Option<TerminalFailure>,
}

pub(super) fn evaluate_terminal(
    channel: &Channel,
    compatibility: TerminalCompatibility,
    summary: &StreamSummary,
    end: StreamEnd,
) -> TerminalVerdict {
    let (strict_terminal_seen, strict_terminal_sequence) = match channel {
        Channel::Responses => (
            summary.responses_completed_event_seen,
            summary.responses_completed_sequence,
        ),
        Channel::Anthropic => (
            summary.message_stop_event_seen,
            summary.message_stop_sequence,
        ),
        Channel::Chat => (summary.done_marker_seen, summary.done_marker_sequence),
    };
    let compatible_terminal_seen = strict_terminal_seen
        || (matches!(channel, Channel::Responses)
            && compatibility == TerminalCompatibility::ResponsesDoneAtEof
            && end == StreamEnd::CleanEof
            && summary.done_marker_seen);
    let terminal_sequence = if strict_terminal_seen {
        Some(strict_terminal_sequence.unwrap_or(0))
    } else if compatible_terminal_seen {
        Some(summary.done_marker_sequence.unwrap_or(0))
    } else {
        None
    };
    let first_protocol_failure = [
        summary.frame_overflowed.then_some((
            summary.frame_overflow_sequence.unwrap_or(0),
            TerminalFailure::FrameTooLarge,
        )),
        summary.error_event_seen.then_some((
            summary.error_event_sequence.unwrap_or(0),
            TerminalFailure::ErrorEvent,
        )),
    ]
    .into_iter()
    .flatten()
    .min_by_key(|(sequence, _)| *sequence);

    if let Some((failure_sequence, failure)) = first_protocol_failure {
        if terminal_sequence.is_none_or(|terminal_sequence| failure_sequence <= terminal_sequence) {
            return terminal_failure(failure);
        }
    }

    if !compatible_terminal_seen {
        return terminal_failure(match end {
            StreamEnd::CleanEof => TerminalFailure::IncompleteEof,
            StreamEnd::TransportError => TerminalFailure::TransportErrorBeforeTerminal,
        });
    }

    TerminalVerdict {
        success: true,
        failure: None,
        trailing_transport_anomaly: end == StreamEnd::TransportError,
        trailing_protocol_anomaly: first_protocol_failure.map(|(_, failure)| failure),
    }
}

fn terminal_failure(failure: TerminalFailure) -> TerminalVerdict {
    TerminalVerdict {
        success: false,
        failure: Some(failure),
        trailing_transport_anomaly: false,
        trailing_protocol_anomaly: None,
    }
}

/// Cheaply identifies chunks that must be ingested before they are forwarded.
///
/// This is deliberately only a candidate detector. A `true` result means the
/// caller must establish its short-lived terminal-publication protection
/// before forwarding the chunk. It does not mean that a terminal frame was
/// found: after forwarding, [`ResponsesStreamState::ingest`] performs JSON
/// validation and exact event matching, so a false-positive candidate guard
/// can be released immediately without moving full SSE parsing onto the relay
/// hot path.
#[derive(Debug, Clone)]
pub(super) struct TerminalPrecheckGuard {
    marker: &'static [u8],
    matched_marker_bytes: usize,
    candidate_frame_open: bool,
    line_has_bytes: bool,
    pending_cr: bool,
    escape_pending: bool,
    unicode_escape_digits: u8,
    unicode_escape_value: u16,
}

impl TerminalPrecheckGuard {
    pub fn new(channel: &Channel) -> Self {
        let marker: &'static [u8] = match channel {
            Channel::Responses => b"response.completed",
            Channel::Anthropic => b"message_stop",
            Channel::Chat => b"[DONE]",
        };
        Self {
            marker,
            matched_marker_bytes: 0,
            candidate_frame_open: false,
            line_has_bytes: false,
            pending_cr: false,
            escape_pending: false,
            unicode_escape_digits: 0,
            unicode_escape_value: 0,
        }
    }

    /// Returns whether this chunk needs exact terminal prechecking.
    ///
    /// The ASCII match is continuous across chunk boundaries. Once a marker
    /// is seen, the guard stays on the precheck path through the end of that
    /// SSE frame, so a frame delimiter arriving in a later chunk is still
    /// ingested before it is forwarded.
    pub fn chunk_requires_precheck(&mut self, chunk: &[u8]) -> bool {
        let mut requires_precheck = self.candidate_frame_open;
        for &byte in chunk {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }

            match byte {
                b'\r' => {
                    self.finish_line();
                    self.pending_cr = true;
                    self.matched_marker_bytes = 0;
                }
                b'\n' => {
                    self.finish_line();
                    self.matched_marker_bytes = 0;
                }
                _ => {
                    self.line_has_bytes = true;
                    if self
                        .decoded_marker_byte(byte)
                        .is_some_and(|byte| self.advance_marker(byte))
                    {
                        self.candidate_frame_open = true;
                        requires_precheck = true;
                    }
                }
            }
        }
        requires_precheck
    }

    /// JSON permits ASCII event names to be written with `\uXXXX` escapes.
    /// Decode only those tiny escape sequences for candidate matching; exact
    /// JSON validity and terminal semantics remain the full parser's job.
    fn decoded_marker_byte(&mut self, byte: u8) -> Option<u8> {
        if self.unicode_escape_digits > 0 {
            let Some(nibble) = ascii_hex_value(byte) else {
                self.unicode_escape_digits = 0;
                self.unicode_escape_value = 0;
                self.matched_marker_bytes = 0;
                return None;
            };
            self.unicode_escape_value = (self.unicode_escape_value << 4) | u16::from(nibble);
            self.unicode_escape_digits += 1;
            if self.unicode_escape_digits <= 4 {
                return None;
            }
            let value = self.unicode_escape_value;
            self.unicode_escape_digits = 0;
            self.unicode_escape_value = 0;
            if value <= u16::from(u8::MAX) {
                return Some(value as u8);
            }
            self.matched_marker_bytes = 0;
            return None;
        }

        if self.escape_pending {
            self.escape_pending = false;
            if byte == b'u' {
                self.unicode_escape_digits = 1;
                self.unicode_escape_value = 0;
            } else {
                self.matched_marker_bytes = 0;
            }
            return None;
        }
        if byte == b'\\' {
            self.escape_pending = true;
            return None;
        }
        Some(byte)
    }

    fn advance_marker(&mut self, byte: u8) -> bool {
        if byte == self.marker[self.matched_marker_bytes] {
            self.matched_marker_bytes += 1;
        } else if byte == self.marker[0] {
            self.matched_marker_bytes = 1;
        } else {
            self.matched_marker_bytes = 0;
        }

        if self.matched_marker_bytes == self.marker.len() {
            self.matched_marker_bytes = 0;
            true
        } else {
            false
        }
    }

    fn finish_line(&mut self) {
        if !self.line_has_bytes {
            self.candidate_frame_open = false;
        }
        self.line_has_bytes = false;
        self.escape_pending = false;
        self.unicode_escape_digits = 0;
        self.unicode_escape_value = 0;
    }
}

fn ascii_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct ResponsesStreamState {
    decoder: sse::SseFrameDecoder,
    summary: StreamSummary,
    next_sequence: u64,
}

impl ResponsesStreamState {
    #[cfg(test)]
    fn with_max_frame_bytes(max_frame_bytes: usize) -> Self {
        Self {
            decoder: sse::SseFrameDecoder::with_max_frame_bytes(max_frame_bytes),
            summary: StreamSummary::default(),
            next_sequence: 0,
        }
    }

    pub fn ingest(&mut self, chunk: &[u8]) -> StreamObservation {
        let output_seen_before = self.summary.model_output_seen;
        for event in self.decoder.push_ordered(chunk) {
            let sequence = self.next_event_sequence();
            match event {
                sse::SseDecodeEvent::Frame(frame) => self.process_frame(frame, sequence),
                sse::SseDecodeEvent::FrameOverflow => {
                    self.summary.frame_overflowed = true;
                    self.summary.frame_overflow_sequence.get_or_insert(sequence);
                }
            }
        }
        StreamObservation {
            model_output_started: !output_seen_before && self.summary.model_output_seen,
            completed_event_seen: self.summary.completed_event_seen,
            responses_completed_event_seen: self.summary.responses_completed_event_seen,
            message_stop_event_seen: self.summary.message_stop_event_seen,
            done_marker_seen: self.summary.done_marker_seen,
        }
    }

    pub fn finish(mut self) -> StreamSummary {
        for frame in self.decoder.finish() {
            let sequence = self.next_event_sequence();
            self.process_frame(frame, sequence);
        }
        self.summary.frame_overflowed |= self.decoder.overflowed();
        self.summary
    }

    fn next_event_sequence(&mut self) -> u64 {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.next_sequence
    }

    fn process_frame(&mut self, frame: sse::SseFrame, sequence: u64) {
        let payload = frame.data.trim();
        if payload.is_empty() {
            return;
        }
        if payload == "[DONE]" {
            self.summary.done_marker_seen = true;
            self.summary.done_marker_sequence.get_or_insert(sequence);
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };

        // An SSE event line is metadata for this complete payload, not a
        // terminal on its own. Parsing first prevents `event:
        // response.completed` plus malformed data from being settled as a
        // successful response.
        if let Some(event) = frame.event.as_deref() {
            self.process_event_type(event, sequence);
        }
        let payload_event_type = value.get("type").and_then(Value::as_str);
        if let Some(event_type) = payload_event_type {
            self.process_event_type(event_type, sequence);
        }
        let effective_event_type = payload_event_type.or(frame.event.as_deref());
        let frame_has_error = value.get("error").is_some_and(|error| !error.is_null())
            || frame.event.as_deref().is_some_and(is_error_event_type)
            || payload_event_type.is_some_and(is_error_event_type);
        self.capture_output_items(&value, effective_event_type);
        self.summary.model_output_seen |= value_has_model_output(&value);
        self.summary.compaction_output_seen |= value_has_compaction_output(&value);
        if value.get("error").is_some_and(|error| !error.is_null()) {
            self.summary.error_event_seen = true;
            self.summary.error_event_sequence.get_or_insert(sequence);
        }
        if frame_has_error {
            self.summary
                .cache_capability_rejection_fields
                .extend(cache_capability_rejection_fields_from_value(&value));
        }
        if self.summary.error_event_seen && self.summary.error_summary.is_none() {
            // Keep a small extra in-memory window so the relay owner can
            // redact a caller-owned cache key before applying the persisted
            // summary limit. This value never leaves the relay unredacted.
            let summary = super::upstream_error_summary_from_value_for_client_key_redaction(&value);
            if !summary.is_empty() {
                self.summary.error_summary = Some(summary);
            }
        }
        self.summary
            .usage
            .merge_provider_snapshot(provider_usage_from_value(&value));
        if let Some(id) = response_id_from_value(&value) {
            self.summary.response_id = Some(id);
        }
    }

    fn process_event_type(&mut self, event_type: &str, sequence: u64) {
        if event_type == "response.completed" {
            self.summary.responses_completed_event_seen = true;
            self.summary
                .responses_completed_sequence
                .get_or_insert(sequence);
        }
        if event_type == "message_stop" {
            self.summary.message_stop_event_seen = true;
            self.summary.message_stop_sequence.get_or_insert(sequence);
        }
        self.summary.completed_event_seen =
            self.summary.responses_completed_event_seen || self.summary.message_stop_event_seen;
        if is_error_event_type(event_type) {
            self.summary.error_event_seen = true;
            self.summary.error_event_sequence.get_or_insert(sequence);
        }
    }

    fn capture_output_items(&mut self, value: &Value, event_type: Option<&str>) {
        if event_type == Some("response.completed") {
            if let Some(items) = value
                .get("response")
                .and_then(|response| response.get("output"))
                .or_else(|| value.get("output"))
                .and_then(Value::as_array)
            {
                self.summary.output_items = items.clone();
                return;
            }
        }
        if event_type != Some("response.output_item.done") {
            return;
        }
        let Some(item) = value.get("item") else {
            return;
        };
        let duplicate = self.summary.output_items.iter().any(|existing| {
            let existing_id = existing.get("id").and_then(Value::as_str);
            let item_id = item.get("id").and_then(Value::as_str);
            (existing_id.is_some() && existing_id == item_id) || existing == item
        });
        if !duplicate {
            self.summary.output_items.push(item.clone());
        }
    }
}

pub(super) fn value_has_compaction_output(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("compaction") {
                return true;
            }
            ["item", "output", "response", "data"]
                .into_iter()
                .filter_map(|key| map.get(key))
                .any(value_has_compaction_output)
        }
        Value::Array(items) => items.iter().any(value_has_compaction_output),
        _ => false,
    }
}

fn is_error_event_type(kind: &str) -> bool {
    matches!(
        kind,
        "error" | "response.failed" | "response.incomplete" | "message_delta_error"
    ) || kind.ends_with(".failed")
        || kind.ends_with(".incomplete")
        || kind.ends_with(".error")
}

pub(super) fn value_has_model_output(value: &Value) -> bool {
    if let Value::Array(items) = value {
        return items.iter().any(value_has_model_output);
    }

    let event_type = value.get("type").and_then(Value::as_str);
    if event_type.is_some_and(|kind| kind.ends_with(".delta"))
        && value.get("delta").is_some_and(delta_has_content)
    {
        return true;
    }
    if event_type == Some("content_block_delta")
        && value.get("delta").is_some_and(delta_has_content)
    {
        return true;
    }

    if event_type == Some("output_text")
        && value
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
    {
        return true;
    }

    if event_type == Some("message") && value.get("content").is_some_and(value_has_model_output) {
        return true;
    }

    value
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| choices.iter().any(value_has_model_output))
        || ["item", "output", "response", "data", "content"]
            .into_iter()
            .filter_map(|key| value.get(key))
            .any(value_has_model_output)
}

fn delta_has_content(value: &Value) -> bool {
    match value {
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => map.values().any(delta_has_content),
        Value::Number(_) | Value::Bool(_) => true,
        Value::Null => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_usage_response_id_and_terminal_state_incrementally() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"event: response.completed\n");
        state.ingest(
            br#"data: {"type":"response.completed","response":{"id":"resp_stream","usage":{"input_tokens":20,"output_tokens":2,"input_tokens_details":{"cached_tokens":19}}}}"#,
        );
        state.ingest(b"\n\ndata: [DONE]\n\n");

        let summary = state.finish();
        assert_eq!(summary.response_id.as_deref(), Some("resp_stream"));
        assert!(summary.completed_event_seen);
        assert!(summary.done_marker_seen);
        assert_eq!(summary.usage.input_tokens, 20);
        assert_eq!(summary.usage.cache_read_tokens, 19);
        assert_eq!(summary.usage.output_tokens, 2);
    }

    #[test]
    fn repeated_cumulative_usage_snapshots_are_not_double_counted() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_usage\",\"usage\":{\"input_tokens\":20,\"output_tokens\":0,\"input_tokens_details\":{\"cached_tokens\":16}}}}\n\n",
        );
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_usage\",\"usage\":{\"input_tokens\":20,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":16}}}}\n\n",
        );

        let summary = state.finish();
        assert_eq!(summary.usage.input_tokens, 20);
        assert_eq!(summary.usage.cache_read_tokens, 16);
        assert_eq!(summary.usage.output_tokens, 2);
    }

    #[test]
    fn prefers_nonzero_responses_cached_tokens_over_zero_chat_compatibility_field() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            br#"data: {"type":"response.completed","response":{"id":"resp_real_shape","usage":{"prompt_tokens":0,"prompt_tokens_details":{"cached_tokens":0},"input_tokens":11261,"output_tokens":5,"input_tokens_details":{"cached_tokens":10752}}}}"#,
        );
        state.ingest(b"\n\n");

        let summary = state.finish();
        assert_eq!(summary.usage.input_tokens, 11_261);
        assert_eq!(summary.usage.cache_read_tokens, 10_752);
        assert_eq!(summary.usage.output_tokens, 5);
    }

    #[test]
    fn detects_compaction_output_in_output_item_event() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque\"}}\n\n",
        );
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_compact\"}}\n\n",
        );

        let summary = state.finish();
        assert!(summary.compaction_output_seen);
        assert!(summary.completed_event_seen);
    }

    #[test]
    fn captures_complete_output_lineage_without_dropping_call_fields() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            br#"data: {"type":"response.output_item.done","item":{"type":"function_call","id":"fc-1","status":"completed","call_id":"call-1","name":"read_file","arguments":"{\"path\":\"README.md\"}","vendor":{"kept":true}}}

"#,
        );
        state.ingest(
            br#"data: {"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","id":"fc-1","status":"completed","call_id":"call-1","name":"read_file","arguments":"{\"path\":\"README.md\"}","vendor":{"kept":true}}]}}

"#,
        );

        let summary = state.finish();
        assert_eq!(summary.output_items.len(), 1);
        assert_eq!(summary.output_items[0]["call_id"], "call-1");
        assert_eq!(summary.output_items[0]["status"], "completed");
        assert_eq!(summary.output_items[0]["vendor"]["kept"], true);
    }

    #[test]
    fn captures_top_level_response_output_when_event_header_marks_completion() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            br#"event: response.completed
data: {"id":"resp-top-level","object":"response","output":[{"type":"function_call","id":"fc-top","status":"completed","call_id":"call-top","name":"probe","arguments":"{}"}]}

"#,
        );

        let summary = state.finish();
        assert!(summary.responses_completed_event_seen);
        assert_eq!(summary.response_id.as_deref(), Some("resp-top-level"));
        assert_eq!(summary.output_items.len(), 1);
        assert_eq!(summary.output_items[0]["call_id"], "call-top");
    }

    #[test]
    fn ordinary_output_does_not_look_like_compaction() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[]}}\n\n",
        );

        assert!(!state.finish().compaction_output_seen);
    }

    #[test]
    fn distinguishes_metadata_from_first_model_output() {
        let mut state = ResponsesStreamState::default();
        let created = state
            .ingest(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp\"}}\n\n");
        assert!(!created.model_output_started);

        let output = state
            .ingest(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n");
        assert!(output.model_output_started);
        assert!(
            !state
                .ingest(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"again\"}\n\n")
                .model_output_started
        );
        assert!(state.finish().model_output_seen);
    }

    #[test]
    fn stream_observation_reports_terminal_events_immediately() {
        let mut responses = ResponsesStreamState::default();
        let completed = responses
            .ingest(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp\"}}\n\n");
        assert!(completed.completed_event_seen);
        assert!(!completed.done_marker_seen);

        let mut chat = ResponsesStreamState::default();
        let done = chat.ingest(b"data: [DONE]\n\n");
        assert!(!done.completed_event_seen);
        assert!(done.done_marker_seen);
    }

    #[test]
    fn terminal_precheck_guard_routes_each_native_terminal_to_exact_ingest() {
        let mut responses_guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut responses = ResponsesStreamState::default();
        let responses_chunk =
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp\"}}\n\n";
        assert!(responses_guard.chunk_requires_precheck(responses_chunk));
        assert!(
            responses
                .ingest(responses_chunk)
                .responses_completed_event_seen
        );

        let mut chat_guard = TerminalPrecheckGuard::new(&Channel::Chat);
        let mut chat = ResponsesStreamState::default();
        let chat_chunk = b"data: [DONE]\n\n";
        assert!(chat_guard.chunk_requires_precheck(chat_chunk));
        assert!(chat.ingest(chat_chunk).done_marker_seen);

        let mut anthropic_guard = TerminalPrecheckGuard::new(&Channel::Anthropic);
        let mut anthropic = ResponsesStreamState::default();
        let anthropic_chunk = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        assert!(anthropic_guard.chunk_requires_precheck(anthropic_chunk));
        assert!(anthropic.ingest(anthropic_chunk).message_stop_event_seen);
    }

    #[test]
    fn terminal_precheck_guard_does_not_promote_markers_inside_ordinary_text() {
        let mut responses_guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut responses = ResponsesStreamState::default();
        let responses_text =
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n";
        assert!(responses_guard.chunk_requires_precheck(responses_text));
        let observation = responses.ingest(responses_text);
        assert!(observation.model_output_started);
        assert!(!observation.responses_completed_event_seen);
        assert!(!responses_guard.chunk_requires_precheck(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ordinary\"}\n\n"
        ));

        let mut chat_guard = TerminalPrecheckGuard::new(&Channel::Chat);
        let mut chat = ResponsesStreamState::default();
        let chat_text = b"data: {\"choices\":[{\"delta\":{\"content\":\"[DONE]\"}}]}\n\n";
        assert!(chat_guard.chunk_requires_precheck(chat_text));
        assert!(!chat.ingest(chat_text).done_marker_seen);

        let mut anthropic_guard = TerminalPrecheckGuard::new(&Channel::Anthropic);
        let mut anthropic = ResponsesStreamState::default();
        let anthropic_text = b"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"message_stop\"}}\n\n";
        assert!(anthropic_guard.chunk_requires_precheck(anthropic_text));
        assert!(!anthropic.ingest(anthropic_text).message_stop_event_seen);
    }

    #[test]
    fn terminal_precheck_guard_does_not_promote_malformed_terminal_data() {
        let mut guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut state = ResponsesStreamState::default();
        let malformed = b"event: response.completed\ndata: not-json\n\n";

        assert!(guard.chunk_requires_precheck(malformed));
        let observation = state.ingest(malformed);
        assert!(!observation.responses_completed_event_seen);
        assert!(!observation.completed_event_seen);
    }

    #[test]
    fn terminal_precheck_guard_keeps_split_candidate_on_slow_path_until_frame_end() {
        let mut guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut state = ResponsesStreamState::default();

        let prefix = b"data: {\"type\":\"response.com";
        assert!(!guard.chunk_requires_precheck(prefix));
        assert!(!state.ingest(prefix).responses_completed_event_seen);

        let marker_suffix = b"pleted\",\"response\":{}}";
        assert!(guard.chunk_requires_precheck(marker_suffix));
        assert!(!state.ingest(marker_suffix).responses_completed_event_seen);

        let frame_end = b"\n\n";
        assert!(guard.chunk_requires_precheck(frame_end));
        assert!(state.ingest(frame_end).responses_completed_event_seen);

        assert!(!guard.chunk_requires_precheck(b"data: {\"type\":\"response.created\"}\n\n"));
    }

    #[test]
    fn terminal_precheck_guard_covers_json_unicode_escaped_event_names() {
        let mut guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut state = ResponsesStreamState::default();
        let escaped = br#"data: {"type":"response\u002ecompleted","response":{"id":"resp"}}

"#;

        assert!(guard.chunk_requires_precheck(escaped));
        assert!(state.ingest(escaped).responses_completed_event_seen);

        let mut split_guard = TerminalPrecheckGuard::new(&Channel::Responses);
        let mut split_state = ResponsesStreamState::default();
        let first = br#"data: {"type":"response\u00"#;
        let second = b"2ecompleted\",\"response\":{}}\n\n";
        assert!(!split_guard.chunk_requires_precheck(first));
        assert!(!split_state.ingest(first).responses_completed_event_seen);
        assert!(split_guard.chunk_requires_precheck(second));
        assert!(split_state.ingest(second).responses_completed_event_seen);
    }

    #[test]
    fn detects_model_output_in_complete_response_json() {
        assert!(value_has_model_output(&serde_json::json!({
            "id": "resp_summary",
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "compacted summary"}]
            }]
        })));
    }

    #[test]
    fn records_error_events_without_treating_done_as_completed() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"data: {\"type\":\"response.failed\",\"error\":{\"message\":\"bad\"}}\n\n");
        state.ingest(b"data: [DONE]\n\n");

        let summary = state.finish();
        assert!(summary.error_event_seen);
        assert_eq!(summary.error_summary.as_deref(), Some("bad"));
        assert!(summary.done_marker_seen);
        assert!(!summary.completed_event_seen);
    }

    #[test]
    fn preserves_split_utf8_before_parsing_sse_json() {
        let mut state = ResponsesStreamState::default();
        let event = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"你\"}\n\n";
        let split_at = event.find('你').unwrap() + 1;

        let first = state.ingest(&event.as_bytes()[..split_at]);
        assert!(!first.model_output_started);
        let second = state.ingest(&event.as_bytes()[split_at..]);

        assert!(second.model_output_started);
    }

    #[test]
    fn parses_crlf_multiline_data_as_one_complete_frame() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"event: response.completed\r\ndata: {\"type\":\"response.completed\",\r\ndata: \"response\":{\"id\":\"resp_crlf\"}}\r\n\r\n",
        );

        let summary = state.finish();
        assert!(summary.completed_event_seen);
        assert_eq!(summary.response_id.as_deref(), Some("resp_crlf"));
    }

    #[test]
    fn ordinary_text_containing_terminal_names_does_not_complete_stream() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed and message_stop are plain text\"}\n\n",
        );

        let summary = state.finish();
        assert!(summary.model_output_seen);
        assert!(!summary.completed_event_seen);
    }

    #[test]
    fn truncated_terminal_frame_at_eof_is_not_accepted() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"data: {\"type\":\"response.completed\",\"response\":{}");

        let summary = state.finish();
        assert!(!summary.completed_event_seen);
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::IncompleteEof)
        );
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::TransportError,
            ),
            terminal_failure(TerminalFailure::TransportErrorBeforeTerminal)
        );
    }

    #[test]
    fn event_only_terminal_does_not_complete_stream() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"event: response.completed\n\n");

        let summary = state.finish();
        assert!(!summary.responses_completed_event_seen);
        assert!(!summary.completed_event_seen);
    }

    #[test]
    fn malformed_data_after_terminal_event_does_not_complete_stream() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"event: response.completed\ndata: not-json\n\n");

        let summary = state.finish();
        assert!(!summary.responses_completed_event_seen);
        assert!(!summary.completed_event_seen);
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::IncompleteEof)
        );
    }

    #[test]
    fn null_error_field_does_not_turn_completion_into_failure() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"error\":null,\"response\":{\"id\":\"resp_null_error\"}}\n\n",
        );

        let summary = state.finish();
        assert!(!summary.error_event_seen);
        assert!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            )
            .success
        );
    }

    #[test]
    fn oversized_frame_before_valid_completion_remains_a_failure() {
        let mut state = ResponsesStreamState::with_max_frame_bytes(128);
        let mut oversized = b"data: ".to_vec();
        oversized.extend(std::iter::repeat(b'x').take(160));
        state.ingest(&oversized);
        state.ingest(
            b"\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_after_overflow\"}}\n\n",
        );

        let summary = state.finish();
        assert!(summary.frame_overflowed);
        assert!(summary.completed_event_seen);
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::FrameTooLarge)
        );
    }

    #[test]
    fn oversized_frame_after_valid_completion_is_a_trailing_anomaly() {
        let mut state = ResponsesStreamState::with_max_frame_bytes(128);
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_before_overflow\"}}\n\n",
        );
        let mut oversized = b"data: ".to_vec();
        oversized.extend(std::iter::repeat(b'x').take(160));
        oversized.extend_from_slice(b"\n\n");
        state.ingest(&oversized);

        let summary = state.finish();
        assert!(summary.frame_overflowed);
        assert!(summary.completed_event_seen);
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            TerminalVerdict {
                success: true,
                failure: None,
                trailing_transport_anomaly: false,
                trailing_protocol_anomaly: Some(TerminalFailure::FrameTooLarge),
            }
        );
    }

    #[test]
    fn error_after_valid_completion_is_a_trailing_anomaly() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_before_error\"}}\n\n",
        );
        state.ingest(
            b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"late\"}}}\n\n",
        );

        let summary = state.finish();
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            TerminalVerdict {
                success: true,
                failure: None,
                trailing_transport_anomaly: false,
                trailing_protocol_anomaly: Some(TerminalFailure::ErrorEvent),
            }
        );
    }

    #[test]
    fn error_before_completion_remains_a_failure() {
        let mut state = ResponsesStreamState::default();
        state.ingest(
            b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"first\"}}}\n\n",
        );
        state.ingest(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_after_error\"}}\n\n",
        );

        let summary = state.finish();
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::ErrorEvent)
        );
    }

    #[test]
    fn native_responses_requires_completed_event() {
        let done_only = StreamSummary {
            done_marker_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &done_only,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::IncompleteEof)
        );

        let completed = StreamSummary {
            responses_completed_event_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &completed,
                StreamEnd::CleanEof,
            ),
            TerminalVerdict {
                success: true,
                failure: None,
                trailing_transport_anomaly: false,
                trailing_protocol_anomaly: None,
            }
        );
    }

    #[test]
    fn responses_done_at_eof_compatibility_requires_clean_eof() {
        let summary = StreamSummary {
            done_marker_seen: true,
            ..StreamSummary::default()
        };
        assert!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::ResponsesDoneAtEof,
                &summary,
                StreamEnd::CleanEof,
            )
            .success
        );
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::ResponsesDoneAtEof,
                &summary,
                StreamEnd::TransportError,
            ),
            terminal_failure(TerminalFailure::TransportErrorBeforeTerminal)
        );
    }

    #[test]
    fn chat_and_anthropic_use_their_native_terminal_markers() {
        let chat = StreamSummary {
            done_marker_seen: true,
            ..StreamSummary::default()
        };
        assert!(
            evaluate_terminal(
                &Channel::Chat,
                TerminalCompatibility::Strict,
                &chat,
                StreamEnd::CleanEof,
            )
            .success
        );

        let anthropic = StreamSummary {
            message_stop_event_seen: true,
            ..StreamSummary::default()
        };
        assert!(
            evaluate_terminal(
                &Channel::Anthropic,
                TerminalCompatibility::Strict,
                &anthropic,
                StreamEnd::CleanEof,
            )
            .success
        );
    }

    #[test]
    fn terminal_events_are_not_interchangeable_between_protocols() {
        let responses = StreamSummary {
            completed_event_seen: true,
            responses_completed_event_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Anthropic,
                TerminalCompatibility::Strict,
                &responses,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::IncompleteEof)
        );

        let anthropic = StreamSummary {
            completed_event_seen: true,
            message_stop_event_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &anthropic,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::IncompleteEof)
        );
    }

    #[test]
    fn error_event_fails_even_when_a_terminal_marker_is_present() {
        let summary = StreamSummary {
            completed_event_seen: true,
            responses_completed_event_seen: true,
            error_event_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::CleanEof,
            ),
            terminal_failure(TerminalFailure::ErrorEvent)
        );
    }

    #[test]
    fn transport_error_after_terminal_is_a_successful_trailing_anomaly() {
        let summary = StreamSummary {
            completed_event_seen: true,
            responses_completed_event_seen: true,
            ..StreamSummary::default()
        };
        assert_eq!(
            evaluate_terminal(
                &Channel::Responses,
                TerminalCompatibility::Strict,
                &summary,
                StreamEnd::TransportError,
            ),
            TerminalVerdict {
                success: true,
                failure: None,
                trailing_transport_anomaly: true,
                trailing_protocol_anomaly: None,
            }
        );
    }

    #[test]
    fn transport_error_before_terminal_is_a_failure() {
        assert_eq!(
            evaluate_terminal(
                &Channel::Anthropic,
                TerminalCompatibility::Strict,
                &StreamSummary::default(),
                StreamEnd::TransportError,
            ),
            terminal_failure(TerminalFailure::TransportErrorBeforeTerminal)
        );
    }
}
