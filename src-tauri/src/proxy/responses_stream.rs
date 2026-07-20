use serde_json::Value;

use crate::{config::Channel, metrics::UsageRecord};

use super::{provider_usage_from_value, response_id_from_value, sse};

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
    pub completed_event_seen: bool,
    pub responses_completed_event_seen: bool,
    pub message_stop_event_seen: bool,
    pub done_marker_seen: bool,
    pub error_event_seen: bool,
    pub error_summary: Option<String>,
    pub compaction_output_seen: bool,
    pub model_output_seen: bool,
    pub frame_overflowed: bool,
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
}

pub(super) fn evaluate_terminal(
    channel: &Channel,
    compatibility: TerminalCompatibility,
    summary: &StreamSummary,
    end: StreamEnd,
) -> TerminalVerdict {
    if summary.frame_overflowed {
        return terminal_failure(TerminalFailure::FrameTooLarge);
    }
    if summary.error_event_seen {
        return terminal_failure(TerminalFailure::ErrorEvent);
    }

    let strict_terminal_seen = match channel {
        Channel::Responses => summary.responses_completed_event_seen,
        Channel::Anthropic => summary.message_stop_event_seen,
        Channel::Chat => summary.done_marker_seen,
    };
    let compatible_terminal_seen = strict_terminal_seen
        || (matches!(channel, Channel::Responses)
            && compatibility == TerminalCompatibility::ResponsesDoneAtEof
            && end == StreamEnd::CleanEof
            && summary.done_marker_seen);

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
    }
}

fn terminal_failure(failure: TerminalFailure) -> TerminalVerdict {
    TerminalVerdict {
        success: false,
        failure: Some(failure),
        trailing_transport_anomaly: false,
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct ResponsesStreamState {
    decoder: sse::SseFrameDecoder,
    summary: StreamSummary,
}

impl ResponsesStreamState {
    #[cfg(test)]
    fn with_max_frame_bytes(max_frame_bytes: usize) -> Self {
        Self {
            decoder: sse::SseFrameDecoder::with_max_frame_bytes(max_frame_bytes),
            summary: StreamSummary::default(),
        }
    }

    pub fn ingest(&mut self, chunk: &[u8]) -> StreamObservation {
        let output_seen_before = self.summary.model_output_seen;
        for frame in self.decoder.push(chunk) {
            self.process_frame(frame);
        }
        self.summary.frame_overflowed |= self.decoder.overflowed();
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
            self.process_frame(frame);
        }
        self.summary.frame_overflowed |= self.decoder.overflowed();
        self.summary
    }

    fn process_frame(&mut self, frame: sse::SseFrame) {
        let payload = frame.data.trim();
        if payload.is_empty() {
            return;
        }
        if payload == "[DONE]" {
            self.summary.done_marker_seen = true;
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
            self.process_event_type(event);
        }
        let event_type = value.get("type").and_then(Value::as_str);
        if let Some(event_type) = event_type {
            self.process_event_type(event_type);
        }
        self.summary.model_output_seen |= value_has_model_output(&value);
        self.summary.compaction_output_seen |= value_has_compaction_output(&value);
        if value.get("error").is_some_and(|error| !error.is_null()) {
            self.summary.error_event_seen = true;
        }
        if self.summary.error_event_seen && self.summary.error_summary.is_none() {
            let summary = super::upstream_error_summary_from_value(&value);
            if !summary.is_empty() {
                self.summary.error_summary = Some(summary);
            }
        }
        self.summary.usage.merge(provider_usage_from_value(&value));
        if let Some(id) = response_id_from_value(&value) {
            self.summary.response_id = Some(id);
        }
    }

    fn process_event_type(&mut self, event_type: &str) {
        self.summary.responses_completed_event_seen |= event_type == "response.completed";
        self.summary.message_stop_event_seen |= event_type == "message_stop";
        self.summary.completed_event_seen =
            self.summary.responses_completed_event_seen || self.summary.message_stop_event_seen;
        self.summary.error_event_seen |= is_error_event_type(event_type);
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
    fn oversized_frame_is_a_failure_even_after_a_valid_completion() {
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
