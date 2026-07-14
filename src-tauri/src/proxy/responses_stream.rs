use serde_json::Value;

use crate::{config::Channel, metrics::UsageRecord};

use super::{provider_usage_from_value, response_id_from_value, sse};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct StreamObservation {
    pub model_output_started: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct StreamSummary {
    pub usage: UsageRecord,
    pub response_id: Option<String>,
    pub completed_event_seen: bool,
    pub done_marker_seen: bool,
    pub error_event_seen: bool,
    pub compaction_output_seen: bool,
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
    if summary.error_event_seen {
        return terminal_failure(TerminalFailure::ErrorEvent);
    }

    let strict_terminal_seen = match channel {
        Channel::Responses | Channel::Anthropic => summary.completed_event_seen,
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
    pending_line: String,
    utf8_remainder: Vec<u8>,
    summary: StreamSummary,
    model_output_seen: bool,
}

impl ResponsesStreamState {
    pub fn ingest(&mut self, chunk: &[u8]) -> StreamObservation {
        let output_seen_before = self.model_output_seen;
        sse::append_utf8_safe(&mut self.pending_line, &mut self.utf8_remainder, chunk);
        while let Some(newline_index) = self.pending_line.find('\n') {
            let line = self.pending_line[..newline_index]
                .trim_end_matches('\r')
                .to_string();
            self.pending_line.drain(..=newline_index);
            self.process_line(&line);
        }
        if self.pending_line.len() > 1_048_576 {
            self.pending_line.clear();
        }
        StreamObservation {
            model_output_started: !output_seen_before && self.model_output_seen,
        }
    }

    pub fn finish(mut self) -> StreamSummary {
        if !self.utf8_remainder.is_empty() {
            self.pending_line
                .push_str(&String::from_utf8_lossy(&self.utf8_remainder));
        }
        if !self.pending_line.is_empty() {
            let line = std::mem::take(&mut self.pending_line);
            self.process_line(line.trim_end_matches('\r'));
        }
        self.summary
    }

    fn process_line(&mut self, line: &str) {
        let Some(payload) = line.trim_start().strip_prefix("data:") else {
            if line.contains("message_stop") {
                self.summary.completed_event_seen = true;
            }
            return;
        };
        let payload = payload.trim();
        if payload.is_empty() {
            return;
        }
        if payload == "[DONE]" {
            self.summary.done_marker_seen = true;
            return;
        }
        if payload.contains("response.completed") || payload.contains("message_stop") {
            self.summary.completed_event_seen = true;
        }
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            self.model_output_seen |= value_has_model_output(&value);
            self.summary.compaction_output_seen |= value_has_compaction_output(&value);
            if value.get("error").is_some()
                || value
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(is_error_event_type)
            {
                self.summary.error_event_seen = true;
            }
            self.summary.usage.merge(provider_usage_from_value(&value));
            if let Some(id) = response_id_from_value(&value) {
                self.summary.response_id = Some(id);
            }
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

fn value_has_model_output(value: &Value) -> bool {
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

    value
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| {
            choices
                .iter()
                .any(|choice| choice.get("delta").is_some_and(delta_has_content))
        })
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
    }

    #[test]
    fn records_error_events_without_treating_done_as_completed() {
        let mut state = ResponsesStreamState::default();
        state.ingest(b"data: {\"type\":\"response.failed\",\"error\":{\"message\":\"bad\"}}\n\n");
        state.ingest(b"data: [DONE]\n\n");

        let summary = state.finish();
        assert!(summary.error_event_seen);
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
            completed_event_seen: true,
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
            completed_event_seen: true,
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
    fn error_event_fails_even_when_a_terminal_marker_is_present() {
        let summary = StreamSummary {
            completed_event_seen: true,
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
