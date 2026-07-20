//! OpenAI Chat Completions SSE -> Anthropic Messages SSE conversion.
//!
//! This adapter intentionally keeps the raw Chat terminal contract: a Chat
//! `finish_reason` is only a pending terminal; an upstream `[DONE]` marker is
//! required before it can emit Anthropic `message_stop`.  That prevents a
//! truncated Chat stream from being recorded as a successful Anthropic reply.

use super::{provider_usage_from_value, sse::SseFrameDecoder};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub(crate) struct ChatToAnthropicStreamSummary {
    pub usage: crate::metrics::UsageRecord,
    pub response_id: Option<String>,
    pub compaction_output_seen: bool,
    pub model_output_seen: bool,
}

pub(crate) type ChatToAnthropicStreamSummaryHandle = Arc<Mutex<ChatToAnthropicStreamSummary>>;

#[derive(Debug)]
struct TextBlock {
    index: u64,
    closed: bool,
}

#[derive(Debug, Default)]
struct ToolBlock {
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Debug)]
struct ChatToAnthropicState {
    response_id: Option<String>,
    model: String,
    message_started: bool,
    finished: bool,
    succeeded: bool,
    done_seen: bool,
    finish_reason: Option<String>,
    next_block_index: u64,
    text: Option<TextBlock>,
    tools: BTreeMap<usize, ToolBlock>,
    usage: Map<String, Value>,
    model_output_seen: bool,
}

impl ChatToAnthropicState {
    fn new(fallback_model: String) -> Self {
        Self {
            response_id: None,
            model: fallback_model,
            message_started: false,
            finished: false,
            succeeded: false,
            done_seen: false,
            finish_reason: None,
            next_block_index: 0,
            text: None,
            tools: BTreeMap::new(),
            usage: Map::new(),
            model_output_seen: false,
        }
    }

    fn ingest(&mut self, event: Option<&str>, data: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        if data.trim() == "[DONE]" {
            self.done_seen = true;
            return self.complete_on_done();
        }
        if event == Some("error") {
            return match serde_json::from_str::<Value>(data) {
                Ok(value) => self.failed_events(Some(&value), "api_error"),
                Err(_) => self.failed_message_events(data, "api_error"),
            };
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return Vec::new();
        };
        if value.get("error").is_some_and(|error| !error.is_null())
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            return self.failed_events(Some(&value), "api_error");
        }
        self.observe_metadata(&value);

        let Some(choice) = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return Vec::new();
        };
        let mut events = Vec::new();
        if let Some(delta) = choice.get("delta") {
            if let Some(text) = delta.get("content").and_then(Value::as_str) {
                events.extend(self.append_text(text));
            }
            if let Some(refusal) = delta.get("refusal").and_then(Value::as_str) {
                // Anthropic Messages has no refusal content block. Preserve a
                // Chat refusal as visible assistant text so refusal-only
                // responses cannot become an empty successful message.
                events.extend(self.append_text(refusal));
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for (fallback_index, tool_call) in tool_calls.iter().enumerate() {
                    events.extend(self.ingest_tool_call(tool_call, fallback_index));
                }
            }
            if let Some(function_call) = delta.get("function_call") {
                events.extend(self.ingest_legacy_function_call(function_call));
            }
            // Chat reasoning fields intentionally do not become Anthropic
            // thinking blocks: an Anthropic thinking block requires a valid
            // upstream signature, and this adapter must never forge one.
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            if !reason.is_empty() {
                self.finish_reason = Some(reason.to_string());
            }
        }
        events
    }

    fn observe_metadata(&mut self, value: &Value) {
        if let Some(id) = value
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            self.response_id = Some(id.to_string());
        }
        if let Some(model) = value
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
        {
            self.model = model.to_string();
        }
        self.merge_usage(value.get("usage").or_else(|| value.pointer("/x_gpt/usage")));
    }

    fn merge_usage(&mut self, usage: Option<&Value>) {
        let Some(usage) = usage.and_then(Value::as_object) else {
            return;
        };
        for key in [
            "input_tokens",
            "prompt_tokens",
            "output_tokens",
            "completion_tokens",
            "total_tokens",
            "input_tokens_details",
            "prompt_tokens_details",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ] {
            if let Some(value) = usage.get(key) {
                self.usage.insert(key.to_string(), value.clone());
            }
        }
    }

    fn ensure_message_started(&mut self) -> Vec<Bytes> {
        if self.message_started {
            return Vec::new();
        }
        self.message_started = true;
        let response_id = self
            .response_id
            .clone()
            .unwrap_or_else(|| format!("chatcmpl_{}", Uuid::new_v4().simple()));
        let message_id = if response_id.starts_with("msg_") {
            response_id
        } else {
            format!("msg_{response_id}")
        };
        vec![anthropic_event(
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    "usage": anthropic_start_usage(&self.usage),
                }
            }),
        )]
    }

    fn next_index(&mut self) -> u64 {
        let index = self.next_block_index;
        self.next_block_index = self.next_block_index.saturating_add(1);
        index
    }

    fn append_text(&mut self, text: &str) -> Vec<Bytes> {
        if text.is_empty() {
            return Vec::new();
        }
        let needs_start = self.text.as_ref().map_or(true, |block| block.closed);
        let mut events = if needs_start {
            let index = self.next_index();
            let mut events = self.ensure_message_started();
            events.push(anthropic_event(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": { "type": "text", "text": "" }
                }),
            ));
            self.text = Some(TextBlock {
                index,
                closed: false,
            });
            events
        } else {
            Vec::new()
        };
        let index = self.text.as_ref().map(|block| block.index).unwrap_or(0);
        events.push(anthropic_event(
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": index,
                "delta": { "type": "text_delta", "text": text }
            }),
        ));
        self.model_output_seen = true;
        events
    }

    fn close_text(&mut self) -> Vec<Bytes> {
        let Some(text) = self.text.as_mut() else {
            return Vec::new();
        };
        if text.closed {
            return Vec::new();
        }
        text.closed = true;
        vec![content_block_stop(text.index)]
    }

    fn ingest_tool_call(&mut self, value: &Value, fallback_index: usize) -> Vec<Bytes> {
        let tool_index = value
            .get("index")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(fallback_index);
        let call_id = value.get("id").and_then(Value::as_str);
        let function = value.get("function").unwrap_or(value);
        let name = function.get("name").and_then(Value::as_str);
        let arguments = function
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.update_tool(tool_index, call_id, name, arguments)
    }

    fn ingest_legacy_function_call(&mut self, value: &Value) -> Vec<Bytes> {
        self.update_tool(
            0,
            None,
            value.get("name").and_then(Value::as_str),
            value
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )
    }

    fn update_tool(
        &mut self,
        tool_index: usize,
        call_id: Option<&str>,
        name: Option<&str>,
        arguments: &str,
    ) -> Vec<Bytes> {
        {
            let tool = self.tools.entry(tool_index).or_default();
            if let Some(call_id) = call_id.filter(|call_id| !call_id.is_empty()) {
                tool.call_id = Some(call_id.to_string());
            }
            if let Some(name) = name.filter(|name| !name.trim().is_empty()) {
                tool.name = Some(name.to_string());
            }
            tool.arguments.push_str(arguments);
        }
        // Chat may interleave deltas for multiple tool calls. Anthropic SSE
        // content blocks cannot overlap, so keep tool arguments buffered until
        // the raw Chat terminal confirms the complete ordered tool list.
        Vec::new()
    }

    fn ensure_terminal_tool_metadata(&mut self) -> Result<(), String> {
        let response_id = self
            .response_id
            .clone()
            .unwrap_or_else(|| "chat".to_string());
        for (tool_index, tool) in &mut self.tools {
            if tool
                .name
                .as_deref()
                .map_or(true, |name| name.trim().is_empty())
            {
                return Err(format!(
                    "Chat tool call {tool_index} ended without a function name"
                ));
            }
            if tool.call_id.as_deref().map_or(true, |id| id.is_empty()) {
                tool.call_id = Some(format!("toolu_{response_id}_{tool_index}"));
            }
            let arguments = tool.arguments.trim();
            match serde_json::from_str::<Value>(arguments) {
                Ok(Value::Object(_)) => {}
                Ok(_) => {
                    return Err(format!(
                        "Chat tool call {tool_index} ended with arguments that are not a complete JSON object"
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "Chat tool call {tool_index} ended without a complete JSON object in arguments"
                    ));
                }
            }
        }
        Ok(())
    }

    fn emit_completed_tools(&mut self) -> Vec<Bytes> {
        let tool_indices = self.tools.keys().copied().collect::<Vec<_>>();
        let mut events = Vec::new();
        for tool_index in tool_indices {
            let (call_id, name, arguments) = {
                let tool = self.tools.get(&tool_index).expect("tool must exist");
                (
                    tool.call_id.clone().unwrap_or_default(),
                    tool.name.clone().unwrap_or_default(),
                    tool.arguments.clone(),
                )
            };
            let index = self.next_index();
            events.push(anthropic_event(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": call_id,
                        "name": name,
                        "input": {}
                    }
                }),
            ));
            if !arguments.is_empty() {
                events.push(anthropic_event(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": "input_json_delta", "partial_json": arguments }
                    }),
                ));
            }
            events.push(content_block_stop(index));
        }
        if !self.tools.is_empty() {
            self.model_output_seen = true;
        }
        events
    }

    fn complete_on_done(&mut self) -> Vec<Bytes> {
        if self.finish_reason.is_none() {
            return self.failed_message_events(
                "Upstream Chat stream sent [DONE] before a finish_reason",
                "stream_truncated",
            );
        }
        if let Err(message) = self.ensure_terminal_tool_metadata() {
            return self.failed_message_events(&message, "api_error");
        }
        let mut events = Vec::new();
        events.extend(self.ensure_message_started());
        events.extend(self.close_text());
        events.extend(self.emit_completed_tools());
        self.finished = true;
        self.succeeded = true;
        events.push(anthropic_event(
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": anthropic_stop_reason(self.finish_reason.as_deref()),
                    "stop_sequence": Value::Null
                },
                "usage": anthropic_terminal_usage(&self.usage),
            }),
        ));
        events.push(anthropic_event(
            "message_stop",
            json!({ "type": "message_stop" }),
        ));
        events
    }

    fn failed_events(&mut self, value: Option<&Value>, fallback_kind: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        self.succeeded = false;
        vec![anthropic_event(
            "error",
            json!({
                "type": "error",
                "error": anthropic_error(value, fallback_kind),
            }),
        )]
    }

    fn failed_message_events(&mut self, message: &str, fallback_kind: &str) -> Vec<Bytes> {
        self.failed_events(
            Some(&json!({ "message": message, "type": fallback_kind })),
            fallback_kind,
        )
    }

    fn stream_summary(&self) -> ChatToAnthropicStreamSummary {
        let raw = json!({ "usage": Value::Object(self.usage.clone()) });
        ChatToAnthropicStreamSummary {
            usage: provider_usage_from_value(&raw),
            response_id: self.response_id.clone(),
            compaction_output_seen: false,
            model_output_seen: self.model_output_seen,
        }
    }
}

fn anthropic_start_usage(usage: &Map<String, Value>) -> Value {
    let mut result = Map::new();
    result.insert(
        "input_tokens".to_string(),
        json!(usage_value(usage, "input_tokens")
            .or_else(|| usage_value(usage, "prompt_tokens"))
            .unwrap_or(0)),
    );
    if let Some(cached) = cached_tokens(usage) {
        result.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    Value::Object(result)
}

fn anthropic_terminal_usage(usage: &Map<String, Value>) -> Value {
    let mut result = Map::new();
    result.insert(
        "output_tokens".to_string(),
        json!(usage_value(usage, "output_tokens")
            .or_else(|| usage_value(usage, "completion_tokens"))
            .unwrap_or(0)),
    );
    if let Some(input) =
        usage_value(usage, "input_tokens").or_else(|| usage_value(usage, "prompt_tokens"))
    {
        result.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(cached) = cached_tokens(usage) {
        result.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    Value::Object(result)
}

fn usage_value(usage: &Map<String, Value>, key: &str) -> Option<u64> {
    usage.get(key).and_then(Value::as_u64)
}

fn cached_tokens(usage: &Map<String, Value>) -> Option<u64> {
    usage
        .get("input_tokens_details")
        .or_else(|| usage.get("prompt_tokens_details"))
        .and_then(|value| value.get("cached_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| usage_value(usage, "cache_read_input_tokens"))
}

fn anthropic_stop_reason(reason: Option<&str>) -> &'static str {
    match reason.unwrap_or("stop") {
        "length" | "max_tokens" => "max_tokens",
        "tool_calls" | "function_call" => "tool_use",
        _ => "end_turn",
    }
}

fn anthropic_error(value: Option<&Value>, fallback_kind: &str) -> Value {
    let error = value
        .and_then(|value| {
            value
                .get("error")
                .filter(|error| !error.is_null())
                .or(Some(value))
        })
        .unwrap_or(&Value::Null);
    let message = error
        .get("message")
        .or_else(|| error.get("detail"))
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .filter(|message| !message.is_empty())
        .unwrap_or("Upstream Chat stream failed");
    let kind = error
        .get("type")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .unwrap_or(fallback_kind);
    json!({ "type": normalize_anthropic_error_kind(kind, message), "message": message })
}

fn normalize_anthropic_error_kind(kind: &str, message: &str) -> &'static str {
    match kind.trim().to_ascii_lowercase().as_str() {
        "invalid_request_error" => "invalid_request_error",
        "authentication_error" => "authentication_error",
        "permission_error" => "permission_error",
        "not_found_error" => "not_found_error",
        "request_too_large" => "request_too_large",
        "rate_limit_error" => "rate_limit_error",
        "api_error" => "api_error",
        "overloaded_error" => "overloaded_error",
        _ => {
            let signal = format!("{kind} {message}").to_ascii_lowercase();
            if signal.contains("rate") || signal.contains("quota") || signal.contains("429") {
                "rate_limit_error"
            } else if signal.contains("overload") || signal.contains("capacity") {
                "overloaded_error"
            } else if signal.contains("auth") || signal.contains("401") {
                "authentication_error"
            } else if signal.contains("permission")
                || signal.contains("forbidden")
                || signal.contains("403")
            {
                "permission_error"
            } else if signal.contains("not found") || signal.contains("404") {
                "not_found_error"
            } else if signal.contains("too large") || signal.contains("413") {
                "request_too_large"
            } else if signal.contains("invalid") || signal.contains("400") {
                "invalid_request_error"
            } else {
                "api_error"
            }
        }
    }
}

fn anthropic_event(event: &str, data: Value) -> Bytes {
    let payload = serde_json::to_string(&data).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!("event: {event}\ndata: {payload}\n\n"))
}

fn content_block_stop(index: u64) -> Bytes {
    anthropic_event(
        "content_block_stop",
        json!({ "type": "content_block_stop", "index": index }),
    )
}

/// Convert a raw Chat Completions stream into Anthropic Messages SSE without
/// buffering the upstream body.  A raw `[DONE]` and a Chat `finish_reason` are
/// both required for an Anthropic success terminal.
pub(crate) fn create_anthropic_sse_stream_from_chat<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    fallback_model: String,
) -> (
    impl Stream<Item = Result<Bytes, std::io::Error>> + Send,
    ChatToAnthropicStreamSummaryHandle,
) {
    let summary = Arc::new(Mutex::new(ChatToAnthropicStreamSummary::default()));
    let summary_for_stream = summary.clone();
    let adapted = async_stream::stream! {
        let mut decoder = SseFrameDecoder::default();
        let mut state = ChatToAnthropicState::new(fallback_model);
        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if state.finished {
                        continue;
                    }
                    let frames = decoder.push(&bytes);
                    if decoder.overflowed() {
                        for event in state.failed_message_events(
                            "Upstream Chat SSE frame exceeded the inspection limit",
                            "stream_frame_too_large",
                        ) {
                            yield Ok(event);
                        }
                        break;
                    }
                    for frame in frames {
                        for event in state.ingest(frame.event.as_deref(), &frame.data) {
                            yield Ok(event);
                        }
                        if state.finished {
                            break;
                        }
                    }
                }
                Err(error) => {
                    if state.succeeded {
                        yield Err(std::io::Error::other(format!(
                            "Stream error after Chat [DONE]: {error}"
                        )));
                    } else {
                        for event in state.failed_message_events(
                            &format!("Upstream Chat stream error: {error}"),
                            "stream_error",
                        ) {
                            yield Ok(event);
                        }
                    }
                    break;
                }
            }
        }

        if !state.finished {
            for event in state.failed_message_events(
                "Upstream Chat stream ended before [DONE]",
                "stream_truncated",
            ) {
                yield Ok(event);
            }
        }
        if let Ok(mut observed) = summary_for_stream.lock() {
            *observed = state.stream_summary();
        }
    };
    (adapted, summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    async fn collect(chunks: Vec<&'static str>) -> (String, ChatToAnthropicStreamSummary) {
        let upstream = stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<Bytes, std::io::Error>(Bytes::from_static(chunk.as_bytes()))),
        );
        let (stream, summary) =
            create_anthropic_sse_stream_from_chat(upstream, "fallback".to_string());
        let output = stream.collect::<Vec<_>>().await;
        let bytes = output
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();
        let observed = summary.lock().unwrap().clone();
        (String::from_utf8(bytes).unwrap(), observed)
    }

    #[tokio::test]
    async fn streams_text_usage_and_terminal_only_after_done() {
        let (output, summary) = collect(vec![
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"gpt-test\",\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":8}}}\n\ndata: [DONE]\n\n",
        ]).await;
        assert!(output.contains("event: message_start"));
        assert!(output.contains("\"text\":\"hel\""));
        assert!(output.contains("\"text\":\"lo\""));
        assert!(output.contains("\"stop_reason\":\"end_turn\""));
        assert_eq!(output.matches("event: message_stop").count(), 1);
        assert_eq!(summary.usage.input_tokens, 10);
        assert_eq!(summary.usage.cache_read_tokens, 8);
    }

    #[tokio::test]
    async fn maps_tool_deltas_and_tool_terminal() {
        let (output, _) = collect(vec![
            "data: {\"id\":\"chatcmpl_tool\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_lookup\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"x\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
        ]).await;
        assert!(output.contains("\"type\":\"tool_use\""));
        assert!(output.contains("\"id\":\"call_lookup\""));
        assert!(output.contains("\"name\":\"lookup\""));
        assert!(output.contains("\"stop_reason\":\"tool_use\""));
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }

    #[tokio::test]
    async fn maps_refusal_deltas_to_visible_anthropic_text() {
        let (output, summary) = collect(vec![
            "data: {\"id\":\"chatcmpl_refusal\",\"model\":\"gpt-test\",\"choices\":[{\"delta\":{\"refusal\":\"I cannot help with that.\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        ])
        .await;

        // Anthropic has no refusal block type.  A refusal must remain visible
        // to the client as assistant text rather than becoming an empty
        // successful message.
        assert!(output.contains("\"type\":\"text\""));
        assert!(output.contains("\"text\":\"I cannot help with that.\""));
        assert_eq!(output.matches("event: message_stop").count(), 1);
        assert!(summary.model_output_seen);
    }

    #[tokio::test]
    async fn rejects_incomplete_tool_arguments_without_success_terminal() {
        let (output, summary) = collect(vec![
            "data: {\"id\":\"chatcmpl_bad_tool\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_bad\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"Paris\\\"\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
        ])
        .await;

        assert!(output.contains("event: error"));
        assert!(output.contains("complete JSON object"));
        assert!(!output.contains("event: message_stop"));
        assert!(!output.contains("\"id\":\"call_bad\""));
        assert!(!summary.model_output_seen);
    }

    #[tokio::test]
    async fn rejects_non_object_tool_arguments_without_success_terminal() {
        let (output, _) = collect(vec![
            "data: {\"id\":\"chatcmpl_array_tool\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_array\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"[]\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
        ])
        .await;

        assert!(output.contains("event: error"));
        assert!(output.contains("not a complete JSON object"));
        assert!(!output.contains("event: message_stop"));
        assert!(!output.contains("\"id\":\"call_array\""));
    }

    #[tokio::test]
    async fn serializes_interleaved_parallel_tool_calls_into_non_overlapping_anthropic_blocks() {
        let (output, _) = collect(vec![
            "data: {\"id\":\"chatcmpl_parallel\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_first\",\"type\":\"function\",\"function\":{\"name\":\"first\",\"arguments\":\"{\\\"a\\\":\"}},{\"index\":1,\"id\":\"call_second\",\"type\":\"function\",\"function\":{\"name\":\"second\",\"arguments\":\"{\\\"b\\\":\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"2}\"}},{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n",
        ])
        .await;

        let first_start = output
            .find("\"id\":\"call_first\"")
            .expect("first tool start");
        let first_delta = output
            .find("\"partial_json\":\"{\\\"a\\\":1}\"")
            .expect("first tool delta");
        let first_stop = output[first_delta..]
            .find("event: content_block_stop")
            .map(|offset| first_delta + offset)
            .expect("first tool stop");
        let second_start = output
            .find("\"id\":\"call_second\"")
            .expect("second tool start");
        let second_delta = output
            .find("\"partial_json\":\"{\\\"b\\\":2}\"")
            .expect("second tool delta");
        let second_stop = output[second_delta..]
            .find("event: content_block_stop")
            .map(|offset| second_delta + offset)
            .expect("second tool stop");

        assert!(first_start < first_delta);
        assert!(first_delta < first_stop);
        assert!(first_stop < second_start);
        assert!(second_start < second_delta);
        assert!(second_delta < second_stop);
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }

    #[tokio::test]
    async fn holds_tool_blocks_until_done_so_truncation_never_leaks_a_partial_tool() {
        let (output, _) = collect(vec![
            "data: {\"id\":\"chatcmpl_partial_tool\",\"choices\":[{\"delta\":{\"content\":\"Working\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_partial\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n",
        ])
        .await;

        assert!(output.contains("\"text\":\"Working\""));
        assert!(output.contains("stream ended before [DONE]"));
        assert!(!output.contains("\"id\":\"call_partial\""));
        assert!(!output.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn error_truncation_and_finish_reason_without_done_never_stop_successfully() {
        let (error, _) = collect(vec![
            "event: error\ndata: {\"error\":{\"type\":\"server_error\",\"message\":\"busy\"}}\n\n",
        ])
        .await;
        assert!(error.contains("event: error"));
        assert!(error.contains("\"type\":\"api_error\""));
        assert!(!error.contains("event: message_stop"));

        let (truncated, _) = collect(vec![
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        ])
        .await;
        assert!(truncated.contains("stream ended before [DONE]"));
        assert!(!truncated.contains("event: message_stop"));

        let (missing_done, _) = collect(vec![
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]).await;
        assert!(missing_done.contains("stream ended before [DONE]"));
        assert!(!missing_done.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn decodes_split_utf8_and_crlf_without_forging_thinking() {
        let payload = "data: {\"id\":\"chatcmpl_utf8\",\"choices\":[{\"delta\":{\"content\":\"你好\",\"reasoning_content\":\"private\"},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n";
        let split = payload.find('你').unwrap() + 1;
        let chunks = vec![
            Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&payload.as_bytes()[..split])),
            Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&payload.as_bytes()[split..])),
        ];
        let upstream = stream::iter(chunks);
        let (stream, _) = create_anthropic_sse_stream_from_chat(upstream, "fallback".to_string());
        let output = stream.collect::<Vec<_>>().await;
        let output = String::from_utf8(
            output
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|bytes| bytes.to_vec())
                .collect(),
        )
        .unwrap();
        assert!(output.contains("你好"));
        assert!(!output.contains("thinking_delta"));
        assert!(!output.contains("signature_delta"));
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }
}
