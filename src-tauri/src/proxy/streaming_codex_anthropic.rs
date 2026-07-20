//! Anthropic Messages SSE -> OpenAI Responses SSE conversion.
//!
//! This is intentionally a one-way, protocol-specific adapter.  It owns only
//! the event decoding and canonical Responses event emission; the caller keeps
//! ownership of transport, bounded relay, disconnect draining, metrics, cache,
//! and settlement.

use super::{
    json_canonical::{canonical_json_string, canonicalize_tool_arguments_str},
    sse::SseFrameDecoder,
    transform_codex_chat::{
        custom_tool_input_from_chat_arguments, response_id_from_chat_id,
        response_tool_call_item_from_chat_name, response_tool_call_item_id_from_chat_name,
        CodexToolContext,
    },
};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

const ANTHROPIC_REASONING_ENVELOPE_PREFIX: &str = "atoapi_anthropic_v1:";

/// Request-derived metadata needed to restore flattened/custom Codex tools
/// while emitting canonical Responses items.  Keep it small and local to this
/// adapter rather than introducing a cross-protocol IR.
#[derive(Debug, Clone, Default)]
pub(crate) struct AnthropicResponsesContext {
    pub(crate) tool_context: CodexToolContext,
}

#[derive(Debug)]
enum BlockState {
    Text {
        output_index: u32,
        item_id: String,
        text: String,
    },
    Thinking {
        output_index: u32,
        item_id: String,
        text: String,
        signature: String,
        redacted_data: Option<String>,
        summary_started: bool,
    },
    Tool {
        output_index: u32,
        item_id: String,
        call_id: String,
        name: String,
        arguments: String,
        initial_input: Option<Value>,
    },
    Ignore,
}

#[derive(Debug)]
struct AnthropicToResponsesState {
    response_started: bool,
    finished: bool,
    succeeded: bool,
    response_id: String,
    model: String,
    created_at: u64,
    next_output_index: u32,
    blocks: BTreeMap<usize, BlockState>,
    output_items: Vec<(u32, Value)>,
    usage: Map<String, Value>,
    stop_reason: Option<String>,
    tool_context: CodexToolContext,
    final_response: Option<Value>,
}

impl AnthropicToResponsesState {
    fn new(context: AnthropicResponsesContext, fallback_model: String) -> Self {
        Self {
            response_started: false,
            finished: false,
            succeeded: false,
            response_id: format!("resp_{}", Uuid::new_v4().simple()),
            model: fallback_model,
            created_at: Utc::now().timestamp().max(0) as u64,
            next_output_index: 0,
            blocks: BTreeMap::new(),
            output_items: Vec::new(),
            usage: Map::new(),
            stop_reason: None,
            tool_context: context.tool_context,
            final_response: None,
        }
    }

    fn ingest(&mut self, event: Option<&str>, data: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }

        if event == Some("error") {
            let (message, kind) = serde_json::from_str::<Value>(data)
                .ok()
                .map(|value| extract_anthropic_error(&value))
                .unwrap_or_else(|| (data.to_string(), None));
            return self.failed_events(message, kind);
        }

        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return Vec::new();
        };
        if value.get("error").is_some_and(|error| !error.is_null())
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            let (message, kind) = extract_anthropic_error(&value);
            return self.failed_events(message, kind);
        }

        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(event)
            .unwrap_or_default();
        match kind {
            "message_start" => self.handle_message_start(&value),
            "content_block_start" => self.handle_content_block_start(&value),
            "content_block_delta" => self.handle_content_block_delta(&value),
            "content_block_stop" => self.handle_content_block_stop(&value),
            "message_delta" => self.handle_message_delta(&value),
            "message_stop" => self.completed_events(),
            // `ping` and future vendor extensions are intentionally ignored.
            _ => Vec::new(),
        }
    }

    fn handle_message_start(&mut self, value: &Value) -> Vec<Bytes> {
        let message = value.get("message").unwrap_or(value);
        if !self.response_started {
            if let Some(id) = message.get("id").and_then(Value::as_str) {
                self.response_id = response_id_from_chat_id(Some(id));
            }
            if let Some(model) = message.get("model").and_then(Value::as_str) {
                if !model.is_empty() {
                    self.model = model.to_string();
                }
            }
        }
        self.merge_usage(message.get("usage").or_else(|| value.get("usage")));
        self.ensure_response_started()
    }

    fn handle_content_block_start(&mut self, value: &Value) -> Vec<Bytes> {
        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        let block = value.get("content_block").unwrap_or(value);
        let block_type = block
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut events = self.ensure_response_started();
        let output_index = self.next_output_index();

        match block_type {
            "text" => {
                let item_id = format!("{}_msg_{index}", self.response_id);
                let initial = block
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.blocks.insert(
                    index,
                    BlockState::Text {
                        output_index,
                        item_id: item_id.clone(),
                        text: initial.clone(),
                    },
                );
                events.push(sse_event(
                    "response.output_item.added",
                    json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": {
                            "id": item_id,
                            "type": "message",
                            "status": "in_progress",
                            "role": "assistant",
                            "content": []
                        }
                    }),
                ));
                events.push(sse_event(
                    "response.content_part.added",
                    json!({
                        "type": "response.content_part.added",
                        "item_id": format!("{}_msg_{index}", self.response_id),
                        "output_index": output_index,
                        "content_index": 0,
                        "part": { "type": "output_text", "text": "", "annotations": [] }
                    }),
                ));
                if !initial.is_empty() {
                    events.push(text_delta_event(
                        &format!("{}_msg_{index}", self.response_id),
                        output_index,
                        &initial,
                    ));
                }
            }
            "thinking" | "redacted_thinking" => {
                let item_id = format!("rs_{}_{}", self.response_id, index);
                let initial = block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let signature = block
                    .get("signature")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let redacted_data = (block_type == "redacted_thinking")
                    .then(|| {
                        block
                            .get("data")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    })
                    .filter(|value| !value.is_empty());
                self.blocks.insert(
                    index,
                    BlockState::Thinking {
                        output_index,
                        item_id: item_id.clone(),
                        text: initial.clone(),
                        signature,
                        redacted_data,
                        summary_started: !initial.is_empty(),
                    },
                );
                events.push(sse_event(
                    "response.output_item.added",
                    json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": {
                            "id": item_id,
                            "type": "reasoning",
                            "status": "in_progress",
                            "summary": []
                        }
                    }),
                ));
                if !initial.is_empty() {
                    events.extend(reasoning_part_added_events(
                        &format!("rs_{}_{}", self.response_id, index),
                        output_index,
                        &initial,
                    ));
                }
            }
            "tool_use" => {
                let call_id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| format!("call_{index}"));
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if name.is_empty() {
                    self.blocks.insert(index, BlockState::Ignore);
                    return events;
                }
                let item_id =
                    response_tool_call_item_id_from_chat_name(&call_id, &name, &self.tool_context);
                let initial_input = block.get("input").cloned().filter(nonempty_json_value);
                self.blocks.insert(
                    index,
                    BlockState::Tool {
                        output_index,
                        item_id: item_id.clone(),
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: String::new(),
                        initial_input: initial_input.clone(),
                    },
                );
                let item = response_tool_call_item_from_chat_name(
                    &item_id,
                    "in_progress",
                    &call_id,
                    &name,
                    "",
                    None,
                    &self.tool_context,
                );
                events.push(sse_event(
                    "response.output_item.added",
                    json!({
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": item
                    }),
                ));
                if let Some(input) = initial_input {
                    let initial_arguments = canonical_json_string(&input);
                    if !self.tool_context.is_custom_tool_chat_name(&name) {
                        events.push(tool_arguments_delta_event(
                            &item_id,
                            output_index,
                            &initial_arguments,
                        ));
                    }
                }
            }
            _ => {
                self.blocks.insert(index, BlockState::Ignore);
            }
        }
        events
    }

    fn handle_content_block_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        let delta = value.get("delta").unwrap_or(value);
        let delta_type = delta
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut events = Vec::new();
        match self.blocks.get_mut(&index) {
            Some(BlockState::Text {
                output_index,
                item_id,
                text,
            }) if delta_type == "text_delta" => {
                let piece = delta
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !piece.is_empty() {
                    text.push_str(piece);
                    events.push(text_delta_event(item_id, *output_index, piece));
                }
            }
            Some(BlockState::Thinking {
                output_index,
                item_id,
                text,
                signature,
                summary_started,
                ..
            }) => match delta_type {
                "thinking_delta" => {
                    let piece = delta
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if !piece.is_empty() {
                        if !*summary_started {
                            events.extend(reasoning_part_added_events(item_id, *output_index, ""));
                            *summary_started = true;
                        }
                        text.push_str(piece);
                        events.push(reasoning_delta_event(item_id, *output_index, piece));
                    }
                }
                "signature_delta" => {
                    if let Some(piece) = delta.get("signature").and_then(Value::as_str) {
                        signature.push_str(piece);
                    }
                }
                _ => {}
            },
            Some(BlockState::Tool {
                output_index,
                item_id,
                name,
                arguments,
                ..
            }) if delta_type == "input_json_delta" => {
                let piece = delta
                    .get("partial_json")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !piece.is_empty() {
                    arguments.push_str(piece);
                    if !self.tool_context.is_custom_tool_chat_name(name) {
                        events.push(tool_arguments_delta_event(item_id, *output_index, piece));
                    }
                }
            }
            _ => {}
        }
        events
    }

    fn handle_content_block_stop(&mut self, value: &Value) -> Vec<Bytes> {
        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        self.finalize_block(index)
    }

    fn handle_message_delta(&mut self, value: &Value) -> Vec<Bytes> {
        self.merge_usage(value.get("usage"));
        if let Some(reason) = value.pointer("/delta/stop_reason").and_then(Value::as_str) {
            self.stop_reason = Some(reason.to_string());
        }
        self.ensure_response_started()
    }

    fn ensure_response_started(&mut self) -> Vec<Bytes> {
        if self.response_started {
            return Vec::new();
        }
        self.response_started = true;
        let mut response = self.base_response("in_progress", Vec::new());
        // Anthropic carries input usage in `message_start`.  Responses stream
        // observers merge every event, so publishing it in both startup events
        // and the final completed event would triple-count cache/input usage.
        // Keep startup usage as the canonical zero placeholder and publish the
        // real cumulative snapshot exactly once at `response.completed`.
        response["usage"] = empty_responses_usage();
        vec![
            sse_event(
                "response.created",
                json!({
                    "type": "response.created",
                    "response": response
                }),
            ),
            sse_event(
                "response.in_progress",
                json!({
                    "type": "response.in_progress",
                    "response": self.base_response_with_usage("in_progress", Vec::new(), empty_responses_usage())
                }),
            ),
        ]
    }

    fn completed_events(&mut self) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        let mut events = self.ensure_response_started();
        let indexes = self.blocks.keys().copied().collect::<Vec<_>>();
        for index in indexes {
            events.extend(self.finalize_block(index));
        }
        let status = if self.stop_reason.as_deref() == Some("max_tokens") {
            "incomplete"
        } else {
            "completed"
        };
        let mut response = self.base_response(status, self.completed_output_items());
        if status == "incomplete" {
            response["incomplete_details"] = json!({ "reason": "max_output_tokens" });
        }
        self.finished = true;
        self.succeeded = true;
        self.final_response = Some(response.clone());
        events.push(sse_event(
            "response.completed",
            json!({ "type": "response.completed", "response": response }),
        ));
        events
    }

    fn failed_events(&mut self, message: String, error_type: Option<String>) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        let mut events = self.ensure_response_started();
        let mut error = json!({ "message": message });
        if let Some(error_type) = error_type.filter(|value| !value.is_empty()) {
            error["type"] = json!(error_type);
        }
        let mut response = self.base_response("failed", self.completed_output_items());
        response["error"] = error;
        self.finished = true;
        self.succeeded = false;
        self.final_response = Some(response.clone());
        events.push(sse_event(
            "response.failed",
            json!({ "type": "response.failed", "response": response }),
        ));
        events
    }

    fn finalize_block(&mut self, index: usize) -> Vec<Bytes> {
        let Some(block) = self.blocks.remove(&index) else {
            return Vec::new();
        };
        match block {
            BlockState::Text {
                output_index,
                item_id,
                text,
            } => {
                let item = json!({
                    "id": item_id,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": text,
                        "annotations": []
                    }]
                });
                self.output_items.push((output_index, item.clone()));
                vec![
                    sse_event(
                        "response.output_text.done",
                        json!({
                            "type": "response.output_text.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": 0,
                            "text": text
                        }),
                    ),
                    sse_event(
                        "response.content_part.done",
                        json!({
                            "type": "response.content_part.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": 0,
                            "part": { "type": "output_text", "text": text, "annotations": [] }
                        }),
                    ),
                    sse_event(
                        "response.output_item.done",
                        json!({
                            "type": "response.output_item.done",
                            "output_index": output_index,
                            "item": item
                        }),
                    ),
                ]
            }
            BlockState::Thinking {
                output_index,
                item_id,
                text,
                signature,
                redacted_data,
                ..
            } => {
                let envelope =
                    anthropic_reasoning_envelope(&text, &signature, redacted_data.as_deref());
                let mut item = json!({
                    "id": item_id,
                    "type": "reasoning",
                    "summary": if text.is_empty() { Value::Array(Vec::new()) } else { json!([{ "type": "summary_text", "text": text }]) }
                });
                if let Some(envelope) = envelope {
                    item["encrypted_content"] = json!(envelope);
                }
                self.output_items.push((output_index, item.clone()));
                let mut events = Vec::new();
                if !text.is_empty() {
                    events.push(sse_event(
                        "response.reasoning_summary_text.done",
                        json!({
                            "type": "response.reasoning_summary_text.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": 0,
                            "text": text
                        }),
                    ));
                    events.push(sse_event(
                        "response.reasoning_summary_part.done",
                        json!({
                            "type": "response.reasoning_summary_part.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": 0,
                            "part": { "type": "summary_text", "text": text }
                        }),
                    ));
                }
                events.push(sse_event(
                    "response.output_item.done",
                    json!({
                        "type": "response.output_item.done",
                        "output_index": output_index,
                        "item": item
                    }),
                ));
                events
            }
            BlockState::Tool {
                output_index,
                item_id,
                call_id,
                name,
                arguments,
                initial_input,
            } => {
                let arguments = if arguments.trim().is_empty() {
                    initial_input
                        .as_ref()
                        .map(canonical_json_string)
                        .unwrap_or_else(|| "{}".to_string())
                } else {
                    canonicalize_tool_arguments_str(&arguments)
                };
                let item = response_tool_call_item_from_chat_name(
                    &item_id,
                    "completed",
                    &call_id,
                    &name,
                    &arguments,
                    None,
                    &self.tool_context,
                );
                self.output_items.push((output_index, item.clone()));
                let mut events = Vec::new();
                if self.tool_context.is_custom_tool_chat_name(&name) {
                    let input = custom_tool_input_from_chat_arguments(&arguments);
                    if !input.is_empty() {
                        events.push(sse_event(
                            "response.custom_tool_call_input.delta",
                            json!({
                                "type": "response.custom_tool_call_input.delta",
                                "item_id": item_id,
                                "output_index": output_index,
                                "delta": input
                            }),
                        ));
                    }
                    events.push(sse_event(
                        "response.custom_tool_call_input.done",
                        json!({
                            "type": "response.custom_tool_call_input.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "input": input
                        }),
                    ));
                } else {
                    events.push(sse_event(
                        "response.function_call_arguments.done",
                        json!({
                            "type": "response.function_call_arguments.done",
                            "item_id": item_id,
                            "output_index": output_index,
                            "arguments": arguments
                        }),
                    ));
                }
                events.push(sse_event(
                    "response.output_item.done",
                    json!({
                        "type": "response.output_item.done",
                        "output_index": output_index,
                        "item": item
                    }),
                ));
                events
            }
            BlockState::Ignore => Vec::new(),
        }
    }

    fn merge_usage(&mut self, usage: Option<&Value>) {
        let Some(usage) = usage.and_then(Value::as_object) else {
            return;
        };
        for key in [
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "cache_creation",
            "server_tool_use",
        ] {
            if let Some(value) = usage.get(key) {
                self.usage.insert(key.to_string(), value.clone());
            }
        }
    }

    fn responses_usage(&self) -> Value {
        let input_tokens = usage_u64(&self.usage, "input_tokens");
        let cache_read = usage_u64(&self.usage, "cache_read_input_tokens");
        let cache_creation = usage_u64(&self.usage, "cache_creation_input_tokens");
        let output_tokens = usage_u64(&self.usage, "output_tokens");
        let total_input = input_tokens
            .saturating_add(cache_read)
            .saturating_add(cache_creation);
        json!({
            "input_tokens": total_input,
            "output_tokens": output_tokens,
            "total_tokens": total_input.saturating_add(output_tokens),
            "input_tokens_details": { "cached_tokens": cache_read },
            "output_tokens_details": { "reasoning_tokens": 0 },
            "cache_read_input_tokens": cache_read,
            "cache_creation_input_tokens": cache_creation
        })
    }

    fn completed_output_items(&self) -> Vec<Value> {
        let mut items = self.output_items.clone();
        items.sort_by_key(|(index, _)| *index);
        items.into_iter().map(|(_, item)| item).collect()
    }

    fn base_response(&self, status: &str, output: Vec<Value>) -> Value {
        self.base_response_with_usage(status, output, self.responses_usage())
    }

    fn base_response_with_usage(&self, status: &str, output: Vec<Value>, usage: Value) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "created_at": self.created_at,
            "status": status,
            "model": self.model,
            "output": output,
            "usage": usage
        })
    }

    fn next_output_index(&mut self) -> u32 {
        let current = self.next_output_index;
        self.next_output_index = self.next_output_index.saturating_add(1);
        current
    }
}

/// Convert a live Anthropic Messages SSE body to Responses SSE without any
/// network side effect.  Errors before a valid `message_stop` become one
/// `response.failed`; a transport error after `message_stop` remains visible to
/// the relay as a trailing anomaly.
pub(crate) fn create_responses_sse_stream_from_anthropic<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut decoder = SseFrameDecoder::default();
        let mut state = AnthropicToResponsesState::new(context, fallback_model);
        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if state.finished {
                        continue;
                    }
                    let frames = decoder.push(&bytes);
                    if decoder.overflowed() {
                        for event in state.failed_events(
                            "Upstream Anthropic Messages SSE frame exceeded the inspection limit".to_string(),
                            Some("stream_frame_too_large".to_string()),
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
                            "Stream error after Anthropic message_stop: {error}"
                        )));
                    } else {
                        for event in state.failed_events(
                            format!("Anthropic Messages stream error: {error}"),
                            Some("stream_error".to_string()),
                        ) {
                            yield Ok(event);
                        }
                    }
                    break;
                }
            }
        }

        if !state.finished {
            for event in state.failed_events(
                "Upstream Anthropic Messages stream ended before message_stop".to_string(),
                Some("stream_truncated".to_string()),
            ) {
                yield Ok(event);
            }
        }
    }
}

/// Convert one non-streaming Anthropic Message JSON response to a canonical
/// Responses JSON response.  It is used only with the already-received body;
/// callers must never issue another upstream request for this fallback.
pub(crate) fn anthropic_json_to_responses_json(
    bytes: &[u8],
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> Result<Vec<u8>, String> {
    let (_, response) = anthropic_json_to_responses_events(bytes, context, fallback_model)?;
    if response.get("status").and_then(Value::as_str) == Some("failed") {
        return serde_json::to_vec(&json!({
            "error": response.get("error").cloned().unwrap_or_else(|| json!({ "message": "Anthropic Messages request failed" }))
        }))
        .map_err(|error| error.to_string());
    }
    serde_json::to_vec(&response).map_err(|error| error.to_string())
}

/// Convert a completed Anthropic Message JSON response to Responses SSE using
/// the same adapter state machine as the live streaming path.
pub(crate) fn anthropic_json_to_responses_sse(
    bytes: &[u8],
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> Result<Vec<u8>, String> {
    let (events, _) = anthropic_json_to_responses_events(bytes, context, fallback_model)?;
    Ok(events
        .into_iter()
        .flat_map(|event| event.to_vec())
        .collect())
}

fn anthropic_json_to_responses_events(
    bytes: &[u8],
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> Result<(Vec<Bytes>, Value), String> {
    let value = serde_json::from_slice::<Value>(bytes).map_err(|error| error.to_string())?;
    let mut state = AnthropicToResponsesState::new(context, fallback_model);
    if value.get("error").is_some_and(|error| !error.is_null())
        || value.get("type").and_then(Value::as_str) == Some("error")
    {
        let (message, kind) = extract_anthropic_error(&value);
        let events = state.failed_events(message, kind);
        let response = state
            .final_response
            .clone()
            .ok_or_else(|| "failed to build a Responses error response".to_string())?;
        return Ok((events, response));
    }

    let mut events = Vec::new();
    let start = json!({ "type": "message_start", "message": value.clone() });
    events.extend(state.handle_message_start(&start));
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        for (index, block) in content.iter().enumerate() {
            let start = json!({
                "type": "content_block_start",
                "index": index,
                "content_block": block
            });
            events.extend(state.handle_content_block_start(&start));
            let stop = json!({ "type": "content_block_stop", "index": index });
            events.extend(state.handle_content_block_stop(&stop));
        }
    }
    let delta = json!({
        "type": "message_delta",
        "delta": { "stop_reason": value.get("stop_reason").cloned().unwrap_or(Value::Null) },
        "usage": value.get("usage").cloned().unwrap_or_else(|| json!({}))
    });
    events.extend(state.handle_message_delta(&delta));
    events.extend(state.completed_events());
    let response = state
        .final_response
        .clone()
        .ok_or_else(|| "failed to build Responses JSON from Anthropic message".to_string())?;
    Ok((events, response))
}

/// Aggregate an already buffered Anthropic SSE body into a Responses JSON
/// response.  This remains a single-response fallback for models explicitly
/// marked non-streaming; it does not perform a second POST.
pub(crate) fn anthropic_sse_to_responses_json(
    bytes: &[u8],
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> Result<Vec<u8>, String> {
    let mut decoder = SseFrameDecoder::default();
    let mut state = AnthropicToResponsesState::new(context, fallback_model);
    for frame in decoder.push(bytes) {
        state.ingest(frame.event.as_deref(), &frame.data);
        if state.finished {
            break;
        }
    }
    if decoder.overflowed() && !state.finished {
        state.failed_events(
            "Upstream Anthropic Messages SSE frame exceeded the inspection limit".to_string(),
            Some("stream_frame_too_large".to_string()),
        );
    }
    if !state.finished {
        for frame in decoder.finish() {
            state.ingest(frame.event.as_deref(), &frame.data);
            if state.finished {
                break;
            }
        }
    }
    if state.succeeded {
        return state
            .final_response
            .and_then(|response| serde_json::to_vec(&response).ok())
            .ok_or_else(|| "Anthropic stream completed without a final response".to_string());
    }
    if !state.finished {
        state.failed_events(
            "Upstream Anthropic Messages stream ended before message_stop".to_string(),
            Some("stream_truncated".to_string()),
        );
    }
    let error = state
        .final_response
        .and_then(|response| response.get("error").cloned())
        .unwrap_or_else(|| json!({ "message": "Anthropic Messages stream failed" }));
    serde_json::to_vec(&json!({ "error": error })).map_err(|error| error.to_string())
}

/// Convert a fully buffered Anthropic SSE response to Responses SSE without
/// degrading structured thinking or tool events into a text-only pseudo stream.
pub(crate) fn anthropic_sse_to_responses_sse(
    bytes: &[u8],
    context: AnthropicResponsesContext,
    fallback_model: String,
) -> Result<Vec<u8>, String> {
    let mut decoder = SseFrameDecoder::default();
    let mut state = AnthropicToResponsesState::new(context, fallback_model);
    let mut events = Vec::new();
    for frame in decoder.push(bytes) {
        events.extend(state.ingest(frame.event.as_deref(), &frame.data));
        if state.finished {
            break;
        }
    }
    if decoder.overflowed() {
        events.extend(state.failed_events(
            "Upstream Anthropic Messages SSE frame exceeded the inspection limit".to_string(),
            Some("stream_frame_too_large".to_string()),
        ));
    }
    if !state.finished {
        for frame in decoder.finish() {
            events.extend(state.ingest(frame.event.as_deref(), &frame.data));
            if state.finished {
                break;
            }
        }
    }
    if !state.finished {
        events.extend(state.failed_events(
            "Upstream Anthropic Messages stream ended before message_stop".to_string(),
            Some("stream_truncated".to_string()),
        ));
    }
    Ok(events
        .into_iter()
        .flat_map(|event| event.to_vec())
        .collect())
}

pub(crate) fn decode_anthropic_reasoning_envelope(value: &Value) -> Option<Value> {
    let encoded = value
        .get("encrypted_content")
        .and_then(Value::as_str)
        .and_then(|content| content.strip_prefix(ANTHROPIC_REASONING_ENVELOPE_PREFIX))?;
    let bytes = STANDARD.decode(encoded).ok()?;
    let envelope = serde_json::from_slice::<Value>(&bytes).ok()?;
    if envelope.get("provider").and_then(Value::as_str) != Some("anthropic") {
        return None;
    }
    if let Some(data) = envelope.get("redacted_data").and_then(Value::as_str) {
        return Some(json!({ "type": "redacted_thinking", "data": data }));
    }
    let thinking = envelope
        .get("thinking")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let signature = envelope
        .get("signature")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if thinking.is_empty() && signature.is_empty() {
        return None;
    }
    Some(json!({
        "type": "thinking",
        "thinking": thinking,
        "signature": signature
    }))
}

/// Preserve a native Anthropic thinking block while routing it through a
/// Responses upstream.  The opaque envelope contains only the original
/// signed/restricted block and is restored only by
/// [`decode_anthropic_reasoning_envelope`]; unsigned thinking is deliberately
/// not promoted because Responses cannot manufacture an Anthropic signature.
pub(crate) fn encode_anthropic_reasoning_envelope(block: &Value) -> Option<Value> {
    let block_type = block.get("type").and_then(Value::as_str)?;
    let (thinking, signature, redacted_data) = match block_type {
        "thinking" => (
            block
                .get("thinking")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            block
                .get("signature")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            None,
        ),
        "redacted_thinking" => (
            "",
            "",
            block
                .get("data")
                .and_then(Value::as_str)
                .filter(|data| !data.is_empty()),
        ),
        _ => return None,
    };
    anthropic_reasoning_envelope(thinking, signature, redacted_data).map(|encrypted_content| {
        json!({
            "type": "reasoning",
            "encrypted_content": encrypted_content,
        })
    })
}

fn nonempty_json_value(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(map) => !map.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::String(value) => !value.is_empty(),
        _ => true,
    }
}

fn anthropic_reasoning_envelope(
    thinking: &str,
    signature: &str,
    redacted_data: Option<&str>,
) -> Option<String> {
    if signature.is_empty() && redacted_data.is_none() {
        return None;
    }
    let envelope = json!({
        "version": 1,
        "provider": "anthropic",
        "thinking": thinking,
        "signature": signature,
        "redacted_data": redacted_data
    });
    serde_json::to_vec(&envelope).ok().map(|bytes| {
        format!(
            "{ANTHROPIC_REASONING_ENVELOPE_PREFIX}{}",
            STANDARD.encode(bytes)
        )
    })
}

fn extract_anthropic_error(value: &Value) -> (String, Option<String>) {
    let error = value.get("error").unwrap_or(value);
    let message = error
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| {
            error
                .get("message")
                .or_else(|| error.get("detail"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| error.to_string());
    let kind = error
        .get("type")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    (message, kind)
}

fn usage_u64(usage: &Map<String, Value>, key: &str) -> u64 {
    usage.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn empty_responses_usage() -> Value {
    json!({
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "input_tokens_details": { "cached_tokens": 0 },
        "output_tokens_details": { "reasoning_tokens": 0 },
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0
    })
}

fn sse_event(event: &str, data: Value) -> Bytes {
    Bytes::from(format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(&data).unwrap_or_default()
    ))
}

fn text_delta_event(item_id: &str, output_index: u32, delta: &str) -> Bytes {
    sse_event(
        "response.output_text.delta",
        json!({
            "type": "response.output_text.delta",
            "item_id": item_id,
            "output_index": output_index,
            "content_index": 0,
            "delta": delta
        }),
    )
}

fn reasoning_part_added_events(item_id: &str, output_index: u32, initial: &str) -> Vec<Bytes> {
    let mut events = vec![sse_event(
        "response.reasoning_summary_part.added",
        json!({
            "type": "response.reasoning_summary_part.added",
            "item_id": item_id,
            "output_index": output_index,
            "summary_index": 0,
            "part": { "type": "summary_text", "text": "" }
        }),
    )];
    if !initial.is_empty() {
        events.push(reasoning_delta_event(item_id, output_index, initial));
    }
    events
}

fn reasoning_delta_event(item_id: &str, output_index: u32, delta: &str) -> Bytes {
    sse_event(
        "response.reasoning_summary_text.delta",
        json!({
            "type": "response.reasoning_summary_text.delta",
            "item_id": item_id,
            "output_index": output_index,
            "summary_index": 0,
            "delta": delta
        }),
    )
}

fn tool_arguments_delta_event(item_id: &str, output_index: u32, delta: &str) -> Bytes {
    sse_event(
        "response.function_call_arguments.delta",
        json!({
            "type": "response.function_call_arguments.delta",
            "item_id": item_id,
            "output_index": output_index,
            "delta": delta
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{stream, StreamExt};

    async fn collect(chunks: Vec<&str>) -> String {
        let chunks = chunks
            .into_iter()
            .map(|chunk| Bytes::copy_from_slice(chunk.as_bytes()))
            .collect::<Vec<_>>();
        let upstream = stream::iter(chunks.into_iter().map(Ok::<Bytes, std::io::Error>));
        create_responses_sse_stream_from_anthropic(
            upstream,
            AnthropicResponsesContext::default(),
            "claude-test".to_string(),
        )
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flat_map(|chunk| chunk.to_vec())
        .map(char::from)
        .collect()
    }

    #[tokio::test]
    async fn converts_text_usage_and_terminal_once() {
        let output = collect(vec![
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":8}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hel\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ])
        .await;
        assert!(output.contains("event: response.created"));
        assert!(output.contains("event: response.output_text.delta"));
        assert!(output.contains("\"delta\":\"hel\""));
        assert!(output.contains("\"input_tokens\":18"));
        assert!(output.contains("\"cached_tokens\":8"));
        assert_eq!(output.matches("event: response.completed").count(), 1);
    }

    #[tokio::test]
    async fn converts_thinking_tool_and_partial_json_without_parsing_early() {
        let output = collect(vec![
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tools\",\"model\":\"claude-test\"}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"b\\\":2,\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"a\\\":1}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":3}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ])
        .await;
        assert!(output.contains("event: response.reasoning_summary_text.delta"));
        assert!(output.contains("atoapi_anthropic_v1:"));
        assert!(output.contains("event: response.function_call_arguments.delta"));
        assert!(output.contains("\"delta\":\"{\\\"b\\\":2,"));
        assert!(output.contains("event: response.function_call_arguments.done"));
        assert!(output.contains("\"arguments\":\"{\\\"a\\\":1,\\\"b\\\":2}\""));
    }

    #[tokio::test]
    async fn error_and_truncated_eof_never_emit_completed() {
        let error = collect(vec![
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
        ])
        .await;
        assert!(error.contains("event: response.failed"));
        assert!(error.contains("busy"));
        assert!(!error.contains("event: response.completed"));

        let truncated = collect(vec![
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_short\"}}\n\n",
        ])
        .await;
        assert!(truncated.contains("event: response.failed"));
        assert!(truncated.contains("stream_truncated"));
        assert!(!truncated.contains("event: response.completed"));
    }

    #[test]
    fn json_fallback_preserves_tool_usage_and_incomplete_status() {
        let json = anthropic_json_to_responses_json(
            br#"{
                "id":"msg_json","type":"message","model":"claude-test","stop_reason":"max_tokens",
                "content":[{"type":"tool_use","id":"toolu_json","name":"lookup","input":{"b":2,"a":1}}],
                "usage":{"input_tokens":10,"cache_read_input_tokens":8,"cache_creation_input_tokens":2,"output_tokens":3}
            }"#,
            AnthropicResponsesContext::default(),
            "fallback".to_string(),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value["status"], "incomplete");
        assert_eq!(value["incomplete_details"]["reason"], "max_output_tokens");
        assert_eq!(value["usage"]["input_tokens"], 20);
        assert_eq!(value["output"][0]["type"], "function_call");
        assert_eq!(value["output"][0]["arguments"], "{\"a\":1,\"b\":2}");
    }

    #[test]
    fn thinking_signature_is_carried_in_a_namespaced_round_trip_envelope() {
        let json = anthropic_json_to_responses_json(
            br#"{
                "id":"msg_thinking","type":"message","model":"claude-test","stop_reason":"tool_use",
                "content":[{"type":"thinking","thinking":"plan","signature":"sig_123"}],
                "usage":{"input_tokens":3,"output_tokens":1}
            }"#,
            AnthropicResponsesContext::default(),
            "fallback".to_string(),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&json).unwrap();
        let reasoning = &value["output"][0];
        assert_eq!(reasoning["type"], "reasoning");
        let restored = decode_anthropic_reasoning_envelope(reasoning).unwrap();
        assert_eq!(restored["type"], "thinking");
        assert_eq!(restored["thinking"], "plan");
        assert_eq!(restored["signature"], "sig_123");
    }

    #[test]
    fn request_history_only_encodes_verified_anthropic_thinking() {
        let encoded = encode_anthropic_reasoning_envelope(&json!({
            "type": "thinking",
            "thinking": "plan",
            "signature": "sig_456"
        }))
        .expect("signed thinking must survive a Responses round trip");
        let restored = decode_anthropic_reasoning_envelope(&encoded).unwrap();
        assert_eq!(restored["thinking"], "plan");
        assert_eq!(restored["signature"], "sig_456");
        assert!(encode_anthropic_reasoning_envelope(&json!({
            "type": "thinking",
            "thinking": "unsigned"
        }))
        .is_none());
    }
}
