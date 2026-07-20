//! OpenAI Responses SSE -> Anthropic Messages SSE conversion.
//!
//! This protocol adapter deliberately owns only complete SSE-frame decoding
//! and event mapping. Transport, retry policy, bounded relay ownership,
//! disconnect draining, cache capture, and settlement remain in
//! `stream_upstream` so a client request still has exactly one upstream owner.

use super::{
    provider_usage_from_value, sse::SseFrameDecoder, value_has_compaction_output,
    value_has_model_output,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Raw Responses terminal metadata observed while producing Anthropic frames.
///
/// The downstream frame has an Anthropic message id, but cache/session
/// settlement must retain the original `resp_*` id and raw Responses usage.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponsesToAnthropicStreamSummary {
    pub usage: crate::metrics::UsageRecord,
    pub response_id: Option<String>,
    pub compaction_output_seen: bool,
    pub model_output_seen: bool,
}

pub(crate) type ResponsesToAnthropicStreamSummaryHandle =
    Arc<Mutex<ResponsesToAnthropicStreamSummary>>;

#[derive(Debug)]
enum AnthropicOutputBlock {
    Text {
        index: u64,
        emitted: String,
        closed: bool,
    },
    Tool {
        index: u64,
        arguments: String,
        emitted_wire_arguments: String,
        custom: bool,
        closed: bool,
    },
    Terminal {
        index: u64,
    },
}

impl AnthropicOutputBlock {
    fn index(&self) -> u64 {
        match self {
            Self::Text { index, .. } | Self::Tool { index, .. } | Self::Terminal { index } => {
                *index
            }
        }
    }

    fn closed(&self) -> bool {
        match self {
            Self::Text { closed, .. } | Self::Tool { closed, .. } => *closed,
            Self::Terminal { .. } => true,
        }
    }
}

#[derive(Debug)]
struct ResponsesToAnthropicState {
    response_id: Option<String>,
    model: String,
    message_started: bool,
    finished: bool,
    succeeded: bool,
    next_block_index: u64,
    blocks: HashMap<String, AnthropicOutputBlock>,
    tool_by_output_index: HashMap<u64, String>,
    pending_tool_arguments: HashMap<String, String>,
    usage: Map<String, Value>,
    tool_seen: bool,
    upstream_compaction_output_seen: bool,
    upstream_model_output_seen: bool,
}

impl ResponsesToAnthropicState {
    fn new(fallback_model: String) -> Self {
        Self {
            response_id: None,
            model: fallback_model,
            message_started: false,
            finished: false,
            succeeded: false,
            next_block_index: 0,
            blocks: HashMap::new(),
            tool_by_output_index: HashMap::new(),
            pending_tool_arguments: HashMap::new(),
            usage: Map::new(),
            tool_seen: false,
            upstream_compaction_output_seen: false,
            upstream_model_output_seen: false,
        }
    }

    fn ingest(&mut self, event: Option<&str>, data: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
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
        self.observe_metadata(&value);
        self.upstream_compaction_output_seen |= value_has_compaction_output(&value);
        self.upstream_model_output_seen |= value_has_model_output(&value);
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(event)
            .unwrap_or_default();
        if value.get("error").is_some_and(|error| !error.is_null())
            || matches!(kind, "error" | "response.failed" | "response.incomplete")
            || kind.ends_with(".failed")
            || kind.ends_with(".incomplete")
            || kind.ends_with(".error")
        {
            return self.failed_events(Some(&value), kind);
        }

        match kind {
            "response.created" | "response.in_progress" => self.ensure_message_started(),
            "response.output_item.added" => self.handle_output_item_added(&value),
            "response.content_part.added" => self.handle_content_part_added(&value),
            "response.output_text.delta" => self.handle_text_delta(&value),
            "response.output_text.done" => self.handle_text_done(&value),
            "response.refusal.delta" => self.handle_refusal_delta(&value),
            "response.refusal.done" => self.handle_refusal_done(&value),
            "response.function_call_arguments.delta" | "response.custom_tool_call_input.delta" => {
                self.handle_tool_delta(&value)
            }
            "response.function_call_arguments.done" | "response.custom_tool_call_input.done" => {
                self.handle_tool_done(&value)
            }
            "response.output_item.done" => self.materialize_output_item(
                value.get("item").unwrap_or(&value),
                value.get("output_index").and_then(Value::as_u64),
            ),
            "response.completed" => self.completed_events(&value),
            // Native Responses reasoning lacks an Anthropic signature.  It is
            // intentionally ignored unless a verified Atoapi envelope appears
            // in an output item and can be restored verbatim.
            _ => Vec::new(),
        }
    }

    fn observe_metadata(&mut self, value: &Value) {
        for candidate in [Some(value), value.get("response")] {
            let Some(candidate) = candidate else {
                continue;
            };
            if let Some(id) = candidate.get("id").and_then(Value::as_str) {
                if !id.is_empty() {
                    self.response_id = Some(id.to_string());
                }
            }
            if let Some(model) = candidate.get("model").and_then(Value::as_str) {
                if !model.is_empty() {
                    self.model = model.to_string();
                }
            }
            self.merge_usage(candidate.get("usage"));
        }
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
            .unwrap_or_else(|| format!("resp_{}", Uuid::new_v4().simple()));
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

    fn ensure_text_block(&mut self, key: &str) -> Vec<Bytes> {
        if self.blocks.contains_key(key) {
            return Vec::new();
        }
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
        self.blocks.insert(
            key.to_string(),
            AnthropicOutputBlock::Text {
                index,
                emitted: String::new(),
                closed: false,
            },
        );
        events
    }

    fn append_text(&mut self, key: &str, text: &str, complete: bool) -> Vec<Bytes> {
        if text.is_empty() && !complete {
            return Vec::new();
        }
        let mut events = self.ensure_text_block(key);
        let Some(AnthropicOutputBlock::Text {
            index,
            emitted,
            closed,
        }) = self.blocks.get_mut(key)
        else {
            return events;
        };
        if *closed {
            return events;
        }
        let delta = if complete {
            missing_suffix(emitted, text)
        } else {
            text.to_string()
        };
        if !delta.is_empty() {
            emitted.push_str(&delta);
            events.push(anthropic_event(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": *index,
                    "delta": { "type": "text_delta", "text": delta }
                }),
            ));
        }
        if complete {
            *closed = true;
            events.push(content_block_stop(*index));
        }
        events
    }

    fn close_text(&mut self, key: &str) -> Vec<Bytes> {
        let Some(AnthropicOutputBlock::Text { index, closed, .. }) = self.blocks.get_mut(key)
        else {
            return Vec::new();
        };
        if *closed {
            return Vec::new();
        }
        *closed = true;
        vec![content_block_stop(*index)]
    }

    fn ensure_tool_from_item(
        &mut self,
        item: &Value,
        output_index: Option<u64>,
    ) -> (Vec<Bytes>, String) {
        let output_index =
            output_index.or_else(|| item.get("output_index").and_then(Value::as_u64));
        let key = tool_item_key(item, output_index);
        if self.blocks.contains_key(&key) {
            return (Vec::new(), key);
        }
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty());
        let Some(name) = name else {
            return (Vec::new(), key);
        };
        let call_id = item
            .get("call_id")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("call_{}", output_index.unwrap_or(self.next_block_index)));
        let custom = item.get("type").and_then(Value::as_str) == Some("custom_tool_call");
        let index = self.next_index();
        let mut events = self.ensure_message_started();
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
        self.blocks.insert(
            key.clone(),
            AnthropicOutputBlock::Tool {
                index,
                arguments: String::new(),
                emitted_wire_arguments: String::new(),
                custom,
                closed: false,
            },
        );
        self.tool_seen = true;
        if let Some(output_index) = output_index {
            self.tool_by_output_index.insert(output_index, key.clone());
        }
        if let Some(pending) = self.pending_tool_arguments.remove(&key) {
            events.extend(self.append_tool_delta(&key, &pending));
        }
        (events, key)
    }

    fn append_tool_delta(&mut self, key: &str, delta: &str) -> Vec<Bytes> {
        if delta.is_empty() {
            return Vec::new();
        }
        let Some(AnthropicOutputBlock::Tool {
            index,
            arguments,
            emitted_wire_arguments,
            custom,
            closed,
            ..
        }) = self.blocks.get_mut(key)
        else {
            self.pending_tool_arguments
                .entry(key.to_string())
                .or_default()
                .push_str(delta);
            return Vec::new();
        };
        if *closed {
            return Vec::new();
        }
        arguments.push_str(delta);
        if *custom {
            return Vec::new();
        }
        emitted_wire_arguments.push_str(delta);
        vec![anthropic_event(
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": *index,
                "delta": { "type": "input_json_delta", "partial_json": delta }
            }),
        )]
    }

    fn complete_tool(&mut self, key: &str, complete: &str) -> Vec<Bytes> {
        let Some(AnthropicOutputBlock::Tool {
            index,
            arguments,
            emitted_wire_arguments,
            custom,
            closed,
            ..
        }) = self.blocks.get_mut(key)
        else {
            if !complete.is_empty() {
                // A `.done` payload is authoritative and contains the full
                // argument string. If deltas arrived before the item metadata,
                // replace their partial buffer instead of concatenating the
                // same prefix twice when the item is materialized later.
                self.pending_tool_arguments
                    .insert(key.to_string(), complete.to_string());
            }
            return Vec::new();
        };
        if *closed {
            return Vec::new();
        }
        let missing_raw = missing_suffix(arguments, complete);
        if !missing_raw.is_empty() {
            arguments.push_str(&missing_raw);
        }
        let wire_complete = if *custom {
            custom_tool_wire_arguments(arguments)
        } else {
            arguments.clone()
        };
        let missing_wire = missing_suffix(emitted_wire_arguments, &wire_complete);
        if !missing_wire.is_empty() {
            emitted_wire_arguments.push_str(&missing_wire);
        }
        *closed = true;
        let mut events = Vec::new();
        if !missing_wire.is_empty() {
            events.push(anthropic_event(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": *index,
                    "delta": { "type": "input_json_delta", "partial_json": missing_wire }
                }),
            ));
        }
        events.push(content_block_stop(*index));
        events
    }

    fn handle_output_item_added(&mut self, value: &Value) -> Vec<Bytes> {
        let item = value.get("item").unwrap_or(value);
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") | Some("custom_tool_call") => {
                let (mut events, key) = self
                    .ensure_tool_from_item(item, value.get("output_index").and_then(Value::as_u64));
                if let Some(initial) = item.get("arguments").or_else(|| item.get("input")) {
                    events.extend(self.append_tool_delta(&key, &json_argument_string(initial)));
                }
                events
            }
            Some("reasoning") => {
                self.materialize_reasoning(item, value.get("output_index").and_then(Value::as_u64))
            }
            Some("message") | Some("compaction") => Vec::new(),
            _ => self.unsupported_output_item(item),
        }
    }

    fn handle_content_part_added(&mut self, value: &Value) -> Vec<Bytes> {
        let part = value.get("part").unwrap_or(value);
        let kind = part.get("type").and_then(Value::as_str).unwrap_or_default();
        if !matches!(kind, "output_text" | "text" | "refusal") {
            return Vec::new();
        }
        let key = event_content_key(value);
        let initial = if kind == "refusal" {
            part.get("refusal").or_else(|| part.get("text"))
        } else {
            part.get("text")
        }
        .and_then(Value::as_str)
        .unwrap_or_default();
        self.append_text(&key, initial, false)
    }

    fn handle_text_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| value.pointer("/delta/text").and_then(Value::as_str))
            .unwrap_or_default();
        self.append_text(&event_content_key(value), delta, false)
    }

    fn handle_text_done(&mut self, value: &Value) -> Vec<Bytes> {
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.append_text(&event_content_key(value), text, true)
    }

    fn handle_refusal_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| value.pointer("/delta/refusal").and_then(Value::as_str))
            .unwrap_or_default();
        self.append_text(&event_content_key(value), delta, false)
    }

    fn handle_refusal_done(&mut self, value: &Value) -> Vec<Bytes> {
        let refusal = value
            .get("refusal")
            .or_else(|| value.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.append_text(&event_content_key(value), refusal, true)
    }

    fn resolve_tool_key(&self, value: &Value) -> String {
        let output_index = value.get("output_index").and_then(Value::as_u64);
        value
            .get("item_id")
            .or_else(|| value.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(|id| format!("tool:{id}"))
            .or_else(|| {
                output_index.and_then(|index| self.tool_by_output_index.get(&index).cloned())
            })
            .unwrap_or_else(|| format!("tool:output_{}", output_index.unwrap_or(0)))
    }

    fn handle_tool_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .or_else(|| value.get("arguments_delta"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if delta.is_empty() {
            return Vec::new();
        }
        let key = self.resolve_tool_key(value);
        if !self.blocks.contains_key(&key) {
            let mut item = value.clone();
            if let Some(object) = item.as_object_mut() {
                object
                    .entry("type".to_string())
                    .or_insert_with(|| Value::String("function_call".to_string()));
                if let Some(item_id) = value.get("item_id").cloned() {
                    object.entry("id".to_string()).or_insert(item_id);
                }
            }
            let (mut events, ensured_key) = self
                .ensure_tool_from_item(&item, value.get("output_index").and_then(Value::as_u64));
            if self.blocks.contains_key(&ensured_key) {
                events.extend(self.append_tool_delta(&ensured_key, delta));
            } else {
                self.pending_tool_arguments
                    .entry(key)
                    .or_default()
                    .push_str(delta);
            }
            return events;
        }
        self.append_tool_delta(&key, delta)
    }

    fn handle_tool_done(&mut self, value: &Value) -> Vec<Bytes> {
        let complete = value
            .get("arguments")
            .or_else(|| value.get("input"))
            .map(json_argument_string)
            .unwrap_or_default();
        self.complete_tool(&self.resolve_tool_key(value), &complete)
    }

    fn materialize_output_item(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => self.materialize_message(item, output_index),
            Some("function_call") | Some("custom_tool_call") => {
                let (mut events, key) = self.ensure_tool_from_item(item, output_index);
                let complete = item
                    .get("arguments")
                    .or_else(|| item.get("input"))
                    .map(json_argument_string)
                    .unwrap_or_default();
                events.extend(self.complete_tool(&key, &complete));
                events
            }
            Some("reasoning") => self.materialize_reasoning(item, output_index),
            Some("compaction") => Vec::new(),
            _ => self.unsupported_output_item(item),
        }
    }

    fn materialize_message(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        let mut events = Vec::new();
        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for (content_index, part) in content.iter().enumerate() {
                let text = if part.get("type").and_then(Value::as_str) == Some("refusal") {
                    part.get("refusal").or_else(|| part.get("text"))
                } else {
                    part.get("text")
                }
                .and_then(Value::as_str)
                .unwrap_or_default();
                if !text.is_empty() {
                    events.extend(self.append_text(
                        &item_content_key(item, output_index, content_index as u64),
                        text,
                        true,
                    ));
                }
            }
        } else if let Some(text) = item
            .get("output_text")
            .or_else(|| item.get("text"))
            .or_else(|| item.get("content"))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            events.extend(self.append_text(&item_content_key(item, output_index, 0), text, true));
        }
        events
    }

    fn materialize_reasoning(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        let key = format!("reasoning:{}", item_identity(item, output_index));
        if self.blocks.contains_key(&key) {
            return Vec::new();
        }
        let Some(block) =
            super::streaming_codex_anthropic::decode_anthropic_reasoning_envelope(item)
        else {
            return Vec::new();
        };
        let index = self.next_index();
        let mut events = self.ensure_message_started();
        match block.get("type").and_then(Value::as_str) {
            Some("redacted_thinking") => {
                events.push(anthropic_event(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {
                            "type": "redacted_thinking",
                            "data": block.get("data").and_then(Value::as_str).unwrap_or_default(),
                        }
                    }),
                ));
            }
            Some("thinking") => {
                events.push(anthropic_event(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": { "type": "thinking", "thinking": "" }
                    }),
                ));
                if let Some(thinking) = block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .filter(|thinking| !thinking.is_empty())
                {
                    events.push(anthropic_event(
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": { "type": "thinking_delta", "thinking": thinking }
                        }),
                    ));
                }
                if let Some(signature) = block
                    .get("signature")
                    .and_then(Value::as_str)
                    .filter(|signature| !signature.is_empty())
                {
                    events.push(anthropic_event(
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": { "type": "signature_delta", "signature": signature }
                        }),
                    ));
                }
            }
            _ => return Vec::new(),
        }
        events.push(content_block_stop(index));
        self.blocks
            .insert(key, AnthropicOutputBlock::Terminal { index });
        events
    }

    fn close_open_blocks(&mut self) -> Vec<Bytes> {
        let mut keys = self
            .blocks
            .iter()
            .filter_map(|(key, block)| (!block.closed()).then_some((block.index(), key.clone())))
            .collect::<Vec<_>>();
        keys.sort_by_key(|(index, _)| *index);
        let mut events = Vec::new();
        for (_, key) in keys {
            let is_text = matches!(
                self.blocks.get(&key),
                Some(AnthropicOutputBlock::Text { .. })
            );
            if is_text {
                events.extend(self.close_text(&key));
            } else if let Some(AnthropicOutputBlock::Tool { arguments, .. }) = self.blocks.get(&key)
            {
                events.extend(self.complete_tool(&key, &arguments.clone()));
            }
        }
        events
    }

    fn unsupported_output_item(&mut self, item: &Value) -> Vec<Bytes> {
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .filter(|kind| !kind.is_empty())
            .unwrap_or("unknown");
        self.failed_message_events(
            &format!("Unsupported Responses output item type for Anthropic bridge: {item_type}"),
            "api_error",
        )
    }

    fn completed_events(&mut self, value: &Value) -> Vec<Bytes> {
        let response = value.get("response").unwrap_or(value);
        self.observe_metadata(response);
        self.upstream_compaction_output_seen |= value_has_compaction_output(response);
        self.upstream_model_output_seen |= value_has_model_output(response);
        let status = response.get("status").and_then(Value::as_str);
        if response.get("error").is_some_and(|error| !error.is_null())
            || matches!(status, Some("failed" | "incomplete" | "cancelled"))
        {
            return self.failed_events(Some(response), "response_completed_failure");
        }

        if let Some(output) = response.get("output").and_then(Value::as_array) {
            if let Some(unsupported) = output.iter().find(|item| {
                !matches!(
                    item.get("type").and_then(Value::as_str),
                    Some(
                        "message"
                            | "function_call"
                            | "custom_tool_call"
                            | "reasoning"
                            | "compaction"
                    )
                )
            }) {
                return self.unsupported_output_item(unsupported);
            }
        }

        // A successful but empty Responses response still has to form a
        // complete Anthropic Messages SSE sequence.
        let mut events = self.ensure_message_started();
        if let Some(output) = response.get("output").and_then(Value::as_array) {
            for (output_index, item) in output.iter().enumerate() {
                events.extend(self.materialize_output_item(item, Some(output_index as u64)));
            }
        }
        events.extend(self.close_open_blocks());
        self.finished = true;
        self.succeeded = true;
        let stop_reason = if self.tool_seen {
            "tool_use"
        } else {
            "end_turn"
        };
        events.push(anthropic_event(
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": stop_reason, "stop_sequence": Value::Null },
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

    fn stream_summary(&self) -> ResponsesToAnthropicStreamSummary {
        let raw = json!({
            "model": self.model,
            "usage": Value::Object(self.usage.clone()),
        });
        ResponsesToAnthropicStreamSummary {
            usage: provider_usage_from_value(&raw),
            response_id: self.response_id.clone(),
            compaction_output_seen: self.upstream_compaction_output_seen,
            model_output_seen: self.upstream_model_output_seen,
        }
    }
}

fn event_item_identity(value: &Value) -> String {
    value
        .get("item_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("output_index")
                .and_then(Value::as_u64)
                .map(|index| format!("output_{index}"))
        })
        .unwrap_or_else(|| "output_0".to_string())
}

fn item_identity(item: &Value, output_index: Option<u64>) -> String {
    item.get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| output_index.map(|index| format!("output_{index}")))
        .unwrap_or_else(|| "output_0".to_string())
}

fn event_content_key(value: &Value) -> String {
    format!(
        "text:{}:{}",
        event_item_identity(value),
        value
            .get("content_index")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    )
}

fn item_content_key(item: &Value, output_index: Option<u64>, content_index: u64) -> String {
    format!("text:{}:{content_index}", item_identity(item, output_index))
}

fn tool_item_key(item: &Value, output_index: Option<u64>) -> String {
    format!("tool:{}", item_identity(item, output_index))
}

fn missing_suffix(emitted: &str, complete: &str) -> String {
    if complete.starts_with(emitted) {
        complete[emitted.len()..].to_string()
    } else if emitted.is_empty() {
        complete.to_string()
    } else {
        String::new()
    }
}

fn json_argument_string(value: &Value) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| super::json_canonical::canonical_json_string(value))
}

fn custom_tool_wire_arguments(arguments: &str) -> String {
    let input = serde_json::from_str::<Value>(arguments)
        .unwrap_or_else(|_| Value::String(arguments.to_string()));
    super::json_canonical::canonical_json_string(&json!({ "input": input }))
}

fn anthropic_start_usage(usage: &Map<String, Value>) -> Value {
    let input_tokens = usage_value(usage, "input_tokens")
        .or_else(|| usage_value(usage, "prompt_tokens"))
        .unwrap_or(0);
    let mut result = Map::new();
    result.insert("input_tokens".to_string(), json!(input_tokens));
    if let Some(cached) = usage
        .get("input_tokens_details")
        .and_then(|value| value.get("cached_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| usage_value(usage, "cache_read_input_tokens"))
    {
        result.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    if let Some(created) = usage_value(usage, "cache_creation_input_tokens") {
        result.insert("cache_creation_input_tokens".to_string(), json!(created));
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
    if let Some(cached) = usage
        .get("input_tokens_details")
        .and_then(|value| value.get("cached_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| usage_value(usage, "cache_read_input_tokens"))
    {
        result.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    if let Some(created) = usage_value(usage, "cache_creation_input_tokens") {
        result.insert("cache_creation_input_tokens".to_string(), json!(created));
    }
    Value::Object(result)
}

fn usage_value(usage: &Map<String, Value>, key: &str) -> Option<u64> {
    usage.get(key).and_then(Value::as_u64)
}

fn anthropic_error(value: Option<&Value>, fallback_kind: &str) -> Value {
    let error = value
        .and_then(|value| {
            value
                .get("error")
                .filter(|error| !error.is_null())
                .or_else(|| {
                    value
                        .pointer("/response/error")
                        .filter(|error| !error.is_null())
                })
        })
        .unwrap_or_else(|| value.unwrap_or(&Value::Null));
    let message = error
        .get("message")
        .or_else(|| error.get("detail"))
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .filter(|message| !message.is_empty())
        .unwrap_or("Upstream Responses stream failed");
    let upstream_kind = error
        .get("type")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .filter(|kind| !kind.is_empty())
        .unwrap_or(fallback_kind);
    let kind = normalize_anthropic_error_kind(upstream_kind, message);
    json!({ "type": kind, "message": message })
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

/// Convert an upstream Responses stream to Anthropic Messages SSE.  The
/// adapter emits a single error frame for failed/truncated input and never
/// emits `message_stop` unless a real successful `response.completed` arrives.
pub(crate) fn create_anthropic_sse_stream_from_responses<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    fallback_model: String,
) -> (
    impl Stream<Item = Result<Bytes, std::io::Error>> + Send,
    ResponsesToAnthropicStreamSummaryHandle,
) {
    let summary = Arc::new(Mutex::new(ResponsesToAnthropicStreamSummary::default()));
    let summary_for_stream = summary.clone();
    let adapted = async_stream::stream! {
        let mut decoder = SseFrameDecoder::default();
        let mut state = ResponsesToAnthropicState::new(fallback_model);
        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if state.finished {
                        continue;
                    }
                    let frames = decoder.push(&bytes);
                    if decoder.overflowed() {
                        for event in state.failed_events(None, "stream_frame_too_large") {
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
                            "Stream error after Responses completion: {error}"
                        )));
                    } else {
                        for event in state.failed_message_events(
                            &format!("Upstream Responses stream error: {error}"),
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
                "Upstream Responses stream ended before response.completed",
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

    async fn collect(chunks: Vec<&'static str>) -> (String, ResponsesToAnthropicStreamSummary) {
        let stream = stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<Bytes, std::io::Error>(Bytes::from_static(chunk.as_bytes()))),
        );
        collect_stream(stream).await
    }

    async fn collect_stream<E: std::error::Error + Send + 'static>(
        upstream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    ) -> (String, ResponsesToAnthropicStreamSummary) {
        let (stream, summary) =
            create_anthropic_sse_stream_from_responses(upstream, "fallback".to_string());
        let output = stream.collect::<Vec<_>>().await;
        let output = String::from_utf8(
            output
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|bytes| bytes.to_vec())
                .collect(),
        )
        .unwrap();
        let summary = summary.lock().unwrap().clone();
        (output, summary)
    }

    #[tokio::test]
    async fn maps_text_usage_and_completed_to_anthropic_sse() {
        let (output, summary) = collect(vec![
            "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n\n",
        ])
        .await;
        let start = output.find("event: message_start").unwrap();
        let text = output.find("\"text\":\"hello\"").unwrap();
        let stop = output.find("event: message_stop").unwrap();
        assert!(start < text && text < stop);
        assert_eq!(output.matches("event: message_stop").count(), 1);
        assert_eq!(summary.response_id.as_deref(), Some("resp_1"));
        assert_eq!(summary.usage.input_tokens, 10);
        assert_eq!(summary.usage.output_tokens, 2);
        assert_eq!(summary.usage.cache_read_tokens, 8);
    }

    #[tokio::test]
    async fn maps_tool_deltas_and_terminal_item_once() {
        let (output, _) = collect(vec![
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":\"{\\\"q\\\":\"}\n\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\",\"output_index\":0,\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tool\",\"status\":\"completed\",\"output\":[{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}]}}\n\n",
        ])
        .await;
        assert_eq!(output.matches("\"type\":\"tool_use\"").count(), 1);
        assert_eq!(output.matches("\"partial_json\":\"{\\\"q\\\":").count(), 1);
        assert_eq!(output.matches("event: content_block_stop").count(), 1);
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }

    #[tokio::test]
    async fn terminal_only_message_and_tool_are_materialized_once() {
        let (output, _) = collect(vec![
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_terminal\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_terminal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"terminal text\"}]},{\"id\":\"fc_terminal\",\"type\":\"function_call\",\"call_id\":\"call_terminal\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}]}}\n\n",
        ])
        .await;
        assert_eq!(output.matches("terminal text").count(), 1);
        assert_eq!(output.matches("call_terminal").count(), 1);
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }

    #[tokio::test]
    async fn errors_and_truncated_eof_never_emit_message_stop() {
        let (error, _) = collect(vec![
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"message\":\"busy\"}}\n\n",
        ])
        .await;
        assert!(error.contains("event: error"));
        assert!(error.contains("busy"));
        assert!(!error.contains("event: message_stop"));

        let (truncated, _) = collect(vec![
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        ])
        .await;
        // `stream_truncated` is an internal cause; Anthropic only accepts its
        // documented error enum on the wire, so the adapter must normalize it.
        assert!(truncated.contains("\"type\":\"api_error\""));
        assert!(truncated.contains("ended before response.completed"));
        assert!(!truncated.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn decodes_split_utf8_and_crlf_before_forwarding_text() {
        let payload = "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_utf8\",\"delta\":\"你好\"}\r\n\r\nevent: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_utf8\",\"status\":\"completed\"}}\r\n\r\n";
        let split = payload.find('你').unwrap() + 1;
        let chunks = vec![
            Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&payload.as_bytes()[..split])),
            Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&payload.as_bytes()[split..])),
        ];
        let (output, _) = collect_stream(stream::iter(chunks)).await;
        assert!(output.contains("你好"));
        assert_eq!(output.matches("event: message_stop").count(), 1);
    }
}
