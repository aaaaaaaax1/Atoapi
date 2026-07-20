//! OpenAI Responses SSE -> Chat Completions SSE conversion.
//!
//! This is a one-way protocol adapter.  It does not own transport, retries,
//! relay lifetime, cache capture, or settlement; `stream_upstream` remains the
//! single owner of all of those responsibilities.

use super::{
    provider_usage_from_value, sse::SseFrameDecoder, value_has_compaction_output,
    value_has_model_output,
};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ChatToolCall {
    index: u64,
    call_id: String,
    name: String,
    arguments: String,
}

/// Raw Responses terminal metadata observed by the protocol adapter.
///
/// The adapter must not leak usage into a Chat SSE response unless the client
/// opted into stream_options.include_usage, but the owning relay still needs
/// the real upstream usage and response id for one-time settlement.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResponsesToChatStreamSummary {
    pub usage: crate::metrics::UsageRecord,
    pub response_id: Option<String>,
    pub compaction_output_seen: bool,
    pub model_output_seen: bool,
}

pub(crate) type ResponsesToChatStreamSummaryHandle = Arc<Mutex<ResponsesToChatStreamSummary>>;

#[derive(Debug)]
struct ResponsesToChatState {
    response_id: Option<String>,
    model: String,
    created: i64,
    role_sent: bool,
    finished: bool,
    succeeded: bool,
    next_tool_index: u64,
    tools: HashMap<String, ChatToolCall>,
    tool_by_output_index: HashMap<u64, String>,
    text_by_item: HashMap<String, String>,
    refusal_by_item: HashMap<String, String>,
    reasoning_by_item: HashMap<String, String>,
    usage: Map<String, Value>,
    include_usage: bool,
    upstream_compaction_output_seen: bool,
    upstream_model_output_seen: bool,
}

impl ResponsesToChatState {
    fn new(fallback_model: String, include_usage: bool) -> Self {
        Self {
            response_id: None,
            model: fallback_model,
            created: Utc::now().timestamp(),
            role_sent: false,
            finished: false,
            succeeded: false,
            next_tool_index: 0,
            tools: HashMap::new(),
            tool_by_output_index: HashMap::new(),
            text_by_item: HashMap::new(),
            refusal_by_item: HashMap::new(),
            reasoning_by_item: HashMap::new(),
            usage: Map::new(),
            include_usage,
            upstream_compaction_output_seen: false,
            upstream_model_output_seen: false,
        }
    }

    fn ingest(&mut self, event: Option<&str>, data: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        let parsed = serde_json::from_str::<Value>(data);
        if event == Some("error") {
            return match parsed {
                Ok(value) => self.failed_events(Some(&value), "error"),
                Err(_) => self.failed_message_events(data, "error"),
            };
        }
        let Ok(value) = parsed else {
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
            "response.output_item.added" => self.handle_output_item_added(&value),
            "response.output_item.done" => self.handle_output_item_done(&value),
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
            "response.reasoning_summary_text.delta" => self.handle_reasoning_delta(&value),
            "response.reasoning_summary_text.done" | "response.reasoning_summary_part.done" => {
                self.handle_reasoning_done(&value)
            }
            "response.completed" => self.completed_events(&value),
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
        ] {
            if let Some(value) = usage.get(key) {
                self.usage.insert(key.to_string(), value.clone());
            }
        }
    }

    fn handle_output_item_added(&mut self, value: &Value) -> Vec<Bytes> {
        let item = value.get("item").unwrap_or(value);
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        if !matches!(item_type, "function_call" | "custom_tool_call") {
            return Vec::new();
        }
        let (created, _) =
            self.ensure_tool_from_item(item, value.get("output_index").and_then(Value::as_u64));
        created
            .map(|tool| self.tool_declaration_chunk(tool))
            .into_iter()
            .collect()
    }

    fn handle_output_item_done(&mut self, value: &Value) -> Vec<Bytes> {
        self.materialize_output_item(
            value.get("item").unwrap_or(value),
            value.get("output_index").and_then(Value::as_u64),
        )
    }

    fn materialize_output_item(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        match item.get("type").and_then(Value::as_str).unwrap_or_default() {
            "message" => self.materialize_message(item, output_index),
            "function_call" | "custom_tool_call" => self.materialize_tool(item, output_index),
            "reasoning" => self.materialize_reasoning(item, output_index),
            _ => Vec::new(),
        }
    }

    fn materialize_message(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        let mut events = Vec::new();
        let mut emitted_content = false;
        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for (content_index, part) in content.iter().enumerate() {
                let part_type = part.get("type").and_then(Value::as_str).unwrap_or_default();
                if part_type == "refusal" {
                    if let Some(refusal) = part
                        .get("refusal")
                        .or_else(|| part.get("text"))
                        .and_then(Value::as_str)
                        .or_else(|| part.as_str())
                        .filter(|refusal| !refusal.is_empty())
                    {
                        emitted_content = true;
                        events.extend(self.emit_completed_refusal(
                            item_content_key(item, output_index, content_index as u64),
                            refusal,
                        ));
                    }
                } else {
                    let text = part.get("text").and_then(Value::as_str).or_else(|| {
                        matches!(part_type, "output_text" | "text" | "input_text")
                            .then(|| part.as_str())
                            .flatten()
                    });
                    if let Some(text) = text.filter(|text| !text.is_empty()) {
                        emitted_content = true;
                        events.extend(self.emit_completed_text(
                            item_content_key(item, output_index, content_index as u64),
                            text,
                        ));
                    }
                }
            }
        } else if let Some(refusal) = item
            .get("refusal")
            .and_then(Value::as_str)
            .filter(|refusal| !refusal.is_empty())
        {
            emitted_content = true;
            events.extend(
                self.emit_completed_refusal(item_content_key(item, output_index, 0), refusal),
            );
        } else if let Some(text) = item
            .get("content")
            .and_then(Value::as_str)
            .or_else(|| item.get("output_text").and_then(Value::as_str))
            .or_else(|| item.get("text").and_then(Value::as_str))
            .filter(|text| !text.is_empty())
        {
            emitted_content = true;
            events.extend(self.emit_completed_text(item_content_key(item, output_index, 0), text));
        }
        if !emitted_content {
            return Vec::new();
        }
        events
    }

    fn materialize_tool(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        let (created, item_id) = self.ensure_tool_from_item(item, output_index);
        let mut events = created
            .map(|tool| self.tool_declaration_chunk(tool))
            .into_iter()
            .collect::<Vec<_>>();
        if events.is_empty() {
            let complete = item
                .get("arguments")
                .or_else(|| item.get("input"))
                .map(json_argument_string)
                .unwrap_or_default();
            events.extend(self.append_tool_completion(&item_id, &complete));
        }
        events
    }

    fn materialize_reasoning(&mut self, item: &Value, output_index: Option<u64>) -> Vec<Bytes> {
        let mut events = Vec::new();
        if let Some(summary) = item.get("summary").and_then(Value::as_array) {
            for (summary_index, part) in summary.iter().enumerate() {
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.as_str())
                    .filter(|text| !text.is_empty())
                {
                    events.extend(self.emit_completed_reasoning(
                        item_summary_key(item, output_index, summary_index as u64),
                        text,
                    ));
                }
            }
        } else if let Some(text) = item
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            events.extend(
                self.emit_completed_reasoning(item_summary_key(item, output_index, 0), text),
            );
        }
        events
    }

    fn handle_refusal_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| value.pointer("/delta/refusal").and_then(Value::as_str))
            .unwrap_or_default();
        if delta.is_empty() {
            return Vec::new();
        }
        self.refusal_by_item
            .entry(event_content_key(value))
            .or_default()
            .push_str(delta);
        vec![self.chat_chunk(json!({ "refusal": delta }), None, None)]
    }

    fn handle_refusal_done(&mut self, value: &Value) -> Vec<Bytes> {
        let refusal = value
            .get("refusal")
            .or_else(|| value.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if refusal.is_empty() {
            return Vec::new();
        }
        self.emit_completed_refusal(event_content_key(value), refusal)
    }

    fn emit_completed_refusal(&mut self, key: String, refusal: &str) -> Vec<Bytes> {
        let emitted = self.refusal_by_item.entry(key).or_default();
        let missing = missing_suffix(emitted, refusal);
        if missing.is_empty() {
            return Vec::new();
        }
        emitted.push_str(&missing);
        vec![self.chat_chunk(json!({ "refusal": missing }), None, None)]
    }

    fn handle_text_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| value.pointer("/delta/text").and_then(Value::as_str))
            .unwrap_or_default();
        if delta.is_empty() {
            return Vec::new();
        }
        let key = event_content_key(value);
        self.text_by_item.entry(key).or_default().push_str(delta);
        vec![self.chat_chunk(json!({ "content": delta }), None, None)]
    }

    fn handle_text_done(&mut self, value: &Value) -> Vec<Bytes> {
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if text.is_empty() {
            return Vec::new();
        }
        self.emit_completed_text(event_content_key(value), text)
    }

    fn emit_completed_text(&mut self, key: String, text: &str) -> Vec<Bytes> {
        let emitted = self.text_by_item.entry(key).or_default();
        let missing = missing_suffix(emitted, text);
        if missing.is_empty() {
            return Vec::new();
        }
        emitted.push_str(&missing);
        vec![self.chat_chunk(json!({ "content": missing }), None, None)]
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
        let Some(item_id) = self.resolve_tool_id(value) else {
            return Vec::new();
        };
        let Some(tool) = self.tools.get_mut(&item_id) else {
            return Vec::new();
        };
        tool.arguments.push_str(delta);
        let index = tool.index;
        vec![self.chat_chunk(
            json!({ "tool_calls": [{ "index": index, "function": { "arguments": delta } }] }),
            None,
            None,
        )]
    }

    fn handle_tool_done(&mut self, value: &Value) -> Vec<Bytes> {
        let complete = value
            .get("arguments")
            .or_else(|| value.get("input"))
            .map(json_argument_string)
            .unwrap_or_default();
        let Some(item_id) = self.resolve_tool_id(value) else {
            return Vec::new();
        };
        self.append_tool_completion(&item_id, &complete)
    }

    fn handle_reasoning_delta(&mut self, value: &Value) -> Vec<Bytes> {
        let delta = value
            .get("delta")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if delta.is_empty() {
            return Vec::new();
        }
        self.reasoning_by_item
            .entry(event_summary_key(value))
            .or_default()
            .push_str(delta);
        vec![self.chat_chunk(json!({ "reasoning_content": delta }), None, None)]
    }

    fn handle_reasoning_done(&mut self, value: &Value) -> Vec<Bytes> {
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .or_else(|| value.pointer("/part/text").and_then(Value::as_str))
            .unwrap_or_default();
        if text.is_empty() {
            return Vec::new();
        }
        self.emit_completed_reasoning(event_summary_key(value), text)
    }

    fn emit_completed_reasoning(&mut self, key: String, text: &str) -> Vec<Bytes> {
        let emitted = self.reasoning_by_item.entry(key).or_default();
        let missing = missing_suffix(emitted, text);
        if missing.is_empty() {
            return Vec::new();
        }
        emitted.push_str(&missing);
        vec![self.chat_chunk(json!({ "reasoning_content": missing }), None, None)]
    }

    fn ensure_tool_from_item(
        &mut self,
        item: &Value,
        output_index: Option<u64>,
    ) -> (Option<ChatToolCall>, String) {
        let output_index = output_index
            .or_else(|| item.get("output_index").and_then(Value::as_u64))
            .unwrap_or(self.next_tool_index);
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("tool_item_{output_index}"));
        self.tool_by_output_index
            .insert(output_index, item_id.clone());
        if self.tools.contains_key(&item_id) {
            return (None, item_id);
        }
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| item_id.clone());
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or("tool")
            .to_string();
        let arguments = item
            .get("arguments")
            .or_else(|| item.get("input"))
            .map(json_argument_string)
            .unwrap_or_default();
        let tool = ChatToolCall {
            index: self.next_tool_index,
            call_id,
            name,
            arguments,
        };
        self.next_tool_index = self.next_tool_index.saturating_add(1);
        self.tools.insert(item_id.clone(), tool.clone());
        (Some(tool), item_id)
    }

    fn tool_declaration_chunk(&mut self, tool: ChatToolCall) -> Bytes {
        self.chat_chunk(
            json!({
                "tool_calls": [{
                    "index": tool.index,
                    "id": tool.call_id,
                    "type": "function",
                    "function": { "name": tool.name, "arguments": tool.arguments }
                }]
            }),
            None,
            None,
        )
    }

    fn append_tool_completion(&mut self, item_id: &str, complete: &str) -> Vec<Bytes> {
        if complete.is_empty() {
            return Vec::new();
        }
        let (index, missing) = {
            let Some(tool) = self.tools.get_mut(item_id) else {
                return Vec::new();
            };
            let missing = missing_suffix(&tool.arguments, complete);
            if missing.is_empty() {
                return Vec::new();
            }
            tool.arguments.push_str(&missing);
            (tool.index, missing)
        };
        vec![self.chat_chunk(
            json!({ "tool_calls": [{ "index": index, "function": { "arguments": missing } }] }),
            None,
            None,
        )]
    }

    fn completed_events(&mut self, value: &Value) -> Vec<Bytes> {
        let response = value.get("response").unwrap_or(value);
        self.observe_metadata(response);
        self.upstream_compaction_output_seen |= value_has_compaction_output(response);
        self.upstream_model_output_seen |= value_has_model_output(response);
        if response.get("error").is_some_and(|error| !error.is_null())
            || response.get("status").and_then(Value::as_str) == Some("failed")
            || response.get("status").and_then(Value::as_str) == Some("incomplete")
        {
            return self.failed_events(Some(response), "response.completed");
        }
        let mut events = Vec::new();
        if let Some(output) = response.get("output").and_then(Value::as_array) {
            for (output_index, item) in output.iter().enumerate() {
                events.extend(self.materialize_output_item(item, Some(output_index as u64)));
            }
        }
        let finish_reason = if self.tools.is_empty() {
            "stop"
        } else {
            "tool_calls"
        };
        let usage = chat_usage_map(&self.usage);
        self.finished = true;
        self.succeeded = true;
        events.push(self.chat_chunk(json!({}), Some(finish_reason), None));
        if self.include_usage {
            if let Some(usage) = usage {
                events.push(self.chat_usage_chunk(usage));
            }
        }
        events.push(Bytes::from_static(b"data: [DONE]\n\n"));
        events
    }

    fn failed_events(&mut self, value: Option<&Value>, fallback_kind: &str) -> Vec<Bytes> {
        self.failed_error_events(response_error(value, fallback_kind))
    }

    fn failed_message_events(&mut self, message: &str, fallback_kind: &str) -> Vec<Bytes> {
        let message = message.trim();
        self.failed_error_events(json!({
            "message": if message.is_empty() { "Upstream Responses stream failed" } else { message },
            "type": fallback_kind,
        }))
    }

    fn failed_error_events(&mut self, error: Value) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        self.succeeded = false;
        vec![sse_data(json!({ "error": error }))]
    }

    fn resolve_tool_id(&self, value: &Value) -> Option<String> {
        let output_index = value.get("output_index").and_then(Value::as_u64);
        value
            .get("item_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                output_index.and_then(|index| self.tool_by_output_index.get(&index).cloned())
            })
    }

    fn chat_chunk(
        &mut self,
        delta: Value,
        finish_reason: Option<&str>,
        usage: Option<Value>,
    ) -> Bytes {
        let mut delta = delta.as_object().cloned().unwrap_or_default();
        if !self.role_sent {
            delta
                .entry("role".to_string())
                .or_insert_with(|| Value::String("assistant".to_string()));
            self.role_sent = true;
        }
        let response_id = self
            .response_id
            .get_or_insert_with(|| format!("chatcmpl_{}", Uuid::new_v4().simple()))
            .clone();
        let mut chunk = json!({
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": 0,
                "delta": Value::Object(delta),
                "finish_reason": finish_reason,
            }]
        });
        if let Some(usage) = usage {
            chunk["usage"] = usage;
        }
        sse_data(chunk)
    }

    fn chat_usage_chunk(&mut self, usage: Value) -> Bytes {
        let response_id = self
            .response_id
            .get_or_insert_with(|| format!("chatcmpl_{}", Uuid::new_v4().simple()))
            .clone();
        sse_data(json!({
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [],
            "usage": usage,
        }))
    }

    fn stream_summary(&self) -> ResponsesToChatStreamSummary {
        let raw = json!({
            "model": self.model,
            "usage": Value::Object(self.usage.clone()),
        });
        ResponsesToChatStreamSummary {
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

fn event_summary_key(value: &Value) -> String {
    format!(
        "reasoning:{}:{}",
        event_item_identity(value),
        value
            .get("summary_index")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    )
}

fn item_summary_key(item: &Value, output_index: Option<u64>, summary_index: u64) -> String {
    format!(
        "reasoning:{}:{summary_index}",
        item_identity(item, output_index)
    )
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
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default())
}

fn response_error(value: Option<&Value>, fallback_kind: &str) -> Value {
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
        .cloned();
    if let Some(error) = error {
        return error;
    }
    let message = value
        .and_then(|value| {
            value
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/response/status").and_then(Value::as_str))
        })
        .filter(|message| !message.is_empty())
        .unwrap_or("Upstream Responses stream failed");
    let error_type = value
        .and_then(|value| value.get("type").and_then(Value::as_str))
        .filter(|kind| !kind.is_empty())
        .unwrap_or(fallback_kind);
    json!({ "message": message, "type": error_type })
}

fn chat_usage_map(usage: &Map<String, Value>) -> Option<Value> {
    let has_usage = usage.contains_key("input_tokens")
        || usage.contains_key("prompt_tokens")
        || usage.contains_key("output_tokens")
        || usage.contains_key("completion_tokens");
    if !has_usage {
        return None;
    }
    let input_tokens = usage_value(usage, "input_tokens")
        .or_else(|| usage_value(usage, "prompt_tokens"))
        .unwrap_or(0);
    let output_tokens = usage_value(usage, "output_tokens")
        .or_else(|| usage_value(usage, "completion_tokens"))
        .unwrap_or(0);
    let cached_tokens = usage
        .get("input_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| usage_value(usage, "cache_read_input_tokens"))
        .unwrap_or(0);
    let total_tokens = usage_value(usage, "total_tokens")
        .unwrap_or_else(|| input_tokens.saturating_add(output_tokens));
    Some(json!({
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_details": { "cached_tokens": cached_tokens }
    }))
}

fn usage_value(usage: &Map<String, Value>, key: &str) -> Option<u64> {
    usage.get(key).and_then(Value::as_u64)
}

fn sse_data(value: Value) -> Bytes {
    let payload = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!("data: {payload}\n\n"))
}

/// Convert an upstream Responses SSE body to Chat Completions SSE.  Errors and
/// premature EOF are represented as one Chat error frame without `[DONE]` so
/// the owning relay records a failed terminal rather than a successful one.
pub(crate) fn create_chat_sse_stream_from_responses<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    fallback_model: String,
    include_usage: bool,
) -> (
    impl Stream<Item = Result<Bytes, std::io::Error>> + Send,
    ResponsesToChatStreamSummaryHandle,
) {
    let summary = Arc::new(Mutex::new(ResponsesToChatStreamSummary::default()));
    let summary_for_stream = summary.clone();
    let adapted = async_stream::stream! {
        let mut decoder = SseFrameDecoder::default();
        let mut state = ResponsesToChatState::new(fallback_model, include_usage);
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
                        for event in state.failed_events(None, "stream_error") {
                            yield Ok(event);
                        }
                    }
                    break;
                }
            }
        }

        if !state.finished {
            for event in state.failed_events(None, "stream_truncated") {
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

    async fn collect(chunks: Vec<&'static str>) -> String {
        collect_with_usage(chunks, false).await.0
    }

    async fn collect_with_usage(
        chunks: Vec<&'static str>,
        include_usage: bool,
    ) -> (String, ResponsesToChatStreamSummary) {
        let stream = stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<Bytes, std::io::Error>(Bytes::from_static(chunk.as_bytes()))),
        );
        let (stream, summary) =
            create_chat_sse_stream_from_responses(stream, "fallback".to_string(), include_usage);
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
    async fn maps_text_usage_and_completed_to_chat_sse() {
        let (output, summary) = collect_with_usage(vec![
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hel\"}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"lo\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n\n",
        ], true)
        .await;
        assert!(output.contains("\"id\":\"resp_1\""));
        assert!(output.contains("\"content\":\"hel\""));
        assert!(output.contains("\"content\":\"lo\""));
        assert!(output.contains("\"finish_reason\":\"stop\""));
        assert!(output.contains("\"cached_tokens\":8"));
        assert!(output.contains("\"choices\":[]"));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
        assert_eq!(summary.usage.input_tokens, 10);
        assert_eq!(summary.usage.cache_read_tokens, 8);
    }

    #[tokio::test]
    async fn maps_tool_argument_deltas_without_duplicate_done_arguments() {
        let output = collect(vec![
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":\"a\"}\n\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\",\"output_index\":0,\"arguments\":\"ab\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tool\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
        ])
        .await;
        assert!(output.contains("\"name\":\"lookup\""));
        assert!(output.contains("\"arguments\":\"a\""));
        assert!(output.contains("\"arguments\":\"b\""));
        assert!(output.contains("\"finish_reason\":\"tool_calls\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
    }

    #[tokio::test]
    async fn error_and_truncated_eof_emit_one_error_without_done() {
        let error = collect(vec![
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"busy\",\"type\":\"server_error\"}}}\n\n",
        ])
        .await;
        assert!(error.contains("busy"));
        assert!(!error.contains("data: [DONE]"));

        let truncated = collect(vec![
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        ])
        .await;
        assert!(truncated.contains("stream_truncated"));
        assert!(!truncated.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn terminal_only_items_and_completed_output_are_materialized_once() {
        let output = collect(vec![
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_terminal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"terminal text\"},{\"type\":\"output_text\",\"text\":\"second segment\"}]}}\n\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"id\":\"fc_terminal\",\"type\":\"function_call\",\"call_id\":\"call_terminal\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_terminal\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_terminal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"terminal text\"},{\"type\":\"output_text\",\"text\":\"second segment\"}]},{\"id\":\"fc_terminal\",\"type\":\"function_call\",\"call_id\":\"call_terminal\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}]}}\n\n",
        ])
        .await;

        assert_eq!(output.matches("terminal text").count(), 1);
        assert_eq!(output.matches("second segment").count(), 1);
        assert_eq!(output.matches("call_terminal").count(), 1);
        assert!(output.contains("\"finish_reason\":\"tool_calls\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
    }

    #[tokio::test]
    async fn completed_output_without_deltas_materializes_message_tool_and_reasoning() {
        let output = collect(vec![
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_completed_only\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_completed_only\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"completed text\"}]},{\"id\":\"reasoning_completed_only\",\"type\":\"reasoning\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"completed reasoning\"}]},{\"id\":\"fc_completed_only\",\"type\":\"function_call\",\"call_id\":\"call_completed_only\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}]}}\n\n",
        ])
        .await;

        assert_eq!(output.matches("completed text").count(), 1);
        assert_eq!(output.matches("completed reasoning").count(), 1);
        assert_eq!(output.matches("call_completed_only").count(), 1);
        assert!(output.contains("\"finish_reason\":\"tool_calls\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
    }

    #[tokio::test]
    async fn refusal_deltas_and_terminal_message_content_are_preserved_once() {
        let output = collect(vec![
            "data: {\"type\":\"response.refusal.delta\",\"item_id\":\"msg_refusal\",\"output_index\":0,\"content_index\":0,\"delta\":\"I cannot\"}\n\ndata: {\"type\":\"response.refusal.done\",\"item_id\":\"msg_refusal\",\"output_index\":0,\"content_index\":0,\"refusal\":\"I cannot help with that\"}\n\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_refusal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"refusal\",\"refusal\":\"I cannot help with that\"}]}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_refusal\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"refusal\",\"refusal\":\"I cannot help with that\"}]}]}}\n\n",
        ])
        .await;

        assert_eq!(output.matches("\"refusal\":\"I cannot\"").count(), 1);
        assert_eq!(output.matches("\"refusal\":\" help with that\"").count(), 1);
        assert!(!output.contains("\"refusal\":\"I cannot help with that\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
    }

    #[tokio::test]
    async fn content_index_and_terminal_reasoning_do_not_drop_segments() {
        let output = collect(vec![
            "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_segments\",\"output_index\":0,\"content_index\":0,\"delta\":\"first\"}\n\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_segments\",\"output_index\":0,\"content_index\":0,\"text\":\"first\"}\n\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_segments\",\"output_index\":0,\"content_index\":1,\"text\":\"second\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.done\",\"item_id\":\"reasoning_terminal\",\"output_index\":1,\"summary_index\":0,\"text\":\"terminal reasoning\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_segments\",\"status\":\"completed\"}}\n\n",
        ])
        .await;

        assert_eq!(output.matches("\"content\":\"first\"").count(), 1);
        assert_eq!(output.matches("\"content\":\"second\"").count(), 1);
        assert_eq!(
            output
                .matches("\"reasoning_content\":\"terminal reasoning\"")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn named_error_frame_preserves_upstream_error_message() {
        let output = collect(vec![
            "event: error\ndata: {\"type\":\"server_error\",\"message\":\"busy upstream\"}\n\n",
        ])
        .await;

        assert!(output.contains("busy upstream"));
        assert!(output.contains("\"type\":\"server_error\""));
        assert!(!output.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn default_chat_stream_omits_usage_from_client_frames() {
        let (output, summary) = collect_with_usage(vec![
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_usage\",\"status\":\"completed\",\"usage\":{\"input_tokens\":9,\"output_tokens\":2}}}\n\n",
        ], false)
        .await;

        assert!(!output.contains("\"usage\":"));
        assert!(output.contains("\"finish_reason\":\"stop\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
        assert_eq!(summary.usage.input_tokens, 9);
        assert_eq!(summary.usage.output_tokens, 2);
    }
}
