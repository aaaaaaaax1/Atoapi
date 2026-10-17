//! Temporary 1.5.8 diagnostics. Remove this module and the TOOL_DIAGNOSTICS
//! hooks to remove the feature; it never participates in routing or retries.
use super::sse::{SseDecodeEvent, SseFrameDecoder};
use crate::config::Channel;
use chrono::Utc;
use serde::{
    de::{MapAccess, Visitor},
    Deserializer,
};
use serde_json::{json, value::RawValue, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender},
        Arc,
    },
};

const MAX_TOOLS: usize = 1024;
const MAX_LINKS: usize = 4096;
const MAX_LINE_BYTES: usize = 2 * 1024 * 1024;
const ROTATE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_RESPONSE_INSPECTION: usize = 8 * 1024 * 1024;
const QUEUE_CAPACITY: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct ToolDiagnostics {
    sender: SyncSender<Value>,
    dropped: Arc<AtomicU64>,
}

impl ToolDiagnostics {
    pub(crate) fn new(directory: &Path) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<Value>(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_for_writer = dropped.clone();
        let path = directory.join("tool-diagnostics.jsonl");
        // A dedicated bounded writer: no filesystem I/O or queue waits on the
        // request/relay tasks. Saturation is explicit, never a request failure.
        let spawned = std::thread::Builder::new()
            .name("tool-diagnostics".into())
            .spawn(move || {
                while let Ok(mut event) = receiver.recv() {
                    event["dropped_events_before"] =
                        json!(dropped_for_writer.swap(0, Ordering::Relaxed));
                    if append_event(&path, &event, ROTATE_BYTES).is_err() {
                        dropped_for_writer.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        if spawned.is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        Self { sender, dropped }
    }

    fn emit(&self, request_id: &str, stage: &str, metadata: Value) {
        let event = json!({
            "schema": 1, "at": Utc::now(), "request_id": request_id,
            "stage": stage, "metadata": metadata
        });
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn ingress(&self, request_id: &str, channel: &Channel, body: &Value) {
        let mut metadata = request_metadata(body);
        metadata["channel"] = json!(channel.label());
        metadata["source"] = json!("received_json");
        self.emit(request_id, "ingress", metadata);
    }

    pub(super) fn egress(&self, request_id: &str, channel: &Channel, wire: &[u8], body: &Value) {
        let mut metadata = request_metadata(body);
        // Read tool roots from actual frozen wire, not an earlier mutable body.
        // Native Responses orders tools/choice/parallel before input; the
        // visitor stops at input and never re-parses a giant conversation.
        match wire_tool_roots(wire, matches!(channel, Channel::Responses)) {
            Some(roots) => {
                let tools = registry_metadata(&roots);
                for key in [
                    "tools",
                    "tools_present",
                    "tool_choice",
                    "parallel_tool_calls",
                    "tools_complete",
                ] {
                    metadata[key] = tools[key].clone();
                }
                // Newer Codex requests carry their callable registry in an
                // input item named `additional_tools`. Keep that bounded
                // metadata from the frozen semantic body alongside the
                // top-level roots parsed from the actual wire.
                metadata["additional_tools_source"] = json!("frozen_semantic_body");
                metadata["source"] = json!("frozen_wire_tool_roots");
            }
            None => {
                metadata["additional_tools_source"] = json!("semantic_fallback");
                metadata["source"] = json!("frozen_semantic_fallback");
            }
        }
        metadata["history_source"] = json!("frozen_semantic_body");
        metadata["channel"] = json!(channel.label());
        self.emit(request_id, "egress", metadata);
    }

    pub(super) fn response(&self, request_id: &str, stage: &str, bytes: &[u8], is_sse: bool) {
        let mut observer = ResponseObserver::new(self.clone(), request_id, stage);
        if is_sse {
            observer.push(bytes);
        } else {
            observer.json(bytes);
        }
        observer.finish();
    }
}

fn append_event(path: &Path, event: &Value, rotate_bytes: u64) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(event)?;
    if bytes.len() > MAX_LINE_BYTES {
        bytes = serde_json::to_vec(&json!({
            "schema": 1, "at": event["at"], "request_id": event["request_id"],
            "stage": event["stage"], "incomplete": "record_size_limit",
            "dropped_events_before": event["dropped_events_before"]
        }))?;
    }
    bytes.push(b'\n');
    if fs::metadata(path)
        .map(|m| m.len() + bytes.len() as u64 > rotate_bytes)
        .unwrap_or(false)
    {
        let previous = path.with_file_name("tool-diagnostics.previous.jsonl");
        if previous.exists() {
            fs::remove_file(&previous)?;
        }
        fs::rename(path, previous)?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(&bytes)
}

// Never persist arbitrary strings (descriptions, args, output, errors, URLs,
// auth, prompts). Normal identifiers remain readable; unusual identifiers are
// represented by a stable hash, not truncated into misleading duplicate names.
fn identifier(value: &str) -> String {
    if !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:/".contains(&b))
    {
        value.to_string()
    } else {
        format!("sha256:{:x}", Sha256::digest(value.as_bytes()))
    }
}

fn copy_identifier(source: &Value, key: &str, target: &mut Value) {
    if let Some(value) = source.get(key).and_then(Value::as_str) {
        target[key] = json!(identifier(value));
    }
}

fn tool_identity(tool: &Value, namespace: &str) -> Value {
    let mut result = json!({});
    for key in [
        "type",
        "name",
        "namespace",
        "server_label",
        "mode",
        "execution",
    ] {
        copy_identifier(tool, key, &mut result);
    }
    if result.get("namespace").is_none() && !namespace.is_empty() {
        result["namespace"] = json!(namespace);
    }
    if let Some(function) = tool.get("function") {
        copy_identifier(function, "name", &mut result);
    }
    if let Some(value) = tool.get("defer_loading").and_then(Value::as_bool) {
        result["defer_loading"] = json!(value);
    }
    result
}

fn collect_tools(
    tools: &[Value],
    namespace: &str,
    depth: usize,
    output: &mut Vec<Value>,
    complete: &mut bool,
) {
    if depth > 16 {
        *complete = false;
        return;
    }
    for tool in tools {
        if output.len() >= MAX_TOOLS {
            *complete = false;
            break;
        }
        let identity = tool_identity(tool, namespace);
        let nested_namespace = if tool.get("type").and_then(Value::as_str) == Some("namespace") {
            identity
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(namespace)
                .to_string()
        } else {
            namespace.to_string()
        };
        output.push(identity);
        if let Some(children) = tool.get("tools").and_then(Value::as_array) {
            collect_tools(children, &nested_namespace, depth + 1, output, complete);
        }
    }
}

fn registry_metadata(body: &Value) -> Value {
    let mut tools = Vec::new();
    let mut complete = true;
    if let Some(items) = body.get("tools").and_then(Value::as_array) {
        collect_tools(items, "", 0, &mut tools, &mut complete);
    }
    let choice = match body.get("tool_choice") {
        Some(Value::String(choice)) => json!(identifier(choice)),
        Some(choice @ Value::Object(_)) => {
            let mut result = tool_identity(choice, "");
            if let Some(items) = choice.get("tools").and_then(Value::as_array) {
                let mut allowed = Vec::new();
                collect_tools(items, "", 0, &mut allowed, &mut complete);
                result["tools"] = json!(allowed);
            }
            result
        }
        _ => Value::Null,
    };
    let additional = additional_tools_metadata(body);
    json!({
        "tools_present": body.get("tools").is_some(), "tools": tools,
        "tools_complete": complete, "tool_choice": choice,
        "parallel_tool_calls": body.get("parallel_tool_calls").and_then(Value::as_bool),
        "additional_tools_present": additional["present"],
        "additional_tools": additional["tools"],
        "additional_tools_complete": additional["complete"]
    })
}

/// Newer Codex Responses requests carry the callable registry in an
/// `input` item named `additional_tools` instead of the top-level `tools`
/// field. Keep only bounded identities here; descriptions, schemas and
/// executable content never enter the diagnostic record.
fn additional_tools_metadata(body: &Value) -> Value {
    let mut tools = Vec::new();
    let mut present = false;
    let mut complete = true;
    if let Some(items) = body.get("input").and_then(Value::as_array) {
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("additional_tools") {
                continue;
            }
            present = true;
            if let Some(nested) = item.get("tools").and_then(Value::as_array) {
                collect_tools(nested, "", 0, &mut tools, &mut complete);
            } else {
                complete = false;
            }
        }
    }
    json!({"present": present, "tools": tools, "complete": complete})
}

#[derive(Default)]
struct Links {
    values: Vec<Value>,
    discovered_tools: Vec<Value>,
    discovered_complete: bool,
    complete: bool,
    examined: usize,
}

impl Links {
    fn new() -> Self {
        Self {
            complete: true,
            discovered_complete: true,
            ..Self::default()
        }
    }
    fn item(&mut self, item: &Value, depth: usize) {
        if depth > 8 || self.examined >= MAX_LINKS {
            self.complete = false;
            return;
        }
        self.examined += 1;
        let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
        let role = item.get("role").and_then(Value::as_str).unwrap_or("");
        if kind == "tool_search_output" {
            if let Some(tools) = item.get("tools").and_then(Value::as_array) {
                collect_tools(
                    tools,
                    "",
                    0,
                    &mut self.discovered_tools,
                    &mut self.discovered_complete,
                );
            }
        }
        if matches!(
            kind,
            "function_call"
                | "custom_tool_call"
                | "tool_search_call"
                | "function_call_output"
                | "custom_tool_call_output"
                | "tool_search_output"
                | "tool_use"
                | "tool_result"
        ) || role == "tool"
        {
            let mut link = json!({"type": if role == "tool" { "tool_result" } else { kind }});
            for key in [
                "id",
                "call_id",
                "tool_call_id",
                "tool_use_id",
                "name",
                "namespace",
                "status",
            ] {
                copy_identifier(item, key, &mut link);
            }
            self.values.push(link);
        }
        if let Some(calls) = item.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                if self.values.len() >= MAX_LINKS {
                    self.complete = false;
                    break;
                }
                let mut link = tool_identity(call, "");
                link["type"] = json!("function_call");
                copy_identifier(call, "id", &mut link);
                self.values.push(link);
            }
        }
        // Only known protocol containers; never traverse tool output/arguments.
        if matches!(kind, "message" | "tool_search_output") || matches!(role, "assistant" | "user")
        {
            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for child in content {
                    if self.examined >= MAX_LINKS {
                        self.complete = false;
                        break;
                    }
                    self.item(child, depth + 1);
                }
            }
        }
    }
    fn history(&mut self, body: &Value) {
        for key in ["input", "messages"] {
            if let Some(items) = body.get(key).and_then(Value::as_array) {
                // Inspect newest protocol items first, so long full replays do
                // not hide the call/result pair closest to the failure.
                for item in items.iter().rev() {
                    if self.examined >= MAX_LINKS {
                        self.complete = false;
                        break;
                    }
                    let links_start = self.values.len();
                    let tools_start = self.discovered_tools.len();
                    self.item(item, 0);
                    self.values[links_start..].reverse();
                    self.discovered_tools[tools_start..].reverse();
                }
            }
        }
        self.values.reverse();
        self.discovered_tools.reverse();
    }
}

fn link_id(link: &Value) -> Option<&str> {
    ["call_id", "tool_call_id", "tool_use_id", "id"]
        .iter()
        .find_map(|key| link.get(key).and_then(Value::as_str))
}

fn pairing(links: &[Value], external: bool, complete: bool) -> Value {
    let mut calls = BTreeSet::new();
    let mut results = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for link in links {
        let Some(id) = link_id(link) else {
            continue;
        };
        let kind = link.get("type").and_then(Value::as_str).unwrap_or("");
        if kind.ends_with("_output") || kind == "tool_result" {
            if !results.insert(id) {
                duplicates.insert(id);
            }
        } else if !calls.insert(id) {
            duplicates.insert(id);
        }
    }
    json!({
        "calls_without_results_in_this_request": calls.difference(&results).collect::<Vec<_>>(),
        "results_without_calls_in_this_request": results.difference(&calls).collect::<Vec<_>>(),
        "duplicate_ids": duplicates, "external_continuation": external,
        "complete": complete, "note": "observations_only_not_an_executor_verdict"
    })
}

fn request_metadata(body: &Value) -> Value {
    let mut result = registry_metadata(body);
    let mut links = Links::new();
    links.history(body);
    result["pairing"] = pairing(
        &links.values,
        body.get("previous_response_id").is_some(),
        links.complete,
    );
    result["history_links"] = json!(links.values);
    result["history_complete"] = json!(links.complete);
    result["history_selection"] = json!("most_recent_protocol_items");
    result["discovered_tools"] = json!(links.discovered_tools);
    result["discovered_tools_complete"] = json!(links.complete && links.discovered_complete);
    result
}

struct WireRoots<'a> {
    output: &'a mut Value,
    stop_at_input: bool,
    stopped: &'a mut bool,
}
impl<'de> Visitor<'de> for WireRoots<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("request object")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if self.stop_at_input && key == "input" {
                *self.stopped = true;
                return Ok(());
            }
            let raw = map.next_value::<&'de RawValue>()?;
            if matches!(
                key.as_str(),
                "tools" | "tool_choice" | "parallel_tool_calls"
            ) {
                if let Ok(value) = serde_json::from_str(raw.get()) {
                    self.output[&key] = value;
                }
            }
        }
        Ok(())
    }
}

fn wire_tool_roots(bytes: &[u8], stop_at_input: bool) -> Option<Value> {
    let mut output = json!({});
    let mut stopped = false;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let result = (&mut deserializer).deserialize_map(WireRoots {
        output: &mut output,
        stop_at_input,
        stopped: &mut stopped,
    });
    (result.is_ok() || stopped).then_some(output)
}

/// Side observer only: returned bytes and the pull/backpressure path are never
/// changed. Inspection has a total byte budget and explicit incomplete status.
pub(super) struct ResponseObserver {
    store: ToolDiagnostics,
    request_id: String,
    stage: String,
    decoder: SseFrameDecoder,
    calls: BTreeMap<String, Value>,
    inspected: usize,
    complete: bool,
    observed: bool,
    terminal: bool,
    error_class: Option<&'static str>,
    missing_output_call_id: Option<String>,
    finalized: bool,
}

impl ResponseObserver {
    pub(super) fn new(store: ToolDiagnostics, request_id: &str, stage: &str) -> Self {
        Self {
            store,
            request_id: request_id.into(),
            stage: stage.into(),
            decoder: SseFrameDecoder::default(),
            calls: BTreeMap::new(),
            inspected: 0,
            complete: true,
            observed: false,
            terminal: false,
            error_class: None,
            missing_output_call_id: None,
            finalized: false,
        }
    }
    pub(super) fn push(&mut self, bytes: &[u8]) {
        if !self.complete {
            return;
        }
        self.inspected = self.inspected.saturating_add(bytes.len());
        if self.inspected > MAX_RESPONSE_INSPECTION {
            self.complete = false;
            return;
        }
        for event in self.decoder.push_ordered(bytes) {
            match event {
                SseDecodeEvent::Frame(frame) => {
                    if frame.data == "[DONE]" {
                        self.terminal = true;
                    } else {
                        self.value_bytes(frame.data.as_bytes());
                    }
                }
                SseDecodeEvent::FrameOverflow => self.complete = false,
            }
        }
    }
    fn value_bytes(&mut self, bytes: &[u8]) {
        match serde_json::from_slice::<Value>(bytes) {
            Ok(value) => self.value(&value),
            Err(_) => self.complete = false,
        }
    }
    fn value(&mut self, value: &Value) {
        self.observed = true;
        if matches!(
            value.get("type").and_then(Value::as_str),
            Some("response.completed" | "response.failed" | "response.incomplete" | "message_stop")
        ) {
            self.terminal = true;
        }
        let error = value
            .get("error")
            .or_else(|| value.pointer("/response/error"));
        if let Some(error) = error {
            let message = error.get("message").and_then(Value::as_str).unwrap_or("");
            if let Some((_, rest)) =
                message.split_once("No tool output found for custom tool call ")
            {
                let id = rest
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_end_matches(['.', ',', ';']);
                self.error_class = Some("missing_custom_tool_output");
                self.missing_output_call_id = Some(identifier(id));
            } else {
                self.error_class = Some("upstream_error");
            }
        }
        let mut links = Links::new();
        if let Some(item) = value.get("item").or_else(|| value.get("content_block")) {
            links.item(item, 0);
        }
        let root = value.get("response").unwrap_or(value);
        if let Some(output) = root
            .get("output")
            .or_else(|| root.get("content"))
            .and_then(Value::as_array)
        {
            for item in output {
                links.item(item, 0);
            }
        }
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            for choice in choices {
                if let Some(message) = choice.get("message").or_else(|| choice.get("delta")) {
                    links.item(message, 0);
                }
            }
        }
        for link in links.values {
            if self.calls.len() >= MAX_TOOLS {
                self.complete = false;
                break;
            }
            let key = serde_json::to_string(&link).unwrap_or_default();
            self.calls.insert(key, link);
        }
        self.complete &= links.complete;
    }
    pub(super) fn json(&mut self, bytes: &[u8]) {
        if bytes.len() > MAX_RESPONSE_INSPECTION {
            self.complete = false;
            return;
        }
        self.value_bytes(bytes);
        self.terminal = self.observed;
    }
    fn emit_summary(&mut self, aborted: bool) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        self.store.emit(
            &self.request_id,
            &self.stage,
            json!({
                "tool_calls": std::mem::take(&mut self.calls).into_values().collect::<Vec<_>>(),
                "complete": self.complete && self.observed && !aborted,
                "observer_aborted": aborted,
                "terminal_event_seen": self.terminal,
                "error_class": self.error_class,
                "missing_output_call_id": self.missing_output_call_id.take(),
                "scope": "proxy_boundary_not_client_execution"
            }),
        );
    }
    pub(super) fn finish(mut self) {
        self.emit_summary(false);
    }
}

impl Drop for ResponseObserver {
    fn drop(&mut self) {
        self.emit_summary(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_is_complete_but_never_contains_content_or_schema() {
        let body = json!({"tools":[{"type":"namespace","name":"functions","tools":[{"type":"custom","name":"apply_patch","description":"SECRET_SCHEMA"}]}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"custom","name":"apply_patch"}]},"input":[{"type":"custom_tool_call","id":"ctc_1","call_id":"call_1","name":"apply_patch","input":"SECRET_ARGS"},{"type":"custom_tool_call_output","call_id":"call_1","output":"SECRET_OUTPUT"},{"role":"user","content":"SECRET_CHAT"}]});
        let result = request_metadata(&body);
        let encoded = result.to_string();
        assert!(!encoded.contains("SECRET"));
        assert_eq!(result["tools"][1]["namespace"], "functions");
        assert_eq!(
            result["pairing"]["calls_without_results_in_this_request"],
            json!([])
        );
        assert_eq!(result["history_links"].as_array().unwrap().len(), 2);
    }
    #[test]
    fn metadata_records_codex_additional_tools_without_content() {
        let body = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [{
                    "type": "namespace",
                    "name": "functions",
                    "description": "SECRET_NAMESPACE_SCHEMA",
                    "tools": [{
                        "type": "custom",
                        "name": "exec",
                        "description": "SECRET_EXEC_SCHEMA",
                        "format": {"type": "grammar", "definition": "SECRET_GRAMMAR"}
                    }]
                }]
            }]
        });
        let result = request_metadata(&body);
        let encoded = result.to_string();
        assert!(!encoded.contains("SECRET"));
        assert_eq!(result["tools_present"], false);
        assert_eq!(result["additional_tools_present"], true);
        assert_eq!(result["additional_tools_complete"], true);
        assert_eq!(result["additional_tools"][1]["name"], "exec");
        assert_eq!(result["additional_tools"][1]["namespace"], "functions");
    }
    #[test]
    fn missing_custom_output_and_wire_tool_loss_are_visible() {
        let input = json!({"tools":[{"type":"custom","name":"apply_patch"}],"input":[{"type":"custom_tool_call","id":"ctc_1","call_id":"call_1","name":"apply_patch"}]});
        assert_eq!(
            request_metadata(&input)["pairing"]["calls_without_results_in_this_request"],
            json!(["call_1"])
        );
        let wire = br#"{"tools":[],"tool_choice":"none","parallel_tool_calls":false,"input":[INVALID_NOT_PARSED]}"#;
        let roots = wire_tool_roots(wire, true).unwrap();
        assert_eq!(roots["tools"], json!([]));
        assert_eq!(roots["tool_choice"], "none");
    }
    #[test]
    fn long_history_keeps_recent_pairing_and_discovered_tool_names_only() {
        let mut input = vec![json!({"role":"user","content":"SECRET_CHAT"}); MAX_LINKS + 2];
        input.extend([
            json!({"type":"tool_search_call","call_id":"search_recent"}),
            json!({"type":"tool_search_output","call_id":"search_recent","tools":[{"type":"namespace","name":"local","tools":[{"type":"custom","name":"exec","description":"SECRET_SCHEMA"}]}]}),
            json!({"type":"custom_tool_call","call_id":"call_recent","name":"exec","input":"SECRET_ARGS"}),
            json!({"type":"custom_tool_call_output","call_id":"call_recent","output":"SECRET_OUTPUT"})
        ]);
        let result = request_metadata(&json!({"input":input}));
        assert_eq!(result["history_complete"], false);
        assert_eq!(result["discovered_tools_complete"], false);
        assert_eq!(result["history_links"][2]["call_id"], "call_recent");
        assert_eq!(result["history_links"][3]["call_id"], "call_recent");
        assert_eq!(
            result["pairing"]["calls_without_results_in_this_request"],
            json!([])
        );
        assert_eq!(result["discovered_tools"][1]["name"], "exec");
        assert!(!result.to_string().contains("SECRET"));
    }
    #[test]
    fn response_error_and_aborted_observer_are_emitted_once_without_error_content() {
        let (sender, receiver) = mpsc::sync_channel(8);
        let store = ToolDiagnostics {
            sender,
            dropped: Arc::new(AtomicU64::new(0)),
        };
        let mut observer = ResponseObserver::new(store.clone(), "test", "upstream_response");
        observer.json(br#"{"error":{"message":"No tool output found for custom tool call call_missing. SECRET_DETAIL"}}"#);
        observer.finish();
        let event = receiver.try_recv().unwrap();
        assert_eq!(
            event["metadata"]["error_class"],
            "missing_custom_tool_output"
        );
        assert_eq!(event["metadata"]["missing_output_call_id"], "call_missing");
        assert_eq!(event["metadata"]["observer_aborted"], false);
        assert!(!event.to_string().contains("SECRET"));
        assert!(receiver.try_recv().is_err());
        drop(ResponseObserver::new(store, "aborted", "upstream_response"));
        let aborted = receiver.try_recv().unwrap();
        assert_eq!(aborted["metadata"]["complete"], false);
        assert_eq!(aborted["metadata"]["observer_aborted"], true);
        assert!(receiver.try_recv().is_err());
    }
    #[test]
    fn rotation_keeps_only_two_bounded_files() {
        let directory =
            std::env::temp_dir().join(format!("atoapi-tool-diag-{}", uuid::Uuid::new_v4()));
        let path = directory.join("tool-diagnostics.jsonl");
        for index in 0..20 {
            append_event(&path, &json!({"index":index}), 80).unwrap();
        }
        assert!(fs::metadata(&path).unwrap().len() <= 80);
        assert!(
            fs::metadata(path.with_file_name("tool-diagnostics.previous.jsonl"))
                .unwrap()
                .len()
                <= 80
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
