#[cfg(test)]
use crate::config::ProviderCacheCapabilityStatus;
use crate::config::{AppConfig, Channel, ProviderCacheCapabilityField, ProviderConfig};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::prepared_wire_request::PreparedResponseBody;

pub(super) const PROVIDER_CACHE_METADATA_FIELDS: [&str; 4] = [
    "prompt_cache_key",
    "prompt_cache_retention",
    "prompt_cache_options",
    "prompt_cache_breakpoint",
];

const PROMPT_CACHE_RETENTION_VALUE: &str = "24h";

#[cfg(test)]
pub(super) const EFFECT_FIELDS: [ProviderCacheCapabilityField; 2] = [
    ProviderCacheCapabilityField::PromptCacheOptions,
    ProviderCacheCapabilityField::PromptCacheBreakpoint,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NativeCachePlan {
    preserve_prompt_cache_key: bool,
    enable_prompt_cache_retention: bool,
    enable_modern_options: bool,
    enable_explicit_breakpoint: bool,
}

/// Records what this request actually inserted before the body was frozen.
/// Presence alone is not evidence: a caller can send an identical field or
/// marker, so validation later joins this receipt with the final wire.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct CacheControlApplicationReceipt {
    changed_fields: Vec<String>,
    injected_fields: Vec<ProviderCacheCapabilityField>,
    injected_breakpoint: bool,
}

impl CacheControlApplicationReceipt {
    pub(super) fn changed_fields(&self) -> &[String] {
        &self.changed_fields
    }

    pub(super) fn injected_fields(&self) -> &[ProviderCacheCapabilityField] {
        &self.injected_fields
    }

    pub(super) const fn injected_breakpoint(&self) -> bool {
        self.injected_breakpoint
    }

    fn mark_changed(&mut self, key: &str) {
        mark_changed_field(&mut self.changed_fields, key);
    }

    fn mark_injected(&mut self, field: ProviderCacheCapabilityField) {
        if !self.injected_fields.contains(&field) {
            self.injected_fields.push(field);
        }
    }
}

#[cfg(test)]
pub(super) fn plan(
    config: &AppConfig,
    provider: &ProviderConfig,
    model: &str,
    channel: &Channel,
    key_id: Option<&str>,
) -> NativeCachePlan {
    plan_with_effect_scope_and_probe_fields(config, provider, model, channel, key_id, None, &[])
}

#[cfg(test)]
pub(super) fn plan_with_probe_fields(
    config: &AppConfig,
    provider: &ProviderConfig,
    model: &str,
    channel: &Channel,
    key_id: Option<&str>,
    controlled_probe_fields: &[ProviderCacheCapabilityField],
) -> NativeCachePlan {
    plan_with_effect_scope_and_probe_fields(
        config,
        provider,
        model,
        channel,
        key_id,
        None,
        controlled_probe_fields,
    )
}

pub(super) fn plan_with_effect_scope_and_probe_fields(
    config: &AppConfig,
    provider: &ProviderConfig,
    model: &str,
    channel: &Channel,
    key_id: Option<&str>,
    effect_scope_id: Option<&str>,
    controlled_probe_fields: &[ProviderCacheCapabilityField],
) -> NativeCachePlan {
    let probe_accepted = |field| {
        config.cache_capability_probe_accepted_for_key(&provider.id, model, channel, key_id, field)
    };
    let promoted = |field| {
        config.cache_capability_effect_promoted_for_scope(
            &provider.id,
            model,
            channel,
            key_id,
            effect_scope_id,
            field,
        )
    };
    let probe_selected = |field| controlled_probe_fields.contains(&field) && probe_accepted(field);
    let allowed = |field| probe_selected(field) || promoted(field);

    NativeCachePlan {
        // `prompt_cache_key` is never a normal routing rule. A schema probe
        // can accept the field without proving that it affects the upstream
        // cache, so retain it only for an administrator-started candidate.
        preserve_prompt_cache_key: probe_selected(ProviderCacheCapabilityField::PromptCacheKey),
        // Retention remains an explicit provider compatibility choice, but
        // ordinary traffic still requires measured, key-realm-scoped effect
        // evidence. A controlled candidate may use an accepted field solely
        // to collect that evidence.
        enable_prompt_cache_retention: provider.prompt_cache_retention_enabled
            && allowed(ProviderCacheCapabilityField::PromptCacheRetention),
        enable_modern_options: allowed(ProviderCacheCapabilityField::PromptCacheOptions),
        // This control has repeatedly been rejected by the active upstream
        // class. Production keeps its schema probe and final-wire diagnostics
        // available, but does not inject it until a placement-proven promotion
        // path exists. Test builds retain the controlled path so rejection and
        // one-shot settlement regressions stay covered.
        enable_explicit_breakpoint: cfg!(test)
            && allowed(ProviderCacheCapabilityField::PromptCacheBreakpoint),
    }
}

#[cfg(test)]
pub(super) fn apply(request: &mut Value, channel: &Channel, plan: NativeCachePlan) -> Vec<String> {
    let mut changed_fields = Vec::new();
    if let Some(object) = request.as_object_mut() {
        if !plan.preserve_prompt_cache_key && object.remove("prompt_cache_key").is_some() {
            mark_changed_field(&mut changed_fields, "prompt_cache_key");
        }
        if plan.enable_prompt_cache_retention {
            let retention = Value::String(PROMPT_CACHE_RETENTION_VALUE.to_string());
            if object.get("prompt_cache_retention") != Some(&retention) {
                object.insert("prompt_cache_retention".to_string(), retention);
                mark_changed_field(&mut changed_fields, "prompt_cache_retention");
            }
        } else if object.remove("prompt_cache_retention").is_some() {
            mark_changed_field(&mut changed_fields, "prompt_cache_retention");
        }
        if plan.enable_modern_options {
            if !object.contains_key("prompt_cache_options") {
                object.insert(
                    "prompt_cache_options".to_string(),
                    json!({"mode": "implicit", "ttl": "30m"}),
                );
                mark_changed_field(&mut changed_fields, "prompt_cache_options");
            }
        } else if object.remove("prompt_cache_options").is_some() {
            mark_changed_field(&mut changed_fields, "prompt_cache_options");
        }
    }

    if plan.enable_explicit_breakpoint {
        if !contains_protocol_cache_breakpoint(request, channel)
            && add_safe_explicit_breakpoint(request, channel)
        {
            if let Some(root_key) = cache_payload_root_key(channel) {
                mark_changed_field(&mut changed_fields, root_key);
            }
        }
    } else {
        record_removed_cache_breakpoint_fields(request, channel, &mut changed_fields);
    }
    changed_fields
}

/// Applies the same cache-control plan through the final wire holder. Every
/// mutation is constrained to a named root, so a late cache control cannot
/// leave the semantic body ahead of a retained draft member.
pub(super) fn apply_prepared(
    request: &mut PreparedResponseBody,
    channel: &Channel,
    plan: NativeCachePlan,
) -> Vec<String> {
    apply_prepared_with_receipt(request, channel, plan)
        .changed_fields
        .clone()
}

pub(super) fn apply_prepared_with_receipt(
    request: &mut PreparedResponseBody,
    channel: &Channel,
    plan: NativeCachePlan,
) -> CacheControlApplicationReceipt {
    let mut receipt = CacheControlApplicationReceipt::default();
    if !plan.preserve_prompt_cache_key && request.remove_root("prompt_cache_key") {
        receipt.mark_changed("prompt_cache_key");
    }
    if plan.enable_prompt_cache_retention {
        let retention = Value::String(PROMPT_CACHE_RETENTION_VALUE.to_string());
        let should_set = request.body().get("prompt_cache_retention") != Some(&retention);
        if should_set && request.set_root("prompt_cache_retention", retention) {
            receipt.mark_changed("prompt_cache_retention");
            receipt.mark_injected(ProviderCacheCapabilityField::PromptCacheRetention);
        }
    } else if request.remove_root("prompt_cache_retention") {
        receipt.mark_changed("prompt_cache_retention");
    }
    if plan.enable_modern_options {
        if request.body().get("prompt_cache_options").is_none()
            && request.set_root(
                "prompt_cache_options",
                json!({"mode": "implicit", "ttl": "30m"}),
            )
        {
            receipt.mark_changed("prompt_cache_options");
            receipt.mark_injected(ProviderCacheCapabilityField::PromptCacheOptions);
        }
    } else if request.remove_root("prompt_cache_options") {
        receipt.mark_changed("prompt_cache_options");
    }

    if plan.enable_explicit_breakpoint {
        if !contains_protocol_cache_breakpoint(request.body(), channel) {
            if let Some(root_key) = cache_payload_root_key(channel) {
                let changed = request
                    .mutate_root_if(
                        root_key,
                        |root| add_safe_explicit_breakpoint_in_root(root, channel),
                        |changed| *changed,
                    )
                    .unwrap_or(false);
                if changed {
                    receipt.mark_changed(root_key);
                    receipt.mark_injected(ProviderCacheCapabilityField::PromptCacheBreakpoint);
                    receipt.injected_breakpoint = true;
                    request.mark_atoapi_protocol_breakpoint_injected();
                }
            }
        }
    } else {
        record_removed_cache_breakpoint_fields_from_prepared(
            request,
            channel,
            &mut receipt.changed_fields,
        );
    }
    receipt
}

pub(super) fn present_fields(
    request: &Value,
    channel: &Channel,
) -> Vec<ProviderCacheCapabilityField> {
    let mut fields = Vec::new();
    if let Some(object) = request.as_object() {
        for (name, field) in [
            (
                "prompt_cache_key",
                ProviderCacheCapabilityField::PromptCacheKey,
            ),
            (
                "prompt_cache_retention",
                ProviderCacheCapabilityField::PromptCacheRetention,
            ),
            (
                "prompt_cache_options",
                ProviderCacheCapabilityField::PromptCacheOptions,
            ),
        ] {
            if object.contains_key(name) {
                fields.push(field);
            }
        }
    }
    if contains_protocol_cache_breakpoint(request, channel) {
        fields.push(ProviderCacheCapabilityField::PromptCacheBreakpoint);
    }
    fields
}

fn mark_changed_field(changed_fields: &mut Vec<String>, key: &str) {
    if !changed_fields.iter().any(|field| field == key) {
        changed_fields.push(key.to_string());
    }
}

fn cache_payload_root_key(channel: &Channel) -> Option<&'static str> {
    match channel {
        Channel::Responses => Some("input"),
        Channel::Chat => Some("messages"),
        Channel::Anthropic => None,
    }
}

#[cfg(test)]
fn record_removed_cache_breakpoint_fields(
    request: &mut Value,
    channel: &Channel,
    changed_fields: &mut Vec<String>,
) {
    let Some(object) = request.as_object_mut() else {
        remove_protocol_cache_breakpoints(request, channel);
        return;
    };
    if object.remove("prompt_cache_breakpoint").is_some() {
        mark_changed_field(changed_fields, "prompt_cache_breakpoint");
    }
    let mut nested_changed = Vec::new();
    let root_key = cache_payload_root_key(channel);
    if let Some(root_key) = root_key {
        if let Some(value) = object.get_mut(root_key) {
            if remove_protocol_cache_breakpoints_from_root(value, channel) {
                nested_changed.push(root_key.to_string());
            }
        }
    }
    for key in nested_changed {
        mark_changed_field(changed_fields, &key);
    }
}

fn record_removed_cache_breakpoint_fields_from_prepared(
    request: &mut PreparedResponseBody,
    channel: &Channel,
    changed_fields: &mut Vec<String>,
) {
    if request.remove_root("prompt_cache_breakpoint") {
        mark_changed_field(changed_fields, "prompt_cache_breakpoint");
    }
    if let Some(root_key) = cache_payload_root_key(channel) {
        let changed = request
            .mutate_root_if(
                root_key,
                |root| remove_protocol_cache_breakpoints_from_root(root, channel),
                |changed| *changed,
            )
            .unwrap_or(false);
        if changed {
            mark_changed_field(changed_fields, root_key);
        }
    }
}

pub(super) fn strip_all(value: &mut Value) {
    if let Some(map) = value.as_object_mut() {
        // Cache metadata is valid only at the request root. Do not recurse:
        // a tool result or JSON schema may legitimately contain a property
        // with the same name, and it is model input rather than a protocol
        // control.
        for field in PROVIDER_CACHE_METADATA_FIELDS {
            map.remove(field);
        }
    }
}

pub(super) fn baseline_probe_body(channel: &Channel, model: &str, nonce: &str) -> Value {
    match channel {
        Channel::Responses => json!({
            "model": model,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": format!("Atoapi cache compatibility check {nonce}.")},
                    {"type": "input_text", "text": "Reply only ACK."}
                ]
            }],
            "store": false,
            "stream": false,
            "max_output_tokens": 32
        }),
        Channel::Chat => json!({
            "model": model,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": format!("Atoapi cache compatibility check {nonce}.")},
                    {"type": "text", "text": "Reply only ACK."}
                ]
            }],
            "stream": false,
            "max_completion_tokens": 32
        }),
        Channel::Anthropic => json!({
            "model": model,
            "messages": [{"role": "user", "content": "Reply only ACK."}],
            "max_tokens": 32,
            "stream": false
        }),
    }
}

pub(super) fn field_probe_body(
    channel: &Channel,
    model: &str,
    nonce: &str,
    field: ProviderCacheCapabilityField,
) -> Value {
    let mut body = baseline_probe_body(channel, model, nonce);
    let Some(object) = body.as_object_mut() else {
        return body;
    };
    match field {
        ProviderCacheCapabilityField::PromptCacheKey => {
            object.insert(
                "prompt_cache_key".to_string(),
                Value::String(format!("atoapi-capability-{nonce}")),
            );
        }
        ProviderCacheCapabilityField::PromptCacheRetention => {
            object.insert(
                "prompt_cache_retention".to_string(),
                Value::String("24h".to_string()),
            );
        }
        ProviderCacheCapabilityField::PromptCacheOptions => {
            object.insert(
                "prompt_cache_options".to_string(),
                json!({"mode": "implicit", "ttl": "30m"}),
            );
        }
        ProviderCacheCapabilityField::PromptCacheBreakpoint => {
            add_probe_breakpoint(&mut body, channel);
        }
    }
    body
}

#[cfg(test)]
pub(super) fn effect_probe_body(
    channel: &Channel,
    model: &str,
    group_nonce: &str,
    request_nonce: &str,
    include_prompt_cache_key: bool,
    enable_modern_options: bool,
    enable_explicit_breakpoint: bool,
) -> Value {
    let stable_prefix = effect_probe_stable_prefix(group_nonce);
    let dynamic_tail = format!("Atoapi effect sample {request_nonce}. Reply only ACK.");
    let mut body = match channel {
        Channel::Responses => json!({
            "model": model,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": stable_prefix},
                    {"type": "input_text", "text": dynamic_tail}
                ]
            }],
            "store": false,
            "stream": false,
            "max_output_tokens": 16
        }),
        Channel::Chat => json!({
            "model": model,
            "messages": [
                {"role": "system", "content": [{"type": "text", "text": stable_prefix}]},
                {"role": "user", "content": [{"type": "text", "text": dynamic_tail}]}
            ],
            "stream": false,
            "max_completion_tokens": 16
        }),
        Channel::Anthropic => return Value::Null,
    };
    if let Some(object) = body.as_object_mut() {
        if include_prompt_cache_key {
            object.insert(
                "prompt_cache_key".to_string(),
                Value::String(format!("atoapi-effect-{group_nonce}")),
            );
        }
        if enable_modern_options {
            object.insert(
                "prompt_cache_options".to_string(),
                json!({"mode": "implicit", "ttl": "30m"}),
            );
        }
    }
    if enable_explicit_breakpoint {
        add_probe_breakpoint(&mut body, channel);
    }
    body
}

#[cfg(test)]
fn effect_probe_stable_prefix(group_nonce: &str) -> String {
    let seed = format!(
        "Atoapi cache effect verification {group_nonce}. The following stable prefix is intentionally repeated for cache measurement. "
    );
    seed.repeat(48)
}

#[cfg(test)]
fn add_safe_explicit_breakpoint(request: &mut Value, channel: &Channel) -> bool {
    if contains_protocol_cache_breakpoint(request, channel) {
        return false;
    }
    let root_key = match channel {
        Channel::Responses => "input",
        Channel::Chat => "messages",
        Channel::Anthropic => return false,
    };
    let Some(root) = request.get_mut(root_key) else {
        return false;
    };
    add_safe_explicit_breakpoint_in_root(root, channel)
}

fn add_safe_explicit_breakpoint_in_root(root: &mut Value, channel: &Channel) -> bool {
    // The cache marker becomes part of the upstream wire.  Mark the first
    // legal protocol block so its position remains stable when an Agent
    // appends later history items.  A tail-relative (for example,
    // penultimate) marker moves on every turn and splits the cache prefix.
    mark_first_protocol_block(root, channel)
}

fn add_probe_breakpoint(request: &mut Value, channel: &Channel) -> bool {
    let root_key = match channel {
        Channel::Responses => "input",
        Channel::Chat => "messages",
        Channel::Anthropic => return false,
    };
    let Some(root) = request.get_mut(root_key) else {
        return false;
    };
    mark_first_protocol_block(root, channel)
}

fn mark_first_protocol_block(value: &mut Value, channel: &Channel) -> bool {
    match value {
        Value::Array(items) => {
            for item in items {
                if mark_first_protocol_block(item, channel) {
                    return true;
                }
            }
        }
        Value::Object(map) => {
            if map
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| supported_breakpoint_type(channel, kind))
            {
                map.insert(
                    "prompt_cache_breakpoint".to_string(),
                    json!({"mode": "explicit"}),
                );
                return true;
            }
            if is_protocol_message(map, channel) {
                if let Some(child) = map.get_mut("content") {
                    if mark_first_protocol_block(child, channel) {
                        return true;
                    }
                }
            }
        }
        _ => {}
    }
    false
}

fn supported_breakpoint_type(channel: &Channel, kind: &str) -> bool {
    match channel {
        Channel::Responses => matches!(kind, "input_text" | "input_image" | "input_file"),
        Channel::Chat => matches!(
            kind,
            "text" | "image_url" | "input_audio" | "file" | "refusal"
        ),
        Channel::Anthropic => false,
    }
}

fn is_protocol_message(map: &serde_json::Map<String, Value>, _channel: &Channel) -> bool {
    if map.get("type").and_then(Value::as_str) == Some("message") {
        return true;
    }
    matches!(
        map.get("role").and_then(Value::as_str),
        Some("system" | "developer" | "user" | "assistant")
    )
}

/// Returns true only for a breakpoint placed in a request position the
/// upstream protocol recognizes. A same-named member inside tool output is
/// user data and must neither affect diagnostics nor be removed.
pub(super) fn contains_protocol_cache_breakpoint(value: &Value, channel: &Channel) -> bool {
    let Some(root) = value.as_object() else {
        return false;
    };
    root.contains_key("prompt_cache_breakpoint")
        || cache_payload_root_key(channel)
            .and_then(|key| root.get(key))
            .is_some_and(|payload| contains_protocol_cache_breakpoint_in_root(payload, channel))
}

pub(super) fn responses_input_contains_protocol_cache_breakpoint(value: &Value) -> bool {
    contains_protocol_cache_breakpoint_in_root(value, &Channel::Responses)
}

/// Returns a compact, versioned digest for exactly one legal Responses
/// breakpoint in the final wire. Source ownership comes from the prepared-body
/// insertion witness, never from this structural shape alone.
pub(super) fn responses_protocol_breakpoint_placement_digest(request: &Value) -> Option<String> {
    let input = request.get("input")?;
    let mut collector = ResponsesBreakpointCollector::default();
    collector.collect(input, &mut Vec::new());
    collector.finish()
}

/// Predicts the placement Atoapi would use before mutation. Existing protocol
/// markers deliberately block prediction: only a fresh local insertion can
/// later become a promotion certificate.
pub(super) fn predicted_responses_breakpoint_placement_digest(request: &Value) -> Option<String> {
    if contains_protocol_cache_breakpoint(request, &Channel::Responses) {
        return None;
    }
    let input = request.get("input")?;
    first_responses_protocol_block(input, &mut Vec::new()).map(|location| location.digest())
}

#[derive(Debug, Clone)]
struct ResponsesBreakpointLocation {
    path: Vec<ResponsesBreakpointPathSegment>,
    block_type: &'static str,
}

impl ResponsesBreakpointLocation {
    fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"atoapi-responses-breakpoint-placement-v2\0");
        hasher.update(self.block_type.as_bytes());
        hasher.update(b"\0");
        for segment in &self.path {
            match segment {
                ResponsesBreakpointPathSegment::ArrayIndex(index) => {
                    hasher.update(b"a");
                    hasher.update(index.to_le_bytes());
                }
                ResponsesBreakpointPathSegment::Content => hasher.update(b"c"),
            }
            hasher.update(b"\0");
        }
        format!("v2:{:x}", hasher.finalize())
    }
}

#[derive(Debug, Clone)]
enum ResponsesBreakpointPathSegment {
    ArrayIndex(u64),
    Content,
}

#[derive(Default)]
struct ResponsesBreakpointCollector {
    location: Option<ResponsesBreakpointLocation>,
    invalid: bool,
}

impl ResponsesBreakpointCollector {
    fn collect(&mut self, value: &Value, path: &mut Vec<ResponsesBreakpointPathSegment>) {
        match value {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    path.push(ResponsesBreakpointPathSegment::ArrayIndex(index as u64));
                    self.collect(item, path);
                    path.pop();
                }
            }
            Value::Object(map) => {
                if is_supported_protocol_block(map, &Channel::Responses) {
                    if let Some(marker) = map.get("prompt_cache_breakpoint") {
                        if is_atoapi_explicit_breakpoint(marker) {
                            let block_type = responses_breakpoint_block_type(map);
                            if self
                                .location
                                .replace(ResponsesBreakpointLocation {
                                    path: path.clone(),
                                    block_type,
                                })
                                .is_some()
                            {
                                self.invalid = true;
                            }
                        } else {
                            self.invalid = true;
                        }
                    }
                    return;
                }
                if is_protocol_message(map, &Channel::Responses) {
                    if let Some(content) = map.get("content") {
                        path.push(ResponsesBreakpointPathSegment::Content);
                        self.collect(content, path);
                        path.pop();
                    }
                }
            }
            _ => {}
        }
    }

    fn finish(self) -> Option<String> {
        (!self.invalid)
            .then_some(self.location)
            .flatten()
            .map(|location| location.digest())
    }
}

fn first_responses_protocol_block(
    value: &Value,
    path: &mut Vec<ResponsesBreakpointPathSegment>,
) -> Option<ResponsesBreakpointLocation> {
    match value {
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                path.push(ResponsesBreakpointPathSegment::ArrayIndex(index as u64));
                let found = first_responses_protocol_block(item, path);
                path.pop();
                if found.is_some() {
                    return found;
                }
            }
            None
        }
        Value::Object(map) => {
            if is_supported_protocol_block(map, &Channel::Responses) {
                let block_type = responses_breakpoint_block_type(map);
                return Some(ResponsesBreakpointLocation {
                    path: path.clone(),
                    block_type,
                });
            }
            if is_protocol_message(map, &Channel::Responses) {
                if let Some(content) = map.get("content") {
                    path.push(ResponsesBreakpointPathSegment::Content);
                    let found = first_responses_protocol_block(content, path);
                    path.pop();
                    return found;
                }
            }
            None
        }
        _ => None,
    }
}

fn responses_breakpoint_block_type(map: &serde_json::Map<String, Value>) -> &'static str {
    match map.get("type").and_then(Value::as_str) {
        Some("input_text") => "input_text",
        Some("input_image") => "input_image",
        Some("input_file") => "input_file",
        _ => unreachable!("only supported Responses blocks reach placement collection"),
    }
}

/// Compares two Responses input items while treating only Atoapi's own legal
/// protocol breakpoint as wire metadata.  Tool output, schemas, call ids and
/// unknown fields remain exact comparisons.  This lets a lineage store the
/// actual final wire input without mistaking its own marker for lost context
/// on the next append-only request.
pub(super) fn responses_input_item_equal_ignoring_protocol_breakpoint(
    left: &Value,
    right: &Value,
) -> bool {
    protocol_value_equal_ignoring_breakpoint(left, right, &Channel::Responses)
}

fn protocol_value_equal_ignoring_breakpoint(
    left: &Value,
    right: &Value,
    channel: &Channel,
) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (Value::Array(left), Value::Array(right)) if left.len() == right.len() => left
            .iter()
            .zip(right)
            .all(|(left, right)| protocol_value_equal_ignoring_breakpoint(left, right, channel)),
        (Value::Object(left), Value::Object(right))
            if is_supported_protocol_block(left, channel)
                && is_supported_protocol_block(right, channel) =>
        {
            protocol_block_equal_ignoring_breakpoint(left, right)
        }
        (Value::Object(left), Value::Object(right))
            if is_protocol_message(left, channel) && is_protocol_message(right, channel) =>
        {
            protocol_message_equal_ignoring_breakpoint(left, right, channel)
        }
        _ => false,
    }
}

fn is_supported_protocol_block(map: &serde_json::Map<String, Value>, channel: &Channel) -> bool {
    map.get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| supported_breakpoint_type(channel, kind))
}

fn protocol_block_equal_ignoring_breakpoint(
    left: &serde_json::Map<String, Value>,
    right: &serde_json::Map<String, Value>,
) -> bool {
    let left_breakpoint = left.get("prompt_cache_breakpoint");
    let right_breakpoint = right.get("prompt_cache_breakpoint");
    let breakpoint_compatible = match (left_breakpoint, right_breakpoint) {
        (None, None) => true,
        (Some(value), None) | (None, Some(value)) => is_atoapi_explicit_breakpoint(value),
        (Some(value), Some(other)) => {
            is_atoapi_explicit_breakpoint(value) && is_atoapi_explicit_breakpoint(other)
        }
    };
    if !breakpoint_compatible {
        return false;
    }
    left.iter()
        .filter(|(key, _)| key.as_str() != "prompt_cache_breakpoint")
        .all(|(key, value)| right.get(key) == Some(value))
        && right
            .keys()
            .filter(|key| key.as_str() != "prompt_cache_breakpoint")
            .all(|key| left.contains_key(key))
}

fn protocol_message_equal_ignoring_breakpoint(
    left: &serde_json::Map<String, Value>,
    right: &serde_json::Map<String, Value>,
    channel: &Channel,
) -> bool {
    left.iter()
        .all(|(key, value)| match (key.as_str(), right.get(key)) {
            ("content", Some(other)) => {
                protocol_value_equal_ignoring_breakpoint(value, other, channel)
            }
            (_, Some(other)) => value == other,
            (_, None) => false,
        })
        && right.keys().all(|key| left.contains_key(key))
}

fn is_atoapi_explicit_breakpoint(value: &Value) -> bool {
    value.as_object().is_some_and(|map| {
        map.len() == 1 && map.get("mode").and_then(Value::as_str) == Some("explicit")
    })
}

fn contains_protocol_cache_breakpoint_in_root(value: &Value, channel: &Channel) -> bool {
    match value {
        Value::Array(items) => items
            .iter()
            .any(|item| contains_protocol_cache_breakpoint_in_root(item, channel)),
        Value::Object(map) => {
            if map
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| supported_breakpoint_type(channel, kind))
                && map.contains_key("prompt_cache_breakpoint")
            {
                return true;
            }
            is_protocol_message(map, channel)
                && map.get("content").is_some_and(|content| {
                    contains_protocol_cache_breakpoint_in_root(content, channel)
                })
        }
        _ => false,
    }
}

#[cfg(test)]
fn remove_protocol_cache_breakpoints(value: &mut Value, channel: &Channel) -> bool {
    let Some(root) = value.as_object_mut() else {
        return false;
    };
    let mut changed = root.remove("prompt_cache_breakpoint").is_some();
    if let Some(root_key) = cache_payload_root_key(channel) {
        if let Some(payload) = root.get_mut(root_key) {
            changed |= remove_protocol_cache_breakpoints_from_root(payload, channel);
        }
    }
    changed
}

fn remove_protocol_cache_breakpoints_from_root(value: &mut Value, channel: &Channel) -> bool {
    match value {
        Value::Array(items) => items.iter_mut().fold(false, |changed, item| {
            changed | remove_protocol_cache_breakpoints_from_root(item, channel)
        }),
        Value::Object(map) => {
            let supported_block = map
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| supported_breakpoint_type(channel, kind));
            let mut changed = supported_block && map.remove("prompt_cache_breakpoint").is_some();
            if is_protocol_message(map, channel) {
                if let Some(content) = map.get_mut("content") {
                    changed |= remove_protocol_cache_breakpoints_from_root(content, channel);
                }
            }
            changed
        }
        _ => false,
    }
}

#[cfg(test)]
fn contains_cache_breakpoint(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.contains_key("prompt_cache_breakpoint")
                || map.values().any(contains_cache_breakpoint)
        }
        Value::Array(items) => items.iter().any(contains_cache_breakpoint),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    const EFFECT_SCOPE_NONE: &str = "cache-effect-v2:test-realm:stream:no-store:bp=none";
    const EFFECT_SCOPE_BREAKPOINT: &str =
        "cache-effect-v2:test-realm:stream:no-store:bp=v2:test-placement";

    fn provider(base_url: &str) -> ProviderConfig {
        ProviderConfig {
            id: "provider-a".to_string(),
            name: "Provider A".to_string(),
            base_url: base_url.to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn third_party_unverified_plan_removes_unconfirmed_controls() {
        let config = AppConfig::default();
        let plan = plan(
            &config,
            &provider("https://third.example/v1"),
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
        );
        let mut body = json!({
            "prompt_cache_key": "stable",
            "prompt_cache_retention": "24h",
            "prompt_cache_options": {"mode": "implicit"},
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        let changed = apply(&mut body, &Channel::Responses, plan);
        assert_eq!(
            changed,
            vec![
                "prompt_cache_key",
                "prompt_cache_retention",
                "prompt_cache_options"
            ]
        );
        assert!(body.get("prompt_cache_key").is_none());
        assert!(body.get("prompt_cache_retention").is_none());
        assert!(body.get("prompt_cache_options").is_none());
        assert!(!contains_cache_breakpoint(&body));
    }

    #[test]
    fn verified_official_modern_controls_replace_legacy_retention_and_add_breakpoint() {
        let mut config = AppConfig::default();
        for field in [
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
        ] {
            config.record_cache_capability_probe(
                "provider-a",
                "gpt-5.6-luna",
                Channel::Responses,
                field,
                ProviderCacheCapabilityStatus::Verified,
                None,
            );
        }
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_BREAKPOINT),
            &EFFECT_FIELDS,
            crate::config::ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let plan = plan_with_effect_scope_and_probe_fields(
            &config,
            &provider("https://api.openai.com/v1"),
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_BREAKPOINT),
            &[],
        );
        let mut body = json!({
            "prompt_cache_key": "stable",
            "prompt_cache_retention": "24h",
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        let changed = apply(&mut body, &Channel::Responses, plan);
        assert_eq!(
            changed,
            vec![
                "prompt_cache_key",
                "prompt_cache_retention",
                "prompt_cache_options",
                "input"
            ]
        );
        assert!(body.get("prompt_cache_retention").is_none());
        assert_eq!(body["prompt_cache_options"]["ttl"], "30m");
        assert_eq!(
            body["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert!(body["input"][0]["content"][1]
            .get("prompt_cache_breakpoint")
            .is_none());
    }

    #[test]
    fn prepared_cache_control_application_matches_the_plain_value_path() {
        let config = AppConfig::default();
        let plan = plan(
            &config,
            &provider("https://api.openai.com/v1"),
            "gpt-5.6",
            &Channel::Responses,
            None,
        );
        let initial = json!({
            "prompt_cache_key": "stable",
            "prompt_cache_retention": "24h",
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        let mut expected = initial.clone();
        let expected_changed = apply(&mut expected, &Channel::Responses, plan);

        let mut prepared = PreparedResponseBody::plain(initial);
        let actual_changed = apply_prepared(&mut prepared, &Channel::Responses, plan);

        assert_eq!(actual_changed, expected_changed);
        assert_eq!(prepared.body(), &expected);
    }

    #[test]
    fn promoted_retention_is_injected_into_the_prepared_wire() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheRetention,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_NONE),
            &[ProviderCacheCapabilityField::PromptCacheRetention],
            crate::config::ProviderCacheEffectStatus::Promoted,
            Some("measured retention benefit".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let mut body = PreparedResponseBody::responses(json!({
            "model":"gpt-5.6-terra",
            "input":[{"type":"message","role":"user","content":"hello"}]
        }));

        let changed = apply_prepared(
            &mut body,
            &Channel::Responses,
            plan_with_effect_scope_and_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some(EFFECT_SCOPE_NONE),
                &[],
            ),
        );

        assert!(changed
            .iter()
            .any(|field| field == "prompt_cache_retention"));
        assert_eq!(
            body.body()["prompt_cache_retention"],
            PROMPT_CACHE_RETENTION_VALUE
        );
    }

    #[test]
    fn promoted_native_controls_apply_only_to_the_measured_effect_scope() {
        let mut config = AppConfig::default();
        for field in [ProviderCacheCapabilityField::PromptCacheRetention] {
            config.record_cache_capability_probe(
                "provider-a",
                "gpt-5.6-terra",
                Channel::Responses,
                field,
                ProviderCacheCapabilityStatus::Verified,
                None,
            );
            config.record_cache_capability_effect_for_scope(
                "provider-a",
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some("cache-effect-v2:realm-a:stream:no-store:bp=none"),
                &[field],
                crate::config::ProviderCacheEffectStatus::Promoted,
                Some("measured only in realm-a stream/no-store".to_string()),
                Some(0),
                Some(512),
                Some(100),
                Some(100),
            );
        }
        let initial = json!({
            "model":"gpt-5.6-terra",
            "input":[{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });

        let mut mismatched = PreparedResponseBody::responses(initial.clone());
        apply_prepared(
            &mut mismatched,
            &Channel::Responses,
            plan_with_effect_scope_and_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some("cache-effect-v2:realm-a:sync:no-store:bp=none"),
                &[],
            ),
        );
        assert!(mismatched.body().get("prompt_cache_retention").is_none());
        assert!(!contains_protocol_cache_breakpoint(
            mismatched.body(),
            &Channel::Responses
        ));

        let mut matched = PreparedResponseBody::responses(initial);
        apply_prepared(
            &mut matched,
            &Channel::Responses,
            plan_with_effect_scope_and_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some("cache-effect-v2:realm-a:stream:no-store:bp=none"),
                &[],
            ),
        );
        assert_eq!(
            matched.body()["prompt_cache_retention"],
            PROMPT_CACHE_RETENTION_VALUE
        );
        assert!(!contains_protocol_cache_breakpoint(
            matched.body(),
            &Channel::Responses
        ));
    }

    #[test]
    fn promoted_breakpoint_uses_the_first_append_stable_protocol_position() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_BREAKPOINT),
            &[ProviderCacheCapabilityField::PromptCacheBreakpoint],
            crate::config::ProviderCacheEffectStatus::Promoted,
            Some("previously measured breakpoint benefit".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let mut body = PreparedResponseBody::responses(json!({
            "model":"gpt-5.6-terra",
            "input":[{"type":"message","role":"user","content":[
                {"type":"input_text","text":"only legal block"}
            ]}]
        }));

        let changed = apply_prepared(
            &mut body,
            &Channel::Responses,
            plan_with_effect_scope_and_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some(EFFECT_SCOPE_BREAKPOINT),
                &[],
            ),
        );

        assert!(changed.iter().any(|field| field == "input"));
        assert!(contains_protocol_cache_breakpoint(
            body.body(),
            &Channel::Responses
        ));
        assert!(present_fields(body.body(), &Channel::Responses)
            .contains(&ProviderCacheCapabilityField::PromptCacheBreakpoint));
        assert_eq!(
            body.body()["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
    }

    #[test]
    fn prepared_plan_preserves_an_existing_protocol_breakpoint() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-terra",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheBreakpoint,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_BREAKPOINT),
            &[ProviderCacheCapabilityField::PromptCacheBreakpoint],
            crate::config::ProviderCacheEffectStatus::Promoted,
            Some("measured breakpoint benefit".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let mut body = PreparedResponseBody::responses(json!({
            "model":"gpt-5.6-terra",
            "input":[{"type":"message","role":"user","content":[{
                "type":"input_text",
                "text":"stable",
                "prompt_cache_breakpoint":{"mode":"explicit"}
            }]}]
        }));

        let changed = apply_prepared(
            &mut body,
            &Channel::Responses,
            plan_with_effect_scope_and_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Responses,
                None,
                Some(EFFECT_SCOPE_BREAKPOINT),
                &[],
            ),
        );

        assert!(!changed.iter().any(|field| field == "input"));
        assert_eq!(
            body.body()["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
    }

    #[test]
    fn chat_tool_content_is_not_treated_as_a_cache_breakpoint_position() {
        let config = AppConfig::default();
        let mut body = json!({
            "messages":[
                {"role":"user","content":[{"type":"text","text":"question"}]},
                {"role":"tool","content":[{
                    "type":"text",
                    "text":"tool output",
                    "prompt_cache_breakpoint":{"source":"tool-data"}
                }]}
            ]
        });

        apply(
            &mut body,
            &Channel::Chat,
            plan(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-terra",
                &Channel::Chat,
                None,
            ),
        );

        assert_eq!(
            body["messages"][1]["content"][0]["prompt_cache_breakpoint"]["source"],
            "tool-data"
        );
    }

    #[test]
    fn explicit_breakpoint_stays_at_the_same_prefix_block_after_an_append() {
        let mut first = json!({
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable prefix"},
                {"type":"input_text","text":"first dynamic tail"}
            ]}]
        });
        assert!(add_safe_explicit_breakpoint(
            &mut first,
            &Channel::Responses
        ));
        assert_eq!(
            first["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );

        let mut appended = first.clone();
        appended["input"].as_array_mut().unwrap().push(json!({
            "type":"message",
            "role":"user",
            "content":[{"type":"input_text","text":"next dynamic tail"}]
        }));
        assert!(!add_safe_explicit_breakpoint(
            &mut appended,
            &Channel::Responses
        ));
        assert_eq!(
            appended["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert!(appended["input"][0]["content"][1]
            .get("prompt_cache_breakpoint")
            .is_none());
        assert!(appended["input"][1]["content"][0]
            .get("prompt_cache_breakpoint")
            .is_none());
    }

    #[test]
    fn breakpoint_placement_digest_is_append_stable_and_fails_closed_when_moved() {
        let mut first = json!({
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable prefix"},
                {"type":"input_text","text":"first tail"}
            ]}]
        });
        assert!(add_safe_explicit_breakpoint(
            &mut first,
            &Channel::Responses
        ));
        let first_digest = responses_protocol_breakpoint_placement_digest(&first)
            .expect("the legal marker must have one exact placement");

        let mut appended = first.clone();
        appended["input"].as_array_mut().unwrap().push(json!({
            "type":"message",
            "role":"user",
            "content":[{"type":"input_text","text":"next tail"}]
        }));
        assert_eq!(
            responses_protocol_breakpoint_placement_digest(&appended),
            Some(first_digest.clone()),
            "append-only history must retain the original cache boundary"
        );

        let mut moved = first;
        moved["input"][0]["content"][0]
            .as_object_mut()
            .unwrap()
            .remove("prompt_cache_breakpoint");
        moved["input"][0]["content"][1]["prompt_cache_breakpoint"] = json!({"mode":"explicit"});
        assert_ne!(
            responses_protocol_breakpoint_placement_digest(&moved),
            Some(first_digest),
            "a moved marker is a final-wire continuity break"
        );

        moved["input"][0]["content"][1]["prompt_cache_breakpoint"] =
            json!({"mode":"caller-defined"});
        assert!(
            responses_protocol_breakpoint_placement_digest(&moved).is_none(),
            "a non-Atoapi marker is never promoted as exact placement evidence"
        );
    }

    #[test]
    fn predicted_breakpoint_placement_matches_only_a_fresh_local_insertion() {
        let mut request = json!({
            "input":[{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"tail"}
            ]}]
        });
        let predicted = predicted_responses_breakpoint_placement_digest(&request)
            .expect("a legal first block has a deterministic predicted placement");
        assert!(add_safe_explicit_breakpoint(
            &mut request,
            &Channel::Responses
        ));
        assert_eq!(
            responses_protocol_breakpoint_placement_digest(&request),
            Some(predicted)
        );
        assert!(
            predicted_responses_breakpoint_placement_digest(&request).is_none(),
            "a caller or prior marker is never reused as a fresh insertion certificate"
        );
    }

    #[test]
    fn lineage_comparison_ignores_only_the_legal_explicit_protocol_marker() {
        let unmarked = json!({
            "type":"message",
            "role":"user",
            "content":[{"type":"input_text","text":"stable"}],
            "x_unknown":{"kept":true}
        });
        let mut marked = unmarked.clone();
        marked["content"][0]["prompt_cache_breakpoint"] = json!({"mode":"explicit"});
        assert!(responses_input_item_equal_ignoring_protocol_breakpoint(
            &marked, &unmarked
        ));

        let mut changed_unknown = unmarked.clone();
        changed_unknown["x_unknown"]["kept"] = json!(false);
        assert!(!responses_input_item_equal_ignoring_protocol_breakpoint(
            &marked,
            &changed_unknown
        ));

        let tool_output = json!({
            "type":"function_call_output",
            "call_id":"call-a",
            "output":{"prompt_cache_breakpoint":{"mode":"explicit"}}
        });
        let tool_output_without_marker = json!({
            "type":"function_call_output",
            "call_id":"call-a",
            "output":{}
        });
        assert!(!responses_input_item_equal_ignoring_protocol_breakpoint(
            &tool_output,
            &tool_output_without_marker
        ));

        let mut invalid_marker = unmarked;
        invalid_marker["content"][0]["prompt_cache_breakpoint"] = json!({"mode":"other"});
        assert!(!responses_input_item_equal_ignoring_protocol_breakpoint(
            &invalid_marker,
            &marked
        ));
    }

    #[test]
    fn verified_third_party_options_require_measured_retention_effect() {
        let mut config = AppConfig::default();
        for field in [
            ProviderCacheCapabilityField::PromptCacheRetention,
            ProviderCacheCapabilityField::PromptCacheOptions,
        ] {
            config.record_cache_capability_probe(
                "provider-a",
                "gpt-5.6-terra",
                Channel::Responses,
                field,
                ProviderCacheCapabilityStatus::Verified,
                None,
            );
        }
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_NONE),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            crate::config::ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let plan = plan_with_effect_scope_and_probe_fields(
            &config,
            &provider("https://third.example/v1"),
            "gpt-5.6-terra",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_NONE),
            &[],
        );
        let mut body = json!({
            "prompt_cache_key": "stable",
            "prompt_cache_retention": "24h",
            "input": []
        });

        let _ = apply(&mut body, &Channel::Responses, plan);

        assert!(body.get("prompt_cache_retention").is_none());
        assert_eq!(body["prompt_cache_options"]["ttl"], "30m");
    }

    #[test]
    fn unsupported_cache_key_does_not_disable_verified_options() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Unsupported,
            Some("field rejected".to_string()),
        );
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheOptions,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_scope(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_NONE),
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            crate::config::ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let plan = plan_with_effect_scope_and_probe_fields(
            &config,
            &provider("https://third.example/v1"),
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            Some(EFFECT_SCOPE_NONE),
            &[],
        );
        let mut body = json!({"prompt_cache_key":"stable","input":[]});
        let changed = apply(&mut body, &Channel::Responses, plan);
        assert_eq!(changed, vec!["prompt_cache_key", "prompt_cache_options"]);
        assert!(body.get("prompt_cache_key").is_none());
        assert_eq!(body["prompt_cache_options"]["mode"], "implicit");
    }

    #[test]
    fn prompt_cache_key_is_probe_only_even_after_positive_effect_evidence() {
        let mut config = AppConfig::default();
        config.record_cache_capability_probe(
            "provider-a",
            "gpt-5.6-luna",
            Channel::Responses,
            ProviderCacheCapabilityField::PromptCacheKey,
            ProviderCacheCapabilityStatus::Verified,
            None,
        );
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            &[ProviderCacheCapabilityField::PromptCacheKey],
            crate::config::ProviderCacheEffectStatus::Promoted,
            Some("controlled key probe had a positive result".to_string()),
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let mut normal = json!({"prompt_cache_key":"caller-key","input":[]});
        apply(
            &mut normal,
            &Channel::Responses,
            plan(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-luna",
                &Channel::Responses,
                None,
            ),
        );
        assert!(normal.get("prompt_cache_key").is_none());

        let mut candidate = json!({"prompt_cache_key":"probe-key","input":[]});
        apply(
            &mut candidate,
            &Channel::Responses,
            plan_with_probe_fields(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-luna",
                &Channel::Responses,
                None,
                &[ProviderCacheCapabilityField::PromptCacheKey],
            ),
        );
        assert_eq!(candidate["prompt_cache_key"], "probe-key");
        assert!(!config.cache_capability_verified_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            ProviderCacheCapabilityField::PromptCacheKey,
        ));
    }

    #[test]
    fn control_cleanup_preserves_same_named_tool_output_data() {
        let config = AppConfig::default();
        let mut body = json!({
            "prompt_cache_key":"caller-key",
            "input":[
                {"type":"message","role":"user","content":[
                    {"type":"input_text","text":"stable","prompt_cache_breakpoint":{"mode":"explicit"}}
                ]},
                {"type":"function_call_output","call_id":"call-a","output":{
                    "prompt_cache_key":"must-survive",
                    "prompt_cache_options":{"source":"tool"},
                    "prompt_cache_breakpoint":{"source":"tool"}
                }}
            ]
        });

        apply(
            &mut body,
            &Channel::Responses,
            plan(
                &config,
                &provider("https://third.example/v1"),
                "gpt-5.6-luna",
                &Channel::Responses,
                None,
            ),
        );

        assert!(body.get("prompt_cache_key").is_none());
        assert!(body["input"][0]["content"][0]
            .get("prompt_cache_breakpoint")
            .is_none());
        assert_eq!(
            body["input"][1]["output"]["prompt_cache_key"],
            "must-survive"
        );
        assert_eq!(
            body["input"][1]["output"]["prompt_cache_options"]["source"],
            "tool"
        );
        assert_eq!(
            body["input"][1]["output"]["prompt_cache_breakpoint"]["source"],
            "tool"
        );
    }

    #[test]
    fn official_modern_model_waits_for_measured_effect_evidence() {
        let config = AppConfig::default();
        let mut official_body = json!({
            "prompt_cache_key":"stable",
            "prompt_cache_retention":"24h",
            "input":[{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        let _ = apply(
            &mut official_body,
            &Channel::Responses,
            plan(
                &config,
                &provider("https://api.openai.com/v1"),
                "gpt-5.6",
                &Channel::Responses,
                None,
            ),
        );
        assert!(official_body.get("prompt_cache_options").is_none());
        assert!(official_body.get("prompt_cache_retention").is_none());

        let third_party_plan = plan(
            &config,
            &provider("https://third.example/v1"),
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
        );
        assert!(!third_party_plan.enable_modern_options);
        assert!(!third_party_plan.enable_explicit_breakpoint);
    }

    #[test]
    fn effect_probe_keeps_a_large_stable_prefix_and_isolates_candidate_controls() {
        let baseline = effect_probe_body(
            &Channel::Responses,
            "gpt-5.6-luna",
            "baseline-group",
            "read",
            false,
            false,
            false,
        );
        let candidate = effect_probe_body(
            &Channel::Responses,
            "gpt-5.6-luna",
            "candidate-group",
            "read",
            true,
            true,
            true,
        );

        assert!(baseline["input"][0]["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.len() > 4_096));
        assert!(baseline.get("prompt_cache_key").is_none());
        assert!(baseline.get("prompt_cache_options").is_none());
        assert!(!contains_cache_breakpoint(&baseline));
        assert!(candidate.get("prompt_cache_key").is_some());
        assert_eq!(candidate["prompt_cache_options"]["ttl"], "30m");
        assert!(contains_cache_breakpoint(&candidate));
    }
}
