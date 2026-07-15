use crate::config::{
    AppConfig, Channel, ProviderCacheCapabilityField, ProviderCacheCapabilityStatus, ProviderConfig,
};
use serde_json::{json, Value};

pub(super) const PROVIDER_CACHE_METADATA_FIELDS: [&str; 4] = [
    "prompt_cache_key",
    "prompt_cache_retention",
    "prompt_cache_options",
    "prompt_cache_breakpoint",
];

pub(super) const EFFECT_FIELDS: [ProviderCacheCapabilityField; 2] = [
    ProviderCacheCapabilityField::PromptCacheOptions,
    ProviderCacheCapabilityField::PromptCacheBreakpoint,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NativeCachePlan {
    preserve_prompt_cache_key: bool,
    preserve_legacy_retention: bool,
    enable_modern_options: bool,
    enable_explicit_breakpoint: bool,
}

pub(super) fn plan(
    config: &AppConfig,
    provider: &ProviderConfig,
    model: &str,
    channel: &Channel,
    key_id: Option<&str>,
) -> NativeCachePlan {
    let status =
        |field| config.cache_capability_status_for_key(&provider.id, model, channel, key_id, field);
    let verified = |field| {
        config.cache_capability_verified_for_key(&provider.id, model, channel, key_id, field)
    };
    let official_modern = is_official_openai(provider)
        && model_uses_modern_prompt_cache_controls(model)
        && status(ProviderCacheCapabilityField::PromptCacheOptions)
            != ProviderCacheCapabilityStatus::Unsupported;
    let enable_modern_options =
        official_modern || verified(ProviderCacheCapabilityField::PromptCacheOptions);
    let enable_explicit_breakpoint = (is_official_openai(provider)
        && model_uses_modern_prompt_cache_controls(model)
        && status(ProviderCacheCapabilityField::PromptCacheBreakpoint)
            != ProviderCacheCapabilityStatus::Unsupported)
        || verified(ProviderCacheCapabilityField::PromptCacheBreakpoint);

    NativeCachePlan {
        preserve_prompt_cache_key: status(ProviderCacheCapabilityField::PromptCacheKey)
            != ProviderCacheCapabilityStatus::Unsupported,
        preserve_legacy_retention: provider.prompt_cache_retention_enabled
            && !enable_modern_options
            && status(ProviderCacheCapabilityField::PromptCacheRetention)
                != ProviderCacheCapabilityStatus::Unsupported,
        enable_modern_options,
        enable_explicit_breakpoint,
    }
}

pub(super) fn apply(request: &mut Value, channel: &Channel, plan: NativeCachePlan) {
    if let Some(object) = request.as_object_mut() {
        if !plan.preserve_prompt_cache_key {
            object.remove("prompt_cache_key");
        }
        if !plan.preserve_legacy_retention {
            object.remove("prompt_cache_retention");
        }
        if plan.enable_modern_options {
            object
                .entry("prompt_cache_options".to_string())
                .or_insert_with(|| json!({"mode": "implicit", "ttl": "30m"}));
        } else {
            object.remove("prompt_cache_options");
        }
    }

    if plan.enable_explicit_breakpoint {
        add_safe_explicit_breakpoint(request, channel);
    } else {
        remove_cache_breakpoints(request);
    }
}

pub(super) fn strip_all(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for field in PROVIDER_CACHE_METADATA_FIELDS {
                map.remove(field);
            }
            for child in map.values_mut() {
                strip_all(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_all(item);
            }
        }
        _ => {}
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

fn effect_probe_stable_prefix(group_nonce: &str) -> String {
    let seed = format!(
        "Atoapi cache effect verification {group_nonce}. The following stable prefix is intentionally repeated for cache measurement. "
    );
    seed.repeat(48)
}

pub(super) fn is_official_openai(provider: &ProviderConfig) -> bool {
    reqwest::Url::parse(provider.base_url.trim())
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == "api.openai.com")
}

fn model_uses_modern_prompt_cache_controls(model: &str) -> bool {
    let Some(version) = model
        .trim()
        .to_ascii_lowercase()
        .strip_prefix("gpt-")
        .map(str::to_string)
    else {
        return false;
    };
    let mut parts = version.split(|ch: char| !ch.is_ascii_digit() && ch != '.');
    let Some(version) = parts.next() else {
        return false;
    };
    let mut numbers = version.split('.');
    let major = numbers.next().and_then(|value| value.parse::<u32>().ok());
    let minor = numbers.next().and_then(|value| value.parse::<u32>().ok());
    matches!((major, minor), (Some(major), Some(minor)) if major > 5 || (major == 5 && minor >= 6))
}

fn add_safe_explicit_breakpoint(request: &mut Value, channel: &Channel) -> bool {
    if contains_cache_breakpoint(request) {
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
    let mut seen = 0usize;
    mark_penultimate_supported_block(root, channel, &mut seen)
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
    mark_first_supported_block(root, channel)
}

fn mark_penultimate_supported_block(
    value: &mut Value,
    channel: &Channel,
    seen: &mut usize,
) -> bool {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut().rev() {
                if mark_penultimate_supported_block(item, channel, seen) {
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
                *seen += 1;
                if *seen >= 2 {
                    map.insert(
                        "prompt_cache_breakpoint".to_string(),
                        json!({"mode": "explicit"}),
                    );
                    return true;
                }
            }
            for key in ["content", "input"] {
                if let Some(child) = map.get_mut(key) {
                    if mark_penultimate_supported_block(child, channel, seen) {
                        return true;
                    }
                }
            }
        }
        _ => {}
    }
    false
}

fn mark_first_supported_block(value: &mut Value, channel: &Channel) -> bool {
    match value {
        Value::Array(items) => {
            for item in items {
                if mark_first_supported_block(item, channel) {
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
            for key in ["content", "input"] {
                if let Some(child) = map.get_mut(key) {
                    if mark_first_supported_block(child, channel) {
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

fn remove_cache_breakpoints(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("prompt_cache_breakpoint");
            for child in map.values_mut() {
                remove_cache_breakpoints(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                remove_cache_breakpoints(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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
    fn third_party_unverified_plan_preserves_legacy_fields_only() {
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
        apply(&mut body, &Channel::Responses, plan);
        assert_eq!(body["prompt_cache_key"], "stable");
        assert_eq!(body["prompt_cache_retention"], "24h");
        assert!(body.get("prompt_cache_options").is_none());
        assert!(!contains_cache_breakpoint(&body));
    }

    #[test]
    fn verified_modern_controls_replace_legacy_retention_and_add_breakpoint() {
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
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            &EFFECT_FIELDS,
            crate::config::ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
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
            "input": [{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        apply(&mut body, &Channel::Responses, plan);
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
        config.record_cache_capability_effect_for_key(
            "provider-a",
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
            &[ProviderCacheCapabilityField::PromptCacheOptions],
            crate::config::ProviderCacheEffectStatus::Promoted,
            None,
            Some(0),
            Some(512),
            Some(100),
            Some(100),
        );
        let plan = plan(
            &config,
            &provider("https://third.example/v1"),
            "gpt-5.6-luna",
            &Channel::Responses,
            None,
        );
        let mut body = json!({"prompt_cache_key":"stable","input":[]});
        apply(&mut body, &Channel::Responses, plan);
        assert!(body.get("prompt_cache_key").is_none());
        assert_eq!(body["prompt_cache_options"]["mode"], "implicit");
    }

    #[test]
    fn official_modern_model_uses_documented_controls_without_alias_inference() {
        let config = AppConfig::default();
        let mut official_body = json!({
            "prompt_cache_key":"stable",
            "prompt_cache_retention":"24h",
            "input":[{"type":"message","role":"user","content":[
                {"type":"input_text","text":"stable"},
                {"type":"input_text","text":"dynamic"}
            ]}]
        });
        apply(
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
        assert!(official_body.get("prompt_cache_options").is_some());
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
