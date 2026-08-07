//! Isolated provider health-probe transport.
//!
//! Health checks are management requests, not relay requests.  This module
//! deliberately owns their client, headers, endpoint construction and body
//! shape so normal cache/session/serialization code cannot add fields or
//! create a second request behind the probe.

use anyhow::Result;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, Proxy,
};
use serde_json::{json, Value};
use std::time::Duration;

use super::ProviderHealthProbeMode;
use crate::config::Channel;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
// Some third-party Responses compatibility layers treat an empty instruction
// field as an invitation to install a large Codex default prompt.  A fixed,
// visible, ordinary assistant instruction keeps the management probe on the
// same lightweight Responses path as standard OpenAI-compatible key tests.
const RESPONSES_HEALTH_PROBE_INSTRUCTIONS: &str = "You are ChatGPT, a helpful assistant.";

pub(super) fn client(use_system_proxy: bool, explicit_proxy_url: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true)
        // A health probe is exactly one management request.  Redirects and
        // reqwest's low-level retries must not turn it into hidden requests.
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never());

    if let Some(proxy_url) = explicit_proxy_url {
        builder = builder.no_proxy().proxy(Proxy::all(proxy_url)?);
    } else if !use_system_proxy {
        builder = builder.no_proxy();
    }

    Ok(builder.build()?)
}

pub(super) fn headers(
    api_key: &str,
    channel: &Channel,
    stream: bool,
    custom_user_agent: Option<&str>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("Bearer {api_key}")) {
        headers.insert(header::AUTHORIZATION, value);
    }
    if let Ok(value) = HeaderValue::from_str(api_key) {
        headers.insert("x-api-key", value);
    }
    let user_agent = custom_user_agent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or_else(|| HeaderValue::from_static(crate::ATOAPI_USER_AGENT));
    headers.insert(header::USER_AGENT, user_agent);
    headers.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    if stream {
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );
    } else {
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
    }
    if matches!(channel, Channel::Anthropic) {
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    }
    headers
}

pub(super) fn endpoint_url(base_url: &str, channel: &Channel) -> Result<String> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        anyhow::bail!("base URL is empty");
    }
    reqwest::Url::parse(trimmed)?;
    let endpoint = channel.endpoint_path();
    if trimmed.ends_with(endpoint)
        || trimmed.ends_with("/chat/completions")
        || trimmed.ends_with("/responses")
        || trimmed.ends_with("/messages")
    {
        Ok(trimmed.to_string())
    } else if trimmed.ends_with("/v1") {
        Ok(format!("{trimmed}{endpoint}"))
    } else {
        Ok(format!("{trimmed}/v1{endpoint}"))
    }
}

pub(super) fn request_body(mode: &ProviderHealthProbeMode, model: &str, prompt: &str) -> Value {
    match mode {
        ProviderHealthProbeMode::MinimalCost => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::ResponsesStreaming => json!({
            "model": model,
            "input": [{
                "role": "user",
                "content": [{ "type": "input_text", "text": prompt }]
            }],
            "instructions": RESPONSES_HEALTH_PROBE_INSTRUCTIONS,
            "stream": true,
            "store": false,
            "max_output_tokens": 1,
        }),
        ProviderHealthProbeMode::ResponsesJson => json!({
            "model": model,
            "input": [{
                "role": "user",
                "content": [{ "type": "input_text", "text": prompt }]
            }],
            "instructions": RESPONSES_HEALTH_PROBE_INSTRUCTIONS,
            "stream": false,
            "store": false,
            "max_output_tokens": 1,
        }),
        ProviderHealthProbeMode::ChatStreaming => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::ChatJson => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": false,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::AnthropicStreaming => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true,
            "max_tokens": 1,
        }),
        ProviderHealthProbeMode::AnthropicJson => json!({
            "model": model,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": false,
            "max_tokens": 1,
        }),
    }
}
