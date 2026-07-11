use anyhow::{anyhow, Context, Result};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, State as AxumState},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use chrono::{Duration, Utc};
use flate2::{write::GzEncoder, Compression};
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    io::Write,
    pin::Pin,
    sync::Arc,
    time::Instant,
};
use tokio::time::{sleep, Duration as TokioDuration};
use uuid::Uuid;

use crate::{
    agent_injection,
    cache::{self, CacheEntry, CacheLookupStatus},
    config::{
        codex_model_alias, codex_model_display_name, model_request_alias,
        normalize_reasoning_effort, provider_model_cache_key, AgentInjectionConfig,
        AgentInjectionKind, AppConfig, CacheMode, Channel, ProviderChannelMode, ProviderConfig,
        SelectedProviderKey,
    },
    metrics::{RequestLog, UsageRecord},
    state::{AppState, PrefixWarmState, ResponseSessionCooldownState, ResponseSessionState},
};

mod codex_chat_common;
mod json_canonical;
mod prefix_guard;
mod sse;
mod streaming_codex_chat;
mod transform_codex_chat;

use prefix_guard::{decide_responses_guard, ResponsesGuardInput};

#[cfg(test)]
const BACKGROUND_PREWARM_MAX_EXTRA_BUCKET_REQUESTS: u64 = 1;
#[cfg(test)]
const BACKGROUND_PREWARM_MIN_NET_SAVE_TOKENS: u64 = 512;
const PREFIX_ERROR_COOLDOWN_SECS: u64 = 45;
const RESPONSE_SESSION_ERROR_COOLDOWN_FIRST_SECS: u64 = 30;
const RESPONSE_SESSION_ERROR_COOLDOWN_SECOND_SECS: u64 = 2 * 60;
const RESPONSE_SESSION_ERROR_COOLDOWN_LONG_SECS: u64 = 5 * 60;
const RESPONSE_SESSION_UNSUPPORTED_COOLDOWN_SECS: u64 = 5 * 60;
#[cfg(test)]
const PREFIX_BACKGROUND_PREWARM_COOLDOWN_SECS: u64 = 60 * 60;
const REQUEST_BODY_GZIP_FALLBACK_COOLDOWN_SECS: u64 = 6 * 60 * 60;
const REQUEST_BODY_GZIP_MIN_BYTES: usize = 614_400;
const REQUEST_BODY_GZIP_WARM_MIN_BYTES: usize = 262_144;
const COMPACT_CHAT_COMPAT_COOLDOWN_SECS: u64 = 15 * 60;
const PROXY_TOKEN_PLACEHOLDER: &str = "PROXY_MANAGED";

#[derive(Debug, Clone)]
struct RouteDecision {
    provider: ProviderConfig,
    upstream_channel: Channel,
    model: String,
}

#[derive(Debug, Clone, Default)]
struct BodyDiagnostics {
    original_body_bytes: u64,
    send_body_bytes: u64,
    send_body_is_delta: bool,
    payload_too_large_rescue_attempted: bool,
    payload_too_large_rescue_used: bool,
    reasoning: ReasoningEffortDiagnostics,
}

#[derive(Debug, Clone, Default)]
struct ReasoningEffortDiagnostics {
    agent: Option<String>,
    configured: Option<String>,
    effective: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct UpstreamRequestDiagnostics {
    network_path: &'static str,
    request_body_bytes: u64,
    request_body_encode_ms: u64,
    attempts: u64,
    retry_wait_ms: u64,
    headers_ms: u64,
    last_attempt_headers_ms: u64,
    http_version: Option<String>,
    remote_addr: Option<String>,
    pool_diagnostic: Option<String>,
    upstream_trace_id: Option<String>,
    upstream_trace_source: Option<String>,
    server_timing: Option<String>,
    timing_source: Option<String>,
    reported_processing_ms: Option<u64>,
    non_processing_ms: Option<u64>,
    gzip_encode_ms: u64,
    gzip_attempted: bool,
    gzip_fallback_used: bool,
    gzip_skipped_cold_stream: bool,
    sent_body_bytes: u64,
}

impl UpstreamRequestDiagnostics {
    fn absorb(&mut self, next: &UpstreamRequestDiagnostics) {
        if !next.network_path.is_empty() {
            self.network_path = next.network_path;
        }
        self.request_body_bytes = next.request_body_bytes;
        self.request_body_encode_ms += next.request_body_encode_ms;
        self.attempts += next.attempts;
        self.retry_wait_ms += next.retry_wait_ms;
        self.headers_ms += next.headers_ms;
        if next.last_attempt_headers_ms > 0 || next.http_version.is_some() {
            self.last_attempt_headers_ms = next.last_attempt_headers_ms;
            self.http_version = next.http_version.clone();
            self.remote_addr = next.remote_addr.clone();
            self.pool_diagnostic = next.pool_diagnostic.clone();
            self.upstream_trace_id = next.upstream_trace_id.clone();
            self.upstream_trace_source = next.upstream_trace_source.clone();
            self.server_timing = next.server_timing.clone();
            self.timing_source = next.timing_source.clone();
            self.reported_processing_ms = next.reported_processing_ms;
            self.non_processing_ms = next.non_processing_ms;
        }
        self.gzip_attempted |= next.gzip_attempted;
        self.gzip_encode_ms += next.gzip_encode_ms;
        self.gzip_fallback_used |= next.gzip_fallback_used;
        self.gzip_skipped_cold_stream |= next.gzip_skipped_cold_stream;
        self.sent_body_bytes = next.sent_body_bytes;
    }
}

struct UpstreamSendOutcome {
    response: reqwest::Response,
    diagnostics: UpstreamRequestDiagnostics,
}

struct UpstreamBodyReadOutcome {
    bytes: Vec<u8>,
    first_chunk_ms: Option<u64>,
    aggregate_done_ms: u64,
    sse_chunks: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct SseStreamMetadataCollector {
    pending_line: String,
    usage: UsageRecord,
    response_id: Option<String>,
    completed_event_seen: bool,
    done_marker_seen: bool,
    error_event_seen: bool,
}

impl SseStreamMetadataCollector {
    fn process_chunk(&mut self, chunk: &[u8]) {
        let text = String::from_utf8_lossy(chunk);
        self.pending_line.push_str(&text);
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
    }

    fn finish(&mut self) {
        if !self.pending_line.is_empty() {
            let line = std::mem::take(&mut self.pending_line);
            self.process_line(line.trim_end_matches('\r'));
        }
    }

    fn process_line(&mut self, line: &str) {
        let Some(payload) = line.trim_start().strip_prefix("data:") else {
            if line.contains("message_stop") {
                self.completed_event_seen = true;
            }
            return;
        };
        let payload = payload.trim();
        if payload.is_empty() {
            return;
        }
        if payload == "[DONE]" {
            self.done_marker_seen = true;
            return;
        }
        if payload.contains("response.completed") || payload.contains("message_stop") {
            self.completed_event_seen = true;
        }
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            if value.get("error").is_some()
                || value
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| {
                        matches!(
                            kind,
                            "error"
                                | "response.failed"
                                | "response.incomplete"
                                | "message_delta_error"
                        ) || kind.ends_with(".failed")
                            || kind.ends_with(".incomplete")
                            || kind.ends_with(".error")
                    })
            {
                self.error_event_seen = true;
            }
            self.usage.merge(provider_usage_from_value(&value));
            if let Some(id) = response_id_from_value(&value) {
                self.response_id = Some(id);
            }
        }
    }
}

fn upstream_body_has_error(bytes: &[u8], content_type: &str) -> bool {
    let text_like = content_type.contains("json")
        || content_type.contains("event-stream")
        || content_type.contains("text");
    if !text_like {
        return false;
    }
    let text = String::from_utf8_lossy(bytes);
    if text.contains("\"type\":\"error\"")
        || text.contains("\"type\":\"response.failed\"")
        || text.contains("\"type\":\"response.incomplete\"")
        || text.contains("\"error\":")
        || text.contains("event: error")
    {
        return true;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return json_value_has_error(&value);
    }
    false
}

fn json_value_has_error(value: &Value) -> bool {
    if value.get("error").is_some() {
        return true;
    }
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| {
            matches!(kind, "error" | "response.failed" | "response.incomplete")
                || kind.ends_with(".failed")
                || kind.ends_with(".incomplete")
                || kind.ends_with(".error")
        })
}

#[derive(Debug, Clone, Default)]
struct TailInputDiagnostics {
    input_items: u64,
    delta_from_session: bool,
    message_chars: u64,
    tool_call_chars: u64,
    tool_output_chars: u64,
    largest_tool_output_chars: u64,
    tool_output_lines: u64,
    tool_output_repeated_line_chars: u64,
    tool_output_timestamp_like_count: u64,
    tool_output_path_like_count: u64,
    tool_output_url_like_count: u64,
    tool_output_hash_like_count: u64,
    tool_output_json_like_chars: u64,
    tool_output_noise_hint: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ToolOutputNoiseDiagnostics {
    lines: u64,
    repeated_line_chars: u64,
    timestamp_like_count: u64,
    path_like_count: u64,
    url_like_count: u64,
    hash_like_count: u64,
    json_like_chars: u64,
    hint: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct PrefixGuardWaitDiagnostics {
    wait_ms: u64,
    reason: Option<String>,
    source: Option<String>,
    state_age_ms: Option<u64>,
    skip_reason: Option<String>,
    budget_exhausted: bool,
    cache_instability_score: Option<u64>,
    seen_bucket_tokens: Option<u64>,
    state_cache_read_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct PrefixLagDiagnostics {
    classification: Option<String>,
    input_delta_tokens: Option<u64>,
    cache_delta_tokens: Option<u64>,
    previous_gap_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct SessionAnchorDiagnostics {
    hash: Option<String>,
    source: Option<String>,
    changed: Option<bool>,
    peer_count: Option<u64>,
}

#[derive(Debug, Clone)]
struct ResponseSessionReuseOutcome {
    body: Value,
    diagnostics: ResponseSessionReuseDiagnostics,
}

#[derive(Debug, Clone)]
struct PreviousResponseCompatBody {
    body: Value,
    reason: &'static str,
}

#[derive(Debug, Clone, Default)]
struct ResponseSessionReuseDiagnostics {
    candidate_count: u64,
    skip_reason: Option<String>,
    exact_key_hit: bool,
    scope_match_count: u64,
    append_delta_match: bool,
    delta_items: u64,
    cooldown_active: bool,
    rejected_status: Option<u16>,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/admin/metrics", get(admin_metrics))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .route("/v1/responses/compact", post(responses_compact_legacy))
        .route("/codex/v1/models", get(codex_list_models))
        .route("/codex/v1/chat/completions", post(codex_chat_completions))
        .route("/codex/v1/responses", post(codex_responses))
        .route(
            "/codex/v1/responses/compact",
            post(codex_responses_compact_legacy),
        )
        .route(
            "/v1/responses/:response_id/compact",
            post(responses_compact),
        )
        .route(
            "/codex/v1/responses/:response_id/compact",
            post(codex_responses_compact),
        )
        .route("/v1/messages", post(messages))
        .layer(DefaultBodyLimit::max(200 * 1024 * 1024))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "service": "atoapi",
        "time": Utc::now()
    }))
}

async fn admin_metrics(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.metrics.snapshot().await)
}

async fn list_models(AxumState(state): AxumState<Arc<AppState>>, headers: HeaderMap) -> Response {
    list_models_for_agent(state, headers, None).await
}

async fn codex_list_models(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    list_models_for_agent(state, headers, Some("codex")).await
}

async fn list_models_for_agent(
    state: Arc<AppState>,
    headers: HeaderMap,
    forced_agent_id: Option<&'static str>,
) -> Response {
    let authorized_agent = match authorize_for_agent(&state, &headers, forced_agent_id).await {
        Ok(agent_id) => agent_id,
        Err(response) => return response,
    };
    let config = state.config.read().await;
    let agent_provider_id = authorized_agent
        .as_deref()
        .and_then(|agent_id| {
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == agent_id)
        })
        .and_then(|agent| agent.provider_id.as_deref());
    let data = config
        .providers
        .iter()
        .filter(|provider| {
            provider.enabled
                && agent_provider_id
                    .map(|provider_id| provider.id == provider_id)
                    .unwrap_or(true)
        })
        .flat_map(|provider| provider_model_list_items(provider, authorized_agent.is_some()))
        .collect::<Vec<_>>();
    Json(json!({ "object": "list", "data": data })).into_response()
}

fn provider_model_list_items(provider: &ProviderConfig, include_codex_aliases: bool) -> Vec<Value> {
    let mut items = Vec::new();
    let mut seen_ids = HashSet::new();
    for model in provider.models.iter().filter(|model| model.enabled) {
        seen_ids.insert(model.id.clone());
        items.push(json!({
            "id": model.id,
            "object": "model",
            "owned_by": provider.name,
            "provider": provider.id,
            "context_window": model.context_window,
            "display_name": model.display_name
        }));
        if include_codex_aliases {
            let mut aliases = Vec::new();
            if let Some(alias) = model_request_alias(model) {
                aliases.push(alias);
            }
            if let Some(alias) = codex_model_alias(&model.id) {
                aliases.push(alias);
            }
            for alias in aliases {
                if !seen_ids.insert(alias.clone()) {
                    continue;
                }
                items.push(json!({
                    "id": alias,
                    "object": "model",
                    "owned_by": provider.name,
                    "provider": provider.id,
                    "context_window": model.context_window,
                    "display_name": codex_model_display_name(&alias),
                    "canonical_id": model.id
                }));
            }
        }
    }
    items
}

async fn chat_completions(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_generation(state, headers, body, Channel::Chat).await
}

async fn responses(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_generation(state, headers, body, Channel::Responses).await
}

async fn responses_compact_legacy(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_responses_compact(state, headers, body, None).await
}

async fn responses_compact(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(response_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_responses_compact(state, headers, body, Some(response_id)).await
}

async fn codex_chat_completions(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_generation_for_agent(state, headers, body, Channel::Chat, Some("codex")).await
}

async fn codex_responses(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_generation_for_agent(state, headers, body, Channel::Responses, Some("codex")).await
}

async fn codex_responses_compact_legacy(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_responses_compact_for_agent(state, headers, body, None, Some("codex")).await
}

async fn codex_responses_compact(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(response_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_responses_compact_for_agent(state, headers, body, Some(response_id), Some("codex")).await
}

async fn messages(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_generation(state, headers, body, Channel::Anthropic).await
}

async fn handle_responses_compact(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    response_id: Option<String>,
) -> Response {
    handle_responses_compact_for_agent(state, headers, body, response_id, None).await
}

async fn handle_responses_compact_for_agent(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    response_id: Option<String>,
    forced_agent_id: Option<&'static str>,
) -> Response {
    let started = Instant::now();
    let request_id = Uuid::new_v4().to_string();

    let authorized_agent = match authorize_for_agent(&state, &headers, forced_agent_id).await {
        Ok(agent_id) => agent_id,
        Err(response) => return response,
    };

    let mut client_request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {err}")),
    };
    if let Some(response_id) = response_id.as_deref() {
        compact_request_set_response_id(&mut client_request, response_id);
    }
    set_stream_flag(&mut client_request, false);

    let config = state.config.read().await.clone();
    let decision = match decide_route(
        &config,
        &client_request,
        &Channel::Responses,
        authorized_agent.as_deref(),
    ) {
        Ok(decision) => decision,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, &err),
    };
    let requested_model_for_log = requested_model_for_log(&client_request, &decision.model);
    let mut selected_provider_key =
        match select_provider_api_key(&state, &decision.provider.id, None, None).await {
            Ok(selected) => selected,
            Err(err) if err.to_string().contains("not configured") => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "provider API key is not configured",
                )
            }
            Err(err) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("failed to select provider key: {err}"),
                )
            }
        };
    let mut api_key = selected_provider_key.secret.clone();
    if api_key.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "provider API key is not configured",
        );
    }

    let mut upstream_body = transform_request_for_channel(
        &client_request,
        &Channel::Responses,
        &decision.upstream_channel,
    );
    if matches!(decision.upstream_channel, Channel::Responses) {
        normalize_responses_request(&mut upstream_body);
    }
    set_request_model(&mut upstream_body, &decision.model);
    set_stream_flag(&mut upstream_body, false);
    let reasoning_diagnostics = apply_model_reasoning_effort(
        &client_request,
        &mut upstream_body,
        &decision.upstream_channel,
        &decision,
    );
    optimize_provider_prefix(&mut upstream_body, &config, &decision);
    let tail_input_diagnostics = tail_input_diagnostics_for_session(
        &state,
        &decision.upstream_channel,
        None,
        None,
        upstream_body.get("input"),
    )
    .await;
    let compact_chat_compat_cooldown_key = compact_chat_compat_cooldown_key(&decision);
    let compact_chat_compat_cooldown_active =
        compact_chat_compat_cooldown_active(&state, &compact_chat_compat_cooldown_key).await;
    let compact_chat_compat = !compact_chat_compat_cooldown_active
        && should_route_responses_compact_via_chat_compat(
            &decision.upstream_channel,
            &tail_input_diagnostics,
            serialized_body_len(&decision.upstream_channel, &upstream_body),
        );

    let compact_url = response_id
        .as_deref()
        .map(|id| responses_compact_url(&decision.provider.base_url, id));
    let mut active_request_channel = if compact_url.is_none() && compact_chat_compat {
        Channel::Chat
    } else {
        decision.upstream_channel.clone()
    };
    let mut active_url = compact_url
        .clone()
        .unwrap_or_else(|| upstream_url(&decision.provider.base_url, &active_request_channel));
    let mut active_upstream_body = if compact_url.is_none() && compact_chat_compat {
        build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &active_request_channel,
            false,
        )
    } else {
        compact_request_body_for_official_endpoint(&upstream_body)
    };
    let mut compact_chat_fast_json = compact_chat_compat
        && matches!(active_request_channel, Channel::Chat)
        && should_use_chat_non_stream_compact_fast_path(
            &tail_input_diagnostics,
            serialized_body_len(&active_request_channel, &active_upstream_body),
        );
    if compact_chat_fast_json {
        set_stream_flag(&mut active_upstream_body, false);
    }
    let send_outcome = match send_upstream_request_to_url_with_diagnostics(
        &state,
        decision.provider.use_system_proxy,
        &active_url,
        &api_key,
        &active_request_channel,
        &active_upstream_body,
        &headers,
        decision.provider.custom_user_agent.as_deref(),
        None,
        decision.provider.request_body_gzip_enabled,
    )
    .await
    {
        Ok(response) => response,
        Err(err) => {
            state
                .metrics
                .record_error("upstream_transport", &err.to_string())
                .await;
            return json_error(
                StatusCode::BAD_GATEWAY,
                &format!("upstream compact request failed: {err}"),
            );
        }
    };
    let mut upstream_request_diagnostics = send_outcome.diagnostics;
    let mut upstream = send_outcome.response;
    let mut upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;

    let mut status = upstream.status().as_u16();
    let mut content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let mut used_fallback = false;

    if let Some(next_key) = try_retry_with_next_provider_key(
        &state,
        &decision.provider.id,
        &selected_provider_key,
        status,
        None,
    )
    .await
    {
        state.metrics.record_retry().await;
        selected_provider_key = next_key;
        api_key = selected_provider_key.secret.clone();
        let send_outcome = match send_upstream_request_to_url_with_diagnostics(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            None,
            decision.provider.request_body_gzip_enabled,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream compact request failed after key failover: {err}"),
                );
            }
        };
        upstream_request_diagnostics.absorb(&send_outcome.diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
    }

    if compact_url.is_some() && should_fallback_compact_to_responses(status) {
        let fallback_bytes = upstream.bytes().await.ok().map(|bytes| bytes.to_vec());
        if let Some(bytes) = fallback_bytes.as_deref() {
            let summary = upstream_error_summary(bytes);
            state
                .metrics
                .record_error("upstream_compact_unsupported", &summary)
                .await;
        }
        state.metrics.record_retry().await;
        let fallback_chat_compat = !compact_chat_compat_cooldown_active
            && (compact_chat_compat
                || should_fallback_official_responses_compact_via_chat_compat(
                    &decision.upstream_channel,
                    &tail_input_diagnostics,
                    serialized_body_len(&decision.upstream_channel, &upstream_body),
                ));
        active_request_channel = if fallback_chat_compat {
            Channel::Chat
        } else {
            decision.upstream_channel.clone()
        };
        active_url = upstream_url(&decision.provider.base_url, &active_request_channel);
        active_upstream_body = build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &active_request_channel,
            false,
        );
        compact_chat_fast_json = fallback_chat_compat
            && matches!(active_request_channel, Channel::Chat)
            && should_use_chat_non_stream_compact_fast_path(
                &tail_input_diagnostics,
                serialized_body_len(&active_request_channel, &active_upstream_body),
            );
        if compact_chat_fast_json {
            set_stream_flag(&mut active_upstream_body, false);
        }
        let send_outcome = match send_upstream_request_to_url_with_diagnostics(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            None,
            decision.provider.request_body_gzip_enabled,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream compact fallback failed: {err}"),
                );
            }
        };
        upstream_request_diagnostics.absorb(&send_outcome.diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        used_fallback = true;
    }

    if should_fallback_chat_compat_compact_to_responses(
        status,
        true,
        matches!(active_request_channel, Channel::Chat),
    ) {
        let chat_error_body = match read_upstream_body_with_diagnostics(
            upstream,
            &content_type,
            started,
            upstream_response_headers_at_ms,
        )
        .await
        {
            Ok(outcome) => outcome.bytes,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_chat_compat_body", &err.to_string())
                    .await;
                Vec::new()
            }
        };
        let error_summary = upstream_error_summary(&chat_error_body);
        state
            .metrics
            .record_error(upstream_error_scope(status, &error_summary), &error_summary)
            .await;
        note_compact_chat_compat_cooldown(&state, &compact_chat_compat_cooldown_key).await;
        state.metrics.record_retry().await;

        active_request_channel = decision.upstream_channel.clone();
        active_url = upstream_url(&decision.provider.base_url, &active_request_channel);
        active_upstream_body = build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &active_request_channel,
            false,
        );
        let send_outcome = match send_upstream_request_to_url_with_diagnostics(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            None,
            decision.provider.request_body_gzip_enabled,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream compact fallback failed: {err}"),
                );
            }
        };
        upstream_request_diagnostics.absorb(&send_outcome.diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        used_fallback = true;
    }

    let is_success_status = StatusCode::from_u16(status)
        .map(|status| status.is_success())
        .unwrap_or(false);
    let body_read = match read_upstream_body_with_diagnostics(
        upstream,
        &content_type,
        started,
        upstream_response_headers_at_ms,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            state
                .metrics
                .record_error("upstream_body", &err.to_string())
                .await;
            return json_error(
                StatusCode::BAD_GATEWAY,
                &format!("failed to read upstream compact body: {err}"),
            );
        }
    };
    let bytes = body_read.bytes;
    let upstream_body_error = upstream_body_has_error(&bytes, &content_type);
    let compact_success_for_cache = is_success_status && !upstream_body_error;
    let key_error_summary = (!compact_success_for_cache).then(|| upstream_error_summary(&bytes));
    note_selected_provider_key_status(
        &state,
        &decision.provider.id,
        &selected_provider_key,
        status,
        key_error_summary.as_deref(),
    )
    .await;

    let provider_prefix_key = openai_prompt_cache_key(&active_upstream_body);
    let provider_prefix_fingerprint = Some(provider_prefix_fingerprint(
        &active_upstream_body,
        &active_request_channel,
    ));
    let prefix_state_key_for_metrics: Option<&str> = None;
    let usage_record = if compact_success_for_cache {
        let usage = collect_provider_usage_for_diagnostics(&bytes, &decision);
        if let Some(record) = usage.as_ref() {
            state.metrics.record_usage(record.clone()).await;
        }
        usage
    } else {
        None
    };
    let gap_breakdown = provider_cache_gap_breakdown(
        &state,
        prefix_state_key_for_metrics,
        None,
        usage_record.as_ref(),
        Some(&tail_input_diagnostics),
    )
    .await;
    let prefix_lag = prefix_lag_diagnostics(
        &state,
        prefix_state_key_for_metrics,
        usage_record.as_ref(),
        gap_breakdown.as_ref(),
        &PrefixGuardWaitDiagnostics::default(),
        &tail_input_diagnostics,
    )
    .await;
    if !compact_success_for_cache {
        let error_summary = upstream_error_summary(&bytes);
        state
            .metrics
            .record_error(upstream_error_scope(status, &error_summary), &error_summary)
            .await;
    }

    let elapsed = started.elapsed().as_millis() as u64;
    let mut request_log = RequestLog {
        id: request_id.clone(),
        at: Utc::now(),
        inbound_request_id: Some(request_id.clone()),
        upstream_request_id: Some(Uuid::new_v4().to_string()),
        upstream_attempt_index: Some(1),
        upstream_attempt_total: Some(upstream_request_diagnostics.attempts),
        client_channel: "responses".to_string(),
        upstream_channel: active_request_channel.label().to_string(),
        provider: decision.provider.name.clone(),
        model: decision.model.clone(),
        requested_model: requested_model_for_log.clone(),
        agent_reasoning_effort: reasoning_diagnostics.agent.clone(),
        configured_reasoning_effort: reasoning_diagnostics.configured.clone(),
        effective_reasoning_effort: reasoning_diagnostics.effective.clone(),
        reasoning_effort_source: reasoning_diagnostics.source.clone(),
        cache_status: if compact_success_for_cache {
            "compact"
        } else {
            "error"
        }
        .to_string(),
        agent_id: authorized_agent.clone(),
        agent_label: authorized_agent
            .as_deref()
            .map(|agent_id| agent_id.to_string()),
        upstream_call_kind: Some("sync".to_string()),
        upstream_call_source: Some(
            if used_fallback && matches!(active_request_channel, Channel::Chat) {
                "compact-fallback-chat-compat"
            } else if compact_chat_compat {
                "compact-chat-compat"
            } else if used_fallback {
                "compact-fallback"
            } else {
                "compact"
            }
            .to_string(),
        ),
        cache_key: None,
        provider_prefix_key,
        provider_prefix_fingerprint,
        provider_cache_diagnostic: usage_record.as_ref().map(provider_cache_diagnostic),
        prefix_guard_wait_ms: None,
        prefix_guard_wait_reason: None,
        prefix_guard_wait_source: None,
        prefix_guard_state_age_ms: None,
        prefix_guard_skip_reason: None,
        prefix_guard_wait_effect: None,
        prefix_lag_classification: None,
        prefix_lag_input_delta_tokens: None,
        prefix_lag_cache_delta_tokens: None,
        prefix_lag_previous_gap_tokens: None,
        prefix_cache_instability_score: None,
        prefix_seen_bucket_tokens: None,
        prefix_state_cache_read_tokens: None,
        status,
        ttft_ms: elapsed,
        upstream_ttft_ms: Some(upstream_ttft_ms(elapsed, None)),
        local_prepare_ms: None,
        upstream_headers_ms: Some(upstream_request_diagnostics.headers_ms),
        upstream_last_attempt_headers_ms: Some(
            upstream_request_diagnostics.last_attempt_headers_ms,
        ),
        upstream_http_version: upstream_request_diagnostics.http_version.clone(),
        upstream_network_path: Some(upstream_request_diagnostics.network_path.to_string()),
        upstream_remote_addr: upstream_request_diagnostics.remote_addr.clone(),
        upstream_pool_diagnostic: upstream_request_diagnostics.pool_diagnostic.clone(),
        upstream_trace_id: upstream_request_diagnostics.upstream_trace_id.clone(),
        upstream_trace_source: upstream_request_diagnostics.upstream_trace_source.clone(),
        upstream_server_timing: upstream_request_diagnostics.server_timing.clone(),
        upstream_timing_source: upstream_request_diagnostics.timing_source.clone(),
        upstream_reported_processing_ms: upstream_request_diagnostics.reported_processing_ms,
        upstream_non_processing_ms: upstream_request_diagnostics.non_processing_ms,
        upstream_first_chunk_ms: body_read.first_chunk_ms,
        stream_upstream_wait_ms: None,
        stream_client_backpressure_ms: None,
        aggregate_done_ms: Some(body_read.aggregate_done_ms),
        upstream_retry_wait_ms: Some(upstream_request_diagnostics.retry_wait_ms),
        upstream_attempts: Some(upstream_request_diagnostics.attempts),
        request_body_bytes: Some(upstream_request_diagnostics.request_body_bytes),
        sent_body_bytes: Some(upstream_request_diagnostics.sent_body_bytes),
        request_body_encode_ms: Some(upstream_request_diagnostics.request_body_encode_ms),
        gzip_encode_ms: Some(upstream_request_diagnostics.gzip_encode_ms),
        gzip_attempted: Some(upstream_request_diagnostics.gzip_attempted),
        gzip_fallback_used: Some(upstream_request_diagnostics.gzip_fallback_used),
        upstream_header_wait_class: Some(upstream_header_wait_class(&upstream_request_diagnostics)),
        total_ms: elapsed,
        input_tokens: usage_record.as_ref().map(|record| record.input_tokens),
        output_tokens: usage_record.as_ref().map(|record| record.output_tokens),
        cache_read_tokens: usage_record.as_ref().map(|record| record.cache_read_tokens),
        cache_shortfall_tokens: usage_record.as_ref().map(provider_cache_shortfall),
        cache_new_tail_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.new_tail_tokens),
        cache_avoidable_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.avoidable_tokens),
        cache_provider_unstable_gap_tokens: gap_breakdown
            .as_ref()
            .map(|gap| gap.provider_unstable_tokens),
        provider_cache_token_ratio: usage_record.as_ref().and_then(provider_cache_ratio),
        tail_input_items: None,
        tail_message_chars: None,
        tail_tool_call_chars: None,
        tail_tool_output_chars: None,
        tail_largest_tool_output_chars: None,
        tail_tool_output_lines: None,
        tail_tool_output_repeated_line_chars: None,
        tail_tool_output_timestamp_like_count: None,
        tail_tool_output_path_like_count: None,
        tail_tool_output_url_like_count: None,
        tail_tool_output_hash_like_count: None,
        tail_tool_output_json_like_chars: None,
        tail_tool_output_noise_hint: None,
        tail_source: None,
        response_session_reused: Some(false),
        response_session_candidate_count: Some(0),
        response_session_skip_reason: Some("compact_non_streaming".to_string()),
        response_session_exact_key_hit: Some(false),
        response_session_scope_match_count: Some(0),
        response_session_append_delta_match: Some(false),
        response_session_delta_items: Some(0),
        response_session_cooldown_active: Some(false),
        response_session_rejected_status: None,
        session_anchor_hash: None,
        session_anchor_source: None,
        session_anchor_changed: None,
        session_anchor_peer_count: None,
        original_body_bytes: Some(serialized_body_len(
            &decision.upstream_channel,
            &upstream_body,
        )),
        send_body_bytes: Some(serialized_body_len(
            &active_request_channel,
            &active_upstream_body,
        )),
        send_body_is_delta: Some(false),
        payload_too_large_rescue_attempted: Some(false),
        payload_too_large_rescue_used: Some(false),
        sse_end_reason: None,
        sse_completed_event_seen: None,
        sse_done_marker_seen: None,
        sse_chunks: body_read.sse_chunks,
    };
    apply_prefix_lag_diagnostics(&mut request_log, prefix_lag);
    apply_tail_input_diagnostics(&mut request_log, &tail_input_diagnostics);
    state.metrics.record_upstream_observation(request_log).await;

    let bytes_for_client = if is_text_event_stream(&content_type) {
        match active_request_channel {
            Channel::Chat => chat_sse_to_non_stream_json(&bytes, &decision.model),
            _ => responses_sse_to_non_stream_json(
                &bytes,
                &decision.model,
                non_sse_compact_compat_for_decision(&config, &decision),
            ),
        }
    } else {
        bytes.clone()
    };
    let response_bytes = transform_response_bytes(
        &Channel::Responses,
        &active_request_channel,
        &decision.model,
        &bytes_for_client,
    );
    let response_bytes = maybe_normalize_responses_json_for_client(
        &response_bytes,
        non_sse_compact_compat_for_decision(&config, &decision),
    );
    raw_response(status, "application/json", response_bytes)
}

async fn handle_generation(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    client_channel: Channel,
) -> Response {
    handle_generation_for_agent(state, headers, body, client_channel, None).await
}

async fn handle_generation_for_agent(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    client_channel: Channel,
    forced_agent_id: Option<&'static str>,
) -> Response {
    let started = Instant::now();
    let request_id = Uuid::new_v4().to_string();

    let authorized_agent = match authorize_for_agent(&state, &headers, forced_agent_id).await {
        Ok(agent_id) => agent_id,
        Err(response) => return response,
    };

    let mut client_request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {err}")),
    };
    let config = state.config.read().await.clone();
    let route_affinity_key = provider_route_affinity_key(&config, &client_request, &client_channel);
    let route_affinity_provider_id =
        lookup_provider_route_affinity(&state, &config, route_affinity_key.as_deref()).await;
    let route_is_agent_bound = route_is_agent_provider_bound(
        &config,
        &client_request,
        &client_channel,
        authorized_agent.as_deref(),
    );
    let mut decision = match decide_route(
        &config,
        &client_request,
        &client_channel,
        authorized_agent.as_deref(),
    ) {
        Ok(decision) => decision,
        Err(err) => return json_error(StatusCode::BAD_REQUEST, &err),
    };
    if !route_is_agent_bound {
        decision = apply_provider_route_affinity(
            &config,
            decision,
            &client_request,
            &client_channel,
            route_affinity_provider_id.as_deref(),
        );
    }
    let codex_responses_chat_compat = should_preempt_codex_responses_via_chat(
        &config,
        forced_agent_id,
        &client_channel,
        &decision,
    );
    if codex_responses_chat_compat {
        decision.upstream_channel = Channel::Chat;
    }
    let requested_model_for_log = requested_model_for_log(&client_request, &decision.model);
    set_request_model(&mut client_request, &decision.model);

    let request_had_stream_field = client_request.get("stream").is_some();
    let client_requested_stream =
        infer_client_requested_stream(&mut client_request, &client_channel, forced_agent_id);
    let codex_defaulted_responses_stream = forced_agent_id == Some("codex")
        && matches!(client_channel, Channel::Responses)
        && !request_had_stream_field
        && client_requested_stream;
    let codex_native_responses_stream = forced_agent_id == Some("codex")
        && matches!(client_channel, Channel::Responses)
        && matches!(decision.upstream_channel, Channel::Responses)
        && client_requested_stream
        && !codex_responses_chat_compat;
    let (agent_log_id, agent_log_label) =
        request_agent_log_fields(&config, authorized_agent.as_deref());
    let codex_chat_tool_context = codex_responses_chat_compat
        .then(|| transform_codex_chat::build_codex_tool_context_from_request(&client_request));
    let mut upstream_body = if codex_responses_chat_compat {
        match transform_codex_chat::responses_to_chat_completions(client_request.clone()) {
            Ok(body) => body,
            Err(err) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to convert Codex Responses request for Chat upstream: {err}"),
                )
            }
        }
    } else {
        transform_request_for_channel(&client_request, &client_channel, &decision.upstream_channel)
    };
    if matches!(decision.upstream_channel, Channel::Responses) && !codex_native_responses_stream {
        normalize_responses_request(&mut upstream_body);
    }
    let reasoning_diagnostics = apply_model_reasoning_effort(
        &client_request,
        &mut upstream_body,
        &decision.upstream_channel,
        &decision,
    );
    if codex_native_responses_stream {
        strip_provider_cache_key_fields(&mut upstream_body);
    } else {
        optimize_provider_prefix(&mut upstream_body, &config, &decision);
    }

    let mut provider_prefix_body = upstream_body.clone();
    if codex_native_responses_stream {
        normalize_responses_request(&mut provider_prefix_body);
        optimize_provider_prefix(&mut provider_prefix_body, &config, &decision);
        copy_responses_prefix_cache_fields_for_native_stream(
            &mut upstream_body,
            &provider_prefix_body,
            &config,
            &decision,
        );
    }

    let cross_protocol_stream = client_requested_stream
        && client_channel != decision.upstream_channel
        && !codex_responses_chat_compat;
    if cross_protocol_stream {
        set_stream_flag(&mut upstream_body, false);
    }
    let provider_prefix_key = openai_prompt_cache_key(&provider_prefix_body);
    let provider_prefix_fingerprint = Some(provider_prefix_fingerprint(
        &provider_prefix_body,
        &decision.upstream_channel,
    ));
    let provider_prefix_control_key = provider_prefix_control_key(
        provider_prefix_fingerprint.as_deref(),
        &decision,
        &decision.upstream_channel,
    );
    let response_session_key = if responses_session_reuse_enabled(&config) {
        responses_session_key(&config, &decision, &upstream_body)
    } else {
        None
    };
    let response_session_scope_key = if responses_session_reuse_enabled(&config) {
        responses_session_scope_key(&config, &decision, &upstream_body)
    } else {
        None
    };
    let provider_prefix_family_key = provider_prefix_family_control_key(
        response_session_scope_key.as_deref(),
        &decision,
        &decision.upstream_channel,
    );

    let no_store = headers
        .get(header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains("no-store"))
        .unwrap_or(false);
    let eligible = config.cache.enabled && !no_store && cache::is_cache_eligible(&client_request);
    let key_material = json!({
        "client_channel": client_channel.label(),
        "upstream_channel": decision.upstream_channel.label(),
        "client_stream": client_requested_stream,
        "request": upstream_body.clone()
    });
    let key = cache::cache_key(
        &key_material,
        &decision.provider.id,
        &decision.model,
        &config.workspace_fingerprint,
    );

    if eligible {
        if let Some(hit) =
            lookup_cache(&state, &[key.as_str()], None, None, &decision, &config).await
        {
            return cache_hit_response(
                &state,
                hit,
                started,
                request_id,
                &client_channel,
                &decision,
                requested_model_for_log.clone(),
                non_sse_compact_compat_for_decision(&config, &decision),
            )
            .await;
        }
    }

    let mut cache_keys = vec![key.clone()];
    let fuzzy_safe = cache::is_fuzzy_cache_safe(&client_request);
    if fuzzy_safe {
        let near_exact_key = cache::near_exact_cache_key(
            &key_material,
            &decision.provider.id,
            &decision.model,
            &config.workspace_fingerprint,
        );
        cache_keys.push(near_exact_key.clone());
    }
    if local_session_keys_enabled(&config) {
        let session_key = cache::session_cache_key(
            &key_material,
            &decision.provider.id,
            &decision.model,
            &config.workspace_fingerprint,
        );
        if !cache_keys.contains(&session_key) {
            cache_keys.push(session_key);
        }
        if fuzzy_safe {
            let session_near_exact_key = cache::session_near_exact_cache_key(
                &key_material,
                &decision.provider.id,
                &decision.model,
                &config.workspace_fingerprint,
            );
            if !cache_keys.contains(&session_near_exact_key) {
                cache_keys.push(session_near_exact_key);
            }
        }
    }
    let metrics_cache_key = metrics_cache_key(&cache_keys);
    let semantic_text = if eligible && config.cache.semantic_enabled && fuzzy_safe {
        cache::semantic_text(&client_request)
    } else {
        None
    };
    let semantic_shape = if semantic_text.is_some() {
        cache::semantic_shape(&client_request)
    } else {
        None
    };

    if eligible {
        let lookup_keys = cache_keys.iter().map(String::as_str).collect::<Vec<_>>();
        if let Some(hit) = lookup_cache(
            &state,
            &lookup_keys,
            semantic_text.as_deref().filter(|_| fuzzy_safe),
            semantic_shape.as_deref(),
            &decision,
            &config,
        )
        .await
        {
            return cache_hit_response(
                &state,
                hit,
                started,
                request_id,
                &client_channel,
                &decision,
                requested_model_for_log.clone(),
                non_sse_compact_compat_for_decision(&config, &decision),
            )
            .await;
        }
    }

    let url = upstream_url(&decision.provider.base_url, &decision.upstream_channel);
    let mut selected_provider_key = match select_provider_api_key(
        &state,
        &decision.provider.id,
        None,
        provider_prefix_control_key.as_deref(),
    )
    .await
    {
        Ok(selected) => selected,
        Err(err) if err.to_string().contains("not configured") => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "provider API key is not configured",
            )
        }
        Err(err) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to select provider key: {err}"),
            )
        }
    };
    let mut api_key = selected_provider_key.secret.clone();
    if api_key.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "provider API key is not configured",
        );
    }

    let skip_prefix_guard_for_sync_responses = responses_sync_main_skips_prefix_guard(
        &client_channel,
        &decision.upstream_channel,
        client_requested_stream,
    );
    let responses_main_retry_guard =
        skip_prefix_guard_for_sync_responses || codex_defaulted_responses_stream;
    if responses_sync_main_prefix_error_cooled_down(
        &state,
        responses_main_retry_guard,
        provider_prefix_control_key.as_deref(),
    )
    .await
    {
        state
            .metrics
            .record_error(
                "responses_sync_main_prefix_error_cooldown",
                "skip sync main request after recent upstream sync failure for same prefix",
            )
            .await;
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream is cooling down after recent sync failure; retry shortly",
        );
    }
    let prefix_guard = if skip_prefix_guard_for_sync_responses || !smart_hit_enabled(&config) {
        None
    } else {
        acquire_provider_prefix_guard(
            &state,
            &decision.upstream_channel,
            provider_prefix_control_key.as_deref(),
            provider_prefix_family_key.as_deref(),
        )
        .await
    };
    let full_response_input = upstream_body.get("input").cloned();
    let tail_input_diagnostics = tail_input_diagnostics_for_session(
        &state,
        &decision.upstream_channel,
        response_session_key.as_deref(),
        response_session_scope_key.as_deref(),
        full_response_input.as_ref(),
    )
    .await;
    let session_anchor_diagnostics = response_session_anchor_diagnostics(
        &state,
        response_session_key.as_deref(),
        response_session_scope_key.as_deref(),
    )
    .await;
    let local_prepare_ms = started.elapsed().as_millis() as u64;
    let prefix_guard_wait = if prefix_guard.is_some() {
        wait_for_provider_prefix_settle(
            &state,
            &decision.upstream_channel,
            provider_prefix_control_key.as_deref(),
            provider_prefix_family_key.as_deref(),
            &tail_input_diagnostics,
            prefix_guard_wait_budget_for_channel(&decision.upstream_channel, started.elapsed()),
        )
        .await
    } else {
        PrefixGuardWaitDiagnostics::default()
    };
    let response_session_guard = acquire_response_session_guard(
        &state,
        &decision.upstream_channel,
        response_session_key.as_deref(),
    )
    .await;
    let response_session_cooldown_key = response_session_error_cooldown_key(&decision);
    let response_session_cooldown_active =
        response_session_cooldown_active(&state, response_session_cooldown_key.as_deref()).await;
    let (upstream_send_body, mut response_session_reuse_diagnostics) =
        if matches!(decision.upstream_channel, Channel::Responses) {
            let mut diagnostics = ResponseSessionReuseDiagnostics::default();
            if let Some(cooldown_skip_reason) = response_session_cooldown_skip_reason(
                &state,
                response_session_cooldown_key.as_deref(),
            )
            .await
            {
                diagnostics.cooldown_active = true;
                if cooldown_skip_reason == "provider_session_delta_unsupported" {
                    diagnostics.skip_reason = Some(cooldown_skip_reason);
                    (upstream_body.clone(), diagnostics)
                } else {
                    diagnostics.skip_reason = Some(cooldown_skip_reason);
                    (upstream_body.clone(), diagnostics)
                }
            } else if !main_response_session_delta_enabled_for_agent(forced_agent_id) {
                diagnostics.skip_reason = Some("codex_main_session_delta_disabled".to_string());
                (upstream_body.clone(), diagnostics)
            } else if should_attempt_main_response_session_delta(
                &config,
                &decision,
                client_requested_stream,
                &upstream_body,
                &tail_input_diagnostics,
                &session_anchor_diagnostics,
            ) {
                let outcome = maybe_reuse_response_session(
                    &state,
                    &upstream_body,
                    response_session_key.as_deref(),
                    response_session_scope_key.as_deref(),
                    &decision,
                    true,
                    false,
                )
                .await;
                if response_session_delta_request(&outcome.body, &upstream_body)
                    && response_session_delta_is_beneficial(
                        &upstream_body,
                        &outcome.body,
                        &tail_input_diagnostics,
                    )
                {
                    (outcome.body, outcome.diagnostics)
                } else {
                    diagnostics = outcome.diagnostics;
                    diagnostics.skip_reason = Some("main_session_delta_not_beneficial".to_string());
                    (upstream_body.clone(), diagnostics)
                }
            } else {
                diagnostics.skip_reason = Some("main_session_delta_guard_not_eligible".to_string());
                (upstream_body.clone(), diagnostics)
            }
        } else {
            (
                upstream_body.clone(),
                ResponseSessionReuseDiagnostics::default(),
            )
        };
    let used_response_session = response_session_delta_request(&upstream_send_body, &upstream_body);
    if used_response_session {
        response_session_reuse_diagnostics.skip_reason = None;
    }
    let mut active_used_response_session = used_response_session;
    let mut diagnostics = body_diagnostics(
        &decision.upstream_channel,
        &upstream_body,
        &upstream_send_body,
        active_used_response_session,
    );
    diagnostics.reasoning = reasoning_diagnostics;
    let mut retried_full_response = false;
    let mut prefix_state_update_key =
        provider_prefix_state_update_key(provider_prefix_control_key.as_deref());
    let responses_non_stream_upstream_sse_compat = should_send_responses_non_stream_as_upstream_sse(
        &client_channel,
        &decision.upstream_channel,
        client_requested_stream,
    );
    let compact_chat_compat_cooldown_key = compact_chat_compat_cooldown_key(&decision);
    let compact_chat_compat_cooldown_active =
        compact_chat_compat_cooldown_active(&state, &compact_chat_compat_cooldown_key).await;
    let responses_non_stream_chat_compat = !compact_chat_compat_cooldown_active
        && should_route_responses_non_stream_compact_via_chat(
            &client_channel,
            &decision.upstream_channel,
            client_requested_stream,
            &tail_input_diagnostics,
            serialized_body_len(&decision.upstream_channel, &upstream_body),
        );
    let mut active_request_channel = if responses_non_stream_chat_compat {
        Channel::Chat
    } else {
        decision.upstream_channel.clone()
    };
    let mut active_responses_non_stream_chat_compat = responses_non_stream_chat_compat;
    let mut responses_sync_main_chat_compat_fallback = false;
    let mut active_url = upstream_url(&decision.provider.base_url, &active_request_channel);

    let mut active_upstream_body = build_active_upstream_body_for_compat(
        &upstream_body,
        &upstream_send_body,
        &config,
        &decision,
        &active_request_channel,
        responses_non_stream_upstream_sse_compat,
    );
    let chat_compat_fast_json = responses_non_stream_chat_compat
        && should_use_chat_non_stream_compact_fast_path(
            &tail_input_diagnostics,
            serialized_body_len(&active_request_channel, &active_upstream_body),
        );
    if chat_compat_fast_json {
        set_stream_flag(&mut active_upstream_body, false);
    }
    diagnostics.send_body_bytes =
        serialized_body_len(&active_request_channel, &active_upstream_body);
    diagnostics.send_body_is_delta =
        response_session_delta_request(&active_upstream_body, &upstream_body);
    if responses_non_stream_chat_compat {
        prefix_state_update_key = None;
    }
    let mut upstream_request_diagnostics = UpstreamRequestDiagnostics::default();
    let skip_gzip_for_cold_stream = should_skip_request_body_gzip_for_cold_stream(
        &active_request_channel,
        client_requested_stream,
        &active_upstream_body,
    );
    let send_outcome = match send_main_upstream_request(
        &state,
        decision.provider.use_system_proxy,
        &active_url,
        &api_key,
        &active_request_channel,
        &active_upstream_body,
        &headers,
        decision.provider.custom_user_agent.as_deref(),
        skip_prefix_guard_for_sync_responses,
        decision.provider.request_body_gzip_enabled && !skip_gzip_for_cold_stream,
        !client_requested_stream,
    )
    .await
    {
        Ok(response) => response,
        Err(err) => {
            record_upstream_transport_failure(
                &state,
                &request_id,
                &started,
                &client_channel,
                &active_request_channel,
                &decision,
                eligible.then_some(metrics_cache_key.as_str()),
                provider_prefix_key.as_deref(),
                provider_prefix_fingerprint.as_deref(),
                &prefix_guard_wait,
                local_prepare_ms,
                &diagnostics,
                &active_upstream_body,
                active_used_response_session,
                &response_session_reuse_diagnostics,
                requested_model_for_log.clone(),
                "main-transport",
            )
            .await;
            state
                .metrics
                .record_error("upstream_transport", &err.to_string())
                .await;
            return json_error(
                StatusCode::BAD_GATEWAY,
                &format!("upstream request failed: {err}"),
            );
        }
    };
    let mut current_send_diagnostics = send_outcome.diagnostics.clone();
    upstream_request_diagnostics.absorb(&current_send_diagnostics);
    upstream_request_diagnostics.gzip_skipped_cold_stream |= skip_gzip_for_cold_stream;
    let mut upstream = send_outcome.response;
    let mut upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;

    let mut status = upstream.status().as_u16();
    let mut content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let mut is_success_status = StatusCode::from_u16(status)
        .map(|status| status.is_success())
        .unwrap_or(false);
    if let Some(next_key) = try_retry_with_next_provider_key(
        &state,
        &decision.provider.id,
        &selected_provider_key,
        status,
        provider_prefix_control_key.as_deref(),
    )
    .await
    {
        record_upstream_response_observation(
            &state,
            &request_id,
            &started,
            &client_channel,
            &active_request_channel,
            &decision,
            eligible.then_some(metrics_cache_key.as_str()),
            provider_prefix_key.as_deref(),
            provider_prefix_fingerprint.as_deref(),
            &prefix_guard_wait,
            local_prepare_ms,
            &diagnostics,
            &active_upstream_body,
            active_used_response_session,
            &response_session_reuse_diagnostics,
            requested_model_for_log.clone(),
            current_non_stream_upstream_call_source(
                skip_prefix_guard_for_sync_responses,
                active_responses_non_stream_chat_compat,
                responses_sync_main_chat_compat_fallback,
            ),
            status,
            &current_send_diagnostics,
            agent_log_id.clone(),
            agent_log_label.clone(),
        )
        .await;
        state.metrics.record_retry().await;
        selected_provider_key = next_key;
        api_key = selected_provider_key.secret.clone();
        let send_outcome = match send_main_upstream_request(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            skip_prefix_guard_for_sync_responses,
            decision.provider.request_body_gzip_enabled && !skip_gzip_for_cold_stream,
            !client_requested_stream,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                record_upstream_transport_failure(
                    &state,
                    &request_id,
                    &started,
                    &client_channel,
                    &active_request_channel,
                    &decision,
                    eligible.then_some(metrics_cache_key.as_str()),
                    provider_prefix_key.as_deref(),
                    provider_prefix_fingerprint.as_deref(),
                    &prefix_guard_wait,
                    local_prepare_ms,
                    &diagnostics,
                    &active_upstream_body,
                    active_used_response_session,
                    &response_session_reuse_diagnostics,
                    requested_model_for_log.clone(),
                    "provider-key-failover-transport",
                )
                .await;
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream request failed after key failover: {err}"),
                );
            }
        };
        current_send_diagnostics = send_outcome.diagnostics.clone();
        upstream_request_diagnostics.absorb(&current_send_diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        is_success_status = StatusCode::from_u16(status)
            .map(|status| status.is_success())
            .unwrap_or(false);
    }
    if should_retry_full_response_after_session_error(status, used_response_session) {
        record_upstream_response_observation(
            &state,
            &request_id,
            &started,
            &client_channel,
            &active_request_channel,
            &decision,
            eligible.then_some(metrics_cache_key.as_str()),
            provider_prefix_key.as_deref(),
            provider_prefix_fingerprint.as_deref(),
            &prefix_guard_wait,
            local_prepare_ms,
            &diagnostics,
            &active_upstream_body,
            active_used_response_session,
            &response_session_reuse_diagnostics,
            requested_model_for_log.clone(),
            current_non_stream_upstream_call_source(
                skip_prefix_guard_for_sync_responses,
                active_responses_non_stream_chat_compat,
                responses_sync_main_chat_compat_fallback,
            ),
            status,
            &current_send_diagnostics,
            agent_log_id.clone(),
            agent_log_label.clone(),
        )
        .await;
        let session_error_summary = match read_upstream_body_with_diagnostics(
            upstream,
            &content_type,
            started,
            upstream_response_headers_at_ms,
        )
        .await
        {
            Ok(outcome) => upstream_error_summary(&outcome.bytes),
            Err(err) => {
                state
                    .metrics
                    .record_error("response_session_delta_error_body", &err.to_string())
                    .await;
                String::new()
            }
        };
        if !session_error_summary.is_empty() {
            state
                .metrics
                .record_error("response_session_delta_rejected", &session_error_summary)
                .await;
        }
        note_response_session_error_cooldown_for_rejection(
            &state,
            response_session_cooldown_key.as_deref(),
            status,
            &session_error_summary,
        )
        .await;
        response_session_reuse_diagnostics.rejected_status = Some(status);
        let stale_response_id = previous_response_id_from_request(&active_upstream_body);
        clear_response_session_reference(
            &state,
            response_session_key.as_deref(),
            stale_response_id.as_deref(),
        )
        .await;
        state.metrics.record_retry().await;
        retried_full_response = true;
        active_upstream_body = build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &active_request_channel,
            responses_non_stream_upstream_sse_compat,
        );
        if chat_compat_fast_json {
            set_stream_flag(&mut active_upstream_body, false);
        }
        active_used_response_session = false;
        diagnostics.send_body_bytes =
            serialized_body_len(&active_request_channel, &active_upstream_body);
        diagnostics.send_body_is_delta = false;
        prefix_state_update_key =
            provider_prefix_state_update_key(provider_prefix_control_key.as_deref());
        let send_outcome = match send_main_upstream_request(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            skip_prefix_guard_for_sync_responses,
            decision.provider.request_body_gzip_enabled,
            !client_requested_stream,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                record_upstream_transport_failure(
                    &state,
                    &request_id,
                    &started,
                    &client_channel,
                    &active_request_channel,
                    &decision,
                    eligible.then_some(metrics_cache_key.as_str()),
                    provider_prefix_key.as_deref(),
                    provider_prefix_fingerprint.as_deref(),
                    &prefix_guard_wait,
                    local_prepare_ms,
                    &diagnostics,
                    &active_upstream_body,
                    active_used_response_session,
                    &response_session_reuse_diagnostics,
                    requested_model_for_log.clone(),
                    "session-delta-full-retry-transport",
                )
                .await;
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream request failed: {err}"),
                );
            }
        };
        current_send_diagnostics = send_outcome.diagnostics.clone();
        upstream_request_diagnostics.absorb(&current_send_diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        is_success_status = StatusCode::from_u16(status)
            .map(|status| status.is_success())
            .unwrap_or(false);
    }
    if should_fallback_responses_sync_main_to_chat_compat(
        status,
        responses_main_retry_guard,
        responses_non_stream_chat_compat,
    ) {
        record_upstream_response_observation(
            &state,
            &request_id,
            &started,
            &client_channel,
            &active_request_channel,
            &decision,
            eligible.then_some(metrics_cache_key.as_str()),
            provider_prefix_key.as_deref(),
            provider_prefix_fingerprint.as_deref(),
            &prefix_guard_wait,
            local_prepare_ms,
            &diagnostics,
            &active_upstream_body,
            active_used_response_session,
            &response_session_reuse_diagnostics,
            requested_model_for_log.clone(),
            current_non_stream_upstream_call_source(
                skip_prefix_guard_for_sync_responses,
                active_responses_non_stream_chat_compat,
                responses_sync_main_chat_compat_fallback,
            ),
            status,
            &current_send_diagnostics,
            agent_log_id.clone(),
            agent_log_label.clone(),
        )
        .await;
        state.metrics.record_retry().await;
        active_upstream_body = build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &Channel::Chat,
            false,
        );
        let chat_fallback_fast_json = should_use_chat_non_stream_compact_fast_path(
            &tail_input_diagnostics,
            serialized_body_len(&Channel::Chat, &active_upstream_body),
        );
        if chat_fallback_fast_json {
            set_stream_flag(&mut active_upstream_body, false);
        }
        active_request_channel = Channel::Chat;
        active_responses_non_stream_chat_compat = true;
        responses_sync_main_chat_compat_fallback = true;
        active_used_response_session = false;
        diagnostics.send_body_bytes = serialized_body_len(&Channel::Chat, &active_upstream_body);
        diagnostics.send_body_is_delta = false;
        prefix_state_update_key = None;
        let chat_url = upstream_url(&decision.provider.base_url, &Channel::Chat);
        let send_outcome = match send_main_upstream_request(
            &state,
            decision.provider.use_system_proxy,
            &chat_url,
            &api_key,
            &Channel::Chat,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            skip_prefix_guard_for_sync_responses,
            decision.provider.request_body_gzip_enabled,
            true,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream request failed: {err}"),
                );
            }
        };
        current_send_diagnostics = send_outcome.diagnostics.clone();
        upstream_request_diagnostics.absorb(&current_send_diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        is_success_status = StatusCode::from_u16(status)
            .map(|status| status.is_success())
            .unwrap_or(false);
    }
    if !responses_sync_main_chat_compat_fallback
        && should_fallback_chat_compat_compact_to_responses(
            status,
            skip_prefix_guard_for_sync_responses,
            active_responses_non_stream_chat_compat,
        )
    {
        record_upstream_response_observation(
            &state,
            &request_id,
            &started,
            &client_channel,
            &active_request_channel,
            &decision,
            eligible.then_some(metrics_cache_key.as_str()),
            provider_prefix_key.as_deref(),
            provider_prefix_fingerprint.as_deref(),
            &prefix_guard_wait,
            local_prepare_ms,
            &diagnostics,
            &active_upstream_body,
            active_used_response_session,
            &response_session_reuse_diagnostics,
            requested_model_for_log.clone(),
            current_non_stream_upstream_call_source(
                skip_prefix_guard_for_sync_responses,
                active_responses_non_stream_chat_compat,
                responses_sync_main_chat_compat_fallback,
            ),
            status,
            &current_send_diagnostics,
            agent_log_id.clone(),
            agent_log_label.clone(),
        )
        .await;
        let chat_error_body = match read_upstream_body_with_diagnostics(
            upstream,
            &content_type,
            started,
            upstream_response_headers_at_ms,
        )
        .await
        {
            Ok(outcome) => outcome.bytes,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_chat_compat_body", &err.to_string())
                    .await;
                Vec::new()
            }
        };
        let error_summary = upstream_error_summary(&chat_error_body);
        state
            .metrics
            .record_error(upstream_error_scope(status, &error_summary), &error_summary)
            .await;
        note_compact_chat_compat_cooldown(&state, &compact_chat_compat_cooldown_key).await;
        state.metrics.record_retry().await;

        active_request_channel = decision.upstream_channel.clone();
        active_url = upstream_url(&decision.provider.base_url, &active_request_channel);
        active_responses_non_stream_chat_compat = false;
        responses_sync_main_chat_compat_fallback = false;
        active_upstream_body = build_active_upstream_body_for_compat(
            &upstream_body,
            &upstream_body,
            &config,
            &decision,
            &active_request_channel,
            responses_non_stream_upstream_sse_compat,
        );
        active_used_response_session = false;
        diagnostics.send_body_bytes =
            serialized_body_len(&active_request_channel, &active_upstream_body);
        diagnostics.send_body_is_delta = false;

        let send_outcome = match send_main_upstream_request(
            &state,
            decision.provider.use_system_proxy,
            &active_url,
            &api_key,
            &active_request_channel,
            &active_upstream_body,
            &headers,
            decision.provider.custom_user_agent.as_deref(),
            skip_prefix_guard_for_sync_responses,
            decision.provider.request_body_gzip_enabled,
            true,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                state
                    .metrics
                    .record_error("upstream_transport", &err.to_string())
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream request failed: {err}"),
                );
            }
        };
        current_send_diagnostics = send_outcome.diagnostics.clone();
        upstream_request_diagnostics.absorb(&current_send_diagnostics);
        upstream = send_outcome.response;
        upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
        status = upstream.status().as_u16();
        content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        is_success_status = StatusCode::from_u16(status)
            .map(|status| status.is_success())
            .unwrap_or(false);
    }
    if should_attempt_response_session_rescue_after_413(
        status,
        active_used_response_session,
        active_responses_non_stream_chat_compat,
        &decision.upstream_channel,
        response_session_cooldown_active,
    ) {
        record_upstream_response_observation(
            &state,
            &request_id,
            &started,
            &client_channel,
            &active_request_channel,
            &decision,
            eligible.then_some(metrics_cache_key.as_str()),
            provider_prefix_key.as_deref(),
            provider_prefix_fingerprint.as_deref(),
            &prefix_guard_wait,
            local_prepare_ms,
            &diagnostics,
            &active_upstream_body,
            active_used_response_session,
            &response_session_reuse_diagnostics,
            requested_model_for_log.clone(),
            current_non_stream_upstream_call_source(
                skip_prefix_guard_for_sync_responses,
                active_responses_non_stream_chat_compat,
                responses_sync_main_chat_compat_fallback,
            ),
            status,
            &current_send_diagnostics,
            agent_log_id.clone(),
            agent_log_label.clone(),
        )
        .await;
        diagnostics.payload_too_large_rescue_attempted = true;
        let rescue_outcome = maybe_rescue_response_session_after_413(
            &state,
            &upstream_body,
            response_session_key.as_deref(),
            response_session_scope_key.as_deref(),
            &decision,
        )
        .await;
        response_session_reuse_diagnostics = rescue_outcome.diagnostics.clone();
        if response_session_delta_request(&rescue_outcome.body, &upstream_body) {
            state.metrics.record_retry().await;
            active_upstream_body = rescue_outcome.body;
            apply_responses_non_stream_upstream_sse_compat(
                &mut active_upstream_body,
                responses_non_stream_upstream_sse_compat,
            );
            active_used_response_session =
                response_session_delta_request(&active_upstream_body, &upstream_body);
            diagnostics.payload_too_large_rescue_used = active_used_response_session;
            diagnostics.send_body_bytes =
                serialized_body_len(&decision.upstream_channel, &active_upstream_body);
            diagnostics.send_body_is_delta = active_used_response_session;
            let send_outcome = match send_main_upstream_request(
                &state,
                decision.provider.use_system_proxy,
                &url,
                &api_key,
                &decision.upstream_channel,
                &active_upstream_body,
                &headers,
                decision.provider.custom_user_agent.as_deref(),
                skip_prefix_guard_for_sync_responses,
                decision.provider.request_body_gzip_enabled,
                !client_requested_stream,
            )
            .await
            {
                Ok(response) => response,
                Err(err) => {
                    record_upstream_transport_failure(
                        &state,
                        &request_id,
                        &started,
                        &client_channel,
                        &decision.upstream_channel,
                        &decision,
                        eligible.then_some(metrics_cache_key.as_str()),
                        provider_prefix_key.as_deref(),
                        provider_prefix_fingerprint.as_deref(),
                        &prefix_guard_wait,
                        local_prepare_ms,
                        &diagnostics,
                        &active_upstream_body,
                        active_used_response_session,
                        &response_session_reuse_diagnostics,
                        requested_model_for_log.clone(),
                        "payload-too-large-rescue-transport",
                    )
                    .await;
                    state
                        .metrics
                        .record_error("upstream_transport", &err.to_string())
                        .await;
                    return json_error(
                        StatusCode::BAD_GATEWAY,
                        &format!("upstream request failed: {err}"),
                    );
                }
            };
            current_send_diagnostics = send_outcome.diagnostics.clone();
            upstream_request_diagnostics.absorb(&current_send_diagnostics);
            upstream = send_outcome.response;
            upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
            status = upstream.status().as_u16();
            content_type = upstream
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            is_success_status = StatusCode::from_u16(status)
                .map(|status| status.is_success())
                .unwrap_or(false);
            if should_retry_full_response_after_session_error(status, active_used_response_session)
            {
                record_upstream_response_observation(
                    &state,
                    &request_id,
                    &started,
                    &client_channel,
                    &decision.upstream_channel,
                    &decision,
                    eligible.then_some(metrics_cache_key.as_str()),
                    provider_prefix_key.as_deref(),
                    provider_prefix_fingerprint.as_deref(),
                    &prefix_guard_wait,
                    local_prepare_ms,
                    &diagnostics,
                    &active_upstream_body,
                    active_used_response_session,
                    &response_session_reuse_diagnostics,
                    requested_model_for_log.clone(),
                    current_non_stream_upstream_call_source(
                        skip_prefix_guard_for_sync_responses,
                        active_responses_non_stream_chat_compat,
                        responses_sync_main_chat_compat_fallback,
                    ),
                    status,
                    &current_send_diagnostics,
                    agent_log_id.clone(),
                    agent_log_label.clone(),
                )
                .await;
                note_response_session_error_cooldown_for_rejection(
                    &state,
                    response_session_cooldown_key.as_deref(),
                    status,
                    "",
                )
                .await;
                response_session_reuse_diagnostics.rejected_status = Some(status);
                let stale_response_id = previous_response_id_from_request(&active_upstream_body);
                clear_response_session_reference(
                    &state,
                    response_session_key.as_deref(),
                    stale_response_id.as_deref(),
                )
                .await;
                state.metrics.record_retry().await;
                retried_full_response = true;
                active_upstream_body = upstream_body.clone();
                apply_responses_non_stream_upstream_sse_compat(
                    &mut active_upstream_body,
                    responses_non_stream_upstream_sse_compat,
                );
                active_used_response_session = false;
                diagnostics.payload_too_large_rescue_used = false;
                diagnostics.send_body_bytes =
                    serialized_body_len(&decision.upstream_channel, &active_upstream_body);
                diagnostics.send_body_is_delta = false;
                prefix_state_update_key =
                    provider_prefix_state_update_key(provider_prefix_control_key.as_deref());
                let send_outcome = match send_main_upstream_request(
                    &state,
                    decision.provider.use_system_proxy,
                    &url,
                    &api_key,
                    &decision.upstream_channel,
                    &active_upstream_body,
                    &headers,
                    decision.provider.custom_user_agent.as_deref(),
                    skip_prefix_guard_for_sync_responses,
                    decision.provider.request_body_gzip_enabled,
                    !client_requested_stream,
                )
                .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        record_upstream_transport_failure(
                            &state,
                            &request_id,
                            &started,
                            &client_channel,
                            &decision.upstream_channel,
                            &decision,
                            eligible.then_some(metrics_cache_key.as_str()),
                            provider_prefix_key.as_deref(),
                            provider_prefix_fingerprint.as_deref(),
                            &prefix_guard_wait,
                            local_prepare_ms,
                            &diagnostics,
                            &active_upstream_body,
                            active_used_response_session,
                            &response_session_reuse_diagnostics,
                            requested_model_for_log.clone(),
                            "payload-too-large-full-retry-transport",
                        )
                        .await;
                        state
                            .metrics
                            .record_error("upstream_transport", &err.to_string())
                            .await;
                        return json_error(
                            StatusCode::BAD_GATEWAY,
                            &format!("upstream request failed: {err}"),
                        );
                    }
                };
                current_send_diagnostics = send_outcome.diagnostics.clone();
                upstream_request_diagnostics.absorb(&current_send_diagnostics);
                upstream = send_outcome.response;
                upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
                status = upstream.status().as_u16();
                content_type = upstream
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("application/json")
                    .to_string();
                is_success_status = StatusCode::from_u16(status)
                    .map(|status| status.is_success())
                    .unwrap_or(false);
            }
        }
    }
    let is_stream = should_proxy_upstream_as_stream(
        is_success_status,
        client_requested_stream,
        cross_protocol_stream,
        &active_upstream_body,
        &content_type,
    );

    if is_stream {
        note_selected_provider_key_status(
            &state,
            &decision.provider.id,
            &selected_provider_key,
            status,
            None,
        )
        .await;
        return stream_upstream(
            state,
            upstream,
            content_type,
            status,
            started,
            request_id,
            client_channel,
            decision,
            eligible,
            cache_keys,
            metrics_cache_key.clone(),
            semantic_text,
            semantic_shape,
            provider_prefix_key.clone(),
            provider_prefix_fingerprint.clone(),
            provider_prefix_family_key.clone(),
            route_affinity_key.clone(),
            config,
            prefix_guard,
            prefix_state_update_key.clone(),
            response_session_guard,
            response_session_key.clone(),
            response_session_scope_key.clone(),
            full_response_input.clone(),
            active_used_response_session,
            retried_full_response,
            active_upstream_body.clone(),
            diagnostics.clone(),
            tail_input_diagnostics.clone(),
            session_anchor_diagnostics.clone(),
            response_session_reuse_diagnostics.clone(),
            codex_chat_tool_context.clone(),
            agent_log_id.clone(),
            agent_log_label.clone(),
            requested_model_for_log.clone(),
            prefix_guard_wait.clone(),
            local_prepare_ms,
            upstream_request_diagnostics.clone(),
            upstream_response_headers_at_ms,
        )
        .await;
    }

    let mut body_read = match read_upstream_body_with_diagnostics(
        upstream,
        &content_type,
        started,
        upstream_response_headers_at_ms,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            state
                .metrics
                .record_error("upstream_body", &err.to_string())
                .await;
            return json_error(
                StatusCode::BAD_GATEWAY,
                &format!("failed to read upstream body: {err}"),
            );
        }
    };
    let mut bytes = body_read.bytes;
    if !is_success_status
        && matches!(active_request_channel, Channel::Responses)
        && active_upstream_body.get("previous_response_id").is_some()
    {
        let error_summary = upstream_error_summary(&bytes);
        if response_session_rejection_classification(status, &error_summary)
            == ResponseSessionRejectionClass::Unsupported
        {
            record_upstream_response_observation(
                &state,
                &request_id,
                &started,
                &client_channel,
                &active_request_channel,
                &decision,
                eligible.then_some(metrics_cache_key.as_str()),
                provider_prefix_key.as_deref(),
                provider_prefix_fingerprint.as_deref(),
                &prefix_guard_wait,
                local_prepare_ms,
                &diagnostics,
                &active_upstream_body,
                active_used_response_session,
                &response_session_reuse_diagnostics,
                requested_model_for_log.clone(),
                current_non_stream_upstream_call_source(
                    skip_prefix_guard_for_sync_responses,
                    active_responses_non_stream_chat_compat,
                    responses_sync_main_chat_compat_fallback,
                ),
                status,
                &current_send_diagnostics,
                agent_log_id.clone(),
                agent_log_label.clone(),
            )
            .await;
            note_response_session_error_cooldown_for_rejection(
                &state,
                response_session_cooldown_key.as_deref(),
                status,
                &error_summary,
            )
            .await;
            if let Some(compat) =
                maybe_prepare_previous_response_compat_body(&state, &active_upstream_body)
                    .await
                    .or_else(|| {
                        strip_previous_response_id_for_compat(
                            &active_upstream_body,
                            "client_previous_response_id_unsupported_retry",
                        )
                    })
            {
                if !error_summary.is_empty() {
                    state
                        .metrics
                        .record_error("client_previous_response_id_unsupported", &error_summary)
                        .await;
                }
                state.metrics.record_retry().await;
                response_session_reuse_diagnostics.rejected_status = Some(status);
                response_session_reuse_diagnostics.skip_reason = Some(compat.reason.to_string());
                active_upstream_body = compat.body;
                apply_responses_non_stream_upstream_sse_compat(
                    &mut active_upstream_body,
                    responses_non_stream_upstream_sse_compat,
                );
                active_used_response_session = false;
                diagnostics.send_body_bytes =
                    serialized_body_len(&active_request_channel, &active_upstream_body);
                diagnostics.send_body_is_delta = false;

                let send_outcome = match send_main_upstream_request(
                    &state,
                    decision.provider.use_system_proxy,
                    &active_url,
                    &api_key,
                    &active_request_channel,
                    &active_upstream_body,
                    &headers,
                    decision.provider.custom_user_agent.as_deref(),
                    skip_prefix_guard_for_sync_responses,
                    decision.provider.request_body_gzip_enabled,
                    !client_requested_stream,
                )
                .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        record_upstream_transport_failure(
                            &state,
                            &request_id,
                            &started,
                            &client_channel,
                            &active_request_channel,
                            &decision,
                            eligible.then_some(metrics_cache_key.as_str()),
                            provider_prefix_key.as_deref(),
                            provider_prefix_fingerprint.as_deref(),
                            &prefix_guard_wait,
                            local_prepare_ms,
                            &diagnostics,
                            &active_upstream_body,
                            active_used_response_session,
                            &response_session_reuse_diagnostics,
                            requested_model_for_log.clone(),
                            "previous-response-id-compat-transport",
                        )
                        .await;
                        state
                            .metrics
                            .record_error("upstream_transport", &err.to_string())
                            .await;
                        return json_error(
                            StatusCode::BAD_GATEWAY,
                            &format!("upstream request failed: {err}"),
                        );
                    }
                };
                current_send_diagnostics = send_outcome.diagnostics.clone();
                upstream_request_diagnostics.absorb(&current_send_diagnostics);
                upstream_response_headers_at_ms = started.elapsed().as_millis() as u64;
                status = send_outcome.response.status().as_u16();
                content_type = send_outcome
                    .response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("application/json")
                    .to_string();
                is_success_status = StatusCode::from_u16(status)
                    .map(|status| status.is_success())
                    .unwrap_or(false);
                body_read = match read_upstream_body_with_diagnostics(
                    send_outcome.response,
                    &content_type,
                    started,
                    upstream_response_headers_at_ms,
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        state
                            .metrics
                            .record_error("upstream_body", &err.to_string())
                            .await;
                        return json_error(
                            StatusCode::BAD_GATEWAY,
                            &format!("failed to read upstream body: {err}"),
                        );
                    }
                };
                bytes = body_read.bytes;
            }
        }
    }
    let bytes_for_client = if matches!(client_channel, Channel::Responses)
        && !client_requested_stream
        && is_text_event_stream(&content_type)
    {
        match active_request_channel {
            Channel::Chat => chat_sse_to_non_stream_json(&bytes, &decision.model),
            _ => responses_sse_to_non_stream_json(
                &bytes,
                &decision.model,
                non_sse_compact_compat_for_decision(&config, &decision),
            ),
        }
    } else {
        bytes.clone()
    };
    if is_success_status && json_body_has_error(&bytes_for_client) {
        let error_summary = upstream_error_summary(&bytes_for_client);
        state
            .metrics
            .record_error("upstream_sse_error", &error_summary)
            .await;
        status = StatusCode::BAD_GATEWAY.as_u16();
        is_success_status = false;
    }
    let key_error_summary = (!is_success_status).then(|| upstream_error_summary(&bytes_for_client));
    note_selected_provider_key_status(
        &state,
        &decision.provider.id,
        &selected_provider_key,
        status,
        key_error_summary.as_deref(),
    )
    .await;
    if is_success_status {
        note_provider_route_affinity(&state, route_affinity_key.as_deref(), &decision.provider.id)
            .await;
    } else {
        maybe_clear_provider_route_affinity_after_status(
            &state,
            route_affinity_key.as_deref(),
            &decision.provider.id,
            status,
            key_error_summary.as_deref(),
        )
        .await;
    }
    let sync_compact_diagnostic_only =
        skip_prefix_guard_for_sync_responses || active_responses_non_stream_chat_compat;
    let usage_observation = if is_success_status && sync_compact_diagnostic_only {
        let raw = collect_provider_usage_for_diagnostics(&bytes, &decision);
        if let Some(record) = raw.as_ref() {
            state.metrics.record_usage(record.clone()).await;
        }
        raw.map(|raw| ProviderUsageObservation {
            effective: raw.clone(),
            raw,
        })
    } else if is_success_status {
        collect_provider_usage(
            &state,
            &bytes,
            &decision,
            prefix_state_update_key.as_deref(),
            active_used_response_session,
        )
        .await
    } else {
        None
    };
    let usage_record = usage_observation.as_ref().map(|item| item.raw.clone());
    let prefix_usage_record = usage_observation
        .as_ref()
        .map(|item| item.effective.clone());
    if is_success_status && matches!(active_request_channel, Channel::Responses) {
        update_response_session(
            &state,
            response_session_key.as_deref(),
            response_session_scope_key.as_deref(),
            full_response_input.as_ref(),
            &bytes,
        )
        .await;
    }
    let gap_breakdown = provider_cache_gap_breakdown_with_guard(
        &state,
        prefix_state_update_key.as_deref(),
        provider_prefix_family_key.as_deref(),
        usage_record.as_ref(),
        Some(&tail_input_diagnostics),
        Some(&prefix_guard_wait),
    )
    .await;
    let prefix_lag = prefix_lag_diagnostics(
        &state,
        prefix_state_update_key.as_deref(),
        usage_record.as_ref(),
        gap_breakdown.as_ref(),
        &prefix_guard_wait,
        &tail_input_diagnostics,
    )
    .await;
    let session_cache_regressed = if is_success_status && !sync_compact_diagnostic_only {
        update_provider_prefix_state_with_tail_and_guard(
            &state,
            prefix_state_update_key.as_deref(),
            provider_prefix_family_key.as_deref(),
            prefix_usage_record.as_ref(),
            &tail_input_diagnostics,
            active_used_response_session,
            retried_full_response,
            prefix_guard_wait.budget_exhausted,
        )
        .await
    } else {
        false
    };
    if session_cache_regressed {
        let stale_response_id = previous_response_id_from_request(&active_upstream_body);
        clear_response_session_reference(
            &state,
            response_session_key.as_deref(),
            stale_response_id.as_deref(),
        )
        .await;
    }
    if !is_success_status {
        let error_summary = upstream_error_summary(&bytes);
        if should_cooldown_prefix_after_status(status)
            || (responses_main_retry_guard
                && should_cooldown_responses_sync_main_after_status(status))
        {
            note_prefix_error_cooldown(&state, prefix_state_update_key.as_deref()).await;
        }
        state
            .metrics
            .record_error(upstream_error_scope(status, &error_summary), &error_summary)
            .await;
    } else if !active_responses_non_stream_chat_compat {
        clear_prefix_error_cooldown(&state, prefix_state_update_key.as_deref()).await;
    }
    let mut response_content_type =
        if matches!(client_channel, Channel::Responses) && !client_requested_stream {
            "application/json".to_string()
        } else {
            content_type.clone()
        };
    let mut response_bytes = if matches!(client_channel, Channel::Responses)
        && matches!(active_request_channel, Channel::Chat)
        && codex_responses_chat_compat
    {
        serde_json::from_slice::<Value>(&bytes_for_client)
            .ok()
            .and_then(|value| {
                codex_chat_tool_context.as_ref().and_then(|tool_context| {
                    transform_codex_chat::chat_completion_to_response_with_context(
                        value,
                        tool_context,
                    )
                    .ok()
                })
            })
            .and_then(|value| serde_json::to_vec(&value).ok())
            .unwrap_or_else(|| {
                transform_response_bytes(
                    &client_channel,
                    &active_request_channel,
                    &decision.model,
                    &bytes_for_client,
                )
            })
    } else {
        transform_response_bytes(
            &client_channel,
            &active_request_channel,
            &decision.model,
            &bytes_for_client,
        )
    };
    if non_sse_compact_compat_for_decision(&config, &decision)
        && matches!(client_channel, Channel::Responses)
    {
        response_bytes = normalize_responses_json_for_client(&response_bytes);
    }
    if cross_protocol_stream {
        response_content_type = "text/event-stream".to_string();
        response_bytes = response_json_to_sse(&client_channel, &response_bytes);
    }
    let elapsed = started.elapsed().as_millis() as u64;
    let mut request_log = RequestLog {
        id: request_id.clone(),
        at: Utc::now(),
        inbound_request_id: Some(request_id.clone()),
        upstream_request_id: Some(Uuid::new_v4().to_string()),
        upstream_attempt_index: Some(1),
        upstream_attempt_total: Some(upstream_request_diagnostics.attempts),
        client_channel: client_channel.label().to_string(),
        upstream_channel: active_request_channel.label().to_string(),
        provider: decision.provider.name.clone(),
        model: decision.model.clone(),
        requested_model: requested_model_for_log.clone(),
        agent_reasoning_effort: None,
        configured_reasoning_effort: None,
        effective_reasoning_effort: None,
        reasoning_effort_source: None,
        cache_status: if is_success_status {
            if sync_compact_diagnostic_only {
                "compact"
            } else if eligible {
                "miss"
            } else {
                "bypass"
            }
        } else {
            "error"
        }
        .to_string(),
        agent_id: agent_log_id.clone(),
        agent_label: agent_log_label.clone(),
        upstream_call_kind: Some(
            if request_body_stream_flag(&active_upstream_body) {
                "stream"
            } else {
                "sync"
            }
            .to_string(),
        ),
        upstream_call_source: Some(
            if active_responses_non_stream_chat_compat {
                if responses_sync_main_chat_compat_fallback {
                    "responses-sync-main-chat-compat-fallback"
                } else {
                    "responses-sync-main-chat-compat"
                }
            } else if skip_prefix_guard_for_sync_responses {
                "responses-sync-main"
            } else {
                "main"
            }
            .to_string(),
        ),
        cache_key: (eligible && is_success_status && !sync_compact_diagnostic_only)
            .then(|| metrics_cache_key.clone()),
        provider_prefix_key: provider_prefix_key.clone(),
        provider_prefix_fingerprint: provider_prefix_fingerprint.clone(),
        provider_cache_diagnostic: usage_record.as_ref().map(provider_cache_diagnostic),
        prefix_guard_wait_ms: Some(prefix_guard_wait.wait_ms),
        prefix_guard_wait_reason: prefix_guard_wait.reason.clone(),
        prefix_guard_wait_source: prefix_guard_wait.source.clone(),
        prefix_guard_state_age_ms: prefix_guard_wait.state_age_ms,
        prefix_guard_skip_reason: prefix_guard_wait.skip_reason.clone(),
        prefix_guard_wait_effect: prefix_guard_wait_effect(
            &prefix_guard_wait,
            usage_record.as_ref(),
            gap_breakdown.as_ref(),
        ),
        prefix_lag_classification: None,
        prefix_lag_input_delta_tokens: None,
        prefix_lag_cache_delta_tokens: None,
        prefix_lag_previous_gap_tokens: None,
        prefix_cache_instability_score: prefix_guard_wait.cache_instability_score,
        prefix_seen_bucket_tokens: prefix_guard_wait.seen_bucket_tokens,
        prefix_state_cache_read_tokens: prefix_guard_wait.state_cache_read_tokens,
        status,
        ttft_ms: elapsed,
        upstream_ttft_ms: Some(upstream_ttft_ms(elapsed, Some(prefix_guard_wait.wait_ms))),
        local_prepare_ms: Some(local_prepare_ms),
        upstream_headers_ms: Some(upstream_request_diagnostics.headers_ms),
        upstream_last_attempt_headers_ms: Some(
            upstream_request_diagnostics.last_attempt_headers_ms,
        ),
        upstream_http_version: upstream_request_diagnostics.http_version.clone(),
        upstream_network_path: Some(upstream_request_diagnostics.network_path.to_string()),
        upstream_remote_addr: upstream_request_diagnostics.remote_addr.clone(),
        upstream_pool_diagnostic: upstream_request_diagnostics.pool_diagnostic.clone(),
        upstream_trace_id: upstream_request_diagnostics.upstream_trace_id.clone(),
        upstream_trace_source: upstream_request_diagnostics.upstream_trace_source.clone(),
        upstream_server_timing: upstream_request_diagnostics.server_timing.clone(),
        upstream_timing_source: upstream_request_diagnostics.timing_source.clone(),
        upstream_reported_processing_ms: upstream_request_diagnostics.reported_processing_ms,
        upstream_non_processing_ms: upstream_request_diagnostics.non_processing_ms,
        upstream_first_chunk_ms: body_read.first_chunk_ms,
        stream_upstream_wait_ms: None,
        stream_client_backpressure_ms: None,
        aggregate_done_ms: Some(body_read.aggregate_done_ms),
        upstream_retry_wait_ms: Some(upstream_request_diagnostics.retry_wait_ms),
        upstream_attempts: Some(upstream_request_diagnostics.attempts),
        request_body_bytes: Some(upstream_request_diagnostics.request_body_bytes),
        sent_body_bytes: Some(upstream_request_diagnostics.sent_body_bytes),
        request_body_encode_ms: Some(upstream_request_diagnostics.request_body_encode_ms),
        gzip_encode_ms: Some(upstream_request_diagnostics.gzip_encode_ms),
        gzip_attempted: Some(upstream_request_diagnostics.gzip_attempted),
        gzip_fallback_used: Some(upstream_request_diagnostics.gzip_fallback_used),
        upstream_header_wait_class: Some(upstream_header_wait_class(&upstream_request_diagnostics)),
        total_ms: elapsed,
        input_tokens: usage_record.as_ref().map(|record| record.input_tokens),
        output_tokens: usage_record.as_ref().map(|record| record.output_tokens),
        cache_read_tokens: usage_record.as_ref().map(|record| record.cache_read_tokens),
        cache_shortfall_tokens: usage_record.as_ref().map(provider_cache_shortfall),
        cache_new_tail_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.new_tail_tokens),
        cache_avoidable_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.avoidable_tokens),
        cache_provider_unstable_gap_tokens: gap_breakdown
            .as_ref()
            .map(|gap| gap.provider_unstable_tokens),
        provider_cache_token_ratio: usage_record.as_ref().and_then(provider_cache_ratio),
        tail_input_items: None,
        tail_message_chars: None,
        tail_tool_call_chars: None,
        tail_tool_output_chars: None,
        tail_largest_tool_output_chars: None,
        tail_tool_output_lines: None,
        tail_tool_output_repeated_line_chars: None,
        tail_tool_output_timestamp_like_count: None,
        tail_tool_output_path_like_count: None,
        tail_tool_output_url_like_count: None,
        tail_tool_output_hash_like_count: None,
        tail_tool_output_json_like_chars: None,
        tail_tool_output_noise_hint: None,
        tail_source: None,
        response_session_reused: Some(active_used_response_session),
        response_session_candidate_count: Some(response_session_reuse_diagnostics.candidate_count),
        response_session_skip_reason: response_session_reuse_diagnostics.skip_reason.clone(),
        response_session_exact_key_hit: Some(response_session_reuse_diagnostics.exact_key_hit),
        response_session_scope_match_count: Some(
            response_session_reuse_diagnostics.scope_match_count,
        ),
        response_session_append_delta_match: Some(
            response_session_reuse_diagnostics.append_delta_match,
        ),
        response_session_delta_items: Some(response_session_reuse_diagnostics.delta_items),
        response_session_cooldown_active: Some(response_session_reuse_diagnostics.cooldown_active),
        response_session_rejected_status: response_session_reuse_diagnostics.rejected_status,
        session_anchor_hash: None,
        session_anchor_source: None,
        session_anchor_changed: None,
        session_anchor_peer_count: None,
        original_body_bytes: None,
        send_body_bytes: None,
        send_body_is_delta: None,
        payload_too_large_rescue_attempted: None,
        payload_too_large_rescue_used: None,
        sse_end_reason: None,
        sse_completed_event_seen: None,
        sse_done_marker_seen: None,
        sse_chunks: body_read.sse_chunks,
    };
    apply_prefix_lag_diagnostics(&mut request_log, prefix_lag);
    apply_session_anchor_diagnostics(&mut request_log, &session_anchor_diagnostics);
    apply_body_diagnostics(&mut request_log, &diagnostics);
    apply_tail_input_diagnostics(&mut request_log, &tail_input_diagnostics);
    state
        .metrics
        .record_upstream_call(request_log.clone())
        .await;
    state.metrics.record_request(request_log, true).await;

    if eligible && is_success_status {
        insert_cache_entries(
            &state,
            cache_keys,
            semantic_text,
            semantic_shape,
            response_content_type.clone(),
            status,
            response_bytes.clone(),
            &decision,
            &config,
        )
        .await;
    }

    raw_response(status, &response_content_type, response_bytes)
}

async fn acquire_provider_prefix_guard(
    state: &AppState,
    channel: &Channel,
    provider_prefix_key: Option<&str>,
    _provider_prefix_family_key: Option<&str>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    if !matches!(channel, Channel::Responses | Channel::Chat) {
        return None;
    }
    let key = provider_prefix_key?;
    let lock = {
        let mut locks = state.prefix_locks.lock().await;
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    if matches!(channel, Channel::Responses) {
        return tokio::time::timeout(responses_foreground_wait_cap(), lock.lock_owned())
            .await
            .ok();
    }
    Some(lock.lock_owned().await)
}

async fn acquire_response_session_guard(
    state: &AppState,
    channel: &Channel,
    response_session_key: Option<&str>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    if !matches!(channel, Channel::Responses) {
        return None;
    }
    let key = response_session_key?;
    let lock = {
        let mut locks = state.prefix_locks.lock().await;
        locks
            .entry(format!("response-session:{key}"))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    Some(lock.lock_owned().await)
}

async fn wait_for_provider_prefix_settle(
    state: &AppState,
    channel: &Channel,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
    current_tail: &TailInputDiagnostics,
    max_wait: Option<TokioDuration>,
) -> PrefixGuardWaitDiagnostics {
    if provider_prefix_key.is_none() && provider_prefix_family_key.is_none() {
        return PrefixGuardWaitDiagnostics {
            skip_reason: Some("no_provider_prefix_key".to_string()),
            ..PrefixGuardWaitDiagnostics::default()
        };
    }
    let state_snapshot = {
        let states = state.prefix_states.lock().await;
        lookup_provider_prefix_state_with_source(
            &states,
            provider_prefix_key,
            provider_prefix_family_key,
        )
        .map(|(source, state)| (source.to_string(), state.clone()))
    };
    if let Some((source, state)) = state_snapshot {
        let state_age = state.finished_at.elapsed();
        let state_age_ms = state_age.as_millis() as u64;
        let cache_instability_score = state.cache_instability_score as u64;
        let seen_bucket_tokens = state.seen_bucket_tokens_128.max(state.seen_bucket_tokens);
        let state_cache_read_tokens = state.cache_read_tokens;

        if matches!(channel, Channel::Responses) {
            let avoidable_tokens = state
                .avoidable_shortfall_tokens_128
                .max(state.avoidable_shortfall_tokens);
            let policy = decide_responses_guard(ResponsesGuardInput {
                source_is_exact: source == "exact",
                avoidable_tokens,
                state_age,
                request_budget: max_wait.unwrap_or_else(responses_foreground_wait_cap),
            });
            if policy.wait.is_zero() {
                return PrefixGuardWaitDiagnostics {
                    skip_reason: policy.skip_reason.map(str::to_string),
                    budget_exhausted: policy.budget_exhausted,
                    source: Some(source),
                    state_age_ms: Some(state_age_ms),
                    cache_instability_score: Some(cache_instability_score),
                    seen_bucket_tokens: Some(seen_bucket_tokens),
                    state_cache_read_tokens: Some(state_cache_read_tokens),
                    ..PrefixGuardWaitDiagnostics::default()
                };
            }
            sleep(policy.wait).await;
            return PrefixGuardWaitDiagnostics {
                wait_ms: policy.wait.as_millis() as u64,
                reason: policy.reason.map(str::to_string),
                source: Some(source),
                state_age_ms: Some(state_age_ms),
                skip_reason: None,
                budget_exhausted: policy.budget_exhausted,
                cache_instability_score: Some(cache_instability_score),
                seen_bucket_tokens: Some(seen_bucket_tokens),
                state_cache_read_tokens: Some(state_cache_read_tokens),
            };
        }

        let wait = provider_prefix_wait_duration_for_channel(channel, &state, current_tail);
        let reason = provider_prefix_wait_reason_for_channel(channel, &state, current_tail);
        if let Some(wait) = wait {
            let requested_wait = wait;
            let wait = max_wait
                .map(|max_wait| requested_wait.min(max_wait))
                .unwrap_or(requested_wait);
            let budget_exhausted = max_wait
                .map(|max_wait| requested_wait >= max_wait)
                .unwrap_or(false);
            if wait.is_zero() {
                return PrefixGuardWaitDiagnostics {
                    skip_reason: Some(
                        max_wait
                            .map(|_| "local_guard_budget_exhausted")
                            .unwrap_or("wait_zero")
                            .to_string(),
                    ),
                    budget_exhausted,
                    source: Some(source),
                    state_age_ms: Some(state_age_ms),
                    cache_instability_score: Some(cache_instability_score),
                    seen_bucket_tokens: Some(seen_bucket_tokens),
                    state_cache_read_tokens: Some(state_cache_read_tokens),
                    ..PrefixGuardWaitDiagnostics::default()
                };
            }
            sleep(wait).await;
            return PrefixGuardWaitDiagnostics {
                wait_ms: wait.as_millis() as u64,
                reason: reason.or_else(|| Some("provider_prefix_settle".to_string())),
                source: Some(source),
                state_age_ms: Some(state_age_ms),
                skip_reason: None,
                budget_exhausted,
                cache_instability_score: Some(cache_instability_score),
                seen_bucket_tokens: Some(seen_bucket_tokens),
                state_cache_read_tokens: Some(state_cache_read_tokens),
            };
        }
        return PrefixGuardWaitDiagnostics {
            skip_reason: Some("settle_window_elapsed".to_string()),
            reason,
            source: Some(source),
            state_age_ms: Some(state_age_ms),
            cache_instability_score: Some(cache_instability_score),
            seen_bucket_tokens: Some(seen_bucket_tokens),
            state_cache_read_tokens: Some(state_cache_read_tokens),
            ..PrefixGuardWaitDiagnostics::default()
        };
    }
    PrefixGuardWaitDiagnostics {
        skip_reason: Some("no_prefix_state".to_string()),
        ..PrefixGuardWaitDiagnostics::default()
    }
}

async fn is_prefix_error_cooled_down(state: &AppState, provider_prefix_key: &str) -> bool {
    let mut cooldowns = state.prefix_error_cooldowns.lock().await;
    match cooldowns.get(provider_prefix_key).copied() {
        Some(until) if until > Instant::now() => true,
        Some(_) => {
            cooldowns.remove(provider_prefix_key);
            false
        }
        None => false,
    }
}

async fn note_prefix_error_cooldown(state: &AppState, provider_prefix_key: Option<&str>) {
    let Some(key) = provider_prefix_key else {
        return;
    };
    let until = Instant::now() + std::time::Duration::from_secs(PREFIX_ERROR_COOLDOWN_SECS);
    state
        .prefix_error_cooldowns
        .lock()
        .await
        .insert(key.to_string(), until);
}

async fn responses_sync_main_prefix_error_cooled_down(
    state: &AppState,
    skip_prefix_guard_for_sync_responses: bool,
    provider_prefix_control_key: Option<&str>,
) -> bool {
    if !skip_prefix_guard_for_sync_responses {
        return false;
    }
    let Some(prefix_key) = provider_prefix_state_update_key(provider_prefix_control_key) else {
        return false;
    };
    is_prefix_error_cooled_down(state, &prefix_key).await
}

async fn clear_prefix_error_cooldown(state: &AppState, provider_prefix_key: Option<&str>) {
    let Some(key) = provider_prefix_key else {
        return;
    };
    state.prefix_error_cooldowns.lock().await.remove(key);
}

fn compact_chat_compat_cooldown_key(decision: &RouteDecision) -> String {
    format!(
        "{}:{}:{}",
        decision.provider.id,
        decision.provider.base_url.trim_end_matches('/'),
        decision.model
    )
}

async fn compact_chat_compat_cooldown_active(state: &AppState, key: &str) -> bool {
    let mut cooldowns = state.compact_chat_compat_cooldowns.lock().await;
    match cooldowns.get(key).copied() {
        Some(until) if until > Instant::now() => true,
        Some(_) => {
            cooldowns.remove(key);
            false
        }
        None => false,
    }
}

async fn note_compact_chat_compat_cooldown(state: &AppState, key: &str) {
    state.compact_chat_compat_cooldowns.lock().await.insert(
        key.to_string(),
        Instant::now() + std::time::Duration::from_secs(COMPACT_CHAT_COMPAT_COOLDOWN_SECS),
    );
}

fn response_session_error_cooldown_key(decision: &RouteDecision) -> Option<String> {
    matches!(decision.upstream_channel, Channel::Responses).then(|| {
        format!(
            "{}:{}:{}",
            decision.upstream_channel.label(),
            decision.provider.id,
            decision.model
        )
    })
}

async fn response_session_cooldown_active(state: &AppState, key: Option<&str>) -> bool {
    response_session_cooldown_skip_reason(state, key)
        .await
        .is_some()
}

async fn response_session_cooldown_skip_reason(
    state: &AppState,
    key: Option<&str>,
) -> Option<String> {
    let Some(key) = key else {
        return None;
    };
    let cooldowns = state.response_session_error_cooldowns.lock().await;
    match cooldowns.get(key).cloned() {
        Some(cooldown) if cooldown.until > Instant::now() => {
            if cooldown.unsupported {
                Some("provider_session_delta_unsupported".to_string())
            } else {
                Some("provider_session_delta_cooldown".to_string())
            }
        }
        Some(_) => None,
        None => None,
    }
}

#[cfg(test)]
async fn note_response_session_error_cooldown(state: &AppState, key: Option<&str>) {
    note_response_session_error_cooldown_internal(state, key, false).await;
}

async fn note_response_session_error_cooldown_for_rejection(
    state: &AppState,
    key: Option<&str>,
    status: u16,
    summary: &str,
) {
    match response_session_rejection_classification(status, summary) {
        ResponseSessionRejectionClass::StaleReference => return,
        ResponseSessionRejectionClass::Unsupported => {
            note_response_session_error_cooldown_internal(state, key, true).await
        }
        ResponseSessionRejectionClass::TransientInvalid => {
            note_response_session_error_cooldown_internal(state, key, false).await
        }
    }
}

async fn note_response_session_error_cooldown_internal(
    state: &AppState,
    key: Option<&str>,
    unsupported: bool,
) {
    let Some(key) = key else {
        return;
    };
    let mut cooldowns = state.response_session_error_cooldowns.lock().await;
    let failures = cooldowns
        .get(key)
        .map(|cooldown| cooldown.failures.saturating_add(1))
        .unwrap_or(1);
    let seconds = response_session_error_cooldown_secs(failures, unsupported);
    cooldowns.insert(
        key.to_string(),
        ResponseSessionCooldownState {
            until: Instant::now() + std::time::Duration::from_secs(seconds),
            failures,
            unsupported,
        },
    );
    drop(cooldowns);
    if let Err(err) = state.persist_runtime_state().await {
        state
            .metrics
            .record_error("runtime_state_save", &err.to_string())
            .await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseSessionRejectionClass {
    StaleReference,
    Unsupported,
    TransientInvalid,
}

fn response_session_rejection_classification(
    status: u16,
    summary: &str,
) -> ResponseSessionRejectionClass {
    let summary = summary.to_ascii_lowercase();
    if summary.contains("previous_response_not_found")
        || (summary.contains("previous response") && summary.contains("not found"))
        || (summary.contains("response id") && summary.contains("not found"))
    {
        return ResponseSessionRejectionClass::StaleReference;
    }
    if summary.contains("unsupported parameter")
        || summary.contains("unknown parameter")
        || summary.contains("unrecognized parameter")
        || (summary.contains("previous_response_id")
            && (summary.contains("unsupported")
                || summary.contains("not supported")
                || summary.contains("only supported")
                || summary.contains("websocket")
                || summary.contains("invalid parameter")))
    {
        return ResponseSessionRejectionClass::Unsupported;
    }
    if matches!(status, 404 | 409 | 410) {
        ResponseSessionRejectionClass::StaleReference
    } else {
        ResponseSessionRejectionClass::TransientInvalid
    }
}

fn response_session_error_cooldown_secs(failures: u32, unsupported: bool) -> u64 {
    if unsupported {
        return RESPONSE_SESSION_UNSUPPORTED_COOLDOWN_SECS;
    }
    match failures {
        0 | 1 => RESPONSE_SESSION_ERROR_COOLDOWN_FIRST_SECS,
        2 => RESPONSE_SESSION_ERROR_COOLDOWN_SECOND_SECS,
        _ => RESPONSE_SESSION_ERROR_COOLDOWN_LONG_SECS,
    }
}

#[cfg(test)]
async fn is_prefix_prewarm_cooled_down(state: &AppState, provider_prefix_key: &str) -> bool {
    let mut cooldowns = state.prefix_prewarm_cooldowns.lock().await;
    match cooldowns.get(provider_prefix_key).copied() {
        Some(until) if until > Instant::now() => true,
        Some(_) => {
            cooldowns.remove(provider_prefix_key);
            false
        }
        None => false,
    }
}

#[cfg(test)]
async fn note_prefix_prewarm_cooldown(
    state: &AppState,
    provider_prefix_key: &str,
    duration: std::time::Duration,
) {
    let until = Instant::now() + duration;
    state
        .prefix_prewarm_cooldowns
        .lock()
        .await
        .insert(provider_prefix_key.to_string(), until);
}

fn should_cooldown_prefix_after_status(status: u16) -> bool {
    let _ = status;
    false
}

fn should_cooldown_responses_sync_main_after_status(status: u16) -> bool {
    matches!(status, 400 | 408 | 413 | 429 | 500..=599)
}

#[cfg(test)]
fn should_cooldown_prefix_after_prewarm_status(status: u16) -> bool {
    status == 429
}

fn provider_prefix_settle_delay(state: &PrefixWarmState) -> TokioDuration {
    if state.input_tokens == 0 {
        return TokioDuration::ZERO;
    }
    if state.seen_bucket_tokens_128 == 0
        && state.seen_bucket_tokens == 0
        && state.cache_read_tokens == 0
    {
        return TokioDuration::ZERO;
    }
    if state.cache_read_tokens == 0 {
        if state.input_tokens >= 128_000 {
            return TokioDuration::from_secs(22);
        }
        if state.input_tokens >= 32_000 {
            return TokioDuration::from_secs(12);
        }
        return TokioDuration::from_secs(4);
    }

    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if avoidable > 65_536 {
        return TokioDuration::from_secs(45);
    } else if avoidable > 32_768 {
        return TokioDuration::from_secs(32);
    } else if avoidable > 16_384 {
        return TokioDuration::from_secs(24);
    } else if avoidable > 4096 {
        return TokioDuration::from_secs(18);
    } else if avoidable > 1024 {
        return TokioDuration::from_secs(12);
    } else if avoidable > 512 {
        return TokioDuration::from_secs(3);
    } else if avoidable > 0 && state.avoidable_shortfall_streak >= 4 {
        return TokioDuration::from_millis(2400);
    } else if avoidable > 0 && state.avoidable_shortfall_streak >= 3 {
        return TokioDuration::from_millis(1800);
    } else if avoidable > 0 && state.avoidable_shortfall_streak >= 2 {
        return TokioDuration::from_millis(1500);
    } else if avoidable > 256 {
        return TokioDuration::from_millis(1500);
    } else if avoidable > 128 {
        return TokioDuration::from_millis(700);
    } else if avoidable > 0 {
        return TokioDuration::from_millis(350);
    }

    let observed_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    let base = if state.input_tokens >= 128_000 && observed_shortfall > 512 {
        TokioDuration::from_millis(1500)
    } else if state.input_tokens >= 128_000 && observed_shortfall > 0 {
        TokioDuration::from_millis(900)
    } else if observed_shortfall > 1024 {
        TokioDuration::from_millis(900)
    } else if observed_shortfall > 512 {
        TokioDuration::from_millis(450)
    } else if observed_shortfall > 256 {
        TokioDuration::from_millis(300)
    } else if observed_shortfall > 128 {
        TokioDuration::from_millis(200)
    } else if observed_shortfall > 0 {
        TokioDuration::from_millis(100)
    } else {
        TokioDuration::from_millis(150)
    };

    base
}

fn chat_provider_prefix_settle_delay(state: &PrefixWarmState) -> TokioDuration {
    let base = provider_prefix_settle_delay(state);
    if state.input_tokens < 32_000 || state.cache_read_tokens == 0 {
        return base;
    }

    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    let avoidable_floor = if avoidable > 65_536 {
        TokioDuration::from_secs(90)
    } else if avoidable > 32_768 {
        TokioDuration::from_secs(60)
    } else if avoidable > 16_384 {
        TokioDuration::from_secs(45)
    } else {
        TokioDuration::ZERO
    };

    let observed_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    let chat_floor = if observed_shortfall > 4096 {
        responses_foreground_wait_cap()
    } else if observed_shortfall > 2048 {
        responses_foreground_wait_cap()
    } else if observed_shortfall > 1024 {
        responses_foreground_wait_cap()
    } else if observed_shortfall > 512 {
        responses_foreground_wait_cap()
    } else if observed_shortfall > 0 {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::from_millis(250)
    };

    base.max(chat_floor)
        .max(avoidable_floor)
        .min(responses_foreground_wait_cap())
}

fn provider_prefix_wait_duration_for_channel(
    channel: &Channel,
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> Option<TokioDuration> {
    let delay = match channel {
        Channel::Chat => chat_provider_prefix_settle_delay(state),
        Channel::Responses => responses_provider_prefix_settle_delay_with_tail(state, current_tail),
        Channel::Anthropic => provider_prefix_settle_delay(state),
    };
    let elapsed = state.finished_at.elapsed();
    if let Some(wait) = delay.checked_sub(elapsed) {
        let wait = wait.max(responses_minimum_avoidable_request_wait(
            channel,
            state,
            current_tail,
        ));
        let wait = wait.max(responses_minimum_new_tail_request_wait(
            channel,
            state,
            current_tail,
        ));
        if matches!(channel, Channel::Responses) {
            let avoidable = state
                .avoidable_shortfall_tokens_128
                .max(state.avoidable_shortfall_tokens);
            return Some(wait.min(responses_avoidable_wait_cap_for_99(state, avoidable)));
        }
        return Some(wait);
    }
    let stale_wait =
        provider_prefix_stale_recovery_wait_for_channel(channel, state, current_tail, elapsed);
    if matches!(channel, Channel::Responses) {
        let avoidable = state
            .avoidable_shortfall_tokens_128
            .max(state.avoidable_shortfall_tokens);
        let minimum_new_tail_wait =
            responses_minimum_new_tail_request_wait(channel, state, current_tail);
        return match stale_wait {
            Some(wait) => Some(
                wait.max(minimum_new_tail_wait)
                    .min(responses_avoidable_wait_cap_for_99(state, avoidable)),
            ),
            None if !minimum_new_tail_wait.is_zero() => Some(
                minimum_new_tail_wait.min(responses_avoidable_wait_cap_for_99(state, avoidable)),
            ),
            None => None,
        };
    }
    stale_wait
}

fn responses_minimum_new_tail_request_wait(
    channel: &Channel,
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> TokioDuration {
    if !matches!(channel, Channel::Responses) {
        return TokioDuration::ZERO;
    }
    if state.cache_read_tokens == 0 || state.input_tokens < 16_000 {
        return TokioDuration::ZERO;
    }
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if avoidable > 0 {
        return TokioDuration::ZERO;
    }
    let observed_bucket_shortfall =
        provider_cache_bucket_max(state.input_tokens).saturating_sub(state.cache_read_tokens);
    if observed_bucket_shortfall == 0 {
        return TokioDuration::ZERO;
    }

    let small_bucket_tail =
        observed_bucket_shortfall <= 2048 && observed_bucket_shortfall % 128 == 0;
    let noisy_or_tool_tail = current_tail.message_chars > 0
        || current_tail.tool_output_chars >= 512
        || state.tail_tool_output_chars >= 512
        || current_tail.tool_output_noise_hint.is_some()
        || state.tail_tool_output_noise_hint.is_some();
    if small_bucket_tail && noisy_or_tool_tail {
        return responses_foreground_wait_cap();
    }
    if responses_medium_tool_tail_carryover_guard(state, current_tail, observed_bucket_shortfall) {
        return responses_foreground_wait_cap();
    }
    if responses_all_stage_tail_carryover_guard(state, current_tail, observed_bucket_shortfall) {
        return responses_foreground_wait_cap();
    }
    if small_bucket_tail
        && state.input_tokens >= 32_000
        && state.small_gap_recovery_streak > 0
        && state.finished_at.elapsed() < std::time::Duration::from_secs(5)
    {
        return responses_foreground_wait_cap();
    }

    TokioDuration::ZERO
}

fn responses_medium_tool_tail_carryover_guard(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    observed_bucket_shortfall: u64,
) -> bool {
    if observed_bucket_shortfall < 2048 || state.input_tokens < 16_000 {
        return false;
    }
    if state.finished_at.elapsed() > std::time::Duration::from_secs(10 * 60) {
        return false;
    }
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return false;
    }
    let current_tool_signal = current_tail.tool_output_chars >= 512
        || current_tail.largest_tool_output_chars >= 512
        || current_tail.tool_output_noise_hint.is_some();
    let previous_tool_signal = state.tail_tool_output_chars >= 2048
        || state.tail_largest_tool_output_chars >= 2048
        || state.tail_tool_output_noise_hint.is_some();

    current_tool_signal || previous_tool_signal
}

fn responses_all_stage_tail_carryover_guard(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    observed_bucket_shortfall: u64,
) -> bool {
    if observed_bucket_shortfall == 0 || state.input_tokens < 8_000 {
        return false;
    }
    if state.cache_read_tokens == 0 {
        return false;
    }
    if state.finished_at.elapsed() > std::time::Duration::from_secs(20 * 60) {
        return false;
    }
    let large_bucket_lag = observed_bucket_shortfall >= 4096;
    let huge_bucket_lag = observed_bucket_shortfall >= 8192;
    let huge_message_tail = huge_bucket_lag && current_tail.message_chars >= 32_000;
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) && current_tail.tool_output_chars == 0
        && !huge_message_tail
    {
        return false;
    }

    let current_tool_signal = current_tail.tool_output_chars >= 2048
        || current_tail.largest_tool_output_chars >= 2048
        || current_tail.tool_call_chars >= 2048
        || (current_tail.tool_output_noise_hint.is_some() && current_tail.tool_output_chars >= 512);
    let current_mixed_signal = matches!(current_tail.source.as_deref(), Some("mixed"))
        && (current_tail.tool_output_chars >= 1024
            || current_tail
                .message_chars
                .saturating_add(current_tail.tool_output_chars)
                >= 8192);
    let current_tail_signal = current_tool_signal || current_mixed_signal || huge_message_tail;
    let previous_tool_signal = state.tail_tool_output_chars >= 4096
        || state.tail_largest_tool_output_chars >= 4096
        || (state.tail_tool_output_noise_hint.is_some() && state.tail_tool_output_chars >= 2048);
    let unstable_large_lag = state.cache_instability_score >= 2 && large_bucket_lag;

    (huge_bucket_lag && (current_tail_signal || previous_tool_signal))
        || (large_bucket_lag
            && (current_tail_signal
                || (previous_tool_signal && current_tail.tool_output_chars > 0)))
        || unstable_large_lag
}

fn responses_minimum_avoidable_request_wait(
    channel: &Channel,
    state: &PrefixWarmState,
    _current_tail: &TailInputDiagnostics,
) -> TokioDuration {
    if !matches!(channel, Channel::Responses) {
        return TokioDuration::ZERO;
    }
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if avoidable == 0 {
        return TokioDuration::ZERO;
    }

    if state.input_tokens < 16_000 && avoidable <= 512 {
        return TokioDuration::from_secs(2);
    }
    responses_foreground_wait_cap()
}

fn provider_prefix_stale_recovery_wait_for_channel(
    channel: &Channel,
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> Option<TokioDuration> {
    if !matches!(channel, Channel::Responses) {
        return None;
    }
    responses_stale_prefix_recovery_wait(state, current_tail, elapsed)
}

fn responses_stale_prefix_recovery_wait(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> Option<TokioDuration> {
    if state.input_tokens < 4096 {
        return None;
    }
    if responses_long_idle_warm_prefix_guard(state, current_tail, elapsed) {
        return Some(responses_foreground_wait_cap());
    }
    if responses_idle_warm_tail_guard(state, current_tail, elapsed) {
        return Some(responses_foreground_wait_cap());
    }

    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    let observed_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    let seen_bucket = state.seen_bucket_tokens_128.max(state.seen_bucket_tokens);
    let severe_cold_read = state.cache_read_tokens == 0 && seen_bucket >= 32_000;
    let unstable_tail = state.cache_instability_score > 0
        && (avoidable > 0
            || observed_shortfall > 0
            || current_tail.tool_output_chars > 0
            || current_tail.message_chars > 0);
    if avoidable == 0 && responses_stale_current_tail_guard(state, current_tail, elapsed) {
        return Some(responses_foreground_wait_cap());
    }
    if elapsed > std::time::Duration::from_secs(20 * 60) {
        return None;
    }

    if state.input_tokens < 16_000 {
        let early_large_tool_tail = current_tail.tool_output_chars >= 8_000
            || current_tail.largest_tool_output_chars >= 8_000;
        if state.cache_read_tokens == 0 && observed_shortfall >= 1024 && early_large_tool_tail {
            return Some(responses_foreground_wait_cap());
        }
        if state.cache_read_tokens == 0 && observed_shortfall >= 1024 {
            return Some(responses_foreground_wait_cap());
        }
        if early_large_tool_tail && observed_shortfall >= 2048 {
            return Some(responses_foreground_wait_cap());
        }
        if avoidable >= 2048 {
            return Some(responses_foreground_wait_cap());
        }
        if avoidable >= 1024 {
            return Some(responses_foreground_wait_cap());
        }
        if avoidable > 0 && state.input_tokens >= 8192 {
            return Some(responses_foreground_wait_cap());
        }
        if avoidable > 0 || (state.small_gap_recovery_streak > 0 && observed_shortfall > 0) {
            return Some(TokioDuration::from_secs(2));
        }
        return None;
    }

    if !(severe_cold_read || avoidable > 0 || unstable_tail) {
        if responses_stale_medium_tool_tail_probe(state, current_tail, elapsed) {
            return Some(responses_foreground_wait_cap());
        }
        if responses_stale_large_tool_tail_catchup(state, current_tail, elapsed) {
            return Some(responses_foreground_wait_cap());
        }
        if responses_long_idle_large_tail_probe(state, current_tail, elapsed) {
            return Some(responses_foreground_wait_cap());
        }
        return None;
    }

    let large_current_tool_tail = current_tail.tool_output_chars >= 20_000
        || current_tail.largest_tool_output_chars >= 12_000;
    let medium_current_tool_tail =
        current_tail.tool_output_chars >= 8_000 || current_tail.largest_tool_output_chars >= 4_000;
    let stale_small_avoidable_risk = avoidable > 0
        && avoidable < 2048
        && state.input_tokens >= 64_000
        && (state.cache_instability_score >= 2
            || elapsed >= std::time::Duration::from_secs(10 * 60)
            || current_tail.message_chars >= 128
            || current_tail.tool_output_chars >= 512
            || current_tail.tool_call_chars >= 512);

    let wait = if severe_cold_read || avoidable >= 32_768 {
        responses_foreground_wait_cap()
    } else if avoidable > 0 && large_current_tool_tail {
        responses_foreground_wait_cap()
    } else if avoidable >= 4096 {
        responses_foreground_wait_cap()
    } else if avoidable >= 2048 {
        responses_foreground_wait_cap()
    } else if avoidable > 0 && medium_current_tool_tail {
        responses_foreground_wait_cap()
    } else if avoidable >= 1024 {
        responses_foreground_wait_cap()
    } else if stale_small_avoidable_risk {
        if elapsed >= std::time::Duration::from_secs(10 * 60) || state.cache_instability_score >= 4
        {
            responses_foreground_wait_cap()
        } else {
            responses_foreground_wait_cap()
        }
    } else if avoidable > 0 && state.input_tokens >= 64_000 {
        TokioDuration::from_secs(2)
    } else if avoidable > 0 {
        TokioDuration::from_secs(2)
    } else if observed_shortfall >= 2048 {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::from_secs(2)
    };

    Some(wait.min(responses_non_avoidable_wait_cap(state)))
}

fn responses_long_idle_warm_prefix_guard(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if elapsed < std::time::Duration::from_secs(20 * 60)
        || elapsed > std::time::Duration::from_secs(6 * 60 * 60)
    {
        return false;
    }
    if state.input_tokens < 64_000 || state.cache_read_tokens < 64_000 {
        return false;
    }
    if state.seen_bucket_tokens_128.max(state.seen_bucket_tokens) < 64_000 {
        return false;
    }
    if state.cache_read_tokens == 0 {
        return false;
    }
    let has_new_tail = current_tail.input_items > 0
        || current_tail.message_chars > 0
        || current_tail.tool_output_chars > 0
        || current_tail.tool_call_chars > 0;
    if !has_new_tail {
        return false;
    }
    let previous_gap = state.shortfall_tokens_128.max(state.shortfall_tokens);
    previous_gap > 0 || state.cache_instability_score > 0 || state.small_gap_recovery_streak > 0
}

fn responses_stale_current_tail_guard(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if state.input_tokens < 8_000 || state.cache_read_tokens == 0 {
        return false;
    }
    if elapsed > std::time::Duration::from_secs(6 * 60 * 60) {
        return false;
    }
    current_tail.message_chars >= 512
        || current_tail.tool_call_chars > 0
        || (current_tail.tool_output_chars > 0
            && (elapsed <= std::time::Duration::from_secs(10)
                || current_tail.tool_output_chars >= 512
                || current_tail.largest_tool_output_chars >= 512))
        || (current_tail.input_items >= 2 && current_tail.message_chars >= 128)
}

fn responses_idle_warm_tail_guard(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if elapsed < std::time::Duration::from_secs(3 * 60)
        || elapsed > std::time::Duration::from_secs(20 * 60)
    {
        return false;
    }
    if state.input_tokens < 64_000 || state.cache_read_tokens < 64_000 {
        return false;
    }
    let seen_bucket = state.seen_bucket_tokens_128.max(state.seen_bucket_tokens);
    if seen_bucket < 64_000 {
        return false;
    }
    let previous_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    if previous_shortfall > 1024 {
        return false;
    }
    let previous_ratio = state.cache_read_tokens as f64 / state.input_tokens.max(1) as f64;
    if previous_ratio < 0.98 {
        return false;
    }
    current_tail.message_chars >= 128
        || current_tail.tool_output_chars > 0
        || current_tail.tool_call_chars > 0
        || current_tail.input_items > 0
}

fn responses_stale_medium_tool_tail_probe(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if state.input_tokens < 32_000 || state.cache_read_tokens < 32_000 {
        return false;
    }
    if elapsed < std::time::Duration::from_secs(30)
        || elapsed > std::time::Duration::from_secs(12 * 60)
    {
        return false;
    }
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return false;
    }
    let medium_tail =
        current_tail.tool_output_chars >= 2_048 || current_tail.largest_tool_output_chars >= 2_048;
    let not_huge_tail =
        current_tail.tool_output_chars < 20_000 && current_tail.largest_tool_output_chars < 12_000;
    medium_tail && not_huge_tail
}

fn responses_stale_large_tool_tail_catchup(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if state.input_tokens < 8_000 || state.cache_read_tokens < 1024 {
        return false;
    }
    if elapsed < std::time::Duration::from_secs(5)
        || elapsed > std::time::Duration::from_secs(8 * 60)
    {
        return false;
    }
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return false;
    }
    current_tail.tool_output_chars >= 18_000 || current_tail.largest_tool_output_chars >= 10_000
}

fn responses_long_idle_large_tail_probe(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
    elapsed: std::time::Duration,
) -> bool {
    if state.input_tokens < 32_000 {
        return false;
    }
    if elapsed < std::time::Duration::from_secs(60) {
        return false;
    }
    if elapsed > std::time::Duration::from_secs(30 * 60) {
        return false;
    }
    if state.cache_read_tokens == 0 {
        return false;
    }
    if current_tail.tool_output_chars < 20_000 && current_tail.largest_tool_output_chars < 8_000 {
        return false;
    }
    !matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    )
}

fn responses_sync_main_skips_prefix_guard(
    client_channel: &Channel,
    upstream_channel: &Channel,
    client_requested_stream: bool,
) -> bool {
    matches!(client_channel, Channel::Responses)
        && matches!(upstream_channel, Channel::Responses)
        && !client_requested_stream
}

fn should_send_responses_non_stream_as_upstream_sse(
    client_channel: &Channel,
    upstream_channel: &Channel,
    client_requested_stream: bool,
) -> bool {
    responses_sync_main_skips_prefix_guard(
        client_channel,
        upstream_channel,
        client_requested_stream,
    )
}

fn should_fallback_responses_sync_main_to_chat_compat(
    status: u16,
    sync_main: bool,
    already_chat_compat: bool,
) -> bool {
    sync_main && !already_chat_compat && matches!(status, 400 | 401 | 405 | 500..=504 | 520 | 524)
}

fn should_fallback_chat_compat_compact_to_responses(
    status: u16,
    sync_main: bool,
    active_chat_compat: bool,
) -> bool {
    sync_main && active_chat_compat && matches!(status, 429 | 500 | 502 | 503 | 504 | 520 | 524)
}

fn apply_responses_non_stream_upstream_sse_compat(body: &mut Value, enabled: bool) {
    if enabled {
        set_stream_flag(body, true);
    }
}

fn should_route_responses_non_stream_compact_via_chat(
    client_channel: &Channel,
    upstream_channel: &Channel,
    client_requested_stream: bool,
    tail: &TailInputDiagnostics,
    body_bytes: u64,
) -> bool {
    if !responses_sync_main_skips_prefix_guard(
        client_channel,
        upstream_channel,
        client_requested_stream,
    ) {
        return false;
    }
    if body_bytes >= 900_000 {
        return tail.input_items >= 256
            || tail.tool_output_chars >= 200_000
            || tail.tool_call_chars >= 120_000
            || matches!(tail.source.as_deref(), Some("mixed"));
    }
    if body_bytes >= 262_144 && matches!(tail.source.as_deref(), Some("mixed")) {
        return true;
    }
    if matches!(tail.source.as_deref(), Some("message")) && tail.tool_output_chars == 0 {
        return body_bytes >= 12_288 && tail.input_items >= 3 && tail.message_chars >= 8_192;
    }

    body_bytes >= 12_288
        && (tail.input_items >= 3
            || tail.message_chars >= 8_192
            || tail.tool_output_chars >= 8_192
            || tail.tool_call_chars >= 8_192
            || matches!(tail.source.as_deref(), Some("mixed")))
}

fn should_route_responses_compact_via_chat_compat(
    upstream_channel: &Channel,
    tail: &TailInputDiagnostics,
    body_bytes: u64,
) -> bool {
    if !matches!(upstream_channel, Channel::Responses) {
        return false;
    }
    should_route_responses_non_stream_compact_via_chat(
        &Channel::Responses,
        upstream_channel,
        false,
        tail,
        body_bytes,
    ) || body_bytes >= 16_384
        || tail.input_items > 0
}

fn should_fallback_official_responses_compact_via_chat_compat(
    upstream_channel: &Channel,
    tail: &TailInputDiagnostics,
    body_bytes: u64,
) -> bool {
    if !matches!(upstream_channel, Channel::Responses) {
        return false;
    }
    should_route_responses_compact_via_chat_compat(upstream_channel, tail, body_bytes)
        || tail.input_items > 0
        || body_bytes >= 8_192
}

fn should_use_chat_non_stream_compact_fast_path(
    _tail: &TailInputDiagnostics,
    _chat_body_bytes: u64,
) -> bool {
    false
}

fn build_active_upstream_body_for_compat(
    upstream_body: &Value,
    upstream_send_body: &Value,
    config: &AppConfig,
    decision: &RouteDecision,
    active_request_channel: &Channel,
    responses_non_stream_upstream_sse_compat: bool,
) -> Value {
    let mut body = if matches!(active_request_channel, Channel::Chat)
        && matches!(decision.upstream_channel, Channel::Responses)
    {
        let mut chat_body = responses_to_chat_request(upstream_body);
        strip_chat_compat_compact_tool_fields(&mut chat_body);
        let mut chat_decision = decision.clone();
        chat_decision.upstream_channel = Channel::Chat;
        optimize_provider_prefix(&mut chat_body, config, &chat_decision);
        chat_body
    } else {
        upstream_send_body.clone()
    };
    if matches!(active_request_channel, Channel::Responses) {
        apply_responses_non_stream_upstream_sse_compat(
            &mut body,
            responses_non_stream_upstream_sse_compat,
        );
    } else if matches!(active_request_channel, Channel::Chat)
        && matches!(decision.upstream_channel, Channel::Responses)
    {
        set_stream_flag(&mut body, true);
    }
    body
}

fn strip_chat_compat_compact_tool_fields(body: &mut Value) {
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.remove("tools");
    object.remove("tool_choice");
    object.remove("parallel_tool_calls");
}

fn provider_prefix_wait_reason_for_channel(
    channel: &Channel,
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> Option<String> {
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    let observed_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    match channel {
        Channel::Responses => {
            if responses_long_idle_warm_prefix_guard(
                state,
                current_tail,
                state.finished_at.elapsed(),
            ) {
                Some("responses_long_idle_warm_prefix_guard".to_string())
            } else if responses_idle_warm_tail_guard(
                state,
                current_tail,
                state.finished_at.elapsed(),
            ) {
                Some("responses_idle_warm_tail_guard".to_string())
            } else if responses_current_tool_output_cap_applies(state, current_tail) {
                Some("responses_current_tool_output_tail_cap".to_string())
            } else if avoidable > 0 {
                Some("responses_avoidable_gap".to_string())
            } else if responses_cold_unstable_recent_warm_floor(state) > TokioDuration::ZERO {
                Some("responses_cold_unstable_recent_warm_guard".to_string())
            } else if state.tail_tool_output_chars >= 1024
                && state.tail_tool_output_noise_hint.is_some()
            {
                Some("responses_noisy_tool_output_tail_guard_1024".to_string())
            } else if state.tail_tool_output_chars >= 8_000 {
                Some("responses_large_tool_output_tail_guard".to_string())
            } else if responses_stale_medium_tool_tail_probe(
                state,
                current_tail,
                state.finished_at.elapsed(),
            ) {
                Some("responses_stale_medium_tool_tail_probe".to_string())
            } else if responses_medium_tool_tail_carryover_guard(
                state,
                current_tail,
                observed_shortfall,
            ) {
                Some("responses_medium_tool_tail_carryover_guard".to_string())
            } else if responses_all_stage_tail_carryover_guard(
                state,
                current_tail,
                observed_shortfall,
            ) {
                Some("responses_all_stage_tail_guard".to_string())
            } else if responses_current_tail_floor(state, current_tail) > TokioDuration::ZERO {
                Some("responses_current_tail_guard".to_string())
            } else if responses_stale_current_tail_guard(
                state,
                current_tail,
                state.finished_at.elapsed(),
            ) {
                Some("responses_stale_current_tail_guard".to_string())
            } else if observed_shortfall > 0 || state.small_gap_recovery_streak > 0 {
                Some("responses_small_tail_guard".to_string())
            } else {
                Some("provider_prefix_settle".to_string())
            }
        }
        Channel::Chat => {
            if avoidable > 0 {
                Some("chat_avoidable_gap".to_string())
            } else if observed_shortfall > 0 {
                Some("chat_new_tail_guard".to_string())
            } else {
                Some("provider_prefix_settle".to_string())
            }
        }
        Channel::Anthropic => {
            if avoidable > 0 {
                Some("anthropic_avoidable_gap".to_string())
            } else if observed_shortfall > 0 {
                Some("anthropic_new_tail_guard".to_string())
            } else {
                Some("provider_prefix_settle".to_string())
            }
        }
    }
}

#[cfg(test)]
fn responses_provider_prefix_settle_delay(state: &PrefixWarmState) -> TokioDuration {
    responses_provider_prefix_settle_delay_with_tail(state, &TailInputDiagnostics::default())
}

fn responses_foreground_wait_cap() -> TokioDuration {
    TokioDuration::from_secs(1)
}

fn prefix_guard_wait_budget_for_channel(
    channel: &Channel,
    elapsed_since_request_start: TokioDuration,
) -> Option<TokioDuration> {
    matches!(channel, Channel::Responses).then(|| {
        responses_foreground_wait_cap()
            .checked_sub(elapsed_since_request_start)
            .unwrap_or(TokioDuration::ZERO)
    })
}

fn cap_responses_foreground_wait(wait: TokioDuration) -> TokioDuration {
    wait.min(responses_foreground_wait_cap())
}

fn responses_base_prefix_settle_delay(state: &PrefixWarmState, avoidable: u64) -> TokioDuration {
    let base = provider_prefix_settle_delay(state);
    if avoidable == 0 {
        return cap_responses_foreground_wait(base);
    }

    let light_base = responses_avoidable_gap_floor(avoidable, state.avoidable_shortfall_streak);
    cap_responses_foreground_wait(base.min(light_base))
}

fn responses_avoidable_gap_floor(tokens: u64, streak: u32) -> TokioDuration {
    if tokens == 0 {
        return TokioDuration::ZERO;
    }

    let base_ms: u64 = if tokens > 90_000 {
        45_000
    } else if tokens > 65_536 {
        32_000
    } else if tokens > 32_768 {
        24_000
    } else if tokens > 16_384 {
        18_000
    } else if tokens > 8192 {
        12_000
    } else if tokens > 4096 {
        8_000
    } else if tokens >= 2048 {
        4_000
    } else if tokens >= 1024 {
        3_000
    } else if tokens >= 512 {
        2_000
    } else {
        500
    };
    let streak_bonus_ms: u64 = if streak >= 4 {
        2_000
    } else if streak >= 2 {
        1_000
    } else {
        0
    };

    cap_responses_foreground_wait(TokioDuration::from_millis(base_ms + streak_bonus_ms))
}

fn responses_bucket_tail_floor(tokens: u64, streak: u32, input_tokens: u64) -> TokioDuration {
    if tokens == 0 {
        return TokioDuration::ZERO;
    }

    let high_context = input_tokens >= 64_000;
    let mid_context = input_tokens >= 16_000;
    let aligned_512 = tokens >= 512 && tokens % 512 == 0;

    let base_secs = if tokens > 65_536 {
        180
    } else if tokens > 32_768 {
        150
    } else if tokens > 16_384 {
        135
    } else if tokens >= 8192 {
        105
    } else if tokens >= 4096 {
        90
    } else if tokens >= 2048 {
        if high_context {
            60
        } else if mid_context {
            45
        } else {
            30
        }
    } else if tokens >= 512 && aligned_512 {
        if high_context {
            45
        } else if mid_context {
            45
        } else {
            2
        }
    } else if tokens > 0 {
        if high_context {
            45
        } else if mid_context {
            30
        } else {
            2
        }
    } else {
        0
    };
    let streak_bonus_secs = if tokens >= 4096 && streak >= 4 {
        18
    } else if tokens >= 4096 && streak >= 2 {
        9
    } else if tokens >= 2048 && streak >= 4 {
        12
    } else if tokens >= 2048 && streak >= 2 {
        6
    } else if streak >= 1 && aligned_512 {
        0
    } else {
        0
    };

    cap_responses_foreground_wait(TokioDuration::from_secs(base_secs + streak_bonus_secs))
}

fn responses_instability_floor(state: &PrefixWarmState) -> TokioDuration {
    if state.cache_instability_score == 0 || state.input_tokens < 16_000 {
        return TokioDuration::ZERO;
    }
    let age = state.finished_at.elapsed();
    let score = state.cache_instability_score;
    let base_secs = if state.input_tokens >= 96_000 {
        180
    } else if state.input_tokens >= 64_000 {
        150
    } else if state.input_tokens >= 32_000 {
        120
    } else {
        75
    };
    let bonus_secs = if score >= 4 {
        60
    } else if score >= 2 {
        30
    } else {
        0
    };
    let floor = cap_responses_foreground_wait(TokioDuration::from_secs(base_secs + bonus_secs));
    floor.checked_sub(age).unwrap_or(TokioDuration::ZERO)
}

fn responses_cold_unstable_recent_warm_floor(state: &PrefixWarmState) -> TokioDuration {
    if state.cache_instability_score < 2 || state.input_tokens < 32_000 {
        return TokioDuration::ZERO;
    }
    if state.cache_read_tokens == 0 {
        return TokioDuration::ZERO;
    }
    let seen_bucket = state.seen_bucket_tokens_128.max(state.seen_bucket_tokens);
    if seen_bucket < 32_000 {
        return TokioDuration::ZERO;
    }
    if state.input_tokens >= 96_000 && state.cache_instability_score >= 4 {
        return responses_foreground_wait_cap();
    }
    let shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    if shortfall > 2048 {
        return TokioDuration::ZERO;
    }
    let age = state.finished_at.elapsed();
    let floor = responses_foreground_wait_cap();
    floor.checked_sub(age).unwrap_or(TokioDuration::ZERO)
}

fn responses_non_avoidable_wait_cap(state: &PrefixWarmState) -> TokioDuration {
    let _ = state;
    responses_foreground_wait_cap()
}

fn responses_avoidable_wait_cap_for_99(state: &PrefixWarmState, avoidable: u64) -> TokioDuration {
    if avoidable == 0 || state.input_tokens == 0 {
        return responses_non_avoidable_wait_cap(state);
    }

    let projected_cached = state.input_tokens.saturating_sub(avoidable);
    let projected_ratio = projected_cached as f64 / state.input_tokens.max(1) as f64;
    if projected_ratio >= 0.99 {
        return responses_non_avoidable_wait_cap(state);
    }

    responses_foreground_wait_cap()
}

fn responses_current_tail_floor(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> TokioDuration {
    if state.input_tokens < 16_000 {
        return TokioDuration::ZERO;
    }

    let mixed_message_tool_tail =
        current_tail.message_chars >= 1024 && current_tail.tool_output_chars > 0;

    if mixed_message_tool_tail {
        responses_foreground_wait_cap()
    } else if current_tail.tool_output_chars >= 20_000 {
        responses_foreground_wait_cap()
    } else if current_tail.tool_output_chars >= 8_000 {
        responses_foreground_wait_cap()
    } else if current_tail.tool_output_chars >= 4_000 {
        responses_foreground_wait_cap()
    } else if current_tail.tool_output_chars >= 1024 {
        responses_foreground_wait_cap()
    } else if current_tail.tool_output_chars > 0 {
        responses_foreground_wait_cap()
    } else if current_tail.message_chars >= 1024 {
        responses_foreground_wait_cap()
    } else if current_tail.message_chars > 0 {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::ZERO
    }
}

fn prefix_guard_wait_effect(
    wait: &PrefixGuardWaitDiagnostics,
    usage_record: Option<&UsageRecord>,
    gap_breakdown: Option<&ProviderCacheGapBreakdown>,
) -> Option<String> {
    if wait.wait_ms == 0 {
        let skip_reason = wait.skip_reason.as_deref()?;
        let suffix = wait
            .source
            .as_deref()
            .map(|source| format!(" source={source}"))
            .unwrap_or_default();
        return Some(format!("skip_reason={skip_reason}{suffix}"));
    }
    let record = usage_record?;
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    let gap = gap_breakdown
        .map(|gap| {
            gap.new_tail_tokens
                .max(gap.avoidable_tokens)
                .max(gap.provider_unstable_tokens)
        })
        .unwrap_or_else(|| provider_cache_shortfall(record));
    let class = if ratio >= 0.995 {
        "excellent"
    } else if ratio >= 0.99 {
        "good"
    } else if ratio >= 0.98 {
        "mixed"
    } else {
        "weak"
    };
    let cost = if wait.wait_ms >= 60_000 {
        "long"
    } else if wait.wait_ms >= 20_000 {
        "medium"
    } else {
        "short"
    };
    Some(format!(
        "{class}_{cost}_wait gap={gap} ratio={ratio:.4} wait_ms={}",
        wait.wait_ms
    ))
}

async fn prefix_lag_diagnostics(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    gap_breakdown: Option<&ProviderCacheGapBreakdown>,
    wait: &PrefixGuardWaitDiagnostics,
    current_tail: &TailInputDiagnostics,
) -> PrefixLagDiagnostics {
    let (Some(key), Some(record)) = (provider_prefix_key, usage_record) else {
        return PrefixLagDiagnostics::default();
    };
    let previous = {
        let states = state.prefix_states.lock().await;
        lookup_provider_prefix_state(&states, key).cloned()
    };
    let Some(previous) = previous else {
        if responses_huge_dynamic_history_cold_read(record, current_tail) {
            return PrefixLagDiagnostics {
                classification: Some("first_prefix_huge_dynamic_history".to_string()),
                ..PrefixLagDiagnostics::default()
            };
        }
        return PrefixLagDiagnostics {
            classification: Some("first_prefix_state".to_string()),
            ..PrefixLagDiagnostics::default()
        };
    };

    let input_delta = record.input_tokens.saturating_sub(previous.input_tokens);
    let cache_delta = record
        .cache_read_tokens
        .saturating_sub(previous.cache_read_tokens);
    let previous_gap = previous.shortfall_tokens_128.max(previous.shortfall_tokens);
    let current_gap = gap_breakdown
        .map(|gap| {
            gap.new_tail_tokens
                .max(gap.avoidable_tokens)
                .max(gap.provider_unstable_tokens)
        })
        .unwrap_or_else(|| provider_cache_shortfall(record));
    let current_avoidable = gap_breakdown
        .map(|gap| gap.avoidable_tokens)
        .unwrap_or_default();
    let current_new_tail = gap_breakdown
        .map(|gap| gap.new_tail_tokens)
        .unwrap_or_default();
    let current_provider_unstable = gap_breakdown
        .map(|gap| gap.provider_unstable_tokens)
        .unwrap_or_default();
    let ratio = provider_cache_ratio(record).unwrap_or_default();
    let real_provider_shortfall = provider_cache_shortfall(record);

    let previous_seen = provider_prefix_raw_seen_bucket(&previous);
    let classification = if record.cache_read_tokens == 0 && real_provider_shortfall >= 1024 {
        if previous_seen >= 32_000 {
            "cold_read_after_warm"
        } else {
            "cold_start"
        }
    } else if responses_tool_tail_burst(current_tail) && real_provider_shortfall >= 4096 {
        "tool_tail_burst_real_tail"
    } else if current_gap == 0 && real_provider_shortfall >= 4096 && ratio < 0.90 {
        "prefix_break_isolated"
    } else if current_gap == 0 {
        "full"
    } else if responses_tool_tail_burst(current_tail) && current_new_tail >= 1024 {
        "tool_tail_burst"
    } else if current_provider_unstable > 0 {
        "provider_waterline_rollback"
    } else if current_avoidable > 0 {
        "avoidable_gap"
    } else if previous_gap > 0
        && cache_delta >= previous_gap.saturating_sub(128)
        && current_new_tail > 0
    {
        "tail_lag_caught_previous_but_opened_new"
    } else if previous_gap > 0 && cache_delta < previous_gap && current_new_tail > 0 {
        "tail_lag_previous_not_caught"
    } else if wait.wait_ms >= 60_000 && ratio < 0.98 && current_new_tail >= 2048 {
        "tail_lag_long_wait_weak"
    } else if current_new_tail >= 2048 && input_delta >= 2048 {
        "new_tail_large_growth"
    } else if current_new_tail > 0 {
        "new_tail_small"
    } else {
        "none"
    };

    PrefixLagDiagnostics {
        classification: Some(classification.to_string()),
        input_delta_tokens: Some(input_delta),
        cache_delta_tokens: Some(cache_delta),
        previous_gap_tokens: Some(previous_gap),
    }
}

fn apply_prefix_lag_diagnostics(log: &mut RequestLog, diagnostics: PrefixLagDiagnostics) {
    log.prefix_lag_classification = diagnostics.classification;
    log.prefix_lag_input_delta_tokens = diagnostics.input_delta_tokens;
    log.prefix_lag_cache_delta_tokens = diagnostics.cache_delta_tokens;
    log.prefix_lag_previous_gap_tokens = diagnostics.previous_gap_tokens;
}

fn upstream_ttft_ms(ttft_ms: u64, prefix_guard_wait_ms: Option<u64>) -> u64 {
    ttft_ms.saturating_sub(prefix_guard_wait_ms.unwrap_or_default())
}

fn upstream_request_body_bytes(channel: &Channel, body: &Value) -> Vec<u8> {
    if matches!(channel, Channel::Responses) {
        serialize_responses_body_for_provider_prefix(body).into_bytes()
    } else {
        serde_json::to_vec(body).unwrap_or_else(|_| b"null".to_vec())
    }
}

fn should_skip_request_body_gzip_for_cold_stream(
    channel: &Channel,
    client_requested_stream: bool,
    body: &Value,
) -> bool {
    if !client_requested_stream || !matches!(channel, Channel::Responses) {
        return false;
    }
    if upstream_request_body_bytes(channel, body).len() < REQUEST_BODY_GZIP_MIN_BYTES {
        return false;
    }
    false
}

fn gzip_request_body(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes)?;
    encoder.finish()
}

fn should_retry_without_gzip(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::BAD_REQUEST
            | reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
            | reqwest::StatusCode::NOT_IMPLEMENTED
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
    )
}

fn request_body_gzip_scope_key(url: &str, channel: &Channel) -> String {
    let channel = match channel {
        Channel::Responses => "responses",
        Channel::Chat => "chat",
        Channel::Anthropic => "anthropic",
    };
    let provider_scope = reqwest::Url::parse(url)
        .ok()
        .and_then(|url| {
            let scheme = url.scheme().to_string();
            let host = url.host_str()?.to_ascii_lowercase();
            let port = url
                .port()
                .map(|port| format!(":{port}"))
                .unwrap_or_default();
            Some(format!("{scheme}://{host}{port}"))
        })
        .unwrap_or_else(|| url.to_string());
    format!("{channel}\0{provider_scope}")
}

fn request_body_gzip_cooldown_key(url: &str, channel: &Channel) -> String {
    format!("gzip\0{}", request_body_gzip_scope_key(url, channel))
}

async fn request_body_gzip_cooldown_active(state: &AppState, key: &str) -> bool {
    let mut cooldowns = state.request_body_gzip_cooldowns.lock().await;
    match cooldowns.get(key).copied() {
        Some(until) if until > Instant::now() => true,
        Some(_) => {
            cooldowns.remove(key);
            false
        }
        None => false,
    }
}

async fn note_request_body_gzip_fallback(state: &AppState, key: &str) {
    state.request_body_gzip_cooldowns.lock().await.insert(
        key.to_string(),
        Instant::now() + TokioDuration::from_secs(REQUEST_BODY_GZIP_FALLBACK_COOLDOWN_SECS),
    );
}

fn request_body_gzip_threshold_bytes(channel: &Channel) -> usize {
    if matches!(channel, Channel::Responses) {
        REQUEST_BODY_GZIP_WARM_MIN_BYTES
    } else {
        REQUEST_BODY_GZIP_MIN_BYTES
    }
}

fn upstream_header_wait_class(diagnostics: &UpstreamRequestDiagnostics) -> String {
    let class = upstream_header_wait_base_class(diagnostics);
    let network_path = if diagnostics.network_path.is_empty() {
        "direct"
    } else {
        diagnostics.network_path
    };
    format!("{network_path}:{class}")
}

fn upstream_header_wait_base_class(diagnostics: &UpstreamRequestDiagnostics) -> &'static str {
    if diagnostics.gzip_skipped_cold_stream {
        return "cold_stream_gzip_skipped";
    }
    if diagnostics.attempts > 1 || diagnostics.retry_wait_ms > 0 {
        return "retry_header_wait";
    }
    if diagnostics.headers_ms >= 20_000 && diagnostics.request_body_bytes >= 1_000_000 {
        return "huge_body_header_wait";
    }
    if diagnostics.headers_ms >= 20_000 && diagnostics.request_body_bytes >= 600_000 {
        return "large_body_header_wait";
    }
    if diagnostics.headers_ms >= 20_000 {
        return "header_wait_slow";
    }
    if diagnostics.headers_ms >= 3_000
        && diagnostics.request_body_bytes >= 1_000_000
        && diagnostics.sent_body_bytes >= 600_000
    {
        return "huge_body_upload_header_wait";
    }
    if diagnostics.headers_ms >= 3_000
        && diagnostics.request_body_bytes >= 600_000
        && diagnostics.sent_body_bytes >= 300_000
    {
        return "large_body_upload_header_wait";
    }
    if diagnostics.request_body_bytes >= 1_000_000 {
        return "huge_body_normal_header";
    }
    if diagnostics.request_body_bytes >= 600_000 {
        return "large_body_normal_header";
    }
    "normal"
}

fn observe_upstream_response_timing(
    diagnostics: &mut UpstreamRequestDiagnostics,
    response: &reqwest::Response,
    headers_ms: u64,
) {
    diagnostics.last_attempt_headers_ms = headers_ms;
    let http_version = format!("{:?}", response.version());
    diagnostics.pool_diagnostic = Some(
        if http_version == "HTTP/2.0" {
            "shared-client:http2-multiplex-capable"
        } else {
            "shared-client:pool-enabled-hit-not-exposed"
        }
        .to_string(),
    );
    diagnostics.http_version = Some(http_version);
    diagnostics.remote_addr = response.remote_addr().map(|address| address.to_string());
    if let Some((source, trace_id)) = upstream_response_trace_id(response.headers()) {
        diagnostics.upstream_trace_source = Some(source.to_string());
        diagnostics.upstream_trace_id = Some(trace_id);
    } else {
        diagnostics.upstream_trace_source = None;
        diagnostics.upstream_trace_id = None;
    }
    diagnostics.server_timing = response_header_values(response.headers(), "server-timing");
    if let Some((source, processing_ms)) = reported_upstream_processing_ms(response.headers()) {
        diagnostics.timing_source = Some(source.to_string());
        diagnostics.reported_processing_ms = Some(processing_ms);
        diagnostics.non_processing_ms = Some(headers_ms.saturating_sub(processing_ms));
    } else {
        diagnostics.timing_source = None;
        diagnostics.reported_processing_ms = None;
        diagnostics.non_processing_ms = None;
    }
}

fn upstream_response_trace_id(headers: &HeaderMap) -> Option<(&'static str, String)> {
    for name in [
        "x-request-id",
        "request-id",
        "x-trace-id",
        "traceparent",
        "cf-ray",
        "x-amzn-trace-id",
    ] {
        let Some(value) = headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        return Some((name, value.chars().take(256).collect()));
    }
    None
}

fn response_header_values(headers: &HeaderMap, name: &str) -> Option<String> {
    let values = headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values.join(", ").chars().take(512).collect())
    }
}

fn reported_upstream_processing_ms(headers: &HeaderMap) -> Option<(&'static str, u64)> {
    if let Some(value) = header_duration_ms(headers, "x-envoy-upstream-service-time", 1.0) {
        return Some(("x-envoy-upstream-service-time", value));
    }
    if let Some(value) = server_timing_duration_ms(headers) {
        return Some(("server-timing", value));
    }
    if let Some(value) = header_duration_ms(headers, "x-response-time", 1.0) {
        return Some(("x-response-time", value));
    }
    header_duration_ms(headers, "x-process-time", 1000.0).map(|value| ("x-process-time", value))
}

fn header_duration_ms(headers: &HeaderMap, name: &str, bare_scale: f64) -> Option<u64> {
    let raw = headers
        .get(name)?
        .to_str()
        .ok()?
        .trim()
        .to_ascii_lowercase();
    parse_duration_ms(&raw, bare_scale)
}

fn parse_duration_ms(raw: &str, bare_scale: f64) -> Option<u64> {
    let (number, scale) = if let Some(value) = raw.strip_suffix("ms") {
        (value.trim(), 1.0)
    } else if let Some(value) = raw.strip_suffix('s') {
        (value.trim(), 1000.0)
    } else {
        (raw.trim(), bare_scale)
    };
    let value = number.parse::<f64>().ok()? * scale;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some(value.round() as u64)
}

fn server_timing_duration_ms(headers: &HeaderMap) -> Option<u64> {
    headers
        .get_all("server-timing")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .flat_map(|metric| metric.split(';').skip(1))
        .filter_map(|parameter| {
            let (name, value) = parameter.trim().split_once('=')?;
            name.trim()
                .eq_ignore_ascii_case("dur")
                .then(|| parse_duration_ms(value.trim().trim_matches('"'), 1.0))
                .flatten()
        })
        .max()
}

fn responses_provider_prefix_settle_delay_with_tail(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> TokioDuration {
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if responses_stable_prefix_needs_no_guard(state, avoidable) {
        return TokioDuration::ZERO;
    }
    let base = responses_base_prefix_settle_delay(state, avoidable);
    if state.input_tokens < 4096 {
        return base;
    }

    let avoidable_floor = if avoidable > 0 {
        responses_avoidable_gap_floor(avoidable, state.avoidable_shortfall_streak).max(
            responses_minimum_avoidable_request_wait(&Channel::Responses, state, current_tail),
        )
    } else {
        TokioDuration::ZERO
    };

    if state.cache_read_tokens == 0 && avoidable == 0 {
        return base
            .max(responses_instability_floor(state))
            .min(responses_non_avoidable_wait_cap(state));
    }

    let observed_shortfall = state.shortfall_tokens_128.max(state.shortfall_tokens);
    let bucket_tail_floor = if avoidable == 0 {
        responses_bucket_tail_floor(
            observed_shortfall,
            state.small_gap_recovery_streak,
            state.input_tokens,
        )
    } else {
        TokioDuration::ZERO
    };
    let noisy_tool_tail_floor = if avoidable == 0
        && state.tail_tool_output_chars >= 1024
        && state.tail_tool_output_noise_hint.is_some()
    {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::ZERO
    };
    let tool_tail_floor = if state.tail_tool_output_chars >= 40_000 {
        responses_foreground_wait_cap()
    } else if state.tail_tool_output_chars >= 20_000 {
        responses_foreground_wait_cap()
    } else if state.tail_tool_output_chars >= 8_000 {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::ZERO
    };
    let current_tail_floor = if avoidable == 0 {
        responses_current_tail_floor(state, current_tail)
    } else {
        TokioDuration::ZERO
    };
    let cold_unstable_recent_warm_floor = if avoidable == 0 {
        responses_cold_unstable_recent_warm_floor(state)
    } else {
        TokioDuration::ZERO
    };
    let large_tail_floor = if avoidable == 0 && observed_shortfall > 65_536 {
        responses_foreground_wait_cap()
    } else if avoidable == 0 && observed_shortfall > 32_768 {
        responses_foreground_wait_cap()
    } else if avoidable == 0 && observed_shortfall > 16_384 {
        responses_foreground_wait_cap()
    } else {
        TokioDuration::ZERO
    };

    let delay = base
        .max(responses_instability_floor(state))
        .max(avoidable_floor)
        .max(bucket_tail_floor)
        .max(noisy_tool_tail_floor)
        .max(tool_tail_floor)
        .max(current_tail_floor)
        .max(cold_unstable_recent_warm_floor)
        .max(large_tail_floor);

    if avoidable == 0 {
        let capped_delay = delay.min(responses_non_avoidable_wait_cap(state));
        if let Some(cap) = responses_current_tool_output_wait_cap(current_tail) {
            return capped_delay.min(cap);
        }
        return capped_delay;
    } else {
        let cap = responses_avoidable_wait_cap_for_99(state, avoidable);
        if let Some(tool_cap) = responses_avoidable_tool_output_wait_cap(state, current_tail) {
            return delay.min(cap).min(tool_cap);
        }
        return delay.min(cap);
    }
}

fn responses_stable_prefix_needs_no_guard(state: &PrefixWarmState, avoidable: u64) -> bool {
    if avoidable > 0
        || state.input_tokens < 4096
        || state.cache_read_tokens == 0
        || state.cache_instability_score > 0
    {
        return false;
    }
    state.cache_read_tokens as f64 / state.input_tokens as f64 >= 0.995
}

fn responses_avoidable_tool_output_wait_cap(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> Option<TokioDuration> {
    if state.cache_instability_score < 2 {
        return None;
    }
    if current_tail.tool_output_chars < 4_000 {
        return None;
    }
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return None;
    }
    if current_tail.tool_output_chars >= 20_000 {
        Some(responses_foreground_wait_cap())
    } else if current_tail.tool_output_chars >= 8_000 {
        Some(responses_foreground_wait_cap())
    } else {
        Some(responses_foreground_wait_cap())
    }
}

fn responses_current_tool_output_wait_cap(
    current_tail: &TailInputDiagnostics,
) -> Option<TokioDuration> {
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return None;
    }
    if current_tail.tool_output_chars >= 20_000 || current_tail.largest_tool_output_chars >= 12_000
    {
        Some(responses_foreground_wait_cap())
    } else if current_tail.delta_from_session && current_tail.tool_output_chars >= 8_000 {
        Some(responses_foreground_wait_cap())
    } else {
        None
    }
}

fn responses_current_tool_output_cap_applies(
    state: &PrefixWarmState,
    current_tail: &TailInputDiagnostics,
) -> bool {
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if avoidable > 0 {
        return false;
    }
    responses_current_tool_output_wait_cap(current_tail).is_some()
}

fn responses_current_tail_makes_avoidable_unreliable(current_tail: &TailInputDiagnostics) -> bool {
    let tool_or_mixed_tail = matches!(
        current_tail.source.as_deref(),
        Some("mixed") | Some("tool_output")
    );
    if tool_or_mixed_tail
        && (current_tail.tool_output_chars >= 20_000
            || current_tail.largest_tool_output_chars >= 20_000)
    {
        return true;
    }
    if tool_or_mixed_tail
        && current_tail.tool_output_chars >= 20_000
        && current_tail.tool_output_noise_hint.is_some()
    {
        return true;
    }
    if current_tail.input_items >= 32
        && current_tail.tool_output_chars >= 80_000
        && (matches!(
            current_tail.source.as_deref(),
            Some("mixed") | Some("tool_output")
        ) || current_tail.tool_output_noise_hint.is_some()
            || current_tail.message_chars >= 8_000)
    {
        return true;
    }
    if current_tail.input_items >= 128
        && current_tail.tool_output_chars >= 100_000
        && current_tail.message_chars >= 8_000
        && current_tail.tool_call_chars >= 8_000
    {
        return true;
    }
    false
}

fn responses_current_tail_blocks_sent_bucket_learning(current_tail: &TailInputDiagnostics) -> bool {
    let tool_or_mixed_tail = matches!(
        current_tail.source.as_deref(),
        Some("mixed") | Some("tool_output")
    );
    if !tool_or_mixed_tail {
        return false;
    }
    if current_tail.tool_output_chars >= 80_000 || current_tail.largest_tool_output_chars >= 32_000
    {
        return true;
    }
    if current_tail.input_items >= 32
        && current_tail.tool_output_chars >= 64_000
        && (current_tail.tool_output_noise_hint.is_some() || current_tail.message_chars >= 8_000)
    {
        return true;
    }
    if current_tail.input_items >= 128
        && current_tail.tool_output_chars >= 80_000
        && current_tail.message_chars >= 8_000
        && current_tail.tool_call_chars >= 8_000
    {
        return true;
    }
    false
}

#[cfg(test)]
async fn update_provider_prefix_state(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    used_response_session: bool,
    retried_full_response: bool,
) -> bool {
    update_provider_prefix_state_with_tail(
        state,
        provider_prefix_key,
        None,
        usage_record,
        &TailInputDiagnostics::default(),
        used_response_session,
        retried_full_response,
    )
    .await
}

#[cfg(test)]
async fn update_provider_prefix_state_with_tail(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    tail_input_diagnostics: &TailInputDiagnostics,
    used_response_session: bool,
    retried_full_response: bool,
) -> bool {
    update_provider_prefix_state_with_tail_and_guard(
        state,
        provider_prefix_key,
        provider_prefix_family_key,
        usage_record,
        tail_input_diagnostics,
        used_response_session,
        retried_full_response,
        false,
    )
    .await
}

async fn update_provider_prefix_state_with_tail_and_guard(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    tail_input_diagnostics: &TailInputDiagnostics,
    used_response_session: bool,
    retried_full_response: bool,
    guard_budget_exhausted: bool,
) -> bool {
    let (Some(key), Some(record)) = (provider_prefix_key, usage_record) else {
        return false;
    };
    if record.input_tokens == 0 {
        return false;
    }
    let now = Instant::now();
    let bucket_max = provider_cache_bucket_max(record.input_tokens);
    let bucket_max_128 = provider_cache_bucket_max_128(record.input_tokens);
    let mut states = state.prefix_states.lock().await;
    let previous = lookup_provider_prefix_state(&states, key).cloned();
    let prefix_break_after_warm_state =
        provider_prefix_break_after_warm_state(previous.as_ref(), record)
            && (record.input_tokens >= 32_000 || responses_tool_tail_burst(tail_input_diagnostics));
    let huge_dynamic_history_cold_read =
        responses_huge_dynamic_history_cold_read(record, tail_input_diagnostics);
    let weak_full_retry_after_session_delta = provider_prefix_weak_full_retry_after_session_delta(
        previous.as_ref(),
        record,
        retried_full_response,
    );
    if prefix_break_after_warm_state
        || weak_full_retry_after_session_delta
        || huge_dynamic_history_cold_read
    {
        let Some(mut preserved) = previous.clone() else {
            return false;
        };
        preserved.finished_at = now;
        let instability_bump = if prefix_break_after_warm_state || huge_dynamic_history_cold_read {
            2
        } else {
            1
        };
        preserved.cache_instability_score = preserved
            .cache_instability_score
            .saturating_add(instability_bump)
            .min(8);
        preserved.shortfall_tokens = provider_cache_shortfall(record);
        preserved.shortfall_tokens_128 = provider_cache_shortfall_128(record);
        preserved.avoidable_shortfall_tokens = 0;
        preserved.avoidable_shortfall_tokens_128 = 0;
        preserved.avoidable_shortfall_streak = 0;
        preserved.small_gap_recovery_streak = 0;
        preserved.tail_tool_output_chars = tail_input_diagnostics.tool_output_chars;
        preserved.tail_largest_tool_output_chars = tail_input_diagnostics.largest_tool_output_chars;
        preserved.tail_tool_output_noise_hint =
            tail_input_diagnostics.tool_output_noise_hint.clone();
        states.insert(key.to_string(), preserved.clone());
        if let Some(alias_key) = provider_prefix_state_alias_key(key) {
            states.insert(alias_key, preserved);
        }
        drop(states);
        if let Err(err) = state.persist_runtime_state().await {
            state
                .metrics
                .record_error("runtime_state_save", &err.to_string())
                .await;
        }
        return false;
    }
    if !provider_prefix_usage_is_safe_to_learn(
        previous.as_ref(),
        record,
        used_response_session,
        retried_full_response,
    ) {
        return false;
    }
    let (previous_seen_bucket, previous_seen_bucket_128) = previous
        .as_ref()
        .map(|state| provider_prefix_calibrated_previous_seen_buckets(state, record))
        .unwrap_or((0, 0));
    let expected_cached_bucket = previous_seen_bucket.min(bucket_max);
    let direct_avoidable_shortfall_tokens =
        expected_cached_bucket.saturating_sub(record.cache_read_tokens);
    let expected_cached_bucket_128 = previous_seen_bucket_128.min(bucket_max_128);
    let direct_avoidable_shortfall_tokens_128 =
        expected_cached_bucket_128.saturating_sub(record.cache_read_tokens);
    let shortfall_tokens = provider_cache_shortfall(record);
    let shortfall_tokens_128 = provider_cache_shortfall_128(record);
    let avoidable_unreliable =
        responses_current_tail_makes_avoidable_unreliable(tail_input_diagnostics);
    let small_avoidable_is_tail_granularity = responses_small_avoidable_tail_granularity(
        previous.as_ref(),
        record,
        shortfall_tokens_128,
        direct_avoidable_shortfall_tokens_128,
        tail_input_diagnostics,
    );
    let capped_provider_rollback = responses_cap_exhausted_provider_waterline_rollback(
        previous.as_ref(),
        record,
        shortfall_tokens_128,
        direct_avoidable_shortfall_tokens_128,
        tail_input_diagnostics,
        guard_budget_exhausted,
    );
    let avoidable_shortfall_tokens = if capped_provider_rollback
        || small_avoidable_is_tail_granularity
        || avoidable_unreliable
    {
        0
    } else {
        direct_avoidable_shortfall_tokens
    };
    let avoidable_shortfall_tokens_128 = if capped_provider_rollback
        || small_avoidable_is_tail_granularity
        || avoidable_unreliable
    {
        0
    } else {
        direct_avoidable_shortfall_tokens_128
    };
    let avoidable_shortfall_streak =
        if avoidable_shortfall_tokens > 0 || avoidable_shortfall_tokens_128 > 0 {
            previous
                .as_ref()
                .map(|state| state.avoidable_shortfall_streak.saturating_add(1))
                .unwrap_or(1)
        } else {
            0
        };
    let small_gap_signal = shortfall_tokens_128 > 0 && shortfall_tokens_128 <= 2048;
    let small_gap_recovery_streak = if small_gap_signal {
        previous
            .as_ref()
            .map(|state| state.small_gap_recovery_streak.saturating_add(1))
            .unwrap_or(1)
    } else if previous
        .as_ref()
        .map(|state| state.small_gap_recovery_streak > 0 && shortfall_tokens_128 == 0)
        .unwrap_or(false)
    {
        1
    } else {
        0
    };
    let previous_instability = previous
        .as_ref()
        .map(|state| state.cache_instability_score)
        .unwrap_or(0);
    let large_avoidable = avoidable_shortfall_tokens.max(avoidable_shortfall_tokens_128);
    let severe_cold_read = record.cache_read_tokens == 0 && bucket_max >= 32_000;
    let cache_instability_score = if severe_cold_read {
        previous_instability.saturating_add(3).min(8)
    } else if capped_provider_rollback {
        previous_instability.saturating_add(1).min(8)
    } else if large_avoidable >= 4096 {
        previous_instability.saturating_add(2).min(8)
    } else if large_avoidable > 0 {
        previous_instability.saturating_add(1).min(8)
    } else if shortfall_tokens_128 <= 512 && record.cache_read_tokens >= 1024 {
        previous_instability.saturating_sub(1)
    } else {
        previous_instability
    };
    let current_shortfall = provider_cache_shortfall(record);
    let current_shortfall_128 = provider_cache_shortfall_128(record);
    let learn_sent_bucket = !capped_provider_rollback
        && !small_avoidable_is_tail_granularity
        && should_learn_sent_provider_bucket(
            previous.as_ref(),
            record,
            current_shortfall,
            current_shortfall_128,
            tail_input_diagnostics,
            used_response_session,
        );
    let sent_bucket_tokens = if learn_sent_bucket {
        provider_cache_bucket_max(record.input_tokens)
    } else {
        record.cache_read_tokens
    };
    let sent_bucket_tokens_128 = if learn_sent_bucket {
        provider_cache_bucket_max_128(record.input_tokens)
    } else {
        record.cache_read_tokens
    };
    let seen_bucket_tokens = if capped_provider_rollback || small_avoidable_is_tail_granularity {
        record.cache_read_tokens.max(sent_bucket_tokens)
    } else {
        previous_seen_bucket
            .max(record.cache_read_tokens)
            .max(sent_bucket_tokens)
    };
    let seen_bucket_tokens_128 = if capped_provider_rollback || small_avoidable_is_tail_granularity
    {
        record.cache_read_tokens.max(sent_bucket_tokens_128)
    } else {
        previous_seen_bucket_128
            .max(record.cache_read_tokens)
            .max(sent_bucket_tokens_128)
    };
    let next = PrefixWarmState {
        finished_at: now,
        input_tokens: record.input_tokens,
        cache_read_tokens: record.cache_read_tokens,
        shortfall_tokens,
        seen_bucket_tokens,
        avoidable_shortfall_tokens,
        avoidable_shortfall_streak,
        shortfall_tokens_128,
        seen_bucket_tokens_128,
        avoidable_shortfall_tokens_128,
        small_gap_recovery_streak,
        cache_instability_score,
        tail_tool_output_chars: tail_input_diagnostics.tool_output_chars,
        tail_largest_tool_output_chars: tail_input_diagnostics.largest_tool_output_chars,
        tail_tool_output_noise_hint: tail_input_diagnostics.tool_output_noise_hint.clone(),
    };
    states.insert(key.to_string(), next.clone());
    if let Some(alias_key) = provider_prefix_state_alias_key(key) {
        states.insert(alias_key, next.clone());
    }
    if let Some(family_key) = provider_prefix_family_key {
        if !capped_provider_rollback
            && should_learn_provider_prefix_family_state(record, tail_input_diagnostics)
        {
            states.insert(family_key.to_string(), next);
        }
    }
    drop(states);
    if let Err(err) = state.persist_runtime_state().await {
        state
            .metrics
            .record_error("runtime_state_save", &err.to_string())
            .await;
    }
    false
}

fn should_learn_sent_provider_bucket(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
    used_response_session: bool,
) -> bool {
    if used_response_session || record.cache_read_tokens == 0 {
        return false;
    }

    let Some(previous) = previous else {
        return false;
    };
    if previous.cache_read_tokens == 0 || previous.seen_bucket_tokens_128 == 0 {
        return false;
    }
    if responses_high_hit_fine_tail_can_learn_sent_bucket(
        previous,
        record,
        current_shortfall,
        current_shortfall_128,
        tail_input_diagnostics,
    ) {
        return true;
    }
    if responses_precise_tool_tail_can_learn_sent_bucket(
        previous,
        record,
        current_shortfall,
        current_shortfall_128,
        tail_input_diagnostics,
    ) {
        return true;
    }
    if responses_small_residual_after_tool_burst_can_learn_sent_bucket(
        previous,
        record,
        current_shortfall,
        current_shortfall_128,
        tail_input_diagnostics,
    ) {
        return true;
    }
    if responses_medium_message_tail_can_learn_sent_bucket(
        previous,
        record,
        current_shortfall,
        current_shortfall_128,
        tail_input_diagnostics,
    ) {
        return true;
    }
    if responses_all_stage_tail_can_learn_sent_bucket(
        previous,
        record,
        current_shortfall,
        current_shortfall_128,
        tail_input_diagnostics,
    ) {
        return true;
    }
    if current_shortfall_128 <= 2048 {
        return false;
    }
    if !(2560..=4096).contains(&current_shortfall)
        || !responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128)
    {
        return false;
    }
    if current_shortfall_128 > current_shortfall.saturating_add(512) {
        return false;
    }
    if record.input_tokens < 32_000 {
        return false;
    }
    if provider_cache_ratio(record).unwrap_or(0.0) < 0.98 {
        return false;
    }
    if responses_current_tail_blocks_sent_bucket_learning(tail_input_diagnostics) {
        return false;
    }
    if tail_input_diagnostics.tool_output_chars >= 40_000 {
        return false;
    }
    true
}

fn responses_tail_gap_is_cache_granular(
    current_shortfall: u64,
    current_shortfall_128: u64,
) -> bool {
    (current_shortfall >= 512 && current_shortfall % 512 == 0)
        || (current_shortfall_128 >= 128 && current_shortfall_128 % 128 == 0)
}

fn responses_tail_gap_128_is_close_to_512_floor(
    current_shortfall: u64,
    current_shortfall_128: u64,
) -> bool {
    current_shortfall_128 <= current_shortfall.saturating_add(512)
}

fn responses_high_hit_fine_tail_can_learn_sent_bucket(
    previous: &PrefixWarmState,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if current_shortfall_128 == 0 || current_shortfall_128 > 2048 {
        return false;
    }
    if !responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128)
        || !responses_tail_gap_128_is_close_to_512_floor(current_shortfall, current_shortfall_128)
    {
        return false;
    }
    if record.input_tokens < 32_000 || record.cache_read_tokens == 0 {
        return false;
    }
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens);
    if previous_seen < 32_000 || record.cache_read_tokens.saturating_add(512) < previous_seen {
        return false;
    }
    if responses_current_tail_blocks_sent_bucket_learning(tail_input_diagnostics) {
        return false;
    }
    let tail_signal = tail_input_diagnostics.message_chars > 0
        || tail_input_diagnostics.tool_output_chars > 0
        || tail_input_diagnostics.largest_tool_output_chars > 0
        || tail_input_diagnostics.tool_call_chars > 0
        || tail_input_diagnostics.tool_output_noise_hint.is_some()
        || previous.small_gap_recovery_streak > 0;
    if !tail_signal {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    if current_shortfall_128 <= 512 {
        ratio >= 0.995
    } else if current_shortfall_128 <= 1024 {
        ratio >= 0.992
    } else {
        ratio >= 0.988
    }
}

fn responses_small_residual_after_tool_burst_can_learn_sent_bucket(
    previous: &PrefixWarmState,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    let previous_tool_burst = previous.tail_tool_output_chars >= 8_000
        || previous.tail_largest_tool_output_chars >= 8_000
        || (previous.tail_tool_output_chars >= 4_096
            && previous.tail_tool_output_noise_hint.is_some());
    if !previous_tool_burst {
        return false;
    }
    if current_shortfall < 128
        || current_shortfall > 4096
        || !responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128)
    {
        return false;
    }
    if !responses_tail_gap_128_is_close_to_512_floor(current_shortfall, current_shortfall_128) {
        return false;
    }
    if record.input_tokens < 16_000 || record.cache_read_tokens == 0 {
        return false;
    }
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens);
    if previous_seen < 8_000 || record.cache_read_tokens.saturating_add(512) < previous_seen {
        return false;
    }
    if responses_current_tail_blocks_sent_bucket_learning(tail_input_diagnostics)
        || responses_tool_tail_burst(tail_input_diagnostics)
    {
        return false;
    }
    let light_current_tail = matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("message") | Some("mixed") | Some("tool_output") | None
    ) && tail_input_diagnostics.tool_output_chars < 4_096
        && tail_input_diagnostics.largest_tool_output_chars < 4_096
        && tail_input_diagnostics.tool_call_chars < 4_096;
    if !light_current_tail {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    if current_shortfall <= 1024 {
        ratio >= 0.985
    } else if current_shortfall <= 2048 {
        ratio >= 0.965
    } else {
        ratio >= 0.955
    }
}
fn responses_medium_message_tail_can_learn_sent_bucket(
    previous: &PrefixWarmState,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if current_shortfall < 128 || current_shortfall > 8192 {
        return false;
    }
    if !responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128)
        || !responses_tail_gap_128_is_close_to_512_floor(current_shortfall, current_shortfall_128)
    {
        return false;
    }
    if record.input_tokens < 16_000 || record.cache_read_tokens == 0 {
        return false;
    }
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens);
    if previous_seen < 8_000 || record.cache_read_tokens.saturating_add(512) < previous_seen {
        return false;
    }
    let message_like_tail = matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("message") | Some("mixed")
    ) && tail_input_diagnostics.message_chars >= 512;
    if !message_like_tail {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    if current_shortfall <= 1024 {
        ratio >= 0.975
    } else if current_shortfall <= 2048 {
        ratio >= 0.955
    } else if current_shortfall <= 4096 {
        ratio >= 0.93
    } else {
        ratio >= 0.90
    }
}

fn responses_all_stage_tail_can_learn_sent_bucket(
    previous: &PrefixWarmState,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if current_shortfall < 128 {
        return false;
    }
    if !responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128)
        || !responses_tail_gap_128_is_close_to_512_floor(current_shortfall, current_shortfall_128)
    {
        return false;
    }
    if record.input_tokens < 16_000 || record.cache_read_tokens == 0 {
        return false;
    }
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens);
    if previous_seen < 8_000 {
        return false;
    }
    if record.cache_read_tokens.saturating_add(512) < previous_seen {
        return false;
    }
    let has_large_tail = tail_input_diagnostics.message_chars >= 8_192
        || tail_input_diagnostics.tool_output_chars >= 8_192
        || tail_input_diagnostics.largest_tool_output_chars >= 8_192
        || tail_input_diagnostics.tool_call_chars >= 8_192;
    if !has_large_tail {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    if current_shortfall > 131_072 {
        ratio >= 0.90
    } else if current_shortfall > 65_536 {
        ratio >= 0.86
    } else if current_shortfall > 32_768 {
        ratio >= 0.80
    } else if matches!(tail_input_diagnostics.source.as_deref(), Some("message")) {
        ratio >= 0.72
    } else if responses_tool_tail_burst(tail_input_diagnostics) {
        ratio >= 0.72
    } else {
        ratio >= 0.90
    }
}

fn responses_precise_tool_tail_can_learn_sent_bucket(
    previous: &PrefixWarmState,
    record: &UsageRecord,
    current_shortfall: u64,
    current_shortfall_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return false;
    }
    if tail_input_diagnostics.tool_output_chars < 512
        && tail_input_diagnostics.largest_tool_output_chars < 512
    {
        return false;
    }
    let aligned_tail_gap =
        responses_tail_gap_is_cache_granular(current_shortfall, current_shortfall_128);
    if current_shortfall < 128 || current_shortfall > 12_288 || !aligned_tail_gap {
        return false;
    }
    if current_shortfall_128 > current_shortfall.saturating_add(512) {
        return false;
    }
    if record.input_tokens < 32_000 || previous.seen_bucket_tokens_128 < 32_000 {
        return false;
    }
    if responses_current_tail_blocks_sent_bucket_learning(tail_input_diagnostics) {
        return false;
    }
    if tail_input_diagnostics.tool_output_chars >= 40_000
        || tail_input_diagnostics.largest_tool_output_chars >= 20_000
    {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    let required_ratio =
        responses_precise_tool_tail_required_ratio(current_shortfall, tail_input_diagnostics);
    ratio >= required_ratio
}

fn responses_precise_tool_tail_required_ratio(
    current_shortfall: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> f64 {
    let aligned_512 = current_shortfall >= 512 && current_shortfall % 512 == 0;
    let aligned_128_small = current_shortfall < 2048 && current_shortfall % 128 == 0;
    let tool_only = matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("tool_output")
    );
    let compact_tool_tail = tail_input_diagnostics.tool_output_chars <= 16_384
        && tail_input_diagnostics.largest_tool_output_chars <= 14_336;
    if aligned_128_small && tool_only && compact_tool_tail {
        0.99
    } else if aligned_512 && tool_only && compact_tool_tail {
        if current_shortfall <= 2048 {
            0.965
        } else if current_shortfall <= 4096 {
            0.955
        } else if current_shortfall <= 8192 {
            0.92
        } else if current_shortfall <= 12_288 {
            // A compact 512-aligned tool tail at ~9k-12k is still useful as a
            // sent-bucket waterline: it does not claim the provider hit is good,
            // it only lets the next same-prefix turn guard the gap instead of
            // repeatedly classifying it as a fresh tail.
            0.90
        } else {
            0.97
        }
    } else if current_shortfall <= 2048 {
        0.97
    } else if current_shortfall <= 4096 {
        0.95
    } else {
        0.92
    }
}
fn responses_small_avoidable_tail_granularity(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    current_shortfall_128: u64,
    direct_avoidable_shortfall_tokens_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if direct_avoidable_shortfall_tokens_128 == 0
        || direct_avoidable_shortfall_tokens_128 > 512
        || current_shortfall_128 > 1024
    {
        return false;
    }
    if record.input_tokens < 32_000 || record.cache_read_tokens < 32_000 {
        return false;
    }
    if provider_cache_ratio(record).unwrap_or_default() < 0.99 {
        return false;
    }
    if responses_tool_tail_burst(tail_input_diagnostics)
        || tail_input_diagnostics.tool_output_chars >= 1024
        || tail_input_diagnostics.tool_call_chars >= 1024
    {
        return false;
    }
    previous.avoidable_shortfall_streak >= 2
        || previous.small_gap_recovery_streak >= 3
        || previous.cache_instability_score >= 3
}

fn responses_cap_exhausted_provider_waterline_rollback(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    current_shortfall_128: u64,
    direct_avoidable_shortfall_tokens_128: u64,
    tail_input_diagnostics: &TailInputDiagnostics,
    guard_budget_exhausted: bool,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if !guard_budget_exhausted
        || direct_avoidable_shortfall_tokens_128 == 0
        || direct_avoidable_shortfall_tokens_128 > 4096
        || current_shortfall_128 == 0
        || current_shortfall_128 > 4096
    {
        return false;
    }
    if direct_avoidable_shortfall_tokens_128 % 128 != 0 || current_shortfall_128 % 128 != 0 {
        return false;
    }
    if record.input_tokens < 32_000 || record.cache_read_tokens < 32_000 {
        return false;
    }
    if provider_cache_ratio(record).unwrap_or_default() < 0.96 {
        return false;
    }
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens)
        .max(previous.cache_read_tokens);
    if previous_seen < 32_000
        || record.cache_read_tokens >= previous_seen
        || record.cache_read_tokens.saturating_add(4096) < previous_seen
        || record.input_tokens.saturating_sub(previous.input_tokens) > 8192
    {
        return false;
    }
    if responses_tool_tail_burst(tail_input_diagnostics)
        || tail_input_diagnostics.tool_output_chars >= 4096
        || tail_input_diagnostics.tool_call_chars >= 4096
    {
        return false;
    }
    true
}

fn should_learn_provider_prefix_family_state(
    record: &UsageRecord,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if record.input_tokens < 1024 || record.cache_read_tokens == 0 {
        return false;
    }
    if responses_family_can_learn_compact_tool_tail(record, tail_input_diagnostics) {
        return true;
    }
    if responses_tool_tail_burst(tail_input_diagnostics) {
        return false;
    }
    provider_cache_ratio(record).unwrap_or(0.0) >= 0.90
}

fn responses_family_can_learn_compact_tool_tail(
    record: &UsageRecord,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if !matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("tool_output")
    ) {
        return false;
    }
    if tail_input_diagnostics.tool_output_chars == 0
        || tail_input_diagnostics.tool_output_chars > 12_288
        || tail_input_diagnostics.largest_tool_output_chars > 12_288
    {
        return false;
    }
    let shortfall = provider_cache_shortfall(record);
    if shortfall == 0 || shortfall > 4096 || shortfall % 512 != 0 {
        return false;
    }
    provider_cache_ratio(record).unwrap_or(0.0) >= 0.975
}

fn provider_prefix_calibrated_previous_seen_buckets(
    previous: &PrefixWarmState,
    record: &UsageRecord,
) -> (u64, u64) {
    let bucket = provider_cache_bucket_max(record.input_tokens);
    let bucket_128 = provider_cache_bucket_max_128(record.input_tokens);
    let previous_seen = previous.seen_bucket_tokens.max(previous.cache_read_tokens);
    let previous_seen_128 = previous
        .seen_bucket_tokens_128
        .max(previous.cache_read_tokens);
    let stale_high_waterline = previous_seen > bucket.saturating_add(16_384)
        || previous_seen_128 > bucket_128.saturating_add(16_384);

    if stale_high_waterline {
        return (record.cache_read_tokens, record.cache_read_tokens);
    }

    (previous_seen, previous_seen_128)
}

fn provider_prefix_control_key(
    provider_prefix_key: Option<&str>,
    decision: &RouteDecision,
    channel: &Channel,
) -> Option<String> {
    let provider_group = provider_prefix_provider_group(decision);
    provider_prefix_key.map(|key| {
        format!(
            "{}\0{}\0{}\0{}",
            provider_group,
            provider_prefix_model_key(decision),
            channel.label(),
            key
        )
    })
}

fn provider_prefix_family_control_key(
    response_session_scope_key: Option<&str>,
    decision: &RouteDecision,
    channel: &Channel,
) -> Option<String> {
    if !matches!(channel, Channel::Responses) {
        return None;
    }
    let provider_group = provider_prefix_provider_group(decision);
    response_session_scope_key.map(|scope| {
        format!(
            "prefix-family\0{}\0{}\0{}\0{}",
            provider_group,
            provider_prefix_model_key(decision),
            channel.label(),
            scope
        )
    })
}

fn provider_prefix_state_alias_key(control_key: &str) -> Option<String> {
    let mut parts = control_key.split('\0');
    let provider_group = parts.next()?;
    let model = parts.next()?;
    let channel = parts.next()?;
    let fingerprint = parts.next()?;
    if parts.next().is_some()
        || provider_group.is_empty()
        || model.is_empty()
        || channel.is_empty()
        || fingerprint.is_empty()
    {
        return None;
    }
    Some(format!(
        "prefix-alias\0{}\0{}\0{}\0{}",
        provider_group, model, channel, fingerprint
    ))
}

fn provider_prefix_provider_group(decision: &RouteDecision) -> String {
    let base_url = decision
        .provider
        .base_url
        .trim()
        .trim_end_matches('/')
        .to_ascii_lowercase();
    if base_url.is_empty() {
        decision.provider.id.clone()
    } else {
        base_url
    }
}

fn provider_prefix_model_key(decision: &RouteDecision) -> String {
    provider_model_cache_key(&decision.provider, &decision.model)
}

fn lookup_provider_prefix_state<'a>(
    states: &'a HashMap<String, PrefixWarmState>,
    key: &str,
) -> Option<&'a PrefixWarmState> {
    let exact = states.get(key);
    let alias = provider_prefix_state_alias_key(key).and_then(|alias| states.get(&alias));
    stronger_prefix_state(exact, alias)
}

fn lookup_provider_prefix_state_with_source<'a>(
    states: &'a HashMap<String, PrefixWarmState>,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
) -> Option<(&'static str, &'a PrefixWarmState)> {
    let direct = provider_prefix_key.and_then(|key| {
        let exact = states.get(key).map(|state| ("exact", state));
        let alias = provider_prefix_state_alias_key(key)
            .and_then(|alias| states.get(&alias))
            .map(|state| ("shared-responses", state));
        stronger_prefix_state_with_source(exact, alias)
    });
    stronger_prefix_state_with_source(
        direct,
        provider_prefix_family_key
            .and_then(|key| states.get(key).map(|state| ("session-sibling", state))),
    )
}

fn provider_prefix_state_update_key(provider_prefix_key: Option<&str>) -> Option<String> {
    provider_prefix_key.map(ToOwned::to_owned)
}

fn stronger_prefix_state<'a>(
    left: Option<&'a PrefixWarmState>,
    right: Option<&'a PrefixWarmState>,
) -> Option<&'a PrefixWarmState> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if prefix_state_strength(right) > prefix_state_strength(left) {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(state), None) | (None, Some(state)) => Some(state),
        (None, None) => None,
    }
}

fn stronger_prefix_state_with_source<'a>(
    left: Option<(&'static str, &'a PrefixWarmState)>,
    right: Option<(&'static str, &'a PrefixWarmState)>,
) -> Option<(&'static str, &'a PrefixWarmState)> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if prefix_state_strength(right.1) > prefix_state_strength(left.1) {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(state), None) | (None, Some(state)) => Some(state),
        (None, None) => None,
    }
}

fn prefix_state_strength(state: &PrefixWarmState) -> u64 {
    provider_prefix_raw_seen_bucket(state)
        .max(state.seen_bucket_tokens_128)
        .max(state.cache_read_tokens)
}

fn provider_prefix_usage_is_safe_to_learn(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    used_response_session: bool,
    retried_full_response: bool,
) -> bool {
    if !used_response_session || retried_full_response {
        return true;
    }
    let Some(previous) = previous else {
        return false;
    };
    response_session_usage_looks_like_full_context(previous, record)
}

fn response_session_usage_looks_like_full_context(
    previous: &PrefixWarmState,
    record: &UsageRecord,
) -> bool {
    let current_bucket = provider_cache_bucket_max(record.input_tokens);
    if current_bucket < 1024 || record.cache_read_tokens == 0 {
        return false;
    }

    let previous_floor = previous
        .seen_bucket_tokens
        .max(previous.cache_read_tokens)
        .max(provider_cache_bucket_max(previous.input_tokens));
    if previous_floor < 1024 {
        return false;
    }

    // Real Responses session reuse can still report full-context usage. Learn
    // from those records so the prefix waterline stays current, but reject tiny
    // delta-only usage because it would poison the full-prefix state.
    current_bucket.saturating_mul(4) >= previous_floor.saturating_mul(3)
}

fn provider_prefix_raw_seen_bucket(state: &PrefixWarmState) -> u64 {
    state.seen_bucket_tokens.max(state.cache_read_tokens)
}

fn provider_prefix_break_after_warm_state(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let previous_seen = provider_prefix_raw_seen_bucket(previous);
    if previous_seen < 32_000 {
        return false;
    }
    let current_bucket = provider_cache_bucket_max(record.input_tokens);
    if current_bucket < 32_000 {
        return false;
    }
    if record.cache_read_tokens == 0 {
        return true;
    }
    let ratio = provider_cache_ratio(record).unwrap_or_default();
    ratio < 0.50 && record.cache_read_tokens.saturating_mul(2) < previous_seen
}

fn provider_prefix_weak_full_retry_after_session_delta(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    retried_full_response: bool,
) -> bool {
    if !retried_full_response {
        return false;
    }
    let Some(previous) = previous else {
        return false;
    };
    if previous.cache_read_tokens < 32_000
        || record.input_tokens < 32_000
        || record.cache_read_tokens == 0
    {
        return false;
    }
    if record.cache_read_tokens.saturating_add(4096) >= previous.cache_read_tokens {
        return false;
    }
    let current_bucket = provider_cache_bucket_max(record.input_tokens);
    let previous_seen = provider_prefix_raw_seen_bucket(previous);
    if current_bucket.saturating_add(16_384) < previous_seen {
        return false;
    }
    provider_cache_ratio(record).unwrap_or_default() < 0.96
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderCacheGapBreakdown {
    total_tokens: u64,
    new_tail_tokens: u64,
    avoidable_tokens: u64,
    provider_unstable_tokens: u64,
}

async fn provider_cache_gap_breakdown(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    tail_input_diagnostics: Option<&TailInputDiagnostics>,
) -> Option<ProviderCacheGapBreakdown> {
    provider_cache_gap_breakdown_with_guard(
        state,
        provider_prefix_key,
        provider_prefix_family_key,
        usage_record,
        tail_input_diagnostics,
        None,
    )
    .await
}

async fn provider_cache_gap_breakdown_with_guard(
    state: &AppState,
    provider_prefix_key: Option<&str>,
    provider_prefix_family_key: Option<&str>,
    usage_record: Option<&UsageRecord>,
    tail_input_diagnostics: Option<&TailInputDiagnostics>,
    prefix_guard_wait: Option<&PrefixGuardWaitDiagnostics>,
) -> Option<ProviderCacheGapBreakdown> {
    let record = usage_record?;
    if record.input_tokens < 1024 {
        return Some(ProviderCacheGapBreakdown {
            total_tokens: 0,
            new_tail_tokens: 0,
            avoidable_tokens: 0,
            provider_unstable_tokens: 0,
        });
    }
    let bucket = provider_cache_bucket_max(record.input_tokens);
    let total = bucket.saturating_sub(record.cache_read_tokens);
    let (previous_exact, previous_best, previous_family) = if let Some(key) = provider_prefix_key {
        let states = state.prefix_states.lock().await;
        (
            states.get(key).cloned(),
            lookup_provider_prefix_state(&states, key).cloned(),
            provider_prefix_family_key.and_then(|key| states.get(key).cloned()),
        )
    } else {
        (None, None, None)
    };
    let cold_read_has_unreliable_dynamic_tail = tail_input_diagnostics
        .map(|tail| {
            responses_current_tail_makes_avoidable_unreliable(tail)
                || responses_tool_tail_burst(tail)
                || tail.tool_output_chars >= 80_000
                || tail.message_chars >= 80_000
        })
        .unwrap_or(false);
    if provider_prefix_break_after_warm_state(previous_exact.as_ref(), record)
        || (provider_prefix_break_after_warm_state(previous_best.as_ref(), record)
            && cold_read_has_unreliable_dynamic_tail)
        || (previous_exact.is_none()
            && previous_best.is_none()
            && provider_prefix_break_after_warm_state(previous_family.as_ref(), record)
            && cold_read_has_unreliable_dynamic_tail)
        || (previous_exact.is_none()
            && previous_best.is_none()
            && responses_huge_dynamic_history_cold_read(
                record,
                tail_input_diagnostics.unwrap_or(&TailInputDiagnostics::default()),
            ))
    {
        return Some(ProviderCacheGapBreakdown {
            total_tokens: total,
            new_tail_tokens: 0,
            avoidable_tokens: 0,
            provider_unstable_tokens: total,
        });
    }
    let avoidable_unreliable = tail_input_diagnostics
        .map(|tail| {
            responses_current_tail_makes_avoidable_unreliable(tail)
                || (record.cache_read_tokens == 0 && responses_tool_tail_burst(tail))
        })
        .unwrap_or(false);
    let direct_avoidable = if avoidable_unreliable {
        0
    } else {
        previous_best
            .as_ref()
            .map(|state| provider_prefix_calibrated_previous_seen_buckets(state, record).0)
            .unwrap_or(0)
            .min(bucket)
            .saturating_sub(record.cache_read_tokens)
    };
    let direct_avoidable_128 = if avoidable_unreliable {
        0
    } else {
        previous_best
            .as_ref()
            .map(|state| provider_prefix_calibrated_previous_seen_buckets(state, record).1)
            .unwrap_or(0)
            .min(provider_cache_bucket_max_128(record.input_tokens))
            .saturating_sub(record.cache_read_tokens)
    };
    let small_avoidable_is_tail_granularity = responses_small_avoidable_tail_granularity(
        previous_best.as_ref(),
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_128,
        tail_input_diagnostics.unwrap_or(&TailInputDiagnostics::default()),
    );
    let provider_unstable_tokens = if responses_cap_exhausted_provider_waterline_rollback(
        previous_exact.as_ref(),
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_128,
        tail_input_diagnostics.unwrap_or(&TailInputDiagnostics::default()),
        prefix_guard_wait
            .map(|wait| wait.budget_exhausted)
            .unwrap_or(false),
    ) {
        direct_avoidable
    } else {
        0
    };
    let direct_avoidable = if provider_unstable_tokens > 0 || small_avoidable_is_tail_granularity {
        0
    } else {
        direct_avoidable
    };
    Some(ProviderCacheGapBreakdown {
        total_tokens: total,
        new_tail_tokens: total
            .saturating_sub(direct_avoidable)
            .saturating_sub(provider_unstable_tokens),
        avoidable_tokens: direct_avoidable,
        provider_unstable_tokens,
    })
}

fn responses_tool_tail_burst(current_tail: &TailInputDiagnostics) -> bool {
    if matches!(
        current_tail.source.as_deref(),
        Some("message") | Some("tool_call")
    ) {
        return false;
    }
    current_tail.tool_output_chars >= 8_000
        || current_tail.largest_tool_output_chars >= 8_000
        || (current_tail.tool_output_chars >= 4_096
            && current_tail.tool_output_noise_hint.is_some())
}

async fn maybe_reuse_response_session(
    state: &AppState,
    request: &Value,
    session_key: Option<&str>,
    session_scope_key: Option<&str>,
    decision: &RouteDecision,
    allow_stream_delta: bool,
    allow_scope_fallback: bool,
) -> ResponseSessionReuseOutcome {
    let mut diagnostics = ResponseSessionReuseDiagnostics::default();
    let Some(key) = session_key else {
        diagnostics.skip_reason = Some("no_session_key".to_string());
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    };
    let Some(current_input) = request.get("input") else {
        diagnostics.skip_reason = Some("no_input".to_string());
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    };
    if request.get("previous_response_id").is_some() {
        diagnostics.skip_reason = Some("already_has_previous_response_id".to_string());
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    }
    if !allow_stream_delta
        && request
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        diagnostics.skip_reason = Some("stream_delta_disabled".to_string());
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    }
    let sessions = {
        let sessions = state.response_sessions.lock().await;
        let mut candidates = Vec::new();
        if let Some(exact) = sessions.get(key) {
            diagnostics.exact_key_hit = true;
            candidates.push(exact.clone());
        }
        diagnostics.scope_match_count =
            count_response_session_scope_matches(&sessions, key, session_scope_key) as u64;
        if allow_scope_fallback {
            candidates.extend(fallback_response_sessions(
                &sessions,
                key,
                session_scope_key,
                current_input,
            ));
        }
        diagnostics.candidate_count = candidates.len() as u64;
        candidates
    };
    if sessions.is_empty() {
        diagnostics.skip_reason = if diagnostics.scope_match_count > 0 {
            Some("no_append_prefix_candidate".to_string())
        } else if session_scope_key.is_none() {
            Some("no_scope_key".to_string())
        } else {
            Some("no_candidate".to_string())
        };
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    }
    let mut saw_unexpired = false;
    let mut saw_delta_rejected = false;
    for session in sessions {
        if session.finished_at.elapsed() > std::time::Duration::from_secs(1800) {
            continue;
        }
        saw_unexpired = true;
        let Some(delta_input) = appended_response_input_delta(&session.input, current_input) else {
            saw_delta_rejected = true;
            continue;
        };
        diagnostics.append_delta_match = true;
        diagnostics.delta_items = response_input_item_count(&delta_input) as u64;

        let mut optimized = request.clone();
        let Some(object) = optimized.as_object_mut() else {
            diagnostics.skip_reason = Some("request_not_object".to_string());
            return ResponseSessionReuseOutcome {
                body: request.clone(),
                diagnostics,
            };
        };
        object.insert(
            "previous_response_id".to_string(),
            Value::String(session.response_id),
        );
        object.insert("input".to_string(), delta_input);
        object.insert("store".to_string(), Value::Bool(true));
        object.insert("model".to_string(), Value::String(decision.model.clone()));
        diagnostics.skip_reason = None;
        return ResponseSessionReuseOutcome {
            body: optimized,
            diagnostics,
        };
    }
    diagnostics.skip_reason = if !saw_unexpired {
        Some("expired".to_string())
    } else if saw_delta_rejected {
        Some("append_delta_rejected".to_string())
    } else {
        Some("no_delta".to_string())
    };
    ResponseSessionReuseOutcome {
        body: request.clone(),
        diagnostics,
    }
}

async fn maybe_rescue_response_session_after_413(
    state: &AppState,
    request: &Value,
    session_key: Option<&str>,
    session_scope_key: Option<&str>,
    decision: &RouteDecision,
) -> ResponseSessionReuseOutcome {
    if request.get("previous_response_id").is_some() {
        let mut diagnostics = ResponseSessionReuseDiagnostics::default();
        diagnostics.skip_reason = Some("already_has_previous_response_id".to_string());
        return ResponseSessionReuseOutcome {
            body: request.clone(),
            diagnostics,
        };
    }
    maybe_reuse_response_session(
        state,
        request,
        session_key,
        session_scope_key,
        decision,
        true,
        true,
    )
    .await
}

fn should_attempt_main_response_session_delta(
    config: &AppConfig,
    decision: &RouteDecision,
    client_requested_stream: bool,
    request: &Value,
    current_tail: &TailInputDiagnostics,
    session_anchor: &SessionAnchorDiagnostics,
) -> bool {
    if !responses_session_reuse_enabled(config)
        || !config.cache.prewarm_enabled
        || !matches!(decision.upstream_channel, Channel::Responses)
        || !client_requested_stream
    {
        return false;
    }
    if !supports_main_response_session_delta(decision) {
        return false;
    }
    if session_anchor.source.as_deref() != Some("exact") {
        return false;
    }
    if request.get("previous_response_id").is_some() {
        return false;
    }

    let body_bytes = serialized_body_len(&Channel::Responses, request);
    if body_bytes >= 96 * 1024 {
        return true;
    }
    if current_tail.delta_from_session
        && (current_tail.tool_output_chars >= 512
            || current_tail.largest_tool_output_chars >= 512
            || current_tail.message_chars >= 512
            || current_tail.input_items >= 2)
    {
        return true;
    }
    current_tail.tool_output_chars >= 2_048
        || current_tail.largest_tool_output_chars >= 2_048
        || current_tail.message_chars >= 8_192
}

fn supports_main_response_session_delta(decision: &RouteDecision) -> bool {
    matches!(decision.upstream_channel, Channel::Responses)
}

fn main_response_session_delta_enabled_for_agent(forced_agent_id: Option<&str>) -> bool {
    forced_agent_id != Some("codex")
}

fn response_session_delta_is_beneficial(
    original: &Value,
    delta: &Value,
    current_tail: &TailInputDiagnostics,
) -> bool {
    if !response_session_delta_request(delta, original) {
        return false;
    }
    let original_bytes = serialized_body_len(&Channel::Responses, original);
    let delta_bytes = serialized_body_len(&Channel::Responses, delta);
    if delta_bytes >= original_bytes {
        return false;
    }
    if original_bytes >= 96 * 1024 {
        return delta_bytes.saturating_mul(5) <= original_bytes.saturating_mul(4);
    }
    if current_tail.tool_output_chars >= 2_048
        || current_tail.largest_tool_output_chars >= 2_048
        || current_tail.message_chars >= 8_192
    {
        return delta_bytes.saturating_mul(10) <= original_bytes.saturating_mul(9);
    }
    delta_bytes.saturating_mul(2) <= original_bytes
}

fn should_attempt_response_session_rescue_after_413(
    status: u16,
    active_used_response_session: bool,
    active_responses_non_stream_chat_compat: bool,
    upstream_channel: &Channel,
    response_session_cooldown_active: bool,
) -> bool {
    status == 413
        && !active_used_response_session
        && !active_responses_non_stream_chat_compat
        && matches!(upstream_channel, Channel::Responses)
        && !response_session_cooldown_active
}

fn count_response_session_scope_matches(
    sessions: &HashMap<String, ResponseSessionState>,
    current_key: &str,
    current_scope_key: Option<&str>,
) -> usize {
    let Some(current_scope_key) = current_scope_key else {
        return 0;
    };
    sessions
        .iter()
        .filter(|(key, session)| {
            key.as_str() != current_key
                && session.finished_at.elapsed() <= std::time::Duration::from_secs(1800)
                && session.scope_key.as_deref() == Some(current_scope_key)
        })
        .count()
}

async fn response_session_anchor_diagnostics(
    state: &AppState,
    current_key: Option<&str>,
    current_scope_key: Option<&str>,
) -> SessionAnchorDiagnostics {
    let Some(current_key) = current_key else {
        return SessionAnchorDiagnostics::default();
    };
    let sessions = state.response_sessions.lock().await;
    let exact = sessions.contains_key(current_key);
    let peer_count =
        count_response_session_scope_matches(&sessions, current_key, current_scope_key);
    let source = if exact {
        "exact"
    } else if peer_count > 0 {
        "scope-sibling"
    } else {
        "new-anchor"
    };
    SessionAnchorDiagnostics {
        hash: Some(current_key.to_string()),
        source: Some(source.to_string()),
        changed: Some(!exact && peer_count > 0),
        peer_count: Some(peer_count as u64),
    }
}

fn apply_session_anchor_diagnostics(log: &mut RequestLog, diagnostics: &SessionAnchorDiagnostics) {
    log.session_anchor_hash = diagnostics.hash.clone();
    log.session_anchor_source = diagnostics.source.clone();
    log.session_anchor_changed = diagnostics.changed;
    log.session_anchor_peer_count = diagnostics.peer_count;
}

#[cfg(test)]
fn fallback_response_session(
    sessions: &HashMap<String, ResponseSessionState>,
    current_key: &str,
    current_scope_key: Option<&str>,
    current_input: &Value,
) -> Option<ResponseSessionState> {
    fallback_response_sessions(sessions, current_key, current_scope_key, current_input)
        .into_iter()
        .next()
}

fn fallback_response_sessions(
    sessions: &HashMap<String, ResponseSessionState>,
    current_key: &str,
    current_scope_key: Option<&str>,
    current_input: &Value,
) -> Vec<ResponseSessionState> {
    let Some(current_scope_key) = current_scope_key else {
        return Vec::new();
    };
    let mut ranked = Vec::new();
    for (key, session) in sessions {
        if key == current_key
            || session.finished_at.elapsed() > std::time::Duration::from_secs(1800)
            || session.scope_key.as_deref() != Some(current_scope_key)
        {
            continue;
        }
        let Some(score) = response_session_fallback_score(&session.input, current_input) else {
            continue;
        };
        ranked.push((
            score,
            response_input_item_count(&session.input),
            session.finished_at.elapsed(),
            session.clone(),
        ));
    }
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    ranked
        .into_iter()
        .map(|(_, _, _, session)| session)
        .collect()
}

fn response_input_item_count(input: &Value) -> usize {
    input.as_array().map(Vec::len).unwrap_or_default()
}

fn response_session_fallback_score(previous: &Value, current: &Value) -> Option<usize> {
    let (Value::Array(previous_items), Value::Array(current_items)) = (previous, current) else {
        return None;
    };
    let previous_essential = response_input_essential_items(previous_items);
    let current_essential = response_input_essential_items(current_items);
    if previous_essential.is_empty() || previous_essential.len() >= current_essential.len() {
        return None;
    }
    if !previous_essential
        .iter()
        .zip(current_essential.iter())
        .all(|(previous, current)| previous.1 == current.1)
    {
        return None;
    }
    Some(previous_essential.len())
}

fn response_session_delta_request(candidate: &Value, original: &Value) -> bool {
    candidate.get("previous_response_id").is_some()
        && original.get("previous_response_id").is_none()
        && candidate.get("input") != original.get("input")
}

fn should_retry_full_response_after_session_error(
    status: u16,
    used_response_session: bool,
) -> bool {
    if !used_response_session {
        return false;
    }
    matches!(status, 400 | 404 | 409 | 410 | 422)
}

async fn clear_response_session_reference(
    state: &AppState,
    session_key: Option<&str>,
    response_id: Option<&str>,
) {
    let mut sessions = state.response_sessions.lock().await;
    if let Some(key) = session_key {
        sessions.remove(key);
    }
    if let Some(response_id) = response_id {
        sessions.retain(|_, session| session.response_id != response_id);
    }
    drop(sessions);
    if let Err(err) = state.persist_runtime_state().await {
        state
            .metrics
            .record_error("runtime_state_save", &err.to_string())
            .await;
    }
}

fn previous_response_id_from_request(request: &Value) -> Option<String> {
    request
        .get("previous_response_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

async fn maybe_prepare_previous_response_compat_body(
    state: &AppState,
    request: &Value,
) -> Option<PreviousResponseCompatBody> {
    let previous_response_id = previous_response_id_from_request(request)?;
    let current_input = request.get("input")?;
    if previous_response_id.trim().is_empty() {
        return None;
    }

    if let Some(previous_input) =
        response_session_input_for_response_id(state, &previous_response_id).await
    {
        if response_input_can_stand_alone_with_previous_session(&previous_input, current_input) {
            return strip_previous_response_id_for_compat(
                request,
                "client_previous_response_id_self_contained_session",
            );
        }
        if let Some(expanded) = expand_previous_response_id_for_compat(
            request,
            &previous_input,
            "client_previous_response_id_expanded_from_local_session",
        ) {
            return Some(expanded);
        }
    }

    if response_input_looks_self_contained_without_previous_response_id(current_input) {
        return strip_previous_response_id_for_compat(
            request,
            "client_previous_response_id_self_contained_input",
        );
    }

    None
}

async fn response_session_input_for_response_id(
    state: &AppState,
    response_id: &str,
) -> Option<Value> {
    let sessions = state.response_sessions.lock().await;
    sessions
        .values()
        .filter(|session| {
            session.response_id == response_id
                && session.finished_at.elapsed() <= std::time::Duration::from_secs(1800)
        })
        .max_by_key(|session| response_input_item_count(&session.input))
        .map(|session| session.input.clone())
}

fn strip_previous_response_id_for_compat(
    request: &Value,
    reason: &'static str,
) -> Option<PreviousResponseCompatBody> {
    let mut body = request.clone();
    let object = body.as_object_mut()?;
    object.remove("previous_response_id")?;
    Some(PreviousResponseCompatBody { body, reason })
}

fn expand_previous_response_id_for_compat(
    request: &Value,
    previous_input: &Value,
    reason: &'static str,
) -> Option<PreviousResponseCompatBody> {
    let mut body = request.clone();
    let object = body.as_object_mut()?;
    object.remove("previous_response_id")?;
    let current_input = object.get("input")?;
    let (Value::Array(previous_items), Value::Array(current_items)) =
        (previous_input, current_input)
    else {
        return None;
    };
    if previous_items.is_empty() || current_items.is_empty() {
        return None;
    }
    if current_items.len() >= previous_items.len()
        && previous_items
            .iter()
            .zip(current_items.iter())
            .all(|(previous, current)| previous == current)
    {
        return Some(PreviousResponseCompatBody { body, reason });
    }
    let mut expanded = previous_items.clone();
    expanded.extend(current_items.iter().cloned());
    object.insert("input".to_string(), Value::Array(expanded));
    Some(PreviousResponseCompatBody { body, reason })
}

fn response_input_can_stand_alone_with_previous_session(
    previous_input: &Value,
    current_input: &Value,
) -> bool {
    let (Some(previous_items), Some(current_items)) =
        (previous_input.as_array(), current_input.as_array())
    else {
        return false;
    };
    response_input_essential_prefix_matches(previous_items, current_items)
        && response_input_items_have_self_contained_context(current_items)
}

fn response_input_looks_self_contained_without_previous_response_id(input: &Value) -> bool {
    input
        .as_array()
        .is_some_and(|items| response_input_items_have_self_contained_context(items))
}

fn response_input_items_have_self_contained_context(items: &[Value]) -> bool {
    if items.is_empty() {
        return false;
    }

    let call_ids = items
        .iter()
        .filter(|item| response_input_item_is_call(item))
        .filter_map(response_input_item_call_id)
        .collect::<HashSet<_>>();
    let mut has_prior_output_context = false;

    for item in items {
        if response_input_item_is_assistant_context(item) || response_input_item_is_call(item) {
            has_prior_output_context = true;
        }
        if response_input_item_is_call_output(item) {
            let Some(call_id) = response_input_item_call_id(item) else {
                return false;
            };
            if !call_ids.contains(&call_id) {
                return false;
            }
            has_prior_output_context = true;
        }
    }

    has_prior_output_context
}

fn response_input_item_is_assistant_context(item: &Value) -> bool {
    item.as_object()
        .and_then(|object| object.get("role"))
        .and_then(Value::as_str)
        .is_some_and(|role| matches!(role, "assistant" | "model"))
}

fn response_input_item_is_call(item: &Value) -> bool {
    response_input_item_type(item).is_some_and(|item_type| {
        item_type.ends_with("_call") && !item_type.ends_with("_call_output")
    })
}

fn response_input_item_is_call_output(item: &Value) -> bool {
    response_input_item_type(item).is_some_and(|item_type| item_type.ends_with("_call_output"))
}

fn response_input_item_type(item: &Value) -> Option<&str> {
    item.as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
}

fn response_input_item_call_id(item: &Value) -> Option<String> {
    item.as_object()
        .and_then(|object| object.get("call_id").or_else(|| object.get("id")))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn appended_response_input_delta(previous: &Value, current: &Value) -> Option<Value> {
    match (previous, current) {
        (Value::Array(previous_items), Value::Array(current_items)) => {
            if response_same_message_text_tail_delta_enabled() {
                if let Some(delta) =
                    appended_response_text_tail_delta(previous_items, current_items)
                {
                    return Some(delta);
                }
            }
            let delta_start = response_input_delta_start_index(previous_items, current_items)?;
            let delta = current_items[delta_start..]
                .iter()
                .filter(|item| response_session_delta_item_is_needed(item))
                .cloned()
                .collect::<Vec<_>>();
            if delta.is_empty() {
                None
            } else {
                Some(Value::Array(delta))
            }
        }
        _ => None,
    }
}

fn response_same_message_text_tail_delta_enabled() -> bool {
    false
}

fn appended_response_text_tail_delta(
    previous_items: &[Value],
    current_items: &[Value],
) -> Option<Value> {
    if previous_items.len() != current_items.len() || previous_items.is_empty() {
        return None;
    }
    let previous_essential = response_input_essential_items(previous_items);
    let current_essential = response_input_essential_items(current_items);
    if previous_essential.len() != current_essential.len() || previous_essential.is_empty() {
        return None;
    }

    let last = previous_essential.len().checked_sub(1)?;
    if !previous_essential
        .iter()
        .take(last)
        .zip(current_essential.iter().take(last))
        .all(|(previous, current)| previous.1 == current.1)
    {
        return None;
    }

    let previous_index = previous_essential[last].0;
    let current_index = current_essential[last].0;
    let previous_item = previous_items.get(previous_index)?;
    let current_item = current_items.get(current_index)?;
    if !response_text_tail_delta_item_is_safe(previous_item, current_item) {
        return None;
    }

    let previous_text = response_message_text(previous_item)?;
    let current_text = response_message_text(current_item)?;
    let suffix = current_text.strip_prefix(&previous_text)?;
    if suffix.trim().is_empty() {
        return None;
    }

    response_message_with_text(current_item, suffix.trim_start())
        .map(|delta_item| Value::Array(vec![delta_item]))
}

fn response_text_tail_delta_item_is_safe(previous: &Value, current: &Value) -> bool {
    let (Some(previous_object), Some(current_object)) = (previous.as_object(), current.as_object())
    else {
        return false;
    };
    let previous_type = previous_object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message");
    let current_type = current_object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message");
    if previous_type != "message" || current_type != "message" {
        return false;
    }
    let previous_role = previous_object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user");
    let current_role = current_object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user");
    previous_role == "user" && current_role == "user"
}

fn response_message_text(item: &Value) -> Option<String> {
    let object = item.as_object()?;
    object
        .get("content")
        .or_else(|| object.get("text"))
        .and_then(extract_text)
}

fn response_message_with_text(template: &Value, text: &str) -> Option<Value> {
    if text.trim().is_empty() {
        return None;
    }
    let object = template.as_object()?;
    let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
    Some(json!({
        "type": "message",
        "role": role,
        "content": [{ "type": "input_text", "text": text }]
    }))
}

fn response_input_delta_start_index(
    previous_items: &[Value],
    current_items: &[Value],
) -> Option<usize> {
    if let Some(index) = response_input_raw_prefix_delta_start_index(previous_items, current_items)
    {
        return Some(index);
    }
    let previous_essential = response_input_essential_items(previous_items);
    let current_essential = response_input_essential_items(current_items);
    if current_essential.len() <= previous_essential.len() {
        return None;
    }
    if !previous_essential
        .iter()
        .zip(current_essential.iter())
        .all(|(previous, current)| previous.1 == current.1)
    {
        return None;
    }
    current_essential
        .get(previous_essential.len())
        .map(|(index, _)| *index)
}

fn response_input_raw_prefix_delta_start_index(
    previous_items: &[Value],
    current_items: &[Value],
) -> Option<usize> {
    if previous_items.is_empty() || previous_items.len() >= current_items.len() {
        return None;
    }
    previous_items
        .iter()
        .zip(current_items.iter())
        .all(|(previous, current)| previous == current)
        .then_some(previous_items.len())
}

fn response_input_essential_prefix_matches(
    previous_items: &[Value],
    current_items: &[Value],
) -> bool {
    let previous_essential = response_input_essential_items(previous_items);
    let current_essential = response_input_essential_items(current_items);
    previous_essential.len() <= current_essential.len()
        && previous_essential
            .iter()
            .zip(current_essential.iter())
            .all(|(previous, current)| previous.1 == current.1)
}

fn response_input_essential_items(items: &[Value]) -> Vec<(usize, Value)> {
    items
        .iter()
        .enumerate()
        .filter(|(_, item)| response_session_delta_item_is_needed(item))
        .map(|(index, item)| (index, comparable_response_input_item(item)))
        .collect()
}

fn response_session_delta_item_is_needed(item: &Value) -> bool {
    let Some(object) = item.as_object() else {
        return true;
    };

    if object
        .get("role")
        .and_then(Value::as_str)
        .is_some_and(|role| matches!(role, "assistant" | "model"))
    {
        return false;
    }

    let Some(item_type) = object.get("type").and_then(Value::as_str) else {
        return true;
    };

    match item_type {
        "reasoning"
        | "function_call"
        | "computer_call"
        | "web_search_call"
        | "file_search_call"
        | "code_interpreter_call"
        | "image_generation_call"
        | "local_shell_call" => false,
        item_type if item_type.ends_with("_call") && !item_type.ends_with("_call_output") => false,
        _ => true,
    }
}

fn comparable_response_input_item(item: &Value) -> Value {
    let mut normalized = item.clone();
    normalize_responses_input(&mut normalized);
    stabilize_responses_provider_prefix(&mut normalized);
    strip_response_session_volatile_fields(&mut normalized);
    strip_response_session_compare_noise(&mut normalized);
    canonicalize_object_keys(&mut normalized, "$.response_session_input_item");
    normalized
}

fn strip_response_session_compare_noise(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("call_id");
            for child in map.values_mut() {
                strip_response_session_compare_noise(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_response_session_compare_noise(item);
            }
        }
        _ => {}
    }
}

async fn update_response_session(
    state: &AppState,
    session_key: Option<&str>,
    session_scope_key: Option<&str>,
    full_response_input: Option<&Value>,
    bytes: &[u8],
) {
    update_response_session_with_id(
        state,
        session_key,
        session_scope_key,
        full_response_input,
        response_id_from_bytes(bytes),
    )
    .await;
}

async fn update_response_session_with_id(
    state: &AppState,
    session_key: Option<&str>,
    session_scope_key: Option<&str>,
    full_response_input: Option<&Value>,
    response_id: Option<String>,
) {
    let (Some(key), Some(input), Some(response_id)) =
        (session_key, full_response_input, response_id)
    else {
        return;
    };
    {
        let mut sessions = state.response_sessions.lock().await;
        if let Some(existing) = sessions.get(key) {
            if !should_replace_response_session(&existing.input, input) {
                return;
            }
        }
        sessions.insert(
            key.to_string(),
            ResponseSessionState {
                response_id,
                input: input.clone(),
                scope_key: session_scope_key.map(ToOwned::to_owned),
                finished_at: Instant::now(),
            },
        );
    }
    if let Err(err) = state.persist_runtime_state().await {
        state
            .metrics
            .record_error("runtime_state_save", &err.to_string())
            .await;
    }
}

fn should_replace_response_session(existing: &Value, next: &Value) -> bool {
    match (existing, next) {
        (Value::Array(existing_items), Value::Array(next_items)) => {
            if response_input_essential_prefix_matches(next_items, existing_items) {
                return false;
            }
            true
        }
        _ => true,
    }
}

fn response_id_from_bytes(bytes: &[u8]) -> Option<String> {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return response_id_from_value(&value);
    }

    let text = String::from_utf8_lossy(bytes);
    let mut last_id = None;
    for line in text.lines() {
        let Some(payload) = line.trim_start().strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            if let Some(id) = response_id_from_value(&value) {
                last_id = Some(id);
            }
        }
    }
    last_id
}

fn response_id_from_value(value: &Value) -> Option<String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/response/id").and_then(Value::as_str))
        .or_else(|| value.pointer("/message/id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn upstream_error_summary(bytes: &[u8]) -> String {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        if let Some(message) = value
            .pointer("/error/message")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
        {
            return truncate_log_message(message);
        }
        return truncate_log_message(&value.to_string());
    }
    truncate_log_message(&String::from_utf8_lossy(bytes))
}

fn upstream_error_scope(status: u16, message: &str) -> &'static str {
    match status {
        401 => "upstream_auth",
        403 => {
            let lower = message.to_ascii_lowercase();
            if message.contains("\u{989d}\u{5ea6}")
                || message.contains("\u{4f59}\u{989d}")
                || lower.contains("quota")
                || lower.contains("balance")
                || lower.contains("insufficient")
            {
                "upstream_quota"
            } else {
                "upstream_forbidden"
            }
        }
        408 => "upstream_timeout",
        413 => "upstream_payload_too_large",
        429 => "upstream_rate_limit",
        500 => "upstream_provider_500",
        502 => "upstream_bad_gateway",
        503 => "upstream_unavailable",
        504 => "upstream_gateway_timeout",
        500..=599 => "upstream_provider_5xx",
        _ => "upstream_status",
    }
}

fn truncate_log_message(message: &str) -> String {
    const MAX_CHARS: usize = 700;
    let normalized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_CHARS {
        normalized
    } else {
        let mut truncated = normalized.chars().take(MAX_CHARS).collect::<String>();
        truncated.push_str("...");
        truncated
    }
}

fn local_session_keys_enabled(config: &AppConfig) -> bool {
    smart_hit_enabled(config)
}

fn responses_session_reuse_enabled(config: &AppConfig) -> bool {
    smart_hit_enabled(config)
}

fn smart_hit_enabled(config: &AppConfig) -> bool {
    config.cache.enabled
        && config.cache.prewarm_enabled
        && matches!(
            config.cache.mode,
            CacheMode::SessionPrewarm | CacheMode::PrefixPrewarm
        )
}

fn metrics_cache_key(cache_keys: &[String]) -> String {
    if cache_keys.len() > 2 {
        cache_keys[2].clone()
    } else if cache_keys.len() > 1 {
        cache_keys[1].clone()
    } else {
        cache_keys
            .first()
            .cloned()
            .unwrap_or_else(|| "missing-cache-key".to_string())
    }
}

fn serialized_body_len(channel: &Channel, body: &Value) -> u64 {
    if matches!(channel, Channel::Responses) {
        serialize_responses_body_for_provider_prefix(body).len() as u64
    } else {
        serde_json::to_vec(body)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0)
    }
}

fn body_diagnostics(
    channel: &Channel,
    original_body: &Value,
    send_body: &Value,
    send_body_is_delta: bool,
) -> BodyDiagnostics {
    BodyDiagnostics {
        original_body_bytes: serialized_body_len(channel, original_body),
        send_body_bytes: serialized_body_len(channel, send_body),
        send_body_is_delta,
        payload_too_large_rescue_attempted: false,
        payload_too_large_rescue_used: false,
        reasoning: ReasoningEffortDiagnostics::default(),
    }
}

fn apply_body_diagnostics(log: &mut RequestLog, diagnostics: &BodyDiagnostics) {
    log.original_body_bytes = Some(diagnostics.original_body_bytes);
    log.send_body_bytes = Some(diagnostics.send_body_bytes);
    log.send_body_is_delta = Some(diagnostics.send_body_is_delta);
    log.payload_too_large_rescue_attempted = Some(diagnostics.payload_too_large_rescue_attempted);
    log.payload_too_large_rescue_used = Some(diagnostics.payload_too_large_rescue_used);
    log.agent_reasoning_effort = diagnostics.reasoning.agent.clone();
    log.configured_reasoning_effort = diagnostics.reasoning.configured.clone();
    log.effective_reasoning_effort = diagnostics.reasoning.effective.clone();
    log.reasoning_effort_source = diagnostics.reasoning.source.clone();
}

async fn tail_input_diagnostics_for_session(
    state: &AppState,
    channel: &Channel,
    session_key: Option<&str>,
    session_scope_key: Option<&str>,
    current_input: Option<&Value>,
) -> TailInputDiagnostics {
    if !matches!(channel, Channel::Responses) {
        return TailInputDiagnostics::default();
    }
    let Some(current_input) = current_input else {
        return TailInputDiagnostics::default();
    };
    let Value::Array(current_items) = current_input else {
        return TailInputDiagnostics::default();
    };
    if current_items.is_empty() {
        return TailInputDiagnostics::default();
    }

    let previous_input = if let Some(key) = session_key {
        let sessions = state.response_sessions.lock().await;
        sessions
            .get(key)
            .map(|session| session.input.clone())
            .or_else(|| {
                fallback_response_sessions(&sessions, key, session_scope_key, current_input)
                    .into_iter()
                    .next()
                    .map(|session| session.input)
            })
    } else {
        None
    };
    let tail_start = previous_input
        .as_ref()
        .and_then(Value::as_array)
        .and_then(|previous_items| response_input_delta_start_index(previous_items, current_items))
        .unwrap_or(0);
    let mut diagnostics = summarize_tail_input_items(&current_items[tail_start..]);
    diagnostics.delta_from_session = previous_input.is_some() && tail_start > 0;
    diagnostics
}

fn summarize_tail_input_items(items: &[Value]) -> TailInputDiagnostics {
    let mut diagnostics = TailInputDiagnostics {
        input_items: items.len() as u64,
        ..TailInputDiagnostics::default()
    };
    let mut has_message = false;
    let mut has_tool_call = false;
    let mut has_tool_output = false;

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        if response_tail_item_is_tool_output(item, item_type) {
            let text = response_tail_tool_output_text(item);
            let chars = text
                .as_deref()
                .map(|text| text.chars().count() as u64)
                .unwrap_or(0);
            diagnostics.tool_output_chars += chars;
            diagnostics.largest_tool_output_chars =
                diagnostics.largest_tool_output_chars.max(chars);
            if let Some(text) = text {
                merge_tool_output_noise(&mut diagnostics, tool_output_noise_diagnostics(&text));
            }
            has_tool_output = true;
        } else if response_tail_item_is_tool_call(item, item_type) {
            diagnostics.tool_call_chars += response_tail_tool_call_chars(item);
            has_tool_call = true;
        } else if response_tail_item_is_message(item, item_type) {
            diagnostics.message_chars += response_tail_message_chars(item);
            has_message = true;
        } else if let Some(text) = extract_text(item) {
            diagnostics.message_chars += text.chars().count() as u64;
            has_message = true;
        }
    }

    diagnostics.source = response_tail_source(has_message, has_tool_call, has_tool_output);
    diagnostics
}

fn response_tail_item_is_tool_output(item: &Value, item_type: &str) -> bool {
    item_type == "function_call_output"
        || item_type.ends_with("_call_output")
        || item.get("call_id").is_some() && item.get("output").is_some()
}

fn response_tail_item_is_tool_call(item: &Value, item_type: &str) -> bool {
    item_type == "function_call"
        || item_type.ends_with("_call") && !item_type.ends_with("_call_output")
        || item.get("arguments").is_some() && item.get("name").is_some()
}

fn response_tail_item_is_message(item: &Value, item_type: &str) -> bool {
    item_type == "message"
        || item.get("role").is_some()
        || item.get("content").is_some()
        || item.get("text").is_some()
}

fn response_tail_tool_output_text(item: &Value) -> Option<String> {
    item.get("output")
        .and_then(extract_text)
        .or_else(|| item.get("content").and_then(extract_text))
        .or_else(|| extract_text(item))
}

fn tool_output_noise_diagnostics(text: &str) -> ToolOutputNoiseDiagnostics {
    let mut line_counts = HashMap::<String, u64>::new();
    let mut lines = 0u64;
    let mut repeated_line_chars = 0u64;
    for line in text.lines() {
        lines += 1;
        let normalized = line.trim();
        if normalized.len() < 16 {
            continue;
        }
        let count = line_counts.entry(normalized.to_string()).or_insert(0);
        if *count > 0 {
            repeated_line_chars += normalized.chars().count() as u64;
        }
        *count += 1;
    }

    let timestamp_like_count = count_timestamp_like(text);
    let path_like_count = count_path_like(text);
    let url_like_count =
        text.matches("https://").count() as u64 + text.matches("http://").count() as u64;
    let hash_like_count = count_hash_like(text);
    let json_like_chars = if looks_json_like(text) {
        text.chars().count() as u64
    } else {
        0
    };
    let hint = tool_output_noise_hint(
        repeated_line_chars,
        timestamp_like_count,
        path_like_count,
        url_like_count,
        hash_like_count,
        json_like_chars,
    );

    ToolOutputNoiseDiagnostics {
        lines,
        repeated_line_chars,
        timestamp_like_count,
        path_like_count,
        url_like_count,
        hash_like_count,
        json_like_chars,
        hint,
    }
}

fn merge_tool_output_noise(
    diagnostics: &mut TailInputDiagnostics,
    noise: ToolOutputNoiseDiagnostics,
) {
    diagnostics.tool_output_lines += noise.lines;
    diagnostics.tool_output_repeated_line_chars += noise.repeated_line_chars;
    diagnostics.tool_output_timestamp_like_count += noise.timestamp_like_count;
    diagnostics.tool_output_path_like_count += noise.path_like_count;
    diagnostics.tool_output_url_like_count += noise.url_like_count;
    diagnostics.tool_output_hash_like_count += noise.hash_like_count;
    diagnostics.tool_output_json_like_chars += noise.json_like_chars;
    diagnostics.tool_output_noise_hint =
        merge_noise_hints(diagnostics.tool_output_noise_hint.take(), noise.hint);
}

fn count_timestamp_like(text: &str) -> u64 {
    let bytes = text.as_bytes();
    let mut count = 0u64;
    for index in 0..bytes.len().saturating_sub(9) {
        if is_digit_window(bytes, index, 4)
            && matches!(bytes[index + 4], b'-' | b'/')
            && is_digit_window(bytes, index + 5, 2)
            && matches!(bytes[index + 7], b'-' | b'/')
            && is_digit_window(bytes, index + 8, 2)
        {
            count += 1;
        }
    }
    for index in 0..bytes.len().saturating_sub(4) {
        if is_digit_window(bytes, index, 2)
            && bytes[index + 2] == b':'
            && is_digit_window(bytes, index + 3, 2)
        {
            count += 1;
        }
    }
    count
}

fn count_path_like(text: &str) -> u64 {
    text.lines()
        .filter(|line| {
            let line = line.trim();
            has_windows_drive_path(line)
                || line.contains("\\\\")
                || line.contains(":/")
                || line.contains("src/")
                || line.contains("/src/")
                || line.contains("node_modules/")
                || line.contains("\\src\\")
                || line.contains("\\node_modules\\")
        })
        .count() as u64
}

fn count_hash_like(text: &str) -> u64 {
    let mut count = 0u64;
    let mut run = 0usize;
    for character in text.chars() {
        if character.is_ascii_hexdigit() {
            run += 1;
        } else {
            if run >= 16 {
                count += 1;
            }
            run = 0;
        }
    }
    if run >= 16 {
        count += 1;
    }
    count
}

fn is_digit_window(bytes: &[u8], start: usize, len: usize) -> bool {
    bytes
        .get(start..start + len)
        .is_some_and(|window| window.iter().all(u8::is_ascii_digit))
}

fn has_windows_drive_path(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.windows(3).any(|window| {
        window[0].is_ascii_alphabetic() && window[1] == b':' && matches!(window[2], b'\\' | b'/')
    })
}

fn looks_json_like(text: &str) -> bool {
    let trimmed = text.trim();
    (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
}

fn tool_output_noise_hint(
    repeated_line_chars: u64,
    timestamp_like_count: u64,
    path_like_count: u64,
    url_like_count: u64,
    hash_like_count: u64,
    json_like_chars: u64,
) -> Option<String> {
    let mut hints = Vec::new();
    if repeated_line_chars >= 32 {
        hints.push("repeated_lines");
    }
    if timestamp_like_count > 0 {
        hints.push("timestamp_like");
    }
    if path_like_count > 0 {
        hints.push("path_like");
    }
    if url_like_count > 0 {
        hints.push("url_like");
    }
    if hash_like_count > 0 {
        hints.push("hash_like");
    }
    if json_like_chars >= 512 {
        hints.push("json_like");
    }
    if hints.is_empty() {
        None
    } else {
        Some(hints.join(","))
    }
}

fn merge_noise_hints(left: Option<String>, right: Option<String>) -> Option<String> {
    let mut hints = Vec::<String>::new();
    for source in [left, right].into_iter().flatten() {
        for hint in source.split(',') {
            if !hint.is_empty() && !hints.iter().any(|existing| existing == hint) {
                hints.push(hint.to_string());
            }
        }
    }
    if hints.is_empty() {
        None
    } else {
        Some(hints.join(","))
    }
}

fn response_tail_tool_call_chars(item: &Value) -> u64 {
    let mut chars = 0u64;
    if let Some(name) = item.get("name").and_then(Value::as_str) {
        chars += name.chars().count() as u64;
    }
    if let Some(arguments) = item.get("arguments").and_then(extract_text) {
        chars += arguments.chars().count() as u64;
    }
    if chars == 0 {
        extract_text(item)
            .map(|text| text.chars().count() as u64)
            .unwrap_or(0)
    } else {
        chars
    }
}

fn response_tail_message_chars(item: &Value) -> u64 {
    item.get("content")
        .and_then(extract_text)
        .or_else(|| item.get("text").and_then(extract_text))
        .or_else(|| extract_text(item))
        .map(|text| text.chars().count() as u64)
        .unwrap_or(0)
}

fn response_tail_source(
    has_message: bool,
    has_tool_call: bool,
    has_tool_output: bool,
) -> Option<String> {
    let kinds = u8::from(has_message) + u8::from(has_tool_call) + u8::from(has_tool_output);
    match kinds {
        0 => Some("none".to_string()),
        1 if has_tool_output => Some("tool_output".to_string()),
        1 if has_tool_call => Some("tool_call".to_string()),
        1 if has_message => Some("message".to_string()),
        _ => Some("mixed".to_string()),
    }
}

fn apply_tail_input_diagnostics(log: &mut RequestLog, diagnostics: &TailInputDiagnostics) {
    if diagnostics.input_items == 0
        && diagnostics.message_chars == 0
        && diagnostics.tool_call_chars == 0
        && diagnostics.tool_output_chars == 0
        && diagnostics.largest_tool_output_chars == 0
        && diagnostics.tool_output_lines == 0
        && diagnostics.tool_output_repeated_line_chars == 0
        && diagnostics.tool_output_timestamp_like_count == 0
        && diagnostics.tool_output_path_like_count == 0
        && diagnostics.tool_output_url_like_count == 0
        && diagnostics.tool_output_hash_like_count == 0
        && diagnostics.tool_output_json_like_chars == 0
        && diagnostics.tool_output_noise_hint.is_none()
        && diagnostics.source.is_none()
    {
        return;
    }
    log.tail_input_items = Some(diagnostics.input_items);
    log.tail_message_chars = Some(diagnostics.message_chars);
    log.tail_tool_call_chars = Some(diagnostics.tool_call_chars);
    log.tail_tool_output_chars = Some(diagnostics.tool_output_chars);
    log.tail_largest_tool_output_chars = Some(diagnostics.largest_tool_output_chars);
    log.tail_tool_output_lines = Some(diagnostics.tool_output_lines);
    log.tail_tool_output_repeated_line_chars = Some(diagnostics.tool_output_repeated_line_chars);
    log.tail_tool_output_timestamp_like_count = Some(diagnostics.tool_output_timestamp_like_count);
    log.tail_tool_output_path_like_count = Some(diagnostics.tool_output_path_like_count);
    log.tail_tool_output_url_like_count = Some(diagnostics.tool_output_url_like_count);
    log.tail_tool_output_hash_like_count = Some(diagnostics.tool_output_hash_like_count);
    log.tail_tool_output_json_like_chars = Some(diagnostics.tool_output_json_like_chars);
    log.tail_tool_output_noise_hint = diagnostics.tool_output_noise_hint.clone();
    log.tail_source = diagnostics.source.clone();
}

fn request_body_stream_flag(body: &Value) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(false)
}

fn current_non_stream_upstream_call_source(
    sync_responses_main: bool,
    active_responses_non_stream_chat_compat: bool,
    responses_sync_main_chat_compat_fallback: bool,
) -> &'static str {
    if active_responses_non_stream_chat_compat {
        if responses_sync_main_chat_compat_fallback {
            "responses-sync-main-chat-compat-fallback"
        } else {
            "responses-sync-main-chat-compat"
        }
    } else if sync_responses_main {
        "responses-sync-main"
    } else {
        "main"
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_upstream_response_observation(
    state: &AppState,
    request_id: &str,
    started: &Instant,
    client_channel: &Channel,
    upstream_channel: &Channel,
    decision: &RouteDecision,
    cache_key: Option<&str>,
    provider_prefix_key: Option<&str>,
    provider_prefix_fingerprint: Option<&str>,
    prefix_guard_wait: &PrefixGuardWaitDiagnostics,
    local_prepare_ms: u64,
    body_diagnostics: &BodyDiagnostics,
    body: &Value,
    used_response_session: bool,
    response_session_reuse_diagnostics: &ResponseSessionReuseDiagnostics,
    requested_model: Option<String>,
    source: &str,
    status: u16,
    upstream_request_diagnostics: &UpstreamRequestDiagnostics,
    agent_log_id: Option<String>,
    agent_log_label: Option<String>,
) {
    let elapsed = started.elapsed().as_millis() as u64;
    let body_bytes = serialized_body_len(upstream_channel, body);
    let original_body_bytes = if body_diagnostics.original_body_bytes > 0 {
        body_diagnostics.original_body_bytes
    } else {
        body_bytes
    };
    let send_body_bytes = if body_diagnostics.send_body_bytes > 0 {
        body_diagnostics.send_body_bytes
    } else {
        body_bytes
    };
    let is_success_status = StatusCode::from_u16(status)
        .map(|code| code.is_success())
        .unwrap_or(false);

    state
        .metrics
        .record_upstream_call(RequestLog {
            id: Uuid::new_v4().to_string(),
            at: Utc::now(),
            inbound_request_id: Some(request_id.to_string()),
            upstream_request_id: Some(Uuid::new_v4().to_string()),
            upstream_attempt_index: Some(1),
            upstream_attempt_total: Some(1),
            client_channel: client_channel.label().to_string(),
            upstream_channel: upstream_channel.label().to_string(),
            provider: decision.provider.name.clone(),
            model: decision.model.clone(),
            requested_model,
            agent_reasoning_effort: body_diagnostics.reasoning.agent.clone(),
            configured_reasoning_effort: body_diagnostics.reasoning.configured.clone(),
            effective_reasoning_effort: body_diagnostics.reasoning.effective.clone(),
            reasoning_effort_source: body_diagnostics.reasoning.source.clone(),
            cache_status: if is_success_status { "miss" } else { "error" }.to_string(),
            agent_id: agent_log_id,
            agent_label: agent_log_label,
            upstream_call_kind: Some(
                if request_body_stream_flag(body) {
                    "stream"
                } else {
                    "sync"
                }
                .to_string(),
            ),
            upstream_call_source: Some(source.to_string()),
            cache_key: cache_key.map(ToOwned::to_owned),
            provider_prefix_key: provider_prefix_key.map(ToOwned::to_owned),
            provider_prefix_fingerprint: provider_prefix_fingerprint.map(ToOwned::to_owned),
            provider_cache_diagnostic: None,
            prefix_guard_wait_ms: Some(prefix_guard_wait.wait_ms),
            prefix_guard_wait_reason: prefix_guard_wait.reason.clone(),
            prefix_guard_wait_source: prefix_guard_wait.source.clone(),
            prefix_guard_state_age_ms: prefix_guard_wait.state_age_ms,
            prefix_guard_skip_reason: prefix_guard_wait.skip_reason.clone(),
            prefix_guard_wait_effect: None,
            prefix_lag_classification: None,
            prefix_lag_input_delta_tokens: None,
            prefix_lag_cache_delta_tokens: None,
            prefix_lag_previous_gap_tokens: None,
            prefix_cache_instability_score: prefix_guard_wait.cache_instability_score,
            prefix_seen_bucket_tokens: prefix_guard_wait.seen_bucket_tokens,
            prefix_state_cache_read_tokens: prefix_guard_wait.state_cache_read_tokens,
            status,
            ttft_ms: elapsed,
            upstream_ttft_ms: Some(upstream_ttft_ms(elapsed, Some(prefix_guard_wait.wait_ms))),
            local_prepare_ms: Some(local_prepare_ms),
            upstream_headers_ms: Some(upstream_request_diagnostics.headers_ms),
            upstream_last_attempt_headers_ms: Some(
                upstream_request_diagnostics.last_attempt_headers_ms,
            ),
            upstream_http_version: upstream_request_diagnostics.http_version.clone(),
            upstream_network_path: Some(upstream_request_diagnostics.network_path.to_string()),
            upstream_remote_addr: upstream_request_diagnostics.remote_addr.clone(),
            upstream_pool_diagnostic: upstream_request_diagnostics.pool_diagnostic.clone(),
            upstream_trace_id: upstream_request_diagnostics.upstream_trace_id.clone(),
            upstream_trace_source: upstream_request_diagnostics.upstream_trace_source.clone(),
            upstream_server_timing: upstream_request_diagnostics.server_timing.clone(),
            upstream_timing_source: upstream_request_diagnostics.timing_source.clone(),
            upstream_reported_processing_ms: upstream_request_diagnostics.reported_processing_ms,
            upstream_non_processing_ms: upstream_request_diagnostics.non_processing_ms,
            upstream_first_chunk_ms: None,
            stream_upstream_wait_ms: None,
            stream_client_backpressure_ms: None,
            aggregate_done_ms: None,
            upstream_retry_wait_ms: Some(upstream_request_diagnostics.retry_wait_ms),
            upstream_attempts: Some(upstream_request_diagnostics.attempts),
            request_body_bytes: Some(body_bytes),
            sent_body_bytes: Some(upstream_request_diagnostics.sent_body_bytes.max(body_bytes)),
            request_body_encode_ms: Some(upstream_request_diagnostics.request_body_encode_ms),
            gzip_encode_ms: Some(upstream_request_diagnostics.gzip_encode_ms),
            gzip_attempted: Some(upstream_request_diagnostics.gzip_attempted),
            gzip_fallback_used: Some(upstream_request_diagnostics.gzip_fallback_used),
            upstream_header_wait_class: Some(upstream_header_wait_class(
                upstream_request_diagnostics,
            )),
            total_ms: elapsed,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_shortfall_tokens: None,
            cache_new_tail_gap_tokens: None,
            cache_avoidable_gap_tokens: None,
            cache_provider_unstable_gap_tokens: None,
            provider_cache_token_ratio: None,
            tail_input_items: None,
            tail_message_chars: None,
            tail_tool_call_chars: None,
            tail_tool_output_chars: None,
            tail_largest_tool_output_chars: None,
            tail_tool_output_lines: None,
            tail_tool_output_repeated_line_chars: None,
            tail_tool_output_timestamp_like_count: None,
            tail_tool_output_path_like_count: None,
            tail_tool_output_url_like_count: None,
            tail_tool_output_hash_like_count: None,
            tail_tool_output_json_like_chars: None,
            tail_tool_output_noise_hint: None,
            tail_source: None,
            response_session_reused: Some(used_response_session),
            response_session_candidate_count: Some(
                response_session_reuse_diagnostics.candidate_count,
            ),
            response_session_skip_reason: response_session_reuse_diagnostics.skip_reason.clone(),
            response_session_exact_key_hit: Some(response_session_reuse_diagnostics.exact_key_hit),
            response_session_scope_match_count: Some(
                response_session_reuse_diagnostics.scope_match_count,
            ),
            response_session_append_delta_match: Some(
                response_session_reuse_diagnostics.append_delta_match,
            ),
            response_session_delta_items: Some(response_session_reuse_diagnostics.delta_items),
            response_session_cooldown_active: Some(
                response_session_reuse_diagnostics.cooldown_active,
            ),
            response_session_rejected_status: response_session_reuse_diagnostics.rejected_status,
            session_anchor_hash: None,
            session_anchor_source: None,
            session_anchor_changed: None,
            session_anchor_peer_count: None,
            original_body_bytes: Some(original_body_bytes),
            send_body_bytes: Some(send_body_bytes),
            send_body_is_delta: Some(body_diagnostics.send_body_is_delta),
            payload_too_large_rescue_attempted: Some(
                body_diagnostics.payload_too_large_rescue_attempted,
            ),
            payload_too_large_rescue_used: Some(body_diagnostics.payload_too_large_rescue_used),
            sse_end_reason: None,
            sse_completed_event_seen: None,
            sse_done_marker_seen: None,
            sse_chunks: None,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn record_upstream_transport_failure(
    state: &AppState,
    request_id: &str,
    started: &Instant,
    client_channel: &Channel,
    upstream_channel: &Channel,
    decision: &RouteDecision,
    cache_key: Option<&str>,
    provider_prefix_key: Option<&str>,
    provider_prefix_fingerprint: Option<&str>,
    prefix_guard_wait: &PrefixGuardWaitDiagnostics,
    local_prepare_ms: u64,
    body_diagnostics: &BodyDiagnostics,
    body: &Value,
    used_response_session: bool,
    response_session_reuse_diagnostics: &ResponseSessionReuseDiagnostics,
    requested_model: Option<String>,
    source: &str,
) {
    let elapsed = started.elapsed().as_millis() as u64;
    let body_bytes = serialized_body_len(upstream_channel, body);
    let original_body_bytes = if body_diagnostics.original_body_bytes > 0 {
        body_diagnostics.original_body_bytes
    } else {
        body_bytes
    };
    let send_body_bytes = if body_diagnostics.send_body_bytes > 0 {
        body_diagnostics.send_body_bytes
    } else {
        body_bytes
    };

    let log = RequestLog {
        id: request_id.to_string(),
        at: Utc::now(),
        inbound_request_id: Some(request_id.to_string()),
        upstream_request_id: Some(Uuid::new_v4().to_string()),
        upstream_attempt_index: Some(1),
        upstream_attempt_total: Some(1),
        client_channel: client_channel.label().to_string(),
        upstream_channel: upstream_channel.label().to_string(),
        provider: decision.provider.name.clone(),
        model: decision.model.clone(),
        requested_model,
        agent_reasoning_effort: body_diagnostics.reasoning.agent.clone(),
        configured_reasoning_effort: body_diagnostics.reasoning.configured.clone(),
        effective_reasoning_effort: body_diagnostics.reasoning.effective.clone(),
        reasoning_effort_source: body_diagnostics.reasoning.source.clone(),
        cache_status: "error".to_string(),
        agent_id: None,
        agent_label: None,
        upstream_call_kind: Some(
            if request_body_stream_flag(body) {
                "stream"
            } else {
                "sync"
            }
            .to_string(),
        ),
        upstream_call_source: Some(source.to_string()),
        cache_key: cache_key.map(ToOwned::to_owned),
        provider_prefix_key: provider_prefix_key.map(ToOwned::to_owned),
        provider_prefix_fingerprint: provider_prefix_fingerprint.map(ToOwned::to_owned),
        provider_cache_diagnostic: None,
        prefix_guard_wait_ms: Some(prefix_guard_wait.wait_ms),
        prefix_guard_wait_reason: prefix_guard_wait.reason.clone(),
        prefix_guard_wait_source: prefix_guard_wait.source.clone(),
        prefix_guard_state_age_ms: prefix_guard_wait.state_age_ms,
        prefix_guard_skip_reason: prefix_guard_wait.skip_reason.clone(),
        prefix_guard_wait_effect: None,
        prefix_lag_classification: None,
        prefix_lag_input_delta_tokens: None,
        prefix_lag_cache_delta_tokens: None,
        prefix_lag_previous_gap_tokens: None,
        prefix_cache_instability_score: prefix_guard_wait.cache_instability_score,
        prefix_seen_bucket_tokens: prefix_guard_wait.seen_bucket_tokens,
        prefix_state_cache_read_tokens: prefix_guard_wait.state_cache_read_tokens,
        status: 0,
        ttft_ms: elapsed,
        upstream_ttft_ms: Some(upstream_ttft_ms(elapsed, Some(prefix_guard_wait.wait_ms))),
        local_prepare_ms: Some(local_prepare_ms),
        upstream_headers_ms: Some(0),
        upstream_last_attempt_headers_ms: Some(0),
        upstream_http_version: None,
        upstream_network_path: None,
        upstream_remote_addr: None,
        upstream_pool_diagnostic: None,
        upstream_trace_id: None,
        upstream_trace_source: None,
        upstream_server_timing: None,
        upstream_timing_source: None,
        upstream_reported_processing_ms: None,
        upstream_non_processing_ms: None,
        upstream_first_chunk_ms: None,
        stream_upstream_wait_ms: None,
        stream_client_backpressure_ms: None,
        aggregate_done_ms: None,
        upstream_retry_wait_ms: Some(0),
        upstream_attempts: Some(1),
        request_body_bytes: Some(body_bytes),
        sent_body_bytes: Some(body_bytes),
        request_body_encode_ms: Some(0),
        gzip_encode_ms: Some(0),
        gzip_attempted: Some(false),
        gzip_fallback_used: Some(false),
        upstream_header_wait_class: Some(format!(
            "{}:transport_error",
            if decision.provider.use_system_proxy {
                "system-proxy"
            } else {
                "direct"
            }
        )),
        total_ms: elapsed,
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        cache_shortfall_tokens: None,
        cache_new_tail_gap_tokens: None,
        cache_avoidable_gap_tokens: None,
        cache_provider_unstable_gap_tokens: None,
        provider_cache_token_ratio: None,
        tail_input_items: None,
        tail_message_chars: None,
        tail_tool_call_chars: None,
        tail_tool_output_chars: None,
        tail_largest_tool_output_chars: None,
        tail_tool_output_lines: None,
        tail_tool_output_repeated_line_chars: None,
        tail_tool_output_timestamp_like_count: None,
        tail_tool_output_path_like_count: None,
        tail_tool_output_url_like_count: None,
        tail_tool_output_hash_like_count: None,
        tail_tool_output_json_like_chars: None,
        tail_tool_output_noise_hint: None,
        tail_source: None,
        response_session_reused: Some(used_response_session),
        response_session_candidate_count: Some(response_session_reuse_diagnostics.candidate_count),
        response_session_skip_reason: response_session_reuse_diagnostics.skip_reason.clone(),
        response_session_exact_key_hit: Some(response_session_reuse_diagnostics.exact_key_hit),
        response_session_scope_match_count: Some(
            response_session_reuse_diagnostics.scope_match_count,
        ),
        response_session_append_delta_match: Some(
            response_session_reuse_diagnostics.append_delta_match,
        ),
        response_session_delta_items: Some(response_session_reuse_diagnostics.delta_items),
        response_session_cooldown_active: Some(response_session_reuse_diagnostics.cooldown_active),
        response_session_rejected_status: response_session_reuse_diagnostics.rejected_status,
        session_anchor_hash: None,
        session_anchor_source: None,
        session_anchor_changed: None,
        session_anchor_peer_count: None,
        original_body_bytes: Some(original_body_bytes),
        send_body_bytes: Some(send_body_bytes),
        send_body_is_delta: Some(body_diagnostics.send_body_is_delta),
        payload_too_large_rescue_attempted: Some(
            body_diagnostics.payload_too_large_rescue_attempted,
        ),
        payload_too_large_rescue_used: Some(body_diagnostics.payload_too_large_rescue_used),
        sse_end_reason: Some("transport_error".to_string()),
        sse_completed_event_seen: None,
        sse_done_marker_seen: None,
        sse_chunks: None,
    };
    state.metrics.record_upstream_call(log.clone()).await;
    state.metrics.record_request(log, true).await;
}

async fn lookup_cache(
    state: &AppState,
    keys: &[&str],
    semantic_text: Option<&str>,
    semantic_shape: Option<&str>,
    decision: &RouteDecision,
    config: &AppConfig,
) -> Option<cache::CacheLookup> {
    for key in keys {
        if let Some(hit) = state.cache.lookup_exact(key, &config.cache).await {
            return Some(hit);
        }
    }

    state
        .cache
        .lookup(
            keys[0],
            semantic_text,
            semantic_shape,
            &decision.provider.id,
            &decision.model,
            &config.workspace_fingerprint,
            &config.cache,
        )
        .await
}

async fn cache_hit_response(
    state: &AppState,
    hit: cache::CacheLookup,
    started: Instant,
    request_id: String,
    client_channel: &Channel,
    decision: &RouteDecision,
    requested_model: Option<String>,
    non_sse_compact_compat: bool,
) -> Response {
    let cache_status = match hit.status {
        CacheLookupStatus::Exact => "exact",
        CacheLookupStatus::Semantic => "semantic",
    };
    let elapsed = started.elapsed().as_millis() as u64;
    let cached_usage = provider_usage_from_bytes(&hit.entry.body);
    let saved_tokens = cached_usage
        .input_tokens
        .saturating_add(cached_usage.output_tokens);
    let cache_key = hit.entry.key.clone();
    state.metrics.record_local_proxy_hit(saved_tokens).await;
    state
        .metrics
        .record_request(
            RequestLog {
                id: request_id,
                at: Utc::now(),
                inbound_request_id: None,
                upstream_request_id: None,
                upstream_attempt_index: None,
                upstream_attempt_total: None,
                client_channel: client_channel.label().to_string(),
                upstream_channel: decision.upstream_channel.label().to_string(),
                provider: decision.provider.name.clone(),
                model: decision.model.clone(),
                requested_model,
                agent_reasoning_effort: None,
                configured_reasoning_effort: None,
                effective_reasoning_effort: None,
                reasoning_effort_source: None,
                cache_status: cache_status.to_string(),
                agent_id: None,
                agent_label: None,
                upstream_call_kind: Some("cache".to_string()),
                upstream_call_source: Some("local_cache".to_string()),
                cache_key: Some(cache_key),
                provider_prefix_key: None,
                provider_prefix_fingerprint: None,
                provider_cache_diagnostic: Some("local-replay".to_string()),
                prefix_guard_wait_ms: None,
                prefix_guard_wait_reason: None,
                prefix_guard_wait_source: None,
                prefix_guard_state_age_ms: None,
                prefix_guard_skip_reason: None,
                prefix_guard_wait_effect: None,
                prefix_lag_classification: None,
                prefix_lag_input_delta_tokens: None,
                prefix_lag_cache_delta_tokens: None,
                prefix_lag_previous_gap_tokens: None,
                prefix_cache_instability_score: None,
                prefix_seen_bucket_tokens: None,
                prefix_state_cache_read_tokens: None,
                status: hit.entry.status,
                ttft_ms: elapsed,
                upstream_ttft_ms: Some(0),
                local_prepare_ms: None,
                upstream_headers_ms: None,
                upstream_last_attempt_headers_ms: None,
                upstream_http_version: None,
                upstream_network_path: None,
                upstream_remote_addr: None,
                upstream_pool_diagnostic: None,
                upstream_trace_id: None,
                upstream_trace_source: None,
                upstream_server_timing: None,
                upstream_timing_source: None,
                upstream_reported_processing_ms: None,
                upstream_non_processing_ms: None,
                upstream_first_chunk_ms: None,
                stream_upstream_wait_ms: None,
                stream_client_backpressure_ms: None,
                aggregate_done_ms: None,
                upstream_retry_wait_ms: None,
                upstream_attempts: None,
                request_body_bytes: None,
                sent_body_bytes: None,
                request_body_encode_ms: None,
                gzip_encode_ms: None,
                gzip_attempted: None,
                gzip_fallback_used: None,
                upstream_header_wait_class: None,
                total_ms: elapsed,
                input_tokens: cached_usage
                    .has_usage()
                    .then_some(cached_usage.input_tokens),
                output_tokens: cached_usage
                    .has_usage()
                    .then_some(cached_usage.output_tokens),
                cache_read_tokens: cached_usage
                    .has_usage()
                    .then_some(cached_usage.cache_read_tokens),
                cache_shortfall_tokens: cached_usage
                    .has_usage()
                    .then_some(provider_cache_shortfall(&cached_usage)),
                cache_new_tail_gap_tokens: None,
                cache_avoidable_gap_tokens: None,
                cache_provider_unstable_gap_tokens: None,
                provider_cache_token_ratio: provider_cache_ratio(&cached_usage),
                tail_input_items: None,
                tail_message_chars: None,
                tail_tool_call_chars: None,
                tail_tool_output_chars: None,
                tail_largest_tool_output_chars: None,
                tail_tool_output_lines: None,
                tail_tool_output_repeated_line_chars: None,
                tail_tool_output_timestamp_like_count: None,
                tail_tool_output_path_like_count: None,
                tail_tool_output_url_like_count: None,
                tail_tool_output_hash_like_count: None,
                tail_tool_output_json_like_chars: None,
                tail_tool_output_noise_hint: None,
                tail_source: None,
                response_session_reused: None,
                response_session_candidate_count: None,
                response_session_skip_reason: None,
                response_session_exact_key_hit: None,
                response_session_scope_match_count: None,
                response_session_append_delta_match: None,
                response_session_delta_items: None,
                response_session_cooldown_active: None,
                response_session_rejected_status: None,
                session_anchor_hash: None,
                session_anchor_source: None,
                session_anchor_changed: None,
                session_anchor_peer_count: None,
                original_body_bytes: None,
                send_body_bytes: None,
                send_body_is_delta: None,
                payload_too_large_rescue_attempted: None,
                payload_too_large_rescue_used: None,
                sse_end_reason: None,
                sse_completed_event_seen: None,
                sse_done_marker_seen: None,
                sse_chunks: None,
            },
            false,
        )
        .await;
    let body = normalize_response_body_for_client(
        client_channel,
        &hit.entry.content_type,
        hit.entry.body,
        non_sse_compact_compat,
    );
    raw_response(hit.entry.status, &hit.entry.content_type, body)
}

async fn insert_cache_entries(
    state: &AppState,
    keys: Vec<String>,
    semantic_text: Option<String>,
    semantic_shape: Option<String>,
    content_type: String,
    status: u16,
    body: Vec<u8>,
    decision: &RouteDecision,
    config: &AppConfig,
) {
    for key in keys {
        let entry = CacheEntry {
            key,
            semantic_text: semantic_text.clone(),
            semantic_shape: semantic_shape.clone(),
            semantic_vector: Vec::new(),
            content_type: content_type.clone(),
            status,
            body: body.clone(),
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(config.cache.max_age_seconds as i64),
            provider_id: decision.provider.id.clone(),
            model: decision.model.clone(),
            workspace_fingerprint: Some(config.workspace_fingerprint.clone()),
        };
        if let Err(err) = state.cache.insert(entry, &config.cache).await {
            state
                .metrics
                .record_error("cache_insert", &err.to_string())
                .await;
        }
    }
}

#[cfg(test)]
fn responses_prewarm_bucket_count(
    channel: &Channel,
    usage_record: Option<&UsageRecord>,
    gap_breakdown: Option<&ProviderCacheGapBreakdown>,
) -> u64 {
    if !matches!(channel, Channel::Responses) {
        return 0;
    }
    let (Some(record), Some(gap)) = (usage_record, gap_breakdown) else {
        return 0;
    };
    if record.input_tokens < 4096 || record.cache_read_tokens == 0 {
        return 0;
    }
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    let net_save_tokens = gap
        .avoidable_tokens
        .max(gap.new_tail_tokens)
        .saturating_sub(record.input_tokens.saturating_sub(record.cache_read_tokens));
    if net_save_tokens < BACKGROUND_PREWARM_MIN_NET_SAVE_TOKENS {
        return 0;
    }

    // Avoidable gaps are already-seen prefix buckets that failed to hit. They
    // get first claim on follow-up prewarm, but this app is cost-first: one
    // user request may add at most one extra upstream request after the base
    // prewarm. More bucket filling made hit-rate graphs prettier while
    // increasing real upstream spend.
    if gap.avoidable_tokens > 0 {
        let max_avoidable = if ratio >= 0.98 {
            8192
        } else if ratio >= 0.95 {
            4096
        } else if ratio >= 0.90 {
            2048
        } else {
            0
        };
        if max_avoidable > 0 && gap.avoidable_tokens <= max_avoidable {
            return BACKGROUND_PREWARM_MAX_EXTRA_BUCKET_REQUESTS;
        }
        return 0;
    }

    // v0.0.52 cost-first baseline: pure new tails must warm naturally through
    // the user's real stream. Do not add a companion non-streaming request.
    0
}

#[cfg(test)]
fn responses_foreground_prewarm_settle_delay(
    trigger_tokens: u64,
    record: &UsageRecord,
) -> TokioDuration {
    let ratio = provider_cache_ratio(record).unwrap_or(0.0);
    if ratio < 0.90 {
        return TokioDuration::from_millis(500);
    }
    if trigger_tokens >= 4096 {
        TokioDuration::from_millis(1800)
    } else if trigger_tokens >= 1536 {
        TokioDuration::from_millis(1400)
    } else if trigger_tokens >= 1024 {
        TokioDuration::from_millis(1200)
    } else if trigger_tokens >= 512 {
        TokioDuration::from_millis(950)
    } else {
        TokioDuration::from_millis(650)
    }
}

#[cfg(test)]
fn foreground_prewarm_responses_missing_state_decision(body: &Value) -> ForegroundPrewarmDecision {
    let _ = body;
    // Cost-first rule from v0.0.52: do not send a companion sync request before
    // the first real stream just because the in-memory prefix state is missing.
    ForegroundPrewarmDecision::Skip("missing_prefix_state_cost_first")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
enum ForegroundPrewarmDecision {
    Run(u64),
    Skip(&'static str),
}

#[cfg(test)]
fn foreground_prewarm_responses_decision(
    state: &PrefixWarmState,
    estimated_current_bucket: Option<u64>,
) -> ForegroundPrewarmDecision {
    let avoidable_decision = foreground_prewarm_responses_avoidable_tokens(state);
    if matches!(avoidable_decision, ForegroundPrewarmDecision::Run(_)) {
        return avoidable_decision;
    }
    let _ = estimated_current_bucket;
    avoidable_decision
}

#[cfg(test)]
fn foreground_prewarm_responses_avoidable_tokens(
    state: &PrefixWarmState,
) -> ForegroundPrewarmDecision {
    if state.input_tokens < 4096 {
        return ForegroundPrewarmDecision::Skip("input_below_4k");
    }
    let avoidable = state
        .avoidable_shortfall_tokens_128
        .max(state.avoidable_shortfall_tokens);
    if state.cache_read_tokens == 0 && avoidable == 0 {
        return ForegroundPrewarmDecision::Skip("cache_read_zero");
    }
    if avoidable == 0 {
        return ForegroundPrewarmDecision::Skip("no_avoidable_gap");
    }
    let effective_cache_read_tokens = if state.cache_read_tokens == 0 && avoidable > 0 {
        state
            .seen_bucket_tokens
            .max(state.seen_bucket_tokens_128)
            .min(provider_cache_bucket_max_128(state.input_tokens))
    } else {
        state.cache_read_tokens
    };
    let ratio = effective_cache_read_tokens as f64 / state.input_tokens.max(1) as f64;
    if !(128..=90_000).contains(&avoidable) {
        return ForegroundPrewarmDecision::Skip("no_avoidable_gap");
    }
    if ratio < 0.90 {
        return ForegroundPrewarmDecision::Skip("ratio_low");
    }
    let trigger_tokens = avoidable;
    let cold_read_regression = state.cache_read_tokens == 0
        && avoidable >= 4096
        && effective_cache_read_tokens >= 32_000
        && ratio >= 0.90;
    let high_waterline = effective_cache_read_tokens >= 90_000 && ratio >= 0.99;
    let near_full = ratio >= 0.992;
    let enough_signal = if trigger_tokens <= 512 {
        if state.input_tokens < 32_000 {
            if state.input_tokens < 8192 && state.small_gap_recovery_streak >= 1 {
                ratio >= 0.90
            } else if state.small_gap_recovery_streak >= 1 {
                ratio >= 0.955
            } else {
                (state.avoidable_shortfall_streak >= 2 || state.small_gap_recovery_streak >= 1)
                    && ratio >= 0.955
            }
        } else if state.small_gap_recovery_streak >= 1 && ratio >= 0.975 {
            true
        } else if high_waterline {
            state.avoidable_shortfall_streak >= 1 || state.small_gap_recovery_streak >= 1
        } else if near_full {
            state.avoidable_shortfall_streak >= 2 || state.small_gap_recovery_streak >= 1
        } else {
            state.avoidable_shortfall_streak >= 4 && ratio >= 0.985
        }
    } else if trigger_tokens <= 2048 {
        if state.input_tokens < 32_000 {
            (state.avoidable_shortfall_streak >= 1 || state.small_gap_recovery_streak >= 1)
                && ratio >= 0.96
        } else if state.avoidable_shortfall_streak >= 1 && ratio >= 0.955 {
            true
        } else if state.small_gap_recovery_streak >= 1 && ratio >= 0.975 {
            true
        } else if high_waterline {
            state.avoidable_shortfall_streak >= 1 || state.small_gap_recovery_streak >= 1
        } else if near_full {
            state.avoidable_shortfall_streak >= 2 || state.small_gap_recovery_streak >= 1
        } else {
            state.avoidable_shortfall_streak >= 2 && ratio >= 0.975
        }
    } else if trigger_tokens <= 4096 {
        if state.small_gap_recovery_streak >= 1 && ratio >= 0.90 {
            true
        } else if high_waterline {
            state.avoidable_shortfall_streak >= 1 || state.small_gap_recovery_streak >= 1
        } else {
            state.avoidable_shortfall_streak >= 2 && ratio >= 0.975
        }
    } else if trigger_tokens <= 16_384 {
        cold_read_regression
            || (effective_cache_read_tokens >= 128_000
                && state.avoidable_shortfall_streak >= 3
                && ratio >= 0.95)
    } else {
        cold_read_regression || (effective_cache_read_tokens >= 128_000 && ratio >= 0.90)
    };
    if enough_signal {
        ForegroundPrewarmDecision::Run(trigger_tokens)
    } else {
        ForegroundPrewarmDecision::Skip("streak_low")
    }
}

#[cfg(test)]
fn should_background_prewarm(
    config: &AppConfig,
    channel: &Channel,
    usage_record: Option<&UsageRecord>,
    gap_breakdown: Option<&ProviderCacheGapBreakdown>,
) -> bool {
    let _ = (config, channel, usage_record, gap_breakdown);
    false
}

async fn select_provider_api_key(
    state: &AppState,
    provider_id: &str,
    exclude_key_id: Option<&str>,
    affinity_key: Option<&str>,
) -> Result<SelectedProviderKey> {
    let affinity_map_key = affinity_key.map(|key| provider_key_affinity_map_key(provider_id, key));
    let preferred_key_id = if exclude_key_id.is_none() {
        if let Some(map_key) = affinity_map_key.as_deref() {
            state
                .provider_key_affinity
                .lock()
                .await
                .get(map_key)
                .cloned()
        } else {
            None
        }
    } else {
        None
    };
    let selected = {
        let mut config = state.config.write().await;
        config
            .select_provider_key_for_request(
                provider_id,
                preferred_key_id.as_deref(),
                exclude_key_id,
            )
            .with_context(|| format!("failed to select provider key for {provider_id}"))?
    };
    let Some(selected) = selected else {
        return Err(anyhow!("provider API key is not configured"));
    };
    if let (Some(map_key), Some(key_id)) = (affinity_map_key, selected.key_id.as_ref()) {
        state
            .provider_key_affinity
            .lock()
            .await
            .insert(map_key, key_id.clone());
    }
    Ok(selected)
}

fn provider_key_affinity_map_key(provider_id: &str, affinity_key: &str) -> String {
    format!("{provider_id}\0{affinity_key}")
}

async fn clear_provider_key_affinity(
    state: &AppState,
    provider_id: &str,
    affinity_key: Option<&str>,
    key_id: Option<&str>,
) {
    let Some(affinity_key) = affinity_key else {
        return;
    };
    let Some(key_id) = key_id else {
        return;
    };
    let map_key = provider_key_affinity_map_key(provider_id, affinity_key);
    let mut affinities = state.provider_key_affinity.lock().await;
    if affinities
        .get(&map_key)
        .map(|current| current == key_id)
        .unwrap_or(false)
    {
        affinities.remove(&map_key);
    }
}

async fn note_selected_provider_key_status(
    state: &AppState,
    provider_id: &str,
    selected: &SelectedProviderKey,
    status: u16,
    error_summary: Option<&str>,
) {
    if selected.key_id.is_none() {
        return;
    }
    let Ok(status_code) = StatusCode::from_u16(status) else {
        return;
    };
    let mut config = state.config.write().await;
    if status_code.is_success() {
        config.mark_provider_key_success(provider_id, selected.key_id.as_deref());
        return;
    }
    if is_provider_key_failure_status(status_code)
        || error_summary
            .map(is_provider_key_failure_message)
            .unwrap_or(false)
    {
        let message = error_summary
            .filter(|message| !message.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("upstream returned HTTP {status_code}"));
        config.mark_provider_key_failure(provider_id, selected.key_id.as_deref(), &message, true);
        if let Err(err) = config.save(&state.config_path) {
            state
                .metrics
                .record_error("multi_key_save", &err.to_string())
                .await;
        }
    }
}

fn is_provider_key_failure_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED
            | StatusCode::PAYMENT_REQUIRED
            | StatusCode::FORBIDDEN
            | StatusCode::TOO_MANY_REQUESTS
    )
}

fn is_provider_key_failure_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("insufficient")
        || lower.contains("quota")
        || lower.contains("balance")
        || lower.contains("billing")
        || lower.contains("credit")
        || lower.contains("invalid api key")
        || lower.contains("invalid_api_key")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || message.contains("\u{4f59}\u{989d}\u{4e0d}\u{8db3}")
        || message.contains("\u{989d}\u{5ea6}\u{4e0d}\u{8db3}")
        || message.contains("\u{6b20}\u{8d39}")
        || message.contains("\u{4f59}\u{989d}")
}

async fn try_retry_with_next_provider_key(
    state: &AppState,
    provider_id: &str,
    selected: &SelectedProviderKey,
    status: u16,
    affinity_key: Option<&str>,
) -> Option<SelectedProviderKey> {
    let status = StatusCode::from_u16(status).ok()?;
    if selected.key_id.is_none() || !is_provider_key_failure_status(status) {
        return None;
    }
    {
        let mut config = state.config.write().await;
        config.mark_provider_key_failure(
            provider_id,
            selected.key_id.as_deref(),
            &format!("upstream returned HTTP {status}"),
            true,
        );
        if let Err(err) = config.save(&state.config_path) {
            state
                .metrics
                .record_error("multi_key_save", &err.to_string())
                .await;
        }
    }
    clear_provider_key_affinity(state, provider_id, affinity_key, selected.key_id.as_deref()).await;
    match select_provider_api_key(state, provider_id, selected.key_id.as_deref(), affinity_key)
        .await
    {
        Ok(next) if next.key_id != selected.key_id => Some(next),
        _ => None,
    }
}

async fn send_main_upstream_request(
    state: &AppState,
    use_system_proxy: bool,
    url: &str,
    api_key: &str,
    channel: &Channel,
    body: &Value,
    inbound_headers: &HeaderMap,
    custom_user_agent: Option<&str>,
    sync_responses_main: bool,
    request_body_gzip_enabled: bool,
    no_internal_retry: bool,
) -> reqwest::Result<UpstreamSendOutcome> {
    let _ = (sync_responses_main, no_internal_retry);
    let max_attempts = Some(1);
    send_upstream_request_to_url_with_diagnostics(
        state,
        use_system_proxy,
        url,
        api_key,
        channel,
        body,
        inbound_headers,
        custom_user_agent,
        max_attempts,
        request_body_gzip_enabled,
    )
    .await
}

fn build_upstream_request_headers(
    inbound_headers: &HeaderMap,
    api_key: &str,
    channel: &Channel,
    stream: bool,
    custom_user_agent: Option<&str>,
    content_encoding_gzip: bool,
) -> HeaderMap {
    let mut outbound = HeaderMap::new();
    for (name, value) in inbound_headers {
        let name_text = name.as_str();
        if upstream_header_is_blocked(name_text)
            || name == header::CONTENT_TYPE
            || name == header::CONTENT_ENCODING
            || name == header::ACCEPT_ENCODING
            || (name == header::USER_AGENT && custom_user_agent.is_some())
        {
            continue;
        }
        outbound.append(name.clone(), value.clone());
    }

    outbound.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("Bearer {api_key}")) {
        outbound.insert(header::AUTHORIZATION, value);
    }
    if let Ok(value) = HeaderValue::from_str(api_key) {
        outbound.insert("x-api-key", value);
    }
    if let Some(user_agent) = custom_user_agent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| HeaderValue::from_str(value).ok())
    {
        outbound.insert(header::USER_AGENT, user_agent);
    }
    if stream {
        outbound.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );
    }
    outbound.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    if content_encoding_gzip {
        outbound.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    if matches!(channel, Channel::Anthropic) && !outbound.contains_key("anthropic-version") {
        outbound.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    }
    outbound
}

fn upstream_header_is_blocked(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
            | "authorization"
            | "x-api-key"
            | "x-goog-api-key"
            | "forwarded"
            | "x-forwarded-for"
            | "x-forwarded-host"
            | "x-forwarded-port"
            | "x-forwarded-proto"
            | "cf-connecting-ip"
            | "cf-ipcountry"
            | "cf-ray"
            | "cf-visitor"
            | "true-client-ip"
            | "fastly-client-ip"
            | "x-azure-clientip"
            | "x-azure-fdid"
            | "x-azure-ref"
            | "akamai-origin-hop"
            | "x-akamai-config-log-detail"
            | "x-request-id"
            | "x-correlation-id"
            | "x-trace-id"
            | "x-amzn-trace-id"
            | "x-b3-traceid"
            | "x-b3-spanid"
            | "x-b3-parentspanid"
            | "x-b3-sampled"
            | "b3"
            | "traceparent"
            | "tracestate"
    ) || name.starts_with("x-forwarded-")
        || name.starts_with("cf-")
        || name.starts_with("x-b3-")
}

async fn send_upstream_request_to_url_with_diagnostics(
    state: &AppState,
    use_system_proxy: bool,
    url: &str,
    api_key: &str,
    channel: &Channel,
    body: &Value,
    inbound_headers: &HeaderMap,
    custom_user_agent: Option<&str>,
    max_attempts_override: Option<usize>,
    request_body_gzip_enabled: bool,
) -> reqwest::Result<UpstreamSendOutcome> {
    const MAX_ATTEMPTS: usize = 3;
    let max_attempts = max_attempts_override
        .unwrap_or(MAX_ATTEMPTS)
        .clamp(1, MAX_ATTEMPTS);
    let body_encode_started = Instant::now();
    let original_body = upstream_request_body_bytes(channel, body);
    let request_body_encode_ms = body_encode_started.elapsed().as_millis() as u64;
    let original_body_len = original_body.len() as u64;
    let gzip_cooldown_key = request_body_gzip_cooldown_key(url, channel);
    let gzip_cooldown_active = request_body_gzip_enabled
        && request_body_gzip_cooldown_active(state, &gzip_cooldown_key).await;
    let gzip_threshold = request_body_gzip_threshold_bytes(channel);
    let use_gzip =
        request_body_gzip_enabled && !gzip_cooldown_active && original_body.len() >= gzip_threshold;
    let gzip_encode_started = Instant::now();
    let compressed_body = if use_gzip {
        gzip_request_body(&original_body).ok()
    } else {
        None
    };
    let gzip_encode_ms = if use_gzip {
        gzip_encode_started.elapsed().as_millis() as u64
    } else {
        0
    };
    let mut diagnostics = UpstreamRequestDiagnostics {
        network_path: if use_system_proxy {
            "system-proxy"
        } else {
            "direct"
        },
        request_body_bytes: original_body_len,
        request_body_encode_ms,
        gzip_encode_ms,
        gzip_attempted: compressed_body.is_some(),
        sent_body_bytes: compressed_body
            .as_ref()
            .map(|body| body.len() as u64)
            .unwrap_or(original_body_len),
        ..UpstreamRequestDiagnostics::default()
    };
    let mut gzip_disabled_after_fallback = false;

    for attempt in 0..max_attempts {
        diagnostics.attempts += 1;
        if attempt > 0 {
            state.metrics.record_retry().await;
        }

        let sending_gzip = compressed_body.is_some() && !gzip_disabled_after_fallback;
        let outbound_headers = build_upstream_request_headers(
            inbound_headers,
            api_key,
            channel,
            request_body_stream_flag(body),
            custom_user_agent,
            sending_gzip,
        );
        let request = state
            .upstream_client(use_system_proxy)
            .post(url)
            .headers(outbound_headers);
        let request = if sending_gzip {
            request.body(
                compressed_body
                    .clone()
                    .unwrap_or_else(|| original_body.clone()),
            )
        } else if matches!(channel, Channel::Responses) {
            request.body(original_body.clone())
        } else {
            request.body(original_body.clone())
        };

        let send_started = Instant::now();
        match request.send().await {
            Ok(response) => {
                let headers_ms = send_started.elapsed().as_millis() as u64;
                diagnostics.headers_ms += headers_ms;
                observe_upstream_response_timing(&mut diagnostics, &response, headers_ms);
                if sending_gzip && should_retry_without_gzip(response.status()) {
                    note_request_body_gzip_fallback(state, &gzip_cooldown_key).await;
                    if attempt + 1 < max_attempts {
                        diagnostics.gzip_fallback_used = true;
                        diagnostics.sent_body_bytes = original_body_len;
                        gzip_disabled_after_fallback = true;
                        continue;
                    }
                }
                if should_retry_status(response.status())
                    && attempt + 1 < max_attempts_for_status(response.status()).min(max_attempts)
                {
                    let delay = upstream_retry_delay(
                        response.status(),
                        response
                            .headers()
                            .get("retry-after")
                            .and_then(|value| value.to_str().ok()),
                        attempt,
                    );
                    diagnostics.retry_wait_ms += delay.as_millis() as u64;
                    sleep(delay).await;
                    continue;
                }
                return Ok(UpstreamSendOutcome {
                    response,
                    diagnostics,
                });
            }
            Err(err) => {
                let headers_ms = send_started.elapsed().as_millis() as u64;
                diagnostics.headers_ms += headers_ms;
                diagnostics.last_attempt_headers_ms = headers_ms;
                diagnostics.http_version = None;
                diagnostics.remote_addr = None;
                diagnostics.pool_diagnostic = None;
                diagnostics.server_timing = None;
                diagnostics.timing_source = None;
                diagnostics.reported_processing_ms = None;
                diagnostics.non_processing_ms = None;
                if attempt + 1 >= max_attempts {
                    return Err(err);
                }
                let delay = network_retry_delay(attempt);
                diagnostics.retry_wait_ms += delay.as_millis() as u64;
                sleep(delay).await;
            }
        }
    }

    unreachable!("retry loop always returns on the final attempt")
}

async fn read_upstream_body_with_diagnostics(
    upstream: reqwest::Response,
    content_type: &str,
    started: Instant,
    headers_at_ms: u64,
) -> reqwest::Result<UpstreamBodyReadOutcome> {
    if is_text_event_stream(content_type) {
        let mut stream = upstream.bytes_stream();
        let mut body = Vec::new();
        let mut first_chunk_at_ms = None;
        let mut chunks = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if first_chunk_at_ms.is_none() {
                first_chunk_at_ms = Some(started.elapsed().as_millis() as u64);
            }
            chunks += 1;
            body.extend_from_slice(&chunk);
        }
        let aggregate_done_ms = started.elapsed().as_millis() as u64;
        Ok(UpstreamBodyReadOutcome {
            bytes: body,
            first_chunk_ms: first_chunk_at_ms.map(|at| at.saturating_sub(headers_at_ms)),
            aggregate_done_ms,
            sse_chunks: Some(chunks),
        })
    } else {
        let bytes = upstream.bytes().await?.to_vec();
        Ok(UpstreamBodyReadOutcome {
            bytes,
            first_chunk_ms: None,
            aggregate_done_ms: started.elapsed().as_millis() as u64,
            sse_chunks: None,
        })
    }
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT || status.is_server_error()
}

fn max_attempts_for_status(status: reqwest::StatusCode) -> usize {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        1
    } else if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        || status == reqwest::StatusCode::GATEWAY_TIMEOUT
    {
        2
    } else {
        3
    }
}

fn upstream_retry_delay(
    status: reqwest::StatusCode,
    retry_after: Option<&str>,
    attempt: usize,
) -> TokioDuration {
    let default = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        match attempt {
            0 => TokioDuration::from_secs(5),
            1 => TokioDuration::from_secs(15),
            _ => TokioDuration::from_secs(30),
        }
    } else {
        TokioDuration::from_millis(600 * (attempt as u64 + 1))
    };

    retry_after
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| TokioDuration::from_secs(seconds.min(30)).max(default))
        .unwrap_or(default)
}

fn network_retry_delay(attempt: usize) -> TokioDuration {
    TokioDuration::from_millis(350 * (attempt as u64 + 1))
}

fn serialize_responses_body_for_provider_prefix(body: &Value) -> String {
    const ORDERED_KEYS: [&str; 22] = [
        "model",
        "prompt_cache_key",
        "prompt_cache_retention",
        "instructions",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "reasoning",
        "text",
        "response_format",
        "temperature",
        "top_p",
        "max_output_tokens",
        "include",
        "stream",
        "store",
        "service_tier",
        "truncation",
        "input",
        "previous_response_id",
        "metadata",
        "user",
    ];

    let Some(map) = body.as_object() else {
        return serde_json::to_string(body).unwrap_or_else(|_| "null".to_string());
    };

    let mut parts = Vec::with_capacity(map.len());
    for key in ORDERED_KEYS {
        if let Some(value) = map.get(key) {
            parts.push(format_json_member(key, value));
        }
    }

    let mut remaining = map
        .keys()
        .filter(|key| !ORDERED_KEYS.contains(&key.as_str()))
        .collect::<Vec<_>>();
    remaining.sort();
    for key in remaining {
        if let Some(value) = map.get(key) {
            parts.push(format_json_member(key, value));
        }
    }

    format!("{{{}}}", parts.join(","))
}

fn serialize_chat_body_for_provider_prefix(body: &Value) -> String {
    const ORDERED_KEYS: [&str; 20] = [
        "model",
        "prompt_cache_key",
        "prompt_cache_retention",
        "messages",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "response_format",
        "max_tokens",
        "max_completion_tokens",
        "temperature",
        "top_p",
        "seed",
        "presence_penalty",
        "frequency_penalty",
        "logit_bias",
        "stop",
        "stream",
        "store",
        "service_tier",
    ];

    let Some(map) = body.as_object() else {
        return serde_json::to_string(body).unwrap_or_else(|_| "null".to_string());
    };

    let mut parts = Vec::with_capacity(map.len());
    for key in ORDERED_KEYS {
        if let Some(value) = map.get(key) {
            parts.push(format_json_member(key, value));
        }
    }

    let mut remaining = map
        .keys()
        .filter(|key| !ORDERED_KEYS.contains(&key.as_str()))
        .collect::<Vec<_>>();
    remaining.sort();
    for key in remaining {
        if let Some(value) = map.get(key) {
            parts.push(format_json_member(key, value));
        }
    }

    format!("{{{}}}", parts.join(","))
}

fn format_json_member(key: &str, value: &Value) -> String {
    let key = serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string());
    let value = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    format!("{key}:{value}")
}

async fn stream_upstream(
    state: Arc<AppState>,
    upstream: reqwest::Response,
    content_type: String,
    status: u16,
    started: Instant,
    request_id: String,
    client_channel: Channel,
    decision: RouteDecision,
    eligible: bool,
    cache_keys: Vec<String>,
    metrics_cache_key: String,
    semantic_text: Option<String>,
    semantic_shape: Option<String>,
    provider_prefix_key: Option<String>,
    provider_prefix_fingerprint: Option<String>,
    provider_prefix_family_key: Option<String>,
    route_affinity_key: Option<String>,
    config: AppConfig,
    _prefix_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    prefix_state_key: Option<String>,
    _response_session_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    response_session_key: Option<String>,
    response_session_scope_key: Option<String>,
    full_response_input: Option<Value>,
    used_response_session: bool,
    retried_full_response: bool,
    active_upstream_body: Value,
    diagnostics: BodyDiagnostics,
    tail_input_diagnostics: TailInputDiagnostics,
    session_anchor_diagnostics: SessionAnchorDiagnostics,
    response_session_reuse_diagnostics: ResponseSessionReuseDiagnostics,
    codex_chat_tool_context: Option<transform_codex_chat::CodexToolContext>,
    agent_log_id: Option<String>,
    agent_log_label: Option<String>,
    requested_model: Option<String>,
    prefix_guard_wait: PrefixGuardWaitDiagnostics,
    local_prepare_ms: u64,
    upstream_request_diagnostics: UpstreamRequestDiagnostics,
    upstream_response_headers_at_ms: u64,
) -> Response {
    // Streaming must behave like a normal proxy: do not hold prefix/session locks
    // for the whole SSE response. The guarded section has already covered request
    // preparation and send; holding it through output serializes unrelated turns
    // and inflates TTFT/total time.
    drop(_prefix_guard);
    drop(_response_session_guard);

    let convert_codex_chat_sse_to_responses_sse = matches!(client_channel, Channel::Responses)
        && matches!(decision.upstream_channel, Channel::Chat);
    let raw_stream = upstream.bytes_stream();
    let mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>> =
        if convert_codex_chat_sse_to_responses_sse {
            let tool_context = codex_chat_tool_context.unwrap_or_default();
            Box::pin(
                streaming_codex_chat::create_responses_sse_stream_from_chat_with_context(
                    raw_stream,
                    tool_context,
                )
                .map(|item| item.map_err(|err| err.to_string())),
            )
        } else {
            Box::pin(raw_stream.map(|item| item.map_err(|err| err.to_string())))
        };
    let mut first_chunk_at: Option<u64> = None;
    let mut cache_body = Vec::new();
    let mut stream_metadata = SseStreamMetadataCollector::default();
    let mut sse_chunks = 0u64;
    let mut sse_end_reason = "upstream_eof".to_string();
    let mut stream_upstream_wait_ms = 0u64;
    let mut stream_client_backpressure_ms = 0u64;
    let state_for_stream = state.clone();
    let response_content_type = if convert_codex_chat_sse_to_responses_sse {
        "text/event-stream".to_string()
    } else {
        content_type.clone()
    };
    let content_type_for_cache = response_content_type.clone();
    let stream_body = async_stream::stream! {
        loop {
            let upstream_wait_started = Instant::now();
            let next_chunk = stream.next().await;
            stream_upstream_wait_ms = stream_upstream_wait_ms
                .saturating_add(upstream_wait_started.elapsed().as_millis() as u64);
            let Some(chunk) = next_chunk else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    state_for_stream
                        .metrics
                        .record_error("upstream_stream", &err)
                        .await;
                    sse_end_reason = "upstream_stream_error".to_string();
                    break;
                }
            };
            if first_chunk_at.is_none() {
                first_chunk_at = Some(started.elapsed().as_millis() as u64);
            }
            sse_chunks += 1;
            stream_metadata.process_chunk(&chunk);
            if eligible {
                cache_body.extend_from_slice(&chunk);
            }
            let client_backpressure_started = Instant::now();
            yield Ok::<Bytes, Infallible>(chunk);
            stream_client_backpressure_ms = stream_client_backpressure_ms
                .saturating_add(client_backpressure_started.elapsed().as_millis() as u64);
        }
        stream_metadata.finish();
        if stream_metadata.error_event_seen && sse_end_reason == "upstream_eof" {
            sse_end_reason = "upstream_sse_error".to_string();
        }
        let stream_success_for_cache =
            (200..300).contains(&status) && sse_end_reason == "upstream_eof";
        if !stream_success_for_cache && sse_end_reason == "upstream_sse_error" {
            state_for_stream
                .metrics
                .record_error("upstream_sse_error", &sse_end_reason)
                .await;
        }
        let total_ms = started.elapsed().as_millis() as u64;
        let usage_observation = if stream_success_for_cache {
            collect_provider_usage_from_record(
                &state_for_stream,
                stream_metadata.usage.clone(),
                &decision,
                prefix_state_key.as_deref(),
                used_response_session,
            ).await
        } else {
            None
        };
        let usage_record = usage_observation.as_ref().map(|item| item.raw.clone());
        let prefix_usage_record = usage_observation
            .as_ref()
            .map(|item| item.effective.clone());
        if stream_success_for_cache {
            update_response_session_with_id(
                &state_for_stream,
                response_session_key.as_deref(),
                response_session_scope_key.as_deref(),
                full_response_input.as_ref(),
                stream_metadata.response_id.clone(),
            ).await;
        }
        let gap_breakdown = provider_cache_gap_breakdown_with_guard(
            &state_for_stream,
            prefix_state_key.as_deref(),
            provider_prefix_family_key.as_deref(),
            usage_record.as_ref(),
            Some(&tail_input_diagnostics),
            Some(&prefix_guard_wait),
        ).await;
        let prefix_lag = prefix_lag_diagnostics(
            &state_for_stream,
            prefix_state_key.as_deref(),
            usage_record.as_ref(),
            gap_breakdown.as_ref(),
            &prefix_guard_wait,
            &tail_input_diagnostics,
        ).await;
        let session_cache_regressed = if stream_success_for_cache {
            update_provider_prefix_state_with_tail_and_guard(
                &state_for_stream,
                prefix_state_key.as_deref(),
                provider_prefix_family_key.as_deref(),
                prefix_usage_record.as_ref(),
                &tail_input_diagnostics,
                used_response_session,
                retried_full_response,
                prefix_guard_wait.budget_exhausted,
            ).await
        } else {
            false
        };
        if session_cache_regressed {
            let stale_response_id = previous_response_id_from_request(&active_upstream_body);
            clear_response_session_reference(
                &state_for_stream,
                response_session_key.as_deref(),
                stale_response_id.as_deref(),
            )
            .await;
        }
        if stream_success_for_cache {
            note_provider_route_affinity(
                &state_for_stream,
                route_affinity_key.as_deref(),
                &decision.provider.id,
            )
            .await;
            clear_prefix_error_cooldown(&state_for_stream, prefix_state_key.as_deref()).await;
        }
        let ttft_ms = first_chunk_at.unwrap_or(total_ms);
        let upstream_first_chunk_ms = ttft_ms.saturating_sub(upstream_response_headers_at_ms);
        let mut request_log = RequestLog {
                id: request_id.clone(),
                at: Utc::now(),
                inbound_request_id: Some(request_id.clone()),
                upstream_request_id: Some(Uuid::new_v4().to_string()),
                upstream_attempt_index: Some(1),
                upstream_attempt_total: Some(upstream_request_diagnostics.attempts),
                client_channel: client_channel.label().to_string(),
                upstream_channel: decision.upstream_channel.label().to_string(),
                provider: decision.provider.name.clone(),
                model: decision.model.clone(),
                requested_model,
                agent_reasoning_effort: None,
                configured_reasoning_effort: None,
                effective_reasoning_effort: None,
                reasoning_effort_source: None,
                cache_status: if stream_success_for_cache {
                    if eligible { "miss" } else { "bypass" }
                } else {
                    "error"
                }.to_string(),
                agent_id: agent_log_id.clone(),
                agent_label: agent_log_label.clone(),
                upstream_call_kind: Some("stream".to_string()),
                upstream_call_source: Some("main".to_string()),
                cache_key: if eligible && stream_success_for_cache { Some(metrics_cache_key.clone()) } else { None },
                provider_prefix_key: provider_prefix_key.clone(),
                provider_prefix_fingerprint: provider_prefix_fingerprint.clone(),
                provider_cache_diagnostic: usage_record.as_ref().map(provider_cache_diagnostic),
                prefix_guard_wait_ms: Some(prefix_guard_wait.wait_ms),
                prefix_guard_wait_reason: prefix_guard_wait.reason.clone(),
                prefix_guard_wait_source: prefix_guard_wait.source.clone(),
                prefix_guard_state_age_ms: prefix_guard_wait.state_age_ms,
                prefix_guard_skip_reason: prefix_guard_wait.skip_reason.clone(),
                prefix_guard_wait_effect: prefix_guard_wait_effect(
                    &prefix_guard_wait,
                    usage_record.as_ref(),
                    gap_breakdown.as_ref(),
                ),
                prefix_lag_classification: None,
                prefix_lag_input_delta_tokens: None,
                prefix_lag_cache_delta_tokens: None,
                prefix_lag_previous_gap_tokens: None,
                prefix_cache_instability_score: prefix_guard_wait.cache_instability_score,
                prefix_seen_bucket_tokens: prefix_guard_wait.seen_bucket_tokens,
                prefix_state_cache_read_tokens: prefix_guard_wait.state_cache_read_tokens,
                status,
                ttft_ms,
                upstream_ttft_ms: Some(upstream_ttft_ms(
                    ttft_ms,
                    Some(prefix_guard_wait.wait_ms),
                )),
                local_prepare_ms: Some(local_prepare_ms),
                upstream_headers_ms: Some(upstream_request_diagnostics.headers_ms),
                upstream_last_attempt_headers_ms: Some(
                    upstream_request_diagnostics.last_attempt_headers_ms,
                ),
                upstream_http_version: upstream_request_diagnostics.http_version.clone(),
                upstream_network_path: Some(upstream_request_diagnostics.network_path.to_string()),
                upstream_remote_addr: upstream_request_diagnostics.remote_addr.clone(),
                upstream_pool_diagnostic: upstream_request_diagnostics.pool_diagnostic.clone(),
                upstream_trace_id: upstream_request_diagnostics.upstream_trace_id.clone(),
                upstream_trace_source: upstream_request_diagnostics.upstream_trace_source.clone(),
                upstream_server_timing: upstream_request_diagnostics.server_timing.clone(),
                upstream_timing_source: upstream_request_diagnostics.timing_source.clone(),
                upstream_reported_processing_ms: upstream_request_diagnostics
                    .reported_processing_ms,
                upstream_non_processing_ms: upstream_request_diagnostics.non_processing_ms,
                upstream_first_chunk_ms: Some(upstream_first_chunk_ms),
                stream_upstream_wait_ms: Some(stream_upstream_wait_ms),
                stream_client_backpressure_ms: Some(stream_client_backpressure_ms),
                aggregate_done_ms: None,
                upstream_retry_wait_ms: Some(upstream_request_diagnostics.retry_wait_ms),
                upstream_attempts: Some(upstream_request_diagnostics.attempts),
                request_body_bytes: Some(upstream_request_diagnostics.request_body_bytes),
                sent_body_bytes: Some(upstream_request_diagnostics.sent_body_bytes),
                request_body_encode_ms: Some(
                    upstream_request_diagnostics.request_body_encode_ms,
                ),
                gzip_encode_ms: Some(upstream_request_diagnostics.gzip_encode_ms),
                gzip_attempted: Some(upstream_request_diagnostics.gzip_attempted),
                gzip_fallback_used: Some(upstream_request_diagnostics.gzip_fallback_used),
                upstream_header_wait_class: Some(upstream_header_wait_class(
                    &upstream_request_diagnostics,
                )),
                total_ms,
                input_tokens: usage_record.as_ref().map(|record| record.input_tokens),
                output_tokens: usage_record.as_ref().map(|record| record.output_tokens),
                cache_read_tokens: usage_record
                    .as_ref()
                    .map(|record| record.cache_read_tokens),
                cache_shortfall_tokens: usage_record.as_ref().map(provider_cache_shortfall),
                cache_new_tail_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.new_tail_tokens),
                cache_avoidable_gap_tokens: gap_breakdown.as_ref().map(|gap| gap.avoidable_tokens),
                cache_provider_unstable_gap_tokens: gap_breakdown
                    .as_ref()
                    .map(|gap| gap.provider_unstable_tokens),
                provider_cache_token_ratio: usage_record.as_ref().and_then(provider_cache_ratio),
                tail_input_items: None,
                tail_message_chars: None,
                tail_tool_call_chars: None,
                tail_tool_output_chars: None,
                tail_largest_tool_output_chars: None,
                tail_tool_output_lines: None,
                tail_tool_output_repeated_line_chars: None,
                tail_tool_output_timestamp_like_count: None,
                tail_tool_output_path_like_count: None,
                tail_tool_output_url_like_count: None,
                tail_tool_output_hash_like_count: None,
                tail_tool_output_json_like_chars: None,
                tail_tool_output_noise_hint: None,
                tail_source: None,
                response_session_reused: Some(used_response_session),
                response_session_candidate_count: Some(
                    response_session_reuse_diagnostics.candidate_count,
                ),
                response_session_skip_reason: response_session_reuse_diagnostics
                    .skip_reason
                    .clone(),
                response_session_exact_key_hit: Some(
                    response_session_reuse_diagnostics.exact_key_hit,
                ),
                response_session_scope_match_count: Some(
                    response_session_reuse_diagnostics.scope_match_count,
                ),
                response_session_append_delta_match: Some(
                    response_session_reuse_diagnostics.append_delta_match,
                ),
                response_session_delta_items: Some(response_session_reuse_diagnostics.delta_items),
                response_session_cooldown_active: Some(
                    response_session_reuse_diagnostics.cooldown_active,
                ),
                response_session_rejected_status: response_session_reuse_diagnostics
                    .rejected_status,
                session_anchor_hash: None,
                session_anchor_source: None,
                session_anchor_changed: None,
                session_anchor_peer_count: None,
                original_body_bytes: None,
                send_body_bytes: None,
                send_body_is_delta: None,
                payload_too_large_rescue_attempted: None,
                payload_too_large_rescue_used: None,
                sse_end_reason: Some(sse_end_reason),
                sse_completed_event_seen: Some(stream_metadata.completed_event_seen),
                sse_done_marker_seen: Some(stream_metadata.done_marker_seen),
                sse_chunks: Some(sse_chunks),
            };
        apply_prefix_lag_diagnostics(&mut request_log, prefix_lag);
        apply_session_anchor_diagnostics(&mut request_log, &session_anchor_diagnostics);
        apply_body_diagnostics(&mut request_log, &diagnostics);
        apply_tail_input_diagnostics(&mut request_log, &tail_input_diagnostics);
        state_for_stream.metrics.record_upstream_call(request_log.clone()).await;
        state_for_stream.metrics.record_request(request_log, true).await;
        if eligible && stream_success_for_cache {
            insert_cache_entries(
                &state_for_stream,
                cache_keys,
                semantic_text,
                semantic_shape,
                content_type_for_cache,
                status,
                cache_body,
                &decision,
                &config,
            ).await;
        }
    };

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, response_content_type)
        .body(Body::from_stream(stream_body))
        .unwrap_or_else(|_| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build stream response",
            )
        })
}

async fn authorize_for_agent(
    state: &AppState,
    headers: &HeaderMap,
    forced_agent_id: Option<&'static str>,
) -> Result<Option<String>, Response> {
    let config = state.config.read().await;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    let x_key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    let presented_key = bearer.or(x_key);

    if let Some(agent_id) = forced_agent_id {
        let Some(agent) = config.agent_injections.iter().find(|agent| {
            agent.id == agent_id && agent.enabled && agent.kind != AgentInjectionKind::ProxyMode
        }) else {
            return Err(json_error(
                StatusCode::UNAUTHORIZED,
                "agent injection is not enabled",
            ));
        };
        if config.local_key.trim().is_empty() {
            return Ok(Some(agent.id.clone()));
        }
        let scoped_key = agent_injection::agent_local_key(&config.local_key, &agent.id);
        let accepted = presented_key == Some(PROXY_TOKEN_PLACEHOLDER)
            || presented_key == Some(config.local_key.as_str())
            || presented_key == Some(scoped_key.as_str());
        if accepted {
            return Ok(Some(agent.id.clone()));
        }
        return Err(json_error(
            StatusCode::UNAUTHORIZED,
            "invalid local proxy key",
        ));
    }

    if config.local_key.trim().is_empty() {
        return Ok(None);
    }
    if presented_key == Some(config.local_key.as_str()) {
        Ok(None)
    } else if let Some(agent) = presented_key.and_then(|key| agent_for_local_key(&config, key)) {
        Ok(Some(agent.id.clone()))
    } else {
        Err(json_error(
            StatusCode::UNAUTHORIZED,
            "invalid local proxy key",
        ))
    }
}

fn agent_for_local_key<'a>(config: &'a AppConfig, key: &str) -> Option<&'a AgentInjectionConfig> {
    config.agent_injections.iter().find(|agent| {
        agent.enabled && agent_injection::agent_local_key(&config.local_key, &agent.id) == key
    })
}

fn route_is_agent_provider_bound(
    config: &AppConfig,
    _request: &Value,
    _client_channel: &Channel,
    authorized_agent_id: Option<&str>,
) -> bool {
    authorized_agent_id
        .and_then(|agent_id| {
            config
                .agent_injections
                .iter()
                .find(|agent| agent.id == agent_id && agent.enabled)
        })
        .and_then(|agent| agent.provider_id.as_deref())
        .is_some()
}
fn provider_route_affinity_key(
    config: &AppConfig,
    request: &Value,
    client_channel: &Channel,
) -> Option<String> {
    if !matches!(client_channel, Channel::Responses) {
        return None;
    }

    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_response_session_volatile_fields(&mut material);
    trim_response_session_input_to_anchor(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.provider_route_affinity_key");

    let model = request
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or("*");
    let mut hasher = Sha256::new();
    hasher.update(config.workspace_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(client_channel.label().as_bytes());
    hasher.update(b"\0");
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(serialize_responses_body_for_provider_prefix(&material).as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

async fn lookup_provider_route_affinity(
    state: &AppState,
    config: &AppConfig,
    affinity_key: Option<&str>,
) -> Option<String> {
    let key = affinity_key?;
    let provider_id = state
        .provider_route_affinity
        .lock()
        .await
        .get(key)
        .cloned()?;
    if config
        .providers
        .iter()
        .any(|provider| provider.id == provider_id && provider.enabled)
    {
        return Some(provider_id);
    }
    state.provider_route_affinity.lock().await.remove(key);
    None
}

async fn note_provider_route_affinity(
    state: &AppState,
    affinity_key: Option<&str>,
    provider_id: &str,
) {
    let Some(key) = affinity_key else {
        return;
    };
    if provider_id.trim().is_empty() {
        return;
    }
    state
        .provider_route_affinity
        .lock()
        .await
        .insert(key.to_string(), provider_id.to_string());
}

async fn maybe_clear_provider_route_affinity_after_status(
    state: &AppState,
    affinity_key: Option<&str>,
    provider_id: &str,
    status: u16,
    error_summary: Option<&str>,
) {
    let Ok(status_code) = StatusCode::from_u16(status) else {
        return;
    };
    if is_provider_key_failure_status(status_code)
        || error_summary
            .map(is_provider_key_failure_message)
            .unwrap_or(false)
    {
        clear_provider_route_affinity(state, affinity_key, provider_id).await;
    }
}

async fn clear_provider_route_affinity(
    state: &AppState,
    affinity_key: Option<&str>,
    provider_id: &str,
) {
    let Some(key) = affinity_key else {
        return;
    };
    let mut affinities = state.provider_route_affinity.lock().await;
    if affinities
        .get(key)
        .map(|current| current == provider_id)
        .unwrap_or(false)
    {
        affinities.remove(key);
    }
}

fn apply_provider_route_affinity(
    config: &AppConfig,
    decision: RouteDecision,
    request: &Value,
    client_channel: &Channel,
    preferred_provider_id: Option<&str>,
) -> RouteDecision {
    if !matches!(client_channel, Channel::Responses) {
        return decision;
    }
    let Some(preferred_provider_id) = preferred_provider_id else {
        return decision;
    };
    if preferred_provider_id == decision.provider.id {
        return decision;
    }
    let Some(provider) = config
        .providers
        .iter()
        .find(|provider| provider.id == preferred_provider_id && provider.enabled)
    else {
        return decision;
    };

    let requested_model = request
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty());
    let model = if let Some(requested_model) = requested_model {
        if let Some(model) = resolve_provider_model_id(provider, requested_model) {
            model
        } else {
            return decision;
        }
    } else if let Some(model) = resolve_provider_model_id(provider, &decision.model) {
        model
    } else if let Some(model) = provider
        .models
        .iter()
        .find(|model| model.enabled)
        .map(|model| model.id.clone())
    {
        model
    } else {
        return decision;
    };

    RouteDecision {
        upstream_channel: effective_upstream_channel_for_provider(config, provider, client_channel),
        provider: provider.clone(),
        model,
    }
}

fn decide_route(
    config: &AppConfig,
    request: &Value,
    client_channel: &Channel,
    authorized_agent_id: Option<&str>,
) -> Result<RouteDecision, String> {
    let requested_model = request
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let authorized_agent = authorized_agent_id.and_then(|agent_id| {
        config
            .agent_injections
            .iter()
            .find(|agent| agent.id == agent_id && agent.enabled)
    });

    let profile = config
        .route_profiles
        .iter()
        .find(|profile| &profile.client_channel == client_channel);
    let preferred_channel = authorized_agent
        .and_then(|agent| agent.provider_id.as_deref())
        .and_then(|provider_id| {
            config
                .providers
                .iter()
                .find(|provider| provider.id == provider_id && provider.enabled)
        })
        .map(|provider| provider.channel.clone())
        .or_else(|| profile.map(|profile| profile.upstream_channel.clone()))
        .unwrap_or_else(|| client_channel.clone());

    let agent_provider = authorized_agent
        .and_then(|agent| agent.provider_id.as_deref())
        .and_then(|provider_id| {
            config
                .providers
                .iter()
                .find(|provider| provider.id == provider_id && provider.enabled)
        });

    let configured_provider = agent_provider.or_else(|| {
        config
            .active_provider_id
            .as_deref()
            .and_then(|provider_id| {
                config
                    .providers
                    .iter()
                    .find(|provider| provider.id == provider_id && provider.enabled)
            })
            .or_else(|| {
                profile
                    .and_then(|profile| profile.provider_id.as_deref())
                    .and_then(|provider_id| {
                        config
                            .providers
                            .iter()
                            .find(|provider| provider.id == provider_id && provider.enabled)
                    })
            })
    });

    let requested_model_provider = requested_model.as_deref().and_then(|model| {
        config.providers.iter().find(|provider| {
            provider.enabled && provider_supports_requested_model(provider, Some(model))
        })
    });

    let provider = if let Some(provider) = agent_provider {
        provider
    } else {
        configured_provider
            .filter(|provider| {
                provider_supports_requested_model(provider, requested_model.as_deref())
            })
            .or(requested_model_provider)
            .or_else(|| {
                config
                    .providers
                    .iter()
                    .find(|provider| provider.enabled && provider.channel == preferred_channel)
            })
            .or_else(|| config.providers.iter().find(|provider| provider.enabled))
            .ok_or_else(|| "no enabled provider configured".to_string())?
    };

    let agent_model = authorized_agent
        .and_then(|agent| agent.model_id.clone())
        .and_then(|model| resolve_provider_model_id(provider, &model));
    let requested_model_for_provider =
        requested_model.and_then(|model| resolve_provider_model_id(provider, &model));

    let model = requested_model_for_provider
        .or(agent_model)
        .or_else(|| {
            profile
                .and_then(|profile| profile.model_alias.as_ref())
                .cloned()
        })
        .or_else(|| {
            provider
                .models
                .iter()
                .find(|model| model.enabled)
                .map(|model| model.id.clone())
        })
        .ok_or_else(|| format!("provider {} has no model configured", provider.name))?;

    Ok(RouteDecision {
        upstream_channel: effective_upstream_channel_for_provider(config, provider, client_channel),
        provider: provider.clone(),
        model,
    })
}

fn effective_upstream_channel_for_provider(
    config: &AppConfig,
    provider: &ProviderConfig,
    client_channel: &Channel,
) -> Channel {
    if config.provider_channel_mode_for_provider(&provider.id) == ProviderChannelMode::Manual {
        return provider.channel.clone();
    }
    auto_upstream_channel_for_provider(provider, client_channel)
}

fn auto_upstream_channel_for_provider(
    provider: &ProviderConfig,
    _client_channel: &Channel,
) -> Channel {
    // Auto mode keeps the provider capability hint for normal requests.
    // Responses compact/non-stream fast paths may switch to Chat later in the
    // pipeline, where payload shape and fallback diagnostics are available.
    provider.channel.clone()
}

fn should_preempt_codex_responses_via_chat(
    _config: &AppConfig,
    forced_agent_id: Option<&str>,
    client_channel: &Channel,
    decision: &RouteDecision,
) -> bool {
    forced_agent_id == Some("codex")
        && matches!(client_channel, Channel::Responses)
        && matches!(decision.upstream_channel, Channel::Chat)
}

fn provider_supports_requested_model(
    provider: &ProviderConfig,
    requested_model: Option<&str>,
) -> bool {
    let Some(model) = requested_model else {
        return true;
    };
    resolve_provider_model_id(provider, model).is_some()
}

fn resolve_provider_model_id(provider: &ProviderConfig, requested_model: &str) -> Option<String> {
    let requested = requested_model.trim();
    if requested.is_empty() {
        return None;
    }
    if provider.models.is_empty() {
        return Some(requested.to_string());
    }
    if let Some(model) = provider
        .models
        .iter()
        .find(|item| item.enabled && item.id == requested)
    {
        return Some(model.id.clone());
    }
    if let Some(model) = provider.models.iter().find(|item| {
        item.enabled
            && model_request_alias(item)
                .map(|alias| alias == requested)
                .unwrap_or(false)
    }) {
        return Some(model.id.clone());
    }
    let requested_alias = requested.to_ascii_lowercase();
    provider
        .models
        .iter()
        .find(|item| {
            item.enabled
                && model_request_alias(item)
                    .map(|alias| alias.to_ascii_lowercase() == requested_alias)
                    .unwrap_or(false)
        })
        .or_else(|| {
            provider.models.iter().find(|item| {
                item.enabled
                    && codex_model_alias(&item.id)
                        .map(|alias| alias == requested_alias)
                        .unwrap_or(false)
            })
        })
        .map(|model| model.id.clone())
}

fn set_request_model(request: &mut Value, model: &str) {
    if let Some(object) = request.as_object_mut() {
        object.insert("model".to_string(), Value::String(model.to_string()));
    }
}

fn apply_model_reasoning_effort(
    client_request: &Value,
    upstream_request: &mut Value,
    upstream_channel: &Channel,
    decision: &RouteDecision,
) -> ReasoningEffortDiagnostics {
    let agent = request_reasoning_effort(client_request);
    let model = decision
        .provider
        .models
        .iter()
        .find(|model| model.id == decision.model)
        .or_else(|| {
            decision.provider.models.iter().find(|model| {
                model_request_alias(model)
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(&decision.model))
            })
        });
    let configured = model
        .filter(|model| model.reasoning_effort_override_enabled)
        .and_then(|model| model.reasoning_effort.as_deref())
        .and_then(normalize_request_reasoning_effort);
    let (effective, source) = if let Some(configured) = configured.clone() {
        let downgrade_ultra = configured == "ultra"
            && model.is_some_and(|model| {
                model
                    .supported_reasoning_efforts
                    .iter()
                    .any(|effort| effort == "max")
                    && !model
                        .supported_reasoning_efforts
                        .iter()
                        .any(|effort| effort == "ultra")
            });
        if downgrade_ultra {
            (
                Some("max".to_string()),
                Some("model_override_ultra_to_max".to_string()),
            )
        } else {
            (Some(configured), Some("model_override".to_string()))
        }
    } else if agent.is_some() {
        (agent.clone(), Some("agent".to_string()))
    } else {
        (None, Some("agent_default".to_string()))
    };

    if let Some(effective) = effective.as_deref() {
        set_request_reasoning_effort(upstream_request, upstream_channel, effective);
    }

    ReasoningEffortDiagnostics {
        agent,
        configured,
        effective,
        source,
    }
}

fn request_reasoning_effort(request: &Value) -> Option<String> {
    [
        request.pointer("/reasoning/effort"),
        request.get("reasoning_effort"),
        request.pointer("/thinking/effort"),
        request.pointer("/thinking/level"),
    ]
    .into_iter()
    .flatten()
    .find_map(Value::as_str)
    .and_then(normalize_request_reasoning_effort)
}

fn normalize_request_reasoning_effort(value: &str) -> Option<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" | "disabled" => Some("none".to_string()),
        normalized => normalize_reasoning_effort(normalized),
    }
}

fn set_request_reasoning_effort(request: &mut Value, channel: &Channel, effort: &str) {
    let Some(object) = request.as_object_mut() else {
        return;
    };
    match channel {
        Channel::Responses => {
            object.remove("reasoning_effort");
            let reasoning = object
                .entry("reasoning".to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if !reasoning.is_object() {
                *reasoning = Value::Object(Map::new());
            }
            if let Some(reasoning) = reasoning.as_object_mut() {
                reasoning.insert("effort".to_string(), Value::String(effort.to_string()));
            }
        }
        Channel::Chat => {
            object.insert(
                "reasoning_effort".to_string(),
                Value::String(effort.to_string()),
            );
            if let Some(reasoning) = object.get_mut("reasoning").and_then(Value::as_object_mut) {
                reasoning.remove("effort");
                if reasoning.is_empty() {
                    object.remove("reasoning");
                }
            }
        }
        Channel::Anthropic => {}
    }
}

fn requested_model_for_log(request: &Value, upstream_model: &str) -> Option<String> {
    let requested = request.get("model").and_then(Value::as_str)?.trim();
    if requested.is_empty() || requested == upstream_model.trim() {
        return None;
    }
    Some(requested.to_string())
}

fn request_agent_log_fields(
    config: &AppConfig,
    authorized_agent_id: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(agent_id) = authorized_agent_id else {
        return (None, None);
    };
    let agent_label = config
        .agent_injections
        .iter()
        .find(|agent| agent.id == agent_id)
        .map(|agent| agent.label.clone())
        .unwrap_or_else(|| agent_id.to_string());
    (Some(agent_id.to_string()), Some(agent_label))
}

fn optimize_provider_prefix(request: &mut Value, config: &AppConfig, decision: &RouteDecision) {
    if !smart_hit_enabled(config) || !matches!(config.cache.mode, CacheMode::PrefixPrewarm) {
        strip_provider_cache_key_fields(request);
        return;
    }

    match decision.upstream_channel {
        Channel::Chat => {
            stabilize_chat_provider_request(request);
            let cache_key =
                provider_prompt_cache_key_for_outbound(config, decision, request, &Channel::Chat);
            if let Some(object) = request.as_object_mut() {
                object.insert("prompt_cache_key".to_string(), Value::String(cache_key));
            }
            apply_openai_prompt_cache_retention(request, decision);
        }
        Channel::Responses => {
            stabilize_responses_provider_prefix(request);
            let cache_key = provider_prompt_cache_key_for_outbound(
                config,
                decision,
                request,
                &Channel::Responses,
            );
            if let Some(object) = request.as_object_mut() {
                object.insert("prompt_cache_key".to_string(), Value::String(cache_key));
            }
            apply_openai_prompt_cache_retention(request, decision);
            canonicalize_object_keys(request, "$.responses_prefix");
        }
        Channel::Anthropic => {
            stabilize_anthropic_provider_request(request);
            add_anthropic_cache_control(request);
        }
    }
}

fn copy_responses_prefix_cache_fields_for_native_stream(
    outbound: &mut Value,
    prefix_body: &Value,
    config: &AppConfig,
    decision: &RouteDecision,
) {
    strip_provider_cache_key_fields(outbound);
    if !smart_hit_enabled(config) || !matches!(config.cache.mode, CacheMode::PrefixPrewarm) {
        return;
    }
    let Some(cache_key) = openai_prompt_cache_key(prefix_body) else {
        return;
    };
    if let Some(object) = outbound.as_object_mut() {
        object.insert("prompt_cache_key".to_string(), Value::String(cache_key));
    }
    apply_openai_prompt_cache_retention(outbound, decision);
}

fn stabilize_responses_provider_prefix(request: &mut Value) {
    if let Some(object) = request.as_object_mut() {
        for key in [
            "request_id",
            "client_request_id",
            "trace_id",
            "span_id",
            "event_id",
            "run_id",
            "session_id",
            "thread_id",
            "nonce",
            "timestamp",
            "created_at",
            "updated_at",
            "traceparent",
            "metadata",
            "user",
        ] {
            object.remove(key);
        }
    }

    stabilize_responses_tool_call_ids(request);
    strip_responses_provider_noise(request);
}

fn stabilize_responses_tool_call_ids(value: &mut Value) {
    let mut call_ids = HashMap::new();
    let mut occurrences = HashMap::new();
    assign_deterministic_function_call_ids(value, &mut call_ids, &mut occurrences);
    if !call_ids.is_empty() {
        replace_responses_call_id_refs(value, &call_ids);
    }
}

fn assign_deterministic_function_call_ids(
    value: &mut Value,
    call_ids: &mut HashMap<String, String>,
    occurrences: &mut HashMap<String, usize>,
) {
    match value {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("function_call") {
                if let Some(old_call_id) = map.get("call_id").and_then(Value::as_str) {
                    let payload = stable_function_call_payload(map);
                    let occurrence = occurrences.entry(payload.clone()).or_insert(0);
                    let stable_call_id = deterministic_call_id(&payload, *occurrence);
                    *occurrence += 1;
                    call_ids.insert(old_call_id.to_string(), stable_call_id.clone());
                    map.insert("call_id".to_string(), Value::String(stable_call_id));
                }
            }
            for child in map.values_mut() {
                assign_deterministic_function_call_ids(child, call_ids, occurrences);
            }
        }
        Value::Array(items) => {
            for item in items {
                assign_deterministic_function_call_ids(item, call_ids, occurrences);
            }
        }
        _ => {}
    }
}

fn replace_responses_call_id_refs(value: &mut Value, call_ids: &HashMap<String, String>) {
    match value {
        Value::Object(map) => {
            if let Some(current) = map.get("call_id").and_then(Value::as_str) {
                if let Some(stable) = call_ids.get(current) {
                    map.insert("call_id".to_string(), Value::String(stable.clone()));
                }
            }
            for child in map.values_mut() {
                replace_responses_call_id_refs(child, call_ids);
            }
        }
        Value::Array(items) => {
            for item in items {
                replace_responses_call_id_refs(item, call_ids);
            }
        }
        _ => {}
    }
}

fn stable_function_call_payload(map: &Map<String, Value>) -> String {
    let mut payload = Value::Object(map.clone());
    strip_responses_provider_noise(&mut payload);
    if let Some(object) = payload.as_object_mut() {
        object.remove("call_id");
    }
    canonical_json_string(&payload)
}

fn deterministic_call_id(payload: &str, occurrence: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    hasher.update(b"\0");
    hasher.update(occurrence.to_string().as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("call_apx_{}", &digest[..24])
}

fn strip_responses_provider_noise(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for key in [
                "id",
                "request_id",
                "client_request_id",
                "trace_id",
                "span_id",
                "event_id",
                "run_id",
                "item_id",
                "tool_call_id",
                "tool_use_id",
                "output_index",
                "content_index",
                "metadata",
                "status",
                "expires_at",
                "completed_at",
                "created_at",
                "updated_at",
            ] {
                map.remove(key);
            }
            for child in map.values_mut() {
                strip_responses_provider_noise(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_responses_provider_noise(item);
            }
        }
        _ => {}
    }
}

fn normalize_responses_request(request: &mut Value) {
    let Some(object) = request.as_object_mut() else {
        return;
    };

    remove_null_or_empty_fields(object);
    if let Some(input) = object.get_mut("input") {
        let mut promoted = None;
        if input.is_string() {
            let original = std::mem::take(input);
            *input = normalized_prompt_input(original);
        } else {
            promoted = promote_responses_instruction_input(input);
            normalize_responses_input(input);
        }
        append_instructions(object, promoted);
    } else if let Some(prompt) = object.remove("prompt") {
        object.insert("input".to_string(), normalized_prompt_input(prompt));
    } else if let Some(messages) = object.remove("messages") {
        append_instructions(object, extract_system_text_from_messages(&messages));
        object.insert("input".to_string(), normalized_messages_input(messages));
    }
    for alias in ["max_tokens", "max_completion_tokens"] {
        if let Some(max_tokens) = object.remove(alias) {
            object
                .entry("max_output_tokens".to_string())
                .or_insert(max_tokens);
        }
    }
    if let Some(tools) = object.get_mut("tools") {
        normalize_tool_definitions(tools);
    }
    if let Some(tool_choice) = object.get_mut("tool_choice") {
        canonicalize_object_keys(tool_choice, "$.tool_choice");
    }
    if let Some(input) = object.get_mut("input") {
        normalize_responses_input(input);
    }
    remove_null_or_empty_fields(object);
    canonicalize_object_keys(request, "$.responses");
}

fn normalized_prompt_input(prompt: Value) -> Value {
    match prompt {
        Value::String(text) => json!([{
            "type": "message",
            "role": "user",
            "content": [{ "type": "input_text", "text": text }]
        }]),
        other => other,
    }
}

fn normalized_messages_input(messages: Value) -> Value {
    let Value::Array(items) = messages else {
        return normalized_prompt_input(messages);
    };
    let normalized = items
        .into_iter()
        .filter(|message| {
            !is_instruction_role(
                message
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        })
        .map(|message| {
            let mut normalized = message;
            normalize_responses_input(&mut normalized);
            normalized
        })
        .collect::<Vec<_>>();
    Value::Array(normalized)
}

fn extract_system_text_from_messages(messages: &Value) -> Option<String> {
    let items = messages.as_array()?;
    let parts = items
        .iter()
        .filter(|message| {
            is_instruction_role(
                message
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        })
        .filter_map(|message| {
            message
                .get("content")
                .or_else(|| message.get("text"))
                .and_then(extract_text)
        })
        .collect::<Vec<_>>();
    merge_text_parts(parts)
}

fn promote_responses_instruction_input(input: &mut Value) -> Option<String> {
    let Value::Array(items) = input else {
        return None;
    };

    let mut promoted = Vec::new();
    let mut retained = Vec::with_capacity(items.len());
    for item in std::mem::take(items) {
        let role = item.get("role").and_then(Value::as_str).unwrap_or_default();
        if is_instruction_role(role) {
            if let Some(text) = item
                .get("content")
                .or_else(|| item.get("text"))
                .and_then(extract_text)
            {
                promoted.push(text);
            }
        } else {
            retained.push(item);
        }
    }
    *items = retained;
    merge_text_parts(promoted)
}

fn append_instructions(object: &mut Map<String, Value>, promoted: Option<String>) {
    let Some(promoted) = promoted else {
        return;
    };
    let mut parts = Vec::new();
    if let Some(existing) = object.get("instructions").and_then(extract_text) {
        parts.push(existing);
    }
    if parts
        .last()
        .map(|existing| existing.trim() != promoted.trim())
        .unwrap_or(true)
    {
        parts.push(promoted);
    }
    if let Some(merged) = merge_text_parts(parts) {
        object.insert("instructions".to_string(), Value::String(merged));
    }
}

fn is_instruction_role(role: &str) -> bool {
    matches!(role, "system" | "developer")
}

fn remove_null_or_empty_fields(object: &mut Map<String, Value>) {
    object.retain(|_, value| match value {
        Value::Null => false,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
        _ => true,
    });
}

fn normalize_tool_definitions(value: &mut Value) {
    let Value::Array(items) = value else {
        canonicalize_object_keys(value, "$.tools");
        return;
    };
    for item in items.iter_mut() {
        normalize_tool_definition(item);
    }
    items.sort_by(|left, right| tool_sort_key(left).cmp(&tool_sort_key(right)));
}

fn normalize_tool_definition(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        canonicalize_object_keys(value, "$.tools[]");
        return;
    };

    if object.get("type").is_none() && object.contains_key("name") {
        object.insert("type".to_string(), Value::String("function".to_string()));
    }

    if let Some(function) = object
        .remove("function")
        .and_then(|value| value.as_object().cloned())
    {
        object
            .entry("type".to_string())
            .or_insert_with(|| Value::String("function".to_string()));
        if let Some(name) = function.get("name").cloned() {
            object.entry("name".to_string()).or_insert(name);
        }
        if let Some(description) = function.get("description").cloned() {
            object
                .entry("description".to_string())
                .or_insert(description);
        }
        if let Some(parameters) = function.get("parameters").cloned() {
            object.entry("parameters".to_string()).or_insert(parameters);
        }
    }

    if let Some(parameters) = object.get_mut("parameters") {
        normalize_json_schema(parameters);
    }
    if let Some(parameters) = object
        .get_mut("function")
        .and_then(Value::as_object_mut)
        .and_then(|function| function.get_mut("parameters"))
    {
        normalize_json_schema(parameters);
    }
    canonicalize_object_keys(value, "$.tools[]");
}

fn tool_sort_key(value: &Value) -> String {
    let tool_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let name = value
        .get("name")
        .or_else(|| value.pointer("/function/name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    format!(
        "{tool_type}\0{name}\0{}",
        serde_json::to_string(value).unwrap_or_default()
    )
}

fn normalize_json_schema(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(required) = map.get_mut("required").and_then(Value::as_array_mut) {
                required.sort_by(|left, right| {
                    left.as_str()
                        .unwrap_or_default()
                        .cmp(right.as_str().unwrap_or_default())
                });
            }
            for child in map.values_mut() {
                normalize_json_schema(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_json_schema(item);
            }
        }
        _ => {}
    }
}

fn canonicalize_object_keys(value: &mut Value, path: &str) {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            let mut normalized = Map::new();
            for key in keys {
                if let Some(mut child) = map.remove(&key) {
                    canonicalize_object_keys(&mut child, &format!("{path}.{key}"));
                    normalized.insert(key, child);
                }
            }
            *map = normalized;
        }
        Value::Array(items) => {
            if path.ends_with(".required") && items.iter().all(Value::is_string) {
                items.sort_by(|left, right| {
                    left.as_str()
                        .unwrap_or_default()
                        .cmp(right.as_str().unwrap_or_default())
                });
                return;
            }
            for item in items {
                canonicalize_object_keys(item, &format!("{path}[]"));
            }
        }
        _ => {}
    }
}

fn normalize_responses_input(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let item_type = map
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if item_type == "message"
                || map.contains_key("role")
                || map.contains_key("content")
                || map.contains_key("text")
            {
                normalize_responses_message(map);
                return;
            }
            for (key, child) in map.iter_mut() {
                if matches!(
                    (item_type.as_str(), key.as_str()),
                    ("function_call", "arguments") | ("function_call_output", "output")
                ) {
                    normalize_json_string(child);
                } else {
                    normalize_responses_input(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_responses_input(item);
            }
        }
        _ => {}
    }
}

fn normalize_responses_message(map: &mut Map<String, Value>) {
    map.entry("type".to_string())
        .or_insert_with(|| Value::String("message".to_string()));
    if !map.contains_key("role") {
        map.insert("role".to_string(), Value::String("user".to_string()));
    }
    if !map.contains_key("content") {
        if let Some(text) = map.remove("text") {
            map.insert("content".to_string(), text);
        }
    }
    if let Some(content) = map.get_mut("content") {
        normalize_content_blocks(content);
    }
}

fn normalize_content_blocks(value: &mut Value) {
    match value {
        Value::String(text) => {
            *value = json!([{ "type": "input_text", "text": text }]);
        }
        Value::Array(items) => {
            for item in items {
                if let Some(object) = item.as_object_mut() {
                    if object.get("type").is_none() && object.contains_key("text") {
                        object.insert("type".to_string(), Value::String("input_text".to_string()));
                    } else if object.get("type").and_then(Value::as_str) == Some("text") {
                        object.insert("type".to_string(), Value::String("input_text".to_string()));
                    }
                }
            }
        }
        _ => {}
    }
}

fn normalize_json_string(value: &mut Value) {
    let Some(text) = value.as_str() else {
        return;
    };
    let trimmed = text.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return;
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
        *value = Value::String(canonical_json_string(&parsed));
    }
}

fn canonical_json_string(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let body = keys
                .into_iter()
                .map(|key| format!("{:?}:{}", key, canonical_json_string(&map[key])))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(items) => {
            let body = items
                .iter()
                .map(canonical_json_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{body}]")
        }
        _ => value.to_string(),
    }
}

fn provider_prompt_cache_key(
    config: &AppConfig,
    decision: &RouteDecision,
    request: &Value,
    channel: &Channel,
) -> String {
    let model_key = provider_prefix_model_key(decision);
    let mut hasher = Sha256::new();
    hasher.update(config.workspace_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(model_key.as_bytes());
    hasher.update(b"\0");
    hasher.update(channel.label().as_bytes());
    hasher.update(b"\0");
    hasher.update(provider_prompt_cache_key_material(request, channel).as_bytes());
    format!("{:x}", hasher.finalize())
}

fn provider_prompt_cache_key_material(request: &Value, channel: &Channel) -> String {
    if !matches!(channel, Channel::Responses) {
        return provider_prefix_sample(request, channel);
    }

    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    strip_provider_prefix_context_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_response_session_volatile_fields(&mut material);
    trim_response_session_input_to_anchor(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.provider_prompt_cache_key");
    serialize_responses_body_for_provider_prefix(&material)
}

fn provider_prompt_cache_key_for_outbound(
    config: &AppConfig,
    decision: &RouteDecision,
    request: &Value,
    channel: &Channel,
) -> String {
    request
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|key| provider_prompt_cache_key_is_valid(key))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| provider_prompt_cache_key(config, decision, request, channel))
}

fn provider_prompt_cache_key_is_valid(key: &str) -> bool {
    !key.is_empty() && key.len() <= 64
}

fn openai_prompt_cache_key(request: &Value) -> Option<String> {
    request
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn provider_prefix_fingerprint(request: &Value, channel: &Channel) -> String {
    let mut hasher = Sha256::new();
    hasher.update(channel.label().as_bytes());
    hasher.update(b"\0");
    hasher.update(provider_prefix_fingerprint_sample(request, channel).as_bytes());
    format!("{:x}", hasher.finalize())
}

fn provider_prefix_sample(request: &Value, channel: &Channel) -> String {
    const SAMPLE_CHARS: usize = 64 * 1024;

    let mut prefix = request.clone();
    strip_provider_cache_key_fields(&mut prefix);
    strip_provider_prefix_context_fields(&mut prefix);
    match channel {
        Channel::Responses => {
            canonicalize_responses_instruction_shape(&mut prefix);
            stabilize_responses_provider_prefix(&mut prefix);
            strip_responses_dynamic_provider_cache_tail(&mut prefix);
            canonicalize_object_keys(&mut prefix, "$.provider_prefix_sample");
        }
        Channel::Chat => {
            stabilize_chat_provider_prefix_sample(&mut prefix);
            strip_chat_dynamic_provider_cache_tail(&mut prefix);
            canonicalize_object_keys(&mut prefix, "$.chat_provider_prefix_sample");
        }
        Channel::Anthropic => {}
    }

    let serialized = if matches!(channel, Channel::Responses) {
        serialize_responses_body_for_provider_prefix(&prefix)
    } else if matches!(channel, Channel::Chat) {
        serialize_chat_body_for_provider_prefix(&prefix)
    } else {
        serde_json::to_string(&prefix).unwrap_or_else(|_| "null".to_string())
    };
    serialized.chars().take(SAMPLE_CHARS).collect()
}

fn provider_prefix_fingerprint_sample(request: &Value, channel: &Channel) -> String {
    const SAMPLE_CHARS: usize = 64 * 1024;

    let mut prefix = request.clone();
    strip_provider_cache_key_fields(&mut prefix);
    strip_provider_prefix_context_fields(&mut prefix);
    match channel {
        Channel::Responses => {
            canonicalize_responses_instruction_shape(&mut prefix);
            stabilize_responses_provider_prefix(&mut prefix);
            trim_responses_provider_cache_tail_to_session_anchor(&mut prefix);
            canonicalize_object_keys(&mut prefix, "$.provider_prefix_fingerprint");
        }
        Channel::Chat => {
            stabilize_chat_provider_prefix_sample(&mut prefix);
            strip_chat_dynamic_provider_cache_tail(&mut prefix);
            canonicalize_object_keys(&mut prefix, "$.chat_provider_prefix_fingerprint");
        }
        Channel::Anthropic => {}
    }

    let serialized = if matches!(channel, Channel::Responses) {
        serialize_responses_body_for_provider_prefix(&prefix)
    } else if matches!(channel, Channel::Chat) {
        serialize_chat_body_for_provider_prefix(&prefix)
    } else {
        serde_json::to_string(&prefix).unwrap_or_else(|_| "null".to_string())
    };
    serialized.chars().take(SAMPLE_CHARS).collect()
}

fn strip_provider_prefix_context_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.remove("model");
    }
}

fn strip_responses_dynamic_provider_cache_tail(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.remove("input");
    object.remove("previous_response_id");
    object.remove("stream");
    object.remove("include");
    object.remove("metadata");
    object.remove("user");
    object.remove("service_tier");
    object.remove("truncation");
}

fn trim_responses_provider_cache_tail_to_session_anchor(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object.remove("previous_response_id");
    object.remove("stream");
    object.remove("include");
    object.remove("metadata");
    object.remove("user");
    object.remove("service_tier");
    object.remove("truncation");
    if let Some(input) = object.get_mut("input") {
        if let Some(items) = input.as_array_mut() {
            if items.len() > 1 {
                items.truncate(1);
            }
        }
    }
}

fn canonicalize_responses_instruction_shape(request: &mut Value) {
    let Some(object) = request.as_object_mut() else {
        return;
    };

    let mut promoted_parts = Vec::new();
    if let Some(existing) = object.get("instructions").and_then(extract_text) {
        promoted_parts.push(existing);
    }

    if let Some(input) = object.get_mut("input") {
        if let Some(promoted) = promote_responses_instruction_input(input) {
            promoted_parts.push(promoted);
        }
        normalize_responses_input(input);
    }
    if let Some(messages) = object.get_mut("messages") {
        if let Some(promoted) = extract_system_text_from_messages(messages) {
            promoted_parts.push(promoted);
        }
        *messages = normalized_messages_input(std::mem::take(messages));
    }

    object.remove("instructions");
    let mut merged_parts = Vec::new();
    for part in promoted_parts {
        let trimmed = part.trim();
        if !trimmed.is_empty()
            && !merged_parts
                .iter()
                .any(|existing: &String| existing == trimmed)
        {
            merged_parts.push(trimmed.to_string());
        }
    }
    if let Some(merged) = merge_text_parts(merged_parts) {
        object.insert("instructions".to_string(), Value::String(merged));
    }
    remove_null_or_empty_fields(object);
}

fn stabilize_chat_provider_prefix_sample(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    for key in [
        "request_id",
        "client_request_id",
        "trace_id",
        "span_id",
        "event_id",
        "run_id",
        "session_id",
        "thread_id",
        "conversation_id",
        "nonce",
        "timestamp",
        "created_at",
        "updated_at",
        "traceparent",
        "metadata",
        "user",
        "stream_options",
    ] {
        object.remove(key);
    }

    if let Some(tools) = object.get_mut("tools") {
        normalize_tool_definitions(tools);
    }
    if let Some(tool_choice) = object.get_mut("tool_choice") {
        canonicalize_object_keys(tool_choice, "$.chat_tool_choice");
    }
    if let Some(response_format) = object.get_mut("response_format") {
        canonicalize_object_keys(response_format, "$.chat_response_format");
    }
    strip_chat_provider_noise(value);
}

fn stabilize_chat_provider_request(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    for key in [
        "request_id",
        "client_request_id",
        "trace_id",
        "span_id",
        "event_id",
        "run_id",
        "nonce",
        "timestamp",
        "created_at",
        "updated_at",
        "traceparent",
    ] {
        object.remove(key);
    }

    if let Some(tools) = object.get_mut("tools") {
        normalize_tool_definitions(tools);
    }
    if let Some(tool_choice) = object.get_mut("tool_choice") {
        canonicalize_object_keys(tool_choice, "$.chat_request_tool_choice");
    }
    if let Some(response_format) = object.get_mut("response_format") {
        canonicalize_object_keys(response_format, "$.chat_request_response_format");
    }
    if let Some(messages) = object.get_mut("messages") {
        normalize_chat_messages_for_request(messages);
    }
    canonicalize_object_keys(value, "$.chat_request");
}

fn normalize_chat_messages_for_request(value: &mut Value) {
    let Value::Array(items) = value else {
        return;
    };
    for item in items {
        normalize_chat_message_for_request(item);
    }
}

fn normalize_chat_message_for_request(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(content) = object.get_mut("content") {
        normalize_chat_content_for_request(content);
    }
    if let Some(tool_calls) = object.get_mut("tool_calls") {
        normalize_chat_tool_calls_for_request(tool_calls);
    }
    if let Some(arguments) = object.get_mut("arguments") {
        normalize_json_string(arguments);
    }
    canonicalize_object_keys(value, "$.chat_request_message");
}

fn normalize_chat_content_for_request(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                canonicalize_object_keys(item, "$.chat_request_content");
            }
        }
        Value::Object(_) => canonicalize_object_keys(value, "$.chat_request_content"),
        _ => {}
    }
}

fn normalize_chat_tool_calls_for_request(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                normalize_chat_tool_calls_for_request(item);
            }
        }
        Value::Object(map) => {
            if let Some(function) = map.get_mut("function") {
                if let Some(arguments) = function.get_mut("arguments") {
                    normalize_json_string(arguments);
                }
                canonicalize_object_keys(function, "$.chat_request_tool_call.function");
            }
            canonicalize_object_keys(value, "$.chat_request_tool_call");
        }
        _ => {}
    }
}

fn strip_chat_dynamic_provider_cache_tail(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    if let Some(messages) = object.get_mut("messages") {
        let stable = chat_stable_prefix_messages(messages);
        if stable.is_empty() {
            object.remove("messages");
        } else {
            *messages = Value::Array(stable);
        }
    }

    object.remove("stream");
    object.remove("metadata");
    object.remove("user");
    object.remove("stream_options");
    object.remove("service_tier");
    object.remove("store");
}

fn chat_stable_prefix_messages(messages: &Value) -> Vec<Value> {
    let Some(items) = messages.as_array() else {
        return Vec::new();
    };
    let mut stable = Vec::new();
    for item in items {
        let Some(role) = item.get("role").and_then(Value::as_str) else {
            break;
        };
        if !matches!(role, "system" | "developer") {
            break;
        }
        let mut cloned = item.clone();
        strip_chat_provider_noise(&mut cloned);
        canonicalize_object_keys(&mut cloned, "$.chat_stable_message");
        stable.push(cloned);
    }
    stable
}

fn strip_chat_provider_noise(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for key in [
                "id",
                "request_id",
                "client_request_id",
                "trace_id",
                "span_id",
                "event_id",
                "run_id",
                "session_id",
                "thread_id",
                "conversation_id",
                "tool_call_id",
                "tool_use_id",
                "item_id",
                "output_index",
                "content_index",
                "metadata",
                "status",
                "expires_at",
                "completed_at",
                "created_at",
                "updated_at",
            ] {
                map.remove(key);
            }
            if let Some(content) = map.get_mut("content") {
                normalize_chat_content_for_prefix(content);
            }
            if let Some(arguments) = map.get_mut("arguments") {
                normalize_json_string(arguments);
            }
            for child in map.values_mut() {
                strip_chat_provider_noise(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_chat_provider_noise(item);
            }
        }
        _ => {}
    }
}

fn normalize_chat_content_for_prefix(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(object) = item.as_object_mut() {
                    if object.get("type").and_then(Value::as_str) == Some("text") {
                        object
                            .entry("type".to_string())
                            .or_insert_with(|| Value::String("text".to_string()));
                    }
                }
                strip_chat_provider_noise(item);
            }
        }
        Value::Object(_) => strip_chat_provider_noise(value),
        _ => {}
    }
}

fn strip_provider_cache_key_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("prompt_cache_key");
            map.remove("prompt_cache_retention");
            for child in map.values_mut() {
                strip_provider_cache_key_fields(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_provider_cache_key_fields(item);
            }
        }
        _ => {}
    }
}

fn apply_openai_prompt_cache_retention(request: &mut Value, decision: &RouteDecision) {
    if !supports_extended_prompt_cache_retention(decision) {
        if let Some(object) = request.as_object_mut() {
            object.remove("prompt_cache_retention");
        }
        return;
    }
    if let Some(object) = request.as_object_mut() {
        object
            .entry("prompt_cache_retention".to_string())
            .or_insert_with(|| Value::String("24h".to_string()));
    }
}

fn supports_extended_prompt_cache_retention(decision: &RouteDecision) -> bool {
    let model = decision.model.trim().to_ascii_lowercase();
    let base_url = decision.provider.base_url.trim().to_ascii_lowercase();
    model.starts_with("gpt-")
        && (base_url.contains("api.openai.com") || decision.provider.prompt_cache_retention_enabled)
}

fn add_anthropic_cache_control(request: &mut Value) {
    const ANTHROPIC_CACHE_BREAKPOINT_LIMIT: usize = 4;
    let mut remaining =
        ANTHROPIC_CACHE_BREAKPOINT_LIMIT.saturating_sub(count_cache_control_fields(request));
    if remaining == 0 {
        return;
    }
    let static_prefix_tokens = estimated_anthropic_static_prefix_tokens(request);

    if let Some(tools) = request.get_mut("tools") {
        if add_cache_control_to_last_object(tools) {
            remaining = remaining.saturating_sub(1);
        }
    }
    if remaining == 0 {
        return;
    }

    if let Some(system) = request.get_mut("system") {
        if add_cache_control_to_content_value(system) {
            remaining = remaining.saturating_sub(1);
        }
    }
    if remaining == 0 {
        return;
    }

    if let Some(messages) = request.get_mut("messages") {
        add_cache_control_to_stable_message_prefix(messages, remaining, static_prefix_tokens);
    }
}

fn stabilize_anthropic_provider_request(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    for key in [
        "request_id",
        "client_request_id",
        "trace_id",
        "span_id",
        "event_id",
        "run_id",
        "nonce",
        "timestamp",
        "created_at",
        "updated_at",
        "traceparent",
    ] {
        object.remove(key);
    }

    if let Some(tools) = object.get_mut("tools") {
        normalize_anthropic_tool_definitions(tools);
    }
    if let Some(tool_choice) = object.get_mut("tool_choice") {
        canonicalize_object_keys(tool_choice, "$.anthropic_request_tool_choice");
    }
    if let Some(system) = object.get_mut("system") {
        normalize_anthropic_content_for_request(system);
    }
    if let Some(messages) = object.get_mut("messages") {
        normalize_anthropic_messages_for_request(messages);
    }
    canonicalize_object_keys(value, "$.anthropic_request");
}

fn normalize_anthropic_tool_definitions(value: &mut Value) {
    let Value::Array(items) = value else {
        canonicalize_object_keys(value, "$.anthropic_tools");
        return;
    };
    for item in items.iter_mut() {
        if let Some(schema) = item.get_mut("input_schema") {
            normalize_json_schema(schema);
        }
        canonicalize_object_keys(item, "$.anthropic_tools[]");
    }
    items.sort_by(|left, right| anthropic_tool_sort_key(left).cmp(&anthropic_tool_sort_key(right)));
}

fn anthropic_tool_sort_key(value: &Value) -> String {
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    format!(
        "{name}\0{}",
        serde_json::to_string(value).unwrap_or_default()
    )
}

fn normalize_anthropic_messages_for_request(value: &mut Value) {
    let Value::Array(items) = value else {
        return;
    };
    for item in items {
        if let Some(object) = item.as_object_mut() {
            if let Some(content) = object.get_mut("content") {
                normalize_anthropic_content_for_request(content);
            }
            canonicalize_object_keys(item, "$.anthropic_request_message");
        }
    }
}

fn normalize_anthropic_content_for_request(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(object) = item.as_object_mut() {
                    if object.get("type").and_then(Value::as_str) == Some("tool_use") {
                        if let Some(input) = object.get_mut("input") {
                            canonicalize_object_keys(input, "$.anthropic_tool_use_input");
                        }
                    }
                    if object.get("type").and_then(Value::as_str) == Some("tool_result") {
                        if let Some(content) = object.get_mut("content") {
                            normalize_anthropic_content_for_request(content);
                        }
                    }
                    canonicalize_object_keys(item, "$.anthropic_content_block");
                }
            }
        }
        Value::Object(_) => canonicalize_object_keys(value, "$.anthropic_content_object"),
        _ => {}
    }
}

fn add_cache_control_to_stable_message_prefix(
    messages: &mut Value,
    remaining: usize,
    static_prefix_tokens: usize,
) -> usize {
    let Some(items) = messages.as_array_mut() else {
        return 0;
    };
    if items.len() < 2 || remaining == 0 {
        return 0;
    }

    let candidates = anthropic_message_cache_breakpoints(items, remaining, static_prefix_tokens);
    if candidates.is_empty() {
        return 0;
    }

    let mut added = 0usize;
    for index in candidates {
        if let Some(message) = items.get_mut(index) {
            if add_cache_control_to_message(message) {
                added += 1;
                if added >= remaining {
                    break;
                }
            }
        }
    }
    added
}

#[derive(Debug, Clone)]
struct AnthropicMessageCacheCandidate {
    index: usize,
    cumulative_tokens: usize,
}

fn anthropic_message_cache_breakpoints(
    items: &[Value],
    remaining: usize,
    static_prefix_tokens: usize,
) -> Vec<usize> {
    const ANTHROPIC_MIN_CACHEABLE_PREFIX_TOKENS: usize = 1024;
    const ANTHROPIC_LOOKBACK_BLOCKS: usize = 20;

    if items.len() < 2 || remaining == 0 {
        return Vec::new();
    }

    let mut cumulative_tokens = static_prefix_tokens;
    let mut stable = Vec::new();
    for (index, message) in items.iter().take(items.len().saturating_sub(1)).enumerate() {
        cumulative_tokens += estimated_anthropic_cache_tokens(message);
        if message_has_cacheable_content(message) {
            stable.push(AnthropicMessageCacheCandidate {
                index,
                cumulative_tokens,
            });
        }
    }

    let eligible = stable
        .into_iter()
        .filter(|candidate| candidate.cumulative_tokens >= ANTHROPIC_MIN_CACHEABLE_PREFIX_TOKENS)
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Vec::new();
    }

    let mut selected = Vec::new();
    let mut max_index = usize::MAX;
    while selected.len() < remaining {
        let candidate = eligible
            .iter()
            .rev()
            .find(|candidate| candidate.index <= max_index);
        let Some(candidate) = candidate else {
            break;
        };
        selected.push(candidate.index);
        max_index = candidate.index.saturating_sub(ANTHROPIC_LOOKBACK_BLOCKS);
    }
    selected
}

fn message_has_cacheable_content(message: &Value) -> bool {
    message
        .as_object()
        .map(|object| object.contains_key("content") || object.contains_key("text"))
        .unwrap_or(false)
}

fn add_cache_control_to_message(message: &mut Value) -> bool {
    let Some(object) = message.as_object_mut() else {
        return false;
    };
    if let Some(content) = object.get_mut("content") {
        return add_cache_control_to_content_value(content);
    }
    if let Some(text) = object.get_mut("text") {
        return add_cache_control_to_content_value(text);
    }
    false
}

fn add_cache_control_to_content_value(value: &mut Value) -> bool {
    match value {
        Value::String(text) => {
            *value = json!([{
                "type": "text",
                "text": text,
                "cache_control": { "type": "ephemeral" }
            }]);
            true
        }
        Value::Array(_) => add_cache_control_to_last_object(value),
        Value::Object(object) => {
            let before = object.contains_key("cache_control");
            object
                .entry("cache_control".to_string())
                .or_insert(json!({ "type": "ephemeral" }));
            !before
        }
        _ => false,
    }
}

fn add_cache_control_to_last_object(value: &mut Value) -> bool {
    if let Value::Array(items) = value {
        if let Some(last) = items.iter_mut().rev().find(|item| item.is_object()) {
            if let Some(object) = last.as_object_mut() {
                let before = object.contains_key("cache_control");
                object
                    .entry("cache_control".to_string())
                    .or_insert(json!({ "type": "ephemeral" }));
                return !before;
            }
        }
    }
    false
}

fn count_cache_control_fields(value: &Value) -> usize {
    match value {
        Value::Object(map) => {
            usize::from(map.contains_key("cache_control"))
                + map.values().map(count_cache_control_fields).sum::<usize>()
        }
        Value::Array(items) => items.iter().map(count_cache_control_fields).sum(),
        _ => 0,
    }
}

fn estimated_anthropic_static_prefix_tokens(request: &Value) -> usize {
    request
        .get("tools")
        .map(estimated_anthropic_cache_tokens)
        .unwrap_or(0)
        + request
            .get("system")
            .map(estimated_anthropic_cache_tokens)
            .unwrap_or(0)
}

fn estimated_anthropic_cache_tokens(value: &Value) -> usize {
    match value {
        Value::String(text) => estimated_text_tokens(text),
        Value::Array(items) => items.iter().map(estimated_anthropic_cache_tokens).sum(),
        Value::Object(map) => map
            .iter()
            .filter(|(key, _)| key.as_str() != "cache_control")
            .map(|(key, value)| {
                estimated_text_tokens(key) + estimated_anthropic_cache_tokens(value)
            })
            .sum(),
        _ => 0,
    }
}

fn estimated_text_tokens(text: &str) -> usize {
    let mut ascii_chars = 0usize;
    let mut non_ascii_chars = 0usize;
    for character in text.chars() {
        if character.is_ascii() {
            ascii_chars += 1;
        } else {
            non_ascii_chars += 1;
        }
    }
    ascii_chars.div_ceil(4) + non_ascii_chars
}

fn transform_request_for_channel(
    request: &Value,
    client_channel: &Channel,
    upstream_channel: &Channel,
) -> Value {
    if client_channel == upstream_channel {
        return request.clone();
    }

    match (client_channel, upstream_channel) {
        (Channel::Chat, Channel::Responses) => chat_to_responses_request(request),
        (Channel::Chat, Channel::Anthropic) => chat_to_anthropic_request(request),
        (Channel::Responses, Channel::Chat) => responses_to_chat_request(request),
        (Channel::Responses, Channel::Anthropic) => responses_to_anthropic_request(request),
        (Channel::Anthropic, Channel::Chat) => anthropic_to_chat_request(request),
        (Channel::Anthropic, Channel::Responses) => anthropic_to_responses_request(request),
        _ => request.clone(),
    }
}

fn set_stream_flag(request: &mut Value, stream: bool) {
    if let Some(object) = request.as_object_mut() {
        object.insert("stream".to_string(), Value::Bool(stream));
    }
}

fn infer_client_requested_stream(
    request: &mut Value,
    client_channel: &Channel,
    forced_agent_id: Option<&str>,
) -> bool {
    if forced_agent_id == Some("codex") && matches!(client_channel, Channel::Responses) {
        set_stream_flag(request, true);
        return true;
    }
    if let Some(stream) = request.get("stream").and_then(Value::as_bool) {
        return stream;
    }
    false
}

fn chat_to_responses_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    if let Some(system) = extract_system_text(request, Channel::Chat) {
        object.insert("instructions".to_string(), Value::String(system));
    }
    let messages = extract_messages(request, Channel::Chat);
    if !messages.is_empty() {
        object.insert("input".to_string(), Value::Array(messages));
    }
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("response_format", "response_format"),
            ("seed", "seed"),
            ("presence_penalty", "presence_penalty"),
            ("frequency_penalty", "frequency_penalty"),
            ("n", "n"),
            ("logit_bias", "logit_bias"),
            ("user", "user"),
            ("parallel_tool_calls", "parallel_tool_calls"),
        ],
    );
    copy_max_tokens(request, &mut object, "max_output_tokens");
    if let Some(effort) = request_reasoning_effort(request) {
        object.insert("reasoning".to_string(), json!({ "effort": effort }));
    }
    if let Some(stop) = request.get("stop").cloned() {
        object.insert("stop".to_string(), stop);
    }
    Value::Object(object)
}

fn chat_to_anthropic_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    if let Some(system) = extract_system_text(request, Channel::Chat) {
        object.insert("system".to_string(), Value::String(system));
    }
    let messages = extract_messages(request, Channel::Chat);
    if !messages.is_empty() {
        object.insert("messages".to_string(), Value::Array(messages));
    }
    let max_tokens = request
        .get("max_tokens")
        .or_else(|| request.get("max_output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(4096);
    object.insert("max_tokens".to_string(), Value::Number(max_tokens.into()));
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("top_k", "top_k"),
            ("thinking", "thinking"),
        ],
    );
    if let Some(stop_sequences) = normalize_stop_sequences(request.get("stop")) {
        object.insert("stop_sequences".to_string(), stop_sequences);
    } else if let Some(stop_sequences) = normalize_stop_sequences(request.get("stop_sequences")) {
        object.insert("stop_sequences".to_string(), stop_sequences);
    }
    Value::Object(object)
}

fn responses_to_chat_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    let mut messages = extract_messages(request, Channel::Responses);
    if let Some(system) = extract_system_text(request, Channel::Responses) {
        messages.insert(
            0,
            json!({
                "role": "system",
                "content": system,
            }),
        );
    }
    if !messages.is_empty() {
        object.insert("messages".to_string(), Value::Array(messages));
    }
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("response_format", "response_format"),
            ("seed", "seed"),
            ("presence_penalty", "presence_penalty"),
            ("frequency_penalty", "frequency_penalty"),
            ("n", "n"),
            ("logit_bias", "logit_bias"),
            ("user", "user"),
            ("parallel_tool_calls", "parallel_tool_calls"),
        ],
    );
    copy_max_tokens(request, &mut object, "max_tokens");
    if let Some(effort) = request_reasoning_effort(request) {
        object.insert("reasoning_effort".to_string(), Value::String(effort));
    }
    if let Some(stop) = request.get("stop").cloned() {
        object.insert("stop".to_string(), stop);
    }
    Value::Object(object)
}

fn responses_to_anthropic_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    if let Some(system) = extract_system_text(request, Channel::Responses) {
        object.insert("system".to_string(), Value::String(system));
    }
    let messages = extract_messages(request, Channel::Responses);
    if !messages.is_empty() {
        object.insert("messages".to_string(), Value::Array(messages));
    }
    let max_tokens = request
        .get("max_output_tokens")
        .or_else(|| request.get("max_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(4096);
    object.insert("max_tokens".to_string(), Value::Number(max_tokens.into()));
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("top_k", "top_k"),
            ("thinking", "thinking"),
        ],
    );
    if let Some(stop_sequences) = normalize_stop_sequences(request.get("stop")) {
        object.insert("stop_sequences".to_string(), stop_sequences);
    }
    Value::Object(object)
}

fn anthropic_to_chat_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    let mut messages = extract_messages(request, Channel::Anthropic);
    if let Some(system) = extract_system_text(request, Channel::Anthropic) {
        messages.insert(
            0,
            json!({
                "role": "system",
                "content": system,
            }),
        );
    }
    if !messages.is_empty() {
        object.insert("messages".to_string(), Value::Array(messages));
    }
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("top_k", "top_k"),
        ],
    );
    copy_max_tokens(request, &mut object, "max_tokens");
    if let Some(stop_sequences) = normalize_stop_sequences(request.get("stop_sequences")) {
        object.insert("stop".to_string(), stop_sequences);
    } else if let Some(stop) = request.get("stop").cloned() {
        object.insert("stop".to_string(), stop);
    }
    Value::Object(object)
}

fn anthropic_to_responses_request(request: &Value) -> Value {
    let mut object = Map::new();
    copy_model(request, &mut object);
    if let Some(system) = extract_system_text(request, Channel::Anthropic) {
        object.insert("instructions".to_string(), Value::String(system));
    }
    let messages = extract_messages(request, Channel::Anthropic);
    if !messages.is_empty() {
        object.insert("input".to_string(), Value::Array(messages));
    }
    let max_tokens = request
        .get("max_tokens")
        .or_else(|| request.get("max_output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(4096);
    object.insert(
        "max_output_tokens".to_string(),
        Value::Number(max_tokens.into()),
    );
    copy_fields(
        request,
        &mut object,
        &[
            ("stream", "stream"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("tools", "tools"),
            ("tool_choice", "tool_choice"),
            ("metadata", "metadata"),
            ("top_k", "top_k"),
        ],
    );
    Value::Object(object)
}

fn copy_model(request: &Value, object: &mut Map<String, Value>) {
    if let Some(model) = request.get("model").cloned() {
        object.insert("model".to_string(), model);
    }
}

fn copy_fields(request: &Value, object: &mut Map<String, Value>, fields: &[(&str, &str)]) {
    for (source, target) in fields {
        if let Some(value) = request.get(source).cloned() {
            if !value.is_null() {
                object.insert((*target).to_string(), value);
            }
        }
    }
}

fn copy_max_tokens(request: &Value, object: &mut Map<String, Value>, target: &str) {
    for source in ["max_tokens", "max_output_tokens"] {
        if let Some(value) = request.get(source).cloned() {
            if !value.is_null() {
                object.insert(target.to_string(), value);
                return;
            }
        }
    }
}

fn normalize_stop_sequences(value: Option<&Value>) -> Option<Value> {
    let value = value?;
    let mut items = Vec::new();
    match value {
        Value::String(text) if !text.trim().is_empty() => {
            items.push(Value::String(text.clone()));
        }
        Value::Array(array) => {
            for item in array {
                if let Some(text) = item.as_str().filter(|text| !text.trim().is_empty()) {
                    items.push(Value::String(text.to_string()));
                }
            }
        }
        _ => {
            if let Some(text) = extract_text(value) {
                if !text.trim().is_empty() {
                    items.push(Value::String(text));
                }
            }
        }
    }
    if items.is_empty() {
        None
    } else {
        Some(Value::Array(items))
    }
}

fn extract_system_text(request: &Value, source: Channel) -> Option<String> {
    let mut parts = Vec::new();
    match source {
        Channel::Chat => {
            if let Some(text) = request.get("system").and_then(extract_text) {
                parts.push(text);
            }
            if let Some(messages) = request.get("messages").and_then(Value::as_array) {
                for message in messages {
                    if is_instruction_role(
                        message
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    ) {
                        if let Some(text) = message
                            .get("content")
                            .or_else(|| message.get("text"))
                            .and_then(extract_text)
                        {
                            parts.push(text);
                        }
                    }
                }
            }
        }
        Channel::Responses => {
            if let Some(text) = request.get("instructions").and_then(extract_text) {
                parts.push(text);
            }
            if let Some(messages) = request.get("messages").and_then(Value::as_array) {
                for message in messages {
                    if is_instruction_role(
                        message
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    ) {
                        if let Some(text) = message
                            .get("content")
                            .or_else(|| message.get("text"))
                            .and_then(extract_text)
                        {
                            parts.push(text);
                        }
                    }
                }
            }
            if let Some(input) = request.get("input") {
                if let Some(text) = input.as_array().and_then(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            is_instruction_role(
                                item.get("role").and_then(Value::as_str).unwrap_or_default(),
                            )
                        })
                        .filter_map(|item| {
                            item.get("content")
                                .or_else(|| item.get("text"))
                                .and_then(extract_text)
                        })
                        .reduce(|left, right| format!("{left}\n\n{right}"))
                }) {
                    parts.push(text);
                }
            }
        }
        Channel::Anthropic => {
            if let Some(text) = request.get("system").and_then(extract_text) {
                parts.push(text);
            }
            if let Some(messages) = request.get("messages").and_then(Value::as_array) {
                for message in messages {
                    if is_instruction_role(
                        message
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    ) {
                        if let Some(text) = message
                            .get("content")
                            .or_else(|| message.get("text"))
                            .and_then(extract_text)
                        {
                            parts.push(text);
                        }
                    }
                }
            }
        }
    }

    merge_text_parts(parts)
}

fn extract_messages(request: &Value, source: Channel) -> Vec<Value> {
    match source {
        Channel::Chat | Channel::Anthropic => request
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| {
                messages
                    .iter()
                    .filter_map(normalize_message)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        Channel::Responses => {
            let mut messages = Vec::new();
            if let Some(items) = request.get("input").and_then(Value::as_array) {
                for item in items {
                    if let Some(message) = normalize_message(item) {
                        messages.push(message);
                    }
                }
            } else if let Some(input) = request.get("input") {
                if let Some(text) = extract_text(input) {
                    messages.push(json!({
                        "role": "user",
                        "content": text,
                    }));
                }
            }
            if messages.is_empty() {
                if let Some(legacy) = request.get("messages").and_then(Value::as_array) {
                    for item in legacy {
                        if let Some(message) = normalize_message(item) {
                            messages.push(message);
                        }
                    }
                }
            }
            messages
        }
    }
}

fn normalize_message(message: &Value) -> Option<Value> {
    let object = message.as_object()?;
    let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
    if is_instruction_role(role) {
        return None;
    }
    let normalized_role = match role {
        "assistant" => "assistant",
        "tool" => "user",
        _ => "user",
    };
    let content = object
        .get("content")
        .or_else(|| object.get("text"))
        .or_else(|| object.get("input_text"))
        .or_else(|| object.get("output_text"))
        .or_else(|| object.get("message"))?;
    let text = extract_text(content)?;
    if text.trim().is_empty() {
        return None;
    }
    Some(json!({
        "role": normalized_role,
        "content": text,
    }))
}

fn extract_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                if let Some(text) = extract_text(item) {
                    parts.push(text);
                }
            }
            merge_text_parts(parts)
        }
        Value::Object(map) => {
            for key in [
                "text",
                "output_text",
                "content",
                "message",
                "input",
                "instructions",
                "system",
                "delta",
                "reason",
            ] {
                if let Some(text) = map.get(key).and_then(extract_text) {
                    return Some(text);
                }
            }
            if let Some(type_value) = map.get("type").and_then(Value::as_str) {
                if matches!(type_value, "text" | "output_text" | "input_text") {
                    if let Some(text) = map.get("text").and_then(Value::as_str) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            return Some(trimmed.to_string());
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn merge_text_parts(parts: Vec<String>) -> Option<String> {
    let merged = parts
        .into_iter()
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let trimmed = merged.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn transform_response_bytes(
    client_channel: &Channel,
    upstream_channel: &Channel,
    model: &str,
    bytes: &[u8],
) -> Vec<u8> {
    if client_channel == upstream_channel {
        return bytes.to_vec();
    }
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return bytes.to_vec();
    };
    if value.get("error").is_some_and(|error| !error.is_null()) {
        return bytes.to_vec();
    }
    let transformed = transform_response_value(client_channel, model, &value);
    serde_json::to_vec(&transformed).unwrap_or_else(|_| bytes.to_vec())
}

fn normalize_response_body_for_client(
    client_channel: &Channel,
    _content_type: &str,
    body: Vec<u8>,
    non_sse_compact_compat: bool,
) -> Vec<u8> {
    if non_sse_compact_compat && matches!(client_channel, Channel::Responses) {
        normalize_responses_json_for_client(&body)
    } else {
        body
    }
}

fn maybe_normalize_responses_json_for_client(bytes: &[u8], enabled: bool) -> Vec<u8> {
    if enabled {
        normalize_responses_json_for_client(bytes)
    } else {
        bytes.to_vec()
    }
}

fn non_sse_compact_compat_for_decision(config: &AppConfig, decision: &RouteDecision) -> bool {
    config.non_sse_compact_compat_enabled_for_provider(&decision.provider.id)
}

fn normalize_responses_json_for_client(bytes: &[u8]) -> Vec<u8> {
    let Ok(mut value) = serde_json::from_slice::<Value>(bytes) else {
        return bytes.to_vec();
    };
    if value.get("error").is_some_and(|error| !error.is_null()) {
        return bytes.to_vec();
    }
    let is_response = value
        .get("object")
        .and_then(Value::as_str)
        .is_some_and(|object| object == "response")
        || value.get("output").is_some_and(Value::is_array);
    if !is_response {
        return bytes.to_vec();
    }

    let response_id = value
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("resp_{}", Uuid::new_v4().simple()));
    let Some(output) = value.get_mut("output").and_then(Value::as_array_mut) else {
        return serde_json::to_vec(&value).unwrap_or_else(|_| bytes.to_vec());
    };
    for (output_index, item) in output.iter_mut().enumerate() {
        let Some(item_object) = item.as_object_mut() else {
            continue;
        };
        if !item_object.get("id").is_some_and(Value::is_string) {
            item_object.insert(
                "id".to_string(),
                Value::String(format!("msg_{}_{}", response_id, output_index)),
            );
        }
        let Some(content) = item_object.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        for content_item in content.iter_mut() {
            let Some(content_object) = content_item.as_object_mut() else {
                continue;
            };
            if !content_object
                .get("annotations")
                .is_some_and(Value::is_array)
            {
                content_object.insert("annotations".to_string(), Value::Array(Vec::new()));
            }
        }
    }

    serde_json::to_vec(&value).unwrap_or_else(|_| bytes.to_vec())
}

fn is_text_event_stream(content_type: &str) -> bool {
    content_type
        .to_ascii_lowercase()
        .contains("text/event-stream")
}

fn should_proxy_upstream_as_stream(
    is_success_status: bool,
    client_requested_stream: bool,
    cross_protocol_stream: bool,
    upstream_body: &Value,
    content_type: &str,
) -> bool {
    if !is_success_status || !client_requested_stream || cross_protocol_stream {
        return false;
    }
    upstream_body
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || is_text_event_stream(content_type)
}

fn responses_sse_to_non_stream_json(
    bytes: &[u8],
    model: &str,
    non_sse_compact_compat: bool,
) -> Vec<u8> {
    if serde_json::from_slice::<Value>(bytes).is_ok() {
        return maybe_normalize_responses_json_for_client(bytes, non_sse_compact_compat);
    }

    let text = String::from_utf8_lossy(bytes);
    let mut current_event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();
    let mut completed_response: Option<Value> = None;
    let mut assembled_text = String::new();
    let mut usage: Option<Value> = None;
    let mut response_id: Option<String> = None;
    let mut response_model: Option<String> = None;

    let flush_event = |event: Option<&str>,
                       data_lines: &mut Vec<String>,
                       completed_response: &mut Option<Value>,
                       assembled_text: &mut String,
                       usage: &mut Option<Value>,
                       response_id: &mut Option<String>,
                       response_model: &mut Option<String>| {
        if data_lines.is_empty() {
            return;
        }
        let payload = data_lines.join("\n");
        data_lines.clear();
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        let event_type = value.get("type").and_then(Value::as_str).or(event);
        if let Some(id) = response_id_from_value(&value) {
            *response_id = Some(id);
        }
        if let Some(model_value) = extract_model(&value) {
            *response_model = Some(model_value);
        }
        if let Some(value_usage) = value
            .get("usage")
            .or_else(|| value.pointer("/response/usage"))
            .cloned()
        {
            *usage = Some(value_usage);
        }
        if event_type == Some("response.completed") {
            if let Some(response) = value.get("response").cloned() {
                if let Some(id) = response_id_from_value(&response) {
                    *response_id = Some(id);
                }
                if let Some(model_value) = extract_model(&response) {
                    *response_model = Some(model_value);
                }
                if let Some(value_usage) = response.get("usage").cloned() {
                    *usage = Some(value_usage);
                }
                *completed_response = Some(response);
                return;
            }
            if value
                .get("object")
                .and_then(Value::as_str)
                .is_some_and(|object| object == "response")
                || value.get("output").is_some_and(Value::is_array)
            {
                *completed_response = Some(value);
                return;
            }
        }
        if value
            .get("object")
            .and_then(Value::as_str)
            .is_some_and(|object| object == "response")
            || value.get("output").is_some_and(Value::is_array)
        {
            *completed_response = Some(value);
            return;
        }
        if event_type == Some("response.output_text.delta") {
            if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                assembled_text.push_str(delta);
            }
        } else if let Some(delta) = value
            .get("delta")
            .and_then(|delta| delta.get("text"))
            .and_then(Value::as_str)
        {
            assembled_text.push_str(delta);
        } else if let Some(delta) = value.get("text").and_then(Value::as_str) {
            assembled_text.push_str(delta);
        }
    };

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            flush_event(
                current_event.as_deref(),
                &mut data_lines,
                &mut completed_response,
                &mut assembled_text,
                &mut usage,
                &mut response_id,
                &mut response_model,
            );
            current_event = None;
            continue;
        }
        if let Some(event) = line.strip_prefix("event:") {
            current_event = Some(event.trim().to_string());
        } else if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.trim_start().to_string());
        }
    }
    flush_event(
        current_event.as_deref(),
        &mut data_lines,
        &mut completed_response,
        &mut assembled_text,
        &mut usage,
        &mut response_id,
        &mut response_model,
    );

    if let Some(response) = completed_response {
        return maybe_normalize_responses_json_for_client(
            &serde_json::to_vec(&response).unwrap_or_else(|_| bytes.to_vec()),
            non_sse_compact_compat,
        );
    }
    if assembled_text.is_empty() {
        return bytes.to_vec();
    }

    let response_id = response_id.unwrap_or_else(|| format!("resp_{}", Uuid::new_v4().simple()));
    let response = json!({
        "id": response_id,
        "object": "response",
        "created_at": Utc::now().timestamp(),
        "model": response_model.unwrap_or_else(|| model.to_string()),
        "status": "completed",
        "output": [{
            "type": "message",
            "id": format!("msg_{}_0", response_id),
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": assembled_text,
                "annotations": [],
            }],
        }],
        "output_text": assembled_text,
        "usage": usage,
    });
    maybe_normalize_responses_json_for_client(
        &serde_json::to_vec(&response).unwrap_or_else(|_| bytes.to_vec()),
        non_sse_compact_compat,
    )
}

fn chat_sse_to_non_stream_json(bytes: &[u8], model: &str) -> Vec<u8> {
    if serde_json::from_slice::<Value>(bytes).is_ok() {
        return bytes.to_vec();
    }

    let text = String::from_utf8_lossy(bytes);
    let mut current_event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();
    let mut assembled_text = String::new();
    let mut response_id: Option<String> = None;
    let mut response_model: Option<String> = None;
    let mut usage: Option<Value> = None;
    let mut finish_reason: Option<String> = None;
    let mut error: Option<Value> = None;
    let mut saw_done_marker = false;
    let mut saw_payload = false;

    let flush_chat_payload = |event: Option<&str>,
                              data_lines: &mut Vec<String>,
                              assembled_text: &mut String,
                              response_id: &mut Option<String>,
                              response_model: &mut Option<String>,
                              usage: &mut Option<Value>,
                              finish_reason: &mut Option<String>,
                              error: &mut Option<Value>,
                              saw_done_marker: &mut bool,
                              saw_payload: &mut bool| {
        if data_lines.is_empty() {
            return;
        }
        let payload = data_lines.join("\n");
        data_lines.clear();
        let payload = payload.trim();
        if payload.is_empty() {
            return;
        }
        if payload == "[DONE]" {
            *saw_done_marker = true;
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        *saw_payload = true;
        if event == Some("error")
            || value.get("error").is_some_and(|error| !error.is_null())
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            *error = Some(normalize_sse_error_value(&value));
            return;
        }
        if let Some(id) = value.get("id").and_then(Value::as_str) {
            *response_id = Some(id.to_string());
        }
        if let Some(model_value) = extract_model(&value) {
            *response_model = Some(model_value);
        }
        if let Some(value_usage) = value
            .get("usage")
            .or_else(|| value.pointer("/x_gpt/usage"))
            .or_else(|| value.pointer("/response/usage"))
            .cloned()
        {
            *usage = Some(value_usage);
        }
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            for choice in choices {
                if let Some(content) = choice.pointer("/delta/content").and_then(extract_text) {
                    assembled_text.push_str(&content);
                } else if let Some(content) =
                    choice.pointer("/message/content").and_then(extract_text)
                {
                    assembled_text.push_str(&content);
                }
                if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                    *finish_reason = Some(reason.to_string());
                }
            }
        }
    };

    for line in text.lines() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            flush_chat_payload(
                current_event.as_deref(),
                &mut data_lines,
                &mut assembled_text,
                &mut response_id,
                &mut response_model,
                &mut usage,
                &mut finish_reason,
                &mut error,
                &mut saw_done_marker,
                &mut saw_payload,
            );
            current_event = None;
            continue;
        }
        let trimmed_start = trimmed.trim_start();
        if let Some(event) = trimmed_start.strip_prefix("event:") {
            current_event = Some(event.trim().to_string());
        } else if let Some(payload) = trimmed_start.strip_prefix("data:") {
            data_lines.push(payload.trim_start().to_string());
        };
    }
    flush_chat_payload(
        current_event.as_deref(),
        &mut data_lines,
        &mut assembled_text,
        &mut response_id,
        &mut response_model,
        &mut usage,
        &mut finish_reason,
        &mut error,
        &mut saw_done_marker,
        &mut saw_payload,
    );

    if let Some(error) = error {
        return serde_json::to_vec(&error).unwrap_or_else(|_| bytes.to_vec());
    }
    if assembled_text.trim().is_empty() && saw_payload && !saw_done_marker {
        let error = json!({
            "error": {
                "message": "chat-compatible compact stream ended without a completion marker or output text",
                "type": "atoapi_compact_stream_incomplete",
            }
        });
        return serde_json::to_vec(&error).unwrap_or_else(|_| bytes.to_vec());
    }

    let response = json!({
        "id": response_id.unwrap_or_else(|| format!("chatcmpl_{}", Uuid::new_v4().simple())),
        "object": "chat.completion",
        "created": Utc::now().timestamp(),
        "model": response_model.unwrap_or_else(|| model.to_string()),
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": assembled_text,
            },
            "finish_reason": finish_reason.unwrap_or_else(|| "stop".to_string()),
        }],
        "usage": usage.unwrap_or_else(|| json!({})),
    });
    serde_json::to_vec(&response).unwrap_or_else(|_| bytes.to_vec())
}

fn normalize_sse_error_value(value: &Value) -> Value {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return json!({ "error": error });
    }
    let message = value
        .pointer("/message")
        .or_else(|| value.pointer("/delta/message"))
        .or_else(|| value.pointer("/response/error/message"))
        .and_then(extract_text)
        .unwrap_or_else(|| value.to_string());
    json!({
        "error": {
            "message": message,
            "type": value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("upstream_sse_error"),
        }
    })
}

fn json_body_has_error(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .is_some_and(|value| value.get("error").is_some_and(|error| !error.is_null()))
}

fn transform_response_value(client_channel: &Channel, model: &str, value: &Value) -> Value {
    let response_id = value
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("resp-{}", Uuid::new_v4().simple()));
    let text = extract_response_text(value).unwrap_or_else(|| value.to_string());
    let finish_reason = extract_finish_reason(value);
    let usage = normalize_usage(value.get("usage"));
    let created = Utc::now().timestamp();
    match client_channel {
        Channel::Chat => json!({
            "id": response_id,
            "object": "chat.completion",
            "created": created,
            "model": extract_model(value).unwrap_or_else(|| model.to_string()),
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": text,
                },
                "finish_reason": finish_reason,
            }],
            "usage": usage,
        }),
        Channel::Responses => json!({
            "id": response_id,
            "object": "response",
            "created_at": created,
            "model": extract_model(value).unwrap_or_else(|| model.to_string()),
            "status": "completed",
            "output": [{
                "type": "message",
                "id": format!("msg-{}", Uuid::new_v4().simple()),
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": text,
                }],
            }],
            "output_text": text,
            "usage": usage,
        }),
        Channel::Anthropic => json!({
            "id": response_id,
            "type": "message",
            "role": "assistant",
            "model": extract_model(value).unwrap_or_else(|| model.to_string()),
            "content": [{
                "type": "text",
                "text": text,
            }],
            "stop_reason": finish_reason.unwrap_or_else(|| "end_turn".to_string()),
            "usage": usage.unwrap_or_else(|| json!({})),
        }),
    }
}

fn response_json_to_sse(channel: &Channel, bytes: &[u8]) -> Vec<u8> {
    let payload = String::from_utf8_lossy(bytes).to_string();
    let Ok(value) = serde_json::from_str::<Value>(&payload) else {
        return format!("data: {payload}\n\ndata: [DONE]\n\n").into_bytes();
    };
    let text = extract_response_text(&value).unwrap_or(payload.clone());
    match channel {
        Channel::Chat => {
            let chunk = json!({
                "id": value.get("id").cloned().unwrap_or_else(|| Value::String(format!("chunk-{}", Uuid::new_v4().simple()))),
                "object": "chat.completion.chunk",
                "created": Utc::now().timestamp(),
                "model": extract_model(&value).unwrap_or_else(|| "unknown".to_string()),
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": text,
                    },
                    "finish_reason": Value::Null,
                }],
            });
            format!(
                "data: {}\n\ndata: [DONE]\n\n",
                serde_json::to_string(&chunk).unwrap_or_else(|_| payload.clone())
            )
            .into_bytes()
        }
        Channel::Responses => {
            let completed = serde_json::to_string(&value).unwrap_or_else(|_| payload.clone());
            let delta = json!({
                "type": "response.output_text.delta",
                "delta": text,
            });
            format!(
                "event: response.output_text.delta\ndata: {}\n\nevent: response.completed\ndata: {}\n\n",
                serde_json::to_string(&delta).unwrap_or_else(|_| "{}".to_string()),
                completed
            )
            .into_bytes()
        }
        Channel::Anthropic => {
            let message_start = json!({
                "type": "message_start",
                "message": {
                    "id": value.get("id").cloned().unwrap_or_else(|| Value::String(format!("msg-{}", Uuid::new_v4().simple()))),
                    "type": "message",
                    "role": "assistant",
                    "model": extract_model(&value).unwrap_or_else(|| "unknown".to_string()),
                    "content": [],
                    "usage": value.get("usage").cloned().unwrap_or_else(|| json!({})),
                }
            });
            let content_delta = json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "text_delta",
                    "text": text,
                }
            });
            format!(
                "event: message_start\ndata: {}\n\nevent: content_block_delta\ndata: {}\n\nevent: message_stop\ndata: {{}}\n\n",
                serde_json::to_string(&message_start).unwrap_or_else(|_| "{}".to_string()),
                serde_json::to_string(&content_delta).unwrap_or_else(|_| "{}".to_string())
            )
            .into_bytes()
        }
    }
}

fn extract_response_text(value: &Value) -> Option<String> {
    if let Some(text) = value.get("output_text").and_then(extract_text) {
        return Some(text);
    }
    if let Some(text) = value.get("text").and_then(extract_text) {
        return Some(text);
    }
    if let Some(choices) = value.get("choices").and_then(Value::as_array) {
        for choice in choices {
            if let Some(text) = choice
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(extract_text)
            {
                return Some(text);
            }
            if let Some(text) = choice
                .get("delta")
                .and_then(|delta| delta.get("content"))
                .and_then(extract_text)
            {
                return Some(text);
            }
        }
    }
    if let Some(output) = value.get("output").and_then(Value::as_array) {
        for item in output {
            if let Some(text) = item
                .get("content")
                .and_then(extract_text)
                .or_else(|| item.get("text").and_then(extract_text))
            {
                return Some(text);
            }
        }
    }
    if let Some(content) = value.get("content").and_then(extract_text) {
        return Some(content);
    }
    if let Some(error) = value.get("error") {
        if let Some(text) = error.get("message").and_then(extract_text) {
            return Some(text);
        }
    }
    None
}

fn extract_finish_reason(value: &Value) -> Option<String> {
    value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("stop_reason")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

fn extract_model(value: &Value) -> Option<String> {
    value
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn normalize_usage(value: Option<&Value>) -> Option<Value> {
    let usage = value?;
    let input_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(Value::as_u64);
    let output_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(Value::as_u64);
    let cached_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .or_else(|| usage.get("cache_read_input_tokens"))
        .and_then(Value::as_u64);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .or_else(|| match (input_tokens, output_tokens) {
            (Some(input), Some(output)) => Some(input + output),
            _ => None,
        });

    let mut object = Map::new();
    if let Some(value) = input_tokens {
        object.insert("input_tokens".to_string(), Value::Number(value.into()));
    }
    if let Some(value) = output_tokens {
        object.insert("output_tokens".to_string(), Value::Number(value.into()));
    }
    if let Some(value) = total_tokens {
        object.insert("total_tokens".to_string(), Value::Number(value.into()));
    }
    if let Some(value) = cached_tokens {
        object.insert("cached_tokens".to_string(), Value::Number(value.into()));
    }
    if object.is_empty() {
        None
    } else {
        Some(Value::Object(object))
    }
}

fn upstream_url(base_url: &str, channel: &Channel) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let endpoint = channel.endpoint_path();
    if trimmed.ends_with(endpoint)
        || trimmed.ends_with("/chat/completions")
        || trimmed.ends_with("/responses")
        || trimmed.ends_with("/messages")
    {
        trimmed.to_string()
    } else if trimmed.ends_with("/v1") {
        format!("{trimmed}{endpoint}")
    } else {
        format!("{trimmed}/v1{endpoint}")
    }
}

fn responses_compact_url(base_url: &str, response_id: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let response_id = response_id.trim_matches('/');
    if trimmed.ends_with("/responses") {
        format!("{trimmed}/{response_id}/compact")
    } else if trimmed.ends_with("/v1") {
        format!("{trimmed}/responses/{response_id}/compact")
    } else {
        format!("{trimmed}/v1/responses/{response_id}/compact")
    }
}

fn compact_request_set_response_id(request: &mut Value, response_id: &str) {
    if let Value::Object(object) = request {
        object.insert(
            "response_id".to_string(),
            Value::String(response_id.to_string()),
        );
    }
}

fn compact_request_body_for_official_endpoint(body: &Value) -> Value {
    let mut compact_body = body.clone();
    if let Value::Object(object) = &mut compact_body {
        object.remove("response_id");
        object.remove("stream");
        object.remove("stream_options");
    }
    compact_body
}

fn should_fallback_compact_to_responses(status: u16) -> bool {
    matches!(status, 400 | 404 | 405 | 501)
}

fn collect_provider_usage_for_diagnostics(
    bytes: &[u8],
    decision: &RouteDecision,
) -> Option<UsageRecord> {
    let mut record = provider_usage_from_bytes(bytes);
    if !record.has_usage() {
        return None;
    }
    record.provider = decision.provider.name.clone();
    if record.model == "unknown" {
        record.model = decision.model.clone();
    }
    Some(record)
}

#[derive(Debug, Clone)]
struct ProviderUsageObservation {
    raw: UsageRecord,
    effective: UsageRecord,
}

async fn collect_provider_usage(
    state: &AppState,
    bytes: &[u8],
    decision: &RouteDecision,
    prefix_state_key: Option<&str>,
    used_response_session: bool,
) -> Option<ProviderUsageObservation> {
    let mut record = provider_usage_from_bytes(bytes);
    if !record.has_usage() {
        return None;
    }
    record.provider = decision.provider.name.clone();
    if record.model == "unknown" {
        record.model = decision.model.clone();
    }
    let is_response_session_delta = provider_usage_is_response_session_delta(
        state,
        prefix_state_key,
        &record,
        used_response_session,
    )
    .await;
    let effective = provider_usage_effective_for_prefix_metrics(&record, is_response_session_delta);
    state.metrics.record_usage(record.clone()).await;
    Some(ProviderUsageObservation {
        raw: record,
        effective,
    })
}

async fn collect_provider_usage_from_record(
    state: &AppState,
    mut record: UsageRecord,
    decision: &RouteDecision,
    prefix_state_key: Option<&str>,
    used_response_session: bool,
) -> Option<ProviderUsageObservation> {
    if !record.has_usage() {
        return None;
    }
    record.provider = decision.provider.name.clone();
    if record.model == "unknown" {
        record.model = decision.model.clone();
    }
    let is_response_session_delta = provider_usage_is_response_session_delta(
        state,
        prefix_state_key,
        &record,
        used_response_session,
    )
    .await;
    let effective = provider_usage_effective_for_prefix_metrics(&record, is_response_session_delta);
    state.metrics.record_usage(record.clone()).await;
    Some(ProviderUsageObservation {
        raw: record,
        effective,
    })
}

async fn provider_usage_is_response_session_delta(
    state: &AppState,
    prefix_state_key: Option<&str>,
    record: &UsageRecord,
    used_response_session: bool,
) -> bool {
    if !used_response_session {
        return false;
    }
    let Some(key) = prefix_state_key else {
        return true;
    };
    let states = state.prefix_states.lock().await;
    let Some(previous) = lookup_provider_prefix_state(&states, key) else {
        return true;
    };
    !response_session_usage_looks_like_full_context(previous, record)
}

fn provider_usage_effective_for_prefix_metrics(
    record: &UsageRecord,
    is_response_session_delta: bool,
) -> UsageRecord {
    let mut effective = record.clone();
    if is_response_session_delta {
        effective.cache_read_tokens = effective
            .cache_read_tokens
            .max(provider_cache_bucket_max(effective.input_tokens));
    }
    effective
}

fn responses_huge_dynamic_history_cold_read(
    record: &UsageRecord,
    tail_input_diagnostics: &TailInputDiagnostics,
) -> bool {
    if record.input_tokens < 32_000 || provider_cache_shortfall(record) < 8192 {
        return false;
    }
    let ratio = provider_cache_ratio(record).unwrap_or_default();
    if ratio >= 0.50 || record.cache_read_tokens >= 32_768 {
        return false;
    }
    let huge_history_tail = tail_input_diagnostics.input_items >= 32
        && (tail_input_diagnostics.message_chars >= 50_000
            || tail_input_diagnostics.tool_output_chars >= 80_000
            || tail_input_diagnostics.largest_tool_output_chars >= 32_000);
    if !huge_history_tail {
        return false;
    }
    matches!(
        tail_input_diagnostics.source.as_deref(),
        Some("mixed") | Some("tool_output") | None
    )
}

fn provider_cache_shortfall(record: &UsageRecord) -> u64 {
    provider_cache_bucket_max(record.input_tokens).saturating_sub(record.cache_read_tokens)
}

fn provider_cache_bucket_max(input_tokens: u64) -> u64 {
    (input_tokens / 512) * 512
}

fn provider_cache_shortfall_128(record: &UsageRecord) -> u64 {
    provider_cache_bucket_max_128(record.input_tokens).saturating_sub(record.cache_read_tokens)
}

fn provider_cache_bucket_max_128(input_tokens: u64) -> u64 {
    if input_tokens < 1024 {
        0
    } else {
        1024 + ((input_tokens - 1024) / 128) * 128
    }
}

fn responses_session_key(
    config: &AppConfig,
    decision: &RouteDecision,
    request: &Value,
) -> Option<String> {
    if !matches!(decision.upstream_channel, Channel::Responses) {
        return None;
    }
    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_response_session_volatile_fields(&mut material);
    trim_response_session_input_to_anchor(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.response_session_key");

    let mut hasher = Sha256::new();
    hasher.update(config.workspace_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(decision.provider.id.as_bytes());
    hasher.update(b"\0");
    hasher.update(provider_prefix_model_key(decision).as_bytes());
    hasher.update(b"\0");
    hasher.update(serialize_responses_body_for_provider_prefix(&material).as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

fn responses_session_scope_key(
    config: &AppConfig,
    decision: &RouteDecision,
    request: &Value,
) -> Option<String> {
    if !matches!(decision.upstream_channel, Channel::Responses) {
        return None;
    }
    let mut material = request.clone();
    strip_provider_cache_key_fields(&mut material);
    canonicalize_responses_instruction_shape(&mut material);
    strip_response_session_volatile_fields(&mut material);
    strip_responses_dynamic_provider_cache_tail(&mut material);
    stabilize_responses_provider_prefix(&mut material);
    canonicalize_object_keys(&mut material, "$.response_session_scope_key");

    let mut hasher = Sha256::new();
    hasher.update(config.workspace_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(decision.provider.id.as_bytes());
    hasher.update(b"\0");
    hasher.update(provider_prefix_model_key(decision).as_bytes());
    hasher.update(b"\0");
    hasher.update(serialize_responses_body_for_provider_prefix(&material).as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

fn strip_response_session_volatile_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("previous_response_id");
            map.remove("store");
            map.remove("stream");
            map.remove("include");
            map.remove("metadata");
            map.remove("user");
            map.remove("service_tier");
            map.remove("truncation");
            for child in map.values_mut() {
                strip_response_session_volatile_fields(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_response_session_volatile_fields(item);
            }
        }
        _ => {}
    }
}

fn trim_response_session_input_to_anchor(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let Some(input) = object.get_mut("input") else {
        return;
    };
    let Some(items) = input.as_array_mut() else {
        return;
    };
    if items.len() > 1 {
        items.truncate(1);
    }
}

fn provider_cache_ratio(record: &UsageRecord) -> Option<f64> {
    if record.input_tokens == 0 {
        None
    } else {
        Some(record.cache_read_tokens as f64 / record.input_tokens as f64)
    }
}

fn provider_cache_diagnostic(record: &UsageRecord) -> String {
    if record.input_tokens == 0 {
        return "provider-no-usage".to_string();
    }

    let bucket_max = (record.input_tokens / 512) * 512;
    if bucket_max < 1024 {
        return "provider-prefix-ineligible-small".to_string();
    }

    if record.cache_read_tokens == 0 {
        return "provider-cold-start".to_string();
    }

    let bucket_gap = bucket_max.saturating_sub(record.cache_read_tokens);
    if bucket_gap == 0 {
        return "provider-warm-full".to_string();
    }
    if bucket_gap <= 512 {
        return "provider-small-gap".to_string();
    }
    if (record.cache_read_tokens as f64 / bucket_max as f64) >= 0.99 {
        return "provider-warm-99".to_string();
    }
    "provider-prefix-break".to_string()
}

fn provider_usage_from_bytes(bytes: &[u8]) -> UsageRecord {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return provider_usage_from_value(&value);
    }

    let mut record = UsageRecord::default();
    let text = String::from_utf8_lossy(bytes);
    for line in text.lines() {
        let Some(payload) = line.trim_start().strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            record.merge(provider_usage_from_value(&value));
        }
    }
    record
}

fn provider_usage_from_value(value: &Value) -> UsageRecord {
    let usage = value
        .get("usage")
        .or_else(|| value.pointer("/message/usage"))
        .or_else(|| value.pointer("/response/usage"));
    let input_tokens = usage
        .and_then(|usage| usage.get("input_tokens"))
        .or_else(|| usage.and_then(|usage| usage.get("prompt_tokens")))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached_tokens = usage
        .and_then(|usage| usage.pointer("/prompt_tokens_details/cached_tokens"))
        .or_else(|| usage.and_then(|usage| usage.pointer("/input_tokens_details/cached_tokens")))
        .or_else(|| usage.and_then(|usage| usage.get("cache_read_input_tokens")))
        .or_else(|| usage.and_then(|usage| usage.get("cached_input_tokens")))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .and_then(|usage| usage.get("output_tokens"))
        .or_else(|| usage.and_then(|usage| usage.get("completion_tokens")))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation_tokens = usage
        .and_then(|usage| usage.get("cache_creation_input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let model = value
        .get("model")
        .or_else(|| value.pointer("/message/model"))
        .or_else(|| value.pointer("/response/model"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    UsageRecord {
        provider: String::new(),
        model,
        input_tokens,
        output_tokens,
        cache_read_tokens: cached_tokens,
        cache_creation_tokens,
    }
}

fn raw_response(status: u16, content_type: &str, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap_or_else(|_| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build response",
            )
        })
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(json!({
            "error": {
                "message": message,
                "type": "atoapi_error"
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::{cache_path, CacheStore},
        config::ModelConfig,
        state::AppState,
    };
    use axum::http::HeaderValue;
    use serde_json::json;
    use std::{
        fs,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    use tokio::net::TcpListener;

    #[test]
    fn codex_responses_defaults_to_stream_when_stream_is_absent() {
        let mut request = json!({ "model": "gpt-5.5", "input": "hello" });

        let stream =
            infer_client_requested_stream(&mut request, &Channel::Responses, Some("codex"));

        assert!(stream);
        assert_eq!(request["stream"], json!(true));
    }

    #[test]
    fn codex_responses_generation_forces_stream_even_when_false() {
        let mut request = json!({ "model": "gpt-5.5", "input": "hello", "stream": false });

        let stream =
            infer_client_requested_stream(&mut request, &Channel::Responses, Some("codex"));

        assert!(stream);
        assert_eq!(request["stream"], json!(true));
    }

    #[test]
    fn normal_responses_still_defaults_to_non_stream() {
        let mut request = json!({ "model": "gpt-5.5", "input": "hello" });

        let stream = infer_client_requested_stream(&mut request, &Channel::Responses, None);

        assert!(!stream);
        assert!(request.get("stream").is_none());
    }

    #[tokio::test]
    async fn codex_responses_accepts_large_request_bodies() {
        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-large-body-limit-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                AppConfig::default(),
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let body = serde_json::to_vec(&json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": "x".repeat(3 * 1024 * 1024)
        }))
        .unwrap();
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/codex/v1/responses"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .unwrap();

        assert_ne!(
            response.status().as_u16(),
            StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
            "large local Codex requests must reach the handler instead of Axum's default body limit"
        );

        fs::remove_dir_all(config_dir).ok();
    }

    #[test]
    fn upstream_ttft_subtracts_only_local_prefix_guard_wait() {
        assert_eq!(upstream_ttft_ms(83_916, Some(10_000)), 73_916);
        assert_eq!(upstream_ttft_ms(5_000, Some(10_000)), 0);
        assert_eq!(upstream_ttft_ms(1_234, None), 1_234);
    }

    #[test]
    fn responses_json_normalizer_adds_non_sse_validation_fields() {
        let bytes = serde_json::to_vec(&json!({
            "id": "resp_compat",
            "object": "response",
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "compact summary"
                }]
            }]
        }))
        .unwrap();

        let normalized = normalize_responses_json_for_client(&bytes);
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["output"][0]["id"], "msg_resp_compat_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
        assert_eq!(value["output"][0]["content"][0]["text"], "compact summary");
    }

    #[test]
    fn responses_json_normalizer_preserves_existing_fields() {
        let bytes = serde_json::to_vec(&json!({
            "id": "resp_keep",
            "object": "response",
            "output": [{
                "type": "message",
                "id": "msg_existing",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "already shaped",
                    "annotations": [{ "type": "file_citation", "file_id": "file_1" }]
                }]
            }]
        }))
        .unwrap();

        let normalized = normalize_responses_json_for_client(&bytes);
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["output"][0]["id"], "msg_existing");
        assert_eq!(
            value["output"][0]["content"][0]["annotations"][0]["file_id"],
            "file_1"
        );
    }

    #[test]
    fn responses_json_normalizer_handles_null_error_and_loose_output_shape() {
        let bytes = serde_json::to_vec(&json!({
            "id": "resp_non_sse",
            "object": "response",
            "status": "completed",
            "error": null,
            "output": [{
                "role": "assistant",
                "content": [{
                    "text": "compact summary"
                }]
            }]
        }))
        .unwrap();

        let normalized = normalize_responses_json_for_client(&bytes);
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["output"][0]["id"], "msg_resp_non_sse_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
        assert_eq!(value["error"], Value::Null);
    }

    #[test]
    fn cached_responses_replay_gets_non_sse_shape_when_enabled() {
        let cached_body = serde_json::to_vec(&json!({
            "id": "resp_cached",
            "object": "response",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "cached compact" }]
            }]
        }))
        .unwrap();

        let normalized = normalize_response_body_for_client(
            &Channel::Responses,
            "application/json",
            cached_body,
            true,
        );
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["output"][0]["id"], "msg_resp_cached_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
    }

    #[test]
    fn cached_responses_replay_keeps_fast_shape_when_non_sse_compat_disabled() {
        let cached_body = serde_json::to_vec(&json!({
            "id": "resp_cached_fast",
            "object": "response",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "cached compact" }]
            }]
        }))
        .unwrap();

        let normalized = normalize_response_body_for_client(
            &Channel::Responses,
            "application/json",
            cached_body,
            false,
        );
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert!(value["output"][0].get("id").is_none());
        assert!(value["output"][0]["content"][0]
            .get("annotations")
            .is_none());
    }

    #[test]
    fn mislabeled_responses_json_gets_non_sse_shape_when_enabled() {
        let body = serde_json::to_vec(&json!({
            "id": "resp_mislabeled",
            "object": "response",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "sync body" }]
            }]
        }))
        .unwrap();

        let normalized = normalize_response_body_for_client(
            &Channel::Responses,
            "text/event-stream",
            body,
            true,
        );
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["output"][0]["id"], "msg_resp_mislabeled_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
    }

    #[test]
    fn real_responses_sse_is_left_unchanged_by_shape_normalizer() {
        let body = b"event: response.output_text.delta\ndata: {\"delta\":\"hi\"}\n\n".to_vec();
        let normalized = normalize_response_body_for_client(
            &Channel::Responses,
            "text/event-stream",
            body.clone(),
            true,
        );
        assert_eq!(normalized, body);
    }

    #[test]
    fn responses_non_stream_accepts_json_with_sse_content_type() {
        let body = serde_json::to_vec(&json!({
            "id": "resp_wrong_type",
            "object": "response",
            "model": "gpt-test",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "compact ok" }]
            }]
        }))
        .unwrap();

        let normalized = responses_sse_to_non_stream_json(&body, "gpt-test", true);
        let value: Value = serde_json::from_slice(&normalized).unwrap();

        assert_eq!(value["object"], "response");
        assert_eq!(value["output"][0]["id"], "msg_resp_wrong_type_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
        assert_eq!(value["output"][0]["content"][0]["text"], "compact ok");
    }

    #[test]
    fn responses_non_stream_aggregates_real_sse_completed_event() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done\",\"object\":\"response\",\"model\":\"gpt-test\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hi\"}]}],\"usage\":{\"input_tokens\":12,\"output_tokens\":1,\"prompt_tokens_details\":{\"cached_tokens\":12}}}}\n\n"
        )
        .as_bytes()
        .to_vec();

        let normalized = responses_sse_to_non_stream_json(&body, "gpt-test", true);
        let value: Value = serde_json::from_slice(&normalized).unwrap();

        assert_eq!(value["id"], "resp_done");
        assert_eq!(value["output"][0]["id"], "msg_resp_done_0");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
        assert_eq!(value["usage"]["input_tokens"], 12);
    }

    #[test]
    fn responses_non_stream_aggregates_delta_only_sse() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n",
            "data: [DONE]\n\n"
        )
        .as_bytes()
        .to_vec();

        let normalized = responses_sse_to_non_stream_json(&body, "gpt-test", true);
        let value: Value = serde_json::from_slice(&normalized).unwrap();

        assert_eq!(value["object"], "response");
        assert_eq!(value["model"], "gpt-test");
        assert_eq!(value["output_text"], "hello world");
        assert_eq!(value["output"][0]["content"][0]["annotations"], json!([]));
    }

    #[test]
    fn stream_proxy_requires_client_stream_true() {
        let body = json!({ "stream": true });
        assert!(should_proxy_upstream_as_stream(
            true,
            true,
            false,
            &body,
            "text/event-stream"
        ));
        assert!(!should_proxy_upstream_as_stream(
            true,
            false,
            false,
            &body,
            "text/event-stream"
        ));
        assert!(!should_proxy_upstream_as_stream(
            true,
            true,
            true,
            &body,
            "text/event-stream"
        ));
    }

    fn prefix_state(
        input_tokens: u64,
        cache_read_tokens: u64,
        shortfall_tokens: u64,
    ) -> PrefixWarmState {
        let record = UsageRecord {
            input_tokens,
            cache_read_tokens,
            ..UsageRecord::default()
        };
        PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens,
            cache_read_tokens,
            shortfall_tokens,
            seen_bucket_tokens: provider_cache_bucket_max(input_tokens),
            avoidable_shortfall_tokens: shortfall_tokens,
            avoidable_shortfall_streak: u32::from(shortfall_tokens > 0),
            shortfall_tokens_128: provider_cache_shortfall_128(&record),
            seen_bucket_tokens_128: provider_cache_bucket_max_128(input_tokens),
            avoidable_shortfall_tokens_128: shortfall_tokens,
            small_gap_recovery_streak: u32::from(shortfall_tokens > 0 && shortfall_tokens <= 2048),
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        }
    }

    #[test]
    fn chat_request_converts_to_anthropic_shape() {
        let request = json!({
            "model": "gpt-test",
            "stream": true,
            "temperature": 0.1,
            "max_tokens": 1024,
            "messages": [
                { "role": "system", "content": "You are concise." },
                { "role": "user", "content": "Ping" }
            ]
        });

        let transformed =
            transform_request_for_channel(&request, &Channel::Chat, &Channel::Anthropic);

        assert_eq!(transformed["model"], "gpt-test");
        assert_eq!(transformed["system"], "You are concise.");
        assert_eq!(transformed["max_tokens"], 1024);
        assert_eq!(transformed["messages"][0]["role"], "user");
        assert_eq!(transformed["messages"][0]["content"], "Ping");
    }

    #[test]
    fn anthropic_response_converts_to_chat_completion_shape() {
        let upstream = json!({
            "id": "msg_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-test",
            "content": [{ "type": "text", "text": "Hello" }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 3, "output_tokens": 2 }
        });
        let bytes = serde_json::to_vec(&upstream).unwrap();

        let transformed =
            transform_response_bytes(&Channel::Chat, &Channel::Anthropic, "claude-test", &bytes);
        let value: Value = serde_json::from_slice(&transformed).unwrap();

        assert_eq!(value["object"], "chat.completion");
        assert_eq!(value["choices"][0]["message"]["role"], "assistant");
        assert_eq!(value["choices"][0]["message"]["content"], "Hello");
        assert_eq!(value["usage"]["input_tokens"], 3);
    }

    #[test]
    fn cross_protocol_stream_wrapper_emits_client_sse() {
        let response = json!({
            "id": "chatcmpl_123",
            "object": "chat.completion",
            "model": "gpt-test",
            "choices": [{
                "message": { "role": "assistant", "content": "Hello" },
                "finish_reason": "stop"
            }]
        });
        let bytes = serde_json::to_vec(&response).unwrap();

        let sse = String::from_utf8(response_json_to_sse(&Channel::Responses, &bytes)).unwrap();

        assert!(sse.contains("event: response.output_text.delta"));
        assert!(sse.contains("event: response.completed"));
        assert!(sse.contains("Hello"));
    }

    #[test]
    fn upstream_url_keeps_explicit_endpoint() {
        assert_eq!(
            upstream_url(
                "https://api.example.com/v1/chat/completions",
                &Channel::Chat
            ),
            "https://api.example.com/v1/chat/completions"
        );
        assert_eq!(
            upstream_url("https://api.example.com/v1", &Channel::Responses),
            "https://api.example.com/v1/responses"
        );
    }

    #[test]
    fn chat_to_responses_promotes_developer_rules_to_instructions() {
        let request = json!({
            "model": "gpt-5.5",
            "messages": [
                { "role": "system", "content": "Follow project policy." },
                { "role": "developer", "content": "Skill trigger: use cache-log-skill when logs are mentioned." },
                { "role": "user", "content": "Please inspect the logs." }
            ],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "cache_log_skill",
                    "parameters": { "type": "object", "properties": {} }
                }
            }]
        });

        let mut transformed = chat_to_responses_request(&request);
        normalize_responses_request(&mut transformed);

        let instructions = transformed
            .get("instructions")
            .and_then(Value::as_str)
            .unwrap();
        assert!(instructions.contains("Follow project policy."));
        assert!(instructions.contains("Skill trigger: use cache-log-skill"));
        assert_eq!(
            transformed.pointer("/input/0/role").and_then(Value::as_str),
            Some("user")
        );
        assert_eq!(
            transformed
                .pointer("/input/0/content/0/text")
                .and_then(Value::as_str),
            Some("Please inspect the logs.")
        );
        assert_eq!(
            transformed
                .get("input")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn responses_normalizer_promotes_developer_input_to_instructions() {
        let mut request = json!({
            "model": "gpt-5.5",
            "instructions": "Existing stable instructions.",
            "input": [
                {
                    "type": "message",
                    "role": "developer",
                    "content": [{ "type": "input_text", "text": "Skill trigger: inspect logs automatically." }]
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": "Please inspect the logs."
                }
            ]
        });

        normalize_responses_request(&mut request);

        let instructions = request.get("instructions").and_then(Value::as_str).unwrap();
        assert!(instructions.contains("Existing stable instructions."));
        assert!(instructions.contains("Skill trigger: inspect logs automatically."));
        assert_eq!(
            request
                .get("input")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            request.pointer("/input/0/role").and_then(Value::as_str),
            Some("user")
        );
    }

    #[test]
    fn responses_messages_promote_developer_without_user_downgrade() {
        let mut request = json!({
            "model": "gpt-5.5",
            "messages": [
                { "role": "system", "content": "System rule." },
                { "role": "developer", "content": "Developer skill rule." },
                { "role": "user", "content": "Run normally." }
            ]
        });

        normalize_responses_request(&mut request);

        let instructions = request.get("instructions").and_then(Value::as_str).unwrap();
        assert!(instructions.contains("System rule."));
        assert!(instructions.contains("Developer skill rule."));
        assert_eq!(
            request
                .get("input")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            request
                .pointer("/input/0/content/0/text")
                .and_then(Value::as_str),
            Some("Run normally.")
        );
    }

    #[test]
    fn prefix_prompt_cache_key_stays_within_provider_limit() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "default-workspace-with-a-long-name".to_string();
        let provider = ProviderConfig {
            id: "71c595c5-a6c7-4a57-a01e-6fd1e4c101f9".to_string(),
            name: "Long Provider".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
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
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let request = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "stable prefix" }]
        });
        let key = provider_prompt_cache_key(&config, &decision, &request, &Channel::Responses);

        assert_eq!(key.len(), 64);
    }

    #[test]
    fn prefix_optimizer_overwrites_dynamic_prompt_cache_key() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "this-user-provided-key-is-too-long-and-dynamic-for-provider-prefix-cache",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        let key = request
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(key.len(), 64);
        assert_eq!(
            key,
            provider_prompt_cache_key(&config, &decision, &request, &Channel::Responses)
        );
    }

    #[test]
    fn smart_hit_disabled_strips_provider_cache_fields_and_session_features() {
        let mut config = AppConfig::default();
        config.cache.enabled = false;
        config.cache.prewarm_enabled = false;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
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
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "client-key",
            "prompt_cache_retention": "24h",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        assert!(request.get("prompt_cache_key").is_none());
        assert!(request.get("prompt_cache_retention").is_none());
        assert!(!responses_session_reuse_enabled(&config));
        assert!(!local_session_keys_enabled(&config));
    }

    #[test]
    fn prefix_optimizer_preserves_valid_client_prompt_cache_key() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "client-session-cache-key",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        assert_eq!(
            request.get("prompt_cache_key").and_then(Value::as_str),
            Some("client-session-cache-key")
        );
    }

    #[test]
    fn chat_prefix_optimizer_stabilizes_actual_outbound_body() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Chat,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Chat,
            model: "gpt-5.5".to_string(),
        };
        let mut left = json!({
            "model": "gpt-5.5",
            "request_id": "req-left",
            "messages": [
                { "role": "system", "content": "Stable instructions" },
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"README.md\",\"limit\":20}"
                        }
                    }]
                }
            ],
            "tools": [{
                "description": "Read file",
                "parameters": {
                    "required": ["limit", "path"],
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "limit": { "type": "integer" }
                    }
                },
                "name": "read_file"
            }],
            "tool_choice": { "function": { "name": "read_file" }, "type": "function" }
        });
        let mut right = json!({
            "tool_choice": { "type": "function", "function": { "name": "read_file" } },
            "tools": [{
                "name": "read_file",
                "type": "function",
                "parameters": {
                    "properties": {
                        "limit": { "type": "integer" },
                        "path": { "type": "string" }
                    },
                    "required": ["path", "limit"],
                    "type": "object"
                },
                "description": "Read file"
            }],
            "messages": [
                { "content": "Stable instructions", "role": "system" },
                {
                    "tool_calls": [{
                        "function": {
                            "arguments": "{\"limit\":20,\"path\":\"README.md\"}",
                            "name": "read_file"
                        },
                        "type": "function",
                        "id": "call_1"
                    }],
                    "role": "assistant"
                }
            ],
            "model": "gpt-5.5",
            "request_id": "req-right"
        });

        optimize_provider_prefix(&mut left, &config, &decision);
        optimize_provider_prefix(&mut right, &config, &decision);

        assert_eq!(left, right);
        assert_eq!(
            left.pointer("/messages/1/tool_calls/0/id")
                .and_then(Value::as_str),
            Some("call_1")
        );
        assert_eq!(
            left.pointer("/messages/1/tool_calls/0/function/arguments")
                .and_then(Value::as_str),
            Some("{\"limit\":20,\"path\":\"README.md\"}")
        );
    }

    #[test]
    fn anthropic_prefix_optimizer_stabilizes_actual_outbound_body() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "anth".to_string(),
            name: "Anthropic".to_string(),
            base_url: "https://api.anthropic.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Anthropic,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Anthropic,
            model: "claude-sonnet".to_string(),
        };
        let mut left = json!({
            "model": "claude-sonnet",
            "trace_id": "trace-left",
            "system": "Stable system",
            "tools": [{
                "description": "Read file",
                "input_schema": {
                    "required": ["limit", "path"],
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "limit": { "type": "integer" }
                    }
                },
                "name": "read_file"
            }],
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "read_file",
                    "input": { "path": "README.md", "limit": 20 }
                }]
            }]
        });
        let mut right = json!({
            "trace_id": "trace-right",
            "messages": [{
                "content": [{
                    "input": { "limit": 20, "path": "README.md" },
                    "name": "read_file",
                    "id": "toolu_1",
                    "type": "tool_use"
                }],
                "role": "user"
            }],
            "tools": [{
                "name": "read_file",
                "input_schema": {
                    "properties": {
                        "limit": { "type": "integer" },
                        "path": { "type": "string" }
                    },
                    "required": ["path", "limit"],
                    "type": "object"
                },
                "description": "Read file"
            }],
            "system": "Stable system",
            "model": "claude-sonnet"
        });

        optimize_provider_prefix(&mut left, &config, &decision);
        optimize_provider_prefix(&mut right, &config, &decision);

        assert_eq!(left, right);
        assert_eq!(count_cache_control_fields(&left), 2);
        assert_eq!(
            left.pointer("/messages/0/content/0/id")
                .and_then(Value::as_str),
            Some("toolu_1")
        );
    }

    #[test]
    fn third_party_responses_provider_does_not_send_prompt_cache_retention() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "torch".to_string(),
            name: "torch".to_string(),
            base_url: "https://torchai.ai/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "prompt_cache_retention": "24h",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        assert!(request.get("prompt_cache_key").is_some());
        assert!(request.get("prompt_cache_retention").is_none());
    }

    #[test]
    fn third_party_responses_provider_can_opt_into_prompt_cache_retention() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "torch".to_string(),
            name: "torch".to_string(),
            base_url: "https://torchai.ai/v1".to_string(),
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
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        assert_eq!(
            request
                .get("prompt_cache_retention")
                .and_then(Value::as_str),
            Some("24h")
        );
    }

    #[test]
    fn official_openai_responses_provider_keeps_prompt_cache_retention() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let provider = ProviderConfig {
            id: "openai".to_string(),
            name: "openai".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut request = json!({
            "model": "gpt-5.5",
            "input": [{ "role": "user", "content": "hello" }]
        });

        optimize_provider_prefix(&mut request, &config, &decision);

        assert_eq!(
            request
                .get("prompt_cache_retention")
                .and_then(Value::as_str),
            Some("24h")
        );
    }

    #[test]
    fn provider_prompt_cache_key_separates_session_anchor_and_ignores_dynamic_noise() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let base = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "project alpha stable prefix" }]
        });
        let same_prefix_with_dynamic_noise = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "external-dynamic-key",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "project alpha stable prefix" }],
            "metadata": { "request": "dynamic" },
            "request_id": "req-dynamic"
        });
        let different_prefix = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "project beta stable prefix" }]
        });
        let changed_stable_header = json!({
            "model": "gpt-5.5",
            "instructions": "different stable system",
            "input": [{ "type": "message", "role": "user", "content": "project alpha stable prefix" }]
        });
        let different_model_decision = RouteDecision {
            model: "gpt-5.4".to_string(),
            ..decision.clone()
        };

        let base_key = provider_prompt_cache_key(&config, &decision, &base, &Channel::Responses);
        let same_key = provider_prompt_cache_key(
            &config,
            &decision,
            &same_prefix_with_dynamic_noise,
            &Channel::Responses,
        );
        let different_key =
            provider_prompt_cache_key(&config, &decision, &different_prefix, &Channel::Responses);
        let changed_header_key = provider_prompt_cache_key(
            &config,
            &decision,
            &changed_stable_header,
            &Channel::Responses,
        );
        let different_model_key = provider_prompt_cache_key(
            &config,
            &different_model_decision,
            &base,
            &Channel::Responses,
        );

        assert_eq!(base_key, same_key);
        assert_ne!(base_key, different_key);
        assert_ne!(base_key, changed_header_key);
        assert_ne!(base_key, different_model_key);
        assert_eq!(
            provider_prefix_fingerprint(&base, &Channel::Responses),
            provider_prefix_fingerprint(&same_prefix_with_dynamic_noise, &Channel::Responses)
        );
        assert_ne!(
            provider_prefix_fingerprint(&base, &Channel::Responses),
            provider_prefix_fingerprint(&different_prefix, &Channel::Responses)
        );
        assert_ne!(
            provider_prefix_fingerprint(&base, &Channel::Responses),
            provider_prefix_fingerprint(&changed_stable_header, &Channel::Responses)
        );
    }

    #[test]
    fn provider_prompt_cache_key_uses_configured_model_for_client_alias() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "agent-codex-hb".to_string(),
            name: "hb / Codex".to_string(),
            base_url: "https://hubway.cc/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: true,
            request_body_gzip_enabled: true,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(256000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let alias_decision = RouteDecision {
            provider: provider.clone(),
            upstream_channel: Channel::Responses,
            model: "codex-auto-review".to_string(),
        };
        let configured_decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let alias_request = json!({
            "model": "codex-auto-review",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "dynamic tail" }]
        });
        let configured_request = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "dynamic tail" }]
        });

        assert_eq!(
            provider_prompt_cache_key(
                &config,
                &alias_decision,
                &alias_request,
                &Channel::Responses
            ),
            provider_prompt_cache_key(
                &config,
                &configured_decision,
                &configured_request,
                &Channel::Responses
            )
        );
        assert_eq!(
            provider_prefix_control_key(
                Some("fingerprint-a"),
                &alias_decision,
                &Channel::Responses
            )
            .as_deref(),
            Some("https://hubway.cc/v1\0gpt-5.5\0responses\0fingerprint-a")
        );
    }

    #[test]
    fn responses_provider_cache_key_uses_stable_session_anchor() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let first = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "first task" }]
        });
        let same_session_appended = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [
                { "type": "message", "role": "user", "content": "first task" },
                { "type": "message", "role": "assistant", "content": "covered" },
                { "type": "message", "role": "user", "content": "continue with a longer tail" }
            ]
        });
        let different_session = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "second task with a longer tail" }]
        });

        let first_cache_key =
            provider_prompt_cache_key(&config, &decision, &first, &Channel::Responses);
        let same_session_cache_key = provider_prompt_cache_key(
            &config,
            &decision,
            &same_session_appended,
            &Channel::Responses,
        );
        let different_session_cache_key =
            provider_prompt_cache_key(&config, &decision, &different_session, &Channel::Responses);
        assert_eq!(first_cache_key, same_session_cache_key);
        assert_ne!(first_cache_key, different_session_cache_key);
        assert_eq!(
            provider_prefix_fingerprint(&first, &Channel::Responses),
            provider_prefix_fingerprint(&same_session_appended, &Channel::Responses)
        );
        assert_ne!(
            provider_prefix_fingerprint(&first, &Channel::Responses),
            provider_prefix_fingerprint(&different_session, &Channel::Responses)
        );
    }

    #[test]
    fn upstream_headers_preserve_client_capabilities_and_replace_local_auth() {
        let mut inbound = HeaderMap::new();
        inbound.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-proxy-key"),
        );
        inbound.insert("x-api-key", HeaderValue::from_static("local-proxy-key"));
        inbound.insert(header::HOST, HeaderValue::from_static("127.0.0.1:18883"));
        inbound.insert(header::CONTENT_LENGTH, HeaderValue::from_static("123"));
        inbound.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        inbound.insert("x-request-id", HeaderValue::from_static("local-request"));
        inbound.insert("traceparent", HeaderValue::from_static("00-local-trace"));
        inbound.insert(
            header::USER_AGENT,
            HeaderValue::from_static("codex-desktop/1.2.3"),
        );
        inbound.insert("openai-beta", HeaderValue::from_static("responses=v1"));
        inbound.insert("x-openai-client-version", HeaderValue::from_static("1.2.3"));

        let outbound = build_upstream_request_headers(
            &inbound,
            "real-upstream-key",
            &Channel::Responses,
            true,
            None,
            false,
        );

        assert_eq!(
            outbound
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer real-upstream-key")
        );
        assert_eq!(
            outbound
                .get("x-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("real-upstream-key")
        );
        assert_eq!(
            outbound
                .get(header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some("codex-desktop/1.2.3")
        );
        assert_eq!(
            outbound
                .get("openai-beta")
                .and_then(|value| value.to_str().ok()),
            Some("responses=v1")
        );
        assert_eq!(
            outbound
                .get("x-openai-client-version")
                .and_then(|value| value.to_str().ok()),
            Some("1.2.3")
        );
        assert_eq!(
            outbound
                .get(header::ACCEPT)
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream")
        );
        assert_eq!(
            outbound
                .get(header::ACCEPT_ENCODING)
                .and_then(|value| value.to_str().ok()),
            Some("identity")
        );
        for blocked in [
            header::HOST.as_str(),
            header::CONTENT_LENGTH.as_str(),
            header::CONNECTION.as_str(),
            "x-request-id",
            "traceparent",
        ] {
            assert!(
                outbound.get(blocked).is_none(),
                "{blocked} must not be forwarded"
            );
        }
    }

    #[test]
    fn upstream_headers_apply_provider_user_agent_override() {
        let mut inbound = HeaderMap::new();
        inbound.insert(
            header::USER_AGENT,
            HeaderValue::from_static("codex-desktop/1.2.3"),
        );

        let outbound = build_upstream_request_headers(
            &inbound,
            "real-upstream-key",
            &Channel::Responses,
            false,
            Some("Provider-Compatible/2.0"),
            true,
        );

        assert_eq!(
            outbound
                .get(header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some("Provider-Compatible/2.0")
        );
        assert_eq!(
            outbound
                .get(header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok()),
            Some("gzip")
        );
    }

    #[test]
    fn invalid_upstream_key_does_not_emit_fake_authorization() {
        let outbound = build_upstream_request_headers(
            &HeaderMap::new(),
            "invalid\nkey",
            &Channel::Responses,
            true,
            None,
            false,
        );

        assert!(outbound.get(header::AUTHORIZATION).is_none());
        assert!(outbound.get("x-api-key").is_none());
    }

    #[test]
    fn upstream_processing_timing_prefers_explicit_envoy_header() {
        let mut headers = HeaderMap::new();
        headers.append(
            "server-timing",
            HeaderValue::from_static("edge;dur=12.5, app;dur=120.4"),
        );
        headers.insert("x-response-time", HeaderValue::from_static("140ms"));
        headers.insert(
            "x-envoy-upstream-service-time",
            HeaderValue::from_static("87"),
        );

        assert_eq!(
            reported_upstream_processing_ms(&headers),
            Some(("x-envoy-upstream-service-time", 87))
        );
        assert_eq!(
            response_header_values(&headers, "server-timing").as_deref(),
            Some("edge;dur=12.5, app;dur=120.4")
        );
    }

    #[test]
    fn upstream_processing_timing_parses_server_and_process_durations() {
        let mut server_headers = HeaderMap::new();
        server_headers.append(
            "server-timing",
            HeaderValue::from_static("edge;dur=12.5, app;dur=120.4"),
        );
        assert_eq!(
            reported_upstream_processing_ms(&server_headers),
            Some(("server-timing", 120))
        );

        let mut process_headers = HeaderMap::new();
        process_headers.insert("x-process-time", HeaderValue::from_static("0.245"));
        assert_eq!(
            reported_upstream_processing_ms(&process_headers),
            Some(("x-process-time", 245))
        );
        assert_eq!(parse_duration_ms("1.5s", 1.0), Some(1500));
        assert_eq!(parse_duration_ms("32ms", 1000.0), Some(32));
    }

    #[test]
    fn upstream_trace_id_reads_response_headers_without_injecting_a_request_id() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-ray", HeaderValue::from_static("fallback-ray"));
        headers.insert("x-request-id", HeaderValue::from_static("upstream-request"));

        assert_eq!(
            upstream_response_trace_id(&headers),
            Some(("x-request-id", "upstream-request".to_string()))
        );
        assert_eq!(upstream_response_trace_id(&HeaderMap::new()), None);
    }

    #[test]
    fn chat_provider_cache_key_keeps_stable_prefix_and_ignores_dynamic_tail() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "Share".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Chat,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let decision = RouteDecision {
            provider,
            upstream_channel: Channel::Chat,
            model: "gpt-5.5".to_string(),
        };
        let first = json!({
            "model": "gpt-5.5",
            "metadata": { "trace": "one" },
            "messages": [
                { "role": "system", "content": "stable project policy" },
                { "role": "developer", "content": "stable tool rules" },
                { "role": "user", "content": "first task" }
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "write_file",
                        "parameters": {
                            "type": "object",
                            "required": ["content", "path"],
                            "properties": {
                                "path": { "type": "string" },
                                "content": { "type": "string" }
                            }
                        }
                    }
                }
            ]
        });
        let second = json!({
            "model": "gpt-5.5",
            "metadata": { "trace": "two" },
            "messages": [
                { "role": "system", "content": "stable project policy" },
                { "role": "developer", "content": "stable tool rules" },
                { "role": "user", "content": "second task with fresh files" },
                { "role": "assistant", "content": "working" },
                { "role": "tool", "tool_call_id": "call_dynamic", "content": "dynamic output" }
            ],
            "tools": [
                {
                    "function": {
                        "parameters": {
                            "required": ["path", "content"],
                            "type": "object",
                            "properties": {
                                "content": { "type": "string" },
                                "path": { "type": "string" }
                            }
                        },
                        "name": "write_file"
                    },
                    "type": "function"
                }
            ]
        });
        let changed_system = json!({
            "model": "gpt-5.5",
            "messages": [
                { "role": "system", "content": "different project policy" },
                { "role": "user", "content": "first task" }
            ]
        });

        assert_eq!(
            provider_prompt_cache_key(&config, &decision, &first, &Channel::Chat),
            provider_prompt_cache_key(&config, &decision, &second, &Channel::Chat)
        );
        assert_ne!(
            provider_prompt_cache_key(&config, &decision, &first, &Channel::Chat),
            provider_prompt_cache_key(&config, &decision, &changed_system, &Channel::Chat)
        );
        assert_eq!(
            provider_prefix_fingerprint(&first, &Channel::Chat),
            provider_prefix_fingerprint(&second, &Channel::Chat)
        );
        assert_eq!(
            provider_prompt_cache_key(&config, &decision, &first, &Channel::Chat).len(),
            64
        );
    }

    #[test]
    fn provider_prompt_cache_key_does_not_split_same_prefix_by_provider_id() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let request = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "project alpha stable prefix" }]
        });
        let share_decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://api.openai.com/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let torch_decision = RouteDecision {
            provider: ProviderConfig {
                id: "torchai".to_string(),
                name: "TorchAI".to_string(),
                base_url: "https://torchai.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };

        assert_eq!(
            provider_prompt_cache_key(&config, &share_decision, &request, &Channel::Responses),
            provider_prompt_cache_key(&config, &torch_decision, &request, &Channel::Responses)
        );
    }

    #[test]
    fn provider_prefix_control_key_is_scoped_per_upstream_provider() {
        let share_decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let api_decision = RouteDecision {
            provider: ProviderConfig {
                id: "api-1".to_string(),
                name: "api.1".to_string(),
                base_url: "https://api.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };

        let share_key = provider_prefix_control_key(
            Some("stable-provider-key"),
            &share_decision,
            &Channel::Responses,
        );
        let api_key = provider_prefix_control_key(
            Some("stable-provider-key"),
            &api_decision,
            &Channel::Responses,
        );

        assert_ne!(share_key, api_key);
    }

    #[test]
    fn provider_prefix_state_alias_reuses_same_model_channel_fingerprint() {
        let share_key = "https://share.example/v1\0gpt-5.5\0responses\0stable-fingerprint";
        let share_clone_key = "https://share.example/v1\0gpt-5.5\0responses\0stable-fingerprint";
        let api_key = "https://api.example/v1\0gpt-5.5\0responses\0stable-fingerprint";

        assert_eq!(
            provider_prefix_state_alias_key(share_key),
            provider_prefix_state_alias_key(share_clone_key)
        );
        assert_ne!(
            provider_prefix_state_alias_key(share_key),
            provider_prefix_state_alias_key(api_key)
        );
        assert_eq!(
            provider_prefix_state_alias_key(share_key).as_deref(),
            Some("prefix-alias\0https://share.example/v1\0gpt-5.5\0responses\0stable-fingerprint")
        );
    }

    #[test]
    fn response_session_delta_only_accepts_array_append() {
        let previous = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "two" }
        ]);
        let appended = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "two" },
            { "type": "message", "role": "user", "content": "three" }
        ]);
        let changed = json!([
            { "type": "message", "role": "user", "content": "one changed" },
            { "type": "message", "role": "assistant", "content": "two" },
            { "type": "message", "role": "user", "content": "three" }
        ]);

        assert_eq!(
            appended_response_input_delta(&previous, &appended),
            Some(json!([{ "type": "message", "role": "user", "content": "three" }]))
        );
        assert_eq!(appended_response_input_delta(&previous, &changed), None);
        assert_eq!(
            appended_response_input_delta(&previous, &json!("plain text")),
            None
        );
    }

    #[test]
    fn large_exact_session_append_uses_raw_prefix_delta() {
        let previous = (0..800)
            .map(|index| {
                json!({
                    "type": "function_call_output",
                    "call_id": format!("call-{index}"),
                    "output": format!("line-{index}: {}", "x".repeat(256))
                })
            })
            .collect::<Vec<_>>();
        let mut current = previous.clone();
        current.push(json!({
            "type": "message",
            "role": "user",
            "content": "continue"
        }));

        assert_eq!(
            response_input_raw_prefix_delta_start_index(&previous, &current),
            Some(800)
        );
        assert_eq!(
            response_input_delta_start_index(&previous, &current),
            Some(800)
        );
    }

    #[test]
    fn response_session_delta_accepts_normalized_equivalent_prefix() {
        let previous = json!([
            {
                "id": "msg_dynamic_old",
                "type": "message",
                "status": "completed",
                "role": "user",
                "content": [{ "type": "text", "text": "one" }],
                "metadata": { "trace": "old" }
            },
            {
                "id": "call_dynamic_old",
                "type": "function_call",
                "call_id": "call_old",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\",\"limit\":20}"
            },
            {
                "id": "out_dynamic_old",
                "type": "function_call_output",
                "call_id": "call_old",
                "output": "{\"ok\":true,\"lines\":2}"
            }
        ]);
        let current = json!([
            {
                "id": "msg_dynamic_new",
                "type": "message",
                "status": "in_progress",
                "role": "user",
                "content": [{ "type": "input_text", "text": "one" }],
                "metadata": { "trace": "new" }
            },
            {
                "id": "call_dynamic_new",
                "type": "function_call",
                "call_id": "call_new",
                "name": "read_file",
                "arguments": "{\"limit\":20,\"path\":\"README.md\"}"
            },
            {
                "id": "out_dynamic_new",
                "type": "function_call_output",
                "call_id": "call_new",
                "output": "{\"lines\":2,\"ok\":true}"
            },
            { "type": "message", "role": "user", "content": "continue" }
        ]);

        assert_eq!(
            appended_response_input_delta(&previous, &current),
            Some(json!([{ "type": "message", "role": "user", "content": "continue" }]))
        );
    }

    #[test]
    fn response_session_delta_skips_model_outputs_already_covered_by_previous_response() {
        let previous = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "thinking done" },
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "{\"ok\":true}"
            }
        ]);
        let current = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "thinking done" },
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "{\"ok\":true}"
            },
            { "type": "message", "role": "user", "content": "continue" }
        ]);

        assert_eq!(
            appended_response_input_delta(&previous, &current),
            Some(json!([{ "type": "message", "role": "user", "content": "continue" }]))
        );
    }

    #[test]
    fn response_session_delta_keeps_new_tool_outputs_after_model_call() {
        let previous = json!([
            { "type": "message", "role": "user", "content": "one" }
        ]);
        let current = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "reasoning", "summary": [] },
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "{\"ok\":true}"
            },
            { "type": "message", "role": "user", "content": "continue" }
        ]);

        assert_eq!(
            appended_response_input_delta(&previous, &current),
            Some(json!([
                {
                    "type": "function_call_output",
                    "call_id": "call_123",
                    "output": "{\"ok\":true}"
                },
                { "type": "message", "role": "user", "content": "continue" }
            ]))
        );
    }

    #[test]
    fn response_session_delta_does_not_send_model_output_only_tail() {
        let previous = json!([
            { "type": "message", "role": "user", "content": "one" }
        ]);
        let current = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "reasoning", "summary": [] },
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            }
        ]);

        assert_eq!(appended_response_input_delta(&previous, &current), None);
    }

    #[test]
    fn response_session_delta_rejects_same_message_user_text_tail_by_default() {
        let previous = json!([
            {
                "type": "message",
                "role": "user",
                "content": [{ "type": "input_text", "text": "inspect cache logs" }]
            }
        ]);
        let current = json!([
            {
                "type": "message",
                "role": "user",
                "content": [{ "type": "input_text", "text": "inspect cache logs and summarize new gaps" }]
            }
        ]);

        assert_eq!(appended_response_input_delta(&previous, &current), None);
    }

    #[test]
    fn response_session_delta_rejects_changed_user_text_tail() {
        let previous = json!([
            { "type": "message", "role": "user", "content": "inspect cache logs" }
        ]);
        let changed = json!([
            { "type": "message", "role": "user", "content": "summarize cache logs" }
        ]);

        assert_eq!(appended_response_input_delta(&previous, &changed), None);
    }

    #[test]
    fn response_session_key_stays_stable_for_appended_turns() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let first = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "project alpha stable anchor" }]
        });
        let appended = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "previous_response_id": "resp_old",
            "input": [
                { "type": "message", "role": "user", "content": "project alpha stable anchor" },
                { "type": "message", "role": "assistant", "content": "done" },
                { "type": "message", "role": "user", "content": "continue" }
            ]
        });
        let different_anchor = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "project beta stable anchor" }]
        });

        let first_key = responses_session_key(&config, &decision, &first).unwrap();
        let appended_key = responses_session_key(&config, &decision, &appended).unwrap();
        let different_key = responses_session_key(&config, &decision, &different_anchor).unwrap();

        assert_eq!(first_key, appended_key);
        assert_ne!(first_key, different_key);
    }

    #[test]
    fn response_session_scope_unifies_equivalent_skill_instruction_shapes() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let mut canonical = json!({
            "model": "gpt-5.5",
            "instructions": "Stable system.\n\nSkill trigger: inspect logs automatically.",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "anchor" }]
        });
        let mut equivalent = json!({
            "model": "gpt-5.5",
            "instructions": "Stable system.",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [
                {
                    "type": "message",
                    "role": "developer",
                    "content": [{ "type": "input_text", "text": "Skill trigger: inspect logs automatically." }]
                },
                { "type": "message", "role": "user", "content": "different tail still same scope" }
            ]
        });
        let mut changed_skill = equivalent.clone();
        changed_skill["input"][0]["content"][0]["text"] =
            Value::String("Skill trigger: use a different tool.".to_string());

        normalize_responses_request(&mut canonical);
        normalize_responses_request(&mut equivalent);
        normalize_responses_request(&mut changed_skill);

        let canonical_scope = responses_session_scope_key(&config, &decision, &canonical).unwrap();
        let equivalent_scope =
            responses_session_scope_key(&config, &decision, &equivalent).unwrap();
        let changed_scope =
            responses_session_scope_key(&config, &decision, &changed_skill).unwrap();

        assert_eq!(canonical_scope, equivalent_scope);
        assert_ne!(canonical_scope, changed_scope);
    }

    #[test]
    fn response_session_fallback_finds_strict_essential_prefix() {
        let current = json!([
            { "type": "message", "role": "user", "content": "anchor" },
            { "type": "message", "role": "assistant", "content": "covered" },
            { "type": "message", "role": "user", "content": "continue" }
        ]);
        let mut sessions = HashMap::new();
        sessions.insert(
            "old-key".to_string(),
            ResponseSessionState {
                response_id: "resp_old".to_string(),
                scope_key: Some("scope-a".to_string()),
                input: json!([
                    { "type": "message", "role": "user", "content": "anchor" },
                    { "type": "message", "role": "assistant", "content": "covered" }
                ]),
                finished_at: Instant::now(),
            },
        );

        let fallback =
            fallback_response_session(&sessions, "new-key", Some("scope-a"), &current).unwrap();

        assert_eq!(fallback.response_id, "resp_old");
    }

    #[tokio::test]
    async fn response_session_reuse_allows_safe_streaming_main_delta() {
        let config = AppConfig::default();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let dir = std::env::temp_dir().join(format!(
            "atoapi-stream-session-skip-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.response_sessions.lock().await.insert(
            "session-a".to_string(),
            ResponseSessionState {
                response_id: "resp_old".to_string(),
                scope_key: Some("scope-a".to_string()),
                input: json!([{ "type": "message", "role": "user", "content": "anchor" }]),
                finished_at: Instant::now(),
            },
        );
        let request = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor" },
                { "type": "message", "role": "user", "content": "continue" }
            ]
        });

        let outcome = maybe_reuse_response_session(
            &state,
            &request,
            Some("session-a"),
            Some("scope-a"),
            &decision,
            true,
            false,
        )
        .await;
        let optimized = outcome.body;

        assert_eq!(optimized["previous_response_id"], "resp_old");
        assert_eq!(
            optimized["input"],
            json!([{ "type": "message", "role": "user", "content": "continue" }])
        );
        assert_eq!(optimized["stream"], true);
        assert_eq!(outcome.diagnostics.candidate_count, 1);
        assert!(outcome.diagnostics.append_delta_match);
        assert_eq!(outcome.diagnostics.delta_items, 1);
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn main_response_session_delta_does_not_use_scope_sibling() {
        let config = AppConfig::default();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let dir = std::env::temp_dir().join(format!(
            "atoapi-main-session-no-scope-fallback-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.response_sessions.lock().await.insert(
            "sibling-key".to_string(),
            ResponseSessionState {
                response_id: "resp_sibling".to_string(),
                scope_key: Some("scope-a".to_string()),
                input: json!([{ "type": "message", "role": "user", "content": "anchor" }]),
                finished_at: Instant::now(),
            },
        );
        let request = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor" },
                { "type": "message", "role": "user", "content": "continue" }
            ]
        });

        let outcome = maybe_reuse_response_session(
            &state,
            &request,
            Some("current-key"),
            Some("scope-a"),
            &decision,
            true,
            false,
        )
        .await;

        assert_eq!(outcome.body, request);
        assert_eq!(outcome.diagnostics.candidate_count, 0);
        assert_eq!(outcome.diagnostics.scope_match_count, 1);
        assert_eq!(
            outcome.diagnostics.skip_reason.as_deref(),
            Some("no_append_prefix_candidate")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn main_response_session_delta_requires_real_size_win() {
        let original = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor" },
                { "type": "message", "role": "user", "content": "tiny" }
            ]
        });
        let delta = json!({
            "model": "gpt-5.5",
            "stream": true,
            "previous_response_id": "resp_old",
            "store": true,
            "input": [
                { "type": "message", "role": "user", "content": "tiny" }
            ]
        });
        let beneficial_original = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor ".repeat(3000) },
                { "type": "message", "role": "user", "content": "tiny" }
            ]
        });

        assert!(!response_session_delta_is_beneficial(
            &original,
            &delta,
            &TailInputDiagnostics::default()
        ));
        assert!(response_session_delta_is_beneficial(
            &beneficial_original,
            &delta,
            &TailInputDiagnostics::default()
        ));
    }

    #[test]
    fn main_response_session_delta_gate_is_strict() {
        let mut config = AppConfig::default();
        config.cache.enabled = true;
        config.cache.prewarm_enabled = true;
        config.cache.mode = CacheMode::PrefixPrewarm;
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://api.openai.com/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let request = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor ".repeat(20_000) }
            ]
        });
        let exact_anchor = SessionAnchorDiagnostics {
            source: Some("exact".to_string()),
            ..SessionAnchorDiagnostics::default()
        };
        let sibling_anchor = SessionAnchorDiagnostics {
            source: Some("scope".to_string()),
            ..SessionAnchorDiagnostics::default()
        };

        assert!(should_attempt_main_response_session_delta(
            &config,
            &decision,
            true,
            &request,
            &TailInputDiagnostics::default(),
            &exact_anchor,
        ));
        assert!(!should_attempt_main_response_session_delta(
            &config,
            &decision,
            true,
            &request,
            &TailInputDiagnostics::default(),
            &sibling_anchor,
        ));
        assert!(!should_attempt_main_response_session_delta(
            &config,
            &decision,
            false,
            &request,
            &TailInputDiagnostics::default(),
            &exact_anchor,
        ));
        let third_party_decision = RouteDecision {
            provider: ProviderConfig {
                base_url: "https://hubway.cc/v1".to_string(),
                ..decision.provider.clone()
            },
            ..decision
        };
        assert!(should_attempt_main_response_session_delta(
            &config,
            &third_party_decision,
            true,
            &request,
            &TailInputDiagnostics::default(),
            &exact_anchor,
        ));
    }

    #[test]
    fn codex_disables_main_response_session_delta() {
        assert!(main_response_session_delta_enabled_for_agent(None));
        assert!(main_response_session_delta_enabled_for_agent(Some("zcode")));
        assert!(!main_response_session_delta_enabled_for_agent(Some(
            "codex"
        )));
    }

    #[tokio::test]
    async fn response_session_rescue_allows_delta_for_413() {
        let config = AppConfig::default();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "share".to_string(),
                name: "Share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-413-rescue-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.response_sessions.lock().await.insert(
            "session-a".to_string(),
            ResponseSessionState {
                response_id: "resp_old".to_string(),
                scope_key: Some("scope-a".to_string()),
                input: json!([{ "type": "message", "role": "user", "content": "anchor" }]),
                finished_at: Instant::now(),
            },
        );
        let request = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                { "type": "message", "role": "user", "content": "anchor" },
                { "type": "message", "role": "user", "content": "continue" }
            ]
        });

        let rescue = maybe_rescue_response_session_after_413(
            &state,
            &request,
            Some("session-a"),
            Some("scope-a"),
            &decision,
        )
        .await;
        let rescued = rescue.body;

        assert_eq!(rescued["previous_response_id"], "resp_old");
        assert_eq!(
            rescued["input"],
            json!([{ "type": "message", "role": "user", "content": "continue" }])
        );
        assert!(rescue.diagnostics.append_delta_match);
        assert_eq!(rescue.diagnostics.delta_items, 1);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn response_session_413_rescue_is_only_for_uncached_responses_overflow() {
        assert!(should_attempt_response_session_rescue_after_413(
            413,
            false,
            false,
            &Channel::Responses,
            false
        ));
        assert!(!should_attempt_response_session_rescue_after_413(
            413,
            true,
            false,
            &Channel::Responses,
            false
        ));
        assert!(!should_attempt_response_session_rescue_after_413(
            413,
            false,
            true,
            &Channel::Responses,
            false
        ));
        assert!(!should_attempt_response_session_rescue_after_413(
            413,
            false,
            false,
            &Channel::Chat,
            false
        ));
        assert!(!should_attempt_response_session_rescue_after_413(
            413,
            false,
            false,
            &Channel::Responses,
            true
        ));
        assert!(!should_attempt_response_session_rescue_after_413(
            400,
            false,
            false,
            &Channel::Responses,
            false
        ));
    }

    #[test]
    fn response_session_fallback_rejects_changed_essential_prefix() {
        let current = json!([
            { "type": "message", "role": "user", "content": "changed" },
            { "type": "message", "role": "user", "content": "continue" }
        ]);
        let mut sessions = HashMap::new();
        sessions.insert(
            "old-key".to_string(),
            ResponseSessionState {
                response_id: "resp_old".to_string(),
                scope_key: Some("scope-a".to_string()),
                input: json!([
                    { "type": "message", "role": "user", "content": "anchor" }
                ]),
                finished_at: Instant::now(),
            },
        );

        assert!(
            fallback_response_session(&sessions, "new-key", Some("scope-a"), &current).is_none()
        );
    }

    #[test]
    fn response_session_fallback_rejects_different_skill_scope() {
        let current = json!([
            { "type": "message", "role": "user", "content": "anchor" },
            { "type": "message", "role": "user", "content": "continue" }
        ]);
        let mut sessions = HashMap::new();
        sessions.insert(
            "old-key".to_string(),
            ResponseSessionState {
                response_id: "resp_without_skill".to_string(),
                scope_key: Some("scope-without-skill".to_string()),
                input: json!([
                    { "type": "message", "role": "user", "content": "anchor" }
                ]),
                finished_at: Instant::now(),
            },
        );

        assert!(fallback_response_session(
            &sessions,
            "new-key",
            Some("scope-with-skill"),
            &current
        )
        .is_none());
    }

    #[test]
    fn response_session_fallback_ranks_longer_matching_candidate() {
        let current = json!([
            { "type": "message", "role": "user", "content": "a" },
            { "type": "message", "role": "assistant", "content": "b" },
            { "type": "message", "role": "user", "content": "c" },
            { "type": "message", "role": "user", "content": "fresh" }
        ]);
        let mut sessions = HashMap::new();
        sessions.insert(
            "short".to_string(),
            ResponseSessionState {
                response_id: "resp_short".to_string(),
                input: json!([{ "type": "message", "role": "user", "content": "a" }]),
                scope_key: Some("scope-a".to_string()),
                finished_at: Instant::now(),
            },
        );
        sessions.insert(
            "long".to_string(),
            ResponseSessionState {
                response_id: "resp_long".to_string(),
                input: json!([
                    { "type": "message", "role": "user", "content": "a" },
                    { "type": "message", "role": "assistant", "content": "b" },
                    { "type": "message", "role": "user", "content": "c" }
                ]),
                scope_key: Some("scope-a".to_string()),
                finished_at: Instant::now(),
            },
        );

        let ranked = fallback_response_sessions(&sessions, "missing", Some("scope-a"), &current);
        assert_eq!(ranked.first().unwrap().response_id, "resp_long");
    }

    #[test]
    fn body_diagnostics_reports_delta_send_body() {
        let original = json!({
            "model": "gpt-5.5",
            "input": [
                { "type": "message", "role": "user", "content": "stable context ".repeat(100) },
                { "type": "message", "role": "user", "content": "fresh" }
            ]
        });
        let delta = json!({
            "model": "gpt-5.5",
            "previous_response_id": "resp_abc",
            "input": [{ "type": "message", "role": "user", "content": "fresh" }]
        });
        let diagnostics = body_diagnostics(&Channel::Responses, &original, &delta, true);
        assert!(diagnostics.send_body_is_delta);
        assert!(diagnostics.send_body_bytes < diagnostics.original_body_bytes);
    }

    #[test]
    fn response_session_update_does_not_replace_longer_prefix_with_older_shorter_input() {
        let shorter = json!([
            { "type": "message", "role": "user", "content": "one" }
        ]);
        let model_output_only_longer = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "two" }
        ]);
        let longer = json!([
            { "type": "message", "role": "user", "content": "one" },
            { "type": "message", "role": "assistant", "content": "two" },
            { "type": "message", "role": "user", "content": "three" }
        ]);
        let different = json!([
            { "type": "message", "role": "user", "content": "changed" }
        ]);

        assert!(!should_replace_response_session(&longer, &shorter));
        assert!(!should_replace_response_session(
            &shorter,
            &model_output_only_longer
        ));
        assert!(should_replace_response_session(&shorter, &longer));
        assert!(should_replace_response_session(&longer, &different));
    }

    #[test]
    fn response_id_is_extracted_from_json_and_sse() {
        let json_bytes = serde_json::to_vec(&json!({
            "id": "resp_json",
            "usage": { "input_tokens": 10 }
        }))
        .unwrap();
        let sse = concat!(
            "event: response.created\n",
            "data: {\"response\":{\"id\":\"resp_stream\"}}\n\n",
            "event: response.completed\n",
            "data: {\"id\":\"resp_stream_done\",\"usage\":{\"input_tokens\":10}}\n\n"
        );

        assert_eq!(
            response_id_from_bytes(&json_bytes),
            Some("resp_json".to_string())
        );
        assert_eq!(
            response_id_from_bytes(sse.as_bytes()),
            Some("resp_stream_done".to_string())
        );
    }

    #[test]
    fn provider_cache_diagnostic_classifies_prefix_failures() {
        assert_eq!(
            provider_cache_diagnostic(&UsageRecord {
                input_tokens: 512,
                cache_read_tokens: 0,
                ..UsageRecord::default()
            }),
            "provider-prefix-ineligible-small"
        );
        assert_eq!(
            provider_cache_diagnostic(&UsageRecord {
                input_tokens: 200_000,
                cache_read_tokens: 0,
                ..UsageRecord::default()
            }),
            "provider-cold-start"
        );
        assert_eq!(
            provider_cache_diagnostic(&UsageRecord {
                input_tokens: 200_000,
                cache_read_tokens: 199_680,
                ..UsageRecord::default()
            }),
            "provider-warm-full"
        );
        assert_eq!(
            provider_cache_diagnostic(&UsageRecord {
                input_tokens: 200_000,
                cache_read_tokens: 150_000,
                ..UsageRecord::default()
            }),
            "provider-prefix-break"
        );
    }

    #[test]
    fn provider_prefix_fingerprint_ignores_responses_metadata_noise() {
        let mut left = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "stable-cache-key",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "same" }],
            "metadata": { "trace": "a" },
            "request_id": "req-a"
        });
        let mut right = json!({
            "model": "gpt-5.5",
            "prompt_cache_key": "stable-cache-key",
            "instructions": "stable system",
            "input": [{ "type": "message", "role": "user", "content": "same" }],
            "metadata": { "trace": "b" },
            "request_id": "req-b"
        });
        stabilize_responses_provider_prefix(&mut left);
        stabilize_responses_provider_prefix(&mut right);

        assert_eq!(
            provider_prefix_fingerprint(&left, &Channel::Responses),
            provider_prefix_fingerprint(&right, &Channel::Responses)
        );
    }

    #[test]
    fn provider_prefix_fingerprint_ignores_dynamic_tail_for_stable_waterline() {
        let stable_prefix = "a".repeat(80 * 1024);
        let first = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [
                { "type": "message", "role": "user", "content": stable_prefix },
                { "type": "message", "role": "user", "content": "tail-one" }
            ],
            "stream": true
        });
        let second = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "input": [
                { "type": "message", "role": "user", "content": "a".repeat(80 * 1024) },
                { "type": "message", "role": "user", "content": "tail-two" }
            ],
            "stream": true
        });

        assert_eq!(
            provider_prefix_fingerprint(&first, &Channel::Responses),
            provider_prefix_fingerprint(&second, &Channel::Responses)
        );
    }

    #[test]
    fn anthropic_prefix_optimizer_marks_tools_system_and_stable_messages() {
        let stable_context = "Stable project context. ".repeat(220);
        let mut request = json!({
            "system": "Stable system instructions",
            "tools": [
                { "name": "read_file", "description": "Read files" },
                { "name": "list_files", "description": "List files" }
            ],
            "messages": [
                { "role": "user", "content": stable_context },
                { "role": "assistant", "content": "Stable summary after reading files" },
                { "role": "user", "content": "Fresh current question" }
            ]
        });

        add_anthropic_cache_control(&mut request);

        assert_eq!(
            request
                .pointer("/tools/1/cache_control/type")
                .and_then(Value::as_str),
            Some("ephemeral")
        );
        assert_eq!(
            request
                .pointer("/system/0/cache_control/type")
                .and_then(Value::as_str),
            Some("ephemeral")
        );
        assert_eq!(
            request
                .pointer("/messages/1/content/0/cache_control/type")
                .and_then(Value::as_str),
            Some("ephemeral")
        );
        assert!(request
            .pointer("/messages/2/content/0/cache_control/type")
            .is_none());
    }

    #[test]
    fn anthropic_prefix_optimizer_adds_long_history_anchor_without_marking_fresh_tail() {
        let mut messages = (0..31)
            .map(|index| {
                json!({
                    "role": if index % 2 == 0 { "user" } else { "assistant" },
                    "content": format!("Stable history block {index}. {}", "repository context ".repeat(120))
                })
            })
            .collect::<Vec<_>>();
        messages.push(json!({
            "role": "user",
            "content": "Fresh current Anthropic question"
        }));
        let mut request = json!({
            "system": "Stable system instructions",
            "tools": [
                { "name": "read_file", "description": "Read files" },
                { "name": "list_files", "description": "List files" }
            ],
            "messages": messages
        });

        add_anthropic_cache_control(&mut request);

        assert_eq!(count_cache_control_fields(&request), 4);
        assert_eq!(
            request
                .pointer("/messages/30/content/0/cache_control/type")
                .and_then(Value::as_str),
            Some("ephemeral")
        );
        assert_eq!(
            request
                .pointer("/messages/10/content/0/cache_control/type")
                .and_then(Value::as_str),
            Some("ephemeral")
        );
        assert!(request
            .pointer("/messages/31/content/0/cache_control/type")
            .is_none());
    }

    #[test]
    fn anthropic_prefix_optimizer_skips_low_value_message_breakpoint() {
        let mut request = json!({
            "messages": [
                { "role": "user", "content": "tiny stable one" },
                { "role": "assistant", "content": "tiny stable two" },
                { "role": "user", "content": "Fresh current question" }
            ]
        });

        add_anthropic_cache_control(&mut request);

        assert_eq!(count_cache_control_fields(&request), 0);
    }

    #[test]
    fn anthropic_token_selector_prefers_latest_eligible_window() {
        let messages = (0..25)
            .map(|index| {
                let content = if index < 23 {
                    "small block".to_string()
                } else {
                    format!(
                        "Large stable block {index}. {}",
                        "workspace facts ".repeat(500)
                    )
                };
                json!({
                    "role": "user",
                    "content": content
                })
            })
            .chain(std::iter::once(json!({
                "role": "user",
                "content": "Fresh current question"
            })))
            .collect::<Vec<_>>();
        let selected = anthropic_message_cache_breakpoints(&messages, 2, 0);

        assert_eq!(selected, vec![24]);
    }

    #[test]
    fn anthropic_prefix_optimizer_respects_existing_breakpoint_limit() {
        let mut request = json!({
            "system": [{
                "type": "text",
                "text": "Already cached system",
                "cache_control": { "type": "ephemeral" }
            }],
            "tools": [
                {
                    "name": "read_file",
                    "description": "Read files",
                    "cache_control": { "type": "ephemeral" }
                }
            ],
            "messages": [
                {
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "Stable one",
                        "cache_control": { "type": "ephemeral" }
                    }]
                },
                {
                    "role": "assistant",
                    "content": [{
                        "type": "text",
                        "text": "Stable two",
                        "cache_control": { "type": "ephemeral" }
                    }]
                },
                { "role": "user", "content": "Fresh question" }
            ]
        });

        add_anthropic_cache_control(&mut request);

        assert_eq!(count_cache_control_fields(&request), 4);
        assert!(request
            .pointer("/messages/2/content/0/cache_control/type")
            .is_none());
    }

    #[test]
    fn responses_normalizer_stabilizes_tools_schema_and_arguments() {
        let mut left = json!({
            "prompt": "Hello",
            "max_tokens": 1024,
            "tools": [{
                "description": "Read file",
                "parameters": {
                    "required": ["encoding", "path"],
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "encoding": { "type": "string" }
                    }
                },
                "name": "read_file"
            }],
            "input": [{
                "type": "function_call",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\",\"encoding\":\"utf-8\"}"
            }]
        });
        let mut right = json!({
            "input": [{
                "name": "read_file",
                "arguments": "{\"encoding\":\"utf-8\",\"path\":\"README.md\"}",
                "type": "function_call"
            }],
            "tools": [{
                "name": "read_file",
                "type": "function",
                "parameters": {
                    "properties": {
                        "encoding": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["path", "encoding"],
                    "type": "object"
                },
                "description": "Read file"
            }],
            "max_output_tokens": 1024
        });

        normalize_responses_request(&mut left);
        normalize_responses_request(&mut right);

        assert_eq!(left["tools"], right["tools"]);
        assert_eq!(left["input"], right["input"]);
        assert_eq!(left["max_output_tokens"], right["max_output_tokens"]);
        assert_eq!(
            left.pointer("/tools/0/type").and_then(Value::as_str),
            Some("function")
        );
        assert_eq!(
            left.pointer("/input/0/arguments").and_then(Value::as_str),
            Some("{\"encoding\":\"utf-8\",\"path\":\"README.md\"}")
        );
    }

    #[test]
    fn responses_normalizer_unifies_second_stage_equivalent_requests() {
        let mut canonical = json!({
            "model": "gpt-route",
            "temperature": 0,
            "instructions": "Stable system instructions",
            "max_output_tokens": 1024,
            "tools": [{
                "type": "function",
                "name": "read_file",
                "description": "Read file",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "encoding": { "type": "string" }
                    },
                    "required": ["encoding", "path"]
                }
            }],
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "Summarize stable Responses cache scenario 7 for the selected workspace."
                }]
            }, {
                "type": "function_call",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\",\"encoding\":\"utf-8\"}"
            }]
        });
        let mut equivalent = json!({
            "model": "gpt-route",
            "temperature": 0,
            "messages": [
                { "role": "system", "content": "Stable system instructions" },
                {
                    "role": "user",
                    "content": "Summarize stable Responses cache scenario 7 for the selected workspace."
                },
                {
                    "type": "function_call",
                    "id": "fc_123",
                    "call_id": "call_123",
                    "name": "read_file",
                    "arguments": "{\"encoding\":\"utf-8\",\"path\":\"README.md\"}"
                }
            ],
            "max_completion_tokens": 1024,
            "tools": [{
                "type": "function",
                "function": {
                    "name": "read_file",
                    "description": "Read file",
                    "parameters": {
                        "properties": {
                            "encoding": { "type": "string" },
                            "path": { "type": "string" }
                        },
                        "required": ["path", "encoding"],
                        "type": "object"
                    }
                }
            }],
            "include": [],
            "reasoning": null
        });

        normalize_responses_request(&mut canonical);
        normalize_responses_request(&mut equivalent);

        assert_eq!(canonical["instructions"], equivalent["instructions"]);
        assert_eq!(
            canonical["max_output_tokens"],
            equivalent["max_output_tokens"]
        );
        assert_eq!(canonical["tools"], equivalent["tools"]);
        assert_eq!(canonical["input"][0], equivalent["input"][0]);
        assert_eq!(
            canonical.pointer("/input/1/arguments"),
            equivalent.pointer("/input/1/arguments")
        );
    }

    #[test]
    fn responses_normalizer_keeps_previous_response_id_distinct() {
        let mut left = json!({
            "previous_response_id": "resp_a",
            "input": "continue"
        });
        let mut right = json!({
            "previous_response_id": "resp_b",
            "input": "continue"
        });

        normalize_responses_request(&mut left);
        normalize_responses_request(&mut right);

        assert_ne!(left["previous_response_id"], right["previous_response_id"]);
    }

    #[test]
    fn responses_prefix_optimizer_stabilizes_mid_prompt_tool_noise() {
        let stable_prefix = "stable project context ".repeat(1400);
        let stable_suffix = " stable repository map".repeat(600);
        let mut left = json!({
            "model": "gpt-route",
            "temperature": 0,
            "request_id": "req-left",
            "metadata": {
                "trace_id": "trace-left",
                "timestamp": "2026-06-18T10:00:00Z"
            },
            "input": [{
                "type": "message",
                "role": "user",
                "id": "msg-left",
                "content": [{ "type": "input_text", "text": stable_prefix }]
            }, {
                "type": "function_call",
                "id": "fc-left",
                "call_id": "call-left-random",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\",\"encoding\":\"utf-8\"}"
            }, {
                "type": "function_call_output",
                "call_id": "call-left-random",
                "output_index": 2,
                "output": format!("file output{}", stable_suffix)
            }]
        });
        let mut right = json!({
            "model": "gpt-route",
            "temperature": 0,
            "request_id": "req-right",
            "metadata": {
                "trace_id": "trace-right",
                "timestamp": "2026-06-18T10:01:00Z"
            },
            "input": [{
                "type": "message",
                "role": "user",
                "id": "msg-right",
                "content": [{ "type": "input_text", "text": stable_prefix }]
            }, {
                "type": "function_call",
                "id": "fc-right",
                "call_id": "call-right-random",
                "name": "read_file",
                "arguments": "{\"encoding\":\"utf-8\",\"path\":\"README.md\"}"
            }, {
                "type": "function_call_output",
                "call_id": "call-right-random",
                "output_index": 9,
                "output": format!("file output{}", stable_suffix)
            }]
        });
        let mut changed = right.clone();
        changed["input"][1]["arguments"] =
            Value::String("{\"encoding\":\"utf-8\",\"path\":\"src/main.rs\"}".to_string());

        normalize_responses_request(&mut left);
        normalize_responses_request(&mut right);
        normalize_responses_request(&mut changed);
        stabilize_responses_provider_prefix(&mut left);
        stabilize_responses_provider_prefix(&mut right);
        stabilize_responses_provider_prefix(&mut changed);
        canonicalize_object_keys(&mut left, "$.responses_prefix");
        canonicalize_object_keys(&mut right, "$.responses_prefix");
        canonicalize_object_keys(&mut changed, "$.responses_prefix");

        let left_text = serde_json::to_string(&left).unwrap();
        let right_text = serde_json::to_string(&right).unwrap();
        assert_eq!(left_text, right_text);
        assert_ne!(
            serde_json::to_string(&left).unwrap(),
            serde_json::to_string(&changed).unwrap()
        );
        assert_eq!(
            left.pointer("/input/1/call_id").and_then(Value::as_str),
            right.pointer("/input/1/call_id").and_then(Value::as_str)
        );
        assert_eq!(
            left.pointer("/input/2/call_id").and_then(Value::as_str),
            right.pointer("/input/2/call_id").and_then(Value::as_str)
        );
    }

    #[test]
    fn responses_prefix_optimizer_stabilizes_self_contained_tools_with_previous_response_id() {
        let stable_prefix = "stable project context ".repeat(1400);
        let stable_suffix = " stable repository map".repeat(600);
        let mut left = json!({
            "model": "gpt-route",
            "previous_response_id": "resp_keep_distinct",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{ "type": "input_text", "text": stable_prefix }]
            }, {
                "type": "function_call",
                "id": "fc-left",
                "call_id": "call-left-random",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\",\"encoding\":\"utf-8\"}"
            }, {
                "type": "function_call_output",
                "call_id": "call-left-random",
                "output": format!("file output{}", stable_suffix)
            }]
        });
        let mut right = json!({
            "model": "gpt-route",
            "previous_response_id": "resp_keep_distinct",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{ "type": "input_text", "text": stable_prefix }]
            }, {
                "type": "function_call",
                "id": "fc-right",
                "call_id": "call-right-random",
                "name": "read_file",
                "arguments": "{\"encoding\":\"utf-8\",\"path\":\"README.md\"}"
            }, {
                "type": "function_call_output",
                "call_id": "call-right-random",
                "output": format!("file output{}", stable_suffix)
            }]
        });

        normalize_responses_request(&mut left);
        normalize_responses_request(&mut right);
        stabilize_responses_provider_prefix(&mut left);
        stabilize_responses_provider_prefix(&mut right);
        canonicalize_object_keys(&mut left, "$.responses_prefix");
        canonicalize_object_keys(&mut right, "$.responses_prefix");

        assert_eq!(left["previous_response_id"], "resp_keep_distinct");
        assert_eq!(left, right);
    }

    #[test]
    fn responses_prefix_optimizer_preserves_external_tool_output_call_id() {
        let mut request = json!({
            "model": "gpt-route",
            "previous_response_id": "resp_external",
            "input": [{
                "type": "function_call_output",
                "call_id": "call_from_previous_response",
                "output": "tool output for an upstream-managed prior call"
            }]
        });

        normalize_responses_request(&mut request);
        stabilize_responses_provider_prefix(&mut request);

        assert_eq!(
            request.pointer("/input/0/call_id").and_then(Value::as_str),
            Some("call_from_previous_response")
        );
    }

    #[test]
    fn active_provider_controls_route_for_any_client_channel() {
        let mut config = AppConfig::default();
        config.providers.push(ProviderConfig {
            id: "openai-like".to_string(),
            name: "OpenAI Compatible".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-route".to_string(),
                request_model_id: None,
                display_name: "gpt-route".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        config.active_provider_id = Some("openai-like".to_string());

        let decision = decide_route(
            &config,
            &json!({ "messages": [{ "role": "user", "content": "ping" }] }),
            &Channel::Anthropic,
            None,
        )
        .unwrap();

        assert_eq!(decision.provider.id, "openai-like");
        assert_eq!(decision.upstream_channel, Channel::Responses);
        assert_eq!(decision.model, "gpt-route");
    }

    #[test]
    fn configured_provider_wins_when_multiple_providers_share_model_id() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "yunshu".to_string(),
                name: "yunshu".to_string(),
                base_url: "https://yunshu.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("share".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.5", "input": "ping" }),
            &Channel::Responses,
            None,
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "gpt-5.5");
    }

    #[tokio::test]
    async fn route_affinity_reuses_previous_responses_provider_for_same_anchor() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace-route-affinity".to_string();
        config.providers = vec![
            ProviderConfig {
                id: "bizd".to_string(),
                name: "bizd".to_string(),
                base_url: "https://bizd.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "ls".to_string(),
                name: "ls".to_string(),
                base_url: "https://ls.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("bizd".to_string());
        let request = json!({
            "model": "gpt-5.5",
            "input": [
                { "type": "message", "role": "user", "content": "stable conversation anchor" },
                { "type": "message", "role": "assistant", "content": "dynamic tail" }
            ]
        });
        let appended = json!({
            "model": "gpt-5.5",
            "input": [
                { "type": "message", "role": "user", "content": "stable conversation anchor" },
                { "type": "message", "role": "assistant", "content": "dynamic tail" },
                { "type": "message", "role": "user", "content": "next turn" }
            ]
        });
        let dir = std::env::temp_dir().join(format!(
            "atoapi-route-affinity-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config.clone(), dir.join("config.toml"), cache).unwrap();
        let affinity_key = provider_route_affinity_key(&config, &request, &Channel::Responses)
            .expect("responses request should have route affinity key");
        assert_eq!(
            provider_route_affinity_key(&config, &appended, &Channel::Responses).as_deref(),
            Some(affinity_key.as_str())
        );
        note_provider_route_affinity(&state, Some(&affinity_key), "ls").await;

        let initial_decision = decide_route(&config, &request, &Channel::Responses, None).unwrap();
        assert_eq!(initial_decision.provider.id, "bizd");
        let preferred_provider =
            lookup_provider_route_affinity(&state, &config, Some(&affinity_key)).await;
        let affinity_decision = apply_provider_route_affinity(
            &config,
            initial_decision,
            &request,
            &Channel::Responses,
            preferred_provider.as_deref(),
        );

        assert_eq!(affinity_decision.provider.id, "ls");
        assert_eq!(affinity_decision.model, "gpt-5.5");
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn route_affinity_does_not_override_agent_bound_provider() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "bizd".to_string(),
                name: "bizd".to_string(),
                base_url: "https://bizd.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "ls".to_string(),
                name: "ls".to_string(),
                base_url: "https://ls.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("bizd".to_string());
        codex.model_id = Some("gpt-5.5".to_string());
        let request = json!({
            "model": "gpt-5.5",
            "input": [{ "type": "message", "role": "user", "content": "stable anchor" }]
        });

        let decision = decide_route(&config, &request, &Channel::Responses, Some("codex")).unwrap();
        assert!(route_is_agent_provider_bound(
            &config,
            &request,
            &Channel::Responses,
            Some("codex")
        ));
        let final_decision =
            if route_is_agent_provider_bound(&config, &request, &Channel::Responses, Some("codex"))
            {
                decision
            } else {
                apply_provider_route_affinity(
                    &config,
                    decision,
                    &request,
                    &Channel::Responses,
                    Some("ls"),
                )
            };

        assert_eq!(final_decision.provider.id, "bizd");
    }
    #[test]
    fn requested_model_provider_wins_when_active_provider_lacks_model() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "yunshu".to_string(),
                name: "yunshu".to_string(),
                base_url: "https://yunshu.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5-share".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5-share".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("yunshu".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.5-share", "input": "ping" }),
            &Channel::Responses,
            None,
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "gpt-5.5-share");
    }

    #[test]
    fn codex_model_list_includes_alias_without_dropping_canonical_id() {
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "nc/gpt-5.6-sol".to_string(),
                request_model_id: None,
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let normal = provider_model_list_items(&provider, false);
        let codex = provider_model_list_items(&provider, true);
        let ids = codex
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert_eq!(normal.len(), 1);
        assert!(ids.contains(&"nc/gpt-5.6-sol"));
        assert!(ids.contains(&"gpt-5.6-sol"));
        assert!(codex.iter().any(|item| {
            item.get("canonical_id").and_then(Value::as_str) == Some("nc/gpt-5.6-sol")
                && item.get("id").and_then(Value::as_str) == Some("gpt-5.6-sol")
        }));
    }

    #[test]
    fn codex_alias_model_routes_to_canonical_provider_model() {
        let mut config = AppConfig::default();
        config.providers = vec![ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "nc/gpt-5.6-sol".to_string(),
                request_model_id: None,
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("nc/gpt-5.6-sol".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.6-sol", "input": "ping" }),
            &Channel::Responses,
            Some("codex"),
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "nc/gpt-5.6-sol");
    }

    #[test]
    fn custom_request_model_mapping_is_listed_for_agents() {
        let provider = ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.6-sol".to_string(),
                request_model_id: Some("gpt-5.5".to_string()),
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let items = provider_model_list_items(&provider, true);
        assert!(items.iter().any(|item| {
            item.get("id").and_then(Value::as_str) == Some("gpt-5.5")
                && item.get("canonical_id").and_then(Value::as_str) == Some("gpt-5.6-sol")
        }));
    }

    #[test]
    fn custom_request_model_routes_to_upstream_model() {
        let mut config = AppConfig::default();
        config.providers = vec![ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.6-sol".to_string(),
                request_model_id: Some("gpt-5.5".to_string()),
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let agent = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        agent.enabled = true;
        agent.provider_id = Some("share".to_string());
        agent.model_id = Some("gpt-5.6-sol".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.5", "input": "ping" }),
            &Channel::Responses,
            Some("codex"),
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "gpt-5.6-sol");
    }

    #[test]
    fn agent_alias_model_routes_to_canonical_provider_model() {
        let mut config = AppConfig::default();
        config.providers = vec![ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "nc/gpt-5.6-sol".to_string(),
                request_model_id: None,
                display_name: "gpt-5.6-sol".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let agent = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "claude-code")
            .unwrap();
        agent.enabled = true;
        agent.provider_id = Some("share".to_string());
        agent.model_id = Some("nc/gpt-5.6-sol".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.6-sol", "input": "ping" }),
            &Channel::Responses,
            Some("claude-code"),
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "nc/gpt-5.6-sol");
    }

    #[test]
    fn authorized_agent_bound_provider_falls_back_from_unknown_client_model() {
        let mut config = AppConfig::default();
        config.providers = vec![ProviderConfig {
            id: "share".to_string(),
            name: "share".to_string(),
            base_url: "https://share.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: vec![crate::config::ModelConfig {
                id: "ui-fallback-model".to_string(),
                request_model_id: None,
                display_name: "ui-fallback-model".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("ui-fallback-model".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "client-requested-model", "input": "ping" }),
            &Channel::Responses,
            Some("codex"),
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "ui-fallback-model");
    }

    #[test]
    fn authorized_agent_route_wins_over_global_active_provider_and_requested_model() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "yunshu".to_string(),
                name: "yunshu".to_string(),
                base_url: "https://yunshu.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("yunshu".to_string());
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("gpt-5.5".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "orc", "input": "ping" }),
            &Channel::Responses,
            Some("codex"),
        )
        .unwrap();

        assert_eq!(decision.provider.id, "share");
        assert_eq!(decision.model, "gpt-5.5");
    }

    #[test]
    fn proxy_mode_scoped_key_routes_to_proxy_mode_agent() {
        let mut config = AppConfig::default();
        config.local_key = "ato-root-key".to_string();
        let proxy_mode = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "proxy-mode")
            .unwrap();
        proxy_mode.enabled = true;

        let key = agent_injection::agent_local_key(&config.local_key, "proxy-mode");
        let agent = agent_for_local_key(&config, &key).unwrap();

        assert_eq!(agent.id, "proxy-mode");
        assert_eq!(agent.kind, AgentInjectionKind::ProxyMode);
    }

    #[test]
    fn global_local_key_uses_active_provider_without_agent_inference() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "yunshu".to_string(),
                name: "yunshu".to_string(),
                base_url: "https://yunshu.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("yunshu".to_string());
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("gpt-5.5".to_string());

        let decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.5", "input": "ping" }),
            &Channel::Responses,
            None,
        )
        .unwrap();

        assert_eq!(decision.provider.id, "yunshu");
        assert_eq!(decision.model, "gpt-5.5");
    }

    #[test]
    fn global_local_key_routes_by_active_or_requested_model_without_agent_inference() {
        let mut config = AppConfig::default();
        config.providers = vec![
            ProviderConfig {
                id: "share".to_string(),
                name: "share".to_string(),
                base_url: "https://share.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "gpt-5.5".to_string(),
                    request_model_id: None,
                    display_name: "gpt-5.5".to_string(),
                    context_window: Some(128000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            ProviderConfig {
                id: "anthropic-upstream".to_string(),
                name: "anthropic-upstream".to_string(),
                base_url: "https://anthropic.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Anthropic,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "claude-opus-4.7".to_string(),
                    request_model_id: None,
                    display_name: "claude-opus-4.7".to_string(),
                    context_window: Some(200000),
                    output_window: None,
                    reasoning_effort_override_enabled: false,
                    reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        ];
        config.active_provider_id = Some("share".to_string());
        let claude_code = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "claude-code")
            .unwrap();
        claude_code.enabled = true;
        claude_code.provider_id = Some("anthropic-upstream".to_string());
        claude_code.model_id = Some("claude-opus-4.7".to_string());
        let codex = config
            .agent_injections
            .iter_mut()
            .find(|agent| agent.id == "codex")
            .unwrap();
        codex.enabled = true;
        codex.provider_id = Some("share".to_string());
        codex.model_id = Some("gpt-5.5".to_string());

        let anthropic_decision = decide_route(
            &config,
            &json!({ "model": "claude-opus-4.7", "messages": [{ "role": "user", "content": "ping" }] }),
            &Channel::Anthropic,
            None,
        )
        .unwrap();
        let responses_decision = decide_route(
            &config,
            &json!({ "model": "gpt-5.5", "input": "ping" }),
            &Channel::Responses,
            None,
        )
        .unwrap();

        assert_eq!(anthropic_decision.provider.id, "anthropic-upstream");
        assert_eq!(anthropic_decision.model, "claude-opus-4.7");
        assert_eq!(responses_decision.provider.id, "share");
        assert_eq!(responses_decision.model, "gpt-5.5");
    }

    #[test]
    fn responses_body_serialization_keeps_stable_prefix_before_dynamic_input() {
        let body = json!({
            "temperature": 0,
            "previous_response_id": "resp_dynamic",
            "stream": true,
            "store": true,
            "include": ["reasoning.encrypted_content"],
            "service_tier": "auto",
            "truncation": "auto",
            "input": [{ "type": "message", "role": "user", "content": "fresh" }],
            "tools": [{ "type": "function", "name": "read_file" }],
            "prompt_cache_key": "stable-cache-key",
            "model": "gpt-5.5",
            "instructions": "stable system",
            "metadata": { "trace": "dynamic" }
        });

        let serialized = serialize_responses_body_for_provider_prefix(&body);

        let model_at = serialized.find("\"model\"").unwrap();
        let cache_key_at = serialized.find("\"prompt_cache_key\"").unwrap();
        let instructions_at = serialized.find("\"instructions\"").unwrap();
        let tools_at = serialized.find("\"tools\"").unwrap();
        let temperature_at = serialized.find("\"temperature\"").unwrap();
        let include_at = serialized.find("\"include\"").unwrap();
        let stream_at = serialized.find("\"stream\"").unwrap();
        let store_at = serialized.find("\"store\"").unwrap();
        let service_tier_at = serialized.find("\"service_tier\"").unwrap();
        let truncation_at = serialized.find("\"truncation\"").unwrap();
        let input_at = serialized.find("\"input\"").unwrap();
        let previous_response_at = serialized.find("\"previous_response_id\"").unwrap();
        let metadata_at = serialized.find("\"metadata\"").unwrap();

        assert!(model_at < cache_key_at);
        assert!(cache_key_at < instructions_at);
        assert!(instructions_at < tools_at);
        assert!(tools_at < temperature_at);
        assert!(temperature_at < include_at);
        assert!(include_at < stream_at);
        assert!(stream_at < store_at);
        assert!(store_at < service_tier_at);
        assert!(service_tier_at < truncation_at);
        assert!(truncation_at < input_at);
        assert!(input_at < previous_response_at);
        assert!(previous_response_at < metadata_at);
        assert!(serde_json::from_str::<Value>(&serialized).is_ok());
    }

    #[test]
    fn stream_usage_extracts_provider_cached_tokens() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":98}}}\n\n",
            "data: {\"usage\":{\"prompt_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":19}}}\n\n",
            "data: [DONE]\n\n"
        );

        let usage = provider_usage_from_bytes(sse.as_bytes());

        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.cache_read_tokens, 117);
    }

    #[test]
    fn stream_metadata_collector_extracts_usage_and_response_id_incrementally() {
        let mut collector = SseStreamMetadataCollector::default();
        collector.process_chunk(b"event: response.completed\n");
        collector.process_chunk(
            br#"data: {"type":"response.completed","response":{"id":"resp_stream","model":"gpt-test","usage":{"input_tokens":20,"output_tokens":2,"input_tokens_details":{"cached_tokens":19}}}}"#,
        );
        collector.process_chunk(b"\n\n");
        collector.process_chunk(b"data: [DONE]\n\n");
        collector.finish();

        assert!(collector.completed_event_seen);
        assert!(collector.done_marker_seen);
        assert_eq!(collector.response_id.as_deref(), Some("resp_stream"));
        assert_eq!(collector.usage.input_tokens, 20);
        assert_eq!(collector.usage.cache_read_tokens, 19);
        assert_eq!(collector.usage.output_tokens, 2);
    }

    #[test]
    fn stream_metadata_collector_marks_sse_error_events() {
        let mut collector = SseStreamMetadataCollector::default();
        collector.process_chunk(
            br#"data: {"type":"response.failed","error":{"message":"provider failed"}}"#,
        );
        collector.finish();

        assert!(collector.error_event_seen);
        assert!(upstream_body_has_error(
            br#"{"type":"response.failed","error":{"message":"provider failed"}}"#,
            "application/json"
        ));
        assert!(upstream_body_has_error(
            b"event: error\ndata: {\"message\":\"bad\"}\n\n",
            "text/event-stream"
        ));
        assert!(!upstream_body_has_error(
            br#"{"type":"response.completed","usage":{"input_tokens":1}}"#,
            "application/json"
        ));
    }

    #[test]
    fn provider_cache_shortfall_uses_provider_bucket_gap() {
        let full_bucket = UsageRecord {
            input_tokens: 22_935,
            cache_read_tokens: 22_528,
            ..UsageRecord::default()
        };
        let missing_one_bucket = UsageRecord {
            input_tokens: 22_747,
            cache_read_tokens: 22_016,
            ..UsageRecord::default()
        };

        assert_eq!(provider_cache_shortfall(&full_bucket), 0);
        assert_eq!(provider_cache_shortfall(&missing_one_bucket), 512);
        assert_eq!(provider_cache_shortfall_128(&full_bucket), 384);
        assert_eq!(provider_cache_shortfall_128(&missing_one_bucket), 640);
    }

    #[test]
    fn provider_cache_shortfall_128_tracks_finer_provider_granularity() {
        let near_full = UsageRecord {
            input_tokens: 185_744,
            cache_read_tokens: 180_736,
            ..UsageRecord::default()
        };
        let tiny_gap = UsageRecord {
            input_tokens: 105_281,
            cache_read_tokens: 105_024,
            ..UsageRecord::default()
        };

        assert_eq!(provider_cache_shortfall(&near_full), 4_608);
        assert_eq!(provider_cache_shortfall_128(&near_full), 4_992);
        assert_eq!(provider_cache_shortfall(&tiny_gap), 0);
        assert_eq!(provider_cache_shortfall_128(&tiny_gap), 192);
    }

    #[test]
    fn response_session_delta_usage_counts_as_effective_full_bucket() {
        let raw_delta = UsageRecord {
            input_tokens: 13_396,
            cache_read_tokens: 5_632,
            ..UsageRecord::default()
        };

        let effective = provider_usage_effective_for_prefix_metrics(&raw_delta, true);

        assert_eq!(effective.input_tokens, 13_396);
        assert_eq!(effective.cache_read_tokens, 13_312);
        assert_eq!(provider_cache_shortfall(&effective), 0);
        assert_eq!(provider_cache_diagnostic(&effective), "provider-warm-full");
    }

    #[tokio::test]
    async fn provider_gap_breakdown_separates_new_tail_from_avoidable_gap() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-gap-breakdown-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            input_tokens: 153_235,
            cache_read_tokens: 153_088,
            ..UsageRecord::default()
        };
        let grown_tail = UsageRecord {
            input_tokens: 157_382,
            cache_read_tokens: 153_600,
            ..UsageRecord::default()
        };
        let regressed_prefix = UsageRecord {
            input_tokens: 157_382,
            cache_read_tokens: 152_576,
            ..UsageRecord::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;

        let grown = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&grown_tail),
            None,
        )
        .await
        .unwrap();
        assert_eq!(grown.total_tokens, 3_584);
        assert_eq!(grown.new_tail_tokens, 3_584);
        assert_eq!(grown.avoidable_tokens, 0);

        let regressed = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&regressed_prefix),
            None,
        )
        .await
        .unwrap();
        assert_eq!(regressed.total_tokens, 4_608);
        assert_eq!(regressed.new_tail_tokens, 4_096);
        assert_eq!(regressed.avoidable_tokens, 512);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_does_not_repeat_short_provider_waterline_rollback() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-short-waterline-rollback-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            input_tokens: 134_144,
            cache_read_tokens: 133_504,
            ..UsageRecord::default()
        };
        let transient_rollback = UsageRecord {
            input_tokens: 134_549,
            cache_read_tokens: 130_432,
            ..UsageRecord::default()
        };
        let recovered = UsageRecord {
            input_tokens: 134_804,
            cache_read_tokens: 133_504,
            ..UsageRecord::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        let guard = PrefixGuardWaitDiagnostics {
            wait_ms: 900,
            budget_exhausted: true,
            ..PrefixGuardWaitDiagnostics::default()
        };
        let rollback_gap = provider_cache_gap_breakdown_with_guard(
            &state,
            Some("main-prefix"),
            None,
            Some(&transient_rollback),
            None,
            Some(&guard),
        )
        .await
        .unwrap();

        assert_eq!(rollback_gap.total_tokens, 3_712);
        assert_eq!(rollback_gap.new_tail_tokens, 640);
        assert_eq!(rollback_gap.avoidable_tokens, 0);
        assert_eq!(rollback_gap.provider_unstable_tokens, 3_072);

        update_provider_prefix_state_with_tail_and_guard(
            &state,
            Some("main-prefix"),
            None,
            Some(&transient_rollback),
            &TailInputDiagnostics::default(),
            false,
            false,
            true,
        )
        .await;
        {
            let states = state.prefix_states.lock().await;
            let calibrated = states.get("main-prefix").unwrap();
            assert_eq!(calibrated.seen_bucket_tokens, 130_432);
            assert_eq!(calibrated.avoidable_shortfall_tokens, 0);
        }

        let recovered_gap =
            provider_cache_gap_breakdown(&state, Some("main-prefix"), None, Some(&recovered), None)
                .await
                .unwrap();
        assert_eq!(recovered_gap.avoidable_tokens, 0);
        assert_eq!(recovered_gap.provider_unstable_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn huge_new_anchor_cold_read_is_provider_unstable_and_not_learned() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-huge-new-anchor-cold-read-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let cold_read = UsageRecord {
            input_tokens: 249_750,
            cache_read_tokens: 3_840,
            ..UsageRecord::default()
        };
        let huge_tool_history = TailInputDiagnostics {
            input_items: 180,
            message_chars: 120_000,
            tool_output_chars: 516_851,
            largest_tool_output_chars: 180_000,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("new-session-prefix"),
            None,
            Some(&cold_read),
            Some(&huge_tool_history),
        )
        .await
        .unwrap();
        let expected_gap = provider_cache_shortfall(&cold_read);

        assert_eq!(gap.total_tokens, expected_gap);
        assert_eq!(gap.new_tail_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, expected_gap);

        update_provider_prefix_state_with_tail(
            &state,
            Some("new-session-prefix"),
            None,
            Some(&cold_read),
            &huge_tool_history,
            false,
            false,
        )
        .await;
        assert!(
            !state
                .prefix_states
                .lock()
                .await
                .contains_key("new-session-prefix"),
            "a huge low-hit new anchor must not become the learned waterline"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_keeps_tail_lag_as_diagnostic_only() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-tail-lag-gap-breakdown-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let first_tail_lag = UsageRecord {
            input_tokens: 197_382,
            cache_read_tokens: 194_048,
            ..UsageRecord::default()
        };
        let next_turn = UsageRecord {
            input_tokens: 198_564,
            cache_read_tokens: 194_560,
            ..UsageRecord::default()
        };
        let current_tail = TailInputDiagnostics {
            tool_output_chars: 138,
            largest_tool_output_chars: 138,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&first_tail_lag),
            false,
            false,
        )
        .await;

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&next_turn),
            Some(&current_tail),
        )
        .await
        .unwrap();

        assert_eq!(gap.total_tokens, 3_584);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, 3_584);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_does_not_inherit_large_waterline_after_shrink() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-shrink-gap-breakdown-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let large_context = UsageRecord {
            input_tokens: 267_930,
            cache_read_tokens: 262_144,
            ..UsageRecord::default()
        };
        let compact_context = UsageRecord {
            input_tokens: 20_897,
            cache_read_tokens: 10_240,
            ..UsageRecord::default()
        };

        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&large_context),
            false,
            false,
        )
        .await;

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&compact_context),
            Some(&TailInputDiagnostics::default()),
        )
        .await
        .unwrap();

        assert_eq!(gap.total_tokens, 10_240);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, 10_240);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn current_tool_tail_preserves_avoidable_accounting_and_prioritizes_it() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-tool-tail-false-avoidable-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            input_tokens: 32_200,
            cache_read_tokens: 31_744,
            ..UsageRecord::default()
        };
        let tool_tail_turn = UsageRecord {
            input_tokens: 32_733,
            cache_read_tokens: 29_184,
            ..UsageRecord::default()
        };
        let current_tail = TailInputDiagnostics {
            tool_output_chars: 9_592,
            largest_tool_output_chars: 5_857,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&tool_tail_turn),
            Some(&current_tail),
        )
        .await
        .unwrap();
        assert_eq!(gap.total_tokens, 3_072);
        assert_eq!(gap.avoidable_tokens, 2_560);
        assert_eq!(gap.new_tail_tokens, 512);

        let mut prefix = prefix_state(32_733, 29_184, 3_072);
        prefix.seen_bucket_tokens = 31_744;
        prefix.seen_bucket_tokens_128 = 31_744;
        prefix.avoidable_shortfall_tokens = 2_560;
        prefix.avoidable_shortfall_tokens_128 = 2_560;
        assert_ne!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &prefix,
                &TailInputDiagnostics::default(),
            ),
            Some("responses_current_tool_output_tail_cap".to_string())
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(&Channel::Responses, &prefix, &current_tail),
            Some("responses_avoidable_gap".to_string())
        );
        assert!(
            responses_provider_prefix_settle_delay_with_tail(&prefix, &current_tail)
                <= TokioDuration::from_secs(30)
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn response_session_delta_without_prefix_state_is_not_a_cold_restart_gap() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-delta-no-prefix-state-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let raw_delta = UsageRecord {
            provider: "newapi".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 13_396,
            output_tokens: 10,
            cache_read_tokens: 5_632,
            cache_creation_tokens: 0,
        };

        let is_delta =
            provider_usage_is_response_session_delta(&state, Some("main-prefix"), &raw_delta, true)
                .await;
        let effective = provider_usage_effective_for_prefix_metrics(&raw_delta, is_delta);

        assert!(is_delta);
        assert_eq!(effective.cache_read_tokens, 13_312);
        assert_eq!(provider_cache_shortfall(&effective), 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_provider_prefix_guard_serializes_same_cache_key() {
        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-lock-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                AppConfig::default(),
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = Vec::new();

        for _ in 0..4 {
            let state_for_task = state.clone();
            let active_for_task = active.clone();
            let max_for_task = max_active.clone();
            let start_for_task = start.clone();
            tasks.push(tokio::spawn(async move {
                start_for_task.wait().await;
                let _guard = acquire_provider_prefix_guard(
                    &state_for_task,
                    &Channel::Responses,
                    Some("same-provider-prefix-key"),
                    None,
                )
                .await
                .unwrap();
                let current = active_for_task.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let previous = max_for_task.load(Ordering::SeqCst);
                    if current <= previous
                        || max_for_task
                            .compare_exchange(previous, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                active_for_task.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stream_upstream_releases_prefix_guard_before_body_is_consumed() {
        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-stream-prefix-release-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                AppConfig::default(),
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );
        let prefix_key = Some("stream-prefix-key".to_string());
        let guard =
            acquire_provider_prefix_guard(&state, &Channel::Responses, prefix_key.as_deref(), None)
                .await;

        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new().route(
            "/stream",
            axum::routing::post(|| async {
                raw_response(
                    200,
                    "text/event-stream",
                    b"data: {\"type\":\"response.completed\",\"usage\":{\"input_tokens\":1,\"input_tokens_details\":{\"cached_tokens\":1}}}\n\n".to_vec(),
                )
            }),
        );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let upstream = state
            .upstream_client(false)
            .post(format!("http://{upstream_addr}/stream"))
            .send()
            .await
            .unwrap();
        let response = stream_upstream(
            state.clone(),
            upstream,
            "text/event-stream".to_string(),
            200,
            Instant::now(),
            "request".to_string(),
            Channel::Responses,
            RouteDecision {
                provider: ProviderConfig {
                    id: "provider".to_string(),
                    name: "provider".to_string(),
                    base_url: format!("http://{upstream_addr}/v1"),
                    models_url: None,
                    is_full_url: false,
                    custom_user_agent: None,
                    api_key_encrypted: Some("key".to_string()),
                    channel: Channel::Responses,
                    prompt_cache_retention_enabled: false,
                    request_body_gzip_enabled: false,
                    use_system_proxy: false,
                    models: Vec::new(),
                    enabled: true,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                model: "model".to_string(),
                upstream_channel: Channel::Responses,
            },
            true,
            vec!["cache-key".to_string()],
            "cache-key".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            AppConfig::default(),
            guard,
            prefix_key.clone(),
            None,
            None,
            None,
            None,
            false,
            false,
            json!({ "stream": true, "input": [] }),
            BodyDiagnostics::default(),
            TailInputDiagnostics::default(),
            SessionAnchorDiagnostics::default(),
            ResponseSessionReuseDiagnostics::default(),
            None,
            None,
            None,
            None,
            PrefixGuardWaitDiagnostics::default(),
            0,
            UpstreamRequestDiagnostics::default(),
            0,
        )
        .await;

        let reacquired = tokio::time::timeout(std::time::Duration::from_millis(200), async {
            acquire_provider_prefix_guard(&state, &Channel::Responses, prefix_key.as_deref(), None)
                .await
        })
        .await;
        assert!(
            reacquired.is_ok(),
            "stream proxy must not hold prefix guard until the SSE body is consumed"
        );
        drop(response);
        fs::remove_dir_all(config_dir).ok();
    }

    #[tokio::test]
    async fn chat_provider_prefix_guard_serializes_same_cache_key() {
        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-chat-prefix-lock-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                AppConfig::default(),
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = Vec::new();

        for _ in 0..4 {
            let state_for_task = state.clone();
            let active_for_task = active.clone();
            let max_for_task = max_active.clone();
            let start_for_task = start.clone();
            tasks.push(tokio::spawn(async move {
                start_for_task.wait().await;
                let _guard = acquire_provider_prefix_guard(
                    &state_for_task,
                    &Channel::Chat,
                    Some("same-chat-provider-prefix-key"),
                    None,
                )
                .await
                .unwrap();
                let current = active_for_task.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let previous = max_for_task.load(Ordering::SeqCst);
                    if current <= previous
                        || max_for_task
                            .compare_exchange(previous, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                active_for_task.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn provider_prefix_settle_delay_tracks_previous_shortfall() {
        let healthy = prefix_state(160_512, 160_000, 512);
        let partial = prefix_state(159_744, 157_184, 2_560);
        let severe = prefix_state(165_376, 160_256, 5_120);
        let first_cold = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 160_256,
            cache_read_tokens: 0,
            shortfall_tokens: 160_256,
            seen_bucket_tokens: 0,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 160_256,
            seen_bucket_tokens_128: 0,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };
        let cold_after_seen = prefix_state(160_256, 0, 160_256);

        assert_eq!(
            provider_prefix_settle_delay(&healthy),
            TokioDuration::from_millis(1500)
        );
        assert_eq!(
            provider_prefix_settle_delay(&partial),
            TokioDuration::from_secs(12)
        );
        assert_eq!(
            provider_prefix_settle_delay(&severe),
            TokioDuration::from_secs(18)
        );
        assert_eq!(
            provider_prefix_settle_delay(&first_cold),
            TokioDuration::ZERO
        );
        assert_eq!(
            provider_prefix_settle_delay(&cold_after_seen),
            TokioDuration::from_secs(22)
        );
    }

    #[test]
    fn provider_prefix_settle_delay_escalates_repeated_avoidable_512() {
        let mut first = prefix_state(160_512, 160_000, 512);
        first.avoidable_shortfall_streak = 1;
        assert_eq!(
            provider_prefix_settle_delay(&first),
            TokioDuration::from_millis(1500)
        );

        let mut repeated = first.clone();
        repeated.avoidable_shortfall_streak = 2;
        assert_eq!(
            provider_prefix_settle_delay(&repeated),
            TokioDuration::from_millis(1500)
        );

        let mut repeated_more = first.clone();
        repeated_more.avoidable_shortfall_streak = 3;
        assert_eq!(
            provider_prefix_settle_delay(&repeated_more),
            TokioDuration::from_millis(1800)
        );

        let settled = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 160_512,
            cache_read_tokens: 160_256,
            shortfall_tokens: 0,
            seen_bucket_tokens: 160_256,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 0,
            seen_bucket_tokens_128: 160_512,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };
        assert_eq!(
            provider_prefix_settle_delay(&settled),
            TokioDuration::from_millis(150)
        );
    }

    #[test]
    fn provider_prefix_settle_delay_scales_for_severe_avoidable_gap() {
        let mut medium = prefix_state(205_312, 168_448, 36_864);
        medium.avoidable_shortfall_tokens = 35_328;
        assert_eq!(
            provider_prefix_settle_delay(&medium),
            TokioDuration::from_secs(32)
        );

        let mut severe = prefix_state(205_312, 122_368, 82_944);
        severe.avoidable_shortfall_tokens = 81_920;
        assert_eq!(
            provider_prefix_settle_delay(&severe),
            TokioDuration::from_secs(45)
        );
    }

    #[test]
    fn responses_prefix_settle_delay_keeps_v019_for_small_gaps_but_guards_large_tail() {
        let mut medium = prefix_state(59_422, 55_808, 3_584);
        medium.avoidable_shortfall_tokens = 2_560;
        medium.avoidable_shortfall_tokens_128 = 2_560;
        assert_eq!(
            provider_prefix_settle_delay(&medium),
            TokioDuration::from_secs(12)
        );

        let mut large = prefix_state(58_605, 50_688, 7_680);
        large.avoidable_shortfall_tokens = 5_120;
        large.avoidable_shortfall_tokens_128 = 5_120;
        assert_eq!(
            provider_prefix_settle_delay(&large),
            TokioDuration::from_secs(18)
        );

        let large_tail = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 67_181,
            cache_read_tokens: 50_176,
            shortfall_tokens: 16_896,
            seen_bucket_tokens: 50_176,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 17_024,
            seen_bucket_tokens_128: 50_176,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };
        assert_eq!(
            provider_prefix_settle_delay(&large_tail),
            TokioDuration::from_millis(900)
        );
        assert_eq!(
            responses_provider_prefix_settle_delay(&large_tail),
            responses_foreground_wait_cap()
        );

        let mut small_bucket_tail = prefix_state(7_898, 7_168, 512);
        small_bucket_tail.small_gap_recovery_streak = 1;
        assert_eq!(
            responses_provider_prefix_settle_delay(&small_bucket_tail),
            responses_foreground_wait_cap()
        );

        let huge_tail = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 164_931,
            cache_read_tokens: 100_352,
            shortfall_tokens: 64_512,
            seen_bucket_tokens: 100_352,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 64_512,
            seen_bucket_tokens_128: 100_352,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };
        assert_eq!(
            responses_provider_prefix_settle_delay(&huge_tail),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_prefix_settle_delay_guards_after_large_tool_output_tail() {
        let mut large_tool_tail = prefix_state(177_194, 168_960, 8_192);
        large_tool_tail.avoidable_shortfall_tokens = 0;
        large_tool_tail.avoidable_shortfall_tokens_128 = 0;
        large_tool_tail.tail_tool_output_chars = 23_573;
        large_tool_tail.tail_largest_tool_output_chars = 11_535;

        assert_eq!(
            responses_provider_prefix_settle_delay(&large_tool_tail),
            responses_foreground_wait_cap()
        );

        let mut huge_tool_tail = prefix_state(169_815, 156_160, 13_312);
        huge_tool_tail.avoidable_shortfall_tokens = 0;
        huge_tool_tail.avoidable_shortfall_tokens_128 = 0;
        huge_tool_tail.tail_tool_output_chars = 45_342;
        huge_tool_tail.tail_largest_tool_output_chars = 19_892;

        assert_eq!(
            responses_provider_prefix_settle_delay(&huge_tool_tail),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_non_avoidable_tail_wait_is_capped_to_normal_ttft_range() {
        let mut cold_then_warm_tail = prefix_state(149_923, 148_992, 512);
        cold_then_warm_tail.avoidable_shortfall_tokens = 0;
        cold_then_warm_tail.avoidable_shortfall_tokens_128 = 0;
        cold_then_warm_tail.small_gap_recovery_streak = 1;
        cold_then_warm_tail.cache_instability_score = 3;
        cold_then_warm_tail.tail_tool_output_chars = 310_461;
        cold_then_warm_tail.tail_largest_tool_output_chars = 310_461;
        cold_then_warm_tail.tail_tool_output_noise_hint = Some("path_like".to_string());

        assert_eq!(
            responses_provider_prefix_settle_delay(&cold_then_warm_tail),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_prefix_settle_delay_guards_noisy_tool_output_from_1024_bucket() {
        let mut noisy_tool_tail = prefix_state(66_912, 65_536, 1_024);
        noisy_tool_tail.avoidable_shortfall_tokens = 0;
        noisy_tool_tail.avoidable_shortfall_tokens_128 = 0;
        noisy_tool_tail.shortfall_tokens = 0;
        noisy_tool_tail.shortfall_tokens_128 = 0;
        noisy_tool_tail.small_gap_recovery_streak = 0;
        noisy_tool_tail.tail_tool_output_chars = 1_024;
        noisy_tool_tail.tail_largest_tool_output_chars = 1_024;
        noisy_tool_tail.tail_tool_output_noise_hint = Some("timestamp_like,path_like".to_string());

        assert_eq!(
            responses_provider_prefix_settle_delay(&noisy_tool_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &noisy_tool_tail,
                &TailInputDiagnostics::default(),
            ),
            Some("responses_noisy_tool_output_tail_guard_1024".to_string())
        );
    }

    #[test]
    fn responses_repeated_small_bucket_tail_keeps_short_guard_without_new_tool_noise() {
        let mut repeated_small_tail = prefix_state(80_583, 79_872, 512);
        repeated_small_tail.avoidable_shortfall_tokens = 0;
        repeated_small_tail.avoidable_shortfall_tokens_128 = 0;
        repeated_small_tail.small_gap_recovery_streak = 2;
        repeated_small_tail.tail_tool_output_chars = 0;
        repeated_small_tail.tail_largest_tool_output_chars = 0;
        repeated_small_tail.tail_tool_output_noise_hint = None;

        assert_eq!(
            responses_minimum_new_tail_request_wait(
                &Channel::Responses,
                &repeated_small_tail,
                &TailInputDiagnostics::default(),
            ),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_medium_bucket_tail_followed_by_small_tool_tail_gets_short_carryover_guard() {
        let mut medium_tail = prefix_state(151_613, 145_408, 6_144);
        medium_tail.avoidable_shortfall_tokens = 0;
        medium_tail.avoidable_shortfall_tokens_128 = 0;
        medium_tail.tail_tool_output_chars = 4_096;
        medium_tail.tail_largest_tool_output_chars = 4_096;

        let current_small_tool_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 184,
            largest_tool_output_chars: 184,
            tool_output_noise_hint: Some("path_like".to_string()),
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_minimum_new_tail_request_wait(
                &Channel::Responses,
                &medium_tail,
                &current_small_tool_tail,
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &medium_tail,
                &current_small_tool_tail,
            ),
            Some("responses_medium_tool_tail_carryover_guard".to_string())
        );
    }

    #[test]
    fn responses_medium_bucket_tail_does_not_guard_pure_message_tail() {
        let mut medium_tail = prefix_state(151_613, 145_408, 6_144);
        medium_tail.avoidable_shortfall_tokens = 0;
        medium_tail.avoidable_shortfall_tokens_128 = 0;
        medium_tail.tail_tool_output_chars = 20_356;
        medium_tail.tail_largest_tool_output_chars = 20_356;

        let current_message_tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 184,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_minimum_new_tail_request_wait(
                &Channel::Responses,
                &medium_tail,
                &current_message_tail,
            ),
            TokioDuration::ZERO
        );
    }

    #[test]
    fn responses_current_tool_output_keeps_tail_guard_for_large_current_tools() {
        let mut repeated_small_tail = prefix_state(252_572, 246_784, 5_632);
        repeated_small_tail.avoidable_shortfall_tokens = 0;
        repeated_small_tail.avoidable_shortfall_tokens_128 = 0;
        repeated_small_tail.small_gap_recovery_streak = 1;

        let mut current_tool_tail = TailInputDiagnostics {
            input_items: 3,
            delta_from_session: true,
            tool_output_chars: 15_272,
            largest_tool_output_chars: 11_520,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_provider_prefix_settle_delay(&repeated_small_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(
                &repeated_small_tail,
                &current_tool_tail,
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &repeated_small_tail,
                &current_tool_tail,
            ),
            Some("responses_current_tool_output_tail_cap".to_string())
        );

        repeated_small_tail.shortfall_tokens = 1_536;
        repeated_small_tail.shortfall_tokens_128 = 1_536;
        assert_eq!(
            responses_provider_prefix_settle_delay(&repeated_small_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(
                &repeated_small_tail,
                &current_tool_tail,
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &repeated_small_tail,
                &current_tool_tail,
            ),
            Some("responses_current_tool_output_tail_cap".to_string())
        );

        repeated_small_tail.avoidable_shortfall_tokens = 1_536;
        repeated_small_tail.avoidable_shortfall_tokens_128 = 1_536;
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(
                &repeated_small_tail,
                &current_tool_tail,
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &repeated_small_tail,
                &current_tool_tail,
            ),
            Some("responses_avoidable_gap".to_string())
        );

        current_tool_tail.delta_from_session = false;
        repeated_small_tail.avoidable_shortfall_tokens = 0;
        repeated_small_tail.avoidable_shortfall_tokens_128 = 0;
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(
                &repeated_small_tail,
                &current_tool_tail,
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &repeated_small_tail,
                &current_tool_tail,
            ),
            Some("responses_current_tail_guard".to_string())
        );
    }

    #[test]
    fn responses_current_medium_tool_output_uses_light_exact_small_gap_guard() {
        let mut stable_small_gap = prefix_state(92_169, 90_624, 1_536);
        stable_small_gap.avoidable_shortfall_tokens = 1_536;
        stable_small_gap.avoidable_shortfall_tokens_128 = 1_536;
        stable_small_gap.avoidable_shortfall_streak = 1;

        let current_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 5_044,
            largest_tool_output_chars: 5_044,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(!responses_current_tail_makes_avoidable_unreliable(
            &current_tail
        ));
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&stable_small_gap, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &stable_small_gap,
                &current_tail,
            ),
            Some("responses_avoidable_gap".to_string())
        );
    }

    #[test]
    fn responses_current_large_tool_output_is_not_counted_as_reliable_avoidable_gap() {
        let mut stable_gap = prefix_state(47_981, 40_448, 7_168);
        stable_gap.avoidable_shortfall_tokens = 7_168;
        stable_gap.avoidable_shortfall_tokens_128 = 7_168;
        stable_gap.avoidable_shortfall_streak = 1;

        let current_tail = TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 23_734,
            largest_tool_output_chars: 11_224,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(responses_current_tail_makes_avoidable_unreliable(
            &current_tail
        ));
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&stable_gap, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &stable_gap,
                &current_tail
            ),
            Some("responses_avoidable_gap".to_string())
        );

        stable_gap.cache_instability_score = 3;
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&stable_gap, &current_tail),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_current_small_tool_output_uses_full_avoidable_wait_floor() {
        let mut stable_gap = prefix_state(38_724, 37_888, 512);
        stable_gap.avoidable_shortfall_tokens = 512;
        stable_gap.avoidable_shortfall_tokens_128 = 512;
        stable_gap.avoidable_shortfall_streak = 1;

        let current_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 1_469,
            largest_tool_output_chars: 1_469,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&stable_gap, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &stable_gap,
                &current_tail
            ),
            Some("responses_avoidable_gap".to_string())
        );
    }

    #[test]
    fn responses_avoidable_gap_uses_light_guard_when_99_percent_is_already_safe() {
        let mut tiny = prefix_state(160_024, 159_232, 512);
        tiny.avoidable_shortfall_tokens = 512;
        tiny.avoidable_shortfall_tokens_128 = 512;
        tiny.avoidable_shortfall_streak = 3;

        assert_eq!(
            responses_provider_prefix_settle_delay(&tiny),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_stable_995_prefix_without_avoidable_gap_has_zero_guard() {
        let mut stable = prefix_state(200_000, 199_168, 512);
        stable.avoidable_shortfall_tokens = 0;
        stable.avoidable_shortfall_tokens_128 = 0;
        stable.cache_instability_score = 0;
        stable.small_gap_recovery_streak = 0;
        stable.tail_tool_output_chars = 0;
        stable.tail_largest_tool_output_chars = 0;
        stable.tail_tool_output_noise_hint = None;
        let current_tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 64,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(
            provider_cache_ratio(&UsageRecord {
                input_tokens: stable.input_tokens,
                cache_read_tokens: stable.cache_read_tokens,
                ..UsageRecord::default()
            })
            .unwrap()
                >= 0.995
        );
        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&stable, &current_tail),
            TokioDuration::ZERO
        );
    }

    #[tokio::test]
    async fn responses_missing_exact_prefix_state_does_not_wait_on_related_prefix() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-missing-prefix-no-wait-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.prefix_states.lock().await.insert(
            "share\0gpt-5.5\0responses\0warm-related".to_string(),
            PrefixWarmState {
                finished_at: Instant::now(),
                input_tokens: 35_000,
                cache_read_tokens: 34_304,
                shortfall_tokens: 0,
                seen_bucket_tokens: 34_304,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: 0,
                seen_bucket_tokens_128: 34_304,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                cache_instability_score: 0,
                tail_tool_output_chars: 0,
                tail_largest_tool_output_chars: 0,
                tail_tool_output_noise_hint: None,
            },
        );

        let wait = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some("share\0gpt-5.5\0responses\0new-prefix"),
            None,
            &TailInputDiagnostics {
                message_chars: 12_000,
                source: Some("message".to_string()),
                ..TailInputDiagnostics::default()
            },
            None,
        )
        .await;

        assert_eq!(wait.wait_ms, 0);
        assert_eq!(wait.source, None);
        assert_eq!(wait.reason, None);
        assert_eq!(wait.skip_reason, Some("no_prefix_state".to_string()));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn responses_prefix_settle_delay_guards_cold_read_avoidable_gap() {
        let mut cold_read_regression = prefix_state(18_071, 0, 17_920);
        cold_read_regression.seen_bucket_tokens = 17_408;
        cold_read_regression.seen_bucket_tokens_128 = 17_408;
        cold_read_regression.avoidable_shortfall_tokens = 17_408;
        cold_read_regression.avoidable_shortfall_tokens_128 = 17_408;
        cold_read_regression.avoidable_shortfall_streak = 1;

        assert_eq!(
            responses_provider_prefix_settle_delay(&cold_read_regression),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &cold_read_regression,
                &TailInputDiagnostics::default(),
            ),
            Some("responses_avoidable_gap".to_string())
        );

        let mut huge_cold_read_regression = prefix_state(125_584, 0, 125_440);
        huge_cold_read_regression.seen_bucket_tokens = 125_440;
        huge_cold_read_regression.seen_bucket_tokens_128 = 125_440;
        huge_cold_read_regression.avoidable_shortfall_tokens = 125_440;
        huge_cold_read_regression.avoidable_shortfall_tokens_128 = 125_440;
        huge_cold_read_regression.avoidable_shortfall_streak = 2;
        assert_eq!(
            responses_provider_prefix_settle_delay(&huge_cold_read_regression),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_large_avoidable_cold_read_keeps_strong_guard() {
        let mut state = prefix_state(174_456, 0, 174_080);
        state.seen_bucket_tokens = 161_280;
        state.seen_bucket_tokens_128 = 161_280;
        state.avoidable_shortfall_tokens = 161_280;
        state.avoidable_shortfall_tokens_128 = 161_280;
        state.avoidable_shortfall_streak = 1;
        state.cache_instability_score = 3;
        state.tail_tool_output_chars = 3_297;

        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(
                &state,
                &TailInputDiagnostics {
                    input_items: 2,
                    message_chars: 512,
                    tool_output_chars: 3_297,
                    largest_tool_output_chars: 3_297,
                    source: Some("mixed".to_string()),
                    ..TailInputDiagnostics::default()
                }
            ),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &state,
                &TailInputDiagnostics::default(),
            ),
            Some("responses_avoidable_gap".to_string())
        );
    }

    #[test]
    fn responses_cold_unstable_recent_warm_guard_is_narrow() {
        let mut unstable_recent_warm = prefix_state(152_948, 152_576, 512);
        unstable_recent_warm.avoidable_shortfall_tokens = 0;
        unstable_recent_warm.avoidable_shortfall_tokens_128 = 0;
        unstable_recent_warm.cache_instability_score = 2;
        unstable_recent_warm.seen_bucket_tokens = 152_576;
        unstable_recent_warm.seen_bucket_tokens_128 = 152_576;

        let floor = responses_cold_unstable_recent_warm_floor(&unstable_recent_warm);
        assert!(floor <= responses_foreground_wait_cap());
        assert!(floor > TokioDuration::ZERO);
        assert_eq!(
            provider_prefix_wait_reason_for_channel(
                &Channel::Responses,
                &unstable_recent_warm,
                &TailInputDiagnostics::default(),
            ),
            Some("responses_cold_unstable_recent_warm_guard".to_string())
        );

        unstable_recent_warm.cache_read_tokens = 0;
        assert_eq!(
            responses_cold_unstable_recent_warm_floor(&unstable_recent_warm),
            TokioDuration::ZERO
        );
    }

    #[test]
    fn upstream_header_wait_class_separates_retry_body_and_slow_header() {
        assert_eq!(
            upstream_header_wait_class(&UpstreamRequestDiagnostics {
                attempts: 2,
                retry_wait_ms: 350,
                headers_ms: 30_000,
                request_body_bytes: 700_000,
                ..UpstreamRequestDiagnostics::default()
            }),
            "direct:retry_header_wait"
        );
        assert_eq!(
            upstream_header_wait_class(&UpstreamRequestDiagnostics {
                attempts: 1,
                retry_wait_ms: 0,
                headers_ms: 30_000,
                request_body_bytes: 1_200_000,
                ..UpstreamRequestDiagnostics::default()
            }),
            "direct:huge_body_header_wait"
        );
        assert_eq!(
            upstream_header_wait_class(&UpstreamRequestDiagnostics {
                attempts: 1,
                retry_wait_ms: 0,
                headers_ms: 30_000,
                request_body_bytes: 120_000,
                ..UpstreamRequestDiagnostics::default()
            }),
            "direct:header_wait_slow"
        );
        assert_eq!(
            upstream_header_wait_class(&UpstreamRequestDiagnostics {
                attempts: 1,
                retry_wait_ms: 0,
                headers_ms: 3_000,
                request_body_bytes: 120_000,
                ..UpstreamRequestDiagnostics::default()
            }),
            "direct:normal"
        );
        assert_eq!(
            upstream_header_wait_class(&UpstreamRequestDiagnostics {
                network_path: "system-proxy",
                attempts: 1,
                headers_ms: 8_000,
                request_body_bytes: 700_000,
                sent_body_bytes: 350_000,
                ..UpstreamRequestDiagnostics::default()
            }),
            "system-proxy:large_body_upload_header_wait"
        );
    }

    #[test]
    fn responses_prefix_settle_delay_guards_all_avoidable_gaps_with_short_cap() {
        let mut tiny = prefix_state(124_609, 123_904, 512);
        tiny.avoidable_shortfall_tokens = 512;
        tiny.avoidable_shortfall_tokens_128 = 512;
        tiny.avoidable_shortfall_streak = 1;
        assert_eq!(
            responses_provider_prefix_settle_delay(&tiny),
            responses_foreground_wait_cap()
        );

        let mut one_k = prefix_state(124_609, 123_392, 1_024);
        one_k.avoidable_shortfall_tokens = 1_024;
        one_k.avoidable_shortfall_tokens_128 = 1_024;
        one_k.avoidable_shortfall_streak = 1;
        assert_eq!(
            responses_provider_prefix_settle_delay(&one_k),
            responses_foreground_wait_cap()
        );

        let mut fifteen_thirty_six = prefix_state(123_816, 121_856, 1_536);
        fifteen_thirty_six.avoidable_shortfall_tokens = 1_536;
        fifteen_thirty_six.avoidable_shortfall_tokens_128 = 1_536;
        fifteen_thirty_six.avoidable_shortfall_streak = 1;
        assert_eq!(
            responses_provider_prefix_settle_delay(&fifteen_thirty_six),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_prefix_settle_delay_guards_avoidable_gap_sizes_without_unbounded_wait() {
        for (gap, expected) in [
            (128, responses_foreground_wait_cap()),
            (512, responses_foreground_wait_cap()),
            (1024, responses_foreground_wait_cap()),
            (1536, responses_foreground_wait_cap()),
            (2560, responses_foreground_wait_cap()),
            (4608, responses_foreground_wait_cap()),
            (9728, responses_foreground_wait_cap()),
            (20_992, responses_foreground_wait_cap()),
            (70_144, responses_foreground_wait_cap()),
            (92_160, responses_foreground_wait_cap()),
        ] {
            let mut state = prefix_state(180_000 + gap, 175_000, gap);
            state.avoidable_shortfall_tokens = gap;
            state.avoidable_shortfall_tokens_128 = gap;
            state.avoidable_shortfall_streak = 1;
            assert_eq!(
                responses_provider_prefix_settle_delay(&state),
                expected,
                "avoidable gap {gap} should be guarded"
            );
        }
    }

    #[test]
    fn responses_non_stream_main_skips_prefix_guard_for_compact_compatibility() {
        assert!(responses_sync_main_skips_prefix_guard(
            &Channel::Responses,
            &Channel::Responses,
            false
        ));
        assert!(!responses_sync_main_skips_prefix_guard(
            &Channel::Responses,
            &Channel::Responses,
            true
        ));
        assert!(!responses_sync_main_skips_prefix_guard(
            &Channel::Chat,
            &Channel::Responses,
            false
        ));
    }

    #[test]
    fn responses_non_stream_main_uses_upstream_sse_compat_without_changing_other_channels() {
        assert!(should_send_responses_non_stream_as_upstream_sse(
            &Channel::Responses,
            &Channel::Responses,
            false
        ));
        assert!(!should_send_responses_non_stream_as_upstream_sse(
            &Channel::Responses,
            &Channel::Responses,
            true
        ));
        assert!(!should_send_responses_non_stream_as_upstream_sse(
            &Channel::Chat,
            &Channel::Responses,
            false
        ));

        let mut responses_body = json!({ "stream": false, "input": [] });
        apply_responses_non_stream_upstream_sse_compat(&mut responses_body, true);
        assert_eq!(responses_body["stream"], true);

        let mut chat_body = json!({ "stream": false, "messages": [] });
        apply_responses_non_stream_upstream_sse_compat(&mut chat_body, false);
        assert_eq!(chat_body["stream"], false);
    }

    #[test]
    fn old_responses_compact_large_mixed_history_can_route_via_chat_compat() {
        let old_tail = TailInputDiagnostics {
            input_items: 823,
            message_chars: 109_203,
            tool_call_chars: 237_772,
            tool_output_chars: 666_225,
            largest_tool_output_chars: 46_766,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            false,
            &old_tail,
            1_229_947,
        ));
        assert!(should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            false,
            &TailInputDiagnostics {
                input_items: 2,
                message_chars: 2,
                tool_output_chars: 175,
                largest_tool_output_chars: 175,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
            484_250,
        ));
        assert!(!should_use_chat_non_stream_compact_fast_path(
            &TailInputDiagnostics {
                input_items: 198,
                message_chars: 30_177,
                tool_output_chars: 452_356,
                largest_tool_output_chars: 46_766,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
            48_495,
        ));
        let message_compact_tail = TailInputDiagnostics {
            input_items: 6,
            message_chars: 26_341,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            false,
            &message_compact_tail,
            33_407,
        ));
        assert!(should_route_responses_compact_via_chat_compat(
            &Channel::Responses,
            &message_compact_tail,
            33_407,
        ));
        assert!(!should_use_chat_non_stream_compact_fast_path(
            &message_compact_tail,
            33_407,
        ));
        let compact_message_tail_under_old_threshold = TailInputDiagnostics {
            input_items: 3,
            message_chars: 8_300,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            false,
            &compact_message_tail_under_old_threshold,
            12_800,
        ));

        let fresh_tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 2,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(!should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            false,
            &fresh_tail,
            621_604,
        ));
        assert!(!should_route_responses_non_stream_compact_via_chat(
            &Channel::Responses,
            &Channel::Responses,
            true,
            &old_tail,
            1_229_947,
        ));
    }

    #[test]
    fn chat_compat_body_uses_chat_shape_and_still_returns_to_responses_client() {
        let config = AppConfig::default();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "p".to_string(),
                name: "p".to_string(),
                base_url: "https://example.test/v1".to_string(),
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
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let responses_body = json!({
            "model": "gpt-5.5",
            "stream": false,
            "instructions": "be concise",
            "tools": [{ "type": "function", "function": { "name": "read_file" } }],
            "tool_choice": "auto",
            "parallel_tool_calls": true,
            "input": [{ "role": "user", "content": "compact" }]
        });
        let active = build_active_upstream_body_for_compat(
            &responses_body,
            &responses_body,
            &config,
            &decision,
            &Channel::Chat,
            true,
        );
        assert!(active.get("messages").is_some());
        assert!(active.get("input").is_none());
        assert_eq!(active["stream"], true);
        assert!(active.get("prompt_cache_key").is_some());
        assert!(active.get("tools").is_none());
        assert!(active.get("tool_choice").is_none());
        assert!(active.get("parallel_tool_calls").is_none());

        let chat_sse = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"gpt-5.5\",\"choices\":[{\"delta\":{\"content\":\"sum\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"gpt-5.5\",\"choices\":[{\"delta\":{\"content\":\"mary\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":8}}}\n\n",
            "data: [DONE]\n\n"
        );
        let chat_response = serde_json::from_slice::<Value>(&chat_sse_to_non_stream_json(
            chat_sse.as_bytes(),
            "gpt-5.5",
        ))
        .unwrap();
        assert_eq!(chat_response["choices"][0]["message"]["content"], "summary");
        let transformed = transform_response_value(&Channel::Responses, "gpt-5.5", &chat_response);
        assert_eq!(transformed["object"], "response");
        assert_eq!(transformed["output_text"], "summary");

        let plain_chat_response = json!({
            "id": "chatcmpl_1",
            "model": "gpt-5.5",
            "choices": [{ "message": { "role": "assistant", "content": "summary" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 2, "prompt_tokens_details": { "cached_tokens": 8 } }
        });
        let transformed =
            transform_response_value(&Channel::Responses, "gpt-5.5", &plain_chat_response);
        assert_eq!(transformed["object"], "response");
        assert_eq!(transformed["output_text"], "summary");
    }

    #[test]
    fn chat_compat_sse_errors_are_not_silently_converted_to_empty_success() {
        let error_sse = concat!(
            "event: error\n",
            "data: {\"error\":{\"message\":\"service validation failed\",\"type\":\"invalid_request_error\"}}\n\n"
        );
        let body = chat_sse_to_non_stream_json(error_sse.as_bytes(), "gpt-5.5");
        let value = serde_json::from_slice::<Value>(&body).unwrap();

        assert_eq!(value["error"]["message"], "service validation failed");
        assert!(json_body_has_error(&body));
    }

    #[test]
    fn chat_compat_sse_incomplete_stream_reports_error() {
        let incomplete_sse = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"model\":\"gpt-5.5\",\"choices\":[{\"delta\":{},\"finish_reason\":null}]}\n\n"
        );
        let body = chat_sse_to_non_stream_json(incomplete_sse.as_bytes(), "gpt-5.5");
        let value = serde_json::from_slice::<Value>(&body).unwrap();

        assert_eq!(value["error"]["type"], "atoapi_compact_stream_incomplete");
    }

    #[test]
    fn responses_prefix_settle_delay_uses_512_bucket_guard_for_new_tail() {
        for tail in [512, 1024, 1536, 2048, 2560, 7168, 9728] {
            let mut state = prefix_state(240_000 + tail, 235_000, tail);
            state.avoidable_shortfall_tokens = 0;
            state.avoidable_shortfall_tokens_128 = 0;
            state.shortfall_tokens = tail;
            state.shortfall_tokens_128 = tail;
            state.small_gap_recovery_streak = 1;
            assert_eq!(
                responses_provider_prefix_settle_delay(&state),
                responses_foreground_wait_cap(),
                "new tail bucket {tail} should be guarded"
            );
        }
    }

    #[test]
    fn responses_current_small_tool_output_gets_tail_guard_without_extra_request() {
        let mut state = prefix_state(127_499, 126_464, 1024);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.small_gap_recovery_streak = 0;
        let current_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 120,
            largest_tool_output_chars: 120,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&state, &current_tail),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn responses_small_tool_tail_keeps_guard_for_30k_regression() {
        let mut state = prefix_state(30_046, 29_184, 512);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 512;
        state.shortfall_tokens_128 = 512;
        state.small_gap_recovery_streak = 1;
        state.tail_tool_output_chars = 128;
        state.tail_largest_tool_output_chars = 128;
        state.tail_tool_output_noise_hint = Some("path_like".to_string());
        let current_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 128,
            largest_tool_output_chars: 128,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&state, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &current_tail),
            Some("responses_current_tail_guard".to_string())
        );
    }

    #[test]
    fn responses_mixed_message_tool_tail_gets_redundant_guard_for_cold_read_regression() {
        let mut state = prefix_state(32_580, 31_232, 1_024);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 1_024;
        state.shortfall_tokens_128 = 1_024;
        state.small_gap_recovery_streak = 1;
        let current_tail = TailInputDiagnostics {
            input_items: 2,
            message_chars: 3_474,
            tool_output_chars: 581,
            largest_tool_output_chars: 581,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_provider_prefix_settle_delay_with_tail(&state, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &current_tail),
            Some("responses_current_tail_guard".to_string())
        );
    }

    #[test]
    fn responses_prefix_settle_delay_does_not_over_guard_quiet_1024_tool_output() {
        let mut quiet_tool_tail = prefix_state(66_912, 65_536, 1_024);
        quiet_tool_tail.avoidable_shortfall_tokens = 0;
        quiet_tool_tail.avoidable_shortfall_tokens_128 = 0;
        quiet_tool_tail.shortfall_tokens = 0;
        quiet_tool_tail.shortfall_tokens_128 = 0;
        quiet_tool_tail.small_gap_recovery_streak = 0;
        quiet_tool_tail.tail_tool_output_chars = 1_024;
        quiet_tool_tail.tail_largest_tool_output_chars = 1_024;
        quiet_tool_tail.tail_tool_output_noise_hint = None;

        assert_eq!(
            responses_provider_prefix_settle_delay(&quiet_tool_tail),
            TokioDuration::from_millis(150)
        );
    }

    #[test]
    fn tool_output_noise_diagnostics_classifies_common_unstable_text() {
        let text = [
            "2026-06-23 21:25:16 building file C:\\workspace\\src\\main.rs",
            "2026-06-23 21:25:17 building file C:\\workspace\\src\\main.rs",
            "https://example.test/result?id=abc",
            "abcdef1234567890abcdef1234567890",
            "same repeated diagnostic line with enough length",
            "same repeated diagnostic line with enough length",
        ]
        .join("\n");

        let diagnostics = tool_output_noise_diagnostics(&text);
        assert!(diagnostics.lines >= 6);
        assert!(diagnostics.timestamp_like_count >= 2);
        assert!(diagnostics.path_like_count >= 2);
        assert!(diagnostics.url_like_count >= 1);
        assert!(diagnostics.hash_like_count >= 1);
        assert!(diagnostics.repeated_line_chars > 0);
        let hint = diagnostics.hint.unwrap();
        assert!(hint.contains("timestamp_like"));
        assert!(hint.contains("path_like"));
        assert!(hint.contains("url_like"));
        assert!(hint.contains("hash_like"));
        assert!(hint.contains("repeated_lines"));
    }

    #[test]
    fn responses_foreground_prewarm_only_for_repeated_high_hit_avoidable_gap() {
        let mut repeated = prefix_state(173_065, 169_472, 3_584);
        repeated.seen_bucket_tokens = 184_320;
        repeated.seen_bucket_tokens_128 = 184_320;
        repeated.avoidable_shortfall_tokens = 3_584;
        repeated.avoidable_shortfall_tokens_128 = 3_584;
        repeated.avoidable_shortfall_streak = 18;

        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&repeated),
            ForegroundPrewarmDecision::Run(3_584)
        );

        let mut low_hit = repeated.clone();
        low_hit.cache_read_tokens = 120_000;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&low_hit),
            ForegroundPrewarmDecision::Skip("ratio_low")
        );

        let mut first_gap = repeated.clone();
        first_gap.avoidable_shortfall_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&first_gap),
            ForegroundPrewarmDecision::Skip("streak_low")
        );

        let mut tiny_gap = prefix_state(96_640, 95_872, 512);
        tiny_gap.seen_bucket_tokens = 96_384;
        tiny_gap.seen_bucket_tokens_128 = 96_512;
        tiny_gap.avoidable_shortfall_tokens = 512;
        tiny_gap.avoidable_shortfall_tokens_128 = 512;
        tiny_gap.avoidable_shortfall_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&tiny_gap),
            ForegroundPrewarmDecision::Run(512)
        );

        let mut normal_first_gap = prefix_state(64_000, 62_976, 512);
        normal_first_gap.seen_bucket_tokens = 63_488;
        normal_first_gap.seen_bucket_tokens_128 = 63_488;
        normal_first_gap.avoidable_shortfall_tokens = 512;
        normal_first_gap.avoidable_shortfall_tokens_128 = 512;
        normal_first_gap.avoidable_shortfall_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&normal_first_gap),
            ForegroundPrewarmDecision::Run(512)
        );

        let mut one_bucket = prefix_state(62_115, 61_952, 128);
        one_bucket.seen_bucket_tokens = 62_080;
        one_bucket.seen_bucket_tokens_128 = 62_080;
        one_bucket.avoidable_shortfall_tokens = 128;
        one_bucket.avoidable_shortfall_tokens_128 = 128;
        one_bucket.avoidable_shortfall_streak = 4;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&one_bucket),
            ForegroundPrewarmDecision::Run(128)
        );

        let mut two_k = prefix_state(104_000, 101_888, 2048);
        two_k.seen_bucket_tokens = 103_936;
        two_k.seen_bucket_tokens_128 = 103_936;
        two_k.avoidable_shortfall_tokens = 2048;
        two_k.avoidable_shortfall_tokens_128 = 2048;
        two_k.avoidable_shortfall_streak = 2;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&two_k),
            ForegroundPrewarmDecision::Run(2048)
        );

        let mut small_context_recovery = prefix_state(15_943, 15_360, 512);
        small_context_recovery.small_gap_recovery_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&small_context_recovery),
            ForegroundPrewarmDecision::Run(512)
        );

        let mut observed_15k_full_high_waterline = prefix_state(15_943, 15_360, 0);
        observed_15k_full_high_waterline.finished_at =
            Instant::now() - std::time::Duration::from_secs(18);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&observed_15k_full_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut tiny_context_recovery = prefix_state(7_898, 7_168, 512);
        tiny_context_recovery.small_gap_recovery_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&tiny_context_recovery),
            ForegroundPrewarmDecision::Run(512)
        );

        let mut observed_40k_avoidable = prefix_state(40_701, 38_912, 1536);
        observed_40k_avoidable.seen_bucket_tokens = 40_448;
        observed_40k_avoidable.seen_bucket_tokens_128 = 40_448;
        observed_40k_avoidable.avoidable_shortfall_tokens = 1536;
        observed_40k_avoidable.avoidable_shortfall_tokens_128 = 1536;
        observed_40k_avoidable.avoidable_shortfall_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&observed_40k_avoidable),
            ForegroundPrewarmDecision::Run(1536)
        );

        let mut observed_44k_small_tail = prefix_state(44_813, 44_032, 512);
        observed_44k_small_tail.small_gap_recovery_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&observed_44k_small_tail),
            ForegroundPrewarmDecision::Run(512)
        );

        let predicted_9728_tail = prefix_state(65_127, 65_024, 0);
        assert_eq!(
            foreground_prewarm_responses_decision(&predicted_9728_tail, Some(65_024 + 9_728)),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        for tail in [512, 1024, 1536, 2048] {
            let mut stable_small_bucket_tail = prefix_state(150_148, 148_480, 0);
            stable_small_bucket_tail.seen_bucket_tokens = 148_480;
            stable_small_bucket_tail.seen_bucket_tokens_128 = 148_480;
            assert_eq!(
                foreground_prewarm_responses_decision(
                    &stable_small_bucket_tail,
                    Some(148_480 + tail)
                ),
                ForegroundPrewarmDecision::Skip("no_avoidable_gap"),
                "predicted {tail} new-tail must not create a companion non-streaming request"
            );
        }

        for tail in [128, 256, 384] {
            let mut high_hit_sub_bucket_tail = prefix_state(150_148, 148_480, 0);
            high_hit_sub_bucket_tail.seen_bucket_tokens = 148_480;
            high_hit_sub_bucket_tail.seen_bucket_tokens_128 = 148_480;
            assert_eq!(
                foreground_prewarm_responses_decision(
                    &high_hit_sub_bucket_tail,
                    Some(148_480 + tail)
                ),
                ForegroundPrewarmDecision::Skip("no_avoidable_gap"),
                "predicted {tail} sub-bucket tail must not create a companion non-streaming request"
            );
        }

        let mut low_hit_predicted_tail = predicted_9728_tail.clone();
        low_hit_predicted_tail.cache_read_tokens = 58_000;
        assert_eq!(
            foreground_prewarm_responses_decision(&low_hit_predicted_tail, Some(65_024 + 9_728)),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut low_hit_small_predicted_tail = prefix_state(150_148, 147_000, 0);
        low_hit_small_predicted_tail.cache_read_tokens = 147_000;
        low_hit_small_predicted_tail.seen_bucket_tokens = 147_000;
        low_hit_small_predicted_tail.seen_bucket_tokens_128 = 147_000;
        assert_eq!(
            foreground_prewarm_responses_decision(
                &low_hit_small_predicted_tail,
                Some(148_480 + 1536)
            ),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut low_hit_sub_bucket_tail = prefix_state(150_148, 147_000, 0);
        low_hit_sub_bucket_tail.cache_read_tokens = 147_000;
        low_hit_sub_bucket_tail.seen_bucket_tokens = 147_000;
        low_hit_sub_bucket_tail.seen_bucket_tokens_128 = 147_000;
        assert_eq!(
            foreground_prewarm_responses_decision(&low_hit_sub_bucket_tail, Some(147_000 + 384)),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut stale_full_high_waterline = prefix_state(93_845, 93_696, 0);
        stale_full_high_waterline.finished_at = Instant::now() - std::time::Duration::from_secs(44);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&stale_full_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let fresh_full_high_waterline = prefix_state(93_845, 93_696, 0);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&fresh_full_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut observed_52k_full_high_waterline = prefix_state(52_691, 52_224, 0);
        observed_52k_full_high_waterline.finished_at =
            Instant::now() - std::time::Duration::from_secs(24);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&observed_52k_full_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut too_early_high_waterline = prefix_state(93_845, 93_696, 0);
        too_early_high_waterline.finished_at = Instant::now() - std::time::Duration::from_secs(20);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&too_early_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut larger_high_waterline = prefix_state(150_016, 149_888, 0);
        larger_high_waterline.finished_at = Instant::now() - std::time::Duration::from_secs(25);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&larger_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );

        let mut cold_read_regression = prefix_state(84_666, 0, 79_360);
        cold_read_regression.seen_bucket_tokens = 81_920;
        cold_read_regression.seen_bucket_tokens_128 = 81_920;
        cold_read_regression.avoidable_shortfall_tokens = 76_800;
        cold_read_regression.avoidable_shortfall_tokens_128 = 76_800;
        cold_read_regression.avoidable_shortfall_streak = 1;
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&cold_read_regression),
            ForegroundPrewarmDecision::Run(76_800)
        );

        let mut stale_64k_high_waterline = prefix_state(74_119, 73_728, 0);
        stale_64k_high_waterline.finished_at = Instant::now() - std::time::Duration::from_secs(56);
        assert_eq!(
            foreground_prewarm_responses_avoidable_tokens(&stale_64k_high_waterline),
            ForegroundPrewarmDecision::Skip("no_avoidable_gap")
        );
    }

    #[test]
    fn responses_foreground_prewarm_does_not_keepalive_full_prefix_without_avoidable_gap() {
        for (input_tokens, cache_read_tokens) in [
            (15_943, 15_360),
            (52_691, 52_224),
            (93_845, 93_696),
            (150_016, 149_888),
            (264_431, 262_656),
        ] {
            let mut full_prefix = prefix_state(input_tokens, cache_read_tokens, 0);
            full_prefix.finished_at = Instant::now() - std::time::Duration::from_secs(120);

            assert_eq!(
                foreground_prewarm_responses_avoidable_tokens(&full_prefix),
                ForegroundPrewarmDecision::Skip("no_avoidable_gap"),
                "full/near-full prefix without an avoidable gap must not create a companion prewarm"
            );
        }
    }

    #[test]
    fn responses_missing_prefix_state_does_not_foreground_sync_full_body() {
        let request = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [
                {
                    "role": "user",
                    "content": "x".repeat(480_000)
                }
            ]
        });

        assert_eq!(
            foreground_prewarm_responses_missing_state_decision(&request),
            ForegroundPrewarmDecision::Skip("missing_prefix_state_cost_first"),
            "missing prefix state must not create a large foreground sync before the real stream"
        );

        let delta_request = json!({
            "model": "gpt-5.5",
            "previous_response_id": "resp_123",
            "input": [
                {
                    "role": "user",
                    "content": "next"
                }
            ]
        });

        assert_eq!(
            foreground_prewarm_responses_missing_state_decision(&delta_request),
            ForegroundPrewarmDecision::Skip("missing_prefix_state_cost_first"),
            "missing prefix state is not enough evidence for foreground prewarm even with a delta body"
        );
    }

    #[test]
    fn provider_prefix_settle_delay_uses_128_bucket_for_tiny_gaps() {
        let mut tiny = prefix_state(105_281, 105_024, 0);
        tiny.avoidable_shortfall_tokens = 0;
        tiny.avoidable_shortfall_tokens_128 = 128;
        tiny.avoidable_shortfall_streak = 1;
        assert_eq!(
            provider_prefix_settle_delay(&tiny),
            TokioDuration::from_millis(350)
        );

        let mut quarter = prefix_state(105_409, 105_024, 0);
        quarter.avoidable_shortfall_tokens = 0;
        quarter.avoidable_shortfall_tokens_128 = 256;
        quarter.avoidable_shortfall_streak = 1;
        assert_eq!(
            provider_prefix_settle_delay(&quarter),
            TokioDuration::from_millis(700)
        );
    }

    #[test]
    fn responses_measured_128_gap_keeps_bounded_guard() {
        let mut state = prefix_state(105_281, 105_024, 0);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 128;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        )
        .expect("a measured avoidable 128-token gap still needs a bounded guard");

        assert!(wait > TokioDuration::ZERO);
        assert!(wait <= responses_foreground_wait_cap());
    }

    #[test]
    fn responses_foreground_prewarm_settle_delay_scales_for_stable_new_tail() {
        let high_hit = UsageRecord {
            input_tokens: 98_325,
            cache_read_tokens: 96_768,
            ..UsageRecord::default()
        };
        assert_eq!(
            responses_foreground_prewarm_settle_delay(512, &high_hit),
            TokioDuration::from_millis(950)
        );
        assert_eq!(
            responses_foreground_prewarm_settle_delay(1024, &high_hit),
            TokioDuration::from_millis(1200)
        );
        assert_eq!(
            responses_foreground_prewarm_settle_delay(1536, &high_hit),
            TokioDuration::from_millis(1400)
        );
        assert_eq!(
            responses_foreground_prewarm_settle_delay(2048, &high_hit),
            TokioDuration::from_millis(1400)
        );
        assert_eq!(
            responses_foreground_prewarm_settle_delay(4096, &high_hit),
            TokioDuration::from_millis(1800)
        );

        let low_hit = UsageRecord {
            input_tokens: 98_325,
            cache_read_tokens: 70_000,
            ..UsageRecord::default()
        };
        assert_eq!(
            responses_foreground_prewarm_settle_delay(1536, &low_hit),
            TokioDuration::from_millis(500)
        );
    }

    #[test]
    fn prewarm_5xx_does_not_cooldown_prefix_but_429_still_does() {
        assert!(!should_cooldown_prefix_after_prewarm_status(500));
        assert!(!should_cooldown_prefix_after_prewarm_status(502));
        assert!(!should_cooldown_prefix_after_prewarm_status(503));
        assert!(should_cooldown_prefix_after_prewarm_status(429));

        assert!(!should_cooldown_prefix_after_status(502));
        assert!(should_cooldown_responses_sync_main_after_status(400));
        assert!(should_cooldown_responses_sync_main_after_status(429));
        assert!(should_cooldown_responses_sync_main_after_status(502));
        assert!(should_cooldown_responses_sync_main_after_status(503));
        assert!(!should_cooldown_responses_sync_main_after_status(401));
    }

    #[test]
    fn chat_prefix_settle_delay_is_more_conservative_for_new_tail_shortfall() {
        let state = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 119_446,
            cache_read_tokens: 114_688,
            shortfall_tokens: 2_560,
            seen_bucket_tokens: 114_688,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 2_688,
            seen_bucket_tokens_128: 114_688,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            cache_instability_score: 0,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
        };

        assert_eq!(
            provider_prefix_settle_delay(&state),
            TokioDuration::from_millis(900)
        );
        assert_eq!(
            chat_provider_prefix_settle_delay(&state),
            responses_foreground_wait_cap()
        );
    }

    #[test]
    fn chat_prefix_settle_delay_extends_after_severe_avoidable_gap() {
        let mut state = prefix_state(199_605, 122_368, 76_800);
        state.avoidable_shortfall_tokens = 70_144;
        state.avoidable_shortfall_tokens_128 = 70_144;

        assert_eq!(
            provider_prefix_settle_delay(&state),
            TokioDuration::from_secs(45)
        );
        assert_eq!(
            chat_provider_prefix_settle_delay(&state),
            responses_foreground_wait_cap()
        );
    }

    #[tokio::test]
    async fn provider_prefix_state_preserves_warm_waterline_on_same_fingerprint_cold_read() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-state-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 216_541,
            output_tokens: 10,
            cache_read_tokens: 216_064,
            cache_creation_tokens: 0,
        };
        let cold_jump = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 216_748,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&cold_jump), false, false)
            .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.input_tokens, warm.input_tokens);
        assert_eq!(kept.cache_read_tokens, warm.cache_read_tokens);
        assert_eq!(kept.shortfall_tokens, 216_576);
        assert_eq!(kept.seen_bucket_tokens, 216_064);
        assert_eq!(kept.avoidable_shortfall_tokens, 0);
        assert_eq!(kept.avoidable_shortfall_streak, 0);
        assert!(kept.cache_instability_score > 0);
        assert!(provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            kept,
            &TailInputDiagnostics::default(),
        )
        .is_some());

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn responses_stale_avoidable_prefix_keeps_bounded_recovery_guard() {
        let mut state = prefix_state(100_313, 0, 99_840);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(43);
        state.seen_bucket_tokens = 99_840;
        state.seen_bucket_tokens_128 = 99_840;
        state.avoidable_shortfall_tokens = 99_328;
        state.avoidable_shortfall_tokens_128 = 99_328;
        state.avoidable_shortfall_streak = 2;
        state.cache_instability_score = 3;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 3,
                tool_output_chars: 2_179,
                largest_tool_output_chars: 1_992,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_small_avoidable_prefix_gets_light_recovery_guard() {
        let mut state = prefix_state(78_608, 77_824, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(18);
        state.seen_bucket_tokens = 78_336;
        state.seen_bucket_tokens_128 = 78_336;
        state.avoidable_shortfall_tokens = 512;
        state.avoidable_shortfall_tokens_128 = 512;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_small_avoidable_prefix_gets_risk_guard_after_idle() {
        let mut state = prefix_state(88_626, 88_064, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(13 * 60);
        state.seen_bucket_tokens = 88_576;
        state.seen_bucket_tokens_128 = 88_576;
        state.avoidable_shortfall_tokens = 512;
        state.avoidable_shortfall_tokens_128 = 512;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_small_avoidable_prefix_gets_risk_guard_for_tail_change() {
        let mut state = prefix_state(89_553, 88_064, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(90);
        state.seen_bucket_tokens = 88_576;
        state.seen_bucket_tokens_128 = 88_576;
        state.avoidable_shortfall_tokens = 512;
        state.avoidable_shortfall_tokens_128 = 512;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 2,
                message_chars: 421,
                tool_output_chars: 581,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_medium_avoidable_prefix_gets_light_recovery_guard() {
        let mut state = prefix_state(90_581, 88_064, 2_048);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(54);
        state.seen_bucket_tokens = 90_112;
        state.seen_bucket_tokens_128 = 90_112;
        state.avoidable_shortfall_tokens = 2_048;
        state.avoidable_shortfall_tokens_128 = 2_048;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 2,
                tool_output_chars: 7_070,
                largest_tool_output_chars: 3_557,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_large_tool_tail_gets_short_catchup_without_avoidable_gap() {
        let mut state = prefix_state(48_343, 31_232, 2_176);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(45);
        state.seen_bucket_tokens = 31_232;
        state.seen_bucket_tokens_128 = 31_232;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 4,
                tool_output_chars: 60_948,
                largest_tool_output_chars: 45_214,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_all_stage_message_tail_gets_short_guard() {
        let mut state = prefix_state(25_057, 8_704, 15_872);
        state.seen_bucket_tokens = 8_704;
        state.seen_bucket_tokens_128 = 8_704;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.avoidable_shortfall_streak = 0;
        state.small_gap_recovery_streak = 0;
        let current_tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 66_218,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_minimum_new_tail_request_wait(&Channel::Responses, &state, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &current_tail),
            Some("responses_all_stage_tail_guard".to_string())
        );
    }

    #[test]
    fn responses_large_tool_tail_learns_sent_bucket_without_extra_request() {
        let mut previous = prefix_state(32_080, 28_160, 3_584);
        previous.seen_bucket_tokens = 28_160;
        previous.seen_bucket_tokens_128 = 28_160;
        previous.avoidable_shortfall_tokens = 0;
        previous.avoidable_shortfall_tokens_128 = 0;
        let record = UsageRecord {
            input_tokens: 41_083,
            cache_read_tokens: 31_744,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            tool_output_chars: 60_400,
            largest_tool_output_chars: 40_592,
            tool_output_lines: 830,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_small_residual_after_tool_burst_learns_sent_bucket() {
        let mut previous = prefix_state(77_746, 77_312, 0);
        previous.seen_bucket_tokens = 77_312;
        previous.seen_bucket_tokens_128 = 77_312;
        previous.tail_tool_output_chars = 28_869;
        previous.tail_largest_tool_output_chars = 16_006;
        previous.avoidable_shortfall_tokens = 0;
        previous.avoidable_shortfall_tokens_128 = 0;
        let record = UsageRecord {
            input_tokens: 79_821,
            cache_read_tokens: 77_312,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 6,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(provider_cache_shortfall(&record), 2_048);
        assert!(should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_current_large_tool_tail_is_classified_as_real_tail() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-tool-tail-real-classification-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let warm = UsageRecord {
                input_tokens: 85_732,
                cache_read_tokens: 84_992,
                ..UsageRecord::default()
            };
            let burst = UsageRecord {
                input_tokens: 97_266,
                cache_read_tokens: 85_504,
                ..UsageRecord::default()
            };
            let tail = TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 60_164,
                largest_tool_output_chars: 22_602,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            };
            update_provider_prefix_state_with_tail(
                &state,
                Some("main-prefix"),
                None,
                Some(&warm),
                &TailInputDiagnostics::default(),
                false,
                false,
            )
            .await;
            let gap = provider_cache_gap_breakdown(
                &state,
                Some("main-prefix"),
                None,
                Some(&burst),
                Some(&tail),
            )
            .await;
            let wait = PrefixGuardWaitDiagnostics {
                wait_ms: 3_000,
                reason: Some("responses_current_tool_output_tail_cap".to_string()),
                source: Some("exact".to_string()),
                skip_reason: None,
                ..PrefixGuardWaitDiagnostics::default()
            };
            let diagnostics = prefix_lag_diagnostics(
                &state,
                Some("main-prefix"),
                Some(&burst),
                gap.as_ref(),
                &wait,
                &tail,
            )
            .await;

            assert_eq!(provider_cache_shortfall(&burst), 11_264);
            assert_eq!(
                diagnostics.classification.as_deref(),
                Some("tool_tail_burst_real_tail")
            );
        });
        fs::remove_dir_all(dir).ok();
    }
    #[test]
    fn responses_stale_current_message_tail_gets_guard_after_window_elapsed() {
        let mut state = prefix_state(52_513, 49_152, 3_072);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(24 * 60);
        state.seen_bucket_tokens = 49_152;
        state.seen_bucket_tokens_128 = 49_152;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 128;
        state.shortfall_tokens_128 = 128;
        let tail = TailInputDiagnostics {
            input_items: 2,
            message_chars: 3_742,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            provider_prefix_wait_duration_for_channel(&Channel::Responses, &state, &tail),
            Some(responses_foreground_wait_cap())
        );
    }

    #[test]
    fn responses_medium_message_tail_learns_sent_bucket_with_high_ratio() {
        let mut previous = prefix_state(53_806, 52_736, 1_024);
        previous.seen_bucket_tokens = 52_736;
        previous.seen_bucket_tokens_128 = 52_736;
        previous.avoidable_shortfall_tokens = 0;
        previous.avoidable_shortfall_tokens_128 = 0;
        let record = UsageRecord {
            input_tokens: 57_637,
            cache_read_tokens: 54_784,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 2,
            message_chars: 4_330,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_sync_main_520_uses_compat_fallbacks() {
        assert!(should_fallback_responses_sync_main_to_chat_compat(
            520, true, false
        ));
        assert!(should_fallback_chat_compat_compact_to_responses(
            520, true, true
        ));
    }

    #[test]
    fn responses_shrunk_large_message_tail_does_not_overlearn_sent_bucket() {
        let mut previous = prefix_state(88_054, 87_808, 0);
        previous.seen_bucket_tokens = 87_808;
        previous.seen_bucket_tokens_128 = 87_808;
        let record = UsageRecord {
            input_tokens: 25_057,
            cache_read_tokens: 8_704,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 66_218,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(!should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_unseen_huge_tail_size_is_covered_by_general_guard() {
        let mut state = prefix_state(140_000, 92_160, 47_616);
        state.seen_bucket_tokens = 92_160;
        state.seen_bucket_tokens_128 = 92_160;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        let current_tail = TailInputDiagnostics {
            input_items: 6,
            message_chars: 180_000,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert_eq!(
            responses_minimum_new_tail_request_wait(&Channel::Responses, &state, &current_tail),
            responses_foreground_wait_cap()
        );
        assert_eq!(
            provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &current_tail),
            Some("responses_all_stage_tail_guard".to_string())
        );
    }

    #[test]
    fn responses_unseen_huge_tail_can_learn_sent_bucket_with_high_hit_ratio() {
        let mut previous = prefix_state(280_000, 260_096, 19_456);
        previous.seen_bucket_tokens = 260_096;
        previous.seen_bucket_tokens_128 = 260_096;
        previous.avoidable_shortfall_tokens = 0;
        previous.avoidable_shortfall_tokens_128 = 0;
        let record = UsageRecord {
            input_tokens: 300_000,
            cache_read_tokens: 260_096,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 6,
            message_chars: 180_000,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_extreme_tail_can_learn_sent_bucket_with_strong_hit_ratio() {
        let mut previous = prefix_state(1_900_000, 1_820_032, 79_872);
        previous.seen_bucket_tokens = 1_820_032;
        previous.seen_bucket_tokens_128 = 1_820_032;
        previous.avoidable_shortfall_tokens = 0;
        previous.avoidable_shortfall_tokens_128 = 0;
        let record = UsageRecord {
            input_tokens: 2_000_000,
            cache_read_tokens: 1_820_032,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 12,
            message_chars: 700_000,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(provider_cache_shortfall(&record) > 131_072);
        assert!(should_learn_sent_provider_bucket(
            Some(&previous),
            &record,
            provider_cache_shortfall(&record),
            provider_cache_shortfall_128(&record),
            &tail,
            false,
        ));
    }

    #[test]
    fn responses_long_idle_warm_prefix_gets_cold_read_guard() {
        let mut state = prefix_state(126_139, 120_832, 3_584);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(44 * 60);
        state.seen_bucket_tokens = 124_416;
        state.seen_bucket_tokens_128 = 124_416;
        state.cache_instability_score = 3;
        let tail = TailInputDiagnostics {
            input_items: 2,
            message_chars: 1_682,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        let wait = provider_prefix_wait_duration_for_channel(&Channel::Responses, &state, &tail);
        let reason = provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &tail);

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
        assert_eq!(
            reason.as_deref(),
            Some("responses_long_idle_warm_prefix_guard")
        );
    }

    #[test]
    fn responses_long_idle_warm_prefix_without_tail_does_not_wait() {
        let mut state = prefix_state(126_139, 120_832, 3_584);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(44 * 60);
        state.seen_bucket_tokens = 124_416;
        state.seen_bucket_tokens_128 = 124_416;
        state.cache_instability_score = 3;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        );

        assert_eq!(wait, None);
    }

    #[test]
    fn responses_stale_small_tool_tail_does_not_get_large_tail_catchup() {
        let mut state = prefix_state(140_301, 139_264, 768);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(45);
        state.seen_bucket_tokens = 139_264;
        state.seen_bucket_tokens_128 = 139_264;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 160,
                largest_tool_output_chars: 160,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, None);
    }

    #[test]
    fn responses_recent_small_tool_tail_lag_gets_short_guard() {
        let mut state = prefix_state(156_915, 156_160, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(3);
        state.seen_bucket_tokens = 156_160;
        state.seen_bucket_tokens_128 = 156_160;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.cache_instability_score = 1;
        state.tail_tool_output_chars = 165;
        state.tail_largest_tool_output_chars = 165;
        state.tail_tool_output_noise_hint = Some("path_like".to_string());

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 165,
                largest_tool_output_chars: 165,
                tool_output_noise_hint: Some("path_like".to_string()),
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_small_bucket_tail_lag_still_gets_guard_after_window_elapsed() {
        let mut state = prefix_state(35_253, 34_304, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(45);
        state.seen_bucket_tokens = 34_304;
        state.seen_bucket_tokens_128 = 34_304;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.cache_instability_score = 0;
        state.small_gap_recovery_streak = 0;
        state.tail_tool_output_chars = 1008;
        state.tail_largest_tool_output_chars = 1008;
        state.tail_tool_output_noise_hint = Some("path_like".to_string());

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 1008,
                largest_tool_output_chars: 1008,
                tool_output_noise_hint: Some("path_like".to_string()),
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_early_anchor_large_tool_tail_gets_short_catchup() {
        let mut state = prefix_state(13_000, 7_680, 5_248);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(18);
        state.seen_bucket_tokens = 7_680;
        state.seen_bucket_tokens_128 = 7_680;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 6,
                tool_output_chars: 44_311,
                largest_tool_output_chars: 11_652,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_early_cold_anchor_large_tool_tail_gets_short_guard() {
        let mut state = prefix_state(7_906, 0, 7_680);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(32);
        state.seen_bucket_tokens = 0;
        state.seen_bucket_tokens_128 = 0;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 6,
                tool_output_chars: 18_498,
                largest_tool_output_chars: 10_712,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_small_context_medium_avoidable_prefix_gets_full_short_guard() {
        let mut state = prefix_state(12_408, 7_680, 2_048);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(38);
        state.seen_bucket_tokens = 12_288;
        state.seen_bucket_tokens_128 = 12_288;
        state.avoidable_shortfall_tokens = 2_048;
        state.avoidable_shortfall_tokens_128 = 2_048;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 10,
                message_chars: 13_072,
                tool_output_chars: 15_395,
                largest_tool_output_chars: 15_297,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_small_context_1024_avoidable_prefix_gets_short_guard() {
        let mut state = prefix_state(12_408, 7_680, 1_024);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(38);
        state.seen_bucket_tokens = 12_288;
        state.seen_bucket_tokens_128 = 12_288;
        state.avoidable_shortfall_tokens = 1_024;
        state.avoidable_shortfall_tokens_128 = 1_024;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_high_context_avoidable_prefix_gets_light_per_request_floor() {
        let mut state = prefix_state(119_002, 115_200, 2_560);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(4);
        state.seen_bucket_tokens = 118_784;
        state.seen_bucket_tokens_128 = 118_784;
        state.avoidable_shortfall_tokens = 2_560;
        state.avoidable_shortfall_tokens_128 = 2_560;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 36,
                message_chars: 1_005,
                tool_call_chars: 8_479,
                tool_output_chars: 2_511,
                largest_tool_output_chars: 1_069,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_high_context_512_avoidable_prefix_gets_light_guard() {
        let mut state = prefix_state(118_113, 117_248, 512);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(4);
        state.seen_bucket_tokens = 117_760;
        state.seen_bucket_tokens_128 = 117_760;
        state.avoidable_shortfall_tokens = 512;
        state.avoidable_shortfall_tokens_128 = 512;
        state.avoidable_shortfall_streak = 1;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 128,
                largest_tool_output_chars: 128,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_avoidable_large_tool_tail_gets_full_short_guard() {
        let mut state = prefix_state(116_443, 100_352, 15_872);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(180);
        state.seen_bucket_tokens = 111_616;
        state.seen_bucket_tokens_128 = 111_616;
        state.avoidable_shortfall_tokens = 11_264;
        state.avoidable_shortfall_tokens_128 = 11_264;
        state.avoidable_shortfall_streak = 1;
        state.cache_instability_score = 2;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 315,
                message_chars: 28_971,
                tool_call_chars: 36_837,
                tool_output_chars: 295_472,
                largest_tool_output_chars: 33_924,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_small_context_stale_cold_start_gets_short_guard() {
        let mut state = prefix_state(7_726, 0, 7_680);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(26);
        state.seen_bucket_tokens = 7_680;
        state.seen_bucket_tokens_128 = 7_680;
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.avoidable_shortfall_streak = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 3,
                message_chars: 13_029,
                source: Some("message".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_stale_full_prefix_does_not_wait_without_instability() {
        let mut state = prefix_state(93_845, 93_696, 0);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(120);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 0;
        state.shortfall_tokens_128 = 0;
        state.cache_instability_score = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics::default(),
        );

        assert_eq!(wait, None);
    }

    #[test]
    fn responses_long_idle_large_tail_gets_probe_guard_even_without_prior_gap() {
        let mut state = prefix_state(65_295, 65_024, 0);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(5 * 60);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 0;
        state.shortfall_tokens_128 = 0;
        state.cache_instability_score = 0;

        let wait = provider_prefix_wait_duration_for_channel(
            &Channel::Responses,
            &state,
            &TailInputDiagnostics {
                input_items: 332,
                message_chars: 30_103,
                tool_call_chars: 37_383,
                tool_output_chars: 115_995,
                largest_tool_output_chars: 33_924,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            },
        );

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
    }

    #[test]
    fn responses_long_idle_medium_tool_tail_gets_probe_guard() {
        let mut state = prefix_state(89_559, 89_088, 0);
        state.finished_at = Instant::now() - std::time::Duration::from_secs(90);
        state.avoidable_shortfall_tokens = 0;
        state.avoidable_shortfall_tokens_128 = 0;
        state.shortfall_tokens = 0;
        state.shortfall_tokens_128 = 0;
        state.cache_instability_score = 0;

        let tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 3_009,
            largest_tool_output_chars: 3_009,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };
        let wait = provider_prefix_wait_duration_for_channel(&Channel::Responses, &state, &tail);
        let reason = provider_prefix_wait_reason_for_channel(&Channel::Responses, &state, &tail);

        assert_eq!(wait, Some(responses_foreground_wait_cap()));
        assert_eq!(
            reason.as_deref(),
            Some("responses_stale_medium_tool_tail_probe")
        );
    }

    #[test]
    fn responses_huge_mixed_history_tail_does_not_mark_old_waterline_as_avoidable() {
        let current_tail = TailInputDiagnostics {
            input_items: 332,
            message_chars: 30_103,
            tool_call_chars: 37_383,
            tool_output_chars: 115_995,
            largest_tool_output_chars: 33_924,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        assert!(responses_current_tail_makes_avoidable_unreliable(
            &current_tail
        ));
    }

    #[tokio::test]
    async fn responses_realistic_large_mixed_tool_tail_is_not_avoidable_gap() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-large-mixed-tool-tail-unreliable-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 57_799,
            output_tokens: 10,
            cache_read_tokens: 54_784,
            cache_creation_tokens: 0,
        };
        let regressed = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 39_474,
            output_tokens: 10,
            cache_read_tokens: 34_816,
            cache_creation_tokens: 0,
        };
        let large_mixed_tail = TailInputDiagnostics {
            input_items: 55,
            message_chars: 13_622,
            tool_call_chars: 3_291,
            tool_output_chars: 112_184,
            largest_tool_output_chars: 14_859,
            tool_output_noise_hint: Some("hash_like,path_like,timestamp_like".to_string()),
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&warm),
            &TailInputDiagnostics::default(),
            false,
            false,
        )
        .await;
        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&regressed),
            Some(&large_mixed_tail),
        )
        .await
        .unwrap();

        assert!(responses_current_tail_makes_avoidable_unreliable(
            &large_mixed_tail
        ));
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, provider_cache_shortfall(&regressed));

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&regressed),
            &large_mixed_tail,
            false,
            false,
        )
        .await;
        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.avoidable_shortfall_tokens, 0);
        assert_eq!(kept.avoidable_shortfall_tokens_128, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_new_anchor_scope_sibling_tail_diagnostics_use_delta_only() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-scope-sibling-tail-diagnostics-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.response_sessions.lock().await.insert(
            "old-anchor".to_string(),
            ResponseSessionState {
                response_id: "resp_old".to_string(),
                input: json!([
                    { "type": "message", "role": "user", "content": "anchor" },
                    { "type": "message", "role": "assistant", "content": "covered" }
                ]),
                scope_key: Some("scope-a".to_string()),
                finished_at: Instant::now(),
            },
        );
        let current_input = json!([
            { "type": "message", "role": "user", "content": "anchor" },
            { "type": "message", "role": "assistant", "content": "covered" },
            { "type": "message", "role": "user", "content": "continue" }
        ]);

        let diagnostics = tail_input_diagnostics_for_session(
            &state,
            &Channel::Responses,
            Some("new-anchor"),
            Some("scope-a"),
            Some(&current_input),
        )
        .await;

        assert!(diagnostics.delta_from_session);
        assert_eq!(diagnostics.input_items, 1);
        assert_eq!(diagnostics.message_chars, "continue".chars().count() as u64);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn first_huge_dynamic_history_cold_read_is_provider_unstable() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-first-huge-history-tail-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let cold_history = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 110_448,
            output_tokens: 196,
            cache_read_tokens: 9_216,
            cache_creation_tokens: 0,
        };
        let huge_mixed_tail = TailInputDiagnostics {
            input_items: 143,
            message_chars: 160_047,
            tool_call_chars: 15_310,
            tool_output_chars: 193_487,
            largest_tool_output_chars: 40_592,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("ls\0gpt-5.5\0responses\0new-fingerprint"),
            None,
            Some(&cold_history),
            Some(&huge_mixed_tail),
        )
        .await
        .unwrap();
        let lag = prefix_lag_diagnostics(
            &state,
            Some("ls\0gpt-5.5\0responses\0new-fingerprint"),
            Some(&cold_history),
            Some(&gap),
            &PrefixGuardWaitDiagnostics::default(),
            &huge_mixed_tail,
        )
        .await;

        let expected_gap = provider_cache_shortfall(&cold_history);
        assert_eq!(gap.total_tokens, expected_gap);
        assert_eq!(gap.new_tail_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, expected_gap);
        assert_eq!(
            lag.classification.as_deref(),
            Some("first_prefix_huge_dynamic_history")
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn family_waterline_only_isolates_huge_dynamic_cold_read_without_avoidable() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-family-huge-history-isolation-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let family_key = "prefix-family\0ls\0gpt-5.5\0responses\0scope-a";
        let warm = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 115_485,
            output_tokens: 10,
            cache_read_tokens: 110_080,
            cache_creation_tokens: 0,
        };
        let cold_history = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 117_557,
            output_tokens: 297,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let huge_mixed_tail = TailInputDiagnostics {
            input_items: 154,
            message_chars: 165_395,
            tool_output_chars: 219_234,
            largest_tool_output_chars: 40_592,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some(family_key), Some(&warm), false, false).await;
        let gap = provider_cache_gap_breakdown(
            &state,
            Some("ls\0gpt-5.5\0responses\0new-exact-fingerprint"),
            Some(family_key),
            Some(&cold_history),
            Some(&huge_mixed_tail),
        )
        .await
        .unwrap();

        let expected_gap = provider_cache_shortfall(&cold_history);
        assert_eq!(gap.total_tokens, expected_gap);
        assert_eq!(gap.new_tail_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, expected_gap);

        fs::remove_dir_all(dir).ok();
    }
    #[tokio::test]
    async fn responses_same_prefix_cold_read_is_provider_unstable_not_avoidable() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-same-prefix-cold-read-isolated-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 112_162,
            output_tokens: 10,
            cache_read_tokens: 111_616,
            cache_creation_tokens: 0,
        };
        let cold_read = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 112_366,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let tiny_tool_tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 161,
            largest_tool_output_chars: 161,
            tool_output_noise_hint: Some("path_like".to_string()),
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&warm),
            &TailInputDiagnostics::default(),
            false,
            false,
        )
        .await;

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&cold_read),
            Some(&tiny_tool_tail),
        )
        .await
        .unwrap();
        let expected_gap = provider_cache_shortfall(&cold_read);
        assert_eq!(gap.total_tokens, expected_gap);
        assert_eq!(gap.new_tail_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, expected_gap);

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&cold_read),
            &tiny_tool_tail,
            false,
            false,
        )
        .await;
        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.cache_read_tokens, warm.cache_read_tokens);
        assert_eq!(kept.avoidable_shortfall_tokens, 0);
        assert_eq!(kept.avoidable_shortfall_tokens_128, 0);
        assert!(kept.cache_instability_score > 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_cold_read_marks_prefix_unstable_after_warm_state() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-instability-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 108_610,
            output_tokens: 10,
            cache_read_tokens: 107_520,
            cache_creation_tokens: 0,
        };
        let cold_gap = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 108_774,
            output_tokens: 10,
            cache_read_tokens: 89_856,
            cache_creation_tokens: 0,
        };
        let recovered = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 108_938,
            output_tokens: 10,
            cache_read_tokens: 108_288,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&cold_gap), false, false)
            .await;
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&recovered), false, false)
            .await;

        let states = state.prefix_states.lock().await;
        let prefix = states.get("main-prefix").unwrap();
        assert!(
            prefix.cache_instability_score > 0,
            "one recovered turn must not erase recent cold-read instability"
        );
        assert!(
            provider_prefix_wait_duration_for_channel(
                &Channel::Responses,
                prefix,
                &TailInputDiagnostics {
                    tool_output_chars: 40,
                    largest_tool_output_chars: 40,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
            )
            .is_some_and(|wait| wait <= TokioDuration::from_secs(30)),
            "recent instability should keep a bounded Responses guard"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn response_session_full_context_usage_advances_prefix_state() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-prefix-state-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 236_227,
            output_tokens: 10,
            cache_read_tokens: 230_912,
            cache_creation_tokens: 0,
        };
        let session_delta_or_full_context = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 242_534,
            output_tokens: 10,
            cache_read_tokens: 236_544,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&session_delta_or_full_context),
            true,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.input_tokens, 242_534);
        assert_eq!(kept.cache_read_tokens, 236_544);
        assert_eq!(kept.seen_bucket_tokens, 236_544);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn small_new_tail_shortfall_does_not_advance_sent_bucket() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 59_601,
            output_tokens: 10,
            cache_read_tokens: 59_392,
            cache_creation_tokens: 0,
        };
        let new_tail_not_yet_cached = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 59_922,
            output_tokens: 10,
            cache_read_tokens: 59_392,
            cache_creation_tokens: 0,
        };
        let same_waterline_next_request = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 60_019,
            output_tokens: 10,
            cache_read_tokens: 59_392,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        let first_gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&new_tail_not_yet_cached),
            None,
        )
        .await
        .unwrap();
        assert_eq!(first_gap.new_tail_tokens, 512);
        assert_eq!(first_gap.avoidable_tokens, 0);

        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&new_tail_not_yet_cached),
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.seen_bucket_tokens, 59_392);
        drop(states);

        let second_gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&same_waterline_next_request),
            None,
        )
        .await
        .unwrap();
        assert_eq!(second_gap.new_tail_tokens, 512);
        assert_eq!(second_gap.avoidable_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn high_hit_medium_new_tail_advances_sent_bucket_for_next_guard() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-medium-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 237_315,
            output_tokens: 10,
            cache_read_tokens: 233_984,
            cache_creation_tokens: 0,
        };
        let medium_tail = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 240_200,
            output_tokens: 10,
            cache_read_tokens: 236_032,
            cache_creation_tokens: 0,
        };
        let next_same_waterline = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 240_412,
            output_tokens: 10,
            cache_read_tokens: 236_032,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&medium_tail), 4_096);
        assert!(provider_cache_ratio(&medium_tail).unwrap() >= 0.98);
        let states = state.prefix_states.lock().await;
        assert!(should_learn_sent_provider_bucket(
            states.get("main-prefix"),
            &medium_tail,
            provider_cache_shortfall(&medium_tail),
            provider_cache_shortfall_128(&medium_tail),
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 5_643,
                largest_tool_output_chars: 5_643,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false
        ));
        drop(states);

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&medium_tail),
            &TailInputDiagnostics {
                input_items: 1,
                tool_output_chars: 5_643,
                largest_tool_output_chars: 5_643,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(medium_tail.input_tokens)
        );
        drop(states);

        let next_gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&next_same_waterline),
            None,
        )
        .await
        .unwrap();
        assert_eq!(next_gap.avoidable_tokens, 4_096);
        assert_eq!(next_gap.new_tail_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn medium_tool_tail_advances_aligned_bucket_for_next_guard() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-medium-tool-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 90_120,
            output_tokens: 10,
            cache_read_tokens: 87_040,
            cache_creation_tokens: 0,
        };
        let medium_tool_tail = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 93_617,
            output_tokens: 10,
            cache_read_tokens: 87_040,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 7_558,
            largest_tool_output_chars: 7_558,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&medium_tool_tail), 6_144);
        assert!(should_learn_sent_provider_bucket(
            state.prefix_states.lock().await.get("main-prefix"),
            &medium_tool_tail,
            provider_cache_shortfall(&medium_tool_tail),
            provider_cache_shortfall_128(&medium_tool_tail),
            &tail,
            false
        ));
        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&medium_tool_tail),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(medium_tool_tail.input_tokens)
        );
        drop(states);

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&medium_tool_tail),
            Some(&tail),
        )
        .await
        .unwrap();
        assert_eq!(gap.avoidable_tokens, 6_144);
        assert_eq!(gap.new_tail_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn high_hit_aligned_large_tool_tail_advances_sent_bucket() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-large-aligned-tool-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 125_300,
            output_tokens: 10,
            cache_read_tokens: 123_392,
            cache_creation_tokens: 0,
        };
        let aligned_tail = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 132_800,
            output_tokens: 10,
            cache_read_tokens: 123_392,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 2,
            tool_output_chars: 13_736,
            largest_tool_output_chars: 13_705,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&aligned_tail), 9_216);
        assert!(provider_cache_ratio(&aligned_tail).unwrap() >= 0.92);
        assert!(should_learn_sent_provider_bucket(
            state.prefix_states.lock().await.get("main-prefix"),
            &aligned_tail,
            provider_cache_shortfall(&aligned_tail),
            provider_cache_shortfall_128(&aligned_tail),
            &tail,
            false
        ));
        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&aligned_tail),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(aligned_tail.input_tokens)
        );
        drop(states);

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&aligned_tail),
            Some(&tail),
        )
        .await
        .unwrap();
        assert_eq!(gap.avoidable_tokens, 9_216);
        assert_eq!(gap.new_tail_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn high_hit_128_granular_tool_tail_advances_sent_bucket() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-128-tool-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 90_881,
            output_tokens: 10,
            cache_read_tokens: 62_336,
            cache_creation_tokens: 0,
        };
        let tail_record = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 93_414,
            output_tokens: 10,
            cache_read_tokens: 90_496,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 6_000,
            largest_tool_output_chars: 6_000,
            tool_output_noise_hint: Some("repeated_lines".to_string()),
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&tail_record), 2_688);
        assert_ne!(provider_cache_shortfall(&tail_record) % 512, 0);
        assert_eq!(provider_cache_shortfall_128(&tail_record) % 128, 0);
        assert!(should_learn_sent_provider_bucket(
            state.prefix_states.lock().await.get("main-prefix"),
            &tail_record,
            provider_cache_shortfall(&tail_record),
            provider_cache_shortfall_128(&tail_record),
            &tail,
            false
        ));

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&tail_record),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(tail_record.input_tokens)
        );
        assert_eq!(
            kept.seen_bucket_tokens_128,
            provider_cache_bucket_max_128(tail_record.input_tokens)
        );
        drop(states);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn large_128_granular_tool_tail_learns_sent_bucket_without_extra_request() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-large-128-tool-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 46_774,
            output_tokens: 10,
            cache_read_tokens: 9_088,
            cache_creation_tokens: 0,
        };
        let large_tail = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 62_618,
            output_tokens: 10,
            cache_read_tokens: 46_464,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 4,
            tool_output_chars: 51_000,
            largest_tool_output_chars: 24_000,
            tool_output_noise_hint: Some("path_like,url_like".to_string()),
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&large_tail), 16_000);
        assert_ne!(provider_cache_shortfall(&large_tail) % 512, 0);
        assert_eq!(provider_cache_shortfall_128(&large_tail) % 128, 0);
        assert!(provider_cache_ratio(&large_tail).unwrap() >= 0.72);
        assert!(should_learn_sent_provider_bucket(
            state.prefix_states.lock().await.get("main-prefix"),
            &large_tail,
            provider_cache_shortfall(&large_tail),
            provider_cache_shortfall_128(&large_tail),
            &tail,
            false
        ));

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&large_tail),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(large_tail.input_tokens)
        );
        assert_eq!(
            kept.seen_bucket_tokens_128,
            provider_cache_bucket_max_128(large_tail.input_tokens)
        );
        drop(states);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn compact_aligned_10k_tool_tail_advances_sent_bucket_for_next_guard() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-10k-tool-tail-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 118_701,
            output_tokens: 10,
            cache_read_tokens: 103_296,
            cache_creation_tokens: 0,
        };
        let compact_tail = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 119_708,
            output_tokens: 10,
            cache_read_tokens: 109_056,
            cache_creation_tokens: 0,
        };
        let next_same_waterline = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 120_433,
            output_tokens: 10,
            cache_read_tokens: 110_080,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 2_873,
            largest_tool_output_chars: 2_873,
            tool_output_noise_hint: Some("path_like,hash_like".to_string()),
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&compact_tail), 10_240);
        assert!(provider_cache_ratio(&compact_tail).unwrap() >= 0.90);
        assert!(should_learn_sent_provider_bucket(
            state.prefix_states.lock().await.get("main-prefix"),
            &compact_tail,
            provider_cache_shortfall(&compact_tail),
            provider_cache_shortfall_128(&compact_tail),
            &tail,
            false
        ));
        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&compact_tail),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(
            kept.seen_bucket_tokens,
            provider_cache_bucket_max(compact_tail.input_tokens)
        );
        drop(states);

        let next_gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&next_same_waterline),
            Some(&tail),
        )
        .await
        .unwrap();
        assert_eq!(next_gap.avoidable_tokens, 9_216);
        assert_eq!(next_gap.new_tail_tokens, 1_024);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn large_low_hit_tool_tail_does_not_advance_sent_bucket() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-large-tail-no-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 46_286,
            output_tokens: 10,
            cache_read_tokens: 45_568,
            cache_creation_tokens: 0,
        };
        let large_tail = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 62_900,
            output_tokens: 10,
            cache_read_tokens: 37_376,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        assert_eq!(provider_cache_shortfall(&large_tail), 25_088);
        let states = state.prefix_states.lock().await;
        assert!(!should_learn_sent_provider_bucket(
            states.get("main-prefix"),
            &large_tail,
            provider_cache_shortfall(&large_tail),
            provider_cache_shortfall_128(&large_tail),
            &TailInputDiagnostics {
                input_items: 4,
                tool_output_chars: 48_336,
                largest_tool_output_chars: 42_942,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false
        ));
        drop(states);

        update_provider_prefix_state_with_tail(
            &state,
            Some("main-prefix"),
            None,
            Some(&large_tail),
            &TailInputDiagnostics {
                input_items: 4,
                tool_output_chars: 48_336,
                largest_tool_output_chars: 42_942,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.seen_bucket_tokens, warm.cache_read_tokens);
        assert!(kept.seen_bucket_tokens < provider_cache_bucket_max(large_tail.input_tokens));

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn prefix_lag_diagnostic_keeps_small_sent_bucket_lag_as_tail_granularity() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-lag-diagnostic-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let previous = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 59_922,
            output_tokens: 10,
            cache_read_tokens: 59_392,
            cache_creation_tokens: 0,
        };
        let next = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 60_019,
            output_tokens: 10,
            cache_read_tokens: 59_392,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&previous), false, false)
            .await;
        let gap =
            provider_cache_gap_breakdown(&state, Some("main-prefix"), None, Some(&next), None)
                .await
                .unwrap();
        let diag = prefix_lag_diagnostics(
            &state,
            Some("main-prefix"),
            Some(&next),
            Some(&gap),
            &PrefixGuardWaitDiagnostics {
                wait_ms: 78_000,
                reason: Some("responses_large_tool_output_tail_guard".to_string()),
                source: Some("exact".to_string()),
                state_age_ms: None,
                skip_reason: None,
                budget_exhausted: false,
                cache_instability_score: Some(0),
                seen_bucket_tokens: Some(59_392),
                state_cache_read_tokens: Some(59_392),
            },
            &TailInputDiagnostics::default(),
        )
        .await;

        assert_eq!(
            diag.classification.as_deref(),
            Some("tail_lag_previous_not_caught")
        );
        assert_eq!(diag.previous_gap_tokens, Some(512));
        assert_eq!(diag.cache_delta_tokens, Some(0));

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn prefix_lag_diagnostic_marks_same_prefix_zero_read_as_cold_read_after_warm() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-lag-cold-read-after-warm-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let previous = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 89_559,
            output_tokens: 10,
            cache_read_tokens: 89_088,
            cache_creation_tokens: 0,
        };
        let zero_read = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 89_800,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&previous), false, false)
            .await;
        let gap = ProviderCacheGapBreakdown {
            total_tokens: 0,
            new_tail_tokens: 0,
            avoidable_tokens: 0,
            provider_unstable_tokens: 0,
        };

        let diag = prefix_lag_diagnostics(
            &state,
            Some("main-prefix"),
            Some(&zero_read),
            Some(&gap),
            &PrefixGuardWaitDiagnostics::default(),
            &TailInputDiagnostics::default(),
        )
        .await;

        assert_eq!(diag.classification.as_deref(), Some("cold_read_after_warm"));
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn prefix_lag_diagnostic_does_not_call_isolated_low_hit_prefix_full() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-lag-isolated-break-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let previous = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 151_000,
            output_tokens: 10,
            cache_read_tokens: 150_528,
            cache_creation_tokens: 0,
        };
        let isolated_break = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 155_768,
            output_tokens: 10,
            cache_read_tokens: 9_216,
            cache_creation_tokens: 0,
        };
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&previous), false, false)
            .await;
        let isolated_gap = ProviderCacheGapBreakdown {
            total_tokens: 0,
            new_tail_tokens: 0,
            avoidable_tokens: 0,
            provider_unstable_tokens: 0,
        };

        let diag = prefix_lag_diagnostics(
            &state,
            Some("main-prefix"),
            Some(&isolated_break),
            Some(&isolated_gap),
            &PrefixGuardWaitDiagnostics::default(),
            &TailInputDiagnostics::default(),
        )
        .await;

        assert_eq!(
            diag.classification.as_deref(),
            Some("prefix_break_isolated")
        );
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_does_not_use_prompt_cache_family_waterline() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-no-prefix-family-gap-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let family_key = "prefix-family\0share\0gpt-5.5\0responses\0stable-prompt-cache-key";
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 80_256,
            output_tokens: 10,
            cache_read_tokens: 79_872,
            cache_creation_tokens: 0,
        };
        let dynamic_fingerprint_regression = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 80_711,
            output_tokens: 10,
            cache_read_tokens: 78_848,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some(family_key), Some(&warm), false, false).await;

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("share\0gpt-5.5\0responses\0new-dynamic-fingerprint"),
            Some(family_key),
            Some(&dynamic_fingerprint_regression),
            None,
        )
        .await
        .unwrap();

        assert_eq!(gap.total_tokens, 1_536);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, 1_536);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn response_session_scope_sibling_can_guard_without_reclassifying_gap() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-scope-sibling-prefix-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let exact_key = "share\0gpt-5.5\0responses\0session-anchor-a";
        let sibling_key = "share\0gpt-5.5\0responses\0session-anchor-b";
        let family_key = "prefix-family\0share\0gpt-5.5\0responses\0scope-stable";
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 80_256,
            output_tokens: 10,
            cache_read_tokens: 79_872,
            cache_creation_tokens: 0,
        };
        let sibling_cold = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 80_711,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state_with_tail(
            &state,
            Some(exact_key),
            Some(family_key),
            Some(&warm),
            &TailInputDiagnostics::default(),
            false,
            false,
        )
        .await;

        let wait = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some(sibling_key),
            Some(family_key),
            &TailInputDiagnostics::default(),
            None,
        )
        .await;
        assert_eq!(wait.source.as_deref(), Some("session-sibling"));

        let gap = provider_cache_gap_breakdown(
            &state,
            Some(sibling_key),
            Some(family_key),
            Some(&sibling_cold),
            None,
        )
        .await
        .unwrap();
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(
            gap.new_tail_tokens,
            provider_cache_bucket_max(sibling_cold.input_tokens)
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn compact_tool_tail_family_waterline_does_not_delay_sibling_session() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-compact-tool-tail-family-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let exact_key = "share\0gpt-5.5\0responses\0session-anchor-a";
        let sibling_key = "share\0gpt-5.5\0responses\0session-anchor-b";
        let family_key = "prefix-family\0share\0gpt-5.5\0responses\0scope-stable";
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 136_931,
            output_tokens: 10,
            cache_read_tokens: 135_680,
            cache_creation_tokens: 0,
        };
        let compact_tail = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 140_175,
            output_tokens: 10,
            cache_read_tokens: 136_704,
            cache_creation_tokens: 0,
        };
        let tail = TailInputDiagnostics {
            input_items: 1,
            tool_output_chars: 10_839,
            largest_tool_output_chars: 10_839,
            tool_output_noise_hint: Some("path_like".to_string()),
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state_with_tail(
            &state,
            Some(exact_key),
            Some(family_key),
            Some(&warm),
            &TailInputDiagnostics::default(),
            false,
            false,
        )
        .await;
        update_provider_prefix_state_with_tail(
            &state,
            Some(exact_key),
            Some(family_key),
            Some(&compact_tail),
            &tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let family = states.get(family_key).unwrap();
        assert_eq!(
            family.seen_bucket_tokens,
            provider_cache_bucket_max(compact_tail.input_tokens)
        );
        drop(states);

        let wait = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some(sibling_key),
            Some(family_key),
            &tail,
            None,
        )
        .await;
        assert_eq!(wait.source.as_deref(), Some("session-sibling"));
        assert_eq!(wait.wait_ms, 0);
        assert_eq!(wait.skip_reason.as_deref(), Some("non_exact_prefix_state"));

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_runtime_guard_waits_only_for_fresh_exact_avoidable_state() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-exact-avoidable-runtime-guard-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let key = "share\0gpt-5.5\0responses\0session-anchor-exact";
        let mut prefix = prefix_state(64_512, 63_872, 512);
        prefix.seen_bucket_tokens = 64_384;
        prefix.seen_bucket_tokens_128 = 64_384;
        prefix.avoidable_shortfall_tokens = 512;
        prefix.avoidable_shortfall_tokens_128 = 512;
        state
            .prefix_states
            .lock()
            .await
            .insert(key.to_string(), prefix);

        let guarded = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some(key),
            None,
            &TailInputDiagnostics::default(),
            Some(responses_foreground_wait_cap()),
        )
        .await;
        assert!(guarded.wait_ms >= 500);
        assert!(guarded.wait_ms <= 1_000);
        assert_eq!(
            guarded.reason.as_deref(),
            Some("responses_exact_avoidable_gap")
        );

        {
            let mut states = state.prefix_states.lock().await;
            let prefix = states.get_mut(key).unwrap();
            prefix.finished_at = Instant::now();
            prefix.avoidable_shortfall_tokens = 0;
            prefix.avoidable_shortfall_tokens_128 = 0;
        }
        let unguarded = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some(key),
            None,
            &TailInputDiagnostics::default(),
            Some(responses_foreground_wait_cap()),
        )
        .await;
        assert_eq!(unguarded.wait_ms, 0);
        assert_eq!(unguarded.skip_reason.as_deref(), Some("no_avoidable_gap"));

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn tool_tail_burst_does_not_write_family_waterline() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-tool-tail-family-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let burst_record = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 96_512,
            output_tokens: 10,
            cache_read_tokens: 88_064,
            cache_creation_tokens: 0,
        };
        let exact_key = "share\0gpt-5.5\0responses\0session-anchor-a";
        let family_key = "prefix-family\0share\0gpt-5.5\0responses\0scope-stable";

        update_provider_prefix_state_with_tail(
            &state,
            Some(exact_key),
            Some(family_key),
            Some(&burst_record),
            &TailInputDiagnostics {
                input_items: 4,
                tool_output_chars: 48_336,
                largest_tool_output_chars: 42_942,
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        assert!(states.get(exact_key).is_some());
        assert!(states.get(family_key).is_none());

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn prefix_state_alias_does_not_drive_cross_provider_foreground_wait() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prefix-alias-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let share_key = "share\0gpt-5.5\0responses\0stable-fingerprint";
        let api_key = "api-1\0gpt-5.5\0responses\0stable-fingerprint";
        let warm_with_small_tail = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 211_046,
            output_tokens: 10,
            cache_read_tokens: 209_920,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(
            &state,
            Some(share_key),
            Some(&warm_with_small_tail),
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        assert!(states.get(api_key).is_none());
        assert!(lookup_provider_prefix_state(&states, api_key).is_none());
        drop(states);

        let wait = wait_for_provider_prefix_settle(
            &state,
            &Channel::Responses,
            Some(api_key),
            None,
            &TailInputDiagnostics::default(),
            None,
        )
        .await;
        assert_eq!(wait.skip_reason.as_deref(), Some("no_prefix_state"));
        assert_eq!(wait.wait_ms, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn cold_dynamic_tail_after_alias_warm_preserves_warm_state() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-alias-cold-dynamic-preserve-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm_key = "https://hubway.cc/v1\0gpt-5.5\0responses\0stable-fingerprint";
        let current_key = "https://hubway.cc/v1\0gpt-5.5\0responses\0stable-fingerprint";
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 239_198,
            output_tokens: 10,
            cache_read_tokens: 238_592,
            cache_creation_tokens: 0,
        };
        let cold_dynamic = UsageRecord {
            provider: "api-1".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 239_324,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let huge_tail = TailInputDiagnostics {
            input_items: 48,
            message_chars: 110_084,
            tool_output_chars: 419_791,
            largest_tool_output_chars: 46_766,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some(warm_key), Some(&warm), false, false).await;
        update_provider_prefix_state_with_tail(
            &state,
            Some(current_key),
            None,
            Some(&cold_dynamic),
            &huge_tail,
            false,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let current = states.get(current_key).unwrap();
        assert_eq!(current.cache_read_tokens, warm.cache_read_tokens);
        assert_eq!(current.seen_bucket_tokens, warm.cache_read_tokens);
        assert!(current.cache_instability_score >= 2);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn cold_dynamic_tail_after_alias_warm_is_not_counted_as_new_tail() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-alias-cold-dynamic-gap-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm_key = "share\0gpt-5.5\0responses\0stable-fingerprint";
        let current_key = "api-1\0gpt-5.5\0responses\0stable-fingerprint";
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 239_198,
            output_tokens: 10,
            cache_read_tokens: 238_592,
            cache_creation_tokens: 0,
        };
        let cold_dynamic = UsageRecord {
            provider: "api-1".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 239_324,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some(warm_key), Some(&warm), false, false).await;
        let gap = provider_cache_gap_breakdown(
            &state,
            Some(current_key),
            None,
            Some(&cold_dynamic),
            Some(&TailInputDiagnostics {
                input_items: 48,
                message_chars: 110_084,
                tool_output_chars: 419_791,
                largest_tool_output_chars: 46_766,
                source: Some("mixed".to_string()),
                ..TailInputDiagnostics::default()
            }),
        )
        .await
        .unwrap();

        let expected_gap = provider_cache_shortfall(&cold_dynamic);
        assert_eq!(gap.total_tokens, expected_gap);
        assert_eq!(gap.new_tail_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, expected_gap);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn exact_prefix_break_after_warm_is_not_counted_as_new_tail() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-exact-prefix-break-gap-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let key = "ls\0gpt-5.5\0responses\0stable-fingerprint";
        let warm = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 251_500,
            output_tokens: 10,
            cache_read_tokens: 250_880,
            cache_creation_tokens: 0,
        };
        let cold = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 218_506,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };
        let weak = UsageRecord {
            provider: "ls".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 219_503,
            output_tokens: 10,
            cache_read_tokens: 9_216,
            cache_creation_tokens: 0,
        };
        let huge_tail = TailInputDiagnostics {
            input_items: 56,
            message_chars: 105_876,
            tool_output_chars: 639_858,
            largest_tool_output_chars: 46_766,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        update_provider_prefix_state(&state, Some(key), Some(&warm), false, false).await;
        let cold_gap =
            provider_cache_gap_breakdown(&state, Some(key), None, Some(&cold), Some(&huge_tail))
                .await
                .unwrap();
        let cold_expected_gap = provider_cache_shortfall(&cold);
        assert_eq!(cold_gap.total_tokens, cold_expected_gap);
        assert_eq!(cold_gap.new_tail_tokens, 0);
        assert_eq!(cold_gap.avoidable_tokens, 0);
        assert_eq!(cold_gap.provider_unstable_tokens, cold_expected_gap);

        update_provider_prefix_state_with_tail(
            &state,
            Some(key),
            None,
            Some(&cold),
            &huge_tail,
            false,
            false,
        )
        .await;
        let weak_gap = provider_cache_gap_breakdown(
            &state,
            Some(key),
            None,
            Some(&weak),
            Some(&TailInputDiagnostics {
                input_items: 2,
                tool_output_chars: 2_533,
                largest_tool_output_chars: 2_253,
                tool_output_noise_hint: Some("path_like".to_string()),
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            }),
        )
        .await
        .unwrap();
        let weak_expected_gap = provider_cache_shortfall(&weak);
        assert_eq!(weak_gap.total_tokens, weak_expected_gap);
        assert_eq!(weak_gap.new_tail_tokens, 0);
        assert_eq!(weak_gap.avoidable_tokens, 0);
        assert_eq!(weak_gap.provider_unstable_tokens, weak_expected_gap);

        update_provider_prefix_state_with_tail(
            &state,
            Some(key),
            None,
            Some(&weak),
            &TailInputDiagnostics {
                input_items: 2,
                tool_output_chars: 2_533,
                largest_tool_output_chars: 2_253,
                tool_output_noise_hint: Some("path_like".to_string()),
                source: Some("tool_output".to_string()),
                ..TailInputDiagnostics::default()
            },
            false,
            false,
        )
        .await;
        let states = state.prefix_states.lock().await;
        let kept = states.get(key).unwrap();
        assert_eq!(kept.cache_read_tokens, warm.cache_read_tokens);
        assert_eq!(kept.seen_bucket_tokens, warm.cache_read_tokens);
        assert_eq!(kept.shortfall_tokens, provider_cache_shortfall(&weak));
        assert!(kept.cache_instability_score >= 2);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_does_not_treat_cross_provider_alias_as_avoidable() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-cross-provider-gap-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let share_key = "share\0gpt-5.5\0responses\0stable-fingerprint";
        let api_key = "api-1\0gpt-5.5\0responses\0stable-fingerprint";
        let share_warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 246_538,
            output_tokens: 10,
            cache_read_tokens: 245_760,
            cache_creation_tokens: 0,
        };
        let api_cold = UsageRecord {
            provider: "api-1".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 246_538,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some(share_key), Some(&share_warm), false, false)
            .await;

        {
            let states = state.prefix_states.lock().await;
            assert!(states.get(api_key).is_none());
            assert!(lookup_provider_prefix_state(&states, api_key).is_none());
        }

        let gap = provider_cache_gap_breakdown(&state, Some(api_key), None, Some(&api_cold), None)
            .await
            .unwrap();

        assert_eq!(
            gap.total_tokens,
            provider_cache_bucket_max(api_cold.input_tokens)
        );
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, gap.total_tokens);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn provider_gap_breakdown_calibrates_stale_high_waterline() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-stale-high-waterline-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        state.prefix_states.lock().await.insert(
            "main-prefix".to_string(),
            PrefixWarmState {
                finished_at: Instant::now(),
                input_tokens: 270_848,
                cache_read_tokens: 270_848,
                shortfall_tokens: 0,
                seen_bucket_tokens: 270_848,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: 0,
                seen_bucket_tokens_128: 271_104,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                cache_instability_score: 8,
                tail_tool_output_chars: 11_356,
                tail_largest_tool_output_chars: 11_356,
                tail_tool_output_noise_hint: Some("path_like".to_string()),
            },
        );
        let weak_after_old_state = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 137_775,
            output_tokens: 10,
            cache_read_tokens: 134_144,
            cache_creation_tokens: 0,
        };

        let gap = provider_cache_gap_breakdown(
            &state,
            Some("main-prefix"),
            None,
            Some(&weak_after_old_state),
            None,
        )
        .await
        .unwrap();

        assert_eq!(gap.total_tokens, 3_584);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, 3_584);

        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&weak_after_old_state),
            false,
            false,
        )
        .await;
        let states = state.prefix_states.lock().await;
        let next = states.get("main-prefix").unwrap();
        assert_eq!(next.seen_bucket_tokens, 134_144);
        assert_eq!(next.avoidable_shortfall_tokens, 0);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_session_anchor_prevents_cross_chat_cold_start_pollution() {
        let mut config = AppConfig::default();
        config.workspace_fingerprint = "workspace".to_string();
        let decision = RouteDecision {
            provider: ProviderConfig {
                id: "bizd".to_string(),
                name: "bizd".to_string(),
                base_url: "https://bizd.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-anchor-prefix-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config.clone(), dir.join("config.toml"), cache).unwrap();
        let chat_a = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "chat A anchor" }]
        });
        let chat_b = json!({
            "model": "gpt-5.5",
            "instructions": "stable system",
            "tools": [{ "type": "function", "name": "read_file" }],
            "input": [{ "type": "message", "role": "user", "content": "chat B anchor" }]
        });
        let key_a = provider_prefix_control_key(
            Some(&provider_prefix_fingerprint(&chat_a, &Channel::Responses)),
            &decision,
            &Channel::Responses,
        )
        .unwrap();
        let key_b = provider_prefix_control_key(
            Some(&provider_prefix_fingerprint(&chat_b, &Channel::Responses)),
            &decision,
            &Channel::Responses,
        )
        .unwrap();
        assert_ne!(key_a, key_b);

        let warm_a = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 200_870,
            output_tokens: 10,
            cache_read_tokens: 199_680,
            cache_creation_tokens: 0,
        };
        let cold_b = UsageRecord {
            provider: "bizd".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 200_870,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some(&key_a), Some(&warm_a), false, false).await;
        let gap = provider_cache_gap_breakdown(&state, Some(&key_b), None, Some(&cold_b), None)
            .await
            .unwrap();

        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(
            gap.new_tail_tokens,
            provider_cache_bucket_max(cold_b.input_tokens)
        );

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn weak_full_retry_after_session_delta_does_not_lower_warm_prefix_state() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-weak-full-retry-waterline-{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).unwrap();
        let state = AppState::for_test(
            AppConfig::default(),
            dir.join("config.toml"),
            CacheStore::load(dir.join("cache.bin")).unwrap(),
        )
        .unwrap();
        let warm = UsageRecord {
            input_tokens: 107_000,
            cache_read_tokens: 95_360,
            ..UsageRecord::default()
        };
        let weak_retry = UsageRecord {
            input_tokens: 107_256,
            cache_read_tokens: 86_400,
            ..UsageRecord::default()
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        update_provider_prefix_state(&state, Some("main-prefix"), Some(&weak_retry), false, true)
            .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.cache_read_tokens, warm.cache_read_tokens);
        assert_eq!(kept.seen_bucket_tokens, warm.cache_read_tokens);
        assert_eq!(kept.shortfall_tokens, provider_cache_shortfall(&weak_retry));

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn response_session_delta_usage_does_not_lower_prefix_state() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-prefix-delta-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 236_227,
            output_tokens: 10,
            cache_read_tokens: 230_912,
            cache_creation_tokens: 0,
        };
        let session_delta_only = UsageRecord {
            provider: "share".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 5_884,
            output_tokens: 10,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&session_delta_only),
            true,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert_eq!(kept.input_tokens, 236_227);
        assert_eq!(kept.cache_read_tokens, 230_912);
        assert_eq!(kept.seen_bucket_tokens, 230_912);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn response_session_delta_usage_does_not_clear_session_as_regression() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-delta-regression-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let warm = UsageRecord {
            provider: "newapi".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 262_662,
            output_tokens: 10,
            cache_read_tokens: 261_632,
            cache_creation_tokens: 0,
        };
        let session_delta_only = UsageRecord {
            provider: "newapi".to_string(),
            model: "gpt-5.5".to_string(),
            input_tokens: 13_396,
            output_tokens: 10,
            cache_read_tokens: 5_632,
            cache_creation_tokens: 0,
        };

        update_provider_prefix_state(&state, Some("main-prefix"), Some(&warm), false, false).await;
        let regressed = update_provider_prefix_state(
            &state,
            Some("main-prefix"),
            Some(&session_delta_only),
            true,
            false,
        )
        .await;

        let states = state.prefix_states.lock().await;
        let kept = states.get("main-prefix").unwrap();
        assert!(!regressed);
        assert_eq!(kept.input_tokens, 262_662);
        assert_eq!(kept.cache_read_tokens, 261_632);
        assert_eq!(kept.seen_bucket_tokens, 261_632);

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn response_session_reuse_is_enabled_for_prefix_max_hit_mode() {
        let mut config = AppConfig::default();
        config.cache.mode = CacheMode::PrefixPrewarm;
        config.cache.enabled = true;
        config.cache.prewarm_enabled = true;

        assert!(responses_session_reuse_enabled(&config));

        config.cache.mode = CacheMode::SessionPrewarm;
        assert!(responses_session_reuse_enabled(&config));

        config.cache.prewarm_enabled = false;
        assert!(!responses_session_reuse_enabled(&config));
    }

    #[tokio::test]
    async fn prefix_prewarm_cooldown_prevents_one_to_one_followup_requests() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-prewarm-cooldown-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();

        assert!(!is_prefix_prewarm_cooled_down(&state, "main-prefix").await);
        note_prefix_prewarm_cooldown(
            &state,
            "main-prefix",
            std::time::Duration::from_secs(PREFIX_BACKGROUND_PREWARM_COOLDOWN_SECS),
        )
        .await;

        assert!(is_prefix_prewarm_cooled_down(&state, "main-prefix").await);
        assert!(!is_prefix_prewarm_cooled_down(&state, "other-prefix").await);

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn active_background_prewarm_is_disabled_even_when_config_says_enabled() {
        let mut config = AppConfig::default();
        config.cache.background_prewarm_enabled = true;
        let cases = [
            (
                Channel::Responses,
                UsageRecord {
                    input_tokens: 166_901,
                    cache_read_tokens: 93_184,
                    ..UsageRecord::default()
                },
                Some(ProviderCacheGapBreakdown {
                    total_tokens: 73_216,
                    new_tail_tokens: 16_384,
                    avoidable_tokens: 56_832,
                    provider_unstable_tokens: 0,
                }),
            ),
            (
                Channel::Responses,
                UsageRecord {
                    input_tokens: 8192,
                    cache_read_tokens: 0,
                    ..UsageRecord::default()
                },
                None,
            ),
            (
                Channel::Chat,
                UsageRecord {
                    input_tokens: 199_605,
                    cache_read_tokens: 122_368,
                    ..UsageRecord::default()
                },
                Some(ProviderCacheGapBreakdown {
                    total_tokens: 76_800,
                    new_tail_tokens: 6656,
                    avoidable_tokens: 70_144,
                    provider_unstable_tokens: 0,
                }),
            ),
            (
                Channel::Anthropic,
                UsageRecord {
                    input_tokens: 64_000,
                    cache_read_tokens: 58_000,
                    ..UsageRecord::default()
                },
                Some(ProviderCacheGapBreakdown {
                    total_tokens: 6144,
                    new_tail_tokens: 4096,
                    avoidable_tokens: 2048,
                    provider_unstable_tokens: 0,
                }),
            ),
        ];

        for (channel, record, gap) in cases {
            assert!(
                !should_background_prewarm(&config, &channel, Some(&record), gap.as_ref()),
                "active prewarm must not create an extra upstream sync request"
            );
        }
    }

    #[test]
    fn responses_prewarm_bucket_count_never_fills_pure_new_tail() {
        let high_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 118_784,
            ..UsageRecord::default()
        };

        for tail in [512, 1024, 1536, 2048] {
            assert_eq!(
                responses_prewarm_bucket_count(
                    &Channel::Responses,
                    Some(&high_hit),
                    Some(&ProviderCacheGapBreakdown {
                        total_tokens: tail,
                        new_tail_tokens: tail,
                        avoidable_tokens: 0,
                        provider_unstable_tokens: 0,
                    }),
                ),
                0,
                "pure new-tail {tail} must not create a companion prewarm"
            );
        }

        let near_full = UsageRecord {
            input_tokens: 139_097,
            cache_read_tokens: 137_216,
            ..UsageRecord::default()
        };
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&near_full),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 9728,
                    new_tail_tokens: 9728,
                    avoidable_tokens: 0,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
    }

    #[test]
    fn responses_prewarm_bucket_count_prioritizes_avoidable_512_buckets() {
        let high_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 118_784,
            ..UsageRecord::default()
        };
        let medium_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 114_000,
            ..UsageRecord::default()
        };
        let low_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 100_000,
            ..UsageRecord::default()
        };

        for (avoidable, expected) in [(512, 0), (1024, 0), (1536, 0), (4096, 1), (8192, 1)] {
            assert_eq!(
                responses_prewarm_bucket_count(
                    &Channel::Responses,
                    Some(&high_hit),
                    Some(&ProviderCacheGapBreakdown {
                        total_tokens: avoidable,
                        new_tail_tokens: 0,
                        avoidable_tokens: avoidable,
                        provider_unstable_tokens: 0,
                    }),
                ),
                expected,
                "avoidable {avoidable} should only run when net savings justify one follow-up prewarm"
            );
        }
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&medium_hit),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 4096,
                    new_tail_tokens: 0,
                    avoidable_tokens: 4096,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&medium_hit),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 8192,
                    new_tail_tokens: 0,
                    avoidable_tokens: 8192,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&low_hit),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 1536,
                    new_tail_tokens: 0,
                    avoidable_tokens: 1536,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
    }

    #[test]
    fn responses_prewarm_bucket_count_is_narrowly_scoped() {
        let high_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 118_784,
            ..UsageRecord::default()
        };
        let low_hit = UsageRecord {
            input_tokens: 120_000,
            cache_read_tokens: 100_000,
            ..UsageRecord::default()
        };
        let small_context = UsageRecord {
            input_tokens: 16_000,
            cache_read_tokens: 15_872,
            ..UsageRecord::default()
        };
        let pure_tail = ProviderCacheGapBreakdown {
            total_tokens: 1536,
            new_tail_tokens: 1536,
            avoidable_tokens: 0,
            provider_unstable_tokens: 0,
        };

        assert_eq!(
            responses_prewarm_bucket_count(&Channel::Chat, Some(&high_hit), Some(&pure_tail)),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(&Channel::Anthropic, Some(&high_hit), Some(&pure_tail)),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(&Channel::Responses, Some(&low_hit), Some(&pure_tail)),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&small_context),
                Some(&pure_tail)
            ),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&high_hit),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 2048,
                    new_tail_tokens: 1536,
                    avoidable_tokens: 512,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
        assert_eq!(
            responses_prewarm_bucket_count(
                &Channel::Responses,
                Some(&low_hit),
                Some(&ProviderCacheGapBreakdown {
                    total_tokens: 9728,
                    new_tail_tokens: 9728,
                    avoidable_tokens: 0,
                    provider_unstable_tokens: 0,
                }),
            ),
            0
        );
    }

    #[test]
    fn response_session_errors_fallback_only_for_invalid_session_statuses() {
        for status in [400, 404, 409, 410, 422] {
            assert!(
                should_retry_full_response_after_session_error(status, true),
                "status {status} should retry as a full Responses request after session reuse fails"
            );
        }
        assert!(!should_retry_full_response_after_session_error(408, true));
        assert!(!should_retry_full_response_after_session_error(429, true));
        assert!(!should_retry_full_response_after_session_error(502, false));
        assert!(!should_retry_full_response_after_session_error(502, true));
        assert!(!should_retry_full_response_after_session_error(503, true));
        assert!(!should_retry_full_response_after_session_error(401, true));
        assert!(!should_retry_full_response_after_session_error(403, true));
    }

    #[test]
    fn response_session_rejection_classifies_stale_id_without_provider_cooldown() {
        assert_eq!(
            response_session_rejection_classification(
                400,
                "Previous response with id 'resp_old' not found. provider_code=previous_response_not_found"
            ),
            ResponseSessionRejectionClass::StaleReference
        );
        assert_eq!(
            response_session_rejection_classification(
                400,
                "Unsupported parameter: previous_response_id"
            ),
            ResponseSessionRejectionClass::Unsupported
        );
        assert_eq!(
            response_session_rejection_classification(
                400,
                "previous_response_id is only supported on Responses WebSocket v2"
            ),
            ResponseSessionRejectionClass::Unsupported
        );
        assert_eq!(
            response_session_rejection_classification(422, "invalid request"),
            ResponseSessionRejectionClass::TransientInvalid
        );
    }

    #[test]
    fn previous_response_unsupported_retry_body_can_expand_from_local_session() {
        let request = json!({
            "model": "gpt-test",
            "previous_response_id": "resp_client_supplied",
            "input": [
                {
                    "role": "user",
                    "content": "continue"
                }
            ]
        });
        let previous_input = json!([
            {
                "role": "system",
                "content": "stable instructions"
            },
            {
                "role": "user",
                "content": "original task"
            }
        ]);

        let compat = expand_previous_response_id_for_compat(
            &request,
            &previous_input,
            "client_previous_response_id_expanded_from_local_session",
        )
        .unwrap();

        assert_eq!(
            compat.reason,
            "client_previous_response_id_expanded_from_local_session"
        );
        assert!(compat.body.get("previous_response_id").is_none());
        assert_eq!(compat.body["input"].as_array().unwrap().len(), 3);
        assert_eq!(compat.body["input"][0], previous_input[0]);
        assert_eq!(compat.body["input"][2], request["input"][0]);
    }

    #[test]
    fn previous_response_unsupported_retry_body_removes_previous_response_id() {
        let request = json!({
            "model": "gpt-test",
            "previous_response_id": "resp_client_supplied",
            "input": [
                {
                    "role": "user",
                    "content": "continue"
                }
            ]
        });

        let compat = strip_previous_response_id_for_compat(
            &request,
            "client_previous_response_id_unsupported_retry",
        )
        .unwrap();

        assert_eq!(
            compat.reason,
            "client_previous_response_id_unsupported_retry"
        );
        assert!(compat.body.get("previous_response_id").is_none());
        assert_eq!(compat.body["input"], request["input"]);
        assert_eq!(
            request["previous_response_id"],
            json!("resp_client_supplied")
        );
    }

    #[tokio::test]
    async fn stale_response_session_rejection_does_not_disable_provider_delta() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-stale-id-no-provider-cooldown-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let key = "responses:bizd:gpt-5.5";

        note_response_session_error_cooldown_for_rejection(
            &state,
            Some(key),
            400,
            "provider_code=previous_response_not_found Previous response not found",
        )
        .await;
        assert!(
            !response_session_cooldown_active(&state, Some(key)).await,
            "a stale response id should clear that session only, not disable provider/model session-delta"
        );

        note_response_session_error_cooldown_for_rejection(
            &state,
            Some(key),
            400,
            "Unsupported parameter: previous_response_id",
        )
        .await;
        assert_eq!(
            response_session_cooldown_skip_reason(&state, Some(key))
                .await
                .as_deref(),
            Some("provider_session_delta_unsupported")
        );

        let websocket_only_key = "responses:bizd:gpt-5.5:websocket-only";
        note_response_session_error_cooldown_for_rejection(
            &state,
            Some(websocket_only_key),
            400,
            "previous_response_id is only supported on Responses WebSocket v2",
        )
        .await;
        assert_eq!(
            response_session_cooldown_skip_reason(&state, Some(websocket_only_key))
                .await
                .as_deref(),
            Some("provider_session_delta_unsupported")
        );
        fs::remove_dir_all(dir).ok();
    }
    #[tokio::test]
    async fn response_session_error_cooldown_is_scoped_to_responses_provider_model() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-cooldown-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let mut provider = ProviderConfig {
            id: "bizd".to_string(),
            name: "bizd".to_string(),
            base_url: "https://bizd.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let responses = RouteDecision {
            provider: provider.clone(),
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let key = response_session_error_cooldown_key(&responses);
        assert!(key.is_some());
        assert!(!response_session_cooldown_active(&state, key.as_deref()).await);
        note_response_session_error_cooldown(&state, key.as_deref()).await;
        assert!(response_session_cooldown_active(&state, key.as_deref()).await);
        assert_eq!(
            response_session_error_cooldown_secs(1, false),
            RESPONSE_SESSION_ERROR_COOLDOWN_FIRST_SECS
        );
        assert_eq!(
            response_session_error_cooldown_secs(2, false),
            RESPONSE_SESSION_ERROR_COOLDOWN_SECOND_SECS
        );
        assert_eq!(
            response_session_error_cooldown_secs(3, false),
            RESPONSE_SESSION_ERROR_COOLDOWN_LONG_SECS
        );
        assert_eq!(
            response_session_error_cooldown_secs(1, true),
            RESPONSE_SESSION_UNSUPPORTED_COOLDOWN_SECS
        );

        provider.channel = Channel::Chat;
        let chat = RouteDecision {
            provider,
            upstream_channel: Channel::Chat,
            model: "gpt-5.5".to_string(),
        };
        assert!(response_session_error_cooldown_key(&chat).is_none());
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn request_body_gzip_fallback_cooldown_skips_repeated_attempts() {
        let config = AppConfig::default();
        let dir =
            std::env::temp_dir().join(format!("atoapi-gzip-cooldown-{}", Uuid::new_v4().simple()));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let key = request_body_gzip_cooldown_key(
            "https://example.test/v1/responses",
            &Channel::Responses,
        );

        assert!(!request_body_gzip_cooldown_active(&state, &key).await);
        note_request_body_gzip_fallback(&state, &key).await;
        assert!(request_body_gzip_cooldown_active(&state, &key).await);
        let same_provider_key = request_body_gzip_cooldown_key(
            "https://EXAMPLE.test/v1/chat/completions",
            &Channel::Responses,
        );
        assert_eq!(key, same_provider_key);
        assert!(request_body_gzip_cooldown_active(&state, &same_provider_key).await);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn cold_large_responses_stream_keeps_opted_in_gzip() {
        let body = json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [{
                "role": "user",
                "content": "x".repeat(700_000)
            }]
        });

        assert!(
            !should_skip_request_body_gzip_for_cold_stream(&Channel::Responses, true, &body,),
            "an explicitly enabled gzip path must not raw-upload a cold large responses stream"
        );
    }

    #[tokio::test]
    async fn sync_main_single_attempt_can_send_gzip_without_extra_retry() {
        let config = AppConfig::default();
        let dir =
            std::env::temp_dir().join(format!("atoapi-sync-main-gzip-{}", Uuid::new_v4().simple()));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let gzip_hits = Arc::new(AtomicUsize::new(0));
        let hits_for_route = hits.clone();
        let gzip_hits_for_route = gzip_hits.clone();
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new().route(
            "/v1/responses",
            post(move |headers: HeaderMap, body: Bytes| {
                let hits = hits_for_route.clone();
                let gzip_hits = gzip_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    if headers
                        .get(header::CONTENT_ENCODING)
                        .and_then(|value| value.to_str().ok())
                        == Some("gzip")
                    {
                        gzip_hits.fetch_add(1, Ordering::SeqCst);
                    }
                    assert!(body.len() < 614_400);
                    (
                        StatusCode::OK,
                        Json(json!({
                            "id": "resp_sync_gzip",
                            "output": [],
                            "usage": { "input_tokens": 1, "output_tokens": 1 }
                        })),
                    )
                }
            }),
        );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let body = json!({
            "model": "gpt-5.5",
            "stream": false,
            "input": [{
                "role": "user",
                "content": "x".repeat(750_000)
            }]
        });
        let inbound_headers = HeaderMap::new();
        let outcome = send_main_upstream_request(
            &state,
            false,
            &format!("http://{upstream_addr}/v1/responses"),
            "upstream-key",
            &Channel::Responses,
            &body,
            &inbound_headers,
            None,
            true,
            true,
            true,
        )
        .await
        .unwrap();

        assert_eq!(outcome.response.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(gzip_hits.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.diagnostics.attempts, 1);
        assert!(outcome.diagnostics.gzip_attempted);
        assert!(!outcome.diagnostics.gzip_fallback_used);
        assert!(outcome.diagnostics.sent_body_bytes < outcome.diagnostics.request_body_bytes);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_gzip_compresses_medium_requests_without_extra_retry() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-medium-gzip-threshold-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let gzip_hits = Arc::new(AtomicUsize::new(0));
        let gzip_hits_for_route = gzip_hits.clone();
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new().route(
            "/v1/responses",
            post(move |headers: HeaderMap| {
                let gzip_hits = gzip_hits_for_route.clone();
                async move {
                    if headers
                        .get(header::CONTENT_ENCODING)
                        .and_then(|value| value.to_str().ok())
                        == Some("gzip")
                    {
                        gzip_hits.fetch_add(1, Ordering::SeqCst);
                    }
                    (
                        StatusCode::OK,
                        Json(json!({
                            "id": "resp_warm_gzip",
                            "output": [],
                            "usage": { "input_tokens": 1, "output_tokens": 1 }
                        })),
                    )
                }
            }),
        );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let url = format!("http://{upstream_addr}/v1/responses");
        let medium_body = json!({
            "model": "gpt-5.5",
            "stream": false,
            "input": [{
                "role": "user",
                "content": "x".repeat(320_000)
            }]
        });
        let inbound_headers = HeaderMap::new();

        let outcome = send_main_upstream_request(
            &state,
            false,
            &url,
            "upstream-key",
            &Channel::Responses,
            &medium_body,
            &inbound_headers,
            None,
            true,
            true,
            true,
        )
        .await
        .unwrap();
        assert_eq!(outcome.response.status(), StatusCode::OK);
        assert!(
            outcome.diagnostics.gzip_attempted,
            "an opted-in responses provider should compress medium request bodies immediately"
        );
        assert_eq!(outcome.diagnostics.attempts, 1);
        assert!(!outcome.diagnostics.gzip_fallback_used);
        assert_eq!(gzip_hits.load(Ordering::SeqCst), 1);

        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn expired_response_session_cooldown_keeps_failure_count() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-session-cooldown-expired-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let provider = ProviderConfig {
            id: "bizd".to_string(),
            name: "bizd".to_string(),
            base_url: "https://bizd.example/v1".to_string(),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: None,
            models: Vec::new(),
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let responses = RouteDecision {
            provider,
            upstream_channel: Channel::Responses,
            model: "gpt-5.5".to_string(),
        };
        let key = response_session_error_cooldown_key(&responses).unwrap();

        note_response_session_error_cooldown(&state, Some(&key)).await;
        {
            let mut cooldowns = state.response_session_error_cooldowns.lock().await;
            let cooldown = cooldowns.get_mut(&key).unwrap();
            cooldown.until = Instant::now() - std::time::Duration::from_secs(1);
            assert_eq!(cooldown.failures, 1);
        }

        assert!(!response_session_cooldown_active(&state, Some(&key)).await);
        note_response_session_error_cooldown(&state, Some(&key)).await;

        let cooldowns = state.response_session_error_cooldowns.lock().await;
        let cooldown = cooldowns.get(&key).unwrap();
        assert_eq!(cooldown.failures, 2);
        let remaining = cooldown.until.saturating_duration_since(Instant::now());
        assert!(remaining.as_secs() > RESPONSE_SESSION_ERROR_COOLDOWN_SECOND_SECS - 60);
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn responses_sync_main_respects_prefix_error_cooldown() {
        let config = AppConfig::default();
        let dir = std::env::temp_dir().join(format!(
            "atoapi-sync-prefix-cooldown-{}",
            Uuid::new_v4().simple()
        ));
        let cache = CacheStore::load(dir.join("cache.bin")).unwrap();
        let state = AppState::for_test(config, dir.join("config.toml"), cache).unwrap();
        let prefix_key = "prefix-sync-cooldown";

        assert!(
            !responses_sync_main_prefix_error_cooled_down(&state, true, Some(prefix_key)).await
        );
        note_prefix_error_cooldown(&state, Some(prefix_key)).await;
        assert!(responses_sync_main_prefix_error_cooled_down(&state, true, Some(prefix_key)).await);
        assert!(
            !responses_sync_main_prefix_error_cooled_down(&state, false, Some(prefix_key)).await,
            "streaming requests must not be blocked by the sync-main cooldown gate"
        );
        assert!(
            !responses_sync_main_prefix_error_cooled_down(&state, true, None).await,
            "missing prefix state key should not block sync requests"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn retry_delay_respects_rate_limit_backoff() {
        assert_eq!(
            upstream_retry_delay(reqwest::StatusCode::TOO_MANY_REQUESTS, None, 0),
            TokioDuration::from_secs(5)
        );
        assert_eq!(
            upstream_retry_delay(reqwest::StatusCode::TOO_MANY_REQUESTS, Some("9"), 0),
            TokioDuration::from_secs(9)
        );
        assert_eq!(
            upstream_retry_delay(reqwest::StatusCode::BAD_GATEWAY, None, 1),
            TokioDuration::from_millis(1200)
        );
        assert_eq!(
            max_attempts_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS),
            1
        );
        assert_eq!(
            max_attempts_for_status(reqwest::StatusCode::GATEWAY_TIMEOUT),
            2
        );
        assert_eq!(max_attempts_for_status(reqwest::StatusCode::BAD_GATEWAY), 3);
    }

    #[test]
    fn upstream_errors_are_scoped_by_cause() {
        assert_eq!(
            upstream_error_scope(429, "Upstream rate limit exceeded"),
            "upstream_rate_limit"
        );
        assert_eq!(
            upstream_error_scope(504, "<html>Gateway Time-out</html>"),
            "upstream_gateway_timeout"
        );
        assert_eq!(
            upstream_error_scope(413, "Payload Too Large"),
            "upstream_payload_too_large"
        );
        assert_eq!(
            upstream_error_scope(403, "insufficient balance, remaining balance: 0"),
            "upstream_quota"
        );
        assert_eq!(upstream_error_scope(401, "Invalid token"), "upstream_auth");
    }

    #[test]
    fn responses_compact_helpers_build_official_url_and_non_stream_body() {
        assert_eq!(
            responses_compact_url("https://api.example.com/v1", "resp_123"),
            "https://api.example.com/v1/responses/resp_123/compact"
        );
        assert_eq!(
            responses_compact_url("https://api.example.com/v1/responses", "resp_123"),
            "https://api.example.com/v1/responses/resp_123/compact"
        );
        assert!(should_fallback_compact_to_responses(404));
        assert!(should_fallback_compact_to_responses(405));
        assert!(should_fallback_compact_to_responses(501));
        assert!(should_fallback_compact_to_responses(400));
        assert!(should_fallback_responses_sync_main_to_chat_compat(
            401, true, false
        ));
        assert!(should_fallback_responses_sync_main_to_chat_compat(
            502, true, false
        ));
        assert!(should_fallback_responses_sync_main_to_chat_compat(
            503, true, false
        ));
        assert!(!should_fallback_responses_sync_main_to_chat_compat(
            401, true, true
        ));
        assert!(should_fallback_chat_compat_compact_to_responses(
            500, true, true
        ));
        assert!(should_fallback_chat_compat_compact_to_responses(
            524, true, true
        ));
        assert!(!should_fallback_chat_compat_compact_to_responses(
            500, false, true
        ));
        assert!(!should_fallback_chat_compat_compact_to_responses(
            500, true, false
        ));

        let body = json!({
            "model": "gpt-5.5",
            "response_id": "resp_123",
            "stream": false,
            "stream_options": { "include_usage": true },
            "input": "compact this"
        });
        let official = compact_request_body_for_official_endpoint(&body);
        assert!(official.get("response_id").is_none());
        assert!(official.get("stream").is_none());
        assert!(official.get("stream_options").is_none());
        assert_eq!(official["model"], "gpt-5.5");
        assert_eq!(official["input"], "compact this");
    }

    #[tokio::test]
    async fn responses_compact_falls_back_to_non_streaming_responses_when_upstream_lacks_endpoint()
    {
        let compact_hits = Arc::new(AtomicUsize::new(0));
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let captured_fallback_body = Arc::new(tokio::sync::Mutex::new(Value::Null));
        let compact_hits_for_route = compact_hits.clone();
        let fallback_hits_for_route = fallback_hits.clone();
        let captured_body_for_route = captured_fallback_body.clone();
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new()
            .route(
                "/v1/responses/:response_id/compact",
                post(move || {
                    let hits = compact_hits_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::NOT_FOUND,
                            Json(json!({
                                "error": { "message": "compact endpoint is not available" }
                            })),
                        )
                    }
                }),
            )
            .route(
                "/v1/chat/completions",
                post(move |Json(body): Json<Value>| {
                    let hits = fallback_hits_for_route.clone();
                    let captured = captured_body_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        *captured.lock().await = body;
                        Json(json!({
                            "id": "chatcmpl_compact_fallback",
                            "object": "chat.completion",
                            "model": "gpt-5.5",
                            "choices": [{
                                "index": 0,
                                "message": { "role": "assistant", "content": "compact summary" },
                                "finish_reason": "stop"
                            }],
                            "usage": {
                                "prompt_tokens": 2048,
                                "completion_tokens": 64,
                                "prompt_tokens_details": { "cached_tokens": 1536 }
                            }
                        }))
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let mut config = AppConfig::default();
        config.local_key = "local-test-key".to_string();
        config.workspace_fingerprint = "workspace-test".to_string();
        config.active_provider_id = Some("mock-responses".to_string());
        config.default_channel = Channel::Responses;
        config.cache.background_prewarm_enabled = true;
        config.providers = vec![ProviderConfig {
            id: "mock-responses".to_string(),
            name: "Mock Responses".to_string(),
            base_url: format!("http://{upstream_addr}/v1"),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: Some("upstream-key".to_string()),
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(300000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-compact-e2e-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                config,
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-test-key"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "model": "gpt-5.5",
                "stream": true,
                "input": [{ "role": "user", "content": "please compact context" }]
            }))
            .unwrap(),
        );

        let response =
            handle_responses_compact(state.clone(), headers, body, Some("resp_old".to_string()))
                .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(response_json["object"], "response");
        assert_eq!(response_json["output_text"], "compact summary");
        assert!(response_json["output"][0]["content"][0]["annotations"].is_null());
        assert_eq!(compact_hits.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 1);

        let captured = captured_fallback_body.lock().await.clone();
        assert_eq!(captured["stream"], true);
        assert_eq!(captured["model"], "gpt-5.5");
        assert!(captured.get("messages").is_some());
        assert!(captured.get("input").is_none());

        let metrics = state.metrics.snapshot().await;
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.upstream_requests, 2);
        assert_eq!(metrics.cache_misses, 0);
        assert_eq!(metrics.provider_input_tokens, 2048);
        assert_eq!(metrics.provider_cached_tokens, 1536);
        assert_eq!(metrics.recent_requests[0].upstream_attempt_total, Some(2));
        assert_eq!(
            metrics.recent_requests[0].upstream_call_source.as_deref(),
            Some("compact-fallback-chat-compat")
        );
        assert_eq!(
            metrics.recent_requests[0].upstream_call_kind.as_deref(),
            Some("sync")
        );
    }

    #[tokio::test]
    async fn codex_responses_provider_uses_native_responses_path() {
        let responses_hits = Arc::new(AtomicUsize::new(0));
        let chat_hits = Arc::new(AtomicUsize::new(0));
        let captured_responses_body = Arc::new(tokio::sync::Mutex::new(Value::Null));
        let captured_responses_accept = Arc::new(tokio::sync::Mutex::new(None::<String>));
        let captured_responses_encoding = Arc::new(tokio::sync::Mutex::new(None::<String>));
        let responses_hits_for_route = responses_hits.clone();
        let chat_hits_for_route = chat_hits.clone();
        let captured_body_for_route = captured_responses_body.clone();
        let captured_accept_for_route = captured_responses_accept.clone();
        let captured_encoding_for_route = captured_responses_encoding.clone();

        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new()
            .route(
                "/v1/responses",
                post(move |headers: HeaderMap, Json(body): Json<Value>| {
                    let hits = responses_hits_for_route.clone();
                    let captured = captured_body_for_route.clone();
                    let captured_accept = captured_accept_for_route.clone();
                    let captured_encoding = captured_encoding_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        let should_fail = body
                            .pointer("/metadata/atoapi_test_upstream_502")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        *captured.lock().await = body;
                        *captured_accept.lock().await = headers
                            .get(header::ACCEPT)
                            .and_then(|value| value.to_str().ok())
                            .map(ToOwned::to_owned);
                        *captured_encoding.lock().await = headers
                            .get(header::ACCEPT_ENCODING)
                            .and_then(|value| value.to_str().ok())
                            .map(ToOwned::to_owned);
                        if should_fail {
                            return raw_response(
                                502,
                                "text/html",
                                b"upstream edge returned 502".to_vec(),
                            );
                        }
                        raw_response(
                            200,
                            "text/event-stream",
                            concat!(
                                "event: response.output_text.delta\n",
                                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                                "event: response.completed\n",
                                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_native\",\"model\":\"gpt-5.5\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n\n"
                            )
                            .as_bytes()
                            .to_vec(),
                        )
                    }
                }),
            )
            .route(
                "/v1/chat/completions",
                post(move || {
                    let hits = chat_hits_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error":{"message":"chat should not be used"}})),
                        )
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let mut config = AppConfig::default();
        config.local_key = "local-test-key".to_string();
        config.workspace_fingerprint = "workspace-test".to_string();
        config.active_provider_id = Some("mock-responses".to_string());
        config.default_channel = Channel::Responses;
        config.providers = vec![ProviderConfig {
            id: "mock-responses".to_string(),
            name: "Mock Responses".to_string(),
            base_url: format!("http://{upstream_addr}/v1"),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Responses,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: Some("upstream-key".to_string()),
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(300000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        config.agent_injections = vec![AgentInjectionConfig {
            id: "codex".to_string(),
            label: "Codex".to_string(),
            kind: AgentInjectionKind::Codex,
            enabled: true,
            provider_id: Some("mock-responses".to_string()),
            model_id: Some("gpt-5.5".to_string()),
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        }];

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-codex-native-responses-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                config,
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-test-key"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "model": "gpt-5.5",
                "stream": false,
                "prompt_cache_key": "client-key-must-not-be-forwarded",
                "prompt_cache_retention": "24h",
                "input": [{ "type": "message", "role": "user", "content": "hi" }]
            }))
            .unwrap(),
        );

        let response = handle_generation_for_agent(
            state.clone(),
            headers.clone(),
            body,
            Channel::Responses,
            Some("codex"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_text = String::from_utf8(response_body.to_vec()).unwrap();
        assert!(response_text.contains("event: response.output_text.delta"));
        assert!(response_text.contains("event: response.completed"));

        assert_eq!(responses_hits.load(Ordering::SeqCst), 1);
        assert_eq!(chat_hits.load(Ordering::SeqCst), 0);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(responses_hits.load(Ordering::SeqCst), 1);
        assert_eq!(chat_hits.load(Ordering::SeqCst), 0);
        let captured = captured_responses_body.lock().await.clone();
        assert_eq!(captured["stream"], true);
        assert!(captured.get("input").is_some());
        let forwarded_cache_key = captured
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .unwrap();
        assert_ne!(forwarded_cache_key, "client-key-must-not-be-forwarded");
        assert!(provider_prompt_cache_key_is_valid(forwarded_cache_key));
        assert_eq!(
            captured
                .get("prompt_cache_retention")
                .and_then(Value::as_str),
            None
        );
        assert_eq!(
            captured_responses_accept.lock().await.as_deref(),
            Some("text/event-stream")
        );
        assert_eq!(
            captured_responses_encoding.lock().await.as_deref(),
            Some("identity")
        );

        let metrics = state.metrics.snapshot().await;
        assert_eq!(
            metrics.recent_requests[0].upstream_call_kind.as_deref(),
            Some("stream")
        );
        assert_eq!(metrics.recent_requests[0].client_channel, "responses");
        assert_eq!(metrics.recent_requests[0].upstream_channel, "responses");
        assert_eq!(
            metrics.recent_requests[0].upstream_network_path.as_deref(),
            Some("direct")
        );
        assert!(metrics.recent_requests[0].upstream_remote_addr.is_some());
        assert_eq!(
            metrics.recent_requests[0]
                .upstream_pool_diagnostic
                .as_deref(),
            Some("shared-client:pool-enabled-hit-not-exposed")
        );

        let failed_body = Bytes::from(
            serde_json::to_vec(&json!({
                "model": "gpt-5.5",
                "stream": false,
                "metadata": { "atoapi_test_upstream_502": true },
                "input": [{ "type": "message", "role": "user", "content": "fail" }]
            }))
            .unwrap(),
        );
        let failed_response = handle_generation_for_agent(
            state.clone(),
            headers,
            failed_body,
            Channel::Responses,
            Some("codex"),
        )
        .await;
        assert_eq!(failed_response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(responses_hits.load(Ordering::SeqCst), 2);
        let failed_capture = captured_responses_body.lock().await.clone();
        assert_eq!(failed_capture["stream"], true);

        let failed_metrics = state.metrics.snapshot().await;
        assert_eq!(failed_metrics.recent_requests[0].status, 502);
        assert_eq!(
            failed_metrics.recent_requests[0]
                .upstream_call_kind
                .as_deref(),
            Some("stream")
        );
        assert_eq!(
            failed_metrics.recent_requests[0]
                .upstream_pool_diagnostic
                .as_deref(),
            Some("shared-client:pool-enabled-hit-not-exposed")
        );

        fs::remove_dir_all(config_dir).ok();
    }

    #[tokio::test]
    async fn codex_responses_chat_provider_uses_chat_compat_stream() {
        let responses_hits = Arc::new(AtomicUsize::new(0));
        let chat_hits = Arc::new(AtomicUsize::new(0));
        let captured_chat_body = Arc::new(tokio::sync::Mutex::new(Value::Null));
        let responses_hits_for_route = responses_hits.clone();
        let chat_hits_for_route = chat_hits.clone();
        let captured_body_for_route = captured_chat_body.clone();

        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new()
            .route(
                "/v1/responses",
                post(move || {
                    let hits = responses_hits_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error":{"message":"responses should not be used"}})),
                        )
                    }
                }),
            )
            .route(
                "/v1/chat/completions",
                post(move |Json(body): Json<Value>| {
                    let hits = chat_hits_for_route.clone();
                    let captured = captured_body_for_route.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        *captured.lock().await = body;
                        raw_response(
                            200,
                            "text/event-stream",
                            concat!(
                                "data: {\"id\":\"chatcmpl_codex\",\"created\":123,\"model\":\"gpt-5.5\",\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n",
                                "data: {\"id\":\"chatcmpl_codex\",\"created\":123,\"model\":\"gpt-5.5\",\"choices\":[{\"delta\":{\"content\":\"llo\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1,\"total_tokens\":11,\"prompt_tokens_details\":{\"cached_tokens\":8}}}\n\n",
                                "data: [DONE]\n\n"
                            )
                            .as_bytes()
                            .to_vec(),
                        )
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let mut config = AppConfig::default();
        config.local_key = "local-test-key".to_string();
        config.workspace_fingerprint = "workspace-test".to_string();
        config.active_provider_id = Some("mock-chat".to_string());
        config.default_channel = Channel::Responses;
        config.providers = vec![ProviderConfig {
            id: "mock-chat".to_string(),
            name: "Mock Chat".to_string(),
            base_url: format!("http://{upstream_addr}/v1"),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Chat,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: Some("upstream-key".to_string()),
            models: vec![crate::config::ModelConfig {
                id: "gpt-5.5".to_string(),
                request_model_id: None,
                display_name: "gpt-5.5".to_string(),
                context_window: Some(300000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        config.agent_injections = vec![AgentInjectionConfig {
            id: "codex".to_string(),
            label: "Codex".to_string(),
            kind: AgentInjectionKind::Codex,
            enabled: true,
            provider_id: Some("mock-chat".to_string()),
            model_id: Some("gpt-5.5".to_string()),
            target_path: None,
            last_injected_at: None,
            last_status: None,
            local_key: None,
            hidden_provider_ids: Vec::new(),
        }];

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-codex-chat-compat-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                config,
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-test-key"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "model": "gpt-5.5",
                "input": [{ "type": "message", "role": "user", "content": "hi" }]
            }))
            .unwrap(),
        );

        let response = handle_generation_for_agent(
            state.clone(),
            headers,
            body,
            Channel::Responses,
            Some("codex"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_text = String::from_utf8(response_body.to_vec()).unwrap();
        assert!(response_text.contains("event: response.output_text.delta"));
        assert!(response_text.contains("event: response.completed"));
        assert!(response_text.contains("\"text\":\"hello\""));

        assert_eq!(responses_hits.load(Ordering::SeqCst), 0);
        assert_eq!(chat_hits.load(Ordering::SeqCst), 1);
        let captured = captured_chat_body.lock().await.clone();
        assert_eq!(captured["stream"], true);
        assert_eq!(captured["stream_options"]["include_usage"], true);
        assert!(captured.get("messages").is_some());
        assert!(captured.get("input").is_none());

        let metrics = state.metrics.snapshot().await;
        assert_eq!(
            metrics.recent_requests[0].upstream_call_kind.as_deref(),
            Some("stream")
        );
        assert_eq!(metrics.recent_requests[0].client_channel, "responses");
        assert_eq!(metrics.recent_requests[0].upstream_channel, "chat");

        fs::remove_dir_all(config_dir).ok();
    }

    fn reasoning_test_decision(configured: Option<&str>, supported: &[&str]) -> RouteDecision {
        RouteDecision {
            provider: ProviderConfig {
                id: "reasoning-provider".to_string(),
                name: "Reasoning Provider".to_string(),
                base_url: "https://reasoning.example/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: true,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                api_key_encrypted: None,
                models: vec![crate::config::ModelConfig {
                    id: "reasoning-model".to_string(),
                    request_model_id: None,
                    display_name: "Reasoning Model".to_string(),
                    context_window: Some(128_000),
                    output_window: None,
                    reasoning_effort_override_enabled: configured.is_some(),
                    reasoning_effort: configured.map(ToOwned::to_owned),
                    supported_reasoning_efforts: supported
                        .iter()
                        .map(|effort| (*effort).to_string())
                        .collect(),
                    supports_tools: true,
                    supports_streaming: true,
                    enabled: true,
                }],
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "reasoning-model".to_string(),
        }
    }

    #[test]
    fn reasoning_override_off_preserves_agent_effort_across_channel_conversion() {
        let client = json!({
            "model": "reasoning-model",
            "reasoning": { "effort": "xhigh" },
            "input": "hello"
        });
        let mut upstream = responses_to_chat_request(&client);
        let decision = reasoning_test_decision(None, &[]);

        let diagnostics =
            apply_model_reasoning_effort(&client, &mut upstream, &Channel::Chat, &decision);

        assert_eq!(upstream["reasoning_effort"], "xhigh");
        assert_eq!(diagnostics.agent.as_deref(), Some("xhigh"));
        assert_eq!(diagnostics.configured, None);
        assert_eq!(diagnostics.effective.as_deref(), Some("xhigh"));
        assert_eq!(diagnostics.source.as_deref(), Some("agent"));
    }

    #[test]
    fn reasoning_override_forces_configured_effort_and_changes_cache_material() {
        let client = json!({
            "model": "reasoning-model",
            "reasoning": { "effort": "low" },
            "input": "hello"
        });
        let low_decision = reasoning_test_decision(Some("low"), &[]);
        let high_decision = reasoning_test_decision(Some("high"), &[]);
        let mut low_body = client.clone();
        let mut high_body = client.clone();

        apply_model_reasoning_effort(&client, &mut low_body, &Channel::Responses, &low_decision);
        let diagnostics = apply_model_reasoning_effort(
            &client,
            &mut high_body,
            &Channel::Responses,
            &high_decision,
        );
        let low_key = cache::cache_key(
            &json!({ "request": low_body }),
            "reasoning-provider",
            "reasoning-model",
            "workspace",
        );
        let high_key = cache::cache_key(
            &json!({ "request": high_body }),
            "reasoning-provider",
            "reasoning-model",
            "workspace",
        );

        assert_eq!(high_body.pointer("/reasoning/effort"), Some(&json!("high")));
        assert_eq!(diagnostics.agent.as_deref(), Some("low"));
        assert_eq!(diagnostics.configured.as_deref(), Some("high"));
        assert_eq!(diagnostics.effective.as_deref(), Some("high"));
        assert_eq!(diagnostics.source.as_deref(), Some("model_override"));
        assert_ne!(low_key, high_key);
    }

    #[test]
    fn ultra_downgrades_to_max_only_when_capability_proves_it() {
        let client = json!({ "reasoning": { "effort": "low" } });
        let mut unknown_body = client.clone();
        let unknown_decision = reasoning_test_decision(Some("ultra"), &[]);
        let unknown = apply_model_reasoning_effort(
            &client,
            &mut unknown_body,
            &Channel::Responses,
            &unknown_decision,
        );
        assert_eq!(unknown.effective.as_deref(), Some("ultra"));

        let mut proven_body = client.clone();
        let proven_decision = reasoning_test_decision(Some("ultra"), &["max"]);
        let proven = apply_model_reasoning_effort(
            &client,
            &mut proven_body,
            &Channel::Responses,
            &proven_decision,
        );
        assert_eq!(
            proven_body.pointer("/reasoning/effort"),
            Some(&json!("max"))
        );
        assert_eq!(proven.effective.as_deref(), Some("max"));
        assert_eq!(
            proven.source.as_deref(),
            Some("model_override_ultra_to_max")
        );
    }

    #[tokio::test]
    async fn passive_warm_miss_then_local_replay_does_not_hit_upstream_again() {
        let upstream_hits = Arc::new(AtomicUsize::new(0));
        let upstream_hits_for_route = upstream_hits.clone();
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream_app = Router::new().route(
            "/v1/chat/completions",
            post(move |Json(_body): Json<Value>| {
                let hits = upstream_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    Json(json!({
                        "id": "chatcmpl_warm",
                        "object": "chat.completion",
                        "model": "warm-model",
                        "choices": [{
                            "index": 0,
                            "message": { "role": "assistant", "content": "warm response" },
                            "finish_reason": "stop"
                        }],
                        "usage": { "prompt_tokens": 10, "completion_tokens": 2 }
                    }))
                }
            }),
        );
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app).await.unwrap();
        });

        let mut config = AppConfig::default();
        config.local_key = "local-test-key".to_string();
        config.workspace_fingerprint = "workspace-test".to_string();
        config.active_provider_id = Some("mock-openai".to_string());
        config.default_channel = Channel::Chat;
        config.providers = vec![ProviderConfig {
            id: "mock-openai".to_string(),
            name: "Mock OpenAI".to_string(),
            base_url: format!("http://{upstream_addr}/v1"),
            models_url: None,
            is_full_url: false,
            custom_user_agent: None,
            channel: Channel::Chat,
            prompt_cache_retention_enabled: false,
            request_body_gzip_enabled: false,
            use_system_proxy: false,
            api_key_encrypted: Some("upstream-key".to_string()),
            models: vec![crate::config::ModelConfig {
                id: "warm-model".to_string(),
                request_model_id: None,
                display_name: "warm-model".to_string(),
                context_window: Some(128000),
                output_window: None,
                reasoning_effort_override_enabled: false,
                reasoning_effort: None,
                supported_reasoning_efforts: Vec::new(),
                supports_tools: true,
                supports_streaming: true,
                enabled: true,
            }],
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];

        let config_dir = std::env::temp_dir().join(format!(
            "atoapi-e2e-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let state = Arc::new(
            AppState::for_test(
                config,
                config_dir.join("config.toml"),
                CacheStore::load(cache_path(&config_dir)).unwrap(),
            )
            .unwrap(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-test-key"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&json!({
                "model": "warm-model",
                "temperature": 0,
                "messages": [{ "role": "user", "content": "cache this stable request" }]
            }))
            .unwrap(),
        );

        let first =
            handle_generation(state.clone(), headers.clone(), body.clone(), Channel::Chat).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);

        let second = handle_generation(state.clone(), headers, body, Channel::Chat).await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);

        let metrics = state.metrics.snapshot().await;
        assert_eq!(metrics.upstream_requests, 1);
        assert_eq!(metrics.response_cache_hits, 1);
        assert!(metrics.eligible_cache_hit_rate >= 0.5);
    }
}
