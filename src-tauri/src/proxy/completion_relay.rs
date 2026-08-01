use super::*;

pub(super) const STREAM_RELAY_CHANNEL_CAPACITY: usize = 32;
pub(super) const STREAM_RELAY_BYTE_BUDGET: usize = 2 * 1024 * 1024;
pub(super) const STREAM_CACHE_CAPTURE_BYTE_LIMIT: usize = STREAM_RELAY_BYTE_BUDGET;

struct RelayBodyChunk {
    bytes: Bytes,
    permit: OwnedSemaphorePermit,
}

pub(super) struct BoundedCacheCapture {
    body: Vec<u8>,
    complete: bool,
}

impl BoundedCacheCapture {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            body: Vec::new(),
            complete: enabled,
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        if !self.complete {
            return;
        }
        if self.body.len().saturating_add(bytes.len()) > STREAM_CACHE_CAPTURE_BYTE_LIMIT {
            self.body = Vec::new();
            self.complete = false;
            return;
        }
        self.body.extend_from_slice(bytes);
    }

    pub(super) fn finish(self) -> Option<Vec<u8>> {
        self.complete.then_some(self.body)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_responses_failure_frame,
        completed_native_full_replay_cache_control_acceptance_allowed,
        GenericResponsesErrorFrameGate, GenericResponsesErrorFrameGateEvent, TerminalFailure,
    };
    use crate::{config::Channel, continuation_lineage::LineageParent};

    #[test]
    fn passive_cache_control_acceptance_requires_a_completed_unambiguous_full_replay() {
        assert!(
            completed_native_full_replay_cache_control_acceptance_allowed(
                true,
                true,
                &Channel::Responses,
                &Channel::Responses,
                false,
                false,
                false,
                &LineageParent::FullReplay,
                false,
            )
        );

        // A WAF/SSE failure can retain HTTP 200, but it is not acceptance.
        assert!(
            !completed_native_full_replay_cache_control_acceptance_allowed(
                false,
                true,
                &Channel::Responses,
                &Channel::Responses,
                false,
                false,
                false,
                &LineageParent::FullReplay,
                false,
            )
        );
        assert!(
            !completed_native_full_replay_cache_control_acceptance_allowed(
                true,
                true,
                &Channel::Responses,
                &Channel::Responses,
                false,
                false,
                false,
                &LineageParent::ExternalContinuation,
                false,
            )
        );
        assert!(
            !completed_native_full_replay_cache_control_acceptance_allowed(
                true,
                true,
                &Channel::Responses,
                &Channel::Responses,
                false,
                false,
                false,
                &LineageParent::FullReplay,
                true,
            )
        );
    }

    #[test]
    fn canonical_failure_frame_keeps_the_upstream_summary_and_native_shape() {
        let frame = canonical_responses_failure_frame(
            "gpt-test",
            Some("resp_upstream"),
            TerminalFailure::ErrorEvent,
            Some("provider overloaded"),
            "local-trace",
            Some("x-request-id"),
            Some("upstream-trace"),
        );
        let frame = std::str::from_utf8(&frame).expect("failure frame must be UTF-8");
        assert!(frame.starts_with("event: response.failed\n"));
        assert!(frame.contains("\"type\":\"response.failed\""));
        assert!(frame.contains("\"id\":\"resp_upstream\""));
        assert!(frame.contains("\"status\":\"failed\""));
        assert!(frame.contains("\"code\":\"upstream_sse_error\""));
        assert!(frame.contains("provider overloaded"));
        assert!(frame.contains("atoapi_trace_id=local-trace"));
        assert!(frame.contains("upstream_trace=x-request-id:upstream-trace"));
    }

    #[test]
    fn generic_error_gate_holds_only_a_generic_error_event() {
        let mut gate = GenericResponsesErrorFrameGate::default();
        let first = gate.push(b"event: error\ndata: {\"type\":");
        assert!(first.is_empty());
        let second = gate.push(b"\"error\",\"error\":{\"message\":\"busy\"}}\n\n");
        assert_eq!(second.len(), 1);
        assert!(matches!(
            second.first(),
            Some(GenericResponsesErrorFrameGateEvent::ErrorFrame(frame))
                if frame.starts_with(b"event: error")
        ));

        let ordinary = gate.push(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
        );
        assert_eq!(ordinary.len(), 1);
        assert!(matches!(
            ordinary.first(),
            Some(GenericResponsesErrorFrameGateEvent::Passthrough(frame))
                if frame.starts_with(b"event: response.output_text.delta")
        ));

        let mut data_only_gate = GenericResponsesErrorFrameGate::default();
        let data_only =
            data_only_gate.push(b"data: {\"type\":\"error\",\"error\":{\"message\":\"busy\"}}\n\n");
        assert!(matches!(
            data_only.first(),
            Some(GenericResponsesErrorFrameGateEvent::ErrorFrame(frame))
                if frame.starts_with(b"data: {\"type\":\"error\"")
        ));
    }
}

pub(super) fn relay_chunk_parts(chunk: &Bytes) -> impl Iterator<Item = Bytes> + '_ {
    (0..chunk.len())
        .step_by(STREAM_RELAY_BYTE_BUDGET)
        .map(move |start| {
            let end = start
                .saturating_add(STREAM_RELAY_BYTE_BUDGET)
                .min(chunk.len());
            chunk.slice(start..end)
        })
}

const GENERIC_RESPONSES_ERROR_FRAME_LIMIT: usize = 64 * 1024;

/// Retains only a candidate generic `event: error` frame. Normal Responses
/// bytes (including comments, SSE ids, and original chunk grouping) remain on
/// the byte-for-byte passthrough path.
#[derive(Default)]
struct GenericResponsesErrorFrameGate {
    mode: GenericResponsesErrorFrameGateMode,
    frame: Vec<u8>,
    line: Vec<u8>,
    line_has_bytes: bool,
    pending_cr: bool,
}

#[derive(Default, PartialEq, Eq)]
enum GenericResponsesErrorFrameGateMode {
    #[default]
    Undecided,
    Passthrough,
    Candidate,
}

enum GenericResponsesErrorFrameGateEvent {
    Passthrough(Bytes),
    ErrorFrame(Bytes),
}

enum RelayStreamChunk {
    Raw(Bytes),
    GenericErrorFrame(Bytes),
}

impl GenericResponsesErrorFrameGate {
    fn push(&mut self, chunk: &[u8]) -> Vec<GenericResponsesErrorFrameGateEvent> {
        let mut events = Vec::new();
        let mut passthrough = Vec::new();
        for &byte in chunk {
            self.push_byte(byte, &mut passthrough, &mut events);
        }
        if !passthrough.is_empty() {
            events.push(GenericResponsesErrorFrameGateEvent::Passthrough(
                Bytes::from(passthrough),
            ));
        }
        events
    }

    fn push_byte(
        &mut self,
        byte: u8,
        passthrough: &mut Vec<u8>,
        events: &mut Vec<GenericResponsesErrorFrameGateEvent>,
    ) {
        if self.pending_cr {
            self.pending_cr = false;
            if byte == b'\n' {
                self.append_byte(byte, passthrough);
                self.finish_line(passthrough, events);
                return;
            }
            self.finish_line(passthrough, events);
        }

        self.append_byte(byte, passthrough);
        match byte {
            b'\r' => self.pending_cr = true,
            b'\n' => self.finish_line(passthrough, events),
            _ => {
                self.line_has_bytes = true;
                self.line.push(byte);
            }
        }
    }

    fn append_byte(&mut self, byte: u8, passthrough: &mut Vec<u8>) {
        match self.mode {
            GenericResponsesErrorFrameGateMode::Passthrough => passthrough.push(byte),
            GenericResponsesErrorFrameGateMode::Undecided
            | GenericResponsesErrorFrameGateMode::Candidate => {
                self.frame.push(byte);
                if self.mode == GenericResponsesErrorFrameGateMode::Candidate
                    && self.frame.len() > GENERIC_RESPONSES_ERROR_FRAME_LIMIT
                {
                    passthrough.extend(std::mem::take(&mut self.frame));
                    self.mode = GenericResponsesErrorFrameGateMode::Passthrough;
                }
            }
        }
    }

    fn finish_line(
        &mut self,
        passthrough: &mut Vec<u8>,
        events: &mut Vec<GenericResponsesErrorFrameGateEvent>,
    ) {
        let line_has_bytes = std::mem::replace(&mut self.line_has_bytes, false);
        let line = std::mem::take(&mut self.line);
        match self.mode {
            GenericResponsesErrorFrameGateMode::Undecided => {
                if !line_has_bytes {
                    passthrough.extend(std::mem::take(&mut self.frame));
                    self.reset_frame();
                    return;
                }
                let line = String::from_utf8_lossy(&line);
                let (field, value) = line.split_once(':').unwrap_or((&line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value).trim();
                if field == "event" {
                    if is_generic_responses_error_event(value) {
                        self.mode = GenericResponsesErrorFrameGateMode::Candidate;
                    } else {
                        passthrough.extend(std::mem::take(&mut self.frame));
                        self.mode = GenericResponsesErrorFrameGateMode::Passthrough;
                    }
                } else if field == "data" {
                    if is_generic_responses_error_data(value) {
                        self.mode = GenericResponsesErrorFrameGateMode::Candidate;
                    } else {
                        passthrough.extend(std::mem::take(&mut self.frame));
                        self.mode = GenericResponsesErrorFrameGateMode::Passthrough;
                    }
                }
            }
            GenericResponsesErrorFrameGateMode::Candidate if !line_has_bytes => {
                if !passthrough.is_empty() {
                    events.push(GenericResponsesErrorFrameGateEvent::Passthrough(
                        Bytes::from(std::mem::take(passthrough)),
                    ));
                }
                let frame = Bytes::from(std::mem::take(&mut self.frame));
                self.reset_frame();
                events.push(GenericResponsesErrorFrameGateEvent::ErrorFrame(frame));
            }
            GenericResponsesErrorFrameGateMode::Candidate => {}
            GenericResponsesErrorFrameGateMode::Passthrough if !line_has_bytes => {
                self.reset_frame();
            }
            GenericResponsesErrorFrameGateMode::Passthrough => {}
        }
    }

    fn reset_frame(&mut self) {
        self.mode = GenericResponsesErrorFrameGateMode::Undecided;
        self.frame.clear();
        self.line.clear();
        self.line_has_bytes = false;
        self.pending_cr = false;
    }
}

fn is_generic_responses_error_event(event: &str) -> bool {
    let event = event.trim();
    event == "error" || event == "message_delta_error" || event.ends_with(".error")
}

/// A number of proxy implementations omit the SSE `event:` field and put the
/// generic error type only in a normal `data:` line. Treat that as the same
/// compatibility failure; otherwise the client may act on the raw error and
/// close before the relay can publish its native `response.failed` at EOF.
fn is_generic_responses_error_data(data: &str) -> bool {
    is_plain_upstream_request_blocked(data)
        || serde_json::from_str::<serde_json::Value>(data)
            .ok()
            .and_then(|value| {
                value
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .map(is_generic_responses_error_event)
            })
            .unwrap_or(false)
}

/// Some third-party Responses relays emit a bare SSE data line such as
/// `data: Request blocked` after a successful HTTP status.  That is a
/// terminal upstream error, not ordinary model text.  Keep the match narrow
/// so normal provider prose remains on the byte-for-byte passthrough path.
fn is_plain_upstream_request_blocked(data: &str) -> bool {
    let normalized = data.trim().to_ascii_lowercase();
    [
        "request blocked",
        "request has been blocked",
        "blocked by waf",
        "blocked by cloudflare",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn generic_responses_error_frame_summary(
    frame: &[u8],
    client_prompt_cache_key_for_redaction: Option<&str>,
) -> String {
    let mut decoder = sse::SseFrameDecoder::default();
    for event in decoder.push_ordered(frame) {
        if let sse::SseDecodeEvent::Frame(frame) = event {
            return upstream_error_summary_redacting(
                frame.data.as_bytes(),
                client_prompt_cache_key_for_redaction,
            );
        }
    }
    upstream_error_summary_redacting(frame, client_prompt_cache_key_for_redaction)
}

fn terminal_failure_message(failure: TerminalFailure, upstream_summary: Option<&str>) -> String {
    let fallback = match failure {
        TerminalFailure::ErrorEvent => "upstream returned an SSE error before completion",
        TerminalFailure::FrameTooLarge => "upstream SSE frame exceeded the inspection limit",
        TerminalFailure::IncompleteEof => "upstream stream ended before a completion event",
        TerminalFailure::TransportErrorBeforeTerminal => {
            "upstream stream connection ended before completion"
        }
    };
    upstream_summary
        .map(str::trim)
        .filter(|summary| !summary.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn failure_message_with_trace(
    message: impl Into<String>,
    request_id: &str,
    upstream_trace_source: Option<&str>,
    upstream_trace_id: Option<&str>,
) -> String {
    let mut message = message.into();
    let mut trace = Vec::new();
    if let Some(request_id) = trace_component(request_id) {
        trace.push(format!("atoapi_trace_id={request_id}"));
    }
    if let (Some(source), Some(trace_id)) = (
        upstream_trace_source.and_then(trace_component),
        upstream_trace_id.and_then(trace_component),
    ) {
        trace.push(format!("upstream_trace={source}:{trace_id}"));
    }
    if !trace.is_empty() {
        message.push_str(" [trace: ");
        message.push_str(&trace.join(", "));
        message.push(']');
    }
    message
}

fn terminal_failure_message_with_trace(
    failure: TerminalFailure,
    upstream_summary: Option<&str>,
    request_id: &str,
    upstream_trace_source: Option<&str>,
    upstream_trace_id: Option<&str>,
) -> String {
    failure_message_with_trace(
        terminal_failure_message(failure, upstream_summary),
        request_id,
        upstream_trace_source,
        upstream_trace_id,
    )
}

fn trace_component(value: &str) -> Option<String> {
    let value = value.trim();
    let value = value
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect::<String>();
    (!value.is_empty()).then_some(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponsesFailureCode {
    UpstreamSseError,
    UpstreamSseFrameTooLarge,
    UpstreamIncompleteEof,
    UpstreamStreamError,
    UpstreamRequestBlocked,
    UpstreamWafBlocked,
    ContextPayloadUnsafe,
}

impl ResponsesFailureCode {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::UpstreamSseError => "upstream_sse_error",
            Self::UpstreamSseFrameTooLarge => "upstream_sse_frame_too_large",
            Self::UpstreamIncompleteEof => "upstream_incomplete_eof",
            Self::UpstreamStreamError => "upstream_stream_error",
            Self::UpstreamRequestBlocked => "upstream_request_blocked",
            Self::UpstreamWafBlocked => "upstream_waf_blocked",
            Self::ContextPayloadUnsafe => "compaction_required",
        }
    }

    pub(super) const fn error_type(self) -> &'static str {
        match self {
            Self::ContextPayloadUnsafe => "context_payload_unsafe",
            _ => self.code(),
        }
    }

    const fn is_upstream_blocked(self) -> bool {
        matches!(
            self,
            Self::UpstreamRequestBlocked | Self::UpstreamWafBlocked
        )
    }
}

fn response_failure_code_for_terminal(
    failure: TerminalFailure,
    upstream_summary: Option<&str>,
    upstream_trace_source: Option<&str>,
) -> ResponsesFailureCode {
    if matches!(failure, TerminalFailure::ErrorEvent)
        && upstream_summary.is_some_and(is_plain_upstream_request_blocked)
    {
        return if upstream_trace_source.is_some_and(|source| source.eq_ignore_ascii_case("cf-ray"))
        {
            ResponsesFailureCode::UpstreamWafBlocked
        } else {
            ResponsesFailureCode::UpstreamRequestBlocked
        };
    }
    match failure {
        TerminalFailure::ErrorEvent => ResponsesFailureCode::UpstreamSseError,
        TerminalFailure::FrameTooLarge => ResponsesFailureCode::UpstreamSseFrameTooLarge,
        TerminalFailure::IncompleteEof => ResponsesFailureCode::UpstreamIncompleteEof,
        TerminalFailure::TransportErrorBeforeTerminal => ResponsesFailureCode::UpstreamStreamError,
    }
}

pub(super) fn canonical_responses_failure_payload(
    model: &str,
    response_id: Option<&str>,
    failure_code: ResponsesFailureCode,
    message: impl Into<String>,
    request_id: &str,
    upstream_trace_source: Option<&str>,
    upstream_trace_id: Option<&str>,
) -> serde_json::Value {
    let response_id = response_id
        .filter(|id| !id.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("resp_atoapi_{}", Uuid::new_v4().simple()));
    serde_json::json!({
        "type": "response.failed",
        "response": {
            "id": response_id,
            "object": "response",
            "created_at": Utc::now().timestamp(),
            "status": "failed",
            "model": model,
            "output": [],
            "error": {
                "type": failure_code.error_type(),
                "code": failure_code.code(),
                "message": failure_message_with_trace(
                    message,
                    request_id,
                    upstream_trace_source,
                    upstream_trace_id,
                ),
            },
        },
    })
}

fn canonical_responses_failure_frame(
    model: &str,
    response_id: Option<&str>,
    failure: TerminalFailure,
    upstream_summary: Option<&str>,
    request_id: &str,
    upstream_trace_source: Option<&str>,
    upstream_trace_id: Option<&str>,
) -> Bytes {
    let payload = canonical_responses_failure_payload(
        model,
        response_id,
        response_failure_code_for_terminal(failure, upstream_summary, upstream_trace_source),
        terminal_failure_message(failure, upstream_summary),
        request_id,
        upstream_trace_source,
        upstream_trace_id,
    );
    let payload = serde_json::to_string(&payload).unwrap_or_else(|_| {
        "{\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}".to_string()
    });
    Bytes::from(format!("event: response.failed\ndata: {payload}\n\n"))
}

fn completed_native_full_replay_cache_control_acceptance_allowed(
    stream_success_for_cache: bool,
    agent_generation: bool,
    client_channel: &Channel,
    upstream_channel: &Channel,
    confirmed_compaction: bool,
    response_session_starts_compaction_epoch: bool,
    used_response_session: bool,
    response_session_parent: &LineageParent,
    suppress_local_full_replay_settlement: bool,
) -> bool {
    stream_success_for_cache
        && agent_generation
        && matches!(client_channel, Channel::Responses)
        && matches!(upstream_channel, Channel::Responses)
        && !confirmed_compaction
        && !response_session_starts_compaction_epoch
        && !used_response_session
        && matches!(response_session_parent, LineageParent::FullReplay)
        && !suppress_local_full_replay_settlement
}

pub(super) async fn stream_upstream(
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
    // Legacy relay shape only. This value is inert and must never influence
    // Provider, model, channel, or Key selection.
    _route_affinity_key: Option<String>,
    config: AppConfig,
    _prefix_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    prefix_state_key: Option<String>,
    response_session_key: Option<String>,
    mut full_response_input: Option<Value>,
    response_session_lease: Option<LineageLease>,
    response_session_parent: LineageParent,
    response_session_starts_compaction_epoch: bool,
    used_response_session: bool,
    retried_full_response: bool,
    diagnostics: BodyDiagnostics,
    tail_input_diagnostics: TailInputDiagnostics,
    session_anchor_diagnostics: SessionAnchorDiagnostics,
    response_session_reuse_diagnostics: ResponseSessionReuseDiagnostics,
    cross_protocol_tool_context: Option<transform_codex_chat::CodexToolContext>,
    agent_log_id: Option<String>,
    agent_log_label: Option<String>,
    requested_model: Option<String>,
    prefix_guard_wait: PrefixGuardWaitDiagnostics,
    local_prepare_ms: u64,
    mut upstream_request_diagnostics: UpstreamRequestDiagnostics,
    mut final_scope_dispatch: Option<FinalScopeDispatchGuard>,
    upstream_response_headers_at_ms: u64,
    agent_attempt_id: Option<String>,
    mut shadow_affinity_decision: Option<ShadowAffinityDecision>,
    agent_generation: bool,
    response_handoff: Option<cache_directed_relay::DispatchHandoff<Response>>,
    client_prompt_cache_key_for_redaction: Option<String>,
    full_replay_risk_scope: Option<ResponsesRouteScope>,
) -> Response {
    // Streaming must behave like a normal proxy: do not hold prefix/session locks
    // for the whole SSE response. The guarded section has already covered request
    // preparation and send; holding it through output serializes unrelated turns
    // and inflates TTFT/total time.
    drop(_prefix_guard);

    let convert_codex_chat_sse_to_responses_sse = matches!(client_channel, Channel::Responses)
        && matches!(decision.upstream_channel, Channel::Chat);
    let convert_anthropic_sse_to_responses_sse = matches!(client_channel, Channel::Responses)
        && matches!(decision.upstream_channel, Channel::Anthropic);
    let convert_anthropic_sse_to_chat_sse = matches!(client_channel, Channel::Chat)
        && matches!(decision.upstream_channel, Channel::Anthropic);
    let convert_chat_sse_to_anthropic_sse = matches!(client_channel, Channel::Anthropic)
        && matches!(decision.upstream_channel, Channel::Chat);
    let convert_responses_sse_to_chat_sse = matches!(client_channel, Channel::Chat)
        && matches!(decision.upstream_channel, Channel::Responses);
    let convert_responses_sse_to_anthropic_sse = matches!(client_channel, Channel::Anthropic)
        && matches!(decision.upstream_channel, Channel::Responses);
    let response_content_type = if convert_codex_chat_sse_to_responses_sse
        || convert_anthropic_sse_to_responses_sse
        || convert_anthropic_sse_to_chat_sse
        || convert_chat_sse_to_anthropic_sse
        || convert_responses_sse_to_chat_sse
        || convert_responses_sse_to_anthropic_sse
    {
        "text/event-stream".to_string()
    } else {
        content_type.clone()
    };
    let content_type_for_cache = response_content_type.clone();
    let (downstream_sender, mut downstream_receiver) =
        mpsc::channel::<Result<RelayBodyChunk, std::io::Error>>(STREAM_RELAY_CHANNEL_CAPACITY);
    let downstream_byte_budget = Arc::new(Semaphore::new(STREAM_RELAY_BYTE_BUDGET));
    let relay_tracker = state.relay_tasks.clone();
    let relay_reservation = if response_handoff.is_none() {
        match relay_tracker.reserve() {
            Ok(reservation) => Some(reservation),
            Err(_) => {
                // The upstream response head already exists, but no relay may
                // expose it after shutdown has stopped accepting owners. Record
                // the failed inbound before returning a local 503.
                upstream_request_diagnostics.final_scope_waterline = final_scope_dispatch
                    .take()
                    .and_then(|guard| guard.finish(None, false, false, None, None));
                let admission_body = json!({ "stream": true });
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
                    &admission_body,
                    used_response_session,
                    &response_session_reuse_diagnostics,
                    requested_model.clone(),
                    upstream_request_diagnostics.final_scope_waterline.clone(),
                    agent_log_id.as_deref(),
                    "stream-relay-admission",
                    upstream_request_diagnostics.attempts,
                    &[],
                    "stream_relay_admission",
                    "proxy relay is shutting down",
                )
                .await;
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "proxy relay is shutting down",
                );
            }
        }
    } else {
        None
    };

    let relay = async move {
        let raw_stream = upstream.bytes_stream();
        let mut responses_to_chat_stream_summary = None;
        let mut responses_to_anthropic_stream_summary = None;
        let mut chat_to_anthropic_stream_summary = None;
        let mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>> =
            if convert_codex_chat_sse_to_responses_sse {
                let tool_context = cross_protocol_tool_context.clone().unwrap_or_default();
                Box::pin(
                    streaming_codex_chat::create_responses_sse_stream_from_chat_with_context_and_model(
                        raw_stream,
                        tool_context,
                        decision.model.clone(),
                    )
                    .map(|item| item.map_err(|err| err.to_string())),
                )
            } else if convert_anthropic_sse_to_responses_sse {
                let context = streaming_codex_anthropic::AnthropicResponsesContext {
                    tool_context: cross_protocol_tool_context.unwrap_or_default(),
                };
                Box::pin(
                    streaming_codex_anthropic::create_responses_sse_stream_from_anthropic(
                        raw_stream,
                        context,
                        decision.model.clone(),
                    )
                    .map(|item| item.map_err(|err| err.to_string())),
                )
            } else if convert_anthropic_sse_to_chat_sse {
                // Keep the raw Anthropic stream pull-based: the existing
                // Anthropic→Responses and Responses→Chat adapters compose
                // without collecting the body, so the first Chat delta can
                // leave the owner relay before `message_stop`.
                let intermediate =
                    streaming_codex_anthropic::create_responses_sse_stream_from_anthropic(
                        raw_stream,
                        streaming_codex_anthropic::AnthropicResponsesContext {
                            tool_context: transform_codex_chat::CodexToolContext::default(),
                        },
                        decision.model.clone(),
                    );
                let (adapted, summary) =
                    streaming_responses_chat::create_chat_sse_stream_from_responses(
                        intermediate,
                        decision.model.clone(),
                        diagnostics.chat_stream_include_usage,
                    );
                responses_to_chat_stream_summary = Some(summary);
                Box::pin(adapted.map(|item| item.map_err(|err| err.to_string())))
            } else if convert_chat_sse_to_anthropic_sse {
                let (adapted, summary) =
                    streaming_chat_anthropic::create_anthropic_sse_stream_from_chat(
                        raw_stream,
                        decision.model.clone(),
                    );
                chat_to_anthropic_stream_summary = Some(summary);
                Box::pin(adapted.map(|item| item.map_err(|err| err.to_string())))
            } else if convert_responses_sse_to_chat_sse {
                let (adapted, summary) =
                    streaming_responses_chat::create_chat_sse_stream_from_responses(
                        raw_stream,
                        decision.model.clone(),
                        diagnostics.chat_stream_include_usage,
                    );
                responses_to_chat_stream_summary = Some(summary);
                Box::pin(adapted.map(|item| item.map_err(|err| err.to_string())))
            } else if convert_responses_sse_to_anthropic_sse {
                let (adapted, summary) =
                    streaming_responses_anthropic::create_anthropic_sse_stream_from_responses(
                        raw_stream,
                        decision.model.clone(),
                    );
                responses_to_anthropic_stream_summary = Some(summary);
                Box::pin(adapted.map(|item| item.map_err(|err| err.to_string())))
            } else {
                Box::pin(raw_stream.map(|item| item.map_err(|err| err.to_string())))
            };
        let mut first_chunk_at: Option<u64> = None;
        let mut first_model_output_at: Option<u64> = None;
        let mut cache_capture = BoundedCacheCapture::new(eligible);
        let mut stream_state = ResponsesStreamState::default();
        let use_generic_responses_error_gate = matches!(client_channel, Channel::Responses)
            && matches!(decision.upstream_channel, Channel::Responses)
            && is_text_event_stream(&content_type);
        let mut generic_responses_error_gate =
            use_generic_responses_error_gate.then(GenericResponsesErrorFrameGate::default);
        let mut responses_completion_seen = false;
        let mut native_failure_relayed = false;
        let mut sse_chunks = 0u64;
        let mut sse_end_reason = "upstream_eof".to_string();
        let mut stream_upstream_wait_ms = 0u64;
        let mut stream_client_backpressure_ms = 0u64;
        // A receiver can close naturally after the terminal event has already
        // entered the relay queue. Keep that internal flow-control condition
        // separate from a user-visible disconnect: the latter is only true
        // when no terminal was successfully accepted by the relay.
        let mut downstream_receiver_closed = false;
        let mut downstream_disconnected = false;
        let mut downstream_disconnect_stage = None;
        let mut first_chunk_accepted_by_relay = false;
        let mut terminal_accepted_by_relay = false;
        let mut stream_end = StreamEnd::CleanEof;
        let mut stream_transport_error = None;
        let mut stream_metric_errors = Vec::<(String, String)>::new();
        let mut terminal_publication = None;
        let mut terminal_precheck = TerminalPrecheckGuard::new(&client_channel);
        let mut generic_responses_failure_code = None;
        let state_for_stream = state.clone();
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
                    stream_metric_errors.push(("upstream_stream".to_string(), err.clone()));
                    sse_end_reason = "upstream_stream_error".to_string();
                    stream_end = StreamEnd::TransportError;
                    stream_transport_error = Some(err);
                    break;
                }
            };
            let chunk_received_at = started.elapsed().as_millis() as u64;
            if first_chunk_at.is_none() {
                first_chunk_at = Some(chunk_received_at);
            }
            sse_chunks += 1;
            let relay_chunks = if let Some(gate) = generic_responses_error_gate.as_mut() {
                gate.push(&chunk)
                    .into_iter()
                    .map(|event| match event {
                        GenericResponsesErrorFrameGateEvent::Passthrough(chunk) => {
                            RelayStreamChunk::Raw(chunk)
                        }
                        GenericResponsesErrorFrameGateEvent::ErrorFrame(chunk) => {
                            RelayStreamChunk::GenericErrorFrame(chunk)
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                relay_chunk_parts(&chunk)
                    .map(RelayStreamChunk::Raw)
                    .collect::<Vec<_>>()
            };
            for relay in relay_chunks {
                if native_failure_relayed && use_generic_responses_error_gate {
                    // A native terminal failure was already published for this
                    // stream. Keep draining the one upstream request for clean
                    // accounting, but never append contradictory events.
                    continue;
                }
                let (state_chunk, relay_chunk, replaces_upstream_error) = match relay {
                    RelayStreamChunk::Raw(chunk) => (chunk.clone(), chunk, false),
                    RelayStreamChunk::GenericErrorFrame(chunk) if !responses_completion_seen => {
                        let summary = generic_responses_error_frame_summary(
                            &chunk,
                            client_prompt_cache_key_for_redaction.as_deref(),
                        );
                        generic_responses_failure_code = Some(response_failure_code_for_terminal(
                            TerminalFailure::ErrorEvent,
                            (!summary.trim().is_empty()).then_some(summary.as_str()),
                            upstream_request_diagnostics
                                .upstream_trace_source
                                .as_deref(),
                        ));
                        // Generic SSE errors usually carry only an `error`
                        // object, not the response id published earlier by
                        // `response.created`. Preserve that known identity so
                        // the downstream terminal belongs to the same
                        // response rather than a locally generated one.
                        let response_id =
                            response_id_from_bytes(&chunk).or_else(|| stream_state.response_id());
                        let failure = canonical_responses_failure_frame(
                            &decision.model,
                            response_id.as_deref(),
                            TerminalFailure::ErrorEvent,
                            (!summary.trim().is_empty()).then_some(summary.as_str()),
                            &request_id,
                            upstream_request_diagnostics
                                .upstream_trace_source
                                .as_deref(),
                            upstream_request_diagnostics.upstream_trace_id.as_deref(),
                        );
                        (chunk, failure, true)
                    }
                    RelayStreamChunk::GenericErrorFrame(chunk) => (chunk.clone(), chunk, false),
                };
                // Keep complete SSE/JSON parsing behind downstream enqueue.
                // A zero-allocation marker scan only installs a provisional
                // publication guard for the rare chunk that may contain a
                // terminal frame; exact parsing below retains or releases it.
                let candidate_publication = (response_session_lease.is_some()
                    && terminal_publication.is_none()
                    && terminal_precheck.chunk_requires_precheck(&relay_chunk))
                .then(|| {
                    response_session_lease.as_ref().map(|lease| {
                        state_for_stream
                            .continuation_lineage
                            .register_terminal_publication(lease.key())
                    })
                })
                .flatten();
                let mut accepted_by_relay = false;
                if !downstream_receiver_closed {
                    let client_backpressure_started = Instant::now();
                    let permit = downstream_byte_budget
                        .clone()
                        .acquire_many_owned(relay_chunk.len() as u32)
                        .await;
                    let send_failed = match permit {
                        Ok(permit) => downstream_sender
                            .send(Ok(RelayBodyChunk {
                                bytes: relay_chunk.clone(),
                                permit,
                            }))
                            .await
                            .is_err(),
                        Err(_) => true,
                    };
                    if send_failed {
                        downstream_receiver_closed = true;
                        if !terminal_accepted_by_relay {
                            downstream_disconnected = true;
                            downstream_disconnect_stage = Some("before_terminal".to_string());
                        }
                    } else {
                        accepted_by_relay = true;
                    }
                    stream_client_backpressure_ms = stream_client_backpressure_ms
                        .saturating_add(client_backpressure_started.elapsed().as_millis() as u64);
                }
                let observation = stream_state.ingest(&state_chunk);
                if replaces_upstream_error {
                    // The upstream failure frame is intentionally withheld from
                    // the client, but still observed for diagnostics. Ingesting
                    // the replacement makes the downstream terminal explicit
                    // and prevents a second synthetic response.failed at EOF.
                    stream_state.ingest(&relay_chunk);
                    native_failure_relayed = true;
                }
                responses_completion_seen |= observation.responses_completed_event_seen;
                if first_model_output_at.is_none() && observation.model_output_started {
                    first_model_output_at = Some(chunk_received_at);
                }
                let terminal_seen = match client_channel {
                    Channel::Responses => observation.responses_completed_event_seen,
                    Channel::Anthropic => observation.message_stop_event_seen,
                    Channel::Chat => observation.done_marker_seen,
                };
                if terminal_seen && terminal_publication.is_none() {
                    // Canonical providers hit the candidate path and already
                    // own the guard before enqueue. The fallback preserves
                    // correctness for an unusual escaped event spelling.
                    terminal_publication = candidate_publication.or_else(|| {
                        response_session_lease.as_ref().map(|lease| {
                            state_for_stream
                                .continuation_lineage
                                .register_terminal_publication(lease.key())
                        })
                    });
                }
                if accepted_by_relay && !first_chunk_accepted_by_relay {
                    first_chunk_accepted_by_relay = true;
                    tokio::task::yield_now().await;
                }
                if accepted_by_relay && terminal_seen {
                    terminal_accepted_by_relay = true;
                }
                cache_capture.push(&relay_chunk);
            }
        }
        let cache_body = cache_capture.finish();
        let mut stream_metadata = stream_state.finish();
        if let Some(summary_handle) = responses_to_chat_stream_summary {
            if let Ok(summary) = summary_handle.lock() {
                if summary.usage.has_usage() {
                    stream_metadata.usage = summary.usage.clone();
                }
                if summary.response_id.is_some() {
                    stream_metadata.response_id = summary.response_id.clone();
                }
                stream_metadata.compaction_output_seen |= summary.compaction_output_seen;
                stream_metadata.model_output_seen |= summary.model_output_seen;
            }
        }
        if let Some(summary_handle) = responses_to_anthropic_stream_summary {
            if let Ok(summary) = summary_handle.lock() {
                if summary.usage.has_usage() {
                    stream_metadata.usage = summary.usage.clone();
                }
                if summary.response_id.is_some() {
                    stream_metadata.response_id = summary.response_id.clone();
                }
                stream_metadata.compaction_output_seen |= summary.compaction_output_seen;
                stream_metadata.model_output_seen |= summary.model_output_seen;
            }
        }
        if let Some(summary_handle) = chat_to_anthropic_stream_summary {
            if let Ok(summary) = summary_handle.lock() {
                if summary.usage.has_usage() {
                    stream_metadata.usage = summary.usage.clone();
                }
                if summary.response_id.is_some() {
                    stream_metadata.response_id = summary.response_id.clone();
                }
                stream_metadata.compaction_output_seen |= summary.compaction_output_seen;
                stream_metadata.model_output_seen |= summary.model_output_seen;
            }
        }
        if let Some(error_summary) = stream_metadata.error_summary.as_mut() {
            *error_summary = summarize_client_prompt_cache_key_error(
                std::mem::take(error_summary),
                client_prompt_cache_key_for_redaction.as_deref(),
            );
        }
        let terminal_verdict = evaluate_terminal(
            &client_channel,
            TerminalCompatibility::Strict,
            &stream_metadata,
            stream_end,
        );
        let terminal_failure_code = generic_responses_failure_code.or_else(|| {
            terminal_verdict.failure.map(|failure| {
                response_failure_code_for_terminal(
                    failure,
                    stream_metadata.error_summary.as_deref(),
                    upstream_request_diagnostics
                        .upstream_trace_source
                        .as_deref(),
                )
            })
        });
        if terminal_verdict.trailing_transport_anomaly {
            sse_end_reason = "upstream_trailing_transport_anomaly".to_string();
        } else if let Some(anomaly) = terminal_verdict.trailing_protocol_anomaly {
            sse_end_reason = "upstream_trailing_protocol_anomaly".to_string();
            let detail = match anomaly {
                TerminalFailure::ErrorEvent => stream_metadata
                    .error_summary
                    .clone()
                    .unwrap_or_else(|| "SSE error event after protocol terminal".to_string()),
                TerminalFailure::FrameTooLarge => {
                    "oversized SSE frame after protocol terminal".to_string()
                }
                TerminalFailure::IncompleteEof => {
                    "incomplete EOF after protocol terminal".to_string()
                }
                TerminalFailure::TransportErrorBeforeTerminal => {
                    "transport error after protocol terminal".to_string()
                }
            };
            stream_metric_errors.push(("upstream_trailing_protocol_anomaly".to_string(), detail));
        } else if let Some(failure_code) = terminal_failure_code {
            sse_end_reason = failure_code.code().to_string();
        }
        if terminal_failure_code.is_some_and(ResponsesFailureCode::is_upstream_blocked) {
            if let (Some(scope), Some(shape)) = (
                full_replay_risk_scope.clone(),
                upstream_request_diagnostics.full_replay_risk_shape,
            ) {
                state_for_stream
                    .full_replay_risk_observations
                    .lock()
                    .await
                    .note_blocked(scope, shape);
            }
        }
        let mut canonical_failure_enqueued = false;
        if !downstream_receiver_closed
            && !terminal_verdict.success
            && matches!(client_channel, Channel::Responses)
            && matches!(decision.upstream_channel, Channel::Responses)
            && !stream_metadata.canonical_responses_failure_seen
        {
            if let Some(failure) = terminal_verdict.failure {
                let failure_frame = canonical_responses_failure_frame(
                    &decision.model,
                    stream_metadata.response_id.as_deref(),
                    failure,
                    stream_metadata.error_summary.as_deref(),
                    &request_id,
                    upstream_request_diagnostics
                        .upstream_trace_source
                        .as_deref(),
                    upstream_request_diagnostics.upstream_trace_id.as_deref(),
                );
                let send_failed = match downstream_byte_budget
                    .clone()
                    .acquire_many_owned(failure_frame.len() as u32)
                    .await
                {
                    Ok(permit) => downstream_sender
                        .send(Ok(RelayBodyChunk {
                            bytes: failure_frame,
                            permit,
                        }))
                        .await
                        .is_err(),
                    Err(_) => true,
                };
                if send_failed {
                    downstream_receiver_closed = true;
                    downstream_disconnected = true;
                    downstream_disconnect_stage = Some("before_terminal".to_string());
                } else {
                    canonical_failure_enqueued = true;
                }
            }
        }
        if !downstream_receiver_closed && !terminal_verdict.success && !canonical_failure_enqueued {
            let relay_error = match terminal_verdict.failure {
                Some(TerminalFailure::TransportErrorBeforeTerminal) => stream_transport_error
                    .unwrap_or_else(|| "upstream stream failed before completion".to_string()),
                Some(TerminalFailure::IncompleteEof) => {
                    "upstream stream ended before a completion event".to_string()
                }
                Some(TerminalFailure::FrameTooLarge) => {
                    "upstream SSE frame exceeded the inspection limit".to_string()
                }
                Some(TerminalFailure::ErrorEvent) | None => String::new(),
            };
            if !relay_error.is_empty()
                && downstream_sender
                    .send(Err(std::io::Error::other(relay_error)))
                    .await
                    .is_err()
            {
                downstream_disconnected = true;
                downstream_disconnect_stage = Some("before_terminal".to_string());
            }
        }
        let stream_success_for_cache = (200..300).contains(&status) && terminal_verdict.success;
        let confirmed_compaction = confirmed_responses_compaction(
            &decision.upstream_channel,
            diagnostics.compaction_trigger_requested,
            diagnostics.trusted_codex_compaction_requested,
            stream_metadata.compaction_output_seen,
            stream_metadata.model_output_seen,
            (200..300).contains(&status),
            terminal_verdict.success,
        );
        let response_session_response_id = stream_metadata.response_id.clone();
        // The client has received every upstream byte. Close its body before
        // usage, cache, metrics, and persistence settlement continue.
        let client_completed_ms = started.elapsed().as_millis() as u64;
        drop(downstream_sender);
        note_selected_provider_key_stream_terminal(
            &state_for_stream,
            &decision.provider.id,
            upstream_request_diagnostics
                .cache_capability_key_id
                .as_deref(),
            stream_success_for_cache,
            stream_metadata.error_summary.as_deref(),
        )
        .await;
        // Passive cache-control acceptance is evidence only for a normal,
        // completed native FullReplay turn.  A successful HTTP head is not
        // sufficient: an SSE error/WAF event, a compaction epoch, an external
        // continuation, or an ambiguous local lineage must never promote a
        // capability for a later request.
        let completed_native_full_replay_for_cache_controls =
            completed_native_full_replay_cache_control_acceptance_allowed(
                stream_success_for_cache,
                agent_generation,
                &client_channel,
                &decision.upstream_channel,
                confirmed_compaction,
                response_session_starts_compaction_epoch,
                used_response_session,
                &response_session_parent,
                upstream_request_diagnostics.suppress_local_full_replay_settlement,
            );
        note_runtime_native_cache_control_acceptance(
            &state_for_stream,
            &decision,
            &decision.upstream_channel,
            &upstream_request_diagnostics,
            completed_native_full_replay_for_cache_controls,
        )
        .await;
        // The terminal event is already visible, so publish the minimal
        // in-memory lineage and waterline control state before releasing its
        // per-lineage publication fence. Slow metrics/persistence remain below.
        let local_full_replay_settlement_allowed =
            !upstream_request_diagnostics.suppress_local_full_replay_settlement;
        let response_session_update = if stream_success_for_cache && !confirmed_compaction {
            if local_full_replay_settlement_allowed {
                let breakpoint_placement_digest = upstream_request_diagnostics
                    .final_wire_receipt
                    .as_ref()
                    .and_then(|receipt| {
                        receipt
                            .cache_controls
                            .breakpoint_placement_digest()
                            .map(ToOwned::to_owned)
                    });
                update_response_session_with_owned_input(
                    &state_for_stream,
                    response_session_lease.as_ref(),
                    &response_session_parent,
                    full_response_input.take(),
                    breakpoint_placement_digest,
                    response_session_response_id.clone(),
                    std::mem::take(&mut stream_metadata.output_items),
                )
                .await
            } else {
                tombstone_ambiguous_response_session(
                    &state_for_stream,
                    response_session_lease.as_ref(),
                    response_session_response_id.clone(),
                )
                .await
            }
        } else {
            None
        };
        let committed_head = committed_waterline_control_head(
            response_session_lease.as_ref(),
            response_session_update.as_ref(),
            response_session_response_id.as_deref(),
        );
        let warm_pending_committed_head = committed_head.clone();
        let rebased_from_head = rebased_waterline_control_head(
            response_session_lease.as_ref(),
            response_session_update.as_ref(),
        );
        let raw_final_scope_usage = (stream_success_for_cache && stream_metadata.usage.has_usage())
            .then_some(&stream_metadata.usage);
        upstream_request_diagnostics.final_scope_waterline =
            final_scope_dispatch.take().and_then(|guard| {
                guard.finish(
                    raw_final_scope_usage,
                    stream_success_for_cache,
                    confirmed_compaction,
                    committed_head,
                    rebased_from_head,
                )
            });
        settle_giant_cold_prefix_pending(
            &state_for_stream,
            &upstream_request_diagnostics,
            prefix_state_key.as_deref(),
            warm_pending_committed_head.as_ref(),
            raw_final_scope_usage,
            stream_success_for_cache,
            confirmed_compaction,
        )
        .await;
        if let Some(publication) = terminal_publication.take() {
            publication.finish();
        }
        if !terminal_verdict.success {
            if let Some(error_summary) = stream_metadata.error_summary.as_deref() {
                note_runtime_cache_capability_rejection(
                    &state_for_stream,
                    &decision,
                    &decision.upstream_channel,
                    &upstream_request_diagnostics,
                    status,
                    error_summary,
                    &stream_metadata.cache_capability_rejection_fields,
                )
                .await;
            }
        }
        let terminal_error_scope = match terminal_verdict.failure {
            Some(TerminalFailure::ErrorEvent) => {
                terminal_failure_code.map(ResponsesFailureCode::code)
            }
            Some(TerminalFailure::IncompleteEof) => Some("upstream_sse_error"),
            Some(TerminalFailure::FrameTooLarge) => Some("upstream_sse_frame_too_large"),
            Some(TerminalFailure::TransportErrorBeforeTerminal) | None => None,
        };
        if !stream_success_for_cache {
            if let Some(error_scope) = terminal_error_scope {
                let detail = terminal_verdict
                    .failure
                    .map(|failure| {
                        terminal_failure_message_with_trace(
                            failure,
                            stream_metadata.error_summary.as_deref(),
                            &request_id,
                            upstream_request_diagnostics
                                .upstream_trace_source
                                .as_deref(),
                            upstream_request_diagnostics.upstream_trace_id.as_deref(),
                        )
                    })
                    .unwrap_or_else(|| sse_end_reason.clone());
                stream_metric_errors.push((error_scope.to_string(), detail));
            }
        }
        let total_ms = client_completed_ms;
        let usage_observation = if stream_success_for_cache {
            collect_provider_usage_from_record(
                &state_for_stream,
                stream_metadata.usage.clone(),
                &decision,
                prefix_state_key.as_deref(),
                used_response_session,
            )
            .await
        } else {
            None
        };
        let usage_record = usage_observation.as_ref().map(|item| item.raw.clone());
        let prefix_usage_record = usage_observation
            .as_ref()
            .map(|item| item.effective.clone());
        let final_responses_static_projection =
            if matches!(decision.upstream_channel, Channel::Responses) {
                FinalResponsesStaticProjection::Observed(
                    upstream_request_diagnostics
                        .final_wire_receipt
                        .as_ref()
                        .and_then(|receipt| {
                            receipt.wire.responses_static_projection_digest.as_deref()
                        }),
                )
            } else {
                FinalResponsesStaticProjection::NotApplicable
            };
        let late_atoapi_mutation_categories = upstream_request_diagnostics
            .final_wire_receipt
            .as_ref()
            .map(|receipt| receipt.wire.atoapi_mutated_static_categories.as_slice())
            .unwrap_or_default();
        let prefix_observation = observe_provider_prefix_usage(
            &state_for_stream,
            prefix_state_key.as_deref(),
            provider_prefix_family_key.as_deref(),
            usage_record.as_ref(),
            prefix_usage_record.as_ref(),
            &tail_input_diagnostics,
            final_responses_static_projection,
            late_atoapi_mutation_categories,
            used_response_session,
            retried_full_response,
            prefix_guard_wait.budget_exhausted,
            prefix_guard_wait.pre_request_avoidable_tokens,
            prefix_guard_wait.recovery_applicable,
            prefix_guard_wait.source.as_deref() == Some("exact") && prefix_guard_wait.wait_ms > 0,
            prefix_guard_wait.exact_settle_window_elapsed && !confirmed_compaction,
            prefix_guard_wait.settled_exact_state_finished_at,
            stream_success_for_cache
                && !confirmed_compaction
                && local_full_replay_settlement_allowed,
        )
        .await;
        let (gap_breakdown, final_scope_rollback_reclassified) =
            reconcile_gap_with_final_scope_rollback(
                prefix_observation.gap,
                upstream_request_diagnostics.final_scope_waterline.as_ref(),
            );
        let mut prefix_lag = usage_record
            .as_ref()
            .map(|record| {
                prefix_lag_diagnostics_from_previous(
                    prefix_observation.previous.as_ref(),
                    record,
                    gap_breakdown.as_ref(),
                    &prefix_guard_wait,
                    &tail_input_diagnostics,
                )
            })
            .unwrap_or_default();
        if prefix_observation.static_wire_drift {
            prefix_lag.classification = Some("static_wire_drift".to_string());
            prefix_lag.static_wire_drift_late_mutation_categories = prefix_observation
                .static_wire_drift_late_mutation_categories
                .clone();
        } else if final_scope_rollback_reclassified {
            prefix_lag.classification = Some("provider_waterline_rollback".to_string());
        }
        if confirmed_compaction {
            let shadow_assignment_key =
                mark_shadow_compaction_boundary(&mut shadow_affinity_decision);
            let _ = finalize_confirmed_responses_compaction(
                &state_for_stream,
                response_session_lease.as_ref(),
                response_session_starts_compaction_epoch,
                prefix_state_key.as_deref(),
                provider_prefix_family_key.as_deref(),
                shadow_assignment_key.as_deref(),
            )
            .await;
        }
        let ttft_ms = first_model_output_at.or(first_chunk_at).unwrap_or(total_ms);
        let upstream_first_chunk_ms = first_chunk_at
            .unwrap_or(total_ms)
            .saturating_sub(upstream_response_headers_at_ms);
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
            provider_id: Some(decision.provider.id.clone()),
            model: decision.model.clone(),
            requested_model,
            agent_reasoning_effort: None,
            configured_reasoning_effort: None,
            effective_reasoning_effort: None,
            reasoning_effort_source: None,
            cache_status: if confirmed_compaction {
                "compact"
            } else if stream_success_for_cache {
                if eligible {
                    "miss"
                } else {
                    "bypass"
                }
            } else {
                "error"
            }
            .to_string(),
            cold_start: None,
            agent_id: agent_log_id.clone(),
            agent_label: agent_log_label.clone(),
            upstream_call_kind: Some("stream".to_string()),
            upstream_call_source: Some(
                if confirmed_compaction {
                    "responses-compaction-v2"
                } else {
                    "main"
                }
                .to_string(),
            ),
            cache_key: if eligible && stream_success_for_cache && !confirmed_compaction {
                Some(metrics_cache_key.clone())
            } else {
                None
            },
            provider_prefix_key: provider_prefix_key.clone(),
            provider_prefix_fingerprint: provider_prefix_fingerprint.clone(),
            outbound_prefix_fingerprints: upstream_request_diagnostics
                .outbound_prefix_fingerprints
                .clone(),
            provider_cache_diagnostic: usage_record.as_ref().map(provider_cache_diagnostic),
            final_scope_waterline: upstream_request_diagnostics.final_scope_waterline.clone(),
            shadow_affinity_mode: None,
            shadow_affinity_arm: None,
            shadow_affinity_realm_id: None,
            shadow_affinity_cohort_id: None,
            shadow_affinity_lane: None,
            shadow_affinity_shard: None,
            shadow_affinity_policy_epoch: None,
            shadow_affinity_anchor_epoch: None,
            shadow_affinity_trusted_identity: None,
            shadow_affinity_decision: None,
            shadow_affinity_skip_reason: None,
            shadow_affinity_policy_compute_ms: None,
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
            static_wire_drift_late_mutation_categories: None,
            prefix_cache_instability_score: prefix_guard_wait.cache_instability_score,
            prefix_seen_bucket_tokens: prefix_guard_wait.seen_bucket_tokens,
            prefix_state_cache_read_tokens: prefix_guard_wait.state_cache_read_tokens,
            status,
            ttft_ms,
            first_byte_ms: first_chunk_at,
            upstream_ttft_ms: Some(upstream_ttft_ms(ttft_ms, Some(prefix_guard_wait.wait_ms))),
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
            upstream_first_chunk_ms: Some(upstream_first_chunk_ms),
            stream_upstream_wait_ms: Some(stream_upstream_wait_ms),
            stream_client_backpressure_ms: Some(stream_client_backpressure_ms),
            aggregate_done_ms: None,
            upstream_retry_wait_ms: Some(upstream_request_diagnostics.retry_wait_ms),
            upstream_attempts: Some(upstream_request_diagnostics.attempts),
            request_body_bytes: Some(upstream_request_diagnostics.request_body_bytes),
            sent_body_bytes: Some(upstream_request_diagnostics.sent_body_bytes),
            request_body_encode_ms: Some(upstream_request_diagnostics.request_body_encode_ms),
            gzip_encode_ms: Some(upstream_request_diagnostics.gzip_encode_ms),
            gzip_attempted: Some(upstream_request_diagnostics.gzip_attempted),
            gzip_fallback_used: Some(upstream_request_diagnostics.gzip_fallback_used),
            upstream_header_wait_class: Some(upstream_header_wait_class(
                &upstream_request_diagnostics,
            )),
            total_ms,
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
            response_context_plan: response_session_reuse_diagnostics.context_plan.clone(),
            response_session_semantic_reuse_items: (response_session_reuse_diagnostics
                .semantic_reuse_items
                > 0)
            .then_some(response_session_reuse_diagnostics.semantic_reuse_items),
            response_session_wire_saved_bytes: (response_session_reuse_diagnostics
                .wire_saved_bytes
                > 0)
            .then_some(response_session_reuse_diagnostics.wire_saved_bytes),
            response_session_wire_saved_ratio: response_session_reuse_diagnostics.wire_saved_ratio,
            response_session_cooldown_active: Some(
                response_session_reuse_diagnostics.cooldown_active,
            ),
            response_session_rejected_status: response_session_reuse_diagnostics.rejected_status,
            session_anchor_hash: None,
            session_anchor_source: None,
            session_anchor_changed: None,
            session_anchor_peer_count: None,
            inbound_body_bytes: None,
            original_body_bytes: None,
            send_body_bytes: None,
            send_body_is_delta: None,
            payload_too_large_rescue_attempted: None,
            payload_too_large_rescue_used: None,
            sse_end_reason: Some(sse_end_reason),
            downstream_disconnected: Some(downstream_disconnected),
            downstream_disconnect_stage,
            sse_completed_event_seen: Some(stream_metadata.completed_event_seen),
            sse_done_marker_seen: Some(stream_metadata.done_marker_seen),
            sse_chunks: Some(sse_chunks),
        };
        apply_prefix_lag_diagnostics(&mut request_log, prefix_lag);
        apply_session_anchor_diagnostics(&mut request_log, &session_anchor_diagnostics);
        apply_body_diagnostics(&mut request_log, &diagnostics);
        apply_tail_input_diagnostics(&mut request_log, &tail_input_diagnostics);
        if agent_generation {
            let (attempt_outcome, inbound_outcome, error_scope, terminal_state) =
                if terminal_verdict.success {
                    (
                        AgentAttemptOutcome::HttpSuccess,
                        AgentInboundOutcome::Success,
                        None,
                        if terminal_verdict.trailing_transport_anomaly {
                            "response_completed_with_trailing_transport_anomaly"
                        } else {
                            "response_completed"
                        },
                    )
                } else {
                    match terminal_verdict.failure {
                        Some(TerminalFailure::IncompleteEof) => (
                            AgentAttemptOutcome::StreamError,
                            AgentInboundOutcome::Incomplete,
                            Some("upstream_incomplete_eof".to_string()),
                            "incomplete_eof",
                        ),
                        Some(TerminalFailure::ErrorEvent) => {
                            let code = terminal_failure_code
                                .unwrap_or(ResponsesFailureCode::UpstreamSseError);
                            (
                                AgentAttemptOutcome::StreamError,
                                AgentInboundOutcome::StreamError,
                                Some(code.code().to_string()),
                                if matches!(code, ResponsesFailureCode::UpstreamWafBlocked) {
                                    "upstream_waf_blocked"
                                } else if code.is_upstream_blocked() {
                                    "upstream_request_blocked"
                                } else {
                                    "sse_error"
                                },
                            )
                        }
                        Some(TerminalFailure::FrameTooLarge) => (
                            AgentAttemptOutcome::StreamError,
                            AgentInboundOutcome::StreamError,
                            Some("upstream_sse_frame_too_large".to_string()),
                            "sse_frame_too_large",
                        ),
                        Some(TerminalFailure::TransportErrorBeforeTerminal) => (
                            AgentAttemptOutcome::StreamError,
                            AgentInboundOutcome::TransportError,
                            Some("upstream_stream_error".to_string()),
                            "transport_error_before_terminal",
                        ),
                        None => (
                            AgentAttemptOutcome::HttpError,
                            AgentInboundOutcome::HttpError,
                            Some("upstream_stream".to_string()),
                            "stream_failed",
                        ),
                    }
                };
            finalize_agent_generation(
                &state_for_stream,
                &request_id,
                agent_attempt_id,
                request_log,
                attempt_outcome,
                inbound_outcome,
                Some(status),
                error_scope,
                terminal_state,
                usage_record.clone(),
                (!confirmed_compaction)
                    .then(|| {
                        response_session_key
                            .clone()
                            .or(session_anchor_diagnostics.hash.clone())
                    })
                    .flatten(),
                stream_metric_errors.clone(),
                shadow_affinity_decision,
                Some(&upstream_request_diagnostics),
            )
            .await;
        } else {
            let mut transaction = MetricsTransaction::upstream(request_log);
            if let Some(usage_record) = usage_record {
                transaction.observe_usage(
                    usage_record,
                    (!confirmed_compaction)
                        .then(|| {
                            response_session_key
                                .as_deref()
                                .or(session_anchor_diagnostics.hash.as_deref())
                        })
                        .flatten(),
                );
            }
            for (scope, message) in stream_metric_errors {
                transaction.observe_error(scope, message);
            }
            state_for_stream.metrics.commit(transaction).await;
        }
        if eligible && stream_success_for_cache && !confirmed_compaction {
            let Some(cache_body) = cache_body else {
                return;
            };
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
            )
            .await;
        }
    };

    let stream_body = async_stream::stream! {
        while let Some(chunk) = downstream_receiver.recv().await {
            match chunk {
                Ok(RelayBodyChunk { bytes, permit }) => {
                    yield Ok::<Bytes, std::io::Error>(bytes);
                    drop(permit);
                }
                Err(error) => yield Err(error),
            }
        }
    };

    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, response_content_type)
        .body(Body::from_stream(stream_body))
        .unwrap_or_else(|_| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to build stream response",
            )
        });

    if let Some(handoff) = response_handoff {
        let _ = handoff.send(response);
        relay.await;
        Response::new(Body::empty())
    } else {
        relay_reservation
            .expect("normal stream responses reserve a relay owner before body construction")
            .spawn(relay);
        response
    }
}
