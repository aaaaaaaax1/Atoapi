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
    route_affinity_key: Option<String>,
    config: AppConfig,
    session_reuse_capability: Option<ProviderResponseSessionReuseCapability>,
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
                    .and_then(|guard| guard.finish(None, false, false, None));
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
        let mut session_error_body = Vec::new();
        let mut stream_state = ResponsesStreamState::default();
        let mut sse_chunks = 0u64;
        let mut sse_end_reason = "upstream_eof".to_string();
        let mut stream_upstream_wait_ms = 0u64;
        let mut stream_client_backpressure_ms = 0u64;
        let mut downstream_disconnected = false;
        let mut downstream_disconnect_stage = None;
        let mut first_chunk_accepted_by_relay = false;
        let mut terminal_accepted_by_relay = false;
        let mut stream_end = StreamEnd::CleanEof;
        let mut stream_transport_error = None;
        let mut stream_metric_errors = Vec::<(String, String)>::new();
        let mut terminal_publication = None;
        let mut terminal_precheck = TerminalPrecheckGuard::new(&client_channel);
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
            for relay_chunk in relay_chunk_parts(&chunk) {
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
                if !downstream_disconnected {
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
                        downstream_disconnected = true;
                        downstream_disconnect_stage = Some(
                            if terminal_accepted_by_relay {
                                "after_terminal"
                            } else {
                                "before_terminal"
                            }
                            .to_string(),
                        );
                    } else {
                        accepted_by_relay = true;
                    }
                    stream_client_backpressure_ms = stream_client_backpressure_ms
                        .saturating_add(client_backpressure_started.elapsed().as_millis() as u64);
                }
                let observation = stream_state.ingest(&relay_chunk);
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
                if used_response_session && session_error_body.len() < 65_536 {
                    let remaining = 65_536usize.saturating_sub(session_error_body.len());
                    session_error_body
                        .extend_from_slice(&relay_chunk[..relay_chunk.len().min(remaining)]);
                }
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
        let terminal_verdict = evaluate_terminal(
            &client_channel,
            TerminalCompatibility::Strict,
            &stream_metadata,
            stream_end,
        );
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
        } else if let Some(failure) = terminal_verdict.failure {
            sse_end_reason = match failure {
                TerminalFailure::ErrorEvent => "upstream_sse_error",
                TerminalFailure::FrameTooLarge => "upstream_sse_frame_too_large",
                TerminalFailure::IncompleteEof => "upstream_incomplete_eof",
                TerminalFailure::TransportErrorBeforeTerminal => "upstream_stream_error",
            }
            .to_string();
        }
        if !downstream_disconnected && !terminal_verdict.success {
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
        // The terminal event is already visible, so publish the minimal
        // in-memory lineage and waterline control state before releasing its
        // per-lineage publication fence. Slow metrics/persistence remain below.
        let response_session_update = if stream_success_for_cache && !confirmed_compaction {
            update_response_session_with_owned_input(
                &state_for_stream,
                response_session_lease.as_ref(),
                &response_session_parent,
                full_response_input.take(),
                response_session_response_id.clone(),
                std::mem::take(&mut stream_metadata.output_items),
            )
            .await
        } else {
            None
        };
        let committed_head = committed_waterline_control_head(
            response_session_lease.as_ref(),
            response_session_update.as_ref(),
            response_session_response_id.as_deref(),
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
                )
            });
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
                )
                .await;
            }
        }
        if used_response_session
            && supports_main_response_session_delta(
                &config,
                &decision,
                session_reuse_capability.as_ref(),
            )
            && stream_metadata.error_event_seen
        {
            let error_summary = upstream_error_summary(&session_error_body);
            let rejection = response_session_rejection_classification(status, &error_summary);
            if !error_summary.is_empty() {
                stream_metric_errors.push((
                    "verified_response_session_delta_rejected".to_string(),
                    error_summary.clone(),
                ));
            }
            let cooldown_key =
                response_session_error_cooldown_key(&decision, session_reuse_capability.as_ref());
            note_response_session_error_cooldown_for_rejection(
                &state_for_stream,
                cooldown_key.as_deref(),
                status,
                &error_summary,
            )
            .await;
            if matches!(
                rejection,
                ResponseSessionRejectionClass::StaleReference
                    | ResponseSessionRejectionClass::Unsupported
            ) {
                clear_response_session_reference(
                    &state_for_stream,
                    response_session_lease.as_ref(),
                    response_session_lease
                        .as_ref()
                        .and_then(LineageLease::head)
                        .map(|head| head.response_id.as_str()),
                )
                .await;
            }
            if rejection == ResponseSessionRejectionClass::Unsupported {
                invalidate_verified_response_session_reuse(
                    &state_for_stream,
                    &decision.provider.id,
                    &decision.model,
                    session_reuse_capability.as_ref(),
                    &error_summary,
                )
                .await;
            }
        }
        let terminal_error_scope = match terminal_verdict.failure {
            Some(TerminalFailure::ErrorEvent | TerminalFailure::IncompleteEof) => {
                Some("upstream_sse_error")
            }
            Some(TerminalFailure::FrameTooLarge) => Some("upstream_sse_frame_too_large"),
            Some(TerminalFailure::TransportErrorBeforeTerminal) | None => None,
        };
        if !stream_success_for_cache {
            if let Some(error_scope) = terminal_error_scope {
                stream_metric_errors.push((error_scope.to_string(), sse_end_reason.clone()));
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
        let prefix_observation = observe_provider_prefix_usage(
            &state_for_stream,
            prefix_state_key.as_deref(),
            provider_prefix_family_key.as_deref(),
            usage_record.as_ref(),
            prefix_usage_record.as_ref(),
            &tail_input_diagnostics,
            used_response_session,
            retried_full_response,
            prefix_guard_wait.budget_exhausted,
            stream_success_for_cache && !confirmed_compaction,
        )
        .await;
        let gap_breakdown = prefix_observation.gap;
        let prefix_lag = usage_record
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
        if stream_success_for_cache {
            note_provider_route_affinity(
                &state_for_stream,
                route_affinity_key.as_deref(),
                &decision.provider.id,
            )
            .await;
            clear_prefix_error_cooldown(&state_for_stream, prefix_state_key.as_deref()).await;
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
                        Some(TerminalFailure::ErrorEvent) => (
                            AgentAttemptOutcome::StreamError,
                            AgentInboundOutcome::StreamError,
                            Some("upstream_sse_error".to_string()),
                            "sse_error",
                        ),
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
