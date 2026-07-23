use super::*;

pub(super) struct AgentOwnerSettlementGuard {
    metrics: MetricsStore,
    request_id: String,
    started: Instant,
    client_channel: Channel,
    agent_id: Option<String>,
    armed: bool,
}

impl AgentOwnerSettlementGuard {
    pub(super) fn new(
        metrics: MetricsStore,
        request_id: String,
        started: Instant,
        client_channel: Channel,
        agent_id: Option<String>,
    ) -> Self {
        Self {
            metrics,
            request_id,
            started,
            client_channel,
            agent_id,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AgentOwnerSettlementGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let transaction = agent_owner_failure_transaction(
            &self.request_id,
            self.started,
            &self.client_channel,
            self.agent_id.clone(),
            "owner_dropped",
            "agent generation owner was dropped before settlement completed",
        );
        let _ = self.metrics.commit_detached(transaction);
    }
}

pub(super) fn agent_owner_failure_transaction(
    request_id: &str,
    started: Instant,
    client_channel: &Channel,
    agent_id: Option<String>,
    terminal_state: &str,
    message: &str,
) -> MetricsTransaction {
    let elapsed = started.elapsed().as_millis() as u64;
    let request_id = request_id.to_string();
    let mut transaction = MetricsTransaction::agent_owner_failure(AgentOwnerFailureSettlement {
        inbound_request_id: request_id.clone(),
        request: RequestLog {
            id: request_id.clone(),
            at: Utc::now(),
            inbound_request_id: Some(request_id),
            client_channel: client_channel.label().to_string(),
            upstream_channel: "unknown".to_string(),
            provider: "unknown".to_string(),
            provider_id: None,
            model: "unknown".to_string(),
            cache_status: "error".to_string(),
            agent_id: agent_id.clone(),
            agent_label: agent_id,
            status: StatusCode::BAD_GATEWAY.as_u16(),
            ttft_ms: elapsed,
            total_ms: elapsed,
            sse_end_reason: Some(terminal_state.to_string()),
            ..RequestLog::default()
        },
        terminal_state: Some(terminal_state.to_string()),
    });
    transaction.observe_error("agent_generation_owner", message.to_string());
    transaction
}

fn agent_attempt_finish(
    outcome: AgentAttemptOutcome,
    status: Option<u16>,
    error_scope: Option<String>,
    terminal_state: Option<String>,
    total_ms: u64,
    diagnostics: Option<&UpstreamRequestDiagnostics>,
) -> AgentAttemptFinish {
    AgentAttemptFinish {
        finished_at: Utc::now(),
        outcome,
        status,
        error_scope,
        terminal_state,
        total_ms,
        upstream_headers_ms: diagnostics.map(|item| item.headers_ms),
        upstream_network_path: diagnostics.map(|item| item.network_path.to_string()),
        request_body_bytes: diagnostics.map(|item| item.request_body_bytes),
        sent_body_bytes: diagnostics.map(|item| item.sent_body_bytes),
        gzip_attempted: diagnostics.map(|item| item.gzip_attempted),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_agent_generation(
    state: &AppState,
    inbound_request_id: &str,
    attempt_id: Option<String>,
    mut request_log: RequestLog,
    attempt_outcome: AgentAttemptOutcome,
    inbound_outcome: AgentInboundOutcome,
    status: Option<u16>,
    error_scope: Option<String>,
    terminal_state: &str,
    usage_record: Option<UsageRecord>,
    usage_cold_start_key: Option<String>,
    request_errors: Vec<(String, String)>,
    shadow_affinity: Option<ShadowAffinityDecision>,
    diagnostics: Option<&UpstreamRequestDiagnostics>,
) {
    apply_shadow_affinity_log_fields(&mut request_log, shadow_affinity.as_ref());
    let validation_ttft_ms = request_log.ttft_ms;
    let shadow_observation = shadow_affinity.as_ref().map(|_| ShadowObservationInput {
        success: matches!(inbound_outcome, AgentInboundOutcome::Success),
        has_usage: request_log.input_tokens.unwrap_or_default() > 0,
        input_tokens: request_log.input_tokens.unwrap_or_default(),
        cache_read_tokens: request_log.cache_read_tokens.unwrap_or_default(),
        giant_tail: request_log.tail_tool_output_chars.unwrap_or_default() >= 80_000
            || request_log
                .tail_largest_tool_output_chars
                .unwrap_or_default()
                >= 80_000,
        compaction_boundary: request_log.cache_status == "compact",
        status: request_log.status,
        ttft_ms: request_log.ttft_ms,
        avoidable_gap_tokens: request_log.cache_avoidable_gap_tokens.unwrap_or_default(),
        provider_unstable_gap_tokens: request_log
            .cache_provider_unstable_gap_tokens
            .unwrap_or_default(),
        attempt_count: request_log
            .upstream_attempt_total
            .or(request_log.upstream_attempts)
            .unwrap_or(1),
    });
    let Some(attempt_id) = attempt_id else {
        state
            .metrics
            .record_error(
                "agent_lifecycle",
                "Agent generation reached terminal commit without an active attempt",
            )
            .await;
        return;
    };
    let mut transaction = MetricsTransaction::agent_terminal(AgentTerminalSettlement {
        inbound_request_id: inbound_request_id.to_string(),
        attempt_id,
        attempt_finish: agent_attempt_finish(
            attempt_outcome,
            status,
            error_scope,
            Some(terminal_state.to_string()),
            request_log.total_ms,
            diagnostics,
        ),
        request: request_log,
        inbound_outcome,
        terminal_state: Some(terminal_state.to_string()),
    });
    if let Some(usage_record) = usage_record {
        transaction.observe_usage(usage_record, usage_cold_start_key.as_deref());
    }
    for (scope, message) in request_errors {
        transaction.observe_error(scope, message);
    }
    if state.metrics.commit(transaction).await != MetricsCommitResult::Applied {
        return;
    }
    if let (Some(decision), Some(observation)) = (shadow_affinity.as_ref(), shadow_observation) {
        if let Some(run_id) = decision.validation_run_id.as_deref() {
            let candidate_fields = diagnostics
                .and_then(|diagnostics| {
                    diagnostics.final_wire_receipt.as_ref().map(|receipt| {
                        controlled_cache_probe_fields_on_final_wire(
                            &diagnostics.controlled_cache_probe_fields,
                            receipt,
                            None,
                        )
                    })
                })
                .unwrap_or_default();
            let completion = state.cache_validation.lock().await.observe(
                run_id,
                CacheValidationObservation {
                    success: observation.success,
                    usage_observed: observation.has_usage,
                    input_tokens: observation.input_tokens,
                    cache_read_tokens: observation.cache_read_tokens,
                    ttft_ms: validation_ttft_ms,
                    candidate_applied: decision.decision == "validation_candidate_applied"
                        && !candidate_fields.is_empty(),
                    candidate_fields,
                    candidate_breakpoint_placement_digest: diagnostics.and_then(|diagnostics| {
                        diagnostics
                            .controlled_cache_probe_breakpoint_placement_digest
                            .clone()
                    }),
                },
                Utc::now(),
            );
            if let Some(evidence) = completion.and_then(|completion| completion.effect_evidence()) {
                record_cache_validation_effect(state, evidence).await;
            }
        }
        let mut store = state.shadow_affinity.lock().await;
        cache_affinity::observe_shadow_affinity(&mut store, decision, observation, Utc::now());
        drop(store);
        state
            .metrics
            .record_shadow_observation(
                observation.success,
                observation.has_usage,
                !observation.has_usage || observation.giant_tail,
            )
            .await;
        state.journal_runtime_state();
    }
}

async fn record_cache_validation_effect(
    state: &AppState,
    evidence: cache_validation::CacheValidationEffectEvidence,
) {
    let mut config = state.config.write().await;
    config.record_cache_capability_effect_for_scope(
        &evidence.provider_id,
        &evidence.model,
        &evidence.channel,
        evidence.key_id.as_deref(),
        Some(&evidence.effect_scope_id),
        &evidence.fields,
        evidence.status,
        Some(evidence.message),
        Some(evidence.baseline_cache_read_tokens),
        Some(evidence.candidate_cache_read_tokens),
        Some(evidence.baseline_ttft_ms),
        Some(evidence.candidate_ttft_ms),
    );
    // Settlement runs after the terminal metric commit and must not wait for
    // disk. The versioned write-behind journal preserves ordering while the
    // stream owner remains free to finish other work.
    state.journal_config(&config);
}

fn apply_shadow_affinity_log_fields(
    request_log: &mut RequestLog,
    decision: Option<&ShadowAffinityDecision>,
) {
    let Some(decision) = decision else {
        return;
    };
    request_log.shadow_affinity_mode = Some(decision.mode.clone());
    request_log.shadow_affinity_arm = Some(shadow_affinity_arm_label(decision.arm));
    request_log.shadow_affinity_realm_id = Some(decision.realm_id.clone());
    request_log.shadow_affinity_cohort_id = Some(decision.cohort_id.clone());
    request_log.shadow_affinity_lane = Some(shadow_cache_lane_label(decision.lane));
    request_log.shadow_affinity_shard = Some(decision.shard);
    request_log.shadow_affinity_policy_epoch = Some(decision.policy_epoch);
    request_log.shadow_affinity_anchor_epoch = Some(decision.anchor_epoch);
    request_log.shadow_affinity_trusted_identity = Some(decision.trusted_identity);
    request_log.shadow_affinity_decision = Some(decision.decision.clone());
    request_log.shadow_affinity_skip_reason = decision.skip_reason.clone();
    request_log.shadow_affinity_policy_compute_ms = Some(decision.policy_compute_ms);
}

fn shadow_cache_lane_label(lane: cache_affinity::ShadowCacheLane) -> String {
    match lane {
        cache_affinity::ShadowCacheLane::Steady => "steady",
        cache_affinity::ShadowCacheLane::ToolBurstQuarantine => "tool_burst_quarantine",
        cache_affinity::ShadowCacheLane::CompactedAnchor => "compacted_anchor",
        cache_affinity::ShadowCacheLane::Transparent => "transparent",
    }
    .to_string()
}

fn shadow_affinity_arm_label(arm: cache_affinity::ShadowAffinityArm) -> String {
    match arm {
        cache_affinity::ShadowAffinityArm::Baseline => "baseline",
        cache_affinity::ShadowAffinityArm::Candidate => "candidate",
    }
    .to_string()
}

pub(super) async fn insert_cache_entries(
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
    let now = Utc::now();
    let expires_at = now + Duration::seconds(config.cache.max_age_seconds as i64);
    let entries = keys
        .into_iter()
        .map(|key| CacheEntry {
            key,
            semantic_text: semantic_text.clone(),
            semantic_shape: semantic_shape.clone(),
            semantic_vector: Vec::new(),
            content_type: content_type.clone(),
            status,
            body: body.clone(),
            created_at: now,
            expires_at,
            provider_id: decision.provider.id.clone(),
            model: decision.model.clone(),
            workspace_fingerprint: Some(config.workspace_fingerprint.clone()),
        })
        .collect();
    if let Err(err) = state.cache.insert_many(entries, &config.cache).await {
        state
            .metrics
            .record_error("cache_insert", &err.to_string())
            .await;
    }
}
