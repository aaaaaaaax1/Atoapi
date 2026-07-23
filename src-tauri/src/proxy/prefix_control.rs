use std::time::{Duration, Instant};

use crate::{metrics::UsageRecord, state::PrefixWarmState};

use super::{
    provider_cache_bucket_max, provider_cache_bucket_max_128, provider_cache_ratio,
    provider_cache_shortfall, provider_cache_shortfall_128, provider_prefix_break_after_warm_state,
    provider_prefix_calibrated_previous_seen_buckets, provider_prefix_usage_is_safe_to_learn,
    provider_prefix_weak_full_retry_after_session_delta,
    responses_cap_exhausted_provider_waterline_rollback,
    responses_current_tail_makes_avoidable_unreliable, responses_huge_dynamic_history_cold_read,
    responses_small_avoidable_tail_granularity, responses_tiny_instability_recovery_tail_is_clean,
    responses_tool_tail_burst, should_learn_provider_prefix_family_state,
    should_learn_sent_provider_bucket, TailInputDiagnostics,
};

const MAX_FOREGROUND_WAIT: Duration = Duration::from_millis(500);
const MAX_EXACT_EVIDENCE_AGE: Duration = Duration::from_secs(30);
const MIN_REPEATED_AVOIDABLE_STREAK: u32 = 2;
pub(super) const MIN_CLEAN_TINY_RECOVERY_STREAK: u32 = 2;
const MAX_STABLE_INSTABILITY_SCORE: u32 = 2;
const STABLE_RECOVERY_MIN_INPUT_TOKENS: u64 = 32_000;
const STABLE_RECOVERY_MIN_RATIO: f64 = 0.98;
const STABLE_RECOVERY_MAX_AVOIDABLE_TOKENS: u64 = 1_024;

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefixControlInput {
    pub source_is_exact: bool,
    pub avoidable_tokens: u64,
    pub fine_avoidable_tokens: u64,
    pub avoidable_shortfall_streak: u32,
    pub cache_instability_score: u32,
    pub tiny_instability_recovery_safe: bool,
    pub current_tail_is_settle_safe: bool,
    pub settle_after_cold_read: bool,
    pub compaction_requested: bool,
    pub state_age: Duration,
    pub request_budget: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PrefixControlDecision {
    pub wait: Duration,
    pub reason: Option<&'static str>,
    pub skip_reason: Option<&'static str>,
    pub budget_exhausted: bool,
}

pub(super) struct PrefixController;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProviderCacheGapBreakdown {
    pub total_tokens: u64,
    pub new_tail_tokens: u64,
    pub avoidable_tokens: u64,
    pub provider_unstable_tokens: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefixGapInput<'a> {
    pub previous_exact: Option<&'a PrefixWarmState>,
    pub previous_best: Option<&'a PrefixWarmState>,
    pub previous_family: Option<&'a PrefixWarmState>,
    pub usage: &'a UsageRecord,
    pub tail: &'a TailInputDiagnostics,
    pub guard_budget_exhausted: bool,
    /// True only when the current frozen native Responses wire changed a
    /// non-`input` projection relative to the state being compared.
    pub static_wire_drift: bool,
}

/// The final wire is the only trustworthy source for a Responses static
/// projection. `NotApplicable` keeps non-Responses channels from changing a
/// stored Responses receipt; `Observed(None)` is a real but unprojectable
/// Responses wire and deliberately fails closed against a prior digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FinalResponsesStaticProjection<'a> {
    NotApplicable,
    Observed(Option<&'a str>),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefixStateInput<'a> {
    pub previous: Option<&'a PrefixWarmState>,
    pub usage: &'a UsageRecord,
    pub tail: &'a TailInputDiagnostics,
    pub used_response_session: bool,
    pub retried_full_response: bool,
    pub guard_budget_exhausted: bool,
    pub final_responses_static_projection: FinalResponsesStaticProjection<'a>,
}

#[derive(Debug, Clone)]
pub(super) struct PrefixStateObservation {
    pub next: Option<PrefixWarmState>,
    pub learn_family: bool,
    pub static_wire_drift: bool,
}

#[derive(Debug, Clone, Copy)]
struct GapEvidence {
    previous_seen_bucket: u64,
    previous_seen_bucket_128: u64,
    direct_avoidable_tokens: u64,
    avoidable_tokens: u64,
    avoidable_tokens_128: u64,
    tail_granularity: bool,
    provider_rollback: bool,
}

impl PrefixController {
    pub fn before_request(input: PrefixControlInput) -> PrefixControlDecision {
        if !input.source_is_exact {
            return skip("non_exact_prefix_state", false);
        }
        if input.settle_after_cold_read && input.state_age < MAX_FOREGROUND_WAIT {
            let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
            if request_budget.is_zero() {
                return skip("local_guard_budget_exhausted", true);
            }
            let requested = MAX_FOREGROUND_WAIT.saturating_sub(input.state_age);
            let wait = requested.min(request_budget);
            return PrefixControlDecision {
                wait,
                reason: Some("responses_recent_cold_read_settle"),
                skip_reason: None,
                budget_exhausted: wait < requested,
            };
        }
        if input.compaction_requested
            && input.state_age < MAX_FOREGROUND_WAIT
            && input.cache_instability_score > 0
        {
            let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
            if request_budget.is_zero() {
                return skip("local_guard_budget_exhausted", true);
            }
            let requested = MAX_FOREGROUND_WAIT.saturating_sub(input.state_age);
            let wait = requested.min(request_budget);
            return PrefixControlDecision {
                wait,
                reason: Some("responses_compaction_prefix_settle"),
                skip_reason: None,
                budget_exhausted: wait < requested,
            };
        }
        if input.avoidable_tokens == 0 {
            return skip("no_avoidable_gap", false);
        }
        if input.state_age >= MAX_EXACT_EVIDENCE_AGE {
            return skip("avoidable_evidence_expired", false);
        }
        if input.avoidable_shortfall_streak < MIN_REPEATED_AVOIDABLE_STREAK {
            // A just-settled exact prefix can expose a first cache-lag gap
            // before there is enough history to call it a repeated pattern.
            // Give only that narrow case the remainder of the 500 ms settle
            // window.  This never delays an ordinary request without an exact
            // observed gap, and avoids waiting on large/ambiguous tails.
            if input.state_age < MAX_FOREGROUND_WAIT
                && input.cache_instability_score <= MAX_STABLE_INSTABILITY_SCORE
                && input.avoidable_tokens <= 4_096
                && input.fine_avoidable_tokens > 0
                && input.fine_avoidable_tokens <= 4_096
            {
                if !input.current_tail_is_settle_safe {
                    return skip("avoidable_gap_current_tail_unreliable", false);
                }
                let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
                if request_budget.is_zero() {
                    return skip("local_guard_budget_exhausted", true);
                }
                let requested = MAX_FOREGROUND_WAIT.saturating_sub(input.state_age);
                let wait = requested.min(request_budget);
                return PrefixControlDecision {
                    wait,
                    reason: Some("responses_first_exact_avoidable_settle"),
                    skip_reason: None,
                    budget_exhausted: wait < requested,
                };
            }
            return skip("avoidable_gap_not_repeated", false);
        }
        let wait_avoidable_tokens = if input.cache_instability_score > MAX_STABLE_INSTABILITY_SCORE
        {
            if !input.tiny_instability_recovery_safe
                || input.settle_after_cold_read
                || !matches!(input.avoidable_tokens, 128 | 256)
                || !matches!(input.fine_avoidable_tokens, 128 | 256)
            {
                return skip("avoidable_gap_unstable", false);
            }
            input.fine_avoidable_tokens
        } else {
            input.avoidable_tokens
        };

        let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
        if request_budget.is_zero() {
            return skip("local_guard_budget_exhausted", true);
        }

        let requested = requested_wait(wait_avoidable_tokens, input.avoidable_shortfall_streak);
        let wait = requested.min(request_budget);
        PrefixControlDecision {
            wait,
            reason: Some("responses_exact_avoidable_gap"),
            skip_reason: None,
            budget_exhausted: wait < requested,
        }
    }

    pub fn classify_gap(input: PrefixGapInput<'_>) -> ProviderCacheGapBreakdown {
        let record = input.usage;
        if record.input_tokens < 1024 {
            return ProviderCacheGapBreakdown {
                total_tokens: 0,
                new_tail_tokens: 0,
                avoidable_tokens: 0,
                provider_unstable_tokens: 0,
            };
        }

        let total_tokens = provider_cache_shortfall(record);
        if input.static_wire_drift {
            // The prior waterline belongs to a different frozen wire shape.
            // Its gap can be useful diagnostic evidence, but it is neither a
            // tail miss nor a local settle opportunity for this request.
            return ProviderCacheGapBreakdown {
                total_tokens,
                new_tail_tokens: total_tokens,
                avoidable_tokens: 0,
                provider_unstable_tokens: 0,
            };
        }
        let cold_read_has_unreliable_dynamic_tail =
            responses_current_tail_makes_avoidable_unreliable(input.tail)
                || responses_tool_tail_burst(input.tail)
                || input.tail.tool_output_chars >= 80_000
                || input.tail.message_chars >= 80_000;
        let cold_read_after_warm = small_context_cold_read_after_warm(input.previous_exact, record)
            || provider_prefix_break_after_warm_state(input.previous_exact, record)
            || (provider_prefix_break_after_warm_state(input.previous_best, record)
                && cold_read_has_unreliable_dynamic_tail)
            || (input.previous_exact.is_none()
                && input.previous_best.is_none()
                && provider_prefix_break_after_warm_state(input.previous_family, record)
                && cold_read_has_unreliable_dynamic_tail)
            || (input.previous_exact.is_none()
                && input.previous_best.is_none()
                && responses_huge_dynamic_history_cold_read(record, input.tail));
        if cold_read_after_warm {
            return ProviderCacheGapBreakdown {
                total_tokens,
                new_tail_tokens: 0,
                avoidable_tokens: 0,
                provider_unstable_tokens: total_tokens,
            };
        }

        let evidence = gap_evidence(
            input.previous_best,
            record,
            input.tail,
            input.guard_budget_exhausted,
        );
        let provider_unstable_tokens = if evidence.provider_rollback {
            evidence.direct_avoidable_tokens
        } else {
            0
        };
        let avoidable_tokens = if provider_unstable_tokens > 0 || evidence.tail_granularity {
            0
        } else {
            evidence.avoidable_tokens
        };

        ProviderCacheGapBreakdown {
            total_tokens,
            new_tail_tokens: total_tokens
                .saturating_sub(avoidable_tokens)
                .saturating_sub(provider_unstable_tokens),
            avoidable_tokens,
            provider_unstable_tokens,
        }
    }

    pub fn observe(input: PrefixStateInput<'_>) -> PrefixStateObservation {
        let record = input.usage;
        if record.input_tokens == 0 {
            return PrefixStateObservation {
                next: None,
                learn_family: false,
                static_wire_drift: false,
            };
        }

        let responses_static_projection_digest = static_projection_digest_for_next(
            input.previous,
            input.final_responses_static_projection,
        );
        if static_projection_drifted(input.previous, input.final_responses_static_projection) {
            // Do not retain the old waterline or its settle/wait evidence
            // after any static frozen-wire change. The current request starts
            // a fresh baseline, so the next turn can only learn from matching
            // static wire evidence.
            return PrefixStateObservation {
                next: Some(PrefixWarmState {
                    finished_at: Instant::now(),
                    input_tokens: record.input_tokens,
                    cache_read_tokens: record.cache_read_tokens,
                    shortfall_tokens: provider_cache_shortfall(record),
                    seen_bucket_tokens: record.cache_read_tokens,
                    avoidable_shortfall_tokens: 0,
                    avoidable_shortfall_streak: 0,
                    shortfall_tokens_128: provider_cache_shortfall_128(record),
                    seen_bucket_tokens_128: record.cache_read_tokens,
                    avoidable_shortfall_tokens_128: 0,
                    small_gap_recovery_streak: 0,
                    recent_clean_tiny_gap_streak: 0,
                    cache_instability_score: 0,
                    settle_after_cold_read: false,
                    tail_tool_output_chars: input.tail.tool_output_chars,
                    tail_largest_tool_output_chars: input.tail.largest_tool_output_chars,
                    tail_tool_output_noise_hint: input.tail.tool_output_noise_hint.clone(),
                    responses_static_projection_digest,
                }),
                learn_family: false,
                static_wire_drift: true,
            };
        }

        let small_context_cold_read = small_context_cold_read_after_warm(input.previous, record);
        let prefix_break_after_warm = small_context_cold_read
            || provider_prefix_break_after_warm_state(input.previous, record)
                && (record.input_tokens >= 32_000 || responses_tool_tail_burst(input.tail));
        let huge_dynamic_history_cold_read =
            responses_huge_dynamic_history_cold_read(record, input.tail);
        let weak_full_retry_after_session_delta =
            provider_prefix_weak_full_retry_after_session_delta(
                input.previous,
                record,
                input.retried_full_response,
            );
        if prefix_break_after_warm
            || weak_full_retry_after_session_delta
            || huge_dynamic_history_cold_read
        {
            let mut preserved = input.previous.cloned().unwrap_or_else(|| PrefixWarmState {
                finished_at: Instant::now(),
                input_tokens: record.input_tokens,
                cache_read_tokens: record.cache_read_tokens,
                shortfall_tokens: provider_cache_shortfall(record),
                seen_bucket_tokens: record.cache_read_tokens,
                avoidable_shortfall_tokens: 0,
                avoidable_shortfall_streak: 0,
                shortfall_tokens_128: provider_cache_shortfall_128(record),
                seen_bucket_tokens_128: record.cache_read_tokens,
                avoidable_shortfall_tokens_128: 0,
                small_gap_recovery_streak: 0,
                recent_clean_tiny_gap_streak: 0,
                cache_instability_score: 0,
                settle_after_cold_read: true,
                tail_tool_output_chars: input.tail.tool_output_chars,
                tail_largest_tool_output_chars: input.tail.largest_tool_output_chars,
                tail_tool_output_noise_hint: input.tail.tool_output_noise_hint.clone(),
                responses_static_projection_digest: responses_static_projection_digest.clone(),
            });
            preserved.finished_at = Instant::now();
            let instability_bump = if prefix_break_after_warm || huge_dynamic_history_cold_read {
                2
            } else {
                1
            };
            preserved.cache_instability_score = preserved
                .cache_instability_score
                .saturating_add(instability_bump)
                .min(8);
            preserved.settle_after_cold_read =
                prefix_break_after_warm || huge_dynamic_history_cold_read;
            preserved.shortfall_tokens = provider_cache_shortfall(record);
            preserved.shortfall_tokens_128 = provider_cache_shortfall_128(record);
            preserved.avoidable_shortfall_tokens = 0;
            preserved.avoidable_shortfall_tokens_128 = 0;
            preserved.avoidable_shortfall_streak = 0;
            preserved.small_gap_recovery_streak = 0;
            preserved.recent_clean_tiny_gap_streak = 0;
            preserved.tail_tool_output_chars = input.tail.tool_output_chars;
            preserved.tail_largest_tool_output_chars = input.tail.largest_tool_output_chars;
            preserved.tail_tool_output_noise_hint = input.tail.tool_output_noise_hint.clone();
            preserved.responses_static_projection_digest = responses_static_projection_digest;
            return PrefixStateObservation {
                next: Some(preserved),
                learn_family: false,
                static_wire_drift: false,
            };
        }

        if !provider_prefix_usage_is_safe_to_learn(
            input.previous,
            record,
            input.used_response_session,
            input.retried_full_response,
        ) {
            return PrefixStateObservation {
                next: None,
                learn_family: false,
                static_wire_drift: false,
            };
        }

        let evidence = gap_evidence(
            input.previous,
            record,
            input.tail,
            input.guard_budget_exhausted,
        );
        let avoidable_shortfall_streak =
            if evidence.avoidable_tokens > 0 || evidence.avoidable_tokens_128 > 0 {
                input
                    .previous
                    .map(|state| state.avoidable_shortfall_streak.saturating_add(1))
                    .unwrap_or(1)
            } else {
                0
            };
        let shortfall_tokens = provider_cache_shortfall(record);
        let shortfall_tokens_128 = provider_cache_shortfall_128(record);
        let small_gap_signal = shortfall_tokens_128 > 0 && shortfall_tokens_128 <= 2048;
        let small_gap_recovery_streak = if small_gap_signal {
            input
                .previous
                .map(|state| state.small_gap_recovery_streak.saturating_add(1))
                .unwrap_or(1)
        } else if input
            .previous
            .map(|state| state.small_gap_recovery_streak > 0 && shortfall_tokens_128 == 0)
            .unwrap_or(false)
        {
            1
        } else {
            0
        };
        let previous_instability = input
            .previous
            .map(|state| state.cache_instability_score)
            .unwrap_or(0);
        let large_avoidable = evidence.avoidable_tokens.max(evidence.avoidable_tokens_128);
        let stable_recovery = previous_instability > 0
            && record.input_tokens >= STABLE_RECOVERY_MIN_INPUT_TOKENS
            && input
                .previous
                .is_some_and(|state| state.input_tokens >= STABLE_RECOVERY_MIN_INPUT_TOKENS)
            && record.cache_read_tokens > 0
            && provider_cache_ratio(record).unwrap_or_default() >= STABLE_RECOVERY_MIN_RATIO
            && large_avoidable <= STABLE_RECOVERY_MAX_AVOIDABLE_TOKENS
            && !evidence.provider_rollback
            && !evidence.tail_granularity
            && !responses_current_tail_makes_avoidable_unreliable(input.tail)
            && !responses_tool_tail_burst(input.tail);
        let severe_cold_read = record.cache_read_tokens == 0
            && (provider_cache_bucket_max(record.input_tokens) >= 32_000
                || small_context_cold_read);
        let cache_instability_score = if severe_cold_read {
            previous_instability.saturating_add(3).min(8)
        } else if evidence.provider_rollback {
            previous_instability.saturating_add(1).min(8)
        } else if stable_recovery {
            previous_instability.saturating_sub(1)
        } else if large_avoidable >= 4096 {
            previous_instability.saturating_add(2).min(8)
        } else if large_avoidable > 0 {
            previous_instability.saturating_add(1).min(8)
        } else if shortfall_tokens_128 <= 512 && record.cache_read_tokens >= 1024 {
            previous_instability.saturating_sub(1)
        } else {
            previous_instability
        };
        let clean_tiny_gap_recovery = stable_recovery
            && matches!(evidence.avoidable_tokens_128, 128 | 256)
            && matches!(
                evidence.avoidable_tokens.max(evidence.avoidable_tokens_128),
                128 | 256
            )
            && responses_tiny_instability_recovery_tail_is_clean(input.tail);
        let recent_clean_tiny_gap_streak = if clean_tiny_gap_recovery {
            input
                .previous
                .map(|state| state.recent_clean_tiny_gap_streak.saturating_add(1))
                .unwrap_or(1)
        } else {
            0
        };
        let learn_sent_bucket = !evidence.provider_rollback
            && !evidence.tail_granularity
            && should_learn_sent_provider_bucket(
                input.previous,
                record,
                shortfall_tokens,
                shortfall_tokens_128,
                input.tail,
                input.used_response_session,
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
        let seen_bucket_tokens = if evidence.provider_rollback || evidence.tail_granularity {
            record.cache_read_tokens.max(sent_bucket_tokens)
        } else {
            evidence
                .previous_seen_bucket
                .max(record.cache_read_tokens)
                .max(sent_bucket_tokens)
        };
        let seen_bucket_tokens_128 = if evidence.provider_rollback || evidence.tail_granularity {
            record.cache_read_tokens.max(sent_bucket_tokens_128)
        } else {
            evidence
                .previous_seen_bucket_128
                .max(record.cache_read_tokens)
                .max(sent_bucket_tokens_128)
        };
        let next = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: record.input_tokens,
            cache_read_tokens: record.cache_read_tokens,
            shortfall_tokens,
            seen_bucket_tokens,
            avoidable_shortfall_tokens: evidence.avoidable_tokens,
            avoidable_shortfall_streak,
            shortfall_tokens_128,
            seen_bucket_tokens_128,
            avoidable_shortfall_tokens_128: evidence.avoidable_tokens_128,
            small_gap_recovery_streak,
            recent_clean_tiny_gap_streak,
            cache_instability_score,
            settle_after_cold_read: severe_cold_read,
            tail_tool_output_chars: input.tail.tool_output_chars,
            tail_largest_tool_output_chars: input.tail.largest_tool_output_chars,
            tail_tool_output_noise_hint: input.tail.tool_output_noise_hint.clone(),
            responses_static_projection_digest,
        };

        PrefixStateObservation {
            next: Some(next),
            learn_family: !evidence.provider_rollback
                && should_learn_provider_prefix_family_state(record, input.tail),
            static_wire_drift: false,
        }
    }
}

fn static_projection_drifted(
    previous: Option<&PrefixWarmState>,
    final_projection: FinalResponsesStaticProjection<'_>,
) -> bool {
    let FinalResponsesStaticProjection::Observed(current) = final_projection else {
        return false;
    };
    previous
        .is_some_and(|previous| previous.responses_static_projection_digest.as_deref() != current)
}

fn static_projection_digest_for_next(
    previous: Option<&PrefixWarmState>,
    final_projection: FinalResponsesStaticProjection<'_>,
) -> Option<String> {
    match final_projection {
        FinalResponsesStaticProjection::NotApplicable => {
            previous.and_then(|previous| previous.responses_static_projection_digest.clone())
        }
        FinalResponsesStaticProjection::Observed(digest) => digest.map(ToOwned::to_owned),
    }
}

fn gap_evidence(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    tail: &TailInputDiagnostics,
    guard_budget_exhausted: bool,
) -> GapEvidence {
    let (previous_seen_bucket, previous_seen_bucket_128) = previous
        .map(|state| provider_prefix_calibrated_previous_seen_buckets(state, record))
        .unwrap_or((0, 0));
    let direct_avoidable_tokens = previous_seen_bucket
        .min(provider_cache_bucket_max(record.input_tokens))
        .saturating_sub(record.cache_read_tokens);
    let direct_avoidable_tokens_128 = previous_seen_bucket_128
        .min(provider_cache_bucket_max_128(record.input_tokens))
        .saturating_sub(record.cache_read_tokens);
    let avoidable_unreliable = responses_current_tail_makes_avoidable_unreliable(tail);
    let tail_granularity = responses_small_avoidable_tail_granularity(
        previous,
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_tokens_128,
        tail,
    );
    let provider_rollback = small_context_provider_waterline_jitter(
        previous,
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_tokens_128,
    ) || responses_cap_exhausted_provider_waterline_rollback(
        previous,
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_tokens_128,
        tail,
        guard_budget_exhausted,
    ) || responses_high_instability_provider_waterline_rollback(
        previous,
        record,
        direct_avoidable_tokens,
        tail,
    );
    let suppress_avoidable = provider_rollback || tail_granularity || avoidable_unreliable;

    GapEvidence {
        previous_seen_bucket,
        previous_seen_bucket_128,
        direct_avoidable_tokens,
        avoidable_tokens: (!suppress_avoidable)
            .then_some(direct_avoidable_tokens)
            .unwrap_or(0),
        avoidable_tokens_128: (!suppress_avoidable)
            .then_some(direct_avoidable_tokens_128)
            .unwrap_or(0),
        tail_granularity,
        provider_rollback,
    }
}

fn responses_high_instability_provider_waterline_rollback(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    direct_avoidable_tokens: u64,
    tail: &TailInputDiagnostics,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if previous.cache_instability_score < 6
        || direct_avoidable_tokens < 8_192
        || record.input_tokens < 32_000
        || record.cache_read_tokens < 32_000
        || record.input_tokens.saturating_sub(previous.input_tokens) > 8_192
    {
        return false;
    }
    if provider_cache_ratio(record).unwrap_or_default() >= 0.97 {
        return false;
    }
    if responses_tool_tail_burst(tail)
        || tail.tool_output_chars >= 4_096
        || tail.tool_call_chars >= 4_096
        || tail.message_chars >= 4_096
    {
        return false;
    }
    true
}

fn small_context_provider_waterline_jitter(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
    current_shortfall_128: u64,
    direct_avoidable_tokens_128: u64,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens)
        .max(previous.cache_read_tokens);
    let current_bucket = provider_cache_bucket_max_128(record.input_tokens);
    if previous_seen < 1024
        || previous_seen >= 32_000
        || current_bucket < 1024
        || current_bucket >= 32_000
        || direct_avoidable_tokens_128 == 0
        || direct_avoidable_tokens_128 > 512
        || current_shortfall_128 == 0
        || current_shortfall_128 > 1024
    {
        return false;
    }
    if current_bucket.saturating_add(1024) < previous_seen
        || record.cache_read_tokens.saturating_add(1024) < previous_seen
    {
        return false;
    }

    // One small-context bucket can move between otherwise equivalent provider
    // reads. A single observation is not evidence that a local wait was useful.
    record.cache_read_tokens.saturating_mul(10) >= current_bucket.saturating_mul(9)
}

fn small_context_cold_read_after_warm(
    previous: Option<&PrefixWarmState>,
    record: &UsageRecord,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let previous_seen = previous
        .seen_bucket_tokens_128
        .max(previous.seen_bucket_tokens)
        .max(previous.cache_read_tokens);
    let current_bucket = provider_cache_bucket_max(record.input_tokens);
    if previous_seen < 1024
        || previous_seen >= 32_000
        || current_bucket < 1024
        || current_bucket >= 32_000
        || record.cache_read_tokens != 0
    {
        return false;
    }
    current_bucket.saturating_add(1024) >= previous_seen
}

fn requested_wait(avoidable_tokens: u64, avoidable_shortfall_streak: u32) -> Duration {
    let gap_ms = avoidable_tokens.min(4_096).saturating_mul(250) / 4_096;
    let streak_ms = avoidable_shortfall_streak
        .saturating_sub(MIN_REPEATED_AVOIDABLE_STREAK)
        .min(5) as u64
        * 75;
    Duration::from_millis(
        100u64
            .saturating_add(gap_ms)
            .saturating_add(streak_ms)
            .min(MAX_FOREGROUND_WAIT.as_millis() as u64),
    )
}

fn skip(reason: &'static str, budget_exhausted: bool) -> PrefixControlDecision {
    PrefixControlDecision {
        wait: Duration::ZERO,
        reason: None,
        skip_reason: Some(reason),
        budget_exhausted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(avoidable_tokens: u64) -> PrefixControlInput {
        PrefixControlInput {
            source_is_exact: true,
            avoidable_tokens,
            fine_avoidable_tokens: avoidable_tokens,
            avoidable_shortfall_streak: 0,
            cache_instability_score: 0,
            tiny_instability_recovery_safe: false,
            current_tail_is_settle_safe: true,
            settle_after_cold_read: false,
            compaction_requested: false,
            state_age: Duration::ZERO,
            request_budget: MAX_FOREGROUND_WAIT,
        }
    }

    fn repeated_input(avoidable_tokens: u64, streak: u32) -> PrefixControlInput {
        PrefixControlInput {
            avoidable_shortfall_streak: streak,
            ..input(avoidable_tokens)
        }
    }

    #[test]
    fn no_evidence_means_zero_wait() {
        let decision = PrefixController::before_request(input(0));
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("no_avoidable_gap"));
    }

    #[test]
    fn recent_cold_read_waits_only_for_remaining_settle_window() {
        let decision = PrefixController::before_request(PrefixControlInput {
            settle_after_cold_read: true,
            state_age: Duration::from_millis(138),
            ..input(0)
        });
        assert_eq!(decision.wait, Duration::from_millis(362));
        assert_eq!(decision.reason, Some("responses_recent_cold_read_settle"));

        let settled = PrefixController::before_request(PrefixControlInput {
            settle_after_cold_read: true,
            state_age: MAX_FOREGROUND_WAIT,
            ..input(0)
        });
        assert_eq!(settled.wait, Duration::ZERO);
    }

    #[test]
    fn compaction_waits_for_recent_unstable_prefix_only() {
        let decision = PrefixController::before_request(PrefixControlInput {
            cache_instability_score: 2,
            compaction_requested: true,
            state_age: Duration::from_millis(364),
            ..input(0)
        });
        assert_eq!(decision.wait, Duration::from_millis(136));
        assert_eq!(decision.reason, Some("responses_compaction_prefix_settle"));

        let stable = PrefixController::before_request(PrefixControlInput {
            compaction_requested: true,
            state_age: Duration::from_millis(364),
            ..input(0)
        });
        assert_eq!(stable.wait, Duration::ZERO);
    }

    #[test]
    fn non_exact_state_never_waits() {
        let decision = PrefixController::before_request(PrefixControlInput {
            source_is_exact: false,
            ..input(4_096)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("non_exact_prefix_state"));
    }

    #[test]
    fn first_exact_avoidable_evidence_does_not_wait() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: Duration::from_secs(9),
            ..input(768)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("avoidable_gap_not_repeated"));
    }

    #[test]
    fn fresh_first_exact_avoidable_evidence_waits_for_the_settle_window() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: Duration::from_millis(140),
            ..input(256)
        });
        assert_eq!(decision.wait, Duration::from_millis(360));
        assert_eq!(
            decision.reason,
            Some("responses_first_exact_avoidable_settle")
        );
        assert!(!decision.budget_exhausted);
    }

    #[test]
    fn fresh_first_exact_avoidable_evidence_skips_an_unreliable_current_tail() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: Duration::from_millis(140),
            current_tail_is_settle_safe: false,
            ..input(256)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(
            decision.skip_reason,
            Some("avoidable_gap_current_tail_unreliable")
        );
    }

    #[test]
    fn repeated_exact_avoidable_evidence_adapts_within_half_second() {
        let short = PrefixController::before_request(repeated_input(512, 2));
        let larger = PrefixController::before_request(repeated_input(4_096, 4));

        assert!(short.wait > Duration::ZERO);
        assert!(larger.wait > short.wait);
        assert!(larger.wait <= MAX_FOREGROUND_WAIT);
        assert_eq!(larger.reason, Some("responses_exact_avoidable_gap"));
    }

    #[test]
    fn unstable_repeated_evidence_does_not_wait() {
        let decision = PrefixController::before_request(PrefixControlInput {
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            ..repeated_input(4_096, 3)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("avoidable_gap_unstable"));
    }

    #[test]
    fn unstable_exact_tiny_gap_recovers_only_with_clean_tail_evidence() {
        for avoidable_tokens in [128, 256] {
            let recovered = PrefixController::before_request(PrefixControlInput {
                cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
                tiny_instability_recovery_safe: true,
                ..repeated_input(avoidable_tokens, 2)
            });
            assert!(recovered.wait > Duration::ZERO);
            assert!(recovered.wait <= MAX_FOREGROUND_WAIT);
            assert_eq!(recovered.reason, Some("responses_exact_avoidable_gap"));
        }

        let coarse_512_with_fine_256 = PrefixController::before_request(PrefixControlInput {
            avoidable_tokens: 512,
            fine_avoidable_tokens: 256,
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            tiny_instability_recovery_safe: true,
            ..repeated_input(512, 2)
        });
        assert_eq!(coarse_512_with_fine_256.wait, Duration::ZERO);
        assert_eq!(
            coarse_512_with_fine_256.skip_reason,
            Some("avoidable_gap_unstable")
        );

        let coarse_512 = PrefixController::before_request(PrefixControlInput {
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            tiny_instability_recovery_safe: true,
            ..repeated_input(512, 2)
        });
        assert_eq!(coarse_512.wait, Duration::ZERO);
        assert_eq!(coarse_512.skip_reason, Some("avoidable_gap_unstable"));

        let large_coarse_gap = PrefixController::before_request(PrefixControlInput {
            avoidable_tokens: 4_096,
            fine_avoidable_tokens: 256,
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            tiny_instability_recovery_safe: true,
            ..repeated_input(4_096, 2)
        });
        assert_eq!(large_coarse_gap.wait, Duration::ZERO);
        assert_eq!(large_coarse_gap.skip_reason, Some("avoidable_gap_unstable"));

        let unsafe_tail = PrefixController::before_request(PrefixControlInput {
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            tiny_instability_recovery_safe: false,
            ..repeated_input(512, 2)
        });
        assert_eq!(unsafe_tail.wait, Duration::ZERO);
        assert_eq!(unsafe_tail.skip_reason, Some("avoidable_gap_unstable"));

        let cold_read = PrefixController::before_request(PrefixControlInput {
            cache_instability_score: MAX_STABLE_INSTABILITY_SCORE + 1,
            tiny_instability_recovery_safe: true,
            settle_after_cold_read: true,
            state_age: Duration::from_millis(500),
            ..repeated_input(512, 2)
        });
        assert_eq!(cold_read.wait, Duration::ZERO);
        assert_eq!(cold_read.skip_reason, Some("avoidable_gap_unstable"));
    }

    #[test]
    fn repeated_evidence_remains_actionable_after_nine_seconds() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: Duration::from_secs(9),
            ..repeated_input(768, 2)
        });
        assert!(decision.wait > Duration::ZERO);
        assert!(decision.wait <= MAX_FOREGROUND_WAIT);
        assert_eq!(decision.reason, Some("responses_exact_avoidable_gap"));
    }

    #[test]
    fn expired_evidence_never_waits() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: MAX_EXACT_EVIDENCE_AGE,
            ..input(4_096)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("avoidable_evidence_expired"));
    }

    #[test]
    fn request_budget_is_a_hard_ceiling() {
        let decision = PrefixController::before_request(PrefixControlInput {
            request_budget: Duration::from_millis(120),
            ..repeated_input(64_000, 4)
        });
        assert_eq!(decision.wait, Duration::from_millis(120));
        assert!(decision.budget_exhausted);
    }

    #[test]
    fn all_tail_sizes_share_the_same_bounded_policy() {
        for tokens in [
            128, 256, 512, 1_024, 4_096, 16_384, 65_536, 262_144, 524_288,
        ] {
            let decision = PrefixController::before_request(repeated_input(tokens, 2));
            assert!(decision.wait > Duration::ZERO);
            assert!(decision.wait <= MAX_FOREGROUND_WAIT);
            assert_eq!(decision.reason, Some("responses_exact_avoidable_gap"));
        }
    }

    #[test]
    fn high_instability_waterline_rollback_is_provider_unstable() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 261_591,
            cache_read_tokens: 251_443,
            shortfall_tokens: 10_240,
            seen_bucket_tokens: 251_443,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 10_148,
            seen_bucket_tokens_128: 251_392,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 261_880,
            cache_read_tokens: 234_958,
            ..UsageRecord::default()
        };
        let gap = PrefixController::classify_gap(PrefixGapInput {
            previous_exact: Some(&previous),
            previous_best: Some(&previous),
            previous_family: None,
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            guard_budget_exhausted: false,
            static_wire_drift: false,
        });

        assert_eq!(gap.total_tokens, 26_674);
        assert_eq!(gap.provider_unstable_tokens, 16_485);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, 10_189);

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("successful usage must update state");
        assert_eq!(next.seen_bucket_tokens, 234_958);
        assert_eq!(next.seen_bucket_tokens_128, 234_958);
        assert_eq!(next.avoidable_shortfall_tokens, 0);
        assert_eq!(next.avoidable_shortfall_tokens_128, 0);
        assert_eq!(next.avoidable_shortfall_streak, 0);
        assert_eq!(next.recent_clean_tiny_gap_streak, 0);
        assert!(!observation.learn_family);
    }

    #[test]
    fn static_wire_drift_resets_the_waterline_without_creating_avoidable_gap() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 100_000,
            cache_read_tokens: 99_840,
            shortfall_tokens: 160,
            seen_bucket_tokens: 99_840,
            avoidable_shortfall_tokens: 128,
            avoidable_shortfall_streak: 2,
            shortfall_tokens_128: 160,
            seen_bucket_tokens_128: 99_840,
            avoidable_shortfall_tokens_128: 128,
            small_gap_recovery_streak: 2,
            recent_clean_tiny_gap_streak: MIN_CLEAN_TINY_RECOVERY_STREAK,
            cache_instability_score: 1,
            settle_after_cold_read: true,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: Some("static-a".to_string()),
        };
        let record = UsageRecord {
            input_tokens: 100_512,
            cache_read_tokens: 96_000,
            ..UsageRecord::default()
        };
        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::Observed(Some(
                "static-b",
            )),
        });

        assert!(observation.static_wire_drift);
        assert!(!observation.learn_family);
        let next = observation.next.expect("drift must seed a fresh baseline");
        assert_eq!(
            next.responses_static_projection_digest.as_deref(),
            Some("static-b")
        );
        assert_eq!(next.avoidable_shortfall_tokens, 0);
        assert_eq!(next.avoidable_shortfall_tokens_128, 0);
        assert!(!next.settle_after_cold_read);

        let gap = PrefixController::classify_gap(PrefixGapInput {
            previous_exact: Some(&previous),
            previous_best: Some(&previous),
            previous_family: None,
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            guard_budget_exhausted: false,
            static_wire_drift: true,
        });
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.provider_unstable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, gap.total_tokens);
    }

    #[test]
    fn cold_read_clears_clean_tiny_gap_recovery_evidence() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 64_000,
            cache_read_tokens: 63_488,
            shortfall_tokens: 512,
            seen_bucket_tokens: 63_488,
            avoidable_shortfall_tokens: 256,
            avoidable_shortfall_streak: 2,
            shortfall_tokens_128: 512,
            seen_bucket_tokens_128: 63_488,
            avoidable_shortfall_tokens_128: 256,
            small_gap_recovery_streak: 2,
            recent_clean_tiny_gap_streak: MIN_CLEAN_TINY_RECOVERY_STREAK,
            cache_instability_score: 3,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let cold_read = UsageRecord {
            input_tokens: 64_512,
            cache_read_tokens: 0,
            ..UsageRecord::default()
        };

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &cold_read,
            tail: &TailInputDiagnostics::default(),
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("cold read must preserve a guarded prefix state");
        assert_eq!(next.recent_clean_tiny_gap_streak, 0);
    }

    #[test]
    fn clean_fine_gap_recovery_builds_runtime_only_wait_evidence() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 99_200,
            cache_read_tokens: 99_008,
            shortfall_tokens: 192,
            seen_bucket_tokens: 99_008,
            avoidable_shortfall_tokens: 256,
            avoidable_shortfall_streak: 1,
            shortfall_tokens_128: 192,
            seen_bucket_tokens_128: 99_008,
            avoidable_shortfall_tokens_128: 256,
            small_gap_recovery_streak: 1,
            recent_clean_tiny_gap_streak: 1,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 100_000,
            cache_read_tokens: 98_752,
            ..UsageRecord::default()
        };

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("clean fine-gap usage must update prefix state");
        assert_eq!(next.avoidable_shortfall_tokens_128, 256);
        assert_eq!(next.avoidable_shortfall_streak, 2);
        assert_eq!(
            next.recent_clean_tiny_gap_streak,
            MIN_CLEAN_TINY_RECOVERY_STREAK
        );
        assert_eq!(next.cache_instability_score, 7);

        let decision = PrefixController::before_request(PrefixControlInput {
            source_is_exact: true,
            avoidable_tokens: next
                .avoidable_shortfall_tokens
                .max(next.avoidable_shortfall_tokens_128),
            fine_avoidable_tokens: next.avoidable_shortfall_tokens_128,
            avoidable_shortfall_streak: next.avoidable_shortfall_streak,
            cache_instability_score: next.cache_instability_score,
            tiny_instability_recovery_safe: next.recent_clean_tiny_gap_streak
                >= MIN_CLEAN_TINY_RECOVERY_STREAK,
            current_tail_is_settle_safe: true,
            settle_after_cold_read: next.settle_after_cold_read,
            compaction_requested: false,
            state_age: Duration::ZERO,
            request_budget: MAX_FOREGROUND_WAIT,
        });
        assert!(decision.wait > Duration::ZERO);
        assert!(decision.wait <= MAX_FOREGROUND_WAIT);
    }

    #[test]
    fn tool_call_tail_cannot_build_clean_fine_gap_recovery_evidence() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 99_200,
            cache_read_tokens: 99_008,
            shortfall_tokens: 192,
            seen_bucket_tokens: 99_008,
            avoidable_shortfall_tokens: 256,
            avoidable_shortfall_streak: 1,
            shortfall_tokens_128: 192,
            seen_bucket_tokens_128: 99_008,
            avoidable_shortfall_tokens_128: 256,
            small_gap_recovery_streak: 1,
            recent_clean_tiny_gap_streak: 1,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 100_000,
            cache_read_tokens: 98_752,
            ..UsageRecord::default()
        };
        let tool_call_tail = TailInputDiagnostics {
            tool_call_chars: 1,
            source: Some("tool_call".to_string()),
            ..TailInputDiagnostics::default()
        };

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &tool_call_tail,
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("tool-call usage still updates the general prefix state");
        assert_eq!(next.recent_clean_tiny_gap_streak, 0);
    }

    #[test]
    fn high_instability_real_tool_tail_is_not_provider_rollback() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 250_000,
            cache_read_tokens: 240_000,
            shortfall_tokens: 9_856,
            seen_bucket_tokens: 240_000,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 9_984,
            seen_bucket_tokens_128: 240_000,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 40_000,
            tail_largest_tool_output_chars: 40_000,
            tail_tool_output_noise_hint: Some("path_like".to_string()),
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 252_000,
            cache_read_tokens: 220_000,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 40_000,
            largest_tool_output_chars: 40_000,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        let gap = PrefixController::classify_gap(PrefixGapInput {
            previous_exact: Some(&previous),
            previous_best: Some(&previous),
            previous_family: None,
            usage: &record,
            tail: &tail,
            guard_budget_exhausted: false,
            static_wire_drift: false,
        });

        assert_eq!(gap.provider_unstable_tokens, 0);
        assert_eq!(gap.avoidable_tokens, 0);
        assert_eq!(gap.new_tail_tokens, gap.total_tokens);
    }

    #[test]
    fn high_instability_score_decays_after_stable_high_hit() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 198_000,
            cache_read_tokens: 190_000,
            shortfall_tokens: 8_000,
            seen_bucket_tokens: 190_000,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 8_000,
            seen_bucket_tokens_128: 190_000,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 200_048,
            cache_read_tokens: 198_912,
            ..UsageRecord::default()
        };

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &TailInputDiagnostics::default(),
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("successful usage must update state");
        assert_eq!(next.cache_instability_score, 7);
    }

    #[test]
    fn high_instability_score_does_not_decay_during_tool_burst() {
        let previous = PrefixWarmState {
            finished_at: Instant::now(),
            input_tokens: 198_000,
            cache_read_tokens: 190_000,
            shortfall_tokens: 8_000,
            seen_bucket_tokens: 190_000,
            avoidable_shortfall_tokens: 0,
            avoidable_shortfall_streak: 0,
            shortfall_tokens_128: 8_000,
            seen_bucket_tokens_128: 190_000,
            avoidable_shortfall_tokens_128: 0,
            small_gap_recovery_streak: 0,
            recent_clean_tiny_gap_streak: 0,
            cache_instability_score: 8,
            settle_after_cold_read: false,
            tail_tool_output_chars: 0,
            tail_largest_tool_output_chars: 0,
            tail_tool_output_noise_hint: None,
            responses_static_projection_digest: None,
        };
        let record = UsageRecord {
            input_tokens: 200_048,
            cache_read_tokens: 198_912,
            ..UsageRecord::default()
        };
        let tail = TailInputDiagnostics {
            tool_output_chars: 4_096,
            largest_tool_output_chars: 4_096,
            tool_output_noise_hint: Some("repeated_lines".to_string()),
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };

        let observation = PrefixController::observe(PrefixStateInput {
            previous: Some(&previous),
            usage: &record,
            tail: &tail,
            used_response_session: false,
            retried_full_response: false,
            guard_budget_exhausted: false,
            final_responses_static_projection: FinalResponsesStaticProjection::NotApplicable,
        });
        let next = observation
            .next
            .expect("successful usage must update state");
        assert_eq!(next.cache_instability_score, 8);
    }
}
