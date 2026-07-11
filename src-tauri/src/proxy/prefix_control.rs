use std::time::{Duration, Instant};

use crate::{metrics::UsageRecord, state::PrefixWarmState};

use super::{
    provider_cache_bucket_max, provider_cache_bucket_max_128, provider_cache_shortfall,
    provider_cache_shortfall_128, provider_prefix_break_after_warm_state,
    provider_prefix_calibrated_previous_seen_buckets, provider_prefix_usage_is_safe_to_learn,
    provider_prefix_weak_full_retry_after_session_delta,
    responses_cap_exhausted_provider_waterline_rollback,
    responses_current_tail_makes_avoidable_unreliable, responses_huge_dynamic_history_cold_read,
    responses_small_avoidable_tail_granularity, responses_tool_tail_burst,
    should_learn_provider_prefix_family_state, should_learn_sent_provider_bucket,
    TailInputDiagnostics,
};

const MAX_FOREGROUND_WAIT: Duration = Duration::from_secs(1);
const MAX_EXACT_EVIDENCE_AGE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefixControlInput {
    pub source_is_exact: bool,
    pub avoidable_tokens: u64,
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
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefixStateInput<'a> {
    pub previous: Option<&'a PrefixWarmState>,
    pub usage: &'a UsageRecord,
    pub tail: &'a TailInputDiagnostics,
    pub used_response_session: bool,
    pub retried_full_response: bool,
    pub guard_budget_exhausted: bool,
}

#[derive(Debug, Clone)]
pub(super) struct PrefixStateObservation {
    pub next: Option<PrefixWarmState>,
    pub learn_family: bool,
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
        if input.avoidable_tokens == 0 {
            return skip("no_avoidable_gap", false);
        }
        if input.state_age >= MAX_EXACT_EVIDENCE_AGE {
            return skip("avoidable_evidence_expired", false);
        }

        let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
        if request_budget.is_zero() {
            return skip("local_guard_budget_exhausted", true);
        }

        let requested = requested_wait(input.avoidable_tokens);
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
        let cold_read_has_unreliable_dynamic_tail =
            responses_current_tail_makes_avoidable_unreliable(input.tail)
                || responses_tool_tail_burst(input.tail)
                || input.tail.tool_output_chars >= 80_000
                || input.tail.message_chars >= 80_000;
        let cold_read_after_warm =
            provider_prefix_break_after_warm_state(input.previous_exact, record)
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
            };
        }

        let prefix_break_after_warm =
            provider_prefix_break_after_warm_state(input.previous, record)
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
            let Some(mut preserved) = input.previous.cloned() else {
                return PrefixStateObservation {
                    next: None,
                    learn_family: false,
                };
            };
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
            preserved.shortfall_tokens = provider_cache_shortfall(record);
            preserved.shortfall_tokens_128 = provider_cache_shortfall_128(record);
            preserved.avoidable_shortfall_tokens = 0;
            preserved.avoidable_shortfall_tokens_128 = 0;
            preserved.avoidable_shortfall_streak = 0;
            preserved.small_gap_recovery_streak = 0;
            preserved.tail_tool_output_chars = input.tail.tool_output_chars;
            preserved.tail_largest_tool_output_chars = input.tail.largest_tool_output_chars;
            preserved.tail_tool_output_noise_hint = input.tail.tool_output_noise_hint.clone();
            return PrefixStateObservation {
                next: Some(preserved),
                learn_family: false,
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
        let severe_cold_read = record.cache_read_tokens == 0
            && provider_cache_bucket_max(record.input_tokens) >= 32_000;
        let cache_instability_score = if severe_cold_read {
            previous_instability.saturating_add(3).min(8)
        } else if evidence.provider_rollback {
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
            cache_instability_score,
            tail_tool_output_chars: input.tail.tool_output_chars,
            tail_largest_tool_output_chars: input.tail.largest_tool_output_chars,
            tail_tool_output_noise_hint: input.tail.tool_output_noise_hint.clone(),
        };

        PrefixStateObservation {
            next: Some(next),
            learn_family: !evidence.provider_rollback
                && should_learn_provider_prefix_family_state(record, input.tail),
        }
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
    let provider_rollback = responses_cap_exhausted_provider_waterline_rollback(
        previous,
        record,
        provider_cache_shortfall_128(record),
        direct_avoidable_tokens_128,
        tail,
        guard_budget_exhausted,
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

fn requested_wait(avoidable_tokens: u64) -> Duration {
    let scaled_ms = avoidable_tokens.saturating_mul(500).saturating_div(2_048);
    Duration::from_millis(500u64.saturating_add(scaled_ms).min(1_000))
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
            state_age: Duration::ZERO,
            request_budget: MAX_FOREGROUND_WAIT,
        }
    }

    #[test]
    fn no_evidence_means_zero_wait() {
        let decision = PrefixController::before_request(input(0));
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("no_avoidable_gap"));
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
    fn nine_second_exact_evidence_remains_actionable() {
        let decision = PrefixController::before_request(PrefixControlInput {
            state_age: Duration::from_secs(9),
            ..input(768)
        });
        assert!(decision.wait >= Duration::from_millis(500));
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
            ..input(64_000)
        });
        assert_eq!(decision.wait, Duration::from_millis(120));
        assert!(decision.budget_exhausted);
    }

    #[test]
    fn all_tail_sizes_share_the_same_bounded_policy() {
        for tokens in [
            128, 256, 512, 1_024, 4_096, 16_384, 65_536, 262_144, 524_288,
        ] {
            let decision = PrefixController::before_request(input(tokens));
            assert!(decision.wait >= Duration::from_millis(500));
            assert!(decision.wait <= MAX_FOREGROUND_WAIT);
            assert_eq!(decision.reason, Some("responses_exact_avoidable_gap"));
        }
    }
}
