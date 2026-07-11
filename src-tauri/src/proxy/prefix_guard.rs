use std::time::Duration;

const MAX_FOREGROUND_WAIT: Duration = Duration::from_secs(1);
const MAX_USEFUL_STATE_AGE: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponsesGuardInput {
    pub source_is_exact: bool,
    pub avoidable_tokens: u64,
    pub state_age: Duration,
    pub request_budget: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponsesGuardDecision {
    pub wait: Duration,
    pub reason: Option<&'static str>,
    pub skip_reason: Option<&'static str>,
    pub budget_exhausted: bool,
}

pub(super) fn decide_responses_guard(input: ResponsesGuardInput) -> ResponsesGuardDecision {
    if !input.source_is_exact {
        return skip("non_exact_prefix_state", false);
    }
    if input.avoidable_tokens == 0 {
        return skip("no_avoidable_gap", false);
    }
    if input.state_age >= MAX_USEFUL_STATE_AGE {
        return skip("avoidable_state_stale", false);
    }

    let request_budget = input.request_budget.min(MAX_FOREGROUND_WAIT);
    if request_budget.is_zero() {
        return skip("local_guard_budget_exhausted", true);
    }

    let freshness_budget = MAX_USEFUL_STATE_AGE.saturating_sub(input.state_age);
    let requested = requested_wait(input.avoidable_tokens);
    let wait = requested.min(freshness_budget).min(request_budget);
    if wait.is_zero() {
        return skip("settle_window_elapsed", request_budget < requested);
    }

    ResponsesGuardDecision {
        wait,
        reason: Some("responses_exact_avoidable_gap"),
        skip_reason: None,
        budget_exhausted: wait < requested && wait == request_budget,
    }
}

fn requested_wait(avoidable_tokens: u64) -> Duration {
    // Continuous scaling avoids size-specific behavior gaps while preserving a
    // short guard for small misses and the one-second foreground ceiling.
    let scaled_ms = avoidable_tokens.saturating_mul(500).saturating_div(2_048);
    Duration::from_millis(500u64.saturating_add(scaled_ms).min(1_000))
}

fn skip(reason: &'static str, budget_exhausted: bool) -> ResponsesGuardDecision {
    ResponsesGuardDecision {
        wait: Duration::ZERO,
        reason: None,
        skip_reason: Some(reason),
        budget_exhausted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(avoidable_tokens: u64) -> ResponsesGuardInput {
        ResponsesGuardInput {
            source_is_exact: true,
            avoidable_tokens,
            state_age: Duration::ZERO,
            request_budget: MAX_FOREGROUND_WAIT,
        }
    }

    #[test]
    fn no_evidence_means_zero_wait() {
        let decision = decide_responses_guard(input(0));
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("no_avoidable_gap"));
    }

    #[test]
    fn non_exact_state_never_waits() {
        let decision = decide_responses_guard(ResponsesGuardInput {
            source_is_exact: false,
            ..input(4_096)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("non_exact_prefix_state"));
    }

    #[test]
    fn stale_state_never_waits() {
        let decision = decide_responses_guard(ResponsesGuardInput {
            state_age: MAX_USEFUL_STATE_AGE,
            ..input(4_096)
        });
        assert_eq!(decision.wait, Duration::ZERO);
        assert_eq!(decision.skip_reason, Some("avoidable_state_stale"));
    }

    #[test]
    fn request_budget_is_a_hard_ceiling() {
        let decision = decide_responses_guard(ResponsesGuardInput {
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
            let decision = decide_responses_guard(input(tokens));
            assert!(decision.wait >= Duration::from_millis(500));
            assert!(decision.wait <= MAX_FOREGROUND_WAIT);
            assert_eq!(decision.reason, Some("responses_exact_avoidable_gap"));
        }
    }
}
