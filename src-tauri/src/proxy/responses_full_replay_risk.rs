//! Safe, payload-free risk policy for third-party Responses FullReplay.
//!
//! The policy intentionally receives only aggregate diagnostics.  It must
//! never retain user messages, tool output, Keys, or serialized request bytes.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ResponsesRouteScope {
    provider_id: String,
    model: String,
    channel: String,
    key_realm: String,
}

impl ResponsesRouteScope {
    pub(crate) fn new(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        channel: impl Into<String>,
        key_realm: impl Into<String>,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            model: model.into(),
            channel: channel.into(),
            key_realm: key_realm.into(),
        }
    }
}

/// A bounded process-local observation that an upstream accepted HTTP headers
/// but then rejected the Responses payload in its SSE stream.  The key realm
/// is already an opaque digest; no request material is retained.
#[derive(Debug, Default)]
pub(crate) struct ResponsesFullReplayRiskObservations {
    blocked_routes: HashMap<ResponsesRouteScope, Instant>,
}

const OBSERVATION_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const OBSERVATION_LIMIT: usize = 256;

impl ResponsesFullReplayRiskObservations {
    pub(crate) fn recently_blocked(&mut self, scope: &ResponsesRouteScope) -> bool {
        self.prune();
        self.blocked_routes.contains_key(scope)
    }

    pub(crate) fn note_blocked(&mut self, scope: ResponsesRouteScope) {
        self.prune();
        if self.blocked_routes.len() >= OBSERVATION_LIMIT
            && !self.blocked_routes.contains_key(&scope)
        {
            if let Some(oldest) = self
                .blocked_routes
                .iter()
                .min_by_key(|(_, observed_at)| **observed_at)
                .map(|(scope, _)| scope.clone())
            {
                self.blocked_routes.remove(&oldest);
            }
        }
        self.blocked_routes.insert(scope, Instant::now());
    }

    fn prune(&mut self) {
        self.blocked_routes
            .retain(|_, observed_at| observed_at.elapsed() <= OBSERVATION_TTL);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FullReplayRiskReason {
    OversizedNoisyToolHistory,
    RoutePreviouslyBlocked,
}

impl FullReplayRiskReason {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::OversizedNoisyToolHistory => "oversized_noisy_tool_history",
            Self::RoutePreviouslyBlocked => "route_previously_blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FullReplayRiskSignals {
    pub input_items: u64,
    pub tool_output_chars: u64,
    pub largest_tool_output_chars: u64,
    pub tool_output_lines: u64,
    pub repeated_line_chars: u64,
    pub timestamp_like_count: u64,
    pub path_like_count: u64,
    pub url_like_count: u64,
    pub hash_like_count: u64,
    pub json_like_chars: u64,
}

impl FullReplayRiskSignals {
    fn noise_indicators(self) -> u8 {
        [
            self.repeated_line_chars > 0,
            self.timestamp_like_count > 0,
            self.path_like_count > 0,
            self.url_like_count > 0,
            self.hash_like_count > 0,
            self.json_like_chars > 0,
        ]
        .into_iter()
        .filter(|value| *value)
        .count() as u8
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FullReplayRiskInput {
    pub is_responses_route: bool,
    pub is_full_replay: bool,
    pub has_native_continuation: bool,
    pub final_body_bytes: u64,
    pub route_previously_blocked: bool,
    pub signals: FullReplayRiskSignals,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FullReplayRiskRejection {
    pub reason: FullReplayRiskReason,
    pub final_body_bytes: u64,
    pub input_items: u64,
    pub tool_output_chars: u64,
    pub tool_output_lines: u64,
    pub noise_indicators: u8,
}

impl FullReplayRiskRejection {
    /// Bounded, aggregate-only evidence suitable for metrics and the client
    /// terminal error.  It intentionally has no provider name, Key, or body
    /// content.
    pub(crate) fn diagnostic_summary(self) -> String {
        format!(
            "reason={}; full_replay=true; final_body_bytes={}; input_items={}; tool_output_chars={}; tool_output_lines={}; noise_indicators={}",
            self.reason.label(),
            self.final_body_bytes,
            self.input_items,
            self.tool_output_chars,
            self.tool_output_lines,
            self.noise_indicators,
        )
    }
}

/// Applies only after the final wire is frozen.  Native continuation/delta
/// routes are never treated as FullReplay and therefore pass through intact.
pub(crate) fn evaluate_full_replay_risk(
    input: FullReplayRiskInput,
) -> Option<FullReplayRiskRejection> {
    if !input.is_responses_route || !input.is_full_replay || input.has_native_continuation {
        return None;
    }

    let signals = input.signals;
    let noise_indicators = signals.noise_indicators();
    let obviously_unsafe = input.final_body_bytes >= 1_500_000
        && signals.input_items >= 512
        && signals.tool_output_chars >= 512_000
        && signals.tool_output_lines >= 8_192
        && noise_indicators >= 2;
    let repeats_known_blocked_shape = input.route_previously_blocked
        && input.final_body_bytes >= 768_000
        && signals.input_items >= 256
        && (signals.tool_output_chars >= 256_000 || signals.largest_tool_output_chars >= 128_000)
        && (signals.tool_output_lines >= 4_096 || noise_indicators >= 2);

    let reason = if obviously_unsafe {
        Some(FullReplayRiskReason::OversizedNoisyToolHistory)
    } else if repeats_known_blocked_shape {
        Some(FullReplayRiskReason::RoutePreviouslyBlocked)
    } else {
        None
    }?;

    Some(FullReplayRiskRejection {
        reason,
        final_body_bytes: input.final_body_bytes,
        input_items: signals.input_items,
        tool_output_chars: signals.tool_output_chars,
        tool_output_lines: signals.tool_output_lines,
        noise_indicators,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noisy_signals() -> FullReplayRiskSignals {
        FullReplayRiskSignals {
            input_items: 948,
            tool_output_chars: 657_315,
            largest_tool_output_chars: 180_000,
            tool_output_lines: 14_871,
            repeated_line_chars: 120_000,
            timestamp_like_count: 200,
            path_like_count: 300,
            url_like_count: 0,
            hash_like_count: 100,
            json_like_chars: 200_000,
        }
    }

    #[test]
    fn unsafe_full_replay_is_rejected_without_payload_inspection() {
        let rejection = evaluate_full_replay_risk(FullReplayRiskInput {
            is_responses_route: true,
            is_full_replay: true,
            has_native_continuation: false,
            final_body_bytes: 1_764_273,
            route_previously_blocked: false,
            signals: noisy_signals(),
        })
        .expect("the confirmed dangerous shape must be rejected");

        assert_eq!(
            rejection.reason,
            FullReplayRiskReason::OversizedNoisyToolHistory
        );
        assert!(!rejection.diagnostic_summary().contains("SENSITIVE"));
    }

    #[test]
    fn native_continuation_and_small_full_replay_pass_through() {
        let base = FullReplayRiskInput {
            is_responses_route: true,
            is_full_replay: true,
            has_native_continuation: false,
            final_body_bytes: 20_000,
            route_previously_blocked: false,
            signals: FullReplayRiskSignals {
                input_items: 3,
                ..FullReplayRiskSignals::default()
            },
        };
        assert!(evaluate_full_replay_risk(base).is_none());
        assert!(evaluate_full_replay_risk(FullReplayRiskInput {
            final_body_bytes: 3_000_000,
            has_native_continuation: true,
            ..base
        })
        .is_none());
    }

    #[test]
    fn prior_route_block_only_tightens_the_same_risky_shape() {
        let mut signals = noisy_signals();
        signals.input_items = 256;
        signals.tool_output_chars = 256_000;
        signals.tool_output_lines = 4_096;
        assert_eq!(
            evaluate_full_replay_risk(FullReplayRiskInput {
                is_responses_route: true,
                is_full_replay: true,
                has_native_continuation: false,
                final_body_bytes: 768_000,
                route_previously_blocked: true,
                signals,
            })
            .map(|rejection| rejection.reason),
            Some(FullReplayRiskReason::RoutePreviouslyBlocked)
        );
    }
}
