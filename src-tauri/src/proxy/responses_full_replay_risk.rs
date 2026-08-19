//! Payload-free observations for third-party Responses FullReplay failures.
//!
//! These observations are diagnostic only. They must never reject, delay, or
//! otherwise change an outbound request, and they never retain user messages,
//! tool output, Keys, or serialized request bytes.

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
    blocked_routes: HashMap<ResponsesRouteScope, BlockedRouteObservation>,
}

const OBSERVATION_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const OBSERVATION_LIMIT: usize = 256;

/// An aggregate-only shape of a full-replay payload that an upstream accepted
/// at HTTP level and then explicitly blocked in its SSE stream.  This must
/// never carry request text, tool output, response ids, or key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockedFullReplayShape {
    pub final_body_bytes: u64,
    pub input_items: u64,
    pub tool_output_chars: u64,
    pub largest_tool_output_chars: u64,
    pub tool_output_lines: u64,
    pub noise_indicators: u8,
}

impl BlockedFullReplayShape {
    pub(crate) fn from_signals(final_body_bytes: u64, signals: FullReplayRiskSignals) -> Self {
        Self {
            final_body_bytes,
            input_items: signals.input_items,
            tool_output_chars: signals.tool_output_chars,
            largest_tool_output_chars: signals.largest_tool_output_chars,
            tool_output_lines: signals.tool_output_lines,
            noise_indicators: signals.noise_indicators(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockedRouteObservation {
    observed_at: Instant,
    shape: BlockedFullReplayShape,
}

impl ResponsesFullReplayRiskObservations {
    pub(crate) fn recent_blocked_shape(
        &mut self,
        scope: &ResponsesRouteScope,
    ) -> Option<BlockedFullReplayShape> {
        self.prune();
        self.blocked_routes
            .get(scope)
            .map(|observation| observation.shape)
    }

    pub(crate) fn note_blocked(
        &mut self,
        scope: ResponsesRouteScope,
        shape: BlockedFullReplayShape,
    ) {
        self.prune();
        if self.blocked_routes.len() >= OBSERVATION_LIMIT
            && !self.blocked_routes.contains_key(&scope)
        {
            if let Some(oldest) = self
                .blocked_routes
                .iter()
                .min_by_key(|(_, observation)| observation.observed_at)
                .map(|(scope, _)| scope.clone())
            {
                self.blocked_routes.remove(&oldest);
            }
        }
        self.blocked_routes.insert(
            scope,
            BlockedRouteObservation {
                observed_at: Instant::now(),
                shape,
            },
        );
    }

    fn prune(&mut self) {
        self.blocked_routes
            .retain(|_, observation| observation.observed_at.elapsed() <= OBSERVATION_TTL);
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
