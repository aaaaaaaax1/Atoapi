use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    metrics::{FinalScopeWaterlineLog, UsageRecord},
    proxy::{PredecessorProofReceipt, TailInputDiagnostics, WaterlineControlHead},
};

const WARM_PENDING_TTL: Duration = Duration::from_secs(22);
const WARM_PENDING_FOLLOWUP_WAIT: Duration = Duration::from_millis(500);
const WARM_PENDING_MAX_FOLLOWUPS: u8 = 3;
const WARM_PENDING_MAX_ENTRIES: usize = 64;

/// The immutable fact that made an exact successor worth holding briefly.
///
/// These labels deliberately describe only aggregate request shape.  They are
/// never persisted, routed, or sent upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMaturityKind {
    GiantColdRoot,
    MaterialToolTail,
    ProviderWaterlineRollback,
    ExactPendingTailLag,
    ToolCallOnlyExactPendingTailLag,
}

impl PendingMaturityKind {
    fn followups(self) -> u8 {
        match self {
            // A provider can take more than one direct child to materialise a
            // genuinely cold giant root.  Keep the existing bounded chain.
            Self::GiantColdRoot => WARM_PENDING_MAX_FOLLOWUPS,
            // A normal tool tail is a different case: wait once for its exact
            // child, then fail open. A later tool result has a different
            // exact predecessor and therefore earns its own one-shot window.
            Self::MaterialToolTail => 1,
            // A proven upstream waterline rollback gets one direct successor.
            // Persistent provider loss must fail open after that one window.
            Self::ProviderWaterlineRollback => 1,
            // An exact final-scope receipt can prove that a small portion of
            // the immediately preceding sent prefix has not yet materialised.
            // It is never a new waterline: only its one direct child may wait.
            Self::ExactPendingTailLag => 1,
            // A tool-call-only parent is a separate, one-shot proof. Tool
            // output remains a hard semantic boundary and never uses it.
            Self::ToolCallOnlyExactPendingTailLag => 1,
        }
    }

    fn ready_at(self, now: Instant, expires_at: Instant) -> Instant {
        match self {
            // Preserve the historical giant-root behaviour: each bounded
            // direct child may use at most 500ms while the cold window remains
            // alive.
            Self::GiantColdRoot => expires_at,
            // A normal tail only needs its immediately following request to
            // cross the short maturation boundary.  If that child arrives
            // later, it sends immediately rather than waiting again.
            Self::MaterialToolTail => now + WARM_PENDING_FOLLOWUP_WAIT,
            Self::ProviderWaterlineRollback => now + WARM_PENDING_FOLLOWUP_WAIT,
            Self::ExactPendingTailLag => now + WARM_PENDING_FOLLOWUP_WAIT,
            Self::ToolCallOnlyExactPendingTailLag => now + WARM_PENDING_FOLLOWUP_WAIT,
        }
    }

    fn wait_reason(self) -> &'static str {
        match self {
            Self::GiantColdRoot => "responses_giant_cold_prefix_warm_pending",
            Self::MaterialToolTail => "responses_material_tool_tail_maturity_pending",
            Self::ProviderWaterlineRollback => "responses_provider_waterline_rollback_pending",
            Self::ExactPendingTailLag => "responses_exact_pending_tail_lag",
            Self::ToolCallOnlyExactPendingTailLag => "responses_tool_call_exact_pending_tail_lag",
        }
    }
}

/// A process-local, one-shot maturity gate for a giant cold FullReplay root,
/// a material tool tail, a proven provider waterline rollback, or a small
/// exact pending tail lag. It never changes a frozen request, provider, Key,
/// route, or number of upstream dispatches; it only gives a proven direct
/// child a bounded opportunity to arrive after the provider finishes warming
/// that exact prefix.
#[derive(Debug, Default)]
pub(crate) struct WarmPendingRegistry {
    entries: HashMap<String, WarmPendingEntry>,
    next_nonce: u64,
}

#[derive(Debug, Clone, Copy)]
struct PendingMaturity {
    kind: PendingMaturityKind,
}

#[derive(Debug, Clone)]
struct WarmPendingEntry {
    expected_predecessor: WaterlineControlHead,
    deadline_at: Instant,
    ready_at: Instant,
    remaining_followups: u8,
    kind: PendingMaturityKind,
    nonce: u64,
}

/// Removing an entry at claim time makes concurrent siblings fail open instead
/// of queueing.  A successful cold child may atomically arm its own direct
/// successor during terminal settlement; a failed child simply leaves nothing
/// behind.
#[derive(Debug, Clone)]
pub(super) struct WarmPendingClaim {
    key: String,
    nonce: u64,
    deadline_at: Instant,
    ready_at: Instant,
    remaining_after_claim: u8,
    kind: PendingMaturityKind,
}

impl WarmPendingClaim {
    pub(super) fn wait_duration(&self) -> Duration {
        self.ready_at
            .saturating_duration_since(Instant::now())
            .min(WARM_PENDING_FOLLOWUP_WAIT)
    }

    pub(super) fn wait_reason(&self) -> &'static str {
        self.kind.wait_reason()
    }

    /// A proven provider rollback belongs to the exact predecessor, but a
    /// large or noisy direct child still has its own real new tail. Do not add
    /// latency to that child merely to chase a prior cache waterline.
    pub(super) fn followup_is_settle_safe(
        &self,
        tail: &TailInputDiagnostics,
        compaction_requested: bool,
    ) -> bool {
        match self.kind {
            PendingMaturityKind::MaterialToolTail
            | PendingMaturityKind::ProviderWaterlineRollback => {
                !compaction_requested && direct_followup_is_settle_safe(tail)
            }
            PendingMaturityKind::ExactPendingTailLag
            | PendingMaturityKind::ToolCallOnlyExactPendingTailLag => {
                !compaction_requested && exact_pending_followup_is_settle_safe(tail)
            }
            PendingMaturityKind::GiantColdRoot => true,
        }
    }
}

impl WarmPendingRegistry {
    pub(super) fn claim(
        &mut self,
        prefix_control_key: Option<&str>,
        scope_digest: Option<&str>,
        full_replay: bool,
        predecessor: &PredecessorProofReceipt,
    ) -> Option<WarmPendingClaim> {
        self.purge_expired();
        if !full_replay || !predecessor.is_exact() {
            return None;
        }
        let expected_predecessor = predecessor.head.as_ref()?;
        let key = entry_key(prefix_control_key?, scope_digest?);
        let entry = self.entries.get(&key)?;
        if entry.expected_predecessor != *expected_predecessor || entry.remaining_followups == 0 {
            return None;
        }
        let entry = self.entries.remove(&key)?;
        Some(WarmPendingClaim {
            key,
            nonce: entry.nonce,
            deadline_at: entry.deadline_at,
            ready_at: entry.ready_at,
            remaining_after_claim: entry.remaining_followups.saturating_sub(1),
            kind: entry.kind,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn settle(
        &mut self,
        claim: Option<&WarmPendingClaim>,
        prefix_control_key: Option<&str>,
        scope_digest: Option<&str>,
        final_scope: Option<&FinalScopeWaterlineLog>,
        committed_head: Option<&WaterlineControlHead>,
        raw_usage: Option<&UsageRecord>,
        tail: &TailInputDiagnostics,
        upstream_succeeded: bool,
        confirmed_compaction: bool,
    ) {
        self.purge_expired();
        let Some(prefix_control_key) = prefix_control_key else {
            return;
        };
        let Some(scope_digest) = scope_digest else {
            return;
        };
        let key = entry_key(prefix_control_key, scope_digest);
        let now = Instant::now();
        if let Some(claim) = claim {
            // A different branch may have armed a newer chain while this
            // claimed child was in flight.  Never let the older terminal
            // result overwrite that newer exact binding.
            if self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.nonce != claim.nonce)
            {
                return;
            }
            if claim.key != key || claim.deadline_at <= now {
                return;
            }
        }

        // A claim was already removed at dispatch.  Any terminal condition that
        // cannot prove a successful, stable child intentionally clears it.
        let Some(final_scope) = final_scope else {
            return;
        };
        if !upstream_succeeded
            || confirmed_compaction
            || final_scope.outcome != "settled"
            || !final_scope.sent_prediction_eligible
            || final_scope.scope_digest != scope_digest
            || !matches!(final_scope.predecessor_proof.as_str(), "root" | "exact")
        {
            return;
        }
        let Some(committed_head) = committed_head else {
            return;
        };

        let Some(maturity) = pending_maturity(raw_usage, tail, final_scope) else {
            return;
        };
        // Exact pending tail lag is deliberately a single direct-child
        // opportunity. Do not turn an unfulfilled child into a repeated
        // foreground-wait chain; a later request must fail open and carry its
        // own ordinary evidence instead.
        if claim.is_some_and(|claim| {
            matches!(
                claim.kind,
                PendingMaturityKind::ExactPendingTailLag
                    | PendingMaturityKind::ToolCallOnlyExactPendingTailLag
            )
        }) {
            return;
        }
        let (deadline_at, ready_at, remaining_followups) = match claim {
            Some(claim) => {
                // A giant root remains one bounded chain.  A newly produced
                // material tool tail is a new maturity fact, so it receives
                // its own single direct child instead of inheriting an old
                // exhausted chain.
                if claim.kind == PendingMaturityKind::GiantColdRoot
                    && maturity.kind == PendingMaturityKind::GiantColdRoot
                {
                    if claim.remaining_after_claim == 0 {
                        return;
                    }
                    (
                        claim.deadline_at,
                        maturity.kind.ready_at(now, claim.deadline_at),
                        claim.remaining_after_claim,
                    )
                } else {
                    let deadline_at = now + WARM_PENDING_TTL;
                    (
                        deadline_at,
                        maturity.kind.ready_at(now, deadline_at),
                        maturity.kind.followups(),
                    )
                }
            }
            None => {
                let deadline_at = now + WARM_PENDING_TTL;
                (
                    deadline_at,
                    maturity.kind.ready_at(now, deadline_at),
                    maturity.kind.followups(),
                )
            }
        };
        self.arm(
            key,
            committed_head.clone(),
            deadline_at,
            ready_at,
            remaining_followups,
            maturity,
        );
    }

    fn arm(
        &mut self,
        key: String,
        expected_predecessor: WaterlineControlHead,
        deadline_at: Instant,
        ready_at: Instant,
        remaining_followups: u8,
        maturity: PendingMaturity,
    ) {
        if remaining_followups == 0 || deadline_at <= Instant::now() {
            return;
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= WARM_PENDING_MAX_ENTRIES {
            // The controller is an optimization only.  Never evict an active
            // unrelated pending chain just to add another wait.
            return;
        }
        self.next_nonce = self.next_nonce.wrapping_add(1).max(1);
        self.entries.insert(
            key,
            WarmPendingEntry {
                expected_predecessor,
                deadline_at,
                ready_at,
                remaining_followups,
                kind: maturity.kind,
                nonce: self.next_nonce,
            },
        );
    }

    fn purge_expired(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.deadline_at > now);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

fn entry_key(prefix_control_key: &str, scope_digest: &str) -> String {
    format!("{prefix_control_key}\0{scope_digest}")
}

fn giant_cold_read(raw_usage: Option<&UsageRecord>) -> bool {
    let Some(record) = raw_usage else {
        return false;
    };
    record.input_tokens >= 128_000
        && record.cache_read_tokens <= 4_096
        && record.input_tokens.saturating_sub(record.cache_read_tokens) >= 65_536
        && record.cache_read_tokens.saturating_mul(100) < record.input_tokens.saturating_mul(5)
}

/// A material tool result is real new context on its first FullReplay send,
/// but its exact successor can often reuse it once the upstream finishes
/// indexing the just-completed request.  This is intentionally limited to
/// aggregate shape: it never examines tool text, and normal user-message
/// tails keep the immediate-send path.
fn material_tool_tail(raw_usage: Option<&UsageRecord>, tail: &TailInputDiagnostics) -> bool {
    let Some(record) = raw_usage else {
        return false;
    };
    if record.input_tokens < 16_384 || tail.input_items == 0 {
        return false;
    }
    let tool_or_mixed = matches!(
        tail.source.as_deref(),
        Some("tool_output") | Some("mixed") | Some("tool_call")
    );
    tool_or_mixed
        && (tail.tool_output_chars >= 8_192
            || tail.largest_tool_output_chars >= 8_192
            || tail.tool_call_chars >= 8_192)
}

fn pending_maturity(
    raw_usage: Option<&UsageRecord>,
    tail: &TailInputDiagnostics,
    final_scope: &FinalScopeWaterlineLog,
) -> Option<PendingMaturity> {
    if giant_cold_read(raw_usage) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::GiantColdRoot,
        })
    } else if material_tool_tail_has_exact_predecessor(raw_usage, tail, final_scope) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::MaterialToolTail,
        })
    } else if provider_waterline_rollback(raw_usage, final_scope) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::ProviderWaterlineRollback,
        })
    } else if exact_pending_tail_lag(raw_usage, tail, final_scope) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::ExactPendingTailLag,
        })
    } else if tool_call_only_exact_pending_tail_lag(raw_usage, tail, final_scope) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::ToolCallOnlyExactPendingTailLag,
        })
    } else {
        None
    }
}

/// The final-scope ledger is the only accepted source for this maturity hint.
/// A raw partial cache read alone could be a genuine new tail; a rollback
/// amount with exact, bound, non-reset lineage proves that portion belonged to
/// the already-settled upstream prefix.
fn provider_waterline_rollback(
    raw_usage: Option<&UsageRecord>,
    final_scope: &FinalScopeWaterlineLog,
) -> bool {
    let Some(record) = raw_usage else {
        return false;
    };
    record.input_tokens >= 16_384
        && record.cache_read_tokens > 0
        && final_scope.predecessor_exact
        && final_scope.predecessor_bound
        && !final_scope.continuity_reset
        && final_scope.rollback_tokens_128 >= 128
        && final_scope.raw_cache_read_tokens == record.cache_read_tokens
}

/// A small residual between an exact predecessor's sent prefix and the raw
/// upstream cache read is a possible materialisation lag, not a reason to
/// advance a local waterline.  Keep this much narrower than generic tail-lag:
/// both the parent and its only direct child must satisfy the existing quiet
/// follow-up shape, and all final-scope evidence must bind the same lineage.
fn exact_pending_tail_lag(
    raw_usage: Option<&UsageRecord>,
    tail: &TailInputDiagnostics,
    final_scope: &FinalScopeWaterlineLog,
) -> bool {
    exact_pending_tail_lag_evidence(raw_usage, final_scope)
        // Tool-bearing tails have their own material-tail policy. Empirical
        // coverage keeps even a one-character tool result on the immediate
        // path, so this exact residual gate is deliberately message-only.
        // The request is never rewritten; this merely avoids adding an
        // unproven foreground wait to a semantic tool continuation.
        && exact_pending_parent_is_settle_safe(tail)
}

/// The common evidence for a small exact materialisation residual.  The parent
/// shape remains deliberately separate: ordinary messages and tool-call-only
/// continuations have different semantic safety gates.
fn exact_pending_tail_lag_evidence(
    raw_usage: Option<&UsageRecord>,
    final_scope: &FinalScopeWaterlineLog,
) -> bool {
    const MIN_INPUT_TOKENS: u64 = 16_384;
    const MIN_PENDING_TOKENS_128: u64 = 128;
    const MAX_PENDING_TOKENS_128: u64 = 2_048;

    let Some(record) = raw_usage else {
        return false;
    };
    let candidate = final_scope.candidate_avoidable_tokens_128;
    record.input_tokens >= MIN_INPUT_TOKENS
        && record.cache_read_tokens > 0
        && final_scope.outcome == "settled"
        && final_scope.sent_prediction_eligible
        && final_scope.predecessor_exact
        && final_scope.predecessor_bound
        && !final_scope.continuity_reset
        && final_scope.rollback_tokens_128 == 0
        && (MIN_PENDING_TOKENS_128..=MAX_PENDING_TOKENS_128).contains(&candidate)
        && final_scope.raw_cache_read_tokens == record.cache_read_tokens
        && final_scope.sent_prefix_bucket_tokens_128 > final_scope.settled_prefix_bucket_tokens_128
}

/// Exact residual maturation is restricted to a small, message-only parent.
/// Tool calls and tool outputs remain semantic boundaries: large ones use the
/// dedicated material-tail policy, while small ones must remain immediate.
fn exact_pending_parent_is_settle_safe(tail: &TailInputDiagnostics) -> bool {
    const MAX_INPUT_ITEMS: u64 = 6;
    const MAX_MESSAGE_CHARS: u64 = 512;
    let tool_bearing = tail.tool_call_chars > 0
        || tail.tool_output_chars > 0
        || tail.largest_tool_output_chars > 0
        || tail.tool_output_lines > 0
        || tail.tool_output_repeated_line_chars > 0
        || tail.tool_output_timestamp_like_count > 0
        || tail.tool_output_path_like_count > 0
        || tail.tool_output_url_like_count > 0
        || tail.tool_output_hash_like_count > 0
        || tail.tool_output_json_like_chars > 0
        || tail.tool_output_noise_hint.is_some()
        || matches!(
            tail.source.as_deref(),
            Some("tool_output" | "tool_call" | "mixed")
        );

    tail.input_items <= MAX_INPUT_ITEMS && tail.message_chars <= MAX_MESSAGE_CHARS && !tool_bearing
}

/// A model-produced tool call can be stable semantic history before any tool
/// result exists. Admit only that narrow shape; an output, output noise, or a
/// mixed message tail belongs to a different boundary and must send
/// immediately.
fn tool_call_only_exact_pending_tail_lag(
    raw_usage: Option<&UsageRecord>,
    tail: &TailInputDiagnostics,
    final_scope: &FinalScopeWaterlineLog,
) -> bool {
    const MAX_INPUT_ITEMS: u64 = 6;
    const MAX_TOOL_CALL_CHARS: u64 = 2_048;

    exact_pending_tail_lag_evidence(raw_usage, final_scope)
        && tail.input_items <= MAX_INPUT_ITEMS
        && tail.message_chars == 0
        && tail.tool_call_chars > 0
        && tail.tool_call_chars <= MAX_TOOL_CALL_CHARS
        && tail.tool_output_chars == 0
        && tail.largest_tool_output_chars == 0
        && tail.tool_output_lines == 0
        && tail.tool_output_repeated_line_chars == 0
        && tail.tool_output_timestamp_like_count == 0
        && tail.tool_output_path_like_count == 0
        && tail.tool_output_url_like_count == 0
        && tail.tool_output_hash_like_count == 0
        && tail.tool_output_json_like_chars == 0
        && tail.tool_output_noise_hint.is_none()
        && matches!(tail.source.as_deref(), Some("tool_call" | "mixed"))
}

fn direct_followup_is_settle_safe(tail: &TailInputDiagnostics) -> bool {
    tail.input_items <= 10
        && tail.message_chars <= 1_024
        && tail.tool_call_chars <= 4_096
        && tail.tool_output_chars < 8_000
        && tail.largest_tool_output_chars < 8_000
        && tail.tool_output_noise_hint.is_none()
}

/// The exact residual gate is intentionally stricter than the generic
/// material-tail/rollback follow-up gate. A tool-bearing child is real
/// semantic input even when its output is tiny, so it must not inherit the
/// message-only pending wait.
fn exact_pending_followup_is_settle_safe(tail: &TailInputDiagnostics) -> bool {
    tail.input_items <= 10
        && tail.message_chars > 0
        && tail.message_chars <= 1_024
        && tail.tool_call_chars == 0
        && tail.tool_output_chars == 0
        && tail.largest_tool_output_chars == 0
        && tail.tool_output_lines == 0
        && tail.tool_output_repeated_line_chars == 0
        && tail.tool_output_timestamp_like_count == 0
        && tail.tool_output_path_like_count == 0
        && tail.tool_output_url_like_count == 0
        && tail.tool_output_hash_like_count == 0
        && tail.tool_output_json_like_chars == 0
        && tail.tool_output_noise_hint.is_none()
        && tail.source.as_deref() == Some("message")
}

/// A material tool result is new context on its own request, but its exact
/// direct successor can arrive before the provider indexes that settled
/// prefix. Require the final-scope proof and a still-unsettled sent bucket;
/// the claim itself only waits for a small, quiet direct child.
fn material_tool_tail_has_exact_predecessor(
    raw_usage: Option<&UsageRecord>,
    tail: &TailInputDiagnostics,
    final_scope: &FinalScopeWaterlineLog,
) -> bool {
    let Some(record) = raw_usage else {
        return false;
    };
    if !material_tool_tail(Some(record), tail)
        || !final_scope.predecessor_exact
        || !final_scope.predecessor_bound
        || final_scope.continuity_reset
        || final_scope.raw_cache_read_tokens != record.cache_read_tokens
        || final_scope.sent_prefix_bucket_tokens_128 <= final_scope.settled_prefix_bucket_tokens_128
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(generation: u64) -> WaterlineControlHead {
        WaterlineControlHead::derive(7, generation, &format!("resp-{generation}"))
            .expect("test response id must produce a control head")
    }

    fn exact(predecessor: WaterlineControlHead) -> PredecessorProofReceipt {
        PredecessorProofReceipt::new(
            super::super::final_scope_waterline::PredecessorProofStatus::Exact,
            Some(predecessor),
            3,
            4,
        )
    }

    fn root_log(scope: &str) -> FinalScopeWaterlineLog {
        FinalScopeWaterlineLog {
            outcome: "settled".to_string(),
            scope_digest: scope.to_string(),
            sent_prediction_eligible: true,
            predecessor_proof: "root".to_string(),
            ..FinalScopeWaterlineLog::default()
        }
    }

    fn exact_log(scope: &str) -> FinalScopeWaterlineLog {
        FinalScopeWaterlineLog {
            outcome: "settled".to_string(),
            scope_digest: scope.to_string(),
            sent_prediction_eligible: true,
            predecessor_proof: "exact".to_string(),
            predecessor_exact: true,
            predecessor_bound: true,
            ..FinalScopeWaterlineLog::default()
        }
    }

    fn exact_log_with_gap(
        scope: &str,
        raw_cache_read_tokens: u64,
        candidate_avoidable_tokens_128: u64,
    ) -> FinalScopeWaterlineLog {
        FinalScopeWaterlineLog {
            raw_cache_read_tokens,
            candidate_avoidable_tokens_128,
            sent_prefix_bucket_tokens_128: raw_cache_read_tokens.saturating_add(128),
            settled_prefix_bucket_tokens_128: raw_cache_read_tokens,
            ..exact_log(scope)
        }
    }

    fn exact_pending_tail_lag_log(
        scope: &str,
        raw_cache_read_tokens: u64,
        candidate_avoidable_tokens_128: u64,
    ) -> FinalScopeWaterlineLog {
        FinalScopeWaterlineLog {
            raw_cache_read_tokens,
            candidate_avoidable_tokens_128,
            sent_prefix_bucket_tokens_128: raw_cache_read_tokens
                .saturating_add(candidate_avoidable_tokens_128),
            settled_prefix_bucket_tokens_128: raw_cache_read_tokens,
            ..exact_log(scope)
        }
    }

    fn exact_rollback_log(
        scope: &str,
        raw_cache_read_tokens: u64,
        rollback_tokens_128: u64,
    ) -> FinalScopeWaterlineLog {
        FinalScopeWaterlineLog {
            raw_cache_read_tokens,
            rollback_tokens_128,
            ..exact_log(scope)
        }
    }

    fn cold() -> UsageRecord {
        UsageRecord {
            input_tokens: 272_621,
            cache_read_tokens: 3_801,
            ..UsageRecord::default()
        }
    }

    fn warm_tool_replay() -> UsageRecord {
        UsageRecord {
            input_tokens: 96_512,
            cache_read_tokens: 88_064,
            ..UsageRecord::default()
        }
    }

    fn material_tool_tail() -> TailInputDiagnostics {
        TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 32_768,
            largest_tool_output_chars: 30_000,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        }
    }

    #[test]
    fn root_cold_read_allows_only_three_direct_exact_children() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let mut current = head(1);

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&root_log(scope)),
            Some(&current),
            Some(&cold()),
            &TailInputDiagnostics::default(),
            true,
            false,
        );
        assert_eq!(registry.len(), 1);

        for generation in 2..=4 {
            let claim = registry
                .claim(Some(prefix), Some(scope), true, &exact(current.clone()))
                .expect("the direct exact successor must claim the pending warm window");
            assert!(claim.wait_duration() > Duration::ZERO);
            assert!(claim.wait_duration() <= WARM_PENDING_FOLLOWUP_WAIT);
            assert!(
                registry
                    .claim(Some(prefix), Some(scope), true, &exact(current.clone()))
                    .is_none(),
                "a concurrent sibling must fail open rather than queue"
            );

            current = head(generation);
            registry.settle(
                Some(&claim),
                Some(prefix),
                Some(scope),
                Some(&exact_log(scope)),
                Some(&current),
                Some(&cold()),
                &TailInputDiagnostics::default(),
                true,
                false,
            );
        }

        assert_eq!(registry.len(), 0);
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(current))
                .is_none(),
            "the controller must stop after its bounded three successors"
        );
    }

    #[test]
    fn wrong_scope_head_non_exact_and_expired_entries_never_wait() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let current = head(1);
        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&root_log(scope)),
            Some(&current),
            Some(&cold()),
            &TailInputDiagnostics::default(),
            true,
            false,
        );

        assert!(registry
            .claim(
                Some(prefix),
                Some("other-scope"),
                true,
                &exact(current.clone())
            )
            .is_none());
        assert!(registry
            .claim(Some(prefix), Some(scope), true, &exact(head(99)))
            .is_none());
        assert!(registry
            .claim(
                Some(prefix),
                Some(scope),
                true,
                &PredecessorProofReceipt::root(7, 1),
            )
            .is_none());

        let key = entry_key(prefix, scope);
        registry
            .entries
            .get_mut(&key)
            .expect("root should have armed a pending entry")
            .deadline_at = Instant::now() - Duration::from_millis(1);
        assert!(registry
            .claim(Some(prefix), Some(scope), true, &exact(current))
            .is_none());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn a_no_gain_material_tail_does_not_suppress_a_new_material_tail() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 88_064, 8_192)),
            Some(&root),
            Some(&warm_tool_replay()),
            &material_tool_tail(),
            true,
            false,
        );
        assert_eq!(registry.len(), 1);

        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
            .expect("only the exact direct child may consume a material-tail maturity window");
        assert_eq!(
            claim.wait_reason(),
            "responses_material_tool_tail_maturity_pending"
        );
        assert!(claim.wait_duration() > Duration::ZERO);
        assert!(claim.wait_duration() <= WARM_PENDING_FOLLOWUP_WAIT);
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
                .is_none(),
            "a sibling must not queue behind the same material tail"
        );

        let child = head(2);
        registry.settle(
            Some(&claim),
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 88_064, 8_192)),
            Some(&child),
            Some(&warm_tool_replay()),
            &TailInputDiagnostics::default(),
            true,
            false,
        );
        assert_eq!(registry.len(), 0);
        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 88_064, 8_192)),
            Some(&child),
            Some(&warm_tool_replay()),
            &material_tool_tail(),
            true,
            false,
        );
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(child))
                .is_some(),
            "a new material tail has a new exact predecessor and must receive its own one-shot maturity window"
        );
    }

    #[test]
    fn a_recovered_material_child_can_arm_the_next_tool_tail() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 88_064, 8_192)),
            Some(&root),
            Some(&warm_tool_replay()),
            &material_tool_tail(),
            true,
            false,
        );
        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
            .expect("first material tail should have exact shortfall evidence");
        let child = head(2);
        let recovered = UsageRecord {
            input_tokens: 97_024,
            cache_read_tokens: 89_088,
            ..UsageRecord::default()
        };
        registry.settle(
            Some(&claim),
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 89_088, 4_096)),
            Some(&child),
            Some(&recovered),
            &material_tool_tail(),
            true,
            false,
        );

        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(child))
                .is_some(),
            "a new material tail may arm its own direct-child maturity window"
        );
    }

    #[test]
    fn exact_material_tool_tail_arms_a_safe_direct_child_without_prior_gap() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_log_with_gap(scope, 88_064, 0)),
            Some(&root),
            Some(&warm_tool_replay()),
            &material_tool_tail(),
            true,
            false,
        );
        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root))
            .expect("an exact material tool tail must give its safe direct child one bounded maturation window even when the prior gap was zero");
        assert_eq!(
            claim.wait_reason(),
            "responses_material_tool_tail_maturity_pending"
        );
        assert!(claim.followup_is_settle_safe(&TailInputDiagnostics::default(), false));
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    tool_output_chars: 8_000,
                    largest_tool_output_chars: 8_000,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "a new material or noisy child must retain immediate dispatch"
        );
    }

    #[test]
    fn exact_provider_waterline_rollback_arms_one_safe_direct_child() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        let rollback = UsageRecord {
            input_tokens: 211_840,
            cache_read_tokens: 180_992,
            ..UsageRecord::default()
        };

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_rollback_log(
                scope,
                rollback.cache_read_tokens,
                30_720,
            )),
            Some(&root),
            Some(&rollback),
            &TailInputDiagnostics::default(),
            true,
            false,
        );
        assert_eq!(registry.len(), 1);

        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
            .expect("only the exact direct child may use a proven rollback window");
        assert_eq!(
            claim.wait_reason(),
            "responses_provider_waterline_rollback_pending"
        );
        assert!(claim.wait_duration() > Duration::ZERO);
        assert!(claim.wait_duration() <= WARM_PENDING_FOLLOWUP_WAIT);
        assert!(claim.followup_is_settle_safe(&TailInputDiagnostics::default(), false));
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    input_items: 3,
                    tool_output_chars: 8_000,
                    largest_tool_output_chars: 8_000,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "a material new tool tail must keep its immediate-send behavior"
        );
        assert!(
            !claim.followup_is_settle_safe(&TailInputDiagnostics::default(), true),
            "compaction already owns its independent maturity boundary"
        );
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(root))
                .is_none(),
            "the provider-rollback window is one-shot and cannot queue siblings"
        );
    }

    #[test]
    fn exact_pending_tail_lag_arms_only_one_quiet_direct_child() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        let replay = UsageRecord {
            input_tokens: 158_170,
            cache_read_tokens: 157_056,
            ..UsageRecord::default()
        };
        let quiet_parent = TailInputDiagnostics {
            input_items: 3,
            message_chars: 62,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_pending_tail_lag_log(
                scope,
                replay.cache_read_tokens,
                512,
            )),
            Some(&root),
            Some(&replay),
            &quiet_parent,
            true,
            false,
        );
        assert_eq!(registry.len(), 1);

        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
            .expect("only the exact direct child may claim a proven pending tail lag");
        assert_eq!(claim.wait_reason(), "responses_exact_pending_tail_lag");
        assert!(claim.wait_duration() > Duration::ZERO);
        assert!(claim.wait_duration() <= WARM_PENDING_FOLLOWUP_WAIT);
        let quiet_message_child = TailInputDiagnostics {
            input_items: 1,
            message_chars: 8,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(claim.followup_is_settle_safe(&quiet_message_child, false));
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    tool_output_chars: 8_000,
                    largest_tool_output_chars: 8_000,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "a material direct child must retain immediate dispatch"
        );
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    input_items: 3,
                    message_chars: 4,
                    tool_output_chars: 1,
                    largest_tool_output_chars: 1,
                    source: Some("mixed".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "even a one-character tool child must retain immediate dispatch"
        );
        assert!(
            !claim.followup_is_settle_safe(&TailInputDiagnostics::default(), true),
            "compaction remains outside the pending tail-lag path"
        );
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(root))
                .is_none(),
            "the exact pending tail-lag window is one-shot and never queues a sibling"
        );

        let child = head(2);
        registry.settle(
            Some(&claim),
            Some(prefix),
            Some(scope),
            Some(&exact_pending_tail_lag_log(
                scope,
                replay.cache_read_tokens,
                512,
            )),
            Some(&child),
            Some(&replay),
            &TailInputDiagnostics::default(),
            true,
            false,
        );
        assert_eq!(
            registry.len(),
            0,
            "an exact pending tail-lag child must not restart a wait chain"
        );
    }

    #[test]
    fn tool_call_only_exact_pending_tail_lag_arms_one_message_child_only() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        let replay = UsageRecord {
            input_tokens: 158_170,
            cache_read_tokens: 157_056,
            ..UsageRecord::default()
        };
        let call_only_parent = TailInputDiagnostics {
            input_items: 2,
            tool_call_chars: 768,
            source: Some("tool_call".to_string()),
            ..TailInputDiagnostics::default()
        };

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&exact_pending_tail_lag_log(
                scope,
                replay.cache_read_tokens,
                768,
            )),
            Some(&root),
            Some(&replay),
            &call_only_parent,
            true,
            false,
        );
        assert_eq!(registry.len(), 1);

        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root.clone()))
            .expect("a bounded tool-call-only parent may arm one exact message child");
        assert_eq!(
            claim.wait_reason(),
            "responses_tool_call_exact_pending_tail_lag"
        );
        assert!(claim.wait_duration() > Duration::ZERO);
        assert!(claim.wait_duration() <= WARM_PENDING_FOLLOWUP_WAIT);
        assert!(claim.followup_is_settle_safe(
            &TailInputDiagnostics {
                input_items: 1,
                message_chars: 12,
                source: Some("message".to_string()),
                ..TailInputDiagnostics::default()
            },
            false,
        ));
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    input_items: 2,
                    tool_output_chars: 1,
                    largest_tool_output_chars: 1,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "tool output must never consume the call-only maturity window"
        );
        assert!(
            registry
                .claim(Some(prefix), Some(scope), true, &exact(root))
                .is_none(),
            "concurrent siblings must fail open rather than queue"
        );

        registry.settle(
            Some(&claim),
            Some(prefix),
            Some(scope),
            Some(&exact_pending_tail_lag_log(
                scope,
                replay.cache_read_tokens,
                768,
            )),
            Some(&head(2)),
            Some(&replay),
            &TailInputDiagnostics {
                input_items: 1,
                message_chars: 12,
                source: Some("message".to_string()),
                ..TailInputDiagnostics::default()
            },
            true,
            false,
        );
        assert_eq!(
            registry.len(),
            0,
            "a tool-call exact pending child must not restart a wait chain"
        );
    }

    #[test]
    fn exact_pending_tail_lag_requires_bound_clean_non_rollback_evidence() {
        let replay = UsageRecord {
            input_tokens: 158_170,
            cache_read_tokens: 157_056,
            ..UsageRecord::default()
        };
        let quiet_tail = TailInputDiagnostics {
            input_items: 3,
            message_chars: 62,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };
        let base = exact_pending_tail_lag_log("scope", replay.cache_read_tokens, 512);
        assert!(exact_pending_tail_lag(Some(&replay), &quiet_tail, &base));

        let mut unbound = base.clone();
        unbound.predecessor_bound = false;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &unbound
        ));

        let mut non_exact = base.clone();
        non_exact.predecessor_exact = false;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &non_exact
        ));

        let mut non_full_replay = base.clone();
        non_full_replay.sent_prediction_eligible = false;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &non_full_replay
        ));

        let mut unsettled = base.clone();
        unsettled.outcome = "failed".to_string();
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &unsettled
        ));

        let mut reset = base.clone();
        reset.continuity_reset = true;
        assert!(!exact_pending_tail_lag(Some(&replay), &quiet_tail, &reset));

        let mut rollback = base.clone();
        rollback.rollback_tokens_128 = 128;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &rollback
        ));

        let mut mismatch = base.clone();
        mismatch.raw_cache_read_tokens = replay.cache_read_tokens.saturating_sub(128);
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &mismatch
        ));

        let mut too_small = base.clone();
        too_small.candidate_avoidable_tokens_128 = 64;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &too_small
        ));

        let mut too_large = base.clone();
        too_large.candidate_avoidable_tokens_128 = 2_176;
        assert!(!exact_pending_tail_lag(
            Some(&replay),
            &quiet_tail,
            &too_large
        ));

        let noisy_parent = TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 8_000,
            largest_tool_output_chars: 8_000,
            source: Some("tool_output".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(!exact_pending_tail_lag(Some(&replay), &noisy_parent, &base));

        let small_tool_parent = TailInputDiagnostics {
            input_items: 3,
            tool_call_chars: 12,
            tool_output_chars: 1_024,
            largest_tool_output_chars: 1_024,
            tool_output_lines: 24,
            tool_output_timestamp_like_count: 2,
            tool_output_path_like_count: 1,
            tool_output_noise_hint: Some("timestamp_like,path_like".to_string()),
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            !exact_pending_tail_lag(Some(&replay), &small_tool_parent, &base),
            "a bounded tool result must preserve the immediate-send contract"
        );

        let oversized_tool_parent = TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 2_049,
            largest_tool_output_chars: 2_049,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            !exact_pending_tail_lag(Some(&replay), &oversized_tool_parent, &base),
            "a material tool result must retain its independent immediate-send contract"
        );

        let line_heavy_parent = TailInputDiagnostics {
            input_items: 3,
            tool_output_chars: 1_024,
            largest_tool_output_chars: 1_024,
            tool_output_lines: 129,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            !exact_pending_tail_lag(Some(&replay), &line_heavy_parent, &base),
            "line-heavy output stays on the immediate-send path"
        );

        let call_only_parent = TailInputDiagnostics {
            input_items: 2,
            tool_call_chars: 768,
            source: Some("tool_call".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            tool_call_only_exact_pending_tail_lag(Some(&replay), &call_only_parent, &base),
            "only a bounded tool-call-only parent may use the separate exact gate"
        );
        let tool_output_parent = TailInputDiagnostics {
            input_items: 2,
            tool_call_chars: 768,
            tool_output_chars: 1,
            largest_tool_output_chars: 1,
            source: Some("mixed".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            !tool_call_only_exact_pending_tail_lag(Some(&replay), &tool_output_parent, &base),
            "any tool output remains on the immediate path"
        );
        let oversized_call_parent = TailInputDiagnostics {
            input_items: 2,
            tool_call_chars: 2_049,
            source: Some("tool_call".to_string()),
            ..TailInputDiagnostics::default()
        };
        assert!(
            !tool_call_only_exact_pending_tail_lag(Some(&replay), &oversized_call_parent, &base),
            "large tool-call bodies do not enter the exact pending path"
        );
    }

    #[test]
    fn exact_pending_tail_lag_failed_or_compacted_child_never_rearms() {
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let replay = UsageRecord {
            input_tokens: 158_170,
            cache_read_tokens: 157_056,
            ..UsageRecord::default()
        };
        let quiet_parent = TailInputDiagnostics {
            input_items: 3,
            message_chars: 62,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        for (upstream_succeeded, confirmed_compaction) in [(false, false), (true, true)] {
            let mut registry = WarmPendingRegistry::default();
            let root = head(1);
            registry.settle(
                None,
                Some(prefix),
                Some(scope),
                Some(&exact_pending_tail_lag_log(
                    scope,
                    replay.cache_read_tokens,
                    512,
                )),
                Some(&root),
                Some(&replay),
                &quiet_parent,
                true,
                false,
            );
            let claim = registry
                .claim(Some(prefix), Some(scope), true, &exact(root))
                .expect("the exact parent must arm one direct child");

            registry.settle(
                Some(&claim),
                Some(prefix),
                Some(scope),
                Some(&exact_pending_tail_lag_log(
                    scope,
                    replay.cache_read_tokens,
                    512,
                )),
                Some(&head(2)),
                Some(&replay),
                &TailInputDiagnostics::default(),
                upstream_succeeded,
                confirmed_compaction,
            );
            assert_eq!(
                registry.len(),
                0,
                "a failed or compacted exact child must not restart a foreground wait chain"
            );
        }
    }

    #[test]
    fn provider_waterline_maturity_requires_exact_bound_non_reset_evidence() {
        let rollback = UsageRecord {
            input_tokens: 211_840,
            cache_read_tokens: 180_992,
            ..UsageRecord::default()
        };
        let exact = exact_rollback_log("scope", rollback.cache_read_tokens, 30_720);
        assert!(provider_waterline_rollback(Some(&rollback), &exact));

        let mut unbound = exact.clone();
        unbound.predecessor_bound = false;
        assert!(!provider_waterline_rollback(Some(&rollback), &unbound));

        let mut reset = exact.clone();
        reset.continuity_reset = true;
        assert!(!provider_waterline_rollback(Some(&rollback), &reset));

        let mut no_rollback = exact;
        no_rollback.rollback_tokens_128 = 0;
        assert!(!provider_waterline_rollback(Some(&rollback), &no_rollback));
    }

    #[test]
    fn material_tool_tail_keeps_precedence_over_a_concurrent_waterline_rollback() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        let replay = warm_tool_replay();
        let mut final_scope = exact_log_with_gap(scope, replay.cache_read_tokens, 8_192);
        final_scope.rollback_tokens_128 = 8_192;

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&final_scope),
            Some(&root),
            Some(&replay),
            &material_tool_tail(),
            true,
            false,
        );

        let claim = registry
            .claim(Some(prefix), Some(scope), true, &exact(root))
            .expect("the material-tail maturity window must still arm");
        assert_eq!(
            claim.wait_reason(),
            "responses_material_tool_tail_maturity_pending"
        );
        assert!(
            !claim.followup_is_settle_safe(
                &TailInputDiagnostics {
                    input_items: 3,
                    tool_output_chars: 32_768,
                    largest_tool_output_chars: 30_000,
                    source: Some("tool_output".to_string()),
                    ..TailInputDiagnostics::default()
                },
                false,
            ),
            "a new material child must retain immediate dispatch instead of consuming latency for its predecessor"
        );
    }

    #[test]
    fn ordinary_message_tail_never_arms_maturity_wait() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let root = head(1);
        let message_tail = TailInputDiagnostics {
            input_items: 1,
            message_chars: 32_768,
            source: Some("message".to_string()),
            ..TailInputDiagnostics::default()
        };

        registry.settle(
            None,
            Some(prefix),
            Some(scope),
            Some(&root_log(scope)),
            Some(&root),
            Some(&warm_tool_replay()),
            &message_tail,
            true,
            false,
        );

        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn expired_claim_cannot_restart_a_fresh_warm_window() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let expired_claim = WarmPendingClaim {
            key: entry_key(prefix, scope),
            nonce: 7,
            deadline_at: Instant::now() - Duration::from_millis(1),
            ready_at: Instant::now() - Duration::from_millis(1),
            remaining_after_claim: 2,
            kind: PendingMaturityKind::GiantColdRoot,
        };

        registry.settle(
            Some(&expired_claim),
            Some(prefix),
            Some(scope),
            Some(&exact_log(scope)),
            Some(&head(2)),
            Some(&cold()),
            &TailInputDiagnostics::default(),
            true,
            false,
        );

        assert_eq!(
            registry.len(),
            0,
            "an expired claimed child is not a new root"
        );
    }
}
