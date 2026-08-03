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
        }
    }

    fn wait_reason(self) -> &'static str {
        match self {
            Self::GiantColdRoot => "responses_giant_cold_prefix_warm_pending",
            Self::MaterialToolTail => "responses_material_tool_tail_maturity_pending",
        }
    }
}

/// A process-local, one-shot maturity gate for a giant cold FullReplay root or
/// a material tool tail. It never changes a frozen request, provider, Key,
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
    } else if material_tool_tail_has_exact_shortfall(raw_usage, tail, final_scope) {
        Some(PendingMaturity {
            kind: PendingMaturityKind::MaterialToolTail,
        })
    } else {
        None
    }
}

/// A large tool result is newly appended context on its own request, so its
/// size alone never proves that waiting helps. Arm a material-tail window only
/// when the exact final-scope settlement proves an older prefix bucket was
/// still absent. This is aggregate upstream usage evidence only.
fn material_tool_tail_has_exact_shortfall(
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
        || final_scope.candidate_avoidable_tokens_128 < 128
        || final_scope.raw_cache_read_tokens != record.cache_read_tokens
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
    fn material_tool_tail_without_exact_shortfall_never_arms() {
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
        assert_eq!(registry.len(), 0);
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
