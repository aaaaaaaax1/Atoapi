use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    metrics::{FinalScopeWaterlineLog, UsageRecord},
    proxy::{PredecessorProofReceipt, WaterlineControlHead},
};

const WARM_PENDING_TTL: Duration = Duration::from_secs(22);
const WARM_PENDING_FOLLOWUP_WAIT: Duration = Duration::from_secs(2);
const WARM_PENDING_MAX_FOLLOWUPS: u8 = 3;
const WARM_PENDING_MAX_ENTRIES: usize = 64;

/// A process-local, one-shot gate for the rare case where a giant FullReplay
/// root is accepted but its upstream cache has not materialised yet.  It never
/// changes a frozen request, provider, Key, route, or number of upstream
/// dispatches; it only gives a proven direct child a bounded opportunity to
/// arrive after the provider finishes warming that exact prefix.
#[derive(Debug, Default)]
pub(crate) struct WarmPendingRegistry {
    entries: HashMap<String, WarmPendingEntry>,
    next_nonce: u64,
}

#[derive(Debug, Clone)]
struct WarmPendingEntry {
    expected_predecessor: WaterlineControlHead,
    deadline_at: Instant,
    remaining_followups: u8,
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
    remaining_after_claim: u8,
}

impl WarmPendingClaim {
    pub(super) fn wait_duration(&self) -> Duration {
        self.deadline_at
            .saturating_duration_since(Instant::now())
            .min(WARM_PENDING_FOLLOWUP_WAIT)
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
            remaining_after_claim: entry.remaining_followups.saturating_sub(1),
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
            || !giant_cold_read(raw_usage)
        {
            return;
        }
        let Some(committed_head) = committed_head else {
            return;
        };

        let (deadline_at, remaining_followups) = match claim {
            Some(claim) => {
                if claim.key != key
                    || claim.deadline_at <= Instant::now()
                    || claim.remaining_after_claim == 0
                {
                    return;
                }
                (claim.deadline_at, claim.remaining_after_claim)
            }
            None => (
                Instant::now() + WARM_PENDING_TTL,
                WARM_PENDING_MAX_FOLLOWUPS,
            ),
        };
        self.arm(
            key,
            committed_head.clone(),
            deadline_at,
            remaining_followups,
        );
    }

    fn arm(
        &mut self,
        key: String,
        expected_predecessor: WaterlineControlHead,
        deadline_at: Instant,
        remaining_followups: u8,
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
                remaining_followups,
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

    fn cold() -> UsageRecord {
        UsageRecord {
            input_tokens: 272_621,
            cache_read_tokens: 3_801,
            ..UsageRecord::default()
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
    fn expired_claim_cannot_restart_a_fresh_warm_window() {
        let mut registry = WarmPendingRegistry::default();
        let prefix = "realm\0provider\0model\0responses\0control";
        let scope = "final-scope";
        let expired_claim = WarmPendingClaim {
            key: entry_key(prefix, scope),
            nonce: 7,
            deadline_at: Instant::now() - Duration::from_millis(1),
            remaining_after_claim: 2,
        };

        registry.settle(
            Some(&expired_claim),
            Some(prefix),
            Some(scope),
            Some(&exact_log(scope)),
            Some(&head(2)),
            Some(&cold()),
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
