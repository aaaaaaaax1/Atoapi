use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex as StdMutex,
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::metrics::{FinalScopeWaterlineLog, UsageRecord};

/// The provider's smallest supported cache bucket. This ledger deliberately
/// never rounds an observed cache read up to this boundary.
pub(super) const FINAL_SCOPE_WATERLINE_BUCKET_TOKENS: u64 = 128;

const DEFAULT_MAX_SCOPES: usize = 1_024;
const DEFAULT_ENTRY_TTL: Duration = Duration::from_secs(15 * 60);
const FINAL_SCOPE_RECLAIM_BUDGET: usize = 8;

const FINAL_SCOPE_PREDECESSOR_PROOF_VERSION: u8 = 1;
const FINAL_SCOPE_CONTROL_HEAD_VERSION: u8 = 1;
const FINAL_SCOPE_OBSERVATION_CAPACITY: usize = 512;
const FINAL_SCOPE_OBSERVATION_OUTCOMES: [&str; 22] = [
    "no_receipt",
    "ineligible",
    "ineligible_unattested_scope",
    "ineligible_static_projection",
    "ineligible_lineage_epoch",
    "ineligible_evidence_version",
    "ineligible_breakpoint",
    "begin_lock_busy",
    "ticketed",
    "observe_only",
    "settle_lock_busy",
    "failed",
    "compaction",
    "missing_usage",
    "zero_input",
    "lineage_rejected",
    "stale_ticket",
    "stale_generation",
    "out_of_order",
    "ambiguous_branch",
    "capacity_full",
    "settled",
];

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FinalScopeOutcomeCount {
    pub outcome: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FinalScopeObservationSnapshot {
    pub version: u8,
    pub capacity: usize,
    pub ring_dropped: u64,
    pub outcomes: Vec<FinalScopeOutcomeCount>,
    pub recent: Vec<FinalScopeWaterlineLog>,
}

/// Compact, process-memory-only Phase 4 evidence. Hot-path writes are either
/// lock-free counters or best-effort `try_lock` ring inserts; neither routing
/// nor upstream dispatch can wait on this registry.
#[derive(Debug)]
pub(crate) struct FinalScopeObservationRegistry {
    counts: [AtomicU64; FINAL_SCOPE_OBSERVATION_OUTCOMES.len()],
    ring_dropped: AtomicU64,
    recent: StdMutex<VecDeque<FinalScopeWaterlineLog>>,
}

impl Default for FinalScopeObservationRegistry {
    fn default() -> Self {
        Self {
            counts: [const { AtomicU64::new(0) }; FINAL_SCOPE_OBSERVATION_OUTCOMES.len()],
            ring_dropped: AtomicU64::new(0),
            recent: StdMutex::new(VecDeque::with_capacity(FINAL_SCOPE_OBSERVATION_CAPACITY)),
        }
    }
}

impl FinalScopeObservationRegistry {
    pub(super) fn record_outcome(&self, outcome: &str) {
        if let Some(index) = FINAL_SCOPE_OBSERVATION_OUTCOMES
            .iter()
            .position(|candidate| *candidate == outcome)
        {
            self.counts[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn try_record(&self, observation: FinalScopeWaterlineLog) {
        let Ok(mut recent) = self.recent.try_lock() else {
            self.ring_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if recent.len() == FINAL_SCOPE_OBSERVATION_CAPACITY {
            recent.pop_front();
        }
        recent.push_back(observation);
    }

    pub(crate) fn snapshot(&self) -> FinalScopeObservationSnapshot {
        let recent = self
            .recent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        let outcomes = FINAL_SCOPE_OBSERVATION_OUTCOMES
            .iter()
            .zip(self.counts.iter())
            .map(|(outcome, count)| FinalScopeOutcomeCount {
                outcome: (*outcome).to_string(),
                count: count.load(Ordering::Relaxed),
            })
            .collect();
        FinalScopeObservationSnapshot {
            version: 2,
            capacity: FINAL_SCOPE_OBSERVATION_CAPACITY,
            ring_dropped: self.ring_dropped.load(Ordering::Relaxed),
            outcomes,
            recent,
        }
    }

    #[cfg(test)]
    pub(super) fn outcome_count(&self, outcome: &str) -> u64 {
        FINAL_SCOPE_OBSERVATION_OUTCOMES
            .iter()
            .position(|candidate| *candidate == outcome)
            .map(|index| self.counts[index].load(Ordering::Relaxed))
            .unwrap_or_default()
    }
}

/// Opaque binding to the exact continuation head accepted by the lineage CAS.
/// The response id is hashed so shadow state never retains or exposes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WaterlineControlHead {
    version: u8,
    pub(super) lineage_epoch: u64,
    pub(super) generation: u64,
    response_id_digest: [u8; 32],
}

impl WaterlineControlHead {
    pub(super) fn derive(lineage_epoch: u64, generation: u64, response_id: &str) -> Option<Self> {
        let response_id = response_id.trim();
        if response_id.is_empty() {
            return None;
        }
        let mut hasher = Sha256::new();
        hasher.update(b"final-scope-control-head-v1\0");
        hasher.update(response_id.as_bytes());
        Some(Self {
            version: FINAL_SCOPE_CONTROL_HEAD_VERSION,
            lineage_epoch,
            generation,
            response_id_digest: hasher.finalize().into(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PredecessorProofStatus {
    Exact,
    Root,
    NoLineage,
    MissingPredecessorInput,
    MissingCurrentInput,
    EmptyPredecessor,
    NotExtended,
    PrefixMismatch,
    FinalInputChanged,
    NotFullReplay,
}

impl PredecessorProofStatus {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Root => "root",
            Self::NoLineage => "no_lineage",
            Self::MissingPredecessorInput => "missing_predecessor_input",
            Self::MissingCurrentInput => "missing_current_input",
            Self::EmptyPredecessor => "empty_predecessor",
            Self::NotExtended => "not_extended",
            Self::PrefixMismatch => "prefix_mismatch",
            Self::FinalInputChanged => "final_input_changed",
            Self::NotFullReplay => "not_full_replay",
        }
    }
}

/// Request-local proof that the current full input begins with every item from
/// the captured lineage head. It contains only counts and an opaque head hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PredecessorProofReceipt {
    pub(super) version: u8,
    pub(super) status: PredecessorProofStatus,
    pub(super) head: Option<WaterlineControlHead>,
    /// Epoch of the current final-wire lineage. A headless root still owns an
    /// epoch, so this cannot be inferred only from `head`.
    pub(super) lineage_epoch: Option<u64>,
    pub(super) predecessor_input_items: u64,
    pub(super) current_input_items: u64,
}

impl PredecessorProofReceipt {
    pub(super) fn new(
        status: PredecessorProofStatus,
        head: Option<WaterlineControlHead>,
        predecessor_input_items: u64,
        current_input_items: u64,
    ) -> Self {
        let lineage_epoch = head.as_ref().map(|head| head.lineage_epoch);
        Self {
            version: FINAL_SCOPE_PREDECESSOR_PROOF_VERSION,
            status,
            head,
            lineage_epoch,
            predecessor_input_items,
            current_input_items,
        }
    }

    pub(super) fn root(lineage_epoch: u64, current_input_items: u64) -> Self {
        let mut receipt = Self::new(PredecessorProofStatus::Root, None, 0, current_input_items);
        receipt.lineage_epoch = Some(lineage_epoch);
        receipt
    }

    pub(super) fn not_full_replay(mut self) -> Self {
        self.status = PredecessorProofStatus::NotFullReplay;
        self.head = None;
        self
    }

    pub(super) fn invalidate_final_input(mut self) -> Self {
        self.status = PredecessorProofStatus::FinalInputChanged;
        self.head = None;
        self
    }

    pub(super) fn is_exact(&self) -> bool {
        self.version == FINAL_SCOPE_PREDECESSOR_PROOF_VERSION
            && self.status == PredecessorProofStatus::Exact
            && self.head.is_some()
    }
}

/// A one-dispatch capability to settle one final scope.
///
/// Tickets are allocated only after the final scope has been attested. The
/// dispatch sequence prevents a slower, older response from changing the
/// waterlines established by a newer successful settlement for the same
/// scope.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct WaterlineTicket {
    pub(super) scope_digest: String,
    /// Lifetime generation of the settled entry or staged lease that issued
    /// this ticket. A recreated digest receives a new generation, so a ticket
    /// that survived TTL expiry and scope recreation can never settle the new
    /// entry.
    pub(super) entry_generation: u64,
    pub(super) dispatch_seq: u64,
    pub(super) sent_prediction_eligible: bool,
    pub(super) predecessor: PredecessorProofReceipt,
    deadline_at: Option<Instant>,
    /// CAS snapshot captured at dispatch. A concurrent sibling that changes
    /// the accepted head makes this ticket observation-only.
    captured_control_head: Option<WaterlineControlHead>,
    pub(super) prior_waterlines: Option<FinalScopeWaterlines>,
    pub(super) prior_settlement_age_ms: Option<u64>,
}

/// The three independent, request-local waterlines for one final scope.
///
/// `observed_cache_read_tokens` is exact upstream telemetry. The two bucketed
/// values are intentionally distinct: a FullReplay send predicts a possible
/// future prefix, while only an upstream cache read can advance `settled`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct FinalScopeWaterlines {
    pub(super) observed_cache_read_tokens: u64,
    pub(super) sent_prefix_bucket_tokens_128: u64,
    pub(super) settled_prefix_bucket_tokens_128: u64,
    /// Latest values are distinct from historical high-water marks. A future
    /// controller must use fresh latest evidence and must never infer cache
    /// availability from either historical maximum alone.
    pub(super) latest_sent_prefix_bucket_tokens_128: u64,
    pub(super) latest_settled_prefix_bucket_tokens_128: u64,
    pub(super) cache_regression_streak: u64,
    pub(super) stable_settlement_streak: u64,
    pub(super) continuity_generation: u64,
    /// The ticket sequence of the last accepted settlement, not merely the
    /// most recently issued dispatch.
    pub(super) dispatch_seq: u64,
}

/// The only facts accepted when settling a waterline ticket.
///
/// Callers must pass the unmodified upstream `UsageRecord`; synthetic or
/// delta-equivalent metrics are deliberately not accepted by this module.
#[derive(Debug, Clone)]
pub(super) struct WaterlineSettlement<'a> {
    pub(super) upstream_succeeded: bool,
    pub(super) compaction: bool,
    pub(super) raw_usage: Option<&'a UsageRecord>,
    /// Present only when this exact request won the lineage CAS and created
    /// the supplied head. Losing siblings cannot advance control evidence.
    pub(super) committed_head: Option<WaterlineControlHead>,
}

impl<'a> WaterlineSettlement<'a> {
    #[cfg(test)]
    pub(super) fn successful(raw_usage: &'a UsageRecord) -> Self {
        Self {
            upstream_succeeded: true,
            compaction: false,
            raw_usage: Some(raw_usage),
            committed_head: WaterlineControlHead::derive(1, 1, "test-response"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WaterlineSettlementOutcome {
    pub(super) status: &'static str,
    pub(super) waterlines: Option<FinalScopeWaterlines>,
    pub(super) continuity_reset: bool,
    pub(super) predecessor_bound: bool,
    pub(super) raw_input_tokens: u64,
    pub(super) raw_cache_read_tokens: u64,
    pub(super) candidate_avoidable_tokens_128: u64,
    pub(super) rollback_tokens_128: u64,
}

impl WaterlineSettlementOutcome {
    pub(super) fn ignored(status: &'static str) -> Self {
        Self {
            status,
            waterlines: None,
            continuity_reset: false,
            predecessor_bound: false,
            raw_input_tokens: 0,
            raw_cache_read_tokens: 0,
            candidate_avoidable_tokens_128: 0,
            rollback_tokens_128: 0,
        }
    }
}

/// Bounded, process-memory-only evidence for final-scope cache waterlines.
///
/// This is a shadow ledger. It has no disk persistence, asynchronous work,
/// routing authority, retry behavior, or influence over reported hit metrics.
/// Only the current scope is checked at its exact TTL boundary. Capacity
/// exhaustion fails open instead of scanning or evicting unrelated scopes on
/// the dispatch/settlement hot path.
#[derive(Debug)]
pub(crate) struct FinalScopeWaterlineLedger {
    entries: HashMap<String, LedgerEntry>,
    slots: Vec<Option<LedgerSlot>>,
    free_slots: Vec<usize>,
    sweep_cursor: usize,
    capacity: usize,
    ttl: Duration,
    next_dispatch_seq: u64,
    next_entry_generation: u64,
}

#[derive(Debug)]
struct LedgerEntry {
    generation: u64,
    slot_index: usize,
    waterlines: FinalScopeWaterlines,
    control_head: Option<WaterlineControlHead>,
    last_settled_seq: u64,
    last_settled_at: Instant,
}

#[derive(Debug, Clone)]
struct LedgerSlot {
    scope_digest: String,
    generation: u64,
}

impl Default for FinalScopeWaterlineLedger {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_SCOPES, DEFAULT_ENTRY_TTL)
    }
}

impl FinalScopeWaterlineLedger {
    pub(super) fn new(capacity: usize, ttl: Duration) -> Self {
        let free_slots = (0..capacity).rev().collect();
        Self {
            entries: HashMap::new(),
            slots: vec![None; capacity],
            free_slots,
            sweep_cursor: 0,
            capacity,
            ttl,
            next_dispatch_seq: 0,
            next_entry_generation: 0,
        }
    }

    /// Starts a new dispatch for an already-attested final scope.
    ///
    /// `eligible` is intentionally separate from `sent_prediction_eligible`.
    /// Non-FullReplay requests receive request-local observation tickets but
    /// never create or mutate control-ledger state.
    #[cfg(test)]
    pub(super) fn try_begin(
        &mut self,
        scope_digest: &str,
        eligible: bool,
        sent_prediction_eligible: bool,
    ) -> Option<WaterlineTicket> {
        let predecessor = self
            .entries
            .get(scope_digest)
            .and_then(|entry| entry.control_head.clone())
            .map(|head| {
                PredecessorProofReceipt::new(PredecessorProofStatus::Exact, Some(head), 1, 2)
            })
            .unwrap_or_else(|| PredecessorProofReceipt::root(1, 1));
        self.try_begin_with_proof(
            scope_digest,
            eligible,
            sent_prediction_eligible,
            predecessor,
        )
    }

    pub(super) fn try_begin_with_proof(
        &mut self,
        scope_digest: &str,
        eligible: bool,
        sent_prediction_eligible: bool,
        predecessor: PredecessorProofReceipt,
    ) -> Option<WaterlineTicket> {
        self.try_begin_with_proof_at(
            scope_digest,
            eligible,
            sent_prediction_eligible,
            predecessor,
            Instant::now(),
        )
    }

    /// Applies a terminal upstream result to a ticket newer than the last
    /// accepted settlement for its scope. Returns `None` for ignored outcomes.
    #[cfg(test)]
    pub(super) fn settle(
        &mut self,
        ticket: &WaterlineTicket,
        settlement: WaterlineSettlement<'_>,
    ) -> Option<FinalScopeWaterlines> {
        self.settle_with_outcome(ticket, settlement).waterlines
    }

    pub(super) fn settle_with_outcome(
        &mut self,
        ticket: &WaterlineTicket,
        settlement: WaterlineSettlement<'_>,
    ) -> WaterlineSettlementOutcome {
        self.settle_with_outcome_at(ticket, settlement, Instant::now())
    }

    /// Reads the current process-memory evidence for an opaque final scope.
    #[cfg(test)]
    pub(super) fn snapshot(&mut self, scope_digest: &str) -> Option<FinalScopeWaterlines> {
        self.snapshot_at(scope_digest, Instant::now())
    }

    #[cfg(test)]
    fn try_begin_at(
        &mut self,
        scope_digest: &str,
        eligible: bool,
        sent_prediction_eligible: bool,
        now: Instant,
    ) -> Option<WaterlineTicket> {
        let predecessor = self
            .entries
            .get(scope_digest)
            .and_then(|entry| entry.control_head.clone())
            .map(|head| {
                PredecessorProofReceipt::new(PredecessorProofStatus::Exact, Some(head), 1, 2)
            })
            .unwrap_or_else(|| PredecessorProofReceipt::root(1, 1));
        self.try_begin_with_proof_at(
            scope_digest,
            eligible,
            sent_prediction_eligible,
            predecessor,
            now,
        )
    }

    fn try_begin_with_proof_at(
        &mut self,
        scope_digest: &str,
        eligible: bool,
        sent_prediction_eligible: bool,
        predecessor: PredecessorProofReceipt,
        now: Instant,
    ) -> Option<WaterlineTicket> {
        if !eligible || scope_digest.is_empty() || self.capacity == 0 {
            return None;
        }
        self.expire_scope_if_needed(scope_digest, now);

        let dispatch_seq = self.next_dispatch_seq.checked_add(1)?;
        let (
            entry_generation,
            deadline_at,
            captured_control_head,
            prior_waterlines,
            prior_settlement_age_ms,
        ) = if let Some(entry) = self.entries.get(scope_digest) {
            (
                entry.generation,
                entry.last_settled_at.checked_add(self.ttl),
                entry.control_head.clone(),
                Some(entry.waterlines),
                now.checked_duration_since(entry.last_settled_at)
                    .map(|age| age.as_millis().min(u64::MAX as u128) as u64),
            )
        } else if !sent_prediction_eligible {
            // Observe-only traffic never consumes a settled-evidence
            // slot. It carries only request-local usage.
            (0, None, None, None, None)
        } else {
            let generation = self.next_entry_generation.checked_add(1)?;
            self.next_entry_generation = generation;
            (generation, now.checked_add(self.ttl), None, None, None)
        };
        self.next_dispatch_seq = dispatch_seq;

        Some(WaterlineTicket {
            scope_digest: scope_digest.to_owned(),
            entry_generation,
            dispatch_seq,
            sent_prediction_eligible,
            predecessor,
            deadline_at,
            captured_control_head,
            prior_waterlines,
            prior_settlement_age_ms,
        })
    }

    #[cfg(test)]
    fn settle_at(
        &mut self,
        ticket: &WaterlineTicket,
        settlement: WaterlineSettlement<'_>,
        now: Instant,
    ) -> Option<FinalScopeWaterlines> {
        self.settle_with_outcome_at(ticket, settlement, now)
            .waterlines
    }

    fn settle_with_outcome_at(
        &mut self,
        ticket: &WaterlineTicket,
        settlement: WaterlineSettlement<'_>,
        now: Instant,
    ) -> WaterlineSettlementOutcome {
        self.expire_scope_if_needed(&ticket.scope_digest, now);
        if ticket.deadline_at.is_some_and(|deadline| now >= deadline) {
            return WaterlineSettlementOutcome::ignored("stale_ticket");
        }
        if !settlement.upstream_succeeded {
            return WaterlineSettlementOutcome::ignored("failed");
        }
        if settlement.compaction {
            return WaterlineSettlementOutcome::ignored("compaction");
        }
        let Some(raw_usage) = settlement.raw_usage else {
            return WaterlineSettlementOutcome::ignored("missing_usage");
        };
        // An output-only or malformed usage record cannot establish a prefix
        // waterline, even if it contains a cache field.
        if raw_usage.input_tokens == 0 {
            return WaterlineSettlementOutcome::ignored("zero_input");
        }
        if !ticket.sent_prediction_eligible {
            return WaterlineSettlementOutcome {
                status: "observe_only",
                waterlines: None,
                continuity_reset: false,
                predecessor_bound: false,
                raw_input_tokens: raw_usage.input_tokens,
                raw_cache_read_tokens: raw_usage.cache_read_tokens,
                candidate_avoidable_tokens_128: 0,
                rollback_tokens_128: 0,
            };
        }
        if ticket.sent_prediction_eligible && settlement.committed_head.is_none() {
            return WaterlineSettlementOutcome::ignored("lineage_rejected");
        }

        if !self.entries.contains_key(&ticket.scope_digest) {
            // Only confirmed, successful raw usage is allowed to consume a
            // settled-evidence slot. Reclaim work is strictly bounded, so this
            // never performs a full-table scan on the settlement path.
            self.reclaim_expired_budget(now, FINAL_SCOPE_RECLAIM_BUDGET);
            let Some(slot_index) = self.free_slots.pop() else {
                return WaterlineSettlementOutcome::ignored("capacity_full");
            };
            self.slots[slot_index] = Some(LedgerSlot {
                scope_digest: ticket.scope_digest.clone(),
                generation: ticket.entry_generation,
            });
            self.entries.insert(
                ticket.scope_digest.clone(),
                LedgerEntry {
                    generation: ticket.entry_generation,
                    slot_index,
                    waterlines: FinalScopeWaterlines::default(),
                    control_head: None,
                    last_settled_seq: 0,
                    last_settled_at: now,
                },
            );
        }

        let Some(entry) = self.entries.get_mut(&ticket.scope_digest) else {
            return WaterlineSettlementOutcome::ignored("stale_ticket");
        };
        if entry.generation != ticket.entry_generation {
            return WaterlineSettlementOutcome::ignored("stale_generation");
        }
        if ticket.dispatch_seq <= entry.last_settled_seq {
            return WaterlineSettlementOutcome::ignored("out_of_order");
        }
        if ticket.captured_control_head != entry.control_head {
            return WaterlineSettlementOutcome::ignored("ambiguous_branch");
        }

        let prior = entry.waterlines;
        let predecessor_bound = entry.control_head.as_ref().is_some_and(|head| {
            ticket.predecessor.is_exact() && ticket.predecessor.head.as_ref() == Some(head)
        });
        let continuity_reset =
            ticket.sent_prediction_eligible && entry.control_head.is_some() && !predecessor_bound;
        if continuity_reset {
            let continuity_generation = entry
                .waterlines
                .continuity_generation
                .saturating_add(1)
                .max(1);
            entry.waterlines = FinalScopeWaterlines {
                continuity_generation,
                ..FinalScopeWaterlines::default()
            };
        } else if ticket.sent_prediction_eligible && entry.waterlines.continuity_generation == 0 {
            entry.waterlines.continuity_generation = 1;
        }

        // Preserve the provider's raw telemetry for observability. This is not
        // clamped, rounded, or replaced by a locally inferred value.
        entry.waterlines.observed_cache_read_tokens = raw_usage.cache_read_tokens;

        // Only a raw upstream cache read can advance the settled waterline.
        // The min bound prevents malformed telemetry from claiming more cached
        // prefix than the exact raw request contained.
        let observed_bucket = bucket_128(raw_usage.cache_read_tokens.min(raw_usage.input_tokens));
        entry.waterlines.settled_prefix_bucket_tokens_128 = entry
            .waterlines
            .settled_prefix_bucket_tokens_128
            .max(observed_bucket);
        let previous_latest_settled = if continuity_reset {
            0
        } else {
            prior.latest_settled_prefix_bucket_tokens_128
        };
        entry.waterlines.latest_settled_prefix_bucket_tokens_128 = observed_bucket;
        if previous_latest_settled == 0 || observed_bucket >= previous_latest_settled {
            entry.waterlines.stable_settlement_streak =
                entry.waterlines.stable_settlement_streak.saturating_add(1);
            entry.waterlines.cache_regression_streak = 0;
        } else {
            entry.waterlines.stable_settlement_streak = 0;
            entry.waterlines.cache_regression_streak =
                entry.waterlines.cache_regression_streak.saturating_add(1);
        }

        let prior_sent = if predecessor_bound {
            prior.latest_sent_prefix_bucket_tokens_128
        } else {
            0
        };
        let prior_settled = if predecessor_bound {
            prior.latest_settled_prefix_bucket_tokens_128
        } else {
            0
        };
        let current_input_bucket = bucket_128(raw_usage.input_tokens);
        let expected_cached_bucket = prior_sent.min(current_input_bucket);
        let candidate_avoidable_tokens_128 = expected_cached_bucket.saturating_sub(observed_bucket);
        let rollback_tokens_128 = prior_settled.saturating_sub(observed_bucket);

        // A successful pure FullReplay may make this prefix available later.
        // It is a prediction only and never participates in observed/settled.
        if ticket.sent_prediction_eligible {
            entry.waterlines.sent_prefix_bucket_tokens_128 = entry
                .waterlines
                .sent_prefix_bucket_tokens_128
                .max(current_input_bucket);
            entry.waterlines.latest_sent_prefix_bucket_tokens_128 = current_input_bucket;
            entry.control_head = settlement.committed_head;
        }
        entry.last_settled_seq = ticket.dispatch_seq;
        entry.last_settled_at = now;
        entry.waterlines.dispatch_seq = ticket.dispatch_seq;

        WaterlineSettlementOutcome {
            status: "settled",
            waterlines: Some(entry.waterlines),
            continuity_reset,
            predecessor_bound,
            raw_input_tokens: raw_usage.input_tokens,
            raw_cache_read_tokens: raw_usage.cache_read_tokens,
            candidate_avoidable_tokens_128,
            rollback_tokens_128,
        }
    }

    #[cfg(test)]
    fn snapshot_at(&mut self, scope_digest: &str, now: Instant) -> Option<FinalScopeWaterlines> {
        self.expire_scope_if_needed(scope_digest, now);
        self.entries.get(scope_digest).map(|entry| entry.waterlines)
    }

    fn expire_scope_if_needed(&mut self, scope_digest: &str, now: Instant) {
        let settled_expired = self
            .entries
            .get(scope_digest)
            .is_some_and(|entry| entry_is_expired(entry, self.ttl, now));
        if settled_expired {
            self.remove_entry(scope_digest);
        }
    }

    fn remove_entry(&mut self, scope_digest: &str) -> Option<LedgerEntry> {
        let entry = self.entries.remove(scope_digest)?;
        if self
            .slots
            .get(entry.slot_index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| {
                slot.scope_digest == scope_digest && slot.generation == entry.generation
            })
        {
            self.slots[entry.slot_index] = None;
            self.free_slots.push(entry.slot_index);
        }
        Some(entry)
    }

    fn reclaim_expired_budget(&mut self, now: Instant, budget: usize) {
        if self.slots.is_empty() {
            return;
        }
        for _ in 0..budget.min(self.slots.len()) {
            let slot_index = self.sweep_cursor % self.slots.len();
            self.sweep_cursor = (slot_index + 1) % self.slots.len();
            let Some(slot) = self.slots[slot_index].clone() else {
                continue;
            };
            let reclaim = self.entries.get(&slot.scope_digest).is_none_or(|entry| {
                entry.generation != slot.generation || entry_is_expired(entry, self.ttl, now)
            });
            if !reclaim {
                continue;
            }
            if self
                .entries
                .get(&slot.scope_digest)
                .is_some_and(|entry| entry.generation == slot.generation)
            {
                self.entries.remove(&slot.scope_digest);
            }
            self.slots[slot_index] = None;
            self.free_slots.push(slot_index);
        }
    }
}

fn bucket_128(tokens: u64) -> u64 {
    (tokens / FINAL_SCOPE_WATERLINE_BUCKET_TOKENS) * FINAL_SCOPE_WATERLINE_BUCKET_TOKENS
}

fn entry_is_expired(entry: &LedgerEntry, ttl: Duration, now: Instant) -> bool {
    now.checked_duration_since(entry.last_settled_at)
        .map(|age| age >= ttl)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(input_tokens: u64, cache_read_tokens: u64) -> UsageRecord {
        UsageRecord {
            input_tokens,
            cache_read_tokens,
            ..UsageRecord::default()
        }
    }

    fn settle_success(
        ledger: &mut FinalScopeWaterlineLedger,
        ticket: &WaterlineTicket,
        usage: &UsageRecord,
    ) -> FinalScopeWaterlines {
        ledger
            .settle(ticket, WaterlineSettlement::successful(usage))
            .expect("successful raw usage should settle the current ticket")
    }

    fn control_head(epoch: u64, generation: u64, response_id: &str) -> WaterlineControlHead {
        WaterlineControlHead::derive(epoch, generation, response_id).unwrap()
    }

    fn proof(
        status: PredecessorProofStatus,
        head: Option<WaterlineControlHead>,
    ) -> PredecessorProofReceipt {
        PredecessorProofReceipt::new(status, head, 2, 3)
    }

    fn successful_with_head<'a>(
        raw_usage: &'a UsageRecord,
        head: WaterlineControlHead,
    ) -> WaterlineSettlement<'a> {
        WaterlineSettlement {
            upstream_succeeded: true,
            compaction: false,
            raw_usage: Some(raw_usage),
            committed_head: Some(head),
        }
    }

    #[test]
    fn only_eligible_scopes_allocate_monotonic_tickets() {
        let mut ledger = FinalScopeWaterlineLedger::default();

        assert!(ledger.try_begin("scope-a", false, true).is_none());
        assert!(ledger.try_begin("", true, true).is_none());

        let first = ledger.try_begin("scope-a", true, true).unwrap();
        let second = ledger.try_begin("scope-b", true, false).unwrap();
        assert_eq!(first.dispatch_seq, 1);
        assert_eq!(second.dispatch_seq, 2);
        assert!(first.sent_prediction_eligible);
        assert!(!second.sent_prediction_eligible);
    }

    #[test]
    fn sent_prediction_uses_exact_128_boundaries_without_becoming_a_hit() {
        let cases = [
            (1_023, 896),
            (1_024, 1_024),
            (1_151, 1_024),
            (1_152, 1_152),
            (13_396, 13_312),
        ];

        for (input_tokens, expected_bucket) in cases {
            let mut ledger = FinalScopeWaterlineLedger::default();
            let ticket = ledger.try_begin("scope-a", true, true).unwrap();
            let state = settle_success(&mut ledger, &ticket, &usage(input_tokens, 0));

            assert_eq!(state.sent_prefix_bucket_tokens_128, expected_bucket);
            assert_eq!(state.observed_cache_read_tokens, 0);
            assert_eq!(state.settled_prefix_bucket_tokens_128, 0);
        }
    }

    #[test]
    fn observed_is_exact_and_settled_is_bounded_by_raw_input() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let ticket = ledger.try_begin("scope-a", true, true).unwrap();
        let state = settle_success(&mut ledger, &ticket, &usage(1_151, 1_151));

        assert_eq!(state.observed_cache_read_tokens, 1_151);
        assert_eq!(state.settled_prefix_bucket_tokens_128, 1_024);
        assert_eq!(state.sent_prefix_bucket_tokens_128, 1_024);

        let newer = ledger.try_begin("scope-a", true, true).unwrap();
        let bounded = settle_success(&mut ledger, &newer, &usage(128, 4_096));
        assert_eq!(bounded.observed_cache_read_tokens, 4_096);
        assert_eq!(bounded.settled_prefix_bucket_tokens_128, 1_024);
    }

    #[test]
    fn delta_like_raw_usage_never_inflates_waterlines() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let ticket = ledger.try_begin("scope-a", true, false).unwrap();

        // A session delta can be given a larger effective cache value elsewhere
        // for legacy diagnostics. The shadow ledger accepts only these raw
        // fields, so a raw cold delta remains a cold read here.
        let raw = usage(256, 0);
        let outcome = ledger.settle_with_outcome(
            &ticket,
            WaterlineSettlement {
                upstream_succeeded: true,
                compaction: false,
                raw_usage: Some(&raw),
                committed_head: None,
            },
        );
        assert_eq!(outcome.status, "observe_only");
        assert_eq!(outcome.raw_input_tokens, 256);
        assert_eq!(outcome.raw_cache_read_tokens, 0);
        assert!(outcome.waterlines.is_none());
        assert!(ledger.snapshot("scope-a").is_none());
    }

    #[test]
    fn observe_only_burst_cannot_change_or_evict_full_replay_control_evidence() {
        let mut ledger = FinalScopeWaterlineLedger::new(1, Duration::from_secs(60));
        let stable = ledger.try_begin("stable", true, true).unwrap();
        let expected = settle_success(&mut ledger, &stable, &usage(4_096, 3_968));

        for index in 0..32 {
            let scope = format!("observe-{index}");
            let ticket = ledger.try_begin(&scope, true, false).unwrap();
            let raw = usage(100_000 + index, 50_000 + index);
            let outcome = ledger.settle_with_outcome(
                &ticket,
                WaterlineSettlement {
                    upstream_succeeded: true,
                    compaction: false,
                    raw_usage: Some(&raw),
                    committed_head: None,
                },
            );
            assert_eq!(outcome.status, "observe_only");
            assert_eq!(outcome.raw_input_tokens, raw.input_tokens);
            assert!(outcome.waterlines.is_none());
        }

        assert_eq!(ledger.entries.len(), 1);
        assert_eq!(ledger.free_slots.len(), 0);
        assert_eq!(ledger.snapshot("stable"), Some(expected));
    }

    #[test]
    fn failed_missing_or_compaction_results_do_not_advance_waterlines() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let initial = ledger.try_begin("scope-a", true, true).unwrap();
        let baseline = settle_success(&mut ledger, &initial, &usage(1_152, 1_024));

        let failed = ledger.try_begin("scope-a", true, true).unwrap();
        assert!(ledger
            .settle(
                &failed,
                WaterlineSettlement {
                    upstream_succeeded: false,
                    compaction: false,
                    raw_usage: Some(&usage(13_396, 13_396)),
                    committed_head: None,
                },
            )
            .is_none());
        let after_failed = ledger.snapshot("scope-a").unwrap();
        assert_eq!(
            after_failed.observed_cache_read_tokens,
            baseline.observed_cache_read_tokens
        );
        assert_eq!(
            after_failed.settled_prefix_bucket_tokens_128,
            baseline.settled_prefix_bucket_tokens_128
        );
        assert_eq!(
            after_failed.sent_prefix_bucket_tokens_128,
            baseline.sent_prefix_bucket_tokens_128
        );

        let no_usage = ledger.try_begin("scope-a", true, true).unwrap();
        assert!(ledger
            .settle(
                &no_usage,
                WaterlineSettlement {
                    upstream_succeeded: true,
                    compaction: false,
                    raw_usage: None,
                    committed_head: None,
                },
            )
            .is_none());

        let compact = ledger.try_begin("scope-a", true, true).unwrap();
        assert!(ledger
            .settle(
                &compact,
                WaterlineSettlement {
                    upstream_succeeded: true,
                    compaction: true,
                    raw_usage: Some(&usage(13_396, 13_396)),
                    committed_head: None,
                },
            )
            .is_none());

        let final_state = ledger.snapshot("scope-a").unwrap();
        assert_eq!(
            final_state.observed_cache_read_tokens,
            baseline.observed_cache_read_tokens
        );
        assert_eq!(
            final_state.settled_prefix_bucket_tokens_128,
            baseline.settled_prefix_bucket_tokens_128
        );
        assert_eq!(
            final_state.sent_prefix_bucket_tokens_128,
            baseline.sent_prefix_bucket_tokens_128
        );
    }

    #[test]
    fn older_ticket_settlement_cannot_change_a_newer_dispatch() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let older = ledger.try_begin("scope-a", true, true).unwrap();
        let newer = ledger.try_begin("scope-a", true, true).unwrap();
        assert_ne!(older.entry_generation, newer.entry_generation);
        let expected = settle_success(&mut ledger, &newer, &usage(1_152, 1_024));

        assert!(ledger
            .settle(
                &older,
                WaterlineSettlement::successful(&usage(13_396, 13_396))
            )
            .is_none());
        assert_eq!(ledger.snapshot("scope-a"), Some(expected));
    }

    #[test]
    fn failed_newer_ticket_does_not_block_an_older_successful_settlement() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let older = ledger.try_begin("scope-a", true, true).unwrap();
        let newer_failed = ledger.try_begin("scope-a", true, true).unwrap();

        assert!(ledger
            .settle(
                &newer_failed,
                WaterlineSettlement {
                    upstream_succeeded: false,
                    compaction: false,
                    raw_usage: None,
                    committed_head: None,
                },
            )
            .is_none());

        let state = settle_success(&mut ledger, &older, &usage(1_152, 1_024));
        assert_eq!(state.dispatch_seq, older.dispatch_seq);
        assert_eq!(state.observed_cache_read_tokens, 1_024);
        assert_eq!(state.settled_prefix_bucket_tokens_128, 1_024);
    }

    #[test]
    fn newer_compaction_or_missing_usage_does_not_block_older_success() {
        let mut ledger = FinalScopeWaterlineLedger::default();

        let older_no_usage = ledger.try_begin("scope-no-usage", true, true).unwrap();
        let newer_no_usage = ledger.try_begin("scope-no-usage", true, true).unwrap();
        assert_ne!(
            older_no_usage.entry_generation,
            newer_no_usage.entry_generation
        );
        assert!(ledger
            .settle(
                &newer_no_usage,
                WaterlineSettlement {
                    upstream_succeeded: true,
                    compaction: false,
                    raw_usage: None,
                    committed_head: None,
                },
            )
            .is_none());
        let no_usage_settled = settle_success(&mut ledger, &older_no_usage, &usage(1_152, 1_024));
        assert_eq!(no_usage_settled.dispatch_seq, older_no_usage.dispatch_seq);

        let older_compaction = ledger.try_begin("scope-compaction", true, true).unwrap();
        let newer_compaction = ledger.try_begin("scope-compaction", true, true).unwrap();
        assert_ne!(
            older_compaction.entry_generation,
            newer_compaction.entry_generation
        );
        assert!(ledger
            .settle(
                &newer_compaction,
                WaterlineSettlement {
                    upstream_succeeded: true,
                    compaction: true,
                    raw_usage: Some(&usage(13_396, 13_396)),
                    committed_head: None,
                },
            )
            .is_none());
        let compaction_settled =
            settle_success(&mut ledger, &older_compaction, &usage(1_152, 1_024));
        assert_eq!(
            compaction_settled.dispatch_seq,
            older_compaction.dispatch_seq
        );
    }

    #[test]
    fn unsettled_ticket_expires_without_reserving_ledger_capacity() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut ledger = FinalScopeWaterlineLedger::new(1, ttl);
        let expired = ledger
            .try_begin_at("scope-a", true, true, now)
            .expect("initial scope ticket");
        let current = ledger
            .try_begin_at("scope-b", true, true, now + Duration::from_secs(1))
            .expect("unsettled tickets do not reserve capacity");

        assert_ne!(expired.entry_generation, current.entry_generation);
        assert!(ledger
            .settle_at(
                &expired,
                WaterlineSettlement::successful(&usage(13_396, 13_396)),
                now + ttl,
            )
            .is_none());

        let settled = ledger
            .settle_at(
                &current,
                WaterlineSettlement::successful(&usage(1_152, 1_024)),
                now + ttl,
            )
            .expect("the still-live ticket may settle");
        assert_eq!(settled.observed_cache_read_tokens, 1_024);
    }

    #[test]
    fn failure_only_distinct_scope_bursts_do_not_evict_settled_evidence() {
        let now = Instant::now();
        let mut ledger = FinalScopeWaterlineLedger::new(1, Duration::from_secs(60));
        let stable = ledger
            .try_begin_at("stable", true, true, now)
            .expect("stable scope ticket");
        let expected = ledger
            .settle_at(
                &stable,
                WaterlineSettlement::successful(&usage(1_152, 1_024)),
                now,
            )
            .expect("settles stable evidence");

        for index in 0..8 {
            let scope = format!("failure-{index}");
            let ticket = ledger
                .try_begin_at(&scope, true, true, now + Duration::from_secs(index + 1))
                .expect("a failure ticket never consumes settled capacity");
            assert!(ledger
                .settle_at(
                    &ticket,
                    WaterlineSettlement {
                        upstream_succeeded: false,
                        compaction: false,
                        raw_usage: None,
                        committed_head: None,
                    },
                    now + Duration::from_secs(index + 1),
                )
                .is_none());
        }
        assert_eq!(ledger.entries.len(), 1);

        assert_eq!(
            ledger.snapshot_at("stable", now + Duration::from_secs(8)),
            Some(expected)
        );
    }

    #[test]
    fn ticket_deadline_rejects_stale_unsettled_generation() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut ledger = FinalScopeWaterlineLedger::new(2, ttl);
        let expired = ledger
            .try_begin_at("scope-a", true, true, now)
            .expect("initial scope ticket");
        let recreated = ledger
            .try_begin_at("scope-a", true, true, now + ttl)
            .expect("TTL-pruned scope ticket");

        assert_ne!(expired.entry_generation, recreated.entry_generation);
        assert!(ledger
            .settle_at(
                &expired,
                WaterlineSettlement::successful(&usage(13_396, 13_396)),
                now + ttl,
            )
            .is_none());
        let settled = ledger
            .settle_at(
                &recreated,
                WaterlineSettlement::successful(&usage(1_152, 1_024)),
                now + ttl,
            )
            .expect("current generation may settle");
        assert_eq!(settled.observed_cache_read_tokens, 1_024);
    }

    #[test]
    fn accessed_scope_expires_at_exact_ttl_without_a_global_scan() {
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        let mut ledger = FinalScopeWaterlineLedger::new(2, ttl);
        let expired = ledger
            .try_begin_at("scope-a", true, true, now)
            .expect("initial scope ticket");

        // Accessing another scope never scans or refreshes scope-a. Only the
        // direct digest check enforces its exact 60-second lifetime.
        ledger
            .try_begin_at("scope-b", true, true, now + Duration::from_secs(40))
            .expect("another scope remains independent");
        let recreated = ledger
            .try_begin_at("scope-a", true, true, now + ttl + Duration::from_secs(1))
            .expect("expired scope must be recreated");

        assert_ne!(expired.entry_generation, recreated.entry_generation);
        assert!(ledger
            .settle_at(
                &expired,
                WaterlineSettlement::successful(&usage(13_396, 13_396)),
                now + ttl + Duration::from_secs(1),
            )
            .is_none());
    }

    #[test]
    fn different_scope_digests_are_strictly_isolated() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let left = ledger.try_begin("opaque-a", true, true).unwrap();
        let right = ledger.try_begin("opaque-b", true, true).unwrap();

        let left_state = settle_success(&mut ledger, &left, &usage(1_152, 1_024));
        let right_state = settle_success(&mut ledger, &right, &usage(13_396, 0));

        assert_eq!(left_state.observed_cache_read_tokens, 1_024);
        assert_eq!(left_state.sent_prefix_bucket_tokens_128, 1_152);
        assert_eq!(right_state.observed_cache_read_tokens, 0);
        assert_eq!(right_state.sent_prefix_bucket_tokens_128, 13_312);
        assert_eq!(ledger.snapshot("opaque-a"), Some(left_state));
        assert_eq!(ledger.snapshot("opaque-b"), Some(right_state));
    }

    #[test]
    fn exact_predecessor_binds_latest_evidence_and_computes_shadow_gap() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let first_head = control_head(7, 1, "resp-first");
        let first = ledger
            .try_begin_with_proof("scope-a", true, true, PredecessorProofReceipt::root(7, 1))
            .unwrap();
        let first_usage = usage(100_001, 0);
        let first_outcome = ledger.settle_with_outcome(
            &first,
            successful_with_head(&first_usage, first_head.clone()),
        );
        assert_eq!(first_outcome.status, "settled");
        assert!(!first_outcome.predecessor_bound);

        let second = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::Exact, Some(first_head)),
            )
            .unwrap();
        assert_eq!(
            second
                .prior_waterlines
                .unwrap()
                .latest_sent_prefix_bucket_tokens_128,
            99_968
        );
        let second_usage = usage(101_000, 99_584);
        let second_outcome = ledger.settle_with_outcome(
            &second,
            successful_with_head(&second_usage, control_head(7, 2, "resp-second")),
        );

        assert!(second_outcome.predecessor_bound);
        assert!(!second_outcome.continuity_reset);
        assert_eq!(second_outcome.candidate_avoidable_tokens_128, 384);
        assert_eq!(second_outcome.rollback_tokens_128, 0);
        let state = second_outcome.waterlines.unwrap();
        assert_eq!(state.latest_settled_prefix_bucket_tokens_128, 99_584);
        assert_eq!(state.latest_sent_prefix_bucket_tokens_128, 100_992);
    }

    #[test]
    fn exact_predecessor_cache_regression_then_recovery_keeps_history_and_freshness_distinct() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let first_head = control_head(11, 1, "resp-first");
        let first = ledger
            .try_begin_with_proof("scope-a", true, true, PredecessorProofReceipt::root(11, 1))
            .unwrap();
        ledger.settle_with_outcome(
            &first,
            successful_with_head(&usage(4_096, 3_968), first_head.clone()),
        );

        let second_head = control_head(11, 2, "resp-second");
        let regressed = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::Exact, Some(first_head)),
            )
            .unwrap();
        let regressed_outcome = ledger.settle_with_outcome(
            &regressed,
            successful_with_head(&usage(4_224, 3_584), second_head.clone()),
        );
        let regressed_state = regressed_outcome.waterlines.unwrap();
        assert_eq!(regressed_state.settled_prefix_bucket_tokens_128, 3_968);
        assert_eq!(
            regressed_state.latest_settled_prefix_bucket_tokens_128,
            3_584
        );
        assert_eq!(regressed_state.cache_regression_streak, 1);
        assert_eq!(regressed_state.stable_settlement_streak, 0);
        assert_eq!(regressed_outcome.rollback_tokens_128, 384);

        let recovered = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::Exact, Some(second_head)),
            )
            .unwrap();
        let recovered_outcome = ledger.settle_with_outcome(
            &recovered,
            successful_with_head(&usage(4_352, 4_224), control_head(11, 3, "resp-third")),
        );
        let recovered_state = recovered_outcome.waterlines.unwrap();
        assert_eq!(recovered_state.settled_prefix_bucket_tokens_128, 4_224);
        assert_eq!(
            recovered_state.latest_settled_prefix_bucket_tokens_128,
            4_224
        );
        assert_eq!(recovered_state.cache_regression_streak, 0);
        assert_eq!(recovered_state.stable_settlement_streak, 1);
        assert_eq!(recovered_outcome.rollback_tokens_128, 0);
    }

    #[test]
    fn divergent_full_replay_resets_historical_waterlines_before_new_baseline() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let first_head = control_head(3, 1, "resp-first");
        let first = ledger
            .try_begin_with_proof("scope-a", true, true, PredecessorProofReceipt::root(3, 2))
            .unwrap();
        let first_usage = usage(200_000, 199_936);
        ledger.settle_with_outcome(
            &first,
            successful_with_head(&first_usage, first_head.clone()),
        );

        let divergent = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::PrefixMismatch, Some(first_head)),
            )
            .unwrap();
        let divergent_usage = usage(20_000, 0);
        let outcome = ledger.settle_with_outcome(
            &divergent,
            successful_with_head(&divergent_usage, control_head(3, 2, "resp-divergent")),
        );

        assert!(outcome.continuity_reset);
        assert!(!outcome.predecessor_bound);
        assert_eq!(outcome.candidate_avoidable_tokens_128, 0);
        let state = outcome.waterlines.unwrap();
        assert_eq!(state.sent_prefix_bucket_tokens_128, 19_968);
        assert_eq!(state.settled_prefix_bucket_tokens_128, 0);
        assert_eq!(state.continuity_generation, 2);
    }

    #[test]
    fn concurrent_sibling_cannot_advance_after_another_head_wins() {
        let mut ledger = FinalScopeWaterlineLedger::default();
        let root_head = control_head(5, 1, "resp-root");
        let root = ledger
            .try_begin_with_proof("scope-a", true, true, PredecessorProofReceipt::root(5, 1))
            .unwrap();
        let root_usage = usage(50_000, 49_920);
        ledger.settle_with_outcome(&root, successful_with_head(&root_usage, root_head.clone()));

        let left = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::Exact, Some(root_head.clone())),
            )
            .unwrap();
        let right = ledger
            .try_begin_with_proof(
                "scope-a",
                true,
                true,
                proof(PredecessorProofStatus::Exact, Some(root_head)),
            )
            .unwrap();
        let left_usage = usage(51_000, 49_920);
        let left_outcome = ledger.settle_with_outcome(
            &left,
            successful_with_head(&left_usage, control_head(5, 2, "resp-left")),
        );
        assert_eq!(left_outcome.status, "settled");

        let right_usage = usage(90_000, 90_000);
        let right_outcome = ledger.settle_with_outcome(
            &right,
            successful_with_head(&right_usage, control_head(5, 2, "resp-right")),
        );
        assert_eq!(right_outcome.status, "ambiguous_branch");
        assert!(right_outcome.waterlines.is_none());
        assert_eq!(
            ledger.snapshot("scope-a").unwrap(),
            left_outcome.waterlines.unwrap()
        );
    }

    #[test]
    fn failed_same_scope_begins_never_refresh_settled_ttl() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut ledger = FinalScopeWaterlineLedger::new(2, ttl);
        let first = ledger
            .try_begin_at("scope-a", true, true, now)
            .expect("baseline ticket");
        let first_usage = usage(10_000, 9_984);
        ledger
            .settle_at(&first, WaterlineSettlement::successful(&first_usage), now)
            .expect("baseline settlement");

        for seconds in [3, 6, 9] {
            let ticket = ledger
                .try_begin_at("scope-a", true, true, now + Duration::from_secs(seconds))
                .expect("failure ticket");
            assert!(ledger
                .settle_at(
                    &ticket,
                    WaterlineSettlement {
                        upstream_succeeded: false,
                        compaction: false,
                        raw_usage: None,
                        committed_head: None,
                    },
                    now + Duration::from_secs(seconds),
                )
                .is_none());
        }

        let recreated = ledger
            .try_begin_at("scope-a", true, true, now + ttl)
            .expect("expired evidence creates a fresh ticket");
        assert_ne!(first.entry_generation, recreated.entry_generation);
        assert!(ledger.snapshot_at("scope-a", now + ttl).is_none());
    }

    #[test]
    fn observation_registry_is_bounded_and_counts_silent_outcomes() {
        let registry = FinalScopeObservationRegistry::default();
        registry.record_outcome("ticketed");
        registry.record_outcome("ticketed");
        registry.record_outcome("settle_lock_busy");
        for index in 0..(FINAL_SCOPE_OBSERVATION_CAPACITY + 3) {
            registry.try_record(FinalScopeWaterlineLog {
                scope_digest: format!("scope-{index}"),
                ..FinalScopeWaterlineLog::default()
            });
        }
        let recent_guard = registry.recent.lock().unwrap();
        registry.try_record(FinalScopeWaterlineLog {
            scope_digest: "must-be-dropped".to_string(),
            ..FinalScopeWaterlineLog::default()
        });
        drop(recent_guard);

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.version, 2);
        assert_eq!(snapshot.ring_dropped, 1);
        assert_eq!(snapshot.recent.len(), FINAL_SCOPE_OBSERVATION_CAPACITY);
        assert_eq!(snapshot.recent.first().unwrap().scope_digest, "scope-3");
        assert!(snapshot
            .recent
            .iter()
            .all(|item| item.scope_digest != "must-be-dropped"));
        assert_eq!(
            snapshot
                .outcomes
                .iter()
                .find(|item| item.outcome == "ticketed")
                .unwrap()
                .count,
            2
        );
        assert_eq!(
            snapshot
                .outcomes
                .iter()
                .find(|item| item.outcome == "settle_lock_busy")
                .unwrap()
                .count,
            1
        );
    }

    #[test]
    fn fixed_budget_sweep_reclaims_unvisited_expired_settled_scope() {
        let now = Instant::now();
        let mut ledger = FinalScopeWaterlineLedger::new(1, Duration::from_secs(10));
        let first = ledger
            .try_begin_at("scope-a", true, true, now)
            .expect("first ticket");
        ledger
            .settle_at(
                &first,
                WaterlineSettlement::successful(&usage(1_152, 1_024)),
                now,
            )
            .expect("first settled evidence");
        let second = ledger
            .try_begin_at("scope-b", true, true, now + Duration::from_secs(1))
            .expect("second ticket");
        let capacity_outcome = ledger.settle_with_outcome_at(
            &second,
            WaterlineSettlement::successful(&usage(1_152, 1_024)),
            now + Duration::from_secs(1),
        );
        assert_eq!(capacity_outcome.status, "capacity_full");

        assert_eq!(first.dispatch_seq, 1);
        assert_eq!(second.dispatch_seq, 2);
        assert!(ledger
            .snapshot_at("scope-a", now + Duration::from_secs(1))
            .is_some());
        assert!(ledger
            .snapshot_at("scope-b", now + Duration::from_secs(1))
            .is_none());

        let admitted = ledger
            .try_begin_at("scope-b", true, true, now + Duration::from_secs(10))
            .expect("new inbound gets a request-local generation");
        ledger
            .settle_at(
                &admitted,
                WaterlineSettlement::successful(&usage(1_152, 1_024)),
                now + Duration::from_secs(10),
            )
            .expect("bounded sweep reclaims the unvisited expired slot");
        assert!(ledger
            .snapshot_at("scope-a", now + Duration::from_secs(10))
            .is_none());
        assert!(ledger
            .snapshot_at("scope-b", now + Duration::from_secs(20))
            .is_none());
    }

    #[test]
    fn fixed_budget_sweep_advances_across_full_capacity_without_an_unbounded_scan() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut ledger = FinalScopeWaterlineLedger::new(16, ttl);
        for index in 0..16 {
            let scope = format!("scope-{index}");
            let ticket = ledger
                .try_begin_at(&scope, true, true, now)
                .expect("bounded ledger ticket");
            let settled_at = if index < FINAL_SCOPE_RECLAIM_BUDGET {
                now + Duration::from_secs(5)
            } else {
                now
            };
            ledger
                .settle_at(
                    &ticket,
                    WaterlineSettlement::successful(&usage(1_152, 1_024)),
                    settled_at,
                )
                .expect("fills one settled slot");
        }

        let first_attempt = ledger.try_begin_at("new-a", true, true, now + ttl).unwrap();
        let first_outcome = ledger.settle_with_outcome_at(
            &first_attempt,
            WaterlineSettlement::successful(&usage(1_152, 1_024)),
            now + ttl,
        );
        assert_eq!(first_outcome.status, "capacity_full");

        let second_attempt = ledger.try_begin_at("new-b", true, true, now + ttl).unwrap();
        let second_outcome = ledger.settle_with_outcome_at(
            &second_attempt,
            WaterlineSettlement::successful(&usage(1_152, 1_024)),
            now + ttl,
        );
        assert_eq!(second_outcome.status, "settled");
        assert!(ledger.entries.contains_key("new-b"));
    }
}
