use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex};

const LINEAGE_HEAD_TTL: Duration = Duration::from_secs(30 * 60);
const LINEAGE_TOMBSTONE_TTL: Duration = Duration::from_secs(5 * 60);
/// FullReplay heads retain the complete request input and output items. Keep
/// their process-local index small enough that many dormant conversations can
/// never accumulate without bound in a long-running desktop proxy.
const LINEAGE_SLOT_CAPACITY: usize = 128;
const LINEAGE_RETAINED_BYTES_CAPACITY: usize = 64 * 1024 * 1024;
const LINEAGE_GC_INTERVAL: u64 = 256;
const LINEAGE_GC_MIN_SLOTS: usize = 128;
/// A small index must still reclaim expired FullReplay bodies. Scanning fewer
/// than 128 slots is cheap; deferring it indefinitely is not.
const LINEAGE_SMALL_INDEX_GC_INTERVAL: u64 = 8;
/// A terminal frame normally reaches EOF in the same network turn. Never let
/// a provider that keeps the tail open turn this small publication fence into
/// an unbounded pre-dispatch wait. Timing out is safe: the next request stays
/// on FullReplay and the lineage CAS will reject an ambiguous late commit.
const TERMINAL_PUBLICATION_WAIT_BUDGET: Duration = Duration::from_millis(1);

#[derive(Debug, Clone)]
pub struct ResponseSessionState {
    pub generation: u64,
    #[allow(dead_code)]
    pub parent_generation: Option<u64>,
    pub response_id: String,
    /// Digest of the final non-input Responses wire shape that created this
    /// response. A native continuation is safe only when the next request
    /// presents the same static shape; the opaque digest never contains the
    /// request body or a credential.
    pub static_projection_digest: Option<String>,
    /// `true` only when the terminal response explicitly carried its complete
    /// output item array. Incremental `output_item.done` observations alone
    /// are useful for diagnostics but never authorize an upstream delta.
    pub output_items_complete: bool,
    pub input: Value,
    pub output_items: Vec<Value>,
    pub finished_at: Instant,
}

/// A successful FullReplay request whose upstream response omitted an id.
///
/// It deliberately contains no semantic continuation id and must never be
/// considered by `head()` or `managed_response_in_other_scope()`. Its only
/// purpose is to prove that a later FullReplay request preserved the exact
/// sent prefix, so cache waterlines do not regress to a synthetic root.
#[derive(Debug, Clone)]
pub struct ControlOnlyReplayState {
    generation: u64,
    control_identity: String,
    input: Value,
    finished_at: Instant,
}

/// A lineage predecessor that is admissible for FullReplay/cache control but
/// not necessarily for semantic response-id continuation.
#[derive(Debug, Clone)]
pub enum LineageControlHead {
    Semantic(Arc<ResponseSessionState>),
    FullReplayOnly(Arc<ControlOnlyReplayState>),
}

impl LineageControlHead {
    pub fn generation(&self) -> u64 {
        match self {
            Self::Semantic(state) => state.generation,
            Self::FullReplayOnly(state) => state.generation,
        }
    }

    pub fn input(&self) -> &Value {
        match self {
            Self::Semantic(state) => &state.input,
            Self::FullReplayOnly(state) => &state.input,
        }
    }

    /// An opaque local identity used exclusively by final-scope waterline
    /// CAS. For semantic heads this is the upstream id; for id-less successful
    /// FullReplay it is a local, non-routable control identity.
    pub fn control_identity(&self) -> &str {
        match self {
            Self::Semantic(state) => &state.response_id,
            Self::FullReplayOnly(state) => &state.control_identity,
        }
    }

    #[cfg(test)]
    pub fn semantic_response_id(&self) -> Option<&str> {
        match self {
            Self::Semantic(state) => Some(&state.response_id),
            Self::FullReplayOnly(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResponseSessionCandidate {
    pub response_id: String,
    /// Final-wire metadata is kept next to, not inside, the semantic input.
    /// It is never a substitute for the original full replay input.
    pub breakpoint_placement_digest: Option<String>,
    /// Final non-input Responses wire shape for the semantic head. Keep this
    /// next to the replay material so a future native delta cannot cross a
    /// tools/instructions/cache-control shape change.
    pub static_projection_digest: Option<String>,
    pub output_items_complete: bool,
    pub input: Value,
    pub output_items: Vec<Value>,
    pub finished_at: Instant,
}

/// Candidate retained only as canonical FullReplay prefix evidence after a
/// successful Responses result lacked a response id.
#[derive(Debug, Clone)]
pub struct ControlOnlyReplayCandidate {
    pub breakpoint_placement_digest: Option<String>,
    pub input: Value,
    pub finished_at: Instant,
}

#[derive(Debug, Clone)]
pub struct LineageLease {
    key: String,
    epoch: u64,
    expected_generation: u64,
    /// `true` only when the bounded terminal-publication fence elapsed before
    /// a semantic head could be safely exposed. This is intentionally carried
    /// into settlement so the narrowly safe rebase path cannot apply to an
    /// ordinary concurrent sibling.
    publication_timed_out: bool,
    /// Semantic response state is intentionally withheld while a terminal
    /// publication fence is still active.  It must never authorize local
    /// `previous_response_id` recovery from a response that has not finished
    /// settling yet.
    head: Option<Arc<ResponseSessionState>>,
    /// The last fully committed input is still a truthful FullReplay prefix
    /// even while a newer sibling is publishing its terminal state.  Keep it
    /// separate from `head`: it is control-only evidence for canonical-prefix
    /// proof and cache waterlines, never a semantic continuation authority.
    control_head: Option<Arc<LineageControlHead>>,
    control_breakpoint_placement_digest: Option<String>,
    /// A response id produced from an ambiguous FullReplay body. It is kept
    /// only so a later local proxy turn can strip it before dispatch; it must
    /// never authorize semantic recovery or cache-prefix proof.
    local_only_response_id: Option<String>,
}

impl LineageLease {
    pub fn key(&self) -> &str {
        &self.key
    }

    #[cfg(test)]
    pub fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    /// Returns the immutable identity of the lineage slot captured by this
    /// lease. The epoch is distinct from the mutable head generation: a
    /// compaction or a slot recreated after eviction receives a new epoch
    /// even when its generation happens to match an older slot.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn publication_timed_out(&self) -> bool {
        self.publication_timed_out
    }

    pub fn head(&self) -> Option<&Arc<ResponseSessionState>> {
        self.head.as_ref()
    }

    /// Returns the last committed FullReplay input that may be used only to
    /// prove an unchanged canonical prefix.  This deliberately remains
    /// available when `head()` is hidden by the terminal publication fence so
    /// concurrent siblings do not repeatedly reset cache-control continuity.
    pub fn control_head(&self) -> Option<&Arc<LineageControlHead>> {
        self.control_head.as_ref()
    }

    /// Cache-control placement associated with `control_head()`.  A `None`
    /// value is meaningful when a control head exists, so callers must first
    /// test `control_head()` rather than treating this as absence of evidence.
    pub fn control_breakpoint_placement_digest(&self) -> Option<&str> {
        self.control_breakpoint_placement_digest.as_deref()
    }

    pub fn has_local_only_response_id(&self, response_id: &str) -> bool {
        !response_id.trim().is_empty()
            && self.local_only_response_id.as_deref() == Some(response_id)
    }
}

#[derive(Debug, Clone)]
pub struct CompactionStart {
    lease: LineageLease,
    parent_matched: bool,
}

impl CompactionStart {
    #[cfg(test)]
    pub fn lease(&self) -> &LineageLease {
        &self.lease
    }

    pub fn into_lease(self) -> LineageLease {
        self.lease
    }

    pub fn parent_matched(&self) -> bool {
        self.parent_matched
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageParent {
    FullReplay,
    ExternalContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageCommitOutcome {
    Applied {
        generation: u64,
    },
    /// A terminal-fence-timed-out FullReplay was proven to be a strict
    /// extension of the head that won while it was in flight. This preserves
    /// continuity without ever accepting an ordinary sibling branch.
    Rebased {
        generation: u64,
        parent_generation: u64,
        parent_response_id: String,
    },
    Tombstoned {
        generation: u64,
    },
    /// The semantic response id is intentionally absent, but the successful
    /// FullReplay input was retained as a control-only predecessor.
    TombstonedWithControl {
        generation: u64,
        control_identity: String,
    },
    Stale {
        expected: u64,
        actual: u64,
    },
    EpochChanged {
        expected: u64,
        actual: u64,
    },
    ParentMismatch,
    Regressive,
    ExternalContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageInvalidateOutcome {
    Applied {
        generation: u64,
    },
    AppliedWithControl {
        generation: u64,
        control_identity: String,
    },
    /// An opaque local response id was retained solely to prevent it from
    /// leaking back to a third-party FullReplay upstream on the next turn.
    AppliedWithLocalReference {
        generation: u64,
    },
    Stale {
        expected: u64,
        actual: u64,
    },
    EpochChanged {
        expected: u64,
        actual: u64,
    },
    ParentMismatch,
}

#[derive(Debug, Clone)]
pub struct ContinuationLineageIndex {
    slots: Arc<Mutex<HashMap<String, LineageSlot>>>,
    terminal_publications: Arc<TerminalPublications>,
    next_epoch: Arc<AtomicU64>,
    operations: Arc<AtomicU64>,
    gc_running: Arc<AtomicBool>,
    max_slots: usize,
    max_retained_bytes: usize,
}

impl Default for ContinuationLineageIndex {
    fn default() -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
            terminal_publications: Arc::new(TerminalPublications::default()),
            next_epoch: Arc::new(AtomicU64::new(1)),
            operations: Arc::new(AtomicU64::new(0)),
            gc_running: Arc::new(AtomicBool::new(false)),
            max_slots: LINEAGE_SLOT_CAPACITY,
            max_retained_bytes: LINEAGE_RETAINED_BYTES_CAPACITY,
        }
    }
}

#[cfg(test)]
impl ContinuationLineageIndex {
    fn with_limits(max_slots: usize, max_retained_bytes: usize) -> Self {
        Self {
            max_slots: max_slots.max(1),
            max_retained_bytes,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LineageSlot {
    epoch: u64,
    generation: u64,
    head: Option<Arc<ResponseSessionState>>,
    control_only: Option<Arc<ControlOnlyReplayState>>,
    breakpoint_placement_digest: Option<String>,
    local_only_response_id: Option<String>,
    updated_at: Instant,
}

#[derive(Debug, Default)]
struct TerminalPublications {
    states: StdMutex<HashMap<String, TerminalPublicationState>>,
}

#[derive(Debug)]
struct TerminalPublicationState {
    owners: u64,
    active: watch::Sender<bool>,
}

/// A non-clone terminal-publication lease. Releasing the last owner wakes all
/// same-lineage requests that arrived after a terminal event became visible.
#[derive(Debug)]
pub struct TerminalPublicationGuard {
    publications: Arc<TerminalPublications>,
    key: Option<String>,
}

impl TerminalPublicationGuard {
    pub fn finish(mut self) {
        self.release();
    }

    fn release(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        self.publications.release(&key);
    }
}

impl Drop for TerminalPublicationGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl TerminalPublications {
    fn register(self: &Arc<Self>, key: &str) -> TerminalPublicationGuard {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = states.get_mut(key) {
            state.owners = state.owners.saturating_add(1);
        } else {
            let (active, _) = watch::channel(true);
            states.insert(
                key.to_string(),
                TerminalPublicationState { owners: 1, active },
            );
        }
        TerminalPublicationGuard {
            publications: self.clone(),
            key: Some(key.to_string()),
        }
    }

    async fn wait(&self, key: &str) -> bool {
        let receiver = {
            self.states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .map(|state| state.active.subscribe())
        };
        let Some(mut receiver) = receiver else {
            return true;
        };
        let wait_for_publication = async {
            while *receiver.borrow_and_update() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        };
        tokio::time::timeout(TERMINAL_PUBLICATION_WAIT_BUDGET, wait_for_publication)
            .await
            .is_ok()
    }

    fn release(&self, key: &str) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = match states.get_mut(key) {
            Some(state) => {
                state.owners = state.owners.saturating_sub(1);
                if state.owners == 0 {
                    state.active.send_replace(false);
                    true
                } else {
                    false
                }
            }
            None => false,
        };
        if remove {
            states.remove(key);
        }
    }
}

impl ContinuationLineageIndex {
    /// Marks the small interval between exposing a terminal event and
    /// publishing its in-memory lineage/control state.
    pub fn register_terminal_publication(&self, key: &str) -> TerminalPublicationGuard {
        self.terminal_publications.register(key)
    }

    pub async fn begin(&self, key: &str) -> LineageLease {
        let publication_completed = self.terminal_publications.wait(key).await;
        let mut slots = self.slots.lock().await;
        if !slots.contains_key(key) {
            slots.insert(key.to_string(), self.new_slot(0, None));
            self.enforce_retention_limits(&mut slots, key);
        }
        let slot = slots
            .get(key)
            .expect("a lineage slot inserted above must be present");
        let lease = LineageLease {
            key: key.to_string(),
            epoch: slot.epoch,
            expected_generation: slot.generation,
            publication_timed_out: !publication_completed,
            // A timed-out publication may still expose the previous head.
            // Force a safe FullReplay lease instead of attempting delta from
            // stale lineage; the generation/epoch CAS still prevents a late
            // sibling from overwriting newer state.
            head: publication_completed.then(|| slot.head.clone()).flatten(),
            // A timeout must not turn a previously committed FullReplay input
            // into a synthetic root. The input remains a truthful predecessor
            // for canonical-prefix proof, but stays separate from `head` so it
            // cannot authorize semantic response-id recovery.
            control_head: slot
                .head
                .clone()
                .map(LineageControlHead::Semantic)
                .or_else(|| {
                    slot.control_only
                        .clone()
                        .map(LineageControlHead::FullReplayOnly)
                })
                .map(Arc::new),
            control_breakpoint_placement_digest: slot.breakpoint_placement_digest.clone(),
            local_only_response_id: slot.local_only_response_id.clone(),
        };
        let slot_count = slots.len();
        drop(slots);
        self.maybe_schedule_gc(slot_count);
        lease
    }

    pub async fn begin_compaction(
        &self,
        key: &str,
        expected_parent_response_id: Option<&str>,
    ) -> CompactionStart {
        let publication_completed = self.terminal_publications.wait(key).await;
        let mut slots = self.slots.lock().await;
        let previous = slots.get(key);
        let parent_matched = publication_completed
            && expected_parent_response_id.is_none_or(|expected| {
                previous
                    .and_then(|slot| slot.head.as_ref())
                    .is_some_and(|head| head.response_id == expected)
            });
        let generation = previous
            .map(|slot| slot.generation)
            .unwrap_or(0)
            .checked_add(1)
            .expect("continuation lineage generation overflow");
        let slot = self.new_slot(generation, None);
        let lease = LineageLease {
            key: key.to_string(),
            epoch: slot.epoch,
            expected_generation: slot.generation,
            publication_timed_out: false,
            head: None,
            control_head: None,
            control_breakpoint_placement_digest: None,
            local_only_response_id: None,
        };
        slots.insert(key.to_string(), slot);
        self.enforce_retention_limits(&mut slots, key);
        let slot_count = slots.len();
        drop(slots);
        self.maybe_schedule_gc(slot_count);
        CompactionStart {
            lease,
            parent_matched,
        }
    }

    pub async fn confirm_compaction(&self, lease: &LineageLease) -> Option<LineageLease> {
        let mut slots = self.slots.lock().await;
        let current = slots.get(lease.key())?;
        if current.epoch != lease.epoch {
            return None;
        }
        let generation = current
            .generation
            .checked_add(1)
            .expect("continuation lineage generation overflow");
        let slot = self.new_slot(generation, None);
        let confirmed = LineageLease {
            key: lease.key.clone(),
            epoch: slot.epoch,
            expected_generation: slot.generation,
            publication_timed_out: false,
            head: None,
            control_head: None,
            control_breakpoint_placement_digest: None,
            local_only_response_id: None,
        };
        slots.insert(lease.key.clone(), slot);
        self.enforce_retention_limits(&mut slots, lease.key());
        let slot_count = slots.len();
        drop(slots);
        self.maybe_schedule_gc(slot_count);
        Some(confirmed)
    }

    pub async fn commit(
        &self,
        lease: &LineageLease,
        parent: &LineageParent,
        candidate: ResponseSessionCandidate,
        replacement_allowed: bool,
    ) -> LineageCommitOutcome {
        if matches!(parent, LineageParent::ExternalContinuation) {
            return LineageCommitOutcome::ExternalContinuation;
        }
        let mut slots = self.slots.lock().await;
        let outcome = apply_commit(&mut slots, lease, parent, candidate, replacement_allowed);
        self.enforce_retention_limits(&mut slots, lease.key());
        let slot_count = slots.len();
        drop(slots);
        if matches!(
            outcome,
            LineageCommitOutcome::Applied { .. } | LineageCommitOutcome::Rebased { .. }
        ) {
            self.maybe_schedule_gc(slot_count);
        }
        outcome
    }

    /// Publishes an already-prepared terminal head without yielding when the
    /// in-memory lineage mutex is uncontended. Contention falls back to the
    /// same async CAS; no disk or network work is part of this handoff.
    pub async fn commit_fast(
        &self,
        lease: &LineageLease,
        parent: &LineageParent,
        candidate: ResponseSessionCandidate,
        replacement_allowed: bool,
    ) -> LineageCommitOutcome {
        if matches!(parent, LineageParent::ExternalContinuation) {
            return LineageCommitOutcome::ExternalContinuation;
        }
        match self.slots.try_lock() {
            Ok(mut slots) => {
                let outcome =
                    apply_commit(&mut slots, lease, parent, candidate, replacement_allowed);
                self.enforce_retention_limits(&mut slots, lease.key());
                let slot_count = slots.len();
                drop(slots);
                if matches!(
                    outcome,
                    LineageCommitOutcome::Applied { .. } | LineageCommitOutcome::Rebased { .. }
                ) {
                    self.maybe_schedule_gc(slot_count);
                }
                outcome
            }
            Err(_) => {
                self.commit(lease, parent, candidate, replacement_allowed)
                    .await
            }
        }
    }

    pub async fn invalidate(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
    ) -> LineageInvalidateOutcome {
        let mut slots = self.slots.lock().await;
        let outcome = apply_invalidation(&mut slots, lease, expected_response_id, None, None);
        let slot_count = slots.len();
        drop(slots);
        if matches!(
            outcome,
            LineageInvalidateOutcome::Applied { .. }
                | LineageInvalidateOutcome::AppliedWithControl { .. }
                | LineageInvalidateOutcome::AppliedWithLocalReference { .. }
        ) {
            self.maybe_schedule_gc(slot_count);
        }
        outcome
    }

    pub async fn invalidate_fast(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
    ) -> LineageInvalidateOutcome {
        match self.slots.try_lock() {
            Ok(mut slots) => {
                let outcome =
                    apply_invalidation(&mut slots, lease, expected_response_id, None, None);
                let slot_count = slots.len();
                drop(slots);
                if matches!(
                    outcome,
                    LineageInvalidateOutcome::Applied { .. }
                        | LineageInvalidateOutcome::AppliedWithControl { .. }
                        | LineageInvalidateOutcome::AppliedWithLocalReference { .. }
                ) {
                    self.maybe_schedule_gc(slot_count);
                }
                outcome
            }
            Err(_) => self.invalidate(lease, expected_response_id).await,
        }
    }

    /// Hides the semantic response head while retaining only the exact
    /// successful FullReplay input for cache-control proof. This is never a
    /// substitute for `previous_response_id` recovery.
    pub async fn tombstone_with_control_prefix(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
        candidate: ControlOnlyReplayCandidate,
    ) -> LineageInvalidateOutcome {
        let mut slots = self.slots.lock().await;
        let outcome = apply_invalidation(
            &mut slots,
            lease,
            expected_response_id,
            Some(candidate),
            None,
        );
        let slot_count = slots.len();
        drop(slots);
        if matches!(outcome, LineageInvalidateOutcome::AppliedWithControl { .. }) {
            self.maybe_schedule_gc(slot_count);
        }
        outcome
    }

    /// Fast-path counterpart to `tombstone_with_control_prefix`; contention
    /// uses the same async CAS and never creates a second state transition.
    pub async fn tombstone_with_control_prefix_fast(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
        candidate: ControlOnlyReplayCandidate,
    ) -> LineageInvalidateOutcome {
        match self.slots.try_lock() {
            Ok(mut slots) => {
                let outcome = apply_invalidation(
                    &mut slots,
                    lease,
                    expected_response_id,
                    Some(candidate),
                    None,
                );
                let slot_count = slots.len();
                drop(slots);
                if matches!(outcome, LineageInvalidateOutcome::AppliedWithControl { .. }) {
                    self.maybe_schedule_gc(slot_count);
                }
                outcome
            }
            Err(_) => {
                self.tombstone_with_control_prefix(lease, expected_response_id, candidate)
                    .await
            }
        }
    }

    /// Invalidates a semantic head while retaining only the newly produced
    /// local response id. The id is an opaque anti-leak reference: a later
    /// FullReplay request may strip it, but can never use it to reconstruct
    /// input or prove a cache prefix.
    pub async fn tombstone_with_local_response_id(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
        local_response_id: String,
    ) -> LineageInvalidateOutcome {
        let mut slots = self.slots.lock().await;
        let outcome = apply_invalidation(
            &mut slots,
            lease,
            expected_response_id,
            None,
            Some(local_response_id),
        );
        let slot_count = slots.len();
        drop(slots);
        if matches!(
            outcome,
            LineageInvalidateOutcome::AppliedWithLocalReference { .. }
        ) {
            self.maybe_schedule_gc(slot_count);
        }
        outcome
    }

    /// Fast-path counterpart to [`Self::tombstone_with_local_response_id`].
    /// It performs no I/O and never creates a second state transition.
    pub async fn tombstone_with_local_response_id_fast(
        &self,
        lease: &LineageLease,
        expected_response_id: Option<&str>,
        local_response_id: String,
    ) -> LineageInvalidateOutcome {
        match self.slots.try_lock() {
            Ok(mut slots) => {
                let outcome = apply_invalidation(
                    &mut slots,
                    lease,
                    expected_response_id,
                    None,
                    Some(local_response_id),
                );
                let slot_count = slots.len();
                drop(slots);
                if matches!(
                    outcome,
                    LineageInvalidateOutcome::AppliedWithLocalReference { .. }
                ) {
                    self.maybe_schedule_gc(slot_count);
                }
                outcome
            }
            Err(_) => {
                self.tombstone_with_local_response_id(
                    lease,
                    expected_response_id,
                    local_response_id,
                )
                .await
            }
        }
    }

    pub async fn is_current(&self, lease: &LineageLease) -> bool {
        self.slots
            .lock()
            .await
            .get(lease.key())
            .is_some_and(|slot| {
                slot.epoch == lease.epoch && slot.generation == lease.expected_generation
            })
    }

    /// A managed response id is valid only for the scope that produced it.
    /// This returns the local replay material when an Agent carries a
    /// provider/model-bound `previous_response_id` across a hot route switch.
    pub async fn managed_response_in_other_scope(
        &self,
        current_key: &str,
        response_id: &str,
    ) -> Option<ResponseSessionState> {
        let response_id = response_id.trim();
        if response_id.is_empty() {
            return None;
        }
        let slots = self.slots.lock().await;
        // Response ids are upstream-owned and may collide across independent
        // routes. A current-scope head or local-only marker therefore wins
        // before looking for a stale copy elsewhere; otherwise an old route's
        // replay material could be spliced into the active route.
        if slots.get(current_key).is_some_and(|slot| {
            slot.head
                .as_ref()
                .is_some_and(|head| head.response_id == response_id)
                || slot.local_only_response_id.as_deref() == Some(response_id)
        }) {
            return None;
        }
        slots.iter().find_map(|(key, slot)| {
            (key != current_key)
                .then(|| slot.head.as_ref())
                .flatten()
                .filter(|head| head.response_id == response_id)
                .map(|head| (**head).clone())
        })
    }

    /// Reports whether an opaque local-only response id belongs to any other
    /// scope. Unlike `managed_response_in_other_scope`, no replay material is
    /// returned: the caller can only strip the id before one ordinary request.
    pub async fn has_local_only_response_id_in_other_scope(
        &self,
        current_key: &str,
        response_id: &str,
    ) -> bool {
        let response_id = response_id.trim();
        if response_id.is_empty() {
            return false;
        }
        let slots = self.slots.lock().await;
        if slots.get(current_key).is_some_and(|slot| {
            slot.head
                .as_ref()
                .is_some_and(|head| head.response_id == response_id)
                || slot.local_only_response_id.as_deref() == Some(response_id)
        }) {
            return false;
        }
        slots.iter().any(|(key, slot)| {
            key != current_key && slot.local_only_response_id.as_deref() == Some(response_id)
        })
    }

    #[cfg(test)]
    pub async fn head(&self, key: &str) -> Option<Arc<ResponseSessionState>> {
        self.slots
            .lock()
            .await
            .get(key)
            .and_then(|slot| slot.head.clone())
    }

    #[cfg(test)]
    pub async fn snapshot_heads(&self) -> HashMap<String, Arc<ResponseSessionState>> {
        self.slots
            .lock()
            .await
            .iter()
            .filter_map(|(key, slot)| slot.head.clone().map(|head| (key.clone(), head)))
            .collect()
    }

    #[cfg(test)]
    pub async fn is_empty(&self) -> bool {
        self.slots
            .lock()
            .await
            .values()
            .all(|slot| slot.head.is_none())
    }

    #[cfg(test)]
    pub async fn contains_head(&self, key: &str) -> bool {
        self.slots
            .lock()
            .await
            .get(key)
            .is_some_and(|slot| slot.head.is_some())
    }

    #[cfg(test)]
    pub async fn seed_for_test(&self, key: &str, mut state: ResponseSessionState) {
        let mut slots = self.slots.lock().await;
        let generation = state.generation.max(1);
        state.generation = generation;
        slots.insert(
            key.to_string(),
            LineageSlot {
                epoch: self.allocate_epoch(),
                generation,
                head: Some(Arc::new(state)),
                control_only: None,
                breakpoint_placement_digest: None,
                local_only_response_id: None,
                updated_at: Instant::now(),
            },
        );
        self.enforce_retention_limits(&mut slots, key);
    }

    #[cfg(test)]
    pub(crate) async fn hold_mutations_for_test(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<HashMap<String, LineageSlot>> {
        self.slots.clone().lock_owned().await
    }

    fn allocate_epoch(&self) -> u64 {
        self.next_epoch.fetch_add(1, Ordering::Relaxed).max(1)
    }

    fn new_slot(&self, generation: u64, head: Option<Arc<ResponseSessionState>>) -> LineageSlot {
        LineageSlot {
            epoch: self.allocate_epoch(),
            generation,
            head,
            control_only: None,
            breakpoint_placement_digest: None,
            local_only_response_id: None,
            updated_at: Instant::now(),
        }
    }

    fn maybe_schedule_gc(&self, slot_count: usize) {
        let operation = self.operations.fetch_add(1, Ordering::Relaxed) + 1;
        let interval = if slot_count < LINEAGE_GC_MIN_SLOTS {
            LINEAGE_SMALL_INDEX_GC_INTERVAL
        } else {
            LINEAGE_GC_INTERVAL
        };
        if operation % interval != 0
            || self
                .gc_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let index = self.clone();
        tokio::spawn(async move {
            index
                .prune_expired(LINEAGE_HEAD_TTL, LINEAGE_TOMBSTONE_TTL)
                .await;
            index.gc_running.store(false, Ordering::Release);
        });
    }

    async fn prune_expired(&self, head_ttl: Duration, tombstone_ttl: Duration) {
        self.slots.lock().await.retain(|_, slot| {
            slot.head
                .as_ref()
                .map(|head| head.finished_at.elapsed() <= head_ttl)
                .or_else(|| {
                    slot.control_only
                        .as_ref()
                        .map(|head| head.finished_at.elapsed() <= head_ttl)
                })
                .unwrap_or_else(|| slot.updated_at.elapsed() <= tombstone_ttl)
        });
    }

    /// Reclaim oldest dormant lineage slots before complete FullReplay bodies
    /// can accumulate without bound in a long-running desktop process. The
    /// current request's scope is protected: evicting another scope merely
    /// drops optional local continuation aid and never affects its upstream
    /// request or the user's selected provider.
    fn enforce_retention_limits(
        &self,
        slots: &mut HashMap<String, LineageSlot>,
        protected_key: &str,
    ) {
        while slots.len() > self.max_slots
            || lineage_slots_retained_bytes(slots) > self.max_retained_bytes
        {
            let oldest = slots
                .iter()
                .filter(|(key, _)| key.as_str() != protected_key)
                .min_by_key(|(_, slot)| slot.updated_at)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                // Do not discard an active body mid-prepare merely to meet a
                // cache budget. Normal terminal settlement/TTL will release
                // it shortly afterwards.
                break;
            };
            slots.remove(&oldest);
        }
    }

    #[cfg(test)]
    async fn prune_all_for_test(&self) {
        self.prune_expired(Duration::ZERO, Duration::ZERO).await;
    }
}

fn lineage_slots_retained_bytes(slots: &HashMap<String, LineageSlot>) -> usize {
    slots
        .iter()
        .map(|(key, slot)| key.len().saturating_add(lineage_slot_retained_bytes(slot)))
        .fold(0usize, usize::saturating_add)
}

fn lineage_slot_retained_bytes(slot: &LineageSlot) -> usize {
    let head_bytes = slot.head.as_ref().map_or(0, |head| {
        head.response_id
            .len()
            .saturating_add(value_retained_bytes(&head.input))
            .saturating_add(
                head.output_items
                    .iter()
                    .map(value_retained_bytes)
                    .fold(0usize, usize::saturating_add),
            )
    });
    let control_bytes = slot
        .control_only
        .as_ref()
        .map_or(0, |head| value_retained_bytes(&head.input));
    head_bytes
        .saturating_add(control_bytes)
        .saturating_add(slot.local_only_response_id.as_ref().map_or(0, String::len))
}

/// Allocation-free, conservative estimate used only for local retention.
/// It never serializes or logs request content.
fn value_retained_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(number) => number.to_string().len(),
        Value::String(text) => text.len(),
        Value::Array(items) => items.iter().map(value_retained_bytes).fold(
            items.len().saturating_mul(std::mem::size_of::<Value>()),
            usize::saturating_add,
        ),
        Value::Object(map) => map.iter().fold(
            map.len()
                .saturating_mul(std::mem::size_of::<(String, Value)>()),
            |total, (key, value)| {
                total
                    .saturating_add(key.len())
                    .saturating_add(value_retained_bytes(value))
            },
        ),
    }
}

fn apply_commit(
    slots: &mut HashMap<String, LineageSlot>,
    lease: &LineageLease,
    parent: &LineageParent,
    candidate: ResponseSessionCandidate,
    replacement_allowed: bool,
) -> LineageCommitOutcome {
    let Some(slot) = slots.get_mut(lease.key()) else {
        return LineageCommitOutcome::EpochChanged {
            expected: lease.epoch,
            actual: 0,
        };
    };
    if slot.epoch != lease.epoch {
        return LineageCommitOutcome::EpochChanged {
            expected: lease.epoch,
            actual: slot.epoch,
        };
    }
    if !replacement_allowed {
        return LineageCommitOutcome::Regressive;
    }
    let current_generation = slot.generation;
    let rebased = current_generation != lease.expected_generation
        && lease.publication_timed_out()
        && matches!(parent, LineageParent::FullReplay)
        && slot.head.as_ref().is_some_and(|current| {
            full_replay_input_strictly_extends(&current.input, &candidate.input)
                && slot.breakpoint_placement_digest == candidate.breakpoint_placement_digest
        });
    if current_generation != lease.expected_generation && !rebased {
        return LineageCommitOutcome::Stale {
            expected: lease.expected_generation,
            actual: current_generation,
        };
    }
    let rebased_parent = rebased.then(|| {
        let parent = slot
            .head
            .as_ref()
            .expect("a rebased commit must have an already committed parent");
        (parent.generation, parent.response_id.clone())
    });
    let generation = current_generation
        .checked_add(1)
        .expect("continuation lineage generation overflow");
    let parent_generation = match parent {
        LineageParent::FullReplay => None,
        LineageParent::ExternalContinuation => return LineageCommitOutcome::ExternalContinuation,
    };
    let head = Arc::new(ResponseSessionState {
        generation,
        parent_generation,
        response_id: candidate.response_id,
        static_projection_digest: candidate.static_projection_digest,
        output_items_complete: candidate.output_items_complete,
        input: candidate.input,
        output_items: candidate.output_items,
        finished_at: candidate.finished_at,
    });
    slot.generation = generation;
    slot.head = Some(head);
    slot.control_only = None;
    slot.breakpoint_placement_digest = candidate.breakpoint_placement_digest;
    slot.local_only_response_id = None;
    slot.updated_at = Instant::now();
    if let Some((parent_generation, parent_response_id)) = rebased_parent {
        // This was captured before replacing the slot. It is used only by the
        // final-scope ledger to prove that the winning head is exactly the
        // parent observed during the strict rebase, never exposed in metrics.
        LineageCommitOutcome::Rebased {
            generation,
            parent_generation,
            parent_response_id,
        }
    } else {
        LineageCommitOutcome::Applied { generation }
    }
}

/// A late terminal-fence request may join the lineage only when its complete
/// FullReplay request literally preserves every item of the newly committed
/// head and appends at least one item. This is intentionally stricter than a
/// generic response replacement: sibling branches, compaction, and external
/// continuations cannot satisfy it.
fn full_replay_input_strictly_extends(previous: &Value, current: &Value) -> bool {
    let (Some(previous_items), Some(current_items)) = (previous.as_array(), current.as_array())
    else {
        return false;
    };
    previous_items.len() < current_items.len()
        && previous_items
            .iter()
            .zip(current_items.iter())
            .all(|(previous, current)| previous == current)
}

fn apply_invalidation(
    slots: &mut HashMap<String, LineageSlot>,
    lease: &LineageLease,
    expected_response_id: Option<&str>,
    control_candidate: Option<ControlOnlyReplayCandidate>,
    local_only_response_id: Option<String>,
) -> LineageInvalidateOutcome {
    let Some(slot) = slots.get_mut(lease.key()) else {
        return LineageInvalidateOutcome::EpochChanged {
            expected: lease.epoch,
            actual: 0,
        };
    };
    if slot.epoch != lease.epoch {
        return LineageInvalidateOutcome::EpochChanged {
            expected: lease.epoch,
            actual: slot.epoch,
        };
    }
    let current_generation = slot.generation;
    if current_generation != lease.expected_generation {
        return LineageInvalidateOutcome::Stale {
            expected: lease.expected_generation,
            actual: current_generation,
        };
    }
    if let Some(expected_response_id) = expected_response_id {
        let current_response_id = slot.head.as_ref().map(|head| head.response_id.as_str());
        if current_response_id != Some(expected_response_id) {
            return LineageInvalidateOutcome::ParentMismatch;
        }
    }
    let generation = current_generation
        .checked_add(1)
        .expect("continuation lineage generation overflow");
    let (control_only, breakpoint_placement_digest, local_only_response_id, outcome) =
        if let Some(candidate) = control_candidate {
            let control_identity = format!(
                "atoapi-full-replay-control-v1:{}:{}",
                slot.epoch, generation
            );
            (
                Some(Arc::new(ControlOnlyReplayState {
                    generation,
                    control_identity: control_identity.clone(),
                    input: candidate.input,
                    finished_at: candidate.finished_at,
                })),
                candidate.breakpoint_placement_digest,
                None,
                LineageInvalidateOutcome::AppliedWithControl {
                    generation,
                    control_identity,
                },
            )
        } else if let Some(response_id) =
            local_only_response_id.filter(|response_id| !response_id.trim().is_empty())
        {
            (
                None,
                None,
                Some(response_id),
                LineageInvalidateOutcome::AppliedWithLocalReference { generation },
            )
        } else {
            (
                None,
                None,
                None,
                LineageInvalidateOutcome::Applied { generation },
            )
        };
    slot.generation = generation;
    slot.head = None;
    slot.control_only = control_only;
    slot.breakpoint_placement_digest = breakpoint_placement_digest;
    slot.local_only_response_id = local_only_response_id;
    slot.updated_at = Instant::now();
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn candidate(response_id: &str, input: &str) -> ResponseSessionCandidate {
        ResponseSessionCandidate {
            response_id: response_id.to_string(),
            breakpoint_placement_digest: None,
            static_projection_digest: None,
            output_items_complete: false,
            input: json!([{"type":"message","role":"user","content":input}]),
            output_items: Vec::new(),
            finished_at: Instant::now(),
        }
    }

    fn candidate_with_input(
        response_id: &str,
        input: Value,
        breakpoint_placement_digest: Option<&str>,
    ) -> ResponseSessionCandidate {
        ResponseSessionCandidate {
            response_id: response_id.to_string(),
            breakpoint_placement_digest: breakpoint_placement_digest.map(ToOwned::to_owned),
            static_projection_digest: None,
            output_items_complete: false,
            input,
            output_items: Vec::new(),
            finished_at: Instant::now(),
        }
    }

    #[tokio::test]
    async fn opaque_local_response_id_blocks_leak_without_becoming_a_semantic_head() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread-a").await;
        assert!(matches!(
            index
                .commit_fast(
                    &root,
                    &LineageParent::FullReplay,
                    candidate("resp-old", "old"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { .. }
        ));
        let lease = index.begin("thread-a").await;
        assert!(matches!(
            index
                .tombstone_with_local_response_id_fast(
                    &lease,
                    Some("resp-old"),
                    "resp-opaque".to_string(),
                )
                .await,
            LineageInvalidateOutcome::AppliedWithLocalReference { .. }
        ));

        let next = index.begin("thread-a").await;
        assert!(next.head().is_none());
        assert!(next.control_head().is_none());
        assert!(next.has_local_only_response_id("resp-opaque"));
        assert!(
            index
                .has_local_only_response_id_in_other_scope("thread-b", "resp-opaque")
                .await
        );
        assert!(
            !index
                .has_local_only_response_id_in_other_scope("thread-a", "resp-opaque")
                .await
        );
    }

    #[tokio::test]
    async fn current_scope_response_id_wins_over_a_colliding_old_scope() {
        let index = ContinuationLineageIndex::default();
        let old = index.begin("old-route").await;
        assert!(matches!(
            index
                .commit_fast(
                    &old,
                    &LineageParent::FullReplay,
                    candidate("resp-shared", "old route history"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { .. }
        ));
        let current = index.begin("current-route").await;
        assert!(matches!(
            index
                .commit_fast(
                    &current,
                    &LineageParent::FullReplay,
                    candidate("resp-shared", "current route history"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { .. }
        ));

        assert!(
            index
                .managed_response_in_other_scope("current-route", "resp-shared")
                .await
                .is_none(),
            "a response id owned by the current scope must never recover replay material from an old route"
        );
    }

    #[tokio::test]
    async fn current_scope_local_only_id_wins_over_a_colliding_old_scope() {
        let index = ContinuationLineageIndex::default();
        let old = index.begin("old-route").await;
        assert!(matches!(
            index
                .tombstone_with_local_response_id_fast(&old, None, "resp-shared".to_string(),)
                .await,
            LineageInvalidateOutcome::AppliedWithLocalReference { .. }
        ));
        let current = index.begin("current-route").await;
        assert!(matches!(
            index
                .tombstone_with_local_response_id_fast(&current, None, "resp-shared".to_string(),)
                .await,
            LineageInvalidateOutcome::AppliedWithLocalReference { .. }
        ));

        assert!(
            !index
                .has_local_only_response_id_in_other_scope("current-route", "resp-shared")
                .await,
            "a local-only id owned by the current scope must not be misclassified as stale from an old route"
        );
    }

    #[tokio::test]
    async fn retention_capacity_evicts_the_oldest_other_scope_without_touching_the_active_one() {
        let index = ContinuationLineageIndex::with_limits(2, usize::MAX);
        for (key, response_id) in [
            ("scope-a", "resp-a"),
            ("scope-b", "resp-b"),
            ("scope-c", "resp-c"),
        ] {
            let lease = index.begin(key).await;
            assert!(matches!(
                index
                    .commit_fast(
                        &lease,
                        &LineageParent::FullReplay,
                        candidate(response_id, key),
                        true,
                    )
                    .await,
                LineageCommitOutcome::Applied { .. }
            ));
            std::thread::sleep(Duration::from_millis(2));
        }

        let heads = index.snapshot_heads().await;
        assert_eq!(heads.len(), 2);
        assert!(!heads.contains_key("scope-a"));
        assert!(heads.contains_key("scope-b"));
        assert!(heads.contains_key("scope-c"));
    }

    #[tokio::test]
    async fn retention_byte_budget_evicts_an_older_full_replay_body() {
        let index = ContinuationLineageIndex::with_limits(8, 700);
        let first = index.begin("scope-a").await;
        assert!(matches!(
            index
                .commit_fast(
                    &first,
                    &LineageParent::FullReplay,
                    candidate_with_input(
                        "resp-a",
                        json!([{ "type": "message", "content": "a".repeat(512) }]),
                        None,
                    ),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { .. }
        ));
        std::thread::sleep(Duration::from_millis(2));
        let second = index.begin("scope-b").await;
        assert!(matches!(
            index
                .commit_fast(
                    &second,
                    &LineageParent::FullReplay,
                    candidate_with_input(
                        "resp-b",
                        json!([{ "type": "message", "content": "b".repeat(512) }]),
                        None,
                    ),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { .. }
        ));

        let heads = index.snapshot_heads().await;
        assert_eq!(heads.len(), 1);
        assert!(!heads.contains_key("scope-a"));
        assert!(heads.contains_key("scope-b"));
    }

    #[tokio::test]
    async fn terminal_publication_fence_waits_for_every_owner_and_then_reads_the_new_head() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        let first = index.register_terminal_publication("thread");
        let second = index.register_terminal_publication("thread");

        let waiting_index = index.clone();
        let waiter = tokio::spawn(async move { waiting_index.begin("thread").await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        assert_eq!(
            index
                .commit_fast(
                    &root,
                    &LineageParent::FullReplay,
                    candidate("resp-root", "root"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 1 }
        );
        first.finish();
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(second);

        let lease = waiter.await.unwrap();
        assert_eq!(lease.expected_generation(), 1);
        assert_eq!(lease.head().unwrap().response_id, "resp-root");
    }

    #[tokio::test]
    async fn terminal_publication_fence_never_waits_for_a_stuck_upstream_tail() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        assert_eq!(
            index
                .commit_fast(
                    &root,
                    &LineageParent::FullReplay,
                    candidate("resp-old", "old"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 1 }
        );
        let publication = index.register_terminal_publication("thread");

        let lease = tokio::time::timeout(
            TERMINAL_PUBLICATION_WAIT_BUDGET + Duration::from_millis(250),
            index.begin("thread"),
        )
        .await
        .expect("a stuck terminal tail must not block the next request indefinitely");

        assert_eq!(lease.expected_generation(), 1);
        assert!(
            lease.head().is_none(),
            "a timed-out fence must force FullReplay instead of exposing the stale head"
        );
        assert_eq!(
            lease
                .control_head()
                .and_then(|head| head.semantic_response_id()),
            Some("resp-old"),
            "the last committed input remains valid control evidence even though it cannot authorize semantic response-id recovery"
        );
        drop(publication);
    }

    #[tokio::test]
    async fn stale_sibling_cannot_overwrite_the_winning_head() {
        let index = ContinuationLineageIndex::default();
        let root_lease = index.begin("thread").await;
        assert_eq!(
            index
                .commit(
                    &root_lease,
                    &LineageParent::FullReplay,
                    candidate("resp-root", "root"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 1 }
        );

        let left = index.begin("thread").await;
        let right = index.begin("thread").await;
        assert_eq!(left.expected_generation(), 1);
        assert_eq!(right.expected_generation(), 1);
        let parent = LineageParent::FullReplay;
        assert_eq!(
            index
                .commit(&right, &parent, candidate("resp-right", "right"), true)
                .await,
            LineageCommitOutcome::Applied { generation: 2 }
        );
        assert_eq!(
            index
                .commit(&left, &parent, candidate("resp-left", "left"), true)
                .await,
            LineageCommitOutcome::Stale {
                expected: 1,
                actual: 2
            }
        );
        let head = index.head("thread").await.unwrap();
        assert_eq!(head.response_id, "resp-right");
        assert_eq!(head.parent_generation, None);
    }

    #[tokio::test]
    async fn terminal_publication_timeout_rebases_only_a_strict_full_replay_extension() {
        let index = ContinuationLineageIndex::default();
        let base = json!([{"type":"message","role":"user","content":"base"}]);
        index
            .seed_for_test(
                "thread",
                ResponseSessionState {
                    generation: 1,
                    parent_generation: None,
                    response_id: "resp-base".to_string(),
                    static_projection_digest: None,
                    output_items_complete: false,
                    input: base.clone(),
                    output_items: Vec::new(),
                    finished_at: Instant::now(),
                },
            )
            .await;
        let terminal_owner = index.register_terminal_publication("thread");
        let parent = index.begin("thread").await;
        let timed_out_child = index.begin("thread").await;
        assert!(timed_out_child.publication_timed_out());
        assert!(timed_out_child.head().is_none());

        let parent_input = json!([
            {"type":"message","role":"user","content":"base"},
            {"type":"message","role":"assistant","content":"terminal"}
        ]);
        assert_eq!(
            index
                .commit(
                    &parent,
                    &LineageParent::FullReplay,
                    candidate_with_input("resp-parent", parent_input.clone(), Some("placement-a")),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 2 }
        );

        let child_input = json!([
            {"type":"message","role":"user","content":"base"},
            {"type":"message","role":"assistant","content":"terminal"},
            {"type":"message","role":"user","content":"next"}
        ]);
        assert_eq!(
            index
                .commit(
                    &timed_out_child,
                    &LineageParent::FullReplay,
                    candidate_with_input("resp-child", child_input.clone(), Some("placement-a")),
                    true,
                )
                .await,
            LineageCommitOutcome::Rebased {
                generation: 3,
                parent_generation: 2,
                parent_response_id: "resp-parent".to_string(),
            }
        );
        let head = index.head("thread").await.unwrap();
        assert_eq!(head.response_id, "resp-child");
        assert_eq!(head.input, child_input);

        drop(terminal_owner);
    }

    #[tokio::test]
    async fn terminal_publication_timeout_does_not_rebase_a_sibling_branch_or_placement_change() {
        let index = ContinuationLineageIndex::default();
        let base = json!([{"type":"message","role":"user","content":"base"}]);
        index
            .seed_for_test(
                "thread",
                ResponseSessionState {
                    generation: 1,
                    parent_generation: None,
                    response_id: "resp-base".to_string(),
                    static_projection_digest: None,
                    output_items_complete: false,
                    input: base,
                    output_items: Vec::new(),
                    finished_at: Instant::now(),
                },
            )
            .await;
        let terminal_owner = index.register_terminal_publication("thread");
        let parent = index.begin("thread").await;
        let timed_out_child = index.begin("thread").await;
        assert!(timed_out_child.publication_timed_out());

        let parent_input = json!([
            {"type":"message","role":"user","content":"base"},
            {"type":"message","role":"assistant","content":"right"}
        ]);
        assert_eq!(
            index
                .commit(
                    &parent,
                    &LineageParent::FullReplay,
                    candidate_with_input("resp-right", parent_input, Some("placement-a")),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 2 }
        );

        let sibling_branch = json!([
            {"type":"message","role":"user","content":"base"},
            {"type":"message","role":"assistant","content":"left"}
        ]);
        assert_eq!(
            index
                .commit(
                    &timed_out_child,
                    &LineageParent::FullReplay,
                    candidate_with_input("resp-left", sibling_branch, Some("placement-a")),
                    true,
                )
                .await,
            LineageCommitOutcome::Stale {
                expected: 1,
                actual: 2,
            }
        );
        assert_eq!(
            index.head("thread").await.unwrap().response_id,
            "resp-right"
        );

        drop(terminal_owner);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_siblings_keep_the_first_completed_commit() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        index
            .commit(
                &root,
                &LineageParent::FullReplay,
                candidate("resp-root", "root"),
                true,
            )
            .await;
        let left = index.begin("thread").await;
        let right = index.begin("thread").await;
        let parent = LineageParent::FullReplay;
        let (release_left, wait_left) = tokio::sync::oneshot::channel::<()>();
        let (release_right, wait_right) = tokio::sync::oneshot::channel::<()>();

        let left_index = index.clone();
        let left_parent = parent.clone();
        let left_task = tokio::spawn(async move {
            let _ = wait_left.await;
            left_index
                .commit(&left, &left_parent, candidate("resp-left", "left"), true)
                .await
        });
        let right_index = index.clone();
        let right_task = tokio::spawn(async move {
            let _ = wait_right.await;
            right_index
                .commit(&right, &parent, candidate("resp-right", "right"), true)
                .await
        });

        release_right.send(()).unwrap();
        assert_eq!(
            right_task.await.unwrap(),
            LineageCommitOutcome::Applied { generation: 2 }
        );
        release_left.send(()).unwrap();
        assert_eq!(
            left_task.await.unwrap(),
            LineageCommitOutcome::Stale {
                expected: 1,
                actual: 2,
            }
        );
        assert_eq!(
            index.head("thread").await.unwrap().response_id,
            "resp-right"
        );
    }

    #[tokio::test]
    async fn stale_failure_cannot_delete_a_newer_head_or_revive_a_tombstone() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        index
            .commit(
                &root,
                &LineageParent::FullReplay,
                candidate("resp-root", "root"),
                true,
            )
            .await;
        let old_failure = index.begin("thread").await;
        let winner = index.begin("thread").await;
        let parent = LineageParent::FullReplay;
        index
            .commit(&winner, &parent, candidate("resp-new", "new"), true)
            .await;
        assert_eq!(
            index.invalidate(&old_failure, Some("resp-root")).await,
            LineageInvalidateOutcome::Stale {
                expected: 1,
                actual: 2
            }
        );

        let tombstone = index.begin("thread").await;
        assert_eq!(
            index.invalidate(&tombstone, Some("resp-new")).await,
            LineageInvalidateOutcome::Applied { generation: 3 }
        );
        assert!(!index.contains_head("thread").await);
        assert_eq!(
            index
                .commit(&winner, &parent, candidate("resp-revived", "old"), true)
                .await,
            LineageCommitOutcome::Stale {
                expected: 1,
                actual: 3
            }
        );
    }

    #[tokio::test]
    async fn compaction_epoch_supersedes_older_requests_but_not_newer_ones() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        index
            .commit(
                &root,
                &LineageParent::FullReplay,
                candidate("resp-root", "root"),
                true,
            )
            .await;

        let old_request = index.begin("thread").await;
        let compaction = index.begin_compaction("thread", Some("resp-root")).await;
        assert!(compaction.parent_matched());
        assert_eq!(
            index
                .commit(
                    &old_request,
                    &LineageParent::FullReplay,
                    candidate("resp-old", "old"),
                    true,
                )
                .await,
            LineageCommitOutcome::EpochChanged {
                expected: old_request.epoch(),
                actual: compaction.lease().epoch(),
            }
        );

        let newer = index.begin("thread").await;
        assert_eq!(newer.epoch(), compaction.lease().epoch());
        assert_eq!(
            index
                .commit(
                    &newer,
                    &LineageParent::FullReplay,
                    candidate("resp-new", "new"),
                    true,
                )
                .await,
            LineageCommitOutcome::Applied { generation: 3 }
        );
        assert_eq!(index.head("thread").await.unwrap().response_id, "resp-new");
    }

    #[tokio::test]
    async fn older_compaction_cannot_fence_a_newer_epoch() {
        let index = ContinuationLineageIndex::default();
        let root = index.begin("thread").await;
        index
            .commit(
                &root,
                &LineageParent::FullReplay,
                candidate("resp-root", "root"),
                true,
            )
            .await;
        let old_request = index.begin("thread").await;
        let newer_compaction = index.begin_compaction("thread", Some("resp-root")).await;
        let newer_request = index.begin("thread").await;
        index
            .commit(
                &newer_request,
                &LineageParent::FullReplay,
                candidate("resp-new", "new"),
                true,
            )
            .await;

        assert!(index.confirm_compaction(&old_request).await.is_none());
        assert_eq!(index.head("thread").await.unwrap().response_id, "resp-new");
        assert_ne!(old_request.epoch(), newer_compaction.lease().epoch());
    }

    #[tokio::test]
    async fn small_index_gc_reclaims_expired_full_replay_heads_without_waiting_for_128_slots() {
        let index = ContinuationLineageIndex::default();
        index
            .seed_for_test(
                "expired",
                ResponseSessionState {
                    generation: 1,
                    parent_generation: None,
                    response_id: "resp-expired".to_string(),
                    static_projection_digest: None,
                    output_items_complete: false,
                    input: json!([{"type":"message","role":"user","content":"expired"}]),
                    output_items: Vec::new(),
                    finished_at: Instant::now()
                        .checked_sub(LINEAGE_HEAD_TTL + Duration::from_secs(1))
                        .expect("the test clock supports a 30 minute offset"),
                },
            )
            .await;

        for _ in 0..LINEAGE_SMALL_INDEX_GC_INTERVAL {
            let _ = index.begin("active").await;
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while index.contains_head("expired").await {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a small index must schedule expiration cleanup before 128 slots exist");
    }

    #[tokio::test]
    async fn pruned_epoch_cannot_be_revived_by_an_old_lease() {
        let index = ContinuationLineageIndex::default();
        let old = index.begin("thread").await;
        index.prune_all_for_test().await;
        let current = index.begin("thread").await;
        assert_eq!(old.expected_generation(), current.expected_generation());
        assert_ne!(old.epoch(), current.epoch());
        assert_eq!(
            index
                .commit(
                    &old,
                    &LineageParent::FullReplay,
                    candidate("resp-old", "old"),
                    true,
                )
                .await,
            LineageCommitOutcome::EpochChanged {
                expected: old.epoch(),
                actual: current.epoch(),
            }
        );
    }

    #[tokio::test]
    async fn compaction_lease_changes_epoch_even_when_other_slot_has_same_generation() {
        let index = ContinuationLineageIndex::default();
        let first = index.begin_compaction("thread-a", None).await;
        let second = index.begin_compaction("thread-b", None).await;

        assert_eq!(
            first.lease().expected_generation(),
            second.lease().expected_generation()
        );
        assert_ne!(first.lease().epoch(), second.lease().epoch());
    }
}
