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
const LINEAGE_GC_INTERVAL: u64 = 256;
const LINEAGE_GC_MIN_SLOTS: usize = 128;
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
    pub input: Value,
    pub output_items: Vec<Value>,
    pub finished_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ResponseSessionCandidate {
    pub response_id: String,
    pub input: Value,
    pub output_items: Vec<Value>,
    pub finished_at: Instant,
}

#[derive(Debug, Clone)]
pub struct LineageLease {
    key: String,
    epoch: u64,
    expected_generation: u64,
    head: Option<Arc<ResponseSessionState>>,
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

    pub fn head(&self) -> Option<&Arc<ResponseSessionState>> {
        self.head.as_ref()
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
    Managed {
        generation: u64,
        response_id: String,
    },
    ExternalContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageCommitOutcome {
    Applied { generation: u64 },
    Tombstoned { generation: u64 },
    Stale { expected: u64, actual: u64 },
    EpochChanged { expected: u64, actual: u64 },
    ParentMismatch,
    Regressive,
    ExternalContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageInvalidateOutcome {
    Applied { generation: u64 },
    Stale { expected: u64, actual: u64 },
    EpochChanged { expected: u64, actual: u64 },
    ParentMismatch,
}

#[derive(Debug, Clone)]
pub struct ContinuationLineageIndex {
    slots: Arc<Mutex<HashMap<String, LineageSlot>>>,
    terminal_publications: Arc<TerminalPublications>,
    next_epoch: Arc<AtomicU64>,
    operations: Arc<AtomicU64>,
    gc_running: Arc<AtomicBool>,
}

impl Default for ContinuationLineageIndex {
    fn default() -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
            terminal_publications: Arc::new(TerminalPublications::default()),
            next_epoch: Arc::new(AtomicU64::new(1)),
            operations: Arc::new(AtomicU64::new(0)),
            gc_running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LineageSlot {
    epoch: u64,
    generation: u64,
    head: Option<Arc<ResponseSessionState>>,
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
        }
        let slot = slots
            .get(key)
            .expect("a lineage slot inserted above must be present");
        let lease = LineageLease {
            key: key.to_string(),
            epoch: slot.epoch,
            expected_generation: slot.generation,
            // A timed-out publication may still expose the previous head.
            // Force a safe FullReplay lease instead of attempting delta from
            // stale lineage; the generation/epoch CAS still prevents a late
            // sibling from overwriting newer state.
            head: publication_completed.then(|| slot.head.clone()).flatten(),
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
            head: None,
        };
        slots.insert(key.to_string(), slot);
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
            head: None,
        };
        slots.insert(lease.key.clone(), slot);
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
        let slot_count = slots.len();
        drop(slots);
        if matches!(outcome, LineageCommitOutcome::Applied { .. }) {
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
                let slot_count = slots.len();
                drop(slots);
                if matches!(outcome, LineageCommitOutcome::Applied { .. }) {
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
        let outcome = apply_invalidation(&mut slots, lease, expected_response_id);
        let slot_count = slots.len();
        drop(slots);
        if matches!(outcome, LineageInvalidateOutcome::Applied { .. }) {
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
                let outcome = apply_invalidation(&mut slots, lease, expected_response_id);
                let slot_count = slots.len();
                drop(slots);
                if matches!(outcome, LineageInvalidateOutcome::Applied { .. }) {
                    self.maybe_schedule_gc(slot_count);
                }
                outcome
            }
            Err(_) => self.invalidate(lease, expected_response_id).await,
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
                updated_at: Instant::now(),
            },
        );
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
            updated_at: Instant::now(),
        }
    }

    fn maybe_schedule_gc(&self, slot_count: usize) {
        let operation = self.operations.fetch_add(1, Ordering::Relaxed) + 1;
        if slot_count < LINEAGE_GC_MIN_SLOTS
            || operation % LINEAGE_GC_INTERVAL != 0
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
                .unwrap_or_else(|| slot.updated_at.elapsed() <= tombstone_ttl)
        });
    }

    #[cfg(test)]
    async fn prune_all_for_test(&self) {
        self.prune_expired(Duration::ZERO, Duration::ZERO).await;
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
    let current_generation = slot.generation;
    if current_generation != lease.expected_generation {
        return LineageCommitOutcome::Stale {
            expected: lease.expected_generation,
            actual: current_generation,
        };
    }
    if !replacement_allowed {
        return LineageCommitOutcome::Regressive;
    }
    if let LineageParent::Managed {
        generation,
        response_id,
    } = parent
    {
        let current = slot.head.as_ref();
        if *generation != lease.expected_generation
            || lease.head().is_none_or(|head| {
                head.generation != *generation || head.response_id != *response_id
            })
            || current.is_none_or(|head| {
                head.generation != *generation || head.response_id != *response_id
            })
        {
            return LineageCommitOutcome::ParentMismatch;
        }
    }
    let generation = current_generation
        .checked_add(1)
        .expect("continuation lineage generation overflow");
    let parent_generation = match parent {
        LineageParent::FullReplay => None,
        LineageParent::Managed { generation, .. } => Some(*generation),
        LineageParent::ExternalContinuation => return LineageCommitOutcome::ExternalContinuation,
    };
    let head = Arc::new(ResponseSessionState {
        generation,
        parent_generation,
        response_id: candidate.response_id,
        input: candidate.input,
        output_items: candidate.output_items,
        finished_at: candidate.finished_at,
    });
    slot.generation = generation;
    slot.head = Some(head);
    slot.updated_at = Instant::now();
    LineageCommitOutcome::Applied { generation }
}

fn apply_invalidation(
    slots: &mut HashMap<String, LineageSlot>,
    lease: &LineageLease,
    expected_response_id: Option<&str>,
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
    slot.generation = generation;
    slot.head = None;
    slot.updated_at = Instant::now();
    LineageInvalidateOutcome::Applied { generation }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn candidate(response_id: &str, input: &str) -> ResponseSessionCandidate {
        ResponseSessionCandidate {
            response_id: response_id.to_string(),
            input: json!([{"type":"message","role":"user","content":input}]),
            output_items: Vec::new(),
            finished_at: Instant::now(),
        }
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
        let parent = LineageParent::Managed {
            generation: 1,
            response_id: "resp-root".to_string(),
        };
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
        assert_eq!(head.parent_generation, Some(1));
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
        let parent = LineageParent::Managed {
            generation: 1,
            response_id: "resp-root".to_string(),
        };
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
        let parent = LineageParent::Managed {
            generation: 1,
            response_id: "resp-root".to_string(),
        };
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
                    &LineageParent::Managed {
                        generation: 1,
                        response_id: "resp-root".to_string(),
                    },
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
