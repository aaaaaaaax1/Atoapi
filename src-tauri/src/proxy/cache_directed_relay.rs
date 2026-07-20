use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Clone, Default)]
pub(crate) struct DispatchTracker {
    inner: Arc<DispatchTrackerInner>,
}

#[derive(Debug)]
struct DispatchTrackerInner {
    next_id: AtomicU64,
    state: Mutex<DispatchTrackerState>,
    notify: tokio::sync::Notify,
}

#[derive(Debug)]
struct DispatchTrackerState {
    accepting: bool,
    aborting: bool,
    tasks: HashMap<u64, Option<tokio::task::AbortHandle>>,
}

impl Default for DispatchTrackerInner {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            state: Mutex::new(DispatchTrackerState {
                accepting: true,
                aborting: false,
                tasks: HashMap::new(),
            }),
            notify: tokio::sync::Notify::new(),
        }
    }
}

struct DispatchGuard {
    id: u64,
    inner: Arc<DispatchTrackerInner>,
}

/// Holds a tracker slot before the owner future is assembled. Dropping an
/// unstarted reservation releases the slot; starting it transfers the guard to
/// the detached task. This lets callers fail closed before exposing a response
/// when shutdown has stopped accepting relay owners.
pub(crate) struct DispatchReservation {
    guard: Option<DispatchGuard>,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        let idle = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("dispatch tracker lock must not be poisoned");
            state.tasks.remove(&self.id).is_some() && state.tasks.is_empty()
        };
        if idle {
            self.inner.notify.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchDrainOutcome {
    Graceful,
    Aborted { task_count: usize },
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("relay task tracker is closed")]
pub(crate) struct DispatchTrackerClosed;

impl DispatchTracker {
    pub(crate) fn reserve(&self) -> Result<DispatchReservation, DispatchTrackerClosed> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("dispatch tracker lock must not be poisoned");
        if !state.accepting {
            return Err(DispatchTrackerClosed);
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        state.tasks.insert(id, None);
        Ok(DispatchReservation {
            guard: Some(DispatchGuard {
                id,
                inner: self.inner.clone(),
            }),
        })
    }

    fn register_abort_handle(&self, id: u64, handle: tokio::task::AbortHandle) {
        let should_abort = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("dispatch tracker lock must not be poisoned");
            let Some(slot) = state.tasks.get_mut(&id) else {
                return handle.abort();
            };
            *slot = Some(handle.clone());
            state.aborting
        };
        if should_abort {
            handle.abort();
        }
    }

    fn is_idle(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("dispatch tracker lock must not be poisoned")
            .tasks
            .is_empty()
    }

    async fn wait_until_idle(&self) {
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_for_idle(&self, timeout: std::time::Duration) -> bool {
        tokio::time::timeout(timeout, self.wait_until_idle())
            .await
            .is_ok()
    }

    pub(crate) async fn close_and_drain(
        &self,
        timeout: std::time::Duration,
    ) -> DispatchDrainOutcome {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("dispatch tracker lock must not be poisoned");
            state.accepting = false;
        }
        if self.wait_for_idle(timeout).await {
            return DispatchDrainOutcome::Graceful;
        }

        let (task_count, abort_handles) = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("dispatch tracker lock must not be poisoned");
            state.aborting = true;
            (
                state.tasks.len(),
                state
                    .tasks
                    .values()
                    .filter_map(Clone::clone)
                    .collect::<Vec<_>>(),
            )
        };
        for handle in abort_handles {
            handle.abort();
        }
        self.wait_until_idle().await;
        DispatchDrainOutcome::Aborted { task_count }
    }

    pub(crate) fn spawn<F>(
        &self,
        future: F,
    ) -> Result<tokio::task::JoinHandle<F::Output>, DispatchTrackerClosed>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Ok(self.reserve()?.spawn(future))
    }
}

impl DispatchReservation {
    pub(crate) fn spawn<F>(mut self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let guard = self
            .guard
            .take()
            .expect("a dispatch reservation can only start one owner task");
        let id = guard.id;
        let tracker = DispatchTracker {
            inner: guard.inner.clone(),
        };
        let (start_tx, start_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _guard = guard;
            let _ = start_rx.await;
            future.await
        });
        tracker.register_abort_handle(id, handle.abort_handle());
        let _ = start_tx.send(());
        handle
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum DispatchOwnerError {
    #[error("agent generation owner stopped before producing a response")]
    Stopped,
    #[error("agent generation owner was rejected during shutdown")]
    Closed,
}

pub(super) struct DispatchHandoff<T> {
    sender: Arc<Mutex<Option<oneshot::Sender<T>>>>,
}

impl<T> Clone for DispatchHandoff<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> DispatchHandoff<T> {
    pub(super) fn send(&self, value: T) -> Result<(), T> {
        let sender = self
            .sender
            .lock()
            .expect("dispatch handoff mutex must not be poisoned")
            .take();
        match sender {
            Some(sender) => sender.send(value),
            None => Err(value),
        }
    }
}

/// Starts request ownership in a detached task before the caller waits for the
/// response head. Dropping the caller only drops the receiver; the work keeps
/// running and owns any result it produces.
#[cfg(test)]
pub(super) async fn dispatch_owned<T, F, Fut>(work: F) -> Result<T, DispatchOwnerError>
where
    T: Send + 'static,
    F: FnOnce(DispatchHandoff<T>) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    dispatch_owned_inner(None, work).await
}

pub(super) async fn dispatch_owned_tracked<T, F, Fut>(
    tracker: DispatchTracker,
    work: F,
) -> Result<T, DispatchOwnerError>
where
    T: Send + 'static,
    F: FnOnce(DispatchHandoff<T>) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    dispatch_owned_inner(Some(tracker), work).await
}

async fn dispatch_owned_inner<T, F, Fut>(
    tracker: Option<DispatchTracker>,
    work: F,
) -> Result<T, DispatchOwnerError>
where
    T: Send + 'static,
    F: FnOnce(DispatchHandoff<T>) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let (response_tx, response_rx) = oneshot::channel();
    let handoff = DispatchHandoff {
        sender: Arc::new(Mutex::new(Some(response_tx))),
    };
    let owner = async move {
        let response = work(handoff.clone()).await;
        let _ = handoff.send(response);
    };
    if let Some(tracker) = tracker {
        tracker
            .spawn(owner)
            .map_err(|_| DispatchOwnerError::Closed)?;
    } else {
        tokio::spawn(owner);
    }
    response_rx.await.map_err(|_| DispatchOwnerError::Stopped)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use tokio::{sync::oneshot, time::timeout};

    use super::*;

    #[tokio::test]
    async fn dispatch_owner_returns_completed_value() {
        assert_eq!(dispatch_owned(|_| async { 42 }).await, Ok(42));
    }

    #[tokio::test]
    async fn dispatch_owner_survives_caller_cancellation_before_result() {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_owner = completed.clone();

        let caller = tokio::spawn(dispatch_owned(move |_| async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            completed_for_owner.store(true, Ordering::SeqCst);
            let _ = completed_tx.send(());
            7
        }));

        started_rx.await.unwrap();
        caller.abort();
        let _ = caller.await;
        release_tx.send(()).unwrap();

        timeout(std::time::Duration::from_secs(1), completed_rx)
            .await
            .expect("owner should finish after its caller is cancelled")
            .unwrap();
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dispatch_owner_drops_unclaimed_result_after_caller_cancellation() {
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_for_owner = dropped.clone();

        let caller = tokio::spawn(dispatch_owned(move |_| async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            DropProbe(dropped_for_owner)
        }));

        started_rx.await.unwrap();
        caller.abort();
        let _ = caller.await;
        release_tx.send(()).unwrap();

        timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("an unclaimed response should be dropped by the owner task");
    }

    #[tokio::test]
    async fn dispatch_owner_can_handoff_before_finishing_settlement() {
        let (release_tx, release_rx) = oneshot::channel();
        let (settled_tx, settled_rx) = oneshot::channel();

        let caller = tokio::spawn(dispatch_owned(move |handoff| async move {
            handoff.send(7).unwrap();
            let _ = release_rx.await;
            let _ = settled_tx.send(());
            8
        }));

        assert_eq!(caller.await.unwrap(), Ok(7));
        release_tx.send(()).unwrap();
        timeout(std::time::Duration::from_secs(1), settled_rx)
            .await
            .expect("owner should continue after the early handoff")
            .unwrap();
    }

    #[tokio::test]
    async fn tracked_dispatch_stays_active_through_post_handoff_settlement() {
        let tracker = DispatchTracker::default();
        let (release_tx, release_rx) = oneshot::channel();
        let caller = tokio::spawn(dispatch_owned_tracked(
            tracker.clone(),
            move |handoff| async move {
                handoff.send(7).unwrap();
                let _ = release_rx.await;
                8
            },
        ));

        assert_eq!(caller.await.unwrap(), Ok(7));
        assert!(
            !tracker
                .wait_for_idle(std::time::Duration::from_millis(20))
                .await
        );
        release_tx.send(()).unwrap();
        assert!(
            tracker
                .wait_for_idle(std::time::Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn close_rejects_new_tasks_after_existing_tasks_settle() {
        let tracker = DispatchTracker::default();
        let handle = tracker.spawn(async { 7 }).unwrap();
        assert_eq!(handle.await.unwrap(), 7);

        assert_eq!(
            tracker
                .close_and_drain(std::time::Duration::from_secs(1))
                .await,
            DispatchDrainOutcome::Graceful
        );
        assert!(tracker.spawn(async {}).is_err());
    }

    #[tokio::test]
    async fn drain_timeout_aborts_registered_tasks_before_returning() {
        let tracker = DispatchTracker::default();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_task = completed.clone();
        let handle = tracker
            .spawn(async move {
                std::future::pending::<()>().await;
                completed_for_task.store(true, Ordering::Release);
            })
            .unwrap();

        assert_eq!(
            tracker
                .close_and_drain(std::time::Duration::from_millis(10))
                .await,
            DispatchDrainOutcome::Aborted { task_count: 1 }
        );
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(!completed.load(Ordering::Acquire));
        assert!(
            tracker
                .wait_for_idle(std::time::Duration::from_millis(10))
                .await
        );
    }
}
