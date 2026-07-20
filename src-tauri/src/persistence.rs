use crate::metrics::MetricsStore;
use anyhow::{anyhow, Result};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};
use tokio::sync::Notify;

const OPERATION_BITS: u32 = 2;
const OPERATION_MASK: u64 = (1 << OPERATION_BITS) - 1;
const WRITE_COALESCE_MS: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOperation {
    Snapshot = 1,
    Delete = 2,
}

impl WriteOperation {
    fn from_packed(value: u64) -> Self {
        match value & OPERATION_MASK {
            2 => Self::Delete,
            _ => Self::Snapshot,
        }
    }
}

type WriteJob = dyn Fn(WriteOperation) -> Result<()> + Send + Sync + 'static;

#[derive(Clone)]
pub(crate) struct WriteBehindCoordinator {
    inner: Arc<WriteBehindInner>,
}

struct WriteBehindInner {
    scope: &'static str,
    requested: AtomicU64,
    settled_version: AtomicU64,
    persisted_version: AtomicU64,
    running: AtomicBool,
    notify: Notify,
    last_failure: Mutex<Option<(u64, String)>>,
    error_reporter: RwLock<Option<MetricsStore>>,
    write_job: Arc<WriteJob>,
}

impl std::fmt::Debug for WriteBehindCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriteBehindCoordinator")
            .field("scope", &self.inner.scope)
            .field("requested_version", &requested_version(&self.inner))
            .field(
                "settled_version",
                &self.inner.settled_version.load(Ordering::Acquire),
            )
            .field(
                "persisted_version",
                &self.inner.persisted_version.load(Ordering::Acquire),
            )
            .field("running", &self.inner.running.load(Ordering::Acquire))
            .finish()
    }
}

impl WriteBehindCoordinator {
    pub(crate) fn new(
        scope: &'static str,
        write_job: impl Fn(WriteOperation) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(WriteBehindInner {
                scope,
                requested: AtomicU64::new(0),
                settled_version: AtomicU64::new(0),
                persisted_version: AtomicU64::new(0),
                running: AtomicBool::new(false),
                notify: Notify::new(),
                last_failure: Mutex::new(None),
                error_reporter: RwLock::new(None),
                write_job: Arc::new(write_job),
            }),
        }
    }

    pub(crate) fn attach_error_reporter(&self, metrics: MetricsStore) {
        *self
            .inner
            .error_reporter
            .write()
            .expect("persistence error reporter lock must not be poisoned") = Some(metrics);
    }

    pub(crate) fn mark_dirty(&self, operation: WriteOperation) -> u64 {
        let packed = self
            .inner
            .requested
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let version = unpack_version(current).saturating_add(1);
                Some(pack(version, operation))
            })
            .expect("persistence generation update must not fail");
        let version = unpack_version(packed).saturating_add(1);
        ensure_worker(&self.inner);
        version
    }

    pub(crate) async fn write_now(&self, operation: WriteOperation) -> Result<()> {
        let version = self.mark_dirty(operation);
        self.wait_for(version).await
    }

    pub(crate) async fn flush_latest(&self) -> Result<()> {
        let version = requested_version(&self.inner);
        if version == 0 {
            return Ok(());
        }
        ensure_worker(&self.inner);
        self.wait_for(version).await
    }

    pub(crate) async fn retry_latest(&self) -> Result<()> {
        let packed = self.inner.requested.load(Ordering::Acquire);
        if unpack_version(packed) == 0 {
            return Ok(());
        }
        self.write_now(WriteOperation::from_packed(packed)).await
    }

    pub(crate) async fn wait_for(&self, version: u64) -> Result<()> {
        loop {
            let notified = self.inner.notify.notified();
            if self.inner.settled_version.load(Ordering::Acquire) >= version {
                break;
            }
            ensure_worker(&self.inner);
            notified.await;
        }
        if self.inner.persisted_version.load(Ordering::Acquire) >= version {
            return Ok(());
        }
        let message = self
            .inner
            .last_failure
            .lock()
            .expect("persistence failure lock must not be poisoned")
            .as_ref()
            .filter(|(failed_version, _)| *failed_version >= version)
            .map(|(_, message)| message.clone())
            .unwrap_or_else(|| {
                "persistence writer stopped before committing the requested version".to_string()
            });
        Err(anyhow!(message))
    }
}

fn pack(version: u64, operation: WriteOperation) -> u64 {
    (version << OPERATION_BITS) | operation as u64
}

fn unpack_version(value: u64) -> u64 {
    value >> OPERATION_BITS
}

fn requested_version(inner: &WriteBehindInner) -> u64 {
    unpack_version(inner.requested.load(Ordering::Acquire))
}

fn ensure_worker(inner: &Arc<WriteBehindInner>) {
    if requested_version(inner) <= inner.settled_version.load(Ordering::Acquire) {
        return;
    }
    if inner
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        inner.running.store(false, Ordering::Release);
        return;
    };
    let inner = inner.clone();
    runtime.spawn(async move { run_worker(inner).await });
}

async fn run_worker(inner: Arc<WriteBehindInner>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(WRITE_COALESCE_MS)).await;
        let packed = inner.requested.load(Ordering::Acquire);
        let version = unpack_version(packed);
        if version <= inner.settled_version.load(Ordering::Acquire) {
            break;
        }
        let operation = WriteOperation::from_packed(packed);
        let write_job = inner.write_job.clone();
        let result = tokio::task::spawn_blocking(move || write_job(operation)).await;
        let result = match result {
            Ok(result) => result,
            Err(err) => Err(anyhow!("persistence blocking writer failed: {err}")),
        };
        match result {
            Ok(()) => {
                inner.persisted_version.store(version, Ordering::Release);
                *inner
                    .last_failure
                    .lock()
                    .expect("persistence failure lock must not be poisoned") = None;
            }
            Err(err) => {
                let message = err.to_string();
                *inner
                    .last_failure
                    .lock()
                    .expect("persistence failure lock must not be poisoned") =
                    Some((version, message.clone()));
                let reporter = inner
                    .error_reporter
                    .read()
                    .expect("persistence error reporter lock must not be poisoned")
                    .clone();
                if let Some(metrics) = reporter {
                    metrics.record_error(inner.scope, &message).await;
                }
            }
        }
        inner.settled_version.store(version, Ordering::Release);
        inner.notify.notify_waiters();
        if requested_version(&inner) <= version {
            break;
        }
    }

    inner.running.store(false, Ordering::Release);
    ensure_worker(&inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn dirty_burst_coalesces_to_one_latest_write() {
        let writes = Arc::new(AtomicUsize::new(0));
        let writes_for_job = writes.clone();
        let coordinator = WriteBehindCoordinator::new("test", move |_| {
            writes_for_job.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        for _ in 0..64 {
            coordinator.mark_dirty(WriteOperation::Snapshot);
        }
        coordinator.flush_latest().await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn slow_writer_does_not_block_dirty_signal() {
        let coordinator = WriteBehindCoordinator::new("test", move |_| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            Ok(())
        });

        let started = std::time::Instant::now();
        coordinator.mark_dirty(WriteOperation::Snapshot);
        assert!(started.elapsed() < std::time::Duration::from_millis(20));
        coordinator.flush_latest().await.unwrap();
    }

    #[tokio::test]
    async fn failed_writer_can_retry_the_latest_operation() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_job = attempts.clone();
        let coordinator = WriteBehindCoordinator::new("test", move |_| {
            if attempts_for_job.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(anyhow!("first write fails"))
            } else {
                Ok(())
            }
        });

        assert!(coordinator
            .write_now(WriteOperation::Snapshot)
            .await
            .is_err());
        coordinator.retry_latest().await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn newer_operation_runs_after_an_inflight_older_version() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let operations = Arc::new(Mutex::new(Vec::new()));
        let attempts_for_job = attempts.clone();
        let started_for_job = started.clone();
        let release_for_job = release.clone();
        let operations_for_job = operations.clone();
        let coordinator = WriteBehindCoordinator::new("test", move |operation| {
            let attempt = attempts_for_job.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                started_for_job.store(true, Ordering::Release);
                while !release_for_job.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
            operations_for_job
                .lock()
                .expect("operation log must not be poisoned")
                .push(operation);
            Ok(())
        });

        coordinator.mark_dirty(WriteOperation::Snapshot);
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        coordinator.mark_dirty(WriteOperation::Delete);
        release.store(true, Ordering::Release);
        coordinator.flush_latest().await.unwrap();

        assert_eq!(
            *operations
                .lock()
                .expect("operation log must not be poisoned"),
            vec![WriteOperation::Snapshot, WriteOperation::Delete]
        );
    }
}
