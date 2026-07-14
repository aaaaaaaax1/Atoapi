use std::future::Future;

use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum DispatchOwnerError {
    #[error("agent generation owner stopped before producing a response")]
    Stopped,
}

/// Starts request ownership in a detached task before the caller waits for the
/// response head. Dropping the caller only drops the receiver; the work keeps
/// running and owns any result it produces.
pub(super) async fn dispatch_owned<T, F>(work: F) -> Result<T, DispatchOwnerError>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let (response_tx, response_rx) = oneshot::channel();
    tokio::spawn(async move {
        let response = work.await;
        let _ = response_tx.send(response);
    });
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
        assert_eq!(dispatch_owned(async { 42 }).await, Ok(42));
    }

    #[tokio::test]
    async fn dispatch_owner_survives_caller_cancellation_before_result() {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_owner = completed.clone();

        let caller = tokio::spawn(dispatch_owned(async move {
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

        let caller = tokio::spawn(dispatch_owned(async move {
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
}
