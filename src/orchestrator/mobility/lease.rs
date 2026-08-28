//! Keeping a lease alive, and telling the holder the instant it is not.
//!
//! Renewing a lease in a loop is the easy half. The half that matters is what
//! happens when renewal stops working: the holder has to find out and stop,
//! and it has to stop *before* the other side concludes the lease has lapsed.
//! A holder that keeps working past its own lease is precisely the second live
//! copy the lease existed to prevent.
//!
//! # Yielding early
//!
//! The guardian does not wait for the lease to expire. It abandons at
//! `ttl - margin` since the last successful renewal, so the holder has given
//! up while the other side still considers the lease live. The margin is the
//! holder's share of the same clock disagreement the other side absorbs with
//! its grace period: both sides err toward there being no owner rather than
//! two.
//!
//! # Losing versus failing
//!
//! A renewal that comes back saying someone else owns the lease is final —
//! there is nothing to retry, and continuing is actively harmful. A renewal
//! that fails to complete is not: the store may be briefly unreachable and the
//! lease may still be ours. Those are retried until the abandon deadline, at
//! which point the outcome is the same, because a lease we cannot prove we
//! hold is one we have to act as though we have lost.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tokio::sync::watch;
use tracing::{debug, warn};

/// The result of one renewal attempt.
#[derive(Debug)]
pub enum RenewOutcome {
    /// The lease is still ours.
    Held,
    /// Someone else has it. Final.
    Lost(LeaseLost),
    /// The attempt did not complete. Retryable until the abandon deadline.
    Failed(anyhow::Error),
}

/// Why a lease is no longer held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseLost {
    /// Another holder owns it now.
    Taken { by: String },
    /// The thing the lease was over no longer exists.
    Gone,
    /// Renewal could not be completed for long enough that the lease must be
    /// assumed lapsed.
    ///
    /// The holder cannot distinguish this from still holding it. It has to
    /// yield anyway: the other side is about to stop waiting.
    Unprovable { last_error: String },
}

impl LeaseLost {
    /// Stable label for metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Taken { .. } => "taken",
            Self::Gone => "gone",
            Self::Unprovable { .. } => "unprovable",
        }
    }
}

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Taken { by } => write!(f, "the lease was taken by {by}"),
            Self::Gone => write!(f, "the leased resource no longer exists"),
            Self::Unprovable { last_error } => {
                write!(f, "the lease could not be renewed: {last_error}")
            }
        }
    }
}

/// A holder's view of whether it still owns the lease.
///
/// Cloneable so several parts of one operation can watch the same lease.
#[derive(Clone, Debug)]
pub struct LeaseWatch {
    rx: watch::Receiver<Option<LeaseLost>>,
}

impl LeaseWatch {
    /// Whether the lease has already been lost, without waiting.
    pub fn lost_now(&self) -> Option<LeaseLost> {
        self.rx.borrow().clone()
    }

    /// Resolves when the lease is lost.
    ///
    /// Meant for the losing arm of a `select!` against the work the lease
    /// protects, so that work is dropped rather than allowed to finish.
    pub async fn lost(&mut self) -> LeaseLost {
        loop {
            if let Some(lost) = self.rx.borrow_and_update().clone() {
                return lost;
            }
            if self.rx.changed().await.is_err() {
                // The guardian is gone without having reported a loss. It can
                // no longer renew, so the lease cannot be relied on.
                return LeaseLost::Unprovable {
                    last_error: "the lease guardian stopped".to_string(),
                };
            }
        }
    }
}

/// Renews a lease in the background until it is released or lost.
pub struct LeaseGuardian {
    tx: Arc<watch::Sender<Option<LeaseLost>>>,
    task: tokio::task::JoinHandle<()>,
}

/// How a guardian paces itself.
#[derive(Clone, Copy, Debug)]
pub struct LeasePacing {
    /// How long the lease stands without renewal, as the other side sees it.
    pub ttl: Duration,
    /// How much of the TTL the holder gives back.
    ///
    /// Covers clock disagreement plus the time between deciding to stop and
    /// actually having stopped.
    pub abandon_margin: Duration,
}

impl LeasePacing {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            // A third: the same fraction as the renewal interval, so a holder
            // that misses two consecutive renewals yields rather than racing
            // the other side's expiry.
            abandon_margin: ttl / 3,
        }
    }

    fn renew_interval(&self) -> Duration {
        // Three attempts inside one TTL, so two can be lost without the lease
        // lapsing on a healthy holder.
        (self.ttl / 3).max(Duration::from_millis(10))
    }

    fn abandon_after(&self) -> Duration {
        self.ttl.saturating_sub(self.abandon_margin)
    }
}

impl LeaseGuardian {
    /// Starts renewing, returning the guardian and a watch for the holder.
    ///
    /// `renew` is called on a fixed cadence and must be cheap enough to finish
    /// well inside one interval; a renewal that outlives its own deadline is
    /// indistinguishable from one that failed.
    pub fn spawn<F>(pacing: LeasePacing, renew: F) -> (Self, LeaseWatch)
    where
        F: Fn() -> BoxFuture<'static, RenewOutcome> + Send + Sync + 'static,
    {
        let (tx, rx) = watch::channel(None);
        let tx = Arc::new(tx);
        let watch_tx = Arc::clone(&tx);
        let task = tokio::spawn(async move {
            let mut last_success = Instant::now();
            let mut last_error = String::new();
            loop {
                tokio::time::sleep(pacing.renew_interval()).await;
                if watch_tx.borrow().is_some() {
                    return;
                }

                // Bounded, because the deadline below is only evaluated once
                // `renew()` returns. A renewal that hangs — a wedged store, a
                // TCP connection that never resets — would otherwise mean the
                // holder is never told it lost the lease, and keeps working
                // long past the point another node may have taken over. The
                // timeout is the renewal interval: a renewal still outstanding
                // when the next one is due has already failed in every sense
                // that matters.
                let attempt = tokio::time::timeout(pacing.renew_interval(), renew()).await;
                let outcome = match attempt {
                    Ok(outcome) => outcome,
                    Err(_) => RenewOutcome::Failed(anyhow::anyhow!(
                        "lease renewal did not answer within {:?}",
                        pacing.renew_interval()
                    )),
                };

                match outcome {
                    RenewOutcome::Held => {
                        last_success = Instant::now();
                        last_error.clear();
                        debug!("renewed lease");
                    }
                    RenewOutcome::Lost(lost) => {
                        warn!(%lost, "lease lost");
                        let _ = watch_tx.send(Some(lost));
                        return;
                    }
                    RenewOutcome::Failed(error) => {
                        last_error = error.to_string();
                        warn!(error = %error, "lease renewal failed");
                    }
                }

                if last_success.elapsed() >= pacing.abandon_after() {
                    let lost = LeaseLost::Unprovable {
                        last_error: if last_error.is_empty() {
                            "no successful renewal within the lease window".to_string()
                        } else {
                            std::mem::take(&mut last_error)
                        },
                    };
                    warn!(%lost, "abandoning lease before it lapses");
                    let _ = watch_tx.send(Some(lost));
                    return;
                }
            }
        });

        (Self { tx, task }, LeaseWatch { rx })
    }

    /// Stops renewing because the holder is finished with the lease.
    ///
    /// Distinct from losing it: no loss is published, because the work the
    /// lease protected completed under it.
    pub fn release(self) {
        drop(self);
    }

    /// Stops renewing and tells watchers the lease is gone.
    ///
    /// For a holder that is giving up: watchers should stop too.
    pub fn surrender(self, lost: LeaseLost) {
        let _ = self.tx.send(Some(lost));
        drop(self);
    }
}

impl Drop for LeaseGuardian {
    fn drop(&mut self) {
        // Aborting rather than detaching. A detached renewal task would keep
        // asserting ownership on behalf of a holder that no longer exists, and
        // would hold the watch channel open so watchers never learn that
        // nothing is renewing any more.
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pacing() -> LeasePacing {
        LeasePacing::new(Duration::from_millis(300))
    }

    #[tokio::test]
    async fn a_healthy_lease_keeps_renewing_and_never_reports_a_loss() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&renewals);
        let (guardian, watch) = LeaseGuardian::spawn(pacing(), move || {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                RenewOutcome::Held
            })
        });

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            renewals.load(Ordering::SeqCst) >= 2,
            "should renew several times inside one TTL"
        );
        assert_eq!(watch.lost_now(), None);
        guardian.release();
    }

    /// A renewal that says someone else owns the lease is final. Retrying it
    /// would keep the holder working while the new owner starts.
    #[tokio::test]
    async fn a_taken_lease_is_reported_immediately_and_renewal_stops() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&renewals);
        let (_guardian, mut watch) = LeaseGuardian::spawn(pacing(), move || {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                RenewOutcome::Lost(LeaseLost::Taken {
                    by: "node-c".to_string(),
                })
            })
        });

        assert_eq!(
            watch.lost().await,
            LeaseLost::Taken {
                by: "node-c".to_string()
            }
        );
        let after_loss = renewals.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            renewals.load(Ordering::SeqCst),
            after_loss,
            "renewal must stop once the lease is known lost"
        );
    }

    /// A transient failure must not surrender a lease that is probably still
    /// ours; the store being briefly unreachable is ordinary.
    #[tokio::test]
    async fn a_single_failed_renewal_does_not_surrender_the_lease() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let (guardian, watch) = LeaseGuardian::spawn(pacing(), move || {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    RenewOutcome::Failed(anyhow::anyhow!("connection reset"))
                } else {
                    RenewOutcome::Held
                }
            })
        });

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(
            watch.lost_now(),
            None,
            "one failure inside the window is not a lost lease"
        );
        guardian.release();
    }

    /// The point of the whole module: a holder that cannot prove it still owns
    /// the lease yields before the other side stops waiting, not after.
    #[tokio::test]
    async fn persistent_failure_abandons_the_lease_before_it_lapses() {
        let pacing = pacing();
        let (_guardian, mut watch) = LeaseGuardian::spawn(pacing, || {
            Box::pin(async { RenewOutcome::Failed(anyhow::anyhow!("store unreachable")) })
        });

        let started = Instant::now();
        let lost = watch.lost().await;
        let elapsed = started.elapsed();

        assert_eq!(lost.kind(), "unprovable");
        assert!(
            lost.to_string().contains("store unreachable"),
            "the reason must survive: {lost}"
        );
        assert!(
            elapsed < pacing.ttl,
            "abandoned after {elapsed:?}, which is not before the {:?} TTL",
            pacing.ttl
        );
    }

    /// A guardian that stops without reporting anything cannot be read as
    /// "still held": nothing is renewing the lease any more.
    #[tokio::test]
    async fn a_dropped_guardian_is_treated_as_a_lost_lease() {
        let (guardian, mut watch) =
            LeaseGuardian::spawn(pacing(), || Box::pin(async { RenewOutcome::Held }));
        drop(guardian);

        assert_eq!(watch.lost().await.kind(), "unprovable");
    }

    /// Finishing the work is not losing the lease; watchers must not be told
    /// the operation failed when it succeeded.
    #[tokio::test]
    async fn releasing_after_the_work_completes_reports_no_loss() {
        let (guardian, watch) =
            LeaseGuardian::spawn(pacing(), || Box::pin(async { RenewOutcome::Held }));
        guardian.release();
        assert_eq!(watch.lost_now(), None);
    }

    #[tokio::test]
    async fn surrender_tells_watchers_to_stop() {
        let (guardian, mut watch) =
            LeaseGuardian::spawn(pacing(), || Box::pin(async { RenewOutcome::Held }));
        guardian.surrender(LeaseLost::Gone);
        assert_eq!(watch.lost().await, LeaseLost::Gone);
    }
}
