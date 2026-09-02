//! Driving one sandbox from one node to another.
//!
//! The pieces around this module each answer one question — can it move, who
//! owns it while it moves, what does the destination need. The saga is the
//! order they run in, and more importantly what happens when a step fails:
//! every failure has to leave exactly one node owning the sandbox, and it has
//! to be a node that can actually run it.
//!
//! # Order
//!
//! 1. Check the destination can run this guest at all. A refusal here costs
//!    nothing and rules the destination out before anything is disturbed.
//! 2. Claim, so the origin stops considering a local resume.
//! 3. Restore on the destination, renewing the claim throughout.
//! 4. Complete the handover, which is the point of no return.
//! 5. Release the origin's copy of the paused state.
//!
//! # Why the restore precedes the commit
//!
//! Marking the sandbox evacuated before it is actually running would lose it
//! outright if the restore then failed: the record says "gone to node-b" and
//! node-b has nothing. Restoring first means a failure before step 4 leaves
//! the sandbox exactly where it started, with a claim that will be released or
//! will lapse.
//!
//! The reverse exposure is a window between "the guest is live on the
//! destination" and "the record says so". A crash there loses the restored
//! guest but keeps the origin's copy, which is the direction to fail in.
//!
//! # The commit's own failures
//!
//! That direction only holds while the record agrees. A commit whose reply is
//! lost may have applied, and a rollback on that reading discards the guest
//! while leaving a tombstone naming the node that discarded it — the origin
//! keeps its copy and is fenced off from it, which is not a direction to fail
//! in but a sandbox no node will run. So the saga distinguishes a write that
//! never went out from one whose answer went missing, and re-reads the record
//! rather than guessing. What it cannot settle it reports as unfinished,
//! keeping the guest: that is the only state consistent with both answers.
//!
//! # Compensation
//!
//! Rollback tears down the destination's partial restore before releasing the
//! claim, never the other way round. Releasing first would let a second
//! destination begin while the first is still holding devices open.
//!
//! # Giving up
//!
//! A caller that wants the migration to stop asks through a
//! [`MoveCancel`](super::evacuation::MoveCancel) rather than by dropping the
//! future. Every compensation above is a step this saga has to *run*, and a
//! dropped future runs none of them; the request is honoured at the two points
//! where there is still something to unwind, and ignored past the point of no
//! return, where abandoning is what creates the ambiguity.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tracing::{info, warn};

use super::claim::{
    ClaimOutcome, CommitFailure, CommitStanding, MobilityCoordinator, ReleaseOutcome,
    DEFAULT_CLAIM_TTL,
};
use super::evacuation::MoveCancel;
use super::lease::{LeaseGuardian, LeaseLost, LeasePacing, LeaseWatch, RenewOutcome};
use super::record::{MobilityRecord, MobilityStore};
use crate::snapshot::{
    DriveForMigration, MigrationFingerprint, MobilityBlocker, OverlaybdLayerRef,
};
use crate::types::SandboxId;

/// The side of a migration that actually touches a runtime.
///
/// Injected so the saga's ordering and compensation can be exercised without a
/// hypervisor: the failure paths are the part worth testing, and they are the
/// part hardest to provoke against real KVM.
#[async_trait]
pub trait MigrationSteps: Send + Sync {
    /// Brings the sandbox up on this node from its committed snapshot.
    async fn restore(&self, record: &MobilityRecord) -> Result<()>;

    /// Tears down a restore that did not finish.
    ///
    /// Must be safe to call when `restore` failed part-way or not at all.
    async fn discard_restored(&self, record: &MobilityRecord) -> Result<()>;

    /// Drops the origin's copy of the paused state after a completed handover.
    ///
    /// Runs after the point of no return, so a failure here is reported but
    /// does not undo the migration: the sandbox is already live elsewhere and
    /// the leftover is reclaimable local state, not a second copy.
    async fn release_origin_state(&self, record: &MobilityRecord) -> Result<()>;
}

/// What happened to a migration attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum MigrationOutcome {
    /// The sandbox now runs on this node.
    Migrated,
    /// This node cannot run the sandbox. Try a different destination.
    Refused(MobilityBlocker),
    /// The sandbox was not available to claim.
    NotClaimable(ClaimOutcome),
    /// The restore failed and the sandbox was left with its origin.
    RolledBack { reason: String },
    /// The guest is live here and the record could not be read to say whether
    /// the handover was recorded.
    ///
    /// Not a rollback, because nothing was unwound. A live guest on this node
    /// is consistent with both states the record could be in; discarding it is
    /// only right for one of them, and wrong for the other in a way nothing can
    /// undo — a tombstone naming a node that threw the guest away fences every
    /// node, including the origin, out of the sandbox for good.
    ///
    /// What remains exposed is the other half: if the record still says
    /// `Claimed`, that claim lapses while this guest runs. That is the lesser
    /// half — bounded by the TTL and recoverable, against a loss that is
    /// neither — and closing it needs a store that answers, which is what the
    /// caller has to wait for.
    Unresolved { reason: String },
}

/// What the saga concluded about its own commit.
enum CommitDecision {
    /// The record names this node. The handover stands.
    Committed,
    /// The handover did not happen and will not. Compensate.
    Unwind(String),
    /// Neither could be established. See [`MigrationOutcome::Unresolved`].
    Unresolved(String),
}

/// How many times an ambiguous commit is re-read before the saga stops trying
/// to decide it.
///
/// Bounded because a guest is held up while this runs, and an outage that
/// outlasts a few reads is not one more read away from settling.
const COMMIT_RESOLUTION_ATTEMPTS: usize = 4;

/// Delay before the second re-read, doubling from there.
///
/// The first re-read is immediate: the write is already over, and the ordinary
/// shape of this failure is a lost reply from a store that is still answering.
const COMMIT_RESOLUTION_BACKOFF: Duration = Duration::from_millis(100);

/// Runs migrations on behalf of a destination node.
pub struct MigrationSaga<S: MobilityStore> {
    coordinator: Arc<MobilityCoordinator<S>>,
    steps: Arc<dyn MigrationSteps>,
    claim_ttl: Duration,
}

impl<S: MobilityStore + 'static> MigrationSaga<S> {
    pub fn new(coordinator: Arc<MobilityCoordinator<S>>, steps: Arc<dyn MigrationSteps>) -> Self {
        Self {
            coordinator,
            steps,
            claim_ttl: DEFAULT_CLAIM_TTL,
        }
    }

    /// Must match the coordinator's TTL; only the renewal cadence is derived
    /// from it here.
    pub fn with_claim_ttl(mut self, claim_ttl: Duration) -> Self {
        self.claim_ttl = claim_ttl;
        self
    }

    /// Migrates `sandbox_id` onto this node.
    ///
    /// `cancel` is how a caller asks for the migration to stop. It is watched
    /// only where the saga can still unwind; see the module's "Giving up".
    pub async fn migrate(
        &self,
        sandbox_id: &SandboxId,
        host: &MigrationFingerprint,
        rootfs_layers: &[OverlaybdLayerRef],
        attached_drives: &[DriveForMigration<'_>],
        cancel: &MoveCancel,
    ) -> Result<MigrationOutcome> {
        let Some(record) = self.coordinator.store().get(sandbox_id).await? else {
            return Ok(MigrationOutcome::NotClaimable(ClaimOutcome::Unknown));
        };

        // Compatibility first: a refusal is free, and claiming a sandbox this
        // node cannot run would block a destination that can.
        if let Err(blocker) = record.can_move_to(host, rootfs_layers, attached_drives) {
            return Ok(MigrationOutcome::Refused(blocker));
        }

        // Same reasoning as the compatibility check, for the same cost: a
        // migration told to stop before it started has nothing to unwind, and
        // claiming only to release again leaves a destination that could have
        // taken the sandbox racing a claim that was never going to be used.
        if cancel.is_requested() {
            return Ok(MigrationOutcome::RolledBack {
                reason: "the migration was cancelled before it claimed the sandbox".to_string(),
            });
        }

        let claimed = match self.coordinator.claim(sandbox_id).await? {
            ClaimOutcome::Claimed(record) => *record,
            other => return Ok(MigrationOutcome::NotClaimable(other)),
        };

        // The restore races its own lease. Losing it has to stop the restore
        // rather than be discovered at the end: a holder still writing the
        // sandbox's devices while another node starts restoring the same state
        // is the exact failure the lease exists to prevent, and finishing work
        // that will be thrown away is the lesser part of it.
        let (guardian, mut lease) = self.guard_claim(*sandbox_id);
        let restored = tokio::select! {
            biased;
            lost = lease.lost() => {
                // Dropping the restore future here cancels it at its next
                // await point; `discard_restored` cleans up whatever it had
                // already built.
                Err(lost.to_string())
            }
            // The restore is the long step, so it is the one a caller giving
            // up is waiting on. Dropping it here is safe for the same reason
            // it is safe above, and only because the rollback below runs.
            () = cancel.requested() => {
                Err("the migration was cancelled before the restore finished".to_string())
            }
            result = self.steps.restore(&claimed) => result.map_err(|error| error.to_string()),
        };

        if let Err(reason) = restored {
            drop(guardian);
            self.roll_back(&claimed, &reason).await;
            return Ok(MigrationOutcome::RolledBack { reason });
        }

        // Point of no return. The guest is live here and the record does not
        // say so yet, so the lease has to stay renewed across this write —
        // releasing it when the restore returned would let the claim go stale
        // during the commit, and a slow or wedged commit would then let the
        // origin resume while this guest is already running.
        //
        // `cancel` is deliberately not consulted from here on. A caller that
        // gives up between a live guest and the record that names its owner
        // is asking for exactly the ambiguity this ordering exists to remove;
        // what remains is one store write, and finishing it is faster than
        // tearing down a restore that worked.
        //
        // The write races the lease all the same. A wedged store can hold it
        // open past the point the guardian gives the claim up, and the origin
        // then resumes a sandbox that is already live here. Losing the lease
        // outranks finishing the handover: the origin is about to take over, so
        // this copy goes whether or not the write landed. What that may leave
        // behind — a tombstone naming a node that discarded its guest — is what
        // the rollback's release path clears.
        let decision = tokio::select! {
            biased;
            lost = lease.lost() => {
                CommitDecision::Unwind(format!("the claim was lost while recording the handover: {lost}"))
            }
            committed = self.coordinator.complete(sandbox_id) => match committed {
                Ok(true) => CommitDecision::Committed,
                Ok(false) => {
                    CommitDecision::Unwind("the claim was lost during the restore".to_string())
                }
                // The read failed, so no write went out and the record is
                // untouched. Unwinding is safe and is the direction to fail in.
                Err(CommitFailure::NeverSent(error)) => CommitDecision::Unwind(format!(
                    "the handover could not be recorded: {error:#}"
                )),
                // The write may well have applied. Discarding the guest before
                // finding out is what loses the sandbox for good.
                Err(CommitFailure::Ambiguous(error)) => {
                    self.settle_ambiguous_commit(sandbox_id, &error).await
                }
            },
        };
        // Only now: the disambiguation above is a series of reads that decide
        // whether this node owns the sandbox, and dropping the renewal first
        // would let the claim go stale underneath them.
        guardian.release();

        match decision {
            CommitDecision::Committed => {}
            CommitDecision::Unwind(reason) => {
                // The guest running here is now the second copy, so it is the
                // one that has to go.
                warn!(%sandbox_id, "{reason}; discarding the restore rather than keeping two copies");
                self.roll_back(&claimed, &reason).await;
                return Ok(MigrationOutcome::RolledBack { reason });
            }
            CommitDecision::Unresolved(reason) => {
                warn!(
                    %sandbox_id,
                    "{reason}; keeping the restored guest, which is the only choice consistent \
                     with both states the record could be in"
                );
                return Ok(MigrationOutcome::Unresolved { reason });
            }
        }

        if let Err(error) = self.steps.release_origin_state(&claimed).await {
            // Reported, not rolled back: the sandbox is live here and the
            // record says so. What is left behind is reclaimable local state
            // on the origin, not a competing copy.
            warn!(
                %sandbox_id,
                error = %error,
                "migration completed but the origin's paused state was not released"
            );
        }

        info!(%sandbox_id, from = %claimed.origin_node_id, "migrated sandbox");
        Ok(MigrationOutcome::Migrated)
    }

    /// Decides an ambiguous commit by asking the record who owns the sandbox.
    ///
    /// The lease is still held throughout. These reads are the whole of what
    /// stands between a lost reply and a sandbox no node will run: `Evacuated`
    /// is terminal, so a rollback that guesses wrong here cannot be taken back.
    async fn settle_ambiguous_commit(
        &self,
        sandbox_id: &SandboxId,
        error: &anyhow::Error,
    ) -> CommitDecision {
        let mut last_error = format!("{error:#}");
        let mut backoff = COMMIT_RESOLUTION_BACKOFF;
        for attempt in 0..COMMIT_RESOLUTION_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            match self.coordinator.commit_standing(sandbox_id).await {
                // Only this node's own commit puts this node's id there, so
                // the write landed and the reply is what went missing.
                Ok(CommitStanding::Committed) => return CommitDecision::Committed,
                // The write did not land and the claim is still ours, so
                // sending it again is the same conditional write as before.
                Ok(CommitStanding::StillClaimed) => {
                    match self.coordinator.complete(sandbox_id).await {
                        Ok(true) => return CommitDecision::Committed,
                        Ok(false) => {
                            return CommitDecision::Unwind(
                                "the claim was lost during the restore".to_string(),
                            )
                        }
                        Err(CommitFailure::NeverSent(error)) => {
                            return CommitDecision::Unwind(format!(
                                "the handover could not be recorded: {error:#}"
                            ))
                        }
                        // Ambiguous again, and for the same reason. Round again
                        // rather than act on a doubt that has not been resolved.
                        Err(CommitFailure::Ambiguous(error)) => last_error = format!("{error:#}"),
                    }
                }
                Ok(CommitStanding::Lost { detail }) => {
                    return CommitDecision::Unwind(format!(
                        "the handover was not recorded: {detail}"
                    ))
                }
                Err(error) => last_error = format!("{error:#}"),
            }
        }
        CommitDecision::Unresolved(format!(
            "the handover was neither confirmed nor ruled out in {COMMIT_RESOLUTION_ATTEMPTS} \
             attempts: {last_error}"
        ))
    }

    /// Tears down the partial restore, then gives the claim back.
    ///
    /// In that order: releasing first would let a second destination start
    /// while this one still holds the sandbox's devices open.
    async fn roll_back(&self, record: &MobilityRecord, reason: &str) {
        if let Err(error) = self.steps.discard_restored(record).await {
            warn!(
                sandbox_id = %record.sandbox_id,
                error = %error,
                "failed to discard a partial restore; not releasing the claim, so it lapses \
                 instead of inviting a second destination in immediately"
            );
            return;
        }
        match self.coordinator.release(&record.sandbox_id).await {
            Ok(ReleaseOutcome::Released) => {
                info!(sandbox_id = %record.sandbox_id, reason, "rolled back migration")
            }
            Ok(ReleaseOutcome::NotHeld) => warn!(
                sandbox_id = %record.sandbox_id,
                "the claim was no longer ours to release"
            ),
            // The commit had landed after all, and the guest it named is the
            // one just torn down. Parking the record hands the sandbox back to
            // the origin, which still holds the paused state; leaving the
            // tombstone would fence every node out of a sandbox nobody runs.
            Ok(ReleaseOutcome::AlreadyCommitted) => {
                match self
                    .coordinator
                    .abandon_evacuation(&record.sandbox_id)
                    .await
                {
                    Ok(true) => warn!(
                        sandbox_id = %record.sandbox_id,
                        reason,
                        "the handover had been recorded after all; parked the record again so \
                         the origin can reclaim the sandbox"
                    ),
                    Ok(false) => warn!(
                        sandbox_id = %record.sandbox_id,
                        "discarded a restore the record still names; the sandbox stays fenced \
                         until the tombstone is reconciled"
                    ),
                    Err(error) => warn!(
                        sandbox_id = %record.sandbox_id,
                        error = %error,
                        "discarded a restore the record still names and could not park it; the \
                         sandbox stays fenced until the tombstone is reconciled"
                    ),
                }
            }
            Err(error) => warn!(
                sandbox_id = %record.sandbox_id,
                error = %error,
                "failed to release the claim; it will lapse"
            ),
        }
    }

    /// Keeps the claim alive for as long as the restore runs.
    ///
    /// Without renewal a restore longer than the TTL expires its own claim,
    /// which is precisely the case migration exists for: large memory images
    /// are what make a restore slow.
    fn guard_claim(&self, sandbox_id: SandboxId) -> (LeaseGuardian, LeaseWatch) {
        let coordinator = Arc::clone(&self.coordinator);
        let node_id = self.coordinator.node_id().to_string();
        LeaseGuardian::spawn(LeasePacing::new(self.claim_ttl), move || {
            let coordinator = Arc::clone(&coordinator);
            let node_id = node_id.clone();
            Box::pin(async move {
                match coordinator.claim(&sandbox_id).await {
                    Ok(ClaimOutcome::Claimed(_)) => RenewOutcome::Held,
                    Ok(ClaimOutcome::AlreadyClaimed { by_node_id, .. }) => {
                        RenewOutcome::Lost(LeaseLost::Taken { by: by_node_id })
                    }
                    // Our own completed handover, seen by a renewal that
                    // raced the commit. We own it; that is not a loss.
                    Ok(ClaimOutcome::AlreadyEvacuated { to_node_id }) if to_node_id == node_id => {
                        RenewOutcome::Held
                    }
                    Ok(ClaimOutcome::AlreadyEvacuated { to_node_id }) => {
                        RenewOutcome::Lost(LeaseLost::Taken { by: to_node_id })
                    }
                    Ok(ClaimOutcome::Unknown) => RenewOutcome::Lost(LeaseLost::Gone),
                    // Retryable rather than final: the write lost a race, and
                    // the next attempt re-reads and gets a definitive answer.
                    Ok(ClaimOutcome::Superseded) => {
                        RenewOutcome::Failed(anyhow::anyhow!("the mobility record was superseded"))
                    }
                    Err(error) => RenewOutcome::Failed(error),
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::mobility::record::{
        LocalMobilityStore, MobilityRecord, MobilityState, MobilityWrite,
    };
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSteps {
        calls: Mutex<Vec<&'static str>>,
        restore_error: Option<&'static str>,
        discard_error: Option<&'static str>,
        restore_delay: Option<Duration>,
    }

    impl RecordingSteps {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MigrationSteps for RecordingSteps {
        async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
            if let Some(delay) = self.restore_delay {
                tokio::time::sleep(delay).await;
            }
            self.calls.lock().unwrap().push("restore");
            match self.restore_error {
                Some(error) => Err(anyhow::anyhow!(error)),
                None => Ok(()),
            }
        }

        async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
            self.calls.lock().unwrap().push("discard");
            match self.discard_error {
                Some(error) => Err(anyhow::anyhow!(error)),
                None => Ok(()),
            }
        }

        async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
            self.calls.lock().unwrap().push("release_origin");
            Ok(())
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        store: LocalMobilityStore,
        sandbox_id: SandboxId,
        fingerprint: MigrationFingerprint,
    }

    async fn fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("open store");
        let metadata = SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "vmlinux-6.1.175".to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            ..SandboxMetadata::default()
        };
        let record = MobilityRecord::for_paused(
            &metadata,
            "node-a",
            "x86_64",
            Some("{}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        );
        store.upsert(&record).await.expect("seed record");
        Fixture {
            _dir: dir,
            store,
            sandbox_id: metadata.id,
            fingerprint: record.fingerprint,
        }
    }

    fn saga(
        store: LocalMobilityStore,
        node: &str,
        steps: Arc<RecordingSteps>,
        ttl: Duration,
    ) -> MigrationSaga<LocalMobilityStore> {
        let coordinator = Arc::new(MobilityCoordinator::new(store, node).with_claim_ttl(ttl));
        MigrationSaga::new(coordinator, steps).with_claim_ttl(ttl)
    }

    #[tokio::test]
    async fn a_successful_migration_restores_then_commits_then_releases_the_origin() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps::default());
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        assert_eq!(
            saga.migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
                .await
                .expect("migrate"),
            MigrationOutcome::Migrated
        );
        assert_eq!(steps.calls(), vec!["restore", "release_origin"]);

        let record = f
            .store
            .get(&f.sandbox_id)
            .await
            .expect("get")
            .expect("record");
        assert!(matches!(
            record.state,
            MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
        ));
    }

    /// An incompatible destination must be ruled out before it claims, or it
    /// blocks a destination that could actually run the sandbox.
    #[tokio::test]
    async fn an_incompatible_destination_refuses_without_claiming() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps::default());
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        let host = MigrationFingerprint {
            kernel_version: "vmlinux-6.1.190".to_string(),
            ..f.fingerprint.clone()
        };
        match saga
            .migrate(&f.sandbox_id, &host, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate")
        {
            MigrationOutcome::Refused(blocker) => assert_eq!(blocker.kind(), "kernel_version"),
            other => panic!("expected a refusal, got {other:?}"),
        }

        assert!(steps.calls().is_empty(), "nothing should have been touched");
        assert_eq!(
            f.store
                .get(&f.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked,
            "the sandbox must stay available to a destination that can run it"
        );
    }

    /// A failed restore must leave the sandbox exactly where it started, with
    /// the destination's partial state gone and the claim given back.
    #[tokio::test]
    async fn a_failed_restore_rolls_back_and_leaves_the_sandbox_parked() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps {
            restore_error: Some("no memory for the guest"),
            ..RecordingSteps::default()
        });
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        match saga
            .migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate")
        {
            MigrationOutcome::RolledBack { reason } => assert!(reason.contains("no memory")),
            other => panic!("expected a rollback, got {other:?}"),
        }

        assert_eq!(steps.calls(), vec!["restore", "discard"]);
        assert_eq!(
            f.store
                .get(&f.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked
        );
    }

    /// If the partial restore cannot be torn down, the claim must NOT be given
    /// back: a second destination arriving while this one still holds the
    /// sandbox's devices is worse than waiting for the claim to lapse.
    #[tokio::test]
    async fn a_failed_teardown_keeps_the_claim_rather_than_inviting_a_second_destination() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps {
            restore_error: Some("restore failed"),
            discard_error: Some("device still busy"),
            ..RecordingSteps::default()
        });
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        saga.migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate");

        let record = f
            .store
            .get(&f.sandbox_id)
            .await
            .expect("get")
            .expect("record");
        assert!(
            matches!(record.state, MobilityState::Claimed { .. }),
            "the claim must be held until it lapses, got {:?}",
            record.state
        );
    }

    /// A sandbox someone else is already restoring must not be taken.
    #[tokio::test]
    async fn a_claimed_sandbox_is_not_migrated() {
        let f = fixture().await;
        MobilityCoordinator::new(f.store.clone(), "node-c")
            .claim(&f.sandbox_id)
            .await
            .expect("claim");

        let steps = Arc::new(RecordingSteps::default());
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        match saga
            .migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate")
        {
            MigrationOutcome::NotClaimable(ClaimOutcome::AlreadyClaimed { by_node_id, .. }) => {
                assert_eq!(by_node_id, "node-c")
            }
            other => panic!("expected the claim to block, got {other:?}"),
        }
        assert!(steps.calls().is_empty());
    }

    /// The case migration exists for: a restore that takes longer than the
    /// claim TTL. Without renewal it would expire its own claim and roll back
    /// a restore that was going fine.
    #[tokio::test]
    async fn a_restore_longer_than_the_ttl_keeps_its_claim() {
        let f = fixture().await;
        let ttl = Duration::from_millis(300);
        let steps = Arc::new(RecordingSteps {
            restore_delay: Some(ttl * 3),
            ..RecordingSteps::default()
        });
        let saga = saga(f.store.clone(), "node-b", steps.clone(), ttl);

        assert_eq!(
            saga.migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
                .await
                .expect("migrate"),
            MigrationOutcome::Migrated,
            "renewal must keep a slow restore's claim alive"
        );
    }

    /// If the claim is taken while the restore is running, the restore must be
    /// cancelled then and there. Letting it finish means writing the sandbox's
    /// devices while the new owner is restoring the same state — the exact
    /// failure the lease exists to prevent.
    #[tokio::test]
    async fn losing_the_claim_mid_restore_cancels_the_restore() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct SlowSteps {
            reached_the_end: Arc<AtomicBool>,
            discarded: Arc<AtomicBool>,
        }

        #[async_trait]
        impl MigrationSteps for SlowSteps {
            async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
                // Cancellation shows up as this future being dropped at the
                // sleep, so the flag past it never gets set.
                tokio::time::sleep(Duration::from_secs(30)).await;
                self.reached_the_end.store(true, Ordering::SeqCst);
                Ok(())
            }

            async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
                self.discarded.store(true, Ordering::SeqCst);
                Ok(())
            }

            async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
                Ok(())
            }
        }

        let f = fixture().await;
        let reached_the_end = Arc::new(AtomicBool::new(false));
        let discarded = Arc::new(AtomicBool::new(false));
        let ttl = Duration::from_millis(300);
        let saga = {
            let coordinator =
                Arc::new(MobilityCoordinator::new(f.store.clone(), "node-b").with_claim_ttl(ttl));
            MigrationSaga::new(
                coordinator,
                Arc::new(SlowSteps {
                    reached_the_end: Arc::clone(&reached_the_end),
                    discarded: Arc::clone(&discarded),
                }),
            )
            .with_claim_ttl(ttl)
        };

        // Steal the claim once the restore is under way.
        let thief_store = f.store.clone();
        let sandbox_id = f.sandbox_id;
        let thief = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            let thief = MobilityCoordinator::new(thief_store, "node-c").with_claim_ttl(ttl);
            // The claim is node-b's and still live, so force the takeover the
            // way an expiry would: write a newer generation.
            //
            // Arbitrated rather than written over the top. An `upsert` here
            // races node-b's renewals: it is refused outright if a renewal
            // landed first, and the test would then sit through the whole
            // thirty-second restore proving nothing, because node-b never
            // lost a claim it never had taken from it.
            for attempt in 0.. {
                let record = thief
                    .store()
                    .get(&sandbox_id)
                    .await
                    .expect("get")
                    .expect("record");
                // Stamped now, not at the epoch: a claim that is already
                // expired would simply be re-taken by node-b's next renewal,
                // and the test would prove nothing.
                let stolen = record.transitioned_to(MobilityState::Claimed {
                    by_node_id: "node-c".to_string(),
                    at_unix_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock after the epoch")
                        .as_millis() as u64,
                });
                match thief
                    .store()
                    .compare_and_set(Some(record.generation), &stolen)
                    .await
                    .expect("steal")
                {
                    MobilityWrite::Applied => break,
                    // A renewal landed between the read and the write. Re-read
                    // and decide again, which is what any real claimant does.
                    MobilityWrite::Superseded => assert!(
                        attempt < 100,
                        "the steal never won a generation race against node-b's renewals"
                    ),
                }
            }
        });

        let outcome = saga
            .migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate");
        thief.await.expect("thief");

        match outcome {
            MigrationOutcome::RolledBack { reason } => {
                assert!(reason.contains("node-c"), "unexpected reason: {reason}")
            }
            other => panic!("expected a rollback, got {other:?}"),
        }
        assert!(
            !reached_the_end.load(Ordering::SeqCst),
            "the restore must have been cancelled, not allowed to finish"
        );
        assert!(
            discarded.load(Ordering::SeqCst),
            "the partial restore must be torn down"
        );
    }

    /// Cancelling has to run the compensations, not skip them. The reason the
    /// caller asks instead of dropping the future is that a dropped future
    /// leaves the destination holding a partial restore under a claim nobody
    /// releases.
    #[tokio::test]
    async fn a_cancellation_mid_restore_rolls_back_and_leaves_the_sandbox_parked() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps {
            restore_delay: Some(Duration::from_secs(30)),
            ..RecordingSteps::default()
        });
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        let cancel = MoveCancel::new();
        let stopper = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                cancel.request();
            })
        };

        match saga
            .migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &cancel)
            .await
            .expect("migrate")
        {
            MigrationOutcome::RolledBack { reason } => {
                assert!(reason.contains("cancelled"), "unexpected reason: {reason}")
            }
            other => panic!("expected a rollback, got {other:?}"),
        }
        stopper.await.expect("stopper");

        assert_eq!(
            steps.calls(),
            vec!["discard"],
            "the restore must be cut short and torn down, not allowed to finish"
        );
        assert_eq!(
            f.store
                .get(&f.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked
        );
    }

    /// A migration told to stop before it started has nothing to unwind, so it
    /// must not take the claim at all: a destination that could actually run
    /// the sandbox would find it claimed by a move already giving up.
    #[tokio::test]
    async fn a_migration_cancelled_before_it_claims_touches_nothing() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps::default());
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        let cancel = MoveCancel::new();
        cancel.request();
        match saga
            .migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &cancel)
            .await
            .expect("migrate")
        {
            MigrationOutcome::RolledBack { reason } => {
                assert!(reason.contains("cancelled"), "unexpected reason: {reason}")
            }
            other => panic!("expected a rollback, got {other:?}"),
        }

        assert!(steps.calls().is_empty(), "nothing should have been touched");
        assert_eq!(
            f.store
                .get(&f.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked,
            "the sandbox must stay available to a destination that will finish"
        );
    }

    /// Past the restore the guest is live here and only the record disagrees.
    /// Honouring a cancellation in that window is what produces the ambiguity
    /// the ordering exists to remove, so the commit finishes regardless.
    #[tokio::test]
    async fn a_cancellation_after_the_restore_does_not_abandon_the_handover() {
        struct CancelWhenRestored {
            cancel: MoveCancel,
        }

        #[async_trait]
        impl MigrationSteps for CancelWhenRestored {
            async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
                // The guest is up on this node the instant this returns.
                self.cancel.request();
                Ok(())
            }

            async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
                panic!("a restore that succeeded must not be torn down");
            }

            async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
                Ok(())
            }
        }

        let f = fixture().await;
        let cancel = MoveCancel::new();
        let coordinator = Arc::new(
            MobilityCoordinator::new(f.store.clone(), "node-b")
                .with_claim_ttl(Duration::from_secs(30)),
        );
        let saga = MigrationSaga::new(
            coordinator,
            Arc::new(CancelWhenRestored {
                cancel: cancel.clone(),
            }),
        )
        .with_claim_ttl(Duration::from_secs(30));

        assert_eq!(
            saga.migrate(&f.sandbox_id, &f.fingerprint, &[], &[], &cancel)
                .await
                .expect("migrate"),
            MigrationOutcome::Migrated
        );
        assert!(
            matches!(
                f.store
                    .get(&f.sandbox_id)
                    .await
                    .expect("get")
                    .expect("record")
                    .state,
                MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
            ),
            "the record must name the node the guest is actually running on"
        );
    }

    #[tokio::test]
    async fn an_unknown_sandbox_is_not_migrated() {
        let f = fixture().await;
        let steps = Arc::new(RecordingSteps::default());
        let saga = saga(
            f.store.clone(),
            "node-b",
            steps.clone(),
            Duration::from_secs(30),
        );

        assert_eq!(
            saga.migrate(
                &SandboxId::new(),
                &f.fingerprint,
                &[],
                &[],
                &MoveCancel::new()
            )
            .await
            .expect("migrate"),
            MigrationOutcome::NotClaimable(ClaimOutcome::Unknown)
        );
        assert!(steps.calls().is_empty());
    }

    /// The renewal arm that has to tell this node's own completed handover from
    /// a rival's. A renewal racing the commit is how this state is reached, and
    /// getting the tie-break backwards either surrenders a lease that is still
    /// ours or keeps renewing one a rival has taken.
    #[tokio::test]
    async fn a_renewal_tells_this_nodes_own_handover_from_a_rivals() {
        let f = fixture().await;
        let destination = MobilityCoordinator::new(f.store.clone(), "node-b");
        destination.claim(&f.sandbox_id).await.expect("claim");
        assert!(destination.complete(&f.sandbox_id).await.expect("complete"));

        let ttl = Duration::from_millis(300);
        let saga = saga(
            f.store.clone(),
            "node-b",
            Arc::new(RecordingSteps::default()),
            ttl,
        );
        let (guardian, mut lease) = saga.guard_claim(f.sandbox_id);

        // Several renewals against this node's own tombstone.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(
            lease.lost_now(),
            None,
            "a renewal that finds our own handover has not lost anything"
        );

        // The same state naming somebody else is a loss, and a final one.
        let record = f
            .store
            .get(&f.sandbox_id)
            .await
            .expect("get")
            .expect("record");
        let taken = record.transitioned_to(MobilityState::Evacuated {
            to_node_id: "node-c".to_string(),
            at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after the epoch")
                .as_millis() as u64,
        });
        f.store.upsert(&taken).await.expect("overwrite");

        let lost = tokio::time::timeout(Duration::from_secs(2), lease.lost())
            .await
            .expect("a handover to another node must be reported as a loss");
        assert_eq!(
            lost,
            LeaseLost::Taken {
                by: "node-c".to_string()
            }
        );
        guardian.release();
    }
}

#[cfg(test)]
mod lost_claim_tests {
    use super::*;
    use crate::orchestrator::mobility::record::{
        LocalMobilityStore, MobilityRecord, MobilityState, MobilityWrite,
    };
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, MigrationFingerprint, SnapshotRuntimeVersions};
    use crate::types::SandboxId;
    use crate::virtualization::VirtualizationMode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    fn metadata() -> SandboxMetadata {
        SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "vmlinux-6.1.175".to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            ..SandboxMetadata::default()
        }
    }

    async fn seeded() -> (
        LocalMobilityStore,
        SandboxId,
        MigrationFingerprint,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("store");
        let metadata = metadata();
        let record = MobilityRecord::for_paused(
            &metadata,
            "node-a",
            "x86_64",
            Some("{}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        );
        let fingerprint = record.fingerprint.clone();
        store.upsert(&record).await.expect("seed");
        (store, metadata.id, fingerprint, dir)
    }

    /// The branch that decides what happens when the guest is already live
    /// here but the claim turned out to be gone. Its own comment calls this
    /// out as "the guest running here is now the second copy", and until now
    /// it had never executed.
    ///
    /// The lease cannot catch this one: the claim is stolen after the restore
    /// returns and before the commit, so the guardian never gets a renewal in
    /// between. Only the commit's own check stands between this and two live
    /// copies.
    #[tokio::test]
    async fn a_claim_lost_between_restore_and_commit_discards_the_restore() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;

        struct StealAtTheEnd {
            store: LocalMobilityStore,
            sandbox_id: SandboxId,
            discarded: Arc<AtomicBool>,
            released_origin: Arc<AtomicBool>,
        }

        #[async_trait]
        impl MigrationSteps for StealAtTheEnd {
            async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
                // The guest is up. A rival takes the claim in the instant
                // before the commit lands.
                let current = self
                    .store
                    .get(&self.sandbox_id)
                    .await
                    .expect("get")
                    .expect("record");
                let stolen = current.transitioned_to(MobilityState::Claimed {
                    by_node_id: "node-c".to_string(),
                    at_unix_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock after the epoch")
                        .as_millis() as u64,
                });
                self.store.upsert(&stolen).await.expect("steal");
                Ok(())
            }

            async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
                self.discarded.store(true, Ordering::SeqCst);
                Ok(())
            }

            async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
                self.released_origin.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let discarded = Arc::new(AtomicBool::new(false));
        let released_origin = Arc::new(AtomicBool::new(false));
        let coordinator = Arc::new(
            MobilityCoordinator::new(store.clone(), "node-b")
                .with_claim_ttl(Duration::from_secs(30)),
        );
        let saga = MigrationSaga::new(
            coordinator,
            Arc::new(StealAtTheEnd {
                store: store.clone(),
                sandbox_id,
                discarded: Arc::clone(&discarded),
                released_origin: Arc::clone(&released_origin),
            }),
        )
        .with_claim_ttl(Duration::from_secs(30));

        let outcome = saga
            .migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate");

        match outcome {
            MigrationOutcome::RolledBack { reason } => assert!(
                reason.contains("claim was lost"),
                "unexpected reason: {reason}"
            ),
            other => panic!("a lost claim must roll back, got {other:?}"),
        }
        assert!(
            discarded.load(Ordering::SeqCst),
            "the guest running here is the second copy and must be torn down"
        );
        assert!(
            !released_origin.load(Ordering::SeqCst),
            "the origin must keep its copy: this node did not take ownership"
        );

        // And the rival still holds it, untouched by our rollback.
        let stored = store.get(&sandbox_id).await.expect("get").expect("record");
        assert!(
            matches!(stored.state, MobilityState::Claimed { ref by_node_id, .. } if by_node_id == "node-c"),
            "the rival's claim must survive our rollback, got {:?}",
            stored.state
        );
    }

    /// A write that loses a race is retryable, not fatal. Treating it as a
    /// lost claim would tear down a healthy restore because two writes
    /// happened to interleave.
    #[tokio::test]
    async fn a_superseded_write_is_not_taken_as_a_lost_claim() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;
        let store = AlwaysSuperseded {
            inner: store,
            calls: Mutex::new(0),
        };

        let coordinator = Arc::new(MobilityCoordinator::new(store, "node-b"));
        let outcome = MigrationSaga::new(coordinator, Arc::new(NoopSteps))
            .migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate");

        // The claim itself could not be written, so the migration never
        // starts — but it reports a race, not a refusal, so the caller retries
        // rather than giving up on the sandbox.
        assert_eq!(
            outcome,
            MigrationOutcome::NotClaimable(ClaimOutcome::Superseded),
            "a lost write race must surface as retryable"
        );
    }

    struct NoopSteps;

    #[async_trait]
    impl MigrationSteps for NoopSteps {
        async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
            Ok(())
        }
        async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
            Ok(())
        }
        async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
            Ok(())
        }
    }

    /// A store whose writes always lose the generation race, which is what a
    /// concurrent writer looks like from inside one coordinator.
    struct AlwaysSuperseded {
        inner: LocalMobilityStore,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl MobilityStore for AlwaysSuperseded {
        async fn upsert(&self, _record: &MobilityRecord) -> Result<MobilityWrite> {
            *self.calls.lock().unwrap() += 1;
            Ok(MobilityWrite::Superseded)
        }

        async fn compare_and_set(
            &self,
            _expected: Option<crate::orchestrator::MobilityGeneration>,
            _record: &MobilityRecord,
        ) -> Result<MobilityWrite> {
            *self.calls.lock().unwrap() += 1;
            Ok(MobilityWrite::Superseded)
        }

        async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> Result<Vec<MobilityRecord>> {
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
            self.inner.remove(sandbox_id).await
        }
    }
}

/// The commit's own failure modes: a write whose answer went missing, and a
/// read that never became a write.
///
/// These are the paths where a wrong compensation is unrecoverable rather than
/// merely wasteful, because `Evacuated` is terminal and no other code in the
/// tree can clear it.
#[cfg(test)]
mod ambiguous_commit_tests {
    use super::*;
    use crate::orchestrator::mobility::record::{
        LocalMobilityStore, MobilityGeneration, MobilityRecord, MobilityState, MobilityStore,
        MobilityWrite,
    };
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, SnapshotRuntimeVersions};
    use crate::types::SandboxId;
    use crate::virtualization::VirtualizationMode;
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn seeded() -> (
        LocalMobilityStore,
        SandboxId,
        MigrationFingerprint,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("store");
        let metadata = SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "vmlinux-6.1.175".to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            ..SandboxMetadata::default()
        };
        let record = MobilityRecord::for_paused(
            &metadata,
            "node-a",
            "x86_64",
            Some("{}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        );
        let fingerprint = record.fingerprint.clone();
        store.upsert(&record).await.expect("seed");
        (store, metadata.id, fingerprint, dir)
    }

    /// Reports what the saga did to the destination's guest.
    #[derive(Default)]
    struct GuestSteps {
        arm: Option<Arc<AtomicBool>>,
        discarded: Arc<AtomicBool>,
        released_origin: Arc<AtomicBool>,
    }

    #[async_trait]
    impl MigrationSteps for GuestSteps {
        async fn restore(&self, _record: &MobilityRecord) -> Result<()> {
            // The guest is live from here on, which is what makes everything
            // after this a decision about a running sandbox.
            if let Some(arm) = &self.arm {
                arm.store(true, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn discard_restored(&self, _record: &MobilityRecord) -> Result<()> {
            self.discarded.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn release_origin_state(&self, _record: &MobilityRecord) -> Result<()> {
            self.released_origin.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn saga_over<S: MobilityStore + 'static>(
        store: S,
        steps: Arc<GuestSteps>,
        ttl: Duration,
    ) -> MigrationSaga<S> {
        let coordinator = Arc::new(MobilityCoordinator::new(store, "node-b").with_claim_ttl(ttl));
        MigrationSaga::new(coordinator, steps).with_claim_ttl(ttl)
    }

    /// The classic ambiguous RPC failure: the store applied the write and the
    /// reply was lost on the way back. Treating that as a lost claim discards
    /// the only live guest and leaves a tombstone naming the node that threw it
    /// away — the origin keeps its paused copy and is fenced off from it, with
    /// nothing in the protocol able to clear an `Evacuated` record. The sandbox
    /// is then unrunnable everywhere.
    #[tokio::test]
    async fn an_ambiguous_commit_whose_write_landed_completes_the_migration() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;
        let steps = Arc::new(GuestSteps::default());
        let saga = saga_over(
            CommitReplyLost {
                inner: store.clone(),
                dropped_reply: AtomicBool::new(false),
            },
            Arc::clone(&steps),
            Duration::from_secs(30),
        );

        let outcome = saga
            .migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate");

        assert_eq!(
            outcome,
            MigrationOutcome::Migrated,
            "the write landed, so the handover stands"
        );
        assert!(
            !steps.discarded.load(Ordering::SeqCst),
            "the record names this node, so its guest is the sandbox and must not be torn down"
        );
        assert!(
            steps.released_origin.load(Ordering::SeqCst),
            "the origin's copy is the second one now"
        );
        assert!(
            matches!(
                store.get(&sandbox_id).await.expect("get").expect("record").state,
                MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
            ),
            "the record must still name this node"
        );
    }

    /// A read that failed sent nothing, so the record is untouched and
    /// unwinding is safe. Conflating it with a lost reply would keep guests
    /// alive on a node that never took the sandbox.
    #[tokio::test]
    async fn a_commit_whose_read_never_reached_the_store_rolls_back() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;
        let unreachable = Arc::new(AtomicBool::new(false));
        let steps = Arc::new(GuestSteps {
            arm: Some(Arc::clone(&unreachable)),
            ..GuestSteps::default()
        });
        let saga = saga_over(
            UnreadableOnceArmed {
                inner: store.clone(),
                armed: Arc::clone(&unreachable),
            },
            Arc::clone(&steps),
            Duration::from_secs(30),
        );

        match saga
            .migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate")
        {
            MigrationOutcome::RolledBack { reason } => assert!(
                reason.contains("could not be recorded"),
                "unexpected reason: {reason}"
            ),
            other => panic!("a write that never went out must roll back, got {other:?}"),
        }
        assert!(
            steps.discarded.load(Ordering::SeqCst),
            "nothing was recorded, so the guest here is the second copy"
        );
    }

    /// When the store stops answering, neither outcome can be ruled out. The
    /// guest stays: it is the only state consistent with both a record that
    /// names this node and one that does not, and discarding it is the half of
    /// that guess nothing can undo.
    #[tokio::test]
    async fn an_unsettled_commit_keeps_the_guest_rather_than_discarding_it() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;
        let steps = Arc::new(GuestSteps::default());
        let saga = saga_over(
            SilentAfterTheCommit {
                inner: store.clone(),
                attempted_commit: Arc::new(AtomicBool::new(false)),
            },
            Arc::clone(&steps),
            Duration::from_secs(30),
        );

        match saga
            .migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new())
            .await
            .expect("migrate")
        {
            MigrationOutcome::Unresolved { reason } => assert!(
                reason.contains("neither confirmed nor ruled out"),
                "unexpected reason: {reason}"
            ),
            other => panic!("an unreadable store must leave the move unsettled, got {other:?}"),
        }
        assert!(
            !steps.discarded.load(Ordering::SeqCst),
            "discarding is the losing move if the record already names this node"
        );
        assert!(
            !steps.released_origin.load(Ordering::SeqCst),
            "the origin's copy is what makes the sandbox recoverable at all"
        );
    }

    /// A store that wedges the commit rather than failing it holds the write
    /// open past the point the guardian gives the claim up, and the origin then
    /// resumes a sandbox that is live here. Losing the lease outranks finishing
    /// the handover, so the guest goes.
    #[tokio::test]
    async fn losing_the_lease_while_the_commit_hangs_discards_the_guest() {
        let (store, sandbox_id, fingerprint, _dir) = seeded().await;
        let wedged = Arc::new(AtomicBool::new(false));
        let steps = Arc::new(GuestSteps {
            arm: Some(Arc::clone(&wedged)),
            ..GuestSteps::default()
        });
        let saga = saga_over(
            WedgedUntilDiscarded {
                inner: store.clone(),
                armed: Arc::clone(&wedged),
                discarded: Arc::clone(&steps.discarded),
            },
            Arc::clone(&steps),
            Duration::from_millis(300),
        );

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            saga.migrate(&sandbox_id, &fingerprint, &[], &[], &MoveCancel::new()),
        )
        .await
        .expect("the saga must give up on a wedged commit before the origin takes over")
        .expect("migrate");

        match outcome {
            MigrationOutcome::RolledBack { reason } => assert!(
                reason.contains("lost while recording the handover"),
                "unexpected reason: {reason}"
            ),
            other => panic!("a lost lease must unwind the handover, got {other:?}"),
        }
        assert!(
            steps.discarded.load(Ordering::SeqCst),
            "the origin is free to resume once the claim lapses, so this guest cannot stay"
        );
        assert_eq!(
            store
                .get(&sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked,
            "once the store answers again the claim goes back, leaving the origin the only owner"
        );
    }

    /// Applies the commit and then loses the reply, once.
    struct CommitReplyLost {
        inner: LocalMobilityStore,
        dropped_reply: AtomicBool,
    }

    #[async_trait]
    impl MobilityStore for CommitReplyLost {
        async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
            self.inner.upsert(record).await
        }

        async fn compare_and_set(
            &self,
            expected: Option<MobilityGeneration>,
            record: &MobilityRecord,
        ) -> Result<MobilityWrite> {
            let applied = self.inner.compare_and_set(expected, record).await?;
            if matches!(record.state, MobilityState::Evacuated { .. })
                && !self.dropped_reply.swap(true, Ordering::SeqCst)
            {
                anyhow::bail!("connection reset while reading the reply");
            }
            Ok(applied)
        }

        async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> Result<Vec<MobilityRecord>> {
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
            self.inner.remove(sandbox_id).await
        }
    }

    /// Stops answering reads once the guest is up, so the commit's own read is
    /// the call that fails.
    struct UnreadableOnceArmed {
        inner: LocalMobilityStore,
        armed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl MobilityStore for UnreadableOnceArmed {
        async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
            self.inner.upsert(record).await
        }

        async fn compare_and_set(
            &self,
            expected: Option<MobilityGeneration>,
            record: &MobilityRecord,
        ) -> Result<MobilityWrite> {
            self.inner.compare_and_set(expected, record).await
        }

        async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
            if self.armed.load(Ordering::SeqCst) {
                anyhow::bail!("the mobility store is unreachable");
            }
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> Result<Vec<MobilityRecord>> {
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
            self.inner.remove(sandbox_id).await
        }
    }

    /// Fails the commit write and then every read after it, which is what an
    /// outage that starts mid-handover looks like.
    struct SilentAfterTheCommit {
        inner: LocalMobilityStore,
        attempted_commit: Arc<AtomicBool>,
    }

    #[async_trait]
    impl MobilityStore for SilentAfterTheCommit {
        async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
            self.inner.upsert(record).await
        }

        async fn compare_and_set(
            &self,
            expected: Option<MobilityGeneration>,
            record: &MobilityRecord,
        ) -> Result<MobilityWrite> {
            if matches!(record.state, MobilityState::Evacuated { .. }) {
                self.attempted_commit.store(true, Ordering::SeqCst);
                anyhow::bail!("connection reset while reading the reply");
            }
            self.inner.compare_and_set(expected, record).await
        }

        async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
            if self.attempted_commit.load(Ordering::SeqCst) {
                anyhow::bail!("the mobility store is unreachable");
            }
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> Result<Vec<MobilityRecord>> {
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
            self.inner.remove(sandbox_id).await
        }
    }

    /// Answers nothing at all from the moment the guest is up until the moment
    /// the destination gives it back, which is what a black-holed connection
    /// does — to a renewal exactly as much as to a write. Hanging rather than
    /// failing is the point: an error would be reported and acted on.
    struct WedgedUntilDiscarded {
        inner: LocalMobilityStore,
        armed: Arc<AtomicBool>,
        discarded: Arc<AtomicBool>,
    }

    impl WedgedUntilDiscarded {
        async fn wedge(&self) {
            while self.armed.load(Ordering::SeqCst) && !self.discarded.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    #[async_trait]
    impl MobilityStore for WedgedUntilDiscarded {
        async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
            self.wedge().await;
            self.inner.upsert(record).await
        }

        async fn compare_and_set(
            &self,
            expected: Option<MobilityGeneration>,
            record: &MobilityRecord,
        ) -> Result<MobilityWrite> {
            self.wedge().await;
            self.inner.compare_and_set(expected, record).await
        }

        async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
            self.wedge().await;
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> Result<Vec<MobilityRecord>> {
            self.wedge().await;
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
            self.wedge().await;
            self.inner.remove(sandbox_id).await
        }
    }
}
