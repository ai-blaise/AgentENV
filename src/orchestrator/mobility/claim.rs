//! Agreeing which node owns a paused sandbox during a handover.
//!
//! Two nodes must not run the same sandbox. A destination that restores a
//! snapshot while the origin resumes the same paused state produces two guests
//! that believe they are one: both write the same drives, both answer for the
//! same sandbox id, and the divergence is unrecoverable by the time anyone
//! notices.
//!
//! # What this is
//!
//! A renewable lease over a mobility record. A destination claims a sandbox,
//! renews while it works, and either completes the handover or releases the
//! claim. The origin refuses to resume locally while a live claim exists.
//!
//! # What this is not
//!
//! It is not mutual exclusion. AgentENV has no consensus store and no resource
//! that honours a fencing token, so a claimant that is partitioned — alive,
//! working, unable to renew — will have its claim expire while it is still
//! restoring. The lease narrows that window; it cannot close it.
//!
//! Two things are done about that rather than pretending otherwise:
//!
//! - The claimant renews, so an expired claim means renewal *stopped*, not that
//!   a fixed budget elapsed. A healthy claimant never expires no matter how
//!   long the restore takes.
//! - The origin waits an additional grace period past expiry before it will
//!   resume locally. A renewal that was in flight when the origin checked lands
//!   inside the grace period, so the origin loses that race deterministically
//!   instead of by timing.
//!
//! Closing the window entirely needs a fencing token honoured where the state
//! actually lives — the ublk device or the repository refusing writes from a
//! superseded generation. That does not exist here, and this module is written
//! to be the thing that supplies the generation when it does.
//!
//! # Clock
//!
//! Claim expiry is a wall-clock deadline, because it is compared by two
//! different machines and a monotonic instant means nothing across a process
//! boundary. Skew therefore moves the deadline. The grace period is sized to
//! absorb ordinary NTP-managed skew, and every ambiguous comparison resolves
//! against the actor that would create a second live copy.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;

use super::record::{MobilityRecord, MobilityState, MobilityStore, MobilityWrite};
use crate::types::SandboxId;

/// How long a claim stands without renewal.
///
/// Long enough that an ordinary restore never has to renew twice to survive,
/// short enough that a dead claimant does not park a sandbox for minutes.
pub const DEFAULT_CLAIM_TTL: Duration = Duration::from_secs(30);

/// Extra time any node waits past a claim's expiry before taking it over.
///
/// The defence against a renewal that was in flight when the taker looked: it
/// must exceed plausible clock skew plus one renewal round trip, and it is
/// only ever paid on a path that is already failing.
///
/// Together with the holder's abandon margin this sets how much clock
/// disagreement the protocol tolerates. `docs/specs/MobilityClaim.tla` checks
/// that two live copies are impossible exactly while skew between any two
/// nodes stays within `abandon_margin + TAKEOVER_GRACE`; with a 30s TTL that
/// is 10s + 15s = 25s, against the sub-second skew NTP-managed hosts hold.
const TAKEOVER_GRACE: Duration = Duration::from_secs(15);

/// The result of trying to claim a sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The claim stands, and this is the record that proves it.
    Claimed(Box<MobilityRecord>),
    /// Another node holds a live claim.
    AlreadyClaimed {
        by_node_id: String,
        expires_in: Duration,
    },
    /// The sandbox already moved.
    AlreadyEvacuated { to_node_id: String },
    /// No record: the sandbox is not paused here, or never was.
    Unknown,
    /// The write lost a race with a concurrent one. Re-read and decide again.
    Superseded,
}

/// Why a handover could not be recorded.
///
/// The two are not interchangeable, and treating them as one is how a sandbox
/// is lost: a read that failed touched nothing, so its caller is free to
/// unwind, while a conditional write whose reply never arrived may well have
/// applied. Unwinding on the second discards the only live copy of a sandbox
/// the record now says lives here, and `Evacuated` is terminal.
#[derive(Debug)]
pub enum CommitFailure {
    /// The record could not be read, so no write was attempted.
    NeverSent(anyhow::Error),
    /// The write went out and its outcome is unknown.
    Ambiguous(anyhow::Error),
}

impl CommitFailure {
    /// The underlying store error, whichever side of the write it came from.
    pub fn error(&self) -> &anyhow::Error {
        match self {
            Self::NeverSent(error) | Self::Ambiguous(error) => error,
        }
    }
}

impl std::fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.error())
    }
}

/// What the record says about a handover this node may have committed.
///
/// Only the node that sent the commit can ask this and get an answer: no other
/// node writes this node's id into an `Evacuated` state, so finding it there is
/// proof the write applied, whatever the reply said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitStanding {
    /// The record names this node as the sandbox's home.
    Committed,
    /// The claim is still this node's, so no commit has applied yet.
    StillClaimed,
    /// Somebody else owns the sandbox, or nobody does.
    Lost { detail: String },
}

/// What happened when a claim was given back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The claim is back and the sandbox is parked again.
    Released,
    /// Not this node's claim to give back: taken, already given back, or never
    /// recorded. A write that lost its race lands here too, because the record
    /// moved on without us either way.
    NotHeld,
    /// This node's own completed handover, which a release must not undo.
    AlreadyCommitted,
}

/// Whether the origin may resume a paused sandbox itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumeFence {
    Allowed,
    /// A destination is mid-handover. Resuming would make two live copies.
    ClaimedElsewhere {
        by_node_id: String,
    },
    /// The sandbox lives on another node now.
    Evacuated {
        to_node_id: String,
    },
}

/// Claims and fences paused sandboxes on behalf of one node.
pub struct MobilityCoordinator<S> {
    store: S,
    node_id: String,
    claim_ttl: Duration,
}

impl<S: MobilityStore> MobilityCoordinator<S> {
    pub fn new(store: S, node_id: impl Into<String>) -> Self {
        Self {
            store,
            node_id: node_id.into(),
            claim_ttl: DEFAULT_CLAIM_TTL,
        }
    }

    pub fn with_claim_ttl(mut self, claim_ttl: Duration) -> Self {
        self.claim_ttl = claim_ttl;
        self
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// Claims a sandbox for this node, or renews a claim it already holds.
    ///
    /// Renewal is the same call deliberately: a claimant that must distinguish
    /// "claim" from "renew" has to track whether its own earlier attempt landed,
    /// and it usually cannot know after a timeout.
    pub async fn claim(&self, sandbox_id: &SandboxId) -> Result<ClaimOutcome> {
        self.claim_at(sandbox_id, SystemTime::now()).await
    }

    async fn claim_at(&self, sandbox_id: &SandboxId, now: SystemTime) -> Result<ClaimOutcome> {
        let Some(record) = self.store.get(sandbox_id).await? else {
            return Ok(ClaimOutcome::Unknown);
        };

        match &record.state {
            MobilityState::Evacuated { to_node_id, .. } => {
                return Ok(ClaimOutcome::AlreadyEvacuated {
                    to_node_id: to_node_id.clone(),
                })
            }
            MobilityState::Claimed {
                by_node_id,
                at_unix_ms,
            } if by_node_id != &self.node_id => {
                if let Some(expires_in) = self.remaining_with_grace(*at_unix_ms, now) {
                    return Ok(ClaimOutcome::AlreadyClaimed {
                        by_node_id: by_node_id.clone(),
                        expires_in,
                    });
                }
                // Expired: the previous claimant stopped renewing. Taking over
                // is the point of expiry, so fall through.
            }
            MobilityState::Claimed { .. } | MobilityState::Parked => {}
        }

        let claimed = record.transitioned_to(MobilityState::Claimed {
            by_node_id: self.node_id.clone(),
            at_unix_ms: unix_millis(now),
        });
        // Conditional on the generation this decision was made from. An
        // unconditional write would supersede whatever it found — including a
        // rival's claim written in the meantime — and both claimants would be
        // told they won. Generation ordering cannot arbitrate here, because
        // the loser's generation is also newer than what it read.
        match self
            .store
            .compare_and_set(Some(record.generation), &claimed)
            .await?
        {
            MobilityWrite::Applied => Ok(ClaimOutcome::Claimed(Box::new(claimed))),
            MobilityWrite::Superseded => Ok(ClaimOutcome::Superseded),
        }
    }

    /// Gives a claim back, leaving the sandbox available again.
    ///
    /// Only the current claimant may release. A stale actor releasing someone
    /// else's claim is exactly the resurrection the generation order exists to
    /// prevent, and it would be worse here: it would invite a second claimant
    /// in while the first is still working.
    pub async fn release(&self, sandbox_id: &SandboxId) -> Result<ReleaseOutcome> {
        let Some(record) = self.store.get(sandbox_id).await? else {
            return Ok(ReleaseOutcome::NotHeld);
        };
        // Answered before the claim check, and separately from it: a caller
        // unwinding an ambiguous commit needs to tell "somebody else has it"
        // from "we finished, and the reply is what went missing".
        if let MobilityState::Evacuated { to_node_id, .. } = &record.state {
            if to_node_id == &self.node_id {
                return Ok(ReleaseOutcome::AlreadyCommitted);
            }
        }
        if !self.holds_claim(&record) {
            return Ok(ReleaseOutcome::NotHeld);
        }
        let released = record.transitioned_to(MobilityState::Parked);
        Ok(
            match self
                .store
                .compare_and_set(Some(record.generation), &released)
                .await?
            {
                MobilityWrite::Applied => ReleaseOutcome::Released,
                MobilityWrite::Superseded => ReleaseOutcome::NotHeld,
            },
        )
    }

    /// Marks the handover finished, with this node as the new home.
    ///
    /// The error side distinguishes a write that never went out from one whose
    /// answer was lost, because only the caller can compensate and the two call
    /// for opposite responses. See [`CommitFailure`].
    pub async fn complete(&self, sandbox_id: &SandboxId) -> Result<bool, CommitFailure> {
        self.complete_at(sandbox_id, SystemTime::now()).await
    }

    async fn complete_at(
        &self,
        sandbox_id: &SandboxId,
        now: SystemTime,
    ) -> Result<bool, CommitFailure> {
        let record = match self.store.get(sandbox_id).await {
            Ok(Some(record)) => record,
            Ok(None) => return Ok(false),
            Err(error) => return Err(CommitFailure::NeverSent(error)),
        };
        if !self.holds_claim(&record) {
            return Ok(false);
        }
        let evacuated = record.transitioned_to(MobilityState::Evacuated {
            to_node_id: self.node_id.clone(),
            at_unix_ms: unix_millis(now),
        });
        // Conditional too: the commit is the point of no return, and it must
        // not overwrite a rival that took the claim while this node restored.
        match self
            .store
            .compare_and_set(Some(record.generation), &evacuated)
            .await
        {
            Ok(MobilityWrite::Applied) => Ok(true),
            Ok(MobilityWrite::Superseded) => Ok(false),
            Err(error) => Err(CommitFailure::Ambiguous(error)),
        }
    }

    /// Whether a commit this node sent actually landed.
    ///
    /// The record is the only witness left after a lost reply, and it is a
    /// sufficient one: this node's id appears in an `Evacuated` state only
    /// because this node's own `complete` put it there.
    pub async fn commit_standing(&self, sandbox_id: &SandboxId) -> Result<CommitStanding> {
        let Some(record) = self.store.get(sandbox_id).await? else {
            return Ok(CommitStanding::Lost {
                detail: "the record is gone".to_string(),
            });
        };
        Ok(match &record.state {
            MobilityState::Evacuated { to_node_id, .. } if to_node_id == &self.node_id => {
                CommitStanding::Committed
            }
            MobilityState::Evacuated { to_node_id, .. } => CommitStanding::Lost {
                detail: format!("the sandbox moved to {to_node_id}"),
            },
            MobilityState::Claimed { by_node_id, .. } if by_node_id == &self.node_id => {
                CommitStanding::StillClaimed
            }
            MobilityState::Claimed { by_node_id, .. } => CommitStanding::Lost {
                detail: format!("the claim is held by {by_node_id}"),
            },
            MobilityState::Parked => CommitStanding::Lost {
                detail: "the claim was given back".to_string(),
            },
        })
    }

    /// Hands back a sandbox this node is recorded as holding but does not have.
    ///
    /// A tombstone is terminal by design — it is what answers a late claimant
    /// with "already gone, and to whom" — which also makes a wrong one
    /// unclearable: it fences every node out of a sandbox whose origin still
    /// holds the paused state. Only the node the tombstone names may park it
    /// again, because only that node can tell "moved here and running" from
    /// "never arrived".
    pub async fn abandon_evacuation(&self, sandbox_id: &SandboxId) -> Result<bool> {
        let Some(record) = self.store.get(sandbox_id).await? else {
            return Ok(false);
        };
        let MobilityState::Evacuated { to_node_id, .. } = &record.state else {
            return Ok(false);
        };
        if to_node_id != &self.node_id {
            return Ok(false);
        }
        let parked = record.transitioned_to(MobilityState::Parked);
        Ok(matches!(
            self.store
                .compare_and_set(Some(record.generation), &parked)
                .await?,
            MobilityWrite::Applied
        ))
    }

    /// Takes the sandbox for a local resume, or explains who has it.
    ///
    /// Taking, not checking. An earlier version only read the record before
    /// resuming, and the TLA+ model in `docs/specs/MobilityClaim.tla` found
    /// the consequence in four steps: reading is not taking, so a destination
    /// claims in the gap between the read and the resume and both nodes end up
    /// live. The origin therefore goes through the same claim as everyone
    /// else, and the record has exactly one holder at a time.
    ///
    /// A sandbox with no record resumes freely: mobility is opt-in, and a node
    /// that never wrote a record for a sandbox is not in a handover.
    pub async fn claim_for_local_resume(&self, sandbox_id: &SandboxId) -> Result<ResumeFence> {
        self.claim_for_local_resume_at(sandbox_id, SystemTime::now())
            .await
    }

    async fn claim_for_local_resume_at(
        &self,
        sandbox_id: &SandboxId,
        now: SystemTime,
    ) -> Result<ResumeFence> {
        // The grace period, not the bare TTL, gates a takeover — a claimant
        // whose renewal was in flight when this ran is still inside it. That
        // is `claim_at`'s own rule, applied here by asking it.
        match self.claim_at(sandbox_id, now).await? {
            // No record: not in a handover, resume freely.
            ClaimOutcome::Unknown => Ok(ResumeFence::Allowed),
            ClaimOutcome::Claimed(_) => Ok(ResumeFence::Allowed),
            ClaimOutcome::AlreadyClaimed { by_node_id, .. } => {
                Ok(ResumeFence::ClaimedElsewhere { by_node_id })
            }
            ClaimOutcome::AlreadyEvacuated { to_node_id } => {
                Ok(ResumeFence::Evacuated { to_node_id })
            }
            // Lost a write race. Refusing is the safe answer: the caller
            // retries and gets a definitive one.
            ClaimOutcome::Superseded => Ok(ResumeFence::ClaimedElsewhere {
                by_node_id: "an unknown concurrent claimant".to_string(),
            }),
        }
    }

    fn holds_claim(&self, record: &MobilityRecord) -> bool {
        matches!(
            &record.state,
            MobilityState::Claimed { by_node_id, .. } if by_node_id == &self.node_id
        )
    }

    fn remaining_with_grace(&self, claimed_at_unix_ms: u64, now: SystemTime) -> Option<Duration> {
        self.remaining_within(claimed_at_unix_ms, now, self.claim_ttl + TAKEOVER_GRACE)
    }

    /// Remaining lifetime of a claim, or `None` once it has lapsed.
    ///
    /// A claim stamped in the future — the claimant's clock runs ahead — is
    /// treated as freshly made rather than as absurd. The alternative is to
    /// call it expired, which hands the sandbox to a second actor on the
    /// strength of a clock disagreement.
    fn remaining_within(
        &self,
        claimed_at_unix_ms: u64,
        now: SystemTime,
        window: Duration,
    ) -> Option<Duration> {
        let now_ms = unix_millis(now);
        let age = Duration::from_millis(now_ms.saturating_sub(claimed_at_unix_ms));
        window.checked_sub(age).filter(|left| !left.is_zero())
    }
}

fn unix_millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::mobility::record::LocalMobilityStore;
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;

    struct Fixture {
        _dir: tempfile::TempDir,
        store: LocalMobilityStore,
        sandbox_id: SandboxId,
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
        }
    }

    fn coordinator(
        store: LocalMobilityStore,
        node: &str,
    ) -> MobilityCoordinator<LocalMobilityStore> {
        MobilityCoordinator::new(store, node)
    }

    /// Advances the clock the coordinator is asked to reason with, rather than
    /// sleeping for it.
    fn seconds_from_now(seconds: u64) -> SystemTime {
        SystemTime::now() + Duration::from_secs(seconds)
    }

    #[tokio::test]
    async fn a_parked_sandbox_can_be_claimed_and_completed() {
        let f = fixture().await;
        let origin = coordinator(f.store.clone(), "node-a");
        let destination = coordinator(f.store.clone(), "node-b");

        assert!(matches!(
            destination.claim(&f.sandbox_id).await.expect("claim"),
            ClaimOutcome::Claimed(_)
        ));
        assert_eq!(
            origin
                .claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("fence"),
            ResumeFence::ClaimedElsewhere {
                by_node_id: "node-b".to_string()
            },
            "the origin must not resume under a live claim"
        );

        assert!(destination.complete(&f.sandbox_id).await.expect("complete"));
        assert_eq!(
            origin
                .claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("fence"),
            ResumeFence::Evacuated {
                to_node_id: "node-b".to_string()
            }
        );
    }

    /// A second destination must be told it is racing rather than allowed to
    /// take over a sandbox someone else is restoring.
    #[tokio::test]
    async fn a_live_claim_blocks_another_destination() {
        let f = fixture().await;
        coordinator(f.store.clone(), "node-b")
            .claim(&f.sandbox_id)
            .await
            .expect("claim");

        let outcome = coordinator(f.store.clone(), "node-c")
            .claim(&f.sandbox_id)
            .await
            .expect("claim");
        match outcome {
            ClaimOutcome::AlreadyClaimed { by_node_id, .. } => assert_eq!(by_node_id, "node-b"),
            other => panic!("expected a live claim to block, got {other:?}"),
        }
    }

    /// Renewal is the same call as claiming, so a claimant that timed out on
    /// its own earlier attempt does not have to know whether it landed.
    #[tokio::test]
    async fn a_claimant_renews_with_the_same_call() {
        let f = fixture().await;
        let destination = coordinator(f.store.clone(), "node-b");

        let first = destination.claim(&f.sandbox_id).await.expect("claim");
        let second = destination.claim(&f.sandbox_id).await.expect("renew");
        assert!(matches!(first, ClaimOutcome::Claimed(_)));
        assert!(matches!(second, ClaimOutcome::Claimed(_)));

        let ClaimOutcome::Claimed(second) = second else {
            unreachable!("asserted above");
        };
        let ClaimOutcome::Claimed(first) = first else {
            unreachable!("asserted above");
        };
        assert!(
            second.generation.supersedes(&first.generation),
            "a renewal must advance the generation"
        );
    }

    /// Expiry means the claimant stopped renewing. Another destination taking
    /// over then is the entire purpose of the lease.
    #[tokio::test]
    async fn an_expired_claim_can_be_taken_over() {
        let f = fixture().await;
        let abandoned =
            coordinator(f.store.clone(), "node-b").with_claim_ttl(Duration::from_secs(1));
        abandoned.claim(&f.sandbox_id).await.expect("claim");

        let taker = coordinator(f.store.clone(), "node-c").with_claim_ttl(Duration::from_secs(1));
        let outcome = taker
            .claim_at(
                &f.sandbox_id,
                seconds_from_now(1 + TAKEOVER_GRACE.as_secs() + 1),
            )
            .await
            .expect("claim");
        assert!(
            matches!(outcome, ClaimOutcome::Claimed(_)),
            "an abandoned claim must not park a sandbox forever, got {outcome:?}"
        );
    }

    /// The window between claim expiry and a takeover is the one place two
    /// live copies could appear. The grace period must keep a rival out of it
    /// even after the TTL has passed.
    #[tokio::test]
    async fn a_rival_waits_out_the_grace_period_before_taking_over() {
        let f = fixture().await;
        let ttl = Duration::from_secs(1);
        coordinator(f.store.clone(), "node-b")
            .with_claim_ttl(ttl)
            .claim(&f.sandbox_id)
            .await
            .expect("claim");
        let origin = coordinator(f.store.clone(), "node-a").with_claim_ttl(ttl);

        // Past the TTL but inside the grace period: still fenced.
        assert!(matches!(
            origin
                .claim_for_local_resume_at(&f.sandbox_id, seconds_from_now(ttl.as_secs() + 1))
                .await
                .expect("fence"),
            ResumeFence::ClaimedElsewhere { .. }
        ));

        // Past both: the claimant has stopped renewing for long enough that a
        // renewal cannot still be in flight.
        assert_eq!(
            origin
                .claim_for_local_resume_at(
                    &f.sandbox_id,
                    seconds_from_now(ttl.as_secs() + TAKEOVER_GRACE.as_secs() + 1)
                )
                .await
                .expect("fence"),
            ResumeFence::Allowed
        );
    }

    /// A claimant whose clock runs ahead stamps a claim in the future. Reading
    /// that as expired would hand the sandbox to a second actor purely because
    /// two machines disagree about the time.
    #[tokio::test]
    async fn a_claim_stamped_in_the_future_is_treated_as_fresh() {
        let f = fixture().await;
        let destination = coordinator(f.store.clone(), "node-b");
        destination
            .claim_at(&f.sandbox_id, SystemTime::now() + Duration::from_secs(600))
            .await
            .expect("claim");

        let other = coordinator(f.store.clone(), "node-c");
        assert!(matches!(
            other.claim(&f.sandbox_id).await.expect("claim"),
            ClaimOutcome::AlreadyClaimed { .. }
        ));
    }

    /// Releasing someone else's claim would invite a second destination in
    /// while the first is still restoring.
    #[tokio::test]
    async fn only_the_claimant_may_release_or_complete() {
        let f = fixture().await;
        coordinator(f.store.clone(), "node-b")
            .claim(&f.sandbox_id)
            .await
            .expect("claim");

        let interloper = coordinator(f.store.clone(), "node-c");
        assert_eq!(
            interloper.release(&f.sandbox_id).await.expect("release"),
            ReleaseOutcome::NotHeld
        );
        assert!(!interloper.complete(&f.sandbox_id).await.expect("complete"));

        let claimant = coordinator(f.store.clone(), "node-b");
        assert_eq!(
            claimant.release(&f.sandbox_id).await.expect("release"),
            ReleaseOutcome::Released
        );
        assert_eq!(
            coordinator(f.store.clone(), "node-a")
                .claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("fence"),
            ResumeFence::Allowed,
            "a released sandbox is available again"
        );
    }

    /// A release that finds this node's own completed handover must say so
    /// rather than report "not ours". The caller asking is one unwinding an
    /// ambiguous commit, and the answer decides whether the sandbox moved.
    #[tokio::test]
    async fn releasing_a_completed_handover_is_reported_rather_than_undone() {
        let f = fixture().await;
        let destination = coordinator(f.store.clone(), "node-b");
        destination.claim(&f.sandbox_id).await.expect("claim");
        assert!(destination.complete(&f.sandbox_id).await.expect("complete"));

        assert_eq!(
            destination.release(&f.sandbox_id).await.expect("release"),
            ReleaseOutcome::AlreadyCommitted
        );
        assert!(
            matches!(
                f.store.get(&f.sandbox_id).await.expect("get").expect("record").state,
                MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
            ),
            "a release must not undo a commit that stands"
        );
    }

    /// Only the node a tombstone names may park it again. Anyone else doing so
    /// would resurrect a sandbox that really did move.
    #[tokio::test]
    async fn only_the_named_destination_may_park_its_own_tombstone() {
        let f = fixture().await;
        let destination = coordinator(f.store.clone(), "node-b");
        destination.claim(&f.sandbox_id).await.expect("claim");
        destination.complete(&f.sandbox_id).await.expect("complete");

        assert!(
            !coordinator(f.store.clone(), "node-c")
                .abandon_evacuation(&f.sandbox_id)
                .await
                .expect("abandon"),
            "a bystander must not clear a tombstone naming another node"
        );
        assert!(matches!(
            f.store
                .get(&f.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Evacuated { .. }
        ));

        assert!(destination
            .abandon_evacuation(&f.sandbox_id)
            .await
            .expect("abandon"));
        assert_eq!(
            coordinator(f.store.clone(), "node-a")
                .claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("fence"),
            ResumeFence::Allowed,
            "the origin still holds the paused state and must be able to reclaim it"
        );
        assert!(
            matches!(
                f.store
                    .get(&f.sandbox_id)
                    .await
                    .expect("get")
                    .expect("record")
                    .state,
                MobilityState::Claimed { ref by_node_id, .. } if by_node_id == "node-a"
            ),
            "resuming locally must take the record, not just read it"
        );
    }

    /// The defect the TLA+ model found: a read-only fence lets a destination
    /// claim in the gap between the origin's check and its resume, and both
    /// end up live. Exactly one of the two must succeed.
    #[tokio::test]
    async fn a_local_resume_and_a_remote_claim_cannot_both_win() {
        let f = fixture().await;
        let origin = coordinator(f.store.clone(), "node-a");
        let destination = coordinator(f.store.clone(), "node-b");

        let resumed = origin
            .claim_for_local_resume(&f.sandbox_id)
            .await
            .expect("resume");
        let claimed = destination.claim(&f.sandbox_id).await.expect("claim");

        assert_eq!(resumed, ResumeFence::Allowed, "the origin got there first");
        match claimed {
            ClaimOutcome::AlreadyClaimed { by_node_id, .. } => assert_eq!(by_node_id, "node-a"),
            other => panic!("the destination must lose the race, got {other:?}"),
        }
    }

    /// And the other way round.
    #[tokio::test]
    async fn a_remote_claim_blocks_a_local_resume() {
        let f = fixture().await;
        coordinator(f.store.clone(), "node-b")
            .claim(&f.sandbox_id)
            .await
            .expect("claim");

        assert_eq!(
            coordinator(f.store.clone(), "node-a")
                .claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("resume"),
            ResumeFence::ClaimedElsewhere {
                by_node_id: "node-b".to_string()
            }
        );
    }

    /// A sandbox that already moved must never be claimed again, expired
    /// claims or not: the state it refers to is being run elsewhere.
    #[tokio::test]
    async fn an_evacuated_sandbox_cannot_be_reclaimed() {
        let f = fixture().await;
        let destination = coordinator(f.store.clone(), "node-b");
        destination.claim(&f.sandbox_id).await.expect("claim");
        destination.complete(&f.sandbox_id).await.expect("complete");

        assert_eq!(
            coordinator(f.store.clone(), "node-c")
                .claim(&f.sandbox_id)
                .await
                .expect("claim"),
            ClaimOutcome::AlreadyEvacuated {
                to_node_id: "node-b".to_string()
            }
        );
    }

    /// Mobility is opt-in. A sandbox with no record is not in a handover and
    /// must resume normally rather than being fenced by an absent decision.
    #[tokio::test]
    async fn a_sandbox_without_a_record_resumes_freely() {
        let f = fixture().await;
        let unknown = SandboxId::new();
        let origin = coordinator(f.store.clone(), "node-a");

        assert_eq!(
            origin
                .claim_for_local_resume(&unknown)
                .await
                .expect("fence"),
            ResumeFence::Allowed
        );
        assert_eq!(
            origin.claim(&unknown).await.expect("claim"),
            ClaimOutcome::Unknown
        );
    }

    /// A node restoring its own paused sandbox holds its own claim. Fencing
    /// against yourself would deadlock the ordinary local resume path.
    #[tokio::test]
    async fn a_node_is_not_fenced_by_its_own_claim() {
        let f = fixture().await;
        let node = coordinator(f.store.clone(), "node-a");
        node.claim(&f.sandbox_id).await.expect("claim");

        assert_eq!(
            node.claim_for_local_resume(&f.sandbox_id)
                .await
                .expect("fence"),
            ResumeFence::Allowed
        );
    }
}

#[cfg(test)]
mod arbitration_tests {
    use super::*;
    use crate::orchestrator::mobility::record::{LocalMobilityStore, MobilityRecord};
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;

    async fn parked() -> (LocalMobilityStore, SandboxId, tempfile::TempDir) {
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
        store
            .upsert(&MobilityRecord::for_paused(
                &metadata,
                "node-a",
                "x86_64",
                Some("{}".to_string()),
                4096,
                ArtifactReach::ClusterShared,
                Some("snap-1".to_string()),
            ))
            .await
            .expect("seed");
        (store, metadata.id, dir)
    }

    /// The property the whole protocol exists for. Claiming used to be a read
    /// followed by an unconditional write: every claimant minted a newer
    /// generation, so every claimant's write superseded what it read and all
    /// of them were told they won. Two winners means two nodes restore one
    /// sandbox and both write its drives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_claimants_produce_exactly_one_winner() {
        let (store, sandbox_id, _dir) = parked().await;

        let mut handles = Vec::new();
        for index in 0..24 {
            let coordinator = MobilityCoordinator::new(store.clone(), format!("node-{index}"));
            handles.push(tokio::spawn(async move {
                matches!(
                    coordinator.claim(&sandbox_id).await.expect("claim"),
                    ClaimOutcome::Claimed(_)
                )
            }));
        }

        let mut winners = 0;
        for handle in handles {
            if handle.await.expect("join") {
                winners += 1;
            }
        }
        assert_eq!(
            winners, 1,
            "exactly one claimant may win; {winners} did, which is two nodes running one sandbox"
        );

        // And the store agrees with whoever that was.
        let stored = store.get(&sandbox_id).await.expect("get").expect("record");
        assert!(
            matches!(stored.state, MobilityState::Claimed { .. }),
            "the winning claim must be the stored state, got {:?}",
            stored.state
        );
    }

    /// The catastrophic pairing: the origin resuming locally while a
    /// destination claims. Both used to succeed, and only the destination had
    /// a lease guardian that would ever find out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_local_resume_racing_a_remote_claim_has_one_winner() {
        for _ in 0..24 {
            let (store, sandbox_id, _dir) = parked().await;
            let origin = MobilityCoordinator::new(store.clone(), "node-a");
            let destination = MobilityCoordinator::new(store.clone(), "node-b");

            let resume = tokio::spawn(async move {
                origin
                    .claim_for_local_resume(&sandbox_id)
                    .await
                    .expect("resume")
            });
            let claim =
                tokio::spawn(async move { destination.claim(&sandbox_id).await.expect("claim") });

            let resumed = matches!(resume.await.expect("join"), ResumeFence::Allowed);
            let claimed = matches!(claim.await.expect("join"), ClaimOutcome::Claimed(_));
            assert!(
                !(resumed && claimed),
                "the origin resumed and a destination claimed the same sandbox"
            );
        }
    }

    /// A stale expectation must lose. This is the mechanism the two tests
    /// above rely on, asserted directly so a regression names itself.
    #[tokio::test]
    async fn a_write_conditional_on_a_stale_generation_is_refused() {
        let (store, sandbox_id, _dir) = parked().await;
        let original = store.get(&sandbox_id).await.expect("get").expect("record");

        let first = original.transitioned_to(MobilityState::Claimed {
            by_node_id: "node-b".to_string(),
            at_unix_ms: 1,
        });
        assert_eq!(
            store
                .compare_and_set(Some(original.generation), &first)
                .await
                .expect("first"),
            MobilityWrite::Applied
        );

        // A rival that read the same original state now writes with the
        // expectation it formed then.
        let second = original.transitioned_to(MobilityState::Claimed {
            by_node_id: "node-c".to_string(),
            at_unix_ms: 2,
        });
        assert_eq!(
            store
                .compare_and_set(Some(original.generation), &second)
                .await
                .expect("second"),
            MobilityWrite::Superseded,
            "a stale expectation must lose even though its generation is newer"
        );

        let stored = store.get(&sandbox_id).await.expect("get").expect("record");
        assert!(
            matches!(stored.state, MobilityState::Claimed { ref by_node_id, .. } if by_node_id == "node-b"),
            "the first writer must still hold it, got {:?}",
            stored.state
        );
    }

    /// Expecting absence is distinct from not checking, and a record that
    /// appeared in between must make the write lose.
    #[tokio::test]
    async fn expecting_absence_loses_once_a_record_exists() {
        let (store, sandbox_id, _dir) = parked().await;
        let existing = store.get(&sandbox_id).await.expect("get").expect("record");
        let replacement = existing.transitioned_to(MobilityState::Parked);

        assert_eq!(
            store
                .compare_and_set(None, &replacement)
                .await
                .expect("cas"),
            MobilityWrite::Superseded,
            "expecting no record must fail when one exists"
        );
    }
}
