//! Emptying a node without emptying it into a wall.
//!
//! Draining a node is not "migrate everything". Most of the paused sandboxes
//! on a node cannot go to most of its neighbours — the runtime has to match,
//! the artifacts have to be readable, and the destination has to have room —
//! and a drain that discovers this one sandbox at a time spends its time
//! claiming sandboxes it then has to release.
//!
//! So planning is separate from executing. The planner answers "where does
//! each of these go, and which ones have nowhere to go" against a snapshot of
//! the fleet, all at once and without touching anything. The executor then
//! walks that plan under a concurrency cap and a failure budget.
//!
//! # Placement order
//!
//! Sandboxes are placed largest-memory-first. Placing the small ones first
//! fills every destination's slack with things that would have fitted
//! anywhere, and strands the large ones — the classic first-fit-decreasing
//! result, and the failure is worse here than in a packing benchmark because
//! a stranded sandbox is one that has to stay on a node being drained.
//!
//! Among compatible destinations, the one with the most room left wins. A
//! drain that packs destinations tight converts one node's problem into
//! several nodes' problems, and the next drain has nowhere to go at all.
//!
//! # Stopping a move
//!
//! A move that overruns is asked to stop; it is never dropped. A migration
//! holds a claim and a half-built guest on the destination and gives them up
//! in a fixed order, and a dropped future runs none of that — see
//! [`MoveCancel`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use tokio::sync::watch;
use tracing::{info, warn};

use super::record::{MobilityRecord, MobilityState};
use crate::snapshot::{
    DriveForMigration, MigrationFingerprint, MobilityBlocker, OverlaybdLayerRef,
};
use crate::types::{SandboxId, SandboxResources};

/// A node that could take sandboxes off the one being drained.
#[derive(Clone, Debug)]
pub struct DestinationCandidate {
    pub node_id: String,
    pub fingerprint: MigrationFingerprint,
    /// Capacity the destination has said it can give away, not its total.
    pub free_cpu: u32,
    pub free_memory_mib: u32,
}

impl DestinationCandidate {
    fn fits(&self, resources: &SandboxResources) -> bool {
        self.free_cpu >= resources.cpu_count && self.free_memory_mib >= resources.memory_mib
    }

    fn reserve(&mut self, resources: &SandboxResources) {
        self.free_cpu = self.free_cpu.saturating_sub(resources.cpu_count);
        self.free_memory_mib = self.free_memory_mib.saturating_sub(resources.memory_mib);
    }
}

/// One sandbox's destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedMove {
    pub sandbox_id: SandboxId,
    pub to_node_id: String,
    pub resources: SandboxResources,
}

/// Why a sandbox has nowhere to go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnplaceableReason {
    /// Mid-handover or already gone. Not this drain's problem.
    NotParked,
    /// No candidate can run this guest, with one representative reason.
    ///
    /// One rather than all: the reasons are almost always the same reason
    /// repeated per node, and a list of forty identical kernel mismatches
    /// buries the one node that differed.
    NoCompatibleNode {
        candidates_considered: usize,
        example: MobilityBlocker,
    },
    /// Compatible nodes exist, but none had room once earlier sandboxes in
    /// this same plan were accounted for.
    NoCapacity { compatible_nodes: usize },
    /// There were no candidates at all.
    NoCandidates,
}

impl UnplaceableReason {
    /// Stable label for metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NotParked => "not_parked",
            Self::NoCompatibleNode { .. } => "no_compatible_node",
            Self::NoCapacity { .. } => "no_capacity",
            Self::NoCandidates => "no_candidates",
        }
    }
}

/// A sandbox the plan could not place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnplaceableSandbox {
    pub sandbox_id: SandboxId,
    pub reason: UnplaceableReason,
}

/// Where every paused sandbox on a node would go.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvacuationPlan {
    pub moves: Vec<PlannedMove>,
    /// Reported rather than dropped: a drain that silently leaves sandboxes
    /// behind reads as a completed drain.
    pub unplaceable: Vec<UnplaceableSandbox>,
}

impl EvacuationPlan {
    /// Whether the node can actually be emptied by this plan.
    pub fn is_complete(&self) -> bool {
        self.unplaceable.is_empty()
    }
}

/// What each sandbox's drives look like, for the compatibility check.
///
/// Supplied by the caller because layer sets come from committed snapshots,
/// which the planner has no business loading.
#[derive(Default)]
pub struct SandboxLayers<'a> {
    pub rootfs: &'a [OverlaybdLayerRef],
    pub attached_drives: &'a [DriveForMigration<'a>],
}

/// Plans where each paused sandbox on this node should go.
///
/// Pure: it reserves capacity within the plan it is building, and touches
/// neither the store nor the fleet.
pub fn plan_evacuation(
    records: &[MobilityRecord],
    candidates: &[DestinationCandidate],
    layers: &HashMap<SandboxId, SandboxLayers<'_>>,
) -> EvacuationPlan {
    let mut remaining: Vec<DestinationCandidate> = candidates.to_vec();
    let mut plan = EvacuationPlan::default();

    let mut movable: Vec<&MobilityRecord> = Vec::new();
    for record in records {
        if record.state == MobilityState::Parked {
            movable.push(record);
        } else {
            plan.unplaceable.push(UnplaceableSandbox {
                sandbox_id: record.sandbox_id,
                reason: UnplaceableReason::NotParked,
            });
        }
    }

    // Largest first, then by id so a plan is reproducible for the same input.
    movable.sort_by(|left, right| {
        right
            .resources
            .memory_mib
            .cmp(&left.resources.memory_mib)
            .then_with(|| left.sandbox_id.cmp(&right.sandbox_id))
    });

    let empty = SandboxLayers::default();
    for record in movable {
        let sandbox_layers = layers.get(&record.sandbox_id).unwrap_or(&empty);
        match choose_destination(record, &mut remaining, sandbox_layers) {
            Ok(to_node_id) => plan.moves.push(PlannedMove {
                sandbox_id: record.sandbox_id,
                to_node_id,
                resources: record.resources,
            }),
            Err(reason) => plan.unplaceable.push(UnplaceableSandbox {
                sandbox_id: record.sandbox_id,
                reason,
            }),
        }
    }

    plan
}

/// Picks the most-free compatible destination and reserves its capacity.
fn choose_destination(
    record: &MobilityRecord,
    candidates: &mut [DestinationCandidate],
    layers: &SandboxLayers<'_>,
) -> Result<String, UnplaceableReason> {
    if candidates.is_empty() {
        return Err(UnplaceableReason::NoCandidates);
    }

    let mut compatible = 0_usize;
    let mut first_blocker: Option<MobilityBlocker> = None;
    let mut best: Option<usize> = None;

    for (index, candidate) in candidates.iter().enumerate() {
        match record.can_move_to(
            &candidate.fingerprint,
            layers.rootfs,
            layers.attached_drives,
        ) {
            Ok(()) => {}
            Err(blocker) => {
                first_blocker.get_or_insert(blocker);
                continue;
            }
        }
        compatible += 1;
        if !candidate.fits(&record.resources) {
            continue;
        }
        let better = match best {
            None => true,
            Some(current) => {
                candidate.free_memory_mib > candidates[current].free_memory_mib
                    || (candidate.free_memory_mib == candidates[current].free_memory_mib
                        && candidate.node_id < candidates[current].node_id)
            }
        };
        if better {
            best = Some(index);
        }
    }

    match best {
        Some(index) => {
            candidates[index].reserve(&record.resources);
            Ok(candidates[index].node_id.clone())
        }
        None if compatible > 0 => Err(UnplaceableReason::NoCapacity {
            compatible_nodes: compatible,
        }),
        None => Err(UnplaceableReason::NoCompatibleNode {
            candidates_considered: candidates.len(),
            example: first_blocker.expect("an incompatible candidate produced a blocker"),
        }),
    }
}

/// A request to stop a move, which the move itself can see.
///
/// A migration owns a claim and a half-restored guest on the destination, and
/// it gives them up in a fixed order so that every failure leaves exactly one
/// node owning the sandbox. Dropping its future — which is what
/// `tokio::time::timeout` does when it elapses — stops it at whichever await
/// it was suspended at and runs none of that: the partial restore stays up on
/// the destination, the claim is never released, and once the claim lapses the
/// origin resumes a sandbox the destination is still holding open. Two live
/// copies, arrived at by a timeout rather than by any step failing.
///
/// So a drain asks, and the move unwinds itself through its own compensations.
#[derive(Clone, Debug)]
pub struct MoveCancel {
    requested: Arc<watch::Sender<bool>>,
}

impl MoveCancel {
    pub fn new() -> Self {
        Self {
            requested: Arc::new(watch::channel(false).0),
        }
    }

    /// Asks the move to stop. Idempotent, and cheap enough to call on a move
    /// that has already finished.
    pub fn request(&self) {
        self.requested.send_replace(true);
    }

    /// Whether a stop has already been asked for.
    pub fn is_requested(&self) -> bool {
        *self.requested.borrow()
    }

    /// Resolves once a stop has been asked for, and stays resolved.
    ///
    /// Meant for an arm of a `select!` inside the move, alongside the work
    /// that has to be unwound.
    pub async fn requested(&self) {
        let mut rx = self.requested.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            // The sender lives in this handle, so the only way `changed`
            // fails is a shutdown that has already taken the move with it.
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for MoveCancel {
    fn default() -> Self {
        Self::new()
    }
}

/// Performs one planned move.
///
/// A migration is driven by the destination, so an implementation here is
/// whatever asks the destination to claim and restore. It is a trait so the
/// drain's bounds can be exercised without a fleet.
///
/// An implementation must watch `cancel` at every point it can still unwind
/// from, and must not treat it as a licence to return early with work half
/// done: the drain stops waiting for a move that overruns, so an executor
/// that ignores the request leaves the sandbox owned by nobody.
#[async_trait::async_trait]
pub trait MoveExecutor: Send + Sync {
    async fn execute(&self, planned: &PlannedMove, cancel: &MoveCancel) -> anyhow::Result<()>;
}

/// How hard a drain is allowed to push.
#[derive(Clone, Copy, Debug)]
pub struct DrainBudget {
    /// Migrations in flight at once.
    ///
    /// A drain is competing with live traffic on both ends, and each move is
    /// a memory image crossing the network. Unbounded parallelism turns an
    /// orderly drain into the incident it was meant to avoid.
    pub max_concurrent: usize,
    /// Failures tolerated before the drain stops.
    ///
    /// A drain that keeps going through a systematic failure — an
    /// unreachable repository, a fleet-wide version skew — burns the whole
    /// plan discovering the same thing once per sandbox.
    pub max_failures: usize,
    /// Ceiling on one move, after which it is asked to stop.
    pub move_timeout: Duration,
    /// How long a move gets to unwind once it has been asked to stop.
    ///
    /// Past this the drain stops waiting, but it still does not sever the
    /// move: an unwind that is merely slow is left to finish, because the
    /// alternative is dropping a migration between its compensations.
    pub unwind_grace: Duration,
}

impl Default for DrainBudget {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_failures: 3,
            move_timeout: Duration::from_secs(300),
            // An unwind is a teardown and two store writes, so this is
            // generous. It is a bound on a failing path, not a target.
            unwind_grace: Duration::from_secs(30),
        }
    }
}

/// What a drain actually managed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    pub migrated: Vec<SandboxId>,
    pub failed: Vec<(SandboxId, String)>,
    /// Moves the drain never attempted, because the failure budget ran out.
    ///
    /// Distinct from failures: these say nothing about whether they would have
    /// worked, and reporting them as failures would send an operator chasing
    /// sandboxes that were merely skipped.
    pub not_attempted: Vec<SandboxId>,
    pub stopped_early: bool,
}

/// Runs one move, bounded but never severed.
///
/// The move runs on its own task so that the drain giving up on it is a
/// decision to stop *waiting*. Dropping the future instead would cancel the
/// migration wherever it happened to be suspended, which skips the
/// compensations that make a failed migration leave exactly one owner.
async fn run_move(
    executor: Arc<dyn MoveExecutor>,
    planned: PlannedMove,
    cancel: MoveCancel,
    budget: DrainBudget,
) -> anyhow::Result<()> {
    let sandbox_id = planned.sandbox_id;
    let mut task = tokio::spawn({
        let cancel = cancel.clone();
        async move { executor.execute(&planned, &cancel).await }
    });

    // Both awaits take the handle by reference: a `JoinHandle` dropped at the
    // end of this function detaches its task rather than aborting it, and a
    // move still unwinding then gets to finish.
    let joined = match tokio::time::timeout(budget.move_timeout, &mut task).await {
        Ok(joined) => joined,
        Err(_) => {
            cancel.request();
            return match tokio::time::timeout(budget.unwind_grace, &mut task).await {
                // The move was past its point of no return when it was asked
                // to stop, and finished the handover instead of unwinding —
                // which is what a migration must do once the guest is live
                // elsewhere. Reporting a failure here would tell an operator
                // the origin still owns a sandbox that has moved, and the
                // record would say otherwise.
                Ok(Ok(Ok(()))) => {
                    warn!(
                        %sandbox_id,
                        "the move overran {:?} but had already committed; counting it as migrated",
                        budget.move_timeout
                    );
                    Ok(())
                }
                // A genuine unwind. Reported as a timeout rather than as
                // whatever the compensation returned, because what the drain
                // knows is that it stopped the move, not why it was slow.
                Ok(Ok(Err(_))) => Err(anyhow::anyhow!(
                    "move timed out after {:?} and was unwound",
                    budget.move_timeout
                )),
                Ok(Err(error)) => Err(anyhow::anyhow!("the move panicked: {error}")),
                Err(_) => Err(anyhow::anyhow!(
                    "move timed out after {:?} and had not unwound {:?} later; it is still running",
                    budget.move_timeout,
                    budget.unwind_grace
                )),
            };
        }
    };

    joined.unwrap_or_else(|error| Err(anyhow::anyhow!("the move panicked: {error}")))
}

/// Executes a plan under `budget`.
pub async fn drain(
    plan: &EvacuationPlan,
    executor: Arc<dyn MoveExecutor>,
    budget: DrainBudget,
) -> DrainReport {
    let mut report = DrainReport::default();
    let concurrency = budget.max_concurrent.max(1);
    let cancels: Vec<MoveCancel> = plan.moves.iter().map(|_| MoveCancel::new()).collect();

    let mut results = stream::iter(plan.moves.iter().enumerate())
        .map(|(index, planned)| {
            let executor = Arc::clone(&executor);
            let cancel = cancels[index].clone();
            let sandbox_id = planned.sandbox_id;
            let planned = planned.clone();
            async move {
                let outcome = run_move(executor, planned, cancel, budget).await;
                (index, sandbox_id, outcome)
            }
        })
        .buffer_unordered(concurrency);

    let mut attempted = vec![false; plan.moves.len()];
    while let Some((index, sandbox_id, outcome)) = results.next().await {
        attempted[index] = true;
        match outcome {
            Ok(()) => report.migrated.push(sandbox_id),
            Err(error) => {
                warn!(%sandbox_id, error = %error, "planned move failed");
                report.failed.push((sandbox_id, error.to_string()));
            }
        }
        if report.failed.len() > budget.max_failures {
            report.stopped_early = true;
            break;
        }
    }

    drop(results);

    if report.stopped_early {
        // The moves still in flight are asked to stop for the same reason a
        // move that overran its ceiling is: the drain is no longer waiting on
        // them, and a migration nobody is waiting for still has to put the
        // sandbox back somewhere. Requests to moves that already finished, or
        // that never started, do nothing.
        for (cancel, attempted) in cancels.iter().zip(attempted.iter()) {
            if !*attempted {
                cancel.request();
            }
        }

        // Everything the stream had not yet delivered is unattempted. Moves
        // still in flight when the budget ran out are counted here too: the
        // drain has stopped waiting on them, so claiming either result would
        // be a guess.
        report.not_attempted = plan
            .moves
            .iter()
            .zip(attempted)
            .filter(|(_, attempted)| !attempted)
            .map(|(planned, _)| planned.sandbox_id)
            .collect();
        warn!(
            migrated = report.migrated.len(),
            failed = report.failed.len(),
            not_attempted = report.not_attempted.len(),
            "drain stopped early after exhausting its failure budget"
        );
    } else {
        info!(
            migrated = report.migrated.len(),
            failed = report.failed.len(),
            "drain finished"
        );
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::{ArtifactReach, ManagedLayer, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;
    use std::sync::Mutex;

    pub(super) fn fingerprint(kernel: &str) -> MigrationFingerprint {
        MigrationFingerprint::from_runtime(
            &SnapshotRuntimeVersions {
                kernel_version: kernel.to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            "x86_64",
            VirtualizationMode::Kvm,
            Some("{}".to_string()),
            4096,
        )
    }

    pub(super) fn record(memory_mib: u32, kernel: &str) -> MobilityRecord {
        let metadata = SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: kernel.to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            resources: SandboxResources {
                cpu_count: 1,
                memory_mib,
                disk_size_mib: 1024,
            },
            ..SandboxMetadata::default()
        };
        MobilityRecord::for_paused(
            &metadata,
            "node-a",
            "x86_64",
            Some("{}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        )
    }

    fn candidate(node_id: &str, free_memory_mib: u32, kernel: &str) -> DestinationCandidate {
        DestinationCandidate {
            node_id: node_id.to_string(),
            fingerprint: fingerprint(kernel),
            free_cpu: 64,
            free_memory_mib,
        }
    }

    fn no_layers() -> HashMap<SandboxId, SandboxLayers<'static>> {
        HashMap::new()
    }

    /// Placing the small sandboxes first fills the slack that the large ones
    /// need. Both fit here only if the 8 GiB one is placed before the 2 GiB one.
    #[test]
    fn the_largest_sandbox_is_placed_first() {
        let small = record(2048, "k1");
        let large = record(8192, "k1");
        let plan = plan_evacuation(
            &[small.clone(), large.clone()],
            &[
                candidate("node-b", 8192, "k1"),
                candidate("node-c", 4096, "k1"),
            ],
            &no_layers(),
        );

        assert!(plan.is_complete(), "both should be placeable: {plan:?}");
        let destination = |id: SandboxId| {
            plan.moves
                .iter()
                .find(|planned| planned.sandbox_id == id)
                .map(|planned| planned.to_node_id.clone())
                .expect("planned")
        };
        assert_eq!(destination(large.sandbox_id), "node-b");
        assert_eq!(destination(small.sandbox_id), "node-c");
    }

    /// Packing destinations tight turns one node's problem into several, so
    /// each sandbox goes to whichever destination has the most room *left*.
    /// node-c starts emptier and takes the first; that drops it below node-b,
    /// which then takes the second.
    #[test]
    fn placement_follows_whichever_destination_has_the_most_room_left() {
        let plan = plan_evacuation(
            &[record(2048, "k1"), record(2048, "k1")],
            &[
                candidate("node-b", 4096, "k1"),
                candidate("node-c", 5120, "k1"),
            ],
            &no_layers(),
        );

        assert_eq!(plan.moves.len(), 2);
        let mut destinations: Vec<&str> = plan
            .moves
            .iter()
            .map(|planned| planned.to_node_id.as_str())
            .collect();
        destinations.sort_unstable();
        assert_eq!(
            destinations,
            vec!["node-b", "node-c"],
            "the second sandbox should follow the reservation, not repeat the first choice"
        );
    }

    /// The rule itself, rather than its consequence for a second sandbox: of
    /// two compatible destinations the emptier one wins.
    ///
    /// Scanned both ways round because one order proves nothing. The scan
    /// keeps a running best, so an inverted comparison still lands on the
    /// right node whenever that node happened to be seen first.
    #[test]
    fn the_emptiest_compatible_destination_wins_whichever_order_it_is_scanned_in() {
        for candidates in [
            vec![
                candidate("node-b", 4096, "k1"),
                candidate("node-c", 8192, "k1"),
            ],
            vec![
                candidate("node-c", 8192, "k1"),
                candidate("node-b", 4096, "k1"),
            ],
        ] {
            let order: Vec<&str> = candidates
                .iter()
                .map(|candidate| candidate.node_id.as_str())
                .collect();
            let plan = plan_evacuation(&[record(1024, "k1")], &candidates, &no_layers());

            assert_eq!(plan.moves.len(), 1, "scanned as {order:?}");
            assert_eq!(
                plan.moves[0].to_node_id, "node-c",
                "the emptier node must win, scanned as {order:?}"
            );
        }
    }

    /// Equal room is broken by the lower node id, so the same fleet plans the
    /// same way however it was listed.
    ///
    /// Both orders again, and for a sharper reason: a comparison that lets an
    /// equally-empty candidate displace the incumbent gives the sandbox to
    /// whichever node was scanned last, which is the right answer in exactly
    /// one of these two orders.
    #[test]
    fn destinations_with_equal_room_are_broken_by_the_lower_node_id() {
        for candidates in [
            vec![
                candidate("node-b", 8192, "k1"),
                candidate("node-c", 8192, "k1"),
            ],
            vec![
                candidate("node-c", 8192, "k1"),
                candidate("node-b", 8192, "k1"),
            ],
        ] {
            let order: Vec<&str> = candidates
                .iter()
                .map(|candidate| candidate.node_id.as_str())
                .collect();
            let plan = plan_evacuation(&[record(1024, "k1")], &candidates, &no_layers());

            assert_eq!(plan.moves.len(), 1, "scanned as {order:?}");
            assert_eq!(
                plan.moves[0].to_node_id, "node-b",
                "a tie must go to the lower id, scanned as {order:?}"
            );
        }
    }

    /// The plan must not over-commit: reserving as it goes is the difference
    /// between a plan and a wish list.
    #[test]
    fn capacity_is_reserved_within_the_plan() {
        let records: Vec<MobilityRecord> = (0..3).map(|_| record(4096, "k1")).collect();
        let plan = plan_evacuation(&records, &[candidate("node-b", 8192, "k1")], &no_layers());

        assert_eq!(plan.moves.len(), 2, "only two 4 GiB sandboxes fit in 8 GiB");
        assert_eq!(plan.unplaceable.len(), 1);
        assert_eq!(plan.unplaceable[0].reason.kind(), "no_capacity");
    }

    /// "No compatible node" and "no room" call for completely different
    /// responses, so the plan must not blur them.
    #[test]
    fn incompatibility_and_exhaustion_are_reported_differently() {
        let plan = plan_evacuation(
            &[record(1024, "k1")],
            &[candidate("node-b", 8192, "k2")],
            &no_layers(),
        );
        match &plan.unplaceable[0].reason {
            UnplaceableReason::NoCompatibleNode {
                candidates_considered,
                example,
            } => {
                assert_eq!(*candidates_considered, 1);
                assert_eq!(example.kind(), "kernel_version");
            }
            other => panic!("expected an incompatibility, got {other:?}"),
        }

        let plan = plan_evacuation(
            &[record(16384, "k1")],
            &[candidate("node-b", 1024, "k1")],
            &no_layers(),
        );
        assert_eq!(
            plan.unplaceable[0].reason,
            UnplaceableReason::NoCapacity {
                compatible_nodes: 1
            }
        );
    }

    #[test]
    fn a_node_with_no_neighbours_cannot_be_drained() {
        let plan = plan_evacuation(&[record(1024, "k1")], &[], &no_layers());
        assert!(!plan.is_complete());
        assert_eq!(plan.unplaceable[0].reason, UnplaceableReason::NoCandidates);
    }

    /// A sandbox already mid-handover is not this drain's to move.
    #[test]
    fn a_claimed_sandbox_is_left_alone() {
        let mut claimed = record(1024, "k1");
        claimed.state = MobilityState::Claimed {
            by_node_id: "node-c".to_string(),
            at_unix_ms: 1,
        };
        let plan = plan_evacuation(&[claimed], &[candidate("node-b", 8192, "k1")], &no_layers());

        assert!(plan.moves.is_empty());
        assert_eq!(plan.unplaceable[0].reason, UnplaceableReason::NotParked);
    }

    /// Layers are per-sandbox, so a node-local drive must strand only its own
    /// sandbox.
    #[test]
    fn a_sandbox_with_unreachable_layers_is_stranded_alone() {
        let mut stranded = record(1024, "k1");
        stranded.artifact_reach = ArtifactReach::NodeLocal;
        let movable = record(1024, "k1");

        let managed = vec![OverlaybdLayerRef::Managed(ManagedLayer {
            digest: "sha256:a".to_string(),
            size: 1,
            uuid: None,
        })];
        let mut layers = HashMap::new();
        layers.insert(
            stranded.sandbox_id,
            SandboxLayers {
                rootfs: &managed,
                attached_drives: &[],
            },
        );

        let plan = plan_evacuation(
            &[stranded.clone(), movable.clone()],
            &[candidate("node-b", 8192, "k1")],
            &layers,
        );

        assert_eq!(plan.moves.len(), 1);
        assert_eq!(plan.moves[0].sandbox_id, movable.sandbox_id);
        assert_eq!(plan.unplaceable[0].sandbox_id, stranded.sandbox_id);
        assert_eq!(plan.unplaceable[0].reason.kind(), "no_compatible_node");
    }

    /// Two runs over the same input must produce the same plan, or a drain
    /// cannot be reviewed before it is run.
    #[test]
    fn planning_is_deterministic() {
        let records: Vec<MobilityRecord> = (0..8)
            .map(|index| record(1024 * (index % 3 + 1), "k1"))
            .collect();
        let candidates = vec![
            candidate("node-b", 8192, "k1"),
            candidate("node-c", 8192, "k1"),
            candidate("node-d", 4096, "k1"),
        ];

        let first = plan_evacuation(&records, &candidates, &no_layers());
        let second = plan_evacuation(&records, &candidates, &no_layers());
        assert_eq!(first, second);
    }

    #[derive(Default)]
    struct ScriptedExecutor {
        failing: Vec<SandboxId>,
        attempted: Mutex<Vec<SandboxId>>,
        delay: Option<Duration>,
    }

    #[async_trait::async_trait]
    impl MoveExecutor for ScriptedExecutor {
        async fn execute(&self, planned: &PlannedMove, _cancel: &MoveCancel) -> anyhow::Result<()> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            self.attempted.lock().unwrap().push(planned.sandbox_id);
            if self.failing.contains(&planned.sandbox_id) {
                anyhow::bail!("restore refused");
            }
            Ok(())
        }
    }

    fn plan_of(count: usize) -> EvacuationPlan {
        EvacuationPlan {
            moves: (0..count)
                .map(|_| PlannedMove {
                    sandbox_id: SandboxId::new(),
                    to_node_id: "node-b".to_string(),
                    resources: SandboxResources::default(),
                })
                .collect(),
            unplaceable: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_drain_reports_every_move_it_made() {
        let plan = plan_of(3);
        let executor = Arc::new(ScriptedExecutor::default());
        let report = drain(&plan, executor.clone(), DrainBudget::default()).await;

        assert_eq!(report.migrated.len(), 3);
        assert!(report.failed.is_empty());
        assert!(!report.stopped_early);
    }

    /// A systematic failure — an unreachable repository, a fleet-wide skew —
    /// must not be rediscovered once per sandbox.
    #[tokio::test]
    async fn a_drain_stops_once_its_failure_budget_is_gone() {
        let plan = plan_of(20);
        let executor = Arc::new(ScriptedExecutor {
            failing: plan
                .moves
                .iter()
                .map(|planned| planned.sandbox_id)
                .collect(),
            ..ScriptedExecutor::default()
        });
        let budget = DrainBudget {
            max_concurrent: 1,
            max_failures: 2,
            ..DrainBudget::default()
        };

        let report = drain(&plan, executor.clone(), budget).await;
        assert!(report.stopped_early);
        assert_eq!(
            report.failed.len(),
            3,
            "stops on the failure past the budget"
        );
        assert_eq!(
            report.migrated.len() + report.failed.len() + report.not_attempted.len(),
            plan.moves.len(),
            "every planned move must be accounted for"
        );
        assert!(
            executor.attempted.lock().unwrap().len() < plan.moves.len(),
            "the drain must not have worked through the whole plan"
        );
    }

    /// A move that hangs must not hold the drain open forever; the budget is
    /// what makes a drain something an operator can wait on. This executor
    /// never looks at the request to stop, which is the case the unwind grace
    /// bounds.
    #[tokio::test]
    async fn a_hung_move_is_timed_out_and_counted_as_a_failure() {
        let plan = plan_of(1);
        let executor = Arc::new(ScriptedExecutor {
            delay: Some(Duration::from_secs(30)),
            ..ScriptedExecutor::default()
        });
        let budget = DrainBudget {
            move_timeout: Duration::from_millis(50),
            unwind_grace: Duration::from_millis(50),
            ..DrainBudget::default()
        };

        let report = drain(&plan, executor, budget).await;
        assert_eq!(report.failed.len(), 1);
        assert!(report.failed[0].1.contains("timed out"));
        assert!(
            report.failed[0].1.contains("still running"),
            "an executor that ignores the request is reported as such: {}",
            report.failed[0].1
        );
    }

    /// The moves still running when the failure budget dies have to be told to
    /// stop. The drain has stopped waiting on them, and a migration nobody is
    /// waiting for still has to put the sandbox back somewhere — so the request
    /// has to reach the moves in flight, not only the ones already finished.
    ///
    /// Observed from inside the executor on purpose: the report's shape is
    /// identical whether or not the request is ever sent.
    #[tokio::test]
    async fn stopping_early_asks_the_moves_still_in_flight_to_stop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct WaitsToBeStopped {
            hanging: SandboxId,
            started: Arc<AtomicBool>,
            stopped: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl MoveExecutor for WaitsToBeStopped {
            async fn execute(
                &self,
                planned: &PlannedMove,
                cancel: &MoveCancel,
            ) -> anyhow::Result<()> {
                if planned.sandbox_id == self.hanging {
                    self.started.store(true, Ordering::SeqCst);
                    cancel.requested().await;
                    self.stopped.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                // Ordering, not timing: the budget must die with a move
                // genuinely in flight or the test proves nothing.
                while !self.started.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                anyhow::bail!("restore refused")
            }
        }

        let plan = plan_of(2);
        let started = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let report = drain(
            &plan,
            Arc::new(WaitsToBeStopped {
                hanging: plan.moves[0].sandbox_id,
                started: Arc::clone(&started),
                stopped: Arc::clone(&stopped),
            }),
            DrainBudget {
                max_concurrent: 2,
                max_failures: 0,
                ..DrainBudget::default()
            },
        )
        .await;

        assert!(report.stopped_early);
        assert_eq!(
            report.not_attempted,
            vec![plan.moves[0].sandbox_id],
            "the hanging move is the one the drain stopped waiting on"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !stopped.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "the drain stopped early but never asked the in-flight move to stop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Concurrency is what keeps a drain from competing with the live traffic
    /// on both ends of every move.
    #[tokio::test]
    async fn a_drain_respects_its_concurrency_cap() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingExecutor {
            in_flight: AtomicUsize,
            peak: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl MoveExecutor for CountingExecutor {
            async fn execute(
                &self,
                _planned: &PlannedMove,
                _cancel: &MoveCancel,
            ) -> anyhow::Result<()> {
                let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let plan = plan_of(12);
        let executor = Arc::new(CountingExecutor {
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let budget = DrainBudget {
            max_concurrent: 3,
            ..DrainBudget::default()
        };

        drain(&plan, executor.clone(), budget).await;
        assert!(
            executor.peak.load(Ordering::SeqCst) <= 3,
            "peaked at {}",
            executor.peak.load(Ordering::SeqCst)
        );
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::tests::{fingerprint, record};
    use super::*;
    use crate::orchestrator::mobility::claim::MobilityCoordinator;
    use crate::orchestrator::mobility::record::{
        LocalMobilityStore, MobilityGeneration, MobilityStore, MobilityWrite,
    };
    use crate::orchestrator::mobility::saga::{MigrationOutcome, MigrationSaga, MigrationSteps};
    use crate::types::SandboxId;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A restore that gets as far as holding the sandbox's state open on the
    /// destination and then makes no further progress. This is where a
    /// per-move timeout finds a real migration: the expensive step, with the
    /// destination already committed to devices it has to give back.
    struct HangingRestore {
        entered_restore: Arc<AtomicBool>,
        held_by_destination: Arc<AtomicBool>,
        discarded: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl MigrationSteps for HangingRestore {
        async fn restore(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            self.entered_restore.store(true, Ordering::SeqCst);
            self.held_by_destination.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        }

        async fn discard_restored(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            self.held_by_destination.store(false, Ordering::SeqCst);
            self.discarded.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn release_origin_state(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// What a drain executes in production: the migration saga, on behalf of
    /// the destination.
    struct SagaExecutor<S: MobilityStore> {
        saga: MigrationSaga<S>,
        host: MigrationFingerprint,
    }

    #[async_trait::async_trait]
    impl<S: MobilityStore + 'static> MoveExecutor for SagaExecutor<S> {
        async fn execute(&self, planned: &PlannedMove, cancel: &MoveCancel) -> anyhow::Result<()> {
            match self
                .saga
                .migrate(&planned.sandbox_id, &self.host, &[], &[], cancel)
                .await?
            {
                MigrationOutcome::Migrated => Ok(()),
                other => anyhow::bail!("the migration did not complete: {other:?}"),
            }
        }
    }

    /// The per-move timeout used to drop the migration's future, which stops
    /// it at whichever await it was suspended at. Every compensation in the
    /// saga is a step it has to *run*, so none of them did: the destination
    /// kept the half-restored sandbox open and the claim was never released,
    /// and once that claim lapsed the origin was free to resume a sandbox the
    /// destination still had. Two live owners, reached by a timeout rather
    /// than by any step failing.
    #[tokio::test]
    async fn a_move_that_overruns_leaves_exactly_one_owner() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("open store");
        let paused = record(2048, "k1");
        store.upsert(&paused).await.expect("seed record");

        let entered_restore = Arc::new(AtomicBool::new(false));
        let held_by_destination = Arc::new(AtomicBool::new(false));
        let discarded = Arc::new(AtomicBool::new(false));
        let ttl = Duration::from_secs(30);
        let saga = MigrationSaga::new(
            Arc::new(MobilityCoordinator::new(store.clone(), "node-b").with_claim_ttl(ttl)),
            Arc::new(HangingRestore {
                entered_restore: Arc::clone(&entered_restore),
                held_by_destination: Arc::clone(&held_by_destination),
                discarded: Arc::clone(&discarded),
            }),
        )
        .with_claim_ttl(ttl);

        let plan = EvacuationPlan {
            moves: vec![PlannedMove {
                sandbox_id: paused.sandbox_id,
                to_node_id: "node-b".to_string(),
                resources: paused.resources,
            }],
            unplaceable: Vec::new(),
        };
        let report = drain(
            &plan,
            Arc::new(SagaExecutor {
                saga,
                host: fingerprint("k1"),
            }),
            DrainBudget {
                move_timeout: Duration::from_millis(300),
                unwind_grace: Duration::from_secs(5),
                ..DrainBudget::default()
            },
        )
        .await;

        assert!(
            entered_restore.load(Ordering::SeqCst),
            "the timeout has to land mid-restore or this test proves nothing"
        );
        assert_eq!(report.failed.len(), 1, "an overrun move is a failed move");
        assert!(
            report.failed[0].1.contains("timed out"),
            "unexpected failure: {}",
            report.failed[0].1
        );
        assert!(
            discarded.load(Ordering::SeqCst),
            "the timeout must unwind the saga through its compensations"
        );
        assert!(
            !held_by_destination.load(Ordering::SeqCst),
            "the destination must not be left holding a sandbox it half-restored"
        );
        assert_eq!(
            store
                .get(&paused.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            MobilityState::Parked,
            "the claim must be given back, leaving the origin the sandbox's only owner"
        );
    }

    /// A move that passes its point of no return while it is being asked to
    /// stop has committed the handover, and the record says so. Reporting it as
    /// a failure that "was unwound" tells an operator the origin still owns a
    /// sandbox that has moved, and the drained-node-empty decision is then made
    /// against the opposite of what the store holds.
    #[tokio::test]
    async fn a_move_that_commits_during_the_unwind_grace_is_reported_as_migrated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("open store");
        let paused = record(2048, "k1");
        store.upsert(&paused).await.expect("seed record");

        let ttl = Duration::from_secs(30);
        let saga = MigrationSaga::new(
            Arc::new(
                MobilityCoordinator::new(
                    SlowCommit {
                        inner: store.clone(),
                    },
                    "node-b",
                )
                .with_claim_ttl(ttl),
            ),
            Arc::new(InstantRestore),
        )
        .with_claim_ttl(ttl);

        let plan = EvacuationPlan {
            moves: vec![PlannedMove {
                sandbox_id: paused.sandbox_id,
                to_node_id: "node-b".to_string(),
                resources: paused.resources,
            }],
            unplaceable: Vec::new(),
        };
        let report = drain(
            &plan,
            Arc::new(SagaExecutor {
                saga,
                host: fingerprint("k1"),
            }),
            DrainBudget {
                // Short enough that the timeout lands inside the commit, long
                // enough that the commit then finishes well inside the grace.
                move_timeout: Duration::from_millis(100),
                unwind_grace: Duration::from_secs(5),
                ..DrainBudget::default()
            },
        )
        .await;

        assert_eq!(
            report.migrated,
            vec![paused.sandbox_id],
            "a committed handover is a migration, whatever the clock said: {:?}",
            report.failed
        );
        assert!(report.failed.is_empty());
        assert!(
            matches!(
                store.get(&paused.sandbox_id).await.expect("get").expect("record").state,
                MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
            ),
            "the report and the record have to agree about who owns the sandbox"
        );
    }

    /// A restore with nothing to do, so the only slow step is the commit.
    struct InstantRestore;

    #[async_trait::async_trait]
    impl MigrationSteps for InstantRestore {
        async fn restore(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            Ok(())
        }

        async fn discard_restored(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            Ok(())
        }

        async fn release_origin_state(&self, _record: &MobilityRecord) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Takes long enough over the commit that the drain's ceiling expires
    /// inside it, which is what a busy scheduler looks like from here.
    struct SlowCommit {
        inner: LocalMobilityStore,
    }

    #[async_trait::async_trait]
    impl MobilityStore for SlowCommit {
        async fn upsert(&self, record: &MobilityRecord) -> anyhow::Result<MobilityWrite> {
            self.inner.upsert(record).await
        }

        async fn compare_and_set(
            &self,
            expected: Option<MobilityGeneration>,
            record: &MobilityRecord,
        ) -> anyhow::Result<MobilityWrite> {
            if matches!(record.state, MobilityState::Evacuated { .. }) {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            self.inner.compare_and_set(expected, record).await
        }

        async fn get(&self, sandbox_id: &SandboxId) -> anyhow::Result<Option<MobilityRecord>> {
            self.inner.get(sandbox_id).await
        }

        async fn list(&self) -> anyhow::Result<Vec<MobilityRecord>> {
            self.inner.list().await
        }

        async fn remove(&self, sandbox_id: &SandboxId) -> anyhow::Result<()> {
            self.inner.remove(sandbox_id).await
        }
    }
}
