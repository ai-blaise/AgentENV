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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
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

/// Performs one planned move.
///
/// A migration is driven by the destination, so an implementation here is
/// whatever asks the destination to claim and restore. It is a trait so the
/// drain's bounds can be exercised without a fleet.
#[async_trait::async_trait]
pub trait MoveExecutor: Send + Sync {
    async fn execute(&self, planned: &PlannedMove) -> anyhow::Result<()>;
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
    /// Ceiling on one move.
    pub move_timeout: Duration,
}

impl Default for DrainBudget {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_failures: 3,
            move_timeout: Duration::from_secs(300),
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

/// Executes a plan under `budget`.
pub async fn drain(
    plan: &EvacuationPlan,
    executor: Arc<dyn MoveExecutor>,
    budget: DrainBudget,
) -> DrainReport {
    let mut report = DrainReport::default();
    let concurrency = budget.max_concurrent.max(1);

    let mut results = stream::iter(plan.moves.iter().enumerate())
        .map(|(index, planned)| {
            let executor = Arc::clone(&executor);
            async move {
                let outcome = tokio::time::timeout(budget.move_timeout, executor.execute(planned))
                    .await
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!(
                            "move timed out after {:?}",
                            budget.move_timeout
                        ))
                    });
                (index, planned.sandbox_id, outcome)
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
        // Everything the stream had not yet delivered is unattempted. Moves
        // still in flight when the budget ran out are counted here too: their
        // outcome was dropped with the stream, so claiming either result would
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

    fn fingerprint(kernel: &str) -> MigrationFingerprint {
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

    fn record(memory_mib: u32, kernel: &str) -> MobilityRecord {
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
        async fn execute(&self, planned: &PlannedMove) -> anyhow::Result<()> {
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
    /// what makes a drain something an operator can wait on.
    #[tokio::test]
    async fn a_hung_move_is_timed_out_and_counted_as_a_failure() {
        let plan = plan_of(1);
        let executor = Arc::new(ScriptedExecutor {
            delay: Some(Duration::from_secs(30)),
            ..ScriptedExecutor::default()
        });
        let budget = DrainBudget {
            move_timeout: Duration::from_millis(50),
            ..DrainBudget::default()
        };

        let report = drain(&plan, executor, budget).await;
        assert_eq!(report.failed.len(), 1);
        assert!(report.failed[0].1.contains("timed out"));
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
            async fn execute(&self, _planned: &PlannedMove) -> anyhow::Result<()> {
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
