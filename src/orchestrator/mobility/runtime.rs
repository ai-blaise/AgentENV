//! Connecting the mobility model to the sandboxes a node is actually holding.
//!
//! The record, the claim and the planner are all pure enough to test on their
//! own, which is how they were built. This is the part that makes them true of
//! the running node: a record appears when a sandbox is paused, is consulted
//! before it resumes locally, and disappears when it is running here again or
//! is gone.
//!
//! # Optional by construction
//!
//! A node with no interest in migration should not carry a durable store for
//! it, so every hook is a no-op when no runtime is installed. That is also
//! what makes the wiring safe to land ahead of the parts that need a
//! hypervisor: nothing changes until an operator asks for it.
//!
//! # What a record means today
//!
//! A paused sandbox's state is written by the persister to node-local storage.
//! Until it is committed to a snapshot the whole cluster can read, no
//! destination can restore it, and the record says so — `snapshot_id` is
//! `None` and every placement attempt is refused with that reason. This is not
//! a limitation of the planner; it is the truthful state of a paused sandbox,
//! and it is exactly what an operator needs to see before scheduling
//! maintenance on the node holding it.

use std::sync::Arc;

use tracing::{debug, warn};

use super::claim::{MobilityCoordinator, ResumeFence};
use super::evacuation::{plan_evacuation, DestinationCandidate, EvacuationPlan};
use super::record::{MobilityRecord, MobilityState, MobilityStore};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::ArtifactReach;
use crate::types::SandboxId;

/// The host facts every record on this node shares.
#[derive(Clone, Debug)]
pub struct NodeMobilityFacts {
    pub cpu_architecture: String,
    /// The cluster-wide CPU template applied to new VMs.
    ///
    /// Shared rather than copied, because it arrives from the scheduler after
    /// startup: a snapshot taken when mobility was installed would be `None`
    /// forever, and `None` correctly makes every sandbox non-migratable — a
    /// guest booted with this machine's own CPU features was told about
    /// whatever this machine happens to have.
    pub cluster_cpu_config: Arc<std::sync::RwLock<Option<String>>>,
    pub memory_page_size: u32,
    /// Whether this node's snapshot repository is readable cluster-wide.
    pub artifact_reach: ArtifactReach,
}

/// The orchestrator's view of mobility.
///
/// A trait rather than the concrete runtime so the orchestrator does not grow
/// a generic parameter for a subsystem most nodes will not enable, and so the
/// hooks can be exercised without a durable store behind them.
#[async_trait::async_trait]
pub trait MobilityHooks: Send + Sync {
    /// Records a sandbox that has just been paused.
    async fn record_paused(&self, metadata: &SandboxMetadata);
    /// Takes the sandbox for a local resume, or reports who holds it.
    async fn claim_for_local_resume(&self, sandbox_id: &SandboxId) -> ResumeFence;
    /// Drops a record because the sandbox is running here again, or is gone.
    async fn forget(&self, sandbox_id: &SandboxId);
    /// Records that a paused sandbox's state now lives in the repository.
    async fn record_committed(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &crate::snapshot::SnapshotId,
    );
    /// Counts records by state, for the node's metrics.
    async fn record_counts(&self) -> MobilityRecordCounts;
    /// Publishes those counts as gauges.
    async fn publish_metrics(&self);
}

#[async_trait::async_trait]
impl<S: MobilityStore + 'static> MobilityHooks for MobilityRuntime<S> {
    async fn record_paused(&self, metadata: &SandboxMetadata) {
        MobilityRuntime::record_paused(self, metadata).await
    }

    async fn claim_for_local_resume(&self, sandbox_id: &SandboxId) -> ResumeFence {
        MobilityRuntime::claim_for_local_resume(self, sandbox_id).await
    }

    async fn forget(&self, sandbox_id: &SandboxId) {
        MobilityRuntime::forget(self, sandbox_id).await
    }

    async fn record_committed(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &crate::snapshot::SnapshotId,
    ) {
        MobilityRuntime::record_committed(self, sandbox_id, snapshot_id).await
    }

    async fn record_counts(&self) -> MobilityRecordCounts {
        MobilityRuntime::record_counts(self).await
    }

    async fn publish_metrics(&self) {
        MobilityRuntime::publish_metrics(self).await
    }
}

/// Keeps this node's mobility records in step with its sandboxes.
pub struct MobilityRuntime<S: MobilityStore> {
    coordinator: Arc<MobilityCoordinator<S>>,
    facts: NodeMobilityFacts,
}

impl<S: MobilityStore> MobilityRuntime<S> {
    pub fn new(coordinator: Arc<MobilityCoordinator<S>>, facts: NodeMobilityFacts) -> Self {
        Self { coordinator, facts }
    }

    pub fn node_id(&self) -> &str {
        self.coordinator.node_id()
    }

    /// Records a sandbox that has just been paused.
    ///
    /// Best effort by contract. The sandbox is already paused and its state
    /// already persisted by the time this runs, so a failure here costs the
    /// ability to migrate it, not the pause. Failing the pause instead would
    /// turn a bookkeeping problem into a running sandbox that could not be
    /// stopped.
    pub async fn record_paused(&self, metadata: &SandboxMetadata) {
        let cpu_config = self
            .facts
            .cluster_cpu_config
            .read()
            .ok()
            .and_then(|config| config.clone());
        let record = MobilityRecord::for_paused(
            metadata,
            self.coordinator.node_id(),
            self.facts.cpu_architecture.clone(),
            cpu_config,
            self.facts.memory_page_size,
            self.facts.artifact_reach,
            // A paused sandbox's state is node-local until something commits
            // it to the repository. Claiming otherwise would let a planner
            // send it somewhere that cannot read it.
            None,
        );
        match self.coordinator.store().upsert(&record).await {
            Ok(outcome) => debug!(
                sandbox_id = %metadata.id,
                ?outcome,
                "recorded a paused sandbox as a migration candidate"
            ),
            Err(error) => warn!(
                sandbox_id = %metadata.id,
                error = %error,
                "failed to record a paused sandbox; it will not be visible to a drain"
            ),
        }
    }

    /// Takes the sandbox for a local resume, or explains who has it.
    ///
    /// Taking rather than checking: a destination that claims between a check
    /// and the resume would leave two nodes running one sandbox.
    pub async fn claim_for_local_resume(&self, sandbox_id: &SandboxId) -> ResumeFence {
        match self.coordinator.claim_for_local_resume(sandbox_id).await {
            Ok(fence) => fence,
            Err(error) => {
                // Refusing on an unreadable store, not allowing. The store is
                // the only thing that knows whether a handover is in flight,
                // and resuming without it is how two copies appear.
                warn!(
                    %sandbox_id,
                    error = %error,
                    "mobility store unreadable; refusing a local resume rather than risking a second copy"
                );
                ResumeFence::ClaimedElsewhere {
                    by_node_id: "an unreadable mobility store".to_string(),
                }
            }
        }
    }

    /// Records that a paused sandbox's state now lives in the repository, so a
    /// destination can restore it.
    ///
    /// Until this, the record truthfully says the sandbox cannot move: its
    /// artifacts were files on this node's disk.
    pub async fn record_committed(
        &self,
        sandbox_id: &SandboxId,
        snapshot_id: &crate::snapshot::SnapshotId,
    ) {
        let Ok(Some(record)) = self.coordinator.store().get(sandbox_id).await else {
            // No record means mobility was enabled after the pause, or the
            // sandbox is not paused. Either way there is nothing to update,
            // and inventing a record here would claim a paused sandbox this
            // node never wrote down.
            return;
        };
        let committed = record.committed_to(snapshot_id.to_string());
        if let Err(error) = self.coordinator.store().upsert(&committed).await {
            warn!(
                %sandbox_id,
                error = %error,
                "published a paused sandbox but could not record it as movable"
            );
        }
    }

    /// Drops a sandbox's record because it is running here again, or is gone.
    pub async fn forget(&self, sandbox_id: &SandboxId) {
        if let Err(error) = self.coordinator.store().remove(sandbox_id).await {
            warn!(
                %sandbox_id,
                error = %error,
                "failed to drop a mobility record; a drain may consider a sandbox that has moved on"
            );
        }
    }

    /// Plans where this node's paused sandboxes would go.
    pub async fn plan_evacuation(&self, candidates: &[DestinationCandidate]) -> EvacuationPlan {
        let records = match self.coordinator.store().list().await {
            Ok(records) => records,
            Err(error) => {
                warn!(error = %error, "failed to read mobility records for an evacuation plan");
                return EvacuationPlan::default();
            }
        };
        // Layer sets come from committed snapshots, which a paused sandbox does
        // not have yet; the empty map is what says so.
        plan_evacuation(&records, candidates, &std::collections::HashMap::new())
    }

    /// Publishes the record counts as gauges.
    ///
    /// Without these the subsystem is invisible: a node where every paused
    /// sandbox is unmovable and one where none are paused at all export
    /// exactly the same nothing, and the difference is what decides whether a
    /// drain will do anything.
    pub async fn publish_metrics(&self) {
        let counts = self.record_counts().await;
        for (state, value) in [
            ("parked", counts.parked),
            ("claimed", counts.claimed),
            ("evacuated", counts.evacuated),
        ] {
            metrics::gauge!("agentenv_mobility_records", "state" => state).set(value as f64);
        }
        metrics::gauge!("agentenv_mobility_stranded_sandboxes")
            .set(counts.stranded_uncommitted as f64);
    }

    /// Counts records by state, for the node's metrics.
    pub async fn record_counts(&self) -> MobilityRecordCounts {
        let mut counts = MobilityRecordCounts::default();
        let Ok(records) = self.coordinator.store().list().await else {
            return counts;
        };
        for record in records {
            match record.state {
                MobilityState::Parked => counts.parked += 1,
                MobilityState::Claimed { .. } => counts.claimed += 1,
                MobilityState::Evacuated { .. } => counts.evacuated += 1,
            }
            if record.snapshot_id.is_none() {
                counts.stranded_uncommitted += 1;
            }
        }
        counts
    }
}

/// Opens the durable store and returns a runtime ready to install.
///
/// Fallible on purpose: the caller decides whether a node that cannot open its
/// store should refuse to start or run without migration. It should run —
/// refusing would turn an optional feature into a boot dependency.
pub async fn open_mobility_runtime(
    store_path: impl Into<std::path::PathBuf>,
    node_id: impl Into<String>,
    facts: NodeMobilityFacts,
) -> anyhow::Result<Arc<dyn MobilityHooks>> {
    let store = super::record::LocalMobilityStore::open(store_path).await?;
    let coordinator = Arc::new(MobilityCoordinator::new(store, node_id));
    Ok(Arc::new(MobilityRuntime::new(coordinator, facts)))
}

/// How many paused sandboxes are in each mobility state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MobilityRecordCounts {
    pub parked: usize,
    pub claimed: usize,
    pub evacuated: usize,
    /// Parked sandboxes that cannot move because their state was never
    /// committed anywhere another node can read.
    ///
    /// Reported separately because it is the number that decides whether a
    /// drain will accomplish anything, and it is invisible from the state
    /// alone — these look parked and available right up until a plan refuses
    /// every one of them.
    pub stranded_uncommitted: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::mobility::record::LocalMobilityStore;
    use crate::orchestrator::SandboxState;
    use crate::snapshot::{MigrationFingerprint, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;

    fn facts() -> NodeMobilityFacts {
        NodeMobilityFacts {
            cpu_architecture: "x86_64".to_string(),
            cluster_cpu_config: Arc::new(std::sync::RwLock::new(Some("{}".to_string()))),
            memory_page_size: 4096,
            artifact_reach: ArtifactReach::ClusterShared,
        }
    }

    fn metadata() -> SandboxMetadata {
        SandboxMetadata {
            state: SandboxState::Paused,
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

    /// Returns the runtime and a handle onto the same store, because RocksDB
    /// is exclusive to one process and a second `open` on the same path fails.
    async fn runtime() -> (
        MobilityRuntime<LocalMobilityStore>,
        LocalMobilityStore,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("open store");
        let coordinator = Arc::new(MobilityCoordinator::new(store.clone(), "node-a"));
        (MobilityRuntime::new(coordinator, facts()), store, dir)
    }

    #[tokio::test]
    async fn pausing_records_a_sandbox_and_resuming_forgets_it() {
        let (runtime, _store, _dir) = runtime().await;
        let metadata = metadata();

        runtime.record_paused(&metadata).await;
        assert_eq!(
            runtime.record_counts().await,
            MobilityRecordCounts {
                parked: 1,
                stranded_uncommitted: 1,
                ..MobilityRecordCounts::default()
            }
        );

        assert_eq!(
            runtime.claim_for_local_resume(&metadata.id).await,
            ResumeFence::Allowed
        );
        runtime.forget(&metadata.id).await;
        assert_eq!(
            runtime.record_counts().await,
            MobilityRecordCounts::default()
        );
    }

    /// A paused sandbox's state lives on this node's disk. Until something
    /// commits it somewhere the cluster can read, a drain accomplishes
    /// nothing — and an operator planning maintenance needs to see that before
    /// they start, not after.
    #[tokio::test]
    async fn an_uncommitted_paused_sandbox_is_reported_as_stranded() {
        let (runtime, _store, _dir) = runtime().await;
        let metadata = metadata();
        runtime.record_paused(&metadata).await;

        let host = MigrationFingerprint::from_runtime(
            &metadata.runtime_versions,
            "x86_64",
            VirtualizationMode::Kvm,
            Some("{}".to_string()),
            4096,
        );
        let plan = runtime
            .plan_evacuation(&[DestinationCandidate {
                node_id: "node-b".to_string(),
                fingerprint: host,
                free_cpu: 64,
                free_memory_mib: 65536,
            }])
            .await;

        assert!(plan.moves.is_empty(), "nothing is movable yet: {plan:?}");
        assert_eq!(plan.unplaceable.len(), 1);
        assert_eq!(plan.unplaceable[0].reason.kind(), "no_compatible_node");
        assert_eq!(runtime.record_counts().await.stranded_uncommitted, 1);
    }

    /// The resume path must refuse while a destination holds a claim, or the
    /// origin and the destination both end up running the sandbox.
    #[tokio::test]
    async fn a_claimed_sandbox_cannot_be_resumed_locally() {
        let (runtime, store, _dir) = runtime().await;
        let metadata = metadata();
        runtime.record_paused(&metadata).await;

        let destination = MobilityCoordinator::new(store, "node-b");
        destination.claim(&metadata.id).await.expect("claim");

        assert_eq!(
            runtime.claim_for_local_resume(&metadata.id).await,
            ResumeFence::ClaimedElsewhere {
                by_node_id: "node-b".to_string()
            }
        );
    }

    /// A sandbox with no record is not in a handover, and mobility is opt-in,
    /// so it must resume freely rather than being fenced by an absent decision.
    #[tokio::test]
    async fn a_sandbox_that_was_never_recorded_resumes_freely() {
        let (runtime, _store, _dir) = runtime().await;
        assert_eq!(
            runtime.claim_for_local_resume(&SandboxId::new()).await,
            ResumeFence::Allowed
        );
    }
}
