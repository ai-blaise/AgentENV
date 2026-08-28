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
    /// Gives back a claim this node took but did not use.
    async fn release_local_claim(&self, sandbox_id: &SandboxId);
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

    async fn release_local_claim(&self, sandbox_id: &SandboxId) {
        MobilityRuntime::release_local_claim(self, sandbox_id).await
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

    /// Gives back a claim this node took for a resume that then failed.
    ///
    /// Without this the sandbox stays fenced until the lease expires, so a
    /// resume that fails for its own reasons — no capacity, a bad snapshot —
    /// also blocks every other node from taking the sandbox for the length of
    /// a TTL, and blocks this node from retrying.
    pub async fn release_local_claim(&self, sandbox_id: &SandboxId) {
        match self.coordinator.release(sandbox_id).await {
            Ok(true) => debug!(%sandbox_id, "released a claim taken for a resume that failed"),
            // Not ours any more, or never recorded. Either way there is
            // nothing to give back.
            Ok(false) => {}
            Err(error) => warn!(
                %sandbox_id,
                error = %error,
                "failed to release a claim after a failed resume; it will lapse"
            ),
        }
    }

    /// Drops a sandbox's record because it is running here again, or is gone.
    ///
    /// Only this node's own records, and only ones nobody else holds. An
    /// unconditional delete would erase an `Evacuated` tombstone — turning
    /// "already gone, and to whom" into "unknown sandbox", which a late
    /// claimant cannot tell from a lost record — and would drop a claim
    /// another node had just been granted, freeing a sandbox that node is in
    /// the middle of restoring.
    pub async fn forget(&self, sandbox_id: &SandboxId) {
        let record = match self.coordinator.store().get(sandbox_id).await {
            Ok(Some(record)) => record,
            // Nothing to drop, or the store cannot be read. Leaving a record
            // in place costs a drain one refused placement; removing one we
            // could not read could remove someone else's claim.
            Ok(None) => return,
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %error,
                    "could not read a mobility record to drop it; leaving it in place"
                );
                return;
            }
        };

        match &record.state {
            // Ours and unclaimed: the ordinary case, a sandbox that resumed
            // here or was deleted.
            MobilityState::Parked => {}
            // Held by this node, which is what a local resume looks like after
            // it took its own claim.
            MobilityState::Claimed { by_node_id, .. } if by_node_id == self.node_id() => {}
            MobilityState::Claimed { by_node_id, .. } => {
                warn!(
                    %sandbox_id,
                    holder = %by_node_id,
                    "not dropping a mobility record another node has claimed"
                );
                return;
            }
            MobilityState::Evacuated { to_node_id, .. } => {
                debug!(
                    %sandbox_id,
                    destination = %to_node_id,
                    "keeping the tombstone for a sandbox that moved"
                );
                return;
            }
        }

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
/// Builds a runtime over an already-open store.
///
/// Separate from [`open_mobility_runtime`] because which store to use is a
/// deployment question: a node in a cluster needs the scheduler-backed one, a
/// node on its own can only have the local one.
pub fn mobility_runtime_with_store<S: MobilityStore + 'static>(
    store: S,
    node_id: impl Into<String>,
    facts: NodeMobilityFacts,
) -> Arc<dyn MobilityHooks> {
    Arc::new(MobilityRuntime::new(
        Arc::new(MobilityCoordinator::new(store, node_id)),
        facts,
    ))
}

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

#[cfg(test)]
mod committed_and_failure_tests {
    use super::*;
    use crate::orchestrator::mobility::record::{LocalMobilityStore, MobilityWrite};
    use crate::orchestrator::store::SandboxMetadata;
    use crate::orchestrator::SandboxState;
    use crate::snapshot::{MigrationFingerprint, SnapshotId, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;
    use async_trait::async_trait;

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

    fn host_fingerprint(metadata: &SandboxMetadata) -> MigrationFingerprint {
        MigrationFingerprint::from_runtime(
            &metadata.runtime_versions,
            "x86_64",
            VirtualizationMode::Kvm,
            Some("{}".to_string()),
            4096,
        )
    }

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

    /// The whole point of committing a paused sandbox: it goes from a record
    /// every plan refuses to one a plan can place. Before this test the entire
    /// sequence — `committed_to`, `record_committed`, and the API path that
    /// calls it — had never executed.
    #[tokio::test]
    async fn committing_a_snapshot_turns_an_unplaceable_record_into_a_placeable_one() {
        let (runtime, _store, _dir) = runtime().await;
        let metadata = metadata();
        let host = host_fingerprint(&metadata);
        runtime.record_paused(&metadata).await;

        let candidate = DestinationCandidate {
            node_id: "node-b".to_string(),
            fingerprint: host,
            free_cpu: 64,
            free_memory_mib: 65536,
        };

        let before = runtime
            .plan_evacuation(std::slice::from_ref(&candidate))
            .await;
        assert!(before.moves.is_empty(), "nothing is movable yet");
        assert_eq!(before.unplaceable.len(), 1);
        assert_eq!(runtime.record_counts().await.stranded_uncommitted, 1);

        let snapshot_id = SnapshotId::generate();
        runtime.record_committed(&metadata.id, &snapshot_id).await;

        let after = runtime.plan_evacuation(&[candidate]).await;
        assert_eq!(
            after.moves.len(),
            1,
            "a committed sandbox must become placeable, got {after:?}"
        );
        assert_eq!(after.moves[0].sandbox_id, metadata.id);
        assert_eq!(after.moves[0].to_node_id, "node-b");
        assert_eq!(
            runtime.record_counts().await.stranded_uncommitted,
            0,
            "it is no longer stranded"
        );
    }

    /// Committing a sandbox this node never recorded must not invent a record.
    /// A fabricated one would advertise a paused sandbox that does not exist
    /// here, and a drain would try to place it.
    #[tokio::test]
    async fn committing_an_unrecorded_sandbox_creates_nothing() {
        let (runtime, _store, _dir) = runtime().await;
        runtime
            .record_committed(&SandboxId::new(), &SnapshotId::generate())
            .await;
        assert_eq!(
            runtime.record_counts().await,
            MobilityRecordCounts::default()
        );
    }

    /// A store that cannot be read is not permission to resume. The store is
    /// the only thing that knows whether a handover is in flight, and guessing
    /// "nobody has it" is how two nodes end up running one sandbox.
    #[tokio::test]
    async fn an_unreadable_store_refuses_a_local_resume() {
        let coordinator = Arc::new(MobilityCoordinator::new(BrokenStore, "node-a"));
        let runtime = MobilityRuntime::new(coordinator, facts());

        let fence = runtime.claim_for_local_resume(&SandboxId::new()).await;
        assert!(
            matches!(fence, ResumeFence::ClaimedElsewhere { .. }),
            "a broken store must fail closed, got {fence:?}"
        );
    }

    /// The remaining store-failure paths must degrade rather than panic or
    /// fail the operation that triggered them.
    #[tokio::test]
    async fn a_broken_store_degrades_without_failing_the_caller() {
        let coordinator = Arc::new(MobilityCoordinator::new(BrokenStore, "node-a"));
        let runtime = MobilityRuntime::new(coordinator, facts());
        let metadata = metadata();

        // A pause has already succeeded by the time these run, so none of them
        // may propagate a failure.
        runtime.record_paused(&metadata).await;
        runtime.forget(&metadata.id).await;
        runtime
            .record_committed(&metadata.id, &SnapshotId::generate())
            .await;
        runtime.publish_metrics().await;

        assert_eq!(
            runtime.record_counts().await,
            MobilityRecordCounts::default(),
            "an unreadable store reports nothing rather than guessing"
        );
        assert!(runtime.plan_evacuation(&[]).await.moves.is_empty());
    }

    /// A store whose every operation fails, for the paths that only run when
    /// the durable layer is broken.
    struct BrokenStore;

    #[async_trait]
    impl MobilityStore for BrokenStore {
        async fn upsert(&self, _record: &MobilityRecord) -> anyhow::Result<MobilityWrite> {
            anyhow::bail!("mobility store is unavailable")
        }

        async fn compare_and_set(
            &self,
            _expected: Option<crate::orchestrator::MobilityGeneration>,
            _record: &MobilityRecord,
        ) -> anyhow::Result<MobilityWrite> {
            anyhow::bail!("mobility store is unavailable")
        }

        async fn get(&self, _sandbox_id: &SandboxId) -> anyhow::Result<Option<MobilityRecord>> {
            anyhow::bail!("mobility store is unavailable")
        }

        async fn list(&self) -> anyhow::Result<Vec<MobilityRecord>> {
            anyhow::bail!("mobility store is unavailable")
        }

        async fn remove(&self, _sandbox_id: &SandboxId) -> anyhow::Result<()> {
            anyhow::bail!("mobility store is unavailable")
        }
    }
}

#[cfg(test)]
mod forget_tests {
    use super::*;
    use crate::orchestrator::mobility::record::LocalMobilityStore;
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::SnapshotRuntimeVersions;
    use crate::virtualization::VirtualizationMode;

    async fn fixture() -> (
        MobilityRuntime<LocalMobilityStore>,
        LocalMobilityStore,
        SandboxId,
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
        let facts = NodeMobilityFacts {
            cpu_architecture: "x86_64".to_string(),
            cluster_cpu_config: Arc::new(std::sync::RwLock::new(Some("{}".to_string()))),
            memory_page_size: 4096,
            artifact_reach: ArtifactReach::ClusterShared,
        };
        let runtime = MobilityRuntime::new(
            Arc::new(MobilityCoordinator::new(store.clone(), "node-a")),
            facts,
        );
        runtime.record_paused(&metadata).await;
        (runtime, store, metadata.id, dir)
    }

    /// Deleting a sandbox locally must not free one another node is in the
    /// middle of restoring. `forget` runs unconditionally from the delete
    /// path, so a claim in flight would otherwise be erased and the sandbox
    /// offered to a second destination.
    #[tokio::test]
    async fn a_record_another_node_has_claimed_is_not_dropped() {
        let (runtime, store, sandbox_id, _dir) = fixture().await;
        MobilityCoordinator::new(store.clone(), "node-b")
            .claim(&sandbox_id)
            .await
            .expect("claim");

        runtime.forget(&sandbox_id).await;

        let record = store
            .get(&sandbox_id)
            .await
            .expect("get")
            .expect("the claim must survive a local forget");
        assert!(
            matches!(record.state, MobilityState::Claimed { ref by_node_id, .. } if by_node_id == "node-b"),
            "expected node-b's claim, got {:?}",
            record.state
        );
    }

    /// The tombstone answers a late claimant with "already gone, and to whom".
    /// Erasing it turns that into "unknown sandbox", which is what a lost
    /// record also looks like — and the two call for opposite responses.
    #[tokio::test]
    async fn an_evacuated_tombstone_survives() {
        let (runtime, store, sandbox_id, _dir) = fixture().await;
        let destination = MobilityCoordinator::new(store.clone(), "node-b");
        destination.claim(&sandbox_id).await.expect("claim");
        destination.complete(&sandbox_id).await.expect("complete");

        runtime.forget(&sandbox_id).await;

        let record = store
            .get(&sandbox_id)
            .await
            .expect("get")
            .expect("the tombstone must survive");
        assert!(matches!(record.state, MobilityState::Evacuated { .. }));
    }

    /// The ordinary cases still drop: a sandbox that resumed here or was
    /// deleted leaves no record behind.
    #[tokio::test]
    async fn a_parked_or_self_claimed_record_is_dropped() {
        let (runtime, store, sandbox_id, _dir) = fixture().await;
        runtime.forget(&sandbox_id).await;
        assert!(store.get(&sandbox_id).await.expect("get").is_none());

        let (runtime, store, sandbox_id, _dir) = fixture().await;
        MobilityCoordinator::new(store.clone(), "node-a")
            .claim(&sandbox_id)
            .await
            .expect("claim");
        runtime.forget(&sandbox_id).await;
        assert!(
            store.get(&sandbox_id).await.expect("get").is_none(),
            "this node's own claim must not block its own cleanup"
        );
    }
}
