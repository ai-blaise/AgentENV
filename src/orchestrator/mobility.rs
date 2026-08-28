//! What another node needs to know before it takes over a paused sandbox.
//!
//! A paused sandbox is a candidate for moving, but only the node holding it
//! knows what it would take: which runtime it was booted against, where its
//! artifacts live, how much it will cost to admit, and whether anyone else has
//! already started claiming it. A mobility record is that knowledge, written
//! down at pause time so a decision can be made without asking the source node
//! anything.
//!
//! # Ordering, not locking
//!
//! Records carry a generation, and a write with an older generation is refused
//! rather than applied. AgentENV has no consensus store, so this is not a lock
//! and does not pretend to be one: it is a total order over versions of the
//! same record, which is what lets a late write from a superseded actor be
//! discarded instead of resurrecting stale state. The claim protocol builds on
//! that ordering; it does not get mutual exclusion from it for free.
//!
//! UUIDv7 supplies the order. It is time-ordered but the ordering is between
//! *values*, so a clock step changes which generation wins, not whether one
//! does — the failure mode is a wrong winner, never two winners in the store.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::{
    assess_mobility, ArtifactReach, DriveForMigration, MigrationFingerprint, MobilityBlocker,
    OverlaybdLayerRef,
};
use crate::types::{SandboxId, SandboxResources};

/// Key prefix for mobility records in the node-local store.
const MOBILITY_KEY_PREFIX: &str = "mobility/v1/";

/// A total order over versions of one sandbox's mobility record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MobilityGeneration(Uuid);

impl MobilityGeneration {
    /// Mints a generation that sorts after every generation minted earlier.
    pub fn now() -> Self {
        Self(Uuid::now_v7())
    }

    /// Whether this generation replaces `other`.
    ///
    /// Equal generations do not supersede: rewriting a record under the same
    /// generation would make the order meaningless.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.0 > other.0
    }
}

impl std::fmt::Display for MobilityGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Where a paused sandbox is in the process of being handed over.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MobilityState {
    /// Held by its origin node and available to move.
    Parked,
    /// A destination has announced it intends to take this sandbox.
    ///
    /// A claim is an announcement, not a grant. It exists so the origin can
    /// refuse to resume locally while a handover is in flight, and so a second
    /// destination sees that it is racing.
    Claimed { by_node_id: String, at_unix_ms: u64 },
    /// The sandbox now lives on another node and this record is a tombstone.
    ///
    /// Kept rather than deleted so a late claim for the same sandbox can be
    /// answered with "already gone, and to whom" instead of "unknown sandbox",
    /// which is indistinguishable from a lost record.
    Evacuated { to_node_id: String, at_unix_ms: u64 },
}

/// Everything a candidate node needs to decide whether it can take a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobilityRecord {
    pub sandbox_id: SandboxId,
    pub origin_node_id: String,
    pub generation: MobilityGeneration,
    /// The runtime the paused guest was booted against.
    pub fingerprint: MigrationFingerprint,
    /// Whether the artifacts backing this sandbox are readable off-node.
    pub artifact_reach: ArtifactReach,
    /// What admitting this sandbox will cost the destination.
    pub resources: SandboxResources,
    /// The snapshot the paused state was published under, when it has one.
    ///
    /// `None` means the paused state exists only as node-local runtime files,
    /// so nothing can take it over until it has been committed.
    pub snapshot_id: Option<String>,
    pub paused_at_unix_ms: u64,
    pub state: MobilityState,
}

impl MobilityRecord {
    /// Builds the record for a sandbox that has just been paused.
    pub fn for_paused(
        metadata: &SandboxMetadata,
        origin_node_id: impl Into<String>,
        cpu_architecture: impl Into<String>,
        cpu_config_json: Option<String>,
        memory_page_size: u32,
        artifact_reach: ArtifactReach,
        snapshot_id: Option<String>,
    ) -> Self {
        Self {
            sandbox_id: metadata.id,
            origin_node_id: origin_node_id.into(),
            generation: MobilityGeneration::now(),
            fingerprint: MigrationFingerprint::from_runtime(
                &metadata.runtime_versions,
                cpu_architecture,
                metadata.virtualization_mode,
                cpu_config_json,
                memory_page_size,
            ),
            artifact_reach,
            resources: metadata.resources,
            snapshot_id,
            paused_at_unix_ms: unix_millis(SystemTime::now()),
            state: MobilityState::Parked,
        }
    }

    /// Whether this sandbox can move to a node with `host`.
    ///
    /// A sandbox whose paused state was never committed is refused outright:
    /// its artifacts are runtime files on the origin, and no fingerprint match
    /// makes them readable elsewhere.
    pub fn can_move_to(
        &self,
        host: &MigrationFingerprint,
        rootfs_layers: &[OverlaybdLayerRef],
        attached_drives: &[DriveForMigration<'_>],
    ) -> Result<(), MobilityBlocker> {
        if self.snapshot_id.is_none() {
            return Err(MobilityBlocker::Artifacts {
                drive_id: "paused-state".to_string(),
                detail: "the paused state has not been committed to a snapshot".to_string(),
            });
        }
        assess_mobility(
            &self.fingerprint,
            host,
            self.artifact_reach,
            rootfs_layers,
            attached_drives,
        )
    }

    /// Returns this record advanced to `state` under a fresh generation.
    pub fn transitioned_to(&self, state: MobilityState) -> Self {
        Self {
            generation: MobilityGeneration::now(),
            state,
            ..self.clone()
        }
    }

    fn key(&self) -> Vec<u8> {
        record_key(&self.sandbox_id)
    }
}

fn record_key(sandbox_id: &SandboxId) -> Vec<u8> {
    format!("{MOBILITY_KEY_PREFIX}{sandbox_id}").into_bytes()
}

fn unix_millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

/// The outcome of writing a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MobilityWrite {
    /// The record was written.
    Applied,
    /// A newer generation was already stored, so the write was discarded.
    ///
    /// Not an error: a superseded actor writing late is the ordinary shape of a
    /// handover race, and the caller's correct response is to re-read.
    Superseded,
}

/// Durable per-node index of paused sandboxes that could move.
#[async_trait]
pub trait MobilityStore: Send + Sync {
    /// Writes `record` unless a newer generation is already stored.
    async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite>;
    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>>;
    async fn list(&self) -> Result<Vec<MobilityRecord>>;
    /// Removes a record outright.
    ///
    /// For a sandbox that resumed locally or was deleted, whose record should
    /// leave no tombstone. A handover uses `MobilityState::Evacuated` instead.
    async fn remove(&self, sandbox_id: &SandboxId) -> Result<()>;
}

/// A [`MobilityStore`] backed by the node's local key/value store.
#[derive(Clone)]
pub struct LocalMobilityStore {
    store: Arc<LocalKvStore>,
}

impl LocalMobilityStore {
    /// Opens the store at `path`.
    ///
    /// `Sync` durability: a mobility record that survives a pause but not a
    /// power loss would leave a paused sandbox invisible to evacuation exactly
    /// when the node is least healthy, which is when it matters.
    pub async fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Self {
            store: Arc::new(LocalKvStore::open(path, LocalStoreDurability::Sync).await?),
        })
    }

    pub fn from_store(store: Arc<LocalKvStore>) -> Self {
        Self { store }
    }

    fn decode(value: &[u8]) -> Result<MobilityRecord> {
        serde_json::from_slice(value).context("decode mobility record")
    }
}

#[async_trait]
impl MobilityStore for LocalMobilityStore {
    async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
        // Read-then-write rather than a compare-and-swap: RocksDB gives no CAS
        // here, and the store is written only by this node's own orchestrator,
        // which serializes its lifecycle transitions per sandbox. The check
        // exists to discard a late write from a superseded in-process task, not
        // to arbitrate between nodes — that is the claim protocol's job.
        if let Some(existing) = self.get(&record.sandbox_id).await? {
            if !record.generation.supersedes(&existing.generation) {
                return Ok(MobilityWrite::Superseded);
            }
        }
        let value = serde_json::to_vec(record).context("encode mobility record")?;
        self.store.put(record.key(), value).await?;
        Ok(MobilityWrite::Applied)
    }

    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
        let Some(value) = self.store.get(record_key(sandbox_id)).await? else {
            return Ok(None);
        };
        Self::decode(&value).map(Some)
    }

    async fn list(&self) -> Result<Vec<MobilityRecord>> {
        self.store
            .scan_prefix(MOBILITY_KEY_PREFIX.as_bytes().to_vec())
            .await?
            .iter()
            .map(|(_, value)| Self::decode(value))
            .collect()
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
        self.store.delete(record_key(sandbox_id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{ManagedLayer, SnapshotRuntimeVersions};
    use crate::virtualization::VirtualizationMode;

    fn metadata() -> SandboxMetadata {
        SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "vmlinux-6.1.175".to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            resources: SandboxResources {
                cpu_count: 2,
                memory_mib: 2048,
                disk_size_mib: 8192,
            },
            ..SandboxMetadata::default()
        }
    }

    fn record() -> MobilityRecord {
        record_for(&metadata())
    }

    fn record_for(metadata: &SandboxMetadata) -> MobilityRecord {
        MobilityRecord::for_paused(
            metadata,
            "node-a",
            "x86_64",
            Some("{}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        )
    }

    async fn store() -> (LocalMobilityStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalMobilityStore::open(dir.path().join("mobility"))
            .await
            .expect("open mobility store");
        (store, dir)
    }

    #[test]
    fn generations_order_and_do_not_supersede_themselves() {
        let first = MobilityGeneration::now();
        let second = MobilityGeneration::now();
        assert!(second.supersedes(&first));
        assert!(!first.supersedes(&second));
        assert!(!first.supersedes(&first));
    }

    #[test]
    fn a_paused_record_carries_the_runtime_the_guest_was_booted_against() {
        let metadata = metadata();
        let record = record_for(&metadata);
        assert_eq!(record.sandbox_id, metadata.id);
        assert_eq!(record.resources, metadata.resources);
        assert_eq!(record.fingerprint.kernel_version, "vmlinux-6.1.175");
        assert_eq!(record.fingerprint.cpu_architecture, "x86_64");
        assert_eq!(record.state, MobilityState::Parked);
    }

    #[tokio::test]
    async fn records_round_trip_through_the_store() {
        let (store, _dir) = store().await;
        let record = record();

        assert_eq!(
            store.upsert(&record).await.expect("upsert"),
            MobilityWrite::Applied
        );
        assert_eq!(
            store.get(&record.sandbox_id).await.expect("get"),
            Some(record.clone())
        );
        assert_eq!(store.list().await.expect("list"), vec![record.clone()]);

        store.remove(&record.sandbox_id).await.expect("remove");
        assert_eq!(store.get(&record.sandbox_id).await.expect("get"), None);
        assert!(store.list().await.expect("list").is_empty());
    }

    /// A late write from a superseded task must not resurrect stale state — for
    /// instance re-parking a sandbox that has since been claimed.
    #[tokio::test]
    async fn an_older_generation_does_not_overwrite_a_newer_one() {
        let (store, _dir) = store().await;
        let stale = record();
        let claimed = stale.transitioned_to(MobilityState::Claimed {
            by_node_id: "node-b".to_string(),
            at_unix_ms: 1,
        });

        store.upsert(&claimed).await.expect("upsert claimed");
        assert_eq!(
            store.upsert(&stale).await.expect("upsert stale"),
            MobilityWrite::Superseded
        );
        assert_eq!(
            store
                .get(&stale.sandbox_id)
                .await
                .expect("get")
                .expect("record")
                .state,
            claimed.state,
            "the newer state must survive the late write"
        );
    }

    /// Rewriting under the same generation is refused too: allowing it would
    /// make the order decide nothing.
    #[tokio::test]
    async fn the_same_generation_does_not_overwrite() {
        let (store, _dir) = store().await;
        let record = record();
        store.upsert(&record).await.expect("upsert");

        let mut same_generation = record.clone();
        same_generation.state = MobilityState::Evacuated {
            to_node_id: "node-b".to_string(),
            at_unix_ms: 2,
        };
        assert_eq!(
            store.upsert(&same_generation).await.expect("upsert"),
            MobilityWrite::Superseded
        );
    }

    /// The paused state of an uncommitted sandbox is a set of runtime files on
    /// the origin. No amount of runtime compatibility makes those readable
    /// elsewhere, so the record must refuse before a destination is chosen.
    #[test]
    fn an_uncommitted_sandbox_cannot_move() {
        let mut record = record();
        record.snapshot_id = None;
        let host = record.fingerprint.clone();

        let blocker = record
            .can_move_to(&host, &[], &[])
            .expect_err("uncommitted paused state");
        assert_eq!(blocker.kind(), "artifacts");
        assert!(
            blocker.to_string().contains("committed"),
            "unexpected: {blocker}"
        );
    }

    #[test]
    fn a_committed_sandbox_moves_to_a_matching_host() {
        let record = record();
        let host = record.fingerprint.clone();
        assert_eq!(record.can_move_to(&host, &[], &[]), Ok(()));

        let mismatched = MigrationFingerprint {
            kernel_version: "vmlinux-6.1.190".to_string(),
            ..host.clone()
        };
        assert_eq!(
            record
                .can_move_to(&mismatched, &[], &[])
                .expect_err("kernel mismatch")
                .kind(),
            "kernel_version"
        );
    }

    /// Reach is carried on the record rather than recomputed at the
    /// destination: only the origin knows which repository backend wrote the
    /// artifacts.
    #[test]
    fn a_node_local_repository_blocks_the_move() {
        let mut record = record();
        record.artifact_reach = ArtifactReach::NodeLocal;
        let host = record.fingerprint.clone();

        let layers = vec![OverlaybdLayerRef::Managed(ManagedLayer {
            digest: "sha256:a".to_string(),
            size: 1,
            uuid: None,
        })];
        assert_eq!(
            record
                .can_move_to(&host, &layers, &[])
                .expect_err("node-local layers")
                .kind(),
            "artifacts"
        );
    }

    /// The tombstone must say where the sandbox went. "Unknown sandbox" is
    /// indistinguishable from a lost record, and a late claimant needs to tell
    /// those apart.
    #[tokio::test]
    async fn an_evacuated_record_names_its_destination() {
        let (store, _dir) = store().await;
        let record = record();
        store.upsert(&record).await.expect("upsert");

        let evacuated = record.transitioned_to(MobilityState::Evacuated {
            to_node_id: "node-b".to_string(),
            at_unix_ms: 7,
        });
        store.upsert(&evacuated).await.expect("upsert evacuated");

        let stored = store
            .get(&record.sandbox_id)
            .await
            .expect("get")
            .expect("record");
        assert!(matches!(
            stored.state,
            MobilityState::Evacuated { ref to_node_id, .. } if to_node_id == "node-b"
        ));
    }
}
