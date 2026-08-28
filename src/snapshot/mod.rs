mod artifact_cache;
pub mod image_export;
mod manager;
pub mod mobility;
#[doc(hidden)]
pub mod mock;
mod p2p;
pub mod repository;
pub(crate) mod runtime_support;
pub mod sealing;
mod types;

pub use manager::SnapshotManager;
pub use mobility::{
    assess_mobility, classify_layers, ArtifactReach, DriveForMigration, DriveMobility,
    MigrationFingerprint, MigrationIncompatibility, MobilityBlocker,
};
pub use repository::{RepositoryError, RepositoryResult, SnapshotListFilter};
pub use sealing::{ArtifactSealingKey, SealScope};
pub(crate) use types::rootfs_snapshot_image_tag;
pub use types::{
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, ResolvedAttachedDrive, RunnableSnapshot,
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind, StartupCommand,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
