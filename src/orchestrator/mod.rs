mod admission;
mod launch_plan;
mod metrics;
mod mobility;
mod persistence;
mod proxy;
mod service;
mod store;
mod types;

use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

pub use admission::{
    global_admission_controller, AdmissionController, AdmissionGuard, AdmissionRejectReason,
    NodeCapacityInputs,
};
pub use metrics::OrchestratorMetrics;
pub use mobility::{
    ClaimOutcome, LocalMobilityStore, MobilityCoordinator, MobilityGeneration, MobilityRecord,
    MobilityState, MobilityStore, MobilityWrite, ResumeFence, DEFAULT_CLAIM_TTL,
};
pub use persistence::{
    DisabledSandboxPersister, FileBackedSandboxPersister, PersistenceResult,
    SandboxPersistenceError, SandboxPersister,
};
pub use proxy::{ProxyLookupResult, ProxyTarget};
pub use service::Orchestrator;
pub use store::{
    InMemoryMetadataStore, MetadataStore, NewTimeout, SandboxListFilter, SandboxMetadata,
    SandboxTimeoutAction,
};
pub use types::{
    CreateSandboxRequest, SandboxLaunchSource, SandboxLifecycleEvent, SandboxLifecycleEventType,
    SandboxState, SnapshotCaptureResult,
};

pub type Result<T> = std::result::Result<T, OrchestratorError>;
pub type SandboxForkOutcome = std::result::Result<SandboxMetadata, OrchestratorError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxOperation {
    Build,
    Start,
    WaitReady,
    Pause,
    Resume,
    Snapshot,
    Fork,
    UpdateNetwork,
    PatchCustomExtensionParams,
    Stop,
}

#[derive(thiserror::Error, Debug)]
pub enum OrchestratorError {
    #[error("failed to load sandbox config")]
    ConfigLoadFailed(#[from] anyhow::Error),

    #[error(
        "{resource} uses virtualization mode '{resource_mode}', but this node runs in mode '{node_mode}'"
    )]
    VirtualizationModeMismatch {
        resource: String,
        resource_mode: VirtualizationMode,
        node_mode: VirtualizationMode,
    },

    #[error("orchestrator is shutting down")]
    ShuttingDown,

    /// The node has lost contact with the scheduler and is refusing new work.
    ///
    /// Distinct from `AdmissionRejected`: that means "I am full", this means
    /// "I cannot be reached, so I should not be accepting placements I may
    /// never be told to release".
    #[error("node has lost contact with the scheduler for {elapsed_secs}s and is not accepting new sandboxes")]
    SchedulerContactLost { elapsed_secs: u64 },

    /// The node refused to start the sandbox from its own view of capacity.
    ///
    /// Placement is advisory and necessarily stale; this is the authoritative
    /// answer. It is a retryable condition, not a fault: the caller should try
    /// another node rather than surface a failure.
    #[error("node at capacity: {reason}")]
    AdmissionRejected {
        reason: AdmissionRejectReason,
        retry_after: std::time::Duration,
    },

    #[error("sandbox {0} not found")]
    SandboxNotFound(SandboxId),

    #[error("sandbox {sandbox_id} is in invalid state {state:?}")]
    InvalidSandboxState {
        sandbox_id: SandboxId,
        state: SandboxState,
    },

    #[error("sandbox {sandbox_id} operation {operation:?} failed: {source}")]
    SandboxOperationFailed {
        sandbox_id: SandboxId,
        operation: SandboxOperation,
        #[source]
        source: anyhow::Error,
    },

    #[error("sandbox {sandbox_id} operation {operation:?} conflicted with another operation")]
    SandboxOperationConflict {
        sandbox_id: SandboxId,
        operation: SandboxOperation,
    },

    #[error("store operation failed: {0}")]
    StoreOperationFailed(#[source] store::StoreError),

    #[error("sandbox persistence failed: {0}")]
    SandboxPersistenceFailed(#[from] SandboxPersistenceError),

    #[error("invalid timeout for {sandbox_id}: {timeout}")]
    InvalidTimeout {
        sandbox_id: SandboxId,
        timeout: String,
    },

    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<store::StoreError> for OrchestratorError {
    fn from(value: store::StoreError) -> Self {
        match value {
            store::StoreError::SandboxNotFound { sandbox_id } => {
                OrchestratorError::SandboxNotFound(sandbox_id)
            }
            other => OrchestratorError::StoreOperationFailed(other),
        }
    }
}

impl From<OrchestratorError> for String {
    fn from(err: OrchestratorError) -> Self {
        err.to_string()
    }
}
