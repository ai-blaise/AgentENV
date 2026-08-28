//! Moving a paused sandbox to another node.
//!
//! [`record`] is what a destination needs to know before it decides; [`claim`]
//! is how the two nodes agree that exactly one of them owns the sandbox while
//! the handover is in flight.

mod claim;
mod evacuation;
mod lease;
mod record;
mod runtime;
mod saga;
mod scheduler_store;

pub use claim::{ClaimOutcome, MobilityCoordinator, ResumeFence, DEFAULT_CLAIM_TTL};
pub use evacuation::{
    drain, plan_evacuation, DestinationCandidate, DrainBudget, DrainReport, EvacuationPlan,
    MoveExecutor, PlannedMove, SandboxLayers, UnplaceableReason, UnplaceableSandbox,
};
pub use lease::{LeaseGuardian, LeaseLost, LeasePacing, LeaseWatch, RenewOutcome};
pub use record::{
    LocalMobilityStore, MobilityGeneration, MobilityRecord, MobilityState, MobilityStore,
    MobilityWrite,
};
pub use runtime::{
    mobility_runtime_with_store, open_mobility_runtime, MobilityHooks, MobilityRecordCounts,
    MobilityRuntime, NodeMobilityFacts,
};
pub use saga::{MigrationOutcome, MigrationSaga, MigrationSteps};
pub use scheduler_store::{scheduler_mobility_store, SchedulerMobilityStore};
