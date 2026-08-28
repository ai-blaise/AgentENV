//! Moving a paused sandbox to another node.
//!
//! [`record`] is what a destination needs to know before it decides; [`claim`]
//! is how the two nodes agree that exactly one of them owns the sandbox while
//! the handover is in flight.

mod claim;
mod evacuation;
mod record;
mod saga;

pub use claim::{ClaimOutcome, MobilityCoordinator, ResumeFence, DEFAULT_CLAIM_TTL};
pub use evacuation::{
    drain, plan_evacuation, DestinationCandidate, DrainBudget, DrainReport, EvacuationPlan,
    MoveExecutor, PlannedMove, SandboxLayers, UnplaceableReason, UnplaceableSandbox,
};
pub use record::{
    LocalMobilityStore, MobilityGeneration, MobilityRecord, MobilityState, MobilityStore,
    MobilityWrite,
};
pub use saga::{MigrationOutcome, MigrationSaga, MigrationSteps};
