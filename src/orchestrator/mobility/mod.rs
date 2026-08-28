//! Moving a paused sandbox to another node.
//!
//! [`record`] is what a destination needs to know before it decides; [`claim`]
//! is how the two nodes agree that exactly one of them owns the sandbox while
//! the handover is in flight.

mod claim;
mod record;

pub use claim::{ClaimOutcome, MobilityCoordinator, ResumeFence, DEFAULT_CLAIM_TTL};
pub use record::{
    LocalMobilityStore, MobilityGeneration, MobilityRecord, MobilityState, MobilityStore,
    MobilityWrite,
};
