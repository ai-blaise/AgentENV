//! AgentENV's latency-sensitive, horizontally scalable control-plane core.

pub mod artifact;
pub mod assignment;
pub mod model;
pub mod placement;
pub mod redis_store;
pub mod registry;
pub mod service;

pub mod proto {
    tonic::include_proto!("scheduler.v1");
}

pub use artifact::ArtifactIndex;
pub use assignment::{
    AssignmentStore, ClaimOutcome, ClaimRequest, InMemoryAssignmentStore, LifecycleBatch,
    LifecycleEvent, LifecycleEventKind, ReconcileRequest, ReconcileResult, StoreError,
};
pub use model::{
    Assignment, AssignmentState, CapacityLimits, Node, NodeObservation, NodeResources,
    PendingResources, PlacementConfig, SandboxResources,
};
pub use placement::{PlacementEngine, PlacementError};
pub use redis_store::RedisAssignmentStore;
pub use registry::{HeartbeatError, NodeRegistry};
pub use service::ControlPlane;
