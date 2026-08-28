//! Node-level observability primitives.
//!
//! This module combines:
//! - static node identity and build metadata
//! - static machine information detected from the host
//! - request-time host resource collection
//! - orchestrator-published runtime counters
//!
//! The resulting [`NodeSnapshot`] is used by the admin/node APIs so requests can
//! read an already-projected view of node state without rescanning all
//! sandboxes on every call.

mod host;
mod kill_switch;
mod machine;
mod model;
pub mod prometheus;
mod reporter;
mod roster;
mod service;

pub use host::{DiskMetric, HostMetrics, HostMetricsCollector};
pub use kill_switch::{global_kill_switch, KillSwitch, KillSwitchAction};
pub use model::{MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
pub use reporter::ObservabilityReporter;
pub use roster::{roster_digest, RosterDigestState, RosterReport};
pub use service::ObservabilityService;
