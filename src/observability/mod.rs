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

/// This host's CPU architecture, as a snapshot records it.
pub fn detect_cpu_architecture() -> String {
    std::env::consts::ARCH.to_string()
}

/// This host's page size.
///
/// Part of a snapshot's compatibility fingerprint: a memory image's layout and
/// its dirty-page tracking are both expressed in pages, so a destination with
/// a different size cannot restore it. Falls back to 4 KiB, which is what
/// every platform AgentENV runs on actually uses — a wrong answer here makes
/// migration refuse rather than misbehave.
pub fn host_page_size() -> u32 {
    // SAFETY: `sysconf` is a pure query with no preconditions.
    let size = unsafe { nix::libc::sysconf(nix::libc::_SC_PAGESIZE) };
    u32::try_from(size).unwrap_or(4096).max(1)
}
pub use reporter::ObservabilityReporter;
pub use roster::{roster_digest, RosterDigestState, RosterReport};
pub use service::ObservabilityService;
