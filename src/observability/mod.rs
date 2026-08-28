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

/// This host's page size, or `0` when it cannot be determined.
///
/// Part of a snapshot's compatibility fingerprint: a memory image's layout and
/// its dirty-page tracking are both expressed in pages, so a destination with
/// a different size cannot restore it.
///
/// A failure reports `0` rather than assuming 4 KiB. Assuming is the wrong
/// direction: almost every host really is 4 KiB, so a guessed 4096 would match
/// the other side and *permit* a migration decided on a value nobody actually
/// read. `0` matches nothing and is rejected by the fingerprint, so an
/// unreadable page size refuses the move instead of waving it through.
pub fn host_page_size() -> u32 {
    // SAFETY: `sysconf` is a pure query with no preconditions.
    let size = unsafe { nix::libc::sysconf(nix::libc::_SC_PAGESIZE) };
    if size <= 0 {
        return 0;
    }
    u32::try_from(size).unwrap_or(0)
}
pub use reporter::ObservabilityReporter;
pub use roster::{roster_digest, RosterDigestState, RosterReport};
pub use service::ObservabilityService;
