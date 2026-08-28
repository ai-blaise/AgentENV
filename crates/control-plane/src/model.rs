use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Immutable routing identity returned to gateways.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub endpoint: String,
    pub generation: u64,
}

impl Node {
    pub fn new(id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            generation: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxResources {
    pub cpu: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PendingResources {
    pub sandboxes: u32,
    pub starting: u32,
    pub cpu: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl PendingResources {
    pub fn for_request(request: SandboxResources) -> Self {
        Self {
            sandboxes: 1,
            starting: 1,
            cpu: u64::from(request.cpu),
            memory_bytes: request.memory_bytes,
            disk_bytes: request.disk_bytes,
        }
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            sandboxes: self.sandboxes.checked_add(other.sandboxes)?,
            starting: self.starting.checked_add(other.starting)?,
            cpu: self.cpu.checked_add(other.cpu)?,
            memory_bytes: self.memory_bytes.checked_add(other.memory_bytes)?,
            disk_bytes: self.disk_bytes.checked_add(other.disk_bytes)?,
        })
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            sandboxes: self.sandboxes.checked_sub(other.sandboxes)?,
            starting: self.starting.checked_sub(other.starting)?,
            cpu: self.cpu.checked_sub(other.cpu)?,
            memory_bytes: self.memory_bytes.checked_sub(other.memory_bytes)?,
            disk_bytes: self.disk_bytes.checked_sub(other.disk_bytes)?,
        })
    }
}

/// Latest trustworthy runtime state for a discovered node.
#[derive(Clone, Debug)]
pub struct NodeObservation {
    pub service_instance_id: String,
    pub cluster_id: String,
    pub version: String,
    pub commit: String,
    pub cpu_architecture: String,
    pub cpu_config_json: String,
    pub p2p_backend: String,
    pub p2p_address: String,
    pub observed_at: Instant,
    pub reported_at_unix_ms: i64,
    pub ready: bool,
    pub active_sandboxes: u32,
    pub paused_sandboxes: u32,
    pub starting_sandboxes: u32,
    pub allocated_cpu: u64,
    pub allocated_memory_bytes: u64,
    pub cpu_count: u64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub lifecycle_stream_id: String,
    pub lifecycle_last_sequence: u64,
}

impl NodeObservation {
    pub fn total_sandboxes(&self) -> Option<u64> {
        u64::from(self.active_sandboxes).checked_add(u64::from(self.paused_sandboxes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeResources {
    pub observed: PendingResources,
    pub pending: PendingResources,
}

impl NodeResources {
    pub fn after_request(&self, request: SandboxResources) -> Option<PendingResources> {
        self.observed
            .checked_add(self.pending)?
            .checked_add(PendingResources::for_request(request))
    }
}

/// Hard post-admission ceilings. `None` disables an individual ceiling.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapacityLimits {
    pub max_sandboxes: Option<u64>,
    pub max_starting: Option<u64>,
    pub max_cpu: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_disk_bytes: Option<u64>,
}

impl CapacityLimits {
    pub fn admits(&self, resources: PendingResources) -> bool {
        within(resources.sandboxes.into(), self.max_sandboxes)
            && within(resources.starting.into(), self.max_starting)
            && within(resources.cpu, self.max_cpu)
            && within(resources.memory_bytes, self.max_memory_bytes)
            && within(resources.disk_bytes, self.max_disk_bytes)
    }
}

fn within(value: u64, limit: Option<u64>) -> bool {
    limit.is_none_or(|limit| value <= limit)
}

#[derive(Clone, Debug)]
pub struct PlacementConfig {
    pub heartbeat_ttl: Duration,
    pub sample_size: usize,
    pub probe_budget: usize,
    pub required_version: Option<String>,
    pub required_commit: Option<String>,
    pub required_cpu_architecture: Option<String>,
    pub limits: CapacityLimits,
    pub default_request: SandboxResources,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            heartbeat_ttl: Duration::from_secs(30),
            sample_size: 3,
            probe_budget: 32,
            required_version: None,
            required_commit: None,
            required_cpu_architecture: None,
            limits: CapacityLimits::default(),
            default_request: SandboxResources {
                cpu: 1,
                memory_bytes: 512 * 1024 * 1024,
                disk_bytes: 8 * 1024 * 1024 * 1024,
            },
        }
    }
}

impl PlacementConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.heartbeat_ttl.is_zero() {
            return Err("heartbeat_ttl must be greater than zero");
        }
        if self.sample_size < 2 {
            return Err("sample_size must be at least 2");
        }
        if self.probe_budget < self.sample_size {
            return Err("probe_budget must be at least sample_size");
        }
        if self.default_request.cpu == 0 || self.default_request.memory_bytes == 0 {
            return Err("default request CPU and memory must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentState {
    Reserved,
    Confirmed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub sandbox_id: String,
    pub node: Node,
    pub state: AssignmentState,
}
