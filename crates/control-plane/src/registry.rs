use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::model::{Node, NodeObservation, PendingResources, PlacementConfig, SandboxResources};

#[derive(Debug, Error, Eq, PartialEq)]
pub enum HeartbeatError {
    #[error("node {0} is not registered")]
    UnknownNode(String),
    #[error("node ID and service instance ID must be non-empty")]
    MissingIdentity,
    #[error("pending resource accounting overflow for node {0}")]
    PendingOverflow(String),
    #[error("pending resource accounting underflow for node {0}")]
    PendingUnderflow(String),
}

#[derive(Clone, Debug)]
pub struct PlacementCandidate {
    pub node: Node,
    pub observation: NodeObservation,
    pub pending: PendingResources,
}

struct NodeRecord {
    node: Node,
    draining: AtomicBool,
    observation: RwLock<Option<NodeObservation>>,
    pending: Mutex<PendingLedger>,
}

#[derive(Clone, Copy)]
struct PendingReservation {
    resources: PendingResources,
    expires_at: Instant,
}

#[derive(Default)]
struct PendingLedger {
    total: PendingResources,
    by_sandbox: HashMap<String, PendingReservation>,
}

impl PendingLedger {
    fn add(
        &mut self,
        sandbox_id: &str,
        resources: PendingResources,
        expires_at: Instant,
    ) -> Option<()> {
        if let Some(previous) = self.by_sandbox.remove(sandbox_id) {
            self.total = self.total.checked_sub(previous.resources)?;
        }
        self.total = self.total.checked_add(resources)?;
        self.by_sandbox.insert(
            sandbox_id.to_string(),
            PendingReservation {
                resources,
                expires_at,
            },
        );
        Some(())
    }

    fn remove(&mut self, sandbox_id: &str) -> Option<()> {
        let Some(reservation) = self.by_sandbox.remove(sandbox_id) else {
            return Some(());
        };
        self.total = self.total.checked_sub(reservation.resources)?;
        Some(())
    }

    fn snapshot(&mut self, now: Instant) -> PendingResources {
        let expired = self
            .by_sandbox
            .iter()
            .filter(|(_, reservation)| reservation.expires_at <= now)
            .map(|(sandbox_id, _)| sandbox_id.clone())
            .collect::<Vec<_>>();
        for sandbox_id in expired {
            if self.remove(&sandbox_id).is_none() {
                self.total = PendingResources::default();
                self.by_sandbox.clear();
                break;
            }
        }
        self.total
    }
}

impl NodeRecord {
    fn new(node: Node, draining: bool) -> Self {
        Self {
            node,
            draining: AtomicBool::new(draining),
            observation: RwLock::new(None),
            pending: Mutex::new(PendingLedger::default()),
        }
    }
}

struct RegistryState {
    by_id: HashMap<String, Arc<NodeRecord>>,
    nodes: Arc<[Arc<NodeRecord>]>,
}

/// Read-optimized node registry. Placement clones one `Arc` and probes a
/// bounded number of nodes; it never scans the fleet on a request path.
pub struct NodeRegistry {
    state: RwLock<RegistryState>,
    sample_sequence: AtomicU64,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RegistryState {
                by_id: HashMap::new(),
                nodes: Arc::from([]),
            }),
            sample_sequence: AtomicU64::new(0x9e37_79b9_7f4a_7c15),
        }
    }

    /// Atomically replaces service-discovery state while retaining heartbeat
    /// observations for nodes whose identity and endpoint did not change.
    pub fn replace_discovered(&self, discovered: impl IntoIterator<Item = (Node, bool)>) {
        let mut state = self.state.write();
        let previous = &state.by_id;
        let mut by_id = HashMap::new();

        for (node, draining) in discovered {
            if node.id.trim().is_empty() || node.endpoint.trim().is_empty() {
                continue;
            }
            let record = previous
                .get(&node.id)
                .filter(|record| record.node.endpoint == node.endpoint)
                .cloned()
                .unwrap_or_else(|| Arc::new(NodeRecord::new(node.clone(), draining)));
            record.draining.store(draining, Ordering::Release);
            by_id.insert(node.id.clone(), record);
        }

        let mut nodes = by_id.values().cloned().collect::<Vec<_>>();
        nodes.sort_unstable_by(|left, right| left.node.id.cmp(&right.node.id));
        state.by_id = by_id;
        state.nodes = Arc::from(nodes);
    }

    pub fn heartbeat(
        &self,
        node_id: &str,
        observation: NodeObservation,
    ) -> Result<Node, HeartbeatError> {
        if node_id.trim().is_empty() || observation.service_instance_id.trim().is_empty() {
            return Err(HeartbeatError::MissingIdentity);
        }
        let record = self
            .state
            .read()
            .by_id
            .get(node_id)
            .cloned()
            .ok_or_else(|| HeartbeatError::UnknownNode(node_id.to_string()))?;

        let mut current = record.observation.write();
        if current
            .as_ref()
            .is_some_and(|old| old.service_instance_id != observation.service_instance_id)
        {
            *record.pending.lock() = PendingLedger::default();
        }
        *current = Some(observation);
        Ok(record.node.clone())
    }

    pub fn unregister(&self, node_id: &str, service_instance_id: &str) -> bool {
        let Some(record) = self.state.read().by_id.get(node_id).cloned() else {
            return false;
        };
        let mut observation = record.observation.write();
        if !observation
            .as_ref()
            .is_some_and(|current| current.service_instance_id == service_instance_id)
        {
            return false;
        }
        *observation = None;
        *record.pending.lock() = PendingLedger::default();
        true
    }

    pub fn resolve(&self, node_id: &str) -> Option<Node> {
        self.state
            .read()
            .by_id
            .get(node_id)
            .map(|record| record.node.clone())
    }

    pub fn list(&self, include_draining: bool) -> Vec<Node> {
        self.state
            .read()
            .nodes
            .iter()
            .filter(|record| include_draining || !record.draining.load(Ordering::Acquire))
            .map(|record| record.node.clone())
            .collect()
    }

    pub fn observation(&self, node_id: &str) -> Option<NodeObservation> {
        let record = self.state.read().by_id.get(node_id).cloned()?;
        let observation = record.observation.read().clone();
        observation
    }

    pub fn pending(&self, node_id: &str, now: Instant) -> Option<PendingResources> {
        let record = self.state.read().by_id.get(node_id).cloned()?;
        let pending = record.pending.lock().snapshot(now);
        Some(pending)
    }

    pub fn is_draining(&self, node_id: &str) -> Option<bool> {
        self.state
            .read()
            .by_id
            .get(node_id)
            .map(|record| record.draining.load(Ordering::Acquire))
    }

    pub fn add_pending(
        &self,
        sandbox_id: &str,
        node_id: &str,
        resources: SandboxResources,
        expires_at: Instant,
    ) -> Result<(), HeartbeatError> {
        let record = self
            .state
            .read()
            .by_id
            .get(node_id)
            .cloned()
            .ok_or_else(|| HeartbeatError::UnknownNode(node_id.to_string()))?;
        record
            .pending
            .lock()
            .add(
                sandbox_id,
                PendingResources::for_request(resources),
                expires_at,
            )
            .ok_or_else(|| HeartbeatError::PendingOverflow(node_id.to_string()))?;
        Ok(())
    }

    pub fn remove_pending(&self, sandbox_id: &str, node_id: &str) -> Result<(), HeartbeatError> {
        let record = self
            .state
            .read()
            .by_id
            .get(node_id)
            .cloned()
            .ok_or_else(|| HeartbeatError::UnknownNode(node_id.to_string()))?;
        record
            .pending
            .lock()
            .remove(sandbox_id)
            .ok_or_else(|| HeartbeatError::PendingUnderflow(node_id.to_string()))?;
        Ok(())
    }

    pub fn sample_eligible(
        &self,
        config: &PlacementConfig,
        request: SandboxResources,
        now: Instant,
        excluded: &HashSet<String>,
    ) -> Vec<PlacementCandidate> {
        let nodes = Arc::clone(&self.state.read().nodes);
        if nodes.is_empty() {
            return Vec::new();
        }

        let seed = mix64(
            self.sample_sequence
                .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed),
        );
        let start = seed as usize % nodes.len();
        let step = coprime_step(mix64(seed) as usize, nodes.len());
        let probes = config.probe_budget.min(nodes.len());
        let mut eligible = Vec::with_capacity(config.sample_size);

        for offset in 0..probes {
            let index = start.wrapping_add(offset.wrapping_mul(step)) % nodes.len();
            let record = &nodes[index];
            if excluded.contains(&record.node.id) || record.draining.load(Ordering::Acquire) {
                continue;
            }
            let Some(observation) = record.observation.read().clone() else {
                continue;
            };
            let pending = record.pending.lock().snapshot(now);
            if !is_eligible(config, &observation, pending, request, now) {
                continue;
            }
            eligible.push(PlacementCandidate {
                node: record.node.clone(),
                observation,
                pending,
            });
            if eligible.len() == config.sample_size {
                break;
            }
        }
        eligible
    }
}

fn is_eligible(
    config: &PlacementConfig,
    observation: &NodeObservation,
    pending: PendingResources,
    request: SandboxResources,
    now: Instant,
) -> bool {
    if !observation.ready
        || now.saturating_duration_since(observation.observed_at) > config.heartbeat_ttl
        || !matches_required(&config.required_version, &observation.version)
        || !matches_required(&config.required_commit, &observation.commit)
        || !matches_required(
            &config.required_cpu_architecture,
            &observation.cpu_architecture,
        )
    {
        return false;
    }

    let observed = PendingResources {
        sandboxes: observation
            .active_sandboxes
            .saturating_add(observation.paused_sandboxes),
        starting: observation.starting_sandboxes,
        cpu: observation.allocated_cpu,
        memory_bytes: observation.allocated_memory_bytes,
        disk_bytes: observation.disk_used_bytes,
    };
    let Some(after) = observed
        .checked_add(pending)
        .and_then(|current| current.checked_add(PendingResources::for_request(request)))
    else {
        return false;
    };

    let physical = crate::model::CapacityLimits {
        max_cpu: config.limits.max_cpu.or(Some(observation.cpu_count)),
        max_memory_bytes: config
            .limits
            .max_memory_bytes
            .or(Some(observation.memory_total_bytes)),
        max_disk_bytes: config
            .limits
            .max_disk_bytes
            .or(Some(observation.disk_total_bytes)),
        ..config.limits
    };
    physical.admits(after)
}

fn matches_required(required: &Option<String>, actual: &str) -> bool {
    required.as_ref().is_none_or(|required| required == actual)
}

fn coprime_step(seed: usize, length: usize) -> usize {
    if length <= 1 {
        return 1;
    }
    let mut step = seed % length;
    if step == 0 {
        step = 1;
    }
    while gcd(step, length) != 1 {
        step += 1;
        if step == length {
            step = 1;
        }
    }
    step
}

fn gcd(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::model::{CapacityLimits, PlacementConfig};

    fn observation(now: Instant) -> NodeObservation {
        NodeObservation {
            service_instance_id: "instance-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            version: "1.0.0".to_string(),
            commit: "abc".to_string(),
            cpu_architecture: "x86_64".to_string(),
            cpu_config_json: String::new(),
            p2p_backend: String::new(),
            p2p_address: String::new(),
            observed_at: now,
            reported_at_unix_ms: 1,
            ready: true,
            active_sandboxes: 0,
            paused_sandboxes: 0,
            starting_sandboxes: 0,
            allocated_cpu: 0,
            allocated_memory_bytes: 0,
            cpu_count: 8,
            memory_used_bytes: 0,
            memory_total_bytes: 16 * 1024 * 1024 * 1024,
            disk_used_bytes: 0,
            disk_total_bytes: 100 * 1024 * 1024 * 1024,
        }
    }

    #[test]
    fn strict_eligibility_rejects_missing_stale_draining_and_over_capacity_nodes() {
        let now = Instant::now();
        let registry = NodeRegistry::new();
        registry.replace_discovered([
            (Node::new("missing", "http://missing"), false),
            (Node::new("stale", "http://stale"), false),
            (Node::new("draining", "http://draining"), true),
            (Node::new("full", "http://full"), false),
            (Node::new("ready", "http://ready"), false),
        ]);

        let mut stale = observation(now - Duration::from_secs(31));
        stale.service_instance_id = "stale".to_string();
        registry.heartbeat("stale", stale).unwrap();
        registry.heartbeat("draining", observation(now)).unwrap();
        let mut full = observation(now);
        full.allocated_cpu = 8;
        registry.heartbeat("full", full).unwrap();
        registry.heartbeat("ready", observation(now)).unwrap();

        let config = PlacementConfig {
            sample_size: 2,
            probe_budget: 5,
            limits: CapacityLimits {
                max_cpu: Some(8),
                ..CapacityLimits::default()
            },
            ..PlacementConfig::default()
        };
        let candidates = registry.sample_eligible(
            &config,
            SandboxResources {
                cpu: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            now,
            &HashSet::new(),
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].node.id, "ready");
    }

    #[test]
    fn sampling_is_bounded_even_for_large_registries() {
        let registry = NodeRegistry::new();
        registry.replace_discovered((0..100_000).map(|index| {
            (
                Node::new(format!("node-{index}"), format!("http://node-{index}")),
                false,
            )
        }));
        let config = PlacementConfig {
            sample_size: 3,
            probe_budget: 32,
            ..PlacementConfig::default()
        };

        // No node has a heartbeat. A fleet scan would be needlessly expensive;
        // the bounded probe returns after inspecting at most 32 records.
        let candidates = registry.sample_eligible(
            &config,
            config.default_request,
            Instant::now(),
            &HashSet::new(),
        );
        assert!(candidates.is_empty());
    }
}
