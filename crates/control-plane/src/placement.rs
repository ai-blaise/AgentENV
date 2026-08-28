use std::collections::HashSet;
use std::time::Instant;

use thiserror::Error;

use crate::model::{CapacityLimits, Node, PendingResources, PlacementConfig, SandboxResources};
use crate::registry::{NodeRegistry, PlacementCandidate};

const SCORE_SCALE: u128 = 1_000_000;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PlacementError {
    #[error("no eligible nodes were found within the bounded probe budget")]
    NoEligibleNodes,
}

pub struct PlacementEngine {
    config: PlacementConfig,
}

impl PlacementEngine {
    pub fn new(config: PlacementConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &PlacementConfig {
        &self.config
    }

    pub fn select(
        &self,
        registry: &NodeRegistry,
        request: SandboxResources,
        now: Instant,
        excluded: &HashSet<String>,
    ) -> Result<Node, PlacementError> {
        registry
            .sample_eligible(&self.config, request, now, excluded)
            .into_iter()
            .min_by(|left, right| {
                candidate_score(left, &self.config.limits, request)
                    .cmp(&candidate_score(right, &self.config.limits, request))
                    .then_with(|| left.node.id.cmp(&right.node.id))
            })
            .map(|candidate| candidate.node)
            .ok_or(PlacementError::NoEligibleNodes)
    }
}

fn candidate_score(
    candidate: &PlacementCandidate,
    configured: &CapacityLimits,
    request: SandboxResources,
) -> u128 {
    let observation = &candidate.observation;
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
        .checked_add(candidate.pending)
        .and_then(|current| current.checked_add(PendingResources::for_request(request)))
    else {
        return u128::MAX;
    };

    let mut score = 0;
    score = score.max(ratio(
        after.cpu,
        configured.max_cpu.or(Some(observation.cpu_count)),
    ));
    score = score.max(ratio(
        after.memory_bytes,
        configured
            .max_memory_bytes
            .or(Some(observation.memory_total_bytes)),
    ));
    score = score.max(ratio(
        after.disk_bytes,
        configured
            .max_disk_bytes
            .or(Some(observation.disk_total_bytes)),
    ));
    score = score.max(ratio(u64::from(after.sandboxes), configured.max_sandboxes));
    score.max(ratio(u64::from(after.starting), configured.max_starting))
}

fn ratio(value: u64, limit: Option<u64>) -> u128 {
    match limit {
        Some(0) => u128::MAX,
        Some(limit) => u128::from(value)
            .saturating_mul(SCORE_SCALE)
            .checked_div(u128::from(limit))
            .unwrap_or(u128::MAX),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::model::NodeObservation;

    fn observed(now: Instant, allocated_cpu: u64) -> NodeObservation {
        NodeObservation {
            service_instance_id: "instance".to_string(),
            cluster_id: "cluster".to_string(),
            version: "1".to_string(),
            commit: "abc".to_string(),
            cpu_architecture: "x86_64".to_string(),
            cpu_config_json: String::new(),
            p2p_backend: String::new(),
            p2p_address: String::new(),
            observed_at: now,
            reported_at_unix_ms: 1,
            ready: true,
            active_sandboxes: allocated_cpu as u32,
            paused_sandboxes: 0,
            starting_sandboxes: 0,
            allocated_cpu,
            allocated_memory_bytes: allocated_cpu * 1024,
            cpu_count: 100,
            memory_used_bytes: 0,
            memory_total_bytes: 100 * 1024,
            disk_used_bytes: allocated_cpu * 1024,
            disk_total_bytes: 100 * 1024,
            lifecycle_stream_id: String::new(),
            lifecycle_last_sequence: 0,
            migration_capabilities: crate::model::MigrationCapabilities::default(),
        }
    }

    #[test]
    fn power_of_n_prefers_the_least_loaded_sampled_node() {
        let now = Instant::now();
        let registry = NodeRegistry::new();
        registry.replace_discovered([
            (Node::new("node-a", "http://a"), false),
            (Node::new("node-b", "http://b"), false),
            (Node::new("node-c", "http://c"), false),
        ]);
        registry.heartbeat("node-a", observed(now, 80)).unwrap();
        registry.heartbeat("node-b", observed(now, 10)).unwrap();
        registry.heartbeat("node-c", observed(now, 50)).unwrap();

        let engine = PlacementEngine::new(PlacementConfig {
            heartbeat_ttl: Duration::from_secs(30),
            sample_size: 3,
            probe_budget: 3,
            default_request: SandboxResources {
                cpu: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            ..PlacementConfig::default()
        })
        .unwrap();
        let selected = engine
            .select(
                &registry,
                engine.config.default_request,
                now,
                &HashSet::new(),
            )
            .unwrap();

        assert_eq!(selected.id, "node-b");
    }
}
