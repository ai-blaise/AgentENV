use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use thiserror::Error;

use crate::model::{
    Assignment, AssignmentState, CapacityLimits, Node, PendingResources, SandboxResources,
};

#[derive(Clone, Debug)]
pub struct ClaimRequest {
    pub sandbox_id: String,
    pub node: Node,
    pub resources: SandboxResources,
    /// Heartbeat-observed usage at candidate selection time. The durable store
    /// combines this with reservations from every scheduler replica.
    pub observed: PendingResources,
    pub limits: CapacityLimits,
    pub now: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimOutcome {
    Claimed(Assignment),
    Existing(Assignment),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum StoreError {
    #[error("assignment input is invalid: {0}")]
    Invalid(&'static str),
    #[error("node {node_id} has no capacity for the requested reservation")]
    CapacityExhausted { node_id: String },
    #[error("sandbox {sandbox_id} is assigned to node {assigned_node}, not {requested_node}")]
    OwnershipConflict {
        sandbox_id: String,
        assigned_node: String,
        requested_node: String,
    },
    #[error("assignment store invariant failed: {0}")]
    Invariant(String),
    #[error("assignment backend unavailable: {0}")]
    Backend(String),
}

#[async_trait]
pub trait AssignmentStore: Send + Sync + 'static {
    async fn lookup(
        &self,
        sandbox_id: &str,
        now: Instant,
    ) -> Result<Option<Assignment>, StoreError>;

    async fn claim(&self, request: ClaimRequest) -> Result<ClaimOutcome, StoreError>;

    async fn confirm(
        &self,
        sandbox_id: &str,
        node: Node,
        now: Instant,
    ) -> Result<Assignment, StoreError>;
}

#[derive(Clone)]
struct TimedAssignment {
    assignment: Assignment,
    expires_at: Instant,
}

#[derive(Clone, Copy)]
struct Reservation {
    resources: PendingResources,
}

#[derive(Default)]
struct InMemoryState {
    assignments: HashMap<String, TimedAssignment>,
    reservations: HashMap<String, Reservation>,
    pending_by_node: HashMap<String, PendingResources>,
}

/// Linearizable single-process assignment store used for tests and explicit
/// single-replica deployments. Multi-replica production deployments use the
/// Redis implementation so assignment claims remain atomic across processes.
pub struct InMemoryAssignmentStore {
    state: Mutex<InMemoryState>,
    reservation_ttl: Duration,
    confirmed_ttl: Duration,
}

impl InMemoryAssignmentStore {
    pub fn new(reservation_ttl: Duration, confirmed_ttl: Duration) -> Result<Self, StoreError> {
        if reservation_ttl.is_zero() {
            return Err(StoreError::Invalid("reservation_ttl must be non-zero"));
        }
        if confirmed_ttl < reservation_ttl {
            return Err(StoreError::Invalid(
                "confirmed_ttl must be at least reservation_ttl",
            ));
        }
        Ok(Self {
            state: Mutex::new(InMemoryState::default()),
            reservation_ttl,
            confirmed_ttl,
        })
    }

    pub fn pending_for_node(&self, node_id: &str, now: Instant) -> PendingResources {
        let mut state = self.state.lock();
        cleanup_expired(&mut state, now);
        state
            .pending_by_node
            .get(node_id)
            .copied()
            .unwrap_or_default()
    }
}

#[async_trait]
impl AssignmentStore for InMemoryAssignmentStore {
    async fn lookup(
        &self,
        sandbox_id: &str,
        now: Instant,
    ) -> Result<Option<Assignment>, StoreError> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(StoreError::Invalid("sandbox_id must be non-empty"));
        }
        let mut state = self.state.lock();
        cleanup_expired(&mut state, now);
        Ok(state
            .assignments
            .get(sandbox_id)
            .map(|timed| timed.assignment.clone()))
    }

    async fn claim(&self, request: ClaimRequest) -> Result<ClaimOutcome, StoreError> {
        validate_claim(&request)?;
        let mut state = self.state.lock();
        cleanup_expired(&mut state, request.now);

        if let Some(existing) = state.assignments.get(&request.sandbox_id) {
            return Ok(ClaimOutcome::Existing(existing.assignment.clone()));
        }

        let request_resources = PendingResources::for_request(request.resources);
        let pending = state
            .pending_by_node
            .get(&request.node.id)
            .copied()
            .unwrap_or_default();
        let Some(after) = request
            .observed
            .checked_add(pending)
            .and_then(|current| current.checked_add(request_resources))
        else {
            return Err(StoreError::CapacityExhausted {
                node_id: request.node.id,
            });
        };
        if !request.limits.admits(after) {
            return Err(StoreError::CapacityExhausted {
                node_id: request.node.id,
            });
        }

        let expires_at = request.now + self.reservation_ttl;
        let assignment = Assignment {
            sandbox_id: request.sandbox_id.clone(),
            node: request.node.clone(),
            state: AssignmentState::Reserved,
        };
        state.assignments.insert(
            request.sandbox_id.clone(),
            TimedAssignment {
                assignment: assignment.clone(),
                expires_at,
            },
        );
        state.reservations.insert(
            request.sandbox_id,
            Reservation {
                resources: request_resources,
            },
        );
        state
            .pending_by_node
            .insert(request.node.id, after_pending(pending, request_resources)?);
        Ok(ClaimOutcome::Claimed(assignment))
    }

    async fn confirm(
        &self,
        sandbox_id: &str,
        node: Node,
        now: Instant,
    ) -> Result<Assignment, StoreError> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() || node.id.trim().is_empty() || node.endpoint.trim().is_empty() {
            return Err(StoreError::Invalid(
                "sandbox_id, node_id, and endpoint must be non-empty",
            ));
        }
        let mut state = self.state.lock();
        cleanup_expired(&mut state, now);

        if let Some(existing) = state.assignments.get(sandbox_id) {
            if existing.assignment.node.id != node.id {
                return Err(StoreError::OwnershipConflict {
                    sandbox_id: sandbox_id.to_string(),
                    assigned_node: existing.assignment.node.id.clone(),
                    requested_node: node.id,
                });
            }
        }

        release_reservation(&mut state, sandbox_id)?;
        let generation = state
            .assignments
            .get(sandbox_id)
            .map_or(node.generation.max(1), |existing| {
                existing.assignment.node.generation.max(1)
            });
        let assignment = Assignment {
            sandbox_id: sandbox_id.to_string(),
            node: Node { generation, ..node },
            state: AssignmentState::Confirmed,
        };
        state.assignments.insert(
            sandbox_id.to_string(),
            TimedAssignment {
                assignment: assignment.clone(),
                expires_at: now + self.confirmed_ttl,
            },
        );
        Ok(assignment)
    }
}

fn validate_claim(request: &ClaimRequest) -> Result<(), StoreError> {
    if request.sandbox_id.trim().is_empty() {
        return Err(StoreError::Invalid("sandbox_id must be non-empty"));
    }
    if request.node.id.trim().is_empty() || request.node.endpoint.trim().is_empty() {
        return Err(StoreError::Invalid(
            "node_id and endpoint must be non-empty",
        ));
    }
    if request.resources.cpu == 0 || request.resources.memory_bytes == 0 {
        return Err(StoreError::Invalid(
            "requested CPU and memory must be greater than zero",
        ));
    }
    Ok(())
}

fn after_pending(
    current: PendingResources,
    added: PendingResources,
) -> Result<PendingResources, StoreError> {
    current
        .checked_add(added)
        .ok_or_else(|| StoreError::Invariant("pending resources overflowed".to_string()))
}

fn cleanup_expired(state: &mut InMemoryState, now: Instant) {
    let expired = state
        .assignments
        .iter()
        .filter(|(_, assignment)| assignment.expires_at <= now)
        .map(|(sandbox_id, _)| sandbox_id.clone())
        .collect::<Vec<_>>();
    for sandbox_id in expired {
        // Expiration cleanup is best effort only in this test backend. Any
        // underflow indicates prior corruption; retain the conservative total.
        let _ = release_reservation(state, &sandbox_id);
        state.assignments.remove(&sandbox_id);
    }
}

fn release_reservation(state: &mut InMemoryState, sandbox_id: &str) -> Result<(), StoreError> {
    let Some(reservation) = state.reservations.remove(sandbox_id) else {
        return Ok(());
    };
    let Some(assignment) = state.assignments.get(sandbox_id) else {
        return Err(StoreError::Invariant(format!(
            "reservation {sandbox_id} has no assignment"
        )));
    };
    let node_id = &assignment.assignment.node.id;
    let current = state.pending_by_node.get(node_id).copied().ok_or_else(|| {
        StoreError::Invariant(format!("reservation {sandbox_id} has no node total"))
    })?;
    let remaining = current.checked_sub(reservation.resources).ok_or_else(|| {
        StoreError::Invariant(format!("reservation {sandbox_id} underflowed node total"))
    })?;
    if remaining == PendingResources::default() {
        state.pending_by_node.remove(node_id);
    } else {
        state.pending_by_node.insert(node_id.clone(), remaining);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn claim(sandbox_id: &str, node_id: &str, now: Instant) -> ClaimRequest {
        ClaimRequest {
            sandbox_id: sandbox_id.to_string(),
            node: Node::new(node_id, format!("http://{node_id}")),
            resources: SandboxResources {
                cpu: 2,
                memory_bytes: 1024,
                disk_bytes: 2048,
            },
            observed: PendingResources::default(),
            limits: CapacityLimits {
                max_sandboxes: Some(2),
                max_starting: Some(2),
                max_cpu: Some(4),
                max_memory_bytes: Some(2048),
                max_disk_bytes: Some(4096),
            },
            now,
        }
    }

    #[tokio::test]
    async fn concurrent_claims_for_one_id_have_one_owner() {
        let now = Instant::now();
        let store = Arc::new(
            InMemoryAssignmentStore::new(Duration::from_secs(30), Duration::from_secs(60)).unwrap(),
        );
        let (left, right) = tokio::join!(
            store.claim(claim("sandbox", "node-a", now)),
            store.claim(claim("sandbox", "node-b", now)),
        );
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ClaimOutcome::Claimed(_)))
                .count(),
            1
        );
        assert_eq!(outcomes[0].assignment(), outcomes[1].assignment());
    }

    #[tokio::test]
    async fn reservations_enforce_capacity_and_release_on_expiration() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        store.claim(claim("first", "node-a", now)).await.unwrap();
        store.claim(claim("second", "node-a", now)).await.unwrap();
        let denied = store
            .claim(claim("third", "node-a", now))
            .await
            .unwrap_err();
        assert!(matches!(denied, StoreError::CapacityExhausted { .. }));

        store
            .claim(claim("third", "node-a", now + Duration::from_secs(11)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn confirmation_releases_pending_and_fences_wrong_node() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        store.claim(claim("sandbox", "node-a", now)).await.unwrap();
        let conflict = store
            .confirm("sandbox", Node::new("node-b", "http://node-b"), now)
            .await
            .unwrap_err();
        assert!(matches!(conflict, StoreError::OwnershipConflict { .. }));

        let confirmed = store
            .confirm("sandbox", Node::new("node-a", "http://node-a"), now)
            .await
            .unwrap();
        assert_eq!(confirmed.state, AssignmentState::Confirmed);
        assert_eq!(
            store.pending_for_node("node-a", now),
            PendingResources::default()
        );
    }

    impl ClaimOutcome {
        fn assignment(&self) -> &Assignment {
            match self {
                Self::Claimed(assignment) | Self::Existing(assignment) => assignment,
            }
        }
    }
}
