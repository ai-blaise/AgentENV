use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventKind {
    Create,
    Delete,
    Pause,
    Resume,
    Fork,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub sandbox_id: String,
    pub kind: LifecycleEventKind,
    pub resources: SandboxResources,
    pub sequence: u64,
    pub event_id: String,
    pub occurred_at_unix_ms: i64,
}

#[derive(Clone, Debug)]
pub struct LifecycleBatch {
    pub node: Node,
    pub service_instance_id: String,
    pub stream_id: String,
    pub events: Vec<LifecycleEvent>,
    pub now: Instant,
}

#[derive(Clone, Debug)]
pub struct ReconcileRequest {
    pub node: Node,
    pub sandbox_ids: Vec<String>,
    pub missing_heartbeat_threshold: u8,
    pub now: Instant,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileResult {
    pub repaired: u64,
    pub removed: u64,
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
    #[error("sandbox {sandbox_id} has been retired and its ID cannot be reused")]
    Retired { sandbox_id: String },
    #[error("assignment store invariant failed: {0}")]
    Invariant(String),
    #[error("lifecycle stream conflict: {0}")]
    SequenceConflict(String),
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

    /// Atomically advance a node event cursor and materialize its route changes.
    async fn apply_lifecycle_batch(&self, batch: LifecycleBatch) -> Result<u64, StoreError>;

    /// Repair missing routes from a full node inventory and remove confirmed
    /// routes only after they are absent from multiple consecutive heartbeats.
    async fn reconcile_node(
        &self,
        request: ReconcileRequest,
    ) -> Result<ReconcileResult, StoreError>;
}

#[derive(Clone)]
struct TimedAssignment {
    assignment: Assignment,
    expires_at: Option<Instant>,
}

#[derive(Clone)]
struct Reservation {
    resources: PendingResources,
    expires_at: Instant,
}

#[derive(Default)]
struct InMemoryState {
    assignments: HashMap<String, TimedAssignment>,
    reservations: HashMap<String, Reservation>,
    pending_by_node: HashMap<String, PendingResources>,
    lifecycle_cursors: HashMap<String, LifecycleCursor>,
    reconcile_misses: HashMap<(String, String), u8>,
    fences: HashMap<String, FenceRecord>,
}

#[derive(Clone, Debug)]
struct FenceRecord {
    owner_node_id: String,
    generation: u64,
    retired: bool,
}

#[derive(Clone, Debug)]
struct LifecycleCursor {
    stream_id: String,
    sequence: u64,
}

/// Linearizable single-process assignment store used for tests and explicit
/// single-replica deployments. Multi-replica production deployments use the
/// Redis implementation so assignment claims remain atomic across processes.
pub struct InMemoryAssignmentStore {
    state: Mutex<InMemoryState>,
    reservation_ttl: Duration,
    lease_ttl: Duration,
}

impl InMemoryAssignmentStore {
    pub fn new(reservation_ttl: Duration, lease_ttl: Duration) -> Result<Self, StoreError> {
        if reservation_ttl.is_zero() {
            return Err(StoreError::Invalid("reservation_ttl must be non-zero"));
        }
        if lease_ttl < reservation_ttl {
            return Err(StoreError::Invalid(
                "lease_ttl must be at least reservation_ttl",
            ));
        }
        Ok(Self {
            state: Mutex::new(InMemoryState::default()),
            reservation_ttl,
            lease_ttl,
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
        validate_owner_fence(&state, &request.sandbox_id, &request.node.id)?;
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

        let generation = generation_for_owner(
            &mut state,
            &request.sandbox_id,
            &request.node.id,
            request.node.generation,
        )?;

        let expires_at = request.now + self.reservation_ttl;
        let assignment = Assignment {
            sandbox_id: request.sandbox_id.clone(),
            node: Node {
                generation,
                ..request.node.clone()
            },
            state: AssignmentState::Reserved,
        };
        state.assignments.insert(
            request.sandbox_id.clone(),
            TimedAssignment {
                assignment: assignment.clone(),
                expires_at: Some(expires_at),
            },
        );
        state.reservations.insert(
            request.sandbox_id,
            Reservation {
                resources: request_resources,
                expires_at,
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

        let generation = generation_for_owner(&mut state, sandbox_id, &node.id, node.generation)?;
        let assignment = Assignment {
            sandbox_id: sandbox_id.to_string(),
            node: Node { generation, ..node },
            state: AssignmentState::Confirmed,
        };
        state.assignments.insert(
            sandbox_id.to_string(),
            TimedAssignment {
                assignment: assignment.clone(),
                expires_at: Some(now + self.lease_ttl),
            },
        );
        Ok(assignment)
    }

    async fn apply_lifecycle_batch(&self, batch: LifecycleBatch) -> Result<u64, StoreError> {
        validate_lifecycle_batch(&batch)?;
        let mut state = self.state.lock();
        cleanup_expired(&mut state, batch.now);
        let cursor = state.lifecycle_cursors.get(&batch.node.id).cloned();
        let acknowledged = cursor.as_ref().map_or(0, |cursor| cursor.sequence);
        let first = batch.events[0].sequence;
        let last = batch.events.last().map_or(first, |event| event.sequence);
        let stream_changed = cursor
            .as_ref()
            .is_some_and(|cursor| cursor.stream_id != batch.stream_id);

        if stream_changed || cursor.is_none() {
            if first != 1 {
                return Err(StoreError::SequenceConflict(format!(
                    "new lifecycle stream for node {} must begin at sequence 1",
                    batch.node.id
                )));
            }
        } else if first > acknowledged.saturating_add(1) {
            return Err(StoreError::SequenceConflict(format!(
                "lifecycle sequence gap for node {}: expected {}, received {first}",
                batch.node.id,
                acknowledged.saturating_add(1)
            )));
        }

        let already_applied = if stream_changed { 0 } else { acknowledged };
        if !stream_changed && last <= already_applied {
            return Ok(already_applied);
        }

        for event in batch
            .events
            .iter()
            .filter(|event| event.sequence > already_applied)
        {
            if matches!(
                event.kind,
                LifecycleEventKind::Create | LifecycleEventKind::Resume | LifecycleEventKind::Fork
            ) {
                if let Some(existing) = state.assignments.get(&event.sandbox_id) {
                    if existing.assignment.node.id != batch.node.id {
                        return Err(StoreError::OwnershipConflict {
                            sandbox_id: event.sandbox_id.clone(),
                            assigned_node: existing.assignment.node.id.clone(),
                            requested_node: batch.node.id.clone(),
                        });
                    }
                }
                validate_owner_fence(&state, &event.sandbox_id, &batch.node.id)?;
            }
        }

        for event in batch
            .events
            .iter()
            .filter(|event| event.sequence > already_applied)
        {
            match event.kind {
                LifecycleEventKind::Create
                | LifecycleEventKind::Resume
                | LifecycleEventKind::Fork => {
                    let generation = generation_for_owner(
                        &mut state,
                        &event.sandbox_id,
                        &batch.node.id,
                        batch.node.generation,
                    )?;
                    state.assignments.insert(
                        event.sandbox_id.clone(),
                        TimedAssignment {
                            assignment: Assignment {
                                sandbox_id: event.sandbox_id.clone(),
                                node: Node {
                                    generation,
                                    ..batch.node.clone()
                                },
                                state: AssignmentState::Confirmed,
                            },
                            expires_at: Some(batch.now + self.lease_ttl),
                        },
                    );
                    state
                        .reconcile_misses
                        .remove(&(batch.node.id.clone(), event.sandbox_id.clone()));
                }
                LifecycleEventKind::Delete => {
                    if state
                        .assignments
                        .get(&event.sandbox_id)
                        .is_some_and(|assignment| assignment.assignment.node.id == batch.node.id)
                    {
                        release_reservation(&mut state, &event.sandbox_id)?;
                        state.assignments.remove(&event.sandbox_id);
                    }
                    retire_fence(&mut state, &event.sandbox_id, &batch.node.id)?;
                    state
                        .reconcile_misses
                        .remove(&(batch.node.id.clone(), event.sandbox_id.clone()));
                }
                LifecycleEventKind::Pause => {
                    if let Some(existing) = state.assignments.get_mut(&event.sandbox_id) {
                        if existing.assignment.node.id == batch.node.id {
                            existing.expires_at = Some(batch.now + self.lease_ttl);
                        }
                    }
                }
            }
        }
        state.lifecycle_cursors.insert(
            batch.node.id,
            LifecycleCursor {
                stream_id: batch.stream_id,
                sequence: last,
            },
        );
        Ok(last)
    }

    async fn reconcile_node(
        &self,
        request: ReconcileRequest,
    ) -> Result<ReconcileResult, StoreError> {
        validate_reconcile_request(&request)?;
        let desired = request.sandbox_ids.iter().cloned().collect::<HashSet<_>>();
        let mut state = self.state.lock();
        cleanup_expired(&mut state, request.now);

        for sandbox_id in &desired {
            if state
                .fences
                .get(sandbox_id)
                .is_some_and(|fence| fence.retired)
            {
                continue;
            }
            validate_owner_fence(&state, sandbox_id, &request.node.id)?;
            if let Some(existing) = state.assignments.get(sandbox_id) {
                if existing.assignment.node.id != request.node.id {
                    return Err(StoreError::OwnershipConflict {
                        sandbox_id: sandbox_id.clone(),
                        assigned_node: existing.assignment.node.id.clone(),
                        requested_node: request.node.id.clone(),
                    });
                }
            }
        }

        let mut result = ReconcileResult::default();
        for sandbox_id in &desired {
            if state
                .fences
                .get(sandbox_id)
                .is_some_and(|fence| fence.retired)
            {
                continue;
            }
            release_reservation(&mut state, sandbox_id)?;
            let generation = generation_for_owner(
                &mut state,
                sandbox_id,
                &request.node.id,
                request.node.generation,
            )?;
            let reconciled_node = Node {
                generation,
                ..request.node.clone()
            };
            let needs_repair = state.assignments.get(sandbox_id).is_none_or(|existing| {
                existing.assignment.state != AssignmentState::Confirmed
                    || existing.assignment.node != reconciled_node
            });
            if needs_repair {
                state.assignments.insert(
                    sandbox_id.clone(),
                    TimedAssignment {
                        assignment: Assignment {
                            sandbox_id: sandbox_id.clone(),
                            node: reconciled_node,
                            state: AssignmentState::Confirmed,
                        },
                        expires_at: Some(request.now + self.lease_ttl),
                    },
                );
                result.repaired = result.repaired.saturating_add(1);
            } else if let Some(existing) = state.assignments.get_mut(sandbox_id) {
                existing.expires_at = Some(request.now + self.lease_ttl);
            }
            state
                .reconcile_misses
                .remove(&(request.node.id.clone(), sandbox_id.clone()));
        }

        let missing = state
            .assignments
            .iter()
            .filter(|(sandbox_id, assignment)| {
                assignment.assignment.node.id == request.node.id
                    && assignment.assignment.state == AssignmentState::Confirmed
                    && !desired.contains(*sandbox_id)
            })
            .map(|(sandbox_id, _)| sandbox_id.clone())
            .collect::<Vec<_>>();
        for sandbox_id in missing {
            let key = (request.node.id.clone(), sandbox_id.clone());
            let misses = state.reconcile_misses.entry(key.clone()).or_default();
            *misses = misses.saturating_add(1);
            if *misses >= request.missing_heartbeat_threshold {
                state.assignments.remove(&sandbox_id);
                state.reconcile_misses.remove(&key);
                result.removed = result.removed.saturating_add(1);
            }
        }
        Ok(result)
    }
}

fn validate_lifecycle_batch(batch: &LifecycleBatch) -> Result<(), StoreError> {
    if batch.node.id.trim().is_empty()
        || batch.node.endpoint.trim().is_empty()
        || batch.service_instance_id.trim().is_empty()
        || batch.stream_id.trim().is_empty()
    {
        return Err(StoreError::Invalid(
            "node, service_instance_id, and stream_id must be non-empty",
        ));
    }
    if batch.events.is_empty() {
        return Err(StoreError::Invalid(
            "lifecycle event batch must be non-empty",
        ));
    }
    let mut expected = batch.events[0].sequence;
    if expected == 0 {
        return Err(StoreError::Invalid(
            "lifecycle event sequence must be greater than zero",
        ));
    }
    for event in &batch.events {
        if event.sequence != expected {
            return Err(StoreError::Invalid(
                "lifecycle event sequences must be contiguous",
            ));
        }
        if event.sandbox_id.trim().is_empty() || event.event_id.trim().is_empty() {
            return Err(StoreError::Invalid(
                "lifecycle sandbox_id and event_id must be non-empty",
            ));
        }
        if event.event_id != format!("{}:{}", batch.stream_id, event.sequence) {
            return Err(StoreError::Invalid(
                "lifecycle event_id must match stream_id and sequence",
            ));
        }
        expected = expected.checked_add(1).ok_or_else(|| {
            StoreError::Invariant("lifecycle event sequence exhausted".to_string())
        })?;
    }
    Ok(())
}

fn generation_for_owner(
    state: &mut InMemoryState,
    sandbox_id: &str,
    node_id: &str,
    requested_generation: u64,
) -> Result<u64, StoreError> {
    if let Some(fence) = state.fences.get(sandbox_id) {
        if fence.retired {
            return Err(StoreError::Retired {
                sandbox_id: sandbox_id.to_string(),
            });
        }
        if fence.owner_node_id != node_id {
            return Err(StoreError::OwnershipConflict {
                sandbox_id: sandbox_id.to_string(),
                assigned_node: fence.owner_node_id.clone(),
                requested_node: node_id.to_string(),
            });
        }
        return Ok(fence.generation);
    }

    let generation = requested_generation.max(1);
    state.fences.insert(
        sandbox_id.to_string(),
        FenceRecord {
            owner_node_id: node_id.to_string(),
            generation,
            retired: false,
        },
    );
    Ok(generation)
}

fn validate_owner_fence(
    state: &InMemoryState,
    sandbox_id: &str,
    node_id: &str,
) -> Result<(), StoreError> {
    let Some(fence) = state.fences.get(sandbox_id) else {
        return Ok(());
    };
    if fence.retired {
        return Err(StoreError::Retired {
            sandbox_id: sandbox_id.to_string(),
        });
    }
    if fence.owner_node_id != node_id {
        return Err(StoreError::OwnershipConflict {
            sandbox_id: sandbox_id.to_string(),
            assigned_node: fence.owner_node_id.clone(),
            requested_node: node_id.to_string(),
        });
    }
    Ok(())
}

fn retire_fence(
    state: &mut InMemoryState,
    sandbox_id: &str,
    node_id: &str,
) -> Result<(), StoreError> {
    if let Some(fence) = state.fences.get_mut(sandbox_id) {
        if fence.owner_node_id != node_id {
            return Err(StoreError::OwnershipConflict {
                sandbox_id: sandbox_id.to_string(),
                assigned_node: fence.owner_node_id.clone(),
                requested_node: node_id.to_string(),
            });
        }
        fence.retired = true;
    } else {
        state.fences.insert(
            sandbox_id.to_string(),
            FenceRecord {
                owner_node_id: node_id.to_string(),
                generation: 1,
                retired: true,
            },
        );
    }
    Ok(())
}

fn validate_reconcile_request(request: &ReconcileRequest) -> Result<(), StoreError> {
    if request.node.id.trim().is_empty() || request.node.endpoint.trim().is_empty() {
        return Err(StoreError::Invalid(
            "reconcile node_id and endpoint must be non-empty",
        ));
    }
    if request.missing_heartbeat_threshold == 0 {
        return Err(StoreError::Invalid(
            "missing heartbeat threshold must be greater than zero",
        ));
    }
    if request
        .sandbox_ids
        .iter()
        .any(|sandbox_id| sandbox_id.trim().is_empty())
    {
        return Err(StoreError::Invalid(
            "reconcile sandbox IDs must be non-empty",
        ));
    }
    Ok(())
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
    let expired_reservations = state
        .reservations
        .iter()
        .filter(|(_, reservation)| reservation.expires_at <= now)
        .map(|(sandbox_id, _)| sandbox_id.clone())
        .collect::<Vec<_>>();
    for sandbox_id in expired_reservations {
        let _ = release_reservation(state, &sandbox_id);
    }
    let expired = state
        .assignments
        .iter()
        .filter(|(_, assignment)| assignment.expires_at.is_some_and(|expiry| expiry <= now))
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

    fn lifecycle_batch(
        node_id: &str,
        stream_id: &str,
        first_sequence: u64,
        events: impl IntoIterator<Item = (&'static str, LifecycleEventKind)>,
    ) -> LifecycleBatch {
        LifecycleBatch {
            node: Node::new(node_id, format!("http://{node_id}")),
            service_instance_id: "instance-1".to_string(),
            stream_id: stream_id.to_string(),
            events: events
                .into_iter()
                .enumerate()
                .map(|(offset, (sandbox_id, kind))| {
                    let sequence = first_sequence + u64::try_from(offset).unwrap();
                    LifecycleEvent {
                        sandbox_id: sandbox_id.to_string(),
                        kind,
                        resources: SandboxResources {
                            cpu: 1,
                            memory_bytes: 1024,
                            disk_bytes: 2048,
                        },
                        sequence,
                        event_id: format!("{stream_id}:{sequence}"),
                        occurred_at_unix_ms: 1,
                    }
                })
                .collect(),
            now: Instant::now(),
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
    async fn confirmation_retains_shadow_capacity_and_fences_wrong_node() {
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
            PendingResources::for_request(claim("ignored", "node-a", now).resources)
        );
        assert_eq!(
            store.pending_for_node("node-a", now + Duration::from_secs(11)),
            PendingResources::default()
        );
        assert!(store
            .lookup("sandbox", now + Duration::from_secs(61))
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            store
                .claim(claim("sandbox", "node-b", now + Duration::from_secs(61)))
                .await
                .unwrap_err(),
            StoreError::OwnershipConflict { .. }
        ));
    }

    #[tokio::test]
    async fn heartbeat_inventory_renews_the_route_lease_without_advancing_generation() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        let mut node = Node::new("node-a", "http://node-a");
        node.generation = 7;
        store.confirm("sandbox", node.clone(), now).await.unwrap();
        store
            .reconcile_node(ReconcileRequest {
                node: Node::new("node-a", "http://node-a"),
                sandbox_ids: vec!["sandbox".to_string()],
                missing_heartbeat_threshold: 3,
                now: now + Duration::from_secs(50),
            })
            .await
            .unwrap();
        let renewed = store
            .lookup("sandbox", now + Duration::from_secs(100))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renewed.node.generation, 7);
        assert!(store
            .lookup("sandbox", now + Duration::from_secs(111))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn lifecycle_batches_are_idempotent_contiguous_and_materialized() {
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        let stream_id = uuid::Uuid::now_v7().to_string();
        let create = || {
            lifecycle_batch(
                "node-a",
                &stream_id,
                1,
                [("sandbox-1", LifecycleEventKind::Create)],
            )
        };
        assert_eq!(store.apply_lifecycle_batch(create()).await.unwrap(), 1);
        assert_eq!(store.apply_lifecycle_batch(create()).await.unwrap(), 1);
        assert!(store
            .lookup("sandbox-1", Instant::now())
            .await
            .unwrap()
            .is_some());

        let gap = lifecycle_batch(
            "node-a",
            &stream_id,
            3,
            [("sandbox-1", LifecycleEventKind::Delete)],
        );
        assert!(matches!(
            store.apply_lifecycle_batch(gap).await.unwrap_err(),
            StoreError::SequenceConflict(_)
        ));

        let delete = lifecycle_batch(
            "node-a",
            &stream_id,
            2,
            [("sandbox-1", LifecycleEventKind::Delete)],
        );
        assert_eq!(store.apply_lifecycle_batch(delete).await.unwrap(), 2);
        assert!(store
            .lookup("sandbox-1", Instant::now())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn lifecycle_materialization_fences_a_different_owner() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        store
            .claim(claim("sandbox-1", "node-a", now))
            .await
            .unwrap();
        let batch = lifecycle_batch(
            "node-b",
            &uuid::Uuid::now_v7().to_string(),
            1,
            [("sandbox-1", LifecycleEventKind::Create)],
        );
        assert!(matches!(
            store.apply_lifecycle_batch(batch).await.unwrap_err(),
            StoreError::OwnershipConflict { .. }
        ));
    }

    #[tokio::test]
    async fn delete_retires_the_sandbox_id_and_inventory_cannot_resurrect_it() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        let stream_id = uuid::Uuid::now_v7().to_string();
        let mut create = lifecycle_batch(
            "node-a",
            &stream_id,
            1,
            [("sandbox-1", LifecycleEventKind::Create)],
        );
        create.now = now;
        store.apply_lifecycle_batch(create).await.unwrap();
        let mut delete = lifecycle_batch(
            "node-a",
            &stream_id,
            2,
            [("sandbox-1", LifecycleEventKind::Delete)],
        );
        delete.now = now;
        store.apply_lifecycle_batch(delete).await.unwrap();

        store
            .reconcile_node(ReconcileRequest {
                node: Node::new("node-a", "http://node-a"),
                sandbox_ids: vec!["sandbox-1".to_string()],
                missing_heartbeat_threshold: 3,
                now,
            })
            .await
            .unwrap();
        assert!(store.lookup("sandbox-1", now).await.unwrap().is_none());
        assert!(matches!(
            store.claim(claim("sandbox-1", "node-a", now)).await,
            Err(StoreError::Retired { .. })
        ));
    }

    #[tokio::test]
    async fn reconciliation_repairs_routes_and_requires_consecutive_misses_to_remove() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        let node = Node::new("node-a", "http://node-a");
        let request = |sandbox_ids: &[&str]| ReconcileRequest {
            node: node.clone(),
            sandbox_ids: sandbox_ids.iter().map(|id| (*id).to_string()).collect(),
            missing_heartbeat_threshold: 3,
            now,
        };

        let repaired = store.reconcile_node(request(&["sandbox-1"])).await.unwrap();
        assert_eq!(repaired.repaired, 1);
        assert!(store.lookup("sandbox-1", now).await.unwrap().is_some());

        for _ in 0..2 {
            let result = store.reconcile_node(request(&[])).await.unwrap();
            assert_eq!(result.removed, 0);
            assert!(store.lookup("sandbox-1", now).await.unwrap().is_some());
        }
        let removed = store.reconcile_node(request(&[])).await.unwrap();
        assert_eq!(removed.removed, 1);
        assert!(store.lookup("sandbox-1", now).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lifecycle_and_reconciliation_never_regress_route_generation() {
        let now = Instant::now();
        let store =
            InMemoryAssignmentStore::new(Duration::from_secs(10), Duration::from_secs(60)).unwrap();
        let mut generation_seven = Node::new("node-a", "http://node-a");
        generation_seven.generation = 7;
        store
            .confirm("sandbox-1", generation_seven, now)
            .await
            .unwrap();

        store
            .reconcile_node(ReconcileRequest {
                node: Node::new("node-a", "http://node-a"),
                sandbox_ids: vec!["sandbox-1".to_string()],
                missing_heartbeat_threshold: 3,
                now,
            })
            .await
            .unwrap();
        let stream_id = uuid::Uuid::now_v7().to_string();
        store
            .apply_lifecycle_batch(lifecycle_batch(
                "node-a",
                &stream_id,
                1,
                [("sandbox-1", LifecycleEventKind::Resume)],
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .lookup("sandbox-1", now)
                .await
                .unwrap()
                .unwrap()
                .node
                .generation,
            7
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
