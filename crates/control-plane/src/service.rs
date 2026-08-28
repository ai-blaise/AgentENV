use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::assignment::{
    AssignmentStore, ClaimOutcome, ClaimRequest, LifecycleBatch, LifecycleEvent,
    LifecycleEventKind, ReconcileRequest, StoreError,
};
use crate::model::{CapacityLimits, Node, NodeObservation, PendingResources, SandboxResources};
use crate::placement::{PlacementEngine, PlacementError};
use crate::proto;
use crate::proto::scheduler_server::Scheduler;
use crate::registry::{HeartbeatError, NodeRegistry};
use crate::ArtifactIndex;

const MIB: u64 = 1024 * 1024;
const MAX_LIFECYCLE_EVENT_BATCH: usize = 256;
const MAX_SANDBOX_INVENTORY: usize = 100_000;

pub struct ControlPlane<S: ?Sized> {
    registry: Arc<NodeRegistry>,
    placement: PlacementEngine,
    assignments: Arc<S>,
    artifacts: ArtifactIndex,
    reservation_ttl: std::time::Duration,
    reconciliation_miss_threshold: u8,
}

impl<S> ControlPlane<S>
where
    S: AssignmentStore + ?Sized,
{
    pub fn new(
        registry: Arc<NodeRegistry>,
        placement: PlacementEngine,
        assignments: Arc<S>,
        reservation_ttl: std::time::Duration,
        reconciliation_miss_threshold: u8,
        artifact_capacity: usize,
        artifact_node_limit: usize,
    ) -> Result<Self, &'static str> {
        if reservation_ttl.is_zero() {
            return Err("reservation_ttl must be greater than zero");
        }
        if reconciliation_miss_threshold == 0 {
            return Err("reconciliation_miss_threshold must be greater than zero");
        }
        Ok(Self {
            registry,
            placement,
            assignments,
            artifacts: ArtifactIndex::new(artifact_capacity, artifact_node_limit)?,
            reservation_ttl,
            reconciliation_miss_threshold,
        })
    }

    fn request_resources(
        &self,
        request: &proto::ScheduleRequest,
    ) -> Result<SandboxResources, Status> {
        let defaults = self.placement.config().default_request;
        let Some(hint) = request.hint.as_ref() else {
            return Ok(defaults);
        };
        let Some(kind) = hint.kind.as_ref() else {
            return Ok(defaults);
        };
        match kind {
            proto::schedule_request_hint::Kind::NewSandbox(_) => Ok(defaults),
            proto::schedule_request_hint::Kind::NewColdSandbox(cold) => Ok(SandboxResources {
                cpu: nonzero_u32(cold.cpu_count, defaults.cpu),
                memory_bytes: mib_to_bytes(nonzero_u64(
                    cold.memory_mb,
                    defaults.memory_bytes / MIB,
                ))?,
                disk_bytes: mib_to_bytes(nonzero_u64(
                    cold.disk_size_mb,
                    defaults.disk_bytes / MIB,
                ))?,
            }),
        }
    }

    fn effective_limits(&self, observation: &NodeObservation) -> CapacityLimits {
        let configured = self.placement.config().limits;
        CapacityLimits {
            max_sandboxes: configured.max_sandboxes,
            max_starting: configured.max_starting,
            max_cpu: configured.max_cpu.or(Some(observation.cpu_count)),
            max_memory_bytes: configured
                .max_memory_bytes
                .or(Some(observation.memory_total_bytes)),
            max_disk_bytes: configured
                .max_disk_bytes
                .or(Some(observation.disk_total_bytes)),
        }
    }

    fn observed_resources(observation: &NodeObservation) -> PendingResources {
        PendingResources {
            sandboxes: observation
                .active_sandboxes
                .saturating_add(observation.paused_sandboxes),
            starting: observation.starting_sandboxes,
            cpu: observation.allocated_cpu,
            memory_bytes: observation.allocated_memory_bytes,
            disk_bytes: observation.disk_used_bytes,
        }
    }

    fn peer_for_node(
        &self,
        node_id: &str,
        cluster_id: &str,
        backend: &str,
        now: Instant,
    ) -> Option<proto::P2pPeer> {
        let node = self.registry.resolve(node_id)?;
        if self.registry.is_draining(node_id)? {
            return None;
        }
        let observation = self.registry.observation(node_id)?;
        if !observation.ready
            || now.saturating_duration_since(observation.observed_at)
                > self.placement.config().heartbeat_ttl
            || (!cluster_id.is_empty() && observation.cluster_id != cluster_id)
            || observation.p2p_address.is_empty()
            || (!backend.is_empty() && observation.p2p_backend != backend)
        {
            return None;
        }
        Some(proto::P2pPeer {
            node_id: node.id,
            endpoint: Some(proto::P2pEndpoint {
                backend: observation.p2p_backend,
                address: observation.p2p_address,
            }),
        })
    }
}

#[tonic::async_trait]
impl<S> Scheduler for ControlPlane<S>
where
    S: AssignmentStore + ?Sized,
{
    async fn schedule(
        &self,
        request: Request<proto::ScheduleRequest>,
    ) -> Result<Response<proto::ScheduleResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = request.sandbox_id.trim();
        if !sandbox_id.is_empty() && Uuid::parse_str(sandbox_id).is_err() {
            return Err(Status::invalid_argument("sandbox_id must be a UUID"));
        }
        if !sandbox_id.is_empty() {
            if let Some(existing) = self
                .assignments
                .lookup(sandbox_id, Instant::now())
                .await
                .map_err(store_status)?
            {
                metrics::counter!("agentenv_control_plane_assignment_replay_total").increment(1);
                return Ok(Response::new(proto::ScheduleResponse {
                    node: Some(node_to_proto(&existing.node)),
                }));
            }
        }

        let resources = self.request_resources(&request)?;
        let mut excluded = HashSet::new();
        let max_attempts = self.placement.config().probe_budget;
        for _ in 0..max_attempts {
            let now = Instant::now();
            let node = self
                .placement
                .select(&self.registry, resources, now, &excluded)
                .map_err(placement_status)?;
            if sandbox_id.is_empty() {
                return Ok(Response::new(proto::ScheduleResponse {
                    node: Some(node_to_proto(&node)),
                }));
            }
            let Some(observation) = self.registry.observation(&node.id) else {
                excluded.insert(node.id);
                continue;
            };
            let claim = ClaimRequest {
                sandbox_id: sandbox_id.to_string(),
                node: node.clone(),
                resources,
                observed: Self::observed_resources(&observation),
                limits: self.effective_limits(&observation),
                now,
            };
            match self.assignments.claim(claim).await {
                Ok(ClaimOutcome::Existing(existing)) => {
                    metrics::counter!("agentenv_control_plane_assignment_replay_total")
                        .increment(1);
                    return Ok(Response::new(proto::ScheduleResponse {
                        node: Some(node_to_proto(&existing.node)),
                    }));
                }
                Ok(ClaimOutcome::Claimed(assignment)) => {
                    self.registry
                        .add_pending(sandbox_id, &node.id, resources, now + self.reservation_ttl)
                        .map_err(heartbeat_status)?;
                    metrics::counter!("agentenv_control_plane_assignment_claim_total").increment(1);
                    return Ok(Response::new(proto::ScheduleResponse {
                        node: Some(node_to_proto(&assignment.node)),
                    }));
                }
                Err(StoreError::CapacityExhausted { node_id }) => {
                    excluded.insert(node_id);
                }
                Err(error) => return Err(store_status(error)),
            }
        }
        Err(Status::unavailable(
            "no node could atomically reserve the requested capacity",
        ))
    }

    async fn list_nodes(
        &self,
        _request: Request<proto::ListNodesRequest>,
    ) -> Result<Response<proto::ListNodesResponse>, Status> {
        Ok(Response::new(proto::ListNodesResponse {
            nodes: self.registry.list(true).iter().map(node_to_proto).collect(),
        }))
    }

    async fn lookup_node(
        &self,
        request: Request<proto::LookupNodeRequest>,
    ) -> Result<Response<proto::LookupNodeResponse>, Status> {
        let sandbox_id = request.into_inner().sandbox_id;
        if sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let assignment = self
            .assignments
            .lookup(&sandbox_id, Instant::now())
            .await
            .map_err(store_status)?
            .ok_or_else(|| Status::not_found("sandbox assignment not found"))?;
        Ok(Response::new(proto::LookupNodeResponse {
            node: Some(node_to_proto(&assignment.node)),
        }))
    }

    async fn record_assignment(
        &self,
        request: Request<proto::RecordAssignmentRequest>,
    ) -> Result<Response<proto::RecordAssignmentResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.trim().is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let requested_node = node_from_proto(request.node.as_ref())?;
        let registered = self
            .registry
            .resolve(&requested_node.id)
            .filter(|node| node.endpoint == requested_node.endpoint)
            .ok_or_else(|| Status::invalid_argument("node is not registered"))?;
        self.assignments
            .confirm(&request.sandbox_id, registered, Instant::now())
            .await
            .map_err(store_status)?;
        Ok(Response::new(proto::RecordAssignmentResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let node_id = request.node_id.trim();
        let node = self
            .registry
            .resolve(node_id)
            .ok_or_else(|| Status::invalid_argument("node is not registered"))?;
        if request.cluster_id.trim().is_empty() || request.service_instance_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "cluster_id and service_instance_id are required",
            ));
        }
        if request.sandbox_ids.len() > MAX_SANDBOX_INVENTORY {
            return Err(Status::resource_exhausted(
                "sandbox inventory exceeds 100000 entries",
            ));
        }
        let mut sandbox_ids = request
            .sandbox_ids
            .iter()
            .map(|sandbox_id| {
                Uuid::parse_str(sandbox_id.trim())
                    .map(|id| id.to_string())
                    .map_err(|_| Status::invalid_argument("sandbox inventory contains a non-UUID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        sandbox_ids.sort_unstable();
        sandbox_ids.dedup();
        let snapshot = request
            .snapshot
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("snapshot is required"))?;
        let lifecycle_stream_id = request.lifecycle_stream_id.trim();
        if lifecycle_stream_id.is_empty() && request.lifecycle_last_sequence != 0 {
            return Err(Status::invalid_argument(
                "lifecycle_stream_id is required when lifecycle_last_sequence is non-zero",
            ));
        }
        if !lifecycle_stream_id.is_empty() && Uuid::parse_str(lifecycle_stream_id).is_err() {
            return Err(Status::invalid_argument(
                "lifecycle_stream_id must be a UUID",
            ));
        }
        let machine = request.machine_info.as_ref();
        let p2p = request.p2p_endpoint.as_ref();
        let (disk_used_bytes, disk_total_bytes) = primary_disk_capacity(snapshot)?;
        let observation = NodeObservation {
            service_instance_id: request.service_instance_id.clone(),
            cluster_id: request.cluster_id,
            version: request.version,
            commit: request.commit,
            cpu_architecture: machine
                .map_or_else(String::new, |info| info.cpu_architecture.clone()),
            cpu_config_json: machine.map_or_else(String::new, |info| info.cpu_config_json.clone()),
            p2p_backend: p2p.map_or_else(String::new, |endpoint| endpoint.backend.clone()),
            p2p_address: p2p.map_or_else(String::new, |endpoint| endpoint.address.clone()),
            observed_at: Instant::now(),
            reported_at_unix_ms: unix_millis(),
            ready: snapshot.status == proto::NodeStatus::Ready as i32,
            active_sandboxes: snapshot.sandbox_count,
            paused_sandboxes: snapshot.paused_sandbox_count,
            starting_sandboxes: snapshot.sandbox_starting_count,
            allocated_cpu: u64::from(snapshot.allocated_cpu),
            allocated_memory_bytes: snapshot.allocated_memory_bytes,
            cpu_count: u64::from(snapshot.cpu_count),
            memory_used_bytes: snapshot.memory_used_bytes,
            memory_total_bytes: snapshot.memory_total_bytes,
            disk_used_bytes,
            disk_total_bytes,
            lifecycle_stream_id: lifecycle_stream_id.to_string(),
            lifecycle_last_sequence: request.lifecycle_last_sequence,
        };
        let reconciled = self
            .assignments
            .reconcile_node(ReconcileRequest {
                node,
                sandbox_ids: sandbox_ids.clone(),
                missing_heartbeat_threshold: self.reconciliation_miss_threshold,
                now: Instant::now(),
            })
            .await
            .map_err(store_status)?;
        self.registry
            .heartbeat(&request.node_id, observation)
            .map_err(heartbeat_status)?;
        for sandbox_id in &sandbox_ids {
            let _ = self.registry.remove_pending(sandbox_id, node_id);
        }
        metrics::counter!("agentenv_control_plane_reconciled_routes_total", "action" => "repaired")
            .increment(reconciled.repaired);
        metrics::counter!("agentenv_control_plane_reconciled_routes_total", "action" => "removed")
            .increment(reconciled.removed);
        Ok(Response::new(proto::HeartbeatResponse {
            cpu_config_json: String::new(),
        }))
    }

    async fn report_sandbox_event(
        &self,
        request: Request<proto::ReportSandboxEventRequest>,
    ) -> Result<Response<proto::ReportSandboxEventResponse>, Status> {
        let request = request.into_inner();
        let node_id = request.node_id.trim();
        let node = self
            .registry
            .resolve(node_id)
            .ok_or_else(|| Status::invalid_argument("node is not registered"))?;
        let observation = self
            .registry
            .observation(node_id)
            .ok_or_else(|| Status::failed_precondition("node has not sent a heartbeat"))?;
        if observation.cluster_id != request.cluster_id {
            return Err(Status::failed_precondition("node cluster does not match"));
        }
        if observation.service_instance_id != request.service_instance_id {
            return Err(Status::failed_precondition("service instance mismatch"));
        }
        let stream_id = Uuid::parse_str(request.lifecycle_stream_id.trim())
            .map_err(|_| Status::invalid_argument("lifecycle_stream_id must be a UUID"))?
            .to_string();
        if observation.lifecycle_stream_id != stream_id {
            return Err(Status::failed_precondition(
                "lifecycle stream does not match the latest heartbeat",
            ));
        }
        if request.events.is_empty() || request.events.len() > MAX_LIFECYCLE_EVENT_BATCH {
            return Err(Status::invalid_argument(
                "lifecycle event batch must contain between 1 and 256 events",
            ));
        }
        let events = request
            .events
            .into_iter()
            .map(|event| lifecycle_event_from_proto(event, &stream_id))
            .collect::<Result<Vec<_>, _>>()?;
        let event_count = events.len();
        let acknowledged = self
            .assignments
            .apply_lifecycle_batch(LifecycleBatch {
                node: node.clone(),
                service_instance_id: request.service_instance_id,
                stream_id,
                events,
                now: Instant::now(),
            })
            .await
            .map_err(store_status)?;
        metrics::counter!("agentenv_control_plane_sandbox_events_total")
            .increment(event_count as u64);
        Ok(Response::new(proto::ReportSandboxEventResponse {
            acknowledged_sequence: acknowledged,
        }))
    }

    async fn list_observed_nodes(
        &self,
        request: Request<proto::ListObservedNodesRequest>,
    ) -> Result<Response<proto::ListObservedNodesResponse>, Status> {
        let cluster_id = request.into_inner().cluster_id;
        let now = Instant::now();
        let nodes = self
            .registry
            .list(true)
            .into_iter()
            .filter_map(|node| {
                let observation = self.registry.observation(&node.id)?;
                if !cluster_id.is_empty() && observation.cluster_id != cluster_id {
                    return None;
                }
                let draining = self.registry.is_draining(&node.id).unwrap_or(true);
                Some(observed_to_proto(
                    node,
                    observation,
                    draining,
                    self.placement.config().heartbeat_ttl,
                    now,
                ))
            })
            .collect();
        Ok(Response::new(proto::ListObservedNodesResponse { nodes }))
    }

    async fn list_p2p_peers(
        &self,
        request: Request<proto::ListP2pPeersRequest>,
    ) -> Result<Response<proto::ListP2pPeersResponse>, Status> {
        let request = request.into_inner();
        let now = Instant::now();
        let peers = self
            .registry
            .list(false)
            .iter()
            .filter(|node| node.id != request.exclude_node_id)
            .filter_map(|node| {
                self.peer_for_node(&node.id, &request.cluster_id, &request.backend, now)
            })
            .collect();
        Ok(Response::new(proto::ListP2pPeersResponse { peers }))
    }

    async fn record_p2p_artifact(
        &self,
        request: Request<proto::RecordP2pArtifactRequest>,
    ) -> Result<Response<proto::RecordP2pArtifactResponse>, Status> {
        let request = request.into_inner();
        validate_artifact_request(
            &request.cluster_id,
            &request.backend,
            &request.key,
            &request.node_id,
        )?;
        let observation = self
            .registry
            .observation(&request.node_id)
            .filter(|observation| observation.cluster_id == request.cluster_id)
            .ok_or_else(|| Status::invalid_argument("node is not observed in the cluster"))?;
        if observation.p2p_backend != request.backend {
            return Err(Status::invalid_argument("node P2P backend does not match"));
        }
        if !self.artifacts.record(
            &request.cluster_id,
            &request.backend,
            &request.key,
            &request.node_id,
        ) {
            return Err(Status::resource_exhausted("artifact index limit reached"));
        }
        Ok(Response::new(proto::RecordP2pArtifactResponse {}))
    }

    async fn forget_p2p_artifact(
        &self,
        request: Request<proto::ForgetP2pArtifactRequest>,
    ) -> Result<Response<proto::ForgetP2pArtifactResponse>, Status> {
        let request = request.into_inner();
        validate_artifact_request(
            &request.cluster_id,
            &request.backend,
            &request.key,
            &request.node_id,
        )?;
        self.artifacts.forget(
            &request.cluster_id,
            &request.backend,
            &request.key,
            &request.node_id,
        );
        Ok(Response::new(proto::ForgetP2pArtifactResponse {}))
    }

    async fn lookup_p2p_artifact(
        &self,
        request: Request<proto::LookupP2pArtifactRequest>,
    ) -> Result<Response<proto::LookupP2pArtifactResponse>, Status> {
        let request = request.into_inner();
        validate_artifact_request(
            &request.cluster_id,
            &request.backend,
            &request.key,
            "lookup",
        )?;
        let now = Instant::now();
        let peers = self
            .artifacts
            .lookup(&request.cluster_id, &request.backend, &request.key)
            .into_iter()
            .filter(|node_id| node_id != &request.exclude_node_id)
            .filter_map(|node_id| {
                self.peer_for_node(&node_id, &request.cluster_id, &request.backend, now)
            })
            .collect();
        Ok(Response::new(proto::LookupP2pArtifactResponse { peers }))
    }

    async fn get_node(
        &self,
        request: Request<proto::GetNodeRequest>,
    ) -> Result<Response<proto::GetNodeResponse>, Status> {
        let request = request.into_inner();
        let node = self
            .registry
            .resolve(request.node_id.trim())
            .ok_or_else(|| Status::not_found("observed node not found"))?;
        let observation = self
            .registry
            .observation(&node.id)
            .filter(|observation| {
                request.cluster_id.is_empty() || observation.cluster_id == request.cluster_id
            })
            .ok_or_else(|| Status::not_found("observed node not found"))?;
        let draining = self.registry.is_draining(&node.id).unwrap_or(true);
        Ok(Response::new(proto::GetNodeResponse {
            node: Some(observed_to_proto(
                node,
                observation,
                draining,
                self.placement.config().heartbeat_ttl,
                Instant::now(),
            )),
        }))
    }

    async fn unregister_node(
        &self,
        request: Request<proto::UnregisterNodeRequest>,
    ) -> Result<Response<proto::UnregisterNodeResponse>, Status> {
        let request = request.into_inner();
        if request.node_id.trim().is_empty() || request.service_instance_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node_id and service_instance_id are required",
            ));
        }
        if let Some(observation) = self.registry.observation(&request.node_id) {
            if observation.service_instance_id != request.service_instance_id {
                return Err(Status::failed_precondition("service instance mismatch"));
            }
        }
        self.registry
            .unregister(&request.node_id, &request.service_instance_id);
        self.artifacts.forget_node(&request.node_id);
        Ok(Response::new(proto::UnregisterNodeResponse {}))
    }
}

fn nonzero_u64(value: u64, fallback: u64) -> u64 {
    if value == 0 {
        fallback
    } else {
        value
    }
}

fn nonzero_u32(value: u32, fallback: u32) -> u32 {
    if value == 0 {
        fallback
    } else {
        value
    }
}

fn mib_to_bytes(value: u64) -> Result<u64, Status> {
    value
        .checked_mul(MIB)
        .ok_or_else(|| Status::invalid_argument("resource request is too large"))
}

fn node_to_proto(node: &Node) -> proto::Node {
    proto::Node {
        node_id: node.id.clone(),
        endpoint: node.endpoint.clone(),
        generation: node.generation,
    }
}

fn node_from_proto(node: Option<&proto::Node>) -> Result<Node, Status> {
    let node = node.ok_or_else(|| Status::invalid_argument("node is required"))?;
    if node.node_id.trim().is_empty() || node.endpoint.trim().is_empty() {
        return Err(Status::invalid_argument(
            "node_id and endpoint are required",
        ));
    }
    Ok(Node {
        id: node.node_id.clone(),
        endpoint: node.endpoint.clone(),
        generation: node.generation.max(1),
    })
}

fn placement_status(error: PlacementError) -> Status {
    match error {
        PlacementError::NoEligibleNodes => Status::unavailable(error.to_string()),
    }
}

fn heartbeat_status(error: HeartbeatError) -> Status {
    match error {
        HeartbeatError::UnknownNode(_) | HeartbeatError::MissingIdentity => {
            Status::invalid_argument(error.to_string())
        }
        HeartbeatError::PendingOverflow(_) | HeartbeatError::PendingUnderflow(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn store_status(error: StoreError) -> Status {
    match error {
        StoreError::Invalid(_) => Status::invalid_argument(error.to_string()),
        StoreError::CapacityExhausted { .. } => Status::unavailable(error.to_string()),
        StoreError::OwnershipConflict { .. } => Status::failed_precondition(error.to_string()),
        StoreError::Retired { .. } => Status::failed_precondition(error.to_string()),
        StoreError::SequenceConflict(_) => Status::failed_precondition(error.to_string()),
        StoreError::Invariant(_) => Status::internal(error.to_string()),
        StoreError::Backend(_) => Status::unavailable("assignment store unavailable"),
    }
}

fn lifecycle_event_from_proto(
    event: proto::SandboxEvent,
    stream_id: &str,
) -> Result<LifecycleEvent, Status> {
    let sandbox_id = Uuid::parse_str(event.sandbox_id.trim())
        .map_err(|_| Status::invalid_argument("event sandbox_id must be a UUID"))?
        .to_string();
    if event.sequence == 0 {
        return Err(Status::invalid_argument(
            "event sequence must be greater than zero",
        ));
    }
    let expected_event_id = format!("{stream_id}:{}", event.sequence);
    if event.event_id != expected_event_id {
        return Err(Status::invalid_argument(
            "event_id must match lifecycle_stream_id and sequence",
        ));
    }
    if event.event_id.len() > 192 {
        return Err(Status::invalid_argument("event_id exceeds size limit"));
    }
    let kind = match proto::SandboxEventType::try_from(event.event_type) {
        Ok(proto::SandboxEventType::Create) => LifecycleEventKind::Create,
        Ok(proto::SandboxEventType::Delete) => LifecycleEventKind::Delete,
        Ok(proto::SandboxEventType::Pause) => LifecycleEventKind::Pause,
        Ok(proto::SandboxEventType::Resume) => LifecycleEventKind::Resume,
        Ok(proto::SandboxEventType::Fork) => LifecycleEventKind::Fork,
        _ => return Err(Status::invalid_argument("event_type is required")),
    };
    Ok(LifecycleEvent {
        sandbox_id,
        kind,
        resources: SandboxResources {
            cpu: event.requested_cpu,
            memory_bytes: event.requested_memory_bytes,
            disk_bytes: event.requested_disk_bytes,
        },
        sequence: event.sequence,
        event_id: event.event_id,
        occurred_at_unix_ms: event.occurred_at_unix_ms,
    })
}

fn primary_disk_capacity(snapshot: &proto::NodeSnapshot) -> Result<(u64, u64), Status> {
    let disk = snapshot
        .disks
        .iter()
        .find(|disk| disk.mount_point == "/")
        .or_else(|| snapshot.disks.iter().max_by_key(|disk| disk.total_bytes));
    let Some(disk) = disk else {
        return Ok((0, 0));
    };
    if disk.used_bytes > disk.total_bytes {
        return Err(Status::invalid_argument(
            "disk used bytes exceeds total bytes",
        ));
    }
    Ok((disk.used_bytes, disk.total_bytes))
}

fn unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn observed_to_proto(
    node: Node,
    observation: NodeObservation,
    draining: bool,
    heartbeat_ttl: std::time::Duration,
    now: Instant,
) -> proto::ObservedNode {
    let fresh = now.saturating_duration_since(observation.observed_at) <= heartbeat_ttl;
    let status = if !fresh {
        proto::NodeStatus::Unhealthy
    } else if draining {
        proto::NodeStatus::Lingering
    } else if observation.ready {
        proto::NodeStatus::Ready
    } else {
        proto::NodeStatus::Connecting
    };
    proto::ObservedNode {
        node_id: node.id,
        endpoint: node.endpoint,
        cluster_id: observation.cluster_id,
        service_instance_id: observation.service_instance_id,
        version: observation.version,
        commit: observation.commit,
        machine_info: Some(proto::MachineInfo {
            cpu_family: String::new(),
            cpu_model: String::new(),
            cpu_model_name: String::new(),
            cpu_architecture: observation.cpu_architecture,
            cpu_config_json: observation.cpu_config_json,
        }),
        snapshot: Some(proto::NodeSnapshot {
            status: status as i32,
            allocated_cpu: u32::try_from(observation.allocated_cpu).unwrap_or(u32::MAX),
            allocated_memory_bytes: observation.allocated_memory_bytes,
            cpu_percent: 0,
            cpu_count: u32::try_from(observation.cpu_count).unwrap_or(u32::MAX),
            memory_used_bytes: observation.memory_used_bytes,
            memory_total_bytes: observation.memory_total_bytes,
            disks: vec![proto::DiskMetric {
                mount_point: "/".to_string(),
                device: String::new(),
                filesystem_type: String::new(),
                used_bytes: observation.disk_used_bytes,
                total_bytes: observation.disk_total_bytes,
            }],
            sandbox_count: observation.active_sandboxes,
            sandbox_starting_count: observation.starting_sandboxes,
            create_successes: 0,
            create_fails: 0,
            reported_at_unix_ms: observation.reported_at_unix_ms,
            paused_sandbox_count: observation.paused_sandboxes,
            paused_allocated_cpu: 0,
            paused_allocated_memory_bytes: 0,
        }),
        last_seen_unix_ms: observation.reported_at_unix_ms,
    }
}

fn validate_artifact_request(
    cluster_id: &str,
    backend: &str,
    key: &str,
    node_id: &str,
) -> Result<(), Status> {
    if cluster_id.trim().is_empty()
        || backend.trim().is_empty()
        || key.trim().is_empty()
        || node_id.trim().is_empty()
    {
        return Err(Status::invalid_argument(
            "cluster_id, backend, key, and node_id are required",
        ));
    }
    if cluster_id.len() > 128 || backend.len() > 64 || key.len() > 1024 || node_id.len() > 128 {
        return Err(Status::invalid_argument(
            "artifact field exceeds size limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::assignment::InMemoryAssignmentStore;
    use crate::model::PlacementConfig;

    fn control_plane() -> ControlPlane<InMemoryAssignmentStore> {
        let registry = Arc::new(NodeRegistry::new());
        registry.replace_discovered([
            (Node::new("node-a", "https://node-a"), false),
            (Node::new("node-b", "https://node-b"), false),
        ]);
        let placement = PlacementEngine::new(PlacementConfig {
            sample_size: 2,
            probe_budget: 2,
            default_request: SandboxResources {
                cpu: 1,
                memory_bytes: 512 * MIB,
                disk_bytes: 1024 * MIB,
            },
            ..PlacementConfig::default()
        })
        .unwrap();
        let assignments = Arc::new(
            InMemoryAssignmentStore::new(Duration::from_secs(30), Duration::from_secs(60)).unwrap(),
        );
        ControlPlane::new(
            registry,
            placement,
            assignments,
            Duration::from_secs(30),
            3,
            100,
            4,
        )
        .unwrap()
    }

    async fn heartbeat(service: &ControlPlane<InMemoryAssignmentStore>, node_id: &str) {
        heartbeat_with_stream(service, node_id, "", 0).await;
    }

    async fn heartbeat_with_stream(
        service: &ControlPlane<InMemoryAssignmentStore>,
        node_id: &str,
        lifecycle_stream_id: &str,
        lifecycle_last_sequence: u64,
    ) {
        heartbeat_with_inventory(
            service,
            node_id,
            lifecycle_stream_id,
            lifecycle_last_sequence,
            Vec::new(),
        )
        .await;
    }

    async fn heartbeat_with_inventory(
        service: &ControlPlane<InMemoryAssignmentStore>,
        node_id: &str,
        lifecycle_stream_id: &str,
        lifecycle_last_sequence: u64,
        sandbox_ids: Vec<String>,
    ) {
        service
            .heartbeat(Request::new(proto::HeartbeatRequest {
                node_id: node_id.to_string(),
                cluster_id: "cluster-1".to_string(),
                service_instance_id: format!("{node_id}-instance"),
                version: "1.0.0".to_string(),
                commit: "abc".to_string(),
                machine_info: Some(proto::MachineInfo {
                    cpu_architecture: "x86_64".to_string(),
                    ..proto::MachineInfo::default()
                }),
                snapshot: Some(proto::NodeSnapshot {
                    status: proto::NodeStatus::Ready as i32,
                    cpu_count: 8,
                    memory_total_bytes: 16 * 1024 * MIB,
                    disks: vec![proto::DiskMetric {
                        mount_point: "/".to_string(),
                        total_bytes: 100 * 1024 * MIB,
                        ..proto::DiskMetric::default()
                    }],
                    ..proto::NodeSnapshot::default()
                }),
                p2p_endpoint: Some(proto::P2pEndpoint {
                    backend: "iroh".to_string(),
                    address: format!("{node_id}.internal:443"),
                }),
                lifecycle_stream_id: lifecycle_stream_id.to_string(),
                lifecycle_last_sequence,
                sandbox_ids,
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stable_schedule_claim_replays_and_confirmation_retains_shadow_capacity() {
        let service = control_plane();
        heartbeat(&service, "node-a").await;
        heartbeat(&service, "node-b").await;
        let sandbox_id = Uuid::now_v7().to_string();
        let request = || {
            Request::new(proto::ScheduleRequest {
                sandbox_id: sandbox_id.clone(),
                ..proto::ScheduleRequest::default()
            })
        };

        let first = service
            .schedule(request())
            .await
            .unwrap()
            .into_inner()
            .node
            .unwrap();
        let replay = service
            .schedule(request())
            .await
            .unwrap()
            .into_inner()
            .node
            .unwrap();
        assert_eq!(first, replay);
        assert_eq!(
            service
                .registry
                .pending(&first.node_id, Instant::now())
                .unwrap()
                .sandboxes,
            1
        );

        service
            .record_assignment(Request::new(proto::RecordAssignmentRequest {
                sandbox_id: sandbox_id.clone(),
                node: Some(first.clone()),
            }))
            .await
            .unwrap();
        assert_eq!(
            service
                .registry
                .pending(&first.node_id, Instant::now())
                .unwrap(),
            PendingResources {
                sandboxes: 1,
                starting: 1,
                cpu: 1,
                memory_bytes: 512 * MIB,
                disk_bytes: 1024 * MIB,
            }
        );
        let looked_up = service
            .lookup_node(Request::new(proto::LookupNodeRequest {
                sandbox_id: sandbox_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .node
            .unwrap();
        assert_eq!(looked_up, first);

        heartbeat_with_inventory(&service, &first.node_id, "", 0, vec![sandbox_id]).await;
        assert_eq!(
            service
                .registry
                .pending(&first.node_id, Instant::now())
                .unwrap(),
            PendingResources::default()
        );
    }

    #[tokio::test]
    async fn scheduling_fails_closed_without_eligible_heartbeats() {
        let service = control_plane();
        let error = service
            .schedule(Request::new(proto::ScheduleRequest {
                sandbox_id: Uuid::now_v7().to_string(),
                ..proto::ScheduleRequest::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn artifact_lookup_only_returns_live_compatible_peers() {
        let service = control_plane();
        heartbeat(&service, "node-a").await;
        heartbeat(&service, "node-b").await;
        service
            .record_p2p_artifact(Request::new(proto::RecordP2pArtifactRequest {
                cluster_id: "cluster-1".to_string(),
                backend: "iroh".to_string(),
                key: "sha256-digest".to_string(),
                node_id: "node-a".to_string(),
            }))
            .await
            .unwrap();
        let peers = service
            .lookup_p2p_artifact(Request::new(proto::LookupP2pArtifactRequest {
                cluster_id: "cluster-1".to_string(),
                backend: "iroh".to_string(),
                key: "sha256-digest".to_string(),
                exclude_node_id: "node-b".to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .peers;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "node-a");
    }

    fn lifecycle_event(
        sandbox_id: &str,
        stream_id: &str,
        sequence: u64,
        event_type: proto::SandboxEventType,
    ) -> proto::SandboxEvent {
        proto::SandboxEvent {
            sandbox_id: sandbox_id.to_string(),
            event_type: event_type as i32,
            requested_cpu: 1,
            requested_memory_bytes: 512 * MIB,
            requested_disk_bytes: 1024 * MIB,
            sequence,
            event_id: format!("{stream_id}:{sequence}"),
            occurred_at_unix_ms: unix_millis(),
        }
    }

    #[tokio::test]
    async fn lifecycle_events_materialize_routes_ack_duplicates_and_reject_gaps() {
        let service = control_plane();
        let sandbox_id = Uuid::now_v7().to_string();
        let stream_id = Uuid::now_v7().to_string();
        heartbeat_with_stream(&service, "node-a", &stream_id, 2).await;
        let request = |sequence, event_type| {
            Request::new(proto::ReportSandboxEventRequest {
                node_id: "node-a".to_string(),
                cluster_id: "cluster-1".to_string(),
                service_instance_id: "node-a-instance".to_string(),
                events: vec![lifecycle_event(
                    &sandbox_id,
                    &stream_id,
                    sequence,
                    event_type,
                )],
                lifecycle_stream_id: stream_id.clone(),
            })
        };

        let first = service
            .report_sandbox_event(request(1, proto::SandboxEventType::Create))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.acknowledged_sequence, 1);
        assert_eq!(
            service
                .lookup_node(Request::new(proto::LookupNodeRequest {
                    sandbox_id: sandbox_id.clone(),
                }))
                .await
                .unwrap()
                .into_inner()
                .node
                .unwrap()
                .node_id,
            "node-a"
        );

        let replay = service
            .report_sandbox_event(request(1, proto::SandboxEventType::Create))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(replay.acknowledged_sequence, 1);
        let gap = service
            .report_sandbox_event(request(3, proto::SandboxEventType::Delete))
            .await
            .unwrap_err();
        assert_eq!(gap.code(), tonic::Code::FailedPrecondition);

        let deleted = service
            .report_sandbox_event(request(2, proto::SandboxEventType::Delete))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(deleted.acknowledged_sequence, 2);
        assert_eq!(
            service
                .lookup_node(Request::new(proto::LookupNodeRequest { sandbox_id }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }

    #[tokio::test]
    async fn lifecycle_events_require_the_current_service_instance() {
        let service = control_plane();
        let stream_id = Uuid::now_v7().to_string();
        heartbeat_with_stream(&service, "node-a", &stream_id, 1).await;
        let error = service
            .report_sandbox_event(Request::new(proto::ReportSandboxEventRequest {
                node_id: "node-a".to_string(),
                cluster_id: "cluster-1".to_string(),
                service_instance_id: "stale-instance".to_string(),
                events: vec![lifecycle_event(
                    &Uuid::now_v7().to_string(),
                    &stream_id,
                    1,
                    proto::SandboxEventType::Create,
                )],
                lifecycle_stream_id: stream_id,
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn heartbeat_inventory_repairs_and_hysteretically_removes_routes() {
        let service = control_plane();
        let sandbox_id = Uuid::now_v7().to_string();
        let stream_id = Uuid::now_v7().to_string();
        heartbeat_with_inventory(&service, "node-a", &stream_id, 0, vec![sandbox_id.clone()]).await;
        assert!(service
            .lookup_node(Request::new(proto::LookupNodeRequest {
                sandbox_id: sandbox_id.clone(),
            }))
            .await
            .is_ok());

        for _ in 0..2 {
            heartbeat_with_stream(&service, "node-a", &stream_id, 0).await;
            assert!(service
                .lookup_node(Request::new(proto::LookupNodeRequest {
                    sandbox_id: sandbox_id.clone(),
                }))
                .await
                .is_ok());
        }
        heartbeat_with_stream(&service, "node-a", &stream_id, 0).await;
        assert_eq!(
            service
                .lookup_node(Request::new(proto::LookupNodeRequest { sandbox_id }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
}
