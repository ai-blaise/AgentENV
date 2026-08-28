use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::Script;

use crate::assignment::{
    AssignmentStore, ClaimOutcome, ClaimRequest, LifecycleBatch, LifecycleEventKind,
    ReconcileRequest, ReconcileResult, StoreError,
};
use crate::migration::{
    validate_begin, validate_update, BeginMigration, MigrationAction, MigrationPhase,
    MigrationRecord, UpdateMigration,
};
use crate::model::{Assignment, AssignmentState, Node};

const DEFAULT_KEY_PREFIX: &str = "agentenv:control-plane";
const CLEANUP_BATCH: usize = 256;

/// Multi-replica assignment store. Each claim and its node-capacity
/// reservation are committed by one Redis script, so a stable sandbox ID can
/// never be assigned to two nodes even when schedulers race.
#[derive(Clone)]
pub struct RedisAssignmentStore {
    connection: ConnectionManager,
    key_prefix: String,
    cluster_id: String,
    reservation_ttl: Duration,
    lease_ttl: Duration,
}

impl RedisAssignmentStore {
    pub async fn connect(
        redis_url: &str,
        cluster_id: &str,
        reservation_ttl: Duration,
        lease_ttl: Duration,
    ) -> Result<Self, StoreError> {
        validate_ttls(reservation_ttl, lease_ttl)?;
        validate_key_component(cluster_id, "cluster_id")?;
        let client = redis::Client::open(redis_url)
            .map_err(|error| StoreError::Backend(format!("parse Redis URL: {error}")))?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(|error| StoreError::Backend(format!("connect to Redis: {error}")))?;
        let store = Self {
            connection,
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            cluster_id: cluster_id.to_string(),
            reservation_ttl,
            lease_ttl,
        };
        let mut connection = store.connection.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("ping Redis: {error}")))?;
        Ok(store)
    }

    fn assignment_key(&self, sandbox_id: &str) -> String {
        format!(
            "{}:{{{}}}:assignment:{}",
            self.key_prefix, self.cluster_id, sandbox_id
        )
    }

    fn fence_key(&self, sandbox_id: &str) -> String {
        format!(
            "{}:{{{}}}:fence:{}",
            self.key_prefix, self.cluster_id, sandbox_id
        )
    }

    fn migration_key(&self, sandbox_id: &str) -> String {
        format!(
            "{}:{{{}}}:migration:{}",
            self.key_prefix, self.cluster_id, sandbox_id
        )
    }

    fn node_keys(&self, node_id: &str) -> [String; 3] {
        let base = format!(
            "{}:{{{}}}:pending:{}",
            self.key_prefix, self.cluster_id, node_id
        );
        [
            format!("{base}:totals"),
            format!("{base}:expires"),
            format!("{base}:resources"),
        ]
    }

    fn reservation_ttl_ms(&self) -> Result<u64, StoreError> {
        duration_millis(self.reservation_ttl, "reservation_ttl")
    }

    fn lease_ttl_ms(&self) -> Result<u64, StoreError> {
        duration_millis(self.lease_ttl, "lease_ttl")
    }

    fn lifecycle_cursor_key(&self, node_id: &str) -> String {
        format!(
            "{}:{{{}}}:lifecycle-cursor:{}",
            self.key_prefix, self.cluster_id, node_id
        )
    }

    fn lifecycle_stream_key(&self) -> String {
        format!(
            "{}:{{{}}}:lifecycle-events",
            self.key_prefix, self.cluster_id
        )
    }

    fn node_routes_key(&self, node_id: &str) -> String {
        format!(
            "{}:{{{}}}:routes:{}",
            self.key_prefix, self.cluster_id, node_id
        )
    }

    fn node_route_misses_key(&self, node_id: &str) -> String {
        format!(
            "{}:{{{}}}:route-misses:{}",
            self.key_prefix, self.cluster_id, node_id
        )
    }
}

#[async_trait]
impl AssignmentStore for RedisAssignmentStore {
    async fn lookup(
        &self,
        sandbox_id: &str,
        _now: Instant,
    ) -> Result<Option<Assignment>, StoreError> {
        validate_key_component(sandbox_id, "sandbox_id")?;
        let mut connection = self.connection.clone();
        let raw = redis::cmd("GET")
            .arg(self.assignment_key(sandbox_id))
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("lookup assignment: {error}")))?;
        raw.map(|value| decode_assignment(&value)).transpose()
    }

    async fn claim(&self, request: ClaimRequest) -> Result<ClaimOutcome, StoreError> {
        validate_key_component(&request.sandbox_id, "sandbox_id")?;
        validate_key_component(&request.node.id, "node_id")?;
        if request.node.endpoint.trim().is_empty() {
            return Err(StoreError::Invalid("endpoint must be non-empty"));
        }

        let assignment = Assignment {
            sandbox_id: request.sandbox_id.clone(),
            node: Node {
                generation: request.node.generation.max(1),
                ..request.node.clone()
            },
            state: AssignmentState::Reserved,
        };
        let encoded = encode_assignment(&assignment)?;
        let assignment_key = self.assignment_key(&request.sandbox_id);
        let [totals_key, expiry_key, resources_key] = self.node_keys(&request.node.id);
        let reservation_ttl_ms = self.reservation_ttl_ms()?;
        let keys_ttl_ms = reservation_ttl_ms.saturating_mul(2);

        let mut connection = self.connection.clone();
        let (code, raw): (i64, String) = Script::new(CLAIM_SCRIPT)
            .key(assignment_key)
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .key(self.fence_key(&request.sandbox_id))
            .arg(encoded)
            .arg(&request.sandbox_id)
            .arg(request.resources.cpu)
            .arg(request.resources.memory_bytes)
            .arg(request.resources.disk_bytes)
            .arg(request.observed.sandboxes)
            .arg(request.observed.starting)
            .arg(request.observed.cpu)
            .arg(request.observed.memory_bytes)
            .arg(request.observed.disk_bytes)
            .arg(request.limits.max_sandboxes.unwrap_or(0))
            .arg(request.limits.max_starting.unwrap_or(0))
            .arg(request.limits.max_cpu.unwrap_or(0))
            .arg(request.limits.max_memory_bytes.unwrap_or(0))
            .arg(request.limits.max_disk_bytes.unwrap_or(0))
            .arg(reservation_ttl_ms)
            .arg(keys_ttl_ms)
            .arg(CLEANUP_BATCH)
            .arg(&request.node.id)
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("claim assignment: {error}")))?;

        match code {
            1 => Ok(ClaimOutcome::Claimed(decode_assignment(&raw)?)),
            0 => Ok(ClaimOutcome::Existing(decode_assignment(&raw)?)),
            -1 => Err(StoreError::CapacityExhausted {
                node_id: request.node.id,
            }),
            -3 => Err(StoreError::Retired {
                sandbox_id: request.sandbox_id,
            }),
            -4 => Err(StoreError::OwnershipConflict {
                sandbox_id: request.sandbox_id,
                assigned_node: raw,
                requested_node: request.node.id,
            }),
            _ => Err(StoreError::Invariant(format!(
                "Redis claim script returned code {code}: {raw}"
            ))),
        }
    }

    async fn confirm(
        &self,
        sandbox_id: &str,
        node: Node,
        _now: Instant,
    ) -> Result<Assignment, StoreError> {
        validate_key_component(sandbox_id, "sandbox_id")?;
        validate_key_component(&node.id, "node_id")?;
        if node.endpoint.trim().is_empty() {
            return Err(StoreError::Invalid("endpoint must be non-empty"));
        }
        let assignment = Assignment {
            sandbox_id: sandbox_id.to_string(),
            node: Node {
                generation: node.generation.max(1),
                ..node
            },
            state: AssignmentState::Confirmed,
        };
        let encoded = encode_assignment(&assignment)?;
        let [totals_key, expiry_key, resources_key] = self.node_keys(&assignment.node.id);
        let mut connection = self.connection.clone();
        let (code, raw): (i64, String) = Script::new(CONFIRM_SCRIPT)
            .key(self.assignment_key(sandbox_id))
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .key(self.node_routes_key(&assignment.node.id))
            .key(self.fence_key(sandbox_id))
            .arg(encoded)
            .arg(sandbox_id)
            .arg(&assignment.node.id)
            .arg(self.lease_ttl_ms()?)
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("confirm assignment: {error}")))?;

        match code {
            1 => decode_assignment(&raw),
            -1 => Err(StoreError::OwnershipConflict {
                sandbox_id: sandbox_id.to_string(),
                assigned_node: ownership_node(&raw)?,
                requested_node: assignment.node.id,
            }),
            -2 => Err(StoreError::Invariant(raw)),
            -3 => Err(StoreError::Retired {
                sandbox_id: sandbox_id.to_string(),
            }),
            _ => Err(StoreError::Invariant(format!(
                "Redis confirm script returned code {code}: {raw}"
            ))),
        }
    }

    async fn apply_lifecycle_batch(&self, batch: LifecycleBatch) -> Result<u64, StoreError> {
        validate_key_component(&batch.node.id, "node_id")?;
        validate_key_component(&batch.service_instance_id, "service_instance_id")?;
        validate_key_component(&batch.stream_id, "stream_id")?;
        if batch.events.is_empty() {
            return Err(StoreError::Invalid(
                "lifecycle event batch must be non-empty",
            ));
        }

        let script = Script::new(APPLY_LIFECYCLE_SCRIPT);
        let mut invocation = script.prepare_invoke();
        let [totals_key, expiry_key, resources_key] = self.node_keys(&batch.node.id);
        invocation
            .key(self.lifecycle_cursor_key(&batch.node.id))
            .key(self.lifecycle_stream_key())
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .key(self.node_routes_key(&batch.node.id))
            .key(self.node_route_misses_key(&batch.node.id));
        for event in &batch.events {
            validate_key_component(&event.sandbox_id, "sandbox_id")?;
            invocation.key(self.assignment_key(&event.sandbox_id));
        }
        for event in &batch.events {
            invocation.key(self.fence_key(&event.sandbox_id));
        }
        invocation
            .arg(&batch.stream_id)
            .arg(&batch.service_instance_id)
            .arg(&batch.node.id)
            .arg(batch.events[0].sequence)
            .arg(batch.events.len())
            .arg(self.lease_ttl_ms()?);
        for event in &batch.events {
            let assignment = Assignment {
                sandbox_id: event.sandbox_id.clone(),
                node: batch.node.clone(),
                state: AssignmentState::Confirmed,
            };
            invocation
                .arg(event.sequence)
                .arg(&event.event_id)
                .arg(lifecycle_kind(event.kind))
                .arg(encode_assignment(&assignment)?)
                .arg(&event.sandbox_id)
                .arg(serde_json::to_string(event).map_err(|error| {
                    StoreError::Invariant(format!("serialize lifecycle event: {error}"))
                })?);
        }

        let mut connection = self.connection.clone();
        let (code, acknowledged, detail): (i64, u64, String) = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("apply lifecycle events: {error}")))?;
        match code {
            1 | 0 => Ok(acknowledged),
            -1 => Err(StoreError::OwnershipConflict {
                sandbox_id: batch.events[0].sandbox_id.clone(),
                assigned_node: ownership_node(&detail)?,
                requested_node: batch.node.id,
            }),
            -2 => Err(StoreError::Invariant(detail)),
            -3 => Err(StoreError::SequenceConflict(detail)),
            -4 => Err(StoreError::Retired { sandbox_id: detail }),
            _ => Err(StoreError::Invariant(format!(
                "Redis lifecycle script returned code {code}: {detail}"
            ))),
        }
    }

    async fn reconcile_node(
        &self,
        request: ReconcileRequest,
    ) -> Result<ReconcileResult, StoreError> {
        validate_key_component(&request.node.id, "node_id")?;
        if request.node.endpoint.trim().is_empty() {
            return Err(StoreError::Invalid("endpoint must be non-empty"));
        }
        if request.missing_heartbeat_threshold == 0 {
            return Err(StoreError::Invalid(
                "missing heartbeat threshold must be greater than zero",
            ));
        }

        let mut desired = request.sandbox_ids;
        desired.sort_unstable();
        desired.dedup();
        for sandbox_id in &desired {
            validate_key_component(sandbox_id, "sandbox_id")?;
        }

        let mut connection = self.connection.clone();
        let mut current = redis::cmd("SMEMBERS")
            .arg(self.node_routes_key(&request.node.id))
            .query_async::<Vec<String>>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("read node route index: {error}")))?;
        current.sort_unstable();
        current.dedup();
        for sandbox_id in &current {
            validate_key_component(sandbox_id, "indexed sandbox_id")?;
        }

        let script = Script::new(RECONCILE_NODE_SCRIPT);
        let mut invocation = script.prepare_invoke();
        let [totals_key, expiry_key, resources_key] = self.node_keys(&request.node.id);
        invocation
            .key(self.node_routes_key(&request.node.id))
            .key(self.node_route_misses_key(&request.node.id))
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .arg(&request.node.id)
            .arg(request.missing_heartbeat_threshold)
            .arg(desired.len())
            .arg(self.lease_ttl_ms()?);
        for sandbox_id in &desired {
            invocation.key(self.assignment_key(sandbox_id));
        }
        for sandbox_id in &current {
            invocation.key(self.assignment_key(sandbox_id));
        }
        for sandbox_id in &desired {
            invocation.key(self.fence_key(sandbox_id));
        }
        for sandbox_id in &desired {
            let assignment = Assignment {
                sandbox_id: sandbox_id.clone(),
                node: request.node.clone(),
                state: AssignmentState::Confirmed,
            };
            invocation
                .arg(sandbox_id)
                .arg(encode_assignment(&assignment)?);
        }
        invocation.arg(current.len());
        for sandbox_id in &current {
            invocation.arg(sandbox_id);
        }
        let (code, repaired, removed, detail): (i64, u64, u64, String) = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("reconcile node routes: {error}")))?;
        match code {
            1 => Ok(ReconcileResult { repaired, removed }),
            -1 => Err(StoreError::OwnershipConflict {
                sandbox_id: desired.first().cloned().unwrap_or_default(),
                assigned_node: ownership_node(&detail)?,
                requested_node: request.node.id,
            }),
            -2 => Err(StoreError::Invariant(detail)),
            _ => Err(StoreError::Invariant(format!(
                "Redis reconciliation script returned code {code}: {detail}"
            ))),
        }
    }

    async fn begin_migration(
        &self,
        request: BeginMigration,
    ) -> Result<MigrationRecord, StoreError> {
        validate_begin(&request).map_err(StoreError::Invalid)?;
        for (value, name) in [
            (&request.migration_id, "migration_id"),
            (&request.sandbox_id, "sandbox_id"),
            (&request.source.id, "source_node_id"),
            (&request.destination.id, "destination_node_id"),
        ] {
            validate_key_component(value, name)?;
        }
        let destination = Node {
            generation: request.expected_generation + 1,
            ..request.destination.clone()
        };
        let record = MigrationRecord {
            migration_id: request.migration_id.clone(),
            sandbox_id: request.sandbox_id.clone(),
            source_generation: request.expected_generation,
            source: request.source.clone(),
            destination: destination.clone(),
            phase: MigrationPhase::Preparing,
            checkpoint_id: None,
            manifest_digest: None,
            durable_coverage: false,
            destination_prepared: false,
            created_at_unix_ms: request.now_unix_ms,
            updated_at_unix_ms: request.now_unix_ms,
            abort_reason: None,
        };
        let [totals_key, expiry_key, resources_key] = self.node_keys(&destination.id);
        let reservation_ttl_ms = self.lease_ttl_ms()?;
        let keys_ttl_ms = reservation_ttl_ms.saturating_mul(2);
        let mut connection = self.connection.clone();
        let (code, raw): (i64, String) = Script::new(BEGIN_MIGRATION_SCRIPT)
            .key(self.assignment_key(&request.sandbox_id))
            .key(self.fence_key(&request.sandbox_id))
            .key(self.migration_key(&request.sandbox_id))
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .arg(encode_migration(&record)?)
            .arg(&request.migration_id)
            .arg(&request.sandbox_id)
            .arg(&request.source.id)
            .arg(request.expected_generation)
            .arg(&destination.id)
            .arg(request.resources.cpu)
            .arg(request.resources.memory_bytes)
            .arg(request.resources.disk_bytes)
            .arg(request.destination_observed.sandboxes)
            .arg(request.destination_observed.starting)
            .arg(request.destination_observed.cpu)
            .arg(request.destination_observed.memory_bytes)
            .arg(request.destination_observed.disk_bytes)
            .arg(request.destination_limits.max_sandboxes.unwrap_or(0))
            .arg(request.destination_limits.max_starting.unwrap_or(0))
            .arg(request.destination_limits.max_cpu.unwrap_or(0))
            .arg(request.destination_limits.max_memory_bytes.unwrap_or(0))
            .arg(request.destination_limits.max_disk_bytes.unwrap_or(0))
            .arg(reservation_ttl_ms)
            .arg(keys_ttl_ms)
            .arg(CLEANUP_BATCH)
            .arg(migration_reservation_id(&request.migration_id))
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("begin migration: {error}")))?;
        match code {
            1 | 0 => decode_migration(&raw),
            -1 => Err(StoreError::CapacityExhausted {
                node_id: destination.id,
            }),
            -2 => Err(StoreError::Invariant(raw)),
            -3 => Err(StoreError::MigrationNotFound),
            -4 => Err(StoreError::MigrationConflict(raw)),
            _ => Err(StoreError::Invariant(format!(
                "Redis begin migration script returned code {code}: {raw}"
            ))),
        }
    }

    async fn update_migration(
        &self,
        request: UpdateMigration,
    ) -> Result<MigrationRecord, StoreError> {
        validate_update(&request).map_err(StoreError::Invalid)?;
        for (value, name) in [
            (&request.migration_id, "migration_id"),
            (&request.sandbox_id, "sandbox_id"),
            (&request.actor_node_id, "actor_node_id"),
        ] {
            validate_key_component(value, name)?;
        }
        let current = self
            .lookup_migration(&request.sandbox_id, &request.migration_id)
            .await?
            .ok_or(StoreError::MigrationNotFound)?;
        let [totals_key, expiry_key, resources_key] = self.node_keys(&current.destination.id);
        let (checkpoint_id, manifest_digest, durable_coverage, reason) =
            migration_action_arguments(&request.action);
        let reservation_ttl_ms = self.lease_ttl_ms()?;
        let mut connection = self.connection.clone();
        let (code, raw): (i64, String) = Script::new(UPDATE_MIGRATION_SCRIPT)
            .key(self.assignment_key(&request.sandbox_id))
            .key(self.fence_key(&request.sandbox_id))
            .key(self.migration_key(&request.sandbox_id))
            .key(self.node_routes_key(&current.source.id))
            .key(self.node_routes_key(&current.destination.id))
            .key(totals_key)
            .key(expiry_key)
            .key(resources_key)
            .arg(&request.migration_id)
            .arg(&request.sandbox_id)
            .arg(&request.actor_node_id)
            .arg(request.action.name())
            .arg(checkpoint_id)
            .arg(manifest_digest)
            .arg(durable_coverage)
            .arg(reason)
            .arg(request.now_unix_ms)
            .arg(reservation_ttl_ms)
            .arg(reservation_ttl_ms.saturating_mul(2))
            .arg(self.lease_ttl_ms()?)
            .arg(migration_reservation_id(&request.migration_id))
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("update migration: {error}")))?;
        match code {
            1 | 0 => decode_migration(&raw),
            -2 => Err(StoreError::Invariant(raw)),
            -3 => Err(StoreError::MigrationNotFound),
            -4 | -5 => Err(StoreError::MigrationConflict(raw)),
            _ => Err(StoreError::Invariant(format!(
                "Redis update migration script returned code {code}: {raw}"
            ))),
        }
    }

    async fn lookup_migration(
        &self,
        sandbox_id: &str,
        migration_id: &str,
    ) -> Result<Option<MigrationRecord>, StoreError> {
        validate_key_component(sandbox_id, "sandbox_id")?;
        validate_key_component(migration_id, "migration_id")?;
        let mut connection = self.connection.clone();
        let raw = redis::cmd("GET")
            .arg(self.migration_key(sandbox_id))
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("lookup migration: {error}")))?;
        raw.map(|raw| decode_migration(&raw))
            .transpose()
            .map(|record| record.filter(|record| record.migration_id == migration_id))
    }
}

fn migration_reservation_id(migration_id: &str) -> String {
    format!("migration.{migration_id}")
}

fn migration_action_arguments(action: &MigrationAction) -> (&str, &str, bool, &str) {
    match action {
        MigrationAction::RecordCheckpoint {
            checkpoint_id,
            manifest_digest,
            durable_coverage,
        } => (checkpoint_id, manifest_digest, *durable_coverage, ""),
        MigrationAction::Abort { reason } => ("", "", false, reason),
        _ => ("", "", false, ""),
    }
}

fn lifecycle_kind(kind: LifecycleEventKind) -> &'static str {
    match kind {
        LifecycleEventKind::Create => "create",
        LifecycleEventKind::Delete => "delete",
        LifecycleEventKind::Pause => "pause",
        LifecycleEventKind::Resume => "resume",
        LifecycleEventKind::Fork => "fork",
    }
}

fn validate_ttls(reservation_ttl: Duration, lease_ttl: Duration) -> Result<(), StoreError> {
    if reservation_ttl.is_zero() {
        return Err(StoreError::Invalid("reservation_ttl must be non-zero"));
    }
    if lease_ttl < reservation_ttl {
        return Err(StoreError::Invalid(
            "lease_ttl must be at least reservation_ttl",
        ));
    }
    duration_millis(reservation_ttl, "reservation_ttl")?;
    duration_millis(lease_ttl, "lease_ttl")?;
    Ok(())
}

fn duration_millis(duration: Duration, name: &'static str) -> Result<u64, StoreError> {
    u64::try_from(duration.as_millis())
        .ok()
        .filter(|value| *value > 0)
        .ok_or(StoreError::Invalid(name))
}

fn validate_key_component(value: &str, name: &'static str) -> Result<(), StoreError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(StoreError::Invalid(name));
    }
    Ok(())
}

fn encode_assignment(assignment: &Assignment) -> Result<String, StoreError> {
    serde_json::to_string(assignment)
        .map_err(|error| StoreError::Invariant(format!("serialize assignment: {error}")))
}

fn decode_assignment(raw: &str) -> Result<Assignment, StoreError> {
    serde_json::from_str(raw)
        .map_err(|error| StoreError::Invariant(format!("decode assignment: {error}")))
}

fn encode_migration(migration: &MigrationRecord) -> Result<String, StoreError> {
    serde_json::to_string(migration)
        .map_err(|error| StoreError::Invariant(format!("serialize migration: {error}")))
}

fn decode_migration(raw: &str) -> Result<MigrationRecord, StoreError> {
    serde_json::from_str(raw)
        .map_err(|error| StoreError::Invariant(format!("decode migration: {error}")))
}

fn ownership_node(raw: &str) -> Result<String, StoreError> {
    if let Ok(assignment) = serde_json::from_str::<Assignment>(raw) {
        return Ok(assignment.node.id);
    }
    if validate_key_component(raw, "fence owner").is_ok() {
        return Ok(raw.to_string());
    }
    Err(StoreError::Invariant(
        "assignment script returned invalid ownership detail".to_string(),
    ))
}

const BEGIN_MIGRATION_SCRIPT: &str = r#"
local assignment_raw = redis.call('GET', KEYS[1])
local fence_raw = redis.call('GET', KEYS[2])
if not assignment_raw or not fence_raw then
  return {-3, 'active assignment or fence is missing'}
end
local assignment_ok, assignment = pcall(cjson.decode, assignment_raw)
local fence_ok, fence = pcall(cjson.decode, fence_raw)
if not assignment_ok or not assignment or not assignment['node']
    or not assignment['node']['id'] or not assignment['node']['generation']
    or not assignment['state'] then
  return {-2, 'corrupt active assignment'}
end
if not fence_ok or not fence or not fence['owner_node_id'] or not fence['generation'] then
  return {-2, 'corrupt ownership fence'}
end
local source_node_id = ARGV[4]
local source_generation = tonumber(ARGV[5])
local destination_node_id = ARGV[6]
if assignment['state'] ~= 'confirmed'
    or assignment['node']['id'] ~= source_node_id
    or tonumber(assignment['node']['generation']) ~= source_generation then
  return {-4, 'source owner or generation does not match the active route'}
end
if fence['retired'] or fence['owner_node_id'] ~= source_node_id
    or tonumber(fence['generation']) ~= source_generation then
  return {-4, 'source owner or generation does not match the ownership fence'}
end
if source_node_id == destination_node_id then
  return {-4, 'migration source and destination must differ'}
end

local requested_ok, requested = pcall(cjson.decode, ARGV[1])
if not requested_ok or not requested or not requested['source'] or not requested['destination']
    or requested['migration_id'] ~= ARGV[2] or requested['sandbox_id'] ~= ARGV[3]
    or requested['source']['id'] ~= source_node_id
    or requested['destination']['id'] ~= destination_node_id
    or tonumber(requested['source_generation']) ~= source_generation
    or tonumber(requested['destination']['generation']) ~= source_generation + 1 then
  return {-2, 'corrupt requested migration'}
end

local existing_raw = redis.call('GET', KEYS[3])
if existing_raw then
  local existing_ok, existing = pcall(cjson.decode, existing_raw)
  if not existing_ok or not existing or not existing['migration_id'] or not existing['phase'] then
    return {-2, 'corrupt migration record'}
  end
  if existing['migration_id'] == ARGV[2] then
    if existing['source']['id'] ~= source_node_id
        or existing['destination']['id'] ~= destination_node_id
        or tonumber(existing['source_generation']) ~= source_generation then
      return {-4, 'migration ID was reused with different parameters'}
    end
    return {0, existing_raw}
  end
  if existing['phase'] ~= 'source_released' and existing['phase'] ~= 'aborted' then
    return {-4, 'another migration is already active'}
  end
end

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local cleanup_limit = tonumber(ARGV[22])
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', now_ms, 'LIMIT', 0, cleanup_limit)
for _, reservation_id in ipairs(expired) do
  local encoded = redis.call('HGET', KEYS[6], reservation_id)
  if encoded then
    local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
    if not cpu then
      return {-2, 'corrupt reservation ' .. reservation_id}
    end
    redis.call('HINCRBY', KEYS[4], 'sandboxes', -1)
    redis.call('HINCRBY', KEYS[4], 'starting', -1)
    redis.call('HINCRBY', KEYS[4], 'cpu', -tonumber(cpu))
    redis.call('HINCRBY', KEYS[4], 'memory', -tonumber(memory))
    redis.call('HINCRBY', KEYS[4], 'disk', -tonumber(disk))
    redis.call('HDEL', KEYS[6], reservation_id)
  end
  redis.call('ZREM', KEYS[5], reservation_id)
end

local pending = redis.call('HMGET', KEYS[4], 'sandboxes', 'starting', 'cpu', 'memory', 'disk')
local after = {
  tonumber(ARGV[10]) + tonumber(pending[1] or '0') + 1,
  tonumber(ARGV[11]) + tonumber(pending[2] or '0') + 1,
  tonumber(ARGV[12]) + tonumber(pending[3] or '0') + tonumber(ARGV[7]),
  tonumber(ARGV[13]) + tonumber(pending[4] or '0') + tonumber(ARGV[8]),
  tonumber(ARGV[14]) + tonumber(pending[5] or '0') + tonumber(ARGV[9])
}
local limits = {
  tonumber(ARGV[15]), tonumber(ARGV[16]), tonumber(ARGV[17]),
  tonumber(ARGV[18]), tonumber(ARGV[19])
}
for index = 1, 5 do
  if limits[index] > 0 and after[index] > limits[index] then
    return {-1, 'destination capacity exhausted'}
  end
end

local reservation_id = ARGV[23]
redis.call('SET', KEYS[3], ARGV[1])
redis.call('HINCRBY', KEYS[4], 'sandboxes', 1)
redis.call('HINCRBY', KEYS[4], 'starting', 1)
redis.call('HINCRBY', KEYS[4], 'cpu', ARGV[7])
redis.call('HINCRBY', KEYS[4], 'memory', ARGV[8])
redis.call('HINCRBY', KEYS[4], 'disk', ARGV[9])
redis.call('HSET', KEYS[6], reservation_id, ARGV[7] .. ':' .. ARGV[8] .. ':' .. ARGV[9])
redis.call('ZADD', KEYS[5], now_ms + tonumber(ARGV[20]), reservation_id)
redis.call('PEXPIRE', KEYS[4], ARGV[21])
redis.call('PEXPIRE', KEYS[5], ARGV[21])
redis.call('PEXPIRE', KEYS[6], ARGV[21])
return {1, ARGV[1]}
"#;

const UPDATE_MIGRATION_SCRIPT: &str = r#"
local raw = redis.call('GET', KEYS[3])
if not raw then
  return {-3, 'migration not found'}
end
local ok, migration = pcall(cjson.decode, raw)
if not ok or not migration or not migration['migration_id'] or not migration['sandbox_id']
    or not migration['source'] or not migration['source']['id']
    or not migration['destination'] or not migration['destination']['id']
    or not migration['phase'] then
  return {-2, 'corrupt migration record'}
end
if migration['migration_id'] ~= ARGV[1] or migration['sandbox_id'] ~= ARGV[2] then
  return {-3, 'migration not found'}
end
local actor = ARGV[3]
local action = ARGV[4]
local source_id = migration['source']['id']
local destination_id = migration['destination']['id']
local source_action = action == 'record_checkpoint' or action == 'quiesce_source'
    or action == 'commit' or action == 'release_source'
local destination_action = action == 'prepare_destination' or action == 'activate_destination'
if source_action and actor ~= source_id then
  return {-4, 'migration action must be submitted by the source'}
end
if destination_action and actor ~= destination_id then
  return {-4, 'migration action must be submitted by the destination'}
end
if action == 'abort' and actor ~= source_id and actor ~= destination_id then
  return {-4, 'migration abort must be submitted by a participating node'}
end

local phase = migration['phase']
local terminal = phase == 'source_released' or phase == 'aborted'
local post_commit = phase == 'committed' or phase == 'destination_active'
    or phase == 'source_released'
local reservation_id = ARGV[13]
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)

local function release_reservation()
  local encoded = redis.call('HGET', KEYS[8], reservation_id)
  if encoded then
    local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
    if not cpu then
      return false
    end
    redis.call('HINCRBY', KEYS[6], 'sandboxes', -1)
    redis.call('HINCRBY', KEYS[6], 'starting', -1)
    redis.call('HINCRBY', KEYS[6], 'cpu', -tonumber(cpu))
    redis.call('HINCRBY', KEYS[6], 'memory', -tonumber(memory))
    redis.call('HINCRBY', KEYS[6], 'disk', -tonumber(disk))
    redis.call('HDEL', KEYS[8], reservation_id)
  end
  redis.call('ZREM', KEYS[7], reservation_id)
  return true
end

if not terminal and not post_commit and action ~= 'abort' then
  local encoded = redis.call('HGET', KEYS[8], reservation_id)
  local expires_at = tonumber(redis.call('ZSCORE', KEYS[7], reservation_id) or '0')
  if not encoded or expires_at <= now_ms then
    if not release_reservation() then
      return {-2, 'corrupt migration reservation'}
    end
    return {-5, 'destination reservation expired before migration completed'}
  end
  redis.call('ZADD', KEYS[7], now_ms + tonumber(ARGV[10]), reservation_id)
  redis.call('PEXPIRE', KEYS[6], ARGV[11])
  redis.call('PEXPIRE', KEYS[7], ARGV[11])
  redis.call('PEXPIRE', KEYS[8], ARGV[11])
end

local changed = false
if action == 'record_checkpoint' then
  if phase ~= 'preparing' and phase ~= 'ready_to_cutover' then
    if migration['checkpoint_id'] == ARGV[5] and migration['manifest_digest'] == ARGV[6]
        and (not (ARGV[7] == '1') or migration['durable_coverage']) then
      return {0, raw}
    end
    return {-4, 'checkpoint cannot change after source quiesce'}
  end
  if migration['checkpoint_id'] and migration['checkpoint_id'] ~= ARGV[5] then
    return {-4, 'checkpoint identity changed within one migration'}
  end
  if migration['manifest_digest'] and migration['manifest_digest'] ~= ARGV[6] then
    return {-4, 'manifest digest changed within one migration'}
  end
  migration['checkpoint_id'] = ARGV[5]
  migration['manifest_digest'] = ARGV[6]
  if ARGV[7] == '1' then
    migration['durable_coverage'] = true
  end
  changed = true
elseif action == 'prepare_destination' then
  if phase ~= 'preparing' and phase ~= 'ready_to_cutover' then
    if migration['destination_prepared'] then
      return {0, raw}
    end
    return {-4, 'destination cannot prepare in the current phase'}
  end
  if not migration['checkpoint_id'] or not migration['manifest_digest'] then
    return {-4, 'destination cannot prepare before checkpoint publication'}
  end
  migration['destination_prepared'] = true
  changed = true
elseif action == 'quiesce_source' then
  if phase == 'ready_to_cutover' then
    migration['phase'] = 'source_quiesced'
    changed = true
  elseif phase ~= 'source_quiesced' and not post_commit then
    return {-4, 'source can only quiesce after durable destination preparation'}
  end
elseif action == 'commit' then
  if phase == 'source_quiesced' then
    if not migration['durable_coverage'] or not migration['destination_prepared'] then
      return {-4, 'migration lacks durable prepared coverage'}
    end
    local assignment_raw = redis.call('GET', KEYS[1])
    local fence_raw = redis.call('GET', KEYS[2])
    if not assignment_raw or not fence_raw then
      return {-4, 'active route or ownership fence expired before commit'}
    end
    local assignment_ok, assignment = pcall(cjson.decode, assignment_raw)
    local fence_ok, fence = pcall(cjson.decode, fence_raw)
    if not assignment_ok or not assignment or not assignment['node']
        or not fence_ok or not fence or not fence['owner_node_id'] then
      return {-2, 'corrupt assignment or ownership fence'}
    end
    local source_generation = tonumber(migration['source_generation'])
    if assignment['state'] ~= 'confirmed' or assignment['node']['id'] ~= source_id
        or tonumber(assignment['node']['generation']) ~= source_generation
        or fence['retired'] or fence['owner_node_id'] ~= source_id
        or tonumber(fence['generation']) ~= source_generation then
      return {-4, 'active ownership changed before migration commit'}
    end
    assignment['node'] = migration['destination']
    assignment['state'] = 'confirmed'
    redis.call('PSETEX', KEYS[1], ARGV[12], cjson.encode(assignment))
    redis.call('SET', KEYS[2], cjson.encode({
      owner_node_id = destination_id,
      generation = tonumber(migration['destination']['generation']),
      retired = false
    }))
    redis.call('SREM', KEYS[4], ARGV[2])
    redis.call('SADD', KEYS[5], ARGV[2])
    migration['phase'] = 'committed'
    changed = true
  elseif not post_commit then
    return {-4, 'migration can only commit after source quiesce'}
  end
elseif action == 'activate_destination' then
  if phase == 'committed' then
    migration['phase'] = 'destination_active'
    changed = true
  elseif phase ~= 'destination_active' and phase ~= 'source_released' then
    return {-4, 'destination can only activate after ownership commits'}
  end
elseif action == 'release_source' then
  if phase == 'destination_active' then
    if not release_reservation() then
      return {-2, 'corrupt migration reservation'}
    end
    migration['phase'] = 'source_released'
    changed = true
  elseif phase ~= 'source_released' then
    return {-4, 'source can only release after destination activation'}
  end
elseif action == 'abort' then
  if post_commit then
    return {-4, 'a committed migration cannot abort back to its source'}
  end
  if phase ~= 'aborted' then
    if not release_reservation() then
      return {-2, 'corrupt migration reservation'}
    end
    migration['phase'] = 'aborted'
    migration['abort_reason'] = ARGV[8]
    changed = true
  end
else
  return {-2, 'unknown migration action'}
end

if migration['phase'] == 'preparing' and migration['durable_coverage']
    and migration['destination_prepared'] then
  migration['phase'] = 'ready_to_cutover'
  changed = true
end
if changed then
  migration['updated_at_unix_ms'] = tonumber(ARGV[9])
  raw = cjson.encode(migration)
  redis.call('SET', KEYS[3], raw)
  return {1, raw}
end
return {0, raw}
"#;

const CLAIM_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[1])
if existing then
  local assignment_ok, assignment = pcall(cjson.decode, existing)
  if not assignment_ok or not assignment or not assignment['node']
      or not assignment['node']['id'] then
    return {-2, 'corrupt assignment'}
  end
  local fence = redis.call('GET', KEYS[5])
  if not fence then
    redis.call('SET', KEYS[5], cjson.encode({
      owner_node_id = assignment['node']['id'],
      generation = tonumber(assignment['node']['generation'] or 1),
      retired = false
    }))
  else
    local fence_ok, decoded = pcall(cjson.decode, fence)
    if not fence_ok or not decoded or decoded['retired']
        or decoded['owner_node_id'] ~= assignment['node']['id']
        or tonumber(decoded['generation']) ~= tonumber(assignment['node']['generation'] or 1) then
      return {-2, 'assignment fence mismatch'}
    end
  end
  return {0, existing}
end

local owner_node_id = ARGV[19]
local generation = 1
local fence = redis.call('GET', KEYS[5])
if fence then
  local ok, decoded = pcall(cjson.decode, fence)
  if not ok or not decoded or not decoded['owner_node_id'] or not decoded['generation'] then
    return {-2, 'corrupt fence'}
  end
  if decoded['retired'] then
    return {-3, 'retired'}
  end
  if decoded['owner_node_id'] ~= owner_node_id then
    return {-4, decoded['owner_node_id']}
  end
  generation = tonumber(decoded['generation'])
end

local assignment_ok, assignment = pcall(cjson.decode, ARGV[1])
if not assignment_ok or not assignment or not assignment['node'] then
  return {-2, 'corrupt requested assignment'}
end
assignment['node']['generation'] = generation
local encoded_assignment = cjson.encode(assignment)

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local cleanup_limit = tonumber(ARGV[18])
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now_ms, 'LIMIT', 0, cleanup_limit)
for _, sandbox_id in ipairs(expired) do
  local encoded = redis.call('HGET', KEYS[4], sandbox_id)
  if encoded then
    local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
    if not cpu then
      return {-2, 'corrupt reservation ' .. sandbox_id}
    end
    redis.call('HINCRBY', KEYS[2], 'sandboxes', -1)
    redis.call('HINCRBY', KEYS[2], 'starting', -1)
    redis.call('HINCRBY', KEYS[2], 'cpu', -tonumber(cpu))
    redis.call('HINCRBY', KEYS[2], 'memory', -tonumber(memory))
    redis.call('HINCRBY', KEYS[2], 'disk', -tonumber(disk))
    redis.call('HDEL', KEYS[4], sandbox_id)
  end
  redis.call('ZREM', KEYS[3], sandbox_id)
end

local pending = redis.call('HMGET', KEYS[2], 'sandboxes', 'starting', 'cpu', 'memory', 'disk')
local after = {
  tonumber(ARGV[6]) + tonumber(pending[1] or '0') + 1,
  tonumber(ARGV[7]) + tonumber(pending[2] or '0') + 1,
  tonumber(ARGV[8]) + tonumber(pending[3] or '0') + tonumber(ARGV[3]),
  tonumber(ARGV[9]) + tonumber(pending[4] or '0') + tonumber(ARGV[4]),
  tonumber(ARGV[10]) + tonumber(pending[5] or '0') + tonumber(ARGV[5])
}
local limits = {
  tonumber(ARGV[11]), tonumber(ARGV[12]), tonumber(ARGV[13]),
  tonumber(ARGV[14]), tonumber(ARGV[15])
}
for i = 1, 5 do
  if limits[i] > 0 and after[i] > limits[i] then
    return {-1, 'capacity'}
  end
end

redis.call('SET', KEYS[5], cjson.encode({
  owner_node_id = owner_node_id,
  generation = generation,
  retired = false
}))
redis.call('PSETEX', KEYS[1], ARGV[16], encoded_assignment)
redis.call('HINCRBY', KEYS[2], 'sandboxes', 1)
redis.call('HINCRBY', KEYS[2], 'starting', 1)
redis.call('HINCRBY', KEYS[2], 'cpu', ARGV[3])
redis.call('HINCRBY', KEYS[2], 'memory', ARGV[4])
redis.call('HINCRBY', KEYS[2], 'disk', ARGV[5])
redis.call('HSET', KEYS[4], ARGV[2], ARGV[3] .. ':' .. ARGV[4] .. ':' .. ARGV[5])
redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[16]), ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[17])
redis.call('PEXPIRE', KEYS[3], ARGV[17])
redis.call('PEXPIRE', KEYS[4], ARGV[17])
return {1, encoded_assignment}
"#;

const CONFIRM_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[1])
local confirmed = ARGV[1]
local owner_node_id = ARGV[3]
local confirmed_ok, confirmed_decoded = pcall(cjson.decode, confirmed)
if not confirmed_ok or not confirmed_decoded or not confirmed_decoded['node'] then
  return {-2, 'corrupt confirmed assignment'}
end
local generation = math.max(tonumber(confirmed_decoded['node']['generation'] or 1), 1)
local fence = redis.call('GET', KEYS[6])
if fence then
  local fence_ok, fence_decoded = pcall(cjson.decode, fence)
  if not fence_ok or not fence_decoded or not fence_decoded['owner_node_id']
      or not fence_decoded['generation'] then
    return {-2, 'corrupt fence'}
  end
  if fence_decoded['retired'] then
    return {-3, 'retired'}
  end
  if fence_decoded['owner_node_id'] ~= owner_node_id then
    return {-1, fence_decoded['owner_node_id']}
  end
  generation = tonumber(fence_decoded['generation'])
end
if existing then
  local ok, decoded = pcall(cjson.decode, existing)
  if not ok or not decoded or not decoded['node'] or not decoded['node']['id'] then
    return {-2, 'corrupt assignment'}
  end
  if decoded['node']['id'] ~= owner_node_id then
    return {-1, decoded['node']['id']}
  end
  generation = math.max(generation, tonumber(decoded['node']['generation'] or 1))
end

confirmed_decoded['node']['generation'] = generation
confirmed = cjson.encode(confirmed_decoded)
redis.call('SET', KEYS[6], cjson.encode({
  owner_node_id = owner_node_id,
  generation = generation,
  retired = false
}))
redis.call('PSETEX', KEYS[1], ARGV[4], confirmed)
redis.call('SADD', KEYS[5], ARGV[2])
return {1, confirmed}
"#;

const APPLY_LIFECYCLE_SCRIPT: &str = r#"
local stream_id = ARGV[1]
local service_instance_id = ARGV[2]
local node_id = ARGV[3]
local first_sequence = tonumber(ARGV[4])
local event_count = tonumber(ARGV[5])
local lease_ttl_ms = tonumber(ARGV[6])
if not first_sequence or not event_count or event_count < 1 or not lease_ttl_ms then
  return {-2, 0, 'invalid lifecycle batch header'}
end

local current_stream = redis.call('HGET', KEYS[1], 'stream_id')
local current_sequence = tonumber(redis.call('HGET', KEYS[1], 'sequence') or '0')
local stream_changed = current_stream and current_stream ~= stream_id
if (not current_stream or stream_changed) and first_sequence ~= 1 then
  return {-3, current_sequence, 'new lifecycle stream must begin at sequence 1'}
end
if not stream_changed and first_sequence > current_sequence + 1 then
  return {-3, current_sequence, 'lifecycle sequence gap'}
end
local already_applied = stream_changed and 0 or current_sequence

for index = 1, event_count do
  local base = 6 + (index - 1) * 6
  local sequence = tonumber(ARGV[base + 1])
  local event_id = ARGV[base + 2]
  local kind = ARGV[base + 3]
  local expected = first_sequence + index - 1
  if sequence ~= expected then
    return {-2, current_sequence, 'lifecycle event sequences are not contiguous'}
  end
  if event_id ~= stream_id .. ':' .. tostring(sequence) then
    return {-2, current_sequence, 'lifecycle event id does not match its stream and sequence'}
  end
  if kind ~= 'create' and kind ~= 'delete' and kind ~= 'pause'
      and kind ~= 'resume' and kind ~= 'fork' then
    return {-2, current_sequence, 'unknown lifecycle event kind'}
  end
  if sequence > already_applied then
    local sandbox_id = ARGV[base + 5]
    local reservation = redis.call('HGET', KEYS[5], sandbox_id)
    if reservation and not string.match(reservation, '^(%d+):(%d+):(%d+)$') then
      return {-2, current_sequence, 'corrupt reservation'}
    end
    local existing = redis.call('GET', KEYS[index + 7])
    if existing then
      local ok, decoded = pcall(cjson.decode, existing)
      if not ok or not decoded or not decoded['node'] or not decoded['node']['id'] then
        return {-2, current_sequence, 'corrupt assignment'}
      end
      if (kind == 'create' or kind == 'resume' or kind == 'fork')
          and decoded['node']['id'] ~= node_id then
        return {-1, current_sequence, decoded['node']['id']}
      end
    end
    local fence = redis.call('GET', KEYS[7 + event_count + index])
    if fence then
      local fence_ok, fence_decoded = pcall(cjson.decode, fence)
      if not fence_ok or not fence_decoded or not fence_decoded['owner_node_id']
          or not fence_decoded['generation'] then
        return {-2, current_sequence, 'corrupt fence'}
      end
      if (kind == 'create' or kind == 'resume' or kind == 'fork')
          and fence_decoded['retired'] then
        return {-4, current_sequence, sandbox_id}
      end
      if (kind == 'create' or kind == 'resume' or kind == 'fork')
          and fence_decoded['owner_node_id'] ~= node_id then
        return {-1, current_sequence, fence_decoded['owner_node_id']}
      end
    end
  end
end

local last_sequence = first_sequence + event_count - 1
if not stream_changed and last_sequence <= already_applied then
  return {0, already_applied, ''}
end

local function release_reservation(sandbox_id)
  local encoded = redis.call('HGET', KEYS[5], sandbox_id)
  if not encoded then
    return true
  end
  local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
  if not cpu then
    return false
  end
  redis.call('HINCRBY', KEYS[3], 'sandboxes', -1)
  redis.call('HINCRBY', KEYS[3], 'starting', -1)
  redis.call('HINCRBY', KEYS[3], 'cpu', -tonumber(cpu))
  redis.call('HINCRBY', KEYS[3], 'memory', -tonumber(memory))
  redis.call('HINCRBY', KEYS[3], 'disk', -tonumber(disk))
  redis.call('HDEL', KEYS[5], sandbox_id)
  redis.call('ZREM', KEYS[4], sandbox_id)
  return true
end

for index = 1, event_count do
  local base = 6 + (index - 1) * 6
  local sequence = tonumber(ARGV[base + 1])
  if sequence > already_applied then
    local event_id = ARGV[base + 2]
    local kind = ARGV[base + 3]
    local assignment = ARGV[base + 4]
    local sandbox_id = ARGV[base + 5]
    local event_json = ARGV[base + 6]
    local fence_key = KEYS[7 + event_count + index]
    if kind == 'create' or kind == 'resume' or kind == 'fork' then
      local existing = redis.call('GET', KEYS[index + 7])
      local generation = 1
      local fence = redis.call('GET', fence_key)
      if fence then
        generation = tonumber(cjson.decode(fence)['generation'])
      end
      local assignment_decoded = cjson.decode(assignment)
      if existing then
        local existing_decoded = cjson.decode(existing)
        generation = math.max(generation, tonumber(existing_decoded['node']['generation'] or 1))
      end
      assignment_decoded['node']['generation'] = generation
      assignment = cjson.encode(assignment_decoded)
      redis.call('SET', fence_key, cjson.encode({
        owner_node_id = node_id,
        generation = generation,
        retired = false
      }))
      redis.call('PSETEX', KEYS[index + 7], lease_ttl_ms, assignment)
      redis.call('SADD', KEYS[6], sandbox_id)
      redis.call('HDEL', KEYS[7], sandbox_id)
    elseif kind == 'delete' then
      if not release_reservation(sandbox_id) then
        return {-2, current_sequence, 'corrupt reservation'}
      end
      local existing = redis.call('GET', KEYS[index + 7])
      if existing then
        local ok, decoded = pcall(cjson.decode, existing)
        if not ok or not decoded or not decoded['node'] or not decoded['node']['id'] then
          return {-2, current_sequence, 'corrupt assignment'}
        end
        if decoded['node']['id'] == node_id then
          local fence = redis.call('GET', fence_key)
          local generation = tonumber(decoded['node']['generation'] or 1)
          if fence then
            generation = tonumber(cjson.decode(fence)['generation'])
          end
          redis.call('SET', fence_key, cjson.encode({
            owner_node_id = node_id,
            generation = generation,
            retired = true
          }))
          redis.call('DEL', KEYS[index + 7])
          redis.call('SREM', KEYS[6], sandbox_id)
          redis.call('HDEL', KEYS[7], sandbox_id)
        end
      end
      local current_fence = redis.call('GET', fence_key)
      if current_fence then
        local fence_decoded = cjson.decode(current_fence)
        if fence_decoded['owner_node_id'] == node_id then
          fence_decoded['retired'] = true
          redis.call('SET', fence_key, cjson.encode(fence_decoded))
        end
      elseif not existing then
        redis.call('SET', fence_key, cjson.encode({
          owner_node_id = node_id,
          generation = 1,
          retired = true
        }))
      end
    elseif kind == 'pause' then
      local existing = redis.call('GET', KEYS[index + 7])
      if existing then
        local decoded = cjson.decode(existing)
        if decoded['node']['id'] == node_id then
          redis.call('PEXPIRE', KEYS[index + 7], lease_ttl_ms)
        end
      end
    end
    redis.call(
      'XADD', KEYS[2], 'MAXLEN', '~', 1000000, '*',
      'event_id', event_id,
      'node_id', node_id,
      'stream_id', stream_id,
      'sequence', tostring(sequence),
      'event', event_json
    )
  end
end
redis.call(
  'HSET', KEYS[1],
  'stream_id', stream_id,
  'service_instance_id', service_instance_id,
  'sequence', last_sequence
)
return {1, last_sequence, ''}
"#;

const RECONCILE_NODE_SCRIPT: &str = r#"
local node_id = ARGV[1]
local missing_threshold = tonumber(ARGV[2])
local desired_count = tonumber(ARGV[3])
local lease_ttl_ms = tonumber(ARGV[4])
if not missing_threshold or missing_threshold < 1 or not desired_count or not lease_ttl_ms then
  return {-2, 0, 0, 'invalid reconciliation header'}
end

local current_count_offset = 4 + desired_count * 2
local current_count = tonumber(ARGV[current_count_offset + 1])
if not current_count then
  return {-2, 0, 0, 'invalid current route count'}
end

local desired = {}
local retired = {}
for index = 1, desired_count do
  local base = 4 + (index - 1) * 2
  local sandbox_id = ARGV[base + 1]
  local assignment_key = KEYS[index + 5]
  local fence_key = KEYS[5 + desired_count + current_count + index]
  local reservation = redis.call('HGET', KEYS[5], sandbox_id)
  if reservation and not string.match(reservation, '^(%d+):(%d+):(%d+)$') then
    return {-2, 0, 0, 'corrupt reservation'}
  end
  local existing = redis.call('GET', assignment_key)
  if existing then
    local ok, decoded = pcall(cjson.decode, existing)
    if not ok or not decoded or not decoded['node'] or not decoded['node']['id'] then
      return {-2, 0, 0, 'corrupt assignment'}
    end
    if decoded['node']['id'] ~= node_id then
      return {-1, 0, 0, decoded['node']['id']}
    end
  end
  local fence = redis.call('GET', fence_key)
  if fence then
    local fence_ok, fence_decoded = pcall(cjson.decode, fence)
    if not fence_ok or not fence_decoded or not fence_decoded['owner_node_id']
        or not fence_decoded['generation'] then
      return {-2, 0, 0, 'corrupt fence'}
    end
    if fence_decoded['retired'] then
      retired[sandbox_id] = true
    elseif fence_decoded['owner_node_id'] ~= node_id then
      return {-1, 0, 0, fence_decoded['owner_node_id']}
    end
  end
  desired[sandbox_id] = true
end

for index = 1, current_count do
  local existing = redis.call('GET', KEYS[desired_count + index + 5])
  if existing then
    local ok, decoded = pcall(cjson.decode, existing)
    if not ok or not decoded or not decoded['node'] or not decoded['node']['id']
        or not decoded['state'] then
      return {-2, 0, 0, 'corrupt indexed assignment'}
    end
  end
end

local repaired = 0
local function release_reservation(sandbox_id)
  local encoded = redis.call('HGET', KEYS[5], sandbox_id)
  if not encoded then
    return
  end
  local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
  redis.call('HINCRBY', KEYS[3], 'sandboxes', -1)
  redis.call('HINCRBY', KEYS[3], 'starting', -1)
  redis.call('HINCRBY', KEYS[3], 'cpu', -tonumber(cpu))
  redis.call('HINCRBY', KEYS[3], 'memory', -tonumber(memory))
  redis.call('HINCRBY', KEYS[3], 'disk', -tonumber(disk))
  redis.call('HDEL', KEYS[5], sandbox_id)
  redis.call('ZREM', KEYS[4], sandbox_id)
end
for index = 1, desired_count do
  local base = 4 + (index - 1) * 2
  local sandbox_id = ARGV[base + 1]
  local encoded = ARGV[base + 2]
  local assignment_key = KEYS[index + 5]
  local fence_key = KEYS[5 + desired_count + current_count + index]
  local existing = redis.call('GET', assignment_key)
  release_reservation(sandbox_id)
  if not retired[sandbox_id] then
    local generation = 1
    local fence = redis.call('GET', fence_key)
    if fence then
      generation = tonumber(cjson.decode(fence)['generation'])
    elseif existing then
      generation = tonumber(cjson.decode(existing)['node']['generation'] or 1)
    end
    local encoded_decoded = cjson.decode(encoded)
    encoded_decoded['node']['generation'] = generation
    encoded = cjson.encode(encoded_decoded)
    if existing ~= encoded then
      repaired = repaired + 1
    end
    redis.call('SET', fence_key, cjson.encode({
      owner_node_id = node_id,
      generation = generation,
      retired = false
    }))
    redis.call('PSETEX', assignment_key, lease_ttl_ms, encoded)
    redis.call('SADD', KEYS[1], sandbox_id)
    redis.call('HDEL', KEYS[2], sandbox_id)
  else
    redis.call('DEL', assignment_key)
    redis.call('SREM', KEYS[1], sandbox_id)
    redis.call('HDEL', KEYS[2], sandbox_id)
  end
end

local removed = 0
for index = 1, current_count do
  local sandbox_id = ARGV[current_count_offset + index + 1]
  if not desired[sandbox_id] then
    local assignment_key = KEYS[desired_count + index + 5]
    local existing = redis.call('GET', assignment_key)
    if not existing then
      redis.call('SREM', KEYS[1], sandbox_id)
      redis.call('HDEL', KEYS[2], sandbox_id)
    else
      local decoded = cjson.decode(existing)
      if decoded['node']['id'] ~= node_id then
        redis.call('SREM', KEYS[1], sandbox_id)
        redis.call('HDEL', KEYS[2], sandbox_id)
      elseif decoded['state'] == 'confirmed' then
        local misses = redis.call('HINCRBY', KEYS[2], sandbox_id, 1)
        if misses >= missing_threshold then
          redis.call('DEL', assignment_key)
          redis.call('SREM', KEYS[1], sandbox_id)
          redis.call('HDEL', KEYS[2], sandbox_id)
          removed = removed + 1
        end
      else
        redis.call('HDEL', KEYS[2], sandbox_id)
      end
    end
  end
end
return {1, repaired, removed, ''}
"#;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::assignment::{ClaimOutcome, ClaimRequest};
    use crate::model::{CapacityLimits, PendingResources, SandboxResources};

    #[test]
    fn key_components_are_strict_and_bounded() {
        for valid in [
            "cluster-1",
            "NODE_2",
            "019c4f58-8a74-7e11-82de-2b87f6ada375",
        ] {
            validate_key_component(valid, "value").unwrap();
        }
        for invalid in ["", "has space", "has{tag}", "slash/value", "line\nfeed"] {
            assert!(
                validate_key_component(invalid, "value").is_err(),
                "{invalid:?}"
            );
        }
        assert!(validate_key_component(&"a".repeat(129), "value").is_err());
    }

    #[test]
    fn assignment_encoding_round_trips() {
        let assignment = Assignment {
            sandbox_id: "sandbox-1".to_string(),
            node: Node::new("node-1", "https://node-1.internal"),
            state: AssignmentState::Reserved,
        };
        assert_eq!(
            decode_assignment(&encode_assignment(&assignment).unwrap()).unwrap(),
            assignment
        );
    }

    #[tokio::test]
    #[ignore = "requires AGENTENV_TEST_REDIS_URL"]
    async fn redis_claims_are_atomic_across_connections_and_release_expired_capacity() {
        let redis_url = std::env::var("AGENTENV_TEST_REDIS_URL")
            .expect("AGENTENV_TEST_REDIS_URL must be set for this test");
        let cluster_id = format!("test-{}", uuid::Uuid::now_v7());
        let left = RedisAssignmentStore::connect(
            &redis_url,
            &cluster_id,
            Duration::from_millis(200),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let right = RedisAssignmentStore::connect(
            &redis_url,
            &cluster_id,
            Duration::from_millis(200),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let now = Instant::now();
        let request = |sandbox_id: &str, node_id: &str| ClaimRequest {
            sandbox_id: sandbox_id.to_string(),
            node: Node::new(node_id, format!("https://{node_id}")),
            resources: SandboxResources {
                cpu: 1,
                memory_bytes: 1024,
                disk_bytes: 1024,
            },
            observed: PendingResources::default(),
            limits: CapacityLimits {
                max_sandboxes: Some(1),
                max_starting: Some(1),
                max_cpu: Some(1),
                max_memory_bytes: Some(1024),
                max_disk_bytes: Some(1024),
            },
            now,
        };

        let (first, replay) = tokio::join!(
            left.claim(request("same-sandbox", "node-a")),
            right.claim(request("same-sandbox", "node-b")),
        );
        let first = first.unwrap();
        let replay = replay.unwrap();
        assert_ne!(
            matches!(first, ClaimOutcome::Claimed(_)),
            matches!(replay, ClaimOutcome::Claimed(_))
        );
        assert_eq!(assignment(&first), assignment(&replay));

        left.claim(request("capacity-1", "node-c")).await.unwrap();
        let denied = right
            .claim(request("capacity-2", "node-c"))
            .await
            .unwrap_err();
        assert!(matches!(denied, StoreError::CapacityExhausted { .. }));

        tokio::time::sleep(Duration::from_millis(250)).await;
        right.claim(request("capacity-2", "node-c")).await.unwrap();

        let lifecycle_sandbox = uuid::Uuid::now_v7().to_string();
        let lifecycle_stream = uuid::Uuid::now_v7().to_string();
        left.claim(request(&lifecycle_sandbox, "node-d"))
            .await
            .unwrap();
        let event = |sequence, kind| crate::assignment::LifecycleEvent {
            sandbox_id: lifecycle_sandbox.clone(),
            kind,
            resources: SandboxResources {
                cpu: 1,
                memory_bytes: 1024,
                disk_bytes: 1024,
            },
            sequence,
            event_id: format!("{lifecycle_stream}:{sequence}"),
            occurred_at_unix_ms: 1,
        };
        let batch = |sequence, kind| LifecycleBatch {
            node: Node::new("node-d", "https://node-d"),
            service_instance_id: "instance-d".to_string(),
            stream_id: lifecycle_stream.clone(),
            events: vec![event(sequence, kind)],
            now: Instant::now(),
        };
        assert_eq!(
            left.apply_lifecycle_batch(batch(1, LifecycleEventKind::Create))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            right
                .apply_lifecycle_batch(batch(1, LifecycleEventKind::Create))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            left.lookup(&lifecycle_sandbox, Instant::now())
                .await
                .unwrap()
                .unwrap()
                .state,
            AssignmentState::Confirmed
        );
        assert!(matches!(
            right
                .apply_lifecycle_batch(batch(3, LifecycleEventKind::Delete))
                .await
                .unwrap_err(),
            StoreError::SequenceConflict(_)
        ));
        assert_eq!(
            left.apply_lifecycle_batch(batch(2, LifecycleEventKind::Delete))
                .await
                .unwrap(),
            2
        );
        assert!(right
            .lookup(&lifecycle_sandbox, Instant::now())
            .await
            .unwrap()
            .is_none());
        let retired_inventory = left
            .reconcile_node(ReconcileRequest {
                node: Node::new("node-d", "https://node-d"),
                sandbox_ids: vec![lifecycle_sandbox.clone()],
                missing_heartbeat_threshold: 3,
                now: Instant::now(),
            })
            .await
            .unwrap();
        assert_eq!(retired_inventory.repaired, 0);
        assert!(matches!(
            left.claim(request(&lifecycle_sandbox, "node-d")).await,
            Err(StoreError::Retired { .. })
        ));
        let mut connection = left.connection.clone();
        let stream_length = redis::cmd("XLEN")
            .arg(left.lifecycle_stream_key())
            .query_async::<u64>(&mut connection)
            .await
            .unwrap();
        assert!(stream_length >= 2);

        let reconcile_sandbox = uuid::Uuid::now_v7().to_string();
        let reconcile = |sandbox_ids: Vec<String>| ReconcileRequest {
            node: Node::new("node-d", "https://node-d"),
            sandbox_ids,
            missing_heartbeat_threshold: 3,
            now: Instant::now(),
        };
        let repaired = left
            .reconcile_node(reconcile(vec![reconcile_sandbox.clone()]))
            .await
            .unwrap();
        assert_eq!(repaired.repaired, 1);
        for _ in 0..2 {
            let result = right.reconcile_node(reconcile(Vec::new())).await.unwrap();
            assert_eq!(result.removed, 0);
            assert!(left
                .lookup(&reconcile_sandbox, Instant::now())
                .await
                .unwrap()
                .is_some());
        }
        let removed = left.reconcile_node(reconcile(Vec::new())).await.unwrap();
        assert_eq!(removed.removed, 1);
        assert!(right
            .lookup(&reconcile_sandbox, Instant::now())
            .await
            .unwrap()
            .is_none());

        let generation_sandbox = uuid::Uuid::now_v7().to_string();
        let mut generation_seven = Node::new("node-e", "https://node-e");
        generation_seven.generation = 7;
        left.confirm(&generation_sandbox, generation_seven, Instant::now())
            .await
            .unwrap();
        right
            .reconcile_node(ReconcileRequest {
                node: Node::new("node-e", "https://node-e"),
                sandbox_ids: vec![generation_sandbox.clone()],
                missing_heartbeat_threshold: 3,
                now: Instant::now(),
            })
            .await
            .unwrap();
        assert_eq!(
            left.lookup(&generation_sandbox, Instant::now())
                .await
                .unwrap()
                .unwrap()
                .node
                .generation,
            7
        );

        let lease_cluster = format!("test-lease-{}", uuid::Uuid::now_v7());
        let lease_store = RedisAssignmentStore::connect(
            &redis_url,
            &lease_cluster,
            Duration::from_millis(50),
            Duration::from_millis(150),
        )
        .await
        .unwrap();
        let lease_sandbox = uuid::Uuid::now_v7().to_string();
        lease_store
            .claim(request(&lease_sandbox, "node-lease"))
            .await
            .unwrap();
        lease_store
            .confirm(
                &lease_sandbox,
                Node::new("node-lease", "https://node-lease"),
                Instant::now(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(175)).await;
        assert!(lease_store
            .lookup(&lease_sandbox, Instant::now())
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            lease_store
                .claim(request(&lease_sandbox, "different-node"))
                .await,
            Err(StoreError::OwnershipConflict { .. })
        ));

        let migration_sandbox = uuid::Uuid::now_v7().to_string();
        let migration_id = uuid::Uuid::now_v7().to_string();
        let mut migration_source = Node::new("node-source", "https://node-source");
        migration_source.generation = 7;
        left.confirm(&migration_sandbox, migration_source.clone(), Instant::now())
            .await
            .unwrap();
        let migration = left
            .begin_migration(BeginMigration {
                migration_id: migration_id.clone(),
                sandbox_id: migration_sandbox.clone(),
                source: migration_source,
                destination: Node::new("node-destination", "https://node-destination"),
                expected_generation: 7,
                resources: SandboxResources {
                    cpu: 1,
                    memory_bytes: 1024,
                    disk_bytes: 2048,
                },
                destination_observed: PendingResources::default(),
                destination_limits: CapacityLimits::default(),
                now: Instant::now(),
                now_unix_ms: 1,
            })
            .await
            .unwrap();
        assert_eq!(migration.destination.generation, 8);
        let update = |actor: &str, action: MigrationAction, now_unix_ms| UpdateMigration {
            migration_id: migration_id.clone(),
            sandbox_id: migration_sandbox.clone(),
            actor_node_id: actor.to_string(),
            action,
            now: Instant::now(),
            now_unix_ms,
        };
        left.update_migration(update(
            "node-source",
            MigrationAction::RecordCheckpoint {
                checkpoint_id: uuid::Uuid::now_v7().to_string(),
                manifest_digest: format!("sha256:{}", "a".repeat(64)),
                durable_coverage: true,
            },
            2,
        ))
        .await
        .unwrap();
        left.update_migration(update(
            "node-destination",
            MigrationAction::PrepareDestination,
            3,
        ))
        .await
        .unwrap();
        left.update_migration(update("node-source", MigrationAction::QuiesceSource, 4))
            .await
            .unwrap();
        let committed = left
            .update_migration(update("node-source", MigrationAction::Commit, 5))
            .await
            .unwrap();
        assert_eq!(committed.phase, MigrationPhase::Committed);
        assert_eq!(
            right
                .lookup(&migration_sandbox, Instant::now())
                .await
                .unwrap()
                .unwrap()
                .node,
            committed.destination
        );
        let stale_stream_id = uuid::Uuid::now_v7().to_string();
        assert!(matches!(
            right
                .apply_lifecycle_batch(LifecycleBatch {
                    node: Node::new("node-source", "https://node-source"),
                    service_instance_id: "source-instance".to_string(),
                    stream_id: stale_stream_id.clone(),
                    events: vec![crate::assignment::LifecycleEvent {
                        sandbox_id: migration_sandbox.clone(),
                        kind: LifecycleEventKind::Resume,
                        resources: SandboxResources {
                            cpu: 1,
                            memory_bytes: 1024,
                            disk_bytes: 2048,
                        },
                        sequence: 1,
                        event_id: format!("{stale_stream_id}:1"),
                        occurred_at_unix_ms: 1,
                    }],
                    now: Instant::now(),
                })
                .await,
            Err(StoreError::OwnershipConflict { .. })
        ));
        left.update_migration(update(
            "node-destination",
            MigrationAction::ActivateDestination,
            6,
        ))
        .await
        .unwrap();
        assert_eq!(
            left.update_migration(update("node-source", MigrationAction::ReleaseSource, 7,))
                .await
                .unwrap()
                .phase,
            MigrationPhase::SourceReleased
        );
    }

    fn assignment(outcome: &ClaimOutcome) -> &Assignment {
        match outcome {
            ClaimOutcome::Claimed(assignment) | ClaimOutcome::Existing(assignment) => assignment,
        }
    }
}
