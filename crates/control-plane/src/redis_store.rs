use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::Script;

use crate::assignment::{AssignmentStore, ClaimOutcome, ClaimRequest, StoreError};
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
    confirmed_ttl: Duration,
}

impl RedisAssignmentStore {
    pub async fn connect(
        redis_url: &str,
        cluster_id: &str,
        reservation_ttl: Duration,
        confirmed_ttl: Duration,
    ) -> Result<Self, StoreError> {
        validate_ttls(reservation_ttl, confirmed_ttl)?;
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
            confirmed_ttl,
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

    fn confirmed_ttl_ms(&self) -> Result<u64, StoreError> {
        duration_millis(self.confirmed_ttl, "confirmed_ttl")
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
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("claim assignment: {error}")))?;

        match code {
            1 => Ok(ClaimOutcome::Claimed(decode_assignment(&raw)?)),
            0 => Ok(ClaimOutcome::Existing(decode_assignment(&raw)?)),
            -1 => Err(StoreError::CapacityExhausted {
                node_id: request.node.id,
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
            .arg(encoded)
            .arg(sandbox_id)
            .arg(&assignment.node.id)
            .arg(self.confirmed_ttl_ms()?)
            .invoke_async(&mut connection)
            .await
            .map_err(|error| StoreError::Backend(format!("confirm assignment: {error}")))?;

        match code {
            1 => decode_assignment(&raw),
            -1 => {
                let existing = decode_assignment(&raw)?;
                Err(StoreError::OwnershipConflict {
                    sandbox_id: sandbox_id.to_string(),
                    assigned_node: existing.node.id,
                    requested_node: assignment.node.id,
                })
            }
            _ => Err(StoreError::Invariant(format!(
                "Redis confirm script returned code {code}: {raw}"
            ))),
        }
    }
}

fn validate_ttls(reservation_ttl: Duration, confirmed_ttl: Duration) -> Result<(), StoreError> {
    if reservation_ttl.is_zero() {
        return Err(StoreError::Invalid("reservation_ttl must be non-zero"));
    }
    if confirmed_ttl < reservation_ttl {
        return Err(StoreError::Invalid(
            "confirmed_ttl must be at least reservation_ttl",
        ));
    }
    duration_millis(reservation_ttl, "reservation_ttl")?;
    duration_millis(confirmed_ttl, "confirmed_ttl")?;
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

const CLAIM_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[1])
if existing then
  return {0, existing}
end

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

redis.call('PSETEX', KEYS[1], ARGV[16], ARGV[1])
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
return {1, ARGV[1]}
"#;

const CONFIRM_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[1])
if existing then
  local ok, decoded = pcall(cjson.decode, existing)
  if not ok or not decoded or not decoded['node'] or not decoded['node']['id'] then
    return {-2, 'corrupt assignment'}
  end
  if decoded['node']['id'] ~= ARGV[3] then
    return {-1, existing}
  end
end

local encoded = redis.call('HGET', KEYS[4], ARGV[2])
if encoded then
  local cpu, memory, disk = string.match(encoded, '^(%d+):(%d+):(%d+)$')
  if not cpu then
    return {-2, 'corrupt reservation'}
  end
  redis.call('HINCRBY', KEYS[2], 'sandboxes', -1)
  redis.call('HINCRBY', KEYS[2], 'starting', -1)
  redis.call('HINCRBY', KEYS[2], 'cpu', -tonumber(cpu))
  redis.call('HINCRBY', KEYS[2], 'memory', -tonumber(memory))
  redis.call('HINCRBY', KEYS[2], 'disk', -tonumber(disk))
  redis.call('HDEL', KEYS[4], ARGV[2])
  redis.call('ZREM', KEYS[3], ARGV[2])
end
redis.call('PSETEX', KEYS[1], ARGV[4], ARGV[1])
return {1, ARGV[1]}
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
    }

    fn assignment(outcome: &ClaimOutcome) -> &Assignment {
        match outcome {
            ClaimOutcome::Claimed(assignment) | ClaimOutcome::Existing(assignment) => assignment,
        }
    }
}
