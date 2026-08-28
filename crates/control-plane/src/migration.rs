use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::model::{CapacityLimits, Node, PendingResources, SandboxResources};

/// Largest integer that Redis Lua can represent without losing precision.
/// Ownership generations are deliberately bounded to this value because Redis
/// scripts use IEEE-754 doubles for numeric JSON fields.
pub const MAX_SAFE_GENERATION: u64 = (1_u64 << 53) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Preparing,
    ReadyToCutover,
    SourceQuiesced,
    Committed,
    DestinationActive,
    SourceReleased,
    Aborted,
}

impl MigrationPhase {
    pub fn is_post_commit(self) -> bool {
        matches!(
            self,
            Self::Committed | Self::DestinationActive | Self::SourceReleased
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::SourceReleased | Self::Aborted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub migration_id: String,
    pub sandbox_id: String,
    pub source_generation: u64,
    pub source: Node,
    pub destination: Node,
    pub phase: MigrationPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_digest: Option<String>,
    pub durable_coverage: bool,
    pub destination_prepared: bool,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BeginMigration {
    pub migration_id: String,
    pub sandbox_id: String,
    pub source: Node,
    pub destination: Node,
    pub expected_generation: u64,
    pub resources: SandboxResources,
    pub destination_observed: PendingResources,
    pub destination_limits: CapacityLimits,
    pub now: Instant,
    pub now_unix_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationAction {
    RecordCheckpoint {
        checkpoint_id: String,
        manifest_digest: String,
        durable_coverage: bool,
    },
    PrepareDestination,
    QuiesceSource,
    Commit,
    ActivateDestination,
    ReleaseSource,
    Abort {
        reason: String,
    },
}

impl MigrationAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::RecordCheckpoint { .. } => "record_checkpoint",
            Self::PrepareDestination => "prepare_destination",
            Self::QuiesceSource => "quiesce_source",
            Self::Commit => "commit",
            Self::ActivateDestination => "activate_destination",
            Self::ReleaseSource => "release_source",
            Self::Abort { .. } => "abort",
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpdateMigration {
    pub migration_id: String,
    pub sandbox_id: String,
    pub actor_node_id: String,
    pub action: MigrationAction,
    pub now: Instant,
    pub now_unix_ms: i64,
}

pub(crate) fn validate_begin(request: &BeginMigration) -> Result<(), &'static str> {
    if request.migration_id.trim().is_empty()
        || request.sandbox_id.trim().is_empty()
        || request.source.id.trim().is_empty()
        || request.source.endpoint.trim().is_empty()
        || request.destination.id.trim().is_empty()
        || request.destination.endpoint.trim().is_empty()
    {
        return Err("migration, sandbox, source, and destination identities are required");
    }
    if request.source.id == request.destination.id {
        return Err("migration source and destination must differ");
    }
    if request.expected_generation == 0 || request.expected_generation >= MAX_SAFE_GENERATION {
        return Err("migration generation is invalid or exhausted");
    }
    if request.resources.cpu == 0 || request.resources.memory_bytes == 0 {
        return Err("migration CPU and memory must be greater than zero");
    }
    Ok(())
}

pub(crate) fn validate_update(request: &UpdateMigration) -> Result<(), &'static str> {
    if request.migration_id.trim().is_empty()
        || request.sandbox_id.trim().is_empty()
        || request.actor_node_id.trim().is_empty()
    {
        return Err("migration, sandbox, and actor identities are required");
    }
    match &request.action {
        MigrationAction::RecordCheckpoint {
            checkpoint_id,
            manifest_digest,
            ..
        } if checkpoint_id.trim().is_empty() || manifest_digest.trim().is_empty() => {
            Err("checkpoint ID and manifest digest are required")
        }
        MigrationAction::Abort { reason } if reason.trim().is_empty() => {
            Err("migration abort reason is required")
        }
        _ => Ok(()),
    }
}
