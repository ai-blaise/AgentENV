//! Mobility records held by the scheduler rather than by each node.
//!
//! The claim protocol exists so a destination can take a paused sandbox from
//! an origin. Those are different machines, and they cannot agree through a
//! store that lives on one of their disks: a destination's claim would be
//! written somewhere the origin never reads, and the origin's resume fence
//! would never fire. [`LocalMobilityStore`] is therefore only correct for a
//! single node talking to itself.
//!
//! The scheduler is already the cluster-wide authority for which node owns a
//! sandbox, already has a durable store behind it, and already holds an open
//! channel to every node. So the records live there and this speaks to it.
//!
//! # The compare-and-set moves too
//!
//! [`LocalMobilityStore::upsert`] reads, compares generations, then writes,
//! which is not atomic even between two threads in one process. Here the
//! scheduler performs the comparison inside a single Redis script and answers
//! whether the write landed, so a claim that loses a race is told so rather
//! than silently overwriting one that should have won.
//!
//! # What a failure means
//!
//! Every method surfaces a transport error rather than papering over it. The
//! callers are written for that: a resume fence that cannot read the store
//! refuses the resume, and the pause-side bookkeeping logs and carries on. An
//! unreachable scheduler must not silently look like "nobody has claimed it".

use anyhow::{Context, Result};
use async_trait::async_trait;
use tonic::transport::Channel;

use super::record::{MobilityRecord, MobilityState, MobilityStore, MobilityWrite};
use crate::proto::scheduler::{
    self, scheduler_client::SchedulerClient, MobilityState as ProtoState,
};
use crate::types::SandboxId;

/// How long a mobility RPC may take before the node gives up on it.
///
/// Matches `GRPC_CALL_TIMEOUT` in the observability reporter: these are calls
/// to the same scheduler over the same network, and a node that waits longer
/// than its own heartbeat interval for one of them is already in trouble.
const MOBILITY_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A [`MobilityStore`] backed by the scheduler.
#[derive(Clone)]
pub struct SchedulerMobilityStore {
    channel: Channel,
    /// Which node's records `list` returns.
    ///
    /// A node asks for its own; an operator draining a node asks for that
    /// node's. Fixed per store because the node-side callers only ever want
    /// their own, and letting them ask for someone else's would make the
    /// metrics quietly wrong.
    node_id: String,
}

impl std::fmt::Debug for SchedulerMobilityStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerMobilityStore")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl SchedulerMobilityStore {
    pub fn new(channel: Channel, node_id: impl Into<String>) -> Self {
        Self {
            channel,
            node_id: node_id.into(),
        }
    }

    /// Wraps a mobility message in a request that carries a deadline.
    ///
    /// Every other node-to-scheduler client sets one; these five were the
    /// exception. It matters most on `record_paused`, which runs inside
    /// `pause_sandbox_impl` after the guest has been frozen and the proxy route
    /// detached, but before the store is moved to `Paused` and the VM is
    /// stopped. `pause_sandbox` runs detached, so the caller's HTTP deadline
    /// does not reach it. A scheduler that is reachable at the TCP level but
    /// wedged therefore left the sandbox in the transitional `Pausing` state --
    /// Firecracker process, guest memory and network slot all still held,
    /// unroutable, and clearable by no API call.
    fn deadlined<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(MOBILITY_RPC_TIMEOUT);
        request
    }

    fn client(&self) -> SchedulerClient<Channel> {
        SchedulerClient::new(self.channel.clone())
    }
}

#[async_trait]
impl MobilityStore for SchedulerMobilityStore {
    async fn upsert(&self, record: &MobilityRecord) -> Result<MobilityWrite> {
        let response = self
            .client()
            .upsert_mobility_record(Self::deadlined(scheduler::UpsertMobilityRecordRequest {
                record: Some(to_proto(record)?),
                // Unconditional: this is the bookkeeping path, ordered by
                // generation. Claims go through compare_and_set.
                expected_generation: String::new(),
                expect_absent: false,
            }))
            .await
            .context("upsert mobility record through the scheduler")?
            .into_inner();
        Ok(if response.applied {
            MobilityWrite::Applied
        } else {
            MobilityWrite::Superseded
        })
    }

    async fn compare_and_set(
        &self,
        expected: Option<super::record::MobilityGeneration>,
        record: &MobilityRecord,
    ) -> Result<MobilityWrite> {
        let response = self
            .client()
            .upsert_mobility_record(Self::deadlined(scheduler::UpsertMobilityRecordRequest {
                record: Some(to_proto(record)?),
                expected_generation: expected
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                // Distinguishes "expect nothing stored" from "do not check",
                // which the empty string alone cannot.
                expect_absent: expected.is_none(),
            }))
            .await
            .context("compare-and-set mobility record through the scheduler")?
            .into_inner();
        Ok(if response.applied {
            MobilityWrite::Applied
        } else {
            MobilityWrite::Superseded
        })
    }

    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<MobilityRecord>> {
        let response = self
            .client()
            .get_mobility_record(Self::deadlined(scheduler::GetMobilityRecordRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .context("read mobility record from the scheduler")?
            .into_inner();
        if !response.found {
            return Ok(None);
        }
        response.record.map(from_proto).transpose()
    }

    async fn list(&self) -> Result<Vec<MobilityRecord>> {
        let response = self
            .client()
            .list_mobility_records(Self::deadlined(scheduler::ListMobilityRecordsRequest {
                origin_node_id: self.node_id.clone(),
            }))
            .await
            .context("list mobility records from the scheduler")?
            .into_inner();
        response.records.into_iter().map(from_proto).collect()
    }

    async fn remove(&self, sandbox_id: &SandboxId) -> Result<()> {
        self.client()
            .remove_mobility_record(Self::deadlined(scheduler::RemoveMobilityRecordRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .context("remove mobility record through the scheduler")?;
        Ok(())
    }
}

/// Converts a record for the wire.
///
/// The fingerprint travels as JSON because the scheduler never interprets it —
/// only a candidate node can judge compatibility — which means it can gain
/// fields without a scheduler release.
fn to_proto(record: &MobilityRecord) -> Result<scheduler::MobilityRecord> {
    let fingerprint_json =
        serde_json::to_string(&record.fingerprint).context("encode the migration fingerprint")?;
    let artifact_reach = serde_json::to_value(record.artifact_reach)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default();

    let (state, holder_node_id, state_at_unix_ms) = match &record.state {
        MobilityState::Parked => (ProtoState::Parked, String::new(), 0),
        MobilityState::Claimed {
            by_node_id,
            at_unix_ms,
        } => (ProtoState::Claimed, by_node_id.clone(), *at_unix_ms as i64),
        MobilityState::Evacuated {
            to_node_id,
            at_unix_ms,
        } => (
            ProtoState::Evacuated,
            to_node_id.clone(),
            *at_unix_ms as i64,
        ),
    };

    Ok(scheduler::MobilityRecord {
        sandbox_id: record.sandbox_id.to_string(),
        origin_node_id: record.origin_node_id.clone(),
        generation: record.generation.to_string(),
        fingerprint_json,
        artifact_reach,
        cpu_count: record.resources.cpu_count,
        memory_mib: record.resources.memory_mib,
        snapshot_id: record.snapshot_id.clone().unwrap_or_default(),
        paused_at_unix_ms: record.paused_at_unix_ms as i64,
        state: state.into(),
        holder_node_id,
        state_at_unix_ms,
    })
}

fn from_proto(record: scheduler::MobilityRecord) -> Result<MobilityRecord> {
    let state = match record.state() {
        ProtoState::Parked => MobilityState::Parked,
        ProtoState::Claimed => MobilityState::Claimed {
            by_node_id: record.holder_node_id.clone(),
            at_unix_ms: record.state_at_unix_ms.max(0) as u64,
        },
        ProtoState::Evacuated => MobilityState::Evacuated {
            to_node_id: record.holder_node_id.clone(),
            at_unix_ms: record.state_at_unix_ms.max(0) as u64,
        },
        // A state this build does not know is not "parked". Reading it as
        // available would hand out a sandbox that is mid-handover, so an
        // unrecognised record is refused instead.
        ProtoState::Unspecified => {
            anyhow::bail!(
                "mobility record for {} has a state this build does not understand",
                record.sandbox_id
            )
        }
    };

    Ok(MobilityRecord {
        sandbox_id: SandboxId::parse_str(&record.sandbox_id)
            .with_context(|| format!("parse sandbox id {:?}", record.sandbox_id))?,
        origin_node_id: record.origin_node_id,
        generation: super::record::MobilityGeneration::parse_str(&record.generation)
            .with_context(|| format!("parse mobility generation {:?}", record.generation))?,
        fingerprint: serde_json::from_str(&record.fingerprint_json)
            .context("decode the migration fingerprint")?,
        artifact_reach: serde_json::from_value(serde_json::Value::String(
            record.artifact_reach.clone(),
        ))
        .with_context(|| format!("decode artifact reach {:?}", record.artifact_reach))?,
        resources: crate::types::SandboxResources {
            cpu_count: record.cpu_count,
            memory_mib: record.memory_mib,
            // Not carried: only cpu and memory gate a placement, and a drain
            // has no use for the disk figure.
            disk_size_mib: 0,
        },
        snapshot_id: Some(record.snapshot_id).filter(|id| !id.is_empty()),
        paused_at_unix_ms: record.paused_at_unix_ms.max(0) as u64,
        state,
    })
}

/// Opens a scheduler-backed store over a lazily connected channel.
pub fn scheduler_mobility_store(
    endpoint: &str,
    node_id: impl Into<String>,
) -> Result<SchedulerMobilityStore> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid scheduler endpoint {endpoint:?}"))?
        .connect_lazy();
    Ok(SchedulerMobilityStore::new(channel, node_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::store::SandboxMetadata;
    use crate::snapshot::ArtifactReach;
    use crate::snapshot::SnapshotRuntimeVersions;
    use crate::virtualization::VirtualizationMode;

    fn record() -> MobilityRecord {
        let metadata = SandboxMetadata {
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "vmlinux-6.1.175".to_string(),
                firecracker_version: "1.15.1".to_string(),
                envd_version: "0.5.15".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: VirtualizationMode::Kvm,
            resources: crate::types::SandboxResources {
                cpu_count: 4,
                memory_mib: 8192,
                disk_size_mib: 16384,
            },
            ..SandboxMetadata::default()
        };
        MobilityRecord::for_paused(
            &metadata,
            "node-a",
            "x86_64",
            Some("{\"cpuid\":[]}".to_string()),
            4096,
            ArtifactReach::ClusterShared,
            Some("snap-1".to_string()),
        )
    }

    /// The wire form has to survive a round trip exactly, because a record
    /// that loses a field on the way through the scheduler makes a sandbox
    /// look different to a destination than it did to its origin.
    #[test]
    fn a_record_round_trips_through_the_wire_form() {
        let original = record();
        let restored = from_proto(to_proto(&original).expect("encode")).expect("decode");

        assert_eq!(restored.sandbox_id, original.sandbox_id);
        assert_eq!(restored.origin_node_id, original.origin_node_id);
        assert_eq!(restored.generation, original.generation);
        assert_eq!(restored.fingerprint, original.fingerprint);
        assert_eq!(restored.artifact_reach, original.artifact_reach);
        assert_eq!(restored.snapshot_id, original.snapshot_id);
        assert_eq!(restored.paused_at_unix_ms, original.paused_at_unix_ms);
        assert_eq!(restored.state, original.state);
        assert_eq!(restored.resources.cpu_count, original.resources.cpu_count);
        assert_eq!(restored.resources.memory_mib, original.resources.memory_mib);
    }

    /// Each state carries its holder and timestamp, and losing either would
    /// leave the origin's fence unable to say who has the sandbox.
    #[test]
    fn every_state_survives_the_wire_form() {
        for state in [
            MobilityState::Parked,
            MobilityState::Claimed {
                by_node_id: "node-b".to_string(),
                at_unix_ms: 1_700_000_000_123,
            },
            MobilityState::Evacuated {
                to_node_id: "node-c".to_string(),
                at_unix_ms: 1_700_000_000_456,
            },
        ] {
            let mut original = record();
            original.state = state.clone();
            let restored = from_proto(to_proto(&original).expect("encode")).expect("decode");
            assert_eq!(restored.state, state);
        }
    }

    /// An uncommitted sandbox is the ordinary case, and "no snapshot" has to
    /// stay distinguishable from "a snapshot whose id is the empty string".
    #[test]
    fn an_uncommitted_record_stays_uncommitted() {
        let mut original = record();
        original.snapshot_id = None;
        let restored = from_proto(to_proto(&original).expect("encode")).expect("decode");
        assert_eq!(restored.snapshot_id, None);
    }

    /// A state a newer node introduced must not be read as "parked" by an
    /// older scheduler's reply: that would advertise a sandbox as available
    /// while it is mid-handover.
    #[test]
    fn an_unknown_state_is_refused_rather_than_read_as_parked() {
        let mut wire = to_proto(&record()).expect("encode");
        wire.state = ProtoState::Unspecified.into();
        let error = from_proto(wire).expect_err("an unknown state must be refused");
        assert!(
            error.to_string().contains("does not understand"),
            "unexpected error: {error}"
        );
    }
}
