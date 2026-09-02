use std::cmp;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{debug, error, info, trace, warn};

use super::{ObservabilityService, RosterDigestState};
use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};
use crate::orchestrator::{SandboxLifecycleEvent, SandboxLifecycleEventType};
use crate::p2p::P2pEndpoint;
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};

const MAX_REPORT_BACKOFF: Duration = Duration::from_secs(60);
const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// The scheduler's wire text for a heartbeat naming a node it does not know.
///
/// Mirrors `NodeNotInRegistryMessage` in
/// `services/scheduler/internal/node_registry.go`. Kept as a named constant on
/// this side too so the two halves of the contract are greppable from each
/// other; both sides pin it in a test.
const NODE_NOT_IN_REGISTRY_MESSAGE: &str = "node is not in scheduler node list";

/// Returned by [`ObservabilityReporter::send_heartbeat`] when the scheduler
/// rejects the heartbeat because this node's ID is not in its configured node
/// list. Detected in [`ObservabilityReporter::record_heartbeat_failure`] to
/// emit an `error!`-level log with an actionable remediation hint rather than a
/// generic transient-failure warning.
#[derive(Debug, thiserror::Error)]
#[error("node is not in the scheduler's configured node list")]
pub(super) struct HeartbeatNodeNotConfigured;

#[derive(Clone)]
struct ReporterConfig {
    scheduler_endpoint: String,
    interval: Duration,
}

pub struct ObservabilityReporter {
    config: ReporterConfig,
    service: Arc<ObservabilityService>,
    scheduler_channel: Channel,
    p2p_endpoint: Option<P2pEndpoint>,
    shutdown_tx: Option<watch::Sender<bool>>,
    heartbeat_join: Option<JoinHandle<()>>,
    event_join: Option<JoinHandle<()>>,
    /// Set to `true` on the first successful heartbeat RPC. Checked in
    /// [`shutdown`] to skip `UnregisterNode` when the reporter never managed
    /// to reach the scheduler at all.
    ever_heartbeat_succeeded: Arc<AtomicBool>,
}

impl ObservabilityReporter {
    pub fn new(
        service: Arc<ObservabilityService>,
        config: &ObservabilitySchedulerReportConfig,
        cluster_config: &ClusterConfig,
        p2p_endpoint: Option<P2pEndpoint>,
    ) -> Result<Option<Self>> {
        let Some(config) = ReporterConfig::resolve(config, cluster_config) else {
            return Ok(None);
        };
        let scheduler_channel = Self::build_scheduler_channel(&config.scheduler_endpoint)?;

        Ok(Some(Self {
            config,
            service,
            scheduler_channel,
            p2p_endpoint,
            shutdown_tx: None,
            heartbeat_join: None,
            event_join: None,
            ever_heartbeat_succeeded: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Spawns the background heartbeat task.
    ///
    /// [`ObservabilityReporter::new`] only builds the reporter without starting
    /// any background work.  Call `start` **exactly once** before calling
    /// [`shutdown`].
    pub fn start(&mut self) {
        if self.shutdown_tx.is_some() {
            warn!("reporter already started, ignoring duplicate start call");
            return;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = self.config.clone();
        let event_config = self.config.clone();
        let service = Arc::clone(&self.service);
        let event_service = Arc::clone(&self.service);
        let scheduler_channel = self.scheduler_channel.clone();
        let event_scheduler_channel = self.scheduler_channel.clone();
        let ever_heartbeat_succeeded = Arc::clone(&self.ever_heartbeat_succeeded);
        let p2p_endpoint = self.p2p_endpoint.clone();
        let mut heartbeat_shutdown_rx = shutdown_rx.clone();
        let mut event_shutdown_rx = shutdown_rx;
        let mut sandbox_event_rx = event_service.subscribe_sandbox_events();

        let kill_switch = super::global_kill_switch();
        // Shared because production and reporting run in separate tasks: the
        // event loop counts, the heartbeat loop reports.
        let emitted_events = Arc::new(AtomicU64::new(0));
        let heartbeat_emitted = Arc::clone(&emitted_events);
        let heartbeat_join = tokio::spawn(async move {
            let mut backoff = config.interval;
            let mut wait = Duration::from_millis(100);
            let mut pending_cpu_config_json = service.take_cpu_config_json();
            let mut rosters = RosterDigestState::default();

            loop {
                if wait > Duration::ZERO {
                    tokio::select! {
                        _ = sleep(wait) => {}
                        changed = heartbeat_shutdown_rx.changed() => {
                            if changed.is_err() || *heartbeat_shutdown_rx.borrow() {
                                info!("observability heartbeat reporter stopping");
                                return;
                            }
                        }
                    }
                }

                match Self::send_heartbeat(
                    &config,
                    &service,
                    &scheduler_channel,
                    &mut pending_cpu_config_json,
                    p2p_endpoint.as_ref(),
                    &mut rosters,
                    heartbeat_emitted.load(Ordering::Relaxed),
                )
                .await
                {
                    Ok(()) => {
                        ever_heartbeat_succeeded.store(true, Ordering::Relaxed);
                        // Contact restored clears the kill switch without
                        // operator action, so a partition that heals does not
                        // leave the node refusing work.
                        kill_switch.record_success();
                        backoff = config.interval;
                        wait = config.interval;
                    }
                    Err(err) => {
                        Self::record_heartbeat_failure(
                            &err,
                            &mut rosters,
                            service.node_id(),
                            &config.scheduler_endpoint,
                            backoff,
                        );
                        wait = backoff;
                        backoff = cmp::min(backoff.saturating_mul(2), MAX_REPORT_BACKOFF);
                    }
                }
            }
        });

        let event_join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = event_shutdown_rx.changed() => {
                        if changed.is_err() || *event_shutdown_rx.borrow() {
                            info!("observability sandbox event reporter stopping");
                            return;
                        }
                    }
                    events = Self::recv_sandbox_event_batch(&mut sandbox_event_rx, &emitted_events) => {
                        let Some(events) = events else {
                            return;
                        };
                        let batch_size = events.len() as u64;
                        let outcome = Self::send_sandbox_events(
                            &event_config,
                            &event_service,
                            &event_scheduler_channel,
                            events,
                        ).await;
                        // Counted once the RPC has resolved rather than when
                        // the batch was drained, because the heartbeat that
                        // reports the count is a separate RPC on a separate
                        // task with no ordering against this one. Counting at
                        // drain time lets a heartbeat overtake a batch still in
                        // flight, and the scheduler reports the whole batch as
                        // lost and then reads its arrival as a node restart.
                        //
                        // On both outcomes: a failed RPC is real loss, and the
                        // scheduler is the only place it can be accounted.
                        emitted_events.fetch_add(batch_size, Ordering::Relaxed);
                        if let Err(err) = outcome {
                            warn!(error = %err, "observability sandbox event batch report failed");
                        }
                    }
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.heartbeat_join = Some(heartbeat_join);
        self.event_join = Some(event_join);

        info!(
            scheduler_endpoint = %self.config.scheduler_endpoint,
            interval_secs = self.config.interval.as_secs(),
            "observability reporter started"
        );
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(join) = self.heartbeat_join.take() {
            if let Err(err) = join.await {
                warn!(error = %err, "observability heartbeat reporter task join failed");
            }
        }

        if let Some(join) = self.event_join.take() {
            if let Err(err) = join.await {
                warn!(error = %err, "observability sandbox event reporter task join failed");
            }
        }

        // If we never succeeded in sending a heartbeat, it's likely the scheduler
        // endpoint is misconfigured or the scheduler is unreachable. In that case,
        // skip the UnregisterNode RPC.
        if !self.ever_heartbeat_succeeded.load(Ordering::Relaxed) {
            debug!("skipping node unregister: no heartbeat ever succeeded");
            return Ok(());
        }

        for attempt in 1..=3 {
            match self.unregister_node().await {
                Ok(()) => {
                    info!(
                        node_id = %self.service.node_id(),
                        service_instance_id = %self.service.service_instance_id(),
                        attempt,
                        "observability node unregistered from scheduler"
                    );
                    return Ok(());
                }
                Err(err) => {
                    warn!(
                        node_id = %self.service.node_id(),
                        service_instance_id = %self.service.service_instance_id(),
                        attempt,
                        error = %err,
                        "failed to unregister node from scheduler during shutdown"
                    );
                    sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        }

        Ok(())
    }

    fn build_scheduler_channel(scheduler_endpoint: &str) -> Result<Channel> {
        let raw_endpoint = scheduler_endpoint.to_string();
        let endpoint = Endpoint::from_shared(raw_endpoint.clone())
            .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
        Ok(endpoint.connect_lazy())
    }

    async fn send_heartbeat(
        config: &ReporterConfig,
        service: &ObservabilityService,
        scheduler_channel: &Channel,
        cpu_config_json: &mut Option<String>,
        p2p_endpoint: Option<&P2pEndpoint>,
        rosters: &mut RosterDigestState,
        emitted_events: u64,
    ) -> Result<()> {
        let mut snapshot = service
            .node_snapshot()
            .await
            .context("failed to collect heartbeat snapshot")?;
        snapshot.machine_info.cpu_config_json = cpu_config_json.clone();
        let node_id = snapshot.node_id.clone();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let roster = rosters.report(&snapshot.sandbox_ids, snapshot.roster_complete);
        let req = Self::build_heartbeat_request(
            snapshot,
            now_ms,
            p2p_endpoint,
            config.interval,
            roster,
            emitted_events,
        );

        let mut request = Request::new(req);
        request.set_timeout(GRPC_CALL_TIMEOUT);
        let response = SchedulerClient::new(scheduler_channel.clone())
            .heartbeat(request)
            .await
            .map_err(Self::classify_heartbeat_status)?
            .into_inner();

        *cpu_config_json = None;
        rosters.observe_response(
            response.roster_digest_accepted,
            response.request_full_roster,
        );

        if !response.cpu_config_json.is_empty() {
            service.store_cluster_cpu_config(response.cpu_config_json);
            info!("received cluster cpu config intersection from scheduler");
        }

        trace!(
            node_id = %node_id,
            scheduler_endpoint = %config.scheduler_endpoint,
            "observability heartbeat sent"
        );

        Ok(())
    }

    /// Turns a rejected heartbeat into the error the failure log reads.
    ///
    /// The scheduler has no machine-readable discriminator for "this node id is
    /// not in my node list", so the diagnosis rides on the wire text. That text
    /// is a cross-language contract, not a log line: the producer is the
    /// exported `NodeNotInRegistryMessage` in
    /// `services/scheduler/internal/node_registry.go`, and
    /// `TestUnknownNodeRejectionCarriesTheWireMessage` pins it there against
    /// this matcher. Matching on the code alone is not an option — the same RPC
    /// returns `InvalidArgument` for a missing node id or service instance id.
    fn classify_heartbeat_status(status: tonic::Status) -> anyhow::Error {
        if status.code() == tonic::Code::InvalidArgument
            && status.message().contains(NODE_NOT_IN_REGISTRY_MESSAGE)
        {
            anyhow::Error::new(HeartbeatNodeNotConfigured)
        } else {
            anyhow::Error::from(status).context("heartbeat rpc failed")
        }
    }

    /// Logs a heartbeat that did not land, and forgets what the scheduler
    /// knows about this node's roster.
    ///
    /// One function rather than one arm per diagnosis, because the roster
    /// consequence is the same for all of them and the diagnosis is only a
    /// choice of log line. A rejected heartbeat needs the reset as much as a
    /// lost one does: the scheduler refuses it before it resolves the roster,
    /// so it recorded nothing, while [`RosterDigestState::report`] has already
    /// stamped that roster as acknowledged and would elide it from here on.
    pub(super) fn record_heartbeat_failure(
        err: &anyhow::Error,
        rosters: &mut RosterDigestState,
        node_id: &str,
        scheduler_endpoint: &str,
        retry_after: Duration,
    ) {
        // A failed heartbeat may mean the scheduler restarted or moved. The
        // next one reintroduces the roster in full rather than assuming the
        // new process inherited what the old one knew.
        rosters.reset();

        if err.is::<HeartbeatNodeNotConfigured>() {
            error!(
                node_id = %node_id,
                scheduler_endpoint = %scheduler_endpoint,
                retry_after_secs = retry_after.as_secs(),
                "scheduler rejected heartbeat: this node is not in the \
                 scheduler's configured node list — ensure \
                 AENV_NODE_ID matches a node name in the scheduler \
                 nodes configuration"
            );
        } else {
            warn!(
                error = %err,
                retry_after_secs = retry_after.as_secs(),
                "observability heartbeat failed"
            );
        }
    }

    /// Drains a batch of lifecycle events, counting the ones dropped before
    /// they could be batched.
    ///
    /// `emitted` is what the heartbeat reports so the scheduler can compare it
    /// against what arrived, and events lost to a lagged receiver never reach
    /// an RPC at all — only the node can see them, so they are counted here.
    /// The events that do get batched are counted by the caller once their RPC
    /// has resolved; counting them here would let a heartbeat outrun the batch
    /// it describes.
    async fn recv_sandbox_event_batch(
        rx: &mut broadcast::Receiver<SandboxLifecycleEvent>,
        emitted: &AtomicU64,
    ) -> Option<Vec<SandboxLifecycleEvent>> {
        let first = loop {
            match rx.recv().await {
                Ok(event) => break event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "observability sandbox event receiver lagged");
                    emitted.fetch_add(skipped, Ordering::Relaxed);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("observability sandbox event channel closed");
                    return None;
                }
            }
        };

        let mut events = Vec::with_capacity(rx.len() + 1);
        events.push(first);
        loop {
            match rx.try_recv() {
                Ok(event) => events.push(event),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    warn!(skipped, "observability sandbox event receiver lagged");
                    emitted.fetch_add(skipped, Ordering::Relaxed);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    debug!("observability sandbox event channel closed");
                    break;
                }
            }
        }

        Some(events)
    }

    async fn send_sandbox_events(
        config: &ReporterConfig,
        service: &ObservabilityService,
        scheduler_channel: &Channel,
        events: Vec<SandboxLifecycleEvent>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let event_count = events.len();
        let mut request = Request::new(Self::build_sandbox_event_request(
            service.node_id(),
            service.cluster_id(),
            service.service_instance_id(),
            events,
        ));
        request.set_timeout(GRPC_CALL_TIMEOUT);
        SchedulerClient::new(scheduler_channel.clone())
            .report_sandbox_event(request)
            .await
            .context("sandbox event batch report rpc failed")?;

        trace!(
            node_id = %service.node_id(),
            event_count,
            scheduler_endpoint = %config.scheduler_endpoint,
            "observability sandbox event batch sent"
        );

        Ok(())
    }

    fn build_heartbeat_request(
        snapshot: super::NodeSnapshot,
        now_ms: i64,
        p2p_endpoint: Option<&P2pEndpoint>,
        interval: Duration,
        roster: super::RosterReport,
        emitted_events: u64,
    ) -> scheduler::HeartbeatRequest {
        scheduler::HeartbeatRequest {
            node_id: snapshot.node_id,
            cluster_id: snapshot.cluster_id.to_string(),
            service_instance_id: snapshot.service_instance_id,
            version: snapshot.version,
            commit: snapshot.commit,
            machine_info: Some(scheduler::MachineInfo {
                cpu_family: snapshot.machine_info.cpu_family,
                cpu_model: snapshot.machine_info.cpu_model,
                cpu_model_name: snapshot.machine_info.cpu_model_name,
                cpu_architecture: snapshot.machine_info.cpu_architecture,
                cpu_config_json: snapshot.machine_info.cpu_config_json.unwrap_or_default(),
                sandbox_backend: snapshot.machine_info.sandbox_backend,
            }),
            snapshot: Some(scheduler::NodeSnapshot {
                // Report the node's real disposition. A draining node that
                // claims Ready keeps attracting placements it is trying to
                // shed, so the scheduler's drain never converges.
                status: if snapshot.draining {
                    scheduler::NodeStatus::Lingering.into()
                } else {
                    scheduler::NodeStatus::Ready.into()
                },
                allocated_cpu: snapshot.metrics.allocated_cpu,
                allocated_memory_bytes: snapshot.metrics.allocated_memory_bytes,
                cpu_percent: snapshot.metrics.cpu_percent,
                cpu_count: snapshot.metrics.cpu_count,
                memory_used_bytes: snapshot.metrics.memory_used_bytes,
                memory_total_bytes: snapshot.metrics.memory_total_bytes,
                disks: snapshot
                    .metrics
                    .disks
                    .into_iter()
                    .map(|disk| scheduler::DiskMetric {
                        mount_point: disk.mount_point,
                        device: disk.device,
                        filesystem_type: disk.filesystem_type,
                        used_bytes: disk.used_bytes,
                        total_bytes: disk.total_bytes,
                    })
                    .collect(),
                sandbox_count: snapshot.sandbox_count,
                sandbox_starting_count: snapshot.sandbox_starting_count,
                create_successes: snapshot.create_successes,
                create_fails: snapshot.create_fails,
                reported_at_unix_ms: now_ms,
                paused_sandbox_count: snapshot.paused_sandbox_count,
                paused_allocated_cpu: snapshot.metrics.paused_allocated_cpu,
                paused_allocated_memory_bytes: snapshot.metrics.paused_allocated_memory_bytes,
            }),
            roster_full: roster.sandbox_ids.is_some(),
            sandbox_ids: roster.sandbox_ids.unwrap_or_default(),
            roster_digest: roster.digest,
            emitted_event_count: emitted_events,
            p2p_endpoint: p2p_endpoint.map(|endpoint| scheduler::P2pEndpoint {
                backend: endpoint.backend.clone(),
                address: endpoint.address.clone(),
            }),
            roster_complete: roster.roster_complete,
            heartbeat_interval_ms: interval.as_millis().try_into().unwrap_or(u64::MAX),
        }
    }

    /// Takes the identity fields rather than the service that holds them, so
    /// the wire mapping — including the MiB-to-bytes conversions the scheduler
    /// accounts capacity with — is exercisable without standing up an
    /// orchestrator.
    fn build_sandbox_event_request(
        node_id: &str,
        cluster_id: uuid::Uuid,
        service_instance_id: &str,
        events: Vec<SandboxLifecycleEvent>,
    ) -> scheduler::ReportSandboxEventRequest {
        scheduler::ReportSandboxEventRequest {
            node_id: node_id.to_string(),
            cluster_id: cluster_id.to_string(),
            service_instance_id: service_instance_id.to_string(),
            events: events
                .into_iter()
                .map(|event| scheduler::SandboxEvent {
                    sandbox_id: event.sandbox_id.to_string(),
                    event_type: Self::map_sandbox_event_type(event.event_type).into(),
                    requested_cpu: event.resources.cpu_count,
                    requested_memory_bytes: u64::from(event.resources.memory_mib) * 1024 * 1024,
                    requested_disk_bytes: u64::from(event.resources.disk_size_mib) * 1024 * 1024,
                })
                .collect(),
        }
    }

    fn map_sandbox_event_type(
        event_type: SandboxLifecycleEventType,
    ) -> scheduler::SandboxEventType {
        match event_type {
            SandboxLifecycleEventType::Create => scheduler::SandboxEventType::Create,
            SandboxLifecycleEventType::Delete => scheduler::SandboxEventType::Delete,
            SandboxLifecycleEventType::Pause => scheduler::SandboxEventType::Pause,
            SandboxLifecycleEventType::Resume => scheduler::SandboxEventType::Resume,
            SandboxLifecycleEventType::Fork => scheduler::SandboxEventType::Fork,
        }
    }

    async fn unregister_node(&self) -> Result<()> {
        let mut request = Request::new(scheduler::UnregisterNodeRequest {
            node_id: self.service.node_id().to_string(),
            service_instance_id: self.service.service_instance_id().to_string(),
        });
        request.set_timeout(GRPC_CALL_TIMEOUT);
        SchedulerClient::new(self.scheduler_channel.clone())
            .unregister_node(request)
            .await
            .context("unregister node rpc failed")?;
        Ok(())
    }
}

impl ReporterConfig {
    fn resolve(
        config: &ObservabilitySchedulerReportConfig,
        cluster_config: &ClusterConfig,
    ) -> Option<Self> {
        if !config.enabled {
            return None;
        }

        let Some(scheduler_endpoint) = cluster_config
            .scheduler_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned)
        else {
            warn!(
                "observability scheduler reporter is enabled but cluster scheduler endpoint is not configured"
            );
            return None;
        };

        Some(ReporterConfig {
            scheduler_endpoint,
            interval: Duration::from_secs(config.interval_secs.max(1)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{ClusterConfig, ObservabilitySchedulerReportConfig};
    use crate::observability::{
        DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot, RosterReport,
    };
    use crate::types::{SandboxId, SandboxResources};

    /// A snapshot whose every field is distinct, so a builder that drops or
    /// swaps one is visible rather than absorbed by a shared default.
    fn make_snapshot(draining: bool) -> NodeSnapshot {
        NodeSnapshot {
            version: "1.2.3".to_string(),
            commit: "deadbeef".to_string(),
            node_id: "node-a".to_string(),
            service_instance_id: "instance-7".to_string(),
            cluster_id: uuid::Uuid::from_u128(0x2a),
            machine_info: MachineInfo {
                cpu_family: "6".to_string(),
                cpu_model: "143".to_string(),
                cpu_model_name: "Xeon".to_string(),
                cpu_architecture: "x86_64".to_string(),
                cpu_config_json: Some("{\"cpuid\":1}".to_string()),
                sandbox_backend: "firecracker".to_string(),
            },
            sandbox_count: 11,
            sandbox_ids: Vec::new(),
            metrics: NodeMetricsSnapshot {
                allocated_cpu: 12,
                allocated_memory_bytes: 13,
                cpu_percent: 14,
                cpu_count: 15,
                memory_used_bytes: 16,
                memory_total_bytes: 17,
                disks: vec![DiskMetric {
                    mount_point: "/".to_string(),
                    device: "/dev/vda1".to_string(),
                    filesystem_type: "ext4".to_string(),
                    used_bytes: 18,
                    total_bytes: 19,
                }],
                paused_allocated_cpu: 20,
                paused_allocated_memory_bytes: 21,
            },
            create_successes: 22,
            create_fails: 23,
            sandbox_starting_count: 24,
            paused_sandbox_count: 25,
            roster_complete: true,
            draining,
        }
    }

    fn full_roster(ids: &[&str]) -> RosterReport {
        RosterReport {
            digest: "digest-1".to_string(),
            sandbox_ids: Some(ids.iter().map(|id| (*id).to_string()).collect()),
            roster_complete: true,
        }
    }

    fn elided_roster() -> RosterReport {
        RosterReport {
            digest: "digest-1".to_string(),
            sandbox_ids: None,
            roster_complete: false,
        }
    }

    fn heartbeat(snapshot: NodeSnapshot, roster: RosterReport) -> scheduler::HeartbeatRequest {
        ObservabilityReporter::build_heartbeat_request(
            snapshot,
            1_700_000_000_123,
            Some(&P2pEndpoint {
                backend: "iroh".to_string(),
                address: "node-key".to_string(),
            }),
            Duration::from_millis(4500),
            roster,
            77,
        )
    }

    /// Every field the scheduler reads off a heartbeat, pinned in one place.
    /// The reporter is the only producer of this message and no Rust test
    /// reaches the RPC, so a field dropped or crossed here is otherwise
    /// invisible until a fleet misreports itself.
    #[test]
    fn a_heartbeat_carries_the_whole_node_snapshot() {
        let req = heartbeat(make_snapshot(false), full_roster(&["sbx-1", "sbx-2"]));

        assert_eq!(req.node_id, "node-a");
        assert_eq!(req.cluster_id, uuid::Uuid::from_u128(0x2a).to_string());
        assert_eq!(req.service_instance_id, "instance-7");
        assert_eq!(req.version, "1.2.3");
        assert_eq!(req.commit, "deadbeef");
        assert_eq!(req.emitted_event_count, 77);
        assert_eq!(req.heartbeat_interval_ms, 4500);

        let machine = req.machine_info.expect("machine info");
        assert_eq!(machine.cpu_family, "6");
        assert_eq!(machine.cpu_model, "143");
        assert_eq!(machine.cpu_model_name, "Xeon");
        assert_eq!(machine.cpu_architecture, "x86_64");
        assert_eq!(machine.cpu_config_json, "{\"cpuid\":1}");
        assert_eq!(machine.sandbox_backend, "firecracker");

        let snapshot = req.snapshot.expect("node snapshot");
        assert_eq!(snapshot.allocated_cpu, 12);
        assert_eq!(snapshot.allocated_memory_bytes, 13);
        assert_eq!(snapshot.cpu_percent, 14);
        assert_eq!(snapshot.cpu_count, 15);
        assert_eq!(snapshot.memory_used_bytes, 16);
        assert_eq!(snapshot.memory_total_bytes, 17);
        assert_eq!(snapshot.sandbox_count, 11);
        assert_eq!(snapshot.sandbox_starting_count, 24);
        assert_eq!(snapshot.create_successes, 22);
        assert_eq!(snapshot.create_fails, 23);
        assert_eq!(snapshot.reported_at_unix_ms, 1_700_000_000_123);
        assert_eq!(snapshot.paused_sandbox_count, 25);
        assert_eq!(snapshot.paused_allocated_cpu, 20);
        assert_eq!(snapshot.paused_allocated_memory_bytes, 21);

        let disk = snapshot.disks.first().expect("one disk");
        assert_eq!(disk.mount_point, "/");
        assert_eq!(disk.device, "/dev/vda1");
        assert_eq!(disk.filesystem_type, "ext4");
        assert_eq!(disk.used_bytes, 18);
        assert_eq!(disk.total_bytes, 19);

        let endpoint = req.p2p_endpoint.expect("p2p endpoint");
        assert_eq!(endpoint.backend, "iroh");
        assert_eq!(endpoint.address, "node-key");
    }

    /// A node with no cpu template dump must send an empty string, not fail to
    /// build the message.
    #[test]
    fn an_absent_cpu_config_becomes_an_empty_string() {
        let mut snapshot = make_snapshot(false);
        snapshot.machine_info.cpu_config_json = None;
        let req = heartbeat(snapshot, full_roster(&[]));
        assert_eq!(req.machine_info.expect("machine info").cpu_config_json, "");
    }

    /// The drain contract's only producer. `filter.go` drops LINGERING nodes
    /// from placement candidates, so a draining node that reports Ready keeps
    /// being handed sandboxes it is shedding. Asserted through the enum rather
    /// than the numeric values so renumbering the proto cannot quietly satisfy
    /// it.
    #[test]
    fn a_draining_node_heartbeats_lingering() {
        for (draining, want) in [
            (true, scheduler::NodeStatus::Lingering),
            (false, scheduler::NodeStatus::Ready),
        ] {
            let req = heartbeat(make_snapshot(draining), full_roster(&[]));
            assert_eq!(
                req.snapshot.expect("node snapshot").status,
                i32::from(want),
                "draining={draining} must heartbeat {want:?}"
            );
        }
    }

    /// The roster fields decide whether the scheduler reaps this node's
    /// bindings, so the report has to reach the wire exactly as
    /// `RosterDigestState` decided it.
    #[test]
    fn the_roster_report_reaches_the_wire_intact() {
        let full = heartbeat(make_snapshot(false), full_roster(&["sbx-1", "sbx-2"]));
        assert!(full.roster_full);
        assert_eq!(full.sandbox_ids, vec!["sbx-1", "sbx-2"]);
        assert_eq!(full.roster_digest, "digest-1");
        assert!(full.roster_complete);

        let elided = heartbeat(make_snapshot(false), elided_roster());
        assert!(!elided.roster_full);
        assert!(
            elided.sandbox_ids.is_empty(),
            "an elided roster carries no ids"
        );
        assert_eq!(elided.roster_digest, "digest-1");
        assert!(
            !elided.roster_complete,
            "an elided heartbeat must not claim authority over ids it did not send"
        );
    }

    /// The scheduler accounts node capacity in bytes from these fields. The
    /// MiB values are chosen so a `+` or `/` in place of either `* 1024`
    /// produces a different number.
    #[test]
    fn a_sandbox_event_reports_resources_in_bytes() {
        let cluster_id = uuid::Uuid::from_u128(0x2a);
        let sandbox_id = SandboxId::new();
        let req = ObservabilityReporter::build_sandbox_event_request(
            "node-a",
            cluster_id,
            "instance-7",
            vec![SandboxLifecycleEvent {
                event_type: SandboxLifecycleEventType::Create,
                sandbox_id,
                resources: SandboxResources {
                    cpu_count: 3,
                    memory_mib: 3,
                    disk_size_mib: 5,
                },
            }],
        );

        assert_eq!(req.node_id, "node-a");
        assert_eq!(req.cluster_id, cluster_id.to_string());
        assert_eq!(req.service_instance_id, "instance-7");

        let event = req.events.first().expect("one event");
        assert_eq!(event.sandbox_id, sandbox_id.to_string());
        assert_eq!(
            event.event_type,
            i32::from(scheduler::SandboxEventType::Create)
        );
        assert_eq!(event.requested_cpu, 3);
        assert_eq!(event.requested_memory_bytes, 3 * 1024 * 1024);
        assert_eq!(event.requested_disk_bytes, 5 * 1024 * 1024);
    }

    /// Every arm, because a wrong one is a lifecycle the scheduler accounts
    /// under the wrong verb and nothing else notices.
    #[test]
    fn every_lifecycle_event_maps_to_its_own_wire_type() {
        for (event_type, want) in [
            (
                SandboxLifecycleEventType::Create,
                scheduler::SandboxEventType::Create,
            ),
            (
                SandboxLifecycleEventType::Delete,
                scheduler::SandboxEventType::Delete,
            ),
            (
                SandboxLifecycleEventType::Pause,
                scheduler::SandboxEventType::Pause,
            ),
            (
                SandboxLifecycleEventType::Resume,
                scheduler::SandboxEventType::Resume,
            ),
            (
                SandboxLifecycleEventType::Fork,
                scheduler::SandboxEventType::Fork,
            ),
        ] {
            assert_eq!(
                ObservabilityReporter::map_sandbox_event_type(event_type),
                want,
                "{event_type:?} must map to {want:?}"
            );
        }
    }

    fn lifecycle_event() -> SandboxLifecycleEvent {
        SandboxLifecycleEvent {
            event_type: SandboxLifecycleEventType::Create,
            sandbox_id: SandboxId::new(),
            resources: SandboxResources::default(),
        }
    }

    #[tokio::test]
    async fn a_batch_drains_everything_already_queued() {
        let (tx, mut rx) = broadcast::channel(8);
        let emitted = AtomicU64::new(0);
        for _ in 0..3 {
            tx.send(lifecycle_event()).expect("receiver alive");
        }

        let batch = ObservabilityReporter::recv_sandbox_event_batch(&mut rx, &emitted)
            .await
            .expect("a batch");
        assert_eq!(batch.len(), 3, "one wake must take the whole backlog");
        assert_eq!(
            emitted.load(Ordering::Relaxed),
            0,
            "delivered events are counted once their RPC resolves, not at drain time"
        );
    }

    /// Events dropped by a lagged receiver never reach an RPC, so the drain is
    /// the only place they can be accounted at all.
    #[tokio::test]
    async fn lagged_events_are_counted_where_they_are_lost() {
        let (tx, mut rx) = broadcast::channel(2);
        let emitted = AtomicU64::new(0);
        for _ in 0..5 {
            tx.send(lifecycle_event()).expect("receiver alive");
        }

        let batch = ObservabilityReporter::recv_sandbox_event_batch(&mut rx, &emitted)
            .await
            .expect("a batch");
        assert_eq!(batch.len(), 2, "only the two still buffered survive");
        assert_eq!(
            emitted.load(Ordering::Relaxed),
            3,
            "the three overwritten events are loss the scheduler cannot see"
        );
    }

    /// A closed channel ends the event task. Returning an empty batch instead
    /// would spin it.
    #[tokio::test]
    async fn a_closed_channel_ends_the_batch_stream() {
        let (tx, mut rx) = broadcast::channel(8);
        let emitted = AtomicU64::new(0);
        drop(tx);

        assert!(
            ObservabilityReporter::recv_sandbox_event_batch(&mut rx, &emitted)
                .await
                .is_none()
        );
    }

    /// The other half of the contract pinned by
    /// `TestUnknownNodeRejectionCarriesTheWireMessage` in
    /// `services/scheduler/internal/heartbeat_ordering_test.go`. If either side
    /// is reworded alone, both suites stay green and every misconfigured node
    /// loses its `AENV_NODE_ID` remediation hint.
    #[test]
    fn an_unknown_node_rejection_is_diagnosed_from_the_wire_message() {
        // The literal rather than the constant, so this pins the contract
        // instead of comparing the matcher against itself. The Go half asserts
        // the same literal against its own exported constant.
        const WIRE: &str = "node is not in scheduler node list";
        assert_eq!(
            NODE_NOT_IN_REGISTRY_MESSAGE, WIRE,
            "reword this and services/scheduler/internal/node_registry.go together, or neither"
        );

        let err =
            ObservabilityReporter::classify_heartbeat_status(tonic::Status::invalid_argument(WIRE));
        assert!(
            err.is::<HeartbeatNodeNotConfigured>(),
            "the scheduler's rejection must raise the actionable diagnosis"
        );
    }

    /// Neither half alone is the diagnosis: the same RPC returns
    /// `InvalidArgument` for a missing node id, and an unreachable scheduler
    /// returns other codes.
    #[test]
    fn other_rejections_stay_generic() {
        for status in [
            tonic::Status::invalid_argument("node_id is required"),
            tonic::Status::unavailable(NODE_NOT_IN_REGISTRY_MESSAGE),
        ] {
            let code = status.code();
            let err = ObservabilityReporter::classify_heartbeat_status(status);
            assert!(
                !err.is::<HeartbeatNodeNotConfigured>(),
                "{code:?} must not be read as a misconfigured node id"
            );
        }
    }

    fn make_cluster_config(endpoint: Option<&str>) -> ClusterConfig {
        ClusterConfig {
            scheduler_endpoint: endpoint.map(|s| s.to_string()),
        }
    }

    fn make_report_config(
        enabled: Option<bool>,
        interval_secs: Option<u64>,
    ) -> ObservabilitySchedulerReportConfig {
        ObservabilitySchedulerReportConfig {
            enabled: enabled.unwrap_or_default(),
            interval_secs: interval_secs.unwrap_or(5),
            kill_switch: crate::cfg::SchedulerReportKillSwitchConfig {
                action: "disabled".to_string(),
                after_secs: 0,
            },
        }
    }

    #[test]
    fn test_resolve_returns_none_when_no_config() {
        let cfg = make_report_config(None, None);
        let cluster = make_cluster_config(None);
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_returns_none_when_report_is_disabled() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(false), Some(10));
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_returns_none_when_endpoint_is_blank() {
        let cluster = make_cluster_config(Some("   "));
        let cfg = make_report_config(Some(true), None);
        let result = ReporterConfig::resolve(&cfg, &cluster);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_uses_config_values() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(true), Some(10));
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.scheduler_endpoint, "http://scheduler:9090");
        assert_eq!(result.interval, Duration::from_secs(10));
    }

    #[test]
    fn test_resolve_clamps_interval_to_minimum_one() {
        let cluster = make_cluster_config(Some("http://scheduler:9090"));
        let cfg = make_report_config(Some(true), Some(0));
        let result = ReporterConfig::resolve(&cfg, &cluster).unwrap();
        assert_eq!(result.interval, Duration::from_secs(1));
    }

    #[test]
    fn test_heartbeat_node_not_configured_is_detectable_via_anyhow() {
        let err = anyhow::Error::new(HeartbeatNodeNotConfigured);
        assert!(
            err.is::<HeartbeatNodeNotConfigured>(),
            "anyhow::Error::is should detect HeartbeatNodeNotConfigured"
        );
    }

    #[test]
    fn test_heartbeat_node_not_configured_displays_message() {
        let msg = HeartbeatNodeNotConfigured.to_string();
        assert!(
            msg.contains("node is not in"),
            "error message should be descriptive, got: {msg}"
        );
    }

    #[test]
    fn test_regular_anyhow_error_is_not_heartbeat_node_not_configured() {
        let err = anyhow::anyhow!("some transient network error");
        assert!(
            !err.is::<HeartbeatNodeNotConfigured>(),
            "generic errors must not be mistaken for HeartbeatNodeNotConfigured"
        );
    }
}
