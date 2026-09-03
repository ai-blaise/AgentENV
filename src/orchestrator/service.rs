use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use futures::stream::StreamExt;
use futures::FutureExt;
use tokio::sync::{broadcast, oneshot, watch, Mutex, OnceCell, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use crate::cfg::{ConfigManager, MemoryControlConfig};
use crate::image::cache::{
    local_image_services_from_global_config, RuntimeImageOwner, RuntimeImageRefs,
};
use crate::observability::prometheus::{record_checkpoint, MetricGuard, SandboxStageTimer};
use crate::sandbox::{
    CheckpointStats, CustomExtensionClient, CustomExtensionParams, EnvdAccessToken,
    FirecrackerPausedState, FirecrackerSandboxFactory, FirecrackerSnapshotManifest,
    FreshSandboxBuildSpec, PausedSandboxState, RuntimeArtifactSet, SandboxAccessTokenGenerator,
    SandboxBackend, SandboxBackendFactory, SandboxCaptureError, SandboxForkSpec,
    SandboxLaunchConfig, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::SnapshotRuntimeVersions;
use crate::types::{bytes_to_mib_ceil, SandboxId, SandboxResources};

use super::admission::{AdmissionController, AdmissionGuard, NodeCapacityInputs};
use super::launch_plan::{CreateLaunchSource, LaunchPlan};
use super::metrics::{
    aggregate_resource_metrics, run_memory_control_pass, MemoryControlPassReport,
    MemoryControlSandbox, MemoryControlState, OrchestratorCounters, OrchestratorMetrics,
    SandboxContribution,
};
use super::mobility::{MobilityHooks, ResumeFence};
use super::persistence::{DisabledSandboxPersister, FileBackedSandboxPersister, SandboxPersister};
use super::proxy::{ProxyLookupResult, ProxyRoute, ProxyRouteTable, ProxyTarget};
use super::store::*;
use super::types::{
    CreateSandboxRequest, SandboxLaunchSource, SandboxLifecycleEvent, SandboxLifecycleEventType,
    SandboxState, SnapshotCaptureResult,
};
use super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;

/// Maximum time to wait for a sandbox to leave a transitional state.
/// Guards against indefinite blocking when a sandbox's in-progress operation
/// never completes (e.g. the task holding the state panics without rolling back).
/// How many times a user operation re-attempts its state claim while a capture
/// holds the sandbox. Two waits and a final try: a capture is one freeze, so
/// more than that means something is re-claiming faster than the caller can
/// win, and erroring is more useful than waiting.
const CAPTURE_WAIT_ATTEMPTS: usize = 3;

/// How long one speculative checkpoint may hold its sandbox.
///
/// The whole point of a checkpoint is that it is short -- it freezes a running
/// guest -- so anything near this is already pathological. It exists because
/// nothing underneath it has a deadline: every Firecracker API call goes
/// through a client with no request, connect or read timeout, so an
/// unresponsive VMM parks the caller permanently and takes the sandbox's state
/// claim with it.
const CHECKPOINT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

const WAIT_TRANSITION_TIMEOUT: Duration = Duration::from_secs(60);
const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;
const MEMORY_CONTROL_PASS_METRIC: &str = "agentenv_memory_control_pass_duration_seconds";

/// Sub-stage timings for a resume, under the same `operation` the API handler
/// already reports the whole call as.
///
/// The handler's single stage says how long a resume took and nothing about
/// where it went. These say which part: the fence, which is a scheduler round
/// trip in a cluster, and the launch, which is the guest coming back. The
/// timer holds nothing but the operation name, so one shared value is the same
/// thing as constructing one per call.
const RESUME_STAGES: SandboxStageTimer = SandboxStageTimer::new("resume");

#[derive(Clone, Debug)]
enum ShutdownOutcome {
    Success,
    Failed(String),
}

impl ShutdownOutcome {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Success,
            Err(OrchestratorError::InternalError(message)) => Self::Failed(message),
            Err(err) => Self::Failed(err.to_string()),
        }
    }

    fn as_result(&self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failed(message) => Err(OrchestratorError::InternalError(message.clone())),
        }
    }
}

/// Committing a paused sandbox's state where the rest of the cluster can read
/// it.
///
/// A drain needs this and the orchestrator cannot do it: the repository and
/// its snapshot manager are built on top of the orchestrator, and handing it
/// either would invert that. So whoever already holds both supplies one.
#[async_trait::async_trait]
pub trait PausedStatePublisher: Send + Sync {
    /// Publishes a paused sandbox's artifacts and records it as movable.
    async fn publish_paused(&self, sandbox_id: SandboxId) -> anyhow::Result<()>;
}

/// What one drain pass managed.
///
/// Shaped for a `preStop` hook to poll. `remaining` is the field that decides
/// whether the process can be killed, and it counts what the node is actually
/// still holding rather than what this pass happened to get through — a pass
/// that did nothing because everything was already paused reports zero, which
/// is the answer the caller needs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrainProgress {
    /// Sandboxes on this node that are not paused yet.
    pub remaining: usize,
    /// Paused sandboxes this pass committed to the repository.
    pub published: usize,
    /// Sandboxes this pass tried and could not pause or publish.
    pub failed: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailedLaunchStage {
    Registered,
    TransitionalPersisted,
    RunningPersisted,
}

impl FailedLaunchStage {
    fn rollback_expected_state(self, plan: &LaunchPlan) -> Option<SandboxState> {
        match self {
            Self::Registered => None,
            Self::TransitionalPersisted => Some(plan.transitional_state()),
            Self::RunningPersisted => Some(SandboxState::Running),
        }
    }

    fn should_detach_proxy_route(self) -> bool {
        matches!(self, Self::RunningPersisted)
    }
}

pub struct Orchestrator<
    S: MetadataStore = InMemoryMetadataStore,
    F: SandboxBackendFactory = crate::sandbox::NodeBackendFactory,
    P: SandboxPersister = FileBackedSandboxPersister,
> {
    store: S,
    factory: F,
    persister: P,
    sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
    proxy_routes: RwLock<ProxyRouteTable>,
    next_proxy_route_version: AtomicU64,
    counters: OrchestratorCounters,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    is_shutting_down: std::sync::atomic::AtomicBool,
    admission: Arc<AdmissionController>,
    /// Set once startup recovery has finished restoring persisted paused
    /// sandboxes into the store.
    ///
    /// Until then this node's sandbox roster is incomplete, and reporting it as
    /// authoritative would let the scheduler read an empty roster as "this node
    /// owns nothing" and delete every binding it holds. Today `load_all` is
    /// awaited before the reporter exists, so the window is not reachable; this
    /// makes that an invariant of the code rather than of construction order.
    roster_complete: std::sync::atomic::AtomicBool,
    shutdown_tx: watch::Sender<bool>,
    shutdown_outcome: OnceCell<ShutdownOutcome>,
    image_refs: Arc<dyn RuntimeImageRefs>,
    access_tokens: SandboxAccessTokenGenerator,
    /// Mobility bookkeeping, when this node participates in migration.
    ///
    /// Empty on a node that does not, so nothing here costs anything until an
    /// operator asks for it. Installed after construction because the durable
    /// store it needs is opened asynchronously, and a node that fails to open
    /// it should still run — without migration rather than not at all.
    mobility: OnceLock<Arc<dyn MobilityHooks>>,
}

impl Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, DisabledSandboxPersister> {
    pub async fn with_in_memory_store() -> Arc<Self> {
        Self::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            DisabledSandboxPersister,
        )
        .await
        .expect("in-memory orchestrator should never fail to initialize")
    }
}

impl<F> Orchestrator<InMemoryMetadataStore, F>
where
    F: SandboxBackendFactory,
{
    pub async fn with_file_backed_store_and_factory(factory: F) -> Result<Arc<Self>> {
        let config = ConfigManager::global_config();
        let store = InMemoryMetadataStore::new();
        let persister = FileBackedSandboxPersister::new(
            config.orchestrator.persisted_sandbox_store_path.clone(),
            config.virtualization_mode,
        );
        Self::new(store, factory, persister).await
    }
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// Installs mobility bookkeeping. Called once at startup; a later call is
    /// ignored so a mis-ordered init cannot swap a live store.
    pub fn install_mobility(&self, mobility: Arc<dyn MobilityHooks>) {
        if self.mobility.set(mobility).is_err() {
            warn!("mobility was already installed; ignoring the later one");
        }
    }

    /// This node's mobility bookkeeping, if it participates in migration.
    pub fn mobility(&self) -> Option<&Arc<dyn MobilityHooks>> {
        self.mobility.get()
    }

    pub async fn new(store: S, factory: F, persister: P) -> Result<Arc<Self>> {
        let image_refs = local_image_services_from_global_config().runtime_refs;
        Self::new_inner(store, factory, persister, image_refs).await
    }

    async fn new_inner(
        store: S,
        factory: F,
        persister: P,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // Restore persisted sandboxes from the previous run, keeping the paused
        // ones (with their state) for the paused-protection reconcile below.
        let persisted = persister.load_all(&factory).await?;
        let managed_seed_must_exist = persisted_sandboxes_require_managed_seed(&persisted);
        let access_tokens = tokio::task::spawn_blocking(move || {
            SandboxAccessTokenGenerator::load_or_create(app_config, managed_seed_must_exist)
        })
        .await
        .context("join sandbox access-token seed loader")??;
        let restored_paused: Vec<(SandboxId, Arc<dyn PausedSandboxState>)> = persisted
            .iter()
            .filter(|metadata| metadata.state == SandboxState::Paused)
            .filter_map(|metadata| {
                metadata
                    .paused_state
                    .as_ref()
                    .map(|paused_state| (metadata.id, Arc::clone(paused_state)))
            })
            .collect();
        for metadata in persisted {
            store.add(metadata).await?;
        }

        let orchestrator = Arc::new(Self {
            store,
            factory,
            persister,
            sandboxes: RwLock::new(HashMap::new()),
            proxy_routes: RwLock::new(ProxyRouteTable::default()),
            next_proxy_route_version: AtomicU64::new(1),
            counters: OrchestratorCounters::default(),
            sandbox_event_tx,
            default_sandbox_timeout: Duration::from_secs(config.default_sandbox_timeout_secs),
            is_shutting_down: std::sync::atomic::AtomicBool::new(false),
            admission: super::admission::global_admission_controller(),
            roster_complete: std::sync::atomic::AtomicBool::new(false),
            shutdown_tx,
            shutdown_outcome: OnceCell::new(),
            image_refs,
            access_tokens,
            mobility: OnceLock::new(),
        });

        // Start the auto-evict task.
        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(
            Arc::clone(&orchestrator),
            evict_interval,
            shutdown_rx.clone(),
        );

        // Speculative checkpointing, when the operator has asked for it. Off by
        // default: it buys a shorter pause with a steady stream of short
        // freezes, which is only the right trade where pauses are on a latency
        // path.
        let checkpoint_interval = Duration::from_millis(config.speculative_checkpoint_interval_ms);
        if !checkpoint_interval.is_zero() {
            Self::start_speculative_checkpoint_task(
                Arc::clone(&orchestrator),
                checkpoint_interval,
                config.speculative_checkpoint_concurrency,
                shutdown_rx,
            );
        }

        // Reconcile durable paused protection, then start maintenance (fail-closed).
        let gc = app_config.image.cache.gc_schedule();
        if gc.enabled {
            match orchestrator
                .reconcile_paused_at_startup(&restored_paused)
                .await
            {
                Ok(()) => {
                    Self::start_local_image_maintenance_task(
                        Arc::clone(&orchestrator),
                        gc.interval,
                        orchestrator.shutdown_tx.subscribe(),
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "local image protection reconcile failed at startup; not starting maintenance (fail-closed)"
                    );
                }
            }
        }

        Self::start_memory_control_task(
            Arc::clone(&orchestrator),
            app_config.machine.memory_control.clone(),
            orchestrator.shutdown_tx.subscribe(),
        );

        orchestrator.roster_complete.store(true, Ordering::Release);

        Ok(orchestrator)
    }

    /// Whether this node's reported sandbox roster is complete, i.e. startup
    /// recovery has finished. See [`Orchestrator::roster_complete`].
    pub fn is_roster_complete(&self) -> bool {
        self.roster_complete.load(Ordering::Acquire)
    }

    async fn run_cancellation_safe<T>(
        self: &Arc<Self>,
        operation: &'static str,
        sandbox_id: SandboxId,
        future: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = future.await;
            if tx.send(result).is_err() {
                debug!(
                    sandbox_id = %sandbox_id,
                    operation,
                    "operation completed after caller stopped waiting"
                );
            }
        });

        rx.await.map_err(|err| {
            OrchestratorError::InternalError(format!(
                "operation task ended before reporting result: {err}"
            ))
        })?
    }

    async fn protect_image_refs(
        &self,
        owner: RuntimeImageOwner,
        artifacts: RuntimeArtifactSet,
        context: &'static str,
    ) -> Result<()> {
        self.image_refs
            .pin(owner, artifacts)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!("pin {context} image refs: {error:#}"))
            })
    }

    async fn release_image_refs(&self, owner: RuntimeImageOwner) {
        self.image_refs.unpin_best_effort(owner).await;
    }

    /// Snapshot the running set's local runtime artifacts for maintenance.
    async fn collect_running_artifacts(&self) -> Vec<(SandboxId, RuntimeArtifactSet)> {
        let handles = {
            self.sandboxes
                .read()
                .await
                .iter()
                .map(|(sandbox_id, handle)| (*sandbox_id, Arc::clone(handle)))
                .collect::<Vec<_>>()
        };
        let mut running = Vec::with_capacity(handles.len());
        for (sandbox_id, handle) in handles {
            let artifacts = {
                let sandbox = handle.lock().await;
                sandbox.runtime_info().runtime_artifacts
            };
            running.push((sandbox_id, artifacts));
        }
        running
    }

    /// Fail-closed startup reconcile before maintenance can run: durably protect
    /// every restored paused sandbox, then drop orphaned paused protection.
    async fn reconcile_paused_at_startup(
        &self,
        restored_paused: &[(SandboxId, Arc<dyn PausedSandboxState>)],
    ) -> Result<()> {
        let mut live_paused = Vec::with_capacity(restored_paused.len());
        for (sandbox_id, paused_state) in restored_paused {
            self.protect_image_refs(
                RuntimeImageOwner::PausedSandbox(*sandbox_id),
                paused_state.runtime_artifacts(),
                "paused sandbox",
            )
            .await?;
            live_paused.push(*sandbox_id);
        }
        self.image_refs
            .reconcile_paused(&live_paused)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "reconcile local image protection: {error:#}"
                ))
            })
    }

    /// Creates and starts a new sandbox from a resolved launch source.
    ///
    /// This call only returns after the sandbox is fully ready and persisted
    /// as `Running`, so callers can treat a successful return as immediately
    /// usable without additional polling.
    pub async fn create_sandbox(
        self: &Arc<Self>,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        self.ensure_scheduler_contact()?;
        // Admission runs before the task spawn and before any side effect, so
        // a rejection costs nothing and leaves no state behind. That is what
        // makes it safe for the caller to place the sandbox elsewhere.
        let admission = self.admit(1, request.requested_resources()).await?;

        let sandbox_id = SandboxId::new();
        let this = Arc::clone(self);
        let result = self
            .run_cancellation_safe("create", sandbox_id, async move {
                this.create_sandbox_inner(sandbox_id, request).await
            })
            .await;

        // Hand accounting to the metadata store only once the sandbox is in
        // it. Until then the reservation is what bounds the node, and on any
        // failure path the guard's Drop returns the capacity.
        //
        // The store starts counting the sandbox slightly before this point, so
        // the two overlap briefly. That double-counts, which errs toward
        // rejecting early; under-counting would let a burst through.
        if result.is_ok() {
            admission.commit();
        }
        result
    }

    /// Refuses new sandboxes while the node is out of contact with the
    /// scheduler.
    ///
    /// Deliberately checked only on the create paths rather than in the shared
    /// lifecycle guard. That guard also covers keep-alive, resume and capture;
    /// blocking those would stop a partitioned node from renewing the very
    /// sandboxes it is trying to protect, and auto-eviction would then delete
    /// them. Refusing new work is the only action that is safe while the node
    /// cannot tell a scheduler restart from a partition.
    fn ensure_scheduler_contact(&self) -> Result<()> {
        let switch = crate::observability::global_kill_switch();
        if !switch.blocks_creates() {
            return Ok(());
        }
        let elapsed_secs = switch
            .since_last_success()
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();
        metrics::counter!("agentenv_kill_switch_rejected_total").increment(1);
        Err(OrchestratorError::SchedulerContactLost { elapsed_secs })
    }

    /// Reserves node capacity for `count` sandboxes, or reports why not.
    async fn admit(
        self: &Arc<Self>,
        count: u32,
        resources: SandboxResources,
    ) -> Result<AdmissionGuard> {
        let capacity = NodeCapacityInputs {
            available_network_slots: crate::sandbox::network_slot_capacity().available(),
        };
        let this = Arc::clone(self);
        match self
            .admission
            .try_admit(count, resources, capacity, || async move {
                this.metrics_snapshot().await.ok()
            })
            .await
        {
            Ok(guard) => Ok(guard),
            Err(reason) => {
                metrics::counter!(
                    "agentenv_admission_rejected_total",
                    "reason" => reason.as_str()
                )
                .increment(1);
                Err(OrchestratorError::AdmissionRejected {
                    reason,
                    retry_after: self.admission.retry_after(),
                })
            }
        }
    }

    #[tracing::instrument(
        name = "create_sandbox",
        skip(self, request),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn create_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        if let Err(err) = self.ensure_accepting_lifecycle_operations() {
            self.counters.record_create_fail(1);
            return Err(err);
        }

        let CreateSandboxRequest {
            source,
            timeout,
            timeout_action,
            user_metadata,
            env_vars,
            auto_resume,
            network_policy,
            custom_extension_params,
            secure,
        } = request;
        let envd_access_token = secure.then(|| self.access_tokens.generate(sandbox_id));
        info!(timeout = ?timeout, "creating sandbox");

        let result = match source {
            SandboxLaunchSource::Snapshot(snapshot) => {
                let record = snapshot.record();
                let committed = snapshot.committed();
                let configured_mode = ConfigManager::global_config().virtualization_mode;
                if committed.virtualization_mode != configured_mode {
                    self.counters.record_create_fail(1);
                    return Err(OrchestratorError::VirtualizationModeMismatch {
                        resource: format!("snapshot {}", record.id),
                        resource_mode: committed.virtualization_mode,
                        node_mode: configured_mode,
                    });
                }
                let launch_image_configs = committed.image_configs.clone();
                let mut extra_mmds = serde_json::Map::new();
                if !launch_image_configs.is_empty() {
                    extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
                };
                // Effective custom config: a launch-provided value overrides the
                // one persisted in the source snapshot; otherwise inherit it.
                // Store the effective value so publishing a snapshot from this
                // sandbox keeps the inherited config instead of dropping it.
                let effective_custom_extension_params = custom_extension_params
                    .clone()
                    .or_else(|| committed.custom_extension_params.clone());
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: record.id.to_string(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    extra_mmds,
                    custom_extension_params: effective_custom_extension_params.clone(),
                    envd_access_token: envd_access_token.clone(),
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: record.id.to_string(),
                    snapshot_alias: record.alias.as_ref().map(ToString::to_string),
                    virtualization_mode: committed.virtualization_mode,
                    runtime_versions: committed.runtime_versions.clone(),
                    resources: *snapshot.resources(),
                    context: committed.context.clone(),
                    startup: committed.startup.clone(),
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params: effective_custom_extension_params,
                    secure,
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::for_create_from_snapshot(
                    sandbox_id,
                    snapshot,
                    launch_config,
                    transitional_metadata,
                    NewTimeout::Set(timeout.unwrap_or(self.default_sandbox_timeout)),
                ))
                .await
            }
            SandboxLaunchSource::Image {
                image_ref,
                overlaybd_config_path,
                context,
                resources,
                extra_drives,
                extra_boot_args,
                image_configs,
            } => {
                let context = *context;
                let resources = resources.unwrap_or_else(default_fresh_sandbox_resources);
                let launch_image_configs = *image_configs;
                let mut extra_mmds = serde_json::Map::new();
                if !launch_image_configs.is_empty() {
                    extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
                };
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: image_ref.clone(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    extra_mmds,
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                };
                let build_spec = FreshSandboxBuildSpec {
                    image_config_path: overlaybd_config_path,
                    context: context.clone(),
                    resources,
                    extra_drives,
                    extra_boot_args,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: image_ref,
                    snapshot_alias: None,
                    virtualization_mode: ConfigManager::global_config().virtualization_mode,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    context,
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
                    secure,
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::for_create_fresh(
                    sandbox_id,
                    build_spec,
                    launch_config,
                    transitional_metadata,
                    NewTimeout::Set(timeout.unwrap_or(self.default_sandbox_timeout)),
                ))
                .await
            }
        };

        match result {
            Ok(metadata) => {
                self.counters.record_create_success(1);
                self.publish_sandbox_event(
                    SandboxLifecycleEventType::Create,
                    metadata.id,
                    metadata.resources,
                );
                Ok(metadata)
            }
            Err(err) => {
                self.counters.record_create_fail(1);
                Err(err)
            }
        }
    }

    /// Forks a running sandbox into multiple new sandboxes on the same node.
    pub async fn fork_sandbox(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        count: u32,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, count, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self),
        fields(source_sandbox_id = %source_sandbox_id, count)
    )]
    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        count: u32,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("forking sandboxes");

        let source_handle = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&source_sandbox_id).cloned()
        }
        .ok_or(OrchestratorError::SandboxNotFound(source_sandbox_id))?;

        self.ensure_scheduler_contact()?;
        // Admit the whole fan-out before the Running -> Forking transition, so
        // a rejection never strands the source in Forking. Children inherit the
        // source's resources.
        let source_resources = self
            .store
            .get(&source_sandbox_id)
            .await
            .map_err(OrchestratorError::from)?
            .ok_or(OrchestratorError::SandboxNotFound(source_sandbox_id))?
            .resources;
        let admission = self.admit(count, source_resources).await?;

        let mut claimed = None;
        for attempt in 0..CAPTURE_WAIT_ATTEMPTS {
            match self
                .store
                .update_if_state(&source_sandbox_id, &[SandboxState::Running], |metadata| {
                    metadata.state = SandboxState::Forking
                })
                .await
            {
                Ok(update) => {
                    claimed = Some(update.previous);
                    break;
                }
                Err(err) => {
                    if self
                        .waited_out_capture(source_sandbox_id, &err, attempt)
                        .await
                    {
                        continue;
                    }
                    return Err(match err {
                        StoreError::StateConflict {
                            actual_state: SandboxState::Killing,
                            ..
                        } => OrchestratorError::SandboxNotFound(source_sandbox_id),
                        StoreError::StateConflict { actual_state, .. } => {
                            OrchestratorError::InvalidSandboxState {
                                sandbox_id: source_sandbox_id,
                                state: actual_state,
                            }
                        }
                        err => OrchestratorError::from(err),
                    });
                }
            }
        }
        let source_metadata = match claimed {
            Some(metadata) => metadata,
            // Every attempt lost to a capture. Reported as the conflict it is
            // rather than retried forever.
            None => {
                return Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id: source_sandbox_id,
                    state: SandboxState::Snapshotting,
                })
            }
        };

        let children_spec = (0..count)
            .map(|_| {
                let sandbox_id = SandboxId::new();
                SandboxForkSpec {
                    sandbox_id,
                    envd_access_token: source_metadata
                        .secure
                        .then(|| self.access_tokens.generate(sandbox_id)),
                }
            })
            .collect::<Vec<_>>();

        // Start to fork the sandbox.
        // This is a single operation that will return a list of results for each child sandbox.
        let fork_result = {
            let mut sandbox = source_handle.lock().await;
            sandbox.fork(&children_spec).await
        };
        let forked_backends = match fork_result {
            Ok(forked_backends) => forked_backends,
            Err(err) => {
                warn!(error = ?err, "failed to fork sandbox");
                self.counters.record_create_fail(u64::from(count));
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&source_sandbox_id)
                        .await;
                    let _ = {
                        let mut sandbox = source_handle.lock().await;
                        sandbox.stop().await
                    };
                    self.store.remove(&source_sandbox_id).await?;
                } else {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &source_sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Forking],
                        )
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: err.into(),
                });
            }
        };

        // Restore the source sandbox's state to Running.
        if let Err(err) = self
            .store
            .update_state_if_state(
                &source_sandbox_id,
                SandboxState::Running,
                &[SandboxState::Forking],
            )
            .await
        {
            warn!(error = ?err, "failed to restore source sandbox metadata after fork");
        }

        // Register each forked sandbox in the store and runtime, and publish events.
        let mut outcomes = Vec::with_capacity(children_spec.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for (child, backend) in children_spec.into_iter().zip(forked_backends) {
            let sandbox_id = child.sandbox_id;
            let backend = match backend {
                Ok(backend) => backend,
                Err(err) => {
                    warn!(%sandbox_id, error = ?err, "failed to start forked sandbox");
                    outcomes.push(Err(Self::fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            metadata.paused_state = None;
            metadata.update_timeout(new_timeout);

            let proxy_target = match Self::proxy_target_from_sandbox(backend.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    Self::stop_failed_fork(backend, sandbox_id).await;
                    outcomes.push(Err(Self::fork_child_error(
                        sandbox_id,
                        anyhow::Error::new(err),
                    )));
                    continue;
                }
            };
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(%sandbox_id, error = ?err, "failed to register forked sandbox");
                Self::stop_failed_fork(backend, sandbox_id).await;
                outcomes.push(Err(Self::fork_child_error(
                    sandbox_id,
                    anyhow::Error::new(err),
                )));
                continue;
            }
            self.sandboxes
                .write()
                .await
                .insert(metadata.id, Arc::new(Mutex::new(backend)));
            self.upsert_proxy_route(metadata.id, proxy_target).await;
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Fork,
                metadata.id,
                metadata.resources,
            );
            // This child is now counted by the store; release its share of the
            // bulk reservation incrementally rather than all at once at the
            // end, so a slow fan-out does not hold capacity for children that
            // are already accounted for.
            admission.commit_one(metadata.resources);
            successes += 1;
            outcomes.push(Ok(metadata));
        }

        // Children that failed to start never reached the store, so their share
        // of the reservation is returned when the guard drops.
        self.counters.record_create_success(successes);
        self.counters
            .record_create_fail(u64::from(count) - successes);
        Ok(outcomes)
    }

    fn fork_child_error(sandbox_id: SandboxId, source: anyhow::Error) -> OrchestratorError {
        OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Fork,
            source,
        }
    }

    async fn stop_failed_fork(mut backend: Box<dyn SandboxBackend>, sandbox_id: SandboxId) {
        if let Err(err) = backend.stop().await {
            warn!(%sandbox_id, error = ?err, "failed to stop unsuccessful fork");
        }
    }

    /// Retrieves the metadata for a sandbox by its ID.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn get_sandbox(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self.store.get(sandbox_id).await?)
    }

    /// Lists all sandboxes with their metadata.
    #[tracing::instrument(skip(self))]
    pub async fn list_sandboxes(&self) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list().await?)
    }

    /// Lists all sandbox IDs currently tracked by the store.
    pub async fn list_sandbox_ids(&self) -> Result<Vec<SandboxId>> {
        Ok(self.store.list_ids().await?)
    }

    /// Lists sandboxes that match the provided filter criteria:
    /// - If `states` is provided, only sandboxes in those states will be included.
    /// - If `user_metadata` is provided, only sandboxes whose user metadata contains
    ///   all the specified key-value pairs will be included.
    /// - If `started_after` is provided, only sandboxes created at or after that instant
    ///   will be included.
    /// - If `template` is provided, only sandboxes using the matching snapshot ID or alias
    ///   will be included.
    #[tracing::instrument(skip(self, filter))]
    pub async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list_filtered(filter).await?)
    }

    pub fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        metadata
            .secure
            .then(|| self.access_tokens.generate(metadata.id))
    }

    pub fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches(sandbox_id, candidate)
    }

    pub fn traffic_access_token(&self, sandbox_id: SandboxId) -> String {
        self.access_tokens.generate_traffic(sandbox_id)
    }

    pub fn validate_traffic_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches_traffic(sandbox_id, candidate)
    }

    /// Resolves the current proxyability of a sandbox without touching the sandbox mutex.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn proxy_lookup_for(&self, sandbox_id: &SandboxId) -> Result<ProxyLookupResult> {
        if let Some(route) = self.proxy_routes.read().await.route(sandbox_id).cloned() {
            trace!(
                version = route.version(),
                "resolved running proxy target from runtime table"
            );
            return Ok(ProxyLookupResult::Ready(route.target().clone()));
        }

        let metadata = self.store.get(sandbox_id).await?;
        Ok(match metadata {
            None => {
                debug!("sandbox has no runtime route or persisted metadata");
                ProxyLookupResult::NotFound
            }
            Some(metadata) if metadata.state == SandboxState::Running => {
                warn!("running sandbox is missing a runtime proxy route");
                ProxyLookupResult::RouteMissing
            }
            Some(metadata) if metadata.state == SandboxState::Paused => {
                debug!(auto_resume = metadata.auto_resume, "sandbox is paused");
                ProxyLookupResult::Paused {
                    auto_resume: metadata.auto_resume,
                }
            }
            Some(metadata) => {
                debug!(state = ?metadata.state, "sandbox exists but is not proxyable");
                ProxyLookupResult::Unavailable(metadata.state)
            }
        })
    }

    /// Updates the keep-alive timeout for a RUNNING sandbox.
    /// If `timeout` is `None`, default timeout will be applied.
    /// If `allow_shorter` is `false`, the update will be skipped if the new TTL is not longer than the existing TTL.
    ///
    /// When the sandbox is in a transitional state that may resolve to `Running`,
    /// this method waits for the transition to complete before re-evaluating the state.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id, allow_shorter = allow_shorter))]
    pub async fn keep_alive_for(
        &self,
        sandbox_id: SandboxId,
        timeout: Option<Duration>,
        allow_shorter: bool,
    ) -> Result<Option<SandboxMetadata>> {
        self.ensure_accepting_lifecycle_operations()?;

        if timeout.is_none() {
            debug!("applying default timeout for keep-alive");
        } else {
            debug!(?timeout, "updating keep-alive timeout");
        }
        let valid_timeout = timeout.unwrap_or(self.default_sandbox_timeout);

        let mut metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => return Err(OrchestratorError::SandboxNotFound(sandbox_id)),
        };

        // If the sandbox is in a transitional state that may lead to Running,
        // wait for the transition to complete before checking whether the
        // keep-alive is applicable.
        if matches!(
            metadata.state,
            SandboxState::Creating
                | SandboxState::Resuming
                | SandboxState::Snapshotting
                | SandboxState::Forking
        ) {
            debug!(state = ?metadata.state, "sandbox in transitional state, waiting before applying keep-alive");
            metadata = self.wait_for_transition(sandbox_id, metadata.state).await?;
        }

        if metadata.state != SandboxState::Running {
            info!(state = ?metadata.state, "cannot update keep-alive timeout in non-running state");
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        let mut timeout_updated = false;
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                let new_expire_time = SystemTime::now().checked_add(valid_timeout);
                if !allow_shorter {
                    if let Some(current_expire) = metadata.expires_at {
                        if let Some(new_expire) = new_expire_time {
                            if new_expire <= current_expire {
                                info!(
                                    current_expire = ?current_expire,
                                    new_expire = ?new_expire,
                                    "new timeout is not longer than current timeout, skipping update",
                                );
                                return;
                            }
                        }
                    }
                }

                metadata.set_timeout(Some(valid_timeout));
                timeout_updated = true;
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "keep-alive update failed due to state conflict");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        if timeout_updated {
            info!(?valid_timeout, "sandbox keep-alive timeout updated");
        }

        Ok(Some(update_result.current))
    }

    /// Stops and deletes the sandbox with the given ID.
    ///
    /// If the sandbox is currently in a transitional state, this method waits for
    /// the in-progress operation to finish before proceeding with deletion, preventing
    /// races where an ongoing operation might overwrite the `Killing` state.
    pub async fn delete_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("deleting sandbox");

        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Killing,
                    &[SandboxState::Running, SandboxState::Paused],
                )
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        debug!("sandbox already in killing state, waiting for delete to finish");
                        match self
                            .wait_for_transition(sandbox_id, SandboxState::Killing)
                            .await
                        {
                            Ok(_) => {
                                // The in-flight delete rolled back to a stable state.
                                // Retry the Killing CAS rather than letting multiple
                                // deleters run concurrently.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted by a concurrent delete");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming => {
                        // An in-progress operation is currently holding the sandbox in this
                        // transitional state.  Wait for it to finish so our Killing transition
                        // doesn't race with the final state write from that operation.
                        debug!(
                            state = ?actual_state,
                            "sandbox in transitional state, waiting before deletion"
                        );
                        match self.wait_for_transition(sandbox_id, actual_state).await {
                            Ok(_) => {
                                // Transition finished; retry the Killing CAS.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                // Sandbox was removed while we waited (e.g. by
                                // another concurrent delete).
                                info!("sandbox was deleted while waiting for transitional state");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    _ => {
                        return Err(OrchestratorError::from(StoreError::StateConflict {
                            sandbox_id,
                            expected_states: vec![SandboxState::Running, SandboxState::Paused],
                            actual_state,
                        }));
                    }
                },
                Err(err) => return Err(OrchestratorError::from(err)),
            }
        };

        self.delete_sandbox_impl(sandbox_id, previous_state).await
    }

    async fn claim_expired_running_sandbox(
        &self,
        sandbox_id: SandboxId,
        cutoff: SystemTime,
        claimed_state: SandboxState,
    ) -> Result<bool> {
        match self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                if metadata.is_expired(cutoff) {
                    metadata.state = claimed_state;
                }
            })
            .await
        {
            Ok(update) if update.current.state == claimed_state => Ok(true),
            Ok(update) => {
                debug!(
                    expires_at = ?update.current.expires_at,
                    ?cutoff,
                    "skipping auto-eviction because sandbox expiry was updated"
                );
                Ok(false)
            }
            Err(StoreError::StateConflict { actual_state, .. }) => {
                debug!(
                    state = ?actual_state,
                    "skipping auto-eviction because sandbox state changed"
                );
                Ok(false)
            }
            Err(StoreError::SandboxNotFound { .. }) => {
                debug!("skipping auto-eviction because sandbox no longer exists");
                Ok(false)
            }
            Err(err) => Err(OrchestratorError::from(err)),
        }
    }

    /// Claims a running sandbox for a speculative checkpoint.
    ///
    /// The read-modify-write happens inside the store's single write lock, so a
    /// user pause, snapshot or delete arriving at the same moment
    /// deterministically wins or loses rather than interleaving with the
    /// freeze. Losing is the common case and is not an error.
    ///
    /// `Snapshotting` is reused rather than adding a state: `SandboxState` is
    /// user-visible through `Display` and the API model, and a checkpoint is
    /// not a thing users asked for or should have to learn about.
    async fn claim_checkpointable_sandbox(
        &self,
        sandbox_id: SandboxId,
        now: SystemTime,
        interval: Duration,
    ) -> Result<bool> {
        match self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                if metadata.checkpoint_due(now, interval) {
                    metadata.state = SandboxState::Snapshotting;
                }
            })
            .await
        {
            Ok(update) if update.current.state == SandboxState::Snapshotting => Ok(true),
            Ok(_) => Ok(false),
            Err(StoreError::StateConflict { .. }) | Err(StoreError::SandboxNotFound { .. }) => {
                Ok(false)
            }
            Err(err) => Err(OrchestratorError::from(err)),
        }
    }

    /// Checkpoints one claimed sandbox and returns it to `Running`.
    ///
    /// The state is restored whatever happens. A checkpoint mutates nothing the
    /// guest depends on, so a failed one must leave a running sandbox running;
    /// leaving it in `Snapshotting` would strand it in a state no user action
    /// can clear.
    async fn checkpoint_claimed_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
        let outcome = match handle {
            Some(handle) => {
                let mut sandbox = handle.lock().await;
                // Bounded, and caught. Every Firecracker API call underneath
                // this goes through a hyper client with no timeout of any kind,
                // so a VMM that stops answering its socket parks the call for
                // good -- and this task holds the claim. Without both guards the
                // restore below is unreachable, the sandbox stays
                // `Snapshotting`, and nothing clears it: delete waits out
                // `WAIT_TRANSITION_TIMEOUT` and returns InvalidSandboxState,
                // pause, snapshot and fork exhaust the capture wait and do the
                // same. The pattern is `run_memory_control_pass`'s, which bounds
                // its backend call for this reason.
                let checkpoint = std::panic::AssertUnwindSafe(sandbox.checkpoint());
                match tokio::time::timeout(CHECKPOINT_CALL_TIMEOUT, checkpoint.catch_unwind()).await
                {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(SandboxCaptureError::recoverable(anyhow::anyhow!(
                        "the backend panicked during a speculative checkpoint"
                    ))),
                    Err(_) => Err(SandboxCaptureError::recoverable(anyhow::anyhow!(
                        "speculative checkpoint did not finish within {}ms",
                        CHECKPOINT_CALL_TIMEOUT.as_millis()
                    ))),
                }
            }
            None => Ok(CheckpointStats::default()),
        };

        let restored = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Snapshotting], |metadata| {
                metadata.state = SandboxState::Running;
                metadata.last_checkpoint_at = Some(SystemTime::now());
            })
            .await;
        if let Err(err) = restored {
            warn!("failed to return sandbox to running after checkpoint: {err}");
        }

        match outcome {
            Ok(stats) => {
                if stats.layer_bytes > 0 {
                    record_checkpoint(stats.layer_bytes, stats.freeze);
                }
                Ok(())
            }
            Err(err) => Err(OrchestratorError::from(anyhow::Error::from(err))),
        }
    }

    /// Checkpoints every running sandbox that is due, bounded in concurrency.
    ///
    /// Bounded because the memory read behind a capture occupies a blocking
    /// thread per concurrent chunk; unbounded, a node with many sandboxes would
    /// spend its blocking pool on speculation and stop serving the proxy.
    async fn checkpoint_due_sandboxes(
        self: &Arc<Self>,
        interval: Duration,
        concurrency: usize,
    ) -> Result<usize> {
        if self.is_shutting_down() || interval.is_zero() {
            return Ok(0);
        }

        let now = SystemTime::now();
        let candidates: Vec<SandboxId> = self
            .store
            .list()
            .await?
            .into_iter()
            .filter(|metadata| {
                metadata.state == SandboxState::Running && metadata.checkpoint_due(now, interval)
            })
            .map(|metadata| metadata.id)
            .collect();

        let permits = Arc::new(Semaphore::new(concurrency.max(1)));
        let mut tasks = JoinSet::new();
        for sandbox_id in candidates {
            let this = Arc::clone(self);
            let permits = Arc::clone(&permits);
            tasks.spawn(async move {
                let _permit = permits.acquire_owned().await.ok()?;
                match this
                    .claim_checkpointable_sandbox(sandbox_id, now, interval)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(err) => {
                        debug!("skipping checkpoint claim: {err}");
                        return None;
                    }
                }
                match this.checkpoint_claimed_sandbox(sandbox_id).await {
                    Ok(()) => Some(()),
                    Err(err) => {
                        warn!(%sandbox_id, "speculative checkpoint failed: {err}");
                        None
                    }
                }
            });
        }

        let mut checkpointed = 0;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Some(())) => checkpointed += 1,
                Ok(None) => {}
                // A task that panicked outside the guard above, or was aborted.
                // Reported rather than discarded: its sandbox may still hold the
                // claim, and silence here is what made that invisible.
                Err(err) => warn!("a speculative checkpoint task did not finish: {err}"),
            }
        }
        Ok(checkpointed)
    }

    /// Background driver for speculative checkpoints.
    ///
    /// Structurally the auto-evict task: a `Weak<Self>` so the orchestrator can
    /// still be dropped, a skipping ticker so a slow sweep does not queue
    /// another, and the same shutdown channel.
    fn start_speculative_checkpoint_task(
        this: Arc<Self>,
        interval: Duration,
        concurrency: usize,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("speculative checkpoint task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("speculative checkpoint task started with interval {interval:?}");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else { break };
                        if let Err(err) =
                            this.checkpoint_due_sandboxes(interval, concurrency).await
                        {
                            warn!("speculative checkpoint sweep failed: {err}");
                        }
                    }
                }
            }
        });
    }

    async fn delete_sandbox_impl(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        previous_state: SandboxState,
    ) -> Result<()> {
        let (handle, removed_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        // If the sandbox is still in memory, attempt to stop it.
        if let Some(handle) = handle {
            let stop_result = {
                let mut sandbox = handle.lock().await;
                sandbox.stop().await
            };

            if let Err(err) = stop_result {
                warn!(error = ?err, "failed to stop sandbox during delete");
                self.sandboxes.write().await.insert(sandbox_id, handle);
                self.restore_proxy_route(sandbox_id, removed_route).await;
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;

                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Stop,
                    source: err,
                });
            }
        }

        // Now the sandbox is successfully stopped, remove its metadata.
        let metadata = self.store.remove(&sandbox_id).await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.resources,
            );
        }
        if let Err(err) = self
            .persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
        {
            warn!(error = ?err, "failed to delete persisted sandbox state");
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        if let Some(mobility) = self.mobility() {
            // A deleted sandbox leaves no tombstone: it did not move, it
            // stopped existing, and a drain that still saw it would keep
            // trying to place something that is gone.
            mobility.forget(&sandbox_id).await;
        }
        info!("sandbox deleted");

        Ok(())
    }

    /// Stops every known sandbox and tears down in-memory runtime state.
    ///
    /// This is single-flight: the first caller performs cleanup and subsequent
    /// callers wait for the same outcome rather than starting duplicate work.
    ///
    /// Cleanup itself is still best-effort: the executor keeps attempting
    /// remaining sandboxes even if individual deletions fail, then returns an
    /// error if any sandbox could not be cleaned up after several passes.
    #[tracing::instrument(skip(self))]
    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let was_already_shutting_down = self.is_shutting_down.swap(true, Ordering::AcqRel);
        let _ = self.shutdown_tx.send_replace(true);

        if !was_already_shutting_down {
            info!("orchestrator shutdown requested; stopping all sandboxes");
        }

        let this = Arc::clone(self);
        let outcome = self
            .shutdown_outcome
            .get_or_init(|| async move {
                ShutdownOutcome::from_result(this.run_shutdown_cleanup().await)
            })
            .await;

        outcome.as_result()
    }

    /// Empties the node: refuses new work, then pauses and publishes what it
    /// is already holding, bounded by `deadline`.
    ///
    /// Refusing comes first, and is irreversible for the life of the process.
    /// A drain that paused first would race the creates still arriving and
    /// would never converge — every sandbox it published could be replaced by
    /// one that landed while it was working.
    ///
    /// Bounded rather than complete. The caller is a preemption warning with a
    /// fixed window, and a pass that overruns it is killed part-way through a
    /// pause. So one pass does what it can under one deadline and reports what
    /// is left, and the caller polls until `remaining` and `failed` are both
    /// zero or its own window runs out — `failed` too, because a sandbox this
    /// pass paused but could not publish is no longer counted in `remaining`
    /// while its state has still not left the node. Sandboxes mid-transition
    /// are left for a later pass
    /// rather than waited on: [`WAIT_TRANSITION_TIMEOUT`] is a minute, and
    /// spending a whole window on one wedged sandbox is exactly how the
    /// sequential shutdown loop this replaces fails to fit any window at all.
    ///
    /// Publishing is skipped entirely on a node with no mobility installed.
    /// The published snapshot is only reachable through the mobility record
    /// that names it, so without one the upload is a copy of every byte the
    /// sandbox owns that nothing will ever read. It is skipped per sandbox for
    /// state an earlier pass already committed, which is what keeps repeated
    /// passes cheap: the caller polls, so most of what a pass sees is what the
    /// pass before it published.
    pub async fn drain(
        self: &Arc<Self>,
        deadline: Duration,
        concurrency: usize,
        publisher: &dyn PausedStatePublisher,
    ) -> Result<DrainProgress> {
        let was_already_closed = self.is_shutting_down.swap(true, Ordering::AcqRel);
        let _ = self.shutdown_tx.send_replace(true);
        if !was_already_closed {
            info!(
                deadline_ms = deadline.as_millis(),
                "node drain requested; this node will admit no further sandboxes"
            );
        }

        let published = std::sync::atomic::AtomicUsize::new(0);
        let failed = std::sync::atomic::AtomicUsize::new(0);

        let candidates = self.store.list().await?;
        let pass = futures::stream::iter(candidates)
            .for_each_concurrent(concurrency.max(1), |metadata| {
                let this = Arc::clone(self);
                let published = &published;
                let failed = &failed;
                async move {
                    let sandbox_id = metadata.id;
                    match metadata.state {
                        SandboxState::Running => {
                            match this.pause_sandbox(sandbox_id).await {
                                Ok(()) => {}
                                // Gone between the roster read and the pause,
                                // which is not a failure to report: there is
                                // nothing left to preserve, and counting it
                                // would send an operator looking for a sandbox
                                // that no longer exists.
                                Err(OrchestratorError::SandboxNotFound(_)) => return,
                                Err(error) => {
                                    warn!(%sandbox_id, error = %error, "drain could not pause a sandbox");
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    return;
                                }
                            }
                        }
                        SandboxState::Paused => {}
                        // Someone else's operation owns this sandbox. It will
                        // be Running or Paused by the next pass, and by then
                        // whoever owns it has finished rather than been
                        // interrupted half-way.
                        _ => return,
                    }
                    let Some(mobility) = this.mobility() else {
                        return;
                    };
                    // Already in the repository, so this pass has nothing to
                    // add. A caller polls until the node is empty, so most
                    // passes are looking mostly at sandboxes an earlier pass
                    // published: re-uploading each one's memory image would
                    // make a poll cost more the longer the node takes to
                    // converge, against storage the sandboxes still running
                    // here are using. And the second upload is not even a
                    // no-op — it commits a new snapshot id over the record, so
                    // the snapshot the last pass published is left in the
                    // repository with nothing naming it.
                    if let Some(snapshot_id) = mobility.committed_snapshot(&sandbox_id).await {
                        debug!(
                            %sandbox_id,
                            %snapshot_id,
                            "drain skipped a sandbox whose paused state is already published"
                        );
                        return;
                    }
                    match publisher.publish_paused(sandbox_id).await {
                        Ok(()) => {
                            published.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            warn!(
                                %sandbox_id,
                                error = ?error,
                                "drain paused a sandbox but could not publish it; it stays on this node"
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });

        if tokio::time::timeout(deadline, pass).await.is_err() {
            warn!(
                deadline_ms = deadline.as_millis(),
                "drain pass hit its deadline; reporting what is left"
            );
        }

        // Counted after the pass rather than tracked through it, so a sandbox
        // another task paused concurrently is not still reported as work.
        let remaining = self
            .store
            .list_filtered(SandboxListFilter {
                excluded_states: Some(vec![SandboxState::Paused]),
                ..SandboxListFilter::matches_all()
            })
            .await?
            .len();

        // `failed` is derived from the store, not accumulated from the arms.
        //
        // It means "paused here, and its state is not in the repository" -- the
        // shipped preStop hook breaks its poll loop on `remaining == 0 &&
        // failed == 0`, so this is the counter that stops a node being declared
        // drained while it holds the only copy of something. Accumulating it
        // from `publish_paused`'s `Err` arm missed the case the deadline
        // creates: when the pass runs out of time it drops every in-flight
        // publish, and a dropped future reaches neither arm. The sandbox was
        // `Paused` by then, so it also fell out of the `remaining` count above,
        // and the pass reported a drained node.
        //
        // `committed_snapshot` is the same authority the pass uses to decide a
        // sandbox needs no publishing, so the two cannot disagree. Without
        // mobility nothing publishes at all and there is nothing to be behind.
        let mut failed = failed.into_inner();
        if let Some(mobility) = self.mobility() {
            let paused = self
                .store
                .list_filtered(SandboxListFilter {
                    states: Some(vec![SandboxState::Paused]),
                    ..SandboxListFilter::matches_all()
                })
                .await?;
            let mut unpublished = 0;
            for metadata in paused {
                if mobility.committed_snapshot(&metadata.id).await.is_none() {
                    unpublished += 1;
                }
            }
            if unpublished > failed {
                warn!(
                    unpublished,
                    counted_from_errors = failed,
                    "drain pass left paused sandboxes whose state is not in the repository"
                );
            }
            failed = unpublished;
        }

        let progress = DrainProgress {
            remaining,
            published: published.into_inner(),
            failed,
        };
        info!(
            remaining = progress.remaining,
            published = progress.published,
            failed = progress.failed,
            "drain pass finished"
        );
        Ok(progress)
    }

    /// Pauses a running sandbox by taking a snapshot and stopping its VM.
    ///
    /// If another `pause_sandbox` call is already in progress for the same
    /// sandbox (`Pausing` state), this call waits for it to complete and then
    /// returns the outcome rather than duplicating the work.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("pausing sandbox");
        // A capture holds `Snapshotting` for the length of a freeze. Speculative
        // checkpoints take it on their own schedule, so a user pause that
        // errored on it would fail for a reason the user did not cause and
        // cannot see. Wait it out and try again instead. Bounded, because
        // waiting forever on a sandbox that keeps being re-claimed is a hang
        // dressed up as patience.
        for attempt in 0..CAPTURE_WAIT_ATTEMPTS {
            match self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Pausing, &[SandboxState::Running])
                .await
            {
                Ok(_) => break,
                Err(err) => {
                    if self.waited_out_capture(sandbox_id, &err, attempt).await {
                        continue;
                    }
                    return self.report_pause_conflict(sandbox_id, err).await;
                }
            }
        }

        self.pause_sandbox_impl(sandbox_id).await
    }

    /// Whether a failed state claim should be retried, and waits if so.
    ///
    /// Speculative checkpoints take `Snapshotting` on their own schedule. A user
    /// operation that failed on it would fail for a reason the user did not
    /// cause, cannot see, and cannot act on -- the sandbox is `Running` as far
    /// as anything they can observe goes. Every other transitional state is
    /// someone's deliberate operation and is still reported as a conflict.
    async fn waited_out_capture(
        &self,
        sandbox_id: SandboxId,
        err: &StoreError,
        attempt: usize,
    ) -> bool {
        let StoreError::StateConflict { actual_state, .. } = err else {
            return false;
        };
        if *actual_state != SandboxState::Snapshotting || attempt + 1 >= CAPTURE_WAIT_ATTEMPTS {
            return false;
        }
        debug!("waiting for an in-flight capture before claiming the sandbox");
        self.wait_for_transition(sandbox_id, SandboxState::Snapshotting)
            .await
            .is_ok()
    }

    /// Turns a failed claim of the pause transition into the caller's answer.
    async fn report_pause_conflict(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        err: StoreError,
    ) -> Result<()> {
        let StoreError::StateConflict { actual_state, .. } = err else {
            return Err(OrchestratorError::from(err));
        };
        match actual_state {
            // Another task is already performing the pause. Wait for it to
            // finish and then report the final outcome.
            SandboxState::Pausing => self.join_concurrent_pause(sandbox_id).await,
            SandboxState::Paused => Ok(()),
            SandboxState::Killing => {
                info!("sandbox is being deleted while pausing");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            _ => {
                info!(state = ?actual_state, "cannot pause sandbox in current state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: actual_state,
                })
            }
        }
    }

    async fn pause_sandbox_impl(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        // Pin paused runtime artifacts before detaching from the running set.
        let runtime_artifacts = {
            let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
            match handle {
                Some(handle) => {
                    let sandbox = handle.lock().await;
                    sandbox.runtime_info().runtime_artifacts
                }
                None => RuntimeArtifactSet::empty(),
            }
        };
        if let Err(error) = self
            .protect_image_refs(
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                runtime_artifacts,
                "paused sandbox",
            )
            .await
        {
            warn!(error = %error, "failed to protect paused runtime artifacts; keeping sandbox Running");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Running, &[SandboxState::Pausing])
                .await;
            return Err(error);
        }

        // Allocate persistence space while the running handle and route are
        // still attached. Allocation does not mutate the backend, so failure
        // only needs to restore metadata and release the temporary image refs.
        let artifact_root = match self.persister.allocate_artifact_root(&sandbox_id).await {
            Ok(artifact_root) => artifact_root,
            Err(err) => {
                warn!(error = ?err, "failed to allocate paused sandbox artifact root");
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let (handle, removed_proxy_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        let Some(handle) = handle else {
            warn!("sandbox handle not found while pausing, removing from store");
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            self.store.remove(&sandbox_id).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Pause the sandbox and capture the paused state for resuming later.
        let paused_state_result = {
            let mut sandbox = handle.lock().await;
            sandbox.pause(artifact_root.as_deref()).await
        };

        // If pausing failed, attempt to put the sandbox back and return an error.
        let paused_state = match paused_state_result {
            Ok(s) => s,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                if err.is_terminal() {
                    // The handle was already detached from `self.sandboxes`
                    // before `pause()`. Do not reinsert it here: the live
                    // runtime may have been mutated and is no longer safe to
                    // keep serving as a running sandbox.
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal pause failure");
                    }
                    self.store.remove(&sandbox_id).await?;
                } else {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Pausing],
                        )
                        .await;
                }
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: err.into(),
                });
            }
        };

        let persisted_metadata = {
            let mut metadata = self
                .store
                .get(&sandbox_id)
                .await?
                .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
            metadata.state = SandboxState::Paused;
            metadata.paused_state = Some(paused_state.clone());
            metadata
        };
        if let Err(err) = self
            .persister
            .persist_paused(
                &persisted_metadata,
                artifact_root.as_deref(),
                paused_state.as_ref(),
            )
            .await
        {
            warn!(error = ?err, "failed to persist paused sandbox state");
            let resume_result = {
                let mut sandbox = handle.lock().await;
                sandbox.resume().await
            };
            if let Err(resume_err) = resume_result {
                warn!(error = ?resume_err, "failed to resume sandbox after pause failure");
                let stop_result = {
                    let mut sandbox = handle.lock().await;
                    sandbox.stop().await
                };
                if let Err(stop_err) = stop_result {
                    warn!(error = ?stop_err, "failed to stop sandbox after pause failure");
                }
                if let Err(error) = self.store.remove(&sandbox_id).await {
                    warn!(error = ?error, "failed to remove sandbox after pause failure");
                }
            } else {
                self.sandboxes.write().await.insert(sandbox_id, handle);
                self.restore_proxy_route(sandbox_id, removed_proxy_route)
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
            }
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to persist paused sandbox state: {err:#}"
            )));
        }
        let resources = persisted_metadata.resources;
        if let Some(mobility) = self.mobility() {
            // After the state is durable and before the sandbox is announced
            // as paused, so a drain that reads the record finds a sandbox that
            // really is paused. Best effort: the pause has already succeeded,
            // and failing it now would leave a running sandbox that cannot be
            // stopped over a bookkeeping problem.
            mobility.record_paused(&persisted_metadata).await;
        }
        self.store.update(persisted_metadata).await?;

        // Stop the sandbox to free up resources.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = ?err, "failed to stop sandbox after pausing");
        }
        self.publish_sandbox_event(SandboxLifecycleEventType::Pause, sandbox_id, resources);
        info!("sandbox paused");

        Ok(())
    }

    /// Resumes a paused sandbox from its snapshot.
    ///
    /// If another `resume_sandbox` call is already in progress (`Resuming`
    /// state), this call waits for the ongoing resume to finish and then
    /// returns the actual outcome (either `Running` or an error) rather than
    /// duplicating the work. On success the sandbox is ready for use when this
    /// method returns.
    pub async fn resume_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        // The fence is taken here rather than inside, and given back here too.
        // Every early return between taking a claim and launching would
        // otherwise leave the sandbox fenced against every other node, and
        // against a retry on this one, for a whole lease period — over a
        // failure that had nothing to do with ownership.
        //
        // Taking rather than reading: a destination that claims between a
        // check and the resume would leave two nodes running one sandbox.
        if let Some(mobility) = self.mobility() {
            // Timed as its own stage. A claim is a round trip to the scheduler
            // in a cluster, and a resume that is slow because of it looks
            // identical, from one timer around the whole call, to a resume
            // that is slow because the guest took a while to come back.
            // Refusals are recorded as the stage's error status, which is what
            // separates "nothing was granted" from "granting took a while".
            let claim = RESUME_STAGES
                .time("claim", async {
                    match mobility.claim_for_local_resume(&sandbox_id).await {
                        ResumeFence::Allowed => Ok(()),
                        refused => Err(refused),
                    }
                })
                .await;
            match claim {
                Ok(()) => {}
                Err(ResumeFence::Allowed) => unreachable!("a granted claim is not a refusal"),
                Err(ResumeFence::ClaimedElsewhere { by_node_id }) => {
                    return Err(OrchestratorError::SandboxHeldElsewhere {
                        sandbox_id,
                        reason: format!("{by_node_id} is taking it over"),
                    });
                }
                Err(ResumeFence::Evacuated { to_node_id }) => {
                    return Err(OrchestratorError::SandboxHeldElsewhere {
                        sandbox_id,
                        reason: format!("it now runs on {to_node_id}"),
                    });
                }
            }
        }

        let this = Arc::clone(self);
        let resumed = self
            .run_cancellation_safe("resume", sandbox_id, async move {
                this.resume_sandbox_inner(sandbox_id, timeout).await
            })
            .await;

        if resumed.is_err() {
            if let Some(mobility) = self.mobility() {
                mobility.release_local_claim(&sandbox_id).await;
            }
        }
        resumed
    }

    #[tracing::instrument(
        name = "resume_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id, timeout = ?timeout)
    )]
    async fn resume_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("resuming sandbox");
        let mut metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;

        // If another resume is in progress, wait for it to complete and
        // re-evaluate the resulting stable state.
        if metadata.state == SandboxState::Resuming {
            metadata = self
                .wait_for_transition(sandbox_id, SandboxState::Resuming)
                .await?;
        }

        match metadata.state {
            SandboxState::Killing => {
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            SandboxState::Running => {
                // Already running — just update the timeout if requested and return.
                return self.maybe_update_running_timeout(sandbox_id, timeout).await;
            }
            SandboxState::Paused => {}
            state => {
                return Err(OrchestratorError::InvalidSandboxState { sandbox_id, state });
            }
        }

        let node_mode = ConfigManager::global_config().virtualization_mode;
        if metadata.virtualization_mode != node_mode {
            return Err(OrchestratorError::VirtualizationModeMismatch {
                resource: format!("paused sandbox {sandbox_id}"),
                resource_mode: metadata.virtualization_mode,
                node_mode,
            });
        }

        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Resuming, &[SandboxState::Paused])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Running => {
                        // Another task already completed the resume.
                        self.maybe_update_running_timeout(sandbox_id, timeout).await
                    }
                    SandboxState::Resuming => {
                        // A second concurrent resume snuck in between our state
                        // read and CAS.  Wait for it and return the outcome.
                        self.join_concurrent_resume(sandbox_id, timeout).await
                    }
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while resuming");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot resume sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        if let Err(err) = self.persister.mark_resuming(&sandbox_id).await {
            warn!(error = ?err, "failed to mark persisted sandbox record as resuming");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Paused, &[SandboxState::Resuming])
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to mark persisted sandbox record as resuming: {err:#}"
            )));
        }

        let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
            warn!("missing paused state while resuming");
            OrchestratorError::InternalError("missing paused state".to_string())
        })?;

        let resumed = RESUME_STAGES
            .time(
                "launch",
                self.launch_sandbox(LaunchPlan::for_resume(
                    sandbox_id,
                    Arc::clone(paused_state),
                    timeout,
                    metadata.resources,
                    metadata
                        .secure
                        .then(|| self.access_tokens.generate(metadata.id)),
                )),
            )
            .await;
        // `resume_sandbox` owns the claim from before the first read to after
        // the last, so a failure here needs nothing: every exit is covered
        // there rather than only the ones that reach this point.
        if let Ok(metadata) = resumed.as_ref() {
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            if let Some(mobility) = self.mobility() {
                // Running here again, so it is no longer a migration
                // candidate. Dropping the record also releases the claim this
                // node took to fence the resume.
                mobility.forget(&sandbox_id).await;
            }
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Resume,
                metadata.id,
                metadata.resources,
            );
        }
        resumed
    }

    /// Captures a snapshot of a running sandbox.
    /// Describes a paused sandbox's artifacts so they can be published.
    ///
    /// A paused sandbox already holds everything a committed snapshot needs;
    /// what it lacks is a repository entry, which is exactly why it cannot
    /// move. This is the read that lets one be created.
    ///
    /// Nothing is mutated and no state transition happens: the artifacts are
    /// copied or hard-linked by the repository's import, so the sandbox stays
    /// resumable here whether or not the publish succeeds. That is what makes
    /// it safe to offer on a sandbox someone is about to resume.
    pub async fn describe_paused_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(SandboxMetadata, FirecrackerSnapshotManifest)> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Paused {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
            OrchestratorError::InternalError(format!(
                "sandbox {sandbox_id} is paused but carries no paused state to publish"
            ))
        })?;
        let firecracker = paused_state
            .downcast_ref::<FirecrackerPausedState>()
            .ok_or_else(|| {
                OrchestratorError::InternalError(format!(
                    "sandbox {sandbox_id} was paused by a backend that cannot publish paused state"
                ))
            })?;

        let manifest = firecracker
            .snapshot_config()
            .to_publishable_manifest()
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "sandbox {sandbox_id} cannot be published: {error:#}"
                ))
            })?;
        Ok((metadata, manifest))
    }

    /// Records that a paused sandbox's state now lives in the repository, so a
    /// destination can restore it.
    ///
    /// Until this, the record says the sandbox cannot move — truthfully, since
    /// its artifacts were node-local files.
    pub async fn mark_paused_snapshot_committed(
        &self,
        sandbox_id: SandboxId,
        snapshot_id: &crate::snapshot::SnapshotId,
    ) {
        if let Some(mobility) = self.mobility() {
            mobility.record_committed(&sandbox_id, snapshot_id).await;
        }
    }

    pub async fn capture_snapshot(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "capture_snapshot",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn capture_snapshot_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("capturing sandbox snapshot");
        for attempt in 0..CAPTURE_WAIT_ATTEMPTS {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Snapshotting,
                    &[SandboxState::Running],
                )
                .await
            {
                Ok(_) => break,
                Err(err) => {
                    if self.waited_out_capture(sandbox_id, &err, attempt).await {
                        continue;
                    }
                    return match err {
                        StoreError::StateConflict {
                            actual_state: SandboxState::Killing,
                            ..
                        } => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
                        StoreError::StateConflict { actual_state, .. } => {
                            Err(OrchestratorError::InvalidSandboxState {
                                sandbox_id,
                                state: actual_state,
                            })
                        }
                        err => Err(OrchestratorError::from(err)),
                    };
                }
            }
        }

        // Get the sandbox handle.
        let handle = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        };
        let Some(handle) = handle else {
            warn!("sandbox handle not found while snapshotting, removing from store");
            self.detach_sandbox_handle_and_route(&sandbox_id).await;
            self.store.remove(&sandbox_id).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Call sandbox backend to capture the snapshot.
        let captured_snapshot_result = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot().await
        };

        // If snapshot capture failed, attempt to roll back to Running state and return an error.
        let captured_snapshot = match captured_snapshot_result {
            Ok(captured_snapshot) => captured_snapshot,
            Err(err) => {
                warn!(error = ?err, "failed to capture sandbox snapshot");
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal snapshot failure");
                    }
                    self.store.remove(&sandbox_id).await?;
                } else {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Snapshotting],
                        )
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Snapshot,
                    source: err.into(),
                });
            }
        };

        // Update the sandbox state back to Running and return the captured snapshot along with the latest metadata.
        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Running,
                &[SandboxState::Snapshotting],
            )
            .await?;
        let metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => {
                warn!("sandbox disappeared after snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
        };

        info!("snapshot captured");
        Ok(SnapshotCaptureResult {
            metadata,
            captured_snapshot,
        })
    }

    pub async fn replace_sandbox_network_policy(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("update_network", sandbox_id, async move {
            this.replace_sandbox_network_policy_inner(sandbox_id, network_policy)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "replace_sandbox_network_policy",
        skip(self, network_policy),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn replace_sandbox_network_policy_inner(
        &self,
        sandbox_id: SandboxId,
        mut network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }
        network_policy.allow_public_traffic = metadata.network_policy.allow_public_traffic;

        let sandbox = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        }
        .ok_or_else(|| OrchestratorError::SandboxOperationConflict {
            sandbox_id,
            operation: SandboxOperation::UpdateNetwork,
        })?;

        let runtime_policy = network_policy.runtime_policy();

        let update_result = {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_network_policy(runtime_policy).await
        };
        update_result.map_err(|source| OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::UpdateNetwork,
            source,
        })?;

        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.network_policy = network_policy;
            })
            .await?;

        Ok(())
    }

    /// Patch the custom extension params of a running sandbox.
    ///
    /// The patch document is passed through verbatim to the custom
    /// extension's patch-params hook, which returns the updated full params.
    /// On hook failure the sandbox keeps its previous params and the
    /// metadata store is left untouched. Returns the new full params (`None`
    /// means empty params).
    pub async fn patch_sandbox_custom_extension_params(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("patch_custom_extension_params", sandbox_id, async move {
            this.patch_sandbox_custom_extension_params_inner(sandbox_id, patch)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "patch_sandbox_custom_extension_params",
        skip(self, patch),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn patch_sandbox_custom_extension_params_inner(
        &self,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        let sandbox = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        }
        .ok_or_else(|| OrchestratorError::SandboxOperationConflict {
            sandbox_id,
            operation: SandboxOperation::PatchCustomExtensionParams,
        })?;

        // Invoke the extension's patch-params hook here (the backend only
        // stores the approved value). The sandbox lock is not held during
        // the hook call so pause/stop are not blocked on extension latency.
        let client = CustomExtensionClient::global().ok_or_else(|| {
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source: anyhow::anyhow!(
                    "custom extension is not configured ([custom_extension].url is unset)"
                ),
            }
        })?;
        let new_params = client
            .hook_patch_params(sandbox_id, patch)
            .await
            .map_err(|source| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source,
            })?;

        {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_custom_extension_params(new_params.clone());
        }

        // NOTE: a concurrent pause may have transitioned the sandbox since the entry check,
        // so this may fail. But it's acceptable since extension state should be transient like network policy
        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.custom_extension_params = new_params.clone();
            })
            .await
            .map_err(|err| match err {
                // Lost a race against a concurrent state transition (e.g.
                // pause): report it as a conflict instead of a 500.
                StoreError::StateConflict {
                    sandbox_id,
                    actual_state,
                    ..
                } => OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: actual_state,
                },
                other => OrchestratorError::from(other),
            })?;

        Ok(new_params)
    }

    /// Returns the current orchestrator metrics snapshot.
    ///
    /// Counter fields are read atomically; resource fields are aggregated by
    /// scanning the metadata store, so the returned snapshot is always
    /// consistent with the orchestrator's current set of sandboxes.
    pub async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
        let mut metrics = OrchestratorMetrics::default();
        self.store
            .list_with_callback(|metadata| {
                aggregate_resource_metrics(
                    &mut metrics,
                    SandboxContribution::new(metadata.state, metadata.resources),
                );
            })
            .await?;
        metrics.create_successes = self.counters.create_successes();
        metrics.create_fails = self.counters.create_fails();
        if let Some(mobility) = self.mobility() {
            // Sampled here rather than on a timer, so the gauges move on the
            // same cadence as everything else on the node snapshot and cannot
            // drift from the counts they are meant to describe.
            mobility.publish_metrics().await;
        }
        Ok(metrics)
    }

    pub fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        self.sandbox_event_tx.subscribe()
    }

    fn publish_sandbox_event(
        &self,
        event_type: SandboxLifecycleEventType,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) {
        let event = SandboxLifecycleEvent {
            event_type,
            sandbox_id,
            resources,
        };
        let _ = self.sandbox_event_tx.send(event);
    }

    /// Waits for `sandbox_id` to leave `transitional_state`, then returns the
    /// resulting metadata. Returns `SandboxNotFound` if the sandbox is removed
    /// while waiting, or `InvalidSandboxState` if the sandbox is still in the
    /// transitional state after the [`WAIT_TRANSITION_TIMEOUT`] elapses.
    async fn wait_for_transition(
        &self,
        sandbox_id: SandboxId,
        transitional_state: SandboxState,
    ) -> Result<SandboxMetadata> {
        let states = [transitional_state];
        let wait = self.store.wait_while_in_states(&sandbox_id, &states);
        match tokio::time::timeout(WAIT_TRANSITION_TIMEOUT, wait).await {
            Ok(Ok(Some(m))) => Ok(m),
            Ok(Ok(None)) => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
            Ok(Err(e)) => Err(OrchestratorError::from(e)),
            Err(_elapsed) => {
                warn!(
                    sandbox_id = %sandbox_id,
                    state = ?transitional_state,
                    "timed out waiting for sandbox to leave transitional state"
                );
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: transitional_state,
                })
            }
        }
    }

    /// Applies `timeout` to `metadata` and persists the change if the sandbox
    /// is still `Running`. Returns the updated metadata. If `timeout` is `None`,
    /// the timeout will be cleared, which indicates no expiration.
    async fn maybe_update_running_timeout(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.update_timeout(timeout);
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "cannot update timeout for sandbox in current state");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        Ok(update_result.current)
    }

    /// Joins a concurrent pause already in progress for the same sandbox.
    /// Waits for the `Pausing` state to resolve and maps the final state to
    /// the appropriate `Ok(())` / `Err(...)` result.
    async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<()> {
        debug!("concurrent pause in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Pausing)
            .await?;
        match m.state {
            SandboxState::Paused => {
                debug!("concurrent pause succeeded");
                Ok(())
            }
            SandboxState::Running => {
                info!("concurrent pause failed; sandbox returned to running state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Running,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted after concurrent pause attempt");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            other => {
                info!(state = ?other, "unexpected state after waiting for concurrent pause");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: other,
                })
            }
        }
    }

    /// Joins a concurrent resume already in progress for the same sandbox.
    /// Waits for the `Resuming` state to resolve, then applies `timeout` if
    /// the sandbox reached `Running`, and returns the final metadata.
    async fn join_concurrent_resume(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        debug!("concurrent resume in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Resuming)
            .await?;
        match m.state {
            SandboxState::Running => self.maybe_update_running_timeout(sandbox_id, timeout).await,
            SandboxState::Paused => {
                info!("concurrent resume failed; sandbox returned to paused state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Paused,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted while resuming");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            state => {
                info!(state = ?state, "unexpected state after waiting for concurrent resume");
                Err(OrchestratorError::InvalidSandboxState { sandbox_id, state })
            }
        }
    }

    /// Automatically pauses or stops sandboxes whose timeout has expired.
    async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because orchestrator is shutting down");
            return Ok(Vec::new());
        }

        let eviction_cutoff = SystemTime::now();
        let expired = self.store.list_expired(eviction_cutoff).await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            let claimed_state = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => SandboxState::Pausing,
                SandboxTimeoutAction::Delete => SandboxState::Killing,
            };
            let result = match self
                .claim_expired_running_sandbox(metadata.id, eviction_cutoff, claimed_state)
                .await
            {
                Ok(true) => match metadata.timeout_action {
                    SandboxTimeoutAction::Pause => self.pause_sandbox_impl(metadata.id).await,
                    SandboxTimeoutAction::Delete => {
                        self.delete_sandbox_impl(metadata.id, SandboxState::Running)
                            .await
                    }
                }
                .map(|_| true),
                Ok(false) => Ok(false),
                Err(err) => Err(err),
            };
            match result {
                Ok(true) => evicted_ids.push(metadata.id),
                Ok(false) => continue,
                Err(err) => warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                ),
            }
        }

        Ok(evicted_ids)
    }

    /// Starts a background task that periodically evicts expired sandboxes.
    /// The eviction policy is defined by the sandbox's [`timeout_action`](SandboxMetadata::timeout_action).
    fn start_auto_evict_task(
        this: Arc<Self>,
        evict_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("auto-evict task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(evict_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("auto-evict task started with interval {:?}", evict_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("auto-evict task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("auto-evict task stopping because orchestrator was dropped");
                            break;
                        };
                        if let Err(err) = this.evict_expired_sandboxes().await {
                            warn!("auto-evict task failed: {err}");
                        }
                    }
                }
            }
        });
    }

    /// Starts a background task that periodically runs local image maintenance
    /// (capacity eviction + fail-closed GC) over the current running set.
    fn start_local_image_maintenance_task(
        this: Arc<Self>,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("local image maintenance task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            info!(interval = ?interval, "local image maintenance task started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("local image maintenance task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("local image maintenance task stopping because orchestrator was dropped");
                            break;
                        };

                        let running = this.collect_running_artifacts().await;
                        if let Err(err) = this.image_refs.maintain_running(running).await {
                            warn!("local image maintenance pass failed: {err:#}");
                        }
                    }
                }
            }
        });
    }

    /// Starts the background loop that keeps each sandbox's guest memory sized
    /// to what it is actually using.
    ///
    /// The `enabled` switch is honoured here rather than at the call site so
    /// that whether the loop runs is decided in the same place as what it
    /// does when it runs.
    fn start_memory_control_task(
        this: Arc<Self>,
        config: MemoryControlConfig,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        if !config.enabled {
            return;
        }
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("memory control task not started: no Tokio runtime available");
            return;
        };

        let interval = Duration::from_secs(config.interval_secs.max(1));
        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // Built once and reused: the collector's first sample pays a
            // blocking window to produce a real CPU figure, and a fresh
            // collector per pass would pay it every pass.
            let host_metrics = crate::observability::HostMetricsCollector::new();
            // Per-sandbox history lives with the loop, not in the metadata
            // store: it is derived, node-local, and meaningless after a
            // restart, and a periodic writer into the store would contend
            // with the sandbox state machine for nothing.
            let mut states: HashMap<SandboxId, MemoryControlState> = HashMap::new();
            info!(interval = ?interval, "memory control task started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("memory control task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("memory control task stopping because orchestrator was dropped");
                            break;
                        };

                        let mut guard =
                            MetricGuard::stage(MEMORY_CONTROL_PASS_METRIC, "pass");
                        let outcome = this
                            .memory_control_pass(&config, &host_metrics, &mut states)
                            .await;
                        guard.finish(&outcome);
                        match outcome {
                            Ok(report) => debug!(?report, "memory control pass complete"),
                            Err(err) => warn!("memory control pass failed: {err:#}"),
                        }
                    }
                }
            }
        });
    }

    async fn memory_control_pass(
        &self,
        config: &MemoryControlConfig,
        host_metrics: &crate::observability::HostMetricsCollector,
        states: &mut HashMap<SandboxId, MemoryControlState>,
    ) -> Result<MemoryControlPassReport> {
        // `collect` samples CPU with a blocking window on its first call, so it
        // must not run on a Tokio worker.
        let host_metrics = host_metrics.clone();
        let host = tokio::task::spawn_blocking(move || host_metrics.collect())
            .await
            .context("collect host metrics for the memory control pass")?;
        let node_under_pressure = host.memory_total_bytes > 0
            && host.memory_used_bytes.saturating_mul(100) / host.memory_total_bytes
                >= u64::from(config.node_memory_high_percent);

        let mut roster = Vec::new();
        self.store
            .list_with_callback(|metadata| {
                roster.push((metadata.id, metadata.state, metadata.resources.memory_mib));
            })
            .await?;

        let handles = self.sandboxes.read().await;
        let sandboxes = roster
            .into_iter()
            .filter_map(|(id, state, ceiling_mib)| {
                handles
                    .get(&id)
                    .cloned()
                    .map(|handle| MemoryControlSandbox {
                        id,
                        state,
                        ceiling_mib,
                        handle,
                    })
            })
            .collect();
        drop(handles);

        Ok(run_memory_control_pass(sandboxes, node_under_pressure, config, states).await)
    }

    #[tracing::instrument(skip(self, plan))]
    async fn launch_sandbox(self: &Arc<Self>, plan: LaunchPlan) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        let sandbox_id = plan.sandbox_id();
        let transitional_state = plan.transitional_state();

        // Build and start the sandbox first, before making any state changes, so that we don't
        // have to roll back any persisted state if the build fails.
        // Meanwhile, the start process can be overlapped with the initial state persistence.
        let mut sandbox = match self.build_sandbox(&plan) {
            Ok(sandbox) => sandbox,
            Err(err) => {
                self.rollback_failed_launch_metadata(&plan, transitional_state)
                    .await;
                return Err(err);
            }
        };

        // Protect artifacts before the backend opens them.
        let startup_artifacts = sandbox.startup_artifacts();
        if let Err(err) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                startup_artifacts,
                "starting sandbox",
            )
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to protect starting runtime artifacts");
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(err);
        }
        if let Err(source) = sandbox.start_nowait().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            if let Err(stop_err) = sandbox.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after start failure");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }
        debug!("sandbox start requested");

        // If the orchestrator started shutting down, stop here before we persist any state.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down just after starting the sandbox");
            if let Err(err) = sandbox.stop().await {
                warn!(error = %format_args!("{err:#}"), "failed to stop sandbox");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_resources =
            resources_with_runtime_info(plan.resources(), sandbox.runtime_info());
        let transitional_metadata = plan.transitional_metadata().map(|metadata| {
            let mut metadata = metadata.clone();
            metadata.resources = runtime_resources;
            metadata
        });

        // Store the sandbox handle in memory.
        let handle = Arc::new(Mutex::new(sandbox));
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());

        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;

        // Persist the sandbox metadata if needed (during creation).
        if let Some(metadata) = transitional_metadata.as_ref() {
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::Registered)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        }

        // Check for shutdown again before we wait for the sandbox to become ready.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down before sandbox became ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        // Wait for the sandbox to be ready
        let wait_result = {
            let sandbox = handle.lock().await;
            sandbox.wait_for_ready().await
        };
        if let Err(source) = wait_result {
            warn!(error = %format_args!("{source:#}"), "sandbox failed to become ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::WaitReady,
                source,
            });
        }

        // Check for shutdown again before we persist the final state and publish the proxy route.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down while sandbox was becoming ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let launch_timeout = plan.timeout();
        let final_metadata = match self
            .store
            .update_if_state(
                &sandbox_id,
                std::slice::from_ref(&transitional_state),
                move |metadata| {
                    metadata.resources = runtime_resources;
                    metadata.state = SandboxState::Running;
                    metadata.update_timeout(launch_timeout);
                },
            )
            .await
        {
            Ok(update) => update.current,
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), "failed to persist final sandbox metadata after launch");
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let proxy_target = {
            let sandbox = handle.lock().await;
            match Self::proxy_target_from_sandbox(sandbox.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "sandbox became ready without a proxy target; rolling back launch");
                    drop(sandbox);
                    self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::RunningPersisted)
                        .await;
                    return Err(err);
                }
            }
        };
        if !self
            .upsert_proxy_route_if_current_handle(sandbox_id, &handle, proxy_target)
            .await
        {
            debug!("skipping runtime proxy route publication because sandbox handle is stale");
        }

        if matches!(plan, LaunchPlan::Resume(_)) {
            if let Err(err) = self.persister.delete_record(&sandbox_id).await {
                warn!(error = %format_args!("{err:#}"), "failed to delete persisted sandbox record after resume");
            }
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    fn build_sandbox(&self, plan: &LaunchPlan) -> Result<Box<dyn SandboxBackend>> {
        let build_result = match plan {
            LaunchPlan::Create(plan) => match &plan.source {
                CreateLaunchSource::Snapshot { snapshot } => self
                    .factory
                    .build_from_snapshot(snapshot, plan.launch_config.clone()),
                CreateLaunchSource::Fresh { build_spec } => self
                    .factory
                    .build((**build_spec).clone(), plan.launch_config.clone()),
            },
            LaunchPlan::Resume(plan) => self.factory.build_from_paused_state(
                plan.sandbox_id,
                plan.paused_state.as_ref(),
                plan.envd_access_token.clone(),
            ),
        };
        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id(),
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    async fn cleanup_failed_launch(
        &self,
        plan: &LaunchPlan,
        handle: SandboxHandle,
        stage: FailedLaunchStage,
    ) {
        let should_rollback_shared_state = self
            .detach_launch_runtime_if_current(
                &plan.sandbox_id(),
                &handle,
                stage.should_detach_proxy_route(),
                stage,
            )
            .await;

        // Stop the sandbox.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = %format_args!("{err:#}"), "failed to stop sandbox while rolling back launch");
        }

        if !should_rollback_shared_state {
            return;
        }

        if let Some(expected_state) = stage.rollback_expected_state(plan) {
            self.rollback_failed_launch_metadata(plan, expected_state)
                .await;
        }
    }

    async fn rollback_failed_launch_metadata(
        &self,
        plan: &LaunchPlan,
        expected_state: SandboxState,
    ) {
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(plan.sandbox_id()))
            .await;
        match plan {
            LaunchPlan::Create(_) => {
                if let Err(err) = self.store.remove(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to remove sandbox metadata during launch rollback");
                }
            }
            LaunchPlan::Resume(_) => {
                if let Err(err) = self
                    .store
                    .update_state_if_state(
                        &plan.sandbox_id(),
                        SandboxState::Paused,
                        std::slice::from_ref(&expected_state),
                    )
                    .await
                {
                    warn!(error = %format_args!("{err:#}"), "failed to restore sandbox metadata during launch rollback");
                }
                if let Err(err) = self.persister.rollback_resuming(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to restore persisted sandbox record lifecycle during launch rollback");
                }
            }
        }
    }

    async fn detach_launch_runtime_if_current(
        &self,
        sandbox_id: &SandboxId,
        handle: &SandboxHandle,
        detach_proxy_route: bool,
        stage: FailedLaunchStage,
    ) -> bool {
        let mut sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(sandbox_id) else {
            return true;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            warn!(
                stage = ?stage,
                "sandbox handle was replaced during failed launch cleanup; skipping shared state rollback"
            );
            return false;
        }

        sandboxes.remove(sandbox_id);

        if detach_proxy_route {
            let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
            if let Some(route) = removed_route.as_ref() {
                debug!(version = route.version(), "removed runtime proxy route");
            }
        }

        drop(sandboxes);
        true
    }

    fn proxy_target_from_sandbox(sandbox: &dyn SandboxBackend) -> Result<ProxyTarget> {
        sandbox
            .host_interaction_ip()
            .map(ProxyTarget::new)
            .ok_or_else(|| {
                warn!("sandbox started without an interaction IP");
                OrchestratorError::InternalError(
                    "sandbox missing host interaction IP after start".to_string(),
                )
            })
    }

    async fn upsert_proxy_route(&self, sandbox_id: SandboxId, target: ProxyTarget) {
        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route = self
            .proxy_routes
            .write()
            .await
            .upsert(sandbox_id, target, version);
        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
    }

    async fn upsert_proxy_route_if_current_handle(
        &self,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
        target: ProxyTarget,
    ) -> bool {
        // Keep the lock order aligned with detach_sandbox_handle_and_route:
        // sandboxes first, then proxy_routes.
        let sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(&sandbox_id) else {
            return false;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            return false;
        }

        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route = self
            .proxy_routes
            .write()
            .await
            .upsert(sandbox_id, target, version);
        drop(sandboxes);

        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
        true
    }

    async fn restore_proxy_route(&self, sandbox_id: SandboxId, route: Option<ProxyRoute>) {
        let Some(route) = route else {
            return;
        };
        self.upsert_proxy_route(sandbox_id, route.target().clone())
            .await;
    }

    async fn detach_sandbox_handle_and_route(
        &self,
        sandbox_id: &SandboxId,
    ) -> (Option<SandboxHandle>, Option<ProxyRoute>) {
        // Keep the lock order aligned with upsert_proxy_route_if_current_handle:
        // sandboxes first, then proxy_routes.
        let mut sandboxes = self.sandboxes.write().await;
        let handle = sandboxes.remove(sandbox_id);

        let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
        if let Some(route) = removed_route.as_ref() {
            debug!(version = route.version(), "removed runtime proxy route");
        }

        drop(sandboxes);
        (handle, removed_route)
    }

    async fn run_shutdown_cleanup(self: &Arc<Self>) -> Result<()> {
        const MAX_SHUTDOWN_PASSES: usize = 3;
        let mut last_failures = Vec::new();

        // Preserve recoverable sandboxes by pausing running VMs before process exit.
        for pass in 1..=MAX_SHUTDOWN_PASSES {
            let sandboxes = self
                .store
                .list_filtered(SandboxListFilter {
                    excluded_states: Some(vec![SandboxState::Paused]),
                    ..SandboxListFilter::matches_all()
                })
                .await?;
            if sandboxes.is_empty() {
                break;
            }
            last_failures.clear();

            info!(
                pass,
                remaining = sandboxes.len(),
                "preserving sandboxes during shutdown"
            );

            for metadata in sandboxes {
                let sandbox_id = metadata.id;
                match metadata.state {
                    SandboxState::Paused => {
                        unreachable!("paused sandboxes should have been filtered out")
                    }
                    SandboxState::Running => {
                        if let Err(err) = self.pause_sandbox_inner(sandbox_id).await {
                            last_failures.push(format!("{sandbox_id}: {err}"));
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming
                    | SandboxState::Killing => {
                        match self.wait_for_transition(sandbox_id, metadata.state).await {
                            Ok(_) | Err(OrchestratorError::SandboxNotFound(_)) => {}
                            Err(err) => {
                                warn!(
                                    sandbox_id = %sandbox_id,
                                    error = ?err,
                                    pass,
                                    "failed to wait for sandbox transition during orchestrator shutdown"
                                );
                                last_failures.push(format!("{sandbox_id}: {err}"));
                            }
                        }
                    }
                }
            }

            if last_failures.is_empty() {
                continue;
            }

            warn!(
                pass,
                failures = last_failures.len(),
                max_passes = MAX_SHUTDOWN_PASSES,
                "shutdown preservation pass completed with failures"
            );
        }

        if !last_failures.is_empty() {
            return Err(OrchestratorError::InternalError(format!(
                "failed to preserve all sandboxes during shutdown after {MAX_SHUTDOWN_PASSES} passes: {}",
                last_failures.join(", ")
            )));
        }

        // Clean up remaining network resources.
        if let Some(manager) = crate::sandbox::NetworkManager::global_if_initialized() {
            if let Err(err) = manager.shutdown() {
                warn!(error = ?err, "failed to clean up network resources during orchestrator shutdown");
            }
        }

        info!("orchestrator shutdown completed");
        Ok(())
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    fn ensure_accepting_lifecycle_operations(&self) -> Result<()> {
        if self.is_shutting_down() {
            info!("rejecting lifecycle operation because orchestrator is shutting down");
            return Err(OrchestratorError::ShuttingDown);
        }

        Ok(())
    }
}

fn persisted_sandboxes_require_managed_seed(persisted: &[SandboxMetadata]) -> bool {
    persisted
        .iter()
        .any(|metadata| metadata.secure || !metadata.network_policy.allow_public_traffic)
}

#[cfg(test)]
impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    pub(crate) async fn set_proxy_target_for_test(
        &self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        state: SandboxState,
    ) {
        self.set_metadata_state_for_test(sandbox_id, state)
            .await
            .expect("seed proxy metadata state for test");

        if state == SandboxState::Running {
            self.upsert_proxy_route(sandbox_id, target).await;
        } else {
            let _ = self.proxy_routes.write().await.remove(&sandbox_id);
        }
    }

    pub(crate) async fn set_metadata_state_for_test(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
    ) -> Result<()> {
        let existing = self.store.get(&sandbox_id).await?;
        match existing {
            Some(mut metadata) => {
                metadata.state = state;
                self.store.update(metadata).await?;
            }
            None => {
                let metadata = SandboxMetadata {
                    id: sandbox_id,
                    state,
                    ..Default::default()
                };
                self.store.add(metadata.clone()).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn set_auto_resume_for_test(
        &self,
        sandbox_id: &SandboxId,
        auto_resume_enabled: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };

        metadata.auto_resume = auto_resume_enabled;
        self.store.update(metadata).await?;

        Ok(())
    }

    pub(crate) async fn set_secure_for_test(
        &self,
        sandbox_id: &SandboxId,
        secure: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.secure = secure;
        self.store.update(metadata).await?;
        Ok(())
    }

    pub(crate) async fn set_allow_public_traffic_for_test(
        &self,
        sandbox_id: &SandboxId,
        allow_public_traffic: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.network_policy.allow_public_traffic = allow_public_traffic;
        self.store.update(metadata).await?;
        Ok(())
    }

    pub(crate) async fn remove_proxy_route_for_test(&self, sandbox_id: &SandboxId) {
        let _ = self.proxy_routes.write().await.remove(sandbox_id);
    }
}

pub(super) fn default_fresh_sandbox_resources() -> SandboxResources {
    let config = ConfigManager::global_config();
    SandboxResources {
        cpu_count: config.machine.vcpu_count,
        memory_mib: config.machine.mem_size_mib,
        // Filled from backend runtime info after the rootfs device is created.
        disk_size_mib: 0,
    }
}

fn resources_with_runtime_info(
    mut resources: SandboxResources,
    runtime_info: SandboxRuntimeInfo,
) -> SandboxResources {
    // This API resource field tracks the rootfs block device size. Attached
    // drives are separately configured storage and are not folded into it.
    if let Some(size) = runtime_info.rootfs_virtual_size {
        resources.disk_size_mib = bytes_to_mib_ceil(size);
    }
    resources
}

fn configured_runtime_versions() -> SnapshotRuntimeVersions {
    let config = ConfigManager::global_config();
    SnapshotRuntimeVersions::new(
        config
            .kernel
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config
            .firecracker
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config.envd.version.clone(),
        config.resolved_tools_version().to_string(),
    )
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "drain_tests.rs"]
mod drain_tests;

#[cfg(test)]
#[path = "resume_stage_tests.rs"]
mod resume_stage_tests;
