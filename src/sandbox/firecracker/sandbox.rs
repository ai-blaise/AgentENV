use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use firecracker_client::models::drive::IoEngine;
use nix::libc;
use tempfile::TempDir;
use tracing::{debug, trace, warn};
use uuid::Uuid;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use super::config::{
    create_firecracker_work_dir, FirecrackerCommonConfig, FirecrackerRuntimePolicy,
    FirecrackerSandboxConfig, FirecrackerSnapshotConfig, PersistentSnapshotRootGuard,
    MAX_EXTRA_DRIVES,
};
use super::manifest::FirecrackerSnapshotManifest;
use super::mmds::MmdsMetadata;
use super::overlaybd_snapshot::{
    build_mem_snapshot_image_config, convert_dirty_memory_to_overlaybd,
    restack_snapshot_overlaybd_device, restack_snapshot_overlaybd_rootfs,
};
use super::pool::{warm_stderr_path, warm_stdout_path, FirecrackerPool};
use super::FirecrackerInstance;
use crate::sandbox::custom_extension::{
    CustomExtensionClient, CustomExtensionHookGuard, CustomExtensionParams,
};

use crate::cfg::{BalloonConfig, ConfigManager};
use crate::observability::prometheus::MetricGuard;
use crate::sandbox::access::EnvdAccessToken;
use crate::sandbox::backend::{
    CapturedSandboxSnapshot, CheckpointStats, MemoryControlCapability, PausedSandboxState,
    RuntimeArtifactSet, SandboxBackend, SandboxCaptureError, SandboxCaptureResult, SandboxExecutor,
    SandboxForkResult, SandboxForkSpec, SandboxMemoryTelemetry, SandboxRuntimeInfo,
};
use crate::sandbox::envd::EnvdInstance;
use crate::sandbox::extra_drive::{
    prepare_extra_drives, DriveMount, ExtraDrive, ExtraDrivePrepareMode, ROOTFS_DRIVE_ID,
    USER_ROOTFS_DRIVE_ID,
};
use crate::sandbox::network::{NetworkManager, SandboxNetworkPolicy, Slot};
use crate::sandbox::process::Executor;
use crate::sandbox::ublk::{
    OverlaybdCompactOutput, OverlaybdConfig, OverlaybdRuntimeHandle, SharedMemDevice, UblkBackend,
    UblkCreateSpec, UblkDeviceManager,
};
use crate::sandbox::SandboxLaunchConfig;
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

// ── Constants ────────────────────────────────────────────────────────────────

const VM_STATE_FILE_NAME: &str = "vm_state.bin";
const ROOTFS_DRIVE_PATH: &str = "rootfs.ext4";
const USER_ROOTFS_DRIVE_PATH: &str = "user-rootfs";

/// Firecracker's `TokenBucket::size` is the number of tokens replenished every
/// `refill_time`, not a per-second rate. Pinning the refill period to 1000 ms
/// makes the configured `*_per_sec` values equal the sustained per-second rate.
const RATE_LIMIT_REFILL_TIME_MS: i64 = 1000;

const MEMORY_HINTING_DURATION_METRIC: &str = "agentenv_memory_snapshot_hinting_duration_seconds";

/// How often the free-page-hinting handshake is re-read. The guest answers on
/// its own schedule, so this trades a little added pause latency against
/// hammering the API socket for the whole of a run.
const FREE_PAGE_HINTING_POLL_INTERVAL: Duration = Duration::from_millis(20);

fn bandwidth_bucket(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::TokenBucket>>> {
    if cfg.bandwidth_bytes_per_sec == 0 {
        return Ok(None);
    }
    let size = i64::try_from(cfg.bandwidth_bytes_per_sec)
        .context("disk bandwidth_bytes_per_sec exceeds Firecracker's i64 range")?;
    let mut bw = firecracker_client::models::TokenBucket::new(RATE_LIMIT_REFILL_TIME_MS, size);
    if cfg.bandwidth_burst_bytes > 0 {
        bw.one_time_burst = Some(
            i64::try_from(cfg.bandwidth_burst_bytes)
                .context("disk bandwidth_burst_bytes exceeds Firecracker's i64 range")?,
        );
    }
    Ok(Some(Box::new(bw)))
}

fn ops_bucket(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::TokenBucket>>> {
    if cfg.iops == 0 {
        return Ok(None);
    }
    let size = i64::try_from(cfg.iops).context("disk iops exceeds Firecracker's i64 range")?;
    let mut ops = firecracker_client::models::TokenBucket::new(RATE_LIMIT_REFILL_TIME_MS, size);
    if cfg.iops_burst > 0 {
        ops.one_time_burst = Some(
            i64::try_from(cfg.iops_burst)
                .context("disk iops_burst exceeds Firecracker's i64 range")?,
        );
    }
    Ok(Some(Box::new(ops)))
}

/// Build the limiter attached to the user rootfs drive at fresh boot (pre-boot
/// `PUT /drives`). Returns `None` when limiting is disabled or no dimension is
/// configured, in which case the drive is added with no limiter.
fn build_disk_rate_limiter(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Option<Box<firecracker_client::models::RateLimiter>>> {
    if !cfg.enabled {
        return Ok(None);
    }
    let bandwidth = bandwidth_bucket(cfg)?;
    let ops = ops_bucket(cfg)?;
    if bandwidth.is_none() && ops.is_none() {
        return Ok(None);
    }
    let mut rl = firecracker_client::models::RateLimiter::new();
    rl.bandwidth = bandwidth;
    rl.ops = ops;
    Ok(Some(Box::new(rl)))
}

/// A token bucket Firecracker interprets as "disable this dimension".
///
/// Firecracker's `PATCH /drives` maps an *absent* token bucket to
/// `BucketUpdate::None` (leave unchanged), so a snapshot-inherited limit cannot
/// be removed by omission. The explicit disable sentinel is a bucket with both
/// `size == 0` and `refill_time == 0`; a mixed bucket (e.g. `size == 0`,
/// `refill_time == 1`) is not the sentinel and can be rejected as an invalid
/// token bucket, failing the resume PATCH. Send both fields as zero.
fn disabled_bucket() -> Box<firecracker_client::models::TokenBucket> {
    Box::new(firecracker_client::models::TokenBucket::new(0, 0))
}

/// Build the limiter to PATCH on resume, reconciling a snapshot-inherited
/// limiter against the node's current config. BOTH buckets are always present:
/// a configured dimension uses its own bucket, an unset dimension is overwritten
/// with a disabled (`size == 0`) bucket so any inherited limit on that dimension
/// is cleared (an omitted bucket would instead be left unchanged; see
/// [`disabled_bucket`]).
fn reconcile_disk_rate_limiter(
    cfg: &crate::cfg::DiskRateLimitConfig,
) -> Result<Box<firecracker_client::models::RateLimiter>> {
    let (bandwidth, ops) = if cfg.enabled {
        (bandwidth_bucket(cfg)?, ops_bucket(cfg)?)
    } else {
        (None, None)
    };
    let mut rl = firecracker_client::models::RateLimiter::new();
    rl.bandwidth = Some(bandwidth.unwrap_or_else(disabled_bucket));
    rl.ops = Some(ops.unwrap_or_else(disabled_bucket));
    Ok(Box::new(rl))
}

pub(super) fn managed_snapshot_base() -> PathBuf {
    ConfigManager::global_config()
        .firecracker
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("aenv"))
        .join("managed-snapshots")
}

// ── FirecrackerSandbox ───────────────────────────────────────────────────────

/// A Firecracker microVM-backed sandbox instance.
///
/// Internal state is managed via the high-level lifecycle methods.
/// Implements [`SandboxBackend`] for use by the Orchestrator.
pub struct FirecrackerSandbox {
    id: SandboxId,
    launch: LaunchMode,
    work_dir: TempDir,
    fc_instance: FirecrackerInstance,
    runtime_policy: FirecrackerRuntimePolicy,
    network_slot: Option<Slot>,
    current_network_policy: Option<SandboxNetworkPolicy>,
    /// Current custom extension params. Initialized from the launch config and
    /// updated via `update_custom_extension_params`; persisted into snapshots
    /// on pause.
    current_custom_extension_params: Option<CustomExtensionParams>,
    envd_instance: Option<EnvdInstance>,
    rootfs_runtime: Option<OverlaybdRuntimeHandle>,
    mem_ublk_device: Option<SharedMemDevice>,
    /// image.json path the memory device was opened with. Used as the device
    /// key to release held background downloads once envd is ready.
    mem_snapshot_image_config_path: Option<PathBuf>,
    /// image.json path of the memory image the next capture must stack onto.
    ///
    /// Memory layers are diffs: `/snapshot/create` is issued with
    /// `SnapshotType::Diff` and the layer is built from the dirty ranges
    /// reported since the previous capture, which resets that bitmap. So each
    /// capture is only meaningful stacked on the one before it.
    ///
    /// This deliberately does not read `launch`. `launch` describes how the VM
    /// was started and is never reassigned — it has readers as far afield as
    /// `startup_artifacts`, which feeds image-reference pinning, so moving it
    /// forward would release a different artifact set than was pinned. It also
    /// deliberately is not `mem_snapshot_image_config_path`, which is the ublk
    /// device key and must keep pointing at the image backing the *live* VM.
    mem_snapshot_parent_config_path: Option<PathBuf>,
    /// The checkpoint directory the next successful capture makes redundant.
    ///
    /// A capture adopts its parent's runtime layers into its own directory --
    /// hard-linked where the filesystem allows and copied otherwise -- and
    /// rewrites every lower to the new location, so once it succeeds the
    /// previous checkpoint's directory is referenced by nothing. The live guest
    /// never read it either: it is still mapping the memory device it booted
    /// against, which is `mem_snapshot_image_config_path`.
    ///
    /// Without this, a sandbox checkpointed on a timer grows its snapshot root
    /// by one dirty-set delta per interval for its whole life, and the only
    /// thing that ever reclaimed it was the sandbox dying. Only directories
    /// this path created are tracked, so a pause's own artifacts are never a
    /// candidate.
    superseded_checkpoint_dir: Option<PathBuf>,
    /// Memory-control devices this VM was observed to have, probed once the
    /// VM is live. All-false until then, and all-false forever for a VM that
    /// has none — see [`MemoryControlCapability`].
    mem_control: MemoryControlCapability,
    /// image.json path the rootfs device was opened with. Also released at
    /// envd ready so a rootfs background download (when enabled) never
    /// waits out the fallback with no notification.
    rootfs_image_config_path: Option<PathBuf>,
    extra_drive_runtimes: Vec<OverlaybdRuntimeHandle>,
    current_rootfs_virtual_size: Option<u64>,
    live_snapshot_root: Option<Arc<PersistentSnapshotRootGuard>>,
    /// Delivers the custom extension stop hook exactly once: `stop()` calls
    /// [`CustomExtensionHookGuard::stop`], otherwise its own drop fires the
    /// best-effort notification. `None` when no start hook was delivered (or
    /// no extension is configured).
    custom_extension_hook_guard: Option<CustomExtensionHookGuard>,
}

// ── SandboxBackend impl ──────────────────────────────────────────────────────

/// Firecracker-specific captured snapshot payload kept alive for publication.
#[derive(Debug)]
pub struct FirecrackerCapturedSnapshot {
    manifest: FirecrackerSnapshotManifest,
    _snapshot_root: Arc<PersistentSnapshotRootGuard>,
}

#[derive(Clone, Debug)]
pub struct FirecrackerPausedState {
    snapshot_config: FirecrackerSnapshotConfig,
}

impl FirecrackerPausedState {
    pub fn new(snapshot_config: FirecrackerSnapshotConfig) -> Self {
        Self { snapshot_config }
    }

    pub fn decode(_artifact_root: PathBuf, state: serde_json::Value) -> Result<Self> {
        let snapshot_config: FirecrackerSnapshotConfig =
            serde_json::from_value(state).context("deserialize Firecracker paused state")?;
        snapshot_config
            .validate_persisted()
            .context("validate Firecracker paused state artifacts")?;
        Ok(Self::new(snapshot_config))
    }

    pub fn snapshot_config(&self) -> &FirecrackerSnapshotConfig {
        &self.snapshot_config
    }
}

impl PausedSandboxState for FirecrackerPausedState {
    fn control_plane_port(&self) -> Option<u16> {
        let port = self.snapshot_config.common.control_plane_port;
        (port != 0).then_some(port)
    }

    fn encode(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.snapshot_config).context("serialize Firecracker paused state")
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(rootfs_and_extra_drive_image_config_paths(
            &self.snapshot_config.common,
        ))
    }
}

impl FirecrackerCapturedSnapshot {
    pub(crate) fn new(
        manifest: FirecrackerSnapshotManifest,
        snapshot_root: Arc<PersistentSnapshotRootGuard>,
    ) -> Self {
        Self {
            manifest,
            _snapshot_root: snapshot_root,
        }
    }

    pub fn manifest(&self) -> &FirecrackerSnapshotManifest {
        &self.manifest
    }
}

#[async_trait]
impl SandboxBackend for FirecrackerSandbox {
    async fn start(&mut self) -> Result<()> {
        FirecrackerSandbox::start(self).await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        FirecrackerSandbox::start_nowait(self).await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        FirecrackerSandbox::wait_for_ready(self).await
    }

    /// Pauses the VM and returns the paused state wrapped as a [`PausedSandboxState`].
    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        let pause_result = match artifact_root {
            Some(artifact_root) => FirecrackerSandbox::pause_to_dir(self, artifact_root)
                .await
                .map(|(snapshot_config, _)| snapshot_config),
            None => FirecrackerSandbox::pause(self).await,
        };
        let snapshot_config = match pause_result {
            Ok(snapshot_config) => snapshot_config,
            Err(err) => {
                let pause_err = SandboxCaptureError::from(err);
                if pause_err.is_terminal() {
                    return Err(pause_err);
                }
                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "pause failed and sandbox could not be resumed: pause error: {pause_err}; resume error: {resume_err:#}"
                    )));
                }
                return Err(pause_err);
            }
        };
        Ok(Arc::new(FirecrackerPausedState::new(snapshot_config)))
    }

    async fn checkpoint(&mut self) -> SandboxCaptureResult<CheckpointStats> {
        // Every failure here is recoverable by construction: nothing the guest
        // depends on is mutated, and `checkpoint_memory` resumes the VM on the
        // way out. Reporting it as terminal would destroy a healthy sandbox
        // over a missed optimisation.
        FirecrackerSandbox::checkpoint_memory(self)
            .await
            .map_err(SandboxCaptureError::recoverable)
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let live_snapshot_root = self
            .live_snapshot_root()
            .await
            .map_err(SandboxCaptureError::from)?;
        let snapshot_dir = live_snapshot_root.path().join(Uuid::now_v7().to_string());

        let (_, manifest) = match self.pause_to_dir(&snapshot_dir).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let snapshot_err = SandboxCaptureError::from(err);
                if snapshot_err.is_terminal() {
                    return Err(snapshot_err);
                }

                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "snapshot capture failed and sandbox could not be resumed: capture error: {snapshot_err}; resume error: {resume_err:#}"
                    )));
                }

                return Err(snapshot_err);
            }
        };
        FirecrackerSandbox::resume(self)
            .await
            .map_err(SandboxCaptureError::terminal)?;

        Ok(CapturedSandboxSnapshot::new(
            FirecrackerCapturedSnapshot::new(manifest, live_snapshot_root),
        ))
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        let snapshot_config = match FirecrackerSandbox::pause(self).await {
            Ok(snapshot_config) => snapshot_config,
            Err(err) => {
                let checkpoint_err = SandboxCaptureError::from(err);
                if checkpoint_err.is_terminal() {
                    return Err(checkpoint_err);
                }
                if let Err(resume_err) = FirecrackerSandbox::resume(self).await {
                    return Err(SandboxCaptureError::terminal(anyhow::anyhow!(
                        "fork checkpoint failed and sandbox could not be resumed: checkpoint error: {checkpoint_err}; resume error: {resume_err:#}"
                    )));
                }
                return Err(checkpoint_err);
            }
        };

        FirecrackerSandbox::resume(self)
            .await
            .map_err(SandboxCaptureError::terminal)?;

        let children = spec
            .iter()
            .map(|child| {
                Self::from_snapshot_config_with_override(
                    snapshot_config.clone(),
                    child.sandbox_id,
                    child.envd_access_token.clone(),
                )
                .map(|child| Box::new(child) as Box<dyn SandboxBackend>)
                .context("build forked sandbox")
            })
            .collect::<Vec<_>>();

        let start_results = futures::future::join_all(children.into_iter().map(|child| {
            async move {
                let mut child = child?;
                match child.start().await {
                    Ok(()) => Ok(child),
                    Err(err) => {
                        if let Err(stop_err) = child.stop().await {
                            warn!(error = ?stop_err, "failed to stop fork child after start failure");
                        }
                        Err(err.context("start forked sandbox"))
                    }
                }
            }
        }))
        .await;
        Ok(start_results)
    }

    async fn resume(&mut self) -> Result<()> {
        FirecrackerSandbox::resume(self).await
    }

    async fn stop(&mut self) -> Result<()> {
        FirecrackerSandbox::stop(self).await
    }

    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        FirecrackerSandbox::host_interaction_ip(self)
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        SandboxRuntimeInfo {
            rootfs_virtual_size: self.current_rootfs_virtual_size,
            runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(
                self.runtime_image_config_paths(),
            ),
            mem_control: self.mem_control,
        }
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        let common = match &self.launch {
            LaunchMode::Fresh(config) => &config.common,
            LaunchMode::Resume(config) => &config.common,
        };
        RuntimeArtifactSet::from_overlaybd_image_configs(rootfs_and_extra_drive_image_config_paths(
            common,
        ))
    }

    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        if let Some(slot) = self.network_slot.as_mut() {
            slot.set_egress_policy(policy.as_ref())
                .context("configure sandbox network policy")?;
            self.current_network_policy = policy;
            Ok(())
        } else if policy.is_none() {
            self.current_network_policy = policy;
            Ok(())
        } else {
            bail!("sandbox has no active network slot")
        }
    }

    async fn memory_telemetry(&self) -> Result<Option<SandboxMemoryTelemetry>> {
        FirecrackerSandbox::memory_telemetry(self).await
    }

    async fn set_memory_plug_target(&mut self, mib: u32) -> Result<()> {
        FirecrackerSandbox::set_memory_plug_target(self, mib).await
    }

    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>) {
        self.current_custom_extension_params = params;
    }
}

// ── SandboxExecutor impl ──────────────────────────────────────────────────────

#[async_trait(?Send)]
impl SandboxExecutor for FirecrackerSandbox {
    fn executor(&self) -> Result<Executor<'_>> {
        let envd = self
            .envd_instance
            .as_ref()
            .context("Sandbox is not running")?;
        Ok(Executor::new(envd))
    }
}

// ── FirecrackerSandbox public API ────────────────────────────────────────────

impl FirecrackerSandbox {
    fn snapshot_rootfs_virtual_size(&self) -> Result<u64> {
        self.current_rootfs_virtual_size.context(
            "rootfs virtual size cache missing; sandbox must record the user image block-device size before snapshot; ensure start() was called before pause() or snapshot",
        )
    }

    /// Create a sandbox handle for a fresh boot.
    ///
    /// This does not start Firecracker; it only prepares the object and its
    /// per-instance work directory.
    pub fn new(config: FirecrackerSandboxConfig) -> Result<Self> {
        Self::new_with_id(config, SandboxId::new())
    }

    pub(crate) fn new_with_id(config: FirecrackerSandboxConfig, id: SandboxId) -> Result<Self> {
        debug!(
            firecracker_binary = %config.common.firecracker_binary.display(),
            kernel_image = %config.kernel_image.display(),
            tools_drive_version = %config.common.tools_drive_version,
            "creating fresh firecracker sandbox"
        );
        Self::build(id, LaunchMode::Fresh(config))
    }

    /// Create a sandbox handle that resumes from the provided snapshot config.
    ///
    /// This only prepares the sandbox object and its per-instance workspace.
    /// Call [`FirecrackerSandbox::start`] or [`FirecrackerSandbox::start_nowait`] to boot it.
    #[tracing::instrument(skip(snapshot))]
    pub fn from_snapshot_config(snapshot: &FirecrackerSnapshotConfig) -> Result<Self> {
        Self::from_snapshot_config_with_override(
            snapshot.clone(),
            SandboxId::new(),
            snapshot.common.envd_access_token.clone(),
        )
    }

    pub(crate) fn from_snapshot_config_with_override(
        mut snapshot: FirecrackerSnapshotConfig,
        id: SandboxId,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Self> {
        // Runtime identity and auth override their values in the source snapshot.
        snapshot.common.envd_access_token = envd_access_token;
        let FirecrackerCommonConfig {
            mmds_metadata,
            envd_access_token,
            ..
        } = &mut snapshot.common;
        let metadata = mmds_metadata.get_or_insert_with(|| MmdsMetadata::new(id, "unknown"));
        metadata.sandbox_id = id.to_string();
        metadata.set_access_token(envd_access_token.as_ref());

        debug!(
            vm_state_path = %snapshot.vm_state_path.display(),
            mem_image_config_path = %snapshot.mem_overlaybd_config.image_config_path.display(),
            rootfs_path = ?snapshot.common.rootfs_image_config.as_ref().map(|rootfs| &rootfs.image_config_path),
            tools_drive_version = %snapshot.common.tools_drive_version,
            "creating firecracker sandbox from snapshot config"
        );
        Self::build(id, LaunchMode::Resume(snapshot))
    }

    /// Create a sandbox handle that boots from a resolved runnable committed snapshot.
    ///
    /// This only prepares the sandbox object and its per-instance workspace.
    /// Call [`FirecrackerSandbox::start`] or [`FirecrackerSandbox::start_nowait`] to boot it.
    pub fn from_snapshot(
        snapshot: &RunnableSnapshot,
        launch_config: &SandboxLaunchConfig,
    ) -> Result<Self> {
        let snapshot_config = Self::snapshot_config_for_launch(snapshot, launch_config)?;

        Self::build(
            launch_config.sandbox_id,
            LaunchMode::Resume(snapshot_config),
        )
    }

    fn snapshot_config_for_launch(
        snapshot: &RunnableSnapshot,
        launch_config: &SandboxLaunchConfig,
    ) -> Result<FirecrackerSnapshotConfig> {
        let mut snapshot_config = FirecrackerSnapshotConfig::from_runnable_snapshot(snapshot)?;
        snapshot_config.common.mmds_metadata = Some(
            MmdsMetadata::new(launch_config.sandbox_id, launch_config.snapshot_id.clone())
                .with_access_token(launch_config.envd_access_token.as_ref())
                .with_extra(launch_config.extra_mmds.clone()),
        );
        snapshot_config.common.envd_access_token = launch_config.envd_access_token.clone();
        snapshot_config.common.network_policy = launch_config.network.clone();

        // Launch-provided custom config overrides the value persisted in the
        // source snapshot; otherwise inherit the snapshot's.
        snapshot_config.common.custom_extension_params = launch_config
            .custom_extension_params
            .clone()
            .or_else(|| snapshot.committed().custom_extension_params.clone());

        if let Some(launch_env_vars) = &launch_config.env_vars {
            snapshot_config
                .common
                .env_vars
                .get_or_insert_default()
                .extend(launch_env_vars.clone());
        }

        Ok(snapshot_config)
    }

    /// Start the sandbox by launching Firecracker and waiting for readiness.
    ///
    /// This waits for the Firecracker API socket and the in-guest envd daemon.
    #[tracing::instrument(skip(self))]
    pub async fn start(&mut self) -> Result<()> {
        debug!("starting firecracker sandbox");
        self.start_nowait().await?;
        self.wait_for_ready().await
    }

    /// Start the sandbox WITHOUT waiting for envd's readiness.
    ///
    /// This only waits for the Firecracker API socket to be available and
    /// returns immediately after VM start command is issued.
    pub(crate) async fn start_nowait(&mut self) -> Result<()> {
        self.launch.validate()?;
        trace!("launch config validated");
        match &self.launch {
            LaunchMode::Fresh(config) => self.start_fresh(config.clone()).await,
            LaunchMode::Resume(config) => self.start_resume(config.clone()).await,
        }
    }

    /// Wait for the sandbox to be fully ready.
    ///
    /// This should be called after `start_nowait()` if you want to interact with the sandbox.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn wait_for_ready(&self) -> Result<()> {
        let Some(envd_instance) = self.envd_instance.as_ref() else {
            return Err(anyhow::anyhow!("envd instance not initialized"));
        };
        envd_instance
            .wait_for_ready(
                self.runtime_policy.envd_timeout,
                self.runtime_policy.envd_poll_interval,
            )
            .await?;
        if let Some(device_key) = &self.mem_snapshot_image_config_path {
            // envd is up: release held background downloads for this memory
            // device. Best-effort — downloads would also start after the
            // fallback timeout.
            UblkDeviceManager::global()
                .notify_sandbox_ready(device_key)
                .await;
        }
        if let Some(device_key) = &self.rootfs_image_config_path {
            // Same release for the rootfs image's background download.
            UblkDeviceManager::global()
                .notify_sandbox_ready(device_key)
                .await;
        }
        envd_instance
            .init(
                self.launch.common().env_vars.clone(),
                self.launch.common().default_workdir.clone(),
                self.launch.common().default_user.clone(),
            )
            .await
    }

    /// Pause the running sandbox and create a snapshot for later resume.
    ///
    /// This produces `vm_state.bin`, an overlaybd memory layer, and rootfs state
    /// owned by the returned [`FirecrackerSnapshotConfig`]. For overlaybd-backed
    /// sandboxes, the snapshot stores a copy of the writable upper under the
    /// snapshot dir.
    ///
    /// The snapshot artifacts are stored in a managed temporary directory that is
    /// automatically cleaned up when the reference count drops to zero.
    /// Use [`FirecrackerSandbox::pause_to_dir`] to specify a custom, caller-managed
    /// directory for the snapshot artifacts.
    ///
    /// The managed snapshot root is structured as `<managed-snapshot-base>/<sandbox_id>/<uuid>`, where:
    /// - `<managed-snapshot-base>` is `[firecracker].work_dir/managed-snapshots`, or
    ///   `<system-temp>/aenv/managed-snapshots` when `work_dir` is unset.
    /// - `<sandbox_id>` is the [`SandboxID`](crate::types::SandboxId), used to group snapshots by sandbox and improve readability.
    pub async fn pause(&mut self) -> Result<FirecrackerSnapshotConfig> {
        let snapshot_root = self.live_snapshot_root().await?;
        snapshot_root.prepare().await?;
        let snapshot_dir = snapshot_root.path().join(Uuid::now_v7().to_string());

        let (mut snapshot, _) = self.pause_to_dir(&snapshot_dir).await?;
        snapshot.managed_snapshot_root = Some(snapshot_root);

        Ok(snapshot)
    }

    /// Pause the running sandbox and persist its snapshot artifacts into a caller-managed directory.
    #[tracing::instrument(skip(self, snapshot_dir))]
    pub async fn pause_to_dir(
        &mut self,
        snapshot_dir: &Path,
    ) -> Result<(FirecrackerSnapshotConfig, FirecrackerSnapshotManifest)> {
        debug!(snapshot_dir = %snapshot_dir.display(), "pausing sandbox");
        // Before the pause, not after: this is the only `fc_instance.pause()`
        // in the file, so one call here covers pause, snapshot, fork and
        // template build, and the guest is still running to answer.
        self.run_free_page_hinting().await;
        self.fc_instance.pause().await?;

        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;

        let snapshot_result = self.snapshot_to_dir(snapshot_dir).await;
        match snapshot_result {
            Ok(snapshot) => {
                self.record_capture_as_chain_parent(&snapshot.0);
                // The capture adopted whatever the last checkpoint left, so its
                // directory is spent. Only on the success arm: a failed capture
                // resumes in place and the next one still diffs against it.
                self.retire_superseded_checkpoint().await;
                Ok(snapshot)
            }
            Err(err) => {
                Self::cleanup_failed_snapshot_dir(snapshot_dir).await;
                Err(err)
            }
        }
    }

    /// Makes `captured` the parent the next capture stacks onto.
    ///
    /// Called only from the success arm above. A failed capture resumes the VM
    /// in place, so the chain it stacks onto has not moved, and advancing the
    /// parent there would leave the next capture diffing against a layer that
    /// was never written.
    ///
    /// Split out of `pause_to_dir` because the rest of that function needs a
    /// live VM and this rule does not; it is the half that can be pinned by a
    /// unit test.
    fn record_capture_as_chain_parent(&mut self, captured: &FirecrackerSnapshotConfig) {
        self.mem_snapshot_parent_config_path =
            Some(captured.mem_overlaybd_config.image_config_path.clone());
    }

    /// Capture dirty memory only, leaving the rootfs alone, and resume.
    ///
    /// The prefix of `snapshot_to_dir` up to and including `mem_image.json`,
    /// and nothing after it: no `restack_snapshot_overlaybd_rootfs`, no
    /// `snapshot_extra_drives`. Those are what make a capture resumable and
    /// also what make it expensive and, on failure, terminal -- the restack
    /// seals the writable upper, so a failure there means the sandbox cannot
    /// resume at all. A checkpoint has neither property because it produces
    /// nothing anyone is allowed to resume from.
    ///
    /// The artifacts land under the same live snapshot root as a pause, so the
    /// next real capture inherits them as its parent chain and the guard frees
    /// them with everything else.
    pub async fn checkpoint_memory(&mut self) -> Result<CheckpointStats> {
        let snapshot_root = self.live_snapshot_root().await?;
        snapshot_root.prepare().await?;
        let checkpoint_dir = snapshot_root.path().join(Uuid::now_v7().to_string());
        tokio::fs::create_dir_all(&checkpoint_dir)
            .await
            .with_context(|| format!("create checkpoint dir {}", checkpoint_dir.display()))?;

        let started = Instant::now();
        self.run_free_page_hinting().await;
        self.fc_instance.pause().await?;

        let captured = self.capture_memory_layer(&checkpoint_dir).await;

        // Resume before reporting the failure. A checkpoint that failed has
        // touched nothing the guest depends on -- that is the whole point of
        // skipping the rootfs -- so leaving the VM paused would turn a cheap
        // miss into an outage.
        let resumed = self.resume().await;
        let freeze = started.elapsed();

        let captured = match captured {
            Ok(captured) => captured,
            Err(err) => {
                Self::cleanup_failed_snapshot_dir(&checkpoint_dir).await;
                resumed.context("resume after a failed checkpoint")?;
                return Err(err);
            }
        };
        resumed.context("resume after checkpoint")?;

        let (mem_image_config_path, layer_bytes) = captured;
        // Only now: the next capture must diff against a layer that exists.
        self.mem_snapshot_parent_config_path = Some(mem_image_config_path);
        // This checkpoint adopted the previous one's layers, so that directory
        // is now unreferenced. Retired after the parent pointer moves, never
        // before: a removal that raced the write it supersedes would take the
        // layers the new config just linked.
        self.retire_superseded_checkpoint().await;
        self.superseded_checkpoint_dir = Some(checkpoint_dir);
        Ok(CheckpointStats {
            layer_bytes,
            freeze,
        })
    }

    /// Removes the checkpoint directory the latest capture made redundant.
    ///
    /// Best effort. A directory left behind costs disk until the sandbox exits
    /// and its root guard drops; failing the capture over it would trade a
    /// bounded leak for a lost checkpoint.
    async fn retire_superseded_checkpoint(&mut self) {
        let Some(directory) = self.superseded_checkpoint_dir.take() else {
            return;
        };
        if let Err(error) = tokio::fs::remove_dir_all(&directory).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    directory = %directory.display(),
                    error = %error,
                    "failed to remove a superseded checkpoint directory"
                );
            }
        }
    }

    /// Writes the memory layer and its image config, returning the config path.
    ///
    /// Split out so `checkpoint_memory` owns the pause/resume pairing and this
    /// owns the capture, and neither can return early past the other's cleanup.
    async fn capture_memory_layer(&self, checkpoint_dir: &Path) -> Result<(PathBuf, u64)> {
        let vm_state_path = checkpoint_dir.join(VM_STATE_FILE_NAME);
        let memory_output = OverlaybdCompactOutput::from_memory_snapshot_config(
            &ConfigManager::global_config().memory_snapshot,
        );
        let (mem_layer_path, _) = self
            .snapshot_memory_to_overlaybd(&vm_state_path, checkpoint_dir, memory_output)
            .await?;
        let layer_bytes = tokio::fs::metadata(&mem_layer_path)
            .await
            .map(|meta| meta.len())
            .unwrap_or(0);

        let mem_image_config = build_mem_snapshot_image_config(
            self.mem_snapshot_parent_config_path.as_deref(),
            &mem_layer_path,
            checkpoint_dir,
            memory_output,
            ConfigManager::global_config()
                .memory_snapshot
                .checkpoint_max_snapshot_layers,
        )
        .await?;
        let mem_image_config_path = checkpoint_dir.join("mem_image.json");
        tokio::fs::write(
            &mem_image_config_path,
            serde_json::to_vec_pretty(&mem_image_config)
                .context("serialize checkpoint mem image config")?,
        )
        .await
        .with_context(|| {
            format!(
                "write checkpoint mem image config to {}",
                mem_image_config_path.display()
            )
        })?;
        Ok((mem_image_config_path, layer_bytes))
    }

    async fn snapshot_to_dir(
        &self,
        snapshot_dir: &Path,
    ) -> Result<(FirecrackerSnapshotConfig, FirecrackerSnapshotManifest)> {
        let vm_state_path = snapshot_dir.join(VM_STATE_FILE_NAME);
        let memory_output = OverlaybdCompactOutput::from_memory_snapshot_config(
            &ConfigManager::global_config().memory_snapshot,
        );
        let (mem_layer_path, mem_virtual_size) = self
            .snapshot_memory_to_overlaybd(&vm_state_path, snapshot_dir, memory_output)
            .await?;

        // Build the memory image config: collect parent layers, make runtime
        // lowers local to this snapshot dir, and compact only if the layer
        // count exceeds the configured maximum.
        //
        // The parent is the previous capture of *this* sandbox, not the image
        // it launched from. A second capture that stacked on the launch-time
        // config would omit the first capture's delta entirely.
        let mem_image_config = build_mem_snapshot_image_config(
            self.mem_snapshot_parent_config_path.as_deref(),
            &mem_layer_path,
            snapshot_dir,
            memory_output,
            ConfigManager::global_config()
                .memory_snapshot
                .max_snapshot_layers,
        )
        .await?;
        let mem_image_config_path = snapshot_dir.join("mem_image.json");
        tokio::fs::write(
            &mem_image_config_path,
            serde_json::to_vec_pretty(&mem_image_config)
                .context("serialize mem image config for persistent dir")?,
        )
        .await
        .with_context(|| {
            format!(
                "write mem image config to {}",
                mem_image_config_path.display()
            )
        })?;

        let mem_overlaybd_config = OverlaybdConfig {
            image_config_path: mem_image_config_path,
            read_only: true,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        };

        let (base_rootfs_path, rootfs_virtual_size) = if self.uses_overlaybd_ublk() {
            let overlaybd_source = self
                .launch
                .common()
                .ublk_config
                .as_ref()
                .map(|config| match &config.backend {
                    UblkBackend::Overlaybd(source) => source,
                })
                .context("overlaybd snapshot requires overlaybd-backed ublk config")?;
            let rootfs_runtime = self
                .rootfs_runtime
                .as_ref()
                .context("overlaybd snapshot requires an active ublk device")?;
            let rootfs_image_path = restack_snapshot_overlaybd_rootfs(
                &rootfs_runtime.device,
                overlaybd_source.read_only,
                &rootfs_runtime.image_config_path,
                snapshot_dir,
            )
            .await
            .context("snapshot overlaybd runtime state to persistent dir")?;
            let size = self
                .snapshot_rootfs_virtual_size()
                .context("persist rootfs virtual size for snapshot")?;
            (rootfs_image_path, size)
        } else {
            let rootfs_path = snapshot_dir.join(ROOTFS_DRIVE_PATH);
            // Preserve the writable disk state alongside the snapshot.
            let current_rootfs = self.work_dir.path().join(ROOTFS_DRIVE_PATH);
            copy_cow(&current_rootfs, &rootfs_path).await?;
            let size = self
                .snapshot_rootfs_virtual_size()
                .context("persist rootfs virtual size for snapshot")?;
            (rootfs_path, size)
        };
        let snapshot_extra_drives = self
            .snapshot_extra_drives(snapshot_dir)
            .await
            .context("snapshot extra drives to persistent dir")?;
        let mut snapshot_common = self.launch.common().clone();
        snapshot_common.network_policy = self.current_network_policy.clone();
        snapshot_common.custom_extension_params = self.current_custom_extension_params.clone();
        snapshot_common.extra_drives = snapshot_extra_drives.clone();
        let mut rootfs_read_only = false;

        // Rewrite the overlaybd backend's image config path to point at the snapshot's rootfs.
        // So that the resumed ublk device uses the captured rootfs layers instead of the original ones.
        if let Some(ublk_config) = snapshot_common.ublk_config.as_mut() {
            let UblkBackend::Overlaybd(source) = &mut ublk_config.backend;
            rootfs_read_only = source.read_only;
            source.image_config_path = base_rootfs_path.clone();
        }
        let runtime_upper_mode = snapshot_common
            .ublk_config
            .as_ref()
            .map(|config| match &config.backend {
                UblkBackend::Overlaybd(source) => source.runtime_upper_mode,
            })
            .unwrap_or(overlaybd::config::UpperMode::LogStructured);
        snapshot_common.rootfs_image_config = Some(OverlaybdConfig {
            image_config_path: base_rootfs_path.clone(),
            read_only: rootfs_read_only,
            runtime_upper_mode,
        });
        snapshot_common.rootfs_virtual_size = Some(rootfs_virtual_size);

        let manifest = FirecrackerSnapshotManifest::new(
            vm_state_path.clone(),
            mem_overlaybd_config.image_config_path.clone(),
            mem_virtual_size,
            base_rootfs_path,
            rootfs_virtual_size,
            &snapshot_extra_drives,
        )
        .context("build firecracker snapshot manifest")?;

        let snapshot = FirecrackerSnapshotConfig {
            common: snapshot_common,
            vm_state_path,
            mem_overlaybd_config,
            mem_virtual_size,
            managed_snapshot_root: None,
        };

        debug!(
            vm_state_path = %snapshot.vm_state_path.display(),
            mem_image_config_path = %snapshot.mem_overlaybd_config.image_config_path.display(),
            rootfs_path = ?snapshot.common.rootfs_image_config.as_ref().map(|rootfs| &rootfs.image_config_path),
            "persistent snapshot created"
        );
        Ok((snapshot, manifest))
    }

    async fn snapshot_memory_to_overlaybd(
        &self,
        vm_state_path: &Path,
        snapshot_dir: &Path,
        memory_output: OverlaybdCompactOutput,
    ) -> Result<(PathBuf, u64)> {
        let mem_overlaybd_dir = snapshot_dir.join("mem_overlaybd");
        let firecracker_pid = self.fc_instance.pid()?;
        self.fc_instance
            .create_state_only_snapshot(vm_state_path)
            .await?;
        // `vm_state.bin` now represents this paused VM state. Any later
        // error aborts this direct snapshot attempt and is propagated to
        // the lifecycle caller for recovery.
        let dirty_ranges = self.fc_instance.get_dirty_memory_ranges().await?;
        convert_dirty_memory_to_overlaybd(
            firecracker_pid,
            &dirty_ranges,
            &mem_overlaybd_dir,
            memory_output,
        )
        .await
        .context("convert dirty memory ranges to overlaybd layer")
    }

    /// Resume a paused sandbox in-place.
    ///
    /// Use this when you want to keep the same sandbox instance.
    pub async fn resume(&self) -> Result<()> {
        debug!("resuming paused sandbox in-place");
        self.fc_instance.resume().await?;
        Ok(())
    }

    /// Resume a new sandbox instance from snapshot config.
    ///
    /// This creates a new sandbox, starts Firecracker, and loads the snapshot config.
    #[tracing::instrument(skip(snapshot))]
    pub async fn resume_from_snapshot_config(snapshot: &FirecrackerSnapshotConfig) -> Result<Self> {
        let mut sandbox = Self::from_snapshot_config(snapshot)?;
        sandbox.start().await?;
        Ok(sandbox)
    }

    /// Stop the Firecracker process and release network resources.
    ///
    /// Sends SIGTERM and waits for exit; if it times out, sends SIGKILL.
    #[tracing::instrument(skip(self))]
    pub async fn stop(&mut self) -> Result<()> {
        debug!("stopping firecracker sandbox");

        self.fc_instance
            .stop(self.runtime_policy.socket_timeout)
            .await?;

        // Clear envd instance
        self.envd_instance = None;

        // Cleanup ublk device (must happen after FC stop, before network cleanup)
        if let Some(runtime) = self.rootfs_runtime.take() {
            if let Err(e) = UblkDeviceManager::global()
                .release_device(&runtime.device)
                .await
            {
                warn!(error = %e, "failed to release ublk device during stop");
            }
        }

        // Shared memory device: release explicitly so a following resume for
        // the same memory image cannot race the detached Drop cleanup.
        if let Some(mem_device) = self.mem_ublk_device.take() {
            if let Err(e) = mem_device.release().await {
                warn!(error = %e, "failed to release shared memory ublk device during stop");
            }
        }

        for runtime in self.extra_drive_runtimes.drain(..) {
            if let Err(e) = UblkDeviceManager::global()
                .release_device(&runtime.device)
                .await
            {
                warn!(error = %e, "failed to release extra drive device during stop");
            }
        }

        // Invoke the stop hook before releasing network resources. Delivery
        // failures are logged inside the client and never fail stop().
        if let Some(guard) = self.custom_extension_hook_guard.take() {
            guard.stop().await;
        }

        // Cleanup network resources. Release tears down the veth and unmounts
        // the namespace, so it goes to the blocking pool rather than parking
        // the worker running this stop.
        if let Some(slot) = self.network_slot.take() {
            let idx = slot.idx;
            tokio::task::spawn_blocking(move || NetworkManager::global().release(slot))
                .await
                .context("network slot release task")?
                .context("Failed to release network slot")?;
            debug!(slot = idx, "network slot released");
        }

        debug!("firecracker sandbox stopped");
        Ok(())
    }

    pub(crate) fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        self.network_slot
            .as_ref()
            .map(|slot| slot.host_interaction_ip)
    }

    pub(crate) fn firecracker_binary_path(&self) -> &Path {
        &self.launch.common().firecracker_binary
    }

    pub(crate) fn tools_drive_version(&self) -> &str {
        &self.launch.common().tools_drive_version
    }

    /// Resolve the Firecracker stdout log path for this sandbox.
    pub fn firecracker_stdout_path(&self) -> PathBuf {
        self.launch
            .common()
            .stdout_path
            .clone()
            .unwrap_or_else(|| self.default_log_dir())
            .join("firecracker-stdout.log")
    }

    /// Resolve the Firecracker stderr log path for this sandbox.
    pub fn firecracker_stderr_path(&self) -> PathBuf {
        self.launch
            .common()
            .stderr_path
            .clone()
            .unwrap_or_else(|| self.default_log_dir())
            .join("firecracker-stderr.log")
    }

    /// Resolve the Firecracker logger output path for this sandbox.
    ///
    /// Lives in the default log directory and is named `firecracker.log`.
    /// Only used when `firecracker_log_level` is set.
    pub fn firecracker_log_path(&self) -> PathBuf {
        self.default_log_dir().join("firecracker.log")
    }

    fn default_log_dir(&self) -> PathBuf {
        self.launch
            .common()
            .serial_output_base_dir
            .clone()
            .map(|p| p.join(self.id.to_string()))
            .unwrap_or_else(|| self.work_dir.path().join("logs"))
    }

    fn uses_overlaybd_ublk(&self) -> bool {
        self.launch.common().ublk_config.is_some()
    }

    fn mmds_metadata(&self, common: &FirecrackerCommonConfig) -> MmdsMetadata {
        common
            .mmds_metadata
            .clone()
            .unwrap_or_else(|| MmdsMetadata::new(self.id, "unknown"))
    }

    /// Return the writable user image path inside this sandbox's Firecracker CWD.
    pub fn work_rootfs_path(&self) -> PathBuf {
        self.work_dir.path().join(USER_ROOTFS_DRIVE_PATH)
    }

    fn new_managed_persistent_snapshot_root(&self) -> Arc<PersistentSnapshotRootGuard> {
        let sandbox_dir = self.id.to_string();
        let root = managed_snapshot_base().join(sandbox_dir);
        Arc::new(PersistentSnapshotRootGuard::new(root))
    }

    async fn live_snapshot_root(&mut self) -> Result<Arc<PersistentSnapshotRootGuard>> {
        if let Some(root) = &self.live_snapshot_root {
            return Ok(Arc::clone(root));
        }
        let root = self
            .launch
            .managed_snapshot_root()
            .unwrap_or_else(|| self.new_managed_persistent_snapshot_root());
        root.prepare().await?;
        self.live_snapshot_root = Some(Arc::clone(&root));
        Ok(root)
    }

    /// Best-effort cleanup of a caller-managed snapshot directory after a failed pause.
    ///
    /// Removes the directory contents so the caller isn't left with a partially-written snapshot.
    async fn cleanup_failed_snapshot_dir(path: &Path) {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                warn!(
                    snapshot_dir = %path.display(),
                    error = %err,
                    "failed to clean up incomplete snapshot directory"
                );
            }
        }
    }
}

/// Overlaybd image config paths a sandbox opens (rootfs + extra drives).
/// For fresh launches these are source configs; for paused states they are
/// snapshot artifact configs.
/// (Memory snapshot layers are remote/repository-backed, never local-only, so
/// they are not included.)
fn rootfs_and_extra_drive_image_config_paths(common: &FirecrackerCommonConfig) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(rootfs) = &common.rootfs_image_config {
        paths.push(rootfs.image_config_path.clone());
    }
    paths.extend(
        common
            .extra_drives
            .iter()
            .map(|drive| drive.image_config_path().to_path_buf()),
    );
    paths
}

/// Build the `agentenv_drives=vdc:/mnt/data,vdd:/mnt/logs:sub/path` boot arg
/// for extra drives. Returns `None` if there are no extra drives.
/// Drive letter mapping: vda = tools drive, vdb = user image, vdc = first extra drive.
/// Each entry is `vd<letter>:<mountPath>[:<subPath>]`, with optional
/// Kubernetes-style `subPath` semantics. API validation rejects `:` in both
/// `mountPath` and `subPath`, so the `:` separators are unambiguous.
fn build_drives_boot_arg(extra_drives: &[ExtraDrive]) -> Option<String> {
    if extra_drives.is_empty() {
        return None;
    }
    assert!(
        extra_drives.len() <= MAX_EXTRA_DRIVES,
        "too many extra drives for guest naming: {} > {}",
        extra_drives.len(),
        MAX_EXTRA_DRIVES
    );
    let entries: Vec<String> = extra_drives
        .iter()
        .enumerate()
        .map(|(i, drive)| {
            let dev_letter = (b'c' + i as u8) as char;
            let mut entry = format!("vd{}:{}", dev_letter, drive.mount_path().display());
            if let Some(sub_path) = drive.sub_path() {
                entry.push(':');
                entry.push_str(&sub_path.display().to_string());
            }
            entry
        })
        .collect();
    Some(format!("agentenv_drives={}", entries.join(",")))
}

fn relocate_warm_log(src: &Path, target: &Path) -> Result<()> {
    if src == target || !src.exists() {
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create firecracker log directory {}", parent.display()))?;
    }
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("remove existing firecracker log {}", target.display()))?;
    }
    match fs::rename(src, target) {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == Some(libc::EXDEV) => {
            fs::copy(src, target).with_context(|| {
                format!(
                    "copy warm firecracker log {} to {} after cross-device rename failed",
                    src.display(),
                    target.display()
                )
            })?;
            match fs::remove_file(src) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).with_context(|| {
                    format!(
                        "remove warm firecracker log {} after cross-device copy",
                        src.display()
                    )
                }),
            }
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "move warm firecracker log {} to {}",
                src.display(),
                target.display()
            )
        }),
    }
}

// ── Drop ─────────────────────────────────────────────────────────────────────

/// Ensure network resources are cleaned up when the sandbox is dropped.
/// This handles cases where stop() wasn't called (panic, early return, etc.)
///
/// For ublk, devices are managed by the daemon process, so we don't need to
/// kill any server process. The device ID is NOT recycled on abnormal drop
/// since proper delete (via the daemon) was not performed.
impl Drop for FirecrackerSandbox {
    fn drop(&mut self) {
        // Fire the best-effort stop notification (fire-and-forget, never
        // blocking drop) before releasing resources.
        self.custom_extension_hook_guard.take();
        // In daemon mode, ublk devices survive sandbox drop. The daemon will
        // clean them up on its own shutdown, or they can be explicitly deleted
        // via the orchestrator's stop() path.
        self.rootfs_runtime.take();
        self.mem_ublk_device.take();
        // In daemon mode, extra-drive devices survive sandbox drop. They are
        // explicitly deleted on the stop() path and otherwise cleaned up when
        // the daemon shuts down.
        self.extra_drive_runtimes.clear();
        if let Some(slot) = self.network_slot.take() {
            // Drop cannot await, and this drop often runs on a Tokio worker.
            NetworkManager::global().release_detached(slot);
        }
    }
}

// ── Memory control ───────────────────────────────────────────────────────────

impl FirecrackerSandbox {
    /// Record which memory-control devices this VM actually has.
    ///
    /// Called once per start, on both the fresh and the resume path. The
    /// resume path is not optional: a snapshot captured before a device existed
    /// comes back without it no matter how this node is configured, and a
    /// snapshot captured with one comes back with it even on a node that has
    /// since turned the feature off. Only the live VM knows.
    ///
    /// Never fails a start. A node whose Firecracker build does not serve
    /// `GET /vm/config` keeps every capability bit false and loses the elastic
    /// features, which is the same position it was in before they existed.
    async fn probe_memory_control(&mut self) {
        let vm_config = match self.fc_instance.vm_config().await {
            Ok(vm_config) => vm_config,
            Err(err) => {
                warn!(
                    sandbox_id = %self.id,
                    error = %err,
                    "memory control capability probe failed; treating the sandbox as having no \
                     balloon or hot-plug device"
                );
                self.mem_control = MemoryControlCapability::default();
                Self::record_memory_capability_metrics(self.mem_control);
                return;
            }
        };

        let balloon = vm_config.balloon.as_deref();
        self.mem_control = MemoryControlCapability {
            balloon: balloon.is_some(),
            balloon_stats: balloon
                .and_then(|balloon| balloon.stats_polling_interval_s)
                .is_some_and(|interval| interval > 0),
            free_page_hinting: balloon
                .and_then(|balloon| balloon.free_page_hinting)
                .unwrap_or(false),
            hotplug: vm_config.memory_hotplug.is_some(),
        };
        Self::record_memory_capability_metrics(self.mem_control);

        if self.mem_control.is_inert() {
            debug!(
                sandbox_id = %self.id,
                "sandbox has no memory control devices; elastic memory is inactive for it"
            );
        }
    }

    fn record_memory_capability_metrics(capability: MemoryControlCapability) {
        for (name, supported) in [
            ("balloon", capability.balloon),
            ("balloon_stats", capability.balloon_stats),
            ("free_page_hinting", capability.free_page_hinting),
            ("hotplug", capability.hotplug),
        ] {
            metrics::counter!(
                "agentenv_sandbox_memory_capability_total",
                "capability" => name,
                "supported" => if supported { "true" } else { "false" },
            )
            .increment(1);
        }
    }

    async fn memory_telemetry(&self) -> Result<Option<SandboxMemoryTelemetry>> {
        if !self.mem_control.balloon_stats {
            return Ok(None);
        }
        let stats = self
            .fc_instance
            .balloon_statistics()
            .await
            .context("sample balloon statistics")?;

        // A field the guest never sent stays absent: the balloon statistics are
        // gated on the guest kernel, and collapsing a missing counter to zero
        // would make a guest that cannot report allocation stalls
        // indistinguishable from one that has had none. A negative value is
        // not meaningful for any of them, so clamp that rather than fail a
        // whole control pass over one odd sample.
        fn non_negative(value: Option<i64>) -> Option<u64> {
            value.map(|value| u64::try_from(value).unwrap_or(0))
        }

        let mut telemetry = SandboxMemoryTelemetry {
            available_bytes: non_negative(stats.available_memory),
            total_bytes: non_negative(stats.total_memory),
            disk_caches_bytes: non_negative(stats.disk_caches),
            oom_kills: non_negative(stats.oom_kill),
            alloc_stalls: non_negative(stats.alloc_stall),
            plugged_mib: None,
            requested_mib: None,
        };

        if self.mem_control.hotplug {
            let status = self
                .fc_instance
                .memory_hotplug_status()
                .await
                .context("read hot-plug memory status")?;
            telemetry.plugged_mib = status
                .plugged_size_mib
                .and_then(|mib| u32::try_from(mib).ok());
            telemetry.requested_mib = status
                .requested_size_mib
                .and_then(|mib| u32::try_from(mib).ok());
        }

        Ok(Some(telemetry))
    }

    async fn set_memory_plug_target(&mut self, mib: u32) -> Result<()> {
        anyhow::ensure!(
            self.mem_control.hotplug,
            "sandbox {} has no virtio-mem device, so its memory ceiling cannot be moved",
            self.id
        );
        self.fc_instance
            .patch_memory_hotplug(mib)
            .await
            .context("set hot-plug memory target")
    }

    /// Ask the guest to declare its free pages so the imminent capture can skip
    /// them.
    ///
    /// Issued while the guest is still executing, and deliberately so. Hinting
    /// is a handshake — the host raises `host_cmd` and waits for the balloon
    /// driver to raise `guest_cmd` to match — and a paused vCPU cannot drain
    /// the balloon queue, so running this after the pause would burn the whole
    /// timeout on every capture and never complete once.
    ///
    /// Best-effort throughout: any failure, and expiry of the timeout, fall
    /// through to a capture that behaves exactly as it would with hinting off.
    async fn run_free_page_hinting(&self) {
        // Read from the launch config rather than the process-global one: it
        // is the same balloon block, resolved for this sandbox, and it is what
        // the resume path already reconciles against the node.
        let config = &self.launch.common().balloon;
        if !config.free_page_hinting_on_capture || !self.mem_control.free_page_hinting {
            return;
        }

        let mut guard = MetricGuard::stage(MEMORY_HINTING_DURATION_METRIC, "hinting");
        let outcome = self.free_page_hinting_round_trip(config).await;
        guard.finish(&outcome);
        if let Err(err) = outcome {
            warn!(
                sandbox_id = %self.id,
                error = %err,
                "free page hinting did not complete; capturing the full dirty set"
            );
        }

        // Stop unconditionally: an abandoned run left armed would leak into the
        // next capture's status poll, which would then read a stale completed
        // handshake and skip its own wait.
        if let Err(err) = self.fc_instance.stop_free_page_hinting().await {
            warn!(sandbox_id = %self.id, error = %err, "failed to stop free page hinting");
        }
    }

    async fn free_page_hinting_round_trip(&self, config: &BalloonConfig) -> Result<()> {
        self.fc_instance.start_free_page_hinting(true).await?;
        let deadline = Duration::from_millis(config.free_page_hinting_timeout_ms);
        tokio::time::timeout(deadline, async {
            loop {
                let status = self.fc_instance.free_page_hinting_status().await?;
                if status.guest_cmd == Some(status.host_cmd) {
                    return Ok(());
                }
                tokio::time::sleep(FREE_PAGE_HINTING_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("guest did not acknowledge free page hinting within {deadline:?}")
        })?
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

impl FirecrackerSandbox {
    fn build(id: SandboxId, launch: LaunchMode) -> Result<Self> {
        let work_dir =
            create_firecracker_work_dir(launch.common().firecracker_work_base_dir.as_deref())?;
        let fc_instance = FirecrackerInstance::new(work_dir.path().to_path_buf());
        let runtime_policy = launch.common().runtime_policy;
        let current_network_policy = launch.common().network_policy.clone();
        let current_custom_extension_params = launch.common().custom_extension_params.clone();
        // A sandbox resumed from a snapshot continues that snapshot's memory
        // chain; one booted fresh starts a new chain.
        let mem_snapshot_parent_config_path = match &launch {
            LaunchMode::Resume(config) => {
                Some(config.mem_overlaybd_config.image_config_path.clone())
            }
            LaunchMode::Fresh(_) => None,
        };
        debug!(work_dir = %work_dir.path().display(), "sandbox work directory prepared");

        Ok(Self {
            id,
            runtime_policy,
            current_rootfs_virtual_size: match &launch {
                LaunchMode::Fresh(_) => None,
                LaunchMode::Resume(config) => config.common.rootfs_virtual_size,
            },
            launch,
            work_dir,
            fc_instance,
            network_slot: None,
            current_network_policy,
            current_custom_extension_params,
            envd_instance: None,
            rootfs_runtime: None,
            mem_ublk_device: None,
            mem_snapshot_image_config_path: None,
            mem_snapshot_parent_config_path,
            superseded_checkpoint_dir: None,
            mem_control: MemoryControlCapability::default(),
            rootfs_image_config_path: None,
            extra_drive_runtimes: Vec::new(),
            live_snapshot_root: None,
            custom_extension_hook_guard: None,
        })
    }

    #[tracing::instrument(skip(self, config))]
    async fn start_fresh(&mut self, config: FirecrackerSandboxConfig) -> Result<()> {
        let work_dir = self.work_dir.path();
        debug!(work_dir = %work_dir.display(), "starting fresh sandbox");

        let global_config = ConfigManager::global_config();

        // ── Tools drive: plain ext4, read-only, shared across sandboxes ──
        // Symlink work_dir/rootfs.ext4 → tools drive so Firecracker can use a
        // relative path inside the work directory.
        self.link_tools_drive(&config.common, work_dir)?;

        // ── User image: overlaybd via ublk, writable, per-sandbox ──
        let user_image_symlink = work_dir.join(USER_ROOTFS_DRIVE_PATH);
        let global_cfg_path = global_config.ublk.overlaybd.global_config_path.clone();
        let rootfs_image_config = config
            .common
            .rootfs_image_config
            .as_ref()
            .context("fresh sandbox rootfs image config is missing")?;
        let runtime_dir = work_dir.join("overlaybd");
        let runtime_device = UblkDeviceManager::global()
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: &rootfs_image_config.image_config_path,
                global_config: &global_cfg_path,
                runtime_dir: &runtime_dir,
                read_only: rootfs_image_config.read_only,
                runtime_upper_mode: rootfs_image_config.runtime_upper_mode,
                requested_virtual_size: config.common.rootfs_virtual_size,
                known_source_virtual_size: None,
                allow_shrink: config.common.rootfs_allow_shrink,
            })
            .await
            .context("create user image overlaybd runtime device")?;
        self.rootfs_image_config_path = Some(rootfs_image_config.image_config_path.clone());
        let device_path = runtime_device.device.device_path().to_path_buf();
        let symlink_result = std::os::unix::fs::symlink(&device_path, &user_image_symlink)
            .context("symlink user-rootfs to ublk device");
        if let Err(err) = symlink_result {
            if let Err(release_err) = UblkDeviceManager::global()
                .release_device(&runtime_device.device)
                .await
            {
                warn!(
                    error = %release_err,
                    "failed to release user image ublk device after symlink failure"
                );
            }
            return Err(err);
        }
        self.rootfs_runtime = Some(OverlaybdRuntimeHandle {
            device: runtime_device.device,
            image_config_path: runtime_device.image_config_path,
            actual_virtual_size: runtime_device.actual_virtual_size,
        });
        self.current_rootfs_virtual_size = Some(runtime_device.actual_virtual_size);

        // ── Boot args: init=/init (tools drive has init baked in) ──
        let mut boot_args = config.boot_args.clone();
        // Tools drive contains /init; ensure boot args include init=/init.
        let missing_explicit_init_arg = match boot_args.as_deref() {
            Some(args) => !args.split_whitespace().any(|arg| arg.starts_with("init=")),
            None => true,
        };
        if missing_explicit_init_arg {
            let init_arg = "init=/init";
            boot_args = Some(match boot_args.take() {
                Some(existing) => format!("{existing} {init_arg}"),
                None => init_arg.to_string(),
            });
        }

        // ── Extra drives ──
        let (extra_drive_attachments, extra_drive_runtimes) =
            if config.common.extra_drives.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let overlaybd_global = global_config.ublk.overlaybd.global_config_path.clone();
                let runtime_upper_mode = global_config.ublk.overlaybd.runtime_upper_mode;
                let allow_shrink = global_config.ublk.overlaybd.allow_shrink;
                prepare_extra_drives(
                    &config.common.extra_drives,
                    &overlaybd_global,
                    self.work_dir.path(),
                    runtime_upper_mode,
                    ExtraDrivePrepareMode::Fresh { allow_shrink },
                )
                .await
                .context("prepare extra drives")?
                .into_parts()
            };
        self.extra_drive_runtimes = extra_drive_runtimes;

        // ── Boot args: extra drive mount points (agentenv_drives=vdc:...) ──
        if let Some(drives_arg) = build_drives_boot_arg(&config.common.extra_drives) {
            boot_args = Some(match boot_args.take() {
                Some(existing) => format!("{existing} {drives_arg}"),
                None => drives_arg,
            });
        }

        // ── Allocate network slot and create network infrastructure ──
        //
        // Slot creation is synchronous and internally joins dedicated threads
        // (netns membership is thread-local, so the work cannot simply be made
        // async). Calling it directly from this async fn parks a runtime worker
        // for the whole of it on every cold create.
        let slot = tokio::task::spawn_blocking(|| NetworkManager::global().allocate_any())
            .await
            .context("network slot allocation task panicked")?
            .context("Failed to allocate network slot")?;
        debug!(slot = slot.idx, "allocated network slot");
        let interaction_ip = slot.host_interaction_ip;

        // Add IP configuration to boot args for the VM.
        // Uses Slot::build_ip_boot_arg() to produce the kernel ip= parameter with a
        // valid DNS server IP in the 8th field. See that method for format details.
        let ip_config = slot.build_ip_boot_arg();
        let netns = slot.namespace_path();
        self.network_slot = Some(slot);
        self.network_slot
            .as_mut()
            .expect("network slot was just assigned")
            .set_egress_policy(config.common.network_policy.as_ref())
            .context("Failed to configure sandbox egress policy")?;
        boot_args = Some(match boot_args.take() {
            Some(existing) => format!("{existing} {ip_config}"),
            None => ip_config,
        });

        // ── Custom extension hook: start-fresh (may contribute extra boot args) ──
        if let Some(client) = CustomExtensionClient::global() {
            let mut guard = CustomExtensionHookGuard::new(client, self.id);
            let extra_boot_args = guard
                .start_fresh(
                    &netns.to_string_lossy(),
                    interaction_ip,
                    config.common.custom_extension_params.as_ref(),
                )
                .await?;
            self.custom_extension_hook_guard = Some(guard);
            if let Some(extra) = extra_boot_args.filter(|args| !args.trim().is_empty()) {
                boot_args = Some(match boot_args.take() {
                    Some(existing) => format!("{existing} {extra}"),
                    None => extra,
                });
            }
        }

        // ── Spawn Firecracker inside the network namespace so it can access tap0 ──
        let firecracker_binary = config.common.firecracker_binary.clone();
        let stdout_path = self.firecracker_stdout_path();
        let stderr_path = self.firecracker_stderr_path();

        self.fc_instance
            .spawn_with_netns(
                &firecracker_binary,
                Some(&stdout_path),
                Some(&stderr_path),
                Some(&netns),
            )
            .await?;

        let envd_base_url = format!(
            "http://{}:{}",
            interaction_ip, config.common.control_plane_port
        );
        self.envd_instance = Some(EnvdInstance::new(
            envd_base_url,
            config.common.envd_access_token.clone(),
        ));

        // ── Configure microVM: tools drive as rootfs + user image + extras ──
        self.fc_instance
            .wait_for_ready(
                self.runtime_policy.socket_timeout,
                self.runtime_policy.socket_poll_interval,
            )
            .await?;
        self.configure_microvm(&config, boot_args.as_deref(), &extra_drive_attachments)
            .await?;
        self.fc_instance.start().await?;
        self.probe_memory_control().await;
        debug!("fresh sandbox started");
        Ok(())
    }

    #[tracing::instrument(skip(self, config))]
    async fn start_resume(&mut self, config: FirecrackerSnapshotConfig) -> Result<()> {
        // NOTE: The virtio-balloon device is NOT configured here. Balloon state
        // is part of vm_state.bin and is restored automatically by Firecracker.
        // Snapshots taken before balloon support was added will simply not have
        // the device — free_page_reporting will be absent for those VMs, which
        // is acceptable during rollout.

        // Fail fast: memory restore requires a ublk device. Check before
        // allocating any resources (Firecracker process, network namespace, …).
        anyhow::ensure!(
            UblkDeviceManager::global().is_available(),
            "snapshot resume requires an available ublk daemon client \
             because memory restore uses a shared ublk device"
        );

        let global_config = ConfigManager::global_config();

        let rootfs_virtual_size = config
            .common
            .rootfs_virtual_size
            .context("snapshot rootfs virtual size is missing")?;
        let rootfs_image_config = config
            .common
            .rootfs_image_config
            .as_ref()
            .context("snapshot rootfs image config is missing")?;
        self.current_rootfs_virtual_size = Some(rootfs_virtual_size);

        if config.common.stdout_path.is_none() && config.common.stderr_path.is_none() {
            if let Some(warm) = FirecrackerPool::global().and_then(|pool| pool.try_acquire()) {
                let warm_dir = warm.work_dir.path();
                let warm_stdout = warm_stdout_path(warm_dir);
                let warm_stderr = warm_stderr_path(warm_dir);
                debug!(
                    slot = warm.slot.idx,
                    pool_work_dir = %warm_dir.display(),
                    "using warm firecracker from pool"
                );

                self.network_slot = Some(warm.slot);
                self.work_dir = warm.work_dir; // Update self.work_dir before relocating logs since the fallback log paths are relative to the work_dir.
                let _cold = std::mem::replace(&mut self.fc_instance, warm.fc_instance);
                if let Err(err) = relocate_warm_log(&warm_stdout, &self.firecracker_stdout_path()) {
                    warn!(error = %err, "failed to relocate warm firecracker stdout log");
                }
                if let Err(err) = relocate_warm_log(&warm_stderr, &self.firecracker_stderr_path()) {
                    warn!(error = %err, "failed to relocate warm firecracker stderr log");
                }
            }
        }

        let fc_cwd = self.work_dir.path();
        let vm_state_src = fs::canonicalize(&config.vm_state_path)
            .unwrap_or_else(|_| config.vm_state_path.clone());
        debug!(
            fc_cwd = %fc_cwd.display(),
            vm_state_path = %vm_state_src.display(),
            "starting sandbox from snapshot config"
        );

        // ── Tools drive: symlink rootfs.ext4 → tools drive (read-only, from snapshot config) ──
        self.link_tools_drive(&config.common, fc_cwd)?;

        // ── User image: restore overlaybd via ublk ──
        if config.common.ublk_config.is_some() {
            let user_image_symlink = fc_cwd.join(USER_ROOTFS_DRIVE_PATH);
            let global_cfg_path = global_config.ublk.overlaybd.global_config_path.clone();
            let runtime_dir = fc_cwd.join("overlaybd");
            let runtime_device = UblkDeviceManager::global()
                .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                    source_image_config: &rootfs_image_config.image_config_path,
                    global_config: &global_cfg_path,
                    runtime_dir: &runtime_dir,
                    read_only: rootfs_image_config.read_only,
                    runtime_upper_mode: rootfs_image_config.runtime_upper_mode,
                    requested_virtual_size: Some(rootfs_virtual_size),
                    known_source_virtual_size: Some(rootfs_virtual_size),
                    allow_shrink: false,
                })
                .await
                .context("create user image overlaybd runtime device for resume")?;
            self.rootfs_image_config_path = Some(rootfs_image_config.image_config_path.clone());
            let device_path = runtime_device.device.device_path().to_path_buf();
            let symlink_result = std::os::unix::fs::symlink(&device_path, &user_image_symlink)
                .context("symlink user-rootfs to ublk device for resume");
            if let Err(err) = symlink_result {
                if let Err(release_err) = UblkDeviceManager::global()
                    .release_device(&runtime_device.device)
                    .await
                {
                    warn!(
                        error = %release_err,
                        "failed to release resumed user image ublk device after symlink failure"
                    );
                }
                return Err(err);
            }
            self.rootfs_runtime = Some(OverlaybdRuntimeHandle {
                device: runtime_device.device,
                image_config_path: runtime_device.image_config_path,
                actual_virtual_size: runtime_device.actual_virtual_size,
            });
            self.current_rootfs_virtual_size = Some(runtime_device.actual_virtual_size);
        }

        // ── Extra drives ──
        self.prepare_snapshot_backing_drives(&config.common.extra_drives)
            .await
            .context("prepare snapshot-backed extra drives for resume")?;

        // ── Network + Firecracker spawn ──
        let needs_socket_wait = self.network_slot.is_none();
        let interaction_ip = if let Some(slot) = self.network_slot.as_ref() {
            slot.host_interaction_ip
        } else {
            let slot = tokio::task::spawn_blocking(|| NetworkManager::global().allocate_any())
                .await
                .context("network slot allocation task panicked")?
                .context("Failed to allocate network slot for resume")?;
            debug!(slot = slot.idx, "allocated network slot for resume");
            let netns = slot.namespace_path();
            let interaction_ip = slot.host_interaction_ip;
            self.network_slot = Some(slot);

            let firecracker_binary = config.common.firecracker_binary.clone();
            let stdout_path = self.firecracker_stdout_path();
            let stderr_path = self.firecracker_stderr_path();

            self.fc_instance
                .spawn_with_netns(
                    &firecracker_binary,
                    Some(&stdout_path),
                    Some(&stderr_path),
                    Some(&netns),
                )
                .await?;

            interaction_ip
        };
        if let Some(slot) = self.network_slot.as_mut() {
            slot.set_egress_policy(config.common.network_policy.as_ref())
                .context("Failed to configure sandbox egress policy for resume")?;
        }

        // ── Custom extension hook: start-resume ──
        if let Some(client) = CustomExtensionClient::global() {
            let slot = self
                .network_slot
                .as_ref()
                .context("network slot must be allocated before start-resume hook")?;
            let mut guard = CustomExtensionHookGuard::new(client, self.id);
            guard
                .start_resume(
                    &slot.namespace_path().to_string_lossy(),
                    slot.host_interaction_ip,
                    config.common.custom_extension_params.as_ref(),
                )
                .await?;
            self.custom_extension_hook_guard = Some(guard);
        }

        let envd_base_url = format!(
            "http://{}:{}",
            interaction_ip, config.common.control_plane_port
        );
        self.envd_instance = Some(EnvdInstance::new(
            envd_base_url,
            config.common.envd_access_token.clone(),
        ));

        let mem_global_config = global_config
            .memory_snapshot
            .overlaybd_global_config_path
            .clone();
        let mem_device = UblkDeviceManager::global()
            .get_or_create_shared_mem(
                &UblkCreateSpec::Overlaybd {
                    image_config: config.mem_overlaybd_config.image_config_path.clone(),
                    global_config: mem_global_config,
                },
                config.mem_virtual_size,
            )
            .await
            .context("create or reuse shared memory ublk device for resume")?;
        let mem_device_path = mem_device.device_path().to_path_buf();
        self.mem_snapshot_image_config_path =
            Some(config.mem_overlaybd_config.image_config_path.clone());
        self.mem_ublk_device = Some(mem_device);

        if needs_socket_wait {
            self.fc_instance
                .wait_for_ready(
                    self.runtime_policy.socket_timeout,
                    self.runtime_policy.socket_poll_interval,
                )
                .await?;
        }

        self.configure_logger(&config.common).await?;

        // Override the network interface to use the new tap0 in our namespace
        let network_overrides = [("eth0", "tap0")];
        self.fc_instance
            .load_snapshot_file(
                &vm_state_src,
                &mem_device_path,
                &network_overrides,
                false,
                config.common.track_dirty_pages,
            )
            .await?;

        let mmds_metadata = self.mmds_metadata(&config.common);
        self.fc_instance.set_mmds(&mmds_metadata).await?;

        // A restored snapshot inherits whatever limiter was active when it was
        // paused, so reconcile against the node's current config while the VM is
        // still loaded-but-paused — before resume() lets the guest issue I/O.
        // Both buckets are always overwritten (configured or unlimited) so an
        // inherited dimension the current config leaves unset is cleared rather
        // than left unchanged.
        let reconciled = reconcile_disk_rate_limiter(&config.common.disk_rate_limit)?;
        self.fc_instance
            .patch_drive_rate_limiter(USER_ROOTFS_DRIVE_ID, reconciled)
            .await
            .context("reconcile disk rate limiter on snapshot resume")?;

        // Same window, same reason: the restored VM's memory devices come from
        // vm_state.bin, so this is the first moment they can be observed and
        // the last moment before the guest starts running against them.
        self.probe_memory_control().await;

        self.fc_instance.resume().await?;

        debug!("sandbox restored from snapshot config");
        Ok(())
    }

    /// Enable Firecracker logging when `firecracker_log_level` is configured.
    ///
    /// Must be called pre-boot (and before snapshot load). When no log level is
    /// set, this is a no-op so the default behaviour is unchanged.
    async fn configure_logger(&self, common: &FirecrackerCommonConfig) -> Result<()> {
        let Some(level) = common.firecracker_log_level.as_deref() else {
            return Ok(());
        };
        if level.trim().is_empty() {
            return Ok(());
        }
        let log_path = self.firecracker_log_path();
        if let Some(parent) = log_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("create firecracker log directory {}", parent.display())
            })?;
        }
        self.fc_instance.set_logger(&log_path, level).await?;
        debug!(
            log_path = %log_path.display(),
            level,
            "firecracker logger enabled"
        );
        Ok(())
    }

    fn link_tools_drive(&self, common: &FirecrackerCommonConfig, work_dir: &Path) -> Result<()> {
        let tools_drive_path = common
            .resolved_tools_drive_path(ConfigManager::global_config())
            .with_context(|| {
                format!(
                    "resolve tools drive version '{}' for sandbox {}",
                    common.tools_drive_version, self.id
                )
            })?;
        let tools_drive_path = fs::canonicalize(&tools_drive_path).with_context(|| {
            format!(
                "open tools drive version '{}' at {} for sandbox {}",
                common.tools_drive_version,
                tools_drive_path.display(),
                self.id
            )
        })?;
        let firecracker_path = work_dir.join(ROOTFS_DRIVE_PATH);

        std::os::unix::fs::symlink(&tools_drive_path, &firecracker_path).with_context(|| {
            format!(
                "link tools drive version '{}' from {} to {} for sandbox {}",
                common.tools_drive_version,
                tools_drive_path.display(),
                firecracker_path.display(),
                self.id
            )
        })?;
        debug!(
            sandbox_id = %self.id,
            tools_drive_version = %common.tools_drive_version,
            source = %tools_drive_path.display(),
            destination = %firecracker_path.display(),
            "linked tools drive into Firecracker work directory"
        );
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn configure_microvm(
        &self,
        config: &FirecrackerSandboxConfig,
        boot_args: Option<&str>,
        extra_drive_attachments: &[DriveMount],
    ) -> Result<()> {
        self.configure_logger(&config.common).await?;

        self.fc_instance
            .set_machine_config(
                config.mem_size_mib,
                config.vcpu_count,
                false,
                config.common.track_dirty_pages,
            )
            .await?;

        if let Some(cpu_json) = config.common.cpu_config_json.as_deref() {
            if !cpu_json.is_empty() {
                self.fc_instance.set_cpu_config(cpu_json).await?;
            }
        }

        let kernel_image =
            fs::canonicalize(&config.kernel_image).unwrap_or_else(|_| config.kernel_image.clone());
        self.fc_instance
            .set_boot_source(&kernel_image, None, boot_args)
            .await?;

        // Drive 0 (/dev/vda): tools drive as root device, always read-only.
        self.fc_instance
            .add_drive(
                ROOTFS_DRIVE_ID,
                Path::new(ROOTFS_DRIVE_PATH),
                true,
                true,
                false,
                IoEngine::Sync,
                None,
            )
            .await
            .with_context(|| {
                format!(
                    "attach tools drive version '{}' as {} for fresh sandbox {}",
                    config.common.tools_drive_version, ROOTFS_DRIVE_PATH, self.id
                )
            })?;

        // Drive 1 (/dev/vdb): user image, writable. The disk rate limiter is
        // applied here as pre-boot drive config (rather than a post-start PATCH)
        // so throttling is in force the instant the guest starts issuing I/O.
        self.fc_instance
            .add_drive(
                USER_ROOTFS_DRIVE_ID,
                Path::new(USER_ROOTFS_DRIVE_PATH),
                false,
                false,
                true,
                IoEngine::Async,
                build_disk_rate_limiter(&config.common.disk_rate_limit)?,
            )
            .await?;

        // Drive 2+ (/dev/vdc...): user extra drives.
        self.configure_extra_drives(extra_drive_attachments).await?;

        if self.network_slot.is_some() {
            // Network interface.
            self.fc_instance
                .add_network_interface("eth0", None, "tap0".to_string(), None, None)
                .await
                .context("Failed to add network interface to microVM")?;

            // MMDS
            self.fc_instance
                .set_mmds_config("eth0")
                .await
                .context("Failed to set MMDS network configuration")?;
            let mmds_metadata = self.mmds_metadata(&config.common);
            self.fc_instance.set_mmds(&mmds_metadata).await?;
        }

        // ── Balloon: enable free page reporting ──
        // Paired with DAMON reclaim (kernel boot args): DAMON reclaims cold
        // pagecache pages inside the guest, and the balloon device reports the
        // resulting free pages to the host VMM so it can release physical memory.
        self.fc_instance.set_balloon(&config.common.balloon).await?;

        Ok(())
    }

    async fn configure_extra_drives(&self, extra_drive_attachments: &[DriveMount]) -> Result<()> {
        for drive in extra_drive_attachments {
            self.fc_instance
                .add_drive(
                    &drive.drive_id,
                    &drive.attachment_path,
                    false,
                    drive.read_only,
                    true,
                    IoEngine::Async,
                    None,
                )
                .await
                .with_context(|| format!("Failed to add extra drive {}", drive.drive_id))?;
        }

        Ok(())
    }

    async fn prepare_snapshot_backing_drives(&mut self, extra_drives: &[ExtraDrive]) -> Result<()> {
        if extra_drives.is_empty() {
            self.extra_drive_runtimes.clear();
            return Ok(());
        }

        let global_config = ConfigManager::global_config();
        let ublk_config = &global_config.ublk;
        let overlaybd_global = ublk_config.overlaybd.global_config_path.clone();
        let runtime_upper_mode = ublk_config.overlaybd.runtime_upper_mode;
        let prepared_extra_drives = prepare_extra_drives(
            extra_drives,
            &overlaybd_global,
            self.work_dir.path(),
            runtime_upper_mode,
            ExtraDrivePrepareMode::Resume,
        )
        .await?;
        let (_attachments_already_in_snapshot, extra_drive_runtimes) =
            prepared_extra_drives.into_parts();
        self.extra_drive_runtimes = extra_drive_runtimes;
        Ok(())
    }

    async fn snapshot_extra_drives(&self, snapshot_dir: &Path) -> Result<Vec<ExtraDrive>> {
        let extra_drives = &self.launch.common().extra_drives;
        if extra_drives.is_empty() {
            return Ok(Vec::new());
        }
        if extra_drives.len() != self.extra_drive_runtimes.len() {
            bail!(
                "extra drive bookkeeping mismatch: {} configured drives but {} prepared devices",
                extra_drives.len(),
                self.extra_drive_runtimes.len()
            );
        }

        let mut snapped = Vec::with_capacity(extra_drives.len());
        for (drive, runtime) in extra_drives.iter().zip(self.extra_drive_runtimes.iter()) {
            let snapshot_image_config_path = restack_snapshot_overlaybd_device(
                &runtime.device,
                drive.read_only(),
                &runtime.image_config_path,
                &snapshot_dir.join("drives").join(drive.drive_id()),
                "drive",
            )
            .await
            .with_context(|| format!("snapshot extra drive '{}'", drive.drive_id()))?;
            snapped.push(
                drive
                    .with_image_config_path(snapshot_image_config_path)
                    .try_with_virtual_size(runtime.actual_virtual_size)?,
            );
        }

        Ok(snapped)
    }

    fn runtime_image_config_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Some(rootfs_runtime) = &self.rootfs_runtime {
            paths.push(rootfs_runtime.image_config_path.clone());
        }
        paths.extend(
            self.extra_drive_runtimes
                .iter()
                .map(|runtime| runtime.image_config_path.clone()),
        );
        paths
    }
}

// ── LaunchMode ───────────────────────────────────────────────────────────────

enum LaunchMode {
    Fresh(FirecrackerSandboxConfig),
    Resume(FirecrackerSnapshotConfig),
}

impl LaunchMode {
    fn common(&self) -> &FirecrackerCommonConfig {
        match self {
            LaunchMode::Fresh(config) => &config.common,
            LaunchMode::Resume(config) => &config.common,
        }
    }

    fn managed_snapshot_root(&self) -> Option<Arc<PersistentSnapshotRootGuard>> {
        match self {
            LaunchMode::Fresh(_) => None,
            LaunchMode::Resume(config) => config.managed_snapshot_root.clone(),
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            LaunchMode::Fresh(config) => config.validate(),
            LaunchMode::Resume(config) => config.validate(),
        }
    }
}

/// Copy a file using reflink (CoW) if available, falling back to a full copy.
async fn copy_cow(src: &Path, dst: &Path) -> Result<()> {
    let src = src.to_path_buf();
    let dst = dst.to_path_buf();
    // File copying can be multi-GB on snapshot paths, so keep the whole
    // reflink-or-copy fallback on a blocking thread.
    tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("cp");
        if cfg!(target_os = "linux") {
            cmd.arg("--reflink=auto");
        }
        let status = cmd.arg(&src).arg(&dst).status();
        if let Ok(s) = status {
            if s.success() {
                return Ok(());
            }
        }
        tracing::warn!(?src, ?dst, "reflink unavailable, falling back to full copy");
        fs::copy(&src, &dst)?;
        Ok(())
    })
    .await
    .context("copy_cow task failed")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::ToolsConfig;
    use crate::sandbox::{SandboxAccessTokenGenerator, SandboxExecutor};
    use crate::snapshot::{CommittedSnapshot, RunnableSnapshot, SnapshotRecord};
    use std::collections::HashMap;

    fn fresh_config() -> FirecrackerSandboxConfig {
        FirecrackerSandboxConfig::new(
            "firecracker".into(),
            "vmlinux.bin".into(),
            "0.1.0".to_string(),
            "user-image.json".into(),
        )
    }

    fn overlaybd_config() -> FirecrackerSandboxConfig {
        let mut config = fresh_config();
        config.common.ublk_config = Some(crate::sandbox::ublk::UblkConfig::overlaybd(
            "overlaybd-image.json".into(),
            false,
        ));
        config
    }

    fn snapshot_config_with_mem_image(mem_image_path: &str) -> FirecrackerSnapshotConfig {
        FirecrackerSnapshotConfig {
            common: overlaybd_config().common,
            vm_state_path: "snapshot/vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: mem_image_path.into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            managed_snapshot_root: None,
        }
    }

    use super::super::socket::fake::{FakeFirecracker, FakeReply};

    /// A sandbox whose Firecracker API socket is served by `fake`, with the
    /// balloon features `balloon` asks for and the matching probed capability.
    ///
    /// The socket path is derived the same way the real one is, so nothing here
    /// reaches around the code under test.
    fn hinting_sandbox(
        balloon: crate::cfg::BalloonConfig,
        capability: MemoryControlCapability,
    ) -> Result<(FirecrackerSandbox, FakeFirecracker)> {
        let mut config = overlaybd_config();
        config.common.balloon = balloon;
        let mut sandbox = FirecrackerSandbox::new(config)?;
        sandbox.mem_control = capability;
        let fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, path| match path {
                "/balloon/hinting/status" => {
                    FakeReply::json(serde_json::json!({"host_cmd": 1, "guest_cmd": 1}))
                }
                _ => FakeReply::no_content(),
            },
        )?;
        Ok((sandbox, fake))
    }

    fn hinting_on() -> crate::cfg::BalloonConfig {
        crate::cfg::BalloonConfig {
            stats_polling_interval_s: 1,
            free_page_hinting: true,
            free_page_hinting_on_capture: true,
            free_page_hinting_timeout_ms: 500,
        }
    }

    fn hinting_capability() -> MemoryControlCapability {
        MemoryControlCapability {
            balloon: true,
            balloon_stats: true,
            free_page_hinting: true,
            hotplug: false,
        }
    }

    #[tokio::test]
    async fn hinting_runs_before_the_vm_is_paused() -> Result<()> {
        // Hinting is a handshake with the guest balloon driver. A paused vCPU
        // cannot drain the balloon queue, so a run issued after PATCH /vm
        // would never complete and every capture would burn the full timeout.
        let (mut sandbox, fake) = hinting_sandbox(hinting_on(), hinting_capability())?;
        let snapshot_dir = sandbox.work_dir.path().join("snapshot");

        // The capture itself cannot succeed without a live Firecracker
        // process to read memory out of; the ordering before that point is
        // what this pins.
        let _ = sandbox.pause_to_dir(&snapshot_dir).await;

        let sequence = fake.path_sequence();
        let pause_at = sequence
            .iter()
            .position(|path| path == "/vm")
            .expect("the VM was never paused");
        let start_at = sequence
            .iter()
            .position(|path| path == "/balloon/hinting/start")
            .expect("hinting never started");
        let stop_at = sequence
            .iter()
            .position(|path| path == "/balloon/hinting/stop")
            .expect("hinting never stopped");

        assert!(
            start_at < pause_at && stop_at < pause_at,
            "the whole hinting run must precede the pause, got {sequence:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn hinting_is_skipped_when_the_run_switch_is_off() -> Result<()> {
        let mut balloon = hinting_on();
        balloon.free_page_hinting_on_capture = false;
        let (mut sandbox, fake) = hinting_sandbox(balloon, hinting_capability())?;
        let snapshot_dir = sandbox.work_dir.path().join("snapshot");

        let _ = sandbox.pause_to_dir(&snapshot_dir).await;

        assert!(
            !fake
                .path_sequence()
                .iter()
                .any(|path| path.starts_with("/balloon/hinting")),
            "the shipped default must not touch the hinting endpoints"
        );
        Ok(())
    }

    #[tokio::test]
    async fn hinting_is_skipped_when_the_vm_has_no_hinting_capable_balloon() -> Result<()> {
        // A snapshot captured before the feature existed comes back without
        // it however this node is configured, so the probed capability — not
        // the config — decides.
        let (mut sandbox, fake) =
            hinting_sandbox(hinting_on(), MemoryControlCapability::default())?;
        let snapshot_dir = sandbox.work_dir.path().join("snapshot");

        let _ = sandbox.pause_to_dir(&snapshot_dir).await;

        assert!(!fake
            .path_sequence()
            .iter()
            .any(|path| path.starts_with("/balloon/hinting")));
        Ok(())
    }

    #[tokio::test]
    async fn a_guest_that_never_answers_does_not_hold_up_the_capture() -> Result<()> {
        let mut config = overlaybd_config();
        config.common.balloon = hinting_on();
        config.common.balloon.free_page_hinting_timeout_ms = 150;
        let mut sandbox = FirecrackerSandbox::new(config)?;
        sandbox.mem_control = hinting_capability();
        let fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, path| match path {
                // The guest never acknowledges: guest_cmd stays behind.
                "/balloon/hinting/status" => {
                    FakeReply::json(serde_json::json!({"host_cmd": 1, "guest_cmd": 0}))
                }
                _ => FakeReply::no_content(),
            },
        )?;
        let snapshot_dir = sandbox.work_dir.path().join("snapshot");

        let started = std::time::Instant::now();
        let _ = sandbox.pause_to_dir(&snapshot_dir).await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(3),
            "an unanswered hinting run must fall through on its timeout, took {elapsed:?}"
        );
        let sequence = fake.path_sequence();
        assert!(
            sequence.iter().any(|path| path == "/balloon/hinting/stop"),
            "an abandoned run must still be stopped, got {sequence:?}"
        );
        assert!(
            sequence.iter().any(|path| path == "/vm"),
            "a hinting timeout must not fail the pause, got {sequence:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_capability_probe_reads_the_live_device_set() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        let _fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, path| {
                assert_eq!(path, "/vm/config");
                FakeReply::json(serde_json::json!({
                    "balloon": {
                        "amount_mib": 0,
                        "deflate_on_oom": true,
                        "stats_polling_interval_s": 1,
                        "free_page_hinting": true,
                    },
                    "memory-hotplug": {"total_size_mib": 1024},
                }))
            },
        )?;

        sandbox.probe_memory_control().await;

        assert_eq!(
            sandbox.runtime_info().mem_control,
            MemoryControlCapability {
                balloon: true,
                balloon_stats: true,
                free_page_hinting: true,
                hotplug: true,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_balloon_without_statistics_is_not_reported_as_stats_capable() -> Result<()> {
        // Statistics cannot be armed after boot, so a device that came back
        // without them is a permanent opt-out for the control loop, not a
        // retryable condition.
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        let _fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, _path| {
                FakeReply::json(serde_json::json!({
                    "balloon": {"amount_mib": 0, "deflate_on_oom": true},
                }))
            },
        )?;

        sandbox.probe_memory_control().await;

        let capability = sandbox.runtime_info().mem_control;
        assert!(capability.balloon);
        assert!(!capability.balloon_stats);
        assert!(!capability.free_page_hinting);
        assert!(sandbox.memory_telemetry().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_probe_leaves_every_capability_off() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        let _fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, _path| FakeReply::bad_request("no such resource"),
        )?;

        sandbox.probe_memory_control().await;

        assert!(sandbox.runtime_info().mem_control.is_inert());
        Ok(())
    }

    #[tokio::test]
    async fn memory_telemetry_reports_the_guest_numbers_and_the_plugged_size() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        sandbox.mem_control = MemoryControlCapability {
            balloon: true,
            balloon_stats: true,
            free_page_hinting: false,
            hotplug: true,
        };
        let _fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, path| match path {
                "/balloon/statistics" => FakeReply::json(serde_json::json!({
                    "target_pages": 0,
                    "actual_pages": 0,
                    "target_mib": 0,
                    "actual_mib": 0,
                    "available_memory": 256_000_000,
                    "total_memory": 1_024_000_000,
                    "disk_caches": 64_000_000,
                    "alloc_stall": 4,
                    "oom_kill": 0,
                })),
                "/hotplug/memory" => FakeReply::json(serde_json::json!({
                    "plugged_size_mib": 256,
                    "requested_size_mib": 512,
                })),
                other => panic!("unexpected path {other}"),
            },
        )?;

        let telemetry = sandbox
            .memory_telemetry()
            .await?
            .expect("a stats-capable balloon must produce a sample");

        assert_eq!(telemetry.available_bytes, Some(256_000_000));
        assert_eq!(telemetry.total_bytes, Some(1_024_000_000));
        assert_eq!(telemetry.disk_caches_bytes, Some(64_000_000));
        assert_eq!(telemetry.alloc_stalls, Some(4));
        assert_eq!(telemetry.oom_kills, Some(0));
        assert_eq!(telemetry.plugged_mib, Some(256));
        assert_eq!(telemetry.requested_mib, Some(512));
        Ok(())
    }

    #[tokio::test]
    async fn a_counter_the_guest_omits_is_reported_as_absent_not_as_zero() -> Result<()> {
        // oom_kill and alloc_stall are gated on the guest kernel version, and
        // total_memory on the driver reporting S_MEMTOT. Collapsing an absent
        // field to zero makes a guest that cannot report distress look like
        // one that has had none, and a guest that reports no total look like
        // one with none at all.
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        sandbox.mem_control = MemoryControlCapability {
            balloon: true,
            balloon_stats: true,
            free_page_hinting: false,
            hotplug: false,
        };
        let _fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, _path| {
                FakeReply::json(serde_json::json!({
                    "target_pages": 0,
                    "actual_pages": 0,
                    "target_mib": 0,
                    "actual_mib": 0,
                    "available_memory": 256_000_000,
                }))
            },
        )?;

        let telemetry = sandbox
            .memory_telemetry()
            .await?
            .expect("a stats-capable balloon must produce a sample");

        assert_eq!(telemetry.oom_kills, None);
        assert_eq!(telemetry.alloc_stalls, None);
        assert_eq!(telemetry.total_bytes, None);
        assert!(telemetry.is_blind_to_distress());
        assert!(
            telemetry.guest_position().is_none(),
            "a sample with no total describes no position"
        );
        Ok(())
    }

    #[tokio::test]
    async fn configure_microvm_arms_the_balloon_the_operator_asked_for() -> Result<()> {
        // Every balloon feature is pre-boot, so this single PUT during
        // configuration is the whole window in which `[machine.balloon]` can
        // reach a microVM. If the launch config is not what is sent, the
        // operator's block is inert and no later call can recover it.
        let mut config = overlaybd_config();
        config.common.balloon = crate::cfg::BalloonConfig {
            stats_polling_interval_s: 9,
            free_page_hinting: true,
            free_page_hinting_on_capture: false,
            free_page_hinting_timeout_ms: 3000,
        };
        let sandbox = FirecrackerSandbox::new(config.clone())?;
        let fake = FakeFirecracker::spawn(
            &sandbox.work_dir.path().join("firecracker.socket"),
            |_method, _path| FakeReply::no_content(),
        )?;

        sandbox.configure_microvm(&config, None, &[]).await?;

        let body = fake
            .body_of("/balloon")
            .expect("configuring a microVM must PUT the balloon");
        assert_eq!(body["stats_polling_interval_s"], 9);
        assert_eq!(body["free_page_hinting"], true);
        Ok(())
    }

    #[tokio::test]
    async fn moving_the_plug_target_is_refused_without_a_virtio_mem_device() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        sandbox.mem_control = hinting_capability();

        let err = FirecrackerSandbox::set_memory_plug_target(&mut sandbox, 1024)
            .await
            .expect_err("a sandbox with no virtio-mem device cannot move its ceiling");

        assert!(err.to_string().contains("no virtio-mem device"));
        Ok(())
    }

    fn rate_limit_cfg() -> crate::cfg::DiskRateLimitConfig {
        crate::cfg::DiskRateLimitConfig {
            enabled: true,
            bandwidth_bytes_per_sec: 0,
            bandwidth_burst_bytes: 0,
            iops: 0,
            iops_burst: 0,
        }
    }

    #[test]
    fn rate_limiter_disabled_returns_none() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = false;
        cfg.bandwidth_bytes_per_sec = 104_857_600;
        assert!(build_disk_rate_limiter(&cfg).unwrap().is_none());
    }

    #[test]
    fn rate_limiter_enabled_but_all_zero_returns_none() {
        assert!(build_disk_rate_limiter(&rate_limit_cfg())
            .unwrap()
            .is_none());
    }

    #[test]
    fn rate_limiter_bandwidth_size_equals_per_second_rate() {
        let mut cfg = rate_limit_cfg();
        cfg.bandwidth_bytes_per_sec = 104_857_600; // 100 MB/s
        cfg.bandwidth_burst_bytes = 10_485_760;
        let rl = build_disk_rate_limiter(&cfg)
            .unwrap()
            .expect("limiter present");
        let bw = rl.bandwidth.expect("bandwidth bucket");
        // With refill pinned to 1000 ms, bucket size == sustained bytes/sec.
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 104_857_600);
        assert_eq!(bw.one_time_burst, Some(10_485_760));
        assert!(rl.ops.is_none());
    }

    #[test]
    fn rate_limiter_iops_bucket_populated() {
        let mut cfg = rate_limit_cfg();
        cfg.iops = 3000;
        cfg.iops_burst = 500;
        let rl = build_disk_rate_limiter(&cfg)
            .unwrap()
            .expect("limiter present");
        let ops = rl.ops.expect("ops bucket");
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
        assert_eq!(ops.one_time_burst, Some(500));
        assert!(rl.bandwidth.is_none());
    }

    #[test]
    fn rate_limiter_rejects_values_beyond_i64_range() {
        let mut cfg = rate_limit_cfg();
        cfg.bandwidth_bytes_per_sec = u64::MAX;
        assert!(build_disk_rate_limiter(&cfg).is_err());
    }

    #[test]
    fn reconcile_disabled_makes_both_buckets_disabled() {
        // Firecracker treats an absent bucket in a PATCH as "leave unchanged", so
        // clearing an inherited limiter requires overwriting BOTH buckets with a
        // disabled (size == 0) bucket rather than sending an empty RateLimiter.
        let mut cfg = rate_limit_cfg();
        cfg.enabled = false;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.size, 0);
        assert_eq!(ops.size, 0);
    }

    #[test]
    fn reconcile_bandwidth_only_clears_inherited_iops() {
        // Enabled with bandwidth but no iops: bandwidth gets its configured
        // bucket, while the unset iops dimension is overwritten with a disabled
        // bucket so a snapshot-inherited IOPS limit does not survive the resume.
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 0;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 100 << 20);
        assert_eq!(ops.size, 0);
    }

    #[test]
    fn reconcile_iops_only_clears_inherited_bandwidth() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 0;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.size, 0);
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
    }

    #[test]
    fn reconcile_both_dimensions_use_configured_buckets() {
        let mut cfg = rate_limit_cfg();
        cfg.enabled = true;
        cfg.bandwidth_bytes_per_sec = 100 << 20;
        cfg.iops = 3000;
        let rl = reconcile_disk_rate_limiter(&cfg).unwrap();
        let bw = rl.bandwidth.expect("bandwidth bucket present");
        let ops = rl.ops.expect("ops bucket present");
        assert_eq!(bw.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(bw.size, 100 << 20);
        assert_eq!(ops.refill_time, RATE_LIMIT_REFILL_TIME_MS);
        assert_eq!(ops.size, 3000);
    }

    #[test]
    fn paused_state_image_cache_paths_use_snapshot_artifact_config() {
        let mut common = fresh_config().common;
        common
            .rootfs_image_config
            .as_mut()
            .expect("fresh config has a rootfs")
            .image_config_path = "snapshot/rootfs/image.json".into();
        let state = FirecrackerPausedState::new(FirecrackerSnapshotConfig {
            common,
            vm_state_path: "snapshot/vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: "snapshot/mem_image.json".into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            managed_snapshot_root: None,
        });

        assert_eq!(
            state.runtime_artifacts(),
            RuntimeArtifactSet::from_overlaybd_image_configs(vec![PathBuf::from(
                "snapshot/rootfs/image.json"
            )])
        );
    }

    #[test]
    fn snapshot_config_runtime_identity_replaces_source_auth() -> Result<()> {
        let source_id = SandboxId::new();
        let child_id = SandboxId::new();
        let generator = SandboxAccessTokenGenerator::new("fork-test-seed")?;
        let source_token = generator.generate(source_id);
        let child_token = generator.generate(child_id);
        let mut common = fresh_config().common;
        common.mmds_metadata =
            Some(MmdsMetadata::new(source_id, "snapshot").with_access_token(Some(&source_token)));
        common.envd_access_token = Some(source_token.clone());
        let snapshot = FirecrackerSnapshotConfig {
            common,
            vm_state_path: "snapshot/vm_state.bin".into(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: "snapshot/mem_image.json".into(),
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            managed_snapshot_root: None,
        };

        let child = FirecrackerSandbox::from_snapshot_config_with_override(
            snapshot,
            child_id,
            Some(child_token.clone()),
        )?;
        let common = child.launch.common();
        let metadata = common.mmds_metadata.as_ref().expect("child MMDS metadata");
        let expected =
            MmdsMetadata::new(child_id, "snapshot").with_access_token(Some(&child_token));

        assert_eq!(child.id, child_id);
        assert_eq!(common.envd_access_token.as_ref(), Some(&child_token));
        assert_eq!(metadata.sandbox_id, child_id.to_string());
        assert_eq!(metadata.access_token_hash, expected.access_token_hash);
        assert_ne!(
            metadata.access_token_hash,
            MmdsMetadata::new(source_id, "snapshot")
                .with_access_token(Some(&source_token))
                .access_token_hash
        );
        Ok(())
    }

    #[test]
    fn paused_state_without_tools_drive_version_remains_readable_but_not_resumable() -> Result<()> {
        let temp = TempDir::new()?;
        let vm_state_path = temp.path().join("vm-state.bin");
        let mem_image_path = temp.path().join("mem-image.json");
        let rootfs_image_path = temp.path().join("rootfs-image.json");
        fs::write(&vm_state_path, b"state")?;
        fs::write(&mem_image_path, b"{}")?;
        fs::write(&rootfs_image_path, b"{}")?;

        let mut common = fresh_config().common;
        common.rootfs_virtual_size = Some(4096);
        common
            .rootfs_image_config
            .as_mut()
            .expect("fresh config has a rootfs")
            .image_config_path = rootfs_image_path;
        let mut value = serde_json::to_value(FirecrackerSnapshotConfig {
            common,
            vm_state_path,
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: mem_image_path,
                read_only: true,
                runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            managed_snapshot_root: None,
        })?;
        let common = value["common"]
            .as_object_mut()
            .expect("common config must be an object");
        common.remove("tools_drive_version");
        common.insert(
            "tools_drive_path".to_string(),
            serde_json::Value::String("/legacy/node/tools.ext4".to_string()),
        );

        let state = FirecrackerPausedState::decode(PathBuf::new(), value)?;

        assert!(state
            .snapshot_config()
            .common
            .tools_drive_version
            .is_empty());
        let err = state
            .snapshot_config()
            .validate()
            .expect_err("legacy paused state must not resume without a tools drive version");
        assert!(err
            .to_string()
            .contains("sandbox state does not record a tools drive version"));
        Ok(())
    }

    #[test]
    fn executor_requires_running_envd_instance() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(fresh_config())?;
        let err = match SandboxExecutor::executor(&sandbox) {
            Ok(_) => panic!("envd should be missing"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Sandbox is not running"));
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_ready_requires_envd_instance() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(fresh_config())?;
        let err = sandbox
            .wait_for_ready()
            .await
            .expect_err("envd should be missing");
        assert!(err.to_string().contains("envd instance not initialized"));
        Ok(())
    }

    #[tokio::test]
    async fn stop_without_process_clears_envd_instance() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        sandbox.envd_instance = Some(EnvdInstance::new(
            format!(
                "http://127.0.0.1:{}",
                ToolsConfig::default().control_plane_port
            ),
            None,
        ));

        sandbox.stop().await?;

        assert!(sandbox.envd_instance.is_none());
        Ok(())
    }

    #[test]
    fn host_interaction_ip_reflects_network_slot_state() -> Result<()> {
        let mut sandbox = FirecrackerSandbox::new(fresh_config())?;
        assert_eq!(sandbox.host_interaction_ip(), None);

        let manager = NetworkManager::new(false, 0, 0);
        let slot = manager.allocate_test_slot()?;
        let expected = slot.host_interaction_ip;
        sandbox.network_slot = Some(slot);

        assert_eq!(sandbox.host_interaction_ip(), Some(expected));
        let slot = sandbox
            .network_slot
            .take()
            .expect("network slot should still be present");
        manager.release(slot)?;
        Ok(())
    }

    #[test]
    fn work_rootfs_path_uses_overlaybd_symlink_path() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        assert_eq!(
            sandbox.work_rootfs_path(),
            sandbox.work_dir.path().join("user-rootfs")
        );
        assert!(sandbox.uses_overlaybd_ublk());
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_uses_custom_mount_path() -> Result<()> {
        let drives = vec![
            ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/data-image.json",
                true,
                "/workspace/data",
                None::<std::path::PathBuf>,
            )?,
            ExtraDrive::try_new_overlaybd("logs", "/tmp/logs-image.json", true)?,
        ];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/workspace/data,vdd:/mnt/logs".to_string())
        );
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_includes_sub_path() -> Result<()> {
        let drives = vec![ExtraDrive::try_new_overlaybd_with_mount_path(
            "data",
            "/tmp/data-image.json",
            true,
            "/mnt/data",
            Some("sub/dir"),
        )?];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/mnt/data:sub/dir".to_string())
        );
        Ok(())
    }

    #[test]
    fn from_snapshot_merges_launch_env_over_snapshot_env() -> Result<()> {
        let mut snapshot_env_vars = HashMap::new();
        snapshot_env_vars.insert("FROM_SNAPSHOT".to_string(), "true".to_string());
        snapshot_env_vars.insert("SHARED_KEY".to_string(), "snapshot".to_string());
        let record = SnapshotRecord {
            id: crate::snapshot::SnapshotId::generate(),
            ..SnapshotRecord::mock_ready(CommittedSnapshot {
                context: crate::snapshot::CommandContext::new(snapshot_env_vars, "/"),
                ..CommittedSnapshot::mock()
            })
        };
        let snapshot = RunnableSnapshot::from_test_manifest(record, Vec::new());
        let launch_config = SandboxLaunchConfig {
            env_vars: Some(HashMap::from([
                ("FROM_LAUNCH".to_string(), "true".to_string()),
                ("SHARED_KEY".to_string(), "launch".to_string()),
            ])),
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
        };

        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)
                .expect("snapshot launch config should merge env vars");
        let env_vars = snapshot_config
            .common
            .env_vars
            .as_ref()
            .expect("env vars should be present after merge");
        assert_eq!(env_vars.get("FROM_SNAPSHOT"), Some(&"true".to_string()));
        assert_eq!(env_vars.get("FROM_LAUNCH"), Some(&"true".to_string()));
        assert_eq!(env_vars.get("SHARED_KEY"), Some(&"launch".to_string()));
        Ok(())
    }

    /// Build custom extension params from a JSON object literal.
    fn params(value: serde_json::Value) -> CustomExtensionParams {
        value
            .as_object()
            .expect("params must be a JSON object")
            .clone()
    }

    #[test]
    fn from_snapshot_custom_extension_params_launch_overrides_snapshot() -> Result<()> {
        let record = SnapshotRecord {
            id: crate::snapshot::SnapshotId::generate(),
            ..SnapshotRecord::mock_ready(CommittedSnapshot {
                custom_extension_params: Some(params(serde_json::json!({"from": "snapshot"}))),
                ..CommittedSnapshot::mock()
            })
        };
        let snapshot = RunnableSnapshot::from_test_manifest(record, Vec::new());

        // Launch-provided value wins over the snapshot-persisted one.
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            custom_extension_params: Some(params(serde_json::json!({"from": "launch"}))),
            ..SandboxLaunchConfig::default()
        };
        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)?;
        assert_eq!(
            snapshot_config.common.custom_extension_params,
            Some(params(serde_json::json!({"from": "launch"})))
        );

        // Without a launch value, the snapshot's persisted config is inherited.
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: "tpl-test".to_string(),
            ..SandboxLaunchConfig::default()
        };
        let snapshot_config =
            FirecrackerSandbox::snapshot_config_for_launch(&snapshot, &launch_config)?;
        assert_eq!(
            snapshot_config.common.custom_extension_params,
            Some(params(serde_json::json!({"from": "snapshot"})))
        );
        Ok(())
    }

    #[test]
    fn snapshot_rootfs_virtual_size_requires_cached_value() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        let err = sandbox
            .snapshot_rootfs_virtual_size()
            .expect_err("missing cached rootfs virtual size should fail");
        assert!(err
            .to_string()
            .contains("ensure start() was called before pause() or snapshot"));
        Ok(())
    }

    #[test]
    fn fresh_launch_starts_a_new_memory_chain() -> Result<()> {
        let sandbox = FirecrackerSandbox::new(overlaybd_config())?;
        assert!(
            sandbox.mem_snapshot_parent_config_path.is_none(),
            "a freshly booted sandbox has no earlier capture to stack onto"
        );
        Ok(())
    }

    #[test]
    fn resumed_launch_continues_the_snapshot_memory_chain() -> Result<()> {
        let parent = PathBuf::from("/snapshots/parent/mem_image.json");
        let snapshot = snapshot_config_with_mem_image("/snapshots/parent/mem_image.json");

        let sandbox = FirecrackerSandbox::from_snapshot_config(&snapshot)?;

        assert_eq!(
            sandbox.mem_snapshot_parent_config_path.as_deref(),
            Some(parent.as_path()),
            "a resumed sandbox must stack its next capture onto the image it resumed from"
        );
        Ok(())
    }

    /// Each capture becomes the parent of the next one.
    ///
    /// This is the whole of the memory chain's shape. A second capture whose
    /// parent is still the launch-time config produces a layer that diffs
    /// against a state two captures old, and the first capture's delta is in
    /// no image at all — silent corruption, visible only when the snapshot is
    /// restored and the guest's memory is missing whatever changed between the
    /// two captures.
    #[test]
    fn each_capture_becomes_the_parent_of_the_next() -> Result<()> {
        let snapshot = snapshot_config_with_mem_image("/snapshots/launch/mem_image.json");
        let mut sandbox = FirecrackerSandbox::from_snapshot_config(&snapshot)?;

        let first = snapshot_config_with_mem_image("/snapshots/capture-1/mem_image.json");
        sandbox.record_capture_as_chain_parent(&first);
        assert_eq!(
            sandbox.mem_snapshot_parent_config_path.as_deref(),
            Some(std::path::Path::new("/snapshots/capture-1/mem_image.json")),
            "the first capture must replace the launch-time parent"
        );

        let second = snapshot_config_with_mem_image("/snapshots/capture-2/mem_image.json");
        sandbox.record_capture_as_chain_parent(&second);
        assert_eq!(
            sandbox.mem_snapshot_parent_config_path.as_deref(),
            Some(std::path::Path::new("/snapshots/capture-2/mem_image.json")),
            "the second capture must stack onto the first, not back onto the launch config"
        );
        Ok(())
    }

    /// Answers one Firecracker API request with 204 on the socket the sandbox
    /// will use, so a test can drive `pause_to_dir` past the VM pause without a
    /// hypervisor.
    fn fake_firecracker_api(socket_path: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
        use http_body_util::Full;
        use hyper::body::Incoming;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Request, Response, StatusCode};
        use hyper_util::rt::TokioIo;

        let listener =
            tokio::net::UnixListener::bind(&socket_path).expect("bind the fake firecracker socket");
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let _ = http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_: Request<Incoming>| async move {
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::NO_CONTENT)
                                .body(Full::new(bytes::Bytes::new()))
                                .expect("build response"),
                        )
                    }),
                )
                .await;
        })
    }

    /// A capture that fails after the VM is paused leaves the chain where it
    /// was.
    ///
    /// This is the arm that matters: the VM has been paused, so the sandbox is
    /// mutated, and the capture then fails while writing the layer.
    /// `pause_to_dir` resumes in place, so the next capture still diffs against
    /// the same parent — advancing it here would point that capture at a
    /// `mem_image.json` nothing ever wrote, and the layer it produced would be
    /// a diff against a state that does not exist.
    #[tokio::test]
    async fn a_failed_capture_leaves_the_memory_chain_alone() -> Result<()> {
        let snapshot = snapshot_config_with_mem_image("/snapshots/parent/mem_image.json");
        let mut sandbox = FirecrackerSandbox::from_snapshot_config(&snapshot)?;
        let before = sandbox.mem_snapshot_parent_config_path.clone();
        assert!(before.is_some(), "the fixture must start with a parent");

        let server = fake_firecracker_api(sandbox.work_dir.path().join("firecracker.socket"));
        let snapshot_dir = sandbox.work_dir.path().join("failed-capture");
        let result = sandbox.pause_to_dir(&snapshot_dir).await;
        server.abort();

        let error = result.expect_err("a sandbox with no live VM cannot write a memory layer");
        assert!(
            !format!("{error:#}").contains("Failed to pause microVM"),
            "the capture must have failed after the pause, not at it: {error:#}"
        );
        assert_eq!(
            sandbox.mem_snapshot_parent_config_path, before,
            "a failed capture advanced the memory chain onto a layer it never wrote"
        );
        Ok(())
    }

    /// The memory layer written by each capture is a diff against the ranges
    /// dirtied since the previous one, and `/snapshot/create` resets that
    /// bitmap. Stacking a second capture onto the launch-time config instead of
    /// onto the first capture silently drops the first capture's delta, so the
    /// parent must be the live VM's most recent capture and must not be
    /// confused with the device key of the image currently backing it.
    #[test]
    fn memory_chain_parent_is_distinct_from_the_live_device_key() -> Result<()> {
        let resumed_from = PathBuf::from("/snapshots/resumed-from/mem_image.json");
        let snapshot = snapshot_config_with_mem_image("/snapshots/resumed-from/mem_image.json");

        let mut sandbox = FirecrackerSandbox::from_snapshot_config(&snapshot)?;
        sandbox.mem_snapshot_image_config_path =
            Some(PathBuf::from("/run/live-device/mem_image.json"));

        assert_eq!(
            sandbox.mem_snapshot_parent_config_path.as_deref(),
            Some(resumed_from.as_path()),
            "the live ublk device key must not be mistaken for the capture parent"
        );
        Ok(())
    }
}
