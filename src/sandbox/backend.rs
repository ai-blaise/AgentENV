//! Abstractions for sandbox backends.
//!
//! [`SandboxBackend`] represents the lifecycle of a single sandbox instance.
//! [`SandboxBackendFactory`] is responsible for constructing new sandbox
//! instances (from scratch, from a committed snapshot, or from paused state).

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::{
    EnvdAccessToken, Executor, FreshSandboxBuildSpec, ProcessHandle, ProcessOpts, ProcessOutput,
    SandboxLaunchConfig, SandboxNetworkPolicy,
};
use crate::sandbox::CustomExtensionParams;
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

/// A concrete sandbox backend's paused state.
///
/// The Orchestrator treats this value as completely opaque: it stores it in
/// [`SandboxMetadata`][crate::orchestrator::SandboxMetadata] after a
/// `pause` call and passes it back to
/// [`SandboxBackendFactory::build_from_paused_state`] when a resume is requested.
/// Concrete implementations own their serialized form.
pub trait PausedSandboxState: Any + fmt::Debug + Send + Sync + 'static {
    fn encode(&self) -> Result<Value>;

    /// Local artifacts this paused sandbox will reopen on resume.
    /// The orchestrator only carries this value to the image-liveness layer; it
    /// does not interpret the backend-specific artifact identities inside it.
    fn runtime_artifacts(&self) -> RuntimeArtifactSet;
    /// Effective envd control-plane port persisted with the paused runtime, when available.
    fn control_plane_port(&self) -> Option<u16> {
        None
    }
}

impl dyn PausedSandboxState {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: PausedSandboxState,
    {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SandboxCaptureError {
    #[error("{0}")]
    Recoverable(#[source] anyhow::Error),
    #[error("{0}")]
    Terminal(#[source] anyhow::Error),
}

impl SandboxCaptureError {
    pub fn recoverable(err: anyhow::Error) -> Self {
        Self::Recoverable(err)
    }

    pub fn terminal(err: anyhow::Error) -> Self {
        Self::Terminal(err)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

impl From<anyhow::Error> for SandboxCaptureError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<Self>() {
            Ok(snapshot_err) => snapshot_err,
            Err(err) => Self::Recoverable(err),
        }
    }
}

pub type SandboxCaptureResult<T> = std::result::Result<T, SandboxCaptureError>;
pub type SandboxForkResult = anyhow::Result<Box<dyn SandboxBackend>>;

#[derive(Clone, Debug)]
pub struct SandboxForkSpec {
    pub sandbox_id: SandboxId,
    pub envd_access_token: Option<EnvdAccessToken>,
}

/// Opaque set of local runtime artifacts a sandbox needs while it is alive.
///
/// Sandbox backends construct this from their runtime config, the orchestrator
/// carries it across lifecycle boundaries, and the image-liveness layer decides
/// how to protect the concrete local artifacts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeArtifactSet {
    overlaybd_image_config_paths: Vec<PathBuf>,
}

impl RuntimeArtifactSet {
    /// No local runtime artifacts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from overlaybd image configs whose local-only layers must stay
    /// available while the sandbox may reopen them.
    pub(crate) fn from_overlaybd_image_configs(overlaybd_image_config_paths: Vec<PathBuf>) -> Self {
        Self {
            overlaybd_image_config_paths,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.overlaybd_image_config_paths.is_empty()
    }

    pub(crate) fn into_overlaybd_image_config_paths(self) -> Vec<PathBuf> {
        self.overlaybd_image_config_paths
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxRuntimeInfo {
    pub rootfs_virtual_size: Option<u64>,
    pub runtime_artifacts: RuntimeArtifactSet,
    pub mem_control: MemoryControlCapability,
}

/// Which memory-control devices a *running* sandbox actually has.
///
/// Derived by probing the live VM, never by reading the config it was launched
/// with. The two do not agree: the balloon and virtio-mem devices are restored
/// from `vm_state.bin` on resume and are deliberately not re-configured, so a
/// sandbox captured before a device existed comes back without it however the
/// resuming node is configured. All-false is both the `Default` and the
/// fail-safe: every consumer treats a missing device as a permanent opt-out
/// and must keep working without it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryControlCapability {
    /// A virtio-balloon device is attached.
    pub balloon: bool,
    /// Guest statistics were armed before boot. They cannot be armed later.
    pub balloon_stats: bool,
    /// The free-page-hinting feature bit was set before boot.
    pub free_page_hinting: bool,
    /// A virtio-mem device is attached, so the guest RAM ceiling is movable.
    pub hotplug: bool,
}

impl MemoryControlCapability {
    /// Whether anything at all can be read or actuated for this sandbox.
    pub fn is_inert(&self) -> bool {
        *self == Self::default()
    }
}

/// One sample of a sandbox's memory position, guest side and device side.
///
/// Byte-valued fields come from the guest's own accounting, which is the only
/// source that does not double-count: sandboxes launched from one template
/// share a single memory ublk device, so a host-side per-process RSS counts the
/// same physical page once for every sandbox that faulted it in.
///
/// The sample is as fresh as the guest's last push, i.e. up to one
/// `stats_polling_interval_s` old, and the first sample after a resume may be
/// older than that.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SandboxMemoryTelemetry {
    /// Guest estimate of memory available for new work without swapping.
    pub available_bytes: Option<u64>,
    /// Guest total usable memory, i.e. the current RAM ceiling.
    pub total_bytes: Option<u64>,
    /// Reclaimable page cache. Counted inside `available_bytes`.
    pub disk_caches_bytes: Option<u64>,
    /// Cumulative guest OOM-killer invocations.
    pub oom_kills: Option<u64>,
    /// Cumulative allocations that fell into the slow path for want of a page.
    pub alloc_stalls: Option<u64>,
    /// Current virtio-mem plugged size, when the device is present.
    pub plugged_mib: Option<u32>,
    /// Current virtio-mem requested size, when the device is present.
    pub requested_mib: Option<u32>,
}

/// The guest-side position a control policy can actually act on.
///
/// Only constructible from a sample in which the guest reported both figures,
/// so a policy holding one of these is never reasoning about numbers the guest
/// never sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestMemoryPosition {
    /// Share of the guest's total memory it reports as available, 0..=100.
    pub available_percent: u64,
    /// The guest's total usable memory, rounded up.
    pub total_mib: u32,
}

impl SandboxMemoryTelemetry {
    /// The guest's position, or `None` when this sample cannot describe one.
    ///
    /// Every field is optional because every field is: the balloon statistics
    /// a guest pushes are gated on its own kernel, so a VM whose rootfs has a
    /// virtio_balloon driver that never reports `S_MEMTOT` pushes a sample
    /// with the total absent for its whole life. Absent is not zero and not
    /// full — reading it as either invents a pressure signal the guest never
    /// sent — so a sample that cannot answer says so.
    pub fn guest_position(&self) -> Option<GuestMemoryPosition> {
        let total_bytes = self.total_bytes.filter(|total| *total > 0)?;
        Some(GuestMemoryPosition {
            available_percent: self.available_bytes?.saturating_mul(100) / total_bytes,
            total_mib: crate::types::bytes_to_mib_ceil(total_bytes),
        })
    }

    /// Whether the guest reports neither of the allocation-distress counters.
    ///
    /// Both are gated on the guest kernel version, so a guest that reports
    /// neither can only ever be judged on its available ratio: the "grow
    /// within one tick of an allocation stall" arm can never fire for it.
    pub fn is_blind_to_distress(&self) -> bool {
        self.oom_kills.is_none() && self.alloc_stalls.is_none()
    }
}

/// Opaque captured snapshot artifacts produced from a running sandbox.
///
/// Unlike [`PausedSandboxState`], this value is intended for one-shot
/// consumption by snapshot publication code. Concrete backends may use it to
/// keep temporary artifact directories alive until publication finishes.
pub struct CapturedSandboxSnapshot {
    inner: Box<dyn Any + Send>,
}

impl CapturedSandboxSnapshot {
    pub fn new<T>(snapshot: T) -> Self
    where
        T: Send + 'static,
    {
        Self {
            inner: Box::new(snapshot),
        }
    }

    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + 'static,
    {
        self.inner.downcast_ref::<T>()
    }

    pub fn downcast<T>(self) -> std::result::Result<T, Self>
    where
        T: Send + 'static,
    {
        match self.inner.downcast::<T>() {
            Ok(inner) => Ok(*inner),
            Err(inner) => Err(Self { inner }),
        }
    }
}

impl fmt::Debug for CapturedSandboxSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedSandboxSnapshot")
            .field("opaque", &true)
            .finish()
    }
}

/// Lifecycle interface for a single sandbox instance.
///
/// Implementors must be `Send + 'static` so that they can be stored inside
/// `Arc<Mutex<Box<dyn SandboxBackend>>>` handles managed by the Orchestrator.
#[async_trait]
pub trait SandboxBackend: Send + 'static {
    /// Start the sandbox and block until readiness.
    async fn start(&mut self) -> Result<()>;

    /// Start the sandbox without waiting for the sandbox to become ready.
    async fn start_nowait(&mut self) -> Result<()>;

    /// Block until the sandbox signals readiness.
    ///
    /// Should be called after [`start_nowait`][Self::start_nowait] before any
    /// workload is submitted.
    async fn wait_for_ready(&self) -> Result<()>;

    /// Pause the sandbox and capture its state for later resume.
    ///
    /// After this call the caller is expected to invoke [`stop`][Self::stop]
    /// to release system resources; the paused state encapsulates everything
    /// needed to resume the sandbox later via
    /// [`SandboxBackendFactory::build_from_paused_state`].
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    ///
    /// For simplicity, [`SandboxCaptureError::Recoverable`] must guarantee the sandbox
    /// has already been restored to a running state before the error is returned.
    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>>;

    /// Resume a paused but not-yet-stopped sandbox from its snapshot.
    ///
    /// Idempotent: calling `resume` more than once must not return an error.
    async fn resume(&mut self) -> Result<()>;

    /// Capture a persistent snapshot from a running sandbox.
    ///
    /// After this call the sandbox is expected to continue running.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot>;

    /// Fork this running sandbox into ready child backends.
    ///
    /// The outer error is reserved for failures before child startup begins.
    /// After the source has been restored, implementations must attempt every
    /// child concurrently and return one result per `spec` entry in the
    /// same order. Successful children stay running when a sibling fails.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates the fork attempt mutated the
    /// source runtime past safe resume, so callers must stop treating the
    /// source as runnable. Child construction/start failures after source
    /// recovery belong in the corresponding [`SandboxForkResult`].
    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>>;

    /// Stop the sandbox and release all associated system resources.
    ///
    /// Idempotent: calling `stop` more than once must not return an error.
    async fn stop(&mut self) -> Result<()>;

    /// Obtain the IP address that the sandbox can use to interact with the host.
    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr>;

    /// Return runtime facts that are only known after the backend has started.
    fn runtime_info(&self) -> SandboxRuntimeInfo;

    /// Local runtime artifacts this sandbox opens on start.
    fn startup_artifacts(&self) -> RuntimeArtifactSet;

    /// Update the sandbox network policy at runtime.
    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()>;

    /// Sample this sandbox's memory position.
    ///
    /// `Ok(None)` means the backend has no device that can answer — no
    /// balloon, statistics never armed, or the VM is not up — and is a
    /// permanent, unexceptional opt-out rather than a failure. `Err` is
    /// reserved for a device that should have answered and did not.
    async fn memory_telemetry(&self) -> Result<Option<SandboxMemoryTelemetry>>;

    /// Move this sandbox's guest RAM ceiling to `mib` of hot-pluggable memory.
    ///
    /// Only meaningful when [`MemoryControlCapability::hotplug`] is set;
    /// callers must check first, because a backend with no virtio-mem device
    /// has no way to honour the request and says so rather than silently
    /// succeeding.
    async fn set_memory_plug_target(&mut self, mib: u32) -> Result<()>;

    /// Update the custom extension params held by the sandbox runtime.
    ///
    /// Plain assignment of an already-approved value: the custom extension
    /// patch-params hook is invoked by the caller (orchestrator layer), not
    /// by the backend. Cannot fail.
    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>);
}

/// Factory interface for creating and restoring sandbox backend instances.
///
/// A single factory instance is stored inside the
/// [`Orchestrator`][crate::orchestrator::Orchestrator] and is used for every
/// `create_sandbox` and `resume_sandbox` request.
pub trait SandboxBackendFactory: Send + Sync + 'static {
    /// Build a brand-new sandbox backend from a high-level launch request.
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Build a sandbox backend from a runnable committed snapshot plus launch request.
    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Decode backend-specific paused state loaded from persistence.
    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>>;

    /// Build a sandbox backend from backend-specific paused state captured by `pause`.
    fn build_from_paused_state(
        &self,
        sandbox_id: crate::types::SandboxId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>>;
}

/// Process execution capability of a running sandbox.
///
/// Implement [`executor`][Self::executor] to provide a [`ProcessClient`][envd::process::ProcessClient]-backed
/// [`Executor`]. The three convenience methods (`run_command`,
/// `run_command_with_opts`, `start_process`) have default implementations that
/// simply call `self.executor()?` and delegate, so callers can continue using
/// the familiar `sandbox.run_command(...)` pattern without boilerplate.
///
/// # Note on `Send`
/// `&Self` may be `!Send` (e.g. `FirecrackerSandbox` holds tonic clients that
/// are `!Sync`), so the generated futures are not required to be `Send`.
#[async_trait(?Send)]
pub trait SandboxExecutor: Send {
    /// Obtain a process executor backed by this sandbox's envd connection.
    ///
    /// Returns an error if the sandbox is not running.
    fn executor(&self) -> Result<Executor<'_>>;

    /// Run a command inside the sandbox and wait for it to complete.
    ///
    /// Returns the captured stdout, stderr, and exit code.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let output = sandbox.run_command("echo", &["hello", "world"]).await?;
    /// assert_eq!(output.exit_code, 0);
    /// println!("{}", output.stdout);
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
        self.executor()?.run_command(cmd, args).await
    }

    /// Run a command with custom options and wait for it to complete.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
    /// use std::collections::HashMap;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let opts = ProcessOpts::new().with_cwd("/tmp");
    /// let output = sandbox.run_command_with_opts("ls", &["-la"], &opts).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command_with_opts(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessOutput> {
        self.executor()?
            .run_command_with_opts(cmd, args, opts)
            .await
    }

    /// Create a directory (and any missing parents) inside the sandbox.
    ///
    /// Goes through envd's filesystem service rather than exec'ing a binary,
    /// so it works in images that ship no userland (scratch, distroless).
    /// An already-existing directory is not an error.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// sandbox.create_dir_all("/home/user/work").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_dir_all(&self, path: &str) -> Result<()> {
        self.executor()?.create_dir_all(path).await
    }

    /// Start a long-running process and return a handle.
    ///
    /// # Example
    /// ```no_run
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let mut handle = sandbox.start_process("cat", &[], &ProcessOpts::default()).await?;
    /// handle.send_stdin(b"hello\n").await?;
    /// handle.kill().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn start_process(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessHandle> {
        self.executor()?.start_process(cmd, args, opts).await
    }
}
