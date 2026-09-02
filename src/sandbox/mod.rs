mod access;
mod backend;
pub(crate) mod custom_extension;
mod envd;
mod extra_drive;
mod firecracker;
pub(crate) mod mock;
mod network;
mod process;
mod ublk;

use std::{collections::HashMap, path::PathBuf};

pub(crate) use custom_extension::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionParams,
};

use crate::types::{ImageConfigs, SandboxId};

pub use ::envd::process::Signal;
pub use access::{EnvdAccessToken, SandboxAccessTokenGenerator};
pub use backend::{
    CapturedSandboxSnapshot, CheckpointStats, GuestMemoryPosition, MemoryControlCapability,
    PausedSandboxState, RuntimeArtifactSet, SandboxBackend, SandboxBackendFactory,
    SandboxCaptureError, SandboxCaptureResult, SandboxExecutor, SandboxForkResult, SandboxForkSpec,
    SandboxMemoryTelemetry, SandboxRuntimeInfo,
};
pub use extra_drive::{
    normalize_mount_path_for_drive, validate_drive_id, validate_mount_path, validate_sub_path,
    ExtraDrive,
};
pub use firecracker::{
    FirecrackerCapturedSnapshot, FirecrackerCommonConfig, FirecrackerPausedState, FirecrackerPool,
    FirecrackerRuntimePolicy, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSandboxFactory, FirecrackerSnapshotConfig, FirecrackerSnapshotManifest,
};
pub use mock::MockBackendFactory;
pub(crate) use network::{prepare_runtime as prepare_network_runtime, NetworkManager};
pub use network::{
    BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
    ALL_INTERNET_TRAFFIC_CIDR,
};
pub use process::{Executor, ProcessHandle, ProcessOpts, ProcessOutput};

/// Current network slot capacity, as admission control reads it.
///
/// Returns zeroed capacity when networking has not been initialized, so
/// querying it never forces the global manager into existence as a side effect
/// of an admission decision.
/// Network slots available right now, for callers outside this module that
/// only need the number.
pub(crate) fn network_slot_capacity_available() -> usize {
    network_slot_capacity().available()
}

pub(crate) fn network_slot_capacity() -> network::NetworkSlotCapacity {
    network::NetworkManager::global_if_initialized()
        .map(|manager| manager.slot_capacity())
        .unwrap_or(network::NetworkSlotCapacity {
            total: 0,
            allocated: 0,
            pooled: 0,
        })
}
/// Fills the warm network slot pool before the node starts serving.
///
/// Exposed here rather than on the manager because the manager is crate
/// private; startup is the only caller.
pub async fn prime_network_slots(timeout: std::time::Duration) -> anyhow::Result<()> {
    network::NetworkManager::prime(timeout).await
}

pub use ublk::{OverlaybdConfig, UblkBackend, UblkConfig, UblkDaemonConfig, UblkDeviceManager};

#[derive(Clone, Debug)]
pub struct FreshSandboxBuildSpec {
    pub image_config_path: PathBuf,
    pub context: crate::snapshot::CommandContext,
    pub resources: crate::types::SandboxResources,
    pub extra_drives: Vec<ExtraDrive>,
    pub extra_boot_args: Option<String>,
}

/// High-level launch request consumed by sandbox backend factories.
///
/// Carries launch-time inputs from upper layers (for example orchestrator)
/// into backend construction.
#[derive(Clone, Debug, Default)]
pub struct SandboxLaunchConfig {
    /// Stable sandbox identity
    pub sandbox_id: SandboxId,
    /// Snapshot/template identity
    pub snapshot_id: String,
    /// One-off environment variable overrides to apply on top of snapshot defaults.
    pub env_vars: Option<HashMap<String, String>>,
    /// Per-sandbox egress policy.
    pub network: Option<SandboxNetworkPolicy>,
    /// Opaque extra key-value pairs merged into the MMDS metadata JSON.
    /// The sandbox layer does not interpret these; they are passed through
    /// as-is to the VM via the Firecracker MMDS interface.
    pub extra_mmds: serde_json::Map<String, serde_json::Value>,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Takes precedence over any value persisted in the source snapshot.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Runtime-only credential used by envd. The token is never serialized and
    /// its Debug representation is redacted.
    pub envd_access_token: Option<EnvdAccessToken>,
}

impl SandboxLaunchConfig {
    pub(crate) fn new(sandbox_id: SandboxId, snapshot_id: impl Into<String>) -> Self {
        Self {
            sandbox_id,
            snapshot_id: snapshot_id.into(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
        }
    }

    pub(crate) fn with_image_configs(mut self, image_configs: &ImageConfigs) -> Self {
        if !image_configs.is_empty() {
            self.extra_mmds
                .insert("imageConfigs".to_string(), image_configs.to_value());
        }
        self
    }
}

/// Which backend the node builds sandboxes with.
///
/// `Firecracker` is the product: a microVM per sandbox, which needs `/dev/kvm`
/// and `ublk_drv` on the host and refuses to start without them. `Mock` is the
/// same node — API, orchestrator, scheduler protocol, observability, mobility
/// records — with an in-process backend that runs no guest at all. It exists
/// so the control plane can be deployed, scaled, and exercised on hosts that
/// cannot virtualize, and it is never a fallback: a node asked for
/// `firecracker` on such a host stops rather than degrading into it.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum SandboxBackendKind {
    #[default]
    Firecracker,
    Mock,
}

impl std::fmt::Display for SandboxBackendKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Firecracker => "firecracker",
            Self::Mock => "mock",
        })
    }
}

impl std::str::FromStr for SandboxBackendKind {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "firecracker" => Ok(Self::Firecracker),
            "mock" => Ok(Self::Mock),
            other => Err(format!(
                "unsupported sandbox backend {other:?}; expected \"firecracker\" or \"mock\""
            )),
        }
    }
}

/// The factory a node runs with, chosen once at startup from `[machine].backend`.
///
/// An enum rather than a `Box<dyn SandboxBackendFactory>` so the orchestrator
/// stays monomorphic over it, exactly as it is over the Firecracker factory
/// today.
pub enum NodeBackendFactory {
    Firecracker(FirecrackerSandboxFactory),
    Mock(MockBackendFactory),
}

impl NodeBackendFactory {
    /// Every build in mock mode says so, because a mock sandbox looks like a
    /// real one from every API and it must never be mistaken for isolation.
    fn warn_mock_build(sandbox: &str) {
        tracing::warn!(
            target: "agentenv",
            sandbox,
            "building a MOCK sandbox: no guest runs and nothing is isolated; \
             this node is configured with [machine].backend = \"mock\""
        );
    }
}

impl SandboxBackendFactory for NodeBackendFactory {
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
    ) -> anyhow::Result<Box<dyn SandboxBackend>> {
        match self {
            Self::Firecracker(factory) => factory.build(build_spec, launch_config),
            Self::Mock(factory) => {
                Self::warn_mock_build("fresh");
                factory.build(build_spec, launch_config)
            }
        }
    }

    fn build_from_snapshot(
        &self,
        snapshot: &crate::snapshot::RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
    ) -> anyhow::Result<Box<dyn SandboxBackend>> {
        match self {
            Self::Firecracker(factory) => factory.build_from_snapshot(snapshot, launch_config),
            Self::Mock(factory) => {
                Self::warn_mock_build("from-snapshot");
                factory.build_from_snapshot(snapshot, launch_config)
            }
        }
    }

    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: serde_json::Value,
    ) -> anyhow::Result<std::sync::Arc<dyn PausedSandboxState>> {
        match self {
            Self::Firecracker(factory) => factory.decode_paused_state(artifact_root, state),
            Self::Mock(factory) => factory.decode_paused_state(artifact_root, state),
        }
    }

    fn build_from_paused_state(
        &self,
        sandbox_id: crate::types::SandboxId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> anyhow::Result<Box<dyn SandboxBackend>> {
        match self {
            Self::Firecracker(factory) => {
                factory.build_from_paused_state(sandbox_id, state, envd_access_token)
            }
            Self::Mock(factory) => {
                Self::warn_mock_build("from-paused-state");
                factory.build_from_paused_state(sandbox_id, state, envd_access_token)
            }
        }
    }
}

#[cfg(test)]
mod backend_kind_tests {
    use super::SandboxBackendKind;

    #[test]
    fn defaults_to_firecracker_and_parses_both_values() {
        assert_eq!(
            SandboxBackendKind::default(),
            SandboxBackendKind::Firecracker
        );
        assert_eq!(
            "Firecracker".parse::<SandboxBackendKind>().unwrap(),
            SandboxBackendKind::Firecracker
        );
        assert_eq!(
            "mock".parse::<SandboxBackendKind>().unwrap(),
            SandboxBackendKind::Mock
        );
        assert!("qemu".parse::<SandboxBackendKind>().is_err());
    }
}
