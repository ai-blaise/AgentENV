//! Deciding whether a paused sandbox may resume on a different node.
//!
//! A snapshot is not portable in general. It captures a guest that was booted
//! against one kernel, one Firecracker build, one CPU feature set and one tools
//! drive, and resuming it against different ones ranges from "works" to
//! "guest faults on an instruction that no longer exists". Nothing about the
//! failure is graceful, and some of it surfaces long after resume.
//!
//! So compatibility is decided before anything is moved, by comparing a
//! fingerprint of what the snapshot was produced against with the same
//! fingerprint of the candidate node. A mismatch makes the sandbox
//! non-migratable — never partially migrated.
//!
//! # Why an explicit field list rather than a hash
//!
//! A single opaque hash would be smaller and would compare in one step, but it
//! cannot say *why* two nodes are incompatible. Operationally that matters more
//! than the comparison cost: "this fleet cannot migrate" is not actionable,
//! whereas "kernel differs: 6.1.175 vs 6.1.190" is. The fields are compared
//! individually and the first mismatch is reported.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::cfg::SnapshotRepositoryBackendKind;
use crate::snapshot::{OverlaybdLayerRef, SnapshotRuntimeVersions};
use crate::virtualization::VirtualizationMode;

/// The properties a paused sandbox depends on its host providing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationFingerprint {
    /// Guest architecture. A cross-architecture move is not a migration.
    pub cpu_architecture: String,
    /// KVM or PVM. Snapshots can only be restored in the mode that produced
    /// them, which the resume path already enforces locally.
    pub virtualization_mode: VirtualizationMode,
    /// Cluster-wide CPU feature intersection applied at boot.
    ///
    /// `None` means the node booted the guest with its own CPU features rather
    /// than a normalized set, which makes the snapshot node-specific: another
    /// node may not implement what the guest has already been told it has.
    pub cpu_config_json: Option<String>,
    pub kernel_version: String,
    pub firecracker_version: String,
    /// The tools drive carries the in-guest init and agent, and a snapshot
    /// records the version it was built against.
    pub tools_drive_version: String,
    /// Memory page size the snapshot's dirty-tracking and layer geometry assume.
    pub memory_page_size: u32,
}

impl MigrationFingerprint {
    pub fn from_runtime(
        versions: &SnapshotRuntimeVersions,
        cpu_architecture: impl Into<String>,
        virtualization_mode: VirtualizationMode,
        cpu_config_json: Option<String>,
        memory_page_size: u32,
    ) -> Self {
        Self {
            cpu_architecture: cpu_architecture.into(),
            virtualization_mode,
            cpu_config_json,
            kernel_version: versions.kernel_version.clone(),
            firecracker_version: versions.firecracker_version.clone(),
            tools_drive_version: versions.tools_drive_version.clone(),
            memory_page_size,
        }
    }

    /// Reports why this snapshot cannot resume against `host`, or `None` when
    /// it can.
    pub fn incompatibility_with(&self, host: &Self) -> Option<MigrationIncompatibility> {
        use MigrationIncompatibility as Reason;

        if self.cpu_architecture != host.cpu_architecture {
            return Some(Reason::CpuArchitecture {
                snapshot: self.cpu_architecture.clone(),
                host: host.cpu_architecture.clone(),
            });
        }
        if self.virtualization_mode != host.virtualization_mode {
            return Some(Reason::VirtualizationMode {
                snapshot: self.virtualization_mode,
                host: host.virtualization_mode,
            });
        }
        if self.kernel_version != host.kernel_version {
            return Some(Reason::KernelVersion {
                snapshot: self.kernel_version.clone(),
                host: host.kernel_version.clone(),
            });
        }
        if self.firecracker_version != host.firecracker_version {
            return Some(Reason::FirecrackerVersion {
                snapshot: self.firecracker_version.clone(),
                host: host.firecracker_version.clone(),
            });
        }
        if self.tools_drive_version != host.tools_drive_version {
            return Some(Reason::ToolsDriveVersion {
                snapshot: self.tools_drive_version.clone(),
                host: host.tools_drive_version.clone(),
            });
        }
        if self.memory_page_size != host.memory_page_size {
            return Some(Reason::MemoryPageSize {
                snapshot: self.memory_page_size,
                host: host.memory_page_size,
            });
        }

        // CPU features are the one field where "absent" is a failure rather
        // than a wildcard. A snapshot booted without a normalized feature set
        // was told about whatever its origin node happened to have, so there is
        // no way to know another node implements it.
        match (
            self.cpu_config_json.as_deref(),
            host.cpu_config_json.as_deref(),
        ) {
            (Some(snapshot), Some(host_config)) if snapshot == host_config => None,
            (Some(snapshot), Some(host_config)) => Some(Reason::CpuFeatures {
                detail: format!(
                    "snapshot and host apply different CPU templates ({} vs {} bytes)",
                    snapshot.len(),
                    host_config.len()
                ),
            }),
            (None, _) => Some(Reason::CpuFeatures {
                detail: "snapshot was booted with node-local CPU features".to_string(),
            }),
            (_, None) => Some(Reason::CpuFeatures {
                detail: "candidate node has no cluster CPU template".to_string(),
            }),
        }
    }

    /// Whether this snapshot can resume against `host`.
    pub fn is_compatible_with(&self, host: &Self) -> bool {
        self.incompatibility_with(host).is_none()
    }
}

/// Why a snapshot cannot resume on a candidate node.
///
/// A closed set: the discriminant is safe as a metric label, and each variant
/// carries both sides so the message is actionable without a second lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationIncompatibility {
    CpuArchitecture {
        snapshot: String,
        host: String,
    },
    VirtualizationMode {
        snapshot: VirtualizationMode,
        host: VirtualizationMode,
    },
    KernelVersion {
        snapshot: String,
        host: String,
    },
    FirecrackerVersion {
        snapshot: String,
        host: String,
    },
    ToolsDriveVersion {
        snapshot: String,
        host: String,
    },
    MemoryPageSize {
        snapshot: u32,
        host: u32,
    },
    CpuFeatures {
        detail: String,
    },
}

impl MigrationIncompatibility {
    /// Stable label for metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::CpuArchitecture { .. } => "cpu_architecture",
            Self::VirtualizationMode { .. } => "virtualization_mode",
            Self::KernelVersion { .. } => "kernel_version",
            Self::FirecrackerVersion { .. } => "firecracker_version",
            Self::ToolsDriveVersion { .. } => "tools_drive_version",
            Self::MemoryPageSize { .. } => "memory_page_size",
            Self::CpuFeatures { .. } => "cpu_features",
        }
    }
}

impl fmt::Display for MigrationIncompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CpuArchitecture { snapshot, host } => {
                write!(f, "cpu architecture differs: {snapshot} vs {host}")
            }
            Self::VirtualizationMode { snapshot, host } => {
                write!(f, "virtualization mode differs: {snapshot} vs {host}")
            }
            Self::KernelVersion { snapshot, host } => {
                write!(f, "kernel differs: {snapshot} vs {host}")
            }
            Self::FirecrackerVersion { snapshot, host } => {
                write!(f, "firecracker differs: {snapshot} vs {host}")
            }
            Self::ToolsDriveVersion { snapshot, host } => {
                write!(f, "tools drive differs: {snapshot} vs {host}")
            }
            Self::MemoryPageSize { snapshot, host } => {
                write!(f, "memory page size differs: {snapshot} vs {host}")
            }
            Self::CpuFeatures { detail } => write!(f, "cpu features are not portable: {detail}"),
        }
    }
}

/// Whether the artifacts a snapshot is made of can be read from another node.
///
/// This is a property of the repository backend, not of any individual layer:
/// a POSIX repository is a directory on one machine's disk, and an object-store
/// repository is reachable from the whole cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactReach {
    /// Every node can fetch the artifact by itself.
    ClusterShared,
    /// Only the node that wrote it can read it.
    NodeLocal,
}

impl From<SnapshotRepositoryBackendKind> for ArtifactReach {
    fn from(kind: SnapshotRepositoryBackendKind) -> Self {
        match kind {
            SnapshotRepositoryBackendKind::Oss => Self::ClusterShared,
            SnapshotRepositoryBackendKind::PosixFs => Self::NodeLocal,
        }
    }
}

/// Whether a drive travels with its sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriveMobility {
    /// Every layer is fetchable from the destination.
    Portable,
    /// At least one layer exists only on the source node.
    ///
    /// A migration that proceeded anyway would resume a guest whose disk
    /// silently fails its first read past the shared layers — worse than not
    /// migrating, because the sandbox is by then already gone from the source.
    NodeLocal { detail: String },
}

/// Classifies one drive's layer stack against the repository's reach.
///
/// External layers name a registry blob and are fetchable anywhere; managed
/// layers live in the repository, so their reach is the repository's.
pub fn classify_layers(reach: ArtifactReach, layers: &[OverlaybdLayerRef]) -> DriveMobility {
    if reach == ArtifactReach::ClusterShared {
        return DriveMobility::Portable;
    }

    let managed = layers
        .iter()
        .filter(|layer| matches!(layer, OverlaybdLayerRef::Managed(_)))
        .count();
    if managed == 0 {
        return DriveMobility::Portable;
    }

    DriveMobility::NodeLocal {
        detail: format!(
            "{managed} of {} layers are managed by a node-local repository",
            layers.len()
        ),
    }
}

/// Why a sandbox cannot be moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MobilityBlocker {
    /// The destination cannot run this guest.
    Runtime(MigrationIncompatibility),
    /// The destination cannot read this guest's disks or memory image.
    Artifacts { drive_id: String, detail: String },
}

impl MobilityBlocker {
    /// Stable label for metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Runtime(reason) => reason.kind(),
            Self::Artifacts { .. } => "artifacts",
        }
    }
}

impl fmt::Display for MobilityBlocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(reason) => write!(f, "{reason}"),
            Self::Artifacts { drive_id, detail } => {
                write!(f, "drive {drive_id} is not portable: {detail}")
            }
        }
    }
}

/// One drive as the mobility check sees it.
pub struct DriveForMigration<'a> {
    pub drive_id: &'a str,
    pub layers: &'a [OverlaybdLayerRef],
}

/// Decides whether a paused sandbox may be moved to a node with `host`.
///
/// Returns the first blocker rather than a list. The alternative — enumerate
/// everything wrong — reads well in a report but invites acting on a partial
/// fix, and the caller's only decision is binary anyway.
///
/// Runtime compatibility is checked before artifact reach, because a runtime
/// mismatch is a property of the destination worth reporting even when the
/// artifacts happen to also be unreachable: it rules out that destination
/// permanently, while unreachable artifacts rule out migration entirely.
pub fn assess_mobility(
    snapshot: &MigrationFingerprint,
    host: &MigrationFingerprint,
    reach: ArtifactReach,
    rootfs_layers: &[OverlaybdLayerRef],
    attached_drives: &[DriveForMigration<'_>],
) -> Result<(), MobilityBlocker> {
    if let Some(reason) = snapshot.incompatibility_with(host) {
        return Err(MobilityBlocker::Runtime(reason));
    }

    // The memory image and vm_state.bin live beside the rootfs layers in the
    // same repository, so the rootfs verdict covers them too.
    if let DriveMobility::NodeLocal { detail } = classify_layers(reach, rootfs_layers) {
        return Err(MobilityBlocker::Artifacts {
            drive_id: "rootfs".to_string(),
            detail,
        });
    }

    for drive in attached_drives {
        if let DriveMobility::NodeLocal { detail } = classify_layers(reach, drive.layers) {
            return Err(MobilityBlocker::Artifacts {
                drive_id: drive.drive_id.to_string(),
                detail,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions() -> SnapshotRuntimeVersions {
        SnapshotRuntimeVersions {
            kernel_version: "vmlinux-6.1.175".to_string(),
            firecracker_version: "1.15.1-patch-v1".to_string(),
            envd_version: "0.5.15".to_string(),
            tools_drive_version: "0.1.0".to_string(),
        }
    }

    fn fingerprint() -> MigrationFingerprint {
        MigrationFingerprint::from_runtime(
            &versions(),
            "x86_64",
            VirtualizationMode::Kvm,
            Some(r#"{"cpuid_modifiers":[]}"#.to_string()),
            4096,
        )
    }

    #[test]
    fn identical_runtimes_are_compatible() {
        let snapshot = fingerprint();
        let host = fingerprint();
        assert_eq!(snapshot.incompatibility_with(&host), None);
        assert!(snapshot.is_compatible_with(&host));
    }

    #[test]
    fn each_field_blocks_migration_and_names_itself() {
        let base = fingerprint();

        let cases: Vec<(MigrationFingerprint, &str)> = vec![
            (
                MigrationFingerprint {
                    cpu_architecture: "aarch64".to_string(),
                    ..base.clone()
                },
                "cpu_architecture",
            ),
            (
                MigrationFingerprint {
                    virtualization_mode: VirtualizationMode::Pvm,
                    ..base.clone()
                },
                "virtualization_mode",
            ),
            (
                MigrationFingerprint {
                    kernel_version: "vmlinux-6.1.190".to_string(),
                    ..base.clone()
                },
                "kernel_version",
            ),
            (
                MigrationFingerprint {
                    firecracker_version: "1.16.0".to_string(),
                    ..base.clone()
                },
                "firecracker_version",
            ),
            (
                MigrationFingerprint {
                    tools_drive_version: "0.2.0".to_string(),
                    ..base.clone()
                },
                "tools_drive_version",
            ),
            (
                MigrationFingerprint {
                    memory_page_size: 2 * 1024 * 1024,
                    ..base.clone()
                },
                "memory_page_size",
            ),
        ];

        for (host, want_kind) in cases {
            let reason = base
                .incompatibility_with(&host)
                .unwrap_or_else(|| panic!("{want_kind} mismatch should block migration"));
            assert_eq!(reason.kind(), want_kind, "reported {reason}");
            // The message must name both sides so an operator can act on it
            // without a second lookup.
            assert!(
                reason.to_string().contains("differs"),
                "message should say what differs: {reason}"
            );
        }
    }

    /// A snapshot booted without a normalized CPU template was told about
    /// whatever its origin node had, so there is no basis for believing another
    /// node implements the same thing. Absent must fail closed, not act as a
    /// wildcard.
    #[test]
    fn missing_cpu_template_blocks_migration() {
        let host = fingerprint();

        let snapshot = MigrationFingerprint {
            cpu_config_json: None,
            ..fingerprint()
        };
        let reason = snapshot
            .incompatibility_with(&host)
            .expect("a node-local CPU feature set is not portable");
        assert_eq!(reason.kind(), "cpu_features");

        let bare_host = MigrationFingerprint {
            cpu_config_json: None,
            ..fingerprint()
        };
        let reason = fingerprint()
            .incompatibility_with(&bare_host)
            .expect("a host with no cluster template cannot accept a normalized snapshot");
        assert_eq!(reason.kind(), "cpu_features");
    }

    #[test]
    fn differing_cpu_templates_block_migration() {
        let host = MigrationFingerprint {
            cpu_config_json: Some(r#"{"cpuid_modifiers":[{"leaf":"0x1"}]}"#.to_string()),
            ..fingerprint()
        };
        let reason = fingerprint()
            .incompatibility_with(&host)
            .expect("different CPU templates are not interchangeable");
        assert_eq!(reason.kind(), "cpu_features");
    }

    /// Architecture is checked before anything else: reporting a kernel
    /// mismatch on a cross-architecture pair would be true but useless.
    #[test]
    fn the_most_fundamental_mismatch_is_reported_first() {
        let host = MigrationFingerprint {
            cpu_architecture: "aarch64".to_string(),
            kernel_version: "vmlinux-9.9.9".to_string(),
            firecracker_version: "9.9.9".to_string(),
            ..fingerprint()
        };
        let reason = fingerprint().incompatibility_with(&host).expect("mismatch");
        assert_eq!(reason.kind(), "cpu_architecture");
    }

    use crate::snapshot::{ExternalLayer, ManagedLayer};

    fn managed(digest: &str) -> OverlaybdLayerRef {
        OverlaybdLayerRef::Managed(ManagedLayer {
            digest: digest.to_string(),
            size: 1024,
            uuid: None,
        })
    }

    fn external(digest: &str) -> OverlaybdLayerRef {
        OverlaybdLayerRef::External(ExternalLayer {
            digest: digest.to_string(),
            repo_blob_url: format!("registry.example/repo/blobs/{digest}"),
            size: 1024,
        })
    }

    #[test]
    fn object_storage_makes_every_layer_portable() {
        let reach = ArtifactReach::from(SnapshotRepositoryBackendKind::Oss);
        assert_eq!(reach, ArtifactReach::ClusterShared);
        assert_eq!(
            classify_layers(reach, &[managed("sha256:a"), external("sha256:b")]),
            DriveMobility::Portable
        );
    }

    /// A managed layer in a POSIX repository is a file on one machine. Treating
    /// it as portable would resume a guest whose disk fails its first read past
    /// the shared layers, after the source copy is already gone.
    #[test]
    fn posix_managed_layers_are_not_portable() {
        let reach = ArtifactReach::from(SnapshotRepositoryBackendKind::PosixFs);
        assert_eq!(reach, ArtifactReach::NodeLocal);
        let verdict = classify_layers(reach, &[external("sha256:a"), managed("sha256:b")]);
        let DriveMobility::NodeLocal { detail } = verdict else {
            panic!("a node-local managed layer must block migration");
        };
        assert!(detail.contains('1'), "should count the layers: {detail}");
    }

    /// A snapshot whose layers all came from a registry is portable even out of
    /// a POSIX repository: nothing needs the source node's disk.
    #[test]
    fn posix_with_only_external_layers_is_portable() {
        let reach = ArtifactReach::NodeLocal;
        assert_eq!(
            classify_layers(reach, &[external("sha256:a"), external("sha256:b")]),
            DriveMobility::Portable
        );
    }

    #[test]
    fn a_fully_shared_sandbox_is_movable() {
        assert_eq!(
            assess_mobility(
                &fingerprint(),
                &fingerprint(),
                ArtifactReach::ClusterShared,
                &[managed("sha256:root")],
                &[DriveForMigration {
                    drive_id: "data",
                    layers: &[managed("sha256:data")],
                }],
            ),
            Ok(())
        );
    }

    /// An unreachable attached drive blocks the whole move. Migrating the
    /// sandbox without one of its disks is not a degraded migration, it is a
    /// broken sandbox.
    #[test]
    fn one_unreachable_attached_drive_blocks_the_sandbox() {
        let blocker = assess_mobility(
            &fingerprint(),
            &fingerprint(),
            ArtifactReach::NodeLocal,
            &[external("sha256:root")],
            &[
                DriveForMigration {
                    drive_id: "shared",
                    layers: &[external("sha256:shared")],
                },
                DriveForMigration {
                    drive_id: "scratch",
                    layers: &[managed("sha256:scratch")],
                },
            ],
        )
        .expect_err("a node-local drive must block the move");

        assert_eq!(blocker.kind(), "artifacts");
        assert!(
            blocker.to_string().contains("scratch"),
            "the blocker must name the drive: {blocker}"
        );
    }

    /// Runtime incompatibility is reported ahead of artifact reach: it rules
    /// out one destination, while unreachable artifacts rule out all of them,
    /// and an operator choosing a destination needs the former first.
    #[test]
    fn runtime_mismatch_outranks_artifact_reach() {
        let host = MigrationFingerprint {
            kernel_version: "vmlinux-6.1.190".to_string(),
            ..fingerprint()
        };
        let blocker = assess_mobility(
            &fingerprint(),
            &host,
            ArtifactReach::NodeLocal,
            &[managed("sha256:root")],
            &[],
        )
        .expect_err("both halves are unsatisfied");
        assert_eq!(blocker.kind(), "kernel_version");
    }
}
