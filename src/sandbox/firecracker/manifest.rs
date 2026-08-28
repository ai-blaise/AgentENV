use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use super::config::FirecrackerSnapshotConfig;
use crate::sandbox::ublk::OverlaybdConfig;
use crate::sandbox::ExtraDrive;

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Manifest describing the on-disk layout of a Firecracker snapshot.
///
/// This is intentionally decoupled from in-memory snapshot representations.
/// Snapshot-layer retrieve artifacts based on the manifest during snapshot
/// publication, and reconstruct the manifest with hydrated paths during snapshot resolution.
///
/// All paths in the manifest should be absolute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerSnapshotManifest {
    /// Schema/version marker for persisted manifest format.
    pub version: u32,
    pub vm_state: FirecrackerVmStateArtifacts,
    pub memory: FirecrackerMemoryArtifacts,
    pub rootfs: FirecrackerRootfsArtifacts,
    pub attached_drives: Vec<FirecrackerAttachedDriveArtifacts>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerVmStateArtifacts {
    #[serde(skip)]
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerMemoryArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerRootfsArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerAttachedDriveArtifacts {
    pub drive_id: String,
    pub read_only: bool,
    #[serde(default)]
    pub mount_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<PathBuf>,
    pub virtual_size: u64,
    #[serde(skip)]
    pub image_config_path: PathBuf,
}

impl FirecrackerSnapshotManifest {
    pub fn new(
        vm_state_path: impl Into<PathBuf>,
        mem_image_config_path: impl Into<PathBuf>,
        mem_virtual_size: u64,
        rootfs_image_config_path: impl Into<PathBuf>,
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> Result<Self> {
        Self {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: vm_state_path.into(),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: mem_image_config_path.into(),
                virtual_size: mem_virtual_size,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: rootfs_image_config_path.into(),
                virtual_size: rootfs_virtual_size,
            },
            attached_drives: Vec::new(),
        }
        .with_extra_drives(attached_drives)
    }

    pub fn extra_drives(&self) -> Vec<ExtraDrive> {
        self.attached_drives
            .iter()
            .map(|drive| ExtraDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: Some(drive.virtual_size),
                mount_path: crate::sandbox::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            })
            .collect()
    }

    pub fn with_extra_drives(&self, extra_drives: &[ExtraDrive]) -> Result<Self> {
        let mut new = self.clone();
        new.attached_drives = extra_drives
            .iter()
            .map(|drive| {
                let virtual_size = drive.virtual_size().ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot attached drive '{}' virtual size must be known",
                        drive.drive_id()
                    )
                })?;
                if virtual_size == 0 {
                    bail!(
                        "snapshot attached drive '{}' virtual size must be non-zero",
                        drive.drive_id()
                    );
                }
                Ok(FirecrackerAttachedDriveArtifacts {
                    drive_id: drive.drive_id().to_string(),
                    read_only: drive.read_only(),
                    mount_path: drive.mount_path().to_path_buf(),
                    sub_path: drive.sub_path().map(Path::to_path_buf),
                    virtual_size,
                    image_config_path: drive.image_config_path().to_path_buf(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(new)
    }
}

#[cfg(test)]
#[doc(hidden)]
impl FirecrackerSnapshotManifest {
    pub(crate) fn for_test(
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> FirecrackerSnapshotManifest {
        let mut manifest = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            0,
            "rootfs/image.json",
            rootfs_virtual_size,
            attached_drives,
        )
        .expect("test snapshot attached drive virtual size must be known");

        for drive in &mut manifest.attached_drives {
            drive.image_config_path = PathBuf::from("drives")
                .join(&drive.drive_id)
                .join("image.json");
        }

        manifest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attached_drive_virtual_size_is_required() {
        let err = serde_json::from_value::<FirecrackerAttachedDriveArtifacts>(serde_json::json!({
            "driveId": "data",
            "readOnly": true,
            "mountPath": "/mnt/data"
        }))
        .expect_err("attached drive artifact should require virtualSize");

        assert!(err.to_string().contains("virtualSize"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized_and_mapped_to_runtime_input() {
        let known = FirecrackerAttachedDriveArtifacts {
            drive_id: "data".to_string(),
            read_only: true,
            mount_path: PathBuf::from("/mnt/data"),
            sub_path: None,
            virtual_size: 4096,
            image_config_path: PathBuf::from("drives/data/image.json"),
        };

        let known_json = serde_json::to_value(&known).unwrap();
        assert_eq!(known_json["virtualSize"], serde_json::json!(4096));

        let manifest = FirecrackerSnapshotManifest {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: PathBuf::from("vm_state.bin"),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: PathBuf::from("mem_image.json"),
                virtual_size: 4096,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: PathBuf::from("rootfs/image.json"),
                virtual_size: 4096,
            },
            attached_drives: vec![known],
        };

        let drives = manifest.extra_drives();
        assert_eq!(drives[0].virtual_size(), Some(4096));
    }

    #[test]
    fn new_rejects_attached_drive_without_virtual_size() {
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: None,
            sub_path: None,
        };

        let err = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[drive],
        )
        .expect_err("snapshot attached drive virtual size should be required");

        assert!(err.to_string().contains("virtual size must be known"));
    }

    #[test]
    fn with_extra_drives_rejects_zero_virtual_size() {
        let manifest = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[],
        )
        .expect("empty attached drives should be valid");
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: Some(0),
            sub_path: None,
        };

        let err = manifest
            .with_extra_drives(&[drive])
            .expect_err("snapshot attached drive virtual size should be non-zero");

        assert!(err.to_string().contains("virtual size must be non-zero"));
    }
}

impl FirecrackerSnapshotConfig {
    /// Describes a paused sandbox's artifacts as a publishable manifest.
    ///
    /// A paused sandbox already has everything a committed snapshot needs —
    /// the VM state file, the memory image, the rootfs and every attached
    /// drive — written to disk by the pause. What it lacks is a repository
    /// entry, which is why a paused sandbox cannot move: its artifacts are
    /// node-local files no other node can read.
    ///
    /// This is the conversion that lets it be published. Nothing is copied or
    /// moved here; the manifest points at the live paused artifacts, and the
    /// repository's import copies or hard-links them, so the sandbox stays
    /// resumable on this node either way.
    pub fn to_publishable_manifest(&self) -> Result<FirecrackerSnapshotManifest> {
        publishable_manifest(
            &self.vm_state_path,
            &self.mem_overlaybd_config,
            self.mem_virtual_size,
            self.common.rootfs_image_config.as_ref(),
            self.common.rootfs_virtual_size,
            &self.common.extra_drives,
        )
    }
}

/// The manifest a set of paused artifacts describes.
///
/// Free-standing so the refusals below can be exercised without assembling a
/// whole runtime config, which is a hundred fields of which six matter here.
pub(crate) fn publishable_manifest(
    vm_state_path: &Path,
    memory: &OverlaybdConfig,
    mem_virtual_size: u64,
    rootfs: Option<&OverlaybdConfig>,
    rootfs_virtual_size: Option<u64>,
    extra_drives: &[ExtraDrive],
) -> Result<FirecrackerSnapshotManifest> {
    let rootfs = rootfs.ok_or_else(|| {
        anyhow!("paused sandbox has no rootfs image config, so there is nothing to publish")
    })?;
    // The recorded size is what a restoring node sizes its block device from,
    // so an absent one has to refuse rather than default: guessing it wrong
    // truncates the guest's disk.
    let rootfs_virtual_size = rootfs_virtual_size.ok_or_else(|| {
        anyhow!("paused sandbox has no recorded rootfs size, so it cannot be published")
    })?;

    FirecrackerSnapshotManifest::new(
        vm_state_path,
        memory.image_config_path.clone(),
        mem_virtual_size,
        rootfs.image_config_path.clone(),
        rootfs_virtual_size,
        extra_drives,
    )
}

#[cfg(test)]
mod publishable_manifest_tests {
    use super::*;

    fn overlaybd(path: &str, read_only: bool) -> OverlaybdConfig {
        OverlaybdConfig {
            image_config_path: PathBuf::from(path),
            read_only,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        }
    }

    fn drives() -> Vec<ExtraDrive> {
        vec![ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/paused/drives/data/image.json"),
            read_only: false,
            mount_path: PathBuf::from("/mnt/data"),
            virtual_size: Some(4096),
            sub_path: None,
        }]
    }

    fn manifest() -> Result<FirecrackerSnapshotManifest> {
        publishable_manifest(
            Path::new("/paused/vm_state.bin"),
            &overlaybd("/paused/memory/image.json", true),
            2 * 1024 * 1024 * 1024,
            Some(&overlaybd("/paused/rootfs/image.json", false)),
            Some(8 * 1024 * 1024 * 1024),
            &drives(),
        )
    }

    #[test]
    fn a_paused_sandbox_describes_every_artifact_it_has() {
        let manifest = manifest().expect("a complete paused config should be publishable");

        assert_eq!(manifest.vm_state.path, Path::new("/paused/vm_state.bin"));
        assert_eq!(
            manifest.memory.image_config_path,
            Path::new("/paused/memory/image.json"),
            "the memory image must be the memory image, not the rootfs"
        );
        assert_eq!(manifest.memory.virtual_size, 2 * 1024 * 1024 * 1024);
        assert_eq!(
            manifest.rootfs.image_config_path,
            Path::new("/paused/rootfs/image.json")
        );
        assert_eq!(manifest.rootfs.virtual_size, 8 * 1024 * 1024 * 1024);

        // Drives have to come along: a snapshot missing one restores a guest
        // whose disk is simply absent.
        assert_eq!(manifest.attached_drives.len(), 1);
        assert_eq!(manifest.attached_drives[0].drive_id, "data");
        assert_eq!(
            manifest.attached_drives[0].image_config_path,
            Path::new("/paused/drives/data/image.json")
        );
    }

    #[test]
    fn a_rootfs_without_a_recorded_size_cannot_be_published() {
        let error = publishable_manifest(
            Path::new("/paused/vm_state.bin"),
            &overlaybd("/paused/memory/image.json", true),
            1024,
            Some(&overlaybd("/paused/rootfs/image.json", false)),
            None,
            &[],
        )
        .expect_err("an unsized rootfs must not be publishable");
        assert!(
            error.to_string().contains("recorded rootfs size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_paused_sandbox_without_a_rootfs_cannot_be_published() {
        let error = publishable_manifest(
            Path::new("/paused/vm_state.bin"),
            &overlaybd("/paused/memory/image.json", true),
            1024,
            None,
            Some(4096),
            &[],
        )
        .expect_err("a sandbox with no rootfs must not be publishable");
        assert!(
            error.to_string().contains("rootfs image config"),
            "unexpected error: {error}"
        );
    }
}
