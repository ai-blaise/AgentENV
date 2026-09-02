use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::config::load_image_config as load_overlaybd_image_config;
use tracing::{debug, warn};

use bytes::Bytes;
use tempfile::NamedTempFile;

use crate::overlaybd::{layer_key_from_digest, LayerMetadata};
use crate::p2p::{
    P2pArtifactKey, P2pPublishMode, P2pPublishRequest, P2pPublishSource, P2pTransport,
};
use crate::snapshot::sealing::{self, ArtifactSealingKey, SealScope};
use crate::snapshot::SnapshotId;

const SNAPSHOT_P2P_KEY_PREFIX: &str = "snapshot/v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotP2pArtifact {
    pub(crate) key: P2pArtifactKey,
    pub(crate) source: P2pPublishSource,
    publish_mode: P2pPublishMode,
    metadata: serde_json::Value,
    /// Set for per-snapshot fixed artifacts, which are sealed before they leave
    /// the node. Content-addressed overlaybd layers carry `None`: their key is
    /// the digest of their plaintext, and the overlaybd facade reads ranges out
    /// of them, so an envelope would break both.
    seal_scope: Option<OwnedSealScope>,
}

/// The seal scope of a fixed artifact, owned because the artifact outlives the
/// borrow that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnedSealScope {
    snapshot_id: String,
    artifact_name: String,
}

impl OwnedSealScope {
    fn as_scope(&self) -> SealScope<'_> {
        SealScope::new(&self.snapshot_id, &self.artifact_name)
    }
}

impl SnapshotP2pArtifact {
    pub(crate) fn fixed(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<PathBuf>,
    ) -> Self {
        let source = source.into();
        let name = name.as_ref();
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Path(source),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
            seal_scope: Some(OwnedSealScope {
                snapshot_id: snapshot_id.to_string(),
                artifact_name: name.to_string(),
            }),
        }
    }

    pub(crate) fn bytes(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<Bytes>,
    ) -> Self {
        let name = name.as_ref();
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Bytes(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
            seal_scope: Some(OwnedSealScope {
                snapshot_id: snapshot_id.to_string(),
                artifact_name: name.to_string(),
            }),
        }
    }

    pub(crate) fn content_addressed_overlaybd_layer(
        source: impl Into<PathBuf>,
        sha256: impl Into<String>,
        size: u64,
    ) -> Self {
        let sha256 = sha256.into();
        let key = layer_key_from_digest(&sha256);
        let metadata = LayerMetadata::from_digest(sha256, Some(size), None).to_value();
        Self {
            key,
            source: P2pPublishSource::Path(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata,
            seal_scope: None,
        }
    }

    /// The layers of a snapshot's rootfs that may be advertised to peers.
    ///
    /// Two things have to hold. The layer must be runtime-generated-delta-free,
    /// and it must be content-addressed.
    ///
    /// Provenance is checked first because it is the one that carries guest
    /// data. The snapshot's own layer is the delta the guest wrote to `/`, and
    /// it wears exactly the same shape as a registry blob: `restack` hands the
    /// sealed upper a `LayerDescriptor` whose digest is a sha256 of the delta's
    /// own bytes, so it arrives here with a non-empty digest and a positive
    /// size. Digest presence therefore says nothing about where a layer came
    /// from — only the filename convention does, which is why every other
    /// caller that has to tell them apart uses the same predicate.
    ///
    /// Advertising the delta would put the guest's filesystem in front of the
    /// whole mesh, unsealed (a content-addressed layer is published in the
    /// clear so the facade can read ranges out of it), in exchange for a lookup
    /// that cannot happen: nothing resolves a snapshot's delta over P2P. That
    /// is the same trade the memory and attached-drive layers are already kept
    /// out of.
    ///
    /// What remains is a copy of a blob any peer could already pull from a
    /// registry, keyed by the digest of that same plaintext.
    pub(crate) fn local_overlaybd_layers(
        image_config_path: &Path,
        registry_digests: &std::collections::HashSet<String>,
    ) -> Vec<Self> {
        let image_config = match load_overlaybd_image_config(image_config_path) {
            Ok(image_config) => image_config,
            Err(error) => {
                warn!(
                    path = %image_config_path.display(),
                    error = %error,
                    "skipping snapshot P2P layer publication because image config could not be loaded"
                );
                return Vec::new();
            }
        };

        image_config
            .lowers
            .into_iter()
            .filter(|layer| !layer.file.is_empty())
            // An allowlist, not a denylist: a layer is advertised only when the
            // committed record calls it External, meaning a registry holds the
            // same bytes. Naming cannot carry this. A sandbox resumed from a
            // snapshot gets every lower back from the repository as
            // `managed-layers/sha256_<digest>.overlaybd.commit`, so the parent's
            // guest delta arrives content-addressed and indistinguishable by
            // filename from a registry blob; a basename test recognised the
            // delta only in the generation that created it. Defaulting to
            // "do not publish" also means a record we cannot read publishes
            // nothing, which is the safe direction for guest bytes.
            .filter(|layer| registry_digests.contains(&layer.digest))
            .filter(|layer| !layer.digest.is_empty() && layer.size > 0)
            .map(|layer| {
                Self::content_addressed_overlaybd_layer(layer.file, layer.digest, layer.size)
            })
            .collect()
    }

    pub(crate) async fn publish(
        &self,
        transport: &Arc<dyn P2pTransport>,
        sealing_state: &sealing::SnapshotSealing,
    ) -> Result<()> {
        // Sealing owns its own temporary file, which must outlive the publish
        // call: `Copy` mode reads the bytes during `publish`.
        let sealed_file;
        let request = match (&self.source, self.seal_scope.as_ref()) {
            (P2pPublishSource::Path(source), None) => {
                P2pPublishRequest::file(self.key.clone(), source.clone())
                    .with_publish_mode(self.publish_mode)
            }
            (P2pPublishSource::Bytes(bytes), None) => {
                P2pPublishRequest::bytes(self.key.clone(), bytes.clone())
            }
            (P2pPublishSource::Path(source), Some(scope)) => {
                sealed_file = seal_to_temp_file(source, scope, sealing_state).await?;
                P2pPublishRequest::file(self.key.clone(), sealed_file.path().to_path_buf())
                    .with_publish_mode(self.publish_mode)
            }
            (P2pPublishSource::Bytes(bytes), Some(scope)) => {
                let key = sealing_key_from(sealing_state, &self.key)?;
                let sealed = sealing::seal_slice(&key, &scope.as_scope(), bytes)
                    .with_context(|| format!("seal snapshot artifact '{}'", self.key))?;
                P2pPublishRequest::bytes(self.key.clone(), Bytes::from(sealed))
            }
        }
        .with_metadata(self.metadata.clone());

        transport
            .publish(&request)
            .await
            .with_context(|| format!("publish snapshot artifact '{}' to P2P", self.key))
    }
}

/// Returns the node's sealing key, or explains that the artifact should not
/// have been offered for publication without one.
///
/// Reaching this without a key is a wiring bug rather than a runtime
/// condition: the publisher decides whether to build fixed artifacts at all,
/// and it only does so when sealing is enabled.
fn sealing_key_from(
    sealing: &sealing::SnapshotSealing,
    key: &P2pArtifactKey,
) -> Result<ArtifactSealingKey> {
    sealing.key().cloned().with_context(|| {
        format!(
            "snapshot artifact '{key}' cannot be published without a sealing secret; \
                 set [snapshot].artifact_sealing_secret (AENV_SNAPSHOT_ARTIFACT_SEALING_SECRET)"
        )
    })
}

/// The fetch-side equivalent, reading the process-wide state.
///
/// Fetching is best effort with a repository fallback, so it needs no injected
/// state the way the publish decision does.
fn sealing_key_for(key: &P2pArtifactKey) -> Result<ArtifactSealingKey> {
    sealing_key_from(&sealing::global_snapshot_sealing(), key)
}

/// Seals `source` into a temporary file beside it.
///
/// Beside it, rather than in the system temp dir, so a multi-gigabyte memory
/// state does not land on a small `/tmp` and so the copy stays on the volume
/// already sized for snapshot artifacts.
async fn seal_to_temp_file(
    source: &Path,
    scope: &OwnedSealScope,
    sealing_state: &sealing::SnapshotSealing,
) -> Result<NamedTempFile> {
    let key = sealing_key_from(
        sealing_state,
        &fixed_artifact_key(&scope.snapshot_id, &scope.artifact_name),
    )?;
    let source = source.to_path_buf();
    let scope = scope.clone();
    tokio::task::spawn_blocking(move || {
        let directory = source.parent().unwrap_or_else(|| Path::new("."));
        let sealed = NamedTempFile::new_in(directory)
            .with_context(|| format!("create sealed staging file in {}", directory.display()))?;
        sealing::seal_path(&key, &scope.as_scope(), &source, sealed.path())
            .with_context(|| format!("seal snapshot artifact {}", source.display()))?;
        Ok(sealed)
    })
    .await
    .context("join snapshot artifact sealing task")?
}

pub(crate) fn fixed_artifact_key(
    snapshot_id: &impl std::fmt::Display,
    name: impl AsRef<str>,
) -> P2pArtifactKey {
    format!(
        "{SNAPSHOT_P2P_KEY_PREFIX}/artifacts/{snapshot_id}/{}",
        name.as_ref()
    )
}

/// Largest `vm_state` a peer may hand this node.
///
/// Firecracker's state file holds vCPU and device state, not guest memory, so
/// it is well under a megabyte for any machine this runs. The limit is set far
/// above that rather than close to it: it exists to stop an unbounded transfer
/// from an unauthenticated peer, not to validate the artifact, which the
/// sealing check does afterwards on content of a size this has already agreed
/// to hold.
pub(crate) const MAX_VM_STATE_BYTES: u64 = 64 * 1024 * 1024;

/// Largest Firecracker manifest a peer may hand this node.
///
/// A manifest names the layers a snapshot is built from — kilobytes, even for
/// a deep stack — and unlike `vm_state` it is read into memory rather than
/// staged on disk.
pub(crate) const MAX_FIRECRACKER_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// Fetches and opens a sealed fixed artifact into `destination`.
///
/// A peer running a build that published this artifact in the clear fails the
/// header check and the caller falls back to the repository. That is the
/// intended rollout behaviour: an unsealed artifact is treated as absent
/// rather than trusted.
pub(crate) async fn fetch_artifact(
    transport: &Arc<dyn P2pTransport>,
    scope: &SealScope<'_>,
    destination: &Path,
    max_bytes: u64,
) -> Result<u64> {
    let key = fixed_artifact_key(&scope.snapshot_id, scope.artifact_name);
    let sealing_key = sealing_key_for(&key)?;
    let Some(descriptor) = transport.lookup(&key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };

    let staging = NamedTempFile::new_in(destination.parent().unwrap_or_else(|| Path::new(".")))
        .context("create sealed download staging file")?;
    transport
        .fetch(&descriptor, staging.path(), max_bytes)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;

    let destination_path = destination.to_path_buf();
    let scope = OwnedSealScope {
        snapshot_id: scope.snapshot_id.to_string(),
        artifact_name: scope.artifact_name.to_string(),
    };
    let size = tokio::task::spawn_blocking(move || {
        sealing::open_path(
            &sealing_key,
            &scope.as_scope(),
            staging.path(),
            &destination_path,
        )
    })
    .await
    .context("join snapshot artifact opening task")?
    .with_context(|| format!("open sealed snapshot P2P artifact '{key}'"))?;

    debug!(key, destination = %destination.display(), size, "fetched snapshot artifact from P2P");
    Ok(size)
}

pub(crate) async fn fetch_artifact_bytes(
    transport: &Arc<dyn P2pTransport>,
    scope: &SealScope<'_>,
    max_bytes: u64,
) -> Result<Bytes> {
    let key = fixed_artifact_key(&scope.snapshot_id, scope.artifact_name);
    let sealing_key = sealing_key_for(&key)?;
    let Some(descriptor) = transport.lookup(&key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };
    let sealed = transport
        .fetch_bytes(&descriptor, max_bytes)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;
    let bytes = Bytes::from(
        sealing::open_slice(&sealing_key, scope, &sealed)
            .with_context(|| format!("open sealed snapshot P2P artifact '{key}'"))?,
    );
    debug!(
        key,
        size = bytes.len(),
        "fetched snapshot artifact from P2P"
    );
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::local::LocalFile;
    use overlaybd::config::{ImageConfig, LayerConfig};
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;
    use std::sync::Arc;
    use uuid::Uuid;

    async fn write_sealed_layer(path: &Path, uuid: Uuid) {
        let index_path = path.with_extension("index");
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).expect("data file"));
        let index_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(index_path).expect("index file"));
        let mut info = LayerInfo::new(data_file, Some(index_file), 8192);
        info.uuid = uuid;
        let file = create_file_rw(info).await.expect("create rw layer");
        file.write_at(0, &[0x5a; 4096]).await.expect("write layer");
        file.close_seal().await.expect("seal layer");
    }

    #[tokio::test]
    async fn local_overlaybd_layers_publish_only_digest_layers() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let descriptorless = temp.path().join("descriptorless.commit");
        let described = temp.path().join("described.commit");
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        write_sealed_layer(&descriptorless, uuid).await;
        std::fs::write(&described, b"described").expect("write described layer");

        let image_config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: descriptorless.display().to_string(),
                    ..Default::default()
                },
                LayerConfig {
                    file: described.display().to_string(),
                    digest:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    size: 9,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&image_config).expect("serialize image config"),
        )
        .expect("write image config");

        // The described layer is the registry-origin one, so the record calls
        // it External and it is the only candidate the guard may pass.
        let registry_digests = std::collections::HashSet::from([
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ]);
        let artifacts =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &registry_digests);

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].publish_mode, P2pPublishMode::Copy);
        assert_eq!(
            artifacts[0].key,
            "overlaybd-layer/v1/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    /// The guest's writes to `/` must never be advertised, and the shape they
    /// arrive in is indistinguishable from a registry blob's: the ublk restack
    /// hands the sealed upper a descriptor, so the delta reaches publication
    /// with a real content digest and a real size (the same shape
    /// `stage_live_runtime_preserves_restacked_layer_descriptor` asserts the
    /// staged config keeps).
    ///
    /// Each row therefore pins one half of one guard. A delta named layer with
    /// a full descriptor pins provenance; the registry-named rows with a
    /// missing digest or a zero size pin each half of the content-addressing
    /// conjunction. Dropping any one of the three advertises a row that must
    /// stay on the node.
    #[tokio::test]
    async fn local_overlaybd_layers_never_advertise_the_snapshot_delta() {
        struct Row {
            file: &'static str,
            described: bool,
            sized: bool,
            /// Whether the committed record calls this layer External, i.e. a
            /// registry holds the same bytes. This is what the guard reads;
            /// the filename is deliberately not.
            external: bool,
            advertised: bool,
        }

        let rows = [
            Row {
                file: "registry.commit",
                described: true,
                sized: true,
                external: true,
                advertised: true,
            },
            // The runtime-generated delta as the restack path produces it.
            Row {
                file: "snapshot.commit",
                described: true,
                sized: true,
                external: false,
                advertised: false,
            },
            // The compaction of a runtime-owned suffix: guest bytes merged
            // into one layer, content-addressed exactly like the delta.
            Row {
                file: "managed-base.commit",
                described: true,
                sized: true,
                external: false,
                advertised: false,
            },
            // The case a basename test cannot see: a sandbox resumed from a
            // snapshot gets every lower back from the repository under this
            // shape, the parent's guest delta included. It carries a digest
            // and a size and is named exactly like a registry blob, so only
            // its absence from the record's External set keeps it home.
            Row {
                file: "sha256_dead10cc.overlaybd.commit",
                described: true,
                sized: true,
                external: false,
                advertised: false,
            },
            Row {
                file: "registry-undescribed.commit",
                described: false,
                sized: true,
                external: true,
                advertised: false,
            },
            Row {
                file: "registry-unsized.commit",
                described: true,
                sized: false,
                external: true,
                advertised: false,
            },
        ];

        let temp = tempfile::TempDir::new().expect("tempdir");
        let mut lowers = Vec::new();
        let mut want_advertised = Vec::new();
        let mut registry_digests = std::collections::HashSet::new();
        for (index, row) in rows.iter().enumerate() {
            let path = temp.path().join(row.file);
            std::fs::write(&path, format!("layer-{index}")).expect("write layer");
            let descriptor = crate::digest::FileDigest::describe(&path)
                .await
                .expect("describe layer");
            if row.advertised {
                want_advertised.push(layer_key_from_digest(&descriptor.sha256));
            }
            if row.external && row.described {
                registry_digests.insert(descriptor.sha256.clone());
            }
            lowers.push(LayerConfig {
                file: path.display().to_string(),
                digest: if row.described {
                    descriptor.sha256
                } else {
                    String::new()
                },
                size: if row.sized { descriptor.size } else { 0 },
                ..Default::default()
            });
        }

        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&ImageConfig {
                lowers,
                ..Default::default()
            })
            .expect("serialize image config"),
        )
        .expect("write image config");

        let advertised =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &registry_digests)
                .into_iter()
                .map(|artifact| artifact.key)
                .collect::<Vec<_>>();

        assert_eq!(
            advertised, want_advertised,
            "only the registry-origin layer may be advertised"
        );
    }

    /// The publish request the transport actually receives, for the one artifact
    /// class that goes out in the clear. Asserted on the request rather than on
    /// the constructor so a later refactor of `publish` cannot seal a layer (the
    /// facade reads ranges out of it) or unseal a fixed artifact.
    #[tokio::test]
    async fn a_content_addressed_layer_publishes_its_plaintext() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let layer_path = temp.path().join("registry.commit");
        std::fs::write(&layer_path, b"layer-plaintext").expect("write layer");

        let transport: Arc<dyn P2pTransport> = Arc::new(crate::p2p::mock::MockTransport::default());
        let artifact = SnapshotP2pArtifact::content_addressed_overlaybd_layer(
            layer_path,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            15,
        );
        artifact
            .publish(&transport, &sealing::SnapshotSealing::disabled())
            .await
            .expect("publishing a layer needs no sealing secret");

        let descriptor = transport
            .lookup(&artifact.key)
            .await
            .expect("lookup")
            .expect("the layer should be advertised");
        let published = transport
            .fetch_bytes(&descriptor, 1024)
            .await
            .expect("fetch");
        assert_eq!(published.as_ref(), b"layer-plaintext");
    }

    /// The sealed round trip a peer-accelerated resume takes: publish under the
    /// node's sealing state, fetch back through the caller's byte cap, open.
    ///
    /// The caps are used as the real callers use them, so shrinking either
    /// constant by a refactor fails here rather than only in production.
    #[tokio::test]
    async fn a_sealed_fixed_artifact_round_trips_within_its_cap() {
        let sealing_key = sealing::install_test_global_sealing();
        let sealing_state = sealing::SnapshotSealing::with_key(sealing_key);
        let transport: Arc<dyn P2pTransport> = Arc::new(crate::p2p::mock::MockTransport::default());
        let snapshot_id = SnapshotId::generate();
        let scoped_id = snapshot_id.to_string();

        let manifest = br#"{"vm_state":"vm_state.bin"}"#;
        SnapshotP2pArtifact::bytes(
            &snapshot_id,
            "firecracker-manifest.json",
            Bytes::from_static(manifest),
        )
        .publish(&transport, &sealing_state)
        .await
        .expect("publish");

        let scope = SealScope::new(&scoped_id, "firecracker-manifest.json");
        let opened = fetch_artifact_bytes(&transport, &scope, MAX_FIRECRACKER_MANIFEST_BYTES)
            .await
            .expect("a sealed artifact opens for a node holding the secret");
        assert_eq!(opened.as_ref(), manifest);

        let temp = tempfile::TempDir::new().expect("tempdir");
        let vm_state = temp.path().join("vm_state.bin");
        std::fs::write(&vm_state, b"vcpu-and-device-state").expect("write vm state");
        SnapshotP2pArtifact::fixed(&snapshot_id, "vm_state.bin", vm_state)
            .publish(&transport, &sealing_state)
            .await
            .expect("publish");

        let destination = temp.path().join("restored-vm_state.bin");
        let scope = SealScope::new(&scoped_id, "vm_state.bin");
        let size = fetch_artifact(&transport, &scope, &destination, MAX_VM_STATE_BYTES)
            .await
            .expect("a sealed artifact opens for a node holding the secret");
        assert_eq!(size, b"vcpu-and-device-state".len() as u64);
        assert_eq!(
            std::fs::read(&destination).expect("read restored"),
            b"vcpu-and-device-state"
        );
    }

    /// The caller's cap is what stands between an unauthenticated peer and an
    /// unbounded write, and it is checked before the seal is: a peer offering
    /// more than the caller agreed to hold is refused, not opened and rejected.
    #[tokio::test]
    async fn a_fixed_artifact_larger_than_its_cap_is_refused() {
        let sealing_key = sealing::install_test_global_sealing();
        let sealing_state = sealing::SnapshotSealing::with_key(sealing_key);
        let transport: Arc<dyn P2pTransport> = Arc::new(crate::p2p::mock::MockTransport::default());
        let snapshot_id = SnapshotId::generate();
        let scoped_id = snapshot_id.to_string();

        SnapshotP2pArtifact::bytes(
            &snapshot_id,
            "firecracker-manifest.json",
            Bytes::from_static(&[7_u8; 4096]),
        )
        .publish(&transport, &sealing_state)
        .await
        .expect("publish");

        let scope = SealScope::new(&scoped_id, "firecracker-manifest.json");
        // Matched rather than `expect_err`: the Ok side is the whole artifact,
        // and printing four kilobytes of it buries the failure.
        let error = match fetch_artifact_bytes(&transport, &scope, 64).await {
            Ok(_) => panic!("a peer must not be able to exceed the caller's bound"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("larger than the 64-byte limit"),
            "expected the caller's own bound to refuse it, got {error:#}"
        );
    }

    /// The rollout property: a peer still publishing in the clear is treated as
    /// absent rather than trusted, so the caller falls back to the repository.
    #[tokio::test]
    async fn an_unsealed_fixed_artifact_is_not_opened() {
        sealing::install_test_global_sealing();
        let transport: Arc<dyn P2pTransport> = Arc::new(crate::p2p::mock::MockTransport::default());
        let snapshot_id = SnapshotId::generate();
        let scoped_id = snapshot_id.to_string();

        let key = fixed_artifact_key(&snapshot_id, "firecracker-manifest.json");
        transport
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                Bytes::from_static(br#"{"vm_state":"vm_state.bin"}"#),
            ))
            .await
            .expect("publish in the clear");

        let scope = SealScope::new(&scoped_id, "firecracker-manifest.json");
        fetch_artifact_bytes(&transport, &scope, MAX_FIRECRACKER_MANIFEST_BYTES)
            .await
            .expect_err("an artifact published in the clear must not be accepted");
    }
}
