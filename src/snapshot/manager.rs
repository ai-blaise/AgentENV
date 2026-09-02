use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::{OverlaybdLayerRef, SNAPSHOT_ARTIFACT_LAYOUT};
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::build_snapshot_backend;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
use crate::snapshot::sealing::{global_snapshot_sealing, SnapshotSealing};
use crate::snapshot::{RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;

#[derive(Clone)]
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Durable reachability of committed snapshots is owned entirely by the
/// [`SnapshotRepository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    /// Held rather than read from the process-wide state at publish time: what
    /// may be advertised is a policy decision, and a policy decision that reads
    /// a global cannot be exercised both ways in one test binary.
    sealing: Arc<SnapshotSealing>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(p2p_transport: Option<Arc<dyn P2pTransport>>) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        Ok(Self::from_parts(
            repository,
            runtime_resolver,
            p2p_transport,
        ))
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
            p2p_transport,
            sealing: global_snapshot_sealing(),
        }
    }

    /// Overrides the sealing state this manager publishes under.
    pub fn with_sealing(mut self, sealing: Arc<SnapshotSealing>) -> Self {
        self.sealing = sealing;
        self
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let record = self
            .repository
            .publish(metadata.clone(), manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let manifest = captured_snapshot
            .downcast_ref::<FirecrackerCapturedSnapshot>()
            .map(|snapshot| snapshot.manifest().clone())
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        let record = self
            .repository
            .publish(metadata.clone(), manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn publish_p2p_artifacts(
        &self,
        record: &SnapshotRecord,
        manifest: &FirecrackerSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        let snapshot_id = &record.id;
        // Nothing is advertised for a snapshot that never committed: the
        // layers it names may still be moving underneath it.
        if record.committed.is_none() {
            return;
        }

        let mut artifacts = Vec::new();

        // Fixed artifacts are the guest's CPU state and the manifest naming
        // every layer it is built from, so they only leave the node sealed.
        // Without a secret they are simply not advertised: resolution falls
        // back to the repository, which costs bandwidth and nothing else.
        if self.sealing.is_enabled() {
            let manifest_bytes = serde_json::to_vec(manifest).expect("manifest should serialize");
            artifacts.push(SnapshotP2pArtifact::fixed(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                manifest.vm_state.path.clone(),
            ));
            artifacts.push(SnapshotP2pArtifact::bytes(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                manifest_bytes,
            ));
        } else {
            warn!(
                %snapshot_id,
                "not advertising snapshot fixed artifacts to P2P because no sealing secret is \
                 configured; set AENV_SNAPSHOT_ARTIFACT_SEALING_SECRET to the same value on \
                 every node to enable peer-accelerated snapshot resolution"
            );
        }

        // Registry-origin rootfs layers are the only layers with a P2P
        // consumer: the overlaybd facade resolves them by registry origin, so
        // what it serves is content that is also a registry blob.
        //
        // Only the rootfs config is offered. The memory layers and the
        // attached-drive layers are guest data with no P2P consumer, and they
        // stay on the node by never being routed here at all. The rootfs
        // delta — the guest's own writes to `/` — travels inside this config
        // and cannot be excluded by not passing it, so `local_overlaybd_layers`
        // drops it on provenance; see the guard there for why its digest is no
        // help. Any of them would go out unsealed, because the range-read
        // facade has no path that opens an envelope.
        // The record is what knows provenance: External means a registry holds
        // these bytes, Managed means this repository does, and only the former
        // may leave the node. The image config alone cannot say which is which.
        let registry_digests: std::collections::HashSet<String> = record
            .committed
            .as_ref()
            .map(|committed| {
                committed
                    .rootfs_layers
                    .iter()
                    .filter_map(|layer| match layer {
                        OverlaybdLayerRef::External(external) => Some(external.digest.clone()),
                        OverlaybdLayerRef::Managed(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &registry_digests,
        ));

        if artifacts.is_empty() {
            return;
        }

        // Publish all artifacts concurrently, but don't fail if any individual artifact fails to publish.
        stream::iter(artifacts)
            .for_each_concurrent(SNAPSHOT_P2P_PUBLISH_CONCURRENCY, |artifact| async move {
                if let Err(error) = artifact.publish(transport, self.sealing.as_ref()).await {
                    warn!(
                        key = %artifact.key,
                        source = %artifact.source,
                        error = %error,
                        "failed to publish snapshot artifact to P2P"
                    );
                }
            })
            .await;
    }

    /// Loads a snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.repository
            .get(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "load committed snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        // Resolve before deleting: after the repository drops the record there
        // is no way to learn which fixed artifacts were advertised for it.
        let advertised = self
            .resolve_snapshot_id_for_unpublish(id_or_alias.as_ref())
            .await;

        self.repository
            .delete(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "delete snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })?;

        if let Some(snapshot_id) = advertised {
            self.unpublish_p2p_artifacts(&snapshot_id).await;
        }
        Ok(())
    }

    /// Resolves the snapshot id whose P2P advertisements should be withdrawn.
    async fn resolve_snapshot_id_for_unpublish(&self, id_or_alias: &str) -> Option<SnapshotId> {
        self.p2p_transport.as_ref()?;
        if let Ok(id) = SnapshotId::parse(id_or_alias) {
            return Some(id);
        }
        self.repository
            .resolve_alias(id_or_alias)
            .await
            .ok()
            .flatten()
    }

    /// Withdraws a deleted snapshot's fixed P2P advertisements.
    ///
    /// Without this nothing ever called unpublish, so the transport's retention
    /// tags were never removed and its gated collector had nothing to collect —
    /// the store grew for the lifetime of the process.
    ///
    /// Only the fixed per-snapshot artifacts are withdrawn. Overlaybd layers are
    /// content-addressed and shared between snapshots, so withdrawing one
    /// snapshot's layer would pull it out from under every other snapshot that
    /// references the same digest; those are owned by the image cache's
    /// retention instead.
    #[tracing::instrument(skip(self), fields(snapshot_id = %snapshot_id))]
    async fn unpublish_p2p_artifacts(&self, snapshot_id: &SnapshotId) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        for name in [
            SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
            SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
        ] {
            let key = crate::snapshot::p2p::fixed_artifact_key(snapshot_id, name);
            match transport.unpublish(&key).await {
                Ok(true) => tracing::debug!(%key, "withdrew snapshot artifact from P2P"),
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    %key,
                    error = %error,
                    "failed to withdraw snapshot artifact from P2P"
                ),
            }
        }
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.repository.resolve_alias(alias).await.with_context(|| {
            format!("resolve committed snapshot alias '{alias}' through repository")
        })
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        self.runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .context("resolve committed snapshot into runnable runtime paths")
    }

    /// Loads a committed snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
    use std::path::Path;
    use tempfile::TempDir;

    fn test_manager(root: &Path) -> SnapshotManager {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
            lock_strategy: Default::default(),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        SnapshotManager::from_parts(repository, runtime_resolver, None)
    }

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, manifest)
            .await
            .expect("seed publish should work");
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "managed").await;

        let resolved = manager
            .resolve_committed_alias("managed")
            .await
            .expect("resolve alias should work");
        assert_eq!(resolved, Some(snapshot_id.clone()));

        let loaded = manager
            .get("managed")
            .await
            .expect("load should work")
            .expect("snapshot should exist");
        assert_eq!(loaded.id, snapshot_id);

        let listed = manager
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
    }

    /// Publishes a mock snapshot under the given sealing state and returns the
    /// transport it advertised to, along with the snapshot id and the digest of
    /// the rootfs lower.
    async fn publish_mock_snapshot(
        sealing: Arc<SnapshotSealing>,
    ) -> (Arc<MockTransport>, SnapshotId, String, TempDir, TempDir) {
        publish_mock_snapshot_as(sealing, None).await.0
    }

    /// The same, keeping the manager so the caller can drive it further, and
    /// optionally giving the snapshot an alias.
    #[allow(clippy::type_complexity)]
    async fn publish_mock_snapshot_as(
        sealing: Arc<SnapshotSealing>,
        alias: Option<&str>,
    ) -> (
        (Arc<MockTransport>, SnapshotId, String, TempDir, TempDir),
        SnapshotManager,
    ) {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
            lock_strategy: Default::default(),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(repository, runtime_resolver, Some(p2p.clone()))
            .with_sealing(sealing);

        let workspace = TempDir::new().expect("tempdir should exist");
        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };

        manager
            .publish(metadata, manifest)
            .await
            .expect("publish should commit");

        let rootfs_layer_digest = crate::digest::FileDigest::describe(&rootfs_lower)
            .await
            .expect("describe rootfs lower")
            .sha256;

        (
            (p2p, snapshot_id, rootfs_layer_digest, tempdir, workspace),
            manager,
        )
    }

    fn test_sealing() -> Arc<SnapshotSealing> {
        Arc::new(SnapshotSealing::with_key(
            crate::snapshot::ArtifactSealingKey::from_bytes(vec![5_u8; 32]).expect("key"),
        ))
    }

    #[tokio::test]
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let (p2p, snapshot_id, rootfs_digest, _repository, _workspace) =
            publish_mock_snapshot(test_sealing()).await;

        for key in [
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
        ] {
            assert!(
                p2p.lookup(&key).await.expect("lookup").is_some(),
                "{key} should have been advertised"
            );
        }

        // This snapshot's rootfs lower is repository-managed: nothing says a
        // registry holds the same bytes, so it stays on the node. That costs
        // nothing, because the facade resolves layers by registry origin and
        // could never have served this one to a peer. The layer that does
        // carry a registry origin is advertised, which
        // `local_overlaybd_layers_never_advertise_the_snapshot_delta` pins.
        assert!(
            p2p.lookup(&layer_key_from_digest(&rootfs_digest))
                .await
                .expect("lookup")
                .is_none(),
            "a repository-managed layer must not be advertised"
        );
    }

    /// The fixed artifacts carry guest CPU state and the manifest naming every
    /// layer the guest is built from. Without a sealing secret they must not be
    /// advertised at all: resolution falls back to the repository, which costs
    /// bandwidth rather than confidentiality.
    #[tokio::test]
    async fn unsealed_nodes_do_not_advertise_fixed_artifacts() {
        let (p2p, snapshot_id, rootfs_digest, _repository, _workspace) =
            publish_mock_snapshot(Arc::new(SnapshotSealing::disabled())).await;

        for key in [
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
        ] {
            assert!(
                p2p.lookup(&key).await.expect("lookup").is_none(),
                "{key} must not be advertised without a sealing secret"
            );
        }

        // The rootfs lower here is repository-managed, so it is not advertised
        // whatever the sealing secret says: provenance decides that, not
        // sealing.
        assert!(p2p
            .lookup(&layer_key_from_digest(&rootfs_digest))
            .await
            .expect("lookup")
            .is_none());
    }

    /// What the transport holds must be ciphertext. This is the property the
    /// whole change exists for, so it is asserted on the bytes rather than
    /// inferred from the code path.
    #[tokio::test]
    async fn advertised_fixed_artifacts_are_ciphertext() {
        let sealing = test_sealing();
        let (p2p, snapshot_id, _rootfs_digest, _repository, _workspace) =
            publish_mock_snapshot(sealing.clone()).await;

        let key = fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let descriptor = p2p
            .lookup(&key)
            .await
            .expect("lookup")
            .expect("manifest should be advertised");
        let published = p2p.fetch_bytes(&descriptor, u64::MAX).await.expect("fetch");

        assert!(
            crate::snapshot::sealing::has_sealed_magic(&published),
            "the advertised manifest must be a sealed envelope"
        );
        assert!(
            !published.windows(9).any(|window| window == b"vm_state."),
            "the advertised manifest must not leak its plaintext"
        );

        let opened = crate::snapshot::sealing::open_slice(
            sealing.key().expect("key"),
            &crate::snapshot::SealScope::new(
                &snapshot_id.to_string(),
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
            ),
            &published,
        )
        .expect("a holder of the secret can open it");
        serde_json::from_slice::<serde_json::Value>(&opened).expect("opens to the manifest");
    }

    /// Deleting a snapshot has to withdraw its fixed advertisements, or the
    /// transport's retention tags are never removed and its gated collector has
    /// nothing to collect — the store then grows for the life of the process,
    /// which is the regression this edge was added to close.
    ///
    /// Deleted by alias rather than by id on purpose. The id path short-circuits
    /// through `SnapshotId::parse`, which succeeds whether or not the record
    /// still exists, so only the alias path also pins the ordering: resolve has
    /// to happen before the repository drops the record, or there is nothing
    /// left to resolve and nothing is withdrawn.
    #[tokio::test]
    async fn delete_by_alias_withdraws_the_fixed_artifacts_it_advertised() {
        let ((p2p, snapshot_id, rootfs_digest, _repository, _workspace), manager) =
            publish_mock_snapshot_as(test_sealing(), Some("withdrawn")).await;

        let fixed = [
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
        ];
        for key in &fixed {
            assert!(
                p2p.lookup(key).await.expect("lookup").is_some(),
                "{key} should have been advertised before the delete"
            );
        }

        manager.delete("withdrawn").await.expect("delete");

        for key in &fixed {
            assert!(
                p2p.lookup(key).await.expect("lookup").is_none(),
                "{key} must be withdrawn on delete"
            );
        }

        // The managed rootfs lower was never advertised, so the delete has
        // nothing to withdraw for it. A registry-origin layer that had been
        // advertised would stay: layers are content-addressed and shared
        // between snapshots, owned by the image cache's retention rather than
        // by this delete.
        assert!(p2p
            .lookup(&layer_key_from_digest(&rootfs_digest))
            .await
            .expect("lookup")
            .is_none());
    }
}
