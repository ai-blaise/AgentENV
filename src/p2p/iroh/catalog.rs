use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Weak};

use anyhow::Context;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::p2p::error::Result;
use crate::p2p::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pEndpoint, P2pPeer, P2pPublishOwner,
};

pub(super) const CATALOG_ALPN: &[u8] = b"/agentenv/artifact-catalog/v1";
pub(super) const MAX_CATALOG_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CATALOG_REQUEST_BYTES: usize = 1024 * 1024;
/// Prefix under which the owner set of a key is stored, in the same database
/// as the descriptors.
///
/// Artifact keys are printable and namespace-prefixed, so a leading NUL cannot
/// collide with one; an entry that does not start with it is a descriptor,
/// which is also what makes a catalog written before ownership existed load
/// unchanged.
const OWNERS_KEY_PREFIX: &[u8] = b"\x00owners\x00";

#[cfg(not(test))]
const DB_DURABILITY: LocalStoreDurability = LocalStoreDurability::Wal;
#[cfg(test)]
const DB_DURABILITY: LocalStoreDurability = LocalStoreDurability::Memory;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogRequest {
    pub(crate) key: P2pArtifactKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogResponse {
    pub(crate) descriptor: Option<P2pArtifactDescriptor>,
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedArtifactCatalog {
    inner: Arc<RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>>,
    /// Publishers still holding each advertised key.
    ///
    /// Kept beside the descriptors rather than inside them because a
    /// descriptor is what goes out on the wire, and who locally retains an
    /// artifact is nobody else's business.
    owners: Arc<RwLock<HashMap<P2pArtifactKey, BTreeSet<P2pPublishOwner>>>>,
    store: LocalKvStore,
    local_provider: P2pPeer,
}

/// What releasing one owner did to an artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerRelease {
    /// The last owner released it; the caller must now withdraw the artifact.
    Withdrawn,
    /// At least one other owner still holds it.
    Retained,
    /// Nothing was advertised under this key.
    Absent,
}

impl PublishedArtifactCatalog {
    pub(super) async fn load(
        db_path: &Path,
        node_id: &str,
        local_endpoint: &P2pEndpoint,
    ) -> Result<Self> {
        let store = LocalKvStore::open(db_path.to_path_buf(), DB_DURABILITY)
            .await
            .with_context(|| format!("open P2P catalog {}", db_path.display()))?;

        let (loaded, mut owners) = store
            .fold(
                (HashMap::new(), HashMap::new()),
                |(descriptors, owners), key, bytes| {
                    if let Some(artifact_key) = key.strip_prefix(OWNERS_KEY_PREFIX) {
                        let artifact_key = String::from_utf8(artifact_key.to_vec())
                            .context("parse P2P catalog owner key")?;
                        let held: BTreeSet<P2pPublishOwner> = serde_json::from_slice(&bytes)
                            .with_context(|| {
                                format!("parse P2P catalog owner set {artifact_key}")
                            })?;
                        owners.insert(artifact_key, held);
                        return Ok(());
                    }
                    let descriptor: P2pArtifactDescriptor = serde_json::from_slice(&bytes)
                        .with_context(|| {
                            format!("parse P2P catalog entry {}", String::from_utf8_lossy(&key))
                        })?;
                    descriptors.insert(descriptor.key.clone(), descriptor);
                    Ok(())
                },
            )
            .await
            .with_context(|| format!("scan P2P catalog {}", db_path.display()))?;

        // A descriptor with no owner record was written before ownership
        // existed, or by a publisher that never named itself. Either way it has
        // exactly one claim on the artifact, which is what unscoped means.
        for key in loaded.keys() {
            owners
                .entry(key.clone())
                .or_insert_with(|| BTreeSet::from([P2pPublishOwner::Unscoped]));
        }
        owners.retain(|key, held| loaded.contains_key(key) && !held.is_empty());

        tracing::debug!(
            catalog = %db_path.display(),
            entry_count = loaded.len(),
            "loaded persisted P2P catalog"
        );
        Ok(Self {
            inner: Arc::new(RwLock::new(loaded)),
            owners: Arc::new(RwLock::new(owners)),
            store,
            local_provider: P2pPeer {
                node_id: node_id.to_string(),
                endpoint: local_endpoint.clone(),
            },
        })
    }

    pub(crate) async fn descriptor_for(
        &self,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        self.inner.read().await.get(key).cloned()
    }

    /// A non-owning handle to the advertised keys.
    ///
    /// Used to re-announce to a scheduler that has forgotten them. The catalog
    /// is the node's own durable record, so it survives a scheduler restart
    /// even though the scheduler's index does not.
    ///
    /// Deliberately weak and over the key map alone rather than the catalog:
    /// a background task holding a full catalog clone would keep its RocksDB
    /// handle open, so a transport that had been dropped could not release the
    /// database lock and a restart in the same process would fail to reopen it.
    pub(crate) fn keys_handle(
        &self,
    ) -> Weak<RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>> {
        Arc::downgrade(&self.inner)
    }

    pub(crate) async fn upsert(&self, descriptor: P2pArtifactDescriptor) -> Result<()> {
        let bytes = serde_json::to_vec(&descriptor).context("serialize P2P catalog entry")?;
        self.store
            .put(descriptor.key.as_bytes(), bytes)
            .await
            .with_context(|| format!("persist P2P catalog entry {}", descriptor.key))?;
        let mut catalog = self.inner.write().await;
        catalog.insert(descriptor.key.clone(), descriptor);
        Ok(())
    }

    pub(crate) async fn remove(
        &self,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        self.store
            .delete(key.as_bytes())
            .await
            .with_context(|| format!("delete P2P catalog entry {key}"))?;
        self.store
            .delete(owners_store_key(key))
            .await
            .with_context(|| format!("delete P2P catalog owner set {key}"))?;
        self.owners.write().await.remove(key);
        let removed = self.inner.write().await.remove(key);
        Ok(removed)
    }

    /// Record that `owner` is advertising `key`.
    pub(crate) async fn add_owner(
        &self,
        key: &P2pArtifactKey,
        owner: P2pPublishOwner,
    ) -> Result<()> {
        let mut owners = self.owners.write().await;
        let held = owners.entry(key.clone()).or_default();
        if !held.insert(owner) {
            return Ok(());
        }
        let bytes = serde_json::to_vec(held).context("serialize P2P catalog owner set")?;
        // Written under the same lock as the in-memory set so a concurrent
        // release cannot persist a set that has already been superseded.
        self.store
            .put(owners_store_key(key), bytes)
            .await
            .with_context(|| format!("persist P2P catalog owner set {key}"))?;
        Ok(())
    }

    /// Drop `owner`'s claim on `key`, reporting whether anyone still holds it.
    pub(crate) async fn release_owner(
        &self,
        key: &P2pArtifactKey,
        owner: P2pPublishOwner,
    ) -> Result<OwnerRelease> {
        let mut owners = self.owners.write().await;
        let Some(held) = owners.get_mut(key) else {
            return Ok(OwnerRelease::Absent);
        };
        held.remove(&owner);
        if held.is_empty() {
            owners.remove(key);
            return Ok(OwnerRelease::Withdrawn);
        }
        let bytes = serde_json::to_vec(held).context("serialize P2P catalog owner set")?;
        self.store
            .put(owners_store_key(key), bytes)
            .await
            .with_context(|| format!("persist P2P catalog owner set {key}"))?;
        Ok(OwnerRelease::Retained)
    }

    #[cfg(test)]
    pub(crate) async fn owners_of(&self, key: &P2pArtifactKey) -> BTreeSet<P2pPublishOwner> {
        self.owners
            .read()
            .await
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}

fn owners_store_key(key: &P2pArtifactKey) -> Vec<u8> {
    let mut store_key = Vec::with_capacity(OWNERS_KEY_PREFIX.len() + key.len());
    store_key.extend_from_slice(OWNERS_KEY_PREFIX);
    store_key.extend_from_slice(key.as_bytes());
    store_key
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogProtocol {
    published_catalog: PublishedArtifactCatalog,
}

impl CatalogProtocol {
    pub(crate) fn new(published_catalog: PublishedArtifactCatalog) -> Self {
        Self { published_catalog }
    }

    async fn descriptor_for_response(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.published_catalog
            .descriptor_for(key)
            .await
            .map(|mut descriptor| {
                descriptor.providers = vec![self.published_catalog.local_provider.clone().into()];
                descriptor
            })
    }
}

impl ProtocolHandler for CatalogProtocol {
    /// Serves lookups until the peer closes the connection.
    ///
    /// This used to answer exactly one stream and then wait out a five-second
    /// close timeout. Clients now hold catalog connections in a pool and ask
    /// many questions over one of them, and the second question on a reused
    /// connection would open a stream nobody was left to accept.
    ///
    /// Requests are served one at a time. A lookup is a map read and a small
    /// write, so pipelining them would buy nothing and would let one peer fan
    /// out unbounded work on this node.
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                // Every way a connection ends arrives here, including the
                // ordinary one where the peer is simply done with it, so this
                // is the end of the handler rather than a failure to report.
                Err(err) => {
                    tracing::trace!(error = %err, "P2P catalog connection closed");
                    return Ok(());
                }
            };
            let request_bytes = recv
                .read_to_end(MAX_CATALOG_REQUEST_BYTES)
                .await
                .map_err(AcceptError::from_err)?;
            let request: CatalogRequest =
                serde_json::from_slice(&request_bytes).map_err(AcceptError::from_err)?;
            let descriptor = self.descriptor_for_response(&request.key).await;
            let found = descriptor.is_some();
            let response = CatalogResponse { descriptor };
            let response_bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
            send.write_all(&response_bytes)
                .await
                .map_err(AcceptError::from_err)?;
            send.finish()?;
            tracing::trace!(key = %request.key, found, "served P2P catalog request");
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use super::*;
    use crate::p2p::types::P2pArtifactProvider;

    fn endpoint(address: &str) -> P2pEndpoint {
        P2pEndpoint {
            backend: "iroh".to_string(),
            address: address.to_string(),
        }
    }

    fn provider(node_id: &str, address: &str) -> P2pArtifactProvider {
        P2pArtifactProvider::from(P2pPeer {
            node_id: node_id.to_string(),
            endpoint: endpoint(address),
        })
    }

    #[tokio::test]
    async fn response_descriptor_uses_current_local_provider() -> anyhow::Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let local_endpoint = endpoint("fresh-local-endpoint");
        let catalog = PublishedArtifactCatalog::load(
            &temp.path().join("catalog.db"),
            "current-node",
            &local_endpoint,
        )
        .await
        .context("load catalog")?;
        let key = "test/p2p/catalog/accept-provider".to_string();
        catalog
            .upsert(P2pArtifactDescriptor {
                key: key.clone(),
                providers: vec![
                    P2pArtifactProvider::Local,
                    provider("stale-node", "stale-endpoint"),
                ],
                backend_locator: Some("blob-hash".to_string()),
                metadata: serde_json::json!({ "kind": "catalog-accept-test" }),
            })
            .await
            .context("upsert descriptor")?;
        let protocol = CatalogProtocol::new(catalog);

        let descriptor = protocol
            .descriptor_for_response(&key)
            .await
            .context("descriptor should be present")?;

        assert_eq!(descriptor.backend_locator, Some("blob-hash".to_string()));
        assert_eq!(
            descriptor.metadata,
            serde_json::json!({ "kind": "catalog-accept-test" })
        );
        assert_eq!(
            descriptor.providers,
            vec![P2pArtifactProvider::from(P2pPeer {
                node_id: "current-node".to_string(),
                endpoint: local_endpoint,
            })]
        );
        Ok(())
    }
}
