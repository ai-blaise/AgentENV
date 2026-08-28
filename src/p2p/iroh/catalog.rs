use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Weak};

use anyhow::Context;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::p2p::error::Result;
use crate::p2p::types::{P2pArtifactDescriptor, P2pArtifactKey, P2pEndpoint, P2pPeer};

pub(super) const CATALOG_ALPN: &[u8] = b"/agentenv/artifact-catalog/v1";
pub(super) const MAX_CATALOG_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CATALOG_REQUEST_BYTES: usize = 1024 * 1024;
/// Time to wait for the client to close the connection after we finish
/// sending. Prevents a slow or misbehaving peer from holding a handler task open.
const CONNECTION_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
    store: LocalKvStore,
    local_provider: P2pPeer,
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

        let loaded = store
            .fold(HashMap::new(), |loaded, key, bytes| {
                let descriptor: P2pArtifactDescriptor = serde_json::from_slice(&bytes)
                    .with_context(|| {
                        format!("parse P2P catalog entry {}", String::from_utf8_lossy(&key))
                    })?;
                loaded.insert(descriptor.key.clone(), descriptor);
                Ok(())
            })
            .await
            .with_context(|| format!("scan P2P catalog {}", db_path.display()))?;

        tracing::debug!(
            catalog = %db_path.display(),
            entry_count = loaded.len(),
            "loaded persisted P2P catalog"
        );
        Ok(Self {
            inner: Arc::new(RwLock::new(loaded)),
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
        let removed = self.inner.write().await.remove(key);
        Ok(removed)
    }
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
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
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
        let _ = tokio::time::timeout(CONNECTION_CLOSE_TIMEOUT, connection.closed()).await;
        Ok(())
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
