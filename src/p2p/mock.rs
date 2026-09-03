use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use super::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pByteStream, P2pEndpoint, P2pError, P2pPublishOwner, P2pPublishRequest, P2pPublishSource,
    P2pResult, P2pTransport,
};

#[derive(Clone, Default)]
pub(crate) struct MockTransport {
    pub(crate) descriptors: Arc<RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>>,
    pub(crate) blobs: Arc<RwLock<HashMap<P2pArtifactKey, Bytes>>>,
    pub(crate) lookup_count: Arc<AtomicUsize>,
    pub(crate) fetch_count: Arc<AtomicUsize>,
    pub(crate) fetch_bytes_count: Arc<AtomicUsize>,
    pub(crate) fetch_range_count: Arc<AtomicUsize>,
    pub(crate) publish_count: Arc<AtomicUsize>,
    /// Every publish request the facade sent, so a test can assert the mode
    /// and owner it chose rather than only that it published.
    pub(crate) published: Arc<RwLock<Vec<P2pPublishRequest>>>,
    pub(crate) gc_requests: Arc<AtomicUsize>,
    pub(crate) lookup_delay: Option<Duration>,
    pub(crate) fetch_range_delay: Option<Duration>,
    pub(crate) fail_lookup: Arc<AtomicBool>,
    pub(crate) fail_publish: Arc<AtomicBool>,
    pub(crate) fail_fetch_range_stream_after_first_chunk: Arc<AtomicBool>,
    /// The stream is handed back promptly and then never yields anything.
    ///
    /// This is the peer that accepts the connection and stalls -- alive enough
    /// to answer keepalives, wedged enough to send no bytes. It is distinct
    /// from `fetch_range_delay`, which delays the call that returns the stream:
    /// a caller that only bounds that call sees this one as a success.
    pub(crate) stall_fetch_range_stream: Arc<AtomicBool>,
}

#[async_trait]
impl P2pTransport for MockTransport {
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        _hints: &[P2pArtifactProviderHint],
    ) -> P2pResult<Option<P2pArtifactDescriptor>> {
        debug!(?key, "looking up");
        self.lookup_count.fetch_add(1, Ordering::Relaxed);
        if let Some(delay) = self.lookup_delay {
            debug!(?key, delay_ms = delay.as_millis(), "lookup delay");
            tokio::time::sleep(delay).await;
        }
        if self.fail_lookup.load(Ordering::Relaxed) {
            warn!(?key, "lookup forced failure");
            return Err(P2pError::Internal(anyhow!("forced lookup failure")));
        }
        let result = self.descriptors.read().await.get(key).cloned();
        debug!(?key, found = result.is_some(), "lookup result");
        Ok(result)
    }

    async fn fetch(
        &self,
        descriptor: &P2pArtifactDescriptor,
        destination: &Path,
        max_bytes: u64,
    ) -> P2pResult<u64> {
        debug!(key = ?descriptor.key, dest = %destination.display(), "fetching");
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let blobs = self.blobs.read().await;
        let bytes = blobs
            .get(&descriptor.key)
            .ok_or_else(|| P2pError::InvalidDescriptor {
                reason: "missing test blob".to_string(),
            })?;
        // Enforced here too, so a caller's cap is exercised by the tests that
        // use this rather than only by the one transport that ships.
        if bytes.len() as u64 > max_bytes {
            return Err(P2pError::ArtifactTooLarge { limit: max_bytes });
        }
        tokio::fs::write(destination, bytes)
            .await
            .map_err(|err| P2pError::Internal(anyhow!("write mock fetch: {err}")))?;
        debug!(key = ?descriptor.key, size = bytes.len(), "fetch wrote bytes");
        Ok(bytes.len() as u64)
    }

    async fn fetch_bytes(
        &self,
        descriptor: &P2pArtifactDescriptor,
        max_bytes: u64,
    ) -> P2pResult<Bytes> {
        debug!(key = ?descriptor.key, "fetch bytes");
        self.fetch_bytes_count.fetch_add(1, Ordering::Relaxed);
        let blobs = self.blobs.read().await;
        let bytes =
            blobs
                .get(&descriptor.key)
                .cloned()
                .ok_or_else(|| P2pError::InvalidDescriptor {
                    reason: "missing test blob".to_string(),
                })?;
        if bytes.len() as u64 > max_bytes {
            return Err(P2pError::ArtifactTooLarge { limit: max_bytes });
        }
        Ok(bytes)
    }

    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> P2pResult<P2pByteStream> {
        debug!(
            key = ?descriptor.key,
            offset,
            len,
            "fetching byte range"
        );
        self.fetch_range_count.fetch_add(1, Ordering::Relaxed);
        if let Some(delay) = self.fetch_range_delay {
            debug!(
                key = ?descriptor.key,
                delay_ms = delay.as_millis(),
                "fetch byte range delay"
            );
            tokio::time::sleep(delay).await;
        }
        let blobs = self.blobs.read().await;
        let bytes = blobs
            .get(&descriptor.key)
            .ok_or_else(|| P2pError::InvalidDescriptor {
                reason: "missing test blob".to_string(),
            })?;
        let start = usize::try_from(offset).map_err(|err| P2pError::InvalidDescriptor {
            reason: format!("invalid offset: {err}"),
        })?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| P2pError::InvalidDescriptor {
                reason: "range overflow".to_string(),
            })?;
        if end > bytes.len() {
            return Err(P2pError::InvalidDescriptor {
                reason: "range outside blob".to_string(),
            });
        }
        let chunks = bytes
            .slice(start..end)
            .chunks(1024)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<P2pResult<Bytes>>>();
        // Small chunks intentionally exercise consumers that handle multi-item streams.
        let chunks = if self
            .fail_fetch_range_stream_after_first_chunk
            .load(Ordering::Relaxed)
        {
            chunks
                .into_iter()
                .take(1)
                .chain(std::iter::once(Err(P2pError::Internal(anyhow!(
                    "forced range stream failure"
                )))))
                .collect()
        } else {
            chunks
        };
        if self.stall_fetch_range_stream.load(Ordering::Relaxed) {
            return Ok(Box::pin(stream::pending()));
        }
        Ok(Box::pin(stream::iter(chunks)))
    }

    async fn publish(&self, request: &P2pPublishRequest) -> P2pResult<()> {
        debug!(key = ?request.key, "publishing");
        self.publish_count.fetch_add(1, Ordering::Relaxed);
        self.published.write().await.push(request.clone());
        if self.fail_publish.load(Ordering::Relaxed) {
            warn!(key = ?request.key, "publish forced failure");
            return Err(P2pError::Internal(anyhow!("forced publish failure")));
        }
        let bytes: Bytes = match &request.source {
            P2pPublishSource::Path(path) => tokio::fs::read(&path)
                .await
                .map_err(|err| P2pError::Internal(anyhow!("read mock publish: {err}")))?
                .into(),
            P2pPublishSource::Bytes(bytes) => bytes.clone(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: request.key.clone(),
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some("mock".to_string()),
            metadata: request.metadata.clone(),
        };
        self.descriptors
            .write()
            .await
            .insert(request.key.clone(), descriptor);
        self.blobs.write().await.insert(request.key.clone(), bytes);
        debug!(key = ?request.key, "published");
        Ok(())
    }

    async fn unpublish(&self, key: &P2pArtifactKey) -> P2pResult<bool> {
        self.unpublish_owned(key, P2pPublishOwner::Unscoped).await
    }

    async fn unpublish_owned(
        &self,
        key: &P2pArtifactKey,
        owner: P2pPublishOwner,
    ) -> P2pResult<bool> {
        let removed = self.descriptors.write().await.remove(key).is_some();
        self.blobs.write().await.remove(key);
        debug!(?key, owner = owner.as_str(), removed, "unpublished");
        Ok(removed)
    }

    async fn request_gc(&self) {
        self.gc_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        Some(P2pEndpoint {
            backend: "mock".to_string(),
            address: "mock".to_string(),
        })
    }
}
