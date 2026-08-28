use std::path::Path;
use std::pin::Pin;

use super::error::{Error, Result};
use super::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProviderHint, P2pEndpoint, P2pPublishRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

pub type P2pByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

#[async_trait]
/// Abstraction for artifact discovery and transfer between peers.
pub trait P2pTransport: Send + Sync {
    /// Resolve an artifact descriptor for a key using the transport's configured discovery.
    async fn lookup(&self, key: &P2pArtifactKey) -> Result<Option<P2pArtifactDescriptor>> {
        self.lookup_with_hints(key, &[]).await
    }

    /// Resolve an artifact descriptor for a key, prioritizing provider hints supplied by callers.
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>>;

    /// Download the artifact described by `descriptor` into `destination` and return its size in bytes.
    ///
    /// `max_bytes` bounds what the peer can make this node write, and is
    /// enforced against the bytes as they arrive. A descriptor names an
    /// artifact that has not been authenticated yet — the sealing check
    /// happens afterwards, on content this call has already materialised — so
    /// the peer is free to offer something arbitrarily large, and only the
    /// caller knows what size the thing it asked for could legitimately be.
    async fn fetch(
        &self,
        descriptor: &P2pArtifactDescriptor,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<u64>;

    /// Download the full artifact described by `descriptor` into memory.
    ///
    /// Callers should prefer [`Self::fetch`] for large artifacts to avoid buffering
    /// the full artifact in the process. `max_bytes` bounds it either way, and
    /// here it bounds resident memory rather than disk.
    async fn fetch_bytes(
        &self,
        descriptor: &P2pArtifactDescriptor,
        max_bytes: u64,
    ) -> Result<Bytes>;

    /// Stream an exact byte range from the artifact described by `descriptor`.
    ///
    /// Implementations must yield exactly `len` bytes or end the stream with an
    /// error. Callers may already have committed response headers before polling
    /// the stream, so short reads must not complete successfully.
    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream>;

    /// Publish a local artifact to the transport.
    ///
    /// Disabled transports return `Ok(())` so callers can treat P2P publishing
    /// as an optional acceleration path.
    async fn publish(&self, request: &P2pPublishRequest) -> Result<()>;

    /// Stop advertising a local artifact.
    ///
    /// Returns `true` when a local publication was removed, `false` when no local artifact existed for the key.
    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool>;

    /// Return the local endpoint if this transport exposes one.
    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        None
    }

    /// Shut down the transport and release resources.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DisabledP2pTransport;

#[async_trait]
impl P2pTransport for DisabledP2pTransport {
    async fn lookup_with_hints(
        &self,
        _key: &P2pArtifactKey,
        _hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        Ok(None)
    }

    async fn fetch(
        &self,
        _descriptor: &P2pArtifactDescriptor,
        _destination: &Path,
        _max_bytes: u64,
    ) -> Result<u64> {
        Err(Error::TransportDisabled)
    }

    async fn fetch_bytes(
        &self,
        _descriptor: &P2pArtifactDescriptor,
        _max_bytes: u64,
    ) -> Result<Bytes> {
        Err(Error::TransportDisabled)
    }

    async fn fetch_byte_range(
        &self,
        _descriptor: &P2pArtifactDescriptor,
        _offset: u64,
        _len: usize,
    ) -> Result<P2pByteStream> {
        Err(Error::TransportDisabled)
    }

    async fn publish(&self, _request: &P2pPublishRequest) -> Result<()> {
        Ok(())
    }

    async fn unpublish(&self, _key: &P2pArtifactKey) -> Result<bool> {
        Ok(false)
    }
}
