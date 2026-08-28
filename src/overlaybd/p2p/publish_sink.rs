//! Advertising freshly cached overlaybd layers to peers.
//!
//! The facade exposes `/p2p-control/publish-layer` for by-reference publication
//! of a downloaded layer, but nothing ever called it: the publish URL was
//! plumbed from the server through the ublk daemon client into the image
//! service and then read only by a `Debug` impl. The consequence is that layers
//! pulled from a registry never entered the P2P store at all — only snapshot
//! commits did — so peers could never serve each other the layers they had just
//! downloaded, which is most of what P2P exists to accelerate.
//!
//! This is the in-process equivalent of that endpoint. Publishing directly
//! avoids a loopback HTTP round trip and, more importantly, the full re-hash
//! the endpoint performs to validate a caller it cannot trust: the image cache
//! has already verified the digest as a condition of storing the file.
//!
//! The sink is a trait with a no-op default so the image cache carries no P2P
//! knowledge and needs no conditional path; startup decides whether a real one
//! is installed.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tracing::{debug, warn};

use crate::p2p::{P2pPublishMode, P2pPublishRequest, P2pTransport};

use super::artifact::{layer_key_from_digest, LayerMetadata};

/// Advertises a layer that has just landed in the local commit store.
#[async_trait]
pub(crate) trait LayerPublishSink: Send + Sync + std::fmt::Debug {
    /// Publish is best-effort by contract: a failure to advertise must never
    /// fail the image operation that produced the layer, because the layer is
    /// perfectly usable locally either way.
    async fn publish_layer(&self, digest: &str, size: u64, path: &Path);
}

/// The default: publishes nothing.
#[derive(Debug)]
pub(crate) struct DisabledLayerPublishSink;

#[async_trait]
impl LayerPublishSink for DisabledLayerPublishSink {
    async fn publish_layer(&self, _digest: &str, _size: u64, _path: &Path) {}
}

/// Publishes through a P2P transport.
pub(crate) struct TransportLayerPublishSink {
    transport: Arc<dyn P2pTransport>,
    /// Directories whose contents may be advertised.
    ///
    /// The same containment check the HTTP endpoint applies. Publishing by
    /// reference hands peers a path the transport will read from later, so it
    /// must not be possible to advertise a file outside the commit store.
    allowed_roots: Vec<PathBuf>,
}

impl std::fmt::Debug for TransportLayerPublishSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // P2pTransport is not Debug, so describe the sink by what it is allowed
        // to advertise rather than by its transport.
        f.debug_struct("TransportLayerPublishSink")
            .field("allowed_roots", &self.allowed_roots)
            .finish_non_exhaustive()
    }
}

impl TransportLayerPublishSink {
    pub(crate) fn new(transport: Arc<dyn P2pTransport>, allowed_roots: Vec<PathBuf>) -> Self {
        Self {
            transport,
            allowed_roots,
        }
    }

    fn is_allowed(&self, path: &Path) -> bool {
        let Ok(canonical) = path.canonicalize() else {
            return false;
        };
        self.allowed_roots.iter().any(|root| {
            root.canonicalize()
                .map(|root| canonical.starts_with(root))
                .unwrap_or(false)
        })
    }
}

#[async_trait]
impl LayerPublishSink for TransportLayerPublishSink {
    async fn publish_layer(&self, digest: &str, size: u64, path: &Path) {
        if !self.is_allowed(path) {
            warn!(
                digest,
                path = %path.display(),
                "refusing to advertise a layer outside the publishable roots"
            );
            return;
        }

        let request = P2pPublishRequest {
            key: layer_key_from_digest(digest),
            source: crate::p2p::P2pPublishSource::Path(path.to_path_buf()),
            metadata: LayerMetadata::from_digest(digest, Some(size), None).to_value(),
            // Reference rather than Copy: the commit store already holds the
            // bytes and outlives the advertisement, so duplicating them into
            // the transport's own store would double the disk cost of every
            // cached layer.
            publish_mode: P2pPublishMode::Reference,
        };

        match self.transport.publish(&request).await {
            Ok(()) => debug!(digest, size, "advertised overlaybd layer to peers"),
            Err(error) => warn!(
                digest,
                error = %error,
                "failed to advertise overlaybd layer; it remains usable locally"
            ),
        }
    }
}

static GLOBAL: OnceLock<Arc<dyn LayerPublishSink>> = OnceLock::new();

/// Installs the process-wide sink. Called once at startup; later calls are
/// ignored so a mis-ordered init cannot silently replace a live sink.
pub(crate) fn set_global_layer_publish_sink(sink: Arc<dyn LayerPublishSink>) {
    if GLOBAL.set(sink).is_err() {
        warn!("layer publish sink was already installed; ignoring the later one");
    }
}

/// Returns the process-wide sink, or a no-op when none was installed.
pub(crate) fn global_layer_publish_sink() -> Arc<dyn LayerPublishSink> {
    GLOBAL
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(DisabledLayerPublishSink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::P2pResult;
    use crate::p2p::{
        P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProviderHint, P2pByteStream, P2pEndpoint,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingTransport {
        published: Mutex<Vec<P2pArtifactKey>>,
    }

    #[async_trait]
    impl P2pTransport for RecordingTransport {
        async fn lookup_with_hints(
            &self,
            _key: &P2pArtifactKey,
            _hints: &[P2pArtifactProviderHint],
        ) -> P2pResult<Option<P2pArtifactDescriptor>> {
            Ok(None)
        }

        async fn fetch(
            &self,
            _descriptor: &P2pArtifactDescriptor,
            _destination: &Path,
        ) -> P2pResult<u64> {
            unreachable!("publish-only transport")
        }

        async fn fetch_bytes(
            &self,
            _descriptor: &P2pArtifactDescriptor,
        ) -> P2pResult<bytes::Bytes> {
            unreachable!("publish-only transport")
        }

        async fn fetch_byte_range(
            &self,
            _descriptor: &P2pArtifactDescriptor,
            _offset: u64,
            _len: usize,
        ) -> P2pResult<P2pByteStream> {
            unreachable!("publish-only transport")
        }

        async fn publish(&self, request: &P2pPublishRequest) -> P2pResult<()> {
            self.published.lock().unwrap().push(request.key.clone());
            Ok(())
        }

        async fn unpublish(&self, _key: &P2pArtifactKey) -> P2pResult<bool> {
            Ok(false)
        }

        fn local_endpoint(&self) -> Option<P2pEndpoint> {
            None
        }
    }

    #[tokio::test]
    async fn publishes_a_layer_inside_an_allowed_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let layer = root.path().join("layer.commit");
        std::fs::write(&layer, b"bytes").expect("write layer");

        let transport = Arc::new(RecordingTransport::default());
        let sink =
            TransportLayerPublishSink::new(transport.clone(), vec![root.path().to_path_buf()]);

        sink.publish_layer("sha256:abc", 5, &layer).await;

        let published = transport.published.lock().unwrap();
        assert_eq!(published.len(), 1, "layer should have been advertised");
        assert!(
            published[0].contains("sha256:abc"),
            "advertised under the layer key namespace, got {}",
            published[0]
        );
    }

    /// Publishing by reference hands peers a path read later, so a file outside
    /// the commit store must never be advertised.
    #[tokio::test]
    async fn refuses_to_publish_outside_the_allowed_roots() {
        let allowed = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let layer = elsewhere.path().join("layer.commit");
        std::fs::write(&layer, b"bytes").expect("write layer");

        let transport = Arc::new(RecordingTransport::default());
        let sink =
            TransportLayerPublishSink::new(transport.clone(), vec![allowed.path().to_path_buf()]);

        sink.publish_layer("sha256:abc", 5, &layer).await;

        assert!(
            transport.published.lock().unwrap().is_empty(),
            "a layer outside the publishable roots must not be advertised"
        );
    }

    /// The default sink must be safe to call unconditionally, so callers need
    /// no P2P-aware branch.
    #[tokio::test]
    async fn disabled_sink_is_a_no_op() {
        DisabledLayerPublishSink
            .publish_layer("sha256:abc", 1, Path::new("/nonexistent"))
            .await;
    }
}
