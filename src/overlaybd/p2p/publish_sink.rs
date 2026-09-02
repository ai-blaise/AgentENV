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
//!
//! The two directions are deliberately asymmetric. Advertising a layer is
//! queued, because a QUIC round trip and a RocksDB write have no business on
//! the path that just committed an image and the layer is perfectly usable
//! whether or not peers hear about it. Withdrawing one is awaited, because
//! publication is by reference: the transport holds the commit file itself, so
//! a queued withdrawal would lose the race against the `remove_file` that
//! follows it and leave peers resolving a descriptor whose bytes are gone.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::p2p::metrics::{self as p2p_metrics, PublishStatus};
use crate::p2p::{P2pPublishMode, P2pPublishOwner, P2pPublishRequest, P2pTransport};

use super::artifact::{layer_key_from_digest, LayerMetadata};

/// Advertisements that may be waiting on the transport at once.
///
/// Deep enough that a burst of layer commits from one image conversion never
/// touches the bound, shallow enough that a transport wedged for minutes costs
/// bounded memory rather than growing without limit behind it. Overflow drops
/// the advertisement: the alternative is to make the commit path wait on the
/// slow thing this queue exists to decouple it from.
const PUBLISH_QUEUE_CAPACITY: usize = 512;

/// Advertises a layer that has just landed in the local commit store.
#[async_trait]
pub(crate) trait LayerPublishSink: Send + Sync + std::fmt::Debug {
    /// Publish is best-effort by contract: a failure to advertise must never
    /// fail the image operation that produced the layer, because the layer is
    /// perfectly usable locally either way. It must also never block that
    /// operation, so this returns as soon as the work is accepted.
    async fn publish_layer(&self, digest: &str, size: u64, path: &Path);

    /// Withdraw a layer this node is about to delete.
    ///
    /// Awaited on purpose, and ordered before the file is unlinked: the
    /// transport was handed the commit file by reference, so between the
    /// unlink and the withdrawal any peer that resolved the descriptor reads a
    /// path with nothing behind it.
    async fn unpublish_layer(&self, digest: &str);

    /// The image cache has finished a maintenance pass over the commit store.
    ///
    /// The transport's collector is gated so it sweeps on events rather than
    /// on every interval, and a commit-store sweep is exactly the moment its
    /// retention is most likely to have just gone stale.
    async fn commit_store_maintained(&self);
}

/// The default: publishes nothing.
#[derive(Debug)]
pub(crate) struct DisabledLayerPublishSink;

#[async_trait]
impl LayerPublishSink for DisabledLayerPublishSink {
    async fn publish_layer(&self, _digest: &str, _size: u64, _path: &Path) {}
    async fn unpublish_layer(&self, _digest: &str) {}
    async fn commit_store_maintained(&self) {}
}

#[derive(Debug)]
struct PublishJob {
    digest: String,
    size: u64,
    path: PathBuf,
}

/// Publishes through a P2P transport.
pub(crate) struct TransportLayerPublishSink {
    transport: Arc<dyn P2pTransport>,
    publish_tx: mpsc::Sender<PublishJob>,
}

impl std::fmt::Debug for TransportLayerPublishSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // P2pTransport is not Debug, so describe the sink by its queue rather
        // than by its transport.
        f.debug_struct("TransportLayerPublishSink")
            .field(
                "queued",
                &(PUBLISH_QUEUE_CAPACITY - self.publish_tx.capacity()),
            )
            .finish_non_exhaustive()
    }
}

/// Directories whose contents may be advertised.
///
/// The same containment check the HTTP endpoint applies. Publishing by
/// reference hands peers a path the transport will read from later, so it must
/// not be possible to advertise a file outside the commit store.
struct PublishRoots(Vec<PathBuf>);

impl PublishRoots {
    fn allows(&self, path: &Path) -> bool {
        let Ok(canonical) = path.canonicalize() else {
            return false;
        };
        self.0.iter().any(|root| {
            root.canonicalize()
                .map(|root| canonical.starts_with(root))
                .unwrap_or(false)
        })
    }
}

impl TransportLayerPublishSink {
    pub(crate) fn new(transport: Arc<dyn P2pTransport>, allowed_roots: Vec<PathBuf>) -> Self {
        let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_QUEUE_CAPACITY);
        tokio::spawn(drain_publish_queue(
            Arc::clone(&transport),
            PublishRoots(allowed_roots),
            publish_rx,
        ));
        Self {
            transport,
            publish_tx,
        }
    }
}

/// Drains queued advertisements one at a time for the life of the sink.
///
/// Serial by design: publication order is the order layers were committed, and
/// a single drainer is what makes a full queue a bounded backlog rather than
/// an unbounded fan-out of concurrent QUIC work.
async fn drain_publish_queue(
    transport: Arc<dyn P2pTransport>,
    roots: PublishRoots,
    mut publish_rx: mpsc::Receiver<PublishJob>,
) {
    while let Some(job) = publish_rx.recv().await {
        let key = layer_key_from_digest(&job.digest);
        if !roots.allows(&job.path) {
            warn!(
                digest = job.digest,
                path = %job.path.display(),
                "refusing to advertise a layer outside the publishable roots"
            );
            p2p_metrics::record_publish(&key, P2pPublishOwner::ImageCache, PublishStatus::Refused);
            continue;
        }

        let request = P2pPublishRequest {
            key,
            source: crate::p2p::P2pPublishSource::Path(job.path),
            metadata: LayerMetadata::from_digest(&job.digest, Some(job.size), None).to_value(),
            // Reference rather than Copy: the commit store already holds the
            // bytes and outlives the advertisement, so duplicating them into
            // the transport's own store would double the disk cost of every
            // cached layer.
            publish_mode: P2pPublishMode::Reference,
            owner: P2pPublishOwner::ImageCache,
        };

        // The transport records its own publish outcome, so this arm only
        // needs to say what happens to the layer either way.
        match transport.publish(&request).await {
            Ok(()) => debug!(
                digest = job.digest,
                size = job.size,
                "advertised overlaybd layer to peers"
            ),
            Err(error) => warn!(
                digest = job.digest,
                error = %error,
                "failed to advertise overlaybd layer; it remains usable locally"
            ),
        }
    }
}

#[async_trait]
impl LayerPublishSink for TransportLayerPublishSink {
    async fn publish_layer(&self, digest: &str, size: u64, path: &Path) {
        let job = PublishJob {
            digest: digest.to_string(),
            size,
            path: path.to_path_buf(),
        };
        if let Err(err) = self.publish_tx.try_send(job) {
            warn!(
                digest,
                reason = %err,
                "dropping a layer advertisement rather than stalling the commit path"
            );
            p2p_metrics::record_publish(
                &layer_key_from_digest(digest),
                P2pPublishOwner::ImageCache,
                PublishStatus::Dropped,
            );
        }
    }

    async fn unpublish_layer(&self, digest: &str) {
        let key = layer_key_from_digest(digest);
        // The same digest can be advertised by snapshot publication as well,
        // so this releases the image cache's claim only; the artifact stays up
        // until every claim on it is gone.
        match self
            .transport
            .unpublish_owned(&key, P2pPublishOwner::ImageCache)
            .await
        {
            Ok(withdrawn) => debug!(digest, withdrawn, "released the image cache's P2P claim"),
            Err(error) => warn!(
                digest,
                error = %error,
                "failed to withdraw an overlaybd layer; peers may resolve a deleted file"
            ),
        }
    }

    async fn commit_store_maintained(&self) {
        self.transport.request_gc().await;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Reports every transport call on a channel, so a test waits for the
    /// drainer rather than sleeping and hoping.
    struct RecordingTransport {
        events: mpsc::UnboundedSender<TransportEvent>,
        /// Held to stall the drainer, proving the commit path does not wait on it.
        publish_gate: Option<Arc<tokio::sync::Semaphore>>,
        gc_requests: Arc<AtomicUsize>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum TransportEvent {
        Published {
            key: P2pArtifactKey,
            owner: P2pPublishOwner,
            mode: P2pPublishMode,
        },
        Unpublished {
            key: P2pArtifactKey,
            owner: P2pPublishOwner,
        },
    }

    fn recording_transport() -> (
        Arc<RecordingTransport>,
        mpsc::UnboundedReceiver<TransportEvent>,
    ) {
        let (events, rx) = mpsc::unbounded_channel();
        (
            Arc::new(RecordingTransport {
                events,
                publish_gate: None,
                gc_requests: Arc::new(AtomicUsize::new(0)),
            }),
            rx,
        )
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
            _max_bytes: u64,
        ) -> P2pResult<u64> {
            unreachable!("publish-only transport")
        }

        async fn fetch_bytes(
            &self,
            _descriptor: &P2pArtifactDescriptor,
            _max_bytes: u64,
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
            if let Some(gate) = &self.publish_gate {
                let _permit = gate.acquire().await;
            }
            let _ = self.events.send(TransportEvent::Published {
                key: request.key.clone(),
                owner: request.owner,
                mode: request.publish_mode,
            });
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
            let _ = self.events.send(TransportEvent::Unpublished {
                key: key.clone(),
                owner,
            });
            Ok(true)
        }

        async fn request_gc(&self) {
            self.gc_requests.fetch_add(1, Ordering::SeqCst);
        }

        fn local_endpoint(&self) -> Option<P2pEndpoint> {
            None
        }
    }

    async fn next_event(rx: &mut mpsc::UnboundedReceiver<TransportEvent>) -> TransportEvent {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("transport should have been called")
            .expect("transport channel should stay open")
    }

    #[tokio::test]
    async fn publishes_a_layer_inside_an_allowed_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let layer = root.path().join("layer.commit");
        std::fs::write(&layer, b"bytes").expect("write layer");

        let (transport, mut events) = recording_transport();
        let sink = TransportLayerPublishSink::new(transport, vec![root.path().to_path_buf()]);

        sink.publish_layer("sha256:abc", 5, &layer).await;

        let TransportEvent::Published { key, owner, mode } = next_event(&mut events).await else {
            panic!("expected a publish");
        };
        assert!(
            key.contains("sha256:abc"),
            "advertised under the layer key namespace, got {key}"
        );
        assert_eq!(owner, P2pPublishOwner::ImageCache);
        assert_eq!(mode, P2pPublishMode::Reference);
    }

    /// Publishing by reference hands peers a path read later, so a file outside
    /// the commit store must never be advertised.
    ///
    /// The allowed layer queued behind it is the ordering anchor: the drainer
    /// is serial, so seeing it first proves the rejected one was skipped rather
    /// than merely still in flight.
    #[tokio::test]
    async fn refuses_to_publish_outside_the_allowed_roots() {
        let allowed = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let outside = elsewhere.path().join("layer.commit");
        std::fs::write(&outside, b"bytes").expect("write layer");
        let inside = allowed.path().join("layer.commit");
        std::fs::write(&inside, b"bytes").expect("write layer");

        let (transport, mut events) = recording_transport();
        let sink = TransportLayerPublishSink::new(transport, vec![allowed.path().to_path_buf()]);

        sink.publish_layer("sha256:outside", 5, &outside).await;
        sink.publish_layer("sha256:inside", 5, &inside).await;

        let TransportEvent::Published { key, .. } = next_event(&mut events).await else {
            panic!("expected a publish");
        };
        assert!(
            key.contains("sha256:inside"),
            "a layer outside the publishable roots must not be advertised, got {key}"
        );
    }

    /// The commit path must not wait on the transport.
    ///
    /// The transport here never returns, so an inline publish would hang this
    /// call; the queue must absorb it and hand control straight back.
    #[tokio::test]
    async fn a_wedged_transport_does_not_stall_the_commit_path() {
        let root = tempfile::tempdir().expect("tempdir");
        let layer = root.path().join("layer.commit");
        std::fs::write(&layer, b"bytes").expect("write layer");

        let (events, _rx) = mpsc::unbounded_channel();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let transport = Arc::new(RecordingTransport {
            events,
            publish_gate: Some(Arc::clone(&gate)),
            gc_requests: Arc::new(AtomicUsize::new(0)),
        });
        let sink = TransportLayerPublishSink::new(transport, vec![root.path().to_path_buf()]);

        // One more than the queue holds, so the overflow arm runs too.
        for index in 0..PUBLISH_QUEUE_CAPACITY + 8 {
            tokio::time::timeout(
                Duration::from_secs(5),
                sink.publish_layer(&format!("sha256:{index}"), 5, &layer),
            )
            .await
            .expect("publish must not block on a wedged transport");
        }
    }

    /// The withdrawal has to have reached the transport before the caller
    /// returns, because the caller's next act is to unlink the file the
    /// transport was handed.
    #[tokio::test]
    async fn withdrawal_reaches_the_transport_before_it_returns() {
        let root = tempfile::tempdir().expect("tempdir");
        let (transport, mut events) = recording_transport();
        let sink = TransportLayerPublishSink::new(transport, vec![root.path().to_path_buf()]);

        sink.unpublish_layer("sha256:abc").await;

        let event = events
            .try_recv()
            .expect("the withdrawal must already have been made, not merely queued");
        let TransportEvent::Unpublished { key, owner } = event else {
            panic!("expected a withdrawal");
        };
        assert!(key.contains("sha256:abc"), "withdrew the wrong key: {key}");
        assert_eq!(
            owner,
            P2pPublishOwner::ImageCache,
            "the image cache must release only its own claim"
        );
    }

    #[tokio::test]
    async fn a_maintenance_pass_arms_the_transport_collector() {
        let root = tempfile::tempdir().expect("tempdir");
        let (transport, _events) = recording_transport();
        let gc_requests = Arc::clone(&transport.gc_requests);
        let sink = TransportLayerPublishSink::new(transport, vec![root.path().to_path_buf()]);

        sink.commit_store_maintained().await;

        assert_eq!(
            gc_requests.load(Ordering::SeqCst),
            1,
            "a commit-store sweep must arm the P2P collector"
        );
    }

    /// The default sink must be safe to call unconditionally, so callers need
    /// no P2P-aware branch.
    #[tokio::test]
    async fn disabled_sink_is_a_no_op() {
        DisabledLayerPublishSink
            .publish_layer("sha256:abc", 1, Path::new("/nonexistent"))
            .await;
        DisabledLayerPublishSink.unpublish_layer("sha256:abc").await;
        DisabledLayerPublishSink.commit_store_maintained().await;
    }
}
