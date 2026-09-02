use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bao_tree::io::BaoContentItem;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, Watcher};
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::api::downloader::{DownloadProgressItem, Downloader};
use iroh_blobs::api::proto::ExportRangesItem;
use iroh_blobs::get;
use iroh_blobs::protocol::{ChunkRanges, ChunkRangesExt, GetRequest};
use iroh_blobs::store::fs::{options::Options as FsStoreOptions, FsStore};
use iroh_blobs::store::{GcConfig, ProtectOutcome};
use iroh_blobs::util::connection_pool::{
    ConnectionPool, Options as ConnectionPoolOptions, PoolConnectError,
};
use iroh_blobs::{BlobFormat, BlobsProtocol, HashAndFormat};
use tracing::{debug, info, instrument, trace, warn};

use super::catalog::{
    CatalogProtocol, CatalogRequest, CatalogResponse, OwnerRelease, PublishedArtifactCatalog,
    CATALOG_ALPN, MAX_CATALOG_RESPONSE_BYTES,
};
use super::IROH_BACKEND_ID;
use crate::digest;
use crate::p2p::config::ResolvedP2pConfig;
use crate::p2p::discovery::P2pPeerDiscovery;
use crate::p2p::error::{Error, Result};
use crate::p2p::metrics::{
    self as p2p_metrics, LookupConnection, LookupResult, PublishStatus, UnpublishStatus,
};
use crate::p2p::transport::P2pTransport;
use crate::p2p::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pEndpoint, P2pPeer, P2pPublishMode, P2pPublishOwner, P2pPublishRequest, P2pPublishSource,
};
use crate::p2p::P2pByteStream;

const CATALOG_DB_DIR: &str = "catalog.db";
const MAX_CONCURRENT_CATALOG_LOOKUPS: usize = 4;
const ENDPOINT_ADDR_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_STORE_GC_INTERVAL: Duration = Duration::from_mins(5);

/// Keys re-announced per cycle, so a node holding many artifacts spreads the
/// work across intervals rather than spiking the scheduler.
const REANNOUNCE_BATCH: usize = 64;
const PUBLISH_TAG_PREFIX: &str = "agentenv:p2p:v1:";

enum BlobFetchSource {
    Local {
        blob_hash: iroh_blobs::Hash,
    },
    Remote {
        blob_hash: iroh_blobs::Hash,
        providers: Vec<iroh::EndpointId>,
    },
}

/// P2P transport based on iroh + iroh-blobs.
///
/// The transport exposes two protocols over one iroh endpoint:
///
/// - iroh-blobs serves and downloads artifact bytes by blob hash.
/// - the AgentENV catalog protocol resolves [`P2pArtifactKey`] values into
///   transport-neutral descriptors whose backend locator contains the iroh
///   blob hash needed by this backend.
///
/// Consumers should keep depending on [`P2pTransport`]; this type is only
/// the concrete backend selected by P2P configuration.
pub struct IrohBlobsP2pTransport {
    node_id: String,
    peer_discovery: Arc<dyn P2pPeerDiscovery>,
    store: FsStore,
    router: Router,
    downloader: Downloader,
    lookup: MemoryLookup,
    /// Artifacts this node has published and can advertise to peers.
    published_catalog: PublishedArtifactCatalog,
    local_endpoint: P2pEndpoint,
    lookup_timeout: Duration,
    /// Per-peer bound on one catalog lookup, distinct from `lookup_timeout`,
    /// which bounds a whole multi-peer lookup.
    catalog_lookup_timeout: Duration,
    fetch_timeout: Duration,
    /// Pooled catalog connections, or `None` when pooling is turned off.
    catalog_pool: Option<ConnectionPool>,
    /// Pooled blob connections, on iroh-blobs' own ALPN.
    ///
    /// Separate from `catalog_pool` because a pool is bound to one ALPN, and
    /// separate from the `Downloader`'s connections because a streamed range
    /// drives the get state machine itself rather than going through the
    /// store. Sized and aged from the same config: a demand read that
    /// re-handshakes per range has not avoided any of the latency the
    /// streaming path exists to avoid.
    blobs_pool: Option<ConnectionPool>,
    /// Catalog connections this node has actually dialled.
    ///
    /// Incremented from the pool's connect callback, so it counts handshakes
    /// rather than lookups. That difference is the whole point of pooling, and
    /// it is not observable any other way: the pool owns connection lifetime
    /// and exposes no accessor for it.
    catalog_connections: Arc<AtomicU64>,
    /// CIDRs a peer endpoint address may name; empty accepts any address.
    cluster_cidrs: Vec<ipnetwork::IpNetwork>,
    pending_gc: Arc<AtomicBool>,
}

/// Slack allowed on top of a requested byte range before a transfer is refused.
///
/// A range download carries more than the bytes asked for: the range is
/// widened to chunk boundaries, and bao verification hashes travel with the
/// payload. Both are proportional to the request, so the budget is a multiple
/// of it plus this, which covers a small range whose overhead dominates.
const RANGE_VERIFICATION_ALLOWANCE: u64 = 1 << 20;

impl IrohBlobsP2pTransport {
    pub async fn new(
        config: ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<Self> {
        Self::new_with_gc_interval(config, node_id, peer_discovery, DEFAULT_STORE_GC_INTERVAL).await
    }

    async fn new_with_gc_interval(
        config: ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
        gc_interval: Duration,
    ) -> Result<Self> {
        let cluster_cidrs = parse_cluster_cidrs(&config.cluster_cidrs)?;
        let store_dir = config.store_dir.join(IROH_BACKEND_ID);
        tokio::fs::create_dir_all(&store_dir)
            .await
            .with_context(|| format!("create P2P blob store dir {}", store_dir.display()))?;

        // The collector is gated so it only sweeps when something has actually
        // been unpublished, rather than walking the whole store every interval.
        //
        // That gate was previously armed only here, at startup. Since nothing
        // in production ever called unpublish, it was never armed again, so the
        // collector never swept during a process lifetime and the store grew
        // without bound. Arming it on a timer as well means a sweep happens on
        // a predictable cadence whether or not an unpublish raced it.
        let pending_gc = Arc::new(AtomicBool::new(true));
        let mut store_options = FsStoreOptions::new(&store_dir);
        store_options.gc = Some(gated_gc_config(gc_interval, pending_gc.clone()));
        let store = FsStore::load_with_opts(store_dir.join("blobs.db"), store_options)
            .await
            .with_context(|| format!("open P2P blob store {}", store_dir.display()))?;

        let lookup = MemoryLookup::new();
        let mut builder = Endpoint::builder(presets::Minimal).address_lookup(lookup.clone());

        if let Some(listen_addr) = config.listen_addr.as_deref() {
            let addr: SocketAddr = listen_addr
                .parse()
                .with_context(|| format!("parse p2p.listen_addr {listen_addr}"))?;
            builder = builder
                .clear_ip_transports()
                .bind_addr(addr)
                .map_err(|err| {
                    Error::internal_message("bind configured P2P listen address", err)
                })?;
        }

        let endpoint = builder.bind().await.context("bind P2P endpoint")?;
        let local_addr = wait_for_dialable_addr(&endpoint)
            .await
            .context("wait for P2P endpoint address")?;
        let local_endpoint = P2pEndpoint::from_iroh_addr(&local_addr)?;
        let catalog_path = store_dir.join(CATALOG_DB_DIR);
        let published_catalog =
            PublishedArtifactCatalog::load(&catalog_path, &node_id, &local_endpoint).await?;

        // Serve both data and metadata from the same endpoint. The scheduler
        // only passes endpoint addresses around; it does not proxy catalog
        // requests or artifact bytes.
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
            .accept(
                CATALOG_ALPN,
                CatalogProtocol::new(published_catalog.clone()),
            )
            .spawn();
        let downloader = Downloader::new(&store, router.endpoint());
        let catalog_connections = Arc::new(AtomicU64::new(0));
        let catalog_pool = build_catalog_pool(
            router.endpoint().clone(),
            &config,
            Arc::clone(&catalog_connections),
        );
        let blobs_pool = build_blobs_pool(router.endpoint().clone(), &config);

        Self::spawn_gc_arming_task(Arc::clone(&pending_gc), gc_interval, store_dir.clone());
        Self::spawn_reannounce_task(
            published_catalog.keys_handle(),
            Arc::clone(&peer_discovery),
            config.reannounce_interval,
        );

        info!(
            node_id,
            store_dir = %store_dir.display(),
            endpoint = %router.endpoint().id(),
            "iroh artifact transport started"
        );

        Ok(Self {
            node_id,
            peer_discovery,
            store,
            router,
            downloader,
            lookup,
            published_catalog,
            local_endpoint,
            lookup_timeout: config.lookup_timeout,
            catalog_lookup_timeout: config.catalog_lookup_timeout,
            fetch_timeout: config.fetch_timeout,
            catalog_pool,
            blobs_pool,
            catalog_connections,
            cluster_cidrs,
            pending_gc,
        })
    }

    #[instrument(level = "debug", skip(self), fields(peer = %peer.node_id))]
    async fn lookup_peer(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<(Option<P2pArtifactDescriptor>, LookupConnection)> {
        // Scheduler endpoints are opaque until this backend parses them. Add
        // the peer address to iroh's in-memory lookup before dialing.
        let addr = peer.endpoint.to_iroh_addr()?;
        self.validate_endpoint_addr(&addr)?;
        self.lookup.add_endpoint_info(addr.clone());

        let (connection, reuse) = match &self.catalog_pool {
            Some(pool) => {
                let dialled_before = self.catalog_connections.load(Ordering::Acquire);
                let connection = pool
                    .get_or_connect(addr.id)
                    .await
                    .map_err(|err| pool_connect_error(&peer.node_id, err))?;
                // Approximate under concurrency, because another lookup may
                // have dialled in between. It is a label, not a decision, and
                // the reading that matters — one dial for many lookups — is
                // exact in the counter it is derived from.
                let reuse = if self.catalog_connections.load(Ordering::Acquire) == dialled_before {
                    LookupConnection::Reused
                } else {
                    LookupConnection::New
                };
                (CatalogConnection::Pooled(connection), reuse)
            }
            None => {
                let connection = self
                    .router
                    .endpoint()
                    .connect(addr, CATALOG_ALPN)
                    .await
                    .with_context(|| format!("connect to P2P catalog peer {}", peer.node_id))?;
                (CatalogConnection::Owned(connection), LookupConnection::New)
            }
        };

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .context("open P2P catalog stream")?;
        let request = CatalogRequest { key: key.clone() };
        let request_bytes =
            serde_json::to_vec(&request).context("serialize P2P catalog lookup request")?;
        send.write_all(&request_bytes)
            .await
            .context("send P2P catalog lookup request")?;
        send.finish().context("finish P2P catalog request stream")?;
        let response_bytes = recv
            .read_to_end(MAX_CATALOG_RESPONSE_BYTES)
            .await
            .context("read P2P catalog lookup response")?;
        let response: CatalogResponse =
            serde_json::from_slice(&response_bytes).context("parse P2P catalog response")?;
        // A pooled connection is closed by the pool when it goes idle, not
        // here: closing it after one lookup is exactly the handshake-per-lookup
        // the pool exists to remove.
        connection.close_if_owned();
        Ok((
            response
                .descriptor
                .and_then(|descriptor| resolve_remote_local_providers(descriptor, peer)),
            reuse,
        ))
    }

    async fn lookup_peer_with_timeout(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        let started = std::time::Instant::now();
        let outcome =
            tokio::time::timeout(self.catalog_lookup_timeout, self.lookup_peer(peer, key)).await;
        let (result, connection) = match &outcome {
            Ok(Ok((Some(_), reuse))) => (LookupResult::Hit, *reuse),
            Ok(Ok((None, reuse))) => (LookupResult::Miss, *reuse),
            Ok(Err(_)) => (LookupResult::Error, LookupConnection::New),
            Err(_) => (LookupResult::Timeout, LookupConnection::New),
        };
        p2p_metrics::record_catalog_lookup(result, connection, started.elapsed());
        outcome
            .map_err(|_| Error::Timeout {
                operation: "lookup P2P artifact from peer",
            })?
            .map(|(descriptor, _)| descriptor)
    }

    /// Bounds a whole multi-peer lookup.
    ///
    /// `catalog_lookup_timeout` bounds one peer; this is the caller-facing
    /// budget for the lookup as a whole, so a long tail of candidates cannot
    /// keep a caller waiting past what it asked for.
    async fn lookup_peers_bounded(
        &self,
        peers: Vec<P2pPeer>,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        match tokio::time::timeout(self.lookup_timeout, self.lookup_peers(peers, key)).await {
            Ok(descriptor) => descriptor,
            Err(_) => {
                debug!(
                    timeout_ms = self.lookup_timeout.as_millis(),
                    "P2P multi-peer lookup exceeded its budget"
                );
                None
            }
        }
    }

    async fn lookup_peers(
        &self,
        peers: Vec<P2pPeer>,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        first_some_buffered_in_order(peers, MAX_CONCURRENT_CATALOG_LOOKUPS, |peer| async move {
            if peer.node_id == self.node_id || peer.endpoint == self.local_endpoint {
                let descriptor = self.get_local(key).await;
                if descriptor.is_some() {
                    trace!("P2P lookup found local descriptor matching peer discovery");
                }
                return descriptor;
            }
            match self.lookup_peer_with_timeout(&peer, key).await {
                Ok(None) => None,
                Ok(Some(descriptor)) => {
                    trace!(peer = %peer.node_id, "P2P lookup found remote descriptor");
                    Some(descriptor)
                }
                Err(err) => {
                    debug!(
                        peer = %peer.node_id,
                        error = %err,
                        "P2P artifact catalog lookup failed; trying remaining peers"
                    );
                    None
                }
            }
        })
        .await
    }

    async fn get_local(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.published_catalog.descriptor_for(key).await
    }

    /// Refuses a blob this node already holds that is larger than the caller's
    /// bound.
    ///
    /// A local hit is a blob this node imported or previously fetched, so it
    /// is not peer-controlled in the way a download is. The bound still
    /// applies: the caller asked for something it can hold, and a local
    /// artifact that has since grown past that is no more holdable for having
    /// come from here.
    async fn ensure_local_blob_within(
        &self,
        blob_hash: iroh_blobs::Hash,
        max_bytes: u64,
    ) -> Result<()> {
        let size = self
            .store
            .observe(blob_hash)
            .await
            .map_err(|err| Error::internal_message("size local P2P blob", err))?
            .size();
        if size > max_bytes {
            return Err(Error::ArtifactTooLarge { limit: max_bytes });
        }
        Ok(())
    }

    /// Export a local blob to a caller-requested destination path and return its size in bytes.
    ///
    /// The bound is checked before the first byte is written, so a refusal
    /// leaves nothing at the destination. Both fetch paths export through
    /// here, including the one that has just downloaded: a blob already
    /// complete in the local store transfers nothing, so the download's
    /// arrival-time bound never sees it.
    async fn export_local_blob(
        &self,
        blob_hash: iroh_blobs::Hash,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<u64> {
        self.ensure_local_blob_within(blob_hash, max_bytes).await?;
        self.store
            .export(blob_hash, destination)
            .await
            .map_err(|err| Error::internal_message("export local P2P blob", err))
    }

    async fn export_local_blob_range(
        &self,
        blob_hash: iroh_blobs::Hash,
        range: Range<u64>,
    ) -> Result<P2pByteStream> {
        let expected_len =
            range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| Error::InvalidDescriptor {
                    reason: "range end before start".to_string(),
                })?;
        let range_start = range.start;
        let range_end = range.end;
        let inner = Box::pin(self.store.export_ranges(blob_hash, range).stream());
        // Every terminal arm below zeroes `remaining`, so it is the single
        // stop signal: a satisfied range and a range that ended in an error
        // both refuse to poll the exporter again.
        let stream = stream::unfold(
            (inner, expected_len),
            move |(mut inner, mut remaining)| async move {
                if remaining == 0 {
                    return None;
                }
                while let Some(item) = inner.next().await {
                    match item {
                        ExportRangesItem::Size(_) => {}
                        ExportRangesItem::Data(leaf) => {
                            let Ok(data_len) = u64::try_from(leaf.data.len()) else {
                                return Some((
                                    Err(Error::InvalidDescriptor {
                                        reason: "exported range chunk length does not fit u64"
                                            .to_string(),
                                    }),
                                    (inner, 0),
                                ));
                            };
                            let Some(leaf_end) = leaf.offset.checked_add(data_len) else {
                                return Some((
                                    Err(Error::InvalidDescriptor {
                                        reason: "exported range chunk end overflow".to_string(),
                                    }),
                                    (inner, 0),
                                ));
                            };
                            let overlap_start = leaf.offset.max(range_start);
                            let overlap_end = leaf_end.min(range_end);
                            if overlap_start >= overlap_end {
                                continue;
                            }
                            let start = (overlap_start - leaf.offset) as usize;
                            let end = (overlap_end - leaf.offset) as usize;
                            let bytes = leaf.data.slice(start..end);
                            remaining = remaining.saturating_sub(bytes.len() as u64);
                            return Some((Ok(bytes), (inner, remaining)));
                        }
                        ExportRangesItem::Error(err) => {
                            return Some((
                                Err(Error::internal_message("export local P2P blob range", err)),
                                (inner, 0),
                            ));
                        }
                    }
                }
                if remaining == 0 {
                    None
                } else {
                    Some((
                        Err(Error::InvalidDescriptor {
                            reason: format!("exported range ended {remaining} bytes short"),
                        }),
                        (inner, 0),
                    ))
                }
            },
        );
        Ok(Box::pin(stream))
    }

    /// Reads a blob this node already holds, refusing one larger than the
    /// caller's bound.
    async fn read_local_blob_bytes(
        &self,
        blob_hash: iroh_blobs::Hash,
        max_bytes: u64,
    ) -> Result<Bytes> {
        self.ensure_local_blob_within(blob_hash, max_bytes).await?;
        self.store
            .get_bytes(blob_hash)
            .await
            .map_err(|err| Error::internal_message("read local P2P blob bytes", err))
    }

    async fn advertise_local_blob(
        &self,
        key: &P2pArtifactKey,
        blob_hash: iroh_blobs::Hash,
        metadata: &serde_json::Value,
        owner: P2pPublishOwner,
    ) -> Result<()> {
        self.store
            .tags()
            .set(publish_tag_name(key), HashAndFormat::raw(blob_hash))
            .await
            .map_err(|err| Error::internal_message("retain local P2P artifact blob", err))?;
        self.published_catalog.add_owner(key, owner).await?;

        let local_descriptor = P2pArtifactDescriptor {
            key: key.clone(),
            // TODO: include other providers if this artifact was previously fetched from a peer.
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some(blob_hash.to_string()),
            metadata: metadata.clone(),
        };
        self.published_catalog.upsert(local_descriptor).await?;
        Ok(())
    }

    async fn try_advertise_fetched_blob(
        &self,
        descriptor: &P2pArtifactDescriptor,
        blob_hash: iroh_blobs::Hash,
    ) {
        // A blob this node fetched for its own use is retained on the node's
        // own account, not on behalf of whoever published it upstream.
        if let Err(err) = self
            .advertise_local_blob(
                &descriptor.key,
                blob_hash,
                &descriptor.metadata,
                P2pPublishOwner::Unscoped,
            )
            .await
        {
            warn!(error = %err, "failed to advertise fetched P2P artifact");
            return;
        }
        if let Err(err) = self.peer_discovery.record_key(&descriptor.key).await {
            warn!(
                error = %err,
                "failed to record fetched P2P artifact in scheduler"
            );
        }
    }

    async fn download_blob(
        &self,
        request: GetRequest,
        providers: Vec<iroh::EndpointId>,
        max_bytes: u64,
    ) -> Result<()> {
        let operation = "download P2P artifact blob";
        tokio::time::timeout(
            self.fetch_timeout,
            self.download_blob_bounded(operation, request, providers, max_bytes),
        )
        .await
        .map_err(|_| Error::Timeout { operation })?
    }

    /// Downloads a blob, giving up once it has taken more than `max_bytes`.
    ///
    /// The bound is applied to the bytes as they arrive rather than to a size
    /// the peer declared, because at this point nothing the peer has said has
    /// been authenticated — the sealing check runs on the result of this call,
    /// not before it. A wall-clock timeout is not a substitute: it bounds how
    /// long a peer may spend filling this node's memory or disk, not how much
    /// it may fill.
    ///
    /// Abandoning the stream cancels the transfer. Whatever partial data
    /// reached the store is untagged — the tag is only written once the fetch
    /// has succeeded — so it is collectable rather than retained.
    async fn download_blob_bounded(
        &self,
        operation: &'static str,
        request: GetRequest,
        providers: Vec<iroh::EndpointId>,
        max_bytes: u64,
    ) -> Result<()> {
        let mut progress = self
            .downloader
            .download(request, providers)
            .stream()
            .await
            .map_err(|err| Error::internal_message(operation, err))?;

        while let Some(item) = progress.next().await {
            match item {
                DownloadProgressItem::Error(err) => {
                    return Err(Error::internal_message(operation, err));
                }
                DownloadProgressItem::DownloadError => {
                    return Err(Error::internal_message(operation, "download failed"));
                }
                DownloadProgressItem::Progress(received) if received > max_bytes => {
                    warn!(
                        received,
                        max_bytes, "abandoning a P2P artifact that exceeded its size limit"
                    );
                    return Err(Error::ArtifactTooLarge { limit: max_bytes });
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn resolve_fetch_source(&self, descriptor: &P2pArtifactDescriptor) -> Result<BlobFetchSource> {
        let blob_hash = blob_hash_from_descriptor(descriptor)?;

        if descriptor
            .providers
            .iter()
            .any(P2pArtifactProvider::is_local)
        {
            return Ok(BlobFetchSource::Local { blob_hash });
        }

        let providers: Vec<_> = descriptor
            .providers
            .iter()
            .filter_map(|provider| {
                let P2pArtifactProvider::Peer(peer) = provider else {
                    return None;
                };
                let addr = peer.endpoint.to_iroh_addr().ok()?;
                // A descriptor names its own providers, and a descriptor is
                // whatever a peer chose to send, so this is the second place a
                // node can be told where to dial.
                self.validate_endpoint_addr(&addr).ok()?;
                let provider_id = addr.id;
                self.lookup.add_endpoint_info(addr);
                Some(provider_id)
            })
            .collect();

        if providers.is_empty() {
            return Err(Error::InvalidDescriptor {
                reason: format!(
                    "no valid P2P providers found in descriptor for key {}",
                    descriptor.key
                ),
            });
        }

        Ok(BlobFetchSource::Remote {
            blob_hash,
            providers,
        })
    }

    /// Refuses an endpoint address outside the cluster's own network.
    ///
    /// Endpoint addresses travel as opaque blobs through the scheduler and
    /// inside descriptors, and every node parses one and dials it. Artifact
    /// bytes are safe either way — they are named by hash and verified on
    /// arrival — but connection targeting is not, so a compromised node can
    /// point its peers wherever it likes. Naming the cluster's own networks
    /// bounds that to addresses the node would already have talked to.
    fn validate_endpoint_addr(&self, addr: &EndpointAddr) -> Result<()> {
        if self.cluster_cidrs.is_empty() {
            return Ok(());
        }
        let mut saw_addr = false;
        for socket_addr in addr.ip_addrs() {
            saw_addr = true;
            if !self
                .cluster_cidrs
                .iter()
                .any(|cidr| cidr.contains(socket_addr.ip()))
            {
                return Err(Error::InvalidDescriptor {
                    reason: format!(
                        "P2P endpoint address {} is outside the configured cluster networks",
                        socket_addr.ip()
                    ),
                });
            }
        }
        if !saw_addr {
            // With no relay and no discovery configured, an address that names
            // no IP cannot be dialled at all; refusing it here keeps the
            // failure at the check rather than in a timeout.
            return Err(Error::InvalidDescriptor {
                reason: "P2P endpoint address names no IP address".to_string(),
            });
        }
        Ok(())
    }
}

fn parse_cluster_cidrs(raw: &[String]) -> Result<Vec<ipnetwork::IpNetwork>> {
    raw.iter()
        .map(|cidr| {
            cidr.parse::<ipnetwork::IpNetwork>()
                .with_context(|| format!("parse p2p.cluster_cidrs entry {cidr:?}"))
                .map_err(Error::from)
        })
        .collect()
}

/// Builds the catalog connection pool, or `None` when pooling is off.
///
/// The pool's own defaults are tuned for short bursts: a 5 s idle timeout is
/// shorter than the peer refresh interval, so a connection to a peer this node
/// polls every 5 s would be torn down and re-handshaken between reads of the
/// same layer.
fn build_catalog_pool(
    endpoint: Endpoint,
    config: &ResolvedP2pConfig,
    connections: Arc<AtomicU64>,
) -> Option<ConnectionPool> {
    if config.catalog_max_connections == 0 {
        info!("P2P catalog connection pooling is disabled; dialing per lookup");
        return None;
    }
    let options = ConnectionPoolOptions {
        idle_timeout: config.catalog_connection_idle,
        max_connections: config.catalog_max_connections,
        ..ConnectionPoolOptions::default()
    }
    .with_on_connected(move |_endpoint, _connection| {
        let connections = Arc::clone(&connections);
        async move {
            connections.fetch_add(1, Ordering::Release);
            p2p_metrics::record_catalog_connection_established();
            Ok(())
        }
    });
    Some(ConnectionPool::new(endpoint, CATALOG_ALPN, options))
}

/// Builds the blob-transfer pool, or `None` when pooling is off.
///
/// Shares the catalog's sizing because both are per-peer connection reuse
/// against the same fleet, and a separate pair of knobs would be two ways to
/// express one intent.
fn build_blobs_pool(endpoint: Endpoint, config: &ResolvedP2pConfig) -> Option<ConnectionPool> {
    if config.catalog_max_connections == 0 {
        return None;
    }
    let options = ConnectionPoolOptions {
        idle_timeout: config.catalog_connection_idle,
        max_connections: config.catalog_max_connections,
        ..ConnectionPoolOptions::default()
    };
    Some(ConnectionPool::new(endpoint, iroh_blobs::ALPN, options))
}

/// Streams one BLAKE3-verified byte range straight off a peer connection.
///
/// The get state machine yields the blob's leaves in order, each stamped with
/// its offset in the blob. Verification happens as they arrive, so a peer
/// cannot substitute bytes; what this adds is that nothing is written down on
/// the way past. Leaves outside the requested range are produced by the
/// protocol -- a range is widened to chunk boundaries -- and are dropped here
/// rather than returned, so the caller sees exactly the bytes it asked for.
fn stream_verified_blob_range(
    connection: iroh_blobs::util::connection_pool::ConnectionRef,
    blob_hash: iroh_blobs::Hash,
    range: std::ops::Range<u64>,
    budget: u64,
) -> impl Stream<Item = Result<Bytes>> + Send + 'static {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(8);
    tokio::spawn(async move {
        // The pool permit lives on the `ConnectionRef`, so it is moved in here
        // rather than cloned out of: releasing it at the end of the call that
        // opened it would have the pool counting this transfer's connection as
        // free for its whole duration.
        let result =
            drive_verified_blob_range((*connection).clone(), blob_hash, range, budget, &tx).await;
        drop(connection);
        if let Err(err) = result {
            let _ = tx.send(Err(err)).await;
        }
    });
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

async fn drive_verified_blob_range(
    connection: iroh::endpoint::Connection,
    blob_hash: iroh_blobs::Hash,
    range: std::ops::Range<u64>,
    budget: u64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes>>,
) -> Result<()> {
    let request = GetRequest::blob_ranges(blob_hash, ChunkRanges::bytes(range.clone()));
    let connected = get::fsm::start(connection, request, Default::default())
        .next()
        .await
        .map_err(|err| Error::Internal(anyhow::anyhow!("open streamed range request: {err}")))?;
    let get::fsm::ConnectedNext::StartRoot(start) = connected
        .next()
        .await
        .map_err(|err| Error::Internal(anyhow::anyhow!("start streamed range: {err}")))?
    else {
        return Err(Error::Internal(anyhow::anyhow!(
            "peer answered a blob range request with no root"
        )));
    };
    let (mut content, _size) = start
        .next()
        .next()
        .await
        .map_err(|err| Error::Internal(anyhow::anyhow!("read streamed range header: {err}")))?;

    let mut delivered = 0_u64;
    loop {
        match content.next().await {
            get::fsm::BlobContentNext::More((next, item)) => {
                let item = item.map_err(|err| {
                    Error::Internal(anyhow::anyhow!("verify streamed range: {err}"))
                })?;
                if let BaoContentItem::Leaf(leaf) = item {
                    if let Some(slice) = slice_to_range(&leaf, &range) {
                        delivered = delivered.saturating_add(slice.len() as u64);
                        // The bound is on what a peer may send, not on what was
                        // asked for: an unbounded reader is a peer-controlled
                        // allocation on a page-fault path.
                        if delivered > budget {
                            return Err(Error::Internal(anyhow::anyhow!(
                                "peer sent {delivered} bytes for a range budgeted at {budget}"
                            )));
                        }
                        if tx.send(Ok(slice)).await.is_err() {
                            // The reader is gone; so is the reason to keep pulling.
                            return Ok(());
                        }
                    }
                }
                content = next;
            }
            get::fsm::BlobContentNext::Done(_) => return Ok(()),
        }
    }
}

/// Clips one leaf to the requested byte range, or drops it entirely.
fn slice_to_range(leaf: &bao_tree::io::Leaf, range: &std::ops::Range<u64>) -> Option<Bytes> {
    let leaf_end = leaf.offset.saturating_add(leaf.data.len() as u64);
    let start = leaf.offset.max(range.start);
    let end = leaf_end.min(range.end);
    if start >= end {
        return None;
    }
    let from = (start - leaf.offset) as usize;
    let to = (end - leaf.offset) as usize;
    Some(leaf.data.slice(from..to))
}

fn pool_connect_error(node_id: &str, err: PoolConnectError) -> Error {
    Error::Internal(anyhow::anyhow!(
        "connect to P2P catalog peer {node_id}: {err}"
    ))
}

/// A catalog connection whose lifetime belongs either to the pool or to us.
enum CatalogConnection {
    Pooled(iroh_blobs::util::connection_pool::ConnectionRef),
    Owned(iroh::endpoint::Connection),
}

impl CatalogConnection {
    async fn open_bi(
        &self,
    ) -> std::result::Result<
        (iroh::endpoint::SendStream, iroh::endpoint::RecvStream),
        iroh::endpoint::ConnectionError,
    > {
        match self {
            Self::Pooled(connection) => connection.open_bi().await,
            Self::Owned(connection) => connection.open_bi().await,
        }
    }

    fn close_if_owned(&self) {
        if let Self::Owned(connection) = self {
            connection.close(0_u32.into(), b"done");
        }
    }
}

#[async_trait]
impl P2pTransport for IrohBlobsP2pTransport {
    #[instrument(skip(self, hints), fields(key = %key, hints_len = hints.len()))]
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        if let Some(descriptor) = self.get_local(key).await {
            trace!("P2P lookup found local descriptor");
            return Ok(Some(descriptor));
        }

        match self.peer_discovery.peers_for_key(key).await {
            Ok(peers) => {
                if let Some(descriptor) = self.lookup_peers_bounded(peers, key).await {
                    trace!("P2P lookup found descriptor through scheduler artifact index");
                    // Currently use short-circuit provider discovery for simplicity.
                    return Ok(Some(descriptor));
                }
            }
            Err(err) => {
                debug!(
                    error = %err,
                    "P2P scheduler artifact lookup failed; falling back to peer discovery"
                );
            }
        }

        let peers = self.peer_discovery.peers_with_hints(hints).await?;
        if let Some(descriptor) = self.lookup_peers_bounded(peers, key).await {
            return Ok(Some(descriptor));
        }
        trace!("P2P lookup completed without descriptor");
        Ok(None)
    }

    #[instrument(
        skip(self, descriptor),
        fields(key = %descriptor.key, destination = %destination.display())
    )]
    async fn fetch(
        &self,
        descriptor: &P2pArtifactDescriptor,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<u64> {
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("create P2P fetch destination dir {}", parent.display())
            })?;
        }

        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                // A local lookup hit still goes through export so callers get the
                // artifact at the requested destination rather than a store path.
                let size = self
                    .export_local_blob(blob_hash, destination, max_bytes)
                    .await?;
                debug!(size, "fetched artifact from local P2P store");
                return Ok(size);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Download into the local FsStore first, then export to the caller's destination.
        self.download_blob(GetRequest::blob(blob_hash), providers, max_bytes)
            .await?;
        self.try_advertise_fetched_blob(descriptor, blob_hash).await;
        let size = self
            .export_local_blob(blob_hash, destination, max_bytes)
            .await?;
        debug!(providers_count, size, "fetched artifact from P2P provider");
        Ok(size)
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key))]
    async fn fetch_bytes(
        &self,
        descriptor: &P2pArtifactDescriptor,
        max_bytes: u64,
    ) -> Result<Bytes> {
        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                let bytes = self.read_local_blob_bytes(blob_hash, max_bytes).await?;
                debug!(
                    size = bytes.len(),
                    "fetched artifact bytes from local P2P store"
                );
                return Ok(bytes);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Full in-memory fetches download the complete blob into the local store
        // first, matching file fetch semantics and allowing this node to serve
        // the artifact to later peers.
        self.download_blob(GetRequest::blob(blob_hash), providers, max_bytes)
            .await?;
        self.try_advertise_fetched_blob(descriptor, blob_hash).await;
        let bytes = self.read_local_blob_bytes(blob_hash, max_bytes).await?;
        debug!(
            providers_count,
            size = bytes.len(),
            "fetched artifact bytes from P2P provider"
        );
        Ok(bytes)
    }

    #[instrument(
        skip(self, descriptor),
        fields(key = %descriptor.key, offset, len)
    )]
    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        if len == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        let end = offset
            .checked_add(u64::try_from(len).map_err(|err| Error::InvalidDescriptor {
                reason: format!("range length does not fit u64: {err}"),
            })?)
            .ok_or_else(|| Error::InvalidDescriptor {
                reason: "range end overflow".to_string(),
            })?;
        let range = offset..end;

        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                let stream = self.export_local_blob_range(blob_hash, range).await?;
                debug!("fetched artifact range from local P2P store");
                return Ok(stream);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Range reads are foreground acceleration only: the downloaded bytes
        // land in the iroh store but are not retention-tagged here, so GC may
        // reclaim them unless the full layer is later published by background
        // download.
        let ranges = ChunkRanges::bytes(range.clone());
        // A range request asks for a bounded slice, but what arrives is still
        // the peer's choice, so the bound is enforced rather than assumed. The
        // allowance covers chunk alignment either side of the range and the
        // bao verification hashes interleaved with the payload.
        let range_budget = (end - offset)
            .saturating_add(RANGE_VERIFICATION_ALLOWANCE)
            .saturating_mul(2);
        self.download_blob(
            GetRequest::blob_ranges(blob_hash, ranges),
            providers,
            range_budget,
        )
        .await?;
        let stream = self.export_local_blob_range(blob_hash, range).await?;
        debug!(providers_count, "fetched artifact range from P2P provider");
        Ok(stream)
    }

    async fn fetch_byte_range_streaming(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        if len == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        let end = offset
            .checked_add(u64::try_from(len).map_err(|err| Error::InvalidDescriptor {
                reason: format!("range length does not fit u64: {err}"),
            })?)
            .ok_or_else(|| Error::InvalidDescriptor {
                reason: "range end overflow".to_string(),
            })?;
        let range = offset..end;

        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            // A local hit is already store-resident; reading it back is the
            // cheapest path there is, not a detour.
            BlobFetchSource::Local { blob_hash } => {
                return self.export_local_blob_range(blob_hash, range).await;
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };

        // One provider, not a split download: splitting is what the store-backed
        // path buys with its store, since partial results from several peers
        // have to be reassembled somewhere. Falling back rather than failing
        // keeps a streamed read no less available than a stored one.
        let Some(provider) = providers.first().copied() else {
            return self.fetch_byte_range(descriptor, offset, len).await;
        };
        let Some(pool) = self.blobs_pool.as_ref() else {
            return self.fetch_byte_range(descriptor, offset, len).await;
        };
        let connection = match pool.get_or_connect(provider).await {
            Ok(connection) => connection,
            Err(err) => {
                debug!(error = %err, "streamed range could not connect; falling back to the store path");
                return self.fetch_byte_range(descriptor, offset, len).await;
            }
        };

        let budget = (end - offset)
            .saturating_add(RANGE_VERIFICATION_ALLOWANCE)
            .saturating_mul(2);
        let stream = stream_verified_blob_range(connection, blob_hash, range, budget);
        p2p_metrics::record_range_stream_started();
        Ok(Box::pin(stream))
    }

    #[instrument(
        skip(self, request),
        fields(key = %request.key, source = %request.source, publish_mode = ?request.publish_mode)
    )]
    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        let result = self.publish_inner(request).await;
        p2p_metrics::record_publish(
            &request.key,
            request.owner,
            if result.is_ok() {
                PublishStatus::Published
            } else {
                PublishStatus::Failed
            },
        );
        result
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        self.unpublish_owned(key, P2pPublishOwner::Unscoped).await
    }

    #[instrument(skip(self), fields(key = %key, owner = owner.as_str()))]
    async fn unpublish_owned(&self, key: &P2pArtifactKey, owner: P2pPublishOwner) -> Result<bool> {
        let result = self.unpublish_inner(key, owner).await;
        p2p_metrics::record_unpublish(
            key,
            match &result {
                Ok(OwnerRelease::Withdrawn) => UnpublishStatus::Withdrawn,
                Ok(OwnerRelease::Retained) => UnpublishStatus::Retained,
                Ok(OwnerRelease::Absent) => UnpublishStatus::Absent,
                Err(_) => UnpublishStatus::Failed,
            },
        );
        Ok(matches!(result?, OwnerRelease::Withdrawn))
    }

    async fn request_gc(&self) {
        self.pending_gc.store(true, Ordering::Release);
    }

    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        Some(self.local_endpoint.clone())
    }

    async fn shutdown(&self) -> Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|err| Error::internal_message("shutdown embedded P2P endpoint", err))
    }
}

impl IrohBlobsP2pTransport {
    async fn publish_inner(&self, request: &P2pPublishRequest) -> Result<()> {
        let imported = match &request.source {
            P2pPublishSource::Path(source) => {
                let import_mode = match request.publish_mode {
                    P2pPublishMode::Copy => ImportMode::Copy,
                    P2pPublishMode::Reference => ImportMode::TryReference,
                };
                self.store
                    .add_path_with_opts(AddPathOptions {
                        path: source.clone(),
                        format: BlobFormat::Raw,
                        mode: import_mode,
                    })
                    .temp_tag()
                    .await
                    .map_err(|err| {
                        Error::internal_message("import file into iroh-blobs store", err)
                    })?
            }
            P2pPublishSource::Bytes(bytes) => self
                .store
                .add_bytes(bytes.clone())
                .temp_tag()
                .await
                .map_err(|err| {
                    Error::internal_message("import bytes into iroh-blobs store", err)
                })?,
        };
        self.advertise_local_blob(
            &request.key,
            imported.hash(),
            &request.metadata,
            request.owner,
        )
        .await?;
        if let Err(err) = self.peer_discovery.record_key(&request.key).await {
            warn!(error = %err, "failed to record P2P artifact in scheduler");
        }
        debug!("published artifact into P2P transport");
        Ok(())
    }

    /// Releases one owner, withdrawing the artifact only when it was the last.
    ///
    /// The retention tag is one deterministic name per key, so deleting it on
    /// any owner's release would hand the collector a blob another publisher
    /// is still advertising. The catalog entry has the same problem: peers
    /// would keep resolving a descriptor whose bytes had been swept.
    async fn unpublish_inner(
        &self,
        key: &P2pArtifactKey,
        owner: P2pPublishOwner,
    ) -> Result<OwnerRelease> {
        match self.published_catalog.release_owner(key, owner).await? {
            OwnerRelease::Absent => {
                debug!("P2P unpublish skipped missing local artifact");
                return Ok(OwnerRelease::Absent);
            }
            OwnerRelease::Retained => {
                debug!("P2P artifact still held by another publisher; keeping it advertised");
                return Ok(OwnerRelease::Retained);
            }
            OwnerRelease::Withdrawn => {}
        }

        if self.published_catalog.remove(key).await?.is_none() {
            debug!("P2P unpublish skipped missing local artifact");
            return Ok(OwnerRelease::Absent);
        };

        self.store
            .tags()
            .delete(publish_tag_name(key))
            .await
            .map_err(|err| Error::internal_message("delete P2P artifact retention tag", err))?;
        self.pending_gc.store(true, Ordering::Release);
        if let Err(err) = self.peer_discovery.forget_key(key).await {
            warn!(error = %err, "failed to forget P2P artifact in scheduler");
        }
        debug!("unpublished artifact from P2P transport");
        Ok(OwnerRelease::Withdrawn)
    }
}

impl Drop for IrohBlobsP2pTransport {
    fn drop(&mut self) {
        if self.router.is_shutdown() {
            return;
        }
        let router = self.router.clone();
        // Drop can run with no runtime current — on a plain std::thread that
        // held the last Arc, or after the runtime is gone at process exit.
        // A bare tokio::spawn panics there, and a panic in drop during an
        // in-flight unwind aborts the process, so a skipped courtesy shutdown
        // is the better trade.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("tokio runtime unavailable during drop, skipping iroh router shutdown");
            return;
        };
        handle.spawn(async move {
            if let Err(err) = router.shutdown().await {
                debug!(error = %err, "embedded iroh-blobs artifact transport shutdown failed");
            }
        });
    }
}

/// Wait until iroh has produced an address that other nodes can dial.
///
/// Binding can complete before the endpoint has enough address information for
/// scheduler advertisement, especially when discovery transports are still
/// initializing.
async fn wait_for_dialable_addr(endpoint: &Endpoint) -> Result<EndpointAddr> {
    let mut watch_addr = endpoint.watch_addr();
    loop {
        let addr = watch_addr.get();
        if !addr.is_empty() {
            return Ok(addr);
        }
        tokio::time::timeout(ENDPOINT_ADDR_TIMEOUT, watch_addr.updated())
            .await
            .map_err(|_| Error::Timeout {
                operation: "wait for P2P endpoint network address",
            })?
            .context("P2P endpoint address watcher disconnected")?;
    }
}

fn blob_hash_from_descriptor(descriptor: &P2pArtifactDescriptor) -> Result<iroh_blobs::Hash> {
    let Some(blob_hash) = descriptor.backend_locator.as_deref() else {
        return Err(Error::InvalidDescriptor {
            reason: format!("missing iroh blob hash locator for key {}", descriptor.key),
        });
    };

    blob_hash.parse().map_err(|err| Error::InvalidDescriptor {
        reason: format!(
            "invalid iroh blob hash locator for key {}: {err}",
            descriptor.key
        ),
    })
}

async fn first_some_buffered_in_order<I, F, Fut, T>(
    items: impl IntoIterator<Item = I>,
    concurrency: usize,
    lookup: F,
) -> Option<T>
where
    F: FnMut(I) -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let mut results = stream::iter(items).map(lookup).buffered(concurrency.max(1));
    while let Some(result) = results.next().await {
        if result.is_some() {
            return result;
        }
    }
    None
}

fn resolve_remote_local_providers(
    mut descriptor: P2pArtifactDescriptor,
    response_peer: &P2pPeer,
) -> Option<P2pArtifactDescriptor> {
    let mut providers = Vec::with_capacity(descriptor.providers.len());
    let mut seen = HashSet::with_capacity(descriptor.providers.len());
    for provider in descriptor.providers {
        let peer = match provider {
            P2pArtifactProvider::Local => response_peer.clone(),
            P2pArtifactProvider::Peer(peer) => peer,
        };
        if seen.insert(peer.clone()) {
            providers.push(P2pArtifactProvider::from(peer));
        }
    }
    if providers.is_empty() {
        return None;
    }
    descriptor.providers = providers;
    Some(descriptor)
}

fn publish_tag_name(key: &P2pArtifactKey) -> String {
    format!("{PUBLISH_TAG_PREFIX}{}", digest::sha256_hex(key.as_bytes()))
}

impl IrohBlobsP2pTransport {
    /// Periodically re-announces this node's published artifacts.
    ///
    /// The scheduler's artifact index is in-memory and lost on restart, and
    /// keys were recorded only at publish and fetch time. A restarted scheduler
    /// therefore stayed permanently empty for everything already published, and
    /// every lookup silently degraded to broad O(nodes) peer polling for the
    /// life of the process.
    ///
    /// The node's own catalog is durable, so it can simply say again what it
    /// holds. Announcements are idempotent, so this needs no coordination with
    /// the scheduler's state; it converges from whatever the scheduler has.
    /// Keys are sent in bounded batches so a node holding many artifacts
    /// spreads the work over several intervals instead of spiking.
    fn spawn_reannounce_task(
        catalog: std::sync::Weak<
            tokio::sync::RwLock<std::collections::HashMap<P2pArtifactKey, P2pArtifactDescriptor>>,
        >,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
        interval: Duration,
    ) {
        if interval.is_zero() {
            return;
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut cursor = 0usize;
            loop {
                ticker.tick().await;
                // The transport is gone; stop rather than resurrect its state.
                let Some(catalog) = catalog.upgrade() else {
                    return;
                };
                let keys: Vec<P2pArtifactKey> = catalog.read().await.keys().cloned().collect();
                drop(catalog);
                if keys.is_empty() {
                    cursor = 0;
                    continue;
                }
                if cursor >= keys.len() {
                    cursor = 0;
                }
                let end = (cursor + REANNOUNCE_BATCH).min(keys.len());
                for key in &keys[cursor..end] {
                    if let Err(error) = peer_discovery.record_key(key).await {
                        // The scheduler being unreachable is transient; the next
                        // cycle will announce the same key again.
                        debug!(%key, error = %error, "re-announce of P2P artifact failed");
                    }
                }
                cursor = end;
            }
        });
    }

    /// Arms the collector gate periodically and reports the store's size.
    ///
    /// Arming slightly ahead of the collector's own interval means each sweep
    /// finds the gate open, so retention actually converges instead of
    /// depending on an unpublish having happened to race the last tick.
    ///
    /// Store size is sampled from the same tick because it is the series that
    /// says whether that convergence is real: the store grows on every publish
    /// and every fetched blob and shrinks only when a sweep runs, so a flat
    /// create/delete loop that ratchets the gauge upward is a retention leak
    /// no other signal would show.
    fn spawn_gc_arming_task(
        pending_gc: Arc<AtomicBool>,
        gc_interval: Duration,
        store_dir: std::path::PathBuf,
    ) {
        if gc_interval.is_zero() {
            return;
        }
        let arm_interval = gc_interval / 2;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(arm_interval.max(Duration::from_secs(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                // Only one owner of the flag remains once the transport is
                // dropped, which is the signal to stop arming.
                if Arc::strong_count(&pending_gc) == 1 {
                    return;
                }
                pending_gc.store(true, Ordering::Release);
                sample_store_bytes(store_dir.clone()).await;
            }
        });
    }
}

/// Reports the on-disk size of the transport's blob store.
///
/// Walked on a blocking thread: the store is a directory tree of blob files
/// and its stat calls have no business on a runtime worker.
async fn sample_store_bytes(store_dir: std::path::PathBuf) {
    let measured = tokio::task::spawn_blocking(move || directory_size(&store_dir)).await;
    match measured {
        Ok(bytes) => p2p_metrics::set_store_bytes(bytes),
        Err(err) => debug!(error = %err, "failed to measure P2P store size"),
    }
}

fn directory_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            total += directory_size(&entry.path());
        } else if let Ok(metadata) = entry.metadata() {
            total += metadata.len();
        }
    }
    total
}

fn gated_gc_config(interval: Duration, pending_gc: Arc<AtomicBool>) -> GcConfig {
    GcConfig {
        interval,
        add_protected: Some(Arc::new(move |_| {
            let pending_gc = pending_gc.clone();
            Box::pin(async move {
                if pending_gc.swap(false, Ordering::AcqRel) {
                    ProtectOutcome::Continue
                } else {
                    ProtectOutcome::Abort
                }
            })
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::{Barrier, Notify};

    use crate::cfg::P2pConfig;
    use crate::p2p::config::ResolvedP2pConfig;
    use crate::p2p::discovery::P2pPeerDiscovery;
    use crate::p2p::transport::P2pTransport;
    use crate::p2p::{NoopP2pPeerDiscovery, P2pPeer, P2pTransportKind, StaticP2pPeerDiscovery};

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn buffered_lookup_preserves_candidate_order() {
        let release_first = Arc::new(Notify::new());
        let first_started = Arc::new(Notify::new());
        let second_finished = Arc::new(Notify::new());

        let lookup = tokio::spawn({
            let release_first = release_first.clone();
            let first_started = first_started.clone();
            let second_finished = second_finished.clone();
            async move {
                first_some_buffered_in_order([0, 1], 2, move |candidate| {
                    let release_first = release_first.clone();
                    let first_started = first_started.clone();
                    let second_finished = second_finished.clone();
                    async move {
                        match candidate {
                            0 => {
                                first_started.notify_one();
                                release_first.notified().await;
                                Some("first")
                            }
                            1 => {
                                second_finished.notify_one();
                                Some("second")
                            }
                            _ => None,
                        }
                    }
                })
                .await
            }
        });

        tokio::time::timeout(TEST_TIMEOUT, async {
            first_started.notified().await;
            second_finished.notified().await;
        })
        .await
        .expect("initial buffered lookups should complete");
        assert!(
            !lookup.is_finished(),
            "a lower-priority result must wait for earlier candidates"
        );

        release_first.notify_one();
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, lookup)
                .await
                .expect("ordered lookup should complete")
                .expect("lookup task"),
            Some("first")
        );
    }

    #[tokio::test]
    async fn buffered_lookup_limits_in_flight_candidates() {
        let current = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let first_window_ready = Arc::new(Barrier::new(5));
        let release_first_window = Arc::new(Barrier::new(5));

        let lookup = tokio::spawn({
            let current = current.clone();
            let maximum = maximum.clone();
            let first_window_ready = first_window_ready.clone();
            let release_first_window = release_first_window.clone();
            async move {
                first_some_buffered_in_order(0..8, 4, move |candidate| {
                    let current = current.clone();
                    let maximum = maximum.clone();
                    let first_window_ready = first_window_ready.clone();
                    let release_first_window = release_first_window.clone();
                    async move {
                        let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(active, Ordering::SeqCst);
                        if candidate < 4 {
                            first_window_ready.wait().await;
                            release_first_window.wait().await;
                        }
                        current.fetch_sub(1, Ordering::SeqCst);
                        None::<()>
                    }
                })
                .await
            }
        });

        tokio::time::timeout(TEST_TIMEOUT, first_window_ready.wait())
            .await
            .expect("initial concurrency window should fill");
        assert_eq!(current.load(Ordering::SeqCst), 4);
        tokio::time::timeout(TEST_TIMEOUT, release_first_window.wait())
            .await
            .expect("initial concurrency window should be released");
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, lookup)
                .await
                .expect("bounded lookup should complete")
                .expect("lookup task"),
            None
        );
        assert_eq!(maximum.load(Ordering::SeqCst), 4);
    }

    async fn collect_range_stream(mut stream: P2pByteStream) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    fn p2p_config(store_dir: std::path::PathBuf) -> P2pConfig {
        P2pConfig {
            enabled: true,
            transport: P2pTransportKind::Iroh,
            store_dir,
            listen_addr: "127.0.0.1:0".to_string(),
            lookup_timeout_ms: 10_000,
            fetch_timeout_ms: 10_000,
            // Test peers are on loopback but the gate machine is heavily
            // loaded; the shipped 100 ms per-peer bound is not a property any
            // of these tests are about.
            catalog_lookup_timeout_ms: 10_000,
            ..P2pConfig::default()
        }
    }

    async fn test_transport(
        config: &P2pConfig,
        node_id: &str,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<IrohBlobsP2pTransport> {
        test_transport_with_gc_interval(config, node_id, peer_discovery, DEFAULT_STORE_GC_INTERVAL)
            .await
    }

    async fn test_transport_with_gc_interval(
        config: &P2pConfig,
        node_id: &str,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
        gc_interval: std::time::Duration,
    ) -> Result<IrohBlobsP2pTransport> {
        IrohBlobsP2pTransport::new_with_gc_interval(
            ResolvedP2pConfig::from_config(config),
            node_id.to_string(),
            peer_discovery,
            gc_interval,
        )
        .await
    }

    async fn test_provider_consumer(
        temp: &tempfile::TempDir,
    ) -> Result<(IrohBlobsP2pTransport, IrohBlobsP2pTransport)> {
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: provider_endpoint,
            }])),
        )
        .await
        .context("start consumer P2P transport")?;
        Ok((provider, consumer))
    }

    fn invalid_endpoint() -> P2pEndpoint {
        P2pEndpoint {
            backend: "iroh".to_string(),
            address: "not an iroh endpoint".to_string(),
        }
    }

    fn first_peer_provider(descriptor: &P2pArtifactDescriptor) -> &P2pPeer {
        descriptor
            .providers
            .iter()
            .find_map(|provider| match provider {
                P2pArtifactProvider::Local => None,
                P2pArtifactProvider::Peer(peer) => Some(peer),
            })
            .expect("descriptor should have a concrete peer provider")
    }

    #[test]
    fn remote_catalog_descriptor_resolves_local_providers_to_response_peer() {
        let peer = P2pPeer {
            node_id: "provider-node".to_string(),
            endpoint: invalid_endpoint(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/remote-local-provider".to_string(),
            providers: vec![
                P2pArtifactProvider::Local,
                P2pArtifactProvider::from(peer.clone()),
            ],
            backend_locator: Some("blob-hash".to_string()),
            metadata: serde_json::Value::Null,
        };

        let descriptor = resolve_remote_local_providers(descriptor, &peer)
            .expect("peer provider should keep descriptor usable");

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::from(peer)]);
    }

    #[test]
    fn remote_catalog_descriptor_with_only_local_provider_uses_response_peer() {
        let peer = P2pPeer {
            node_id: "provider-node".to_string(),
            endpoint: invalid_endpoint(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/remote-only-local-provider".to_string(),
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some("blob-hash".to_string()),
            metadata: serde_json::Value::Null,
        };

        let descriptor = resolve_remote_local_providers(descriptor, &peer)
            .expect("response peer should make descriptor usable");

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::from(peer)]);
    }

    /// Nothing about an artifact is known to be true until it has been
    /// opened, and by then the bytes are already here. So the limit has to
    /// stop the transfer, not judge it afterwards — and it has to hold against
    /// a peer whose blob is simply bigger than the caller can take, which is
    /// indistinguishable from a hostile one at this point.
    #[tokio::test]
    async fn a_fetch_refuses_an_artifact_bigger_than_the_caller_allows() -> Result<()> {
        crate::logging::init_for_tests();

        tokio::time::timeout(TEST_TIMEOUT, async {
            let temp = tempfile::tempdir().context("create temp test dir")?;
            let (provider, consumer) = test_provider_consumer(&temp).await?;

            let key = "test/p2p/iroh/oversized".to_string();
            let bytes = vec![7u8; 512 * 1024];
            provider
                .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
                .await
                .context("publish oversized artifact")?;

            let descriptor = consumer
                .lookup(&key)
                .await?
                .context("expected descriptor from provider")?;

            let limit = 64 * 1024;
            let destination = temp.path().join("refused.bin");
            let error = consumer
                .fetch(&descriptor, &destination, limit)
                .await
                .expect_err("a blob past the limit must not be written");
            assert!(
                matches!(error, Error::ArtifactTooLarge { limit: reported } if reported == limit),
                "expected a size refusal, got {error:?}"
            );

            // Refused, and not left behind for a later caller to trip over.
            assert!(
                !destination.exists(),
                "a refused fetch must leave no artifact at the destination"
            );
            assert!(
                consumer.get_local(&key).await.is_none(),
                "a refused artifact must not be advertised onward"
            );

            // The same artifact within a limit that allows it still arrives,
            // so the bound is a bound and not a broken transfer.
            let allowed = temp.path().join("allowed.bin");
            let size = consumer
                .fetch(&descriptor, &allowed, bytes.len() as u64 * 2)
                .await
                .context("fetch the same artifact under a sufficient limit")?;
            assert_eq!(size, bytes.len() as u64);

            // The consumer now holds the blob, so the same request resolves
            // locally and never reaches the downloader. A local hit is not
            // peer-controlled, but the caller's bound is about what it can
            // hold in memory, and that does not change with where the bytes
            // came from — so this path has to refuse too, and it is the only
            // way to reach the check that makes it.
            let local = consumer
                .get_local(&key)
                .await
                .context("the fetched artifact should be advertised locally")?;
            let error = consumer
                .fetch_bytes(&local, limit)
                .await
                .expect_err("a local blob past the limit must not be read into memory");
            assert!(
                matches!(error, Error::ArtifactTooLarge { limit: reported } if reported == limit),
                "expected a size refusal from the local path, got {error:?}"
            );

            // The disk path is the same promise: a local hit is exported
            // straight to the caller's destination, so the bound has to stop
            // it before the first byte lands there.
            let refused_local = temp.path().join("refused-local.bin");
            let error = consumer
                .fetch(&local, &refused_local, limit)
                .await
                .expect_err("a local blob past the limit must not be written to disk");
            assert!(
                matches!(error, Error::ArtifactTooLarge { limit: reported } if reported == limit),
                "expected a size refusal from the local disk path, got {error:?}"
            );
            assert!(
                !refused_local.exists(),
                "a refused local fetch must leave no artifact at the destination"
            );

            let held = consumer
                .fetch_bytes(&local, bytes.len() as u64 * 2)
                .await
                .context("read the local artifact under a sufficient limit")?;
            assert_eq!(held.len(), bytes.len());

            let allowed_local = temp.path().join("allowed-local.bin");
            let size = consumer
                .fetch(&local, &allowed_local, bytes.len() as u64 * 2)
                .await
                .context("export the local artifact under a sufficient limit")?;
            assert_eq!(size, bytes.len() as u64);

            consumer.shutdown().await.context("shutdown consumer P2P")?;
            provider.shutdown().await.context("shutdown provider P2P")?;
            Ok(())
        })
        .await
        .context("oversized fetch test timed out")?
    }

    #[tokio::test]
    async fn transport_looks_up_and_fetches_from_peer_over_loopback() -> Result<()> {
        crate::logging::init_for_tests();

        tokio::time::timeout(TEST_TIMEOUT, async {
            let temp = tempfile::tempdir().context("create temp test dir")?;
            let (provider, consumer) = test_provider_consumer(&temp).await?;

            let key = "test/p2p/iroh/network-fetch".to_string();
            let bytes = b"artifact bytes served through the embedded iroh-blobs transport";
            provider
                .publish(
                    &P2pPublishRequest::bytes(key.clone(), bytes.as_slice())
                        .with_metadata(serde_json::json!({ "kind": "network-integration-test" })),
                )
                .await
                .context("publish provider bytes artifact")?;

            let sibling_key = "test/p2p/iroh/network-fetch-sibling".to_string();
            let sibling_source = temp.path().join("sibling-artifact.bin");
            tokio::fs::write(&sibling_source, b"sibling artifact bytes")
                .await
                .context("write sibling artifact")?;
            provider
                .publish(&P2pPublishRequest::file(
                    sibling_key.clone(),
                    sibling_source,
                ))
                .await
                .context("publish sibling provider artifact")?;

            assert!(provider.get_local(&sibling_key).await.is_some());
            assert!(
                consumer.get_local(&key).await.is_none(),
                "consumer must not satisfy lookup from its own local catalog"
            );

            let descriptor = consumer
                .lookup(&key)
                .await?
                .context("expected descriptor from provider")?;
            assert_eq!(descriptor.key, key);
            assert_eq!(first_peer_provider(&descriptor).node_id, "provider-node");
            assert_eq!(
                descriptor.metadata,
                serde_json::json!({ "kind": "network-integration-test" })
            );
            assert!(blob_hash_from_descriptor(&descriptor).is_ok());
            assert_eq!(first_peer_provider(&descriptor).endpoint.backend, "iroh");

            let destination = temp.path().join("downloaded").join("artifact.bin");
            let fetched_size = consumer
                .fetch(&descriptor, &destination, u64::MAX)
                .await
                .context("fetch provider blob over P2P")?;
            assert_eq!(fetched_size, bytes.len() as u64);

            let downloaded = tokio::fs::read(&destination)
                .await
                .context("read downloaded artifact")?;
            assert_eq!(downloaded, bytes);
            let local_descriptor = consumer
                .get_local(&key)
                .await
                .context("consumer should advertise fetched artifact")?;
            assert_eq!(local_descriptor.providers, vec![P2pArtifactProvider::Local]);
            assert_eq!(
                blob_hash_from_descriptor(&local_descriptor)?,
                blob_hash_from_descriptor(&descriptor)?
            );

            let relay = test_transport(
                &p2p_config(temp.path().join("relay-store")),
                "relay-node",
                Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                    node_id: "consumer-node".to_string(),
                    endpoint: consumer
                        .local_endpoint()
                        .context("consumer transport should expose a local endpoint")?,
                }])),
            )
            .await
            .context("start relay P2P transport")?;

            let relay_descriptor = relay
                .lookup(&key)
                .await?
                .context("relay should find consumer as provider after fetch")?;
            assert_eq!(
                first_peer_provider(&relay_descriptor).node_id,
                "consumer-node"
            );
            assert_eq!(
                first_peer_provider(&relay_descriptor).endpoint,
                consumer.local_endpoint().unwrap()
            );
            assert_eq!(
                blob_hash_from_descriptor(&relay_descriptor)?,
                blob_hash_from_descriptor(&descriptor)?
            );

            let relay_destination = temp.path().join("relay-download.bin");
            let relay_fetched_size = relay
                .fetch(&relay_descriptor, &relay_destination, u64::MAX)
                .await
                .context("relay fetch blob from consumer")?;
            assert_eq!(relay_fetched_size, bytes.len() as u64);
            let relay_downloaded = tokio::fs::read(&relay_destination)
                .await
                .context("read relay downloaded artifact")?;
            assert_eq!(relay_downloaded, bytes);

            relay.shutdown().await.context("shutdown relay P2P")?;
            consumer.shutdown().await.context("shutdown consumer P2P")?;
            provider.shutdown().await.context("shutdown provider P2P")?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("P2P network test timed out"))?
    }

    #[tokio::test]
    async fn transport_fetch_range_reads_exact_bytes_without_advertising_partial_blob() -> Result<()>
    {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/range-fetch".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let offset = 7777_u64;
        let len = 33_333_usize;
        let fetched = consumer
            .fetch_byte_range(&descriptor, offset, len)
            .await
            .context("fetch range from provider")?;
        let fetched = collect_range_stream(fetched).await?;
        assert_eq!(
            fetched.as_slice(),
            &bytes[offset as usize..offset as usize + len]
        );
        assert!(
            consumer.get_local(&key).await.is_none(),
            "range-only fetch must not advertise this node as a full artifact provider"
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// A streamed range returns the right bytes and leaves nothing behind.
    ///
    /// This is the whole difference between the two paths, and it is not
    /// visible in what they return -- both return the same bytes. The
    /// store-backed path verifies into the store and reads back out, so the
    /// blob is resident afterwards; the streaming path verifies in flight, so
    /// it is not. Asserting the bytes alone would pass against a
    /// `fetch_byte_range_streaming` that just called `fetch_byte_range`, which
    /// is exactly the default this overrides.
    #[tokio::test]
    async fn a_streamed_range_returns_the_bytes_without_storing_the_blob() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/streamed-range".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let store_dir = temp.path().join("consumer-store");
        let before = directory_size(&store_dir);

        let offset = 7777_u64;
        let len = 33_333_usize;
        let streamed = consumer
            .fetch_byte_range_streaming(&descriptor, offset, len)
            .await
            .context("stream range from provider")?;
        let streamed = collect_range_stream(streamed).await?;

        assert_eq!(
            streamed.as_slice(),
            &bytes[offset as usize..offset as usize + len],
            "a streamed range must return exactly the bytes asked for"
        );
        let after = directory_size(&store_dir);
        assert_eq!(
            after, before,
            "a streamed range grew the local store from {before} to {after} bytes; \
             the store round trip is the cost this path exists to avoid"
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// The store-backed path is the other half of that contract.
    ///
    /// Without this, the assertion above could be satisfied by a streaming path
    /// that silently fetched nothing, and by a store path that had stopped
    /// storing -- which would break background prefill, whose entire purpose is
    /// that the bytes stay.
    #[tokio::test]
    async fn a_stored_range_does_leave_the_blob_behind() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/stored-range".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let store_dir = temp.path().join("consumer-store");
        let before = directory_size(&store_dir);

        let fetched = consumer
            .fetch_byte_range(&descriptor, 7777, 33_333)
            .await
            .context("fetch range from provider")?;
        let _ = collect_range_stream(fetched).await?;

        let after = directory_size(&store_dir);
        assert!(
            after > before,
            "the store-backed path left nothing local ({before} -> {after} bytes); \
             background prefill depends on the bytes staying"
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// A range running off the end of the blob has to end the stream in an
    /// error. Callers may already have committed response headers by the time
    /// they poll it, so a short read that finishes cleanly is a truncation
    /// they have no way left to report.
    #[tokio::test]
    async fn a_local_range_export_that_ends_short_fails_the_stream() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let transport = test_transport(
            &p2p_config(temp.path().join("store")),
            "self-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start P2P transport")?;

        let key = "test/p2p/iroh/short-range".to_string();
        let bytes = vec![3u8; 4096];
        transport
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish artifact")?;
        let local = transport
            .get_local(&key)
            .await
            .context("a published artifact should resolve locally")?;

        let stream = transport
            .fetch_byte_range(&local, 0, bytes.len() * 2)
            .await
            .context("open a range longer than the blob")?;
        // Report the length rather than the body: a truncated success is the
        // interesting fact, and 4 KiB of payload in the failure is not.
        let error = collect_range_stream(stream)
            .await
            .map(|bytes| bytes.len())
            .expect_err("a range past the end of the blob must not complete successfully");
        assert!(
            error.to_string().contains("bytes short"),
            "expected a short-read refusal, got {error:?}"
        );

        transport.shutdown().await.context("shutdown P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn transport_fetch_bytes_reads_full_blob_and_advertises_it() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/bytes-fetch".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let fetched = consumer
            .fetch_bytes(&descriptor, u64::MAX)
            .await
            .context("fetch full blob bytes from provider")?;
        assert_eq!(fetched.as_ref(), bytes.as_slice());
        let local_descriptor = consumer
            .get_local(&key)
            .await
            .context("consumer should advertise full bytes fetch")?;
        assert_eq!(local_descriptor.providers, vec![P2pArtifactProvider::Local]);
        assert_eq!(
            blob_hash_from_descriptor(&local_descriptor)?,
            blob_hash_from_descriptor(&descriptor)?
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn fetch_rejects_descriptor_without_blob_hash_locator() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start consumer P2P transport")?;
        let source = temp.path().join("source-artifact.bin");
        tokio::fs::write(&source, b"unpublished bytes")
            .await
            .context("write source artifact")?;
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/missing-blob-hash".to_string(),
            providers: vec![P2pArtifactProvider::from(P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: invalid_endpoint(),
            })],
            backend_locator: None,
            metadata: serde_json::Value::Null,
        };

        let err = consumer
            .fetch(&descriptor, &temp.path().join("downloaded.bin"), u64::MAX)
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, Error::InvalidDescriptor { .. }));
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn published_catalog_survives_transport_restart() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let store_dir = temp.path().join("provider-store");
        let config = p2p_config(store_dir);
        let key = "test/p2p/iroh/persisted-catalog".to_string();
        let bytes = b"artifact bytes retained across an iroh transport restart";

        let provider = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start provider P2P transport")?;
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.as_slice()))
            .await
            .context("publish artifact")?;
        let before_restart = provider.get_local(&key).await;
        assert_eq!(
            before_restart
                .as_ref()
                .context("descriptor before restart")?
                .providers,
            vec![P2pArtifactProvider::Local]
        );
        provider.shutdown().await.context("shutdown provider P2P")?;
        drop(provider);

        let restarted = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("restart provider P2P transport")?;
        let descriptor = restarted
            .get_local(&key)
            .await
            .context("list local catalog after restart")?;
        assert_eq!(descriptor.key, key);
        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::Local]);
        assert!(blob_hash_from_descriptor(&descriptor).is_ok());

        let destination = temp.path().join("downloaded-after-restart.bin");
        let fetched_size = restarted
            .fetch(&descriptor, &destination, u64::MAX)
            .await
            .context("fetch persisted local artifact")?;
        assert_eq!(fetched_size, bytes.len() as u64);
        let downloaded = tokio::fs::read(&destination)
            .await
            .context("read downloaded artifact")?;
        assert_eq!(downloaded, bytes);
        restarted
            .shutdown()
            .await
            .context("shutdown restarted provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn unpublish_stops_remote_lookup_and_persists_across_restart() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let store_dir = temp.path().join("provider-store");
        let config = p2p_config(store_dir.clone());
        let key = "test/p2p/iroh/unpublish-persists".to_string();
        let provider = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: provider_endpoint.clone(),
            }])),
        )
        .await
        .context("start consumer P2P transport")?;

        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        assert!(
            consumer.lookup(&key).await?.is_some(),
            "consumer should find provider before unpublish"
        );

        assert!(provider.unpublish(&key).await?);

        assert!(
            consumer.lookup(&key).await?.is_none(),
            "consumer should stop finding provider after unpublish"
        );
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        drop(provider);

        let restarted = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("restart provider P2P transport")?;
        assert!(restarted.get_local(&key).await.is_none());
        restarted
            .shutdown()
            .await
            .context("shutdown restarted provider P2P")?;
        Ok(())
    }

    /// Captures the label sets a counter was incremented under. Installed per
    /// thread, which is why the tests using it drive a current-thread runtime.
    #[derive(Default, Clone)]
    struct CounterSpy {
        observed: Arc<std::sync::Mutex<Vec<String>>>,
    }

    struct SpyCounter {
        rendered: String,
        observed: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl metrics::CounterFn for SpyCounter {
        fn increment(&self, _value: u64) {
            self.observed.lock().unwrap().push(self.rendered.clone());
        }
        fn absolute(&self, _value: u64) {}
    }

    impl metrics::Recorder for CounterSpy {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            let mut labels: Vec<String> = key
                .labels()
                .map(|label| format!("{}={}", label.key(), label.value()))
                .collect();
            labels.sort();
            metrics::Counter::from_arc(Arc::new(SpyCounter {
                rendered: format!("{} {}", key.name(), labels.join(" ")),
                observed: Arc::clone(&self.observed),
            }))
        }
        fn register_gauge(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// Publication and withdrawal have to be readable per publisher, because
    /// the sha256 namespace has two of them and "the store is bounded" is an
    /// assertion about their sum. The labels are drawn from closed sets: the
    /// key contributes its namespace and never its digest.
    #[test]
    fn publish_and_withdraw_are_counted_per_publisher() {
        let spy = CounterSpy::default();
        let observed = Arc::clone(&spy.observed);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        metrics::with_local_recorder(&spy, || {
            runtime.block_on(async {
                let temp = tempfile::tempdir().expect("temp dir");
                let transport = test_transport(
                    &p2p_config(temp.path().join("store")),
                    "counted-node",
                    Arc::new(NoopP2pPeerDiscovery),
                )
                .await
                .expect("start transport");

                let key = "overlaybd-layer/v1/sha256:counted".to_string();
                transport
                    .publish(
                        &P2pPublishRequest::bytes(key.clone(), b"counted bytes".as_slice())
                            .with_owner(P2pPublishOwner::ImageCache),
                    )
                    .await
                    .expect("publish");
                transport
                    .unpublish_owned(&key, P2pPublishOwner::ImageCache)
                    .await
                    .expect("unpublish");

                transport.shutdown().await.expect("shutdown");
            })
        });

        let observed = observed.lock().unwrap();
        assert!(
            observed.iter().any(|rendered| rendered
                == "agentenv_p2p_publish_total key_class=overlaybd_layer source=image_cache status=published"),
            "a publish must be attributed to its publisher, saw {observed:?}"
        );
        assert!(
            observed.iter().any(|rendered| rendered
                == "agentenv_p2p_unpublish_total key_class=overlaybd_layer status=withdrawn"),
            "the last withdrawal must be recorded as such, saw {observed:?}"
        );
    }

    /// Catalog lookups used to dial, ask one question and close, so a layer
    /// read at block granularity paid a QUIC handshake per block. The pool has
    /// to turn many lookups into one connection, and the server has to keep
    /// accepting streams on it.
    #[tokio::test]
    async fn repeated_lookups_against_one_peer_dial_once() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;
        let key = "test/p2p/iroh/pooled-lookup".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"pooled lookup bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        for _ in 0..20 {
            assert!(
                consumer.lookup(&key).await?.is_some(),
                "consumer should resolve the provider's descriptor"
            );
        }

        assert_eq!(
            consumer.catalog_connections.load(Ordering::Acquire),
            1,
            "twenty lookups against one peer must ride one connection"
        );
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// Zero connections is the documented escape hatch for a cluster where
    /// some nodes still serve one catalog stream per connection, so it has to
    /// actually restore the old dial-per-lookup path.
    #[tokio::test]
    async fn pooling_can_be_turned_off() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let mut config = p2p_config(temp.path().join("consumer-store"));
        config.catalog_max_connections = 0;
        let consumer = test_transport(
            &config,
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: provider_endpoint,
            }])),
        )
        .await
        .context("start consumer P2P transport")?;

        let key = "test/p2p/iroh/unpooled-lookup".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"unpooled lookup bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        assert!(consumer.lookup(&key).await?.is_some());
        assert_eq!(
            consumer.catalog_connections.load(Ordering::Acquire),
            0,
            "with pooling off the pool is never built, so it dials nothing"
        );
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// A peer's endpoint address is an opaque blob the scheduler relays and
    /// every node dials. Naming the cluster's networks is what stops a
    /// compromised node steering its peers at an address outside them.
    #[tokio::test]
    async fn an_endpoint_outside_the_cluster_networks_is_refused() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let mut config = p2p_config(temp.path().join("guarded-store"));
        config.cluster_cidrs = vec!["10.0.0.0/8".to_string()];
        let transport = test_transport(&config, "guarded-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start guarded P2P transport")?;

        let outside = EndpointAddr::new(iroh::SecretKey::generate().public())
            .with_ip_addr("203.0.113.7:4433".parse::<SocketAddr>().expect("addr"));
        let inside = EndpointAddr::new(iroh::SecretKey::generate().public())
            .with_ip_addr("10.1.2.3:4433".parse::<SocketAddr>().expect("addr"));

        assert!(
            matches!(
                transport.validate_endpoint_addr(&outside),
                Err(Error::InvalidDescriptor { .. })
            ),
            "an address outside the cluster networks must be refused"
        );
        transport
            .validate_endpoint_addr(&inside)
            .context("an address inside the cluster networks must be accepted")?;

        transport.shutdown().await.context("shutdown P2P")?;
        Ok(())
    }

    /// Two publishers share the `overlaybd-layer/v1/sha256:` namespace — the
    /// image cache when a layer lands in the commit store, and snapshot
    /// publication for every lower of a committed chain — and each has its own
    /// removal edge. Retention is one tag and one catalog entry per key, so
    /// the first edge to run must not take the artifact out from under the
    /// other.
    #[tokio::test]
    async fn an_artifact_survives_until_its_last_owner_releases_it() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;

        let key = "overlaybd-layer/v1/sha256:shared".to_string();
        for owner in [P2pPublishOwner::ImageCache, P2pPublishOwner::Unscoped] {
            provider
                .publish(
                    &P2pPublishRequest::bytes(key.clone(), b"shared layer bytes".as_slice())
                        .with_owner(owner),
                )
                .await
                .context("publish artifact")?;
        }
        let descriptor = provider
            .get_local(&key)
            .await
            .context("descriptor after publish")?;
        let blob_hash = blob_hash_from_descriptor(&descriptor)?;
        let tag_name = publish_tag_name(&key);

        assert!(
            !provider
                .unpublish_owned(&key, P2pPublishOwner::ImageCache)
                .await?,
            "releasing one of two owners must not withdraw the artifact"
        );
        assert!(
            provider.get_local(&key).await.is_some(),
            "the other owner is still advertising this key"
        );
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_some(),
            "the retention tag protects bytes the remaining owner still needs"
        );

        assert!(
            provider.unpublish(&key).await?,
            "releasing the last owner must withdraw the artifact"
        );
        assert!(provider.get_local(&key).await.is_none());
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_none(),
            "the last release must drop the retention tag"
        );
        let _ = blob_hash;

        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// The owner set is what keeps a shared artifact alive, so it has to
    /// survive a process restart the same way the catalog entry does.
    #[tokio::test]
    async fn owner_sets_survive_a_transport_restart() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let config = p2p_config(temp.path().join("provider-store"));
        let provider = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start provider P2P transport")?;

        let key = "overlaybd-layer/v1/sha256:restart-shared".to_string();
        for owner in [P2pPublishOwner::ImageCache, P2pPublishOwner::Unscoped] {
            provider
                .publish(
                    &P2pPublishRequest::bytes(key.clone(), b"shared layer bytes".as_slice())
                        .with_owner(owner),
                )
                .await
                .context("publish artifact")?;
        }
        provider.shutdown().await.context("shutdown provider P2P")?;
        drop(provider);

        let restarted = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("restart provider P2P transport")?;
        assert_eq!(
            restarted.published_catalog.owners_of(&key).await,
            std::collections::BTreeSet::from([
                P2pPublishOwner::ImageCache,
                P2pPublishOwner::Unscoped
            ]),
            "both owners must be recovered from the persisted catalog"
        );
        assert!(
            !restarted
                .unpublish_owned(&key, P2pPublishOwner::ImageCache)
                .await?,
            "a restart must not collapse two owners into one"
        );
        assert!(restarted.get_local(&key).await.is_some());
        restarted
            .shutdown()
            .await
            .context("shutdown restarted provider P2P")?;
        Ok(())
    }

    /// The transport's collector is gated so it only sweeps when something
    /// asks it to, which is what makes the image cache's maintenance pass a
    /// reachable arming event rather than a comment.
    #[tokio::test]
    async fn requesting_gc_arms_the_collector_gate() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;

        provider.pending_gc.store(false, Ordering::Release);
        provider.request_gc().await;

        assert!(
            provider.pending_gc.load(Ordering::Acquire),
            "a maintenance signal must arm the collector"
        );
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn unpublish_deletes_retention_tag_and_removes_blob() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport_with_gc_interval(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
            Duration::from_millis(100),
        )
        .await
        .context("start provider P2P transport")?;

        let key = "test/p2p/iroh/unpublish-gc".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes eligible for gc".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        let descriptor = provider
            .get_local(&key)
            .await
            .context("descriptor before unpublish")?;
        let blob_hash = blob_hash_from_descriptor(&descriptor)?;
        let tag_name = publish_tag_name(&key);
        assert!(
            !provider
                .unpublish(&"test/p2p/iroh/missing-unpublish".to_string())
                .await?,
            "missing key unpublish should be a no-op"
        );
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_some(),
            "publish should create deterministic retention tag"
        );
        assert!(provider
            .store
            .blobs()
            .has(blob_hash)
            .await
            .map_err(|err| Error::internal_message("check blob presence", err))?);

        assert!(provider.unpublish(&key).await?);

        assert!(provider.get_local(&key).await.is_none());
        assert!(
            provider.lookup(&key).await?.is_none(),
            "local lookup should stop after unpublish"
        );
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_none(),
            "unpublish should remove deterministic retention tag"
        );

        // Wait for the GC pass to remove the blob from the store after unpublish.
        let start = std::time::Instant::now();
        loop {
            if !provider
                .store
                .blobs()
                .has(blob_hash)
                .await
                .map_err(|err| Error::internal_message("check blob presence", err))?
            {
                break;
            }
            if start.elapsed() >= Duration::from_secs(5) {
                return Err(Error::Timeout {
                    operation: "wait for P2P blob GC",
                });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn fetch_rejects_descriptor_without_providers() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/missing-provider-endpoint".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        let descriptor = consumer.lookup(&key).await?.context("descriptor")?;
        let descriptor = P2pArtifactDescriptor {
            providers: Vec::new(),
            ..descriptor
        };

        let err = consumer
            .fetch(&descriptor, &temp.path().join("downloaded.bin"), u64::MAX)
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, Error::InvalidDescriptor { .. }));
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn lookup_ignores_unreachable_peer_and_returns_first_descriptor() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![
                P2pPeer {
                    node_id: "bad-node".to_string(),
                    endpoint: invalid_endpoint(),
                },
                P2pPeer {
                    node_id: "provider-node".to_string(),
                    endpoint: provider_endpoint,
                },
            ])),
        )
        .await
        .context("start consumer P2P transport")?;

        let key = "test/p2p/iroh/lookup-skips-bad-peer".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("lookup provider catalog")?;

        assert_eq!(first_peer_provider(&descriptor).node_id, "provider-node");
        assert!(blob_hash_from_descriptor(&descriptor).is_ok());
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    /// A key this node already holds never reaches peer lookup at all: the
    /// local catalog answers first, whatever discovery has to say. The
    /// self-peer branch inside `lookup_peers` is a different guard, pinned
    /// separately below.
    #[tokio::test]
    async fn lookup_answers_from_the_local_catalog_before_consulting_discovery() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let self_transport = test_transport(
            &p2p_config(temp.path().join("self-store")),
            "self-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start self P2P transport")?;
        let self_endpoint = self_transport
            .local_endpoint()
            .context("self transport should expose a local endpoint")?;
        self_transport
            .shutdown()
            .await
            .context("shutdown self P2P")?;
        let self_transport = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "self-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "self-node".to_string(),
                endpoint: self_endpoint,
            }])),
        )
        .await
        .context("start consumer P2P transport")?;

        let key = "test/p2p/iroh/self-peer".to_string();
        self_transport
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"local artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        let descriptor = self_transport
            .lookup(&key)
            .await?
            .context("lookup local catalog")?;

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::Local]);
        self_transport
            .shutdown()
            .await
            .context("shutdown self P2P")?;
        Ok(())
    }

    /// Discovery rows are the scheduler's view of the fleet, and they can name
    /// this node under either identity: the right node id carrying an address
    /// this process no longer listens on, or the right address under a node id
    /// it no longer uses. Either way the answer is this node's own catalog.
    ///
    /// `lookup` answers from the catalog before discovery is consulted, so the
    /// branch is only reachable by calling `lookup_peers` directly. An
    /// unreachable endpoint and a foreign node id make a dial distinguishable
    /// from a local hit: dialling the first fails, and dialling the second
    /// comes back through the catalog protocol with a peer provider rather
    /// than a local one.
    #[tokio::test]
    async fn lookup_peers_answers_locally_for_either_self_identity() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let transport = test_transport(
            &p2p_config(temp.path().join("store")),
            "self-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start P2P transport")?;
        let local_endpoint = transport
            .local_endpoint()
            .context("transport should expose a local endpoint")?;

        let key = "test/p2p/iroh/self-identity".to_string();
        transport
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"local artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        let matches_node_id = P2pPeer {
            node_id: "self-node".to_string(),
            endpoint: invalid_endpoint(),
        };
        let matches_endpoint = P2pPeer {
            node_id: "some-other-node".to_string(),
            endpoint: local_endpoint,
        };
        for peer in [matches_node_id, matches_endpoint] {
            let descriptor = transport
                .lookup_peers(vec![peer.clone()], &key)
                .await
                .with_context(|| format!("{peer:?} should resolve to the local catalog"))?;
            assert_eq!(
                descriptor.providers,
                vec![P2pArtifactProvider::Local],
                "{peer:?} names this node, so the descriptor must come from its own catalog"
            );
        }

        transport.shutdown().await.context("shutdown P2P")?;
        Ok(())
    }

    /// Drop is a courtesy shutdown, and the runtime it wants is not always
    /// there: the last reference can be released on a plain thread or after
    /// the runtime is gone at process exit. A panic in drop during an in-flight
    /// unwind aborts the process, which is a far worse outcome than an iroh
    /// router left for the OS to reclaim.
    #[test]
    fn dropping_a_live_transport_off_the_runtime_does_not_panic() {
        let runtime = tokio::runtime::Runtime::new().expect("build a test runtime");
        let temp = tempfile::tempdir().expect("create temp test dir");
        let transport = runtime
            .block_on(test_transport(
                &p2p_config(temp.path().join("store")),
                "self-node",
                Arc::new(NoopP2pPeerDiscovery),
            ))
            .expect("start P2P transport");

        // Not shut down first: the drop path under test is the one that still
        // has a live router to dispose of.
        std::thread::spawn(move || drop(transport))
            .join()
            .expect("dropping a live transport off the runtime must not panic");
    }
}
