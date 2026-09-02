use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::body::Body;
use axum::extract::{ConnectInfo, Json, Request, State};
use axum::http::header::{
    ACCEPT_RANGES, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, RANGE, USER_AGENT,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

use crate::digest;
use crate::p2p::metrics::{self as p2p_metrics, FacadeDescriptorSource, FacadeOutcome, FacadePath};
use crate::p2p::{
    P2pArtifactKey, P2pPublishMode, P2pPublishOwner, P2pPublishRequest, P2pTransport,
};

use super::artifact::{layer_artifact_key, CanonicalBlobIdentity, LayerMetadata, SHA256_HEX_LEN};
use super::cache::{DescriptorCache, OriginIdentity, OriginKeyCache, ResolvedLayer};

const DEFAULT_LOOKUP_TIMEOUT: Duration = Duration::from_millis(300);
const DEFAULT_FETCH_RANGE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_DESCRIPTOR_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_DESCRIPTOR_MISS_CACHE_TTL: Duration = Duration::from_secs(5);
const DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES: usize = 16 * 1024;
/// Origin URLs whose derived identity is remembered.
///
/// One entry per distinct layer URL the node is reading, so the bound only has
/// to cover the layers of the images open at once, not their blocks.
const DEFAULT_ORIGIN_KEY_CACHE_MAX_ENTRIES: usize = 4 * 1024;
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const ADDRESS_PREFIX: &str = "/p2p-http";
const UUID_ADDRESS_PREFIX: &str = "/p2p-uuid";
const PUBLISH_LAYER_PATH: &str = "/p2p-control/publish-layer";

#[derive(Debug, Clone)]
pub struct P2pHttpFacadeConfig {
    pub lookup_timeout: Duration,
    pub fetch_range_timeout: Duration,
    pub descriptor_cache_ttl: Duration,
    pub descriptor_miss_cache_ttl: Duration,
    pub descriptor_cache_max_entries: usize,
    pub allowed_publish_roots: Vec<PathBuf>,
    /// The subset of `allowed_publish_roots` whose files the image cache keeps
    /// alive for as long as they are advertised.
    ///
    /// Publication by reference hands the transport a path it reads from
    /// later, which is only safe where something guarantees the file outlives
    /// the advertisement. Anything else — a background-download cache, say,
    /// whose entries are evicted by `remove_dir_all` — has to be copied into
    /// the transport's own store instead.
    pub commit_store_roots: Vec<PathBuf>,
}

impl Default for P2pHttpFacadeConfig {
    fn default() -> Self {
        Self {
            lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
            fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
            descriptor_cache_ttl: DEFAULT_DESCRIPTOR_CACHE_TTL,
            descriptor_miss_cache_ttl: DEFAULT_DESCRIPTOR_MISS_CACHE_TTL,
            descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
            allowed_publish_roots: Vec::new(),
            commit_store_roots: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct P2pHttpFacadeHandle {
    address: String,
    uuid_address: String,
    publish_address: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl P2pHttpFacadeHandle {
    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn uuid_address(&self) -> &str {
        &self.uuid_address
    }

    pub fn publish_address(&self) -> &str {
        &self.publish_address
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(DEFAULT_SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(join_result) => {
                    join_result.context("p2p http facade task join failed")?;
                }
                Err(_) => {
                    warn!(
                        timeout_secs = DEFAULT_SHUTDOWN_TIMEOUT.as_secs(),
                        "p2p http facade shutdown timed out; aborting task"
                    );
                    task.abort();
                    if let Err(err) = task.await {
                        if !err.is_cancelled() {
                            return Err(err).context("p2p http facade task join failed");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for P2pHttpFacadeHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub async fn start_http_facade_with_config(
    transport: Arc<dyn P2pTransport>,
    config: P2pHttpFacadeConfig,
) -> Result<P2pHttpFacadeHandle> {
    P2pHttpFacadeServer::start(transport, config).await
}

#[derive(Clone)]
struct P2pHttpFacadeState {
    transport: Arc<dyn P2pTransport>,
    client: Client,
    lookup_timeout: Duration,
    fetch_range_timeout: Duration,
    descriptor_cache: DescriptorCache,
    origin_keys: OriginKeyCache,
    allowed_publish_roots: Arc<Vec<PathBuf>>,
    commit_store_roots: Arc<Vec<PathBuf>>,
}

struct P2pHttpFacadeServer;

impl P2pHttpFacadeServer {
    async fn start(
        transport: Arc<dyn P2pTransport>,
        config: P2pHttpFacadeConfig,
    ) -> Result<P2pHttpFacadeHandle> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind p2p http facade")?;
        let addr = listener
            .local_addr()
            .context("read p2p http facade local addr")?;
        let address = format!("http://{addr}{ADDRESS_PREFIX}");
        let uuid_address = format!("http://{addr}{UUID_ADDRESS_PREFIX}");
        let publish_address = format!("http://{addr}{PUBLISH_LAYER_PATH}");
        let allowed_publish_roots =
            canonicalize_publish_roots(config.allowed_publish_roots).await?;
        let commit_store_roots = canonicalize_publish_roots(config.commit_store_roots).await?;
        let state = P2pHttpFacadeState {
            transport,
            client: Client::new(),
            lookup_timeout: config.lookup_timeout,
            fetch_range_timeout: config.fetch_range_timeout,
            descriptor_cache: DescriptorCache::new(
                config.descriptor_cache_ttl,
                config.descriptor_miss_cache_ttl,
                config.descriptor_cache_max_entries,
            ),
            origin_keys: OriginKeyCache::new(DEFAULT_ORIGIN_KEY_CACHE_MAX_ENTRIES),
            allowed_publish_roots: Arc::new(allowed_publish_roots),
            commit_store_roots: Arc::new(commit_store_roots),
        };
        let app = Router::new()
            .route(
                &format!("{ADDRESS_PREFIX}/{{*origin}}"),
                get(handle_http_facade),
            )
            .route(
                &format!("{UUID_ADDRESS_PREFIX}/{{uuid}}"),
                get(handle_uuid_facade),
            )
            .route(PUBLISH_LAYER_PATH, post(handle_publish_layer))
            .with_state(state);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            if let Err(err) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            {
                warn!(error = %err, "p2p http facade exited with error");
            }
        });

        info!(
            address,
            uuid_address, publish_address, "p2p http facade started"
        );
        Ok(P2pHttpFacadeHandle {
            address,
            uuid_address,
            publish_address,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }
}

async fn canonicalize_publish_roots(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::with_capacity(roots.len());
    for root in roots {
        tokio::fs::create_dir_all(&root)
            .await
            .with_context(|| format!("create P2P publish root {}", root.display()))?;
        out.push(
            tokio::fs::canonicalize(&root)
                .await
                .with_context(|| format!("canonicalize P2P publish root {}", root.display()))?,
        );
    }
    Ok(out)
}

async fn handle_http_facade(
    State(state): State<P2pHttpFacadeState>,
    request: Request,
) -> Response<Body> {
    let mut timing = FacadeRequestTiming::new(FacadePath::Http);
    match handle_http_facade_result(state, request, &mut timing).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn handle_uuid_facade(
    State(state): State<P2pHttpFacadeState>,
    axum::extract::Path(uuid): axum::extract::Path<String>,
    request: Request,
) -> Response<Body> {
    let mut timing = FacadeRequestTiming::new(FacadePath::Uuid);
    match handle_uuid_facade_result(state, uuid, request, &mut timing).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

/// Records how a facade request was served, on the way out.
///
/// The interesting label is `outcome`: a P2P miss is a fallback to the origin
/// and must read as ordinary traffic, not as an error. Nothing in the existing
/// instrumentation could tell those apart, so "P2P failure is invisible" was
/// an assertion rather than a measurement.
///
/// The duration ends when the response headers are committed, not when the
/// body finishes: both sources stream, and what this exists to measure is the
/// resolution work in front of the stream rather than the transfer itself.
struct FacadeRequestTiming {
    path: FacadePath,
    start: std::time::Instant,
    outcome: FacadeOutcome,
    descriptor: FacadeDescriptorSource,
}

impl FacadeRequestTiming {
    fn new(path: FacadePath) -> Self {
        Self {
            path,
            start: std::time::Instant::now(),
            // Overwritten by whichever arm actually serves the request; a
            // request that returns before then failed before choosing a source.
            outcome: FacadeOutcome::Error,
            descriptor: FacadeDescriptorSource::Unresolved,
        }
    }
}

impl Drop for FacadeRequestTiming {
    fn drop(&mut self) {
        p2p_metrics::record_facade_request(
            self.path,
            self.outcome,
            self.descriptor,
            self.start.elapsed(),
        );
    }
}

async fn handle_http_facade_result(
    state: P2pHttpFacadeState,
    request: Request,
    timing: &mut FacadeRequestTiming,
) -> std::result::Result<Response<Body>, HttpFacadeError> {
    reject_conditional_headers(request.headers())?;

    let (origin_path, origin_url) = split_origin_url(request.uri())
        .map_err(|err| HttpFacadeError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    let request_range = parse_single_range(request.headers())?;
    let len = request_range.len()?;
    let identity = resolve_origin_identity(&state, &origin_path)?;

    let (layer, descriptor_source) =
        lookup_descriptor(&state, &identity.key, &identity.canonical).await;
    timing.descriptor = descriptor_source;
    if let Some(layer) = layer {
        match fetch_p2p_range(&state, &layer, &request_range, len).await {
            Ok(range_body) => {
                timing.outcome = FacadeOutcome::P2p;
                return build_range_body_response(
                    &request_range,
                    range_body.response_len,
                    range_body.total_size,
                    range_body.body,
                );
            }
            Err(err) => {
                warn!(
                    key = %identity.key,
                    origin_url = %origin_url,
                    error = ?err,
                    "p2p layer range fetch failed; falling back to origin"
                );
            }
        }
    }

    let origin = fetch_origin_range_stream(
        &state.client,
        &origin_url,
        request.headers(),
        &request_range,
    )
    .await
    .map_err(|err| {
        warn!(origin_url = %origin_url, error = ?err, "origin range fetch failed");
        HttpFacadeError::new(StatusCode::BAD_GATEWAY, "failed to fetch origin range")
    })?;
    timing.outcome = FacadeOutcome::Origin;
    build_range_body_response(
        &request_range,
        origin.response_len,
        origin.total_size,
        origin.body,
    )
}

/// Derives the artifact identity of an origin URL, remembering the answer.
///
/// The derivation is a pure function of the URL path — the presigned query is
/// deliberately excluded from the canonical identity — so it is memoised on the
/// path. Validation runs before an entry is stored, which is what lets a later
/// hit skip it: the same path always yields the same scheme.
fn resolve_origin_identity(
    state: &P2pHttpFacadeState,
    origin_path: &str,
) -> std::result::Result<OriginIdentity, HttpFacadeError> {
    if let Some(identity) = state.origin_keys.get(origin_path) {
        return Ok(identity);
    }
    validate_origin_url(origin_path)?;
    let canonical = CanonicalBlobIdentity::from_origin_url(origin_path)
        .map_err(|err| HttpFacadeError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    let identity = OriginIdentity {
        key: layer_artifact_key(&canonical),
        canonical,
    };
    state
        .origin_keys
        .insert(origin_path.to_string(), identity.clone());
    Ok(identity)
}

async fn handle_uuid_facade_result(
    state: P2pHttpFacadeState,
    uuid: String,
    request: Request,
    timing: &mut FacadeRequestTiming,
) -> std::result::Result<Response<Body>, HttpFacadeError> {
    reject_conditional_headers(request.headers())?;
    let request_range = parse_single_range(request.headers())?;
    let len = request_range.len()?;
    let uuid = Uuid::parse_str(&uuid)
        .map_err(|err| HttpFacadeError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    let canonical = CanonicalBlobIdentity::from_uuid(&uuid);
    let artifact_key = layer_artifact_key(&canonical);
    let (layer, descriptor_source) = lookup_descriptor(&state, &artifact_key, &canonical).await;
    timing.descriptor = descriptor_source;
    let Some(layer) = layer else {
        return Err(HttpFacadeError::new(
            StatusCode::NOT_FOUND,
            format!("p2p uuid layer {uuid} was not found"),
        ));
    };

    let range_body = fetch_p2p_range(&state, &layer, &request_range, len)
        .await
        .map_err(|err| {
            warn!(
                key = %artifact_key,
                uuid = %uuid,
                error = ?err,
                "p2p uuid layer range fetch failed"
            );
            HttpFacadeError::new(
                StatusCode::BAD_GATEWAY,
                "failed to fetch p2p uuid layer range",
            )
        })?;
    timing.outcome = FacadeOutcome::P2p;
    build_range_body_response(
        &request_range,
        range_body.response_len,
        range_body.total_size,
        range_body.body,
    )
}

/// True for keys in a namespace no publisher can ever write.
///
/// An origin URL with no `sha256:` in its path derives an
/// `overlaybd-layer/v1/url/<hash>` key, and every publisher emits either a
/// digest key or a uuid key. Such a lookup is a guaranteed permanent miss that
/// still costs a full lookup budget, once per block read, for the life of the
/// device.
fn is_unpublishable_key(canonical: &CanonicalBlobIdentity) -> bool {
    canonical.digest.is_none() && !canonical.key.starts_with("uuid/")
}

async fn lookup_descriptor(
    state: &P2pHttpFacadeState,
    artifact_key: &P2pArtifactKey,
    canonical: &CanonicalBlobIdentity,
) -> (Option<ResolvedLayer>, FacadeDescriptorSource) {
    if is_unpublishable_key(canonical) {
        debug!(key = %artifact_key, "p2p key namespace has no publisher; skipping lookup");
        return (None, FacadeDescriptorSource::ShortCircuit);
    }
    if let Some(cached) = state.descriptor_cache.get(artifact_key) {
        return (cached, FacadeDescriptorSource::Cached);
    }
    let descriptor = match tokio::time::timeout(
        state.lookup_timeout,
        state.transport.lookup(artifact_key),
    )
    .await
    {
        Ok(Ok(descriptor)) => descriptor,
        Ok(Err(err)) => {
            debug!(key = %artifact_key, error = %err, "p2p lookup failed; caching as miss");
            None
        }
        Err(_) => {
            // Cache the timeout as a short miss, the same as a lookup error.
            //
            // Not caching it meant every subsequent block read of the same
            // layer re-paid the full lookup budget before falling back to the
            // origin. A slow or unreachable peer therefore added that budget to
            // every read rather than to the first one, which is the opposite of
            // what an acceleration path should do. The miss TTL is short, so a
            // peer that recovers is picked up again quickly.
            debug!(
                key = %artifact_key,
                timeout_ms = state.lookup_timeout.as_millis(),
                "p2p lookup timed out; caching as a short miss"
            );
            state.descriptor_cache.insert(artifact_key.clone(), None);
            return (None, FacadeDescriptorSource::Resolved);
        }
    };

    let Some(descriptor) = descriptor else {
        state.descriptor_cache.insert(artifact_key.clone(), None);
        return (None, FacadeDescriptorSource::Resolved);
    };
    // Parse and check the metadata once, here, and cache the result. Both used
    // to happen again on every range request against the cached descriptor.
    let layer = match LayerMetadata::from_descriptor(&descriptor)
        .and_then(|metadata| metadata.validate(canonical).map(|()| metadata))
    {
        Ok(metadata) => ResolvedLayer {
            descriptor,
            metadata,
        },
        Err(err) => {
            debug!(
                key = %artifact_key,
                error = %err,
                "p2p descriptor failed static metadata validation; not caching"
            );
            return (None, FacadeDescriptorSource::Resolved);
        }
    };
    state
        .descriptor_cache
        .insert(artifact_key.clone(), Some(layer.clone()));
    (Some(layer), FacadeDescriptorSource::Resolved)
}

async fn fetch_p2p_range(
    state: &P2pHttpFacadeState,
    layer: &ResolvedLayer,
    request_range: &RequestRange,
    len: usize,
) -> Result<RangeBody> {
    let metadata = &layer.metadata;
    let fetch_len = p2p_fetch_len(request_range, len, metadata.size())?;
    let bytes = tokio::time::timeout(
        state.fetch_range_timeout,
        // The streaming variant: this is a demand read on a page-fault path,
        // so the bytes have one consumer and no reason to be written down on
        // the way to it. Prefill is what wants the store, and it takes the
        // other method.
        state.transport.fetch_byte_range_streaming(
            &layer.descriptor,
            request_range.start,
            fetch_len,
        ),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "p2p range fetch timed out after {}ms",
            state.fetch_range_timeout.as_millis()
        )
    })?
    .context("fetch p2p layer range")?;
    // The timeout covers reaching first bytes, not the whole transfer: the
    // streaming path returns as soon as the connection is established and the
    // request is on the wire, and the bytes then flow through the response
    // body. A local provider behaves the same way, for the same reason.
    // Once this stream is attached to the response body, headers are committed;
    // any mid-stream P2P failure is reported through HTTP framing instead of
    // falling back to origin.
    Ok(RangeBody {
        body: Body::from_stream(bytes),
        response_len: fetch_len as u64,
        total_size: metadata.size(),
    })
}

fn p2p_fetch_len(
    request_range: &RequestRange,
    requested_len: usize,
    total_size: Option<u64>,
) -> Result<usize> {
    let Some(total) = total_size else {
        return Ok(requested_len);
    };
    if request_range.start >= total {
        bail!("requested range starts past layer end");
    }
    let available = total - request_range.start;
    let available = usize::try_from(available).unwrap_or(usize::MAX);
    Ok(requested_len.min(available))
}

async fn fetch_origin_range_stream(
    client: &Client,
    origin_url: &str,
    request_headers: &HeaderMap,
    request_range: &RequestRange,
) -> Result<OriginRangeStream> {
    let mut req = client.get(origin_url).header(
        RANGE,
        format!("bytes={}-{}", request_range.start, request_range.end),
    );
    for (name, value) in forwarded_headers(request_headers) {
        req = req.header(name, value);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("fetch origin range {origin_url}"))?;
    if resp.status() == StatusCode::OK {
        bail!("origin ignored Range request and returned 200");
    }
    if resp.status() != StatusCode::PARTIAL_CONTENT {
        let status = resp.status();
        bail!("origin returned unexpected status {status}");
    }
    let headers = resp.headers().clone();
    let content_range = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .context("origin response missing Content-Range")?;
    let parsed_range = parse_content_range(content_range).context("parse Content-Range")?;
    if parsed_range.start != request_range.start {
        bail!(
            "origin returned wrong range start: got {}, expected {}",
            parsed_range.start,
            request_range.start
        );
    }
    if parsed_range.end > request_range.end {
        bail!(
            "origin returned range past request: got {}, max {}",
            parsed_range.end,
            request_range.end
        );
    }
    let response_len = parsed_range
        .end
        .checked_sub(parsed_range.start)
        .and_then(|value| value.checked_add(1))
        .context("origin Content-Range length overflow")?;
    if let Some(content_length) = headers.get(CONTENT_LENGTH) {
        let content_length = content_length
            .to_str()
            .context("origin Content-Length is not valid UTF-8")?
            .parse::<u64>()
            .context("origin Content-Length is not a valid integer")?;
        if content_length != response_len {
            bail!(
                "origin Content-Length does not match Content-Range: got {content_length}, expected {response_len}"
            );
        }
    }

    // The body remains streaming, so the facade cannot re-count bytes without
    // buffering the full range. We advertise the Content-Range length as
    // Content-Length; truncated origins are detected by HTTP framing and the
    // downstream reader instead.
    Ok(OriginRangeStream {
        body: Body::from_stream(resp.bytes_stream()),
        response_len,
        total_size: parsed_range.total,
    })
}

fn build_range_body_response(
    request_range: &RequestRange,
    response_len: u64,
    total_size: Option<u64>,
    body: Body,
) -> std::result::Result<Response<Body>, HttpFacadeError> {
    let range_end = request_range
        .start
        .checked_add(response_len)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "empty range"))?;
    let total = total_size
        .map(|value| value.to_string())
        .unwrap_or_else(|| "*".to_string());
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_LENGTH, response_len.to_string())
        .header(
            CONTENT_RANGE,
            format!("bytes {}-{range_end}/{total}", request_range.start),
        )
        .body(body)
        .map_err(|err| HttpFacadeError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

async fn handle_publish_layer(
    State(state): State<P2pHttpFacadeState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(request): Json<PublishLayerRequest>,
) -> Response<Body> {
    match handle_publish_layer_result(state, addr, request).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn handle_publish_layer_result(
    state: P2pHttpFacadeState,
    addr: SocketAddr,
    request: PublishLayerRequest,
) -> std::result::Result<Response<Body>, HttpFacadeError> {
    // This control endpoint is intentionally localhost-only for the co-located
    // ublk daemon. Localhost is not treated as fully trusted; path allowlisting
    // plus size/digest verification below are the real publication gates.
    if !addr.ip().is_loopback() {
        return Err(HttpFacadeError::new(
            StatusCode::FORBIDDEN,
            "publish endpoint only accepts localhost clients",
        ));
    }
    validate_digest(&request.digest)?;
    let source = tokio::fs::canonicalize(&request.path)
        .await
        .map_err(|err| {
            HttpFacadeError::new(
                StatusCode::BAD_REQUEST,
                format!("invalid layer path {}: {err}", request.path.display()),
            )
        })?;
    validate_publish_path(&source, &state.allowed_publish_roots)?;

    let metadata = tokio::fs::metadata(&source)
        .await
        .map_err(|err| HttpFacadeError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    if !metadata.is_file() {
        return Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            "publish path is not a regular file",
        ));
    }
    if metadata.len() != request.size {
        return Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "layer size mismatch: got {}, expected {}",
                metadata.len(),
                request.size
            ),
        ));
    }
    let described = digest::FileDigest::describe(&source).await.map_err(|err| {
        HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            format!("hash layer file {}: {err}", source.display()),
        )
    })?;
    // The daemon verifies downloaded layers before requesting publication, but
    // the facade re-hashes the file because localhost is only a process
    // boundary, not a trust boundary.
    if described.size != request.size || described.sha256 != request.digest {
        return Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            "layer digest mismatch",
        ));
    }

    let digest = request.digest.clone();
    let canonical = CanonicalBlobIdentity::from_digest(&digest);
    let key = layer_artifact_key(&canonical);
    let source_url_hash = request
        .source_url
        .as_deref()
        .map(|source_url| digest::sha256_hex(source_url.as_bytes()));
    let layer_metadata = LayerMetadata::from_digest(digest, Some(request.size), source_url_hash);
    // Reference mode leaves the bytes where they are and hands the transport
    // the path, which is only safe under a root whose files outlive the
    // advertisement. The commit store is such a root: its entries are held by
    // the image cache's own GC. Anything else is copied, because a root that
    // evicts — a background-download cache clears its entries with
    // `remove_dir_all` — would leave peers resolving a descriptor pointing at
    // nothing.
    let (publish_mode, owner) = if path_is_under(&source, &state.commit_store_roots) {
        (P2pPublishMode::Reference, P2pPublishOwner::ImageCache)
    } else {
        (P2pPublishMode::Copy, P2pPublishOwner::Facade)
    };
    let publish = P2pPublishRequest::file(key.clone(), source)
        .with_metadata(layer_metadata.to_value())
        .with_publish_mode(publish_mode)
        .with_owner(owner);
    state.transport.publish(&publish).await.map_err(|err| {
        HttpFacadeError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("publish layer artifact failed: {err}"),
        )
    })?;
    state.descriptor_cache.remove(&key);

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("published"))
        .map_err(|err| HttpFacadeError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

fn validate_publish_path(
    path: &Path,
    allowed_roots: &[PathBuf],
) -> std::result::Result<(), HttpFacadeError> {
    if allowed_roots.is_empty() {
        return Err(HttpFacadeError::new(
            StatusCode::FORBIDDEN,
            "no publish roots are configured",
        ));
    }
    if path_is_under(path, allowed_roots) {
        return Ok(());
    }
    Err(HttpFacadeError::new(
        StatusCode::FORBIDDEN,
        "publish path is outside allowed roots",
    ))
}

fn path_is_under(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

fn forwarded_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    [AUTHORIZATION, USER_AGENT]
        .into_iter()
        .filter_map(|name| headers.get(&name).map(|value| (name, value.clone())))
        .collect()
}

fn reject_conditional_headers(headers: &HeaderMap) -> std::result::Result<(), HttpFacadeError> {
    for name in headers.keys() {
        if name.as_str().to_ascii_lowercase().starts_with("if-") {
            return Err(HttpFacadeError::new(
                StatusCode::PRECONDITION_FAILED,
                "conditional requests are not supported",
            ));
        }
    }
    Ok(())
}

/// Splits the routed request into the origin URL's path and the full URL.
///
/// The path alone is the cache key and the input to identity derivation: a
/// presigned query changes on every request and deliberately takes no part in
/// either. The full URL is what an origin fallback actually fetches.
fn split_origin_url(uri: &axum::http::Uri) -> Result<(String, String)> {
    let path_and_query = uri
        .path_and_query()
        .ok_or_else(|| anyhow!("request URI missing path"))?
        .as_str();
    let raw_path = path_and_query
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(path_and_query);
    let Some(origin) = raw_path.strip_prefix(&format!("{ADDRESS_PREFIX}/")) else {
        bail!("request path must start with {ADDRESS_PREFIX}/");
    };
    if origin.is_empty() {
        bail!("missing origin url");
    }
    let full = match uri.query() {
        Some(query) => format!("{origin}?{query}"),
        None => origin.to_string(),
    };
    Ok((origin.to_string(), full))
}

/// HTTP byte range with an inclusive end offset.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RequestRange {
    start: u64,
    end: u64,
}

impl RequestRange {
    fn len(&self) -> std::result::Result<usize, HttpFacadeError> {
        let len = self
            .end
            .checked_sub(self.start)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "range is too large")
            })?;
        usize::try_from(len).map_err(|_| {
            HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "range is too large")
        })
    }
}

fn parse_single_range(headers: &HeaderMap) -> std::result::Result<RequestRange, HttpFacadeError> {
    let raw = headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| HttpFacadeError::new(StatusCode::BAD_REQUEST, "missing Range header"))?
        .trim();
    if raw.contains(',') {
        return Err(HttpFacadeError::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "multiple ranges are not supported",
        ));
    }
    let raw = raw.strip_prefix("bytes=").ok_or_else(|| {
        HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "unsupported Range unit")
    })?;
    let (start, end) = raw.split_once('-').ok_or_else(|| {
        HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid Range header")
    })?;
    if start.is_empty() || end.is_empty() {
        return Err(HttpFacadeError::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "open-ended ranges are not supported",
        ));
    }
    let start = start.parse::<u64>().map_err(|_| {
        HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range start")
    })?;
    let end = end.parse::<u64>().map_err(|_| {
        HttpFacadeError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range end")
    })?;
    if end < start {
        return Err(HttpFacadeError::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "range end is before range start",
        ));
    }
    Ok(RequestRange { start, end })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(raw: &str) -> Result<ContentRange> {
    let raw = raw
        .trim()
        .strip_prefix("bytes ")
        .context("Content-Range must use bytes unit")?;
    let (range, total) = raw
        .split_once('/')
        .context("Content-Range missing total separator")?;
    let (start, end) = range
        .split_once('-')
        .context("Content-Range missing range separator")?;
    let start = start
        .parse::<u64>()
        .context("invalid Content-Range start")?;
    let end = end.parse::<u64>().context("invalid Content-Range end")?;
    if end < start {
        bail!("Content-Range end before start");
    }
    let total = if total == "*" {
        None
    } else {
        Some(
            total
                .parse::<u64>()
                .context("invalid Content-Range total")?,
        )
    };
    Ok(ContentRange { start, end, total })
}

fn validate_origin_url(origin_url: &str) -> std::result::Result<(), HttpFacadeError> {
    let parsed = Url::parse(origin_url).map_err(|err| {
        HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            format!("invalid origin url: {err}"),
        )
    })?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        scheme => Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            format!("unsupported origin url scheme {scheme}"),
        )),
    }
}

fn validate_digest(digest: &str) -> std::result::Result<(), HttpFacadeError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            "digest must use sha256:<hex>",
        ));
    };
    if hex.len() != SHA256_HEX_LEN || !hex.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(HttpFacadeError::new(
            StatusCode::BAD_REQUEST,
            "digest must use sha256:<64 hex chars>",
        ));
    }
    Ok(())
}

struct RangeBody {
    body: Body,
    response_len: u64,
    total_size: Option<u64>,
}

struct OriginRangeStream {
    body: Body,
    response_len: u64,
    total_size: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PublishLayerRequest {
    path: PathBuf,
    digest: String,
    size: u64,
    // This may be a presigned registry URL. The facade uses it only to derive
    // source_url_hash metadata and never stores or logs the raw value.
    #[serde(default)]
    source_url: Option<String>,
}

#[derive(Debug)]
struct HttpFacadeError {
    status: StatusCode,
    message: String,
}

impl HttpFacadeError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn into_response(self) -> Response<Body> {
        let mut response = Response::new(Body::from(self.message));
        *response.status_mut() = self.status;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::{mock::MockTransport, P2pArtifactDescriptor, P2pArtifactProvider};
    use axum::extract::State as AxumState;
    use axum::http::HeaderMap as AxumHeaderMap;
    use axum::routing::get;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct OriginState {
        blob: Arc<Vec<u8>>,
        hits: Arc<AtomicUsize>,
        force_200: bool,
    }

    async fn origin_handler(
        AxumState(state): AxumState<OriginState>,
        headers: AxumHeaderMap,
    ) -> Response<Body> {
        state.hits.fetch_add(1, Ordering::Relaxed);
        if state.force_200 {
            return Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(state.blob.as_ref().clone()))
                .expect("200 response");
        }
        let (start, requested_end) = parse_origin_range(&headers).expect("range");
        let len = state.blob.len() as u64;
        if start >= len {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(CONTENT_RANGE, format!("bytes */{len}"))
                .body(Body::empty())
                .expect("416 response");
        }
        let end = requested_end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE, format!("bytes {start}-{end}/{len}"))
            .header(CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    fn parse_origin_range(headers: &AxumHeaderMap) -> Option<(u64, u64)> {
        let raw = headers.get(RANGE)?.to_str().ok()?.trim();
        let raw = raw.strip_prefix("bytes=")?;
        let (start, end) = raw.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    async fn spawn_origin(state: OriginState) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/{*path}", get(origin_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve origin");
        });
        (format!("http://{addr}"), handle)
    }

    async fn start_test_facade(
        transport: Arc<dyn P2pTransport>,
        allowed_publish_roots: Vec<PathBuf>,
    ) -> P2pHttpFacadeHandle {
        start_test_facade_with_config(
            transport,
            P2pHttpFacadeConfig {
                lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
                fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
                descriptor_cache_ttl: Duration::from_secs(60),
                descriptor_miss_cache_ttl: Duration::from_secs(60),
                descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
                commit_store_roots: allowed_publish_roots.clone(),
                allowed_publish_roots,
            },
        )
        .await
    }

    async fn start_test_facade_with_config(
        transport: Arc<dyn P2pTransport>,
        config: P2pHttpFacadeConfig,
    ) -> P2pHttpFacadeHandle {
        P2pHttpFacadeServer::start(transport, config)
            .await
            .expect("start facade")
    }

    async fn get_range(url: &str, start: u64, end: u64) -> reqwest::Response {
        try_get_range(url, start, end).await.expect("request")
    }

    async fn try_get_range(url: &str, start: u64, end: u64) -> reqwest::Result<reqwest::Response> {
        Client::new()
            .get(url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
    }

    fn layer_descriptor(key: P2pArtifactKey, digest: &str, size: u64) -> P2pArtifactDescriptor {
        P2pArtifactDescriptor {
            key,
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some("mock".to_string()),
            metadata: LayerMetadata::from_digest(digest, Some(size), None).to_value(),
        }
    }

    fn uuid_layer_descriptor(key: P2pArtifactKey, uuid: Uuid, size: u64) -> P2pArtifactDescriptor {
        P2pArtifactDescriptor {
            key,
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some("mock".to_string()),
            metadata: LayerMetadata::from_uuid(uuid, Some(size)).to_value(),
        }
    }

    #[test]
    fn origin_url_rejects_unsupported_scheme() {
        let err = validate_origin_url("file:///etc/passwd").expect_err("scheme should be rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("unsupported origin url scheme"));
    }

    #[test]
    fn route_reconstructs_origin_url_with_query_and_encoded_path() {
        let uri = "/p2p-http/https://s3.example.com/bucket/a%2Fb?X-Amz-Signature=abc"
            .parse()
            .unwrap();
        let (path, full) = split_origin_url(&uri).unwrap();
        assert_eq!(path, "https://s3.example.com/bucket/a%2Fb");
        assert_eq!(
            full,
            "https://s3.example.com/bucket/a%2Fb?X-Amz-Signature=abc"
        );
    }

    #[tokio::test]
    async fn p2p_hit_skips_origin_and_returns_exact_range() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let canonical = CanonicalBlobIdentity::from_digest(digest);
        let key = layer_artifact_key(&canonical);
        let transport = Arc::new(MockTransport::default());
        transport.descriptors.write().await.insert(
            key.clone(),
            layer_descriptor(key.clone(), digest, blob.len() as u64),
        );
        transport
            .blobs
            .write()
            .await
            .insert(key, Bytes::from(blob.clone()));

        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");
        let resp = get_range(&format!("{}/{}", facade.address(), origin_url), 100, 199).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[100..200]);
        assert_eq!(hits.load(Ordering::Relaxed), 0);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn p2p_uuid_hit_returns_exact_range_without_origin() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let canonical = CanonicalBlobIdentity::from_uuid(&uuid);
        let key = layer_artifact_key(&canonical);
        let transport = Arc::new(MockTransport::default());
        transport.descriptors.write().await.insert(
            key.clone(),
            uuid_layer_descriptor(key.clone(), uuid, blob.len() as u64),
        );
        transport
            .blobs
            .write()
            .await
            .insert(key, Bytes::from(blob.clone()));

        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let resp = get_range(&format!("{}/{}", facade.uuid_address(), uuid), 100, 199).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[100..200]);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p2p_uuid_miss_returns_not_found_without_origin_fallback() {
        let uuid = Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap();
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;

        let resp = get_range(&format!("{}/{}", facade.uuid_address(), uuid), 0, 63).await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 0);
        facade.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p2p_uuid_range_fetch_failure_returns_bad_gateway() {
        let uuid = Uuid::parse_str("33333333-4444-5555-6666-777777777777").unwrap();
        let canonical = CanonicalBlobIdentity::from_uuid(&uuid);
        let key = layer_artifact_key(&canonical);
        let transport = Arc::new(MockTransport::default());
        transport
            .descriptors
            .write()
            .await
            .insert(key.clone(), uuid_layer_descriptor(key, uuid, 4096));
        let facade = start_test_facade(transport.clone(), Vec::new()).await;

        let resp = get_range(&format!("{}/{}", facade.uuid_address(), uuid), 0, 63).await;

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn p2p_hit_clamps_range_at_layer_end() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let digest = "sha256:abababababababababababababababababababababababababababababababab";
        let canonical = CanonicalBlobIdentity::from_digest(digest);
        let key = layer_artifact_key(&canonical);
        let transport = Arc::new(MockTransport::default());
        transport.descriptors.write().await.insert(
            key.clone(),
            layer_descriptor(key.clone(), digest, blob.len() as u64),
        );
        transport
            .blobs
            .write()
            .await
            .insert(key, Bytes::from(blob.clone()));

        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");
        let resp = get_range(&format!("{}/{}", facade.address(), origin_url), 4000, 5000).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            resp.headers().get(CONTENT_RANGE).unwrap(),
            "bytes 4000-4095/4096"
        );
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[4000..4096]);
        assert_eq!(hits.load(Ordering::Relaxed), 0);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn p2p_mid_stream_error_does_not_fallback_to_origin() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let digest = "sha256:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let canonical = CanonicalBlobIdentity::from_digest(digest);
        let key = layer_artifact_key(&canonical);
        let transport = Arc::new(MockTransport::default());
        transport
            .fail_fetch_range_stream_after_first_chunk
            .store(true, Ordering::Relaxed);
        transport.descriptors.write().await.insert(
            key.clone(),
            layer_descriptor(key.clone(), digest, blob.len() as u64),
        );
        transport
            .blobs
            .write()
            .await
            .insert(key, Bytes::from(blob.clone()));

        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");
        let result = try_get_range(&format!("{}/{}", facade.address(), origin_url), 0, 2047).await;

        assert!(result.is_err());
        assert_eq!(hits.load(Ordering::Relaxed), 0);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn p2p_miss_falls_back_to_origin_without_publish() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let resp = get_range(&format!("{}/{origin}/blob", facade.address()), 0, 63).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[0..64]);
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        assert_eq!(transport.publish_count.load(Ordering::Relaxed), 0);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn descriptor_cache_avoids_repeated_lookup_on_miss() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits,
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc?sig=one");

        let first = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;
        assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
        let second = get_range(&format!("{}/{}", facade.address(), origin_url), 64, 127).await;
        assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(transport.lookup_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn descriptor_cache_retries_lookup_after_miss_ttl() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob),
            hits,
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade_with_config(
            transport.clone(),
            P2pHttpFacadeConfig {
                lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
                fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
                descriptor_cache_ttl: Duration::from_secs(60),
                descriptor_miss_cache_ttl: Duration::from_millis(10),
                descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
                allowed_publish_roots: Vec::new(),
                commit_store_roots: Vec::new(),
            },
        )
        .await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee?sig=one");

        let first = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;
        assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
        tokio::time::sleep(Duration::from_millis(25)).await;
        let second = get_range(&format!("{}/{}", facade.address(), origin_url), 64, 127).await;
        assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(transport.lookup_count.load(Ordering::Relaxed), 2);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn lookup_errors_are_cached_as_short_misses() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob),
            hits,
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        transport.fail_lookup.store(true, Ordering::Relaxed);
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff?sig=one");

        let first = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;
        assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
        let second = get_range(&format!("{}/{}", facade.address(), origin_url), 64, 127).await;
        assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(transport.lookup_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    /// A lookup timeout is cached as a short miss, like a lookup error.
    ///
    /// This previously retried on every read. The intent was that a timeout is
    /// not a definite "no" — the peer may be slow but hold the data — so the
    /// acceleration path should keep trying. In practice a layer is read many
    /// times, so an unreachable peer added the full lookup budget to *every*
    /// read before falling back to origin, which is worse than losing P2P for
    /// one short miss TTL. The miss TTL is deliberately much shorter than the
    /// hit TTL, so a peer that recovers is picked up again quickly.
    #[tokio::test]
    async fn lookup_timeouts_are_cached_as_short_misses() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport {
            lookup_delay: Some(Duration::from_millis(50)),
            ..Default::default()
        });
        let facade = start_test_facade_with_config(
            transport.clone(),
            P2pHttpFacadeConfig {
                lookup_timeout: Duration::from_millis(10),
                fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
                descriptor_cache_ttl: Duration::from_secs(60),
                descriptor_miss_cache_ttl: Duration::from_secs(60),
                descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
                allowed_publish_roots: Vec::new(),
                commit_store_roots: Vec::new(),
            },
        )
        .await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/sha256:abababababababababababababababababababababababababababababababab?sig=one");

        let first = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;
        assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
        let second = get_range(&format!("{}/{}", facade.address(), origin_url), 64, 127).await;
        assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            transport.lookup_count.load(Ordering::Relaxed),
            1,
            "the second read must not re-pay the lookup budget"
        );
        assert_eq!(
            hits.load(Ordering::Relaxed),
            2,
            "both reads served by origin"
        );
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn slow_p2p_range_fetch_falls_back_to_origin() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let canonical = CanonicalBlobIdentity::from_digest(digest);
        let key = layer_artifact_key(&canonical);
        // The delay only has to exceed the 10 ms fetch_range_timeout below.
        // 50 ms did that with a margin a loaded 224-core host closed often
        // enough to flake; an hour cannot be closed, and the timeout drops
        // the sleep, so nothing actually waits.
        let transport = Arc::new(MockTransport {
            fetch_range_delay: Some(Duration::from_secs(3600)),
            ..Default::default()
        });
        transport.descriptors.write().await.insert(
            key.clone(),
            layer_descriptor(key.clone(), digest, blob.len() as u64),
        );
        transport
            .blobs
            .write()
            .await
            .insert(key, Bytes::from(blob.clone()));

        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let facade = start_test_facade_with_config(
            transport.clone(),
            P2pHttpFacadeConfig {
                lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
                fetch_range_timeout: Duration::from_millis(10),
                descriptor_cache_ttl: Duration::from_secs(60),
                descriptor_miss_cache_ttl: Duration::from_secs(60),
                descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
                allowed_publish_roots: Vec::new(),
                commit_store_roots: Vec::new(),
            },
        )
        .await;
        let origin_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");

        let resp = get_range(&format!("{}/{}", facade.address(), origin_url), 100, 199).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[100..200]);
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        assert_eq!(transport.fetch_range_count.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn origin_200_for_range_is_rejected_and_not_published() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob),
            hits: Arc::new(AtomicUsize::new(0)),
            force_200: true,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let resp = get_range(&format!("{}/{origin}/blob", facade.address()), 0, 63).await;

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(transport.publish_count.load(Ordering::Relaxed), 0);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    #[tokio::test]
    async fn publish_layer_endpoint_validates_and_publishes_full_layer() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer_dir = temp.path().join("commits/sha256-dddd");
        tokio::fs::create_dir_all(&layer_dir).await.unwrap();
        let layer_path = layer_dir.join("overlaybd.commit");
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&layer_path, &blob).await.unwrap();
        let digest = digest::sha256_digest(&blob);
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), vec![temp.path().to_path_buf()]).await;

        let resp = Client::new()
            .post(facade.publish_address())
            .json(&PublishLayerRequest {
                path: layer_path,
                digest: digest.clone(),
                size: blob.len() as u64,
                source_url: Some("https://registry.example/blob".to_string()),
            })
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(transport.publish_count.load(Ordering::Relaxed), 1);
        let key = layer_artifact_key(&CanonicalBlobIdentity::from_digest(&digest));
        assert!(transport.descriptors.read().await.contains_key(&key));
        facade.shutdown().await.unwrap();
    }

    /// Publishing by reference hands the transport a path it reads from later,
    /// which only holds where something guarantees the file outlives the
    /// advertisement. The commit store does; an evictable cache would not, so
    /// a root outside the commit store has to be copied instead.
    #[tokio::test]
    async fn a_publish_root_outside_the_commit_store_is_copied_not_referenced() {
        let commit_store = tempfile::tempdir().expect("commit store");
        let evictable = tempfile::tempdir().expect("evictable cache");
        let layer_path = evictable.path().join("data");
        let blob = b"downloaded layer bytes".to_vec();
        tokio::fs::write(&layer_path, &blob).await.unwrap();
        let digest = digest::sha256_digest(&blob);
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade_with_config(
            transport.clone(),
            P2pHttpFacadeConfig {
                lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
                fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
                descriptor_cache_ttl: Duration::from_secs(60),
                descriptor_miss_cache_ttl: Duration::from_secs(60),
                descriptor_cache_max_entries: DEFAULT_DESCRIPTOR_CACHE_MAX_ENTRIES,
                allowed_publish_roots: vec![
                    commit_store.path().to_path_buf(),
                    evictable.path().to_path_buf(),
                ],
                commit_store_roots: vec![commit_store.path().to_path_buf()],
            },
        )
        .await;

        let resp = Client::new()
            .post(facade.publish_address())
            .json(&PublishLayerRequest {
                path: layer_path,
                digest,
                size: blob.len() as u64,
                source_url: None,
            })
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let published = transport.published.read().await;
        let request = published.first().expect("one publish");
        assert_eq!(
            request.publish_mode,
            P2pPublishMode::Copy,
            "a root the image cache does not keep alive must be copied"
        );
        assert_eq!(request.owner, P2pPublishOwner::Facade);
        drop(published);
        facade.shutdown().await.unwrap();
    }

    /// A commit-store layer is the case reference mode exists for, and it must
    /// be attributed to the image cache so the cache's own delete withdraws it.
    #[tokio::test]
    async fn a_commit_store_layer_is_published_by_reference_for_the_image_cache() {
        let commit_store = tempfile::tempdir().expect("commit store");
        let layer_path = commit_store.path().join("overlaybd.commit");
        let blob = b"commit store layer".to_vec();
        tokio::fs::write(&layer_path, &blob).await.unwrap();
        let transport = Arc::new(MockTransport::default());
        let facade =
            start_test_facade(transport.clone(), vec![commit_store.path().to_path_buf()]).await;

        let resp = Client::new()
            .post(facade.publish_address())
            .json(&PublishLayerRequest {
                path: layer_path,
                digest: digest::sha256_digest(&blob),
                size: blob.len() as u64,
                source_url: None,
            })
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let published = transport.published.read().await;
        let request = published.first().expect("one publish");
        assert_eq!(request.publish_mode, P2pPublishMode::Reference);
        assert_eq!(request.owner, P2pPublishOwner::ImageCache);
        drop(published);
        facade.shutdown().await.unwrap();
    }

    /// An origin URL with no digest in its path derives a `url/` key, and no
    /// publisher ever writes that namespace. Consulting the transport for it
    /// is a guaranteed miss paid for once per block read.
    #[tokio::test]
    async fn a_url_namespace_key_never_reaches_the_transport() {
        let blob: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        let origin_url = format!("{origin}/signed/object/path?sig=one");

        let resp = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;

        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[0..64]);
        assert_eq!(
            transport.lookup_count.load(Ordering::Relaxed),
            0,
            "a key namespace with no publisher must not be looked up"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    /// Proves the memo is consulted rather than merely populated.
    ///
    /// A memoised identity that nothing reads back is indistinguishable from
    /// no memo at all, and the saving — two URL parses, a digest scan and a
    /// key format, once per block read — is not visible in any other signal.
    /// So the cache is seeded with an answer the derivation would never
    /// produce, and the request has to come back with it.
    #[tokio::test]
    async fn a_memoised_origin_identity_is_used_instead_of_re_deriving() {
        let transport = Arc::new(MockTransport::default());
        let state = P2pHttpFacadeState {
            transport,
            client: Client::new(),
            lookup_timeout: DEFAULT_LOOKUP_TIMEOUT,
            fetch_range_timeout: DEFAULT_FETCH_RANGE_TIMEOUT,
            descriptor_cache: DescriptorCache::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                16,
            ),
            origin_keys: OriginKeyCache::new(16),
            allowed_publish_roots: Arc::new(Vec::new()),
            commit_store_roots: Arc::new(Vec::new()),
        };
        let origin_path = "https://example.com/v2/ns/repo/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        state.origin_keys.insert(
            origin_path.to_string(),
            OriginIdentity {
                canonical: CanonicalBlobIdentity::from_digest("sha256:memoised"),
                key: "memoised-key".to_string(),
            },
        );

        let identity = resolve_origin_identity(&state, origin_path).expect("identity");

        assert_eq!(
            identity.key, "memoised-key",
            "the memo must be read back, not re-derived"
        );
    }

    /// The presigned query changes on every request and deliberately takes no
    /// part in the identity, so a second range on the same layer resolves the
    /// same key and reuses the same descriptor.
    #[tokio::test]
    async fn repeat_ranges_reuse_the_derived_origin_identity() {
        let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let digest = "sha256:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        let hits = Arc::new(AtomicUsize::new(0));
        let (origin, origin_handle) = spawn_origin(OriginState {
            blob: Arc::new(blob.clone()),
            hits: hits.clone(),
            force_200: false,
        })
        .await;
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), Vec::new()).await;
        // A presigned query that changes between requests, which the canonical
        // identity must ignore for the memo to hit at all.
        let first_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");
        let second_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=two");

        get_range(&format!("{}/{}", facade.address(), first_url), 0, 63).await;
        get_range(&format!("{}/{}", facade.address(), second_url), 64, 127).await;

        assert_eq!(
            transport.lookup_count.load(Ordering::Relaxed),
            1,
            "the second range must reuse the cached descriptor"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 2);
        facade.shutdown().await.unwrap();
        origin_handle.abort();
    }

    /// Captures the label sets a metric was recorded under, so a test can
    /// assert what an operator would actually see. Installed per thread, which
    /// is why the scenario below drives a current-thread runtime: every task it
    /// spawns is then polled on the thread holding the recorder.
    #[derive(Default, Clone)]
    struct MetricSpy {
        observed: Arc<std::sync::Mutex<Vec<String>>>,
    }

    struct SpyHistogram {
        rendered: String,
        observed: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl metrics::HistogramFn for SpyHistogram {
        fn record(&self, _value: f64) {
            self.observed.lock().unwrap().push(self.rendered.clone());
        }
    }

    fn render_metric_key(key: &metrics::Key) -> String {
        let mut labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();
        labels.sort();
        format!("{} {}", key.name(), labels.join(" "))
    }

    impl metrics::Recorder for MetricSpy {
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
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::noop()
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
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::from_arc(Arc::new(SpyHistogram {
                rendered: render_metric_key(key),
                observed: Arc::clone(&self.observed),
            }))
        }
    }

    /// Turning P2P on must not turn a peer that cannot answer into an error.
    /// Nothing in the existing instrumentation could tell a peer hit from a
    /// fallback to origin, so this property had to be argued rather than read.
    #[test]
    fn a_failing_peer_is_recorded_as_an_origin_fallback() {
        let spy = MetricSpy::default();
        let observed = Arc::clone(&spy.observed);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        metrics::with_local_recorder(&spy, || {
            runtime.block_on(async {
                let blob: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
                let digest =
                    "sha256:dededededededededededededededededededededededededededededededede";
                let hits = Arc::new(AtomicUsize::new(0));
                let (origin, origin_handle) = spawn_origin(OriginState {
                    blob: Arc::new(blob.clone()),
                    hits: hits.clone(),
                    force_200: false,
                })
                .await;
                let transport = Arc::new(MockTransport::default());
                transport.fail_lookup.store(true, Ordering::Relaxed);
                let facade = start_test_facade(transport.clone(), Vec::new()).await;
                let origin_url = format!("{origin}/v2/ns/repo/blobs/{digest}?sig=one");

                let resp = get_range(&format!("{}/{}", facade.address(), origin_url), 0, 63).await;

                assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
                assert_eq!(resp.bytes().await.unwrap().as_ref(), &blob[0..64]);
                assert_eq!(hits.load(Ordering::Relaxed), 1);
                facade.shutdown().await.unwrap();
                origin_handle.abort();
            })
        });

        let observed = observed.lock().unwrap();
        assert!(
            observed.iter().any(|rendered| rendered
                == "agentenv_p2p_facade_request_duration_seconds descriptor=resolved outcome=origin path=http"),
            "a peer that cannot answer must be recorded as an origin fallback, saw {observed:?}"
        );
        assert!(
            !observed
                .iter()
                .any(|rendered| rendered.contains("outcome=error")),
            "a P2P failure that the origin covered is not an error, saw {observed:?}"
        );
    }

    #[tokio::test]
    async fn publish_layer_endpoint_rejects_path_outside_allowed_roots() {
        let allowed = tempfile::tempdir().expect("allowed");
        let outside = tempfile::tempdir().expect("outside");
        let layer_path = outside.path().join("overlaybd.commit");
        let blob = b"outside".to_vec();
        tokio::fs::write(&layer_path, &blob).await.unwrap();
        let transport = Arc::new(MockTransport::default());
        let facade = start_test_facade(transport.clone(), vec![allowed.path().to_path_buf()]).await;

        let resp = Client::new()
            .post(facade.publish_address())
            .json(&PublishLayerRequest {
                path: layer_path,
                digest: digest::sha256_digest(&blob),
                size: blob.len() as u64,
                source_url: None,
            })
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(transport.publish_count.load(Ordering::Relaxed), 0);
        facade.shutdown().await.unwrap();
    }
}
