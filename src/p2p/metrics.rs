//! Instrumentation for the artifact transport.
//!
//! Nothing under `src/p2p/` or `src/overlaybd/p2p/` was instrumented at all,
//! and the one adjacent family — `agentenv_overlaybd_remote_read_*` — derives
//! its `source` label from whether an accelerate address is configured, so it
//! reports the same value for a peer hit and for a facade fallback to origin.
//! No existing metric could tell whether P2P was serving anything, which is
//! why every claim about this subsystem had to be argued from code rather than
//! measured.
//!
//! Every label here is drawn from a closed set. Artifact keys embed a digest
//! and endpoints embed a node identity, so neither may appear in a label; the
//! key contributes only its namespace.

use crate::p2p::{P2pArtifactKey, P2pPublishOwner};

/// Namespace of an artifact key, as a bounded label.
///
/// Callers name keys by concatenating a namespace with an identity, so the
/// namespace is the only part of a key with a fixed cardinality.
pub(crate) fn key_class(key: &P2pArtifactKey) -> &'static str {
    if key.starts_with("overlaybd-layer/v1/uuid/") {
        "overlaybd_layer_uuid"
    } else if key.starts_with("overlaybd-layer/v1/url/") {
        "overlaybd_layer_url"
    } else if key.starts_with("overlaybd-layer/v1/") {
        "overlaybd_layer"
    } else if key.starts_with("snapshot/v1/") {
        "snapshot_fixed"
    } else {
        "other"
    }
}

/// Outcome of a publish attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishStatus {
    Published,
    Failed,
    /// The publish queue was full, so the advertisement was abandoned rather
    /// than allowed to stall the operation that produced the artifact.
    Dropped,
    /// The path was outside every root this node may advertise from.
    Refused,
}

impl PublishStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Failed => "failed",
            Self::Dropped => "dropped",
            Self::Refused => "refused",
        }
    }
}

pub(crate) fn record_publish(key: &P2pArtifactKey, source: P2pPublishOwner, status: PublishStatus) {
    metrics::counter!(
        "agentenv_p2p_publish_total",
        "key_class" => key_class(key),
        "source" => source.as_str(),
        "status" => status.as_str(),
    )
    .increment(1);
}

/// Outcome of releasing one owner's claim on an artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnpublishStatus {
    /// The last owner released it and the artifact was withdrawn.
    Withdrawn,
    /// Another owner still holds it, so it stays advertised.
    Retained,
    /// No publication existed for the key.
    Absent,
    Failed,
}

impl UnpublishStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Withdrawn => "withdrawn",
            Self::Retained => "retained",
            Self::Absent => "absent",
            Self::Failed => "failed",
        }
    }
}

pub(crate) fn record_unpublish(key: &P2pArtifactKey, status: UnpublishStatus) {
    metrics::counter!(
        "agentenv_p2p_unpublish_total",
        "key_class" => key_class(key),
        "status" => status.as_str(),
    )
    .increment(1);
}

/// On-disk size of the transport's own blob store.
///
/// The store grows on publish and on every fetched blob, and shrinks only when
/// the gated collector sweeps, so this is the series that shows whether
/// retention converges over a create/snapshot/delete loop.
pub(crate) fn set_store_bytes(bytes: u64) {
    metrics::gauge!("agentenv_p2p_store_bytes").set(bytes as f64);
}

/// Outcome of a catalog lookup against one peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupResult {
    Hit,
    Miss,
    Timeout,
    Error,
}

impl LookupResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

/// Whether a lookup rode a pooled connection or paid for a new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupConnection {
    New,
    Reused,
}

impl LookupConnection {
    fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Reused => "reused",
        }
    }
}

pub(crate) fn record_catalog_lookup(
    result: LookupResult,
    connection: LookupConnection,
    elapsed: std::time::Duration,
) {
    metrics::histogram!(
        "agentenv_p2p_catalog_lookup_duration_seconds",
        "result" => result.as_str(),
        "conn" => connection.as_str(),
    )
    .record(elapsed.as_secs_f64());
}

/// A catalog connection this node actually dialled.
///
/// Counted rather than gauged: the pool owns connection lifetime and exposes
/// no close hook, so the live count is not observable from here. What the
/// pooling change has to prove is that N lookups against one peer dial once,
/// and a counter states that directly.
pub(crate) fn record_catalog_connection_established() {
    metrics::counter!("agentenv_p2p_catalog_connections_total").increment(1);
}

/// What the facade's descriptor cache did with a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DescriptorCacheResult {
    /// A cached descriptor was returned.
    Hit,
    /// A cached negative was returned, so no lookup was made.
    Miss,
    /// Nothing usable was cached; the caller has to resolve.
    Absent,
    Insert,
}

impl DescriptorCacheResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Absent => "absent",
            Self::Insert => "insert",
        }
    }
}

pub(crate) fn record_descriptor_cache(result: DescriptorCacheResult) {
    metrics::counter!(
        "agentenv_p2p_descriptor_cache_total",
        "result" => result.as_str(),
    )
    .increment(1);
}

/// Which facade route served a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FacadePath {
    Http,
    Uuid,
}

impl FacadePath {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Uuid => "uuid",
        }
    }
}

/// Where a facade request's bytes came from.
///
/// `origin` is the label that has to be readable as normal: a peer that cannot
/// answer is a fallback, not a failure, and nothing in the existing
/// instrumentation could tell the two apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FacadeOutcome {
    P2p,
    Origin,
    Error,
}

impl FacadeOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::P2p => "p2p",
            Self::Origin => "origin",
            Self::Error => "error",
        }
    }
}

/// How the request obtained the descriptor it used, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FacadeDescriptorSource {
    /// The request never got as far as needing one.
    Unresolved,
    /// Served out of the facade's descriptor cache.
    Cached,
    /// Resolved through the transport.
    Resolved,
    /// Refused before the transport, because no publisher can ever write the
    /// key namespace the request derived.
    ShortCircuit,
}

impl FacadeDescriptorSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unresolved => "unresolved",
            Self::Cached => "cached",
            Self::Resolved => "resolved",
            Self::ShortCircuit => "shortcircuit",
        }
    }
}

pub(crate) fn record_facade_request(
    path: FacadePath,
    outcome: FacadeOutcome,
    descriptor: FacadeDescriptorSource,
    elapsed: std::time::Duration,
) {
    metrics::histogram!(
        "agentenv_p2p_facade_request_duration_seconds",
        "path" => path.as_str(),
        "outcome" => outcome.as_str(),
        "descriptor" => descriptor.as_str(),
    )
    .record(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_class_reports_the_namespace_and_never_the_identity() {
        assert_eq!(
            key_class(&"overlaybd-layer/v1/sha256:abc".to_string()),
            "overlaybd_layer"
        );
        assert_eq!(
            key_class(&"overlaybd-layer/v1/uuid/1-2-3".to_string()),
            "overlaybd_layer_uuid"
        );
        assert_eq!(
            key_class(&"overlaybd-layer/v1/url/deadbeef".to_string()),
            "overlaybd_layer_url"
        );
        assert_eq!(
            key_class(&"snapshot/v1/artifacts/s1/vm_state".to_string()),
            "snapshot_fixed"
        );
        assert_eq!(key_class(&"something/else".to_string()), "other");
    }
}
