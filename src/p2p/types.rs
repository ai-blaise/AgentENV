use std::fmt;
use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Stable lookup identity for an artifact in the project-wide P2P catalog.
///
/// Producers encode the full cache context into this string and validate the
/// returned descriptor metadata before trusting fetched bytes.
pub type P2pArtifactKey = String;

/// Catalog entry returned by lookup and used by fetch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct P2pArtifactDescriptor {
    /// Stable artifact lookup key.
    pub key: P2pArtifactKey,
    /// Providers that can serve this artifact.
    pub providers: Vec<P2pArtifactProvider>,
    /// Transport-specific locator used by the matching backend to fetch bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_locator: Option<String>,
    /// Module-defined metadata used to validate and interpret the artifact.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Provider entry in a P2P artifact descriptor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum P2pArtifactProvider {
    /// Placeholder for the node that owns the local catalog entry.
    Local,
    /// Concrete provider safe to send to other nodes.
    Peer(P2pPeer),
}

impl P2pArtifactProvider {
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }
}

impl From<P2pPeer> for P2pArtifactProvider {
    fn from(peer: P2pPeer) -> Self {
        Self::Peer(peer)
    }
}

/// Local artifact source to publish into the P2P catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pPublishSource {
    /// Path to local artifact bytes.
    Path(PathBuf),
    /// In-memory artifact bytes.
    Bytes(Bytes),
}

impl fmt::Display for P2pPublishSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{}", path.display()),
            Self::Bytes(bytes) => write!(f, "<{} bytes>", bytes.len()),
        }
    }
}

/// Local artifact plus descriptor to publish into the P2P catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pPublishRequest {
    /// Stable artifact lookup key.
    pub key: P2pArtifactKey,
    /// Artifact source.
    pub source: P2pPublishSource,
    /// Module-defined metadata advertised with the descriptor.
    pub metadata: serde_json::Value,
    /// Hint for how the transport should retain/import a path-backed local artifact.
    pub publish_mode: P2pPublishMode,
    /// Which subsystem's retention this publication belongs to.
    pub owner: P2pPublishOwner,
}

impl P2pPublishRequest {
    pub fn file(key: impl Into<P2pArtifactKey>, source: impl Into<PathBuf>) -> Self {
        Self {
            key: key.into(),
            source: P2pPublishSource::Path(source.into()),
            metadata: serde_json::Value::Null,
            publish_mode: P2pPublishMode::default(),
            owner: P2pPublishOwner::default(),
        }
    }

    pub fn bytes(key: impl Into<P2pArtifactKey>, bytes: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            source: P2pPublishSource::Bytes(bytes.into()),
            metadata: serde_json::Value::Null,
            publish_mode: P2pPublishMode::Copy,
            owner: P2pPublishOwner::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_publish_mode(mut self, publish_mode: P2pPublishMode) -> Self {
        self.publish_mode = publish_mode;
        self
    }

    pub fn with_owner(mut self, owner: P2pPublishOwner) -> Self {
        self.owner = owner;
        self
    }
}

/// Which subsystem's retention a publication belongs to.
///
/// The `overlaybd-layer/v1/sha256:<digest>` namespace has two independent
/// publishers — the image cache, when a layer lands in the commit store, and
/// snapshot publication, for every lower of a committed chain — and each has
/// its own removal edge. Retention is per key, so without an owner the first
/// removal to run would drop the shared entry and hand the collector a blob
/// the other publisher is still advertising. An artifact is withdrawn once the
/// last owner releases it.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum P2pPublishOwner {
    /// Publishers that have exactly one publish and one unpublish edge and so
    /// need no scoping of their own. Also what a catalog written before
    /// ownership existed resolves to.
    #[default]
    Unscoped,
    /// The local image cache's commit store.
    ImageCache,
    /// The overlaybd facade's control endpoint.
    Facade,
}

impl P2pPublishOwner {
    /// Stable, bounded label for metrics and for the persisted owner set.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unscoped => "unscoped",
            Self::ImageCache => "image_cache",
            Self::Facade => "facade",
        }
    }
}

/// Import/retention hint for publish.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum P2pPublishMode {
    /// Transport may copy bytes into its own store.
    #[default]
    Copy,
    /// Transport may retain or index the existing local file when supported.
    Reference,
}

/// Serializable address for a node's artifact transport endpoint.
///
/// The scheduler stores this opaquely and returns it to other nodes; only the
/// matching transport backend is expected to parse `address`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct P2pEndpoint {
    /// Backend that understands `address`, currently `iroh`.
    pub backend: String,
    /// Backend-specific serialized endpoint address.
    pub address: String,
}

/// Candidate peer returned by discovery and used by transport backends.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct P2pPeer {
    /// AgentENV node ID from scheduler/observability.
    pub node_id: String,
    /// Artifact transport endpoint advertised by that node.
    pub endpoint: P2pEndpoint,
}

/// Optional provider information supplied by a module that already has
/// artifact locality context outside of scheduler discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct P2pArtifactProviderHint {
    /// AgentENV node ID if the producer is known.
    pub node_id: Option<String>,
    /// Transport endpoint if the producer endpoint is known.
    pub endpoint: Option<P2pEndpoint>,
}
