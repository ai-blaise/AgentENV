use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("P2P artifact transport is disabled")]
    TransportDisabled,

    #[error("invalid P2P artifact descriptor: {reason}")]
    InvalidDescriptor { reason: String },

    #[error("invalid P2P artifact catalog at {path:?}: {reason}")]
    InvalidCatalog { path: PathBuf, reason: String },

    #[error("P2P operation timed out: {operation}")]
    Timeout { operation: &'static str },

    /// A peer offered more bytes than the caller is willing to hold.
    ///
    /// Nothing about a descriptor is trustworthy before the artifact it names
    /// has been opened and authenticated, and the size is no exception: it is
    /// whatever the peer chooses to send. The limit is the caller's, applied
    /// as the bytes arrive rather than to a number the peer supplied.
    #[error("P2P artifact is larger than the {limit}-byte limit for this fetch")]
    ArtifactTooLarge { limit: u64 },

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl Error {
    pub(crate) fn internal_message(
        operation: &'static str,
        source: impl std::fmt::Display,
    ) -> Self {
        Self::Internal(anyhow::anyhow!("{operation}: {source}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
