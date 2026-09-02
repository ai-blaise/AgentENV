use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::cfg::P2pConfig;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    Disabled,
    Iroh,
}

impl P2pTransportKind {
    pub(crate) fn backend_id(self) -> Option<&'static str> {
        match self {
            Self::Disabled => None,
            Self::Iroh => Some(super::iroh::IROH_BACKEND_ID),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedP2pConfig {
    pub transport: P2pTransportKind,
    pub store_dir: PathBuf,
    pub listen_addr: Option<String>,
    pub lookup_timeout: Duration,
    pub fetch_timeout: Duration,
    pub peer_discovery_refresh_interval: Duration,
    /// How often this node re-announces its published artifacts.
    pub reannounce_interval: Duration,
    /// How long an idle pooled catalog connection is kept.
    pub catalog_connection_idle: Duration,
    /// Peers the catalog pool may hold connections to; zero disables pooling.
    pub catalog_max_connections: usize,
    /// Per-peer bound on one catalog lookup.
    pub catalog_lookup_timeout: Duration,
    /// CIDRs a peer endpoint address may name; empty accepts any address.
    pub cluster_cidrs: Vec<String>,
}

impl ResolvedP2pConfig {
    pub(crate) fn from_config(p2p: &P2pConfig) -> Self {
        let transport = if p2p.enabled {
            p2p.transport
        } else {
            P2pTransportKind::Disabled
        };

        Self {
            transport,
            store_dir: p2p.store_dir.clone(),
            listen_addr: Some(str::trim(p2p.listen_addr.as_str()))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            reannounce_interval: Duration::from_secs(p2p.reannounce_interval_secs),
            lookup_timeout: Duration::from_millis(p2p.lookup_timeout_ms),
            fetch_timeout: Duration::from_millis(p2p.fetch_timeout_ms),
            peer_discovery_refresh_interval: Duration::from_secs(
                p2p.peer_discovery_refresh_interval_secs,
            )
            .max(Duration::from_secs(1)),
            catalog_connection_idle: Duration::from_secs(p2p.catalog_connection_idle_secs),
            catalog_max_connections: p2p.catalog_max_connections,
            catalog_lookup_timeout: Duration::from_millis(p2p.catalog_lookup_timeout_ms),
            cluster_cidrs: p2p.cluster_cidrs.clone(),
        }
    }
}
