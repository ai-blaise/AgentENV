mod artifact;
mod cache;
mod facade;
mod publish_sink;

use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

use crate::{
    cfg::AppConfig, image::cache::local_image_services_from_app_config, p2p::P2pTransport,
};
pub(crate) use artifact::{layer_key_from_digest, LayerMetadata};
use facade::{start_http_facade_with_config, P2pHttpFacadeConfig, P2pHttpFacadeHandle};
pub(crate) use publish_sink::global_layer_publish_sink;
use publish_sink::{set_global_layer_publish_sink, TransportLayerPublishSink};

#[derive(Debug)]
pub struct OverlaybdP2pRuntime {
    facade: Option<P2pHttpFacadeHandle>,
}

impl OverlaybdP2pRuntime {
    pub fn disabled() -> Self {
        Self { facade: None }
    }

    /// Starts the overlaybd P2P facade, or explains why it will not run.
    ///
    /// Failing to start is only tolerable standalone, where P2P is an
    /// optimisation and the origin serves every read. In a cluster it is a
    /// load-bearing path -- a resume pulls its memory pages from the node that
    /// paused the sandbox -- so a node that silently disabled it would come up
    /// healthy and serve every page from the origin instead, which is slower
    /// than a cold start and shows up as nothing but latency.
    pub async fn start_from_app_config(
        config: &AppConfig,
        transport: Arc<dyn P2pTransport>,
    ) -> anyhow::Result<Self> {
        if !config.p2p.enabled {
            return Ok(OverlaybdP2pRuntime::disabled());
        }
        let clustered = config.cluster.scheduler_endpoint.is_some();
        if transport.local_endpoint().is_none() {
            if clustered {
                anyhow::bail!(
                    "p2p is enabled and this node is clustered, but the transport has no local \
                     endpoint; peers cannot reach this node, so every cross-node resume would \
                     fall back to the origin"
                );
            }
            warn!("p2p transport has no local endpoint; continuing with overlaybd p2p disabled");
            return Ok(OverlaybdP2pRuntime::disabled());
        }

        let overlaybd_config = &config.ublk.overlaybd;
        // Today the only publishable root is the image cache's commit store,
        // whose files the cache keeps alive for as long as they are
        // advertised, so every root is also a commit-store root. Naming the
        // two separately is what makes adding a third root — an evictable
        // download cache, say — a copy rather than a dangling reference.
        let publishable_roots = local_image_services_from_app_config(config)
            .overlaybd_layers
            .publishable_roots();
        let mut facade_config = P2pHttpFacadeConfig {
            commit_store_roots: publishable_roots.clone(),
            allowed_publish_roots: publishable_roots.clone(),
            ..Default::default()
        };
        facade_config.lookup_timeout =
            Duration::from_millis(overlaybd_config.p2p_lookup_timeout_ms);
        facade_config.fetch_range_timeout =
            Duration::from_millis(overlaybd_config.p2p_fetch_range_timeout_ms);

        // Install the in-process publish sink so layers downloaded from a
        // registry actually enter the P2P store. Without it only snapshot
        // commits were ever advertised.
        set_global_layer_publish_sink(Arc::new(TransportLayerPublishSink::new(
            Arc::clone(&transport),
            publishable_roots,
        )));

        match start_http_facade_with_config(transport, facade_config).await {
            Ok(facade) => {
                info!(
                    address = %facade.address(),
                    uuid_address = %facade.uuid_address(),
                    publish_address = %facade.publish_address(),
                    "enabled p2p http facade for overlaybd"
                );
                Ok(OverlaybdP2pRuntime {
                    facade: Some(facade),
                })
            }
            Err(err) if clustered => Err(err.context("start p2p http facade for a clustered node")),
            Err(err) => {
                warn!(error = %err, "failed to start p2p http facade; continuing with overlaybd p2p disabled");
                Ok(OverlaybdP2pRuntime::disabled())
            }
        }
    }

    pub fn read_facade_address(&self) -> Option<&str> {
        self.facade.as_ref().map(P2pHttpFacadeHandle::address)
    }

    pub fn uuid_address(&self) -> Option<&str> {
        self.facade.as_ref().map(P2pHttpFacadeHandle::uuid_address)
    }

    pub fn publish_address(&self) -> Option<&str> {
        self.facade
            .as_ref()
            .map(P2pHttpFacadeHandle::publish_address)
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(facade) = self.facade {
            facade.shutdown().await?;
        }
        Ok(())
    }
}
