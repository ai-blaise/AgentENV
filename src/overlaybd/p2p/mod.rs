mod artifact;
mod cache;
mod facade;
mod publish_sink;

use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

use crate::{
    cfg::AppConfig, image::cache::local_image_services_from_app_config, p2p::P2pTransport,
};
pub(crate) use artifact::{layer_key_from_digest, layer_key_from_uuid, LayerMetadata};
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

    pub async fn start_from_app_config(
        config: &AppConfig,
        transport: Arc<dyn P2pTransport>,
    ) -> Self {
        if !config.p2p.enabled {
            return OverlaybdP2pRuntime::disabled();
        }
        if transport.local_endpoint().is_none() {
            return OverlaybdP2pRuntime::disabled();
        }

        let overlaybd_config = &config.ublk.overlaybd;
        let mut facade_config = P2pHttpFacadeConfig {
            allowed_publish_roots: local_image_services_from_app_config(config)
                .overlaybd_layers
                .publishable_roots(),
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
            local_image_services_from_app_config(config)
                .overlaybd_layers
                .publishable_roots(),
        )));

        match start_http_facade_with_config(transport, facade_config).await {
            Ok(facade) => {
                info!(
                    address = %facade.address(),
                    uuid_address = %facade.uuid_address(),
                    publish_address = %facade.publish_address(),
                    "enabled p2p http facade for overlaybd"
                );
                OverlaybdP2pRuntime {
                    facade: Some(facade),
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to start p2p http facade; continuing with overlaybd p2p disabled");
                OverlaybdP2pRuntime::disabled()
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
