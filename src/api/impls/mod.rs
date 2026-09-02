mod admin;
mod attached_drives;
pub(crate) mod auth;
mod pagination;
mod sandbox;
mod snapshots;
mod template;
mod template_helpers;

use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;

use super::proxy::{build_proxy_client, ProxyClient};
use crate::api_key::ApiKey;
use crate::image::ImageResolver;
use crate::observability::ObservabilityService;
#[cfg(test)]
use crate::orchestrator::AdmissionRejectReason;
use crate::orchestrator::{Orchestrator, OrchestratorError};
use crate::snapshot::repository::RepositoryError;
use crate::snapshot::SnapshotManager;
use crate::template::TemplateBuilder;
use agentenv_http_server::{apis, models};

#[derive(Clone, Debug)]
pub struct Claims;

#[derive(Clone)]
pub struct ApiImpl {
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    observability: Option<Arc<ObservabilityService>>,
    proxy_client: ProxyClient,
    sandbox_proxy_domains: Vec<String>,
    api_key: ApiKey,
    proxy_timeouts: crate::api::proxy::ProxyTimeouts,
    /// A capacity refusal to answer creates with, in place of the node's own
    /// admission gate.
    ///
    /// That gate is a process-wide singleton built from global config, so no
    /// test can put a node at its real limits. The wire shape of this refusal
    /// is a contract the gateway keys its create retries on, and only a
    /// response the node itself produced can pin it.
    #[cfg(test)]
    forced_admission_refusal: Option<(AdmissionRejectReason, std::time::Duration)>,
}

impl ApiImpl {
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        snapshot_manager: Arc<SnapshotManager>,
        template_builder: Arc<TemplateBuilder>,
        image_resolver: Arc<ImageResolver>,
        observability: Option<Arc<ObservabilityService>>,
        sandbox_proxy_domains: Vec<String>,
        api_key: ApiKey,
    ) -> Self {
        let proxy_timeouts = crate::api::proxy::ProxyTimeouts::default();
        Self {
            orchestrator,
            snapshot_manager,
            template_builder,
            image_resolver,
            observability,
            proxy_client: build_proxy_client(proxy_timeouts.connect),
            sandbox_proxy_domains,
            api_key,
            proxy_timeouts,
            #[cfg(test)]
            forced_admission_refusal: None,
        }
    }

    /// Makes creates answer with the capacity refusal `reason` describes.
    #[cfg(test)]
    pub(crate) fn refusing_creates(
        mut self,
        reason: AdmissionRejectReason,
        retry_after: std::time::Duration,
    ) -> Self {
        self.forced_admission_refusal = Some((reason, retry_after));
        self
    }

    /// The refusal a create should answer with without asking the orchestrator.
    fn forced_admission_refusal(&self) -> Option<OrchestratorError> {
        #[cfg(test)]
        {
            self.forced_admission_refusal.map(|(reason, retry_after)| {
                OrchestratorError::AdmissionRejected {
                    reason,
                    retry_after,
                }
            })
        }
        #[cfg(not(test))]
        {
            None
        }
    }

    /// Replaces how long proxied requests wait on an upstream.
    ///
    /// Only tests that are about the waiting itself should need this: the
    /// shipped values are the ones a sandbox behind a slow upstream depends
    /// on, and shortening them anywhere else turns ordinary traffic into a
    /// race.
    #[cfg(test)]
    pub(crate) fn with_proxy_timeouts(
        mut self,
        proxy_timeouts: crate::api::proxy::ProxyTimeouts,
    ) -> Self {
        // The connect deadline lives in the connector, so it is fixed when the
        // client is built rather than read per request.
        self.proxy_client = build_proxy_client(proxy_timeouts.connect);
        self.proxy_timeouts = proxy_timeouts;
        self
    }

    pub(crate) fn proxy_timeouts(&self) -> crate::api::proxy::ProxyTimeouts {
        self.proxy_timeouts
    }

    pub(crate) fn orchestrator(&self) -> Arc<Orchestrator> {
        Arc::clone(&self.orchestrator)
    }

    pub(crate) fn proxy_client(&self) -> &ProxyClient {
        &self.proxy_client
    }

    pub(crate) fn sandbox_proxy_domains(&self) -> &[String] {
        &self.sandbox_proxy_domains
    }

    pub(crate) fn image_resolver(&self) -> Arc<ImageResolver> {
        Arc::clone(&self.image_resolver)
    }

    /// Returns the optional observability service backing node/admin
    /// observability endpoints. This is `None` when the server is configured
    /// with `observability.enabled = false`.
    pub(crate) fn observability(&self) -> Option<Arc<ObservabilityService>> {
        self.observability.as_ref().map(Arc::clone)
    }

    fn error(code: i32, message: impl Into<String>) -> models::Error {
        models::Error::new(code, message.into())
    }

    fn internal_error(err: &dyn std::error::Error) -> models::Error {
        let mut message = err.to_string();
        let mut current = err.source();
        while let Some(source) = current {
            let cause = source.to_string();
            if cause != message {
                message.push_str(": ");
                message.push_str(&cause);
            }
            current = source.source();
        }
        Self::error(500, message)
    }

    /// Maps an orchestrator failure to the body of a 500.
    ///
    /// `models::Error::from` picks a code from what the failure means, which is
    /// right wherever a matching response variant exists — and wrong the moment
    /// the only variant left is the 500, because the body would then contradict
    /// the status the client reads it under.
    fn server_error(err: OrchestratorError) -> models::Error {
        let mut error = models::Error::from(err);
        error.code = 500;
        error
    }

    fn repository_error(err: &RepositoryError) -> models::Error {
        match err {
            RepositoryError::InvalidRequest { .. } => Self::error(400, err.to_string()),
            RepositoryError::SnapshotNotFound { .. }
            | RepositoryError::AliasNotFound { .. }
            | RepositoryError::ArtifactNotFound { .. }
            | RepositoryError::ManagedLayerNotFound { .. } => Self::error(404, err.to_string()),
            RepositoryError::AliasConflict { .. } | RepositoryError::IntegrityMismatch { .. } => {
                Self::error(409, err.to_string())
            }
            RepositoryError::Unsupported { .. } => Self::error(500, err.to_string()),
            RepositoryError::Backend { .. } => Self::internal_error(err),
        }
    }

    fn snapshot_manager_error(err: &AnyhowError) -> models::Error {
        if let Some(repo_err) = err
            .chain()
            .find_map(|e| e.downcast_ref::<RepositoryError>())
        {
            Self::repository_error(repo_err)
        } else {
            Self::internal_error(err.as_ref())
        }
    }

    /// Maps repository errors produced by build/publish flows to a client-facing
    /// error. `AliasConflict`, `InvalidRequest`, and `IntegrityMismatch` are
    /// treated as client-side input problems and returned as 400; any other
    /// variant falls back to `None` so the caller can choose a 500 default.
    fn bad_request_for_repository_build_error(err: &RepositoryError) -> Option<models::Error> {
        match err {
            RepositoryError::InvalidRequest { .. }
            | RepositoryError::AliasConflict { .. }
            | RepositoryError::IntegrityMismatch { .. } => Some(Self::error(400, err.to_string())),
            _ => None,
        }
    }

    /// Dispatches a built-up `models::Error` into either a 400 or 500 response
    /// variant based on its `code`. Keeps build/publish endpoints consistent
    /// across templates and snapshots.
    fn client_or_server_response<R>(
        err: models::Error,
        bad_request: impl FnOnce(models::Error) -> R,
        server_error: impl FnOnce(models::Error) -> R,
    ) -> R {
        if err.code == 400 {
            bad_request(err)
        } else {
            server_error(err)
        }
    }
}

#[async_trait]
impl apis::ErrorHandler<()> for ApiImpl {}

#[async_trait]
impl apis::default::Default<()> for ApiImpl {
    async fn health_get(
        &self,
        _method: &http::Method,
        _host: &headers::Host,
        _cookies: &axum_extra::extract::CookieJar,
    ) -> Result<apis::default::HealthGetResponse, ()> {
        Ok(apis::default::HealthGetResponse::Status204_TheServiceIsHealthy)
    }
}
