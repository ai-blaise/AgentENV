use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use crate::orchestrator::{Orchestrator, PausedStatePublisher};
use crate::snapshot::{
    SnapshotId, SnapshotManager, SnapshotPublishMetadata, SnapshotPublishSource,
};
use crate::types::SandboxId;
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

/// Publishes a paused sandbox through the node's own snapshot repository.
///
/// The drain's counterpart to `POST /sandboxes/{id}/snapshots` on a paused
/// sandbox, minus the alias: an operator names a snapshot they asked for, and
/// a drain publishes one sandbox per record with no name to collide on the
/// next drain.
struct RepositoryPausedStatePublisher {
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
}

#[async_trait]
impl PausedStatePublisher for RepositoryPausedStatePublisher {
    async fn publish_paused(&self, sandbox_id: SandboxId) -> anyhow::Result<()> {
        let (metadata, manifest) = self
            .orchestrator
            .describe_paused_snapshot(sandbox_id)
            .await?;
        let snapshot_id = SnapshotId::generate();
        self.snapshot_manager
            .publish(
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    alias: None,
                    source: SnapshotPublishSource::Sandbox {
                        source_sandbox_id: metadata.id.to_string(),
                    },
                    context: metadata.context.clone(),
                    startup: metadata.startup.clone(),
                    resources: metadata.resources,
                    runtime_versions: metadata.runtime_versions.clone(),
                    virtualization_mode: metadata.virtualization_mode,
                    image_configs: metadata.image_configs.clone(),
                    custom_extension_params: metadata.custom_extension_params.clone(),
                },
                manifest,
            )
            .await?;

        // After the publish, never before: a record that named a snapshot the
        // repository does not hold would offer a destination a sandbox it
        // cannot restore.
        self.orchestrator
            .mark_paused_snapshot_committed(sandbox_id, &snapshot_id)
            .await;
        Ok(())
    }
}

impl From<MachineInfo> for models::MachineInfo {
    fn from(machine_info: MachineInfo) -> Self {
        models::MachineInfo::new(
            machine_info.cpu_family,
            machine_info.cpu_model,
            machine_info.cpu_model_name,
            machine_info.cpu_architecture,
        )
    }
}

impl From<DiskMetric> for models::DiskMetrics {
    fn from(disk: DiskMetric) -> Self {
        models::DiskMetrics::new(
            disk.mount_point,
            disk.device,
            disk.filesystem_type,
            disk.used_bytes,
            disk.total_bytes,
        )
    }
}

impl From<NodeMetricsSnapshot> for models::NodeMetrics {
    fn from(metrics: NodeMetricsSnapshot) -> Self {
        models::NodeMetrics::new(
            metrics.allocated_cpu,
            metrics.cpu_percent,
            metrics.cpu_count,
            metrics.allocated_memory_bytes,
            metrics.memory_used_bytes,
            metrics.memory_total_bytes,
            metrics
                .disks
                .into_iter()
                .map(models::DiskMetrics::from)
                .collect(),
            metrics.paused_allocated_cpu,
            metrics.paused_allocated_memory_bytes,
        )
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
        models::Node::new(
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.cluster_id.to_string(),
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.sandbox_starting_count,
            node.paused_sandbox_count,
        )
    }
}

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    async fn admin_drain_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &Option<models::DrainRequest>,
    ) -> Result<AdminDrainPostResponse, ()> {
        let config = self.drain_config();
        let deadline_ms = body
            .as_ref()
            .and_then(|request| request.deadline_ms)
            .unwrap_or(config.deadline_ms);
        if deadline_ms == 0 {
            // A zero deadline reads as "do nothing", and answering it with a
            // pass that pauses nothing but has already closed the node to new
            // work is the worst of both.
            return Ok(AdminDrainPostResponse::Status400_BadRequest(Self::error(
                400,
                "deadlineMs must be greater than zero",
            )));
        }

        let publisher = RepositoryPausedStatePublisher {
            orchestrator: self.orchestrator(),
            snapshot_manager: Arc::clone(&self.snapshot_manager),
        };
        match self
            .orchestrator
            .drain(
                Duration::from_millis(deadline_ms),
                config.concurrency,
                &publisher,
            )
            .await
        {
            Ok(progress) => {
                let count = |value: usize| u32::try_from(value).unwrap_or(u32::MAX);
                Ok(AdminDrainPostResponse::Status200_TheDrainPassCompleted(
                    models::DrainProgress::new(
                        count(progress.remaining),
                        count(progress.published),
                        count(progress.failed),
                    ),
                ))
            }
            Err(err) => Ok(AdminDrainPostResponse::Status500_ServerError(
                Self::server_error(err),
            )),
        }
    }

    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // When observability is disabled, the collection endpoint exposes
            // no nodes rather than returning a partial or synthetic record.
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        };
        if query_params
            .cluster_id
            .is_some_and(|cluster_id| cluster_id != observability.cluster_id())
        {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        }
        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };
        Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
            vec![models::Node::from(node)],
        ))
    }

    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // A disabled observability service behaves like node details are
            // unavailable on this process.
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };
        let cluster_mismatch = query_params
            .cluster_id
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesNodeIdGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };

        let detail = models::NodeDetail::new(
            node.cluster_id.to_string(),
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.paused_sandbox_count,
        );
        Ok(NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(detail))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use tower::ServiceExt;

    use super::*;
    use crate::api::impls::auth::API_KEY_HEADER;
    use crate::cfg::DrainConfig;
    use crate::orchestrator::{CreateSandboxRequest, SandboxLaunchSource, SandboxTimeoutAction};
    use crate::sandbox::mock::{MockAction, MockBehavior, MockOperation};
    use crate::sandbox::SandboxNetworkPolicy;
    use crate::snapshot::RunnableSnapshot;

    const TEST_API_KEY: &str =
        "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// How long a mocked pause takes when a test wants one to still be running
    /// when the deadline arrives.
    ///
    /// Far longer than the deadlines below, so a pass that respects its
    /// deadline and a pass that runs to completion are separated by seconds
    /// rather than by scheduling noise.
    const SLOW_PAUSE: Duration = Duration::from_secs(5);

    /// A node whose drain knobs and pause latency the test chooses.
    struct TestNode {
        api: Arc<ApiImpl>,
        behavior: Arc<MockBehavior>,
        /// Held for the life of the node: the persister writes paused state
        /// underneath it, and a pause into a deleted directory fails for
        /// reasons that have nothing to do with what these tests assert.
        _root: tempfile::TempDir,
    }

    impl TestNode {
        /// Builds a node running drains under `drain_config`.
        ///
        /// The shipped `[orchestrator.drain]` values are the same numbers the
        /// handler's literals would be, and the global config is a process-wide
        /// `OnceLock`, so a test that cannot choose its own values cannot tell
        /// a live config read from a hardcoded constant.
        async fn with_drain_config(drain_config: DrainConfig) -> Self {
            Self::build(Some(drain_config)).await
        }

        /// Builds a node running drains under this node's own configuration.
        async fn configured() -> Self {
            Self::build(None).await
        }

        async fn build(drain_config: Option<DrainConfig>) -> Self {
            let root = tempfile::tempdir().expect("temp dir");
            let behavior = Arc::new(MockBehavior::new());
            let orchestrator = crate::orchestrator::Orchestrator::new(
                crate::orchestrator::InMemoryMetadataStore::new(),
                crate::sandbox::NodeBackendFactory::Mock(
                    crate::sandbox::mock::MockBackendFactory::with_behavior(Arc::clone(&behavior)),
                ),
                crate::orchestrator::FileBackedSandboxPersister::new_for_test(
                    root.path().to_path_buf(),
                ),
            )
            .await
            .expect("in-memory orchestrator should not fail to construct");
            let api = ApiImpl::new(
                orchestrator,
                Arc::new(crate::snapshot::mock::mock_snapshot_manager()),
                Arc::new(crate::template::TemplateBuilder::new()),
                Arc::new(crate::image::ImageResolver::new(
                    &crate::cfg::AppConfig::default(),
                )),
                None,
                Vec::new(),
                crate::api_key::ApiKey::new(TEST_API_KEY).unwrap(),
            );
            let api = match drain_config {
                Some(drain_config) => api.with_drain_config(drain_config),
                None => api,
            };
            Self {
                api: Arc::new(api),
                behavior,
                _root: root,
            }
        }

        /// Adds a running sandbox whose pause takes `pause` to finish.
        async fn add_running_sandbox(&self, case: &str, pause: Duration) {
            self.behavior
                .push_action(MockOperation::Pause, MockAction::SucceedAfter(pause));
            self.api
                .orchestrator()
                .create_sandbox(CreateSandboxRequest {
                    source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
                    timeout: Some(Duration::from_secs(600)),
                    timeout_action: SandboxTimeoutAction::Pause,
                    user_metadata: Some(HashMap::from([("case".to_string(), case.to_string())])),
                    env_vars: None,
                    network_policy: SandboxNetworkPolicy::default(),
                    custom_extension_params: None,
                    auto_resume: false,
                    secure: false,
                })
                .await
                .expect("the mock backend should create a sandbox");
        }

        /// POSTs `body` to `/admin/drain` through the router that serves it,
        /// and reports how long the node took to answer.
        async fn drain(&self, body: &'static str) -> (u16, serde_json::Value, Duration) {
            let started = Instant::now();
            let response = crate::api::server::new(Arc::clone(&self.api))
                .oneshot(
                    http::Request::builder()
                        .method(http::Method::POST)
                        .uri("/admin/drain")
                        .header(http::header::HOST, "localhost")
                        .header(API_KEY_HEADER, TEST_API_KEY)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            let elapsed = started.elapsed();
            let status = response.status().as_u16();
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .unwrap()
                .to_bytes();
            (
                status,
                serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
                elapsed,
            )
        }
    }

    /// The endpoint a `preStop` hook polls, exercised through the router that
    /// serves it. A drain reached only through `Orchestrator::drain` is a
    /// library function, and the hook has no way to call one.
    #[tokio::test]
    async fn drain_answers_with_the_progress_a_prestop_hook_polls() {
        let node = TestNode::configured().await;

        let (status, body, _) = node.drain(r#"{"deadlineMs":5000}"#).await;

        assert_eq!(status, 200);
        assert_eq!(
            body,
            serde_json::json!({"remaining": 0, "published": 0, "failed": 0})
        );
    }

    /// An omitted deadline means the node's configured one, not none.
    ///
    /// The pass is put under a sandbox that takes [`SLOW_PAUSE`] to pause and a
    /// configured deadline far shorter than that, so a handler that reads the
    /// configuration answers in milliseconds with the sandbox still to do,
    /// while one that supplies its own fallback waits out the pause.
    #[tokio::test]
    async fn an_omitted_deadline_falls_back_to_the_configured_one() {
        let node = TestNode::with_drain_config(DrainConfig {
            concurrency: 4,
            deadline_ms: 150,
        })
        .await;
        node.add_running_sandbox("configured-deadline", SLOW_PAUSE)
            .await;

        let (status, body, elapsed) = node.drain("{}").await;

        assert_eq!(status, 200);
        assert_eq!(
            body["remaining"], 1,
            "the configured deadline must cut the pass short of the pause"
        );
        assert!(
            elapsed < SLOW_PAUSE / 2,
            "the pass must be bounded by the configured 150ms, took {elapsed:?}"
        );
    }

    /// A requested deadline is the one the pass runs under.
    ///
    /// The configured deadline is longer than the pause here, so a handler that
    /// ignored the request body would run the pause to completion instead of
    /// answering with what is left.
    #[tokio::test]
    async fn a_requested_deadline_is_the_one_the_pass_runs_under() {
        let node = TestNode::with_drain_config(DrainConfig {
            concurrency: 4,
            deadline_ms: 30_000,
        })
        .await;
        node.add_running_sandbox("requested-deadline", SLOW_PAUSE)
            .await;

        let (status, body, elapsed) = node.drain(r#"{"deadlineMs":150}"#).await;

        assert_eq!(status, 200);
        assert_eq!(
            body["remaining"], 1,
            "the requested deadline must cut the pass short of the pause"
        );
        assert!(
            elapsed < SLOW_PAUSE / 2,
            "the pass must be bounded by the requested 150ms, took {elapsed:?}"
        );
    }

    /// The pass runs at the node's configured concurrency.
    ///
    /// Each pause writes a memory image while the sandboxes still here are
    /// serving traffic, so an operator who narrows this expects it narrowed.
    /// Three sandboxes at concurrency one take three pauses' worth of time;
    /// anything wider finishes in about one.
    #[tokio::test]
    async fn the_pass_runs_at_the_configured_concurrency() {
        const PAUSE: Duration = Duration::from_millis(400);
        let node = TestNode::with_drain_config(DrainConfig {
            concurrency: 1,
            deadline_ms: 30_000,
        })
        .await;
        for index in 0..3 {
            node.add_running_sandbox(&format!("serial-{index}"), PAUSE)
                .await;
        }

        let (status, body, elapsed) = node.drain("{}").await;

        assert_eq!(status, 200);
        assert_eq!(body["remaining"], 0, "every sandbox must have been paused");
        assert!(
            elapsed >= PAUSE * 5 / 2,
            "concurrency 1 must pause three sandboxes one at a time, took {elapsed:?}"
        );
    }

    /// A zero deadline is refused rather than served: it reads as "do
    /// nothing", and a pass that does nothing has still closed the node to new
    /// work, which is the worst of both.
    #[tokio::test]
    async fn a_zero_deadline_is_refused() {
        let node = TestNode::configured().await;

        let (status, body, _) = node.drain(r#"{"deadlineMs":0}"#).await;

        assert_eq!(status, 400);
        assert_eq!(body["code"], 400);
    }

    /// The `[orchestrator.drain]` block a node ships with is the one it runs
    /// on.
    ///
    /// The handler reads these through the process-wide configuration, so a
    /// value that stops arriving there — a renamed key, a block dropped from
    /// the shipped file, a default in `cfg.rs` that drifted away from it —
    /// silently changes how every drained node behaves.
    #[test]
    fn the_shipped_drain_configuration_is_what_a_node_parses() {
        let drain = &crate::cfg::ConfigManager::global_config()
            .orchestrator
            .drain;

        assert_eq!(drain.concurrency, 4);
        assert_eq!(drain.deadline_ms, 30_000);
    }
}
