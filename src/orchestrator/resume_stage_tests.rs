//! What a resume's stage timings have to say to be worth reading.
//!
//! The API handler already times a resume as one stage, which answers "how
//! long" and nothing else. These pin the two sub-stages that separate the
//! answers an operator actually acts on: a fence that is slow because it is a
//! scheduler round trip, and a launch that is slow because the guest is.

use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};

use super::super::mobility::{open_mobility_runtime, NodeMobilityFacts};
use super::super::persistence::DisabledSandboxPersister;
use super::super::types::SandboxLaunchSource;
use super::*;
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::{ArtifactReach, RunnableSnapshot};

/// One `agentenv_sandbox_stage_duration_seconds` observation, in the labels
/// that decide what it means.
type StageObservation = (String, String, String);

/// Captures histogram observations so a test can read back what was emitted.
///
/// Installed per-thread, so it does not disturb the process-wide recorder the
/// server installs.
#[derive(Default)]
struct StageSpy {
    observed: Arc<StdMutex<Vec<StageObservation>>>,
}

struct SpyHistogram {
    observation: StageObservation,
    observed: Arc<StdMutex<Vec<StageObservation>>>,
}

impl metrics::HistogramFn for SpyHistogram {
    fn record(&self, _value: f64) {
        self.observed
            .lock()
            .expect("observed lock")
            .push(self.observation.clone());
    }
}

impl metrics::Recorder for StageSpy {
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
        let label = |wanted: &str| {
            key.labels()
                .find(|label| label.key() == wanted)
                .map(|label| label.value().to_string())
                .unwrap_or_default()
        };
        metrics::Histogram::from_arc(Arc::new(SpyHistogram {
            observation: (key.name().to_string(), label("stage"), label("status")),
            observed: Arc::clone(&self.observed),
        }))
    }
}

/// The resume path's sub-stages, recorded through the process's real metric
/// call sites rather than through a test-only counter.
#[test]
fn a_resume_records_its_claim_and_its_launch_separately() {
    crate::logging::init_for_tests();
    // A current-thread runtime keeps every spawned task on this thread, which
    // is where the recorder is installed: `with_local_recorder` is
    // thread-local, so a work-stealing runtime would drop half the
    // observations on other threads.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime");
    let spy = StageSpy::default();
    let observed = Arc::clone(&spy.observed);

    metrics::with_local_recorder(&spy, || {
        runtime.block_on(async {
            let store_dir = tempfile::tempdir().expect("temp dir");
            let orchestrator = Orchestrator::new_inner(
                InMemoryMetadataStore::new(),
                MockBackendFactory::new(),
                DisabledSandboxPersister,
                local_image_services_from_global_config().runtime_refs,
            )
            .await
            .expect("in-memory orchestrator should not fail to construct");

            let facts = NodeMobilityFacts {
                cpu_architecture: "x86_64".to_string(),
                cluster_cpu_config: Arc::new(std::sync::RwLock::new(Some("{}".to_string()))),
                memory_page_size: 4096,
                artifact_reach: ArtifactReach::ClusterShared,
            };
            let mobility = open_mobility_runtime(store_dir.path(), "resume-stage-node", facts)
                .await
                .expect("a mobility store on a temp dir should open");
            orchestrator.install_mobility(mobility);

            let created = orchestrator
                .create_sandbox(CreateSandboxRequest {
                    source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
                    timeout: Some(Duration::from_secs(600)),
                    timeout_action: SandboxTimeoutAction::Pause,
                    user_metadata: None,
                    env_vars: None,
                    network_policy: SandboxNetworkPolicy::default(),
                    custom_extension_params: None,
                    auto_resume: false,
                    secure: false,
                })
                .await
                .expect("create");
            orchestrator.pause_sandbox(created.id).await.expect("pause");
            orchestrator
                .resume_sandbox(created.id, NewTimeout::UseExisting)
                .await
                .expect("resume");
        })
    });

    let stages: HashSet<StageObservation> = observed
        .lock()
        .expect("observed lock")
        .iter()
        .filter(|(name, _, _)| name == "agentenv_sandbox_stage_duration_seconds")
        .cloned()
        .collect();

    for stage in ["claim", "launch"] {
        assert!(
            stages.contains(&(
                "agentenv_sandbox_stage_duration_seconds".to_string(),
                stage.to_string(),
                "ok".to_string()
            )),
            "a resume must time its {stage} stage; recorded {stages:?}"
        );
    }
}
