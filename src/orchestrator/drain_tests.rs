//! What a drain has to be true of, to be worth calling from a `preStop` hook.
//!
//! A drain is invoked by a preemption warning and answered by a kill, so its
//! failure modes are all "the process died while the answer was still being
//! computed". The tests here are about the shape that makes that survivable:
//! the node stops admitting work before anything is paused, one pass is
//! bounded, and what it reports is the node's real state rather than a tally
//! of what this particular pass got through.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex as StdMutex;

use super::super::mobility::{open_mobility_runtime, NodeMobilityFacts};
use super::super::persistence::DisabledSandboxPersister;
use super::super::types::SandboxLaunchSource;
use super::*;
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::{ArtifactReach, RunnableSnapshot, SnapshotId};

type TestOrchestrator =
    Orchestrator<InMemoryMetadataStore, MockBackendFactory, DisabledSandboxPersister>;

async fn make_orchestrator() -> Arc<TestOrchestrator> {
    Orchestrator::new_inner(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        local_image_services_from_global_config().runtime_refs,
    )
    .await
    .expect("in-memory orchestrator should not fail to construct")
}

fn create_request(case: &str) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        timeout: Some(Duration::from_secs(600)),
        timeout_action: SandboxTimeoutAction::Pause,
        user_metadata: Some(HashMap::from([("case".to_string(), case.to_string())])),
        env_vars: None,
        network_policy: SandboxNetworkPolicy::default(),
        custom_extension_params: None,
        auto_resume: false,
        secure: false,
    }
}

/// Installs a real mobility runtime, which is what decides whether a drain
/// publishes at all.
async fn install_mobility(orchestrator: &Arc<TestOrchestrator>, store_dir: &std::path::Path) {
    let facts = NodeMobilityFacts {
        cpu_architecture: "x86_64".to_string(),
        cluster_cpu_config: Arc::new(std::sync::RwLock::new(Some("{}".to_string()))),
        memory_page_size: 4096,
        artifact_reach: ArtifactReach::ClusterShared,
    };
    let mobility = open_mobility_runtime(store_dir, "drain-test-node", facts)
        .await
        .expect("a mobility store on a temp dir should open");
    orchestrator.install_mobility(mobility);
}

/// A publisher that records what the drain asked it to do, and what the node
/// looked like at the moment it was asked.
#[derive(Default)]
struct SpyPublisher {
    published: StdMutex<Vec<SandboxId>>,
    /// Whether the node was already refusing new work each time it was called.
    closed_when_called: StdMutex<Vec<bool>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    hold: Option<Duration>,
    fail: bool,
    /// Whether to record the commit the way the shipped publisher does.
    commits: bool,
    orchestrator: StdMutex<Option<Arc<TestOrchestrator>>>,
}

impl SpyPublisher {
    fn watching(orchestrator: &Arc<TestOrchestrator>) -> Self {
        Self {
            orchestrator: StdMutex::new(Some(Arc::clone(orchestrator))),
            ..Self::default()
        }
    }

    /// A publisher that finishes the job: it commits each sandbox to the
    /// mobility record, which is what `RepositoryPausedStatePublisher` does
    /// after a successful upload and what tells a later pass the sandbox is
    /// already in the repository.
    fn committing(orchestrator: &Arc<TestOrchestrator>) -> Self {
        Self {
            commits: true,
            orchestrator: StdMutex::new(Some(Arc::clone(orchestrator))),
            ..Self::default()
        }
    }

    fn failing() -> Self {
        Self {
            fail: true,
            ..Self::default()
        }
    }

    fn holding(hold: Duration) -> Self {
        Self {
            hold: Some(hold),
            ..Self::default()
        }
    }

    fn published(&self) -> Vec<SandboxId> {
        self.published.lock().expect("published lock").clone()
    }

    fn closed_when_called(&self) -> Vec<bool> {
        self.closed_when_called.lock().expect("closed lock").clone()
    }

    fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(AtomicOrdering::Relaxed)
    }
}

#[async_trait::async_trait]
impl PausedStatePublisher for SpyPublisher {
    async fn publish_paused(&self, sandbox_id: SandboxId) -> anyhow::Result<()> {
        let orchestrator = self.orchestrator.lock().expect("orchestrator lock").clone();
        if let Some(orchestrator) = &orchestrator {
            self.closed_when_called
                .lock()
                .expect("closed lock")
                .push(orchestrator.is_shutting_down());
        }
        let in_flight = self.in_flight.fetch_add(1, AtomicOrdering::AcqRel) + 1;
        self.peak_in_flight
            .fetch_max(in_flight, AtomicOrdering::AcqRel);
        if let Some(hold) = self.hold {
            tokio::time::sleep(hold).await;
        }
        self.in_flight.fetch_sub(1, AtomicOrdering::AcqRel);
        if self.fail {
            return Err(anyhow::anyhow!("the repository refused this publish"));
        }
        self.published
            .lock()
            .expect("published lock")
            .push(sandbox_id);
        if self.commits {
            if let Some(orchestrator) = &orchestrator {
                orchestrator
                    .mark_paused_snapshot_committed(sandbox_id, &SnapshotId::generate())
                    .await;
            }
        }
        Ok(())
    }
}

/// The node must stop admitting work *before* the pass pauses anything.
///
/// A drain that pauses first races the creates still arriving: every sandbox
/// it publishes can be replaced by one that landed while it was working, and
/// the node never converges. The publisher runs inside the pass, so what it
/// sees is what a create arriving mid-pass would have seen.
#[tokio::test]
async fn the_node_stops_admitting_work_before_the_pass_pauses_anything() -> Result<()> {
    crate::logging::init_for_tests();
    let store_dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = make_orchestrator().await;
    install_mobility(&orchestrator, store_dir.path()).await;
    orchestrator
        .create_sandbox(create_request("closed-first"))
        .await?;

    let publisher = SpyPublisher::watching(&orchestrator);
    orchestrator
        .drain(Duration::from_secs(30), 4, &publisher)
        .await?;

    assert_eq!(
        publisher.closed_when_called(),
        vec![true],
        "the node must already be refusing creates by the time a drain publishes"
    );
    assert!(matches!(
        orchestrator
            .create_sandbox(create_request("after-drain"))
            .await,
        Err(OrchestratorError::ShuttingDown)
    ));
    Ok(())
}

/// Publishing is what makes a paused sandbox reachable from another node, and
/// it is gated on mobility being installed.
///
/// Off — the shipped default — the pass still pauses everything, but uploads
/// nothing: the published snapshot is only reachable through the mobility
/// record naming it, so without one it is a copy of every byte the sandbox
/// owns that nothing will ever read.
#[tokio::test]
async fn publishing_is_gated_on_mobility_being_installed() -> Result<()> {
    crate::logging::init_for_tests();
    for mobility_installed in [false, true] {
        let store_dir = tempfile::tempdir().expect("temp dir");
        let orchestrator = make_orchestrator().await;
        if mobility_installed {
            install_mobility(&orchestrator, store_dir.path()).await;
        }
        let created = orchestrator.create_sandbox(create_request("gate")).await?;

        let publisher = SpyPublisher::default();
        let progress = orchestrator
            .drain(Duration::from_secs(30), 4, &publisher)
            .await?;

        assert_eq!(
            orchestrator
                .get_sandbox(&created.id)
                .await?
                .expect("the sandbox survives a drain")
                .state,
            SandboxState::Paused,
            "a drain pauses whether or not it publishes"
        );
        assert_eq!(progress.remaining, 0);
        if mobility_installed {
            assert_eq!(publisher.published(), vec![created.id]);
            assert_eq!(progress.published, 1);
        } else {
            assert!(
                publisher.published().is_empty(),
                "a node with no mobility installed must publish nothing"
            );
            assert_eq!(progress.published, 0);
        }
    }
    Ok(())
}

/// A second pass over an unchanged node publishes nothing again.
///
/// One pass is bounded and the caller polls, so a node that takes several
/// passes to converge shows every later pass the sandboxes the first one
/// already published. Re-uploading each of their memory images would make the
/// poll cost grow with the number of passes, against storage the sandboxes
/// still running here are using — and the second upload is not a wasteful
/// no-op but a correctness loss, because committing a fresh snapshot id over
/// the record leaves the snapshot the first pass published with nothing
/// naming it.
#[tokio::test]
async fn a_second_pass_does_not_republish_what_the_first_committed() -> Result<()> {
    crate::logging::init_for_tests();
    let store_dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = make_orchestrator().await;
    install_mobility(&orchestrator, store_dir.path()).await;
    let created = orchestrator.create_sandbox(create_request("twice")).await?;

    let publisher = SpyPublisher::committing(&orchestrator);
    let first = orchestrator
        .drain(Duration::from_secs(30), 4, &publisher)
        .await?;
    let committed = orchestrator
        .mobility()
        .expect("mobility is installed")
        .committed_snapshot(&created.id)
        .await;
    let second = orchestrator
        .drain(Duration::from_secs(30), 4, &publisher)
        .await?;

    assert_eq!(first.published, 1);
    assert_eq!(
        second.published, 0,
        "a sandbox whose paused state is already in the repository is not published again"
    );
    assert_eq!(
        publisher.published(),
        vec![created.id],
        "the publisher must be asked for each sandbox once, not once per pass"
    );
    assert_eq!(
        orchestrator
            .mobility()
            .expect("mobility is installed")
            .committed_snapshot(&created.id)
            .await,
        committed,
        "the record must still name the snapshot the first pass published"
    );
    Ok(())
}

/// A sandbox mid-transition is left for a later pass, not waited on.
///
/// `WAIT_TRANSITION_TIMEOUT` is a minute per sandbox. A pass that waits spends
/// its whole window on one wedged sandbox and pauses nothing else — which is
/// exactly how the sequential shutdown loop this replaces fails to fit any
/// preemption window.
#[tokio::test]
async fn a_wedged_transition_does_not_consume_the_pass() -> Result<()> {
    crate::logging::init_for_tests();
    let orchestrator = make_orchestrator().await;
    let running = orchestrator
        .create_sandbox(create_request("wedged"))
        .await?;
    let wedged = SandboxId::new();
    orchestrator
        .set_metadata_state_for_test(wedged, SandboxState::Resuming)
        .await?;

    let publisher = SpyPublisher::default();
    let progress = tokio::time::timeout(
        Duration::from_secs(10),
        orchestrator.drain(Duration::from_secs(30), 4, &publisher),
    )
    .await
    .expect("a drain must not wait out a wedged transition")?;

    assert_eq!(
        orchestrator
            .get_sandbox(&running.id)
            .await?
            .expect("the running sandbox survives")
            .state,
        SandboxState::Paused,
        "the wedged sandbox must not have starved the one that could be paused"
    );
    assert_eq!(
        progress.remaining, 1,
        "the wedged sandbox is still work, and the caller polls again for it"
    );
    Ok(())
}

/// A sandbox that is gone by the time the pass reaches it is not a failure.
///
/// `failed` is what an operator chases, so putting a sandbox in it that no
/// longer exists sends them looking for nothing. A roster read is a snapshot,
/// and a delete landing after it is ordinary.
#[tokio::test]
async fn a_sandbox_that_vanished_is_not_reported_as_a_failure() -> Result<()> {
    crate::logging::init_for_tests();
    let orchestrator = make_orchestrator().await;
    // Running in the store with no runtime behind it, which is what a sandbox
    // deleted between the roster read and the pause looks like from here.
    orchestrator
        .set_metadata_state_for_test(SandboxId::new(), SandboxState::Running)
        .await?;

    let publisher = SpyPublisher::default();
    let progress = orchestrator
        .drain(Duration::from_secs(30), 4, &publisher)
        .await?;

    assert_eq!(
        progress,
        DrainProgress {
            remaining: 0,
            published: 0,
            failed: 0
        }
    );
    Ok(())
}

/// A publish that fails is a sandbox that stays here, not a sandbox that was
/// lost. The pause stands, and the failure is reported as its own number.
#[tokio::test]
async fn a_failed_publish_keeps_the_pause_and_is_counted_separately() -> Result<()> {
    crate::logging::init_for_tests();
    let store_dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = make_orchestrator().await;
    install_mobility(&orchestrator, store_dir.path()).await;
    let created = orchestrator
        .create_sandbox(create_request("publish-fails"))
        .await?;

    let publisher = SpyPublisher::failing();
    let progress = orchestrator
        .drain(Duration::from_secs(30), 4, &publisher)
        .await?;

    assert_eq!(
        orchestrator
            .get_sandbox(&created.id)
            .await?
            .expect("the sandbox survives a failed publish")
            .state,
        SandboxState::Paused
    );
    assert_eq!(
        progress,
        DrainProgress {
            remaining: 0,
            published: 0,
            failed: 1
        }
    );
    Ok(())
}

/// The concurrency cap is a real ceiling, not a suggestion.
///
/// Each publish writes a memory image while the sandboxes still on this node
/// are serving traffic, so a pass that runs them all at once turns an orderly
/// drain into the incident it exists to avoid.
#[tokio::test]
async fn the_pass_never_exceeds_its_concurrency() -> Result<()> {
    crate::logging::init_for_tests();
    let store_dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = make_orchestrator().await;
    install_mobility(&orchestrator, store_dir.path()).await;
    for index in 0..4 {
        orchestrator
            .create_sandbox(create_request(&format!("bounded-{index}")))
            .await?;
    }

    let publisher = SpyPublisher::holding(Duration::from_millis(50));
    let progress = orchestrator
        .drain(Duration::from_secs(30), 2, &publisher)
        .await?;

    assert_eq!(progress.published, 4);
    assert!(
        publisher.peak_in_flight() <= 2,
        "concurrency 2 must never run more than two publishes at once, saw {}",
        publisher.peak_in_flight()
    );
    Ok(())
}

/// One pass is bounded by its deadline and answers with what is left.
///
/// The caller is a preemption warning with a window nobody here chooses, and a
/// pass that runs to completion is a pass that gets killed part-way through
/// with no answer at all.
#[tokio::test]
async fn a_pass_that_overruns_its_deadline_still_answers() -> Result<()> {
    crate::logging::init_for_tests();
    let store_dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = make_orchestrator().await;
    install_mobility(&orchestrator, store_dir.path()).await;
    orchestrator
        .create_sandbox(create_request("overrun"))
        .await?;

    let publisher = SpyPublisher::holding(Duration::from_secs(120));
    let progress = tokio::time::timeout(
        Duration::from_secs(10),
        orchestrator.drain(Duration::from_millis(100), 1, &publisher),
    )
    .await
    .expect("a drain must return when its deadline elapses")?;

    // Paused before the publish began, so nothing is left to preserve even
    // though the publish never finished.
    assert_eq!(progress.remaining, 0);
    assert_eq!(progress.published, 0);
    Ok(())
}
