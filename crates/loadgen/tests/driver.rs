//! Drives the load generator against a stub of the sandbox API.
//!
//! The stub answers the same three routes a real node does, so these tests
//! exercise the driver's own request sequence and its error taxonomy rather
//! than a re-statement of them. What each test pins is the behaviour that
//! makes a run's numbers trustworthy: that a lost binding is reported, that a
//! healthy node reports nothing, and that an open loop stays open.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentenv_loadgen::{Driver, LoadPlan, Mode, RequestRecord, Source, Tally, Target};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tokio::sync::mpsc;

/// What the stub does when asked about a sandbox it just handed out.
#[derive(Clone, Copy)]
enum LookupBehaviour {
    /// The healthy case.
    Running,
    /// The control plane has forgotten a sandbox it just created — the
    /// signature of a create that never acquired a scheduler binding, or of a
    /// binding reconciled away underneath a live sandbox.
    NotFound,
}

#[derive(Clone)]
struct StubState {
    lookup: LookupBehaviour,
    create_delay: Duration,
    created: Arc<AtomicU64>,
    /// Which create route each request arrived on, in order.
    routes: Arc<Mutex<Vec<&'static str>>>,
}

async fn create_warm(State(state): State<StubState>) -> impl IntoResponse {
    state
        .routes
        .lock()
        .expect("routes mutex")
        .push("/sandboxes");
    create_sandbox(State(state)).await
}

async fn create_cold(State(state): State<StubState>) -> impl IntoResponse {
    state
        .routes
        .lock()
        .expect("routes mutex")
        .push("/sandboxes-cold");
    create_sandbox(State(state)).await
}

async fn create_sandbox(State(state): State<StubState>) -> impl IntoResponse {
    tokio::time::sleep(state.create_delay).await;
    let seq = state.created.fetch_add(1, Ordering::SeqCst);
    let id = format!("sbx-{seq}");
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-agentenv-sandbox-id",
        HeaderValue::from_str(&id).expect("sandbox id is a valid header value"),
    );
    (
        StatusCode::CREATED,
        headers,
        Json(serde_json::json!({ "sandboxID": id })),
    )
}

async fn get_sandbox(
    State(state): State<StubState>,
    Path(sandbox_id): Path<String>,
) -> impl IntoResponse {
    match state.lookup {
        LookupBehaviour::Running => (
            StatusCode::OK,
            Json(serde_json::json!({ "sandboxID": sandbox_id, "state": "running" })),
        )
            .into_response(),
        LookupBehaviour::NotFound => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_sandbox(Path(_sandbox_id): Path<String>) -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Starts the stub on an ephemeral port and returns its base URL together
/// with the list every create appends its route to.
async fn spawn_stub_recording(
    lookup: LookupBehaviour,
    create_delay: Duration,
) -> (String, Arc<Mutex<Vec<&'static str>>>) {
    let routes = Arc::new(Mutex::new(Vec::new()));
    let state = StubState {
        lookup,
        create_delay,
        created: Arc::new(AtomicU64::new(0)),
        routes: Arc::clone(&routes),
    };
    let app = Router::new()
        .route("/sandboxes", post(create_warm))
        .route("/sandboxes-cold", post(create_cold))
        .route("/sandboxes/{sandbox_id}", get(get_sandbox))
        .route("/sandboxes/{sandbox_id}", delete(delete_sandbox))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("stub should bind an ephemeral port");
    let addr = listener.local_addr().expect("stub should report its port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), routes)
}

async fn spawn_stub(lookup: LookupBehaviour, create_delay: Duration) -> String {
    spawn_stub_recording(lookup, create_delay).await.0
}

fn target(base_url: String) -> Target {
    target_from(base_url, Source::Template("ubuntu".to_string()))
}

fn target_from(base_url: String, source: Source) -> Target {
    Target {
        base_url,
        api_key: "loadgen-test-key".to_string(),
        source,
        timeout_secs: 60,
        proxy_port: None,
        cleanup: true,
        request_timeout: Duration::from_secs(5),
    }
}

async fn run(plan: LoadPlan, target: Target) -> (Vec<RequestRecord>, Tally) {
    let driver = Driver::new(target).expect("driver should build");
    let (tx, mut rx) = mpsc::channel::<RequestRecord>(1024);
    let collector = tokio::spawn(async move {
        let mut records = Vec::new();
        let mut tally = Tally::new();
        while let Some(record) = rx.recv().await {
            tally.observe(&record);
            records.push(record);
        }
        (records, tally)
    });
    driver.run(plan, tx).await;
    collector.await.expect("collector should not panic")
}

/// The one number the whole generator exists to produce.
///
/// A node that answers 201 and then 404 for the same id has created a sandbox
/// the control plane cannot route to. If the driver retried that 404 as though
/// the sandbox were merely slow to start, the run would report a timeout and
/// the defect would read as latency.
#[tokio::test]
async fn a_404_on_a_sandbox_this_run_created_is_reported() {
    let base_url = spawn_stub(LookupBehaviour::NotFound, Duration::ZERO).await;
    let plan = LoadPlan {
        requests: 4,
        concurrency: 4,
        mode: Mode::Closed,
        seed: 1,
        ready_timeout: Duration::from_secs(2),
    };

    let (records, mut tally) = run(plan, target(base_url)).await;
    let summary = tally.summary(1.0);

    assert_eq!(records.len(), 4);
    assert_eq!(
        summary.self_inflicted_404, 4,
        "every lost sandbox should be counted, got {summary:?}"
    );
    assert_eq!(summary.created, 4);
    assert_eq!(summary.errors.get("ready:404"), Some(&4));
}

/// A stage that failed contributes no latency to that stage's quantiles.
///
/// The ready stage of a lost sandbox ends in a 404, which comes back far
/// faster than a sandbox becomes ready. Recording it as a ready latency means
/// the more sandboxes a control plane loses, the better its ready percentiles
/// look — the summary would flatter exactly the run it exists to condemn.
#[tokio::test]
async fn a_failed_stage_contributes_no_latency_to_its_quantiles() {
    let base_url = spawn_stub(LookupBehaviour::NotFound, Duration::ZERO).await;
    let plan = LoadPlan {
        requests: 4,
        concurrency: 4,
        mode: Mode::Closed,
        seed: 1,
        ready_timeout: Duration::from_secs(2),
    };

    let (records, mut tally) = run(plan, target(base_url)).await;
    let summary = tally.summary(1.0);

    assert_eq!(summary.errors.get("ready:404"), Some(&4));
    assert!(
        records.iter().all(|record| record.ready_ms.is_none()),
        "a sandbox that never reached running reported a ready latency: {records:?}"
    );
    assert_eq!(
        summary.ready.p50, 0.0,
        "failed ready stages entered the ready quantiles: {summary:?}"
    );
    assert_eq!(summary.ready.max, 0.0, "{summary:?}");
    assert!(
        records.iter().all(|record| record.create_ms.is_some()),
        "the create stage succeeded and must still be measured: {records:?}"
    );
}

#[tokio::test]
async fn a_healthy_node_produces_no_control_plane_errors() {
    let base_url = spawn_stub(LookupBehaviour::Running, Duration::ZERO).await;
    let plan = LoadPlan {
        requests: 8,
        concurrency: 4,
        mode: Mode::Closed,
        seed: 1,
        ready_timeout: Duration::from_secs(2),
    };

    let (_records, mut tally) = run(plan, target(base_url)).await;
    let summary = tally.summary(2.0);

    assert_eq!(summary.completed, 8);
    assert_eq!(summary.created, 8);
    assert_eq!(summary.self_inflicted_404, 0);
    assert_eq!(summary.bad_gateway, 0);
    assert!(summary.errors.is_empty(), "unexpected errors: {summary:?}");
    assert_eq!(summary.create.count, 8);
    assert_eq!(summary.ready.count, 8);
    assert_eq!(summary.creates_per_sec, 4.0);
}

/// An open loop that waits for a permit is a closed loop wearing a rate flag.
///
/// The distinction is the whole reason the mode exists: under a closed loop a
/// saturated system reports fewer requests and unchanged latency, which reads
/// as healthy. Arrivals that cannot be served must be counted as shed.
#[tokio::test]
async fn open_loop_sheds_arrivals_it_cannot_admit() {
    let base_url = spawn_stub(LookupBehaviour::Running, Duration::from_millis(100)).await;
    let plan = LoadPlan {
        requests: 20,
        concurrency: 1,
        mode: Mode::Open {
            rate_per_sec: 200.0,
        },
        seed: 42,
        ready_timeout: Duration::from_secs(2),
    };

    let (records, mut tally) = run(plan, target(base_url)).await;
    let summary = tally.summary(1.0);

    assert_eq!(records.len(), 20);
    assert!(
        summary.shed > 0,
        "an arrival rate of 200/s against one 100ms server should shed, got {summary:?}"
    );
    assert_eq!(
        summary.shed + summary.completed,
        20,
        "every arrival should be either served or shed: {summary:?}"
    );
}

/// A mock-backend node has no templates, so the generator has to reach it
/// through the cold-create route or measure nothing but 400s.
#[tokio::test]
async fn an_image_source_creates_through_the_cold_route() {
    let (base_url, routes) = spawn_stub_recording(LookupBehaviour::Running, Duration::ZERO).await;
    let plan = LoadPlan {
        requests: 3,
        concurrency: 3,
        mode: Mode::Closed,
        seed: 1,
        ready_timeout: Duration::from_secs(2),
    };

    let (_records, mut tally) = run(
        plan,
        target_from(base_url, Source::Image("ubuntu:24.04".to_string())),
    )
    .await;
    let summary = tally.summary(1.0);

    assert_eq!(summary.created, 3);
    let seen = routes.lock().expect("routes mutex").clone();
    assert_eq!(
        seen,
        vec!["/sandboxes-cold"; 3],
        "created on the wrong route"
    );
}

#[tokio::test]
async fn a_template_source_creates_through_the_warm_route() {
    let (base_url, routes) = spawn_stub_recording(LookupBehaviour::Running, Duration::ZERO).await;
    let plan = LoadPlan {
        requests: 2,
        concurrency: 2,
        mode: Mode::Closed,
        seed: 1,
        ready_timeout: Duration::from_secs(2),
    };

    let (_records, _tally) = run(plan, target(base_url)).await;

    let seen = routes.lock().expect("routes mutex").clone();
    assert_eq!(seen, vec!["/sandboxes"; 2], "created on the wrong route");
}
