//! The HTTP driver: what one request does, and how the plan schedules them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode};
use tokio::sync::{mpsc, Semaphore};

use crate::arrivals::PoissonArrivals;
use crate::report::{Outcome, RequestRecord, Stage};

/// Header the node sets on a successful create, and the one the proxy routes
/// by. Both spellings live in `src/api/openapi.yml`.
const SANDBOX_ID_HEADER: &str = "x-agentenv-sandbox-id";
const TARGET_PORT_HEADER: &str = "x-agentenv-target-port";
const API_KEY_HEADER: &str = "x-api-key";

/// What each request creates its sandbox from.
#[derive(Clone, Debug)]
pub enum Source {
    /// `POST /sandboxes` with a template id, which the node resolves to a
    /// committed snapshot in its repository.
    Template(String),
    /// `POST /sandboxes-cold` with an image reference. This is the path a
    /// mock-backend node can serve: its snapshot repository is empty, while its
    /// image resolver answers every reference with a placeholder. Same
    /// orchestrator create, same gateway binding path.
    Image(String),
}

/// Where the load goes and how it authenticates.
#[derive(Clone, Debug)]
pub struct Target {
    /// Base URL of a node or a gateway, without a trailing slash.
    pub base_url: String,
    pub api_key: String,
    pub source: Source,
    /// Sandbox lifetime requested at create, in seconds. Long enough that the
    /// eviction sweep does not race a run, short enough that an abandoned run
    /// cleans itself up.
    pub timeout_secs: u64,
    /// Guest port to send the first proxied request to. `None` skips the proxy
    /// stage, which is the right default against a mock-backend node: nothing
    /// is listening inside a sandbox that has no guest.
    pub proxy_port: Option<u16>,
    /// Whether to delete each sandbox once its stages are measured.
    pub cleanup: bool,
    pub request_timeout: Duration,
}

/// Closed loop or open loop.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// `concurrency` requests in flight at all times; the offered rate is
    /// whatever the system will take.
    Closed,
    /// Arrivals at a fixed rate regardless of how the system is coping;
    /// `concurrency` becomes an in-flight ceiling and arrivals that hit it are
    /// shed rather than queued.
    Open { rate_per_sec: f64 },
}

/// One run.
#[derive(Clone, Copy, Debug)]
pub struct LoadPlan {
    pub requests: u64,
    pub concurrency: usize,
    pub mode: Mode,
    pub seed: u64,
    /// How long to keep polling `GET /sandboxes/{id}` for `running`.
    pub ready_timeout: Duration,
}

/// Drives one [`LoadPlan`] against one [`Target`].
pub struct Driver {
    client: Client,
    target: Arc<Target>,
}

impl Driver {
    pub fn new(target: Target) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(API_KEY_HEADER),
            HeaderValue::from_str(&target.api_key)
                .context("API key is not a valid header value")?,
        );
        let client = Client::builder()
            .default_headers(headers)
            .timeout(target.request_timeout)
            // One run opens as many connections as its concurrency; letting the
            // pool idle them out mid-run would measure connection setup rather
            // than the control plane.
            .pool_max_idle_per_host(1024)
            .build()
            .context("build HTTP client")?;

        Ok(Self {
            client,
            target: Arc::new(target),
        })
    }

    /// Runs the plan, sending each request's record down `records` as it
    /// completes. Returns how long the offered load ran for.
    pub async fn run(&self, plan: LoadPlan, records: mpsc::Sender<RequestRecord>) -> Duration {
        let permits = Arc::new(Semaphore::new(plan.concurrency));
        let started = Instant::now();
        let mut tasks = Vec::new();
        let mut arrivals = match plan.mode {
            Mode::Closed => None,
            Mode::Open { rate_per_sec } => Some(PoissonArrivals::new(rate_per_sec, plan.seed)),
        };

        for seq in 0..plan.requests {
            let permit = match &mut arrivals {
                None => Arc::clone(&permits)
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed"),
                Some(arrivals) => {
                    tokio::time::sleep(arrivals.next_gap()).await;
                    match Arc::clone(&permits).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let mut record = RequestRecord::new(seq);
                            record.outcome = Outcome::Shed;
                            let _ = records.send(record).await;
                            continue;
                        }
                    }
                }
            };

            let client = self.client.clone();
            let target = Arc::clone(&self.target);
            let records = records.clone();
            tasks.push(tokio::spawn(async move {
                let record = one_request(&client, &target, plan.ready_timeout, seq).await;
                drop(permit);
                let _ = records.send(record).await;
            }));
        }

        for task in tasks {
            let _ = task.await;
        }
        started.elapsed()
    }
}

/// Walks one sandbox through create, ready, proxy and cleanup.
///
/// A stage that fails ends the request: the later stages measure nothing once
/// the sandbox is not there, and reporting a ready latency for a create that
/// never returned an id would flatter every quantile.
async fn one_request(
    client: &Client,
    target: &Target,
    ready_timeout: Duration,
    seq: u64,
) -> RequestRecord {
    let mut record = RequestRecord::new(seq);

    let (path, body) = match &target.source {
        Source::Template(template_id) => (
            "/sandboxes",
            serde_json::json!({
                "templateID": template_id,
                "timeout": target.timeout_secs,
                "autoPause": false,
            }),
        ),
        Source::Image(image) => (
            "/sandboxes-cold",
            serde_json::json!({
                "image": image,
                "timeout": target.timeout_secs,
                "autoPause": false,
            }),
        ),
    };

    // A stage's latency is recorded only when that stage succeeded. Time to
    // failure is not a latency: a refused connection or a 404 comes back
    // faster than the real thing, so a run that lost sandboxes would report
    // better percentiles than one that lost none. The failure is already
    // counted, by stage and status, in the error taxonomy.
    let started = Instant::now();
    let created = client
        .post(format!("{}{path}", target.base_url))
        .json(&body)
        .send()
        .await;

    let response = match created {
        Ok(response) => response,
        Err(error) => {
            record.outcome = transport(Stage::Create, &error);
            return record;
        }
    };
    if response.status() != StatusCode::CREATED {
        record.outcome = Outcome::Status {
            stage: Stage::Create,
            status: response.status().as_u16(),
        };
        return record;
    }
    record.create_ms = Some(elapsed_ms(started));

    // The header is the contract the gateway binds on; falling back to the
    // body would hide a node that stopped setting it.
    let sandbox_id = response
        .headers()
        .get(SANDBOX_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let Some(sandbox_id) = sandbox_id else {
        record.outcome = Outcome::Transport {
            stage: Stage::Create,
            error: format!("201 without a {SANDBOX_ID_HEADER} header"),
        };
        return record;
    };
    record.sandbox_id = Some(sandbox_id.clone());

    let ready_started = Instant::now();
    match wait_until_running(client, target, &sandbox_id, ready_timeout).await {
        Ok(()) => record.ready_ms = Some(elapsed_ms(ready_started)),
        Err(outcome) => {
            record.outcome = outcome;
            cleanup(client, target, &sandbox_id).await;
            return record;
        }
    }

    if let Some(port) = target.proxy_port {
        let proxy_started = Instant::now();
        match first_proxy_request(client, target, &sandbox_id, port).await {
            Ok(()) => record.proxy_ms = Some(elapsed_ms(proxy_started)),
            Err(outcome) => {
                record.outcome = outcome;
                cleanup(client, target, &sandbox_id).await;
                return record;
            }
        }
    }

    cleanup(client, target, &sandbox_id).await;
    record
}

/// Polls the sandbox until it reports `running`.
async fn wait_until_running(
    client: &Client,
    target: &Target,
    sandbox_id: &str,
    ready_timeout: Duration,
) -> Result<(), Outcome> {
    let deadline = Instant::now() + ready_timeout;
    loop {
        let response = client
            .get(format!("{}/sandboxes/{sandbox_id}", target.base_url))
            .send()
            .await
            .map_err(|error| transport(Stage::Ready, &error))?;

        let status = response.status();
        if status.is_success() {
            let state = response
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|body| {
                    body.get("state")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                });
            match state.as_deref() {
                Some("running") => return Ok(()),
                // A sandbox that reached a terminal state is not going to
                // become ready; waiting out the deadline would report it as a
                // timeout and hide what actually happened.
                Some("killed") | Some("error") => {
                    return Err(Outcome::Transport {
                        stage: Stage::Ready,
                        error: format!("sandbox reached state {}", state.unwrap_or_default()),
                    })
                }
                _ => {}
            }
        } else {
            return Err(Outcome::Status {
                stage: Stage::Ready,
                status: status.as_u16(),
            });
        }

        if Instant::now() >= deadline {
            return Err(Outcome::Transport {
                stage: Stage::Ready,
                error: "sandbox never reported running".to_string(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn first_proxy_request(
    client: &Client,
    target: &Target,
    sandbox_id: &str,
    port: u16,
) -> Result<(), Outcome> {
    let response = client
        .get(format!("{}/proxy/", target.base_url))
        .header(SANDBOX_ID_HEADER, sandbox_id)
        .header(TARGET_PORT_HEADER, port.to_string())
        .send()
        .await
        .map_err(|error| transport(Stage::Proxy, &error))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(Outcome::Status {
            stage: Stage::Proxy,
            status: response.status().as_u16(),
        })
    }
}

/// Deletes the sandbox, ignoring the result.
///
/// Cleanup failures are deliberately not an outcome: a run that reported a
/// failed delete as a failed create would misattribute the defect, and the
/// sandbox timeout reaps whatever is left behind either way.
async fn cleanup(client: &Client, target: &Target, sandbox_id: &str) {
    if !target.cleanup {
        return;
    }
    let _ = client
        .delete(format!("{}/sandboxes/{sandbox_id}", target.base_url))
        .send()
        .await;
}

fn transport(stage: Stage, error: &reqwest::Error) -> Outcome {
    Outcome::Transport {
        stage,
        error: error.to_string(),
    }
}

fn elapsed_ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}
