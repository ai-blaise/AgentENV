//! `aenv-loadgen` — burst-create load against a node or a gateway.
//!
//! Reads the same environment contract the e2e suites use
//! (`scripts/tests/e2e/lib/helpers.sh`), so it drops into any mode
//! `run_e2e.sh` can bring up: `AENV_URL`, `AENV_API_KEY`, `AENV_TEMPLATE_ID`.

use std::io::Write;
use std::time::Duration;

use agentenv_loadgen::{Driver, LoadPlan, Mode, RequestRecord, Source, Tally, Target};
use anyhow::{bail, Context, Result};
use clap::Parser;
use tokio::sync::mpsc;

#[derive(Debug, Parser)]
#[command(
    name = "aenv-loadgen",
    about = "Burst-create load generator for the AgentENV HTTP API"
)]
struct Cli {
    /// Base URL of a node or a gateway.
    #[arg(long, env = "AENV_URL", default_value = "http://127.0.0.1:3000")]
    url: String,

    #[arg(long, env = "AENV_API_KEY")]
    api_key: String,

    #[arg(long, env = "AENV_TEMPLATE_ID", default_value = "ubuntu")]
    template_id: String,

    /// Create from this image through POST /sandboxes-cold instead of from a
    /// template. Required against a mock-backend node, whose snapshot
    /// repository holds no templates.
    #[arg(long)]
    image: Option<String>,

    /// Total number of sandboxes to create.
    #[arg(long, short = 'n', default_value_t = 100)]
    requests: u64,

    /// Requests in flight (closed loop), or the in-flight ceiling above which
    /// open-loop arrivals are shed.
    #[arg(long, short = 'c', default_value_t = 16)]
    concurrency: usize,

    /// `closed` holds the concurrency constant; `open` offers a fixed arrival
    /// rate, which is the mode that can show saturation.
    #[arg(long, default_value = "closed")]
    mode: String,

    /// Arrivals per second in open mode.
    #[arg(long, default_value_t = 10.0)]
    rate: f64,

    /// Seed for the open-loop arrival schedule, so a run can be replayed.
    #[arg(long, default_value_t = 0x5EED_0000_0000_0001)]
    seed: u64,

    /// Sandbox lifetime requested at create.
    #[arg(long, default_value_t = 120)]
    sandbox_timeout_secs: u64,

    /// Measure a first proxied request to this guest port. Leave unset against
    /// a mock-backend node: there is no guest to answer.
    #[arg(long)]
    proxy_port: Option<u16>,

    /// Keep the sandboxes a run creates instead of deleting them.
    #[arg(long, default_value_t = false)]
    no_cleanup: bool,

    /// Per-HTTP-request timeout.
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,

    /// How long to poll for a sandbox to report `running`.
    #[arg(long, default_value_t = 60)]
    ready_timeout_secs: u64,

    /// Write the per-request newline-delimited JSON here instead of stdout.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mode = match cli.mode.as_str() {
        "closed" => Mode::Closed,
        "open" => Mode::Open {
            rate_per_sec: cli.rate,
        },
        other => bail!("--mode must be 'closed' or 'open', got '{other}'"),
    };
    if cli.concurrency == 0 {
        bail!("--concurrency must be at least 1");
    }

    let source = match cli.image {
        Some(image) => Source::Image(image),
        None => Source::Template(cli.template_id),
    };
    let target = Target {
        base_url: cli.url.trim_end_matches('/').to_string(),
        api_key: cli.api_key,
        source,
        timeout_secs: cli.sandbox_timeout_secs,
        proxy_port: cli.proxy_port,
        cleanup: !cli.no_cleanup,
        request_timeout: Duration::from_secs(cli.request_timeout_secs),
    };
    let plan = LoadPlan {
        requests: cli.requests,
        concurrency: cli.concurrency,
        mode,
        seed: cli.seed,
        ready_timeout: Duration::from_secs(cli.ready_timeout_secs),
    };

    let driver = Driver::new(target)?;
    let (tx, mut rx) = mpsc::channel::<RequestRecord>(1024);

    // The sink runs beside the load so a slow disk cannot back-pressure the
    // offered rate into something other than what was asked for.
    let out_path = cli.out.clone();
    let sink = tokio::spawn(async move {
        let mut sink: Box<dyn Write + Send> = match out_path {
            Some(path) => Box::new(
                std::fs::File::create(&path)
                    .with_context(|| format!("create {}", path.display()))?,
            ),
            None => Box::new(std::io::stdout()),
        };
        let mut tally = Tally::new();
        while let Some(record) = rx.recv().await {
            tally.observe(&record);
            let line = serde_json::to_string(&record).context("serialize request record")?;
            writeln!(sink, "{line}").context("write request record")?;
        }
        sink.flush().context("flush request records")?;
        Ok::<Tally, anyhow::Error>(tally)
    });

    let elapsed = driver.run(plan, tx).await;
    let mut tally = sink.await.context("record sink panicked")??;
    let summary = tally.summary(elapsed.as_secs_f64());

    eprintln!(
        "{}",
        serde_json::to_string_pretty(&summary).context("serialize summary")?
    );

    // A run that lost a sandbox it created is a failed run, not a slow one.
    // The exit status is what a CI gate reads.
    if summary.self_inflicted_404 > 0 || summary.bad_gateway > 0 {
        bail!(
            "control plane lost sandboxes this run created: {} self-inflicted 404s, {} 502s",
            summary.self_inflicted_404,
            summary.bad_gateway
        );
    }
    Ok(())
}
