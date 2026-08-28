use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agentenv_control_plane::proto::scheduler_server::SchedulerServer;
use agentenv_control_plane::{
    AssignmentStore, CapacityLimits, ControlPlane, InMemoryAssignmentStore, Node, NodeRegistry,
    PlacementConfig, PlacementEngine, RedisAssignmentStore, SandboxResources,
};
use anyhow::{bail, Context};
use clap::Parser;
use tokio::sync::watch;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[command(name = "agentenv-control-plane")]
#[command(about = "AgentENV Rust scheduler and routing control plane")]
struct Args {
    #[arg(
        long,
        env = "AGENTENV_CONTROL_PLANE_LISTEN",
        default_value = "0.0.0.0:9090"
    )]
    grpc_listen: SocketAddr,
    #[arg(
        long,
        env = "AGENTENV_CONTROL_PLANE_METRICS_LISTEN",
        default_value = "0.0.0.0:9101"
    )]
    metrics_listen: SocketAddr,
    #[arg(long, env = "AGENTENV_CLUSTER_ID")]
    cluster_id: String,
    /// Static node in `node-id=http[s]://host:port` form. Repeat per node.
    #[arg(
        long = "node",
        env = "AGENTENV_CONTROL_PLANE_NODES",
        value_delimiter = ','
    )]
    nodes: Vec<NodeSpec>,
    #[arg(long = "draining-node-id", value_delimiter = ',')]
    draining_node_ids: Vec<String>,
    #[arg(long, env = "AGENTENV_REDIS_URL")]
    redis_url: Option<String>,
    /// Explicitly allow process-local assignment state. Unsafe with replicas.
    #[arg(long, default_value_t = false)]
    allow_ephemeral_state: bool,
    #[arg(long, default_value_t = 30)]
    heartbeat_ttl_seconds: u64,
    #[arg(long, default_value_t = 120)]
    reservation_ttl_seconds: u64,
    #[arg(long, default_value_t = 3600)]
    assignment_ttl_seconds: u64,
    #[arg(long, default_value_t = 3)]
    sample_size: usize,
    #[arg(long, default_value_t = 32)]
    probe_budget: usize,
    #[arg(long)]
    required_version: Option<String>,
    #[arg(long)]
    required_commit: Option<String>,
    #[arg(long)]
    required_cpu_architecture: Option<String>,
    #[arg(long)]
    max_sandboxes: Option<u64>,
    #[arg(long)]
    max_starting: Option<u64>,
    #[arg(long)]
    max_cpu: Option<u64>,
    #[arg(long)]
    max_memory_bytes: Option<u64>,
    #[arg(long)]
    max_disk_bytes: Option<u64>,
    #[arg(long, default_value_t = 1)]
    default_request_cpu: u32,
    #[arg(long, default_value_t = 512)]
    default_request_memory_mb: u64,
    #[arg(long, default_value_t = 8192)]
    default_request_disk_mb: u64,
    #[arg(long, default_value_t = 1_000_000)]
    artifact_capacity: usize,
    #[arg(long, default_value_t = 32)]
    artifact_node_limit: usize,
    #[arg(long, env = "AGENTENV_CONTROL_PLANE_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    #[arg(long, env = "AGENTENV_CONTROL_PLANE_TLS_KEY")]
    tls_key: Option<PathBuf>,
    #[arg(long, env = "AGENTENV_CONTROL_PLANE_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,
    /// Explicitly serve plaintext gRPC. Intended only for trusted test networks.
    #[arg(long, default_value_t = false)]
    allow_insecure_transport: bool,
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_filter: String,
    #[arg(long, default_value_t = false)]
    log_json: bool,
}

#[derive(Clone)]
struct NodeSpec(Node);

impl FromStr for NodeSpec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (node_id, endpoint) = value
            .split_once('=')
            .ok_or_else(|| "node must use node-id=http[s]://host:port form".to_string())?;
        if node_id.is_empty()
            || node_id.len() > 128
            || !node_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err("node ID contains invalid characters or exceeds 128 bytes".to_string());
        }
        let parsed =
            url::Url::parse(endpoint).map_err(|error| format!("invalid endpoint: {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(
                "endpoint must be an http(s) URL without credentials, query, or fragment"
                    .to_string(),
            );
        }
        Ok(Self(Node::new(node_id, endpoint)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(&args.log_filter, args.log_json)?;
    validate_args(&args)?;
    agentenv_observability::init_prometheus_recorder()?;
    let tls = load_tls(&args).await?;

    let reservation_ttl = Duration::from_secs(args.reservation_ttl_seconds);
    let assignment_ttl = Duration::from_secs(args.assignment_ttl_seconds);
    let assignments: Arc<dyn AssignmentStore> = match args.redis_url.as_deref() {
        Some(redis_url) => Arc::new(
            RedisAssignmentStore::connect(
                redis_url,
                &args.cluster_id,
                reservation_ttl,
                assignment_ttl,
            )
            .await?,
        ),
        None => {
            warn!("using process-local assignment state; horizontal replicas are unsafe");
            Arc::new(InMemoryAssignmentStore::new(
                reservation_ttl,
                assignment_ttl,
            )?)
        }
    };

    let registry = Arc::new(NodeRegistry::new());
    let draining = args
        .draining_node_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    registry.replace_discovered(
        args.nodes
            .iter()
            .map(|spec| (spec.0.clone(), draining.contains(&spec.0.id))),
    );
    let placement = PlacementEngine::new(PlacementConfig {
        heartbeat_ttl: Duration::from_secs(args.heartbeat_ttl_seconds),
        sample_size: args.sample_size,
        probe_budget: args.probe_budget,
        required_version: args.required_version,
        required_commit: args.required_commit,
        required_cpu_architecture: args.required_cpu_architecture,
        limits: CapacityLimits {
            max_sandboxes: args.max_sandboxes,
            max_starting: args.max_starting,
            max_cpu: args.max_cpu,
            max_memory_bytes: args.max_memory_bytes,
            max_disk_bytes: args.max_disk_bytes,
        },
        default_request: SandboxResources {
            cpu: args.default_request_cpu,
            memory_bytes: mib_to_bytes(args.default_request_memory_mb)?,
            disk_bytes: mib_to_bytes(args.default_request_disk_mb)?,
        },
    })
    .map_err(anyhow::Error::msg)?;
    let service = ControlPlane::new(
        registry,
        placement,
        assignments,
        reservation_ttl,
        args.artifact_capacity,
        args.artifact_node_limit,
    )
    .map_err(anyhow::Error::msg)?;

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SchedulerServer<ControlPlane<dyn AssignmentStore>>>()
        .await;
    let scheduler_service =
        SchedulerServer::new(service).max_decoding_message_size(32 * 1024 * 1024);

    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }

    let metrics_listener = tokio::net::TcpListener::bind(args.metrics_listen)
        .await
        .with_context(|| format!("bind metrics listener {}", args.metrics_listen))?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(wait_for_signal(shutdown_tx));

    info!(
        grpc_listen = %args.grpc_listen,
        metrics_listen = %args.metrics_listen,
        node_count = args.nodes.len(),
        redis_enabled = args.redis_url.is_some(),
        tls_enabled = args.tls_cert.is_some(),
        "AgentENV Rust control plane starting"
    );

    let grpc_shutdown = wait_for_shutdown(shutdown_rx.clone());
    let metrics_shutdown = wait_for_shutdown(shutdown_rx);
    let grpc = server
        .add_service(health_service)
        .add_service(scheduler_service)
        .serve_with_shutdown(args.grpc_listen, grpc_shutdown);
    let metrics = agentenv_observability::serve_metrics(metrics_listener, metrics_shutdown);
    tokio::try_join!(async { grpc.await.context("serve gRPC") }, async {
        metrics.await.context("serve metrics")
    })?;
    Ok(())
}

fn validate_args(args: &Args) -> anyhow::Result<()> {
    if args.cluster_id.is_empty()
        || args.cluster_id.len() > 128
        || !args
            .cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("cluster-id contains invalid characters or exceeds 128 bytes");
    }
    if args.nodes.is_empty() {
        bail!("at least one --node is required");
    }
    if args.redis_url.is_none() && !args.allow_ephemeral_state {
        bail!("--redis-url is required unless --allow-ephemeral-state is explicit");
    }
    if args.assignment_ttl_seconds < args.reservation_ttl_seconds {
        bail!("assignment TTL must be at least the reservation TTL");
    }
    let tls_count = [
        args.tls_cert.is_some(),
        args.tls_key.is_some(),
        args.tls_client_ca.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if tls_count != 0 && tls_count != 3 {
        bail!("TLS cert, key, and client CA must be configured together");
    }
    if tls_count == 0 && !args.allow_insecure_transport {
        bail!("mTLS is required unless --allow-insecure-transport is explicit");
    }
    Ok(())
}

async fn load_tls(args: &Args) -> anyhow::Result<Option<ServerTlsConfig>> {
    let (Some(cert_path), Some(key_path), Some(ca_path)) =
        (&args.tls_cert, &args.tls_key, &args.tls_client_ca)
    else {
        return Ok(None);
    };
    let cert = tokio::fs::read(cert_path)
        .await
        .with_context(|| format!("read TLS certificate {}", cert_path.display()))?;
    let key = tokio::fs::read(key_path)
        .await
        .with_context(|| format!("read TLS private key {}", key_path.display()))?;
    let client_ca = tokio::fs::read(ca_path)
        .await
        .with_context(|| format!("read TLS client CA {}", ca_path.display()))?;
    Ok(Some(
        ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(client_ca)),
    ))
}

fn init_logging(filter: &str, json: bool) -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)
        .with_context(|| format!("parse log filter {filter:?}"))?;
    if json {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;
    }
    Ok(())
}

fn mib_to_bytes(mib: u64) -> anyhow::Result<u64> {
    mib.checked_mul(1024 * 1024)
        .context("resource default is too large")
}

async fn wait_for_signal(shutdown: watch::Sender<bool>) {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    warn!(%error, "Ctrl+C handler failed");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "Ctrl+C handler failed");
    }
    let _ = shutdown.send(true);
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_spec_rejects_credentials_and_invalid_ids() {
        assert!(NodeSpec::from_str("node-1=https://node.internal:8000").is_ok());
        assert!(NodeSpec::from_str("bad/id=https://node.internal").is_err());
        assert!(NodeSpec::from_str("node=https://user:pass@node.internal").is_err());
        assert!(NodeSpec::from_str("node=file:///tmp/socket").is_err());
    }
}
