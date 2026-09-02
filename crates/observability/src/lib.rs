use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use axum::http::{header::CONTENT_TYPE, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, Router};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

const TEXT_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

// Keep in sync with services/shared/observability/buckets.go.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    120.0, 300.0, 600.0, 900.0, 1200.0, 1800.0,
];

/// Fine-grained buckets for the two sampled ZFile read-path duration
/// histograms: 1µs..500µs in 1/2/5 steps, then the standard
/// `DURATION_BUCKETS` ladder from 1ms to 30s. Values are seconds, strictly
/// increasing; the exporter appends the implicit `+Inf` bucket itself.
/// Attached only to the two exact metric names in
/// [`init_prometheus_recorder`] via [`Matcher::Full`], so every other (and
/// any future) histogram keeps `DURATION_BUCKETS`.
const ZFILE_FINE_DURATION_BUCKETS: &[f64] = &[
    0.000_001, 0.000_002, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250,
    0.000_500, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
    30.0,
];

/// Buckets for histograms whose unit is bytes, in 1/2/5 steps from 1 MiB to
/// 64 GiB. `DURATION_BUCKETS` is a ladder of seconds that tops out at 1800, so
/// a byte-valued histogram left on it lands every observation in `+Inf` and
/// reports no distribution at all.
const BYTE_BUCKETS: &[f64] = &[
    1_048_576.0,
    2_097_152.0,
    5_242_880.0,
    10_485_760.0,
    26_214_400.0,
    52_428_800.0,
    104_857_600.0,
    262_144_000.0,
    536_870_912.0,
    1_073_741_824.0,
    2_147_483_648.0,
    5_368_709_120.0,
    10_737_418_240.0,
    26_843_545_600.0,
    68_719_476_736.0,
];

/// Buckets for histograms whose unit is MiB of guest memory, covering the
/// sandbox sizes this fleet runs. Same reason as [`BYTE_BUCKETS`]: on the
/// seconds ladder every sandbox at or above 1800 MiB is indistinguishable.
const MIB_BUCKETS: &[f64] = &[
    128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131_072.0,
];

/// Histograms that do not measure seconds, with the ladder each one needs.
/// Every other histogram in the workspace is a `*_duration_seconds` and keeps
/// [`DURATION_BUCKETS`].
const NON_DURATION_BUCKETS: &[(&str, &[f64])] = &[
    ("agentenv_memory_snapshot_layer_bytes", BYTE_BUCKETS),
    ("agentenv_memory_control_plug_target_mib", MIB_BUCKETS),
];

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// The recorder configuration this process exports with.
///
/// Split out from [`init_prometheus_recorder`] because installing a recorder
/// is a once-per-process action: this is the part a test can build and render
/// without claiming the global slot.
fn configured_builder() -> anyhow::Result<PrometheusBuilder> {
    let mut builder = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)?
        // Per-metric overrides take precedence over the global buckets
        // above (full match beats the default); both ZFile histograms share
        // the same microsecond-resolution bucket set.
        .set_buckets_for_metric(
            Matcher::Full("agentenv_overlaybd_zfile_pread_duration_seconds".to_owned()),
            ZFILE_FINE_DURATION_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("agentenv_overlaybd_zfile_decompress_duration_seconds".to_owned()),
            ZFILE_FINE_DURATION_BUCKETS,
        )?;
    for (metric, buckets) in NON_DURATION_BUCKETS {
        builder = builder.set_buckets_for_metric(Matcher::Full((*metric).to_owned()), buckets)?;
    }
    Ok(builder)
}

pub fn init_prometheus_recorder() -> anyhow::Result<()> {
    // Startup code can call init more than once in tests; only a true
    // concurrent first initialization is treated as an error.
    if PROMETHEUS_HANDLE.get().is_some() {
        return Ok(());
    }

    let handle = configured_builder()?.install_recorder()?;
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let upkeep = handle.clone();
        runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                upkeep.run_upkeep();
            }
        });
    }
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow::anyhow!("prometheus recorder was initialized concurrently"))?;
    Ok(())
}

pub async fn metrics_handler() -> Response {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => ([(CONTENT_TYPE, TEXT_CONTENT_TYPE)], handle.render()).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "prometheus recorder is not initialized",
        )
            .into_response(),
    }
}

pub async fn serve_metrics(
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        Router::new().route("/metrics", get(metrics_handler)),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

#[cfg(test)]
mod tests {
    use super::{configured_builder, NON_DURATION_BUCKETS};

    /// The first bucket with a finite `le` that caught an observation, or
    /// `None` when only the implicit `+Inf` bucket did.
    fn finite_bucket_hit(rendered: &str, metric: &str) -> Option<String> {
        let prefix = format!("{metric}_bucket{{");
        rendered
            .lines()
            .filter(|line| line.starts_with(&prefix) && !line.contains("le=\"+Inf\""))
            .find(|line| !line.ends_with(" 0"))
            .map(str::to_owned)
    }

    #[test]
    fn a_byte_valued_histogram_does_not_land_entirely_in_the_infinity_bucket() {
        // The default ladder is seconds and stops at 1800, so a histogram of
        // bytes or of guest MiB left on it puts every observation past the
        // last finite bucket: `_sum` and `_count` survive, the distribution
        // and every quantile derived from it do not.
        let recorder = configured_builder()
            .expect("the exported recorder configuration must build")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            // A memory layer around 400 MB and a 4 GiB sandbox: ordinary
            // values for both metrics, both far past 1800.
            metrics::histogram!("agentenv_memory_snapshot_layer_bytes").record(419_430_400.0);
            metrics::histogram!("agentenv_memory_control_plug_target_mib").record(4096.0);
        });

        let rendered = handle.render();
        for (metric, _) in NON_DURATION_BUCKETS {
            assert!(
                finite_bucket_hit(&rendered, metric).is_some(),
                "{metric} landed only in +Inf, so it reports no distribution:\n{rendered}"
            );
        }
    }

    #[test]
    fn duration_histograms_keep_the_shared_seconds_ladder() {
        // The counterpart: a per-metric override must not leak onto anything
        // else, so a millisecond duration still lands in a millisecond bucket.
        let recorder = configured_builder()
            .expect("the exported recorder configuration must build")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("agentenv_memory_control_pass_duration_seconds").record(0.004);
        });

        let rendered = handle.render();
        let hit = finite_bucket_hit(&rendered, "agentenv_memory_control_pass_duration_seconds")
            .expect("a 4ms duration must land in a finite bucket");
        assert!(
            hit.contains("le=\"0.005\""),
            "expected the 5ms bucket, got: {hit}"
        );
    }
}
