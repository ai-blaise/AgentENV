//! What building one network slot costs.
//!
//! A slot is a network namespace, a veth pair, a tap device, addresses, routes
//! and a namespace-local `iptables-restore`. Nearly all of it is serialized on
//! RTNL, so this is the ceiling on how fast a node can hand out sandboxes with
//! networking, and it is what a warm pool pre-pays per slot it holds.
//!
//! The warm pool is turned **off** for the measurement. With it on, `release`
//! returns the slot to the pool and the next `allocate_any` takes the same one
//! straight back, so the loop measures a queue pop -- around a microsecond,
//! four orders of magnitude off, and worse than useless because it looks like
//! an answer.
//!
//! Needs `CAP_NET_ADMIN` and a kernel that permits netns creation;
//! `make bench-network-slot` supplies the first. Without either, every
//! allocation fails and this says so instead of reporting a number.
use std::time::{Duration, Instant};

use agentenv::sandbox::network_slot_batch_round_trip;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const SAMPLE_SIZE: usize = 10;
const WARM_UP: Duration = Duration::from_millis(500);
const MEASUREMENT: Duration = Duration::from_secs(10);

/// Points the global config at a scratch tree with the warm pool disabled.
///
/// Returns the tempdir, which has to outlive the run: the config resolves the
/// namespace directory relative to it.
fn init_isolated_config() -> anyhow::Result<tempfile::TempDir> {
    let temp = tempfile::tempdir()?;
    let config_path = temp.path().join("bench.toml");
    std::fs::write(
        &config_path,
        format!(
            "home_path = \"{}\"\n\
             \n\
             [pool.network]\n\
             # Off, or every iteration after the first is a pool pop rather than\n\
             # a slot build, and the benchmark measures the wrong thing.\n\
             enabled = false\n\
             maintenance_enabled = false\n\
             startup_prewarm = false\n",
            temp.path().display()
        ),
    )?;
    agentenv::cfg::ConfigManager::init_global_from_path(&config_path)?;
    Ok(temp)
}

fn bench_slot_lifecycle(c: &mut Criterion) {
    let _config = match init_isolated_config() {
        Ok(temp) => temp,
        Err(error) => {
            eprintln!("network-slot benchmark: skipping; config would not load: {error:#}");
            return;
        }
    };

    if let Err(error) = network_slot_batch_round_trip(1) {
        eprintln!(
            "network-slot benchmark: skipping; this host cannot build network slots: {error:#}"
        );
        return;
    }

    let mut group = c.benchmark_group("network_slot");
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);
    group.measurement_time(MEASUREMENT);

    // One slot, built and torn down. This is what a create pays on a pool miss
    // and what a bank of depth B pays B times to fill.
    group.throughput(Throughput::Elements(1));
    group.bench_function("build_teardown", |b| {
        b.iter(|| {
            network_slot_batch_round_trip(1).expect("slot round trip failed mid-measurement");
        })
    });

    // Batches, to see whether the cost stays linear as slots coexist. It should
    // be close to it, because the work is serialized on RTNL; a departure is
    // the thing worth knowing before deepening a bank.
    for batch in [4_usize, 16] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(
            BenchmarkId::new("build_teardown_batch", batch),
            &batch,
            |b, &batch| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        network_slot_batch_round_trip(batch).expect("slot batch round trip");
                    }
                    start.elapsed()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_slot_lifecycle);
criterion_main!(benches);
