//! What the metadata store costs when several things want it at once.
//!
//! The store is one `RwLock<HashMap>` shared by every path on the node: the
//! create path takes it exclusively, and the heartbeat, the metrics snapshot
//! and the eviction sweep all walk the whole map under a read lock. At one
//! sandbox that is obviously fine. The question is whether it is still fine at
//! the sandbox counts a dense node is meant to reach, and the honest answer
//! before this was that nobody had measured it.
//!
//! These are ignored by default because they are measurements, not assertions:
//! their numbers depend on the host, and a threshold tuned to one machine
//! fails on another for reasons that have nothing to do with the code. Run
//! them deliberately and compare against the recorded baseline in
//! `docs/src/internals/metadata-store-baseline.md`.

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::orchestrator::store::{InMemoryMetadataStore, MetadataStore};
    use crate::orchestrator::{SandboxMetadata, SandboxState};
    use crate::types::SandboxId;

    fn sandbox(state: SandboxState) -> SandboxMetadata {
        SandboxMetadata {
            state,
            ..SandboxMetadata::default()
        }
    }

    async fn store_with(count: usize) -> (Arc<InMemoryMetadataStore>, Vec<SandboxId>) {
        let store = Arc::new(InMemoryMetadataStore::default());
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            let metadata = sandbox(SandboxState::Running);
            ids.push(metadata.id);
            store.add(metadata).await.expect("add");
        }
        (store, ids)
    }

    /// How a full scan — what every heartbeat does to build its roster — grows
    /// with the number of sandboxes on the node.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "measurement, not an assertion; see docs/src/internals/metadata-store-baseline.md"]
    async fn measure_full_scan_cost_by_sandbox_count() {
        for count in [16_usize, 128, 512, 2048] {
            let (store, _ids) = store_with(count).await;

            let iterations = 200;
            let started = Instant::now();
            for _ in 0..iterations {
                let mut seen = 0_usize;
                store.list_with_callback(|_| seen += 1).await.expect("scan");
                assert_eq!(seen, count);
            }
            let per_scan = started.elapsed() / iterations;

            println!(
                "sandboxes={count:5} scan={:>9.3}us per_sandbox={:>7.3}ns",
                per_scan.as_secs_f64() * 1e6,
                per_scan.as_secs_f64() * 1e9 / count as f64,
            );
        }
    }

    /// What a create costs while the node's read paths are running.
    ///
    /// The interesting number is not the raw write rate but how much of it
    /// survives contention: an exclusive lock behind continuous whole-map
    /// scans is the shape that degrades quietly as a node fills up.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "measurement, not an assertion; see docs/src/internals/metadata-store-baseline.md"]
    async fn measure_write_throughput_under_concurrent_scans() {
        for readers in [0_usize, 1, 4, 16] {
            let (store, _ids) = store_with(512).await;
            let stop = Arc::new(AtomicBool::new(false));
            let scans = Arc::new(AtomicU64::new(0));

            let mut handles = Vec::with_capacity(readers);
            for _ in 0..readers {
                let store = Arc::clone(&store);
                let stop = Arc::clone(&stop);
                let scans = Arc::clone(&scans);
                handles.push(tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let mut seen = 0_usize;
                        let _ = store.list_with_callback(|_| seen += 1).await;
                        scans.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }

            let writes = 2000;
            let started = Instant::now();
            for _ in 0..writes {
                store
                    .add(sandbox(SandboxState::Creating))
                    .await
                    .expect("add");
            }
            let elapsed = started.elapsed();

            stop.store(true, Ordering::Relaxed);
            for handle in handles {
                let _ = handle.await;
            }

            println!(
                "readers={readers:2} writes/s={:>9.0} per_write={:>8.3}us scans={}",
                writes as f64 / elapsed.as_secs_f64(),
                elapsed.as_secs_f64() * 1e6 / writes as f64,
                scans.load(Ordering::Relaxed),
            );
        }
    }

    /// Whether a long read starves writers.
    ///
    /// Tokio's `RwLock` is write-preferring, so a writer waiting behind an
    /// in-progress read should not be overtaken by readers that arrive after
    /// it. This measures the worst wait a create sees while scans are
    /// continuous — the number that decides whether a full node's create
    /// latency is bounded or merely usually fine.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "measurement, not an assertion; see docs/src/internals/metadata-store-baseline.md"]
    async fn measure_worst_write_wait_under_continuous_scans() {
        let (store, _ids) = store_with(2048).await;
        let stop = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            handles.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let mut seen = 0_usize;
                    let _ = store.list_with_callback(|_| seen += 1).await;
                }
            }));
        }

        // Let the readers get going, so the measurement is of a contended
        // lock rather than of an idle one.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut worst = Duration::ZERO;
        let mut total = Duration::ZERO;
        let samples = 500;
        for _ in 0..samples {
            let started = Instant::now();
            store
                .add(sandbox(SandboxState::Creating))
                .await
                .expect("add");
            let waited = started.elapsed();
            total += waited;
            worst = worst.max(waited);
        }

        stop.store(true, Ordering::Relaxed);
        for handle in handles {
            let _ = handle.await;
        }

        println!(
            "mean_write={:>8.3}us worst_write={:>8.3}us",
            total.as_secs_f64() * 1e6 / samples as f64,
            worst.as_secs_f64() * 1e6,
        );
    }
}
