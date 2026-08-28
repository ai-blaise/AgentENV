//! Node-local admission control for sandbox creation.
//!
//! Placement decides *where* a sandbox should go from a cluster view that is
//! necessarily stale — the scheduler reads a snapshot a node reported up to a
//! heartbeat interval ago. Admission decides whether the sandbox may actually
//! start *here*, from state only this node can see, and is the authority.
//! Rejecting cheaply and letting the scheduler retry elsewhere is what makes a
//! stale placement decision safe.
//!
//! # Why this cannot read the metadata store alone
//!
//! Sandbox metadata reaches the store only after the backend has been built and
//! started — after network slot allocation, image work, device acquisition and
//! the Firecracker spawn. Everything derived from the store is therefore blind
//! to the entire expensive phase of a create, and `starting_sandbox_count` is
//! *not* the number of creates in flight. Under a burst, a node can accept far
//! past its limits while every reading of its own metrics still looks healthy.
//!
//! The controller closes that gap by holding its own pending counters. A
//! reservation is added at admission and removed either when the sandbox
//! becomes visible in the store, or on drop if the create failed before that.
//! Occupancy is exact at every instant: creates the store cannot see are
//! counted by `pending`, creates it can see are counted by the store, and never
//! both.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::cfg::AdmissionConfig;
use crate::orchestrator::OrchestratorMetrics;
use crate::types::SandboxResources;

/// Why a node refused to start a sandbox.
///
/// A closed set, so it is safe to use as a metric label and stable enough for
/// a client to branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectReason {
    SandboxCount,
    StartingSandboxCount,
    AllocatedCpu,
    AllocatedMemory,
    SandboxCountIncludingPaused,
    NetworkSlots,
}

impl AdmissionRejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SandboxCount => "sandbox_count",
            Self::StartingSandboxCount => "starting_sandbox_count",
            Self::AllocatedCpu => "allocated_cpu",
            Self::AllocatedMemory => "allocated_memory",
            Self::SandboxCountIncludingPaused => "sandbox_count_including_paused",
            Self::NetworkSlots => "network_slots",
        }
    }
}

impl std::fmt::Display for AdmissionRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Capacity a node can see about itself that the metadata store does not carry.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeCapacityInputs {
    /// Network slots that can be acquired without building one, i.e. free
    /// capacity plus what is already warm in the pool.
    pub available_network_slots: usize,
}

/// Counters for creates that have been admitted but are not yet visible in the
/// metadata store.
#[derive(Debug, Default)]
struct PendingReservations {
    creates: AtomicU32,
    cpu: AtomicU32,
    memory_bytes: AtomicU64,
    network_slots: AtomicU32,
}

/// A held admission reservation.
///
/// The reservation is released exactly once: by [`AdmissionGuard::commit`] when
/// the sandbox becomes visible in the metadata store, or by `Drop` when the
/// create failed before that. Every failure path therefore returns the
/// capacity without needing to remember to.
#[derive(Debug)]
pub struct AdmissionGuard {
    pending: Arc<PendingReservations>,
    /// What this guard still owes back to `pending`.
    ///
    /// Tracked as a remaining balance rather than the original amount so that
    /// partial releases (`commit_one`) and the final release on drop cannot
    /// both subtract the same capacity. Releasing twice would wrap these
    /// counters and permanently understate the node's load.
    remaining: PendingReservations,
    released: AtomicBool,
}

impl AdmissionGuard {
    /// Releases the whole remaining reservation because the sandbox is now
    /// counted by the metadata store.
    pub fn commit(&self) {
        self.release();
    }

    /// Releases one child's share of a bulk reservation, used by fork as each
    /// child is registered. Never releases more than remains.
    pub fn commit_one(&self, resources: SandboxResources) {
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let cpu = resources.cpu_count;
        let memory = memory_bytes(resources);
        if take_u32(&self.remaining.creates, 1) > 0 {
            self.pending.creates.fetch_sub(1, Ordering::AcqRel);
        }
        let cpu = take_u32(&self.remaining.cpu, cpu);
        if cpu > 0 {
            self.pending.cpu.fetch_sub(cpu, Ordering::AcqRel);
        }
        let memory = take_u64(&self.remaining.memory_bytes, memory);
        if memory > 0 {
            self.pending
                .memory_bytes
                .fetch_sub(memory, Ordering::AcqRel);
        }
        if take_u32(&self.remaining.network_slots, 1) > 0 {
            self.pending.network_slots.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let creates = self.remaining.creates.swap(0, Ordering::AcqRel);
        let cpu = self.remaining.cpu.swap(0, Ordering::AcqRel);
        let memory = self.remaining.memory_bytes.swap(0, Ordering::AcqRel);
        let slots = self.remaining.network_slots.swap(0, Ordering::AcqRel);
        if creates > 0 {
            self.pending.creates.fetch_sub(creates, Ordering::AcqRel);
        }
        if cpu > 0 {
            self.pending.cpu.fetch_sub(cpu, Ordering::AcqRel);
        }
        if memory > 0 {
            self.pending
                .memory_bytes
                .fetch_sub(memory, Ordering::AcqRel);
        }
        if slots > 0 {
            self.pending
                .network_slots
                .fetch_sub(slots, Ordering::AcqRel);
        }
    }

    /// Marks the reservation as owing nothing, for the disabled-gate guard
    /// which never reserved anything in the first place.
    fn forget(&self) {
        self.released.store(true, Ordering::Release);
    }
}

/// Subtracts up to `amount` from `counter`, returning how much was actually
/// taken. Never wraps below zero.
fn take_u32(counter: &AtomicU32, amount: u32) -> u32 {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let take = current.min(amount);
        if take == 0 {
            return 0;
        }
        match counter.compare_exchange_weak(
            current,
            current - take,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return take,
            Err(observed) => current = observed,
        }
    }
}

fn take_u64(counter: &AtomicU64, amount: u64) -> u64 {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let take = current.min(amount);
        if take == 0 {
            return 0;
        }
        match counter.compare_exchange_weak(
            current,
            current - take,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return take,
            Err(observed) => current = observed,
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.release();
    }
}

fn memory_bytes(resources: SandboxResources) -> u64 {
    u64::from(resources.memory_mib).saturating_mul(1024 * 1024)
}

/// Cached copy of the node's own metrics.
///
/// `metrics_snapshot` walks every sandbox under the store's read lock, so
/// calling it per admission decision would make the gate itself the
/// bottleneck under exactly the burst it exists to survive. The cache is
/// short-lived; the pending counters carry the burst in between refreshes.
#[derive(Debug)]
struct CachedMetrics {
    metrics: OrchestratorMetrics,
    taken_at: Instant,
}

/// Node-local capacity gate. See the module docs.
#[derive(Debug)]
pub struct AdmissionController {
    config: AdmissionConfig,
    pending: Arc<PendingReservations>,
    cached: Mutex<Option<CachedMetrics>>,
}

impl AdmissionController {
    pub fn new(config: AdmissionConfig) -> Self {
        Self {
            config,
            pending: Arc::new(PendingReservations::default()),
            cached: Mutex::new(None),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn retry_after(&self) -> Duration {
        Duration::from_secs(self.config.retry_after_secs.max(1))
    }

    /// Reserves capacity for `count` sandboxes of `resources`.
    ///
    /// `load_metrics` supplies the node's own metrics and is only invoked when
    /// the cache has expired.
    pub async fn try_admit<F, Fut>(
        &self,
        count: u32,
        resources: SandboxResources,
        capacity: NodeCapacityInputs,
        load_metrics: F,
    ) -> Result<AdmissionGuard, AdmissionRejectReason>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<OrchestratorMetrics>>,
    {
        let cpu = resources.cpu_count.saturating_mul(count);
        let memory = memory_bytes(resources).saturating_mul(u64::from(count));
        let guard = AdmissionGuard {
            pending: Arc::clone(&self.pending),
            remaining: PendingReservations {
                creates: AtomicU32::new(count),
                cpu: AtomicU32::new(cpu),
                memory_bytes: AtomicU64::new(memory),
                network_slots: AtomicU32::new(count),
            },
            released: AtomicBool::new(false),
        };

        if !self.config.enabled {
            // Disabled: hand back a guard that reserves nothing, so callers
            // need no second code path.
            guard.forget();
            return Ok(guard);
        }

        let metrics = self.metrics(load_metrics).await;

        // Reserve first, then validate against the post-reservation totals, so
        // concurrent admissions cannot both observe pre-reservation capacity
        // and both succeed.
        self.pending.creates.fetch_add(count, Ordering::AcqRel);
        self.pending.cpu.fetch_add(cpu, Ordering::AcqRel);
        self.pending
            .memory_bytes
            .fetch_add(memory, Ordering::AcqRel);
        self.pending
            .network_slots
            .fetch_add(count, Ordering::AcqRel);

        if let Some(reason) = self.exceeded(&metrics, capacity) {
            // Dropping the guard returns the reservation.
            return Err(reason);
        }
        Ok(guard)
    }

    async fn metrics<F, Fut>(&self, load_metrics: F) -> OrchestratorMetrics
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<OrchestratorMetrics>>,
    {
        let max_age = Duration::from_millis(self.config.snapshot_max_age_ms);
        let mut cached = self.cached.lock().await;
        if let Some(entry) = cached.as_ref() {
            if entry.taken_at.elapsed() < max_age {
                return entry.metrics.clone();
            }
        }
        match load_metrics().await {
            Some(metrics) => {
                *cached = Some(CachedMetrics {
                    metrics: metrics.clone(),
                    taken_at: Instant::now(),
                });
                metrics
            }
            // A metrics failure must not open the gate, but it also must not
            // wedge the node: fall back to the last known value if there is
            // one, and otherwise to zeroes plus the pending counters, which
            // still bound a burst.
            None => cached
                .as_ref()
                .map(|entry| entry.metrics.clone())
                .unwrap_or_default(),
        }
    }

    fn exceeded(
        &self,
        metrics: &OrchestratorMetrics,
        capacity: NodeCapacityInputs,
    ) -> Option<AdmissionRejectReason> {
        let pending_creates = self.pending.creates.load(Ordering::Acquire);
        let pending_cpu = self.pending.cpu.load(Ordering::Acquire);
        let pending_memory = self.pending.memory_bytes.load(Ordering::Acquire);
        let pending_slots = self.pending.network_slots.load(Ordering::Acquire) as usize;

        let sandbox_count = metrics
            .running_sandbox_count
            .saturating_add(pending_creates);
        let starting = metrics
            .starting_sandbox_count
            .saturating_add(pending_creates);
        let allocated_cpu = metrics.allocated_cpu.saturating_add(pending_cpu);
        let allocated_memory = metrics
            .allocated_memory_bytes
            .saturating_add(pending_memory);

        if let Some(limit) = self.config.max_sandbox_count {
            if sandbox_count > limit {
                return Some(AdmissionRejectReason::SandboxCount);
            }
        }
        if let Some(limit) = self.config.max_sandbox_starting_count {
            if starting > limit {
                return Some(AdmissionRejectReason::StartingSandboxCount);
            }
        }
        if let Some(limit) = self.config.max_allocated_cpu {
            if allocated_cpu > limit {
                return Some(AdmissionRejectReason::AllocatedCpu);
            }
        }
        if let Some(limit) = self.config.max_allocated_memory_bytes {
            if allocated_memory > limit {
                return Some(AdmissionRejectReason::AllocatedMemory);
            }
        }
        if let Some(limit) = self.config.max_sandbox_count_including_paused {
            let total = sandbox_count.saturating_add(metrics.paused_sandbox_count);
            if total > limit {
                return Some(AdmissionRejectReason::SandboxCountIncludingPaused);
            }
        }
        if let Some(floor) = self.config.min_free_network_slots {
            // Slots already reserved by in-flight creates are not available to
            // this one, even though they have not been taken from the pool yet.
            let available = capacity
                .available_network_slots
                .saturating_sub(pending_slots);
            if available < floor as usize {
                return Some(AdmissionRejectReason::NetworkSlots);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resources(cpu: u32, memory_mib: u32) -> SandboxResources {
        SandboxResources {
            cpu_count: cpu,
            memory_mib,
            disk_size_mib: 0,
        }
    }

    fn config() -> AdmissionConfig {
        AdmissionConfig {
            enabled: true,
            max_sandbox_count: None,
            max_sandbox_starting_count: None,
            max_allocated_cpu: None,
            max_allocated_memory_bytes: None,
            max_sandbox_count_including_paused: None,
            min_free_network_slots: None,
            retry_after_secs: 2,
            snapshot_max_age_ms: 200,
        }
    }

    async fn empty_metrics() -> Option<OrchestratorMetrics> {
        Some(OrchestratorMetrics::default())
    }

    #[tokio::test]
    async fn disabled_controller_admits_everything() {
        let mut cfg = config();
        cfg.enabled = false;
        cfg.max_sandbox_count = Some(0);
        let controller = AdmissionController::new(cfg);

        let guard = controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await;
        assert!(guard.is_ok(), "a disabled gate must admit unconditionally");
    }

    /// The store cannot see a create until after the VM has started, so a burst
    /// of concurrent admissions all read the same zeroed metrics. Without the
    /// pending counters every one of them would be admitted.
    #[tokio::test]
    async fn pending_reservations_bound_a_burst_the_store_cannot_see() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(3);
        let controller = AdmissionController::new(cfg);

        let mut admitted = Vec::new();
        for _ in 0..3 {
            admitted.push(
                controller
                    .try_admit(
                        1,
                        resources(1, 128),
                        NodeCapacityInputs::default(),
                        empty_metrics,
                    )
                    .await
                    .expect("within limit"),
            );
        }

        let rejected = controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await;
        assert_eq!(rejected.err(), Some(AdmissionRejectReason::SandboxCount));

        // Releasing a reservation frees the capacity again.
        admitted.pop();
        controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await
            .expect("capacity returned on drop");
    }

    /// A rejected admission must not leak the reservation it took while
    /// evaluating, or the node ratchets itself closed.
    #[tokio::test]
    async fn rejected_admission_returns_its_reservation() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(1);
        let controller = AdmissionController::new(cfg);

        for _ in 0..10 {
            let rejected = controller
                .try_admit(
                    2,
                    resources(1, 128),
                    NodeCapacityInputs::default(),
                    empty_metrics,
                )
                .await;
            assert!(rejected.is_err());
        }

        controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await
            .expect("repeated rejections must not consume capacity");
    }

    /// Committing hands accounting to the metadata store; the guard must not
    /// then subtract again on drop and under-count the node.
    #[tokio::test]
    async fn commit_then_drop_releases_exactly_once() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(1);
        let controller = AdmissionController::new(cfg);

        {
            let guard = controller
                .try_admit(
                    1,
                    resources(1, 128),
                    NodeCapacityInputs::default(),
                    empty_metrics,
                )
                .await
                .expect("within limit");
            guard.commit();
        }

        assert_eq!(controller.pending.creates.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn network_slot_floor_accounts_for_in_flight_creates() {
        let mut cfg = config();
        cfg.min_free_network_slots = Some(2);
        let controller = AdmissionController::new(cfg);
        let capacity = NodeCapacityInputs {
            available_network_slots: 3,
        };

        let _first = controller
            .try_admit(1, resources(1, 128), capacity, empty_metrics)
            .await
            .expect("3 available, 1 reserved, 2 remain");

        let rejected = controller
            .try_admit(1, resources(1, 128), capacity, empty_metrics)
            .await;
        assert_eq!(rejected.err(), Some(AdmissionRejectReason::NetworkSlots));
    }

    #[tokio::test]
    async fn bulk_admission_charges_every_child() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(4);
        let controller = AdmissionController::new(cfg);

        let _guard = controller
            .try_admit(
                4,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await
            .expect("exactly at the limit");

        let rejected = controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await;
        assert_eq!(rejected.err(), Some(AdmissionRejectReason::SandboxCount));
    }

    /// A metrics failure must neither open the gate nor wedge the node.
    #[tokio::test]
    async fn metrics_failure_falls_back_without_opening_the_gate() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(1);
        let controller = AdmissionController::new(cfg);

        let _first = controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                || async { None },
            )
            .await
            .expect("first admission within limit");

        let rejected = controller
            .try_admit(
                1,
                resources(1, 128),
                NodeCapacityInputs::default(),
                || async { None },
            )
            .await;
        assert_eq!(
            rejected.err(),
            Some(AdmissionRejectReason::SandboxCount),
            "pending counters must still bound the burst when metrics are unavailable"
        );
    }

    /// A bulk reservation released child-by-child and then dropped must return
    /// exactly what it took. Subtracting the original amount again on drop
    /// would wrap the counters and permanently understate the node's load,
    /// which silently disables the gate.
    #[tokio::test]
    async fn partial_commits_then_drop_release_exactly_once() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(4);
        let controller = AdmissionController::new(cfg);

        {
            let guard = controller
                .try_admit(
                    4,
                    resources(2, 256),
                    NodeCapacityInputs::default(),
                    empty_metrics,
                )
                .await
                .expect("within limit");

            // Two children registered, two failed to start.
            guard.commit_one(resources(2, 256));
            guard.commit_one(resources(2, 256));
        }

        assert_eq!(controller.pending.creates.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.cpu.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.memory_bytes.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.network_slots.load(Ordering::Acquire), 0);

        // Capacity is fully available again.
        controller
            .try_admit(
                4,
                resources(1, 128),
                NodeCapacityInputs::default(),
                empty_metrics,
            )
            .await
            .expect("all capacity returned");
    }

    /// Committing every child individually must also leave the counters at
    /// zero once the guard drops.
    #[tokio::test]
    async fn committing_every_child_leaves_no_residue() {
        let mut cfg = config();
        cfg.max_sandbox_count = Some(3);
        let controller = AdmissionController::new(cfg);

        {
            let guard = controller
                .try_admit(
                    3,
                    resources(1, 128),
                    NodeCapacityInputs::default(),
                    empty_metrics,
                )
                .await
                .expect("within limit");
            for _ in 0..3 {
                guard.commit_one(resources(1, 128));
            }
        }

        assert_eq!(controller.pending.creates.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.cpu.load(Ordering::Acquire), 0);
    }

    /// commit_one must never release more than the guard still owes, even if
    /// called more times than the reservation covered.
    #[tokio::test]
    async fn over_committing_cannot_underflow_the_pool() {
        let controller = AdmissionController::new(config());

        {
            let guard = controller
                .try_admit(
                    1,
                    resources(1, 128),
                    NodeCapacityInputs::default(),
                    empty_metrics,
                )
                .await
                .expect("admitted");
            for _ in 0..5 {
                guard.commit_one(resources(1, 128));
            }
        }

        assert_eq!(controller.pending.creates.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.cpu.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending.memory_bytes.load(Ordering::Acquire), 0);
    }
}
