use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, Result};
#[cfg(test)]
use index_set::BitSet;
use index_set::{slot_count, AtomicBitSet, SharedBitSet};
use ipnetwork::Ipv4Network;
use nix::libc;
use std::time::Duration;

use tracing::{debug, info, trace, warn};
use warm_pool::{PoolConfig, PoolMaintenanceAction, WarmPool};

/// How often [`NetworkManager::prime`] rechecks the pool while filling.
const POOL_PRIME_POLL_INTERVAL: Duration = Duration::from_millis(50);

use crate::observability::prometheus::MetricGuard;

use super::egress_proxy::EgressProxy;
use super::iptables_util::{
    apply_iptables_commands, iptables_backend, IptablesRestoreCommand, OpenFailurePolicy,
};
use super::{NetworkAddressPlan, NetworkError, Slot, HOST_VETH_PREFIX, MAX_SLOTS};

const CONFLICT_SAMPLE_LIMIT: usize = 5;
/// Latency of a whole slot operation: what the throughput harness reads its
/// percentiles from.
const SLOT_OPERATION_DURATION: &str = "agentenv_network_slot_operation_duration_seconds";
const ERR_SHUTTING_DOWN: &str = "Network manager is shutting down";

/// Publication state for the five global host rules.
///
/// A module of its own so `installed` has exactly one writer: publishing the
/// flag first and applying afterwards let a concurrent filler build a slot
/// whose namespace SNATs into the host-interaction CIDR while the host
/// INPUT/FORWARD/MASQUERADE rules that make that traffic legal were still
/// being written — rare with one filler, routine once refill runs several at
/// a time. Outside this module the flag can only be raised by completing an
/// apply.
mod host_iptables_gate {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use anyhow::Result;

    pub(super) struct GlobalHostIptables {
        installed: AtomicBool,
        install_gate: Mutex<()>,
    }

    impl GlobalHostIptables {
        pub(super) const fn new() -> Self {
            Self {
                installed: AtomicBool::new(false),
                install_gate: Mutex::new(()),
            }
        }

        pub(super) fn is_installed(&self) -> bool {
            self.installed.load(Ordering::Acquire)
        }

        /// Runs `apply` once for the process, with losers blocked until it
        /// commits.
        ///
        /// A failed apply leaves the flag down and the gate open, so the next
        /// caller retries: the rules are the precondition for every slot, and a
        /// node that gave up on them once would serve broken sandboxes forever.
        pub(super) fn install_once(&self, apply: impl FnOnce() -> Result<()>) -> Result<()> {
            if self.is_installed() {
                return Ok(());
            }

            let _gate = self
                .install_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.is_installed() {
                return Ok(());
            }

            apply()?;
            self.installed.store(true, Ordering::Release);
            Ok(())
        }

        /// Clears the flag, reporting whether the rules were installed.
        pub(super) fn take_installed(&self) -> bool {
            self.installed.swap(false, Ordering::AcqRel)
        }
    }
}

use host_iptables_gate::GlobalHostIptables;

static HOST_IPTABLES: GlobalHostIptables = GlobalHostIptables::new();

/// How long shutdown waits for detached slot releases to finish.
const BACKGROUND_RELEASE_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Slot releases handed off by `Drop`.
///
/// `Drop` cannot await, and releasing inline parks whatever thread runs it for
/// a veth delete plus an umount loop — on the sandbox drop path, a Tokio
/// worker. Detaching the work outright is worse: between the handoff and its
/// completion the slot is in neither the warm pool nor the bitmap, so a
/// shutdown racing it leaks the veth and the namespace mount, and the next
/// boot's `reserve_existing_host_veth_slots` turns that leak into a
/// permanently burned slot index. Shutdown therefore waits for them.
struct BackgroundReleases {
    tally: Mutex<ReleaseTally>,
    drained: std::sync::Condvar,
}

/// Releases handed to the pool, outstanding and in total.
///
/// The total is what distinguishes a release that was tracked and has since
/// finished from one that was never handed here at all: `in_flight` reads zero
/// for both.
struct ReleaseTally {
    in_flight: usize,
    handed_off: u64,
}

impl BackgroundReleases {
    const fn new() -> Self {
        Self {
            tally: Mutex::new(ReleaseTally {
                in_flight: 0,
                handed_off: 0,
            }),
            drained: std::sync::Condvar::new(),
        }
    }

    /// Runs `release` on the blocking pool, or inline when there is no runtime
    /// to hand it to — `Drop` also runs from plain threads and from the
    /// process-exit hook.
    fn spawn(&'static self, release: impl FnOnce() + Send + 'static) {
        {
            let mut tally = self
                .tally
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tally.in_flight += 1;
            tally.handed_off += 1;
        }

        let run = move || {
            release();
            self.finish();
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(run);
            }
            Err(_) => run(),
        }
    }

    fn finish(&self) {
        let mut tally = self
            .tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tally.in_flight = tally.in_flight.saturating_sub(1);
        if tally.in_flight == 0 {
            self.drained.notify_all();
        }
    }

    /// Waits for the outstanding releases, reporting whether they all finished.
    fn drain(&self, timeout: Duration) -> bool {
        let tally = self
            .tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (tally, _) = self
            .drained
            .wait_timeout_while(tally, timeout, |tally| tally.in_flight > 0)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tally.in_flight == 0
    }

    /// Every release handed to the pool so far.
    #[cfg(test)]
    fn handed_off(&self) -> u64 {
        self.tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .handed_off
    }
}

static BACKGROUND_RELEASES: BackgroundReleases = BackgroundReleases::new();

/// Shortest pause after a failed refill batch.
const FILL_BACKOFF_MIN: Duration = Duration::from_millis(50);
/// Longest pause after repeated refill failures.
const FILL_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Longest a declined maintenance cycle sleeps before returning.
///
/// The worker re-enters immediately while the pool is below its target, so the
/// pause has to happen inside the cycle. Capping it keeps the stop flag —
/// checked between cycles — visible within this long at shutdown.
const FILL_DECLINE_SLEEP_MAX: Duration = Duration::from_millis(100);

/// Whether a refill batch may run now.
///
/// A failing fill is otherwise an unthrottled spin: the maintenance worker
/// recomputes its action, still finds the pool below target, and re-enters with
/// no sleep — forking `iptables-restore` and running a dozen RTNL operations
/// per iteration for as long as whatever broke stays broken. Exhaustion is
/// worse than slow: no amount of retrying produces a slot index that is not
/// there, so it is latched off entirely until one is returned.
#[derive(Debug)]
struct FillGate {
    next_allowed_at_millis: AtomicU64,
    backoff_millis: AtomicU64,
    slots_exhausted: AtomicBool,
}

impl FillGate {
    const fn new() -> Self {
        Self {
            next_allowed_at_millis: AtomicU64::new(0),
            backoff_millis: AtomicU64::new(0),
            slots_exhausted: AtomicBool::new(false),
        }
    }

    /// `None` when a fill may run, otherwise how long the caller should wait.
    fn blocked_for(&self, now_millis: u64) -> Option<Duration> {
        if self.slots_exhausted.load(Ordering::Acquire) {
            // Not a duration: only a returned slot can clear this.
            return Some(FILL_DECLINE_SLEEP_MAX);
        }
        let next = self.next_allowed_at_millis.load(Ordering::Acquire);
        (next > now_millis).then(|| Duration::from_millis(next - now_millis))
    }

    fn record_failure(&self, now_millis: u64) {
        let previous = self.backoff_millis.load(Ordering::Acquire);
        let next = if previous == 0 {
            FILL_BACKOFF_MIN.as_millis() as u64
        } else {
            (previous.saturating_mul(2)).min(FILL_BACKOFF_MAX.as_millis() as u64)
        };
        self.backoff_millis.store(next, Ordering::Release);
        self.next_allowed_at_millis
            .store(now_millis.saturating_add(next), Ordering::Release);
    }

    fn record_success(&self) {
        self.backoff_millis.store(0, Ordering::Release);
        self.next_allowed_at_millis.store(0, Ordering::Release);
    }

    fn note_slots_exhausted(&self) {
        self.slots_exhausted.store(true, Ordering::Release);
    }

    fn note_slot_returned(&self) {
        self.slots_exhausted.store(false, Ordering::Release);
    }
}

static MANAGER: OnceLock<NetworkManager> = OnceLock::new();

extern "C" fn network_manager_exit_hook() {
    let _ = std::panic::catch_unwind(|| {
        if let Some(manager) = NetworkManager::global_if_initialized() {
            if let Err(err) = manager.shutdown_inner(true) {
                warn!(error = %err, "network manager shutdown on process exit failed");
            }
        }
    });
}

fn register_process_exit_hook(handler: extern "C" fn()) -> i32 {
    // SAFETY: `handler` uses C ABI and `atexit` accepts callbacks with signature `extern "C" fn()`.
    unsafe { libc::atexit(handler) }
}

struct NetworkManagerConfig {
    pool: PoolConfig,
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
    fill_concurrency: usize,
}

pub(crate) struct NetworkManager {
    /// Bitmap tracking allocated slots.
    allocated: AtomicBitSet<{ slot_count::from_bits(MAX_SLOTS) }>,

    /// How many bits `allocated` holds.
    ///
    /// Carried alongside the bitmap rather than read out of it: `AtomicBitSet`
    /// exposes no popcount that does not walk all 512 words, and the admission
    /// gate reads this on every create. Maintained only by
    /// [`NetworkManager::take_slot_bit`], [`NetworkManager::take_next_slot_bit`]
    /// and [`NetworkManager::free_slot_bit`], which are the only places a bit
    /// moves.
    allocated_count: AtomicUsize,

    /// Warm slots ready for immediate reuse.
    pool: WarmPool<Slot>,

    /// Configured address plan for newly allocated network slots.
    address_plan: NetworkAddressPlan,

    /// Directory containing persistent namespace bind mounts.
    netns_dir: PathBuf,

    /// Namespace-local transparent proxy for egress capabilities that require
    /// connection inspection or mediation.
    egress_proxy: Arc<EgressProxy>,

    /// Rejects new allocations once shutdown cleanup starts.
    shutting_down: AtomicBool,

    /// Slots built concurrently per refill batch.
    fill_concurrency: usize,

    /// Throttles refill after a failure, and stops it entirely when the slot
    /// space is exhausted.
    fill_gate: FillGate,

    /// Monotonic base for [`FillGate`]'s millisecond deadlines.
    started_at: std::time::Instant,
}

/// Network slot capacity as admission control needs to read it.
///
/// `pooled` slots keep their bitmap bit while they sit in the warm pool, so
/// `allocated` alone understates what is immediately available: a node with a
/// full pool and no running sandboxes would look saturated. Admission must
/// compare against [`NetworkSlotCapacity::available`], which counts a pooled
/// slot as free because acquiring it costs no netlink work at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NetworkSlotCapacity {
    pub total: usize,
    pub allocated: usize,
    pub pooled: usize,
}

impl NetworkSlotCapacity {
    pub fn available(&self) -> usize {
        self.total
            .saturating_sub(self.allocated)
            .saturating_add(self.pooled)
    }
}

impl NetworkManager {
    /// Global network manager for slot allocation across all sandboxes.
    pub fn global() -> &'static Self {
        let manager = MANAGER.get_or_init(|| {
            // Register a process exit hook to clean up network resources on shutdown.
            // This is necessary because the global manager is a static singleton and
            // does not run Drop handlers at exit.
            // Without this hook, this would result in a slot resource leak during testing,
            // benchmarking, and other scenarios that do not follow the graceful shutdown path.
            let rc = register_process_exit_hook(network_manager_exit_hook);
            if rc != 0 {
                warn!(
                    code = rc,
                    "failed to register network manager process-exit hook"
                );
            }

            let cfg = crate::cfg::ConfigManager::global_config();
            Self::with_config(NetworkManagerConfig {
                pool: cfg.network_pool_config(),
                address_plan: NetworkAddressPlan::from_config(&cfg.network)
                    .expect("validated network.internal config should produce an address plan"),
                netns_dir: cfg.runtime_path.join("netns"),
                fill_concurrency: cfg.network_pool_fill_concurrency(),
            })
        });

        manager.ensure_pool_maintenance_worker_started();
        manager
    }

    /// Returns the global network manager only if it has already been initialized.
    ///
    /// This is useful for shutdown paths that must avoid side effects (like host
    /// inspection logs) when networking was never used in the current process.
    pub fn global_if_initialized() -> Option<&'static Self> {
        MANAGER.get()
    }

    #[cfg(test)]
    pub(crate) fn new(
        maintenance_enabled: bool,
        low_watermark: usize,
        high_watermark: usize,
    ) -> Self {
        Self::with_config(NetworkManagerConfig {
            pool: PoolConfig {
                low_watermark,
                high_watermark,
                maintenance_enabled,
                startup_prewarm: true,
            },
            address_plan: NetworkAddressPlan::default(),
            netns_dir: std::env::temp_dir().join("aenv-network-tests/netns"),
            fill_concurrency: 1,
        })
    }

    fn with_config(config: NetworkManagerConfig) -> Self {
        let egress_proxy = EgressProxy::new();
        let manager = Self {
            allocated: AtomicBitSet::new(),
            allocated_count: AtomicUsize::new(0),
            pool: WarmPool::named(config.pool, "network"),
            address_plan: config.address_plan,
            netns_dir: config.netns_dir,
            egress_proxy,
            shutting_down: AtomicBool::new(false),
            fill_concurrency: config.fill_concurrency.max(1),
            fill_gate: FillGate::new(),
            started_at: std::time::Instant::now(),
        };

        // Reserve slot 0 (invalid for IP addresses)
        let _ = manager.take_slot_bit(0);
        let existing_slots = manager.reserve_existing_host_veth_slots();
        manager.log_external_conflicts(&existing_slots);

        if let Err(e) = manager.install_global_host_iptables() {
            warn!(error = %e, "failed to install global host iptables rules during network manager init");
        }

        manager
    }

    /// Fills the warm slot pool toward its low watermark before serving traffic.
    ///
    /// Slot creation measures ~43ms and does not speed up past four in
    /// parallel, so a node that starts and is immediately handed a burst of
    /// creates spends most of a second building slots inside the critical path.
    /// The maintenance worker refills asynchronously, which fixes the steady
    /// state but not the first burst — that is what this is for.
    ///
    /// Never an error: a partially warm pool is slower, not broken, and the
    /// cold path stays available. Blocking startup on it would trade a latency
    /// problem for an availability one.
    pub async fn prime(timeout: Duration) -> Result<()> {
        let Some(manager) = Self::global_if_initialized() else {
            debug!("network manager not initialized; skipping slot prime");
            return Ok(());
        };
        manager.prime_pool(timeout).await
    }

    async fn prime_pool(&self, timeout: Duration) -> Result<()> {
        let config = self.pool.config();
        if !config.maintenance_enabled {
            debug!("network pool maintenance disabled; skipping slot prime");
            return Ok(());
        }
        if !config.startup_prewarm {
            debug!("network pool startup prewarm disabled; skipping slot prime");
            return Ok(());
        }

        let target = config.low_watermark;
        if target == 0 || self.pool.len() >= target {
            return Ok(());
        }

        info!(
            low_watermark = target,
            current = self.pool.len(),
            timeout_ms = timeout.as_millis(),
            "priming network slot pool"
        );

        let started = std::time::Instant::now();
        self.pool.request_maintenance();
        loop {
            if self.pool.len() >= target {
                info!(
                    warm = self.pool.len(),
                    elapsed_ms = started.elapsed().as_millis(),
                    "network slot pool primed"
                );
                return Ok(());
            }
            if started.elapsed() >= timeout {
                warn!(
                    warm = self.pool.len(),
                    target, "network slot pool prime timed out; continuing with partial warm-up"
                );
                return Ok(());
            }
            tokio::time::sleep(POOL_PRIME_POLL_INTERVAL).await;
        }
    }

    fn shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Takes the bit for `idx`, keeping [`Self::allocated_count`] in step.
    ///
    /// Returns what the bitmap returns: `Some(false)` when this call took the
    /// bit, `Some(true)` when it was already taken, `None` when `idx` is out of
    /// range. Only the transition counts, so a duplicate insert cannot inflate
    /// the total.
    fn take_slot_bit(&self, idx: usize) -> Option<bool> {
        let was_set = self.allocated.insert(idx)?;
        if !was_set {
            self.allocated_count.fetch_add(1, Ordering::Relaxed);
        }
        Some(was_set)
    }

    /// Takes the next free bit, keeping [`Self::allocated_count`] in step.
    ///
    /// Exhaustion is latched here rather than at the call site: the bitmap is
    /// where it is discovered, and every caller wants refill to stop until a
    /// slot comes back.
    fn take_next_slot_bit(&self) -> Option<usize> {
        let Some(idx) = self.allocated.set_next_free_bit() else {
            self.fill_gate.note_slots_exhausted();
            return None;
        };
        self.allocated_count.fetch_add(1, Ordering::Relaxed);
        Some(idx)
    }

    /// Releases the bit for `idx`, keeping [`Self::allocated_count`] in step.
    ///
    /// Returns `Some(true)` when this call released a set bit, `Some(false)`
    /// when it was already clear, `None` when `idx` is out of range.
    fn free_slot_bit(&self, idx: usize) -> Option<bool> {
        let was_set = self.allocated.remove(idx)?;
        if was_set {
            self.allocated_count.fetch_sub(1, Ordering::Relaxed);
            self.fill_gate.note_slot_returned();
        }
        Some(was_set)
    }

    /// Current slot capacity. See [`NetworkSlotCapacity`] for why pooled slots
    /// count as available rather than as consumed.
    pub(crate) fn slot_capacity(&self) -> NetworkSlotCapacity {
        NetworkSlotCapacity {
            total: MAX_SLOTS,
            allocated: self.allocated_count.load(Ordering::Relaxed),
            pooled: self.pool.len(),
        }
    }

    fn ensure_pool_maintenance_worker_started(&'static self) {
        self.pool.start_maintenance_worker(move || {
            if let Err(err) = self.run_pool_maintenance_cycle() {
                warn!(error = %err, "network pool maintenance cycle failed");
            }
        });
    }

    /// Allocates a network slot with the given index atomically.
    #[cfg(test)]
    pub fn allocate_slot(&self, idx: u32) -> Result<Slot> {
        if idx == 0 || idx as usize >= MAX_SLOTS {
            return Err(anyhow!(
                "Slot index {} out of range (max {})",
                idx,
                MAX_SLOTS - 1
            ));
        }

        match self.take_slot_bit(idx as usize) {
            // insert returns true when the bit WAS already set (duplicate)
            Some(false) => Ok(Slot::new(
                idx,
                self.address_plan,
                self.netns_dir.clone(),
                Arc::clone(&self.egress_proxy),
            )
            .expect("Slot index just validated")),
            Some(true) => Err(anyhow!("Slot {} already allocated", idx)),
            None => Err(anyhow!("Slot index {} out of range", idx)),
        }
    }

    #[cfg(test)]
    pub(crate) fn allocate_test_slot(&self) -> Result<Slot> {
        for idx in 1..MAX_SLOTS {
            if !self.allocated.has(idx) {
                return self.allocate_slot(idx as u32);
            }
        }
        Err(anyhow!("No available test slots"))
    }

    /// Release only the bitmap bit for a slot index.
    fn release_slot_bit(&self, idx: u32) -> Result<()> {
        if idx == 0 || idx as usize >= MAX_SLOTS {
            return Err(anyhow!("Slot index {} out of range", idx));
        }

        match self.free_slot_bit(idx as usize) {
            // remove returns true when the bit WAS set (successful release)
            Some(true) => Ok(()),
            Some(false) => Err(anyhow!("Slot {} not allocated", idx)),
            None => Err(anyhow!("Slot index {} out of range", idx)),
        }
    }

    fn cleanup_slot_and_release_bit(&self, slot: Slot) -> Result<()> {
        self.cleanup_slot_and_release_bit_inner(slot, false)
    }

    fn cleanup_slot_and_release_bit_inner(&self, slot: Slot, sync_cleanup: bool) -> Result<()> {
        let mut metric = MetricGuard::operation(SLOT_OPERATION_DURATION, "cleanup");
        let result = self.cleanup_slot_and_release_bit_timed(slot, sync_cleanup);
        metric.finish(&result);
        result
    }

    fn cleanup_slot_and_release_bit_timed(&self, mut slot: Slot, sync_cleanup: bool) -> Result<()> {
        let idx = slot.idx;
        let cleanup_result = slot.cleanup(sync_cleanup);
        let bitset_result = self.release_slot_bit(idx);
        match (cleanup_result, bitset_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), Ok(())) => Err(e.into()),
            (Ok(()), Err(e)) => Err(e),
            (Err(ce), Err(be)) => {
                warn!(cleanup_error = %ce, "network slot cleanup failed alongside bitset release error");
                Err(be)
            }
        }
    }

    pub(crate) fn cleanup_allocated_slot(&self, slot: Slot, sync_cleanup: bool) -> Result<()> {
        self.cleanup_slot_and_release_bit_inner(slot, sync_cleanup)
    }

    /// Builds `count` slots and tears them all down, never touching the pool.
    ///
    /// The measurement path. `release` returns a slot to the warm pool while
    /// the pool is under its high watermark -- that is its job -- so a loop of
    /// `allocate_any`/`release` builds one slot and then recycles it, and times
    /// a queue pop rather than a netns, a veth pair and a tap device. Four
    /// orders of magnitude, and it looks like an answer.
    ///
    /// Slots are held until the whole batch is built so the cost measured is
    /// `count` slots coexisting, which is the shape a bank fills in.
    pub(crate) fn build_and_destroy_slots(&self, count: usize) -> Result<()> {
        let mut slots = Vec::with_capacity(count);
        let mut first_error = None;
        for _ in 0..count {
            match self.allocate_fresh_slot() {
                Ok(slot) => slots.push(slot),
                Err(error) => {
                    first_error = Some(error);
                    break;
                }
            }
        }
        // Tear down whatever was built, including on the failing path: a slot
        // dropped here is a leaked netns, veth pair and tap device, and its
        // index is not reused until the process restarts.
        //
        // Synchronously, unlike the release path. Asynchronous teardown frees
        // the slot *index* immediately and removes the devices afterwards, so
        // the next allocation can be handed an index whose veth still exists
        // and fail adding its default route. The warm pool normally hides that
        // -- a released slot is held, not rebuilt -- but a loop that rebuilds
        // every time walks straight into it. It is also what makes the number
        // honest: a measurement that returns before teardown finishes is
        // timing half the work.
        for slot in slots {
            if let Err(error) = self.cleanup_allocated_slot(slot, /* sync_cleanup */ true) {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Find and allocate the next available slot.
    /// Slot 0 is reserved at init, so returned indices are always >= 1.
    ///
    /// Fast path: reuse a warm slot from the pool.
    /// Slow path: allocate a new index and set up kernel network resources.
    pub fn allocate_any(&self) -> Result<Slot> {
        let result = self.acquire_any_slot();
        // Republished here rather than only from the maintenance cycle: that
        // cycle runs on a worker that does not start at all when pool
        // maintenance is off, and even when it is on it wakes only on a
        // watermark crossing. Occupancy has to move with the load that changes
        // it, not with the refill.
        self.publish_pool_metrics();
        result
    }

    fn acquire_any_slot(&self) -> Result<Slot> {
        if self.shutting_down() {
            return Err(anyhow!(ERR_SHUTTING_DOWN));
        }

        // Fast path: reuse a warm slot from the pool.
        if let Some(slot) = self.pool.try_acquire() {
            if self.shutting_down() {
                let _ = self.cleanup_slot_and_release_bit(slot);
                return Err(anyhow!(ERR_SHUTTING_DOWN));
            }
            debug!(slot = slot.idx, "reused warm network slot from pool");
            if self.pool.len() < self.pool.config().low_watermark {
                self.pool.request_maintenance();
            }
            return Ok(slot);
        }

        // Slow path: allocate a new index and set up kernel network resources.
        self.allocate_fresh_slot()
    }

    fn allocate_fresh_slot(&self) -> Result<Slot> {
        let mut metric = MetricGuard::operation(SLOT_OPERATION_DURATION, "allocate_fresh");
        let result = self.allocate_fresh_slot_inner();
        metric.finish(&result);
        result
    }

    fn allocate_fresh_slot_inner(&self) -> Result<Slot> {
        if self.shutting_down() {
            return Err(anyhow!(ERR_SHUTTING_DOWN));
        }

        // The global host rules are the precondition for every slot, so a slot
        // is never built before they are published. If the install at manager
        // construction failed — most often for want of CAP_NET_ADMIN — this
        // retries it, and a concurrent filler waits for that attempt rather
        // than proceeding against a half-written chain.
        if !HOST_IPTABLES.is_installed() {
            self.install_global_host_iptables()
                .map_err(NetworkError::HostIptablesError)?;
        }

        match self.take_next_slot_bit() {
            Some(idx) => {
                let mut slot = Slot::new(
                    idx as u32,
                    self.address_plan,
                    self.netns_dir.clone(),
                    Arc::clone(&self.egress_proxy),
                )
                .expect("BitSet index within valid range");
                if let Err(e) = slot.create_network() {
                    let _ = self.free_slot_bit(idx);
                    return Err(anyhow!("Failed to create network: {e}"));
                }

                if self.shutting_down() {
                    if let Err(cleanup_error) = self.cleanup_slot_and_release_bit(slot) {
                        return Err(anyhow!(
                            "Network manager is shutting down and failed to cleanup newly allocated slot {}: {}",
                            idx,
                            cleanup_error
                        ));
                    }
                    return Err(anyhow!(ERR_SHUTTING_DOWN));
                }

                Ok(slot)
            }
            None => Err(anyhow!("No available slots")),
        }
    }

    #[cfg(test)]
    fn compute_maintenance_action(&self, pool_len: usize) -> PoolMaintenanceAction {
        self.pool.compute_maintenance_action(pool_len)
    }

    /// Publishes slot-pool occupancy. There were previously no warm-pool
    /// metrics at all, so pool starvation was only visible as create latency.
    fn publish_pool_metrics(&self) {
        let capacity = self.slot_capacity();
        metrics::gauge!("agentenv_pool_size", "pool" => "network").set(capacity.pooled as f64);
        metrics::gauge!("agentenv_network_slots_allocated").set(capacity.allocated as f64);
        metrics::gauge!("agentenv_network_slots_available").set(capacity.available() as f64);
    }

    /// Builds up to `to_fill` warm slots, `fill_concurrency` at a time.
    ///
    /// Slot creation is synchronous and thread-bound (netns membership is
    /// thread-local), so batches are built on scoped threads rather than tasks.
    /// Each slot costs a dozen RTNL-serialized netlink operations, two of which
    /// hold RTNL across a `synchronize_net()`, so concurrency here trades
    /// refill latency against peak kernel lock pressure and does not scale
    /// linearly.
    fn fill_warm_slots(&self, to_fill: usize) -> Result<()> {
        let mut remaining = to_fill;
        let mut cleanup_failures: Vec<String> = Vec::new();
        while remaining > 0 && !self.shutting_down() {
            let batch = remaining.min(self.fill_concurrency);
            let results: Vec<Result<Slot>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..batch)
                    .map(|_| scope.spawn(|| self.allocate_fresh_slot()))
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .unwrap_or_else(|_| Err(anyhow!("network slot refill thread panicked")))
                    })
                    .collect()
            });

            let mut saw_failure = false;
            for result in results {
                match result {
                    Ok(slot) => {
                        let slot_idx = slot.idx;
                        match self.pool.try_push_bounded(slot) {
                            Ok(()) => trace!(
                                slot = slot_idx,
                                pool_len = self.pool.len(),
                                "refilled warm network slot"
                            ),
                            // Accumulated rather than propagated: one slot that
                            // will not clean up must not abandon the peers
                            // built beside it in the same batch, which are
                            // holding bitmap bits and kernel devices.
                            Err(slot) => {
                                if let Err(err) = self.cleanup_slot_and_release_bit(slot) {
                                    cleanup_failures.push(format!("slot {slot_idx}: {err}"));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        saw_failure = true;
                        debug!(error = %err, "skipping pool refill attempt");
                    }
                }
            }

            if saw_failure {
                // Stop this cycle rather than continuing: the next attempt is
                // paced by `fill_gate`, so a persistent failure costs one batch
                // per backoff interval instead of a full core.
                self.fill_gate.record_failure(self.now_millis());
                metrics::counter!(
                    "agentenv_pool_fill_total",
                    "pool" => "network",
                    "status" => "error",
                )
                .increment(1);
                break;
            }
            metrics::counter!(
                "agentenv_pool_fill_total",
                "pool" => "network",
                "status" => "ok",
            )
            .increment(batch as u64);
            self.fill_gate.record_success();
            remaining -= batch;
        }

        slot_cleanup_result(cleanup_failures)
    }

    /// Milliseconds since this manager was constructed, for [`FillGate`].
    fn now_millis(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn run_pool_maintenance_cycle(&self) -> Result<()> {
        self.publish_pool_metrics();
        let action = self.pool.compute_maintenance_action(self.pool.len());

        match action {
            PoolMaintenanceAction::Fill(to_fill) => {
                if let Some(wait) = self.fill_gate.blocked_for(self.now_millis()) {
                    // Slept here, not returned immediately: the worker re-enters
                    // as long as the pool is below target, so returning would
                    // turn the backoff into a busy loop.
                    std::thread::sleep(wait.min(FILL_DECLINE_SLEEP_MAX));
                    return Ok(());
                }
                self.fill_warm_slots(to_fill)?;
            }
            PoolMaintenanceAction::Drain(to_drain) => {
                let mut cleanup_failures: Vec<String> = Vec::new();
                for _ in 0..to_drain {
                    let maybe_slot = self.pool.try_drain_one();
                    let Some(slot) = maybe_slot else {
                        break;
                    };
                    let slot_idx = slot.idx;
                    if let Err(err) = self.cleanup_slot_and_release_bit(slot) {
                        cleanup_failures.push(format!("slot {slot_idx}: {err}"));
                        continue;
                    }
                    metrics::counter!("agentenv_pool_drain_total", "pool" => "network")
                        .increment(1);
                    trace!(
                        slot = slot_idx,
                        "drained excess warm network slot from pool"
                    );
                }
                slot_cleanup_result(cleanup_failures)?;
            }
            PoolMaintenanceAction::Idle => {}
        }

        Ok(())
    }

    /// Release a network slot. If the pool has room the slot is cached warm for
    /// reuse by the next `allocate_any()` call.
    ///
    /// When pool maintenance is enabled, release always enqueues the slot first;
    /// slots above the high watermark are drained asynchronously by the
    /// maintenance worker.
    ///
    /// When pool maintenance is disabled, this keeps the previous bounded-pool
    /// behavior and cleans up immediately once the pool reaches high watermark.
    pub fn release(&self, slot: Slot) -> Result<()> {
        let mut metric = MetricGuard::operation(SLOT_OPERATION_DURATION, "release");
        let result = self.release_slot(slot);
        metric.finish(&result);
        self.publish_pool_metrics();
        result
    }

    /// Releases a slot from a context that cannot await.
    ///
    /// The work is the same as [`Self::release`]; only the thread it runs on
    /// differs. Tracked so [`Self::shutdown`] can wait for it rather than
    /// racing it.
    pub fn release_detached(&'static self, slot: Slot) {
        let slot_idx = slot.idx;
        BACKGROUND_RELEASES.spawn(move || {
            if let Err(err) = self.release(slot) {
                warn!(slot = slot_idx, error = %err, "background network slot release failed");
            }
        });
    }

    fn release_slot(&self, slot: Slot) -> Result<()> {
        if self.shutting_down() {
            return self.cleanup_slot_and_release_bit(slot);
        }
        let slot_idx = slot.idx;
        match self.pool.release(slot) {
            Ok(()) => {
                debug!(
                    slot = slot_idx,
                    pool_len = self.pool.len(),
                    "returned network slot to pool"
                );
                Ok(())
            }
            Err(slot) => self.cleanup_slot_and_release_bit(slot),
        }
    }

    /// Cleans up all warm slots cached in the pool.
    ///
    /// This is intended for process shutdown because the global manager is a
    /// static singleton and is not guaranteed to run Drop handlers at exit.
    ///
    /// After shutdown, the manager rejects new allocations and releases without
    /// caching to prevent new resource usage and avoid silent failures from leftover slots in the pool.
    pub fn shutdown(&self) -> Result<()> {
        self.shutdown_inner(false)
    }

    fn shutdown_inner(&self, sync_cleanup: bool) -> Result<()> {
        self.shutting_down.store(true, Ordering::Release);
        // Before draining the pool: a release still in flight owns a slot that
        // is in neither the pool nor the bitmap, and cleaning up around it
        // would leave its veth and namespace mount behind.
        if !BACKGROUND_RELEASES.drain(BACKGROUND_RELEASE_DRAIN_TIMEOUT) {
            warn!(
                timeout_ms = BACKGROUND_RELEASE_DRAIN_TIMEOUT.as_millis(),
                "background network slot releases did not finish before shutdown"
            );
        }
        let drained_slots = self.pool.drain_all();
        let had_slots = !drained_slots.is_empty();
        let mut failures = Vec::new();

        if had_slots {
            debug!(
                slot_count = drained_slots.len(),
                "draining warm network slot pool during shutdown"
            );
            for slot in drained_slots {
                let idx = slot.idx;
                if let Err(err) = self.cleanup_slot_and_release_bit_inner(slot, sync_cleanup) {
                    failures.push(format!("slot {idx} cleanup failed: {err}"));
                }
            }
        }

        self.egress_proxy.shutdown();
        self.cleanup_global_host_iptables();

        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "failed to clean up pooled network slots during shutdown: {}",
                failures.join(" | ")
            ))
        }
    }

    /// Reserve slots whose host-side `veth-<idx>` interfaces already exist.
    ///
    /// This avoids collisions with stale interfaces left by previous runs or other
    /// concurrently running processes.
    fn reserve_existing_host_veth_slots(&self) -> Vec<usize> {
        let net_dir = Path::new("/sys/class/net");
        let entries = match fs::read_dir(net_dir) {
            Ok(entries) => entries,
            Err(err) => {
                warn!(path = %net_dir.display(), error = %err, "failed to scan host interfaces");
                return Vec::new();
            }
        };

        let mut reserved_slots = Vec::new();
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let Some(slot_idx) = slot_index_from_host_veth_name(&name) else {
                continue;
            };

            let _ = self.take_slot_bit(slot_idx);
            reserved_slots.push(slot_idx);
        }

        reserved_slots.sort_unstable();
        reserved_slots.dedup();
        reserved_slots
    }

    fn log_external_conflicts(&self, existing_slots: &[usize]) {
        if !existing_slots.is_empty() {
            let samples = existing_slots
                .iter()
                .copied()
                .take(CONFLICT_SAMPLE_LIMIT)
                .collect::<Vec<_>>();
            warn!(
                slot_count = existing_slots.len(),
                sample_slots = ?samples,
                "detected existing host veth interfaces using AgentENV slot naming; another sandbox runtime may already be active or previous cleanup may have left conflicting devices behind"
            );
        }

        let network_range_patterns = self.address_plan.conflict_patterns();
        let firewall_conflict_patterns = std::iter::once(HOST_VETH_PREFIX.to_string())
            .chain(network_range_patterns.iter().cloned())
            .collect::<Vec<_>>();

        self.check_conflicts(
            "host interface addresses",
            "ip",
            &["-o", "addr", "show"],
            &[],
            &network_range_patterns,
            "detected host interface addresses overlapping AgentENV network ranges; another program may already be using the same address space",
        );
        self.check_conflicts(
            "host routes",
            "ip",
            &["-o", "route", "show"],
            &[],
            &network_range_patterns,
            "detected host routes overlapping AgentENV network ranges; another program may already be using the same address space",
        );
        self.check_conflicts(
            "iptables-save",
            "iptables-save",
            &[],
            &[crate::privileges::CAP_NET_ADMIN],
            &firewall_conflict_patterns,
            "detected iptables rules referencing AgentENV interfaces or network ranges; sandbox traffic may be redirected, filtered, or rewritten by another program",
        );
        self.check_conflicts(
            "nft ruleset",
            "nft",
            &["list", "ruleset"],
            &[crate::privileges::CAP_NET_ADMIN],
            &firewall_conflict_patterns,
            "detected nftables rules referencing AgentENV interfaces or network ranges; sandbox traffic may be redirected, filtered, or rewritten by another program",
        );
    }

    fn check_conflicts(
        &self,
        source: &str,
        command: &str,
        args: &[&str],
        capabilities: &'static [i32],
        patterns: &[String],
        message: &str,
    ) {
        let Some(output) = run_command(command, args, capabilities) else {
            return;
        };
        let report = collect_conflict_report(&output, patterns);
        if report.total_matches == 0 {
            return;
        }

        warn!(
            source,
            match_count = report.total_matches,
            sample_lines = ?report.samples,
            "{message}"
        );
    }

    fn install_global_host_iptables(&self) -> Result<()> {
        install_host_iptables(
            &HOST_IPTABLES,
            self.address_plan.host_interaction_cidr(),
            apply_global_host_iptables,
        )
    }

    fn cleanup_global_host_iptables(&self) {
        if !HOST_IPTABLES.take_installed() {
            return;
        }

        let delete_commands =
            global_host_iptables_delete_commands(self.address_plan.host_interaction_cidr());

        if let Err(e) = apply_iptables_commands(
            &delete_commands,
            OpenFailurePolicy::WarnAndIgnore("failed to open iptables for cleanup"),
        ) {
            warn!(error = %e, "failed to cleanup global host iptables rules");
        }
    }
}

/// Publishes the global host rules through `state`, once per process.
///
/// `state` and `apply` are parameters rather than the static and the real
/// restore so the ordering can be exercised where production reads it: an
/// applier that reports what the flag looked like while it ran is the only way
/// to see the flag being raised before the rules commit, and a test that drives
/// `install_once` with a state of its own cannot see it at all.
fn install_host_iptables(
    state: &GlobalHostIptables,
    host_interaction_cidr: Ipv4Network,
    apply: fn(Ipv4Network) -> Result<()>,
) -> Result<()> {
    state.install_once(|| apply(host_interaction_cidr))
}

fn apply_global_host_iptables(host_interaction_cidr: Ipv4Network) -> Result<()> {
    let commands = global_host_iptables_commands(host_interaction_cidr);
    apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)?;
    // The backend decides whether the lock options on every later restore mean
    // anything: nft accepts and discards them because it never opens
    // /run/xtables.lock. Recorded once, where the first restore of the process
    // has just proved the binary works.
    info!(
        backend = iptables_backend().as_str(),
        "installed global host iptables rules for sandbox networking"
    );
    Ok(())
}

/// Folds per-slot cleanup failures into one error for the cycle.
fn slot_cleanup_result(failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "network slot cleanup failed: {}",
        failures.join(" | ")
    ))
}

fn global_host_iptables_commands(
    host_interaction_cidr: Ipv4Network,
) -> [IptablesRestoreCommand; 5] {
    let cidr = host_interaction_cidr.to_string();
    [
        // Guest replies to host-initiated proxy/envd connections must remain
        // reachable, while all other guest-to-host traffic is rejected below.
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "INPUT",
            position: 1,
            rule: format!(
                "-i {HOST_VETH_PREFIX}+ -s {cidr} -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Insert {
            table: "filter",
            chain: "INPUT",
            position: 2,
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j REJECT"),
        },
        // Packets are SNATted to the host interaction CIDR inside the sandbox namespace before
        // they enter the host FORWARD chain, so hosts with a DROP FORWARD policy
        // need to accept this post-SNAT source range.
        IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j ACCEPT"),
        },
        IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: format!(
                "-o {HOST_VETH_PREFIX}+ -d {cidr} -m state --state RELATED,ESTABLISHED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Append {
            table: "nat",
            chain: "POSTROUTING",
            rule: format!("-s {cidr} -j MASQUERADE"),
        },
    ]
}

fn global_host_iptables_delete_commands(
    host_interaction_cidr: Ipv4Network,
) -> [IptablesRestoreCommand; 5] {
    let cidr = host_interaction_cidr.to_string();
    [
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "INPUT",
            rule: format!(
                "-i {HOST_VETH_PREFIX}+ -s {cidr} -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "INPUT",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j REJECT"),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "FORWARD",
            rule: format!("-i {HOST_VETH_PREFIX}+ -s {cidr} -j ACCEPT"),
        },
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain: "FORWARD",
            rule: format!(
                "-o {HOST_VETH_PREFIX}+ -d {cidr} -m state --state RELATED,ESTABLISHED -j ACCEPT"
            ),
        },
        IptablesRestoreCommand::Delete {
            table: "nat",
            chain: "POSTROUTING",
            rule: format!("-s {cidr} -j MASQUERADE"),
        },
    ]
}

impl Drop for NetworkManager {
    fn drop(&mut self) {
        if let Err(err) = self.shutdown() {
            warn!(error = %err, "network manager drop cleanup failed");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ConflictReport {
    total_matches: usize,
    samples: Vec<String>,
}

fn slot_index_from_host_veth_name(name: &str) -> Option<usize> {
    let slot_str = name.strip_prefix(HOST_VETH_PREFIX)?;
    let slot_idx = slot_str.parse::<usize>().ok()?;
    if slot_idx == 0 || slot_idx >= MAX_SLOTS {
        return None;
    }
    Some(slot_idx)
}

fn collect_conflict_report(output: &str, patterns: &[String]) -> ConflictReport {
    let mut total_matches = 0;
    let mut samples = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !patterns
            .iter()
            .any(|pattern| line.contains(pattern.as_str()))
        {
            continue;
        }

        total_matches += 1;
        if samples.len() < CONFLICT_SAMPLE_LIMIT {
            samples.push(line.to_string());
        }
    }

    ConflictReport {
        total_matches,
        samples,
    }
}

fn run_command(command: &str, args: &[&str], capabilities: &'static [i32]) -> Option<String> {
    let output = match crate::privileges::run_with_scoped_capabilities(capabilities, || {
        Command::new(command)
            .args(args)
            .output()
            .map_err(anyhow::Error::from)
    }) {
        Ok(output) => output,
        Err(err) => {
            debug!(command, args = ?args, error = %err, "failed to inspect host networking state");
            return None;
        }
    };

    if !output.status.success() {
        debug!(
            command,
            args = ?args,
            exit_code = output.status.code().unwrap_or_default(),
            stderr = %String::from_utf8_lossy(&output.stderr),
            "network conflict inspection command exited unsuccessfully"
        );
        return None;
    }

    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use index_set::BitSet;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Captures gauge sets and counter increments so a test can read back what
    /// was actually emitted. Installed per-thread, so it does not disturb the
    /// process-wide recorder the server installs.
    #[derive(Default)]
    struct MetricSpy {
        gauges: Arc<Mutex<HashMap<String, f64>>>,
        counters: Arc<Mutex<HashMap<String, u64>>>,
    }

    struct SpyGauge {
        name: String,
        gauges: Arc<Mutex<HashMap<String, f64>>>,
    }

    struct SpyCounter {
        name: String,
        counters: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl metrics::CounterFn for SpyCounter {
        fn increment(&self, value: u64) {
            *self
                .counters
                .lock()
                .unwrap()
                .entry(self.name.clone())
                .or_default() += value;
        }
        fn absolute(&self, value: u64) {
            self.counters
                .lock()
                .unwrap()
                .insert(self.name.clone(), value);
        }
    }

    impl metrics::GaugeFn for SpyGauge {
        fn increment(&self, _value: f64) {}
        fn decrement(&self, _value: f64) {}
        fn set(&self, value: f64) {
            self.gauges.lock().unwrap().insert(self.name.clone(), value);
        }
    }

    impl metrics::Recorder for MetricSpy {
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
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::from_arc(Arc::new(SpyCounter {
                name: key.name().to_string(),
                counters: Arc::clone(&self.counters),
            }))
        }
        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::from_arc(Arc::new(SpyGauge {
                name: key.name().to_string(),
                gauges: Arc::clone(&self.gauges),
            }))
        }
        fn register_histogram(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    fn manager_with_capacity(capacity: usize) -> NetworkManager {
        NetworkManager::new(false, capacity, capacity)
    }

    fn test_slot(idx: u32) -> Slot {
        Slot::new(
            idx,
            NetworkAddressPlan::default(),
            std::env::temp_dir().join("aenv-network-tests/netns"),
            EgressProxy::new(),
        )
        .unwrap()
    }

    fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn has_network_runtime_capabilities() -> bool {
        let required =
            (1u64 << crate::privileges::CAP_NET_ADMIN) | (1u64 << crate::privileges::CAP_SYS_ADMIN);
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix("CapEff:"))
                    .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            })
            .is_some_and(|caps| caps & required == required)
    }

    fn parse_child_marker(output: &str, key: &str) -> Option<String> {
        let prefix = format!("{key}=");
        output
            .lines()
            .find_map(|line| line.trim().strip_prefix(&prefix).map(str::to_owned))
    }

    fn netns_exists(namespace_id: &str) -> bool {
        crate::cfg::ConfigManager::global_config()
            .runtime_path
            .join("netns")
            .join(namespace_id)
            .exists()
    }

    fn host_veth_exists(slot_idx: u32) -> bool {
        let veth_name = format!("{HOST_VETH_PREFIX}{slot_idx}");
        command_stdout("ip", &["-o", "link", "show", &veth_name])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    fn host_route_exists(host_interaction_ip: &str) -> bool {
        let route = format!("{host_interaction_ip}/32");
        command_stdout("ip", &["-o", "route", "show", &route])
            .map(|s| s.lines().any(|line| line.contains(&route)))
            .unwrap_or(false)
    }

    fn wait_until(
        timeout: Duration,
        interval: Duration,
        mut predicate: impl FnMut() -> bool,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(interval);
        }
    }

    /// Admission's `min_free_network_slots` floor and the capacity gauges both
    /// read this. It was the bitmap's word count for a while — a compile-time
    /// 512 whatever the node was doing — which is a floor that can never fire
    /// and a dashboard that never moves.
    #[test]
    fn slot_capacity_tracks_what_is_actually_allocated() {
        let manager = manager_with_capacity(0);
        // Slot 0 is reserved during construction, and any host `veth-<idx>`
        // left by another process is reserved with it, so the count is read
        // relative to what construction produced.
        let baseline = manager.slot_capacity();
        assert_eq!(baseline.total, MAX_SLOTS);
        assert!(baseline.allocated >= 1, "slot 0 is reserved on init");

        let first = manager.allocate_test_slot().unwrap();
        let second = manager.allocate_test_slot().unwrap();
        let second_idx = second.idx as usize;
        assert_eq!(
            manager.slot_capacity().allocated,
            baseline.allocated + 2,
            "an allocation must consume capacity"
        );
        assert_eq!(
            manager.slot_capacity().available(),
            MAX_SLOTS - baseline.allocated - 2
        );

        assert_eq!(
            manager.take_slot_bit(first.idx as usize),
            Some(true),
            "the bit is already taken"
        );
        assert_eq!(
            manager.slot_capacity().allocated,
            baseline.allocated + 2,
            "a duplicate take is not a new slot"
        );

        manager.release(first).unwrap();
        assert_eq!(manager.slot_capacity().allocated, baseline.allocated + 1);

        manager.release(second).unwrap();
        assert_eq!(
            manager.free_slot_bit(second_idx),
            Some(false),
            "the bit is already free"
        );
        assert_eq!(
            manager.slot_capacity().allocated,
            baseline.allocated,
            "a double release must not double-count"
        );
    }

    /// The gauges have to move with the load that changes them. Publishing
    /// only from the maintenance cycle leaves them frozen between watermark
    /// crossings, and absent entirely on a node with `[pool.network]`
    /// maintenance off — that worker never starts.
    #[test]
    fn allocation_and_release_publish_the_slot_gauges() {
        let spy = MetricSpy::default();
        let gauges = Arc::clone(&spy.gauges);
        let manager = manager_with_capacity(2);

        // Warm one slot first so `allocate_any` can take the pooled path;
        // building a fresh one needs netlink privileges a unit test lacks.
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;
        manager.release(slot).unwrap();
        let allocated = manager.slot_capacity().allocated;

        let reused = metrics::with_local_recorder(&spy, || manager.allocate_any().unwrap());
        assert_eq!(reused.idx, idx);
        {
            let published = gauges.lock().unwrap();
            assert_eq!(
                published.get("agentenv_pool_size").copied(),
                Some(0.0),
                "allocating must publish occupancy without waiting for a refill cycle"
            );
            assert_eq!(
                published.get("agentenv_network_slots_available").copied(),
                Some((MAX_SLOTS - allocated) as f64)
            );
        }

        metrics::with_local_recorder(&spy, || manager.release(reused).unwrap());
        let published = gauges.lock().unwrap();
        assert_eq!(
            published.get("agentenv_pool_size").copied(),
            Some(1.0),
            "releasing must republish occupancy"
        );
        assert_eq!(
            published.get("agentenv_network_slots_allocated").copied(),
            Some(allocated as f64)
        );
        assert_eq!(
            published.get("agentenv_network_slots_available").copied(),
            Some((MAX_SLOTS - allocated + 1) as f64)
        );
    }

    #[test]
    fn slot_zero_is_reserved_on_init() {
        let manager = manager_with_capacity(0);
        // Slot 0 is reserved internally so allocate_any will never return it.
        // Verify by checking the raw bitset directly.
        assert!(manager.allocated.has(0));
    }

    #[test]
    fn allocate_any_never_returns_slot_zero() {
        let manager = manager_with_capacity(0);
        // First allocation must skip the reserved slot 0
        let before = manager.slot_capacity().allocated;
        let idx = manager
            .take_next_slot_bit()
            .expect("at least one free slot");
        assert!(idx >= 1, "allocate_any returned reserved slot 0");
        // take_next_slot_bit is the only path a real node takes to a brand-new
        // slot, so the accounting has to move here or slot_capacity reports the
        // construction baseline forever: min_free_network_slots could never
        // fire and both gauges would sit flat while the node filled up.
        assert_eq!(
            manager.slot_capacity().allocated,
            before + 1,
            "a fresh allocation must be counted"
        );
    }

    #[test]
    fn reject_allocation_at_boundaries() {
        let manager = manager_with_capacity(0);

        // Slot 0 is reserved
        let err = manager.allocate_slot(0).unwrap_err().to_string();
        assert!(err.contains("out of range"));

        // Slot MAX_SLOTS is out of range
        let err = manager
            .allocate_slot(MAX_SLOTS as u32)
            .unwrap_err()
            .to_string();
        assert!(err.contains("out of range"));

        // Slot MAX_SLOTS - 1 is the last valid index
        let last_slot = manager.allocate_slot((MAX_SLOTS - 1) as u32).unwrap();
        drop(last_slot);
    }

    #[test]
    fn duplicate_allocation_is_rejected() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;

        let err = manager.allocate_slot(idx).unwrap_err().to_string();
        assert!(err.contains("already allocated"));
        drop(slot);
    }

    #[test]
    fn release_then_reallocate() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;

        // Manually release via the local manager.
        manager.release(slot).unwrap();

        // Should be able to allocate the same index again
        let slot2 = manager.allocate_slot(idx).unwrap();
        assert_eq!(slot2.idx, idx);
        drop(slot2);
    }

    #[test]
    fn double_release_is_rejected() {
        let manager = manager_with_capacity(0);
        let slot = manager.allocate_test_slot().unwrap();
        let idx = slot.idx;
        manager.release(slot).unwrap();

        let err = manager.release_slot_bit(idx).unwrap_err().to_string();
        assert!(err.contains("not allocated"));
    }

    #[test]
    fn release_slot_zero_is_rejected() {
        let manager = manager_with_capacity(0);
        let err = manager.release_slot_bit(0).unwrap_err().to_string();
        assert!(err.contains("out of range"));
    }

    #[test]
    fn slot_index_from_host_veth_name_parses_valid_slot() {
        assert_eq!(slot_index_from_host_veth_name("veth-42"), Some(42));
    }

    #[test]
    fn slot_index_from_host_veth_name_rejects_non_matching_names() {
        assert_eq!(slot_index_from_host_veth_name("eth0"), None);
        assert_eq!(slot_index_from_host_veth_name("veth-nope"), None);
        assert_eq!(slot_index_from_host_veth_name("veth-0"), None);
    }

    #[test]
    fn collect_conflict_report_keeps_samples_and_total_count() {
        let output = "\
            10.11.0.1 via 10.12.0.3 dev veth-1\n\
            iifname \"veth-1\" tcp dport 443 redirect to :5017\n\
            unrelated line\n\
            ip saddr 10.11.0.2 oifname \"eth0\" masquerade\n\
        ";
        let patterns = vec![
            HOST_VETH_PREFIX.to_string(),
            "10.11.".to_string(),
            "10.12.".to_string(),
        ];
        let report = collect_conflict_report(output, &patterns);
        assert_eq!(report.total_matches, 3);
        assert_eq!(report.samples.len(), 3);
        assert!(report.samples[0].contains("10.11.0.1"));
        assert!(report.samples[1].contains("veth-1"));
        assert!(report.samples[2].contains("10.11.0.2"));
    }

    #[test]
    fn maintenance_action_fills_to_current_target() {
        let manager = NetworkManager::new(true, 4, 10);
        assert_eq!(
            manager.compute_maintenance_action(2),
            PoolMaintenanceAction::Fill(2)
        );
        assert_eq!(
            manager.compute_maintenance_action(7),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn maintenance_action_drains_above_high_watermark() {
        let manager = NetworkManager::new(true, 2, 4);
        assert_eq!(
            manager.compute_maintenance_action(8),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            manager.compute_maintenance_action(4),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn maintenance_cycle_drains_excess_warm_slots() {
        let manager = NetworkManager::new(true, 0, 2);
        for idx in 1..=4u32 {
            let _ = manager.take_slot_bit(idx as usize);
            manager.pool.release(test_slot(idx)).unwrap();
        }

        manager.run_pool_maintenance_cycle().unwrap();

        let pool_len = manager.pool.len();
        assert_eq!(pool_len, 2);
        assert!(manager.allocated.has(1));
        assert!(manager.allocated.has(2));
        assert!(!manager.allocated.has(3));
        assert!(!manager.allocated.has(4));
    }

    #[test]
    fn concurrent_allocate_any_produces_unique_slots() {
        let n = 200;
        // Pre-populate the pool so allocate_any() always uses the fast path (no create_network).
        let manager = Arc::new(manager_with_capacity(n));
        for idx in 1..=(n as u32) {
            let _ = manager.take_slot_bit(idx as usize);
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();

        let allocated: HashSet<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All indices must be unique
        assert_eq!(allocated.len(), n);
        // None should be slot 0
        assert!(!allocated.contains(&0));
    }

    #[test]
    fn release_returns_slot_to_pool_when_under_capacity() {
        let manager = manager_with_capacity(5);
        // Pre-insert a slot into the bitset (simulating a prior allocation)
        let _ = manager.take_slot_bit(10);
        let slot = test_slot(10);
        // release() should push the slot into the pool (capacity=5, pool is empty)
        manager.release(slot).unwrap();
        let pool_len = manager.pool.len();
        assert_eq!(pool_len, 1, "slot should be in pool");
        // Clean up: drain pool.
        while let Some(s) = manager.pool.try_drain_one() {
            drop(s);
        }
        let _ = manager.free_slot_bit(10);
    }

    #[test]
    fn allocate_any_prefers_pool_over_bitset() {
        let manager = manager_with_capacity(5);
        // Manually put a slot (idx=42) in the pool and mark it allocated in the bitset
        let _ = manager.take_slot_bit(42);
        let pooled = test_slot(42);
        manager.pool.try_push_bounded(pooled).unwrap();

        // allocate_any() must pop from pool (idx=42) without calling create_network()
        // We verify: (1) the returned idx is 42, (2) no other slot was allocated
        let slot = manager.allocate_any().unwrap();
        assert_eq!(slot.idx, 42, "allocate_any must return the pooled slot");
        assert!(manager.pool.is_empty(), "pool must be empty after pop");
        drop(slot);
        let _ = manager.free_slot_bit(42);
    }

    #[test]
    fn release_cleans_up_when_pool_is_full() {
        // high_watermark = 0: release() must NOT push to pool, must call cleanup() instead
        let manager = manager_with_capacity(0);
        let _ = manager.take_slot_bit(7);
        let slot = test_slot(7);
        manager.release(slot).unwrap();
        // Pool must be empty (slot was NOT cached)
        assert!(manager.pool.is_empty(), "pool must be empty");
        // Bitset bit must be freed
        assert!(
            !manager.allocated.has(7),
            "bitset bit must be cleared after release with full pool"
        );
    }

    #[test]
    fn release_enqueues_above_high_watermark_and_drains_async() {
        let manager = NetworkManager::new(true, 0, 0);
        let _ = manager.take_slot_bit(17);
        let slot = test_slot(17);

        manager.release(slot).unwrap();

        assert_eq!(
            manager.pool.len(),
            1,
            "slot should be enqueued first even when above high watermark"
        );
        assert!(
            manager.allocated.has(17),
            "bitset should remain allocated until async drain runs"
        );

        manager.run_pool_maintenance_cycle().unwrap();

        assert!(manager.pool.is_empty());
        assert!(
            !manager.allocated.has(17),
            "bitset bit must be released after maintenance drain"
        );
    }

    #[test]
    fn shutdown_cleanup_drains_pool_and_releases_bits() {
        let manager = manager_with_capacity(4);
        let _ = manager.take_slot_bit(11);
        let _ = manager.take_slot_bit(12);

        manager.pool.try_push_bounded(test_slot(11)).unwrap();
        manager.pool.try_push_bounded(test_slot(12)).unwrap();

        manager.shutdown().unwrap();

        assert!(manager.pool.is_empty(), "pool must be empty");
        assert!(!manager.allocated.has(11), "slot 11 bit must be released");
        assert!(!manager.allocated.has(12), "slot 12 bit must be released");
    }

    #[test]
    fn shutdown_cleanup_is_noop_for_empty_pool() {
        let manager = manager_with_capacity(4);
        manager.shutdown().unwrap();
    }

    #[test]
    fn shutdown_cleanup_is_idempotent() {
        let manager = manager_with_capacity(4);
        let _ = manager.take_slot_bit(14);
        manager.pool.try_push_bounded(test_slot(14)).unwrap();

        manager.shutdown().unwrap();
        manager.shutdown().unwrap();

        assert!(manager.pool.is_empty());
        assert!(!manager.allocated.has(14));
    }

    #[test]
    fn allocate_any_is_rejected_after_shutdown_cleanup() {
        let manager = manager_with_capacity(4);
        let _ = manager.take_slot_bit(9);
        manager.pool.try_push_bounded(test_slot(9)).unwrap();

        manager.shutdown().unwrap();

        let err = manager.allocate_any().unwrap_err().to_string();
        assert!(err.contains("shutting down"));
    }

    #[test]
    fn release_does_not_cache_slot_after_shutdown_cleanup() {
        let manager = manager_with_capacity(4);
        manager.shutdown().unwrap();

        let _ = manager.take_slot_bit(13);
        let slot = test_slot(13);
        manager.release(slot).unwrap();

        assert!(manager.pool.is_empty(), "pool must remain empty");
        assert!(
            !manager.allocated.has(13),
            "slot bit must be released instead of kept warm"
        );
    }

    #[test]
    fn release_race_with_shutdown_does_not_recache_slot() {
        let manager = Arc::new(manager_with_capacity(4));
        let _ = manager.take_slot_bit(15);
        let slot = test_slot(15);

        let release_manager = manager.clone();
        let release_thread = std::thread::spawn(move || {
            release_manager.release(slot).unwrap();
        });

        let shutdown_manager = manager.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_manager.shutdown().unwrap();
        });

        release_thread.join().unwrap();
        shutdown_thread.join().unwrap();

        assert!(manager.pool.is_empty());
        assert!(!manager.allocated.has(15));
    }

    #[test]
    fn concurrent_release_with_pool() {
        use std::sync::Arc;

        // Pool capacity of 10 with 50 concurrent releases.
        let manager = Arc::new(manager_with_capacity(10));

        // Pre-populate bitset for slots 1..=50 and build Slot values.
        // We bypass allocate_any() because create_network() requires host capabilities.
        // Instead we manually mark slots as allocated in the bitset.
        for idx in 1u32..=50 {
            let _ = manager.take_slot_bit(idx as usize);
        }
        let slot_values: Vec<Slot> = (1u32..=50).map(test_slot).collect();

        // Release all 50 slots concurrently — at most 10 should end up in the pool.
        let handles: Vec<_> = slot_values
            .into_iter()
            .map(|slot| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    m.release(slot).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Pool must not exceed capacity.
        let pool_len = manager.pool.len();
        assert!(pool_len <= 10, "pool exceeded capacity: {pool_len}");

        // Clean up remaining pool slots.
        while let Some(slot) = manager.pool.try_drain_one() {
            drop(slot);
        }
        // Free any remaining bitset bits (for slots that were cleaned up, not pooled).
        for idx in 1u32..=50 {
            let _ = manager.free_slot_bit(idx as usize);
        }
    }

    #[test]
    fn concurrent_allocate_and_release_cycle() {
        let total = 50;
        // Pre-populate the pool so allocate_any() uses the fast path (no create_network).
        let manager = Arc::new(manager_with_capacity(total));
        for idx in 1..=(total as u32) {
            let _ = manager.take_slot_bit(idx as usize);
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }

        // Phase 1: Allocate 50 slots concurrently from the pool
        let handles: Vec<_> = (0..total)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();
        let first_batch: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Phase 2: Release even-indexed results concurrently (just freeing the bitset bit)
        let to_release: Vec<u32> = first_batch.iter().copied().filter(|i| i % 2 == 0).collect();
        let release_count = to_release.len();
        let handles: Vec<_> = to_release
            .into_iter()
            .map(|idx| {
                let m = manager.clone();
                std::thread::spawn(move || m.release_slot_bit(idx).unwrap())
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Phase 3: Re-allocate the released slots (slow path — bitset only, no create_network in test)
        // Re-add them to the pool so the test remains root-free.
        for idx in first_batch.iter().copied().filter(|i| i % 2 == 0) {
            let _ = manager.take_slot_bit(idx as usize); // re-mark as allocated
            manager.pool.try_push_bounded(test_slot(idx)).unwrap();
        }
        let handles: Vec<_> = (0..release_count)
            .map(|_| {
                let m = manager.clone();
                std::thread::spawn(move || {
                    let slot = m.allocate_any().unwrap();
                    let idx = slot.idx;
                    drop(slot);
                    idx
                })
            })
            .collect();
        let second_batch: HashSet<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All new allocations must be unique and non-zero
        assert_eq!(second_batch.len(), release_count);
        assert!(!second_batch.contains(&0));
    }

    /// Serializes the tests that build real slots on the host.
    ///
    /// They share one machine: slot indices, the host routing table, the
    /// `veth-N` namespace and the global iptables chains are all process-wide,
    /// so two of them running at once fail each other in ways that look like
    /// product bugs -- a slot whose default route "already exists", built by
    /// the other test a millisecond earlier.
    ///
    /// This never bit before because only one such test actually ran: the
    /// others guard on `geteuid() == 0` and skip under the capability runner,
    /// which grants CAP_NET_ADMIN without root. That made the guard look like a
    /// permission check when it was also, accidentally, the serialization.
    ///
    /// Poison is ignored: a panicking test has already failed, and refusing to
    /// run every later one because of it turns one failure into a cascade.
    static HOST_NETWORK_TESTS: Mutex<()> = Mutex::new(());

    fn lock_host_network() -> std::sync::MutexGuard<'static, ()> {
        HOST_NETWORK_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and intentionally crashes a child process"]
    fn panic_exit_with_pooled_slot_is_cleaned_by_exit_hook() {
        let _host_network = lock_host_network();
        const CHILD_ENV: &str = "AENV_NETWORK_PANIC_CHILD";
        const TEST_NAME: &str =
            "sandbox::network::manager::tests::panic_exit_with_pooled_slot_is_cleaned_by_exit_hook";

        if std::env::var_os(CHILD_ENV).is_some() {
            let manager = NetworkManager::global();
            let slot = manager
                .allocate_any()
                .expect("child failed to allocate real network slot");

            let slot_idx = slot.idx;
            let netns_id = slot.namespace_id.clone();
            let host_veth = format!("{HOST_VETH_PREFIX}{slot_idx}");

            let host_veth_exists = command_stdout("ip", &["-o", "link", "show", &host_veth])
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            assert!(
                host_veth_exists,
                "child failed to observe allocated host veth interface: {host_veth}"
            );

            let netns_exists = netns_exists(&netns_id);
            assert!(
                netns_exists,
                "child failed to observe allocated network namespace: {netns_id}"
            );

            println!("SLOT_IDX={slot_idx}");
            println!("NETNS_ID={netns_id}");
            println!("HOST_INTERACTION_IP={}", slot.host_interaction_ip);

            manager
                .release(slot)
                .expect("child failed to return slot to warm pool");

            panic!("intentional panic for crash-cleanup validation");
        }

        if !has_network_runtime_capabilities() {
            eprintln!("skipping crash-cleanup validation: required capabilities are missing");
            return;
        }

        let exe = std::env::current_exe().expect("failed to locate current test binary");
        let output = Command::new(exe)
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--ignored")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .output()
            .expect("failed to launch panic child process");

        let child_stdout = String::from_utf8_lossy(&output.stdout);
        let child_stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            !output.status.success(),
            "child process should fail due to intentional panic, stdout={child_stdout}, stderr={child_stderr}"
        );

        let slot_idx = parse_child_marker(&child_stdout, "SLOT_IDX")
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or_else(|| {
                panic!(
                    "child did not emit slot index marker, stdout={child_stdout}, stderr={child_stderr}"
                )
            });
        let netns_id = parse_child_marker(&child_stdout, "NETNS_ID").unwrap_or_else(|| {
            panic!("child did not emit netns marker, stdout={child_stdout}, stderr={child_stderr}")
        });
        let host_interaction_ip =
            parse_child_marker(&child_stdout, "HOST_INTERACTION_IP").unwrap_or_else(
                || {
                    panic!(
                        "child did not emit host interaction IP marker, stdout={child_stdout}, stderr={child_stderr}"
                    )
                },
            );

        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !netns_exists(&netns_id)
            }),
            "network namespace leaked after panic child exit: {netns_id}"
        );
        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !host_veth_exists(slot_idx)
            }),
            "host veth leaked after panic child exit: {HOST_VETH_PREFIX}{slot_idx}"
        );
        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(50), || {
                !host_route_exists(&host_interaction_ip)
            }),
            "host route leaked after panic child exit: {host_interaction_ip}/32"
        );
    }

    /// A pooled slot costs no netlink work to acquire, so admission must count
    /// it as available. Deriving free capacity from the allocation bitmap alone
    /// would make a node with a full warm pool look saturated and reject
    /// creates it could serve instantly.
    #[test]
    fn slot_capacity_counts_pooled_slots_as_available() {
        let capacity = NetworkSlotCapacity {
            total: 100,
            allocated: 40,
            pooled: 10,
        };
        assert_eq!(capacity.available(), 70);
    }

    #[test]
    fn slot_capacity_saturates_rather_than_underflowing() {
        let capacity = NetworkSlotCapacity {
            total: 4,
            allocated: 8,
            pooled: 0,
        };
        assert_eq!(capacity.available(), 0);
    }

    /// Measures network slot creation throughput at a given concurrency.
    ///
    /// Each fresh slot costs a dozen RTNL-serialized netlink operations, two of
    /// which hold RTNL across a `synchronize_net()`, plus several fork/execs.
    /// That is the suspected ceiling on per-node cold creates, and whether it
    /// actually is decides whether deeper netlink work — socket reuse,
    /// replacing the `ip` shell-outs, a pre-created device bank — is worth
    /// building. Answering that by measurement rather than inference is the
    /// whole point of this test.
    ///
    /// Ignored by default: it creates real namespaces and devices, so it needs
    /// root and must not run alongside anything else touching them. Run with:
    ///
    /// ```text
    /// sudo -E cargo test -p agentenv --lib network_slot_creation_throughput -- --ignored --nocapture
    /// ```
    /// The measurement path must not quietly become a pool pop.
    ///
    /// `release` returns a slot to the warm pool while the pool is under its
    /// high watermark, so a build/teardown loop written on `allocate_any` +
    /// `release` builds one slot and then recycles it forever. That is not a
    /// slow measurement, it is a wrong one: it reported 418ns where a slot
    /// actually costs about 53ms, and nothing about the number said so.
    ///
    /// The property that keeps it honest is that a batch leaves nothing behind
    /// -- no pooled slot to hand back on the next call, and no allocated bits.
    #[test]
    #[ignore = "mutates host network state; needs CAP_NET_ADMIN"]
    fn build_and_destroy_slots_leaves_nothing_pooled() {
        let _host_network = lock_host_network();
        // A pool that would happily cache what is released into it, so the test
        // fails if the batch path ever starts using it.
        let manager = NetworkManager::new(
            /* maintenance_enabled */ false, /* low_watermark */ 2,
            /* high_watermark */ 64,
        );

        // Slot 0 is reserved at construction, so the interesting quantity is
        // the change, not the absolute.
        let allocated_before = manager.allocated_count.load(Ordering::Acquire);

        // Probed rather than guarded on uid: the capability runner grants
        // CAP_NET_ADMIN without being root, and a uid check would skip exactly
        // where this is meant to run.
        if let Err(error) = manager.build_and_destroy_slots(2) {
            eprintln!("skipping: this host cannot build network slots: {error:#}");
            return;
        }

        assert_eq!(
            manager.pool.len(),
            0,
            "the measurement path released slots into the warm pool, so a loop over \
             it recycles one slot instead of building any"
        );
        assert_eq!(
            manager.allocated_count.load(Ordering::Acquire),
            allocated_before,
            "slots were built and not torn down; each one is a leaked netns, veth \
             pair and tap device, and its index is never reused"
        );
    }

    #[test]
    #[ignore = "requires root and mutates host network state"]
    fn network_slot_creation_throughput() {
        let _host_network = lock_host_network();
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipping: network slot creation requires root");
            return;
        }

        const SLOTS_PER_ROUND: usize = 32;

        for concurrency in [1usize, 2, 4, 8] {
            let manager = NetworkManager::new(
                /* maintenance_enabled */ false, /* low_watermark */ 0,
                /* high_watermark */ 0,
            );

            let start = std::time::Instant::now();
            let mut created = Vec::with_capacity(SLOTS_PER_ROUND);
            let mut failures = 0usize;

            let mut remaining = SLOTS_PER_ROUND;
            while remaining > 0 {
                let batch = remaining.min(concurrency);
                let results: Vec<Result<Slot>> = std::thread::scope(|scope| {
                    let handles: Vec<_> = (0..batch)
                        .map(|_| scope.spawn(|| manager.allocate_any()))
                        .collect();
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle
                                .join()
                                .unwrap_or_else(|_| Err(anyhow!("slot allocation thread panicked")))
                        })
                        .collect()
                });
                for result in results {
                    match result {
                        Ok(slot) => created.push(slot),
                        Err(err) => {
                            failures += 1;
                            eprintln!("slot allocation failed: {err:#}");
                        }
                    }
                }
                remaining -= batch;
            }

            let elapsed = start.elapsed();
            let ok = created.len();
            eprintln!(
                "concurrency={concurrency:2} slots={ok:3} failures={failures:2} \
                 elapsed={elapsed:?} per_slot={:?} slots_per_sec={:.1}",
                elapsed.checked_div(ok.max(1) as u32).unwrap_or_default(),
                ok as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
            );

            for slot in created {
                if let Err(err) = manager.cleanup_slot_and_release_bit(slot) {
                    eprintln!("slot cleanup failed: {err:#}");
                }
            }
        }
    }

    /// Publishing the flag before the rules were applied let a second filler
    /// build a slot against a half-written host chain. The loser must wait for
    /// the winner's restore to commit, not skip past it.
    #[test]
    fn a_second_installer_waits_for_the_first_restore_to_commit() {
        static STATE: GlobalHostIptables = GlobalHostIptables::new();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        // `Sender`/`Receiver` are `Send` but not `Sync`, and the scoped threads
        // borrow them.
        let entered_tx = Mutex::new(entered_tx);
        let release_rx = Mutex::new(release_rx);
        let applies = AtomicUsize::new(0);
        let loser_returned = AtomicBool::new(false);

        std::thread::scope(|scope| {
            let winner = scope.spawn(|| {
                STATE.install_once(|| {
                    applies.fetch_add(1, Ordering::SeqCst);
                    entered_tx
                        .lock()
                        .unwrap()
                        .send(())
                        .expect("winner announces it is applying");
                    release_rx
                        .lock()
                        .unwrap()
                        .recv()
                        .expect("winner waits to be released");
                    Ok(())
                })
            });

            entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("the winner should reach its apply");

            let loser = scope.spawn(|| {
                let result = STATE.install_once(|| {
                    applies.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                });
                loser_returned.store(true, Ordering::SeqCst);
                result
            });

            std::thread::sleep(Duration::from_millis(100));
            // Observed before releasing the winner: asserting here would leave
            // the winner blocked inside its apply and the scope would wait for
            // it forever, turning a failure into a hang.
            let returned_early = loser_returned.load(Ordering::SeqCst);

            release_tx.send(()).expect("release the winner");
            winner.join().expect("winner thread").expect("winner apply");
            loser.join().expect("loser thread").expect("loser apply");

            assert!(
                !returned_early,
                "the loser returned while the host rules were still being written"
            );
        });

        assert_eq!(
            applies.load(Ordering::SeqCst),
            1,
            "the rules should be applied exactly once per process"
        );
        assert!(STATE.is_installed());
    }

    /// The same ordering, at the static every slot reads. The helper test above
    /// proves `install_once`'s own logic; this one proves the install the
    /// manager actually performs publishes through it, rather than raising the
    /// flag around it.
    #[test]
    fn the_global_rules_are_published_only_after_they_commit() {
        static PROBE_RAN: AtomicBool = AtomicBool::new(false);
        static FLAG_SEEN_UP: AtomicBool = AtomicBool::new(false);

        fn probe(_host_interaction_cidr: Ipv4Network) -> Result<()> {
            FLAG_SEEN_UP.store(HOST_IPTABLES.is_installed(), Ordering::SeqCst);
            PROBE_RAN.store(true, Ordering::SeqCst);
            // Failing leaves the static exactly as this test found it.
            Err(anyhow!("probe restore"))
        }

        // An earlier successful install in this process would short-circuit
        // before the probe and make the assertions vacuous.
        let was_installed = HOST_IPTABLES.take_installed();
        let cidr = NetworkAddressPlan::default().host_interaction_cidr();

        let result = install_host_iptables(&HOST_IPTABLES, cidr, probe);

        if was_installed {
            let _ = install_host_iptables(&HOST_IPTABLES, cidr, |_| Ok(()));
        }
        assert!(result.is_err(), "the probe restore should have failed");
        assert!(
            PROBE_RAN.load(Ordering::SeqCst),
            "the install returned without applying anything"
        );
        assert!(
            !FLAG_SEEN_UP.load(Ordering::SeqCst),
            "the rules were published while the restore was still being written"
        );
    }

    /// The same invariant, driven through the method the node actually calls.
    ///
    /// `the_global_rules_are_published_only_after_they_commit` exercises
    /// `install_host_iptables`, one frame below `install_global_host_iptables`,
    /// so a version of the latter that publishes first and applies afterwards
    /// is invisible to it. Asserting on the static after a failed install is
    /// enough to see that: a publish-before-commit leaves the flag up even
    /// though nothing was written to the host.
    #[test]
    fn a_failed_global_install_leaves_nothing_published() {
        fn refuse(_host_interaction_cidr: Ipv4Network) -> Result<()> {
            Err(anyhow!("restore refused"))
        }

        // An earlier success in this process would short-circuit the install
        // and make the assertion vacuous.
        let was_installed = HOST_IPTABLES.take_installed();
        let cidr = NetworkAddressPlan::default().host_interaction_cidr();

        let result = install_host_iptables(&HOST_IPTABLES, cidr, refuse);

        assert!(
            result.is_err(),
            "the probe refused, so the install must fail"
        );
        assert!(
            !HOST_IPTABLES.is_installed(),
            "a failed restore must leave the global rules unpublished; a flag set \
             before the apply would claim rules the host never received"
        );

        if was_installed {
            let _ = install_host_iptables(&HOST_IPTABLES, cidr, |_| Ok(()));
        }
    }

    /// A failed restore must leave the rules unpublished: they are the
    /// precondition for every slot, so the next caller has to retry rather
    /// than inherit a claim that nothing honored.
    #[test]
    fn a_failed_install_stays_unpublished_and_is_retried() {
        static STATE: GlobalHostIptables = GlobalHostIptables::new();

        let failed = STATE.install_once(|| Err(anyhow!("iptables-restore failed")));
        assert!(failed.is_err());
        assert!(!STATE.is_installed());

        STATE
            .install_once(|| Ok(()))
            .expect("the retry should apply");
        assert!(STATE.is_installed());
    }

    /// A refill that keeps failing used to re-enter with no sleep at all: the
    /// worker recomputes its action, still finds the pool below target, and
    /// runs the whole failing batch again. The pause has to grow, and it has
    /// to be bounded.
    #[test]
    fn a_failing_fill_backs_off_exponentially_between_bounds() {
        let gate = FillGate::new();
        assert_eq!(gate.blocked_for(0), None, "a fresh gate allows a fill");

        gate.record_failure(0);
        assert_eq!(gate.blocked_for(0), Some(FILL_BACKOFF_MIN));
        assert_eq!(gate.blocked_for(49), Some(Duration::from_millis(1)));
        assert_eq!(gate.blocked_for(50), None, "the pause must end");

        gate.record_failure(50);
        assert_eq!(gate.blocked_for(50), Some(Duration::from_millis(100)));

        for failure in 0..20 {
            gate.record_failure(failure * 10_000);
        }
        assert_eq!(
            gate.blocked_for(190_000),
            Some(FILL_BACKOFF_MAX),
            "the backoff must stay bounded"
        );
    }

    #[test]
    fn a_successful_fill_clears_the_backoff() {
        let gate = FillGate::new();
        gate.record_failure(0);
        assert!(gate.blocked_for(0).is_some());

        gate.record_success();
        assert_eq!(gate.blocked_for(0), None);
    }

    /// Exhaustion is not a slow failure, it is a fixed one: no amount of
    /// waiting produces a slot index that is not there. Only a returned slot
    /// clears it.
    #[test]
    fn exhaustion_blocks_refill_until_a_slot_comes_back() {
        let gate = FillGate::new();
        gate.note_slots_exhausted();
        assert!(gate.blocked_for(0).is_some());
        assert!(
            gate.blocked_for(u64::MAX).is_some(),
            "time alone must not clear exhaustion"
        );

        gate.note_slot_returned();
        assert_eq!(gate.blocked_for(0), None);
    }

    /// The latch has to be set and cleared by the real allocation path, not
    /// only by its own accessors.
    #[test]
    fn running_out_of_slot_indices_latches_the_gate_until_one_is_released() {
        let manager = NetworkManager::new(false, 0, 0);
        let mut taken = Vec::new();
        while let Some(idx) = manager.take_next_slot_bit() {
            taken.push(idx);
        }
        // How many were handed out is not the property, and it is not fixed:
        // construction reserves slot 0 and every `veth-N` already on the host,
        // which on a shared machine is whatever else is running there.
        assert!(
            !taken.is_empty(),
            "the manager should have had indices to hand out"
        );

        assert!(
            manager.fill_gate.blocked_for(u64::MAX).is_some(),
            "exhaustion should have latched the refill gate"
        );

        let released = taken.pop().expect("at least one slot was taken");
        manager.free_slot_bit(released);
        assert_eq!(
            manager.fill_gate.blocked_for(0),
            None,
            "a returned slot should reopen the gate"
        );
    }

    /// The backoff throttles nothing unless the cycle that refills consults it.
    /// Exhaustion is the plainest case: no amount of retrying produces a slot
    /// index that is not there, so the cycle must not attempt a fill at all —
    /// and an attempt, whether it succeeds or fails, counts itself.
    #[test]
    fn an_exhausted_gate_stops_the_cycle_before_it_attempts_a_fill() {
        let spy = MetricSpy::default();
        let counters = Arc::clone(&spy.counters);
        let manager = manager_with_capacity(1);
        manager.fill_gate.note_slots_exhausted();

        metrics::with_local_recorder(&spy, || {
            manager
                .run_pool_maintenance_cycle()
                .expect("a declined cycle is not an error")
        });

        assert_eq!(
            counters
                .lock()
                .unwrap()
                .get("agentenv_pool_fill_total")
                .copied(),
            None,
            "the cycle attempted a refill while the slot space was exhausted"
        );
    }

    /// A slot that will not clean up must not abandon the slots built beside
    /// it: they hold bitmap bits and kernel devices, and the cycle used to
    /// return on the first failure.
    #[test]
    fn cleanup_failures_are_aggregated_rather_than_returned_one_at_a_time() {
        assert!(slot_cleanup_result(Vec::new()).is_ok());

        let err = slot_cleanup_result(vec!["slot 4: busy".to_string(), "slot 9: busy".to_string()])
            .expect_err("failures should surface");
        let message = err.to_string();
        assert!(message.contains("slot 4"), "unexpected error: {message}");
        assert!(message.contains("slot 9"), "unexpected error: {message}");
    }

    /// `Drop` cannot await, so it hands the release off — but it has to hand it
    /// to the pool shutdown drains. A bare `std::thread::spawn` is untracked:
    /// between the handoff and its completion the slot is in neither the warm
    /// pool nor the bitmap, so a shutdown that raced it would leave the veth
    /// and the namespace mount behind, and the next boot would reserve that
    /// index forever.
    ///
    /// `release_detached` is the only path to that handoff, and no unit test
    /// reaches its one production caller (`FirecrackerSandbox::drop`), so the
    /// count moves by exactly one.
    #[test]
    fn a_detached_release_is_handed_to_the_drained_pool() {
        // `&'static self`, as the sandbox drop path holds it.
        let manager: &'static NetworkManager = Box::leak(Box::new(manager_with_capacity(1)));
        let slot = manager.allocate_test_slot().expect("a slot to release");
        let handed_off = BACKGROUND_RELEASES.handed_off();

        manager.release_detached(slot);

        assert_eq!(
            BACKGROUND_RELEASES.handed_off(),
            handed_off + 1,
            "the release was detached without being tracked"
        );
        assert!(
            BACKGROUND_RELEASES.drain(Duration::from_secs(5)),
            "the drain timed out waiting for a detached release"
        );
    }

    /// And shutdown has to wait for what was handed off, at the static the
    /// handoff goes to.
    #[test]
    fn shutdown_waits_for_a_detached_release_to_finish() {
        let manager = manager_with_capacity(1);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));

        let task_finished = Arc::clone(&finished);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();
        BACKGROUND_RELEASES.spawn(move || {
            release_rx.recv().expect("the release waits to be let go");
            task_finished.store(true, Ordering::SeqCst);
        });

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            let _ = release_tx.send(());
        });

        manager.shutdown().expect("shutdown");

        assert!(
            finished.load(Ordering::SeqCst),
            "shutdown returned while a detached release was still running"
        );
    }
}
