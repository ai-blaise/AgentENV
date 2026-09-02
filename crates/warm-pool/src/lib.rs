//! Generic warm resource pool with watermark-based maintenance.
//!
//! This crate provides reusable pool mechanics for resources that are expensive
//! to create but can be reset and reused. It handles:
//! - Watermark-based refill/drain decisions
//! - Background maintenance worker with condvar signaling
//! - Shutdown coordination with safe resource cleanup
//! - Process exit hooks for static singleton pools
//!
//! Resource-specific create/reset/delete logic is provided via trait hooks.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

/// Label value for a pool that did not name itself.
const UNNAMED_POOL: &str = "unnamed";

/// Idle resources tolerated above `high_watermark` before draining.
///
/// Without it a pool sitting at its high watermark destroys a resource every
/// time a release pushes it one over and builds one again the moment an
/// acquisition takes it one under — steady-state churn of exactly the
/// expensive thing the pool exists to keep. Sized off the watermark so a deep
/// pool tolerates proportionally more slack, with a floor of one so small
/// pools get hysteresis at all.
///
/// A `high_watermark` of zero is not a small pool, it is no pool: releases must
/// go straight back to cleanup, so that case keeps no slack at all.
fn drain_deadband(high_watermark: usize) -> usize {
    if high_watermark == 0 {
        return 0;
    }
    (high_watermark / 8).max(1)
}

/// Action computed by watermark logic for the maintenance worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMaintenanceAction {
    /// Refill the pool by creating N new resources.
    Fill(usize),
    /// Drain N excess resources from the pool.
    Drain(usize),
    /// Pool is within watermarks; no action needed.
    Idle,
}

/// Signal state for the maintenance worker condvar.
#[derive(Debug, Default)]
struct PoolMaintenanceSignal {
    /// Work is pending (refill or drain needed).
    pending: bool,
    /// Worker should exit.
    stop: bool,
}

/// Configuration for a warm pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Target lower bound for idle resource count.
    pub low_watermark: usize,
    /// Upper bound for idle resource count.
    ///
    /// Maintenance starts by refilling toward `low_watermark`, then grows the
    /// refill target geometrically toward this bound when acquisitions drain
    /// the pool below the low watermark. It is only a strict insertion cap when
    /// maintenance is disabled.
    pub high_watermark: usize,
    /// Enable background maintenance worker.
    pub maintenance_enabled: bool,
    /// Whether the pool fills itself toward `low_watermark` at startup rather
    /// than waiting for the first acquisition to drain it.
    ///
    /// Turning it off means the first callers pay full construction cost. That
    /// is the right trade only where boot time matters more than first-request
    /// latency, so it defaults on.
    pub startup_prewarm: bool,
}

impl PoolConfig {
    /// Validate and normalize config values.
    pub fn validate(mut self) -> Self {
        if self.low_watermark > self.high_watermark {
            tracing::warn!(
                low = self.low_watermark,
                high = self.high_watermark,
                "low_watermark > high_watermark; clamping low to high"
            );
            self.low_watermark = self.high_watermark;
        }
        self
    }
}

/// Generic warm pool for reusable resources.
///
/// `T` is the pooled resource type. Resource-specific create/reset/delete
/// logic is provided via closures or trait objects passed to pool methods.
///
/// `T` must be `Send` because resources may be created/destroyed on the
/// maintenance worker thread.
///
/// The built-in maintenance worker requires `start_maintenance_worker` to be
/// called on a `&'static WarmPool<T>` because the worker thread owns the
/// callback for the rest of the pool lifetime.
///
/// With maintenance enabled, `high_watermark` is a drain target rather than a
/// strict insertion cap. Release paths may temporarily exceed it, and the
/// maintenance worker is expected to drain excess resources.
pub struct WarmPool<T: Send> {
    /// Idle resources ready for reuse.
    pool: Mutex<VecDeque<T>>,
    /// Watermark config.
    config: PoolConfig,
    /// Metric label identifying this pool. Bounded by construction: one value
    /// per pool in the process.
    pool_name: &'static str,
    /// Whether an acquisition drained the pool since the last maintenance
    /// cycle. Read by the fill-target decay, which must not shrink the target
    /// of a pool that is still under load.
    pressure_since_last_cycle: AtomicBool,
    /// Current refill target. Starts at the low watermark and grows toward the
    /// high watermark under acquisition pressure. This intentionally ratchets
    /// upward for the process lifetime: after a node observes bursty demand, it
    /// keeps extra warm capacity instead of shrinking back to cold-start
    /// behavior.
    fill_target: Mutex<usize>,
    /// Background maintenance worker state.
    maintenance_signal: Mutex<PoolMaintenanceSignal>,
    /// Wakes the maintenance worker.
    maintenance_cv: Condvar,
    /// Maintenance worker thread handle.
    maintenance_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Ensures the maintenance worker only starts once.
    maintenance_started: AtomicBool,
    /// Rejects new allocations once shutdown cleanup starts.
    shutting_down: AtomicBool,
}

impl<T: Send> WarmPool<T> {
    /// Create a new warm pool with the given config.
    pub fn new(config: PoolConfig) -> Self {
        Self::named(config, UNNAMED_POOL)
    }

    /// Create a new warm pool that labels its metrics with `pool_name`.
    ///
    /// The name is `&'static str` rather than a `String` so the metric label
    /// cannot become unbounded: there is one value per pool in the process.
    pub fn named(config: PoolConfig, pool_name: &'static str) -> Self {
        let config = config.validate();
        let fill_target = config.low_watermark.min(config.high_watermark);
        metrics::gauge!("agentenv_pool_low_watermark", "pool" => pool_name)
            .set(config.low_watermark as f64);
        metrics::gauge!("agentenv_pool_high_watermark", "pool" => pool_name)
            .set(config.high_watermark as f64);
        metrics::gauge!("agentenv_pool_fill_target", "pool" => pool_name).set(fill_target as f64);
        Self {
            pool: Mutex::new(VecDeque::new()),
            fill_target: Mutex::new(fill_target),
            pool_name,
            pressure_since_last_cycle: AtomicBool::new(false),
            config,
            maintenance_signal: Mutex::new(PoolMaintenanceSignal::default()),
            maintenance_cv: Condvar::new(),
            maintenance_worker: Mutex::new(None),
            maintenance_started: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Check if the pool is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Return the validated pool configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Return the current number of idle resources.
    pub fn len(&self) -> usize {
        self.pool.lock().unwrap().len()
    }

    /// Return whether the pool currently has no idle resources.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compute the maintenance action based on current pool size.
    pub fn compute_maintenance_action(&self, pool_len: usize) -> PoolMaintenanceAction {
        let fill_target = self.current_fill_target();
        if pool_len < fill_target {
            let to_fill = fill_target.saturating_sub(pool_len);
            if to_fill > 0 {
                return PoolMaintenanceAction::Fill(to_fill);
            }
        }
        if pool_len > self.config.high_watermark + drain_deadband(self.config.high_watermark) {
            // Drain the full excess in one maintenance cycle. Resource-specific
            // cleanup happens outside the pool lock, and shutdown paths already
            // have to tolerate draining the whole pool.
            let to_drain = pool_len - self.config.high_watermark;
            if to_drain > 0 {
                return PoolMaintenanceAction::Drain(to_drain);
            }
        }
        PoolMaintenanceAction::Idle
    }

    fn current_fill_target(&self) -> usize {
        (*self.fill_target.lock().unwrap()).min(self.config.high_watermark)
    }

    fn grow_fill_target_after_pressure(&self, pool_len: usize) {
        if pool_len >= self.config.low_watermark || self.config.high_watermark == 0 {
            return;
        }

        let low = self.config.low_watermark.min(self.config.high_watermark);
        let mut target = self.fill_target.lock().unwrap();
        let next = (*target)
            .max(low)
            .max(1)
            .saturating_mul(2)
            .min(self.config.high_watermark);
        *target = next;
        metrics::gauge!("agentenv_pool_fill_target", "pool" => self.pool_name).set(next as f64);
    }

    /// Lowers the refill target after a cycle that saw no acquisition pressure.
    ///
    /// The target only ever grew, for the life of the process. Paired with a
    /// drain from above, that turns one burst into permanent churn: the target
    /// keeps demanding resources the drain keeps destroying. Halving per quiet
    /// cycle gives back the burst capacity slowly enough that a bursty node
    /// keeps it and an idle one does not.
    pub fn decay_fill_target_when_quiet(&self, pool_len: usize) {
        if self.pressure_since_last_cycle.swap(false, Ordering::AcqRel) {
            return;
        }

        let low = self.config.low_watermark.min(self.config.high_watermark);
        let mut target = self.fill_target.lock().unwrap();
        if pool_len < *target || *target <= low {
            return;
        }
        let next = (*target / 2).max(low);
        *target = next;
        metrics::gauge!("agentenv_pool_fill_target", "pool" => self.pool_name).set(next as f64);
    }

    fn record_acquisition_pressure(&self, pool_len: usize) {
        self.pressure_since_last_cycle
            .store(true, Ordering::Release);
        self.grow_fill_target_after_pressure(pool_len);
        if matches!(
            self.compute_maintenance_action(pool_len),
            PoolMaintenanceAction::Fill(_)
        ) {
            self.request_maintenance();
        }
    }

    /// Request the maintenance worker to wake up and check watermarks.
    pub fn request_maintenance(&self) {
        if !self.config.maintenance_enabled || self.is_shutting_down() {
            return;
        }

        let mut signal = self.maintenance_signal.lock().unwrap();
        if signal.stop {
            return;
        }
        signal.pending = true;
        self.maintenance_cv.notify_one();
    }

    /// Try to acquire a resource from the pool (fast path).
    ///
    /// Returns `Some(resource)` if one is available, `None` if the pool is empty.
    /// Acquisition pressure grows the refill target and wakes maintenance even
    /// on a miss, allowing a burst that starts from an empty pool to influence
    /// future warm capacity.
    pub fn try_acquire(&self) -> Option<T> {
        if self.is_shutting_down() {
            return None;
        }
        let mut pool = self.pool.lock().unwrap();
        let resource = pool.pop_front();
        let next_pool_len = pool.len();
        drop(pool);
        self.record_acquire(resource.is_some(), next_pool_len);
        resource
    }

    /// Try to acquire the first resource matching `predicate`.
    ///
    /// This is useful when only some idle resources are reusable for a request.
    pub fn try_acquire_where(&self, mut predicate: impl FnMut(&T) -> bool) -> Option<T> {
        if self.is_shutting_down() {
            return None;
        }
        let mut pool = self.pool.lock().unwrap();
        let resource = pool
            .iter()
            .position(&mut predicate)
            .and_then(|idx| pool.remove(idx));
        let next_pool_len = pool.len();
        drop(pool);
        self.record_acquire(resource.is_some(), next_pool_len);
        resource
    }

    fn record_acquire(&self, hit: bool, pool_len: usize) {
        metrics::counter!(
            "agentenv_pool_acquire_total",
            "pool" => self.pool_name,
            "result" => if hit { "hit" } else { "miss" },
        )
        .increment(1);
        metrics::gauge!("agentenv_pool_size", "pool" => self.pool_name).set(pool_len as f64);
        self.record_acquisition_pressure(pool_len);
    }

    /// Try to enqueue an idle resource only if the pool is below the high watermark.
    ///
    /// This is intended for maintenance refill paths that have just created a
    /// resource and need a final bounded insert before publishing it as idle.
    pub fn try_push_bounded(&self, resource: T) -> Result<(), T> {
        if !self.is_shutting_down() {
            let mut pool = self.pool.lock().unwrap();
            if !self.is_shutting_down() && pool.len() < self.config.high_watermark {
                pool.push_back(resource);
                return Ok(());
            }
        }
        Err(resource)
    }

    /// Drain one idle resource from the back of the pool.
    pub fn try_drain_one(&self) -> Option<T> {
        let mut pool = self.pool.lock().unwrap();
        pool.pop_back()
    }

    /// Return a resource to the pool.
    ///
    /// If maintenance is enabled, enqueues the resource even when the pool is
    /// above the high watermark so the maintenance worker owns all drain
    /// decisions. If maintenance is disabled, respects the high watermark and
    /// returns `Err(resource)` when the pool is full.
    pub fn release(&self, resource: T) -> Result<(), T> {
        if !self.is_shutting_down() {
            let mut pool = self.pool.lock().unwrap();
            // Re-check after taking the lock to avoid racing shutdown.
            if !self.is_shutting_down()
                && (self.config.maintenance_enabled || pool.len() < self.config.high_watermark)
            {
                let next_pool_len = pool.len() + 1;
                pool.push_back(resource);
                drop(pool);
                metrics::counter!(
                    "agentenv_pool_release_total",
                    "pool" => self.pool_name,
                    "result" => "pooled",
                )
                .increment(1);
                metrics::gauge!("agentenv_pool_size", "pool" => self.pool_name)
                    .set(next_pool_len as f64);
                if next_pool_len < self.config.low_watermark
                    || next_pool_len > self.config.high_watermark
                {
                    self.request_maintenance();
                }
                return Ok(());
            }
        }
        // Pool is full or shutting down: return the resource to the caller,
        // which destroys it rather than pooling it.
        metrics::counter!(
            "agentenv_pool_release_total",
            "pool" => self.pool_name,
            "result" => "rejected",
        )
        .increment(1);
        Err(resource)
    }

    /// Drain all resources from the pool and return them.
    ///
    /// This is intended for shutdown cleanup. After calling this, the pool
    /// rejects new releases.
    pub fn drain_all(&self) -> Vec<T> {
        self.shutting_down.store(true, Ordering::Release);
        self.stop_maintenance_worker();

        let mut pool = self.pool.lock().unwrap();
        pool.drain(..).collect()
    }

    /// Start the background maintenance worker if not already started.
    ///
    /// The worker runs the provided `run_cycle` closure in a loop, which
    /// should call `compute_maintenance_action` and perform the necessary
    /// create/delete operations.
    ///
    /// One cycle is requested immediately unless `startup_prewarm` is off.
    pub fn start_maintenance_worker<F>(&'static self, run_cycle: F)
    where
        F: Fn() + Send + 'static,
    {
        if !self.config.maintenance_enabled {
            return;
        }
        if self.maintenance_started.swap(true, Ordering::AcqRel) {
            return;
        }

        match std::thread::Builder::new()
            .name("warm-pool-maintenance".to_string())
            .spawn(move || self.maintenance_worker_loop(run_cycle))
        {
            Ok(handle) => {
                *self.maintenance_worker.lock().unwrap() = Some(handle);
                // Without this the pool stays empty until the first acquisition
                // drops it below the low watermark, so the first callers pay
                // full construction cost — which is exactly what the pool
                // exists to avoid.
                if self.config.startup_prewarm {
                    self.request_maintenance();
                }
            }
            Err(err) => {
                self.maintenance_started.store(false, Ordering::Release);
                tracing::warn!(error = %err, "failed to start warm pool maintenance worker");
            }
        }
    }

    fn maintenance_worker_loop<F>(&self, run_cycle: F)
    where
        F: Fn(),
    {
        let mut has_immediate_work = false;
        loop {
            let mut signal = self.maintenance_signal.lock().unwrap();
            if !has_immediate_work {
                while !signal.stop && !signal.pending {
                    signal = self.maintenance_cv.wait(signal).unwrap();
                }
            }
            // Checked on both paths. A cycle that leaves work outstanding —
            // a fill that keeps failing, a watermark that cannot be reached —
            // sets `has_immediate_work` every time round, and a stop flag read
            // only in the waiting branch would then never be seen: the worker
            // spins, `stop_maintenance_worker` blocks on the join, and process
            // shutdown blocks behind it.
            if signal.stop {
                break;
            }
            signal.pending = false;
            drop(signal);

            if self.is_shutting_down() {
                break;
            }

            run_cycle();

            if self.is_shutting_down() {
                break;
            }

            has_immediate_work = {
                let pool_len = self.pool.lock().unwrap().len();
                self.decay_fill_target_when_quiet(pool_len);
                !matches!(
                    self.compute_maintenance_action(pool_len),
                    PoolMaintenanceAction::Idle
                )
            };
        }
    }

    fn stop_maintenance_worker(&self) {
        if !self.maintenance_started.load(Ordering::Acquire) {
            return;
        }

        {
            let mut signal = self.maintenance_signal.lock().unwrap();
            signal.stop = true;
            signal.pending = true;
            self.maintenance_cv.notify_all();
        }

        if let Some(handle) = self.maintenance_worker.lock().unwrap().take() {
            if let Err(err) = handle.join() {
                tracing::warn!(?err, "warm pool maintenance worker panicked during join");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation_clamps_low_to_high() {
        let config = PoolConfig {
            low_watermark: 64,
            high_watermark: 32,
            maintenance_enabled: true,
            startup_prewarm: false,
        }
        .validate();
        assert_eq!(config.low_watermark, 32);
        assert_eq!(config.high_watermark, 32);
    }

    #[test]
    fn compute_maintenance_action_fills_to_initial_low_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 4,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert_eq!(
            pool.compute_maintenance_action(2),
            PoolMaintenanceAction::Fill(2)
        );
        assert_eq!(
            pool.compute_maintenance_action(7),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn acquisition_pressure_grows_fill_target_geometrically() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });

        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(2)
        );

        pool.release(1).unwrap();
        pool.release(2).unwrap();
        assert_eq!(pool.try_acquire(), Some(1));
        assert_eq!(
            pool.compute_maintenance_action(1),
            PoolMaintenanceAction::Fill(3)
        );

        assert_eq!(pool.try_acquire(), Some(2));
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );
    }

    #[test]
    fn acquisition_misses_grow_fill_target_geometrically() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });

        assert_eq!(pool.try_acquire(), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(4)
        );

        assert_eq!(pool.try_acquire(), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(8)
        );

        assert_eq!(pool.try_acquire_where(|_| true), None);
        assert_eq!(
            pool.compute_maintenance_action(0),
            PoolMaintenanceAction::Fill(10)
        );
    }

    #[test]
    fn acquisition_miss_requests_maintenance() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 10,
            maintenance_enabled: true,
            startup_prewarm: false,
        });

        assert!(!pool.maintenance_signal.lock().unwrap().pending);
        assert_eq!(pool.try_acquire(), None);
        assert!(pool.maintenance_signal.lock().unwrap().pending);
    }

    #[test]
    fn compute_maintenance_action_drains_above_high() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 4,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert_eq!(
            pool.compute_maintenance_action(8),
            PoolMaintenanceAction::Drain(4)
        );
        assert_eq!(
            pool.compute_maintenance_action(4),
            PoolMaintenanceAction::Idle
        );
    }

    #[test]
    fn try_acquire_returns_none_when_empty() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn try_acquire_returns_resource_when_available() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(42).unwrap();
        assert_eq!(pool.try_acquire(), Some(42));
    }

    #[test]
    fn release_respects_high_watermark_when_maintenance_disabled() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        assert!(pool.release(1).is_ok());
        assert!(pool.release(2).is_ok());
        assert_eq!(pool.release(3), Err(3));
    }

    #[test]
    fn release_allows_maintenance_worker_to_drain_above_high_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 2,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert!(pool.release(1).is_ok());
        assert!(pool.release(2).is_ok());
        assert!(pool.release(3).is_ok());
        assert_eq!(pool.pool.lock().unwrap().len(), 3);
    }

    #[test]
    fn drain_all_empties_pool_and_sets_shutting_down() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(1).unwrap();
        pool.release(2).unwrap();
        let drained = pool.drain_all();
        assert_eq!(drained, vec![1, 2]);
        assert!(pool.is_shutting_down());
        assert!(pool.pool.lock().unwrap().is_empty());
    }

    #[test]
    fn try_acquire_returns_none_after_shutdown() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.release(42).unwrap();
        pool.drain_all();
        assert!(pool.try_acquire().is_none());
    }

    #[test]
    fn release_rejects_after_shutdown() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 10,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        pool.drain_all();
        assert_eq!(pool.release(42), Err(42));
    }

    /// A pool sitting at its high watermark used to destroy a resource the
    /// moment a release pushed it one over, and build one again the moment an
    /// acquisition took it one under. The deadband is what stops a deep pool
    /// from turning one burst into permanent create/destroy churn.
    #[test]
    fn draining_waits_for_a_deadband_above_the_high_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 64,
            high_watermark: 64,
            maintenance_enabled: true,
            startup_prewarm: false,
        });

        assert_eq!(
            pool.compute_maintenance_action(65),
            PoolMaintenanceAction::Idle,
            "one resource over the watermark is not worth destroying"
        );
        assert_eq!(
            pool.compute_maintenance_action(72),
            PoolMaintenanceAction::Idle
        );
        assert_eq!(
            pool.compute_maintenance_action(73),
            PoolMaintenanceAction::Drain(9),
            "past the deadband the whole excess goes"
        );
    }

    /// A zero high watermark means the pool is off: a released resource has to
    /// be destroyed, not held as slack. The throughput harness isolates the
    /// cold path this way.
    #[test]
    fn a_disabled_pool_keeps_no_slack() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 0,
            high_watermark: 0,
            maintenance_enabled: true,
            startup_prewarm: false,
        });
        assert_eq!(
            pool.compute_maintenance_action(1),
            PoolMaintenanceAction::Drain(1)
        );
    }

    /// The refill target used to ratchet up for the process lifetime, so a
    /// single burst left the pool permanently demanding resources that the
    /// drain above the high watermark keeps destroying.
    #[test]
    fn a_quiet_cycle_lowers_the_refill_target_toward_the_low_watermark() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 16,
            maintenance_enabled: false,
            startup_prewarm: false,
        });

        // Three misses in a row: the burst the target is meant to absorb.
        for _ in 0..3 {
            assert_eq!(pool.try_acquire(), None);
        }
        assert_eq!(pool.current_fill_target(), 16);

        // The cycle that observed the pressure must not decay.
        pool.decay_fill_target_when_quiet(16);
        assert_eq!(pool.current_fill_target(), 16);

        pool.decay_fill_target_when_quiet(16);
        assert_eq!(pool.current_fill_target(), 8);
        pool.decay_fill_target_when_quiet(16);
        assert_eq!(pool.current_fill_target(), 4);
        pool.decay_fill_target_when_quiet(16);
        assert_eq!(pool.current_fill_target(), 2);
        pool.decay_fill_target_when_quiet(16);
        assert_eq!(
            pool.current_fill_target(),
            2,
            "the target must not decay below the low watermark"
        );
    }

    /// Decay must not shrink the target of a pool that is still being drained
    /// by acquisitions, and must not act while the pool is still filling.
    #[test]
    fn decay_leaves_a_pool_under_pressure_alone() {
        let pool = WarmPool::<u32>::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 16,
            maintenance_enabled: false,
            startup_prewarm: false,
        });
        for _ in 0..3 {
            assert_eq!(pool.try_acquire(), None);
        }
        assert_eq!(pool.current_fill_target(), 16);

        pool.decay_fill_target_when_quiet(16);
        assert_eq!(pool.try_acquire(), None);
        pool.decay_fill_target_when_quiet(16);
        assert_eq!(
            pool.current_fill_target(),
            16,
            "a cycle that saw an acquisition must not decay the target"
        );

        pool.decay_fill_target_when_quiet(1);
        assert_eq!(
            pool.current_fill_target(),
            16,
            "a pool still below its target is not idle capacity"
        );
    }
}

#[cfg(test)]
mod maintenance_worker_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn config(startup_prewarm: bool) -> PoolConfig {
        PoolConfig {
            low_watermark: 2,
            high_watermark: 4,
            maintenance_enabled: true,
            startup_prewarm,
        }
    }

    /// The pool is leaked because `start_maintenance_worker` takes
    /// `&'static self`; a test process exits long before that matters.
    fn leaked(startup_prewarm: bool) -> &'static WarmPool<u32> {
        Box::leak(Box::new(WarmPool::new(config(startup_prewarm))))
    }

    /// Counts maintenance cycles in the moments after the worker starts, with
    /// a cycle that actually satisfies the watermark so the loop settles.
    fn cycles_after_start(startup_prewarm: bool) -> usize {
        let pool = leaked(startup_prewarm);
        let cycles = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cycles);
        pool.start_maintenance_worker(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            if let PoolMaintenanceAction::Fill(to_fill) =
                pool.compute_maintenance_action(pool.len())
            {
                for _ in 0..to_fill {
                    let _ = pool.release(0);
                }
            }
        });
        // Long enough for a requested cycle to have run. The worker is
        // otherwise idle, so no cycle appears on its own.
        std::thread::sleep(Duration::from_millis(200));
        let observed = cycles.load(Ordering::SeqCst);
        pool.stop_maintenance_worker();
        observed
    }

    /// Without a startup cycle the pool stays empty until the first
    /// acquisition drains it, so the first callers pay full construction cost
    /// — which is what the pool exists to avoid.
    #[test]
    fn prewarm_requests_a_cycle_as_soon_as_the_worker_starts() {
        assert!(
            cycles_after_start(true) >= 1,
            "startup prewarm should have run a maintenance cycle"
        );
        assert!(
            cycles_after_start(true) < 1000,
            "a cycle that meets the watermark should settle, not spin"
        );
    }

    /// The flag used to be advisory while the worker always requested a cycle,
    /// so turning prewarm off did nothing at all.
    #[test]
    fn disabling_prewarm_leaves_the_pool_alone_until_demand_arrives() {
        assert_eq!(
            cycles_after_start(false),
            0,
            "no cycle should run before something asks for a resource"
        );
    }

    /// The decay reaches a real pool only through the worker loop, which
    /// recomputes the target between cycles. A loop that skips it leaves a node
    /// that saw one burst demanding that capacity for the rest of its life,
    /// while the drain above the high watermark keeps destroying what the
    /// target keeps demanding.
    #[test]
    fn the_worker_loop_decays_the_refill_target_between_cycles() {
        let pool: &'static WarmPool<u32> = Box::leak(Box::new(WarmPool::new(PoolConfig {
            low_watermark: 2,
            high_watermark: 16,
            maintenance_enabled: true,
            startup_prewarm: false,
        })));

        // Three misses in a row: the burst that ratchets the target up to the
        // high watermark.
        for _ in 0..3 {
            assert_eq!(pool.try_acquire(), None);
        }
        assert_eq!(pool.current_fill_target(), 16);
        // Held at the target, so each cycle sees idle capacity rather than a
        // pool that is still filling.
        for _ in 0..16 {
            pool.release(0).expect("the pool should accept a release");
        }

        pool.start_maintenance_worker(|| {});
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.current_fill_target() == 16 && Instant::now() < deadline {
            pool.request_maintenance();
            std::thread::sleep(Duration::from_millis(5));
        }
        pool.stop_maintenance_worker();

        assert!(
            pool.current_fill_target() < 16,
            "the worker loop never decayed the ratcheted refill target"
        );
    }

    /// A cycle that never satisfies the watermark — a fill that keeps failing,
    /// a resource the host cannot currently build — leaves work outstanding
    /// every time round. The worker must still stop: otherwise the join blocks
    /// forever and process shutdown blocks behind it.
    #[test]
    fn a_worker_whose_cycles_never_finish_the_work_can_still_be_stopped() {
        let pool = leaked(true);
        pool.start_maintenance_worker(|| {
            // Never adds anything, so the pool stays below the watermark and
            // the loop always believes it has immediate work.
            std::thread::sleep(Duration::from_millis(1));
        });
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        pool.stop_maintenance_worker();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "stopping took {:?}; the worker never observed the stop flag",
            started.elapsed()
        );
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Captures counter increments and gauge sets so a test can read back what
    /// the pool actually emitted. Installed per-thread, so it does not disturb
    /// a process-wide recorder.
    #[derive(Default, Clone)]
    struct MetricSpy {
        values: Arc<Mutex<HashMap<String, f64>>>,
    }

    struct SpyMetric {
        key: String,
        values: Arc<Mutex<HashMap<String, f64>>>,
    }

    fn key_with_labels(key: &metrics::Key) -> String {
        let mut labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();
        labels.sort();
        format!("{}{{{}}}", key.name(), labels.join(","))
    }

    impl metrics::CounterFn for SpyMetric {
        fn increment(&self, value: u64) {
            *self
                .values
                .lock()
                .unwrap()
                .entry(self.key.clone())
                .or_insert(0.0) += value as f64;
        }
        fn absolute(&self, value: u64) {
            self.values
                .lock()
                .unwrap()
                .insert(self.key.clone(), value as f64);
        }
    }

    impl metrics::GaugeFn for SpyMetric {
        fn increment(&self, _value: f64) {}
        fn decrement(&self, _value: f64) {}
        fn set(&self, value: f64) {
            self.values.lock().unwrap().insert(self.key.clone(), value);
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
            metrics::Counter::from_arc(Arc::new(SpyMetric {
                key: key_with_labels(key),
                values: Arc::clone(&self.values),
            }))
        }
        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::from_arc(Arc::new(SpyMetric {
                key: key_with_labels(key),
                values: Arc::clone(&self.values),
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

    /// The pool had no metrics at all: starvation was visible only as latency
    /// in whatever the pool fed. Emitting from the crate rather than from one
    /// consumer is what gives all three pools the same numbers.
    #[test]
    fn the_pool_publishes_its_watermarks_and_its_acquisitions() {
        let spy = MetricSpy::default();
        let values = Arc::clone(&spy.values);
        metrics::with_local_recorder(&spy, || {
            let pool = WarmPool::<u32>::named(
                PoolConfig {
                    low_watermark: 2,
                    high_watermark: 16,
                    maintenance_enabled: false,
                    startup_prewarm: false,
                },
                "network",
            );

            assert_eq!(pool.try_acquire(), None);
            pool.release(7).unwrap();
            assert_eq!(pool.try_acquire(), Some(7));
        });

        let values = values.lock().unwrap();
        assert_eq!(
            values.get("agentenv_pool_low_watermark{pool=network}"),
            Some(&2.0)
        );
        assert_eq!(
            values.get("agentenv_pool_high_watermark{pool=network}"),
            Some(&16.0)
        );
        assert_eq!(
            values.get("agentenv_pool_fill_target{pool=network}"),
            Some(&8.0),
            "both acquisitions left the pool below the low watermark, so both \
             grew the refill target, and each growth should be published"
        );
        assert_eq!(
            values.get("agentenv_pool_acquire_total{pool=network,result=miss}"),
            Some(&1.0)
        );
        assert_eq!(
            values.get("agentenv_pool_acquire_total{pool=network,result=hit}"),
            Some(&1.0)
        );
        assert_eq!(
            values.get("agentenv_pool_release_total{pool=network,result=pooled}"),
            Some(&1.0)
        );
    }
}
