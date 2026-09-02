use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::SandboxState;
use crate::cfg::MemoryControlConfig;
use crate::sandbox::{SandboxBackend, SandboxMemoryTelemetry};
use crate::types::{SandboxId, SandboxResources};

/// A point-in-time snapshot of orchestrator metrics, returned by
/// `Orchestrator::metrics_snapshot()`.
///
/// The two creation counters are accumulated incrementally (see
/// [`OrchestratorCounters`]). All other resource fields are derived directly
/// from the live sandbox metadata at the time of the snapshot, so they cannot
/// drift from the orchestrator's actual state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrchestratorMetrics {
    pub create_successes: u64,
    pub create_fails: u64,
    pub running_sandbox_count: u32,
    pub starting_sandbox_count: u32,
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    /// Number of sandboxes currently in the `Paused` state.
    pub paused_sandbox_count: u32,
    pub paused_allocated_cpu: u32,
    pub paused_allocated_memory_bytes: u64,
}

/// Monotonic creation counters maintained by the orchestrator.
///
/// These cannot be derived from the metadata store because they accumulate
/// outcomes of historical attempts (including failures whose metadata was
/// removed). They are stored as atomics so updates do not require a lock.
#[derive(Debug, Default)]
pub(crate) struct OrchestratorCounters {
    create_successes: AtomicU64,
    create_fails: AtomicU64,
}

impl OrchestratorCounters {
    pub fn record_create_success(&self, count: u64) {
        self.create_successes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_create_fail(&self, count: u64) {
        self.create_fails.fetch_add(count, Ordering::Relaxed);
    }

    pub fn create_successes(&self) -> u64 {
        self.create_successes.load(Ordering::Relaxed)
    }

    pub fn create_fails(&self) -> u64 {
        self.create_fails.load(Ordering::Relaxed)
    }
}

/// The metrics contribution of a single sandbox in a given state.
///
/// This is the single source of truth for how a sandbox state maps to runtime
/// resource metrics:
///
/// - `running_sandbox_count` counts every state in which the VM is alive and
///   logically owned by the orchestrator from a serving / capacity
///   perspective: `Running`, plus the short-lived transitional states
///   `Pausing`, `Snapshotting`, and `Killing` where the VM is still up but is
///   being moved out of the running set. This matches the historical
///   incremental-counter behavior, where `running_sandbox_count` was not
///   decremented on entry to those transitional states.
/// - `starting_sandbox_count` counts only `Creating` and `Resuming`.
/// - Allocated CPU / memory are counted in every state except `Paused`, since
///   a paused sandbox has released its VM-side resources.
/// - `paused_sandbox_count` and the `paused_allocated_*` fields are populated
///   only for the `Paused` state, and are tracked separately from the active
///   running set so that schedulers can apply an "including paused" ceiling
///   without conflating the two.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SandboxContribution {
    running_sandbox_count: u32,
    starting_sandbox_count: u32,
    allocated_cpu: u32,
    allocated_memory_bytes: u64,
    paused_sandbox_count: u32,
    paused_allocated_cpu: u32,
    paused_allocated_memory_bytes: u64,
}

impl SandboxContribution {
    pub(crate) fn new(state: SandboxState, resources: SandboxResources) -> Self {
        let is_paused = matches!(state, SandboxState::Paused);
        let counts_as_running = matches!(
            state,
            SandboxState::Running
                | SandboxState::Pausing
                | SandboxState::Snapshotting
                | SandboxState::Forking
                | SandboxState::Killing
        );
        let counts_as_starting = matches!(state, SandboxState::Creating | SandboxState::Resuming);
        let memory_bytes = u64::from(resources.memory_mib) * 1024 * 1024;
        Self {
            running_sandbox_count: u32::from(counts_as_running),
            starting_sandbox_count: u32::from(counts_as_starting),
            allocated_cpu: if !is_paused { resources.cpu_count } else { 0 },
            allocated_memory_bytes: if !is_paused { memory_bytes } else { 0 },
            paused_sandbox_count: u32::from(is_paused),
            paused_allocated_cpu: if is_paused { resources.cpu_count } else { 0 },
            paused_allocated_memory_bytes: if is_paused { memory_bytes } else { 0 },
        }
    }
}

/// Adds one sandbox's runtime resource contribution into an
/// [`OrchestratorMetrics`] snapshot under construction.
///
/// The counter fields (`create_successes` / `create_fails`) are intentionally
/// untouched and must be filled in by the caller from [`OrchestratorCounters`].
pub(crate) fn aggregate_resource_metrics(
    metrics: &mut OrchestratorMetrics,
    contribution: SandboxContribution,
) {
    metrics.running_sandbox_count = metrics
        .running_sandbox_count
        .saturating_add(contribution.running_sandbox_count);
    metrics.starting_sandbox_count = metrics
        .starting_sandbox_count
        .saturating_add(contribution.starting_sandbox_count);
    metrics.allocated_cpu = metrics
        .allocated_cpu
        .saturating_add(contribution.allocated_cpu);
    metrics.allocated_memory_bytes = metrics
        .allocated_memory_bytes
        .saturating_add(contribution.allocated_memory_bytes);
    metrics.paused_sandbox_count = metrics
        .paused_sandbox_count
        .saturating_add(contribution.paused_sandbox_count);
    metrics.paused_allocated_cpu = metrics
        .paused_allocated_cpu
        .saturating_add(contribution.paused_allocated_cpu);
    metrics.paused_allocated_memory_bytes = metrics
        .paused_allocated_memory_bytes
        .saturating_add(contribution.paused_allocated_memory_bytes);
}

// ── Memory-pressure control policy ───────────────────────────────────────────

/// What the control loop remembers about one sandbox between passes.
///
/// Kept by the loop itself rather than in the metadata store: it is derived,
/// per-node, and worthless after a restart or a move to another host, and
/// writing it back would put a periodic writer in contention with the sandbox
/// state machine for no gain.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MemoryControlState {
    previous: Option<SandboxMemoryTelemetry>,
    /// Consecutive passes this sandbox has looked over-provisioned. A shrink
    /// costs the guest a re-fault of everything it gives back, so one idle
    /// sample is not enough to pay for one.
    slack_passes: u32,
}

/// Where the control loop wants a sandbox's hot-pluggable memory to sit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryPlugDecision {
    Hold,
    Grow { target_mib: u32 },
    Shrink { target_mib: u32 },
}

impl MemoryPlugDecision {
    /// Stable label for the decision counter. Bounded to three values.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Hold => "hold",
            Self::Grow { .. } => "grow",
            Self::Shrink { .. } => "shrink",
        }
    }

    pub(crate) fn target_mib(&self) -> Option<u32> {
        match self {
            Self::Hold => None,
            Self::Grow { target_mib } | Self::Shrink { target_mib } => Some(*target_mib),
        }
    }
}

impl MemoryControlState {
    /// Fold one telemetry sample in and say where the plug target should go.
    ///
    /// `ceiling_mib` is the sandbox's requested memory, which stays the
    /// contract: the loop moves the guest's *actual* RAM around underneath it
    /// but never above it, so the number the API reported at creation remains
    /// the amount the sandbox can ever reach.
    ///
    /// `node_under_pressure` is the node-level backpressure the design calls
    /// for. Without it a loop that reacts to per-guest pressure will happily
    /// plug a host into its own OOM killer, because every guest on a
    /// contended host reports pressure at once.
    pub(crate) fn observe(
        &mut self,
        sample: SandboxMemoryTelemetry,
        ceiling_mib: u32,
        node_under_pressure: bool,
        config: &MemoryControlConfig,
    ) -> MemoryPlugDecision {
        // A guest that did not report its totals has said nothing about its
        // position, and silence is not slack. Folding a blank sample into the
        // hysteresis would strip a device-present/driver-absent guest to zero
        // one step at a time, so it is dropped: neither the history nor the
        // slack count moves, and the next real sample is still compared
        // against the last real one.
        let Some(position) = sample.guest_position() else {
            return MemoryPlugDecision::Hold;
        };
        let previous = self.previous.replace(sample);

        let Some(plugged_mib) = sample.plugged_mib else {
            // No virtio-mem device: telemetry is still worth collecting for
            // reporting, but there is nothing to move.
            self.slack_passes = 0;
            return MemoryPlugDecision::Hold;
        };

        let distressed = position.available_percent < u64::from(config.grow_available_percent)
            || counter_advanced(previous, sample);

        if distressed && !node_under_pressure {
            self.slack_passes = 0;
            // The ceiling governs the guest's *total* memory, while the value
            // being moved is the hot-plug region alone. Under the boot-floor
            // split the guest boots with a floor and virtio-mem supplies the
            // rest, so headroom has to be measured against what the guest
            // already has: clamping the plugged size to the ceiling would let
            // the total reach floor + ceiling. A guest that already holds its
            // full memoryMB therefore has no room, which is the contract.
            let headroom = ceiling_mib.saturating_sub(position.total_mib);
            let step = config.max_step_mib.min(headroom);
            return if step == 0 {
                MemoryPlugDecision::Hold
            } else {
                MemoryPlugDecision::Grow {
                    target_mib: plugged_mib.saturating_add(step),
                }
            };
        }

        let slack = position.available_percent > u64::from(config.shrink_available_percent);
        if !slack {
            // Under node pressure too. A guest at 0% available with a moving
            // OOM counter is not over-provisioned: unplugging its pages
            // deepens the guest OOM and returns nothing to the host, because
            // the guest cannot release what it is faulting. Node pressure
            // refuses growth for everyone; it does not license reclaim from
            // a guest that has no slack to give.
            self.slack_passes = 0;
            return MemoryPlugDecision::Hold;
        }

        // A host over its watermark reclaims from the slackest guests without
        // waiting out the hysteresis: the hysteresis exists to stop a sandbox
        // oscillating, and a host running out of memory is a worse outcome
        // than one sandbox re-faulting its working set.
        if !node_under_pressure {
            self.slack_passes = self.slack_passes.saturating_add(1);
            if self.slack_passes < config.shrink_hysteresis_passes {
                return MemoryPlugDecision::Hold;
            }
        }
        if plugged_mib == 0 {
            return MemoryPlugDecision::Hold;
        }

        self.slack_passes = 0;
        let step = config.max_step_mib.min(plugged_mib);
        MemoryPlugDecision::Shrink {
            target_mib: plugged_mib - step,
        }
    }
}

/// Whether the guest reported new allocation distress since the last sample.
///
/// Both are cumulative counters, so only their movement is a signal; their
/// absolute values say what happened at some point in the VM's whole life.
/// A first sample has nothing to compare against and reports no movement.
fn counter_advanced(
    previous: Option<SandboxMemoryTelemetry>,
    current: SandboxMemoryTelemetry,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    counter_moved(previous.oom_kills, current.oom_kills)
        || counter_moved(previous.alloc_stalls, current.alloc_stalls)
}

/// A counter the guest does not report stays absent for the VM's whole life,
/// so an absent reading is not a value to compare — it contributes no signal
/// in either direction rather than reading as a fall to zero or a rise from it.
fn counter_moved(previous: Option<u64>, current: Option<u64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current > previous)
}

/// One sandbox as the control loop sees it: what it is allowed to reach, what
/// state it is in, and the handle that has to be acquired without waiting.
pub(crate) struct MemoryControlSandbox {
    pub id: SandboxId,
    pub state: SandboxState,
    pub ceiling_mib: u32,
    pub handle: Arc<Mutex<Box<dyn SandboxBackend>>>,
}

/// What one pass did. Logged by the loop and asserted on by its tests, so the
/// counters below can never drift from what the pass actually performed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemoryControlPassReport {
    pub sampled: u32,
    pub skipped_busy: u32,
    pub skipped_not_running: u32,
    pub skipped_unsupported: u32,
    /// Backend calls abandoned on [`BACKEND_CALL_TIMEOUT`].
    pub skipped_timeout: u32,
    /// Samples in which the guest reported no usable totals. Counted rather
    /// than acted on: an operator seeing these climb has a guest whose driver
    /// never reports, which no amount of policy tuning will fix.
    pub blank_samples: u32,
    /// Samples from a guest that reports neither allocation-distress counter,
    /// so its only signal is the available ratio.
    pub blind_to_distress: u32,
    pub grown: u32,
    pub shrunk: u32,
    pub actuation_failed: u32,
}

/// How long one backend call may hold a sandbox's mutex before the control
/// loop abandons it.
///
/// The Firecracker API client sets no timeout of its own, and every
/// orchestrator lifecycle operation — pause, resume, snapshot, fork, delete —
/// waits unbounded on the same mutex. An unresponsive API socket must
/// therefore cost this pass one sandbox, not the node's whole control plane.
const BACKEND_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Sample every sandbox that will answer without waiting, and move the ones
/// that need it.
///
/// `node_under_pressure` is the node-level backpressure: with it set, nothing
/// grows and slack is reclaimed immediately.
pub(crate) async fn run_memory_control_pass(
    sandboxes: Vec<MemoryControlSandbox>,
    node_under_pressure: bool,
    config: &MemoryControlConfig,
    states: &mut HashMap<SandboxId, MemoryControlState>,
) -> MemoryControlPassReport {
    let mut report = MemoryControlPassReport::default();
    states.retain(|id, _| sandboxes.iter().any(|sandbox| sandbox.id == *id));

    for sandbox in sandboxes {
        // Every transitional state either has a capture in flight or is on its
        // way out. A plug or unplug during a capture would move pages the
        // dirty-range path cannot describe: an unplug is a host-side discard,
        // which is neither dirty nor clean, and only the excluded userfaultfd
        // path models a removed page at all.
        if sandbox.state != SandboxState::Running {
            report.skipped_not_running += 1;
            record_skip("not_running");
            continue;
        }

        // try_lock, never lock().await: the same mutex is held for the whole
        // of a pause, snapshot or fork, which includes reading the entire
        // dirty set out of the guest. Waiting for it would turn a control loop
        // into a queue behind every capture on the node.
        let Ok(mut backend) = sandbox.handle.try_lock() else {
            report.skipped_busy += 1;
            record_skip("busy");
            continue;
        };

        let capability = backend.runtime_info().mem_control;
        if !capability.balloon_stats {
            report.skipped_unsupported += 1;
            record_skip("unsupported");
            continue;
        }
        let telemetry = match tokio::time::timeout(BACKEND_CALL_TIMEOUT, backend.memory_telemetry())
            .await
        {
            Ok(Ok(Some(telemetry))) => telemetry,
            Ok(Ok(None)) => {
                report.skipped_unsupported += 1;
                record_skip("unsupported");
                continue;
            }
            Ok(Err(err)) => {
                report.skipped_unsupported += 1;
                record_skip("error");
                debug!(sandbox_id = %sandbox.id, error = %err, "memory telemetry sample failed");
                continue;
            }
            Err(_) => {
                report.skipped_timeout += 1;
                record_skip("timeout");
                warn!(
                    sandbox_id = %sandbox.id,
                    "memory telemetry sample did not answer; abandoning it so the sandbox \
                     lock is not held against every lifecycle operation"
                );
                continue;
            }
        };
        report.sampled += 1;
        if telemetry.guest_position().is_none() {
            // Not a skip in the loop's control flow — `observe` holds on a
            // blank sample by itself — but it is a sandbox the loop can never
            // act on, so it is counted where an operator looks for that.
            report.blank_samples += 1;
            record_skip("blank");
        }
        if telemetry.is_blind_to_distress() {
            report.blind_to_distress += 1;
        }

        let decision = states.entry(sandbox.id).or_default().observe(
            telemetry,
            sandbox.ceiling_mib,
            node_under_pressure,
            config,
        );
        metrics::counter!(
            "agentenv_memory_control_decisions_total",
            "decision" => decision.label(),
        )
        .increment(1);

        let Some(target_mib) = decision.target_mib() else {
            continue;
        };
        if !capability.hotplug {
            // The decision is still worth counting — it is what the loop would
            // do for a guest whose ceiling can move — but this one has no
            // device to move.
            continue;
        }
        match tokio::time::timeout(
            BACKEND_CALL_TIMEOUT,
            backend.set_memory_plug_target(target_mib),
        )
        .await
        {
            Ok(Ok(())) => {
                metrics::histogram!("agentenv_memory_control_plug_target_mib")
                    .record(f64::from(target_mib));
                match decision {
                    MemoryPlugDecision::Grow { .. } => report.grown += 1,
                    MemoryPlugDecision::Shrink { .. } => report.shrunk += 1,
                    MemoryPlugDecision::Hold => {}
                }
            }
            Ok(Err(err)) => {
                report.actuation_failed += 1;
                warn!(
                    sandbox_id = %sandbox.id,
                    target_mib,
                    error = %err,
                    "failed to move the memory plug target"
                );
            }
            Err(_) => {
                report.skipped_timeout += 1;
                record_skip("timeout");
                warn!(
                    sandbox_id = %sandbox.id,
                    target_mib,
                    "moving the memory plug target did not answer; abandoning it so the \
                     sandbox lock is not held against every lifecycle operation"
                );
            }
        }
    }

    report
}

/// Reasons are a closed set, so this label stays bounded however many
/// sandboxes the node runs.
fn record_skip(reason: &'static str) {
    metrics::counter!(
        "agentenv_memory_control_sandboxes_skipped_total",
        "reason" => reason,
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
    };
    use crate::orchestrator::{SandboxMetadata, SandboxState};
    use crate::types::SandboxResources;

    fn meta(state: SandboxState, cpu: u32, memory_mib: u32) -> SandboxMetadata {
        SandboxMetadata {
            state,
            resources: SandboxResources {
                cpu_count: cpu,
                memory_mib,
                disk_size_mib: 0,
            },
            ..Default::default()
        }
    }

    fn aggregate<'a>(metas: impl IntoIterator<Item = &'a SandboxMetadata>) -> OrchestratorMetrics {
        let mut metrics = OrchestratorMetrics::default();
        for metadata in metas {
            aggregate_resource_metrics(
                &mut metrics,
                SandboxContribution::new(metadata.state, metadata.resources),
            );
        }
        metrics
    }

    use super::{
        run_memory_control_pass, MemoryControlSandbox, MemoryControlState, MemoryPlugDecision,
        BACKEND_CALL_TIMEOUT,
    };
    use crate::cfg::MemoryControlConfig;
    use crate::sandbox::mock::{MockAction, MockBehavior, MockOperation, MockSandboxBackend};
    use crate::sandbox::{
        MemoryControlCapability, SandboxBackend, SandboxMemoryTelemetry, SandboxRuntimeInfo,
    };
    use crate::types::SandboxId;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    fn control_config() -> MemoryControlConfig {
        MemoryControlConfig {
            enabled: true,
            interval_secs: 15,
            grow_available_percent: 10,
            shrink_available_percent: 40,
            shrink_hysteresis_passes: 3,
            max_step_mib: 256,
            node_memory_high_percent: 85,
        }
    }

    /// The boot floor every guest starts with under the design's split: the
    /// guest boots on `BOOT_FLOOR_MIB` and virtio-mem supplies the rest, so
    /// the guest's total is always strictly larger than the hot-plug region
    /// the loop moves.
    const BOOT_FLOOR_MIB: u32 = 512;

    /// A guest holding `plugged_mib` on top of the boot floor and reporting
    /// `available_percent` of its total memory as available.
    fn sample(available_percent: u64, plugged_mib: u32) -> SandboxMemoryTelemetry {
        let total_bytes = u64::from(BOOT_FLOOR_MIB + plugged_mib) * 1024 * 1024;
        SandboxMemoryTelemetry {
            available_bytes: Some(total_bytes * available_percent / 100),
            total_bytes: Some(total_bytes),
            disk_caches_bytes: Some(0),
            oom_kills: Some(0),
            alloc_stalls: Some(0),
            plugged_mib: Some(plugged_mib),
            requested_mib: Some(plugged_mib),
        }
    }

    #[test]
    fn a_starved_guest_grows_by_one_step() {
        let config = control_config();
        let mut state = MemoryControlState::default();
        // 5% available, below the 10% grow threshold.
        let decision = state.observe(sample(5, 512), 2048, false, &config);
        assert_eq!(decision, MemoryPlugDecision::Grow { target_mib: 768 });
    }

    #[test]
    fn growth_stops_at_the_requested_ceiling() {
        // memoryMB stays the contract: the loop moves real RAM underneath it
        // and never past it. The ceiling governs the guest's TOTAL memory
        // while the value being moved is the hot-plug region, and the two
        // differ by the boot floor — clamping the plugged size to the ceiling
        // would let the total reach floor + memoryMB.
        let config = control_config();
        let mut state = MemoryControlState::default();
        // Guest total 900 MiB (512 floor + 388 plugged) under a 1024 ceiling:
        // 124 MiB of room, less than one 256 MiB step.
        let decision = state.observe(sample(5, 388), 1024, false, &config);
        assert_eq!(
            decision,
            MemoryPlugDecision::Grow { target_mib: 512 },
            "the step must be clamped by the room left under the ceiling"
        );

        // A guest whose total already equals its ceiling has no room at all,
        // however small its hot-plug region is.
        let mut at_ceiling = MemoryControlState::default();
        assert_eq!(
            at_ceiling.observe(sample(5, 512), 1024, false, &config),
            MemoryPlugDecision::Hold
        );
    }

    #[test]
    fn an_allocation_stall_grows_a_guest_that_still_looks_idle() {
        // The ratio alone is a lagging signal: a guest can be thrashing its
        // allocator while the last pushed sample still shows slack.
        let config = control_config();
        let mut state = MemoryControlState::default();
        assert_eq!(
            state.observe(sample(90, 512), 2048, false, &config),
            MemoryPlugDecision::Hold
        );

        let mut stalled = sample(90, 512);
        stalled.alloc_stalls = Some(1);
        assert_eq!(
            state.observe(stalled, 2048, false, &config),
            MemoryPlugDecision::Grow { target_mib: 768 }
        );
    }

    #[test]
    fn a_first_sample_is_never_read_as_a_counter_advance() {
        // The counters are cumulative over the VM's whole life, so a nonzero
        // first reading says nothing about now.
        let config = control_config();
        let mut state = MemoryControlState::default();
        let mut historical = sample(90, 512);
        historical.alloc_stalls = Some(4200);
        historical.oom_kills = Some(3);
        assert_eq!(
            state.observe(historical, 2048, false, &config),
            MemoryPlugDecision::Hold
        );
    }

    #[test]
    fn slack_must_persist_before_anything_is_taken_back() {
        // A shrink makes the guest re-fault whatever it gives up, so one idle
        // sample must not pay for one.
        let config = control_config();
        let mut state = MemoryControlState::default();
        for pass in 1..config.shrink_hysteresis_passes {
            assert_eq!(
                state.observe(sample(90, 512), 2048, false, &config),
                MemoryPlugDecision::Hold,
                "shrank on pass {pass}, before the hysteresis elapsed"
            );
        }
        assert_eq!(
            state.observe(sample(90, 512), 2048, false, &config),
            MemoryPlugDecision::Shrink { target_mib: 256 }
        );
    }

    #[test]
    fn one_busy_sample_resets_the_slack_count() {
        let config = control_config();
        let mut state = MemoryControlState::default();
        state.observe(sample(90, 512), 2048, false, &config);
        state.observe(sample(90, 512), 2048, false, &config);
        // Back inside the band: neither slack nor distress.
        assert_eq!(
            state.observe(sample(30, 512), 2048, false, &config),
            MemoryPlugDecision::Hold
        );
        assert_eq!(
            state.observe(sample(90, 512), 2048, false, &config),
            MemoryPlugDecision::Hold,
            "the hysteresis count must restart after a busy pass"
        );
    }

    #[test]
    fn a_host_over_its_watermark_refuses_growth_and_reclaims_at_once() {
        // Without this, every guest on a contended host reports pressure
        // simultaneously and the loop plugs the host into its own OOM killer.
        let config = control_config();
        let mut starved = MemoryControlState::default();
        assert_eq!(
            starved.observe(sample(5, 512), 2048, true, &config),
            MemoryPlugDecision::Hold,
            "node pressure refuses growth; it does not license reclaim"
        );

        let mut idle = MemoryControlState::default();
        assert_eq!(
            idle.observe(sample(90, 512), 2048, true, &config),
            MemoryPlugDecision::Shrink { target_mib: 256 },
            "node pressure must not wait out the per-sandbox hysteresis"
        );
    }

    #[test]
    fn node_pressure_does_not_reclaim_from_a_starving_guest() {
        // A guest at 0% available whose OOM killer is running is not
        // over-provisioned. Unplugging its pages deepens the guest OOM and
        // returns nothing to the host, because the guest cannot release what
        // it is faulting.
        let config = control_config();
        let mut state = MemoryControlState::default();
        let mut first = sample(0, 512);
        first.oom_kills = Some(1);
        assert_eq!(
            state.observe(first, 2048, true, &config),
            MemoryPlugDecision::Hold
        );

        let mut killing = sample(0, 512);
        killing.oom_kills = Some(2);
        assert_eq!(
            state.observe(killing, 2048, true, &config),
            MemoryPlugDecision::Hold,
            "a guest with no slack must not be reclaimed from under node pressure"
        );

        // And the hold is not a permanent refusal: the same guest, once it is
        // genuinely slack, is still reclaimed without waiting out hysteresis.
        assert_eq!(
            state.observe(sample(90, 512), 2048, true, &config),
            MemoryPlugDecision::Shrink { target_mib: 256 }
        );
    }

    #[test]
    fn a_guest_with_no_movable_ceiling_is_only_observed() {
        let config = control_config();
        let mut state = MemoryControlState::default();
        let mut no_device = sample(5, 0);
        no_device.plugged_mib = None;
        assert_eq!(
            state.observe(no_device, 2048, false, &config),
            MemoryPlugDecision::Hold
        );
    }

    #[test]
    fn an_unreported_total_is_never_acted_on_in_either_direction() {
        // A guest whose driver never reports S_MEMTOT pushes a blank sample
        // for its whole life. Reading it as 0% available would grow it
        // forever; reading it as 100% available is worse — the slack count
        // accumulates and the loop strips the guest's hot-plug region to zero
        // one step per hysteresis window. Silence is neither.
        let config = control_config();
        let mut state = MemoryControlState::default();
        let blank = SandboxMemoryTelemetry {
            plugged_mib: Some(512),
            ..SandboxMemoryTelemetry::default()
        };
        for pass in 0..config.shrink_hysteresis_passes + 2 {
            assert_eq!(
                state.observe(blank, 2048, false, &config),
                MemoryPlugDecision::Hold,
                "a blank sample moved the plug target on pass {pass}"
            );
        }
    }

    #[test]
    fn a_blank_sample_does_not_disturb_the_slack_history() {
        // The blank sample is dropped, not counted: a guest that goes quiet
        // for one pass must neither have its accumulated slack reset nor have
        // that pass counted towards a shrink.
        let config = control_config();
        let mut state = MemoryControlState::default();
        for _ in 1..config.shrink_hysteresis_passes {
            assert_eq!(
                state.observe(sample(90, 512), 2048, false, &config),
                MemoryPlugDecision::Hold
            );
        }

        let blank = SandboxMemoryTelemetry {
            plugged_mib: Some(512),
            ..SandboxMemoryTelemetry::default()
        };
        assert_eq!(
            state.observe(blank, 2048, false, &config),
            MemoryPlugDecision::Hold
        );
        assert_eq!(
            state.observe(sample(90, 512), 2048, false, &config),
            MemoryPlugDecision::Shrink { target_mib: 256 },
            "a blank pass must neither reset nor advance the hysteresis"
        );
    }

    #[test]
    fn a_counter_the_guest_never_reports_is_not_read_as_movement() {
        // oom_kill and alloc_stall are both gated on the guest kernel
        // version. A guest that reports neither must not look like one whose
        // counters just moved, nor like one that has definitively had none.
        let config = control_config();
        let mut state = MemoryControlState::default();
        let mut blind = sample(90, 512);
        blind.oom_kills = None;
        blind.alloc_stalls = None;
        assert!(blind.is_blind_to_distress());
        assert_eq!(
            state.observe(blind, 2048, false, &config),
            MemoryPlugDecision::Hold
        );
        assert_eq!(
            state.observe(blind, 2048, false, &config),
            MemoryPlugDecision::Hold,
            "an absent counter must not read as a counter advance"
        );
    }

    // ── Pass-level behaviour, against the mock backend ───────────────────────

    fn stats_capable(hotplug: bool) -> SandboxRuntimeInfo {
        SandboxRuntimeInfo {
            mem_control: MemoryControlCapability {
                balloon: true,
                balloon_stats: true,
                free_page_hinting: false,
                hotplug,
            },
            ..SandboxRuntimeInfo::default()
        }
    }

    fn mock_handle(behavior: &Arc<MockBehavior>) -> Arc<Mutex<Box<dyn SandboxBackend>>> {
        let backend: Box<dyn SandboxBackend> = Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(behavior),
            None,
        ));
        Arc::new(Mutex::new(backend))
    }

    fn entry(
        id: SandboxId,
        state: SandboxState,
        handle: Arc<Mutex<Box<dyn SandboxBackend>>>,
    ) -> MemoryControlSandbox {
        MemoryControlSandbox {
            id,
            state,
            ceiling_mib: 2048,
            handle,
        }
    }

    #[tokio::test]
    async fn a_locked_sandbox_is_skipped_and_the_pass_still_finishes() {
        // The same mutex is held for the whole of a capture, which reads the
        // entire dirty set out of the guest. A pass that waited for it would
        // serialize behind every pause on the node.
        let busy_behavior = Arc::new(MockBehavior::new());
        busy_behavior.set_runtime_info(stats_capable(true));
        busy_behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);
        let free_behavior = Arc::new(MockBehavior::new());
        free_behavior.set_runtime_info(stats_capable(true));
        free_behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);

        let busy = mock_handle(&busy_behavior);
        let free = mock_handle(&free_behavior);
        let held = busy.clone().lock_owned().await;

        let mut states = HashMap::new();
        // Bounded on purpose: a pass that waits for the lock instead of
        // skipping it never returns while the capture holds it.
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            run_memory_control_pass(
                vec![
                    entry(SandboxId::new(), SandboxState::Running, busy),
                    entry(SandboxId::new(), SandboxState::Running, free),
                ],
                false,
                &control_config(),
                &mut states,
            ),
        )
        .await
        .expect("the pass must not wait behind a held sandbox lock");
        drop(held);

        assert_eq!(report.skipped_busy, 1);
        assert_eq!(report.sampled, 1);
        assert_eq!(report.grown, 1);
        assert_eq!(busy_behavior.plug_targets(), Vec::<u32>::new());
        assert_eq!(free_behavior.plug_targets(), vec![768]);
    }

    #[tokio::test]
    async fn a_sandbox_being_captured_is_never_plugged() {
        // An unplug is a host-side discard, and the dirty-range capture path
        // has no concept of a removed page, so a resize mid-capture would drop
        // guest memory silently.
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(true));
        behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);

        let mut states = HashMap::new();
        for state in [
            SandboxState::Snapshotting,
            SandboxState::Pausing,
            SandboxState::Forking,
            SandboxState::Killing,
            SandboxState::Resuming,
        ] {
            let report = run_memory_control_pass(
                vec![entry(SandboxId::new(), state, mock_handle(&behavior))],
                false,
                &control_config(),
                &mut states,
            )
            .await;
            assert_eq!(report.skipped_not_running, 1, "state {state:?} was sampled");
        }
        assert_eq!(behavior.plug_targets(), Vec::<u32>::new());
    }

    #[tokio::test]
    async fn a_sandbox_without_balloon_statistics_is_a_permanent_opt_out() {
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(SandboxRuntimeInfo::default());
        behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);

        let mut states = HashMap::new();
        let report = run_memory_control_pass(
            vec![entry(
                SandboxId::new(),
                SandboxState::Running,
                mock_handle(&behavior),
            )],
            false,
            &control_config(),
            &mut states,
        )
        .await;

        assert_eq!(report.skipped_unsupported, 1);
        assert_eq!(report.sampled, 0);
        assert_eq!(behavior.plug_targets(), Vec::<u32>::new());
    }

    #[tokio::test]
    async fn no_growth_is_issued_while_the_node_is_over_its_watermark() {
        let starved = Arc::new(MockBehavior::new());
        starved.set_runtime_info(stats_capable(true));
        starved.set_memory_telemetry(vec![Some(sample(5, 512))]);
        let idle = Arc::new(MockBehavior::new());
        idle.set_runtime_info(stats_capable(true));
        idle.set_memory_telemetry(vec![Some(sample(90, 512))]);

        let mut states = HashMap::new();
        let report = run_memory_control_pass(
            vec![
                entry(
                    SandboxId::new(),
                    SandboxState::Running,
                    mock_handle(&starved),
                ),
                entry(SandboxId::new(), SandboxState::Running, mock_handle(&idle)),
            ],
            true,
            &control_config(),
            &mut states,
        )
        .await;

        assert_eq!(report.grown, 0);
        assert_eq!(report.shrunk, 1);
        assert_eq!(
            starved.plug_targets(),
            Vec::<u32>::new(),
            "a starved guest must neither grow nor be reclaimed from under node pressure"
        );
        assert_eq!(
            idle.plug_targets(),
            vec![256],
            "an over-provisioned guest is reclaimed at once under node pressure"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_backend_call_does_not_park_the_sandbox_lock() {
        // The pass holds the per-sandbox mutex across the backend call, and
        // that mutex is what pause, resume, snapshot, fork and delete all wait
        // on with an unbounded `lock().await`. The Firecracker API client sets
        // no timeout of its own, so an unresponsive API socket would otherwise
        // park the sandbox's lock forever and take the node's whole control
        // plane with it.
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(true));
        behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);
        behavior.push_action(
            MockOperation::MemoryTelemetry,
            MockAction::SucceedAfter(Duration::from_secs(600)),
        );
        // Fired from inside the backend call, with the sandbox lock already
        // held, so the contention below is real rather than a race with the
        // spawned task starting.
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&entered);
        behavior.set_on_operation(
            MockOperation::MemoryTelemetry,
            Arc::new(move || signal.notify_one()),
        );

        let handle = mock_handle(&behavior);
        let pass_handle = Arc::clone(&handle);
        let pass = tokio::spawn(async move {
            let mut states = HashMap::new();
            run_memory_control_pass(
                vec![entry(SandboxId::new(), SandboxState::Running, pass_handle)],
                false,
                &control_config(),
                &mut states,
            )
            .await
        });
        entered.notified().await;

        // Generously more than the loop's own bound and far less than the
        // hung call: with the bound in place the lock frees as soon as the
        // pass gives up, and without it this never acquires.
        let lock_wait = BACKEND_CALL_TIMEOUT * 4;
        let acquired = tokio::time::timeout(lock_wait, handle.lock()).await;
        assert!(
            acquired.is_ok(),
            "a lifecycle operation waited {lock_wait:?} behind a hung control pass"
        );
        drop(acquired);

        let report = pass.await.expect("the pass task must not panic");
        assert_eq!(report.skipped_timeout, 1);
        assert_eq!(report.sampled, 0);
        assert_eq!(behavior.plug_targets(), Vec::<u32>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_actuation_does_not_park_the_sandbox_lock() {
        // The same hazard on the write side: moving the plug target is a
        // second unbounded HTTP round trip made with the sandbox lock held.
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(true));
        behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);
        behavior.push_action(
            MockOperation::SetMemoryPlugTarget,
            MockAction::SucceedAfter(Duration::from_secs(600)),
        );
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&entered);
        behavior.set_on_operation(
            MockOperation::SetMemoryPlugTarget,
            Arc::new(move || signal.notify_one()),
        );

        let handle = mock_handle(&behavior);
        let pass_handle = Arc::clone(&handle);
        let pass = tokio::spawn(async move {
            let mut states = HashMap::new();
            run_memory_control_pass(
                vec![entry(SandboxId::new(), SandboxState::Running, pass_handle)],
                false,
                &control_config(),
                &mut states,
            )
            .await
        });
        entered.notified().await;

        let lock_wait = BACKEND_CALL_TIMEOUT * 4;
        let acquired = tokio::time::timeout(lock_wait, handle.lock()).await;
        assert!(
            acquired.is_ok(),
            "a lifecycle operation waited {lock_wait:?} behind a hung actuation"
        );
        drop(acquired);

        let report = pass.await.expect("the pass task must not panic");
        assert_eq!(report.skipped_timeout, 1);
        assert_eq!(report.grown, 0);
    }

    #[tokio::test]
    async fn a_guest_that_reports_no_totals_is_counted_and_never_actuated() {
        // Device present, driver silent: the sample arrives every pass and
        // says nothing. The loop must hold, and an operator must be able to
        // see why rather than watching the guest get stripped.
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(true));
        behavior.set_memory_telemetry(vec![Some(SandboxMemoryTelemetry {
            plugged_mib: Some(512),
            ..SandboxMemoryTelemetry::default()
        })]);

        let config = control_config();
        let mut states = HashMap::new();
        let id = SandboxId::new();
        for _ in 0..config.shrink_hysteresis_passes + 2 {
            let report = run_memory_control_pass(
                vec![entry(id, SandboxState::Running, mock_handle(&behavior))],
                false,
                &config,
                &mut states,
            )
            .await;
            assert_eq!(report.blank_samples, 1);
            assert_eq!(report.blind_to_distress, 1);
            assert_eq!(report.shrunk, 0);
            assert_eq!(report.grown, 0);
        }
        assert_eq!(
            behavior.plug_targets(),
            Vec::<u32>::new(),
            "a guest that reports nothing must never be unplugged"
        );
    }

    #[tokio::test]
    async fn a_guest_with_no_virtio_mem_device_is_sampled_but_never_actuated() {
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(false));
        behavior.set_memory_telemetry(vec![Some(sample(5, 512))]);

        let mut states = HashMap::new();
        let report = run_memory_control_pass(
            vec![entry(
                SandboxId::new(),
                SandboxState::Running,
                mock_handle(&behavior),
            )],
            false,
            &control_config(),
            &mut states,
        )
        .await;

        assert_eq!(report.sampled, 1);
        assert_eq!(report.grown, 0);
        assert_eq!(behavior.plug_targets(), Vec::<u32>::new());
    }

    #[tokio::test]
    async fn history_for_a_departed_sandbox_does_not_accumulate() {
        let behavior = Arc::new(MockBehavior::new());
        behavior.set_runtime_info(stats_capable(true));
        behavior.set_memory_telemetry(vec![Some(sample(50, 512))]);

        let config = control_config();
        let mut states = HashMap::new();
        let gone = SandboxId::new();
        run_memory_control_pass(
            vec![entry(gone, SandboxState::Running, mock_handle(&behavior))],
            false,
            &config,
            &mut states,
        )
        .await;
        assert!(states.contains_key(&gone));

        run_memory_control_pass(Vec::new(), false, &config, &mut states).await;
        assert!(
            states.is_empty(),
            "a deleted sandbox must not be remembered"
        );
    }

    #[test]
    fn counters_accumulate_monotonically() {
        let counters = OrchestratorCounters::default();
        counters.record_create_success(1);
        counters.record_create_success(1);
        counters.record_create_fail(1);
        counters.record_create_fail(3);
        assert_eq!(
            (counters.create_successes(), counters.create_fails()),
            (2, 4)
        );
    }

    #[test]
    fn counters_accumulate_multiple_successes() {
        let counters = OrchestratorCounters::default();
        counters.record_create_success(3);
        counters.record_create_fail(1);
        assert_eq!(
            (counters.create_successes(), counters.create_fails()),
            (3, 1)
        );
    }

    #[test]
    fn aggregate_counts_running_and_starting() {
        let metas = [
            meta(SandboxState::Running, 2, 256),
            meta(SandboxState::Running, 1, 128),
            meta(SandboxState::Creating, 4, 512),
            meta(SandboxState::Resuming, 1, 64),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 2);
        assert_eq!(metrics.starting_sandbox_count, 2);
        assert_eq!(metrics.allocated_cpu, 2 + 1 + 4 + 1);
        assert_eq!(
            metrics.allocated_memory_bytes,
            u64::from(256u32 + 128 + 512 + 64) * 1024 * 1024
        );
    }

    #[test]
    fn aggregate_excludes_only_paused_from_resources() {
        // Paused is the only state that has released its VM-side resources.
        // Killing still holds CPU/memory because the VM has not yet stopped,
        // and it counts toward `running_sandbox_count` as a transitional
        // exit from the running set. Paused sandboxes are tracked separately
        // in the paused_* fields.
        let metas = [
            meta(SandboxState::Paused, 8, 1024),
            meta(SandboxState::Killing, 2, 256),
            meta(SandboxState::Running, 1, 128),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 2);
        assert_eq!(metrics.starting_sandbox_count, 0);
        assert_eq!(metrics.allocated_cpu, 2 + 1);
        assert_eq!(
            metrics.allocated_memory_bytes,
            u64::from(256u32 + 128) * 1024 * 1024
        );
        assert_eq!(metrics.paused_sandbox_count, 1);
        assert_eq!(metrics.paused_allocated_cpu, 8);
        assert_eq!(metrics.paused_allocated_memory_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn aggregate_treats_pausing_and_snapshotting_as_running_with_resources() {
        // Pausing / Snapshotting keep the VM alive while the orchestrator
        // transitions the sandbox out of the live serving set, so they
        // contribute to both `running_sandbox_count` and the allocated
        // CPU / memory totals.
        let metas = [
            meta(SandboxState::Pausing, 2, 256),
            meta(SandboxState::Snapshotting, 1, 128),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 2);
        assert_eq!(metrics.starting_sandbox_count, 0);
        assert_eq!(metrics.allocated_cpu, 3);
        assert_eq!(metrics.allocated_memory_bytes, (256u64 + 128) * 1024 * 1024);
        assert_eq!(metrics.paused_sandbox_count, 0);
        assert_eq!(metrics.paused_allocated_cpu, 0);
        assert_eq!(metrics.paused_allocated_memory_bytes, 0);
    }

    #[test]
    fn aggregate_paused_sums_independently_of_active_resources() {
        // Multiple paused sandboxes accumulate into the paused_* totals only,
        // never into allocated_cpu / allocated_memory_bytes. Active sandboxes
        // contribute only to the active fields.
        let metas = [
            meta(SandboxState::Paused, 4, 512),
            meta(SandboxState::Paused, 2, 128),
            meta(SandboxState::Running, 1, 64),
        ];
        let metrics = aggregate(metas.iter());
        assert_eq!(metrics.running_sandbox_count, 1);
        assert_eq!(metrics.allocated_cpu, 1);
        assert_eq!(metrics.allocated_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(metrics.paused_sandbox_count, 2);
        assert_eq!(metrics.paused_allocated_cpu, 4 + 2);
        assert_eq!(
            metrics.paused_allocated_memory_bytes,
            (512u64 + 128) * 1024 * 1024
        );
    }

    #[test]
    fn aggregate_empty_produces_default() {
        let metrics = aggregate(std::iter::empty());
        assert_eq!(metrics.running_sandbox_count, 0);
        assert_eq!(metrics.starting_sandbox_count, 0);
        assert_eq!(metrics.allocated_cpu, 0);
        assert_eq!(metrics.allocated_memory_bytes, 0);
        assert_eq!(metrics.paused_sandbox_count, 0);
        assert_eq!(metrics.paused_allocated_cpu, 0);
        assert_eq!(metrics.paused_allocated_memory_bytes, 0);
    }
}
