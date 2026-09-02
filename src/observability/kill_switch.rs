//! Node-side reaction to losing contact with the scheduler.
//!
//! A node that cannot reach the scheduler keeps running everything it has while
//! the scheduler drops its bindings and, before health-gated placement, kept
//! sending it new work. Nothing on the node noticed. The kill switch closes the
//! node's half of that: after a bounded period without a successful heartbeat,
//! the node stops accepting new sandboxes.
//!
//! # Why this is not simply "pause everything"
//!
//! With a single scheduler replica, a scheduler restart is indistinguishable
//! from a partition from the node's point of view. An action that paused every
//! sandbox would then pause the fleet on every scheduler deploy — converting a
//! routine operation into a fleet-wide outage. Refusing new work is safe under
//! that ambiguity; pausing existing work is not, so it stays opt-in and is
//! meant for deployments with a replicated scheduler.
//!
//! # Clock
//!
//! The elapsed time is measured on a monotonic clock. Sandbox expiry elsewhere
//! uses the wall clock because it is API-visible, but a safety margin must not
//! move when NTP steps: a backward step would silently extend the window, and a
//! forward step would fire the switch on a healthy node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// What a node does once it has lost contact for longer than the threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KillSwitchAction {
    /// Do nothing. The default: the switch ships inert.
    #[default]
    Disabled,
    /// Refuse new sandboxes; leave running ones alone.
    ///
    /// Safe under the restart/partition ambiguity, because it gives up only
    /// work the node has not started yet.
    BlockCreates,
}

/// Tracks how long a node has been out of contact with the scheduler.
#[derive(Debug)]
pub struct KillSwitch {
    action: KillSwitchAction,
    threshold: Duration,
    /// Monotonic instant of the last successful heartbeat, or of construction
    /// if none has succeeded yet.
    ///
    /// A `Mutex<Instant>` rather than an atomic because `Instant` is not
    /// representable as one; the critical section is a single load or store and
    /// is taken at most once per heartbeat and once per create.
    last_success: Mutex<Instant>,
    /// Whether a heartbeat has ever succeeded.
    ///
    /// Before the first success there is nothing to have lost contact *with*.
    /// Firing during startup — while the scheduler is still coming up, or on a
    /// node brought online first — would refuse work for a condition that has
    /// never held.
    ever_succeeded: AtomicBool,
}

/// Process-wide kill switch.
///
/// The heartbeat loop feeds it and the create path reads it, and those live in
/// different subsystems with no shared owner, so a singleton is what connects
/// them without threading a handle through unrelated call sites.
static GLOBAL: std::sync::OnceLock<std::sync::Arc<KillSwitch>> = std::sync::OnceLock::new();

/// Returns the process-wide kill switch, initializing it from global config on
/// first use.
pub fn global_kill_switch() -> std::sync::Arc<KillSwitch> {
    std::sync::Arc::clone(GLOBAL.get_or_init(|| {
        let cfg = &crate::cfg::ConfigManager::global_config()
            .observability
            .scheduler_report
            .kill_switch;
        let action = match cfg.action.trim().to_ascii_lowercase().as_str() {
            "block_creates" => KillSwitchAction::BlockCreates,
            _ => KillSwitchAction::Disabled,
        };
        std::sync::Arc::new(KillSwitch::new(action, Duration::from_secs(cfg.after_secs)))
    }))
}

impl KillSwitch {
    pub fn new(action: KillSwitchAction, threshold: Duration) -> Self {
        Self {
            action,
            threshold,
            last_success: Mutex::new(Instant::now()),
            ever_succeeded: AtomicBool::new(false),
        }
    }

    /// A switch that never fires, for deployments with no scheduler.
    pub fn disabled() -> Self {
        Self::new(KillSwitchAction::Disabled, Duration::ZERO)
    }

    /// Records a successful heartbeat, resetting the window.
    pub fn record_success(&self) {
        self.ever_succeeded.store(true, Ordering::Release);
        if let Ok(mut last) = self.last_success.lock() {
            *last = Instant::now();
        }
    }

    /// Whether the node should currently refuse new sandboxes.
    pub fn blocks_creates(&self) -> bool {
        match self.action {
            KillSwitchAction::Disabled => false,
            KillSwitchAction::BlockCreates => self.tripped(),
        }
    }

    /// How long since the last successful heartbeat, if one has ever succeeded.
    pub fn since_last_success(&self) -> Option<Duration> {
        if !self.ever_succeeded.load(Ordering::Acquire) {
            return None;
        }
        self.last_success.lock().ok().map(|last| last.elapsed())
    }

    fn tripped(&self) -> bool {
        // No zero-is-off sentinel. A zero threshold used to disable the switch
        // regardless of the action, which turned the documented enable recipe —
        // name an action, leave the undocumented window alone — into a switch
        // that armed nothing. Config validation refuses that combination now,
        // and disabling is only `action = "disabled"`.
        match self.since_last_success() {
            Some(elapsed) => elapsed > self.threshold,
            // Never connected: see `ever_succeeded`.
            None => false,
        }
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_switch_never_blocks() {
        let switch = KillSwitch::disabled();
        switch.record_success();
        assert!(!switch.blocks_creates());
    }

    /// Before the first successful heartbeat there is nothing to have lost
    /// contact with. Firing here would refuse work on a node brought up before
    /// its scheduler.
    #[test]
    fn never_connected_node_does_not_trip() {
        let switch = KillSwitch::new(KillSwitchAction::BlockCreates, Duration::from_nanos(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!switch.blocks_creates());
        assert_eq!(switch.since_last_success(), None);
    }

    #[test]
    fn trips_after_the_threshold_elapses() {
        let switch = KillSwitch::new(KillSwitchAction::BlockCreates, Duration::from_millis(20));
        switch.record_success();
        assert!(!switch.blocks_creates(), "fresh heartbeat must not trip");

        std::thread::sleep(Duration::from_millis(40));
        assert!(switch.blocks_creates(), "threshold elapsed without contact");
    }

    /// Contact restored must clear the switch without operator action; a
    /// partition that heals should not leave the node permanently refusing.
    #[test]
    fn recovers_when_contact_is_restored() {
        let switch = KillSwitch::new(KillSwitchAction::BlockCreates, Duration::from_millis(20));
        switch.record_success();
        std::thread::sleep(Duration::from_millis(40));
        assert!(switch.blocks_creates());

        switch.record_success();
        assert!(
            !switch.blocks_creates(),
            "a successful heartbeat must clear it"
        );
    }

    /// A zero window is not a second way to disable the switch. Treating it as
    /// one is what made the documented enable recipe — set `action`, leave
    /// `after_secs` at its default — produce a switch that never fired.
    #[test]
    fn a_zero_window_is_not_an_off_switch() {
        let switch = KillSwitch::new(KillSwitchAction::BlockCreates, Duration::ZERO);
        switch.record_success();
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            switch.blocks_creates(),
            "an action with a zero window must fire, not silently disable"
        );
    }

    /// The kill switch's own default window, checked against the config
    /// default that feeds it. An operator who names an action and touches
    /// nothing else must get a switch that fires.
    #[test]
    fn the_configured_default_window_arms_the_switch() {
        let cfg = crate::cfg::ObservabilitySchedulerReportConfig::default().kill_switch;
        assert_eq!(cfg.action, "disabled");
        assert!(
            cfg.after_secs > 0,
            "a default window of zero would leave `action = \"block_creates\"` inert"
        );

        let switch = KillSwitch::new(
            KillSwitchAction::BlockCreates,
            Duration::from_secs(cfg.after_secs),
        );
        switch.record_success();
        assert!(!switch.blocks_creates(), "a fresh heartbeat must not trip");
    }
}
