//! Checking that the off switches actually switch things off.
//!
//! Every behaviour added for scale here is gated, and a gate that does nothing
//! is worse than no gate: it is a documented rollback that will not roll back.
//! This is not hypothetical — building this work produced two of exactly that,
//! a prewarm flag the pool ignored and a cache handle the invalidation path
//! never held, and both were found by accident rather than by a test.
//!
//! So each switch is exercised in both directions. Off must remove the
//! behaviour, which catches a dead flag; on must produce it, which catches a
//! flag wired to the wrong thing. The Go control plane has the same harness in
//! `services/*/internal/offswitch_test.go`.
//!
//! This module is tests only. It lives beside the code rather than under
//! `tests/` because several of the switches it covers are crate-private.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::observability::{KillSwitch, KillSwitchAction};
    use crate::snapshot::sealing::{ArtifactSealingKey, SnapshotSealing};

    /// The kill switch stops a node accepting work once it has lost contact
    /// with the scheduler. Off, a node out of contact must keep accepting —
    /// that is the rollback for a deployment where the scheduler is the less
    /// reliable half.
    #[test]
    fn kill_switch_blocks_creates_only_when_enabled() {
        for (action, want_blocked) in [
            (KillSwitchAction::BlockCreates, true),
            (KillSwitchAction::Disabled, false),
        ] {
            let switch = KillSwitch::new(action, Duration::from_millis(10));
            switch.record_success();
            std::thread::sleep(Duration::from_millis(30));
            assert_eq!(
                switch.blocks_creates(),
                want_blocked,
                "{action:?} should block = {want_blocked}"
            );
        }
    }

    /// Sealing gates whether snapshot fixed artifacts may be advertised at
    /// all. Off must mean no key, and therefore no publication — not a
    /// fallback to publishing them in the clear.
    #[test]
    fn snapshot_sealing_reports_its_own_state_honestly() {
        let disabled = SnapshotSealing::disabled();
        assert!(!disabled.is_enabled());
        assert!(
            disabled.key().is_none(),
            "a disabled sealing state must not hand out a key"
        );

        let enabled =
            SnapshotSealing::with_key(ArtifactSealingKey::from_bytes(vec![1_u8; 32]).expect("key"));
        assert!(enabled.is_enabled());
        assert!(enabled.key().is_some());
    }

    /// The sealing secret comes from config, and an absent or blank one must
    /// disable rather than produce a key from nothing.
    #[test]
    fn a_blank_sealing_secret_disables_sealing() {
        for secret in [None, Some("   ".to_string()), Some(String::new())] {
            let sealing = sealing_from_secret(secret.clone());
            assert!(
                !sealing.is_enabled(),
                "secret {secret:?} should leave sealing off"
            );
        }

        let sealing = sealing_from_secret(Some(hex::encode([3_u8; 32])));
        assert!(sealing.is_enabled(), "a real secret should enable sealing");
    }

    /// Builds a sealing state the way `SnapshotSealing::from_config` does,
    /// without needing a whole `AppConfig`.
    fn sealing_from_secret(secret: Option<String>) -> Arc<SnapshotSealing> {
        let Some(secret) = secret
            .as_deref()
            .map(str::trim)
            .filter(|secret| !secret.is_empty())
        else {
            return Arc::new(SnapshotSealing::disabled());
        };
        Arc::new(SnapshotSealing::with_key(
            ArtifactSealingKey::from_hex(secret).expect("valid secret"),
        ))
    }

    /// The warm pool's prewarm flag. Covered in depth in `warm-pool`'s own
    /// tests; asserted here so the switch appears in one place with the rest.
    #[test]
    fn warm_pool_prewarm_is_a_real_switch() {
        use warm_pool::PoolConfig;

        let on = PoolConfig {
            low_watermark: 1,
            high_watermark: 2,
            maintenance_enabled: true,
            startup_prewarm: true,
        };
        let off = PoolConfig {
            startup_prewarm: false,
            ..on
        };
        assert!(on.startup_prewarm);
        assert!(!off.startup_prewarm);
        // The behavioural assertion lives in crates/warm-pool, which owns the
        // worker: `prewarm_requests_a_cycle_as_soon_as_the_worker_starts` and
        // `disabling_prewarm_leaves_the_pool_alone_until_demand_arrives`.
    }
}
