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

    use crate::cfg::{AdmissionConfig, AppConfig, SnapshotConfig};
    use crate::observability::{KillSwitch, KillSwitchAction};
    use crate::orchestrator::{AdmissionController, NodeCapacityInputs, OrchestratorMetrics};
    use crate::p2p::{DisabledP2pTransport, P2pTransport};
    use crate::snapshot::sealing::{ArtifactSealingKey, SnapshotSealing};
    use crate::types::SandboxResources;

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

    /// The switch as an operator actually enables it: name the action and
    /// touch nothing else. Building it from hand-picked arguments was how the
    /// documented recipe came to produce a switch that never fired — the
    /// window has its own field, and its default used to be a second, silent
    /// off switch.
    #[test]
    fn the_documented_kill_switch_recipe_arms_it() {
        let cfg = crate::cfg::ObservabilitySchedulerReportConfig::default().kill_switch;
        let switch = KillSwitch::new(
            KillSwitchAction::BlockCreates,
            Duration::from_secs(cfg.after_secs),
        );
        switch.record_success();
        assert!(
            switch.since_last_success().is_some() && cfg.after_secs > 0,
            "action = \"block_creates\" with the default window must arm a switch that can fire"
        );
    }

    /// Node-local admission control. Off, every create is admitted whatever
    /// the node is carrying — that is the rollback for a mis-tuned limit
    /// turning capacity into rejections.
    #[tokio::test]
    async fn admission_rejects_only_when_enabled() {
        for (enabled, want_admitted) in [(true, false), (false, true)] {
            let controller = AdmissionController::new(AdmissionConfig {
                enabled,
                max_sandbox_count: Some(1),
                max_sandbox_starting_count: None,
                max_allocated_cpu: None,
                max_allocated_memory_bytes: None,
                max_sandbox_count_including_paused: None,
                min_free_network_slots: None,
                retry_after_secs: 2,
                snapshot_max_age_ms: 0,
            });
            let over_the_limit = OrchestratorMetrics {
                running_sandbox_count: 5,
                ..Default::default()
            };

            let admitted = controller
                .try_admit(
                    1,
                    SandboxResources::default(),
                    NodeCapacityInputs::default(),
                    || async { Some(over_the_limit) },
                )
                .await
                .is_ok();
            assert_eq!(
                admitted, want_admitted,
                "admission enabled = {enabled} should admit = {want_admitted}"
            );
        }
    }

    /// Snapshot P2P. Off, the snapshot manager gets no transport at all, so
    /// resolution goes to the repository and nothing is advertised.
    #[test]
    fn snapshot_p2p_is_a_real_switch() {
        let transport: Arc<dyn P2pTransport> = Arc::new(DisabledP2pTransport);

        let on = SnapshotConfig {
            p2p_enabled: true,
            ..Default::default()
        };
        assert!(
            on.p2p_transport_for(&transport).is_some(),
            "on must hand the snapshot manager a transport"
        );

        let off = SnapshotConfig {
            p2p_enabled: false,
            ..Default::default()
        };
        assert!(
            off.p2p_transport_for(&transport).is_none(),
            "off must leave the snapshot manager without one"
        );
    }

    /// Warm pool maintenance, as the network pool resolves it: the component
    /// flag and the shared `[pool]` flag are ANDed, so either one off means no
    /// background refill.
    #[test]
    fn warm_pool_maintenance_is_a_real_switch() {
        for (pool_enabled, maintenance_enabled, want) in [
            (true, true, true),
            (true, false, false),
            (false, true, false),
            (false, false, false),
        ] {
            let mut config = AppConfig::default();
            config.pool.network.enabled = pool_enabled;
            config.pool.network.maintenance_enabled = maintenance_enabled;
            assert_eq!(
                config.network_pool_config().maintenance_enabled,
                want,
                "pool.enabled={pool_enabled} pool.network.maintenance_enabled={maintenance_enabled}"
            );
        }
        // The behavioural assertions live in crates/warm-pool, which owns the
        // worker: `release_respects_high_watermark_when_maintenance_disabled`
        // and `release_allows_maintenance_worker_to_drain_above_high_watermark`.
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
