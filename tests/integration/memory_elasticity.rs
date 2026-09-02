//! Elastic-memory behaviour that only a real microVM can answer.
//!
//! Everything here needs `/dev/kvm`, and the free-page-hinting cases
//! additionally need a Firecracker build that carries the `/balloon/hinting/*`
//! patch. They are `#[ignore]`d so a host without either still runs the suite,
//! and so the outstanding hardware-bound checks stay countable.

use std::path::Path;

use agentenv::cfg::BalloonConfig;
use agentenv::sandbox::{
    FirecrackerSandbox, MemoryControlCapability, SandboxBackend, SandboxExecutor,
};
use anyhow::{bail, Context, Result};

use crate::common;

/// Enough to make the difference between "the guest freed it" and "the capture
/// wrote it out anyway" unmistakable in the layer size.
const BALLAST_MIB: u32 = 1024;
const PATTERN_MIB: u32 = 64;
const PATTERN_PATH: &str = "/dev/shm/hinting-pattern";
const PATTERN_BYTE: &str = "\\253";

fn balloon(free_page_hinting: bool, on_capture: bool) -> BalloonConfig {
    BalloonConfig {
        stats_polling_interval_s: 1,
        free_page_hinting,
        free_page_hinting_on_capture: on_capture,
        free_page_hinting_timeout_ms: 5000,
    }
}

async fn run(sandbox: &FirecrackerSandbox, script: &str) -> Result<String> {
    let output = sandbox
        .executor()?
        .run_command("sh", &["-c", script])
        .await?;
    if output.exit_code != 0 {
        bail!(
            "guest command failed ({}): {}",
            output.exit_code,
            output.stderr
        );
    }
    Ok(output.stdout)
}

/// Dirty a large region and hand it straight back, which is the shape hinting
/// is supposed to exploit: pages the guest no longer wants but that the dirty
/// bitmap still reports.
async fn allocate_then_free(sandbox: &FirecrackerSandbox) -> Result<()> {
    run(
        sandbox,
        &format!(
            "dd if=/dev/zero of=/dev/shm/ballast bs=1M count={BALLAST_MIB} 2>/dev/null; \
             rm -f /dev/shm/ballast; sync"
        ),
    )
    .await
    .map(|_| ())
}

async fn memory_layer_bytes(snapshot_dir: &Path) -> Result<u64> {
    let layer = snapshot_dir.join("mem_overlaybd/overlaybd.commit");
    Ok(tokio::fs::metadata(&layer)
        .await
        .with_context(|| format!("stat memory layer {}", layer.display()))?
        .len())
}

async fn capture_layer_bytes(hinting: BalloonConfig) -> Result<u64> {
    let mut config = common::default_sandbox_config()?;
    config.common.balloon = hinting;
    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;
    allocate_then_free(&sandbox).await?;

    let snapshot_dir = tempfile::tempdir()?;
    let (_snapshot, _manifest) = sandbox.pause_to_dir(snapshot_dir.path()).await?;
    let bytes = memory_layer_bytes(snapshot_dir.path()).await?;
    sandbox.stop().await?;
    Ok(bytes)
}

/// dep-0. The whole of the free-page-hinting case rests on a property no part
/// of the Firecracker API contract states: that a hinting run removes pages
/// from `/vm/dirty-memory-ranges`. If the two captures come out the same size,
/// hinting buys nothing on this binary and `free_page_hinting_on_capture` must
/// stay off.
#[tokio::test]
#[ignore = "requires /dev/kvm"]
async fn hinting_shrinks_the_memory_layer_of_a_freed_allocation() -> Result<()> {
    common::setup().await;

    let without = capture_layer_bytes(balloon(false, false)).await?;
    let with = capture_layer_bytes(balloon(true, true)).await?;

    println!("memory layer bytes: without hinting {without}, with hinting {with}");
    assert!(
        with < without,
        "hinting did not remove freed pages from the capture: {with} >= {without}"
    );
    Ok(())
}

/// dep-0's correctness half, and the difference between an optimisation and
/// silent guest-memory corruption.
///
/// The hinting run happens while the guest is executing, so the guest can
/// re-allocate and write a hinted page before the pause lands. That is only
/// safe if the patched Firecracker treats a hint as a one-shot dirty-bit clear
/// that a subsequent write re-sets. If it is instead a skip list for the whole
/// capture, the pattern written below is dropped from the layer and reads back
/// as zeros.
#[tokio::test]
#[ignore = "requires /dev/kvm"]
async fn a_page_written_after_the_hint_survives_the_capture() -> Result<()> {
    common::setup().await;

    let mut config = common::default_sandbox_config()?;
    config.common.balloon = balloon(true, true);
    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;
    allocate_then_free(&sandbox).await?;

    // Rewrite the pattern continuously across the capture, so writes land in
    // the window between the hint and the pause rather than only before it.
    run(
        &sandbox,
        &format!(
            "nohup sh -c 'while true; do \
               head -c {}m /dev/zero | tr \"\\\\000\" \"{PATTERN_BYTE}\" > {PATTERN_PATH}.tmp; \
               mv {PATTERN_PATH}.tmp {PATTERN_PATH}; \
             done' >/dev/null 2>&1 &",
            PATTERN_MIB
        ),
    )
    .await?;

    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    let distinct = run(
        &resumed,
        &format!("tr -d \"{PATTERN_BYTE}\" < {PATTERN_PATH} | wc -c"),
    )
    .await?;
    resumed.stop().await?;

    assert_eq!(
        distinct.trim(),
        "0",
        "bytes re-written after the hint did not survive the capture"
    );
    Ok(())
}

/// The probe must answer for a VM restored from a snapshot, not just a fresh
/// boot, and a snapshot captured before the balloon existed must still resume.
#[tokio::test]
#[ignore = "requires /dev/kvm"]
async fn the_capability_probe_answers_on_both_start_paths() -> Result<()> {
    common::setup().await;

    let mut config = common::default_sandbox_config()?;
    config.common.balloon = balloon(false, false);
    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;

    let fresh = sandbox.runtime_info().mem_control;
    assert!(
        fresh.balloon,
        "a freshly booted VM reports its configured balloon device"
    );
    assert!(fresh.balloon_stats, "statistics were armed pre-boot");
    assert!(!fresh.free_page_hinting);

    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    assert_eq!(
        resumed.runtime_info().mem_control,
        fresh,
        "the restored VM must report the devices it was captured with"
    );
    resumed.stop().await?;
    Ok(())
}

/// A VM whose balloon was never given statistics opts out permanently, and
/// nothing about that may fail a start.
#[tokio::test]
#[ignore = "requires /dev/kvm"]
async fn a_statless_balloon_opts_out_without_failing_the_boot() -> Result<()> {
    common::setup().await;

    let mut config = common::default_sandbox_config()?;
    config.common.balloon = BalloonConfig {
        stats_polling_interval_s: 0,
        ..balloon(false, false)
    };
    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;

    let capability = sandbox.runtime_info().mem_control;
    assert_eq!(
        capability,
        MemoryControlCapability {
            balloon: true,
            balloon_stats: false,
            free_page_hinting: false,
            hotplug: false,
        }
    );
    assert!(
        sandbox.memory_telemetry().await?.is_none(),
        "a statless balloon must report no sample rather than an error"
    );
    sandbox.stop().await?;
    Ok(())
}

/// The design asserts statistics polling costs "~0"; nothing measured it. This
/// reports the idle guest CPU time accumulated over a fixed window at each
/// interval so a default can be picked from data rather than assumption.
#[tokio::test]
#[ignore = "requires /dev/kvm"]
async fn statistics_polling_cost_at_each_interval() -> Result<()> {
    common::setup().await;

    for interval_s in [0u32, 1, 5, 15] {
        let mut config = common::default_sandbox_config()?;
        config.common.balloon = BalloonConfig {
            stats_polling_interval_s: interval_s,
            ..balloon(false, false)
        };
        let mut sandbox = FirecrackerSandbox::new(config)?;
        sandbox.start().await?;

        let before = run(&sandbox, "cut -d' ' -f2,3,4 /proc/stat | head -1").await?;
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        let after = run(&sandbox, "cut -d' ' -f2,3,4 /proc/stat | head -1").await?;

        println!(
            "stats_polling_interval_s={interval_s}: guest busy jiffies before [{}] after [{}]",
            before.trim(),
            after.trim()
        );
        sandbox.stop().await?;
    }
    Ok(())
}
