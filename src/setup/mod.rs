mod deps;
mod kvm;
mod network_capacity;
pub mod overlaybd;
mod packages;
mod ublk;

use anyhow::{Context, Result};
use nix::sys::resource::{getrlimit, setrlimit, Resource};
use nix::unistd::{Gid, Group, User};
use std::fs;
use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::sandbox::SandboxBackendKind;

/// Provision runtime dependencies into the configured deps path.
pub async fn ensure_dependencies(config: &AppConfig) -> Result<()> {
    let deps_path = config.deps_path.clone();
    info!(path = %deps_path.display(), "ensuring dependencies");
    deps::ensure(config, &deps_path).await
}

/// Checks that the host can run the configured sandbox backend.
///
/// In `firecracker` mode every failure is fatal. A node that cannot reach
/// `/dev/kvm` or `ublk_drv` cannot run a single sandbox, and starting anyway
/// would advertise capacity it does not have to a scheduler that would place
/// on it. In `mock` mode the same checks run and are reported, but nothing in
/// the node depends on them, so they only warn: running where they fail is the
/// point of that mode, and hiding the result would make a mock node look like
/// a virtualizing one in the logs.
pub fn preflight_hypervisor(config: &AppConfig) -> Result<()> {
    match config.machine.backend {
        SandboxBackendKind::Firecracker => {
            packages::check_runtime()?;
            info!(virtualization_mode = %config.virtualization_mode, "checking virtualization availability");
            kvm::check(config.virtualization_mode)?;
            info!("checking ublk module and permissions");
            ublk::check()?;
            Ok(())
        }
        SandboxBackendKind::Mock => {
            warn!(
                "sandbox backend is \"mock\": this node runs NO guests and isolates NOTHING; \
                 it exists to run the control plane where the host cannot virtualize"
            );
            let checks = [
                ("runtime packages", packages::check_runtime()),
                ("kvm", kvm::check(config.virtualization_mode)),
                ("ublk", ublk::check()),
            ];
            for (check, result) in checks {
                if let Err(error) = result {
                    warn!(check, error = %error, "host check failed; advisory under the mock backend");
                }
            }
            Ok(())
        }
    }
}

/// Provision host runtime packages and downloaded runtime assets without
/// requiring VM device access. Intended for `--setup-only` and image builds.
pub async fn ensure_provisioning(config: &AppConfig) -> Result<()> {
    packages::ensure()?;
    ensure_dependencies(config).await?;
    deps::write_generated_overlaybd_global_configs(config, None)
}

/// Provision machine-wide prerequisites for the configured runtime account.
/// This mode is intentionally separate from `--setup-only`, which is also used
/// while assembling release artifacts on unprivileged builders.
pub fn ensure_host(config: &AppConfig, runtime_user: &str, runtime_group: &str) -> Result<()> {
    if !nix::unistd::Uid::effective().is_root() {
        anyhow::bail!("host provisioning requires root; rerun with sudo");
    }
    let runtime_gid = validate_runtime_account(runtime_user, runtime_group)?;

    if kvm::add_user_to_group(runtime_user, runtime_group)? {
        warn!(
            user = runtime_user,
            group = runtime_group,
            "runtime user added to the configured runtime group; restart its session before starting AENV manually"
        );
    }
    if kvm::add_user_to_group(runtime_user, "kvm")? {
        warn!(
            user = runtime_user,
            "runtime user added to the kvm group; restart its session before starting AENV manually"
        );
    }
    ublk::provision(runtime_group)?;
    overlaybd::install_system_default_config(&config.deps_path, runtime_gid)?;
    network_capacity::install_persistent_config().context("install /etc/sysctl.d/99-aenv.conf")?;
    fs::write("/proc/sys/net/ipv4/ip_forward", "1\n").context("enable host IPv4 forwarding")?;
    network_capacity::check_and_adjust();
    Ok(())
}

fn validate_runtime_account(runtime_user: &str, runtime_group: &str) -> Result<Gid> {
    if !is_valid_runtime_account_name(runtime_user) || !is_valid_runtime_account_name(runtime_group)
    {
        anyhow::bail!(
            "runtime user {runtime_user:?} or group {runtime_group:?} is invalid (must match [a-z_][a-z0-9_-]*)"
        );
    }

    let user = User::from_name(runtime_user)
        .with_context(|| format!("look up runtime user {runtime_user}"))?
        .with_context(|| format!("runtime user does not exist: {runtime_user}"))?;
    if user.uid.is_root() {
        anyhow::bail!("runtime user must be non-root");
    }

    let group = Group::from_name(runtime_group)
        .with_context(|| format!("look up runtime group {runtime_group}"))?
        .with_context(|| format!("runtime group does not exist: {runtime_group}"))?;
    if group.gid.as_raw() == 0 {
        anyhow::bail!("runtime group must be non-root");
    }
    Ok(group.gid)
}

fn is_valid_runtime_account_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

/// Run all environment setup steps. Fails fast if any prerequisite is unmet.
///
/// Steps:
/// 1. Verify the configured KVM/PVM host mode and `/dev/kvm` access
/// 2. Ensure ublk kernel module is loaded and permissions are set
/// 3. Download dependencies (firecracker, kernel, tools drive, overlaybd) if missing
pub async fn ensure_environment(
    config: &AppConfig,
    p2p_facade_address: Option<&str>,
) -> Result<()> {
    info!("running environment setup");

    fs::create_dir_all(&config.runtime_path).with_context(|| {
        format!(
            "create AENV runtime directory {}",
            config.runtime_path.display()
        )
    })?;
    crate::sandbox::prepare_network_runtime(&config.runtime_path)
        .context("prepare AENV network namespace runtime directory")?;

    // 1-3. What the configured backend needs from this host.
    preflight_hypervisor(config)?;

    // 4. Download dependencies and generate overlaybd runtime configs. The
    // generated configs are read at orchestrator construction whatever the
    // backend; the downloads are only for guests, which mock mode has none of.
    if config.machine.backend == SandboxBackendKind::Firecracker {
        ensure_dependencies(config).await?;
    }
    deps::write_generated_overlaybd_global_configs(config, p2p_facade_address)?;

    // 5. set nofile limit to a relative large value
    let (soft_limit, hard_limit) = getrlimit(Resource::RLIMIT_NOFILE).context("get rlimit")?;
    if soft_limit < 65536 {
        setrlimit(Resource::RLIMIT_NOFILE, 65536.min(hard_limit), hard_limit)
            .context("set rlimit")?;
    }

    // 6. Host networking is provisioned separately and only validated here.
    let ip_forward = fs::read_to_string("/proc/sys/net/ipv4/ip_forward")
        .context("read host IPv4 forwarding setting")?;
    if ip_forward.trim() != "1" {
        match config.machine.backend {
            SandboxBackendKind::Firecracker => {
                anyhow::bail!(
                    "host IPv4 forwarding is disabled; run `server --setup-host` as root"
                );
            }
            // No guest traffic to forward.
            SandboxBackendKind::Mock => {
                warn!("host IPv4 forwarding is disabled; advisory in mock mode")
            }
        }
    }

    network_capacity::check();

    info!("environment setup complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_valid_runtime_account_name;

    #[test]
    fn runtime_account_names_match_installer_validation() {
        for name in ["aenv", "_aenv", "aenv-1", "aenv_service"] {
            assert!(is_valid_runtime_account_name(name), "{name}");
        }
        for name in ["", "Aenv", "a.env", "-aenv", "aenv service"] {
            assert!(!is_valid_runtime_account_name(name), "{name}");
        }
    }
}

#[cfg(test)]
mod preflight_tests {
    use super::preflight_hypervisor;
    use crate::cfg::AppConfig;
    use crate::sandbox::SandboxBackendKind;
    use std::path::Path;

    /// The property the mock backend must never erode: a node asked for
    /// Firecracker on a host that cannot provide it stops. Only meaningful
    /// where the check can fail, so it stands down on a host with `/dev/kvm`.
    #[test]
    fn firecracker_mode_refuses_a_host_it_cannot_run_on() {
        if Path::new("/dev/kvm").exists() {
            return;
        }
        let mut config = AppConfig::default();
        config.machine.backend = SandboxBackendKind::Firecracker;
        assert!(
            preflight_hypervisor(&config).is_err(),
            "a firecracker node without /dev/kvm must refuse to start"
        );
    }

    #[test]
    fn mock_mode_starts_on_a_host_without_kvm() {
        let mut config = AppConfig::default();
        config.machine.backend = SandboxBackendKind::Mock;
        preflight_hypervisor(&config).expect("mock mode must not depend on the hypervisor");
    }
}
