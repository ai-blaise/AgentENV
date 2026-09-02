mod address_plan;
mod egress_proxy;
mod iptables_util;
mod manager;
mod policy;
mod resolver;
mod slot;

use std::path::Path;

use anyhow::Context;

pub(crate) use address_plan::NetworkAddressPlan;
pub(crate) use manager::{NetworkManager, NetworkSlotCapacity};
pub use policy::{
    BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
    ALL_INTERNET_TRAFFIC_CIDR,
};
pub(crate) use slot::Slot;

pub(crate) const MAX_SLOTS: usize = crate::cfg::network::NETWORK_MAX_SLOTS;
const NETNS_PREFIX: &str = "agentenv-ns-";
const HOST_VETH_PREFIX: &str = "veth-";

/// What startup should do with one file in the namespace directory.
#[derive(Debug, PartialEq, Eq)]
enum StaleNamespace {
    /// Not ours: another AgentENV process on this host owns it.
    Leave,
    /// Ours, and the slot index it belonged to.
    Reap(usize),
    /// Ours by prefix but not by naming: a namespace file from a build that
    /// did not stamp the owner. Removed, but no veth is attributed to it.
    RemoveOnly,
}

/// Decides the fate of a namespace file by its name.
///
/// Startup used to unmount and delete every file carrying the prefix, which is
/// wrong the moment two AgentENV processes share a host — the case the
/// process-scoped host-iptables flag already acknowledges. It also could not
/// reap the leftover veths of a crashed run, because nothing tied a `veth-N`
/// to a namespace file. Both fall out of putting the owner and the slot index
/// in the name.
fn classify_namespace_file(name: &str, owner: &str) -> StaleNamespace {
    let Some(rest) = name.strip_prefix(NETNS_PREFIX) else {
        return StaleNamespace::Leave;
    };
    let Some((file_owner, slot)) = rest.rsplit_once('-') else {
        return StaleNamespace::RemoveOnly;
    };
    match slot.parse::<usize>() {
        // Only this node's own leftovers are reaped. Another instance's
        // namespace may be live, and its veth certainly may be.
        Ok(idx) if file_owner == owner => StaleNamespace::Reap(idx),
        Ok(_) => StaleNamespace::Leave,
        Err(_) => StaleNamespace::RemoveOnly,
    }
}

/// The `service_instance_id` shipped in `config/default.toml`.
///
/// A constant, so every AgentENV started from the packaged configuration
/// derives the same owner from it.
const PACKAGED_INSTANCE_ID: &str = "service-instance-a";

/// The owner stamped in when nothing is configured at all.
const UNIDENTIFIED_OWNER: &str = "unidentified";

/// The identity stamped into this node's namespace file names.
struct NamespaceOwner {
    /// Sanitised so the name parses unambiguously: the slot index is
    /// everything after the last `-`, so the owner must not contain one.
    id: String,
    /// Whether the id tells this process apart from another AgentENV on the
    /// same host.
    ///
    /// False for the packaged default and for no configuration at all: both
    /// make two processes compute the same owner, so each classifies the
    /// other's namespace files as its own leftovers.
    distinguishing: bool,
}

impl NamespaceOwner {
    fn from_configured(configured: Option<&str>) -> Self {
        // Empty is unset, as `NodeIdentity::from_config` also reads it.
        let configured = configured.filter(|id| !id.is_empty());
        Self {
            id: sanitize_owner_id(configured.unwrap_or(UNIDENTIFIED_OWNER)),
            distinguishing: configured.is_some_and(|id| id != PACKAGED_INSTANCE_ID),
        }
    }
}

fn namespace_owner() -> &'static NamespaceOwner {
    static OWNER: std::sync::OnceLock<NamespaceOwner> = std::sync::OnceLock::new();
    OWNER.get_or_init(|| {
        let config = crate::cfg::ConfigManager::global_config();
        NamespaceOwner::from_configured(config.node_identity.service_instance_id.as_deref())
    })
}

pub(crate) fn namespace_owner_id() -> &'static str {
    &namespace_owner().id
}

fn sanitize_owner_id(id: &str) -> String {
    let sanitized: String = id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        UNIDENTIFIED_OWNER.to_string()
    } else {
        sanitized
    }
}

/// The namespace file name for one slot of this node.
pub(crate) fn namespace_file_name(idx: u32) -> String {
    format!("{NETNS_PREFIX}{}-{idx}", namespace_owner_id())
}

pub(crate) fn prepare_runtime(runtime_path: &Path) -> anyhow::Result<()> {
    // Before anything in this process can unshare or setns: the baseline
    // namespace is whatever the capturing thread is in, so capturing it late
    // risks capturing a sandbox's.
    slot::capture_host_ns_fd()?;

    let directory = runtime_path.join("netns");
    std::fs::create_dir_all(&directory)?;
    for idx in clean_stale_namespaces(&directory, namespace_owner())? {
        reap_stale_host_veth(idx);
    }
    Ok(())
}

/// Unmounts and deletes this node's leftover namespace files, returning the
/// slot indices whose host veth may be reaped along with them.
///
/// The owner is passed rather than read from [`namespace_owner`] so the
/// decision can be exercised for an id the packaged configuration produces and
/// for one an operator configured.
fn clean_stale_namespaces(directory: &Path, owner: &NamespaceOwner) -> anyhow::Result<Vec<usize>> {
    if !owner.distinguishing {
        tracing::warn!(
            owner = %owner.id,
            "[node_identity].service_instance_id is unset or still the packaged \
             default, so this node cannot tell its own leftover network \
             namespaces from another AgentENV's live ones; leaving stale host \
             veth interfaces in place"
        );
    }

    let mut reapable = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let fate = classify_namespace_file(&name.to_string_lossy(), &owner.id);
        if fate == StaleNamespace::Leave {
            continue;
        }
        let path = entry.path();
        // Remove every stacked bind mount before deleting the namespace file.
        loop {
            match nix::mount::umount2(&path, nix::mount::MntFlags::MNT_DETACH) {
                Ok(()) => continue,
                Err(nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) => break,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("unmount stale AENV network namespace {}", path.display())
                    })
                }
            }
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        // `veth-N` may be the live host end of another AgentENV's running
        // sandbox, and deleting it breaks that sandbox. Only an owner that
        // distinguishes this process from its peers makes the interface
        // attributable to a run of this one.
        if let StaleNamespace::Reap(idx) = fate {
            if owner.distinguishing {
                reapable.push(idx);
            }
        }
    }
    Ok(reapable)
}

/// Deletes the host veth left behind by a crashed run of this node.
///
/// Without this, `reserve_existing_host_veth_slots` marks the index allocated
/// and never frees it, so every crash permanently burns a slot index — the
/// cost that grows with how deep the warm pool is kept.
fn reap_stale_host_veth(idx: usize) {
    let name = format!("{HOST_VETH_PREFIX}{idx}");
    if !Path::new("/sys/class/net").join(&name).exists() {
        return;
    }

    let deleted = crate::privileges::run_with_scoped_capabilities(
        &[crate::privileges::CAP_NET_ADMIN],
        || {
            std::process::Command::new("ip")
                .args(["link", "del", &name])
                .output()
                .map_err(anyhow::Error::from)
        },
    );
    match deleted {
        Ok(output) if output.status.success() => {
            tracing::info!(interface = %name, "reaped a network interface left by a previous run")
        }
        Ok(output) => tracing::warn!(
            interface = %name,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "failed to reap a network interface left by a previous run"
        ),
        Err(error) => tracing::warn!(
            interface = %name,
            error = %error,
            "failed to reap a network interface left by a previous run"
        ),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
    #[error("Namespace operation failed: {0}")]
    NamespaceError(anyhow::Error),
    #[error("Host iptables operation failed: {0}")]
    HostIptablesError(anyhow::Error),
    #[error("Netlink error: {0}")]
    NetlinkError(#[from] rtnetlink::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("IP network error: {0}")]
    IpNetworkError(#[from] ipnetwork::IpNetworkError),
    #[error("Nix error: {0}")]
    NixError(#[from] nix::Error),
    #[error("Slot index out of range (max {max}): {idx}")]
    SlotOutOfRange { idx: u32, max: u32 },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Startup used to delete every namespace file carrying the prefix, live
    /// ones belonging to a second AgentENV process included.
    #[test]
    fn another_instances_namespace_file_is_left_alone() {
        assert_eq!(
            classify_namespace_file("agentenv-ns-node_b-7", "node_a"),
            StaleNamespace::Leave
        );
        assert_eq!(
            classify_namespace_file("agentenv-ns-node_a-7", "node_a"),
            StaleNamespace::Reap(7)
        );
        assert_eq!(
            classify_namespace_file("something-else", "node_a"),
            StaleNamespace::Leave
        );
        // A file from a build that stamped a bare UUID: still ours to remove,
        // but no slot index can be attributed to it.
        assert_eq!(
            classify_namespace_file("agentenv-ns-0198f0f0-1c2d-7c8f-9a3b-2f1d4c5e6a7b", "node_a"),
            StaleNamespace::RemoveOnly
        );
    }

    /// The slot index is everything after the last `-`, so the owner must not
    /// contain one.
    #[test]
    fn the_namespace_file_name_parses_back_to_its_slot() {
        assert_eq!(
            sanitize_owner_id("service-instance-a"),
            "service_instance_a"
        );
        assert_eq!(sanitize_owner_id(""), "unidentified");

        let name = format!(
            "{NETNS_PREFIX}{}-{}",
            sanitize_owner_id("service-instance-a"),
            41
        );
        assert_eq!(
            classify_namespace_file(&name, "service_instance_a"),
            StaleNamespace::Reap(41)
        );
    }

    /// The baseline namespace is captured from whichever thread asks first.
    /// Left lazy, that is a thread deep in slot creation — and if it had
    /// already entered a sandbox namespace, every later host-side veth would be
    /// moved into that sandbox instead of the host. Startup captures it while
    /// the process still has only its own namespace.
    ///
    /// Counted rather than tested for presence: another test in the same
    /// process fills the same cell lazily, so "the fd exists" is true whether
    /// or not startup asked for it.
    #[test]
    fn prepare_runtime_captures_the_baseline_namespace() {
        let temp = tempfile::tempdir().expect("temp runtime dir");
        let before = slot::startup_captures();
        prepare_runtime(temp.path()).expect("prepare the network runtime");
        assert!(
            slot::startup_captures() > before,
            "startup should have captured the baseline network namespace"
        );
    }

    /// A directory entry naming a stale namespace.
    ///
    /// A dangling symlink rather than a file: the cleanup unmounts every entry
    /// it is about to delete, and an unprivileged `umount2` refuses a real file
    /// with `EPERM` while an unresolvable path reports `ENOENT`, which is the
    /// "nothing left to detach" the loop is written for.
    fn stale_namespace_entry(directory: &Path, owner: &NamespaceOwner, idx: usize) -> PathBuf {
        let path = directory.join(format!("{NETNS_PREFIX}{}-{idx}", owner.id));
        std::os::unix::fs::symlink("/proc/self/ns/net-does-not-exist", &path)
            .expect("write a stale namespace entry");
        path
    }

    /// `config/default.toml` ships `service_instance_id` as a constant, so two
    /// AgentENV processes on one host compute the same owner and each reads the
    /// other's namespace files as its own leftovers. Deleting the file is what
    /// startup always did; deleting `veth-N` with it would tear the live host
    /// end out of the peer's running sandbox.
    #[test]
    fn the_packaged_owner_id_reaps_no_host_veth() {
        let temp = tempfile::tempdir().expect("temp netns dir");
        let packaged = NamespaceOwner::from_configured(Some(PACKAGED_INSTANCE_ID));
        let path = stale_namespace_entry(temp.path(), &packaged, 7);

        let reapable = clean_stale_namespaces(temp.path(), &packaged).expect("clean the directory");

        assert!(
            reapable.is_empty(),
            "an owner the whole host shares cannot attribute veth-7 to this process"
        );
        assert!(
            !path.exists(),
            "the namespace file itself is still removed, as it always was"
        );
    }

    /// An unset id is the same collision: every process sanitises it to the
    /// same word.
    #[test]
    fn an_unset_owner_id_reaps_no_host_veth() {
        let temp = tempfile::tempdir().expect("temp netns dir");
        let unset = NamespaceOwner::from_configured(None);
        stale_namespace_entry(temp.path(), &unset, 9);

        assert!(clean_stale_namespaces(temp.path(), &unset)
            .expect("clean the directory")
            .is_empty());
    }

    /// An operator-set id names this process and no other, which is what makes
    /// a leftover `veth-N` attributable to a crashed run of it.
    #[test]
    fn a_configured_owner_id_reaps_its_own_host_veth() {
        let temp = tempfile::tempdir().expect("temp netns dir");
        let configured = NamespaceOwner::from_configured(Some("node-a-7f3c"));
        stale_namespace_entry(temp.path(), &configured, 11);

        assert_eq!(
            clean_stale_namespaces(temp.path(), &configured).expect("clean the directory"),
            vec![11]
        );
    }
}
