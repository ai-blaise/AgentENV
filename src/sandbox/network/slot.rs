use std::collections::HashMap;
use std::fs::{self, File};
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use futures::{stream::TryStreamExt, StreamExt};
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{
    InfoData, InfoKind, InfoVeth, LinkAttribute, LinkFlags, LinkInfo,
};
use netlink_packet_route::{AddressFamily, RouteNetlinkMessage};
use nix::libc;
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use rtnetlink::packet_core::{
    NetlinkMessage, NetlinkPayload, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST,
};
use rtnetlink::{new_connection, Handle};
use tracing::{debug, info, warn};

use crate::observability::prometheus::MetricGuard;

use super::egress_proxy::EgressProxy;
use super::iptables_util::{
    apply_iptables_commands, group_commands_by_table, IptablesRestoreCommand, OpenFailurePolicy,
};
use super::policy::{
    namespace_egress_chain_commands, set_namespace_egress_policy, SandboxNetworkPolicy,
};
use super::{NetworkAddressPlan, NetworkError, HOST_VETH_PREFIX, MAX_SLOTS};

/// Process-wide baseline network namespace fd.
///
/// Captured once from the calling thread. Namespace membership is per-thread
/// and `/proc/thread-self/ns/net` names the *caller's* namespace, so the
/// capture has to happen before any thread in the process can `unshare` or
/// `setns`: a first capture taken on a thread already inside a sandbox
/// namespace would send every later host-side veth into that sandbox, silently
/// and for the life of the process. [`capture_host_ns_fd`] does that at
/// startup; the lazy path below only covers callers that skipped it.
static HOST_NS_FD: OnceLock<OwnedFd> = OnceLock::new();

/// How many times the startup capture has run.
///
/// Whether `HOST_NS_FD` is filled says nothing about who filled it: the lazy
/// path in [`host_ns_fd`] fills the same cell from whatever thread asks first,
/// which is precisely the case the startup capture exists to pre-empt. Counted
/// so that call can be observed on its own.
static STARTUP_CAPTURES: AtomicUsize = AtomicUsize::new(0);

/// Captures the baseline network namespace, if it is not captured already.
///
/// Called from `prepare_runtime` during server startup, on the startup thread,
/// before the network manager or any slot exists.
pub(crate) fn capture_host_ns_fd() -> Result<()> {
    if HOST_NS_FD.get().is_none() {
        let file = File::open(HOST_NS_PATH)
            .with_context(|| format!("open the baseline network namespace at {HOST_NS_PATH}"))?;
        let _ = HOST_NS_FD.set(OwnedFd::from(file));
    }
    STARTUP_CAPTURES.fetch_add(1, Ordering::Release);
    Ok(())
}

/// How many times [`capture_host_ns_fd`] has run in this process.
#[cfg(test)]
pub(super) fn startup_captures() -> usize {
    STARTUP_CAPTURES.load(Ordering::Acquire)
}

const HOST_NS_PATH: &str = "/proc/thread-self/ns/net";

/// Latency of one stage of slot setup, so the per-slot cost can be attributed
/// rather than inferred from the total.
const SLOT_STAGE_DURATION: &str = "agentenv_network_slot_stage_duration_seconds";

const TUN_DEVICE_PATH: &str = "/dev/net/tun";
const VPEER_NAME: &str = "vpeer";
const TAP_NAME: &str = "tap0";

/// `_IOW(kind, number, size)`, the encoding the tun ioctls are declared with.
const fn iow(kind: u8, number: u8, size: usize) -> libc::c_ulong {
    const WRITE_DIRECTION: libc::c_ulong = 1 << 30;
    WRITE_DIRECTION
        | ((size as libc::c_ulong) << 16)
        | ((kind as libc::c_ulong) << 8)
        | number as libc::c_ulong
}

const TUNSETIFF: libc::c_ulong = iow(b'T', 202, std::mem::size_of::<libc::c_int>());
const TUNSETPERSIST: libc::c_ulong = iow(b'T', 203, std::mem::size_of::<libc::c_int>());

/// The `ifreq` TUNSETIFF reads.
///
/// Written out here, and pinned by a test, because the kernel reads a fixed
/// 40-byte structure: a layout that disagreed would be read as different flags
/// rather than rejected.
#[repr(C)]
#[derive(Clone, Copy)]
struct TunSetIffRequest {
    name: [libc::c_char; libc::IFNAMSIZ],
    flags: libc::c_short,
    padding: [u8; 22],
}

/// Builds the TUNSETIFF request for a tap device.
///
/// `IFF_VNET_HDR` is deliberately absent: iproute2 does not set it either, and
/// Firecracker sets it itself when it opens the tap by name. Setting it here
/// would silently change guest offload behavior.
fn tun_set_iff_request(name: &str) -> Result<TunSetIffRequest> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() >= libc::IFNAMSIZ {
        return Err(anyhow!("interface name {name:?} does not fit IFNAMSIZ"));
    }

    let mut request = TunSetIffRequest {
        name: [0; libc::IFNAMSIZ],
        flags: (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short,
        padding: [0; 22],
    };
    for (slot, byte) in request.name.iter_mut().zip(bytes) {
        *slot = *byte as libc::c_char;
    }
    Ok(request)
}

/// Interface index by name, in the caller's network namespace.
fn interface_index(name: &str) -> Result<u32> {
    let name = std::ffi::CString::new(name).context("interface name contains a NUL")?;
    // SAFETY: `name` is a NUL-terminated C string that outlives the call.
    let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
    if index == 0 {
        return Err(std::io::Error::last_os_error()).context("look up the interface index");
    }
    Ok(index)
}

/// Whether the in-process tap path should defer to the shell-out.
///
/// Refusal is the expected case: capabilities are per-thread and this thread's
/// are whatever the caller left it holding. A missing `/dev/net/tun` counts
/// too — the shell-out is no more likely to succeed, but it is the path this
/// replaced and it reports the failure the way the node already knows.
fn is_permission_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
        )
    })
}

const ARP_RETRANS_TIME_MS: &str = "100";
const NEIGH_SYSCTL_RETRIES: usize = 5;
const NEIGH_SYSCTL_RETRY_DELAY_MS: u64 = 20;

/// Get a borrowed reference to the host network namespace fd.
pub(super) fn host_ns_fd() -> BorrowedFd<'static> {
    HOST_NS_FD
        .get_or_init(|| {
            warn!(
                "capturing the baseline network namespace lazily; it should have been \
                 captured at startup"
            );
            let file = File::open(HOST_NS_PATH)
                .expect("Failed to open host network namespace from /proc/thread-self/ns/net");
            OwnedFd::from(file)
        })
        .as_fd()
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub idx: u32,
    pub namespace_id: String,
    pub host_interaction_ip: Ipv4Addr,
    pub veth_host_ip: Ipv4Addr, // The IP on the Host side interface
    pub veth_vm_ip: Ipv4Addr,   // The IP on the VM/NS side interface (vpeer)
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
    egress_proxy: Arc<EgressProxy>,
    cleanup_armed: bool,
    /// Whether this namespace's user egress chain currently contains rules.
    /// Warm-pool reuse preserves the namespace, so the next tenant may need to
    /// clear rules left by the previous tenant.
    user_egress_rules_present: bool,
}

struct NamespaceSetup {
    idx: u32,
    namespace_id: String,
    veth_vm_ip: Ipv4Addr,
    veth_host_ip: Ipv4Addr,
    host_interaction_ip: Ipv4Addr,
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
}

/// The host's shared rtnetlink connection.
///
/// Slot setup and teardown each opened their own netlink socket — a socket, a
/// bind and a spawned task per operation, several per slot. `Handle` is a
/// multiplexing sender over one connection and is explicitly meant to be
/// cloned, so one connection serves the whole process.
///
/// The connection lives on a dedicated thread with its own current-thread
/// runtime rather than on whichever runtime happened to create it first: a
/// connection spawned onto a caller's runtime dies with it, taking every
/// outstanding request on other threads with it. That thread also runs the
/// requests themselves, so no caller builds a runtime of its own.
///
/// This deliberately does not cover the in-namespace configuration path: that
/// runs on a thread that has already `setns`'d into the sandbox's namespace,
/// and a netlink socket carries the namespace it was opened in. Sharing a host
/// socket there would silently configure the host.
static HOST_NETLINK: OnceLock<std::result::Result<HostNetlink, String>> = OnceLock::new();

/// Work handed to the shared netlink thread.
type NetlinkJob = Box<dyn FnOnce(Handle) -> BoxFuture<'static, ()> + Send>;

#[derive(Clone)]
struct HostNetlink {
    jobs: tokio::sync::mpsc::UnboundedSender<NetlinkJob>,
}

fn host_netlink() -> Result<HostNetlink> {
    HOST_NETLINK
        .get_or_init(|| {
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
            let (job_tx, mut job_rx) = tokio::sync::mpsc::unbounded_channel::<NetlinkJob>();
            thread::Builder::new()
                .name("agentenv-netlink".to_string())
                .spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "build the netlink connection runtime: {error}"
                            )));
                            return;
                        }
                    };
                    // `new_connection` registers the netlink socket with the
                    // reactor, so it has to run inside the runtime rather than
                    // beside it. Building the socket outside panics the moment
                    // it looks for a reactor that is not there.
                    runtime.block_on(async move {
                        let (connection, handle, _) = match new_connection() {
                            Ok(parts) => parts,
                            Err(error) => {
                                let _ =
                                    ready_tx.send(Err(format!("connect to host netlink: {error}")));
                                return;
                            }
                        };
                        tokio::spawn(connection);
                        if ready_tx.send(Ok(HostNetlink { jobs: job_tx })).is_err() {
                            return;
                        }
                        // Owns the connection and the work queue for the life
                        // of the process. Each job is spawned rather than
                        // awaited so a refill batch's slots overlap.
                        while let Some(job) = job_rx.recv().await {
                            tokio::spawn(job(handle.clone()));
                        }
                    });
                })
                .map_err(|error| format!("spawn the netlink connection thread: {error}"))?;

            ready_rx
                .recv()
                .map_err(|_| "the netlink connection thread exited during startup".to_string())?
        })
        .clone()
        .map_err(|error| anyhow!("{error}"))
}

/// Runs one host-netlink operation on the shared connection thread.
///
/// Jobs must not block: one current-thread runtime drives every request, so a
/// job that parks its thread parks the whole connection.
///
/// Each host-side operation used to build a current-thread runtime on a freshly
/// spawned thread just to block on one request — twice per slot lifetime, on
/// top of the socket each of them opened. The work now travels to the thread
/// that already owns a runtime and a connection; the caller blocks on the
/// answer exactly as it blocked on the thread join before.
fn run_on_host_netlink<F, T>(job: F) -> Result<T>
where
    F: FnOnce(Handle) -> BoxFuture<'static, Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let netlink = host_netlink()?;
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    netlink
        .jobs
        .send(Box::new(move |handle| {
            Box::pin(async move {
                let _ = result_tx.send(job(handle).await);
            })
        }))
        .map_err(|_| anyhow!("the shared netlink worker is gone"))?;

    result_rx
        .recv()
        .map_err(|_| anyhow!("the shared netlink worker dropped the request"))?
}

/// Whether the kernel accepts an on-link /32 route added over netlink.
///
/// Adding a route whose gateway sits on a /31 point-to-point link used to fail
/// on some kernel configurations, which is why this path shelled out to `ip`.
/// Rather than keep paying a fork and exec per slot for a compatibility case
/// that may not apply, the first attempt goes over netlink and the answer is
/// remembered: a kernel that refuses it refuses it every time.
static NETLINK_ROUTE_ADD_WORKS: OnceLock<bool> = OnceLock::new();

/// Whether the host-side netlink job left an `ip route` call for its caller.
///
/// The shell-out cannot run inside the job. `run_with_scoped_capabilities`
/// spawns a thread and joins it, and the netlink worker's current-thread
/// runtime also drives the connection, so a job that parks there parks every
/// other slot's in-flight request with it.
#[derive(Debug, PartialEq, Eq)]
enum HostRouteFallback {
    /// The route is installed; the caller has nothing left to do.
    Installed,
    /// The kernel refused the on-link /32 over netlink. The caller adds it with
    /// `ip route`, on its own thread.
    ShellOut,
}

impl Slot {
    fn host_veth_name(idx: u32) -> String {
        format!("{HOST_VETH_PREFIX}{idx}")
    }

    pub(super) fn new(
        idx: u32,
        address_plan: NetworkAddressPlan,
        netns_dir: PathBuf,
        egress_proxy: Arc<EgressProxy>,
    ) -> Result<Self, NetworkError> {
        // Validation for zero and overflow.
        if idx == 0 || idx >= (MAX_SLOTS as u32) {
            return Err(NetworkError::SlotOutOfRange {
                idx,
                max: (MAX_SLOTS as u32) - 1,
            });
        }

        // Named for this node and this slot rather than for a fresh UUID: it
        // is what lets startup tell its own leftovers from a second AgentENV
        // process's live namespaces, and what ties a leftover `veth-N` back to
        // the namespace it belonged to.
        let namespace_id = super::namespace_file_name(idx);
        let (host_interaction_ip, veth_host_ip, veth_vm_ip) = address_plan
            .slot_ips(idx)
            .map_err(NetworkError::NamespaceError)?;

        Ok(Self {
            idx,
            namespace_id,
            host_interaction_ip,
            veth_host_ip,
            veth_vm_ip,
            address_plan,
            netns_dir,
            egress_proxy,
            cleanup_armed: false,
            user_egress_rules_present: false,
        })
    }

    /// Creates the network infrastructure for this slot using a separate thread
    /// to isolate namespace operations.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip
        )
    )]
    pub(super) fn create_network(&mut self) -> Result<(), NetworkError> {
        // Arm drop cleanup as soon as we begin touching kernel networking state.
        // If setup fails midway, Drop can still perform best-effort cleanup.
        self.cleanup_armed = true;

        // Capture individual fields rather than cloning `self`. Slot is not Clone
        // intentionally — a clone would carry Drop semantics and tear down the live
        // network when the thread finishes.
        let setup = NamespaceSetup {
            idx: self.idx,
            namespace_id: self.namespace_id.clone(),
            veth_vm_ip: self.veth_vm_ip,
            veth_host_ip: self.veth_host_ip,
            host_interaction_ip: self.host_interaction_ip,
            address_plan: self.address_plan,
            netns_dir: self.netns_dir.clone(),
        };
        let idx = setup.idx;
        let veth_host_ip = setup.veth_host_ip;
        let veth_vm_ip = setup.veth_vm_ip;
        let host_interaction_ip = setup.host_interaction_ip;

        // The baseline namespace this slot's host-side veth is moved back to.
        // Captured at startup from the startup thread; see `HOST_NS_FD`.
        let host_ns_fd = host_ns_fd();

        // Spawn a thread to perform namespace operations safely.
        let handle = thread::spawn(move || Self::setup_namespace_internal(setup, host_ns_fd));

        match handle.join() {
            Ok(result) => result.map_err(NetworkError::NamespaceError),
            Err(e) => Err(NetworkError::NamespaceError(anyhow!(
                "Network setup thread panicked: {:?}",
                e
            ))),
        }?;

        // Configure the Host side now, on the shared netlink connection.
        let mut host_stage = MetricGuard::stage(SLOT_STAGE_DURATION, "host_configure");
        let host_result = run_on_host_netlink(move |handle| {
            Box::pin(Self::configure_host_interface_async(
                handle,
                idx,
                veth_host_ip,
                veth_vm_ip,
                host_interaction_ip,
            ))
        });
        host_stage.finish(&host_result);
        let fallback = host_result.map_err(NetworkError::NamespaceError)?;

        let veth_name = Self::host_veth_name(idx);
        // Deliberately outside the netlink job: see [`HostRouteFallback`].
        if fallback == HostRouteFallback::ShellOut {
            Self::add_host_interaction_route_via_ip(&veth_name, host_interaction_ip, veth_vm_ip)
                .map_err(NetworkError::NamespaceError)?;
        }

        // Reduce ARP retransmit delay on host-side veth to avoid resume tail latency (issue #272).
        Self::tune_neigh_retrans_time_ms(&veth_name);

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            slot = setup.idx,
            namespace_id = %setup.namespace_id,
            veth_vm_ip = %setup.veth_vm_ip,
            veth_host_ip = %setup.veth_host_ip,
            host_interaction_ip = %setup.host_interaction_ip
        )
    )]
    fn setup_namespace_internal(
        setup: NamespaceSetup,
        host_ns_fd: BorrowedFd<'static>,
    ) -> Result<()> {
        let NamespaceSetup {
            idx,
            namespace_id,
            veth_vm_ip,
            veth_host_ip,
            host_interaction_ip,
            address_plan,
            netns_dir,
        } = setup;

        // 1. Create/Open Target Network Namespace
        if !netns_dir.exists() {
            fs::create_dir_all(&netns_dir).with_context(|| {
                format!(
                    "Failed to create AENV network namespace directory {}",
                    netns_dir.display()
                )
            })?;
        }
        let netns_path = netns_dir.join(&namespace_id);
        if !netns_path.exists() {
            File::create(&netns_path).context("Failed to create netns file")?;
        }

        let mut netns_stage = MetricGuard::stage(SLOT_STAGE_DURATION, "netns_create");
        let netns_result = (|| -> Result<()> {
            unshare(CloneFlags::CLONE_NEWNET).context("Failed to unshare(CLONE_NEWNET)")?;
            // Bind mount the new namespace to make it persistent/named
            mount(
                Some("/proc/thread-self/ns/net"),
                &netns_path,
                None::<&str>,
                MsFlags::MS_BIND,
                None::<&str>,
            )
            .context("Failed to bind mount new namespace")
        })();
        netns_stage.finish(&netns_result);
        netns_result?;

        // Configure interfaces inside the namespace via netlink
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to build tokio runtime in thread")?;
        let tap_ip = address_plan.tap_ip();
        let vm_link_prefix = address_plan.vm_link_prefix();

        let mut configure_stage = MetricGuard::stage(SLOT_STAGE_DURATION, "ns_configure");
        let configure_result = rt.block_on(Self::configure_namespace_interfaces(
            idx,
            veth_vm_ip,
            veth_host_ip,
            tap_ip,
            vm_link_prefix,
            host_ns_fd,
        ));
        configure_stage.finish(&configure_result);
        configure_result?;

        // Enable IP forwarding inside this namespace so packets received on tap0
        // (from the VM) can be forwarded to vpeer (towards the host/internet).
        fs::write("/proc/sys/net/ipv4/ip_forward", "1")
            .context("Failed to enable IP forwarding in namespace")?;

        // Reduce ARP retransmit delay for faster resume (issue #272)
        Self::tune_neigh_retrans_time_ms("tap0");
        Self::tune_neigh_retrans_time_ms("vpeer");

        // IPTables Setup
        let mut iptables_stage = MetricGuard::stage(SLOT_STAGE_DURATION, "iptables_apply");
        let iptables_result = Self::configure_namespace_iptables_rules(
            host_interaction_ip,
            veth_vm_ip,
            address_plan.vm_ip(),
            &address_plan.internal_egress_denied_cidrs(),
        );
        iptables_stage.finish(&iptables_result);
        iptables_result
    }

    /// Configures all network interfaces inside the namespace:
    /// creates veth pair, moves host end back, sets up loopback/vpeer/tap0, and adds default route.
    #[tracing::instrument(
        skip(host_ns_fd),
        fields(
            slot = idx,
            veth_vm_ip = %veth_vm_ip,
            veth_host_ip = %veth_host_ip
        )
    )]
    async fn configure_namespace_interfaces(
        idx: u32,
        veth_vm_ip: Ipv4Addr,
        veth_host_ip: Ipv4Addr,
        tap_ip: Ipv4Addr,
        vm_link_prefix: u8,
        host_ns_fd: BorrowedFd<'_>,
    ) -> Result<()> {
        let (connection, handle, _) = new_connection().context("Failed to connect to netlink")?;
        tokio::spawn(connection);

        // Create Veth Pair (veth-{idx} and vpeer)
        let veth_name = Self::host_veth_name(idx);
        let vpeer_name = VPEER_NAME;

        let mut veth_stage = MetricGuard::stage(SLOT_STAGE_DURATION, "veth_create");
        let veth_result = Self::create_veth_pair(&handle, &veth_name).await;
        veth_stage.finish(&veth_result);
        veth_result?;

        // One dump for both remaining names. A fresh namespace holds four
        // devices, so a dump costs less than the name lookups this replaced —
        // and where the host end still has to be moved out, it has to happen
        // while that end is still here.
        let indices = link_indices(&handle, &[&veth_name, "lo", vpeer_name]).await?;

        if let Some(veth_index) = indices.get(veth_name.as_str()).copied() {
            // The host end was created in this namespace and has to be moved
            // back out. This is `dev_change_net_namespace()`, which holds RTNL
            // across `synchronize_net()`; the peer-in-namespace path above
            // exists to skip it.
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = veth_index;
            msg.attributes
                .push(LinkAttribute::NetNsFd(host_ns_fd.as_raw_fd()));
            count_netlink_op("RTM_SETLINK");
            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to move veth to host ns")?;
        } else if !create_veth_peer_in_namespace() {
            return Err(anyhow!("Created veth interface not found"));
        }

        // Configure IPs inside NS
        // Loopback UP
        if let Some(lo_index) = indices.get("lo").copied() {
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = lo_index;
            msg.header.flags.insert(LinkFlags::Up);
            msg.header.change_mask.insert(LinkFlags::Up);

            count_netlink_op("RTM_SETLINK");
            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to set lo up")?;
        }

        // Vpeer setup
        // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
        if let Some(vpeer_index) = indices.get(vpeer_name).copied() {
            // Add IP without broadcast (RFC 3021 for /31)
            Self::add_address_no_broadcast(&handle, vpeer_index, veth_vm_ip, 31)
                .await
                .context("Failed to add address to vpeer")?;

            // Set vpeer UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = vpeer_index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            count_netlink_op("RTM_SETLINK");
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set vpeer up")?;
        }

        // Create tap0, in process. The ioctl answers with nothing, but the
        // device is addressable by name immediately, so its index comes from
        // `if_nametoindex` rather than the RTM_GETLINK this used to need.
        let tap_index = Self::create_tap_interface(TAP_NAME)?;

        count_netlink_op("RTM_NEWADDR");
        handle
            .address()
            .add(tap_index, IpAddr::V4(tap_ip), vm_link_prefix)
            .execute()
            .await
            .context("Failed to add address to tap0")?;

        let mut link_msg = netlink_packet_route::link::LinkMessage::default();
        link_msg.header.index = tap_index;
        link_msg.header.flags.insert(LinkFlags::Up);
        link_msg.header.change_mask.insert(LinkFlags::Up);
        count_netlink_op("RTM_SETLINK");
        handle
            .link()
            .set(link_msg)
            .execute()
            .await
            .context("Failed to set tap0 up")?;

        // Add default route via veth_host_ip (the host side of the veth pair)
        // Using rtnetlink's RouteMessageBuilder API
        let route_msg = rtnetlink::RouteMessageBuilder::<std::net::Ipv4Addr>::new()
            .gateway(veth_host_ip)
            .build();
        count_netlink_op("RTM_NEWROUTE");
        handle
            .route()
            .add(route_msg)
            .execute()
            .await
            .context("Failed to add default route")?;

        Ok(())
    }

    /// Builds the kernel `ip=` boot argument for this slot's VM network configuration.
    ///
    /// Format: `ip=<vm_ip>::<tap_ip>:<netmask>:<hostname>:<iface>:<autoconf>:<dns>`
    pub(crate) fn build_ip_boot_arg(&self) -> String {
        let dns_ip = self.guest_dns_server();
        format!(
            "ip={}::{}:{}:instance:eth0:off:{}",
            self.address_plan.vm_ip(),
            self.address_plan.tap_ip(),
            self.address_plan.vm_link_mask(),
            dns_ip
        )
    }

    pub(crate) fn guest_dns_server(&self) -> Ipv4Addr {
        resolve_guest_dns_server()
    }

    pub(crate) fn namespace_path(&self) -> std::path::PathBuf {
        self.netns_dir.join(&self.namespace_id)
    }

    pub(crate) fn set_egress_policy(
        &mut self,
        policy: Option<&SandboxNetworkPolicy>,
    ) -> Result<()> {
        // Policy updates run through the owning FirecrackerSandbox's mutable
        // operation lock; cleanup owns this Slot exclusively. Avoid holding a
        // second lock across namespace I/O, iptables, or proxy joins.
        let wants_rules = policy.is_some_and(SandboxNetworkPolicy::has_runtime_egress_rules);
        if !wants_rules && !self.user_egress_rules_present {
            return Ok(());
        }

        let requires_egress_proxy = policy.is_some_and(SandboxNetworkPolicy::requires_egress_proxy);
        let had_active_proxy_policy = self.egress_proxy.has_active(self.host_interaction_ip);
        if requires_egress_proxy {
            self.egress_proxy
                .ensure_listener(self.host_interaction_ip, &self.namespace_path())?;
            self.egress_proxy.prepare(
                self.host_interaction_ip,
                policy.expect("proxy policy must be present when interception is requested"),
            );
            // There was no old proxy policy to preserve. Activating before the
            // redirect is installed keeps the old default-allow behavior while
            // the namespace rules are being committed.
            if !had_active_proxy_policy {
                self.egress_proxy.activate(self.host_interaction_ip);
            }
        }

        let netns_path = self.namespace_path();
        let egress_proxy_port = self.egress_proxy.port();
        let policy = policy.cloned();
        let handle = thread::spawn(move || -> Result<()> {
            let netns = File::open(&netns_path).with_context(|| {
                format!("failed to open network namespace {}", netns_path.display())
            })?;
            nix::sched::setns(netns.as_fd(), CloneFlags::CLONE_NEWNET)
                .context("failed to enter sandbox network namespace")?;
            set_namespace_egress_policy(policy.as_ref(), egress_proxy_port)
        });

        let result = match handle.join() {
            Ok(result) => result,
            Err(e) => Err(anyhow!("egress policy setup thread panicked: {:?}", e)),
        };
        if result.is_ok() {
            self.user_egress_rules_present = wants_rules;
            if requires_egress_proxy && had_active_proxy_policy {
                self.egress_proxy.activate(self.host_interaction_ip);
            } else if !requires_egress_proxy {
                self.egress_proxy.deactivate(self.host_interaction_ip);
            }
        } else if requires_egress_proxy {
            if had_active_proxy_policy {
                self.egress_proxy.discard_pending(self.host_interaction_ip);
            } else {
                self.egress_proxy.teardown(self.host_interaction_ip);
            }
        }
        result
    }

    /// Configures iptables rules inside the namespace for VM traffic routing.
    /// This includes:
    /// - Enabling IP forwarding so the namespace can route between tap0 and vpeer.
    /// - FORWARD rules to permit traffic between the VM (tap0) and the host veth (vpeer).
    /// - SNAT/DNAT for host<->VM communication via host_interaction_ip.
    ///
    /// The egress chains go in the same invocation. They used to be a second
    /// `iptables-restore` immediately after this one, which cost a second fork
    /// and a second xtables-lock acquisition per slot for rules that end up in
    /// the same two tables of the same namespace.
    #[tracing::instrument(fields(vm_ip = %vm_ip, host_interaction_ip = %host_interaction_ip))]
    fn configure_namespace_iptables_rules(
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        vm_ip: Ipv4Addr,
        internal_egress_denied_cidrs: &[String],
    ) -> Result<()> {
        let commands = Self::namespace_iptables_commands(
            host_interaction_ip,
            veth_vm_ip,
            vm_ip,
            internal_egress_denied_cidrs,
        );
        apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)
            .context("apply AgentENV namespace iptables rules")
    }

    fn namespace_iptables_commands(
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        vm_ip: Ipv4Addr,
        internal_egress_denied_cidrs: &[String],
    ) -> Vec<IptablesRestoreCommand> {
        let mut commands = Self::namespace_routing_commands(host_interaction_ip, veth_vm_ip, vm_ip);
        commands.extend(namespace_egress_chain_commands(
            resolve_guest_dns_server(),
            internal_egress_denied_cidrs,
        ));
        group_commands_by_table(commands)
    }

    fn namespace_routing_commands(
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        vm_ip: Ipv4Addr,
    ) -> Vec<IptablesRestoreCommand> {
        vec![
            // FORWARD: Allow traffic from VM (tap0) to host/internet (vpeer).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i tap0 -o vpeer -j ACCEPT".to_string(),
            },
            // FORWARD: Allow established/related traffic from host/internet (vpeer) back to VM (tap0).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i vpeer -o tap0 -m state --state RELATED,ESTABLISHED -j ACCEPT".to_string(),
            },
            // SNAT: Rewrite source IP from the VM to the slot's host interaction IP.
            // This covers both host<->VM communication and internet-bound traffic from the VM.
            // The host then applies its own MASQUERADE to reach the internet.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: format!("-o vpeer -s {} -j SNAT --to {}", vm_ip, host_interaction_ip),
            },
            // Namespace-local egress proxy connections originate from vpeer's
            // address rather than the guest address above. Give them the same
            // routable slot identity so host FORWARD/MASQUERADE rules apply.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: format!("-o vpeer -s {veth_vm_ip} -j SNAT --to {host_interaction_ip}"),
            },
            // DNAT: Rewrite destination IP from the host interaction IP to the VM.
            // This allows the host to reach the VM using the unique HostIP.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "PREROUTING",
                rule: format!("-i vpeer -d {} -j DNAT --to {}", host_interaction_ip, vm_ip),
            },
        ]
    }

    /// Creates the veth pair for this slot.
    ///
    /// Two shapes, selected by config. The default creates both ends inside the
    /// namespace and lets the caller move the host end back out. The other
    /// hands the namespace's fd to the host connection and has the kernel
    /// register the peer directly in it, which removes the move — the one
    /// operation on this path documented to hold RTNL across an RCU grace
    /// period.
    async fn create_veth_pair(handle: &Handle, veth_name: &str) -> Result<()> {
        if create_veth_peer_in_namespace() {
            let veth_name = veth_name.to_string();
            // This thread is inside the sandbox namespace, so this is its fd.
            // Held open across the request: the host connection resolves it in
            // this process's descriptor table.
            let netns = File::open(HOST_NS_PATH)
                .context("open the sandbox network namespace for the veth peer")?;
            let netns_fd = netns.as_raw_fd();
            // Issued from the host connection: the pair's host end must land in
            // the host namespace, and a netlink socket writes to the namespace
            // it was opened in.
            let created = run_on_host_netlink(move |host_handle| {
                Box::pin(async move {
                    let message = veth_create_message(&veth_name, Some(netns_fd));
                    count_netlink_op("RTM_NEWLINK");
                    host_handle
                        .link()
                        .add(message)
                        .execute()
                        .await
                        .context("Failed to create veth pair with the peer in the namespace")
                })
            });
            drop(netns);
            return created;
        }

        let message = veth_create_message(veth_name, None);
        count_netlink_op("RTM_NEWLINK");
        handle
            .link()
            .add(message)
            .execute()
            .await
            .context("Failed to create veth pair")
    }

    /// Creates the guest-facing tap device and returns its interface index.
    ///
    /// The `tun` driver registers no netlink `newlink` operation, so a tap
    /// device cannot be created over rtnetlink at all — `ip tuntap` opens
    /// /dev/net/tun and issues TUNSETIFF, and so does this. Doing it here
    /// removes a fork, a capability-scoped thread, and the RTM_GETLINK that had
    /// to find the device afterwards.
    fn create_tap_interface(name: &str) -> Result<u32> {
        match Self::create_tap_via_ioctl(name) {
            Ok(index) => Ok(index),
            // Asked of the kernel rather than of `/proc/self/status`: that
            // file describes the thread-group leader, and these capabilities
            // are per-thread. The shell-out acquires CAP_NET_ADMIN on a thread
            // of its own, so it can still succeed where this failed.
            Err(error) if is_permission_error(&error) => {
                warn!(
                    error = %error,
                    "TUNSETIFF refused; falling back to the `ip tuntap` shell-out"
                );
                Self::create_tap_via_ip(name)
            }
            Err(error) => Err(error),
        }
    }

    fn create_tap_via_ioctl(name: &str) -> Result<u32> {
        let request = tun_set_iff_request(name)?;
        let tun = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(TUN_DEVICE_PATH)
            .with_context(|| format!("open {TUN_DEVICE_PATH}"))?;

        // SAFETY: TUNSETIFF reads one `ifreq` through the pointer, `request`
        // is exactly that layout (pinned by `tun_set_iff_request_matches_the_kernel_ifreq`),
        // and the fd is open for the duration of the call.
        let created = unsafe { libc::ioctl(tun.as_raw_fd(), TUNSETIFF, &request as *const _) };
        if created < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("TUNSETIFF for {name}"));
        }

        // Without this the device disappears when `tun` is dropped at the end
        // of this function; `ip tuntap add` sets it for the same reason.
        // SAFETY: TUNSETPERSIST takes an int by value on an open tun fd.
        let persisted = unsafe { libc::ioctl(tun.as_raw_fd(), TUNSETPERSIST, 1 as libc::c_int) };
        if persisted < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("TUNSETPERSIST for {name}"));
        }

        interface_index(name)
    }

    fn create_tap_via_ip(name: &str) -> Result<u32> {
        let status = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["tuntap", "add", name, "mode", "tap"])
                    .status()
                    .context("Failed to execute ip tuntap")
            },
        )?;
        if !status.success() {
            return Err(anyhow!("ip tuntap add failed"));
        }
        interface_index(name)
    }

    fn tune_neigh_retrans_time_ms(interface: &str) {
        let retrans_path = format!("/proc/sys/net/ipv4/neigh/{interface}/retrans_time_ms");

        for attempt in 0..=NEIGH_SYSCTL_RETRIES {
            match fs::write(&retrans_path, ARP_RETRANS_TIME_MS) {
                Ok(()) => {
                    return;
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::NotFound
                        && attempt < NEIGH_SYSCTL_RETRIES =>
                {
                    debug!(
                        interface,
                        path = %retrans_path,
                        attempt = attempt + 1,
                        error = %err,
                        "ARP retransmit sysctl not ready; retrying"
                    );
                    thread::sleep(Duration::from_millis(NEIGH_SYSCTL_RETRY_DELAY_MS));
                }
                Err(err) => {
                    warn!(
                        interface,
                        path = %retrans_path,
                        error = %err,
                        "failed to configure ARP retransmit delay"
                    );
                    return;
                }
            }
        }
    }

    #[tracing::instrument(
        fields(
            slot = idx,
            host_veth = %Self::host_veth_name(idx),
            veth_host_ip = %veth_host_ip,
            veth_vm_ip = %veth_vm_ip,
            host_interaction_ip = %host_interaction_ip
        )
    )]
    async fn configure_host_interface_async(
        handle: Handle,
        idx: u32,
        veth_host_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        host_interaction_ip: Ipv4Addr,
    ) -> Result<HostRouteFallback> {
        let veth_name = Self::host_veth_name(idx);

        // Wait/Check for interface
        count_netlink_op("RTM_GETLINK");
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();
        if let Some(link) = links.try_next().await? {
            // Add only the veth link IP, not the host_interaction_ip.
            // host_interaction_ip is used as a routing destination, not an interface address
            //
            // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
            Self::add_address_no_broadcast(&handle, link.header.index, veth_host_ip, 31)
                .await
                .context("Failed to add IP to host veth")?;

            // Set UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = link.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            count_netlink_op("RTM_SETLINK");
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set host veth up")?;

            // Route host traffic for host_interaction_ip through the namespace's
            // vpeer address; the namespace's DNAT rule then rewrites it to the
            // VM's internal address.
            Self::add_host_interaction_route(
                &handle,
                &NETLINK_ROUTE_ADD_WORKS,
                link.header.index,
                &veth_name,
                host_interaction_ip,
                veth_vm_ip,
            )
            .await
        } else {
            Err(anyhow!(
                "Host veth interface {} not found after move",
                veth_name
            ))
        }
    }

    /// Adds the host-to-namespace route, over netlink when the kernel allows it.
    ///
    /// The gateway sits on a /31 point-to-point link, which some kernel
    /// configurations reject over netlink. The first slot finds out; every slot
    /// after it takes the answer from `netlink_works` rather than re-probing,
    /// so a working kernel never forks and a rejecting one forks exactly as
    /// often as it did before.
    ///
    /// Runs on the shared netlink worker, so it never forks itself: a refusing
    /// kernel is reported back as [`HostRouteFallback::ShellOut`] and the
    /// caller runs `ip route` on its own thread.
    ///
    /// `netlink_works` is passed rather than read from
    /// [`NETLINK_ROUTE_ADD_WORKS`] so a test can drive the refusing-kernel
    /// branch without latching that answer for the whole process.
    async fn add_host_interaction_route(
        handle: &Handle,
        netlink_works: &OnceLock<bool>,
        link_index: u32,
        veth_name: &str,
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
    ) -> Result<HostRouteFallback> {
        if netlink_works.get() != Some(&false) {
            count_netlink_op("RTM_NEWROUTE");
            let route = rtnetlink::RouteMessageBuilder::<Ipv4Addr>::new()
                .destination_prefix(host_interaction_ip, 32)
                .gateway(veth_vm_ip)
                .output_interface(link_index)
                // iproute2 stamps RTPROT_BOOT on a route added from the command
                // line; the builder defaults to RTPROT_STATIC. The tag is what
                // route-flushing tools filter on, so the two paths have to
                // agree — `the_netlink_route_and_the_ip_route_are_the_same_route`
                // caught them disagreeing.
                .protocol(netlink_packet_route::route::RouteProtocol::Boot)
                .build();
            match handle.route().add(route).execute().await {
                Ok(()) => {
                    let _ = netlink_works.set(true);
                    return Ok(HostRouteFallback::Installed);
                }
                Err(error) if netlink_works.get() == Some(&true) => {
                    // Netlink has worked on this kernel before, so this is a
                    // real failure for this route rather than a capability gap.
                    return Err(anyhow!(
                        "Failed to add route to {host_interaction_ip}/32 via {veth_vm_ip} \
                         dev {veth_name} over netlink: {error}"
                    ));
                }
                Err(error) => {
                    let _ = netlink_works.set(false);
                    warn!(
                        error = %error,
                        "kernel rejected an on-link /32 route over netlink; \
                         falling back to `ip route` for the life of this process"
                    );
                }
            }
        }

        Ok(HostRouteFallback::ShellOut)
    }

    fn add_host_interaction_route_via_ip(
        veth_name: &str,
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
    ) -> Result<()> {
        let output = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args([
                        "route",
                        "add",
                        &format!("{host_interaction_ip}/32"),
                        "via",
                        &veth_vm_ip.to_string(),
                        "dev",
                        veth_name,
                    ])
                    .output()
                    .context("Failed to execute ip route add")
            },
        )?;

        if output.status.success() {
            return Ok(());
        }
        Err(anyhow!(
            "Failed to add route to {}/32 via {} dev {}: {}",
            host_interaction_ip,
            veth_vm_ip,
            veth_name,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }

    /// Async helper to delete veth interface.
    /// Idempotent: succeeds even if the interface doesn't exist.
    async fn delete_veth_interface_async(handle: Handle, idx: u32) -> Result<()> {
        let veth_name = Self::host_veth_name(idx);
        count_netlink_op("RTM_GETLINK");
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();

        // try_next returns Err if interface doesn't exist (ENODEV), treat as success
        match links.try_next().await {
            Ok(Some(link)) => {
                // Interface exists, try to delete; ignore "not found" race
                count_netlink_op("RTM_DELLINK");
                if let Err(e) = handle.link().del(link.header.index).execute().await {
                    let msg = e.to_string();
                    if !msg.contains("No such device") && !msg.contains("ENODEV") {
                        return Err(e.into());
                    }
                }
            }
            Ok(None) => {} // Interface not found
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("No such device") && !msg.contains("ENODEV") {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Deletes the host veth over the shared netlink connection.
    ///
    /// This is the regular (non-shutdown) cleanup path.
    fn delete_host_veth_interface_over_netlink(idx: u32) -> Result<()> {
        run_on_host_netlink(move |handle| Box::pin(Self::delete_veth_interface_async(handle, idx)))
    }

    /// Deletes host veth using `ip link del`.
    ///
    /// Kept synchronous as the fallback path for shutdown/exit cleanup where
    /// Tokio context may already be unavailable.
    fn delete_host_veth_interface_sync(idx: u32) -> Result<()> {
        let veth_name = Self::host_veth_name(idx);
        let output = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["link", "del", &veth_name])
                    .output()
                    .context("Failed to execute ip link del")
            },
        )?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr_lower = stderr.to_lowercase();
        if stderr_lower.contains("cannot find device")
            || stderr_lower.contains("no such device")
            || stderr_lower.contains("not found")
        {
            return Ok(());
        }

        Err(anyhow!(
            "Failed to delete veth interface {}: {}",
            veth_name,
            stderr.trim()
        ))
    }

    /// Tries netlink veth cleanup first and falls back to the `ip link del`
    /// shell-out on either regular error or panic.
    #[tracing::instrument(fields(slot = idx, host_veth = %Self::host_veth_name(idx)))]
    fn delete_host_veth_interface(idx: u32) -> Result<()> {
        match std::panic::catch_unwind(|| Self::delete_host_veth_interface_over_netlink(idx)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                info!(
                    slot = idx,
                    error = %err,
                    "netlink slot cleanup failed; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
            Err(_) => {
                info!(
                    slot = idx,
                    "netlink slot cleanup panicked; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
        }
    }

    /// Cleans up the network resources for this slot.
    /// This includes deleting the host-side veth interface, removing the network namespace,
    /// and removing the host-side MASQUERADE rule.
    /// Idempotent: safe to call multiple times or concurrently.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip,
            force_sync
        )
    )]
    pub(super) fn cleanup(&mut self, force_sync: bool) -> Result<(), NetworkError> {
        // Skip cleanup for slots that never attempted network setup.
        // This avoids touching host networking state for logical-only Slot values.
        if !self.cleanup_armed {
            return Ok(());
        }
        self.cleanup_armed = false;

        // A namespace-local listener pins the namespace. Stop proxy acceptance
        // before removing the veth and unmounting the namespace, including
        // panic/drop cleanup paths that bypass the normal release path.
        self.egress_proxy.teardown(self.host_interaction_ip);

        // 1. Delete Host Veth Interface (this destroys the pair)
        let delete_result = if force_sync {
            Self::delete_host_veth_interface_sync(self.idx)
        } else {
            Self::delete_host_veth_interface(self.idx)
        };
        if let Err(e) = delete_result {
            self.cleanup_armed = true;
            return Err(NetworkError::NamespaceError(e));
        }

        // 2. Unmount Netns Bind Mount (may need multiple unmounts if mounted multiple times)
        let netns_path = self.namespace_path();
        let path = netns_path.as_path();
        if path.exists() {
            loop {
                match nix::mount::umount(path) {
                    Ok(_) => continue,
                    Err(nix::errno::Errno::EINVAL) => break, // Not mounted anymore
                    Err(nix::errno::Errno::ENOENT) => break, // File removed by another process
                    Err(e) => {
                        self.cleanup_armed = true;
                        return Err(NetworkError::NamespaceError(anyhow!(
                            "Failed to unmount netns: {}",
                            e
                        )));
                    }
                }
            }

            // 3. Delete Netns File (ignore NotFound - another process may have deleted it)
            match fs::remove_file(path) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    self.cleanup_armed = true;
                    return Err(NetworkError::IoError(e));
                }
            }
        }

        Ok(())
    }

    /// Add an IPv4 address to an interface without setting broadcast address.
    /// This is needed for /31 point-to-point links (RFC 3021) where there is no
    /// broadcast address. The rtnetlink crate's AddressMessageBuilder incorrectly
    /// calculates a broadcast address for /31 networks.
    async fn add_address_no_broadcast(
        handle: &Handle,
        if_index: u32,
        addr: Ipv4Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let mut msg = AddressMessage::default();
        msg.header.family = AddressFamily::Inet;
        msg.header.prefix_len = prefix_len;
        msg.header.index = if_index;

        // Add Address and Local attributes (required for IPv4)
        // Do NOT add Broadcast attribute for /31 networks
        msg.attributes
            .push(AddressAttribute::Address(IpAddr::V4(addr)));
        msg.attributes
            .push(AddressAttribute::Local(IpAddr::V4(addr)));

        count_netlink_op("RTM_NEWADDR");
        let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewAddress(msg));
        req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;

        let mut response = handle.clone().request(req)?;
        while let Some(message) = response.next().await {
            if let NetlinkPayload::Error(err) = message.payload {
                return Err(anyhow!("Netlink error: {:?}", err));
            }
        }
        Ok(())
    }
}

/// The fields that decide whether two routes are the same route.
///
/// The netlink path and the `ip route` path have to produce identical kernel
/// state, and "the command exited zero" does not say that: the defect this
/// guards against is a builder that omits `RTA_OIF`, leaving the kernel to
/// resolve the gateway by lookup instead of being told the egress device — on
/// a freshly brought-up /31 exactly the fragile case.
#[cfg(test)]
use netlink_packet_route::route::{RouteAddress, RouteAttribute, RouteMessage};

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RouteIdentity {
    destination: Option<IpAddr>,
    destination_prefix_length: u8,
    gateway: Option<IpAddr>,
    output_interface: Option<u32>,
    table: u8,
    protocol: u8,
    scope: u8,
    kind: u8,
}

#[cfg(test)]
fn route_identity(route: &RouteMessage) -> RouteIdentity {
    let mut identity = RouteIdentity {
        destination: None,
        destination_prefix_length: route.header.destination_prefix_length,
        gateway: None,
        output_interface: None,
        table: route.header.table,
        protocol: route.header.protocol.into(),
        scope: route.header.scope.into(),
        kind: route.header.kind.into(),
    };

    for attribute in &route.attributes {
        match attribute {
            RouteAttribute::Destination(RouteAddress::Inet(address)) => {
                identity.destination = Some(IpAddr::V4(*address));
            }
            RouteAttribute::Gateway(RouteAddress::Inet(address)) => {
                identity.gateway = Some(IpAddr::V4(*address));
            }
            RouteAttribute::Oif(index) => identity.output_interface = Some(*index),
            RouteAttribute::Table(table) => {
                // RTA_TABLE carries the table when it does not fit the header
                // byte; when both are present they agree.
                identity.table = (*table).min(u8::MAX as u32) as u8;
            }
            _ => {}
        }
    }

    identity
}

/// Dumps the IPv4 main-table routes as identities, for equivalence checks.
#[cfg(test)]
async fn route_identities(handle: &Handle) -> Result<Vec<RouteIdentity>> {
    let mut routes = handle
        .route()
        .get(rtnetlink::RouteMessageBuilder::<Ipv4Addr>::new().build())
        .execute();
    let mut identities = Vec::new();
    while let Some(route) = routes.try_next().await? {
        identities.push(route_identity(&route));
    }
    Ok(identities)
}

/// Builds the `RTM_NEWLINK` message for a veth pair.
///
/// `peer_netns_fd` puts `IFLA_NET_NS_FD` inside the peer's nested attribute
/// block, which is where `veth_newlink` reads it: the kernel resolves the
/// peer's namespace from that block and registers it there directly. Absent,
/// both ends are created wherever the message is sent and the caller has to
/// move one of them.
fn veth_create_message(
    veth_name: &str,
    peer_netns_fd: Option<std::os::fd::RawFd>,
) -> netlink_packet_route::link::LinkMessage {
    let mut veth_msg = netlink_packet_route::link::LinkMessage::default();
    veth_msg
        .attributes
        .push(LinkAttribute::IfName(veth_name.to_string()));

    let mut peer_msg = netlink_packet_route::link::LinkMessage::default();
    peer_msg
        .attributes
        .push(LinkAttribute::IfName(VPEER_NAME.to_string()));
    if let Some(fd) = peer_netns_fd {
        peer_msg.attributes.push(LinkAttribute::NetNsFd(fd));
    }

    veth_msg.attributes.push(LinkAttribute::LinkInfo(vec![
        LinkInfo::Kind(InfoKind::Veth),
        LinkInfo::Data(InfoData::Veth(InfoVeth::Peer(peer_msg))),
    ]));
    veth_msg
}

/// Whether to register the veth peer directly in the sandbox namespace.
fn create_veth_peer_in_namespace() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        crate::cfg::ConfigManager::global_config()
            .network
            .slot
            .create_veth_peer_in_namespace
    })
}

/// Counts one netlink operation.
///
/// The label set is the fixed list of message types this module sends, so it
/// stays bounded. This is the measurement the per-slot netlink inventory is
/// read from: an op-count delta per slot is asserted directly rather than
/// inferred from latency.
fn count_netlink_op(op: &'static str) {
    metrics::counter!("agentenv_network_slot_netlink_ops_total", "op" => op).increment(1);
}

/// Interface indices for `wanted`, from a single RTM_GETLINK dump.
async fn link_indices(handle: &Handle, wanted: &[&str]) -> Result<HashMap<String, u32>> {
    count_netlink_op("RTM_GETLINK");
    let mut links = handle.link().get().execute();
    let mut found = HashMap::new();
    while let Some(link) = links.try_next().await? {
        let Some(name) = link_name(&link) else {
            continue;
        };
        if wanted.contains(&name.as_str()) {
            found.insert(name, link.header.index);
        }
    }
    Ok(found)
}

fn link_name(link: &netlink_packet_route::link::LinkMessage) -> Option<String> {
    link.attributes
        .iter()
        .find_map(|attribute| match attribute {
            LinkAttribute::IfName(name) => Some(name.clone()),
            _ => None,
        })
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup(true) {
            warn!(slot = self.idx, error = %e, "slot drop cleanup failed");
        }
    }
}

fn resolve_guest_dns_server() -> Ipv4Addr {
    for path in ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"] {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Some(ip) = parse_nameserver_ipv4(&contents) {
                return ip;
            }
        }
    }

    let fallback = Ipv4Addr::new(8, 8, 8, 8);
    warn!(dns = %fallback, "falling back to public DNS for guest network");
    fallback
}

fn parse_nameserver_ipv4(contents: &str) -> Option<Ipv4Addr> {
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(directive) = parts.next() else {
            continue;
        };
        if directive != "nameserver" {
            continue;
        }

        let Some(candidate) = parts.next() else {
            continue;
        };
        let ip = match candidate.parse::<Ipv4Addr>() {
            Ok(ip) => ip,
            Err(_) => continue,
        };

        if ip.is_loopback() || ip.is_unspecified() {
            continue;
        }

        return Some(ip);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::iptables_util::build_restore_script;
    use super::*;

    /// The rules of one rendered script, keyed by table, in the order the
    /// script commits them.
    fn rules_by_table(script: &str) -> Vec<(String, Vec<String>)> {
        let mut tables: Vec<(String, Vec<String>)> = Vec::new();
        for line in script.lines() {
            if let Some(table) = line.strip_prefix('*') {
                tables.push((table.to_string(), Vec::new()));
            } else if line != "COMMIT" {
                tables
                    .last_mut()
                    .expect("a rule line must follow a table header")
                    .1
                    .push(line.to_string());
            }
        }
        tables
    }

    /// The namespace's routing rules and its egress chains used to be two
    /// `iptables-restore` invocations back to back on the same thread in the
    /// same namespace — two forks, two xtables-lock acquisitions. Merging them
    /// is only safe if the resulting ruleset is the one the two invocations
    /// produced: these rules are appended and inserted at positions that
    /// depend on their order within a table.
    #[test]
    fn the_merged_namespace_batch_commits_what_the_two_batches_committed() {
        let host_interaction_ip = Ipv4Addr::new(10, 11, 0, 7);
        let veth_vm_ip = Ipv4Addr::new(10, 12, 0, 15);
        let vm_ip = Ipv4Addr::new(169, 254, 0, 21);
        let denied = vec!["10.0.0.0/8".to_string()];

        let routing = build_restore_script(&Slot::namespace_routing_commands(
            host_interaction_ip,
            veth_vm_ip,
            vm_ip,
        ));
        let egress = build_restore_script(&group_commands_by_table(
            namespace_egress_chain_commands(resolve_guest_dns_server(), &denied),
        ));
        let merged = build_restore_script(&Slot::namespace_iptables_commands(
            host_interaction_ip,
            veth_vm_ip,
            vm_ip,
            &denied,
        ));

        let mut expected: Vec<(String, Vec<String>)> = Vec::new();
        for (table, rules) in rules_by_table(&routing)
            .into_iter()
            .chain(rules_by_table(&egress))
        {
            match expected.iter_mut().find(|(name, _)| name == &table) {
                Some((_, existing)) => existing.extend(rules),
                None => expected.push((table, rules)),
            }
        }

        assert_eq!(
            rules_by_table(&merged),
            expected,
            "the merged batch must commit each table's rules in the order the \
             two separate batches committed them"
        );
    }

    /// The three in-namespace name lookups became one dump, so the name has to
    /// be read out of the dumped message rather than asked for.
    #[test]
    fn a_dumped_link_is_matched_by_its_name_attribute() {
        let mut named = netlink_packet_route::link::LinkMessage::default();
        named.header.index = 12;
        named
            .attributes
            .push(LinkAttribute::IfName("vpeer".to_string()));
        assert_eq!(link_name(&named).as_deref(), Some("vpeer"));

        let unnamed = netlink_packet_route::link::LinkMessage::default();
        assert_eq!(link_name(&unnamed), None);
    }

    /// The peer's namespace has to travel inside the nested `VETH_INFO_PEER`
    /// block: that is where `veth_newlink` reads it, and it is what makes the
    /// separate `RTM_SETLINK` move — the one operation here that holds RTNL
    /// across an RCU grace period — unnecessary.
    #[test]
    fn the_veth_peer_carries_its_namespace_in_the_nested_peer_block() {
        let peer_of = |message: &netlink_packet_route::link::LinkMessage| {
            message
                .attributes
                .iter()
                .find_map(|attribute| match attribute {
                    LinkAttribute::LinkInfo(info) => Some(info.clone()),
                    _ => None,
                })
                .expect("the message carries link info")
                .into_iter()
                .find_map(|info| match info {
                    LinkInfo::Data(InfoData::Veth(InfoVeth::Peer(peer))) => Some(peer),
                    _ => None,
                })
                .expect("the link info carries a peer")
        };

        let in_namespace = veth_create_message("veth-9", Some(41));
        let peer = peer_of(&in_namespace);
        assert!(
            peer.attributes
                .iter()
                .any(|attribute| matches!(attribute, LinkAttribute::NetNsFd(41))),
            "the peer must carry the namespace it is to be created in: {peer:?}"
        );
        assert!(peer
            .attributes
            .iter()
            .any(|attribute| matches!(attribute, LinkAttribute::IfName(name) if name == "vpeer")));
        assert!(
            !in_namespace
                .attributes
                .iter()
                .any(|attribute| matches!(attribute, LinkAttribute::NetNsFd(_))),
            "only the peer moves; the host end stays where the message is sent"
        );

        let both_ends_here = veth_create_message("veth-9", None);
        assert!(
            !peer_of(&both_ends_here)
                .attributes
                .iter()
                .any(|attribute| matches!(attribute, LinkAttribute::NetNsFd(_))),
            "without a namespace fd the pair is created wherever the message is sent"
        );
    }

    /// The lever is off until it has been compared against the moved-end path
    /// on the target kernel: a kernel that ignored the nested attribute would
    /// put both ends in the wrong namespace and fail every create.
    #[test]
    fn the_peer_in_namespace_path_ships_off() {
        assert!(!crate::cfg::NetworkSlotConfig::default().create_veth_peer_in_namespace);
        assert!(!create_veth_peer_in_namespace());
    }

    /// TUNSETIFF and TUNSETPERSIST as the kernel declares them. Written out as
    /// literals: a wrong direction or size bit produces a request the driver
    /// does not recognise, which fails as `ENOTTY` rather than as a bad flag.
    #[test]
    fn the_tun_ioctl_request_codes_match_the_kernel() {
        assert_eq!(TUNSETIFF, 0x4004_54ca);
        assert_eq!(TUNSETPERSIST, 0x4004_54cb);
    }

    /// The kernel reads a fixed 40-byte `ifreq`. `IFF_VNET_HDR` must stay off:
    /// iproute2 does not set it, and Firecracker sets it itself when it opens
    /// the tap by name — setting it here would silently change guest offload
    /// behavior.
    #[test]
    fn tun_set_iff_request_matches_the_kernel_ifreq() {
        assert_eq!(std::mem::size_of::<TunSetIffRequest>(), 40);

        let request = tun_set_iff_request("tap0").expect("tap0 fits IFNAMSIZ");
        // `c_char` is `i8` on x86_64 and `u8` on aarch64, so this cast
        // reinterprets on one target and is a no-op on the other. clippy only
        // ever sees the target it is running for, and calls it redundant on
        // aarch64 -- which is why the gate was green on the x86_64 build host
        // and red on an arm64 one.
        #[allow(clippy::unnecessary_cast)]
        let name: Vec<u8> = request.name.iter().map(|byte| *byte as u8).collect();
        assert_eq!(&name[..4], b"tap0");
        assert!(
            name[4..].iter().all(|byte| *byte == 0),
            "the name must be NUL padded"
        );
        assert_eq!(
            request.flags,
            (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short
        );
        assert_eq!(
            request.flags as libc::c_int & libc::IFF_VNET_HDR,
            0,
            "IFF_VNET_HDR must not be set here"
        );

        assert!(tun_set_iff_request("").is_err());
        assert!(tun_set_iff_request("an-interface-name-far-too-long").is_err());
    }

    /// The in-process path either creates the device or is refused for want of
    /// CAP_NET_ADMIN — and the refusal has to be recognised as one, because
    /// that is what selects the `ip tuntap` fallback. A wrong request code
    /// would fail here as an unclassified error instead.
    #[test]
    fn creating_a_tap_device_is_either_done_here_or_refused_as_a_permission_error() {
        let name = "aenvtaptest0";
        match Slot::create_tap_via_ioctl(name) {
            Ok(index) => {
                assert!(index > 0, "a created device must have an index");
                let _ = Command::new("ip").args(["link", "del", name]).status();
            }
            Err(error) => assert!(
                is_permission_error(&error),
                "TUNSETIFF failed for a reason the fallback does not recognise: {error:#}"
            ),
        }
    }

    /// The netlink route and the `ip route` route have to be the same route.
    /// The defect this exists for is a builder that omits `RTA_OIF`: the
    /// command still succeeds, and the kernel resolves the gateway by lookup
    /// instead of being told the device.
    #[test]
    fn route_identity_notices_a_missing_output_interface() {
        let with_oif = rtnetlink::RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(Ipv4Addr::new(10, 11, 0, 3), 32)
            .gateway(Ipv4Addr::new(10, 12, 0, 7))
            .output_interface(9)
            .build();
        let same = rtnetlink::RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(Ipv4Addr::new(10, 11, 0, 3), 32)
            .gateway(Ipv4Addr::new(10, 12, 0, 7))
            .output_interface(9)
            .build();
        let without_oif = rtnetlink::RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(Ipv4Addr::new(10, 11, 0, 3), 32)
            .gateway(Ipv4Addr::new(10, 12, 0, 7))
            .build();

        assert_eq!(route_identity(&with_oif), route_identity(&same));
        assert_ne!(route_identity(&with_oif), route_identity(&without_oif));
        assert_eq!(route_identity(&with_oif).output_interface, Some(9));
        assert_eq!(route_identity(&without_oif).output_interface, None);
        assert_eq!(
            route_identity(&with_oif).destination,
            Some(IpAddr::V4(Ipv4Addr::new(10, 11, 0, 3)))
        );
    }

    fn test_slot(idx: u32, address_plan: NetworkAddressPlan) -> Result<Slot, NetworkError> {
        Slot::new(
            idx,
            address_plan,
            std::env::temp_dir().join("aenv-network-tests/netns"),
            EgressProxy::new(),
        )
    }

    fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn host_veth_exists(slot_idx: u32) -> bool {
        let veth_name = Slot::host_veth_name(slot_idx);
        command_stdout("ip", &["-o", "link", "show", &veth_name])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    /// Serialises the tests that build real slots.
    ///
    /// They pick an index by scanning the host for an unused `veth-N`, so two
    /// running at once pick the same one and collide in the kernel.
    static HOST_NETWORK_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn unused_test_slot() -> Slot {
        let address_plan = NetworkAddressPlan::default();
        (30_000..MAX_SLOTS as u32)
            .find(|idx| !host_veth_exists(*idx))
            .and_then(|idx| test_slot(idx, address_plan).ok())
            .expect("failed to find an unused high-numbered network test slot")
    }

    /// The name `Slot::new` stamps is what startup reads back: a leftover
    /// `veth-N` is attributed to a crashed run only through the namespace file
    /// that belonged to it. A bare UUID parses as nobody's slot, so the reaper
    /// never fires and `reserve_existing_host_veth_slots` burns that index for
    /// the life of the node.
    #[test]
    fn a_slots_namespace_name_names_its_owner_and_its_index() {
        let slot = test_slot(37, NetworkAddressPlan::default()).expect("slot 37 is in range");

        assert_eq!(
            super::super::classify_namespace_file(
                &slot.namespace_id,
                super::super::namespace_owner_id()
            ),
            super::super::StaleNamespace::Reap(37)
        );
    }

    #[test]
    fn test_slot_ip_calculation() {
        let address_plan = NetworkAddressPlan::default();

        // Test Slot 1
        let slot1 = test_slot(1, address_plan).expect("Slot 1 should be valid");
        assert_eq!(slot1.idx, 1);
        assert_eq!(slot1.host_interaction_ip.to_string(), "10.11.0.1");

        // Base 10.12.0.0. Offset 1*2 = 2.
        // Host: .2, VM: .3
        assert_eq!(slot1.veth_host_ip.to_string(), "10.12.0.2");
        assert_eq!(slot1.veth_vm_ip.to_string(), "10.12.0.3");

        // Test Slot 2
        let slot2 = test_slot(2, address_plan).expect("Slot 2 should be valid");
        assert_eq!(slot2.host_interaction_ip.to_string(), "10.11.0.2");
        // Offset 2*2 = 4. Host .4, VM .5
        assert_eq!(slot2.veth_host_ip.to_string(), "10.12.0.4");
        assert_eq!(slot2.veth_vm_ip.to_string(), "10.12.0.5");
    }

    #[test]
    fn custom_address_plan_calculates_slot_ips_and_boot_arg() {
        let config = crate::cfg::NetworkConfig {
            egress: crate::cfg::NetworkEgressConfig::default(),
            internal: crate::cfg::NetworkInternalConfig {
                host_interaction_cidr: "100.64.0.0/16".to_string(),
                veth_cidr: "100.65.0.0/16".to_string(),
            },
            iptables: crate::cfg::NetworkIptablesConfig::default(),
            slot: crate::cfg::NetworkSlotConfig::default(),
        };
        let address_plan = NetworkAddressPlan::from_config(&config).unwrap();
        let slot = test_slot(2, address_plan).expect("slot should be valid");

        assert_eq!(slot.host_interaction_ip.to_string(), "100.64.0.2");
        assert_eq!(slot.veth_host_ip.to_string(), "100.65.0.4");
        assert_eq!(slot.veth_vm_ip.to_string(), "100.65.0.5");
        assert!(slot
            .build_ip_boot_arg()
            .starts_with("ip=169.254.0.21::169.254.0.22:255.255.255.252:"));
    }

    #[test]
    fn test_slot_overflow() {
        let address_plan = NetworkAddressPlan::default();

        // Max valid index is 32767
        let max_valid = 32767;
        let slot = test_slot(max_valid, address_plan);
        assert!(slot.is_ok());

        // 32768 should fail
        let overflow = test_slot(32768, address_plan);
        assert!(overflow.is_err());
        match overflow {
            Err(NetworkError::SlotOutOfRange { idx, max }) => {
                assert_eq!(idx, 32768);
                assert_eq!(max, 32767);
            }
            _ => panic!("Expected SlotOutOfRange error"),
        }
    }

    #[test]
    fn parse_nameserver_ipv4_prefers_non_loopback_ipv4() {
        let conf = r#"
            # generated by systemd-resolved
            nameserver 127.0.0.53
            nameserver 10.0.0.2
            nameserver 8.8.8.8
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 0, 0, 2))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_ignores_non_ipv4_entries() {
        let conf = r#"
            nameserver ::1
            nameserver not_an_ip
            search example.com
        "#;
        assert_eq!(parse_nameserver_ipv4(conf), None);
    }

    #[test]
    fn parse_nameserver_ipv4_accepts_link_local_dns() {
        let conf = "nameserver 169.254.169.253\n";
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(169, 254, 169, 253))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_skips_malformed_nameserver_lines() {
        let conf = r#"
            nameserver
            nameserver 10.1.2.1
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 1, 2, 1))
        );
    }

    #[test]
    fn empty_egress_policy_skips_known_clean_slot() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        let empty_policy = SandboxNetworkPolicy::default();

        slot.set_egress_policy(None).unwrap();
        slot.set_egress_policy(Some(&empty_policy)).unwrap();

        assert!(!slot.user_egress_rules_present);
    }

    #[test]
    fn failed_egress_cleanup_keeps_slot_marked_dirty() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        slot.user_egress_rules_present = true;

        assert!(slot.set_egress_policy(None).is_err());

        assert!(slot.user_egress_rules_present);
    }

    #[test]
    fn failed_egress_apply_keeps_clean_slot_marked_clean() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        let policy = SandboxNetworkPolicy::new(
            true,
            crate::sandbox::network::BaseSandboxNetworkPolicy::Deny,
            crate::sandbox::network::SandboxNetworkEgressPolicy::default(),
        );

        assert!(slot.set_egress_policy(Some(&policy)).is_err());

        assert!(!slot.user_egress_rules_present);
    }

    /// The equivalence the netlink route path rests on: the route it installs
    /// and the route `ip route add` installs must be the same seven fields.
    /// Needs a real veth, so it runs only where the capabilities exist.
    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and affects system configuration"]
    fn the_netlink_route_and_the_ip_route_are_the_same_route() {
        crate::logging::init_for_tests();
        let _serialized = HOST_NETWORK_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut slot = unused_test_slot();
        slot.create_network().expect("create the slot network");

        let host_interaction_ip = slot.host_interaction_ip;
        let veth_vm_ip = slot.veth_vm_ip;
        let veth_name = Slot::host_veth_name(slot.idx);

        let identity_of = |ip: Ipv4Addr| -> Option<RouteIdentity> {
            run_on_host_netlink(move |handle| {
                Box::pin(async move {
                    let identities = route_identities(&handle).await?;
                    Ok(identities
                        .into_iter()
                        .find(|identity| identity.destination == Some(IpAddr::V4(ip))))
                })
            })
            .expect("dump routes")
        };

        let over_netlink = identity_of(host_interaction_ip)
            .expect("the netlink path should have installed the route");

        let deleted = Command::new("ip")
            .args([
                "route",
                "del",
                &format!("{host_interaction_ip}/32"),
                "via",
                &veth_vm_ip.to_string(),
                "dev",
                &veth_name,
            ])
            .status()
            .expect("delete the netlink-installed route");
        assert!(deleted.success(), "the route should have been removable");

        Slot::add_host_interaction_route_via_ip(&veth_name, host_interaction_ip, veth_vm_ip)
            .expect("install the same route through `ip route`");
        let over_ip = identity_of(host_interaction_ip)
            .expect("the `ip` path should have installed the route");

        assert_eq!(
            over_netlink, over_ip,
            "the two paths installed different routes"
        );
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and affects system configuration"]
    fn test_network_lifecycle() {
        crate::logging::init_for_tests();
        let _serialized = HOST_NETWORK_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Use a free high slot ID to avoid collisions with dev/prod and stale
        // devices from interrupted test runs.
        let mut slot = unused_test_slot();

        // 1. Create Network
        // This requires CAP_NET_ADMIN and CAP_SYS_ADMIN.
        match slot.create_network() {
            Ok(_) => {}
            Err(e) => {
                // If it fails due to permissions, we skip, otherwise fail
                let err_str = e.to_string();
                if err_str.contains("Operation not permitted") || err_str.contains("EPERM") {
                    println!("Skipping test due to lack of permissions");
                    return;
                }
                panic!("Failed to create network: {:?}", e);
            }
        }

        // 2. Verify Namespace File
        let netns_path = slot.namespace_path();
        assert!(
            netns_path.exists(),
            "Namespace file should exist after creation"
        );

        // 3. Verify Host Veth Interface
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let slot_idx = slot.idx;
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            let link = links.try_next().await.unwrap();
            assert!(
                link.is_some(),
                "{} should exist on host",
                Slot::host_veth_name(slot_idx)
            );
        });

        // 4. Cleanup
        let clean_res = slot.cleanup(false);
        assert!(clean_res.is_ok(), "cleanup should succeed");

        // 5. Verify Removal
        assert!(!netns_path.exists(), "Namespace file should be removed");
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            // ENODEV (-19) is returned when interface doesn't exist, which is expected
            let link = links.try_next().await.unwrap_or(None);
            assert!(
                link.is_none(),
                "{} should be gone",
                Slot::host_veth_name(slot_idx)
            );
        });
    }
}

#[cfg(test)]
mod host_netlink_tests {
    use super::*;

    /// The shared connection has to outlive whatever runtime first asked for a
    /// handle: a connection spawned onto a caller's runtime would already be
    /// dead by the second call, and would take every in-flight request on
    /// other threads down with it.
    /// A link dump, as a request that exercises the whole connection.
    fn dump_one_link() -> Result<()> {
        run_on_host_netlink(|handle| {
            Box::pin(async move {
                let mut links = handle.link().get().execute();
                links.try_next().await?;
                Ok(())
            })
        })
    }

    #[test]
    fn the_shared_connection_outlives_the_runtime_that_created_it() {
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("scratch runtime");
            // Dropped at the end of this block, exactly as a caller's runtime
            // is. Not tolerated as "netlink is unavailable here": AF_NETLINK
            // exists in every Linux network namespace and opening a socket
            // needs no privilege, so a failure means the connection is broken.
            runtime
                .block_on(async { dump_one_link() })
                .expect("the first request should be served");
        }

        dump_one_link().expect("the connection should still serve requests");
    }

    /// Every host-side operation used to spawn a thread and build a runtime to
    /// block on one netlink request. The work must now run on the thread that
    /// already owns the connection.
    #[test]
    fn host_netlink_work_runs_on_the_shared_connection_thread() {
        let thread_name = run_on_host_netlink(|_handle| {
            Box::pin(async move {
                Ok(thread::current()
                    .name()
                    .map(str::to_string)
                    .unwrap_or_default())
            })
        })
        .expect("the shared netlink worker should run the job");

        assert_eq!(
            thread_name, "agentenv-netlink",
            "the request ran on {thread_name:?} rather than the shared connection thread"
        );
    }

    /// A kernel that refuses the on-link /32 must not make the netlink worker
    /// fork: `run_with_scoped_capabilities` joins a thread, and this worker's
    /// current-thread runtime drives the connection for every slot on the node,
    /// so the job reports the fallback back to its caller instead of running
    /// it. Driven on the real shared worker, through the real production
    /// function, with the refusing-kernel answer supplied rather than latched.
    #[test]
    fn a_refusing_kernel_hands_the_shell_out_back_to_the_caller() {
        static REFUSES_THE_ROUTE: OnceLock<bool> = OnceLock::new();
        let _ = REFUSES_THE_ROUTE.set(false);

        let outcome = run_on_host_netlink(|handle| {
            Box::pin(async move {
                Slot::add_host_interaction_route(
                    &handle,
                    &REFUSES_THE_ROUTE,
                    // Never dereferenced: the refusal is decided before any
                    // message is built.
                    0,
                    "veth-none-here",
                    Ipv4Addr::new(169, 254, 0, 1),
                    Ipv4Addr::new(169, 254, 0, 2),
                )
                .await
            })
        })
        .expect("the job must return rather than shell out on the netlink worker");

        assert_eq!(outcome, HostRouteFallback::ShellOut);
    }

    /// Requests overlap: a refill batch builds several slots at once, and a
    /// worker that awaited each job in turn would serialize their host-side
    /// configuration behind one another.
    #[test]
    fn queued_netlink_jobs_overlap_rather_than_serializing() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        std::thread::scope(|scope| {
            let first = scope.spawn(move || {
                run_on_host_netlink(move |_handle| {
                    Box::pin(async move {
                        started_tx.send(()).expect("the first job announces itself");
                        let _ = release_rx.await;
                        Ok(())
                    })
                })
            });

            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("the first job should start");

            run_on_host_netlink(|_handle| Box::pin(async move { Ok(()) }))
                .expect("a second job must not wait behind the first");

            release_tx.send(()).expect("release the first job");
            first.join().expect("first job thread").expect("first job");
        });
    }
}
