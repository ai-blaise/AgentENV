# Network slot creation baseline

Measured on a 224-core Linux host with
`sandbox::network::manager::tests::network_slot_creation_throughput`:

```bash
sudo -E cargo test -p agentenv --lib network_slot_creation_throughput -- --ignored --nocapture
```

Each round creates 32 fresh slots at the given fill concurrency and tears them
down again.

| Fill concurrency | Slots/sec | Per slot | Failures |
|-----------------:|----------:|---------:|---------:|
| 1 | 23.4 | 42.8 ms | 0 |
| 2 | 31.4 | 31.8 ms | 0 |
| 4 | 37.5 | 26.7 ms | 0 |
| 8 | 39.6 | 25.2 ms | 0 |

## What this settles

**Slot creation is the per-node cold-create ceiling.** A single slot costs
~43 ms serially. For comparison, placement after bounded candidate selection
costs 190 µs at 10,000 nodes — three orders of magnitude less. Control-plane
work is not what limits creates per node.

**Concurrency helps, and then stops helping.** Going from 1 to 8 concurrent
builds yields 1.7×, not 8×, and almost all of that is reached by 4
(37.5 → 39.6 from 4 to 8). The work is serialized in the kernel, which is what
a dozen RTNL-mutating netlink operations per slot — two of them holding RTNL
across a `synchronize_net()` — predicts. This is the measurement behind the
`[pool.network].fill_concurrency` default of 4: past that, the extra threads
contend rather than help.

**A pre-created slot bank is justified, not speculative.** Since the path is
kernel-serialized, no amount of concurrency raises the ~40 slots/sec ceiling.
The only way past it is to keep slots off the create path entirely, which is
what a deep warm bank does. Reducing the non-RTNL cost — reusing the netlink
socket rather than opening three per slot, replacing the `ip tuntap` and
`ip route` fork/execs — shaves the remaining per-slot constant but does not
move the ceiling.

**Concurrent setup no longer fails.** Zero failures at concurrency 8. Before
`iptables-restore` was given `--wait`, concurrent slot setup failed outright on
the xtables lock rather than queueing, and the pool refill loop abandoned its
fill on the first such error.
