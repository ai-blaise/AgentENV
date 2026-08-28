# Specifications

Formal models of the protocols where getting it wrong is not recoverable by
retrying. Each one is model-checked, and the results below are from actual
runs, not from reading the spec.

## `MobilityClaim.tla`

The claim-and-lease protocol in `src/orchestrator/mobility/`, which decides
which node runs a paused sandbox during a handover. The property is that two
nodes never both run one sandbox: two guests that believe they are one write
the same drives, and by the time anyone notices, the divergence cannot be
undone.

There is no consensus store behind this. Ownership is a wall-clock lease, so
the two sides compare deadlines using clocks that need not agree, and the
constants exist to make the holder yield before a rival starts. The model says
exactly how much disagreement that buys.

### Result

Two live copies are impossible **iff clock disagreement between any two nodes
stays within `abandon_margin + takeover_grace`**. Checked exhaustively over one
origin, two destinations, and a bounded clock:

| abandon margin | takeover grace | max skew | outcome |
| --- | --- | --- | --- |
| 1 | 1 | 0 | no error (13,494 states) |
| 1 | 1 | 1 | no error (132,792 states) |
| 1 | 1 | 2 | no error (478,692 states) |
| 1 | 1 | 3 | `AtMostOneLiveCopy` violated |
| 2 | 1 | 3 | no error |
| 2 | 1 | 4 | `AtMostOneLiveCopy` violated |

The bound tracks the sum rather than either constant alone, which is why both
appear in the implementation and why neither can be tuned on its own.

In the shipped configuration — a 30s TTL, a 10s abandon margin, and a 15s
takeover grace — that is 25 seconds of tolerable skew, against the sub-second
skew NTP-managed hosts hold. A fleet without time synchronisation is outside
what this protocol can promise.

### What the model assumes

A holder past its abandon deadline stops **before anything else happens** —
no clock tick, no rival claim, no restore completing. That is an obligation on
the implementation, not a convenience: the guardian must tear the guest down in
less time than the margin covers. It is stated in the spec rather than hidden,
because without it no skew bound holds at all — "the holder may abandon"
permits behaviours where it never does.

### What it found

Two defects, both fixed:

- **Reading the fence is not taking it.** The origin used to check whether a
  sandbox was claimed and then resume it, and TLC produced a four-step trace
  where a destination claims in the gap and both nodes end up live. The origin
  now takes the claim like every other node.
- **The lease has to outlive the restore.** The saga released its guardian when
  the restore returned, leaving the claim unrenewed across the commit; a slow
  commit then let a rival take over a sandbox that was already running. The
  guardian is now released after the commit lands.

### Running it

`tla2tools.jar` from the [TLA+ releases](https://github.com/tlaplus/tlaplus/releases):

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -config MobilityClaim.cfg -workers 8 MobilityClaim.tla
```

`MobilityClaimSkewed.cfg` is the same model with skew past the bound, and is
expected to fail — it is there so a change that silently widens the tolerance
is noticed.
