---------------------------- MODULE MobilityClaim ----------------------------
(***************************************************************************)
(* The claim-and-lease protocol that decides which node runs a paused       *)
(* sandbox, in `src/orchestrator/mobility/`.                                *)
(*                                                                          *)
(* The property worth proving is small and absolute: never two live copies  *)
(* of one sandbox. Two guests that believe they are one both write the same *)
(* drives, and the divergence is unrecoverable by the time anyone notices.  *)
(*                                                                          *)
(* There is no consensus store here. Ownership is a wall-clock lease, so    *)
(* the two sides compare deadlines using clocks that need not agree, and    *)
(* the constants are chosen so the holder yields before the other side      *)
(* stops waiting. This spec exists to say exactly how much disagreement     *)
(* that tolerates.                                                          *)
(*                                                                          *)
(* The origin is modelled as just another claimant. An earlier version gave *)
(* it a separate "check the fence, then resume" path, and TLC found the     *)
(* obvious consequence in four steps: checking is not taking, so a          *)
(* destination can claim in the gap and both end up live. The origin        *)
(* therefore takes the claim like anyone else.                              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Origin,          \* the node holding the paused sandbox
    Destinations,    \* nodes that may take it
    TTL,             \* how long a claim stands without renewal
    AbandonMargin,   \* how much of the TTL the holder gives back
    ResumeGrace,     \* how long past expiry a rival waits before taking over
    MaxSkew,         \* bound on per-node clock offset
    MaxTime          \* state-space bound on the reference clock

Nodes == {Origin} \union Destinations

VARIABLES
    recState,    \* "parked" | "claimed" | "owned"
    recHolder,   \* who holds the claim, or who owns the sandbox
    recStamp,    \* when the holder last wrote, on the HOLDER's clock
    running,     \* nodes with a live guest
    restoring,   \* nodes part-way through bringing one up
    lastRenew,   \* per node: its own last successful write, on its own clock
    clock,       \* reference time, which no node can read
    skew         \* per-node offset from reference time

vars ==
    <<recState, recHolder, recStamp, running, restoring, lastRenew, clock, skew>>

\* What a node believes the time is. Nothing reads `clock` directly, which is
\* the point: the protocol only ever compares one node's reading of its own
\* clock against a timestamp written by another node's.
LocalTime(n) == clock + skew[n]

\* A rival waits out the TTL *and* the grace period before taking over, so a
\* renewal that was in flight when it looked lands inside the window.
ClaimBlocks(n) ==
    /\ recState = "claimed"
    /\ recHolder # n
    /\ LocalTime(n) < recStamp + TTL + ResumeGrace

\* The holder yields short of the TTL, so it stops before a rival starts.
\*
\* Measured against the holder's own last successful write, not against the
\* record: once a rival has written, the record's timestamp is the rival's and
\* says nothing about how long this holder has been out of contact. A holder
\* learns it was taken over only when a renewal fails, which is why it cannot
\* rely on noticing and must fall back on its own clock.
LeaseUnprovableTo(n) == LocalTime(n) >= lastRenew[n] + TTL - AbandonMargin

TypeOK ==
    /\ recState \in {"parked", "claimed", "owned"}
    /\ recHolder \in Nodes
    /\ recStamp \in Int
    /\ running \subseteq Nodes
    /\ restoring \subseteq Nodes
    /\ running \intersect restoring = {}
    /\ lastRenew \in [Nodes -> Int]
    /\ clock \in 0..MaxTime
    /\ skew \in [Nodes -> 0..MaxSkew]

Init ==
    /\ recState = "parked"
    /\ recHolder = Origin
    /\ recStamp = 0
    /\ running = {}
    /\ restoring = {}
    /\ lastRenew = [n \in Nodes |-> 0]
    /\ clock = 0
    /\ skew \in [Nodes -> 0..MaxSkew]

Tick ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ UNCHANGED <<recState, recHolder, recStamp, running, restoring, lastRenew, skew>>

(***************************************************************************)
(* The timing assumption, stated rather than assumed away.                  *)
(*                                                                          *)
(* A holder past its abandon deadline is modelled as stopping instantly:    *)
(* while any holder owes an abandon, nothing else happens — no clock tick,  *)
(* no rival claim, no restore completing. That is not free. It is a real    *)
(* obligation on the implementation: the guardian must tear the guest down  *)
(* in less time than the margin covers. The margin exists to pay for it.    *)
(*                                                                          *)
(* Without this the spec cannot hold at any skew, because "the holder may   *)
(* abandon" permits behaviours where it simply never does.                  *)
(***************************************************************************)
MustAbandon(n) ==
    /\ n \in restoring \union running
    \* An owner holds the sandbox outright and is not leasing anything.
    /\ ~(recState = "owned" /\ recHolder = n)
    /\ LeaseUnprovableTo(n)

PendingAbandon == \E n \in Nodes : MustAbandon(n)

(***************************************************************************)
(* Claiming, which is also renewing: a claimant that timed out on its own   *)
(* earlier attempt cannot tell whether it landed, so the two are one call.  *)
(*                                                                          *)
(* A sandbox that is "owned" is running somewhere and is not up for grabs.  *)
(***************************************************************************)
Claim(n) ==
    /\ recState # "owned"
    /\ n \notin running
    /\ ~ClaimBlocks(n)
    /\ recState' = "claimed"
    /\ recHolder' = n
    /\ recStamp' = LocalTime(n)
    /\ lastRenew' = [lastRenew EXCEPT ![n] = LocalTime(n)]
    /\ restoring' = restoring \union {n}
    /\ UNCHANGED <<running, clock, skew>>

Renew(n) ==
    /\ n \in restoring \union running
    /\ recState = "claimed"
    /\ recHolder = n
    /\ recStamp' = LocalTime(n)
    /\ lastRenew' = [lastRenew EXCEPT ![n] = LocalTime(n)]
    /\ UNCHANGED <<recState, recHolder, running, restoring, clock, skew>>

(***************************************************************************)
(* The guardian. A holder that has been taken over, or that cannot prove it *)
(* still owns the lease, stops at once — including tearing down a guest it  *)
(* had already brought up.                                                  *)
(***************************************************************************)
Abandon(n) ==
    /\ n \in restoring \union running
    /\ ~(recState = "owned" /\ recHolder = n)
    \* Either the deadline passed, or a renewal came back saying the record
    \* names someone else. The second is a discovery, not a guarantee: a
    \* holder that cannot reach the store never makes it.
    /\ \/ LeaseUnprovableTo(n)
       \/ recState # "claimed"
       \/ recHolder # n
    /\ restoring' = restoring \ {n}
    /\ running' = running \ {n}
    /\ UNCHANGED <<recState, recHolder, recStamp, lastRenew, clock, skew>>

\* The guest is live, before the record says so. This gap is why the lease
\* has to stay renewed across the commit rather than being released here.
RestoreDone(n) ==
    /\ n \in restoring
    /\ recState = "claimed"
    /\ recHolder = n
    /\ running' = running \union {n}
    /\ restoring' = restoring \ {n}
    /\ UNCHANGED <<recState, recHolder, recStamp, lastRenew, clock, skew>>

\* The point of no return.
Commit(n) ==
    /\ n \in running
    /\ recState = "claimed"
    /\ recHolder = n
    /\ recState' = "owned"
    /\ recHolder' = n
    /\ recStamp' = LocalTime(n)
    /\ lastRenew' = [lastRenew EXCEPT ![n] = LocalTime(n)]
    /\ UNCHANGED <<running, restoring, clock, skew>>

\* Giving up before anything was brought up.
Release(n) ==
    /\ n \in restoring
    /\ recState = "claimed"
    /\ recHolder = n
    /\ recState' = "parked"
    /\ recStamp' = LocalTime(n)
    /\ restoring' = restoring \ {n}
    /\ UNCHANGED <<recHolder, running, lastRenew, clock, skew>>

\* The owner pauses again, putting the sandbox back up for grabs.
Repark(n) ==
    /\ recState = "owned"
    /\ recHolder = n
    /\ n \in running
    /\ recState' = "parked"
    /\ recStamp' = LocalTime(n)
    /\ running' = running \ {n}
    /\ UNCHANGED <<recHolder, restoring, lastRenew, clock, skew>>

Next ==
    \/ /\ PendingAbandon
       /\ \E n \in Nodes : Abandon(n)
    \/ /\ ~PendingAbandon
       /\ \/ Tick
          \/ \E n \in Nodes :
               Claim(n) \/ Renew(n) \/ RestoreDone(n)
                 \/ Commit(n) \/ Release(n) \/ Repark(n)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The one property that matters.                                          *)
(***************************************************************************)
AtMostOneLiveCopy == Cardinality(running) <= 1

\* A live guest is one the record names. Catches a guest that outlived its
\* claim in a run where `AtMostOneLiveCopy` happens not to notice.
\*
\* Evaluated only when nobody owes an abandon, because the instant between a
\* rival claiming and the previous holder tearing down is a state the real
\* system passes through too.
OnlyTheHolderRuns ==
    PendingAbandon \/ \A n \in running : recHolder = n

=============================================================================
