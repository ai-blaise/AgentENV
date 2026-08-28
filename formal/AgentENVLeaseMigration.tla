---------------------- MODULE AgentENVLeaseMigration ----------------------
EXTENDS Integers, FiniteSets, TLC

(***************************************************************************)
(* Original executable model of AgentENV's route lease and migration       *)
(* fencing protocol. A prepared destination is inert. Cutover first        *)
(* quiesces the source, then advances the durable generation, then permits  *)
(* the destination to execute.                                               *)
(***************************************************************************)

CONSTANTS Nodes, NoNode, MaxGeneration
ASSUME NoNode \notin Nodes
ASSUME MaxGeneration \in Nat \ {0}

Phases == {"active", "preparing", "quiesced", "committed"}

VARIABLES owner, generation, phase, source, destination,
          executing, runtimeGeneration, leaseValid, durableCoverage

vars == <<owner, generation, phase, source, destination,
          executing, runtimeGeneration, leaseValid, durableCoverage>>

Init ==
    /\ owner \in Nodes
    /\ generation = 1
    /\ phase = "active"
    /\ source = owner
    /\ destination = NoNode
    /\ executing = [node \in Nodes |-> node = owner]
    /\ runtimeGeneration = [node \in Nodes |-> IF node = owner THEN 1 ELSE 0]
    /\ leaseValid = TRUE
    /\ durableCoverage = TRUE

BeginMigration(destinationNode) ==
    /\ phase = "active"
    /\ leaseValid
    /\ generation < MaxGeneration
    /\ destinationNode \in Nodes \ {owner}
    /\ phase' = "preparing"
    /\ source' = owner
    /\ destination' = destinationNode
    /\ UNCHANGED <<owner, generation, executing, runtimeGeneration,
                    leaseValid, durableCoverage>>

PrepareDestination ==
    /\ phase = "preparing"
    /\ destination \in Nodes
    /\ runtimeGeneration' = [runtimeGeneration EXCEPT
                                ![destination] = generation + 1]
    /\ UNCHANGED <<owner, generation, phase, source, destination,
                    executing, leaseValid, durableCoverage>>

EstablishDurableCoverage ==
    /\ phase \in {"preparing", "quiesced"}
    /\ durableCoverage' = TRUE
    /\ UNCHANGED <<owner, generation, phase, source, destination,
                    executing, runtimeGeneration, leaseValid>>

QuiesceSource ==
    /\ phase = "preparing"
    /\ leaseValid
    /\ durableCoverage
    /\ ~executing[destination]
    /\ phase' = "quiesced"
    /\ executing' = [executing EXCEPT ![source] = FALSE]
    /\ UNCHANGED <<owner, generation, source, destination,
                    runtimeGeneration, leaseValid, durableCoverage>>

CommitOwnership ==
    /\ phase = "quiesced"
    /\ ~executing[source]
    /\ runtimeGeneration[destination] = generation + 1
    /\ owner' = destination
    /\ generation' = generation + 1
    /\ phase' = "committed"
    /\ leaseValid' = TRUE
    /\ UNCHANGED <<source, destination, executing, runtimeGeneration,
                    durableCoverage>>

ActivateDestination ==
    /\ phase = "committed"
    /\ leaseValid
    /\ runtimeGeneration[owner] = generation
    /\ executing' = [executing EXCEPT ![owner] = TRUE]
    /\ phase' = "active"
    /\ source' = owner
    /\ destination' = NoNode
    /\ UNCHANGED <<owner, generation, runtimeGeneration,
                    leaseValid, durableCoverage>>

AbortBeforeCommit ==
    /\ phase \in {"preparing", "quiesced"}
    /\ owner = source
    /\ leaseValid
    /\ executing' = [executing EXCEPT ![source] = TRUE]
    /\ phase' = "active"
    /\ destination' = NoNode
    /\ UNCHANGED <<owner, generation, source, runtimeGeneration,
                    leaseValid, durableCoverage>>

LoseDurableCoverage ==
    /\ phase = "preparing"
    /\ durableCoverage' = FALSE
    /\ UNCHANGED <<owner, generation, phase, source, destination,
                    executing, runtimeGeneration, leaseValid>>

ExpireLease ==
    /\ leaseValid
    /\ leaseValid' = FALSE
    /\ executing' = [executing EXCEPT ![owner] = FALSE]
    /\ UNCHANGED <<owner, generation, phase, source, destination,
                    runtimeGeneration, durableCoverage>>

RenewLease ==
    /\ leaseValid
    /\ leaseValid' = TRUE
    /\ UNCHANGED <<owner, generation, phase, source, destination,
                    executing, runtimeGeneration, durableCoverage>>

StaleActivation(node) ==
    /\ node \in Nodes
    /\ runtimeGeneration[node] < generation
    /\ UNCHANGED vars

Next ==
    \/ \E node \in Nodes: BeginMigration(node)
    \/ PrepareDestination
    \/ EstablishDurableCoverage
    \/ QuiesceSource
    \/ CommitOwnership
    \/ ActivateDestination
    \/ AbortBeforeCommit
    \/ LoseDurableCoverage
    \/ ExpireLease
    \/ RenewLease
    \/ \E node \in Nodes: StaleActivation(node)

TypeOK ==
    /\ owner \in Nodes
    /\ generation \in 1..MaxGeneration
    /\ phase \in Phases
    /\ source \in Nodes
    /\ destination \in Nodes \cup {NoNode}
    /\ executing \in [Nodes -> BOOLEAN]
    /\ runtimeGeneration \in [Nodes -> Nat]
    /\ leaseValid \in BOOLEAN
    /\ durableCoverage \in BOOLEAN

AtMostOneExecuting == Cardinality({node \in Nodes: executing[node]}) <= 1

ExecutingOwnerIsCurrent ==
    \A node \in Nodes:
        executing[node] =>
            /\ node = owner
            /\ runtimeGeneration[node] = generation
            /\ leaseValid

PreparedDestinationIsFenced ==
    phase \in {"preparing", "quiesced"} => ~executing[destination]

NoCleanupBeforeCoverage ==
    phase = "quiesced" => durableCoverage

Safety ==
    /\ TypeOK
    /\ AtMostOneExecuting
    /\ ExecutingOwnerIsCurrent
    /\ PreparedDestinationIsFenced
    /\ NoCleanupBeforeCoverage

Spec == Init /\ [][Next]_vars

=============================================================================
