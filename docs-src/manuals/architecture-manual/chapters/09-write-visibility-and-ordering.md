Chapter 10

# Write Visibility And Ordering

## Visibility

Payload persistence is not visible data until metadata commits a new AllocationEntry.

<div class="summary" markdown="1">

NAMRBD treats metadata commit as the point where logical contents become visible. Physical payload can be persisted before the commit, but readers observe new data only after a committed AllocationEntry points at the new PhysicalObjectRef.

Visible contents change by metadata swap, not by in-place overwrite of a reachable Physical Object. If payload write succeeds but metadata commit fails, the payload object is cleanup work, not visible volume data.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Write Visibility Point</div>

<div class="flow" markdown="1">

<div class="box-accent">persist payload object</div>

<div class="arrow">-\></div>

<div class="box">prepare PhysicalObjectRef</div>

<div class="arrow">-\></div>

<div class="box">commit AllocationEntry</div>

<div class="arrow">-\></div>

<div class="box-soft">new read view observes data</div>

</div>

</div>

## Append-Only Physical Rule

A write that changes logical content should create a new physical object or object generation, then publish metadata that points to it. Old PhysicalObjectRefs remain protected until reachability proves no live, snapshot, clone, materialize, delete, rebuild, scrub, or pending operation root references them.

## Ordering Scopes

| Scope | Guarantee | Review Note |
|----|----|----|
| One kernel-gateway path connection | FIFO submission and completion ordering at the default one-outstanding policy. | Connection-local ordering is not cluster-wide ordering. |
| Same logical range | Visible order follows committed SBS metadata order. | Metadata commit is the read-after-write authority. |
| Different gateways | Concurrent streams can exist for the same volume. | Fresh committed reads rely on SBS metadata visibility and fencing rules. |
| Guarded performance modes | May explore weaker or different latency tradeoffs only with explicit validation. | They are not the baseline correctness rule. |

## FLUSH And FUA

FLUSH/FUA review should ask which completion point is being acknowledged: payload persistence, metadata commit, or a stronger durability boundary. The first correctness question is always whether a subsequent committed read sees the expected logical content. Same-range visibility follows SBS metadata commit order, while guarded performance modes remain separate from the correctness baseline until validated.

## Guarded Performance Mode Warning

Guarded performance modes are allowed to warn when they intentionally explore a weaker or less-proven completion boundary. The warning must say which baseline rule is being relaxed, but it must not silently redefine product correctness. A guarded mode completion is acceptable only when the harness records the active mode, the acknowledged boundary, and the read-after-write evidence used for that run.

| Mode Signal | Required Observable | Do Not Claim |
|----|----|----|
| Relaxed outstanding or batching behavior | Mode name, path count, outstanding limit, FLUSH/FUA behavior, and same-range read-after-write result. | Cluster-wide ordering unless the SBS metadata commit evidence proves it. |
| Payload-before-metadata acknowledgement experiment | Explicit warning plus later metadata commit or failure cleanup evidence. | Visible durability before AllocationEntry commit. |
| Cross-gateway performance path | Gateway ids, attachment generations, fencing state, first/last error, and committed read verification. | That connection-local FIFO provides multi-gateway ordering. |

[\<- Previous](08-erasure-coding-backend.md) [Next: Read Views -\>](10-read-views-snapshots-and-clones.md)
