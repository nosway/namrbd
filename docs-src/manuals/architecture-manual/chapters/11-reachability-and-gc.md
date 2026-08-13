Chapter 12

# Reachability And GC

## GC Roots

- Live allocation pages
- Snapshot roots
- Clone deltas
- Pending operations

<div class="summary" markdown="1">

Payload objects remain live while any authoritative metadata root references their PhysicalObjectRef. GC is therefore a metadata reachability problem first, and a backend delete operation second.

Reachability roots win over counters and local store inventory. `sbs-service` owns global reachability; `sbs-data` executes local deletes when instructed.

</div>

## Reachability Roots

| Root | Why It Protects Objects |
|----|----|
| Live volume allocation pages | Current committed live reads may resolve to these objects. |
| Snapshot root pages | Immutable snapshot reads depend on captured mappings. |
| Clone base snapshot refs | Clone reads may fall back to the base root. |
| Clone delta pages | Clone-owned writes reference separate objects. |
| Pending operations | Materialize, delete, rebuild, scrub, and backend operations may hold temporary roots. |

<div class="diagram" markdown="1">

<div class="diagram-title">Reachability Mark And Sweep</div>

<div class="flow" markdown="1">

<div class="box-accent">enumerate roots</div>

<div class="arrow">-\></div>

<div class="box">build live PhysicalObjectRef set</div>

<div class="arrow">-\></div>

<div class="box">compare candidates</div>

<div class="arrow">-\></div>

<div class="box-soft">delete by backend type</div>

</div>

</div>

## Protected And Reclaimable

A detached object can be protected if a snapshot, clone, or pending operation still references it. It becomes reclaimable only after authoritative roots no longer include it and the safety window has passed. The backend descriptor then tells the delete implementation whether it is deleting replicated chunks or EC shards.

Deleting a volume removes the live volume authority, but it is not by itself proof that node-local physical space was returned. Reclaim evidence must include protected-reference status plus `sbs-data` before/after free-byte observations. If snapshots, clones, Backup/DR artifacts, governance holds, dedupe/repack roots, EC repair work, or maintenance operations are present or unknown, the reclaim view must report blocked, skipped, or evidence-required state instead of claiming completed reclaim.

The `/api/v1/sbs/reclaim` view and dashboard reclaim panels are reporting surfaces only. They can explain why reclaim is blocked or evidence is incomplete, but they must not turn a volume delete, logical zero, or local inventory difference into a physical-space-return claim without the required before/after evidence.

## Why Local Inventory Is Not Enough

A local `sbs-data` node can see its payload objects, but it cannot know whether a snapshot root or clone delta elsewhere still references them. Global reachability is a cluster metadata question.

## Maintenance Holds

Maintenance operations can protect objects even when the live allocation page no longer points at them. The hold must be explicit in SBS metadata so a resumed operation and GC reach the same answer after process restart.

| Operation | Temporary Root | Release Condition |
|----|----|----|
| Materialize / flatten | Source snapshot root and any copied or re-referenced PhysicalObjectRefs. | Target mapping is durable, verified, and no longer needs base fallback. |
| Rebuild | Old shard refs and replacement shard refs until commit is complete. | Replacement descriptor is published or operation is safely rolled back. |
| Scrub | Objects under verification and repair candidates. | Verification completes or repair hands ownership to rebuild metadata. |
| Rebalance / drain | Source and target backend refs while data is copied and validated. | Placement transition publishes the replacement descriptor and old refs become normal GC candidates. |

## GC State Machine

| State | Meaning | Next Step |
|----|----|----|
| `candidate` | A backend object exists in inventory or retired-ref metadata. | Compare against authoritative roots. |
| `protected` | At least one live, snapshot, clone, materialize, rebuild, scrub, rebalance, drain, delete, or pending operation root references it. | Keep object and record the protecting root class. |
| `reclaimable` | No authoritative root references the object and the safety window has passed. | Dispatch backend-specific delete for replicated chunks or EC shards. |
| `deleted` | All backend deletes completed and metadata no longer needs the candidate record. | Keep compact audit evidence or remove transient state. |

[\<- Previous](10-read-views-snapshots-and-clones.md) [Next: Zero, Discard, And Reclaim -\>](12-zero-discard-and-reclaim.md)
