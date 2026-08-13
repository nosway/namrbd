Chapter 11

# Read Views, Snapshots, And Clones

## Read Views

- Live volume
- Snapshot root
- Clone delta
- Materialized clone

<div class="summary" markdown="1">

A read view is an explicit identity for resolving logical ranges. Live volumes, snapshots, clones, and materialized clones resolve through different roots. `VolumeRevision` can be useful in logs, but it is not the identity for snapshot or clone reads.

Snapshot and clone reads use explicit read-view roots. Parent overwrites, discard, expansion, or later live metadata commits do not change an already captured snapshot root.

</div>

<figure class="architecture-figure" markdown="1">

![Read view lifecycle diagram for live volumes, snapshots, clones, and materialization](../assets/diagrams/read-view-lifecycle.svg)

<figcaption>Shared English SVG for read-view lifecycle. Snapshot roots, clone deltas, materialization, flatten, and GC protection are shown as one lineage.</figcaption>

</figure>

## Read-View Types

| Read View | Identity | Resolution Behavior |
|----|----|----|
| Live volume | `volume_id` plus current committed metadata. | Resolve from live allocation pages. |
| Snapshot | `snapshot_id` and `snapshot_root_id`. | Resolve from immutable captured snapshot root pages. |
| Clone | `clone_id`, base root, and clone delta. | Resolve clone delta first, then base snapshot root, then zero. |
| Materialized clone | Independent volume-like identity. | Resolve from independent allocation pages. |

<div class="diagram" markdown="1">

<div class="diagram-title">Clone read resolution</div>

<div class="flow" markdown="1">

<div class="box-accent">logical read</div>

<div class="arrow">-\></div>

<div class="box">clone delta</div>

<div class="arrow">else</div>

<div class="box">base snapshot root</div>

<div class="arrow">else</div>

<div class="box-soft">zero range</div>

</div>

</div>

## Snapshot Cut Point

Snapshot create captures committed allocation metadata at the cut point. Writes committed before the cut are visible in the snapshot; writes committed after the cut are not. The returned `snapshot_root_id` is the authority for future snapshot reads.

## Clone Writes

Clone writes allocate new Physical Objects and publish clone delta metadata. They do not mutate the source snapshot root and do not mutate the source volume. This is copy-on-write at the logical mapping layer and append-only at the physical payload layer.

## Materialization

Materialization produces an independent volume-like mapping. The source snapshot reference is released only after the independent mapping is durable and readable without depending on the clone base read-view.

## Clone, Materialize, And Flatten Lifecycle

| Stage | Metadata Shape | Read Behavior | Release Boundary |
|----|----|----|----|
| Snapshot capture | Persist `snapshot_id` and immutable `snapshot_root_id`. | Snapshot reads resolve only through the captured root. | The source volume can keep writing without changing the snapshot root. |
| Clone-like view | Create `clone_id`, base snapshot ref, and empty or sparse clone delta pages. | Reads check clone delta first, then base snapshot root, then zero. | The base snapshot stays protected while the clone can fall back to it. |
| Clone writes | Publish clone-owned AllocationEntries for changed ranges. | Changed ranges read clone objects; unchanged ranges read the base root. | Old source and snapshot objects are not overwritten in place. |
| Materialize | Copy or re-reference the resolved view into independent volume allocation pages. | The target can be read without consulting the clone base after commit. | Base refs are released only after independent mapping is durable and verified. |
| Flatten | Convert remaining fallback ranges into target-owned mappings and drop base dependency. | Reads become volume-local even for ranges that originally fell back to the snapshot. | GC may reclaim former base-only objects only after all other roots are gone. |

[\<- Previous](09-write-visibility-and-ordering.md) [Next: Reachability And GC -\>](11-reachability-and-gc.md)
