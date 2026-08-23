Chapter 13

Advanced feature note: Enterprise EC reclaim material describes work under
development and validation, not a public v1.0 support claim. See
[Feature Status](../../../feature-status.md).

# Zero, Discard, And Reclaim

## Operations

- zero
- discard / UNMAP
- WRITE_ZEROES
- DISCARD / fstrim

<div class="summary" markdown="1">

Zero and discard both make future reads return zeroes, but they are different product operations. Zero guarantees logical zero content. Discard/UNMAP also tells the backend that the old physical backing is no longer needed by the live view and can become protected or reclaimable according to reachability.

True discard preserves read-after-zero, protects snapshot/clone read views, detaches old backing from the live allocation view, and exposes reclaim or reclaimable accounting.

</div>

## Operation Identity

| Operation | Logical Read Result | Reclaim Meaning |
|----|----|----|
| `zero` | Future reads return zeroes. | Physical reclaim is optional. |
| `discard` / UNMAP | Future live reads return zeroes. | Old live backing is detached and may become reclaimable. |
| Kernel `WRITE_ZEROES` | Maps to zero semantics. | Does not by itself advertise reclaim. |
| Kernel `DISCARD` / `fstrim` | Maps to discard semantics. | Requires true reclaim path and observability. |

<div class="diagram" markdown="1">

<div class="diagram-title">Observable Zero Fallback</div>

<div class="flow" markdown="1">

<div class="box-accent">DISCARD request</div>

<div class="arrow">if reclaim-aligned</div>

<div class="box">detach live backing</div>

<div class="arrow">else</div>

<div class="box-soft">complete as zero fallback</div>

</div>

</div>

## Policy Decision

The preferred architecture for partial or unaligned discard is observable zero fallback. If the requested range cannot safely detach backend objects at the reclaim geometry, NAMRBD should still make future live reads return zeroes when the caller's contract allows it, but it must report the operation as `policy=zero_fallback`, leave `reclaimable_bytes=0`, and avoid claiming true backend reclaim. Reject remains available for an edition, backend, or caller contract that cannot accept zero fallback.

<div class="diagram" markdown="1">

<div class="diagram-title">Aligned discard result</div>

<div class="flow" markdown="1">

<div class="box-accent">live AllocationEntry points at object A</div>

<div class="arrow">discard</div>

<div class="box">live AllocationEntry becomes zero</div>

<div class="arrow">old ref</div>

<div class="box-soft">object A protected or reclaimable</div>

</div>

</div>

## Replicated And EC Discard <span class="edition-boundary-inline">Includes Enterprise edition only EC reclaim</span>

For reclaim-aligned replicated ranges, discard can publish a zero live view and retire old replicated backing objects. For EC ranges, full-stripe/page aligned discard can publish a metadata-only zero view and retire old EC PhysicalObjectRefs. In both cases, snapshot-before-discard reads must continue to see old data. A path should not be described as reclaim unless old backing refs are detached from the live view.

## Observability

| Field | Question Answered |
|----|----|
| `operation` | Was this zero or discard? |
| `policy` | Was it true reclaim, zero fallback, or partial reject? |
| `aligned_to_reclaim_geometry` | Was the requested range eligible for reclaim? |
| `discard_bytes` / `logical_zero_bytes` | How much logical space was affected? |
| `reclaimable_bytes` / `reclaimed_bytes` | Did storage become reclaimable or reclaimed? |
| `protected_reference_check_passed` | Were snapshot, clone, Backup/DR, governance, maintenance, and backend-specific references checked before claiming reclaim? |
| `before_free_bytes` / `after_free_bytes` | Did node-local `sbs-data` free-byte evidence prove physical space was returned? |
| `completed_claimed` / `evidence_required` | Is the report explicitly avoiding a physical reclaim claim until the required evidence exists? |

Partial or unaligned requests should be observable as zero fallback when supported: reads return zeroes, snapshot-before-discard reads remain protected, and reclaim counters stay unchanged. The report must make the fallback visible so operators do not mistake logical zeroing for physical reclamation.

Operations APIs and dashboard panels should preserve that distinction: `policy=zero_fallback` is a successful logical zero outcome, not proof that backing objects were detached or that node-local free bytes increased.

[\<- Previous](11-reachability-and-gc.md) [Next: Kernel-Gateway Dataplane -\>](13-kernel-gateway-dataplane.md)
