Chapter 6

# Logical Storage Geometry

## Geometry

- Volume size
- Allocation Page
- Allocation Chunk
- Placement Extent

<div class="summary" markdown="1">

Logical Storage Geometry describes the volume address space before NAMRBD chooses a replicated or erasure-coded backend layout. It is deliberately backend-independent: the same Volume byte range, Allocation Pages, Allocation Chunks, and Placement Extents exist whether the payload is later stored as replica chunks or EC shards.

Allocation geometry is part of a volume's stable shape. It defines how logical ranges are named, paged, aligned, and planned. Chapter 7 then explains how those logical units become AllocationEntries that point to backend-neutral PhysicalObjectRefs.

</div>

<figure class="architecture-figure" markdown="1">

![Logical storage geometry layout showing volume extents, allocation chunks, ZERO and DATA entries, and a 4 KiB boundary split](../assets/diagrams/logical-storage-geometry-layout.svg)

<figcaption>Logical geometry separates placement planning from allocation state. Placement Extents choose routing and failure-domain policy, while AllocationEntries decide whether each logical chunk is ZERO or DATA.</figcaption>

</figure>

## What Is Logical Here

The units in this chapter are not local disk files, Pebble keys, replica chunks, EC stripes, or EC shards. They are stable names for ranges in the logical volume. They let control-plane and metadata code decide which range is being updated, which allocation metadata page owns the entry, and which placement planning range applies, without knowing the final backend payload layout.

| Logical Unit | Relationship | Not The Same As |
|----|----|----|
| Volume byte range | The full logical block device address space exported to the host. | A preallocated set of physical bytes. |
| Allocation Page | A metadata container for AllocationEntries covering a contiguous logical range. | A memory page, disk page, or backend payload object. |
| Allocation Chunk | The logical unit that an AllocationEntry maps to zero state or a PhysicalObjectRef. | A replicated physical chunk or an EC shard. |
| Placement Extent | A planning range used to choose placement, replica set, failure-domain spread, or shard placement constraints. | The physical object itself. It does not store payload bytes. |

## Geometry Terms

| Term | Meaning | Why It Matters |
|----|----|----|
| Allocation Chunk | Logical allocation unit in the volume address space. | Read/write/discard mapping decisions are expressed over logical chunks. |
| Allocation Page | Metadata page that contains allocation entries for a logical range. | Metadata operations can update page-sized ownership domains. |
| Placement Extent | Planning unit for replica set or failure-domain assignment. | Placement can be reasoned about without confusing it with physical chunk IDs. |
| Physical Object (handoff term) | Backend-neutral persisted payload object introduced after logical mapping. | It marks where this chapter stops and Chapter 7 starts. |

## Small Geometry Example

For example, if a 1 GiB volume uses 4 MiB Allocation Chunks and 64 MiB Allocation Pages, one Allocation Page contains 16 Allocation Chunks. A 128 MiB Placement Extent can group two Allocation Pages for placement planning. These numbers do not describe physical storage. The same logical range can later be stored as a replicated object or as an EC stripe.

<div class="diagram" markdown="1">

<div class="diagram-title">Logical geometry example</div>

<div class="flow" markdown="1">

<div class="box-accent">Volume 0..1GiB</div>

<div class="arrow">split</div>

<div class="box">

Allocation Page 0\
0..64MiB

</div>

<div class="arrow">contains</div>

<div class="box">

16 Allocation Chunks\
4MiB each

</div>

<div class="arrow">planned by</div>

<div class="box-soft">

Placement Extent\
0..128MiB

</div>

</div>

</div>

## Relationship To Physical Storage

A logical range consumes physical storage only after metadata publishes an AllocationEntry that points to a PhysicalObjectRef. Until then, the range can be represented as missing or zero metadata and reads return zeroes. When data is written, Chapter 7 takes over: the AllocationEntry points to a PhysicalObjectRef, and that ref carries an opaque backend descriptor. Chapters 8 and 9 then explain how the descriptor reaches replicated chunks or EC stripe/shard objects on `sbs-data` stores.

<div class="diagram" markdown="1">

<div class="diagram-title">Where physical storage starts</div>

<div class="flow" markdown="1">

<div class="box-accent">

Allocation Chunk\
logical range

</div>

<div class="arrow">mapped by</div>

<div class="box">

AllocationEntry\
zero or data

</div>

<div class="arrow">if data</div>

<div class="box">

PhysicalObjectRef\
backend-neutral object

</div>

<div class="arrow">then</div>

<div class="box-soft">

replica chunks or EC shards\
backend-specific storage

</div>

</div>

</div>

## Thin Provisioning And Zero Semantics

Thin provisioning follows directly from the logical geometry. NAMRBD can expose a large Volume byte range while allocating backend payload only for chunks that have committed non-zero data. A newly created volume, an expanded tail, or a discarded range can therefore be represented by missing AllocationEntries or explicit zero entries instead of physical zero objects.

| Operation Or State | Logical Geometry Effect | Physical Storage Effect |
|----|----|----|
| New volume or expanded tail | The Volume byte range exists, but affected Allocation Chunks have no data entry yet. | No payload object is required. Reads synthesize zeroes. |
| Non-zero write | The write range is split across Allocation Chunks and the owning Allocation Pages are updated. | Payload is persisted first, then metadata publishes PhysicalObjectRefs for the affected chunks. |
| `WRITE_ZEROES` | Future reads for the range must return zeroes. When geometry and policy allow, metadata can publish zero AllocationEntries. | It does not by itself promise physical reclaim. Old refs remain protected until reachability and operation policy classify them. |
| `DISCARD` / UNMAP | The live view is changed so the discarded logical chunks read as zero. | If the request is reclaim-aligned and policy allows, old PhysicalObjectRefs are detached from the live view and become protected or reclaimable according to snapshot/clone reachability. |

Alignment matters because operations are committed through Allocation Pages and Allocation Chunk spans. A request that crosses page or reclaim-geometry boundaries may need multiple metadata updates or may be rejected or reported as a zero fallback by the policy described in Chapter 13.

## Handoff To Chapter 7

This chapter stops at logical naming and alignment. Chapter 7 starts at the moment an AllocationEntry is resolved or committed. It explains the common chain used by reads, writes, snapshots, clones, discard, and GC: logical view to AllocationEntry, AllocationEntry to PhysicalObjectRef, then PhysicalObjectRef to a replicated or EC backend descriptor.

## Expansion Boundary

Grow-only expansion increases the volume size and exposes new logical range. It does not change allocation chunk size, allocation page size, backend type, EC profile, stripe unit, or placement extent size. The new range starts as zero/unallocated and is allocated lazily when written. The host-side online path reloads gateway-visible size before resizing the kernel device.

[\<- Previous](04-metadata-authority.md) [Next: Logical-To-Physical Mapping -\>](06-logical-to-physical-mapping.md)
