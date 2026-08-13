Chapter 7

# Logical-To-Physical Mapping

## Core Chain

1.  Logical view
2.  Allocation resolver
3.  AllocationEntry
4.  PhysicalObjectRef
5.  Backend descriptor

<div class="summary" markdown="1">

Chapter 6 defines the logical geometry: volume byte ranges, Allocation Pages, Allocation Chunks, and Placement Extents. This chapter starts at the resolver boundary where those logical ranges become AllocationEntries and, for data-bearing ranges, backend-neutral PhysicalObjectRefs.

The central storage substrate maps logical read/write views to backend-neutral physical object references using SBS metadata stored in TiKV. Snapshot, clone, discard, and GC code should operate at the AllocationEntry and PhysicalObjectRef level. Backend readers and deleters inspect replicated or EC descriptors only after that resolution.

Logical storage truth is not a dense physical chunk sequence. It is an ordered set of AllocationEntries that point at PhysicalObjectRefs or zero state.

</div>

<figure class="architecture-figure" markdown="1">

![Logical-to-physical mapping diagram from read view to backend payload](../assets/diagrams/logical-to-physical-mapping.svg)

<figcaption>Shared English SVG for logical-to-physical mapping. Offset calculation finds the AllocationEntry, then ZERO and DATA branches decide whether <code>sbs-data</code> is touched. Writes publish visibility only through the committed AllocationEntry.</figcaption>

</figure>

<figure class="architecture-figure" markdown="1">

![Common logical storage substrate diagram showing zero short-circuit, data object resolution, and backend boundary](../assets/diagrams/common-logical-storage-substrate.svg)

<figcaption>The common storage substrate is backend-neutral. Replicated and EC volumes both split requests by geometry, resolve an AllocationEntry table, short-circuit ZERO spans, and make writes visible only at AllocationEntry metadata commit.</figcaption>

</figure>

## Common Versus Backend-Specific Boundaries

The most important review boundary is the point where common logical storage stops and backend payload execution begins. Above `PhysicalObjectRef`, replicated and EC volumes share the same read-view, zero, write publication, snapshot, clone, discard, reachability, and GC rules. Below `PhysicalObjectRef`, the backend descriptor decides whether the payload work is replica quorum I/O or EC stripe/shard I/O.

| Layer | Common To All Backends | Backend-Specific After The Boundary |
|----|----|----|
| Logical request shape | Volume size, block size, Allocation Page, Allocation Chunk, Placement Extent, read-view identity. | None. The request is still logical and backend-neutral. |
| Visibility metadata | AllocationPageRecord, AllocationEntry kind, PhysicalObjectRef identity, VolumeState revision, mutation operation/idempotency record. | Descriptor payload fields under the PhysicalObjectRef. |
| Read behavior | Zero entries synthesize bytes without `sbs-data`; data/shared entries resolve a PhysicalObjectRef from the selected read view. | Replicated reads choose eligible replicas. EC reads choose enough data/parity shards and may reconstruct. |
| Write publication | Payload is persisted first, but the new logical contents become visible only when `sbs-service` commits the AllocationEntry and advances metadata. | Replicated writes satisfy replica quorum. EC writes encode a stripe generation and persist shard payloads. |
| Retirement and GC | Old PhysicalObjectRefs stay protected while live views, snapshots, clones, or pending operations reference them. | Replicated delete removes replica chunks. EC delete removes shard objects after reachability allows it. |

## Input From Logical Geometry

The resolver receives a view identity and a logical byte range. Geometry determines which Allocation Pages and Allocation Chunks are involved. Mapping determines what each chunk span currently means: zero, data backed by a PhysicalObjectRef, or shared data reachable from another read view.

| From Chapter 6 | Resolver Use | Mapping Output |
|----|----|----|
| Volume byte range | Bounds the requested logical I/O and read-view identity. | Ordered logical spans. |
| Allocation Page | Selects the metadata pages that own the requested chunk spans. | AllocationPageRecords with committed entries. |
| Allocation Chunk | Provides the unit for zero, data, shared, pending, or deleted state. | AllocationEntries. |
| Placement Extent | Constrains placement and backend target choice for new data-bearing objects. | Placement references inside PhysicalObjectRefs or backend descriptors. |

## TiKV Metadata Used By Mapping

TiKV is the SBS cluster metadata authority for the records below. `sbs-service` reads and mutates these records; gateways consume resolved or published views through SBS APIs and must not treat a raw TiKV cache as their own storage truth. The default key root is `sbs/cluster`, and the paths shown here are representative key families rather than a public API.

| TiKV Record | Representative Key Family | Mapping Role |
|----|----|----|
| VolumeSpecRecord | `admin/volumes/<volume_id>/spec` | Provides size, block size, allocation page size, allocation chunk size, placement extent size, redundancy backend, replication factor, and EC profile fields used to interpret logical ranges. |
| VolumeState | `volumes/<volume_id>/meta/state` | Provides epoch, revision, status, topology mode, and redundancy backend. Writers use it for fencing and committed read paths use it to choose a stable current view. |
| AllocationPageRecord | `volumes/<volume_id>/allocation/pages/<page_no>` | Stores the logical mapping entries for a page. Each AllocationExtentRecord names a logical chunk span, its kind, and either zero state, a modern `backing_ref`, or a compatibility replicated `physical_chunk_start`. |
| PhysicalObjectRecord | `volumes/<volume_id>/physical_objects/<object_id>` | Resolves a `backing_ref` into backend-neutral object metadata: backend type, placement ref, logical length, generation, checksum, state, and the replicated or EC descriptor. |
| ECStripeRecord and ECShardRecord | `volumes/<volume_id>/ec/stripes/<stripe_id>/generations/<generation>` | For EC-backed objects, resolves the EC descriptor to a stripe generation and shard placement: data/coding role, role index, zone, node, store, shard object id, checksum, and size. |
| ReplicaSetState | `volumes/<volume_id>/replicasets/<replica_set_id>` | Connects a replicated placement reference to replica members, primary preference, read/write quorum, epoch, and failure domains. |
| SnapshotRecord, snapshot root pages, CloneRecord, clone delta pages | `snapshots/<snapshot_id>/...`, `clones/<clone_id>/...` | Provide alternate read-view roots. Snapshot reads resolve captured allocation pages. Clone reads overlay clone delta pages before falling back to the base snapshot root. |
| MutationOperationRecord and idempotency records | `volumes/<volume_id>/operations/...`, `volumes/<volume_id>/idem/...` | Track in-flight or completed writes, discards, placement changes, and maintenance effects so retries and leader changes do not publish duplicate or partial visibility. |
| NodeMembershipRecord, NodeHealthDetailRecord, TopologyZoneRecord | `nodes/...`, `topology/zones/...` | Feed placement and published target views. They constrain which physical endpoints can serve a resolved object but are not themselves AllocationEntries. |

## Read Mapping Walkthrough

A foreground read does not scan physical storage looking for data. It resolves metadata first, then dispatches only the physical objects named by committed metadata. The same resolver shape applies to live, snapshot, clone, and materialized clone views; the difference is which allocation page root is read.

<div class="diagram" markdown="1">

<div class="diagram-title">TiKV metadata to physical data</div>

<div class="flow" markdown="1">

<div class="box-accent">view + byte range</div>

<div class="arrow">geometry</div>

<div class="box">AllocationPageRecords</div>

<div class="arrow">entries</div>

<div class="box">zero or PhysicalObjectRef</div>

<div class="arrow">backend</div>

<div class="box-soft">sbs-data payload chunks or shards</div>

</div>

</div>

| Step | TiKV Metadata Read | Result |
|----|----|----|
| 1\. Bound the request | Read VolumeSpecRecord and VolumeState for size, geometry, backend, epoch, and revision. | The byte range is checked against volume size and translated into allocation page numbers and logical chunk spans. |
| 2\. Select the read view | For live reads, use live allocation pages. For snapshot or clone reads, use SnapshotRecord, snapshot root pages, CloneRecord, and clone delta pages. | The resolver chooses the ordered page roots that define visible content for this view. |
| 3\. Load allocation pages | Read AllocationPageRecords for the involved page numbers. Missing compatible pages can synthesize zero pages. | Each page yields AllocationExtentRecords covering logical chunk spans. |
| 4\. Normalize entries | Convert allocation extents into AllocationEntries. `kind=zero` has no PhysicalObjectRef; `kind=data` and `kind=shared` must resolve a backing object. | The logical read result becomes zero spans or data spans with PhysicalObjectRefs. |
| 5\. Resolve backing | If `backing_ref` is present, read PhysicalObjectRecord. If the ref is EC, also read ECStripeRecord. Compatibility replicated extents can synthesize a replicated PhysicalObjectRef from `physical_chunk_start` and `chunk_count`. | The backend reader receives a complete replicated or EC descriptor. |
| 6\. Dispatch to physical storage | Use ReplicaSetState, EC shard records, node health, topology, and published target views to choose eligible `sbs-data` endpoints. | Replicated reads fetch from eligible replica chunks; EC reads fetch or reconstruct from data/coding shards. Zero spans never touch physical storage. |

## From Metadata To Store Objects

| Allocation Shape | Metadata Resolution | Physical Store Meaning |
|----|----|----|
| Zero or missing allocation | No PhysicalObjectRef is produced. | The read path synthesizes zero bytes. No Pebble payload object, replica chunk, or EC shard is required. |
| Replicated compatibility extent | `physical_chunk_start` and `chunk_count` are converted into a replicated PhysicalObjectRef in memory. | The replicated backend uses the descriptor and placement state to read replica payload chunks from eligible `sbs-data` stores. |
| PhysicalObjectRecord with replicated backend | `backing_ref` loads PhysicalObjectRecord with `backend_type=replicated` and a replicated descriptor. | The object is read from replica chunks according to placement, quorum, generation, and target health. |
| PhysicalObjectRecord with EC backend | `backing_ref` loads PhysicalObjectRecord with `backend_type=ec`; the EC descriptor loads ECStripeRecord and ECShardRecords. | The EC backend reads enough shard objects from `sbs-data` stores to serve or reconstruct the requested data. |

The mapping output is therefore precise but layered: AllocationEntry decides what the logical view contains, PhysicalObjectRef decides which backend object represents data, and the backend descriptor decides which physical chunks or shards are touched.

## Worked Read Example

A single read can cross different mapping shapes. One span may be zero and touch no physical store, the next may resolve through a replicated PhysicalObjectRef, and the last may resolve through an EC PhysicalObjectRef and ECStripeRecord. The caller sees one ordered byte stream; the resolver keeps the spans separate until backend dispatch.

| Logical Span | Metadata Records | Physical Work |
|----|----|----|
| Zero span | AllocationEntry `kind=zero`. | Synthesize zeroes; no Pebble payload object is read. |
| Replicated data span | AllocationEntry plus PhysicalObjectRecord or compatibility `physical_chunk_start`. | Read an eligible replica physical chunk from `sbs-data`. |
| EC data span | AllocationEntry, PhysicalObjectRecord, EC descriptor, ECStripeRecord. | Read enough data/coding shards to serve or reconstruct the bytes. |

## AllocationEntry States

| State | Read Behavior | PhysicalObjectRef |
|----|----|----|
| `zero` | Return zeroes for the logical range. | Absent. |
| `allocated` | Read from a backend physical object. | Present. |
| `shared` | Read from an object reachable by more than one view. | Present. |
| `deleted` | Not visible to committed reads. | Absent from committed read view. |
| `pending` | Operation-local intermediate state. | Not visible to committed reads. |

## PhysicalObjectRef

A PhysicalObjectRef is the common reference used by live volumes, snapshot roots, clone deltas, materialization operations, reachability scans, and GC. It carries backend type, object identity, placement reference, logical length, generation/checksum data where available, and an opaque backend descriptor.

## Backend Descriptors

| Backend | Descriptor May Contain | Who Should Inspect It |
|----|----|----|
| Replicated | Replica set, replica physical chunk references, quorum metadata, placement reference. | Replicated backend read/write/delete implementation. |
| EC | EC profile, stripe id, shard refs, data/coding shard checksums, generation. | EC backend read/write/rebuild/scrub implementation. |

Code above backend dispatch should not read backend-specific descriptor details. GC and read-view resolution operate on PhysicalObjectRefs, which lets EC descriptors fit under the same resolver semantics as replicated descriptors.

## Write Publication

Writes use the same records in the opposite direction. The backend first persists payload chunks or shards on `sbs-data` stores and prepares a PhysicalObjectRecord or compatibility replicated descriptor. Then `sbs-service` commits AllocationPageRecords and advances VolumeState revision through TiKV. The metadata commit is the visibility point. If payload persistence succeeds but the TiKV metadata commit fails, the payload is not visible volume data; it is unreferenced cleanup work for later reachability and GC.

[\<- Previous](05-logical-storage-geometry.md) [Next: Replicated Backend -\>](07-replicated-backend.md)
