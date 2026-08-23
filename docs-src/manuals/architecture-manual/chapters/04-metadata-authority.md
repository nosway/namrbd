Chapter 5

Advanced feature note: Enterprise metadata records describe designs under
development and validation, not public v1.0 support claims. See
[Feature Status](../../../feature-status.md).

# Metadata Authority

## Stores

- etcd: host/gateway control
- TiKV: SBS cluster metadata
- Pebble: node-local payload/store

<div class="summary" markdown="1">

NAMRBD has multiple metadata stores because different state categories have different owners. `etcd` handles host/gateway control-plane metadata. TiKV stores SBS cluster authority. Local Pebble stores `sbs-data` node-local metadata and payload objects.

A component may cache or observe state it does not own, but it must not mutate that state or interpret it as authority. State mutations need to go to the owning metadata authority.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Mutation finds the owner first</div>

<div class="flow" markdown="1">

<div class="box-accent">Host/gateway control</div>

<div class="arrow">etcd</div>

<div class="box">SBS allocation/topology/read views</div>

<div class="arrow">TiKV</div>

<div class="box">Payload execution state</div>

<div class="arrow">Pebble</div>

<div class="box-soft">observable caches do not mutate authority</div>

</div>

</div>

## Authority Table

| Store | Primary Contents | Write Authority | Examples |
|----|----|----|----|
| etcd | Gateway/control-plane metadata, attachments, generation, gateway registry, published summary cache. | Gateway control-plane path and `sbs-service` for published summary cache. | `/namrbd/volumes/<id>/spec`, `/attachments/current`, `/generations/current`. |
| TiKV | SBS cluster metadata: volumes, allocation pages, placement, replica sets, topology, operations. | `sbs-service` leader. | `sbs/cluster/volumes/<id>/allocation/pages/`, node membership, operation records. |
| local Pebble | Node-local volume materialization, local idempotency, local store metadata, payload chunks or shards. | `sbs-data` node. | `volumes/<id>/state`, `volumes/<prefix>/chunks/`. |

<div class="diagram" markdown="1">

<div class="diagram-title">Metadata separation</div>

<div class="grid" markdown="1">

<div class="mini-card" markdown="1">

### Host/control

Attachment, generation, gateway registry, and published cache live in etcd.

</div>

<div class="mini-card" markdown="1">

### SBS authority

Allocation, placement, topology, and operation truth live in TiKV.

</div>

<div class="mini-card" markdown="1">

### Node local

Payload and local execution state live in Pebble on each `sbs-data` node.

</div>

</div>

</div>

## Metadata Terms

This chapter uses the following NAMRBD terms consistently. The important boundary is that logical allocation metadata describes what a read view should see, while backend descriptors describe how a selected payload object is stored.

| Term | Definition | Authority Boundary |
|----|----|----|
| VolumeSpecRecord | Create-time volume shape: size, block size, allocation chunk size, allocation page size, placement extent size, redundancy backend, replication factor, or EC profile fields. | Owned by SBS metadata. Later control paths may read it, but volume geometry is not recomputed by gateways or kernels. |
| VolumeState | Mutable volume status: epoch, revision, placement policy, topology mode, protection policy, redundancy backend, and availability status. | Owned by `sbs-service`. Writers use epoch and revision to fence stale mutations. |
| Allocation Page | A logical metadata page that covers a stable range of allocation chunks and stores allocation entries for that range. | TiKV is the durable authority. Gateways may cache the resolved view but do not own allocation truth. |
| Allocation Chunk | The logical allocation unit inside an allocation page. Read, write, discard, snapshot, and clone resolution are expressed over chunk spans. | Logical unit only. It is not a replica chunk, EC stripe, or local Pebble object. |
| AllocationEntry | A mapping from a logical chunk span to `zero`, `data`, or `shared` state, optionally carrying a PhysicalObjectRef, generation, and checksum. | This is the logical read-view truth. Backend-specific physical chunk and shard details stay below it. |
| Physical Object | A backend-neutral persisted payload object referenced by logical allocation metadata. | Reachability is decided by AllocationEntries, snapshot roots, clone deltas, and pending operation records. |
| PhysicalObjectRef | A reference from allocation metadata to a physical object. It includes backend type, object id, placement ref, logical length, generation, checksum, and a backend descriptor. | Snapshot, clone, read-view, and GC logic may carry the ref without decoding replicated or EC internals. |
| Backend Descriptor | The replicated or EC-specific layout attached to a PhysicalObjectRef, such as physical chunk start/count or EC stripe and shard references. | Opaque above backend dispatch. It is not the logical allocation truth. |
| Read View | The identity resolved by reads: live volume, snapshot root, clone overlay, or materialized clone. It is separate from a raw VolumeRevision. | The resolver returns AllocationEntry spans. It does not open replica connections, decode EC, or delete payload objects. |
| Operation Record | A durable mutation, placement, snapshot, clone, delete, or maintenance progress record with idempotency and replay state. | Used by `sbs-service` to finish or classify partially completed work after retries or leader changes. |
| Backup/DR Control Record | Enterprise Backup/DR state for backup targets, policies, run records, restore-drilled artifact availability, retention holds, purge plans, status summaries, plus remote DR replication links, recovery points, and shipping manifests. | Persisted by `sbs-service`. U-CTRL-003A state is control-plane identity, manifest binding, and shipping-worker admission only, not a gateway, data-node, kernel, remote transfer completion, promote, or failover authority. |
| Security/Compliance Control Record | Enterprise security state for security providers, policies, volume bindings, data keys, key-access leases, rotation plans, audit events, crypto erase plans, and encrypted backup-artifact evidence. | Persisted by `sbs-service`. Gateways consume the resulting admission, lease, and unwrap authority; kernels apply gateway admission results and do not own key material or KMS state. |

## Common Mapping Shape

<div class="diagram" markdown="1">

<div class="diagram-title">Logical metadata to backend payload</div>

<div class="flow" markdown="1">

<div class="box-accent">

Read view\
live, snapshot, clone

</div>

<div class="arrow">resolves</div>

<div class="box">

AllocationEntry\
logical chunk span

</div>

<div class="arrow">refers to</div>

<div class="box">

PhysicalObjectRef\
backend-neutral ref

</div>

<div class="arrow">dispatches by</div>

<div class="box-soft">

Backend descriptor\
replicated or EC

</div>

</div>

</div>

The allocation resolver maps logical Allocation Chunk ranges to Physical Objects. Snapshot roots and clone deltas store AllocationEntries, not backend-private physical chunks or EC shards. A visible content change is therefore a metadata swap: the new committed AllocationEntry points at a new PhysicalObjectRef, while old reachable read views keep their older refs.

Payload persistence precedes metadata publication. A normal write persists a payload object or shard set, prepares AllocationEntries, commits metadata through SBS authority, and only then becomes visible to the relevant read view. If the metadata commit fails after payload persistence, the payload is unreferenced and later becomes a GC candidate.

## Core SBS Metadata Records

| Record Family | Key Fields | Used For |
|----|----|----|
| VolumeSpecRecord | `volume_id`, `size_bytes`, `block_size`, `chunk_size_bytes`, `extent_page_bytes`, `extent_size_bytes`, `replication_factor`, EC profile fields. | Defines immutable geometry and redundancy settings that later allocation, placement, and attach paths must respect. |
| VolumeState | `epoch`, `revision`, `placement_policy_id`, `topology_mode`, `protection_policy`, `redundancy_backend`, `status`. | Fences writers and publishes the current mutable SBS volume state. |
| AllocationPageRecord and AllocationExtentRecord | `page_no`, `page_bytes`, `chunk_size_bytes`, `logical_chunk_start`, `chunk_count`, `kind`, `backing_ref`, `generation`, `checksum`. | Stores logical mapping truth for zero, data, and shared ranges. Compatibility fields such as `physical_chunk_start` are replicated backend descriptors, not logical truth. |
| PhysicalObjectRecord and PhysicalObjectRef | `object_id`, `backend_type`, `placement_ref`, `logical_length`, `generation`, `checksum`, `state`, replicated descriptor, EC descriptor. | Connects logical AllocationEntries to backend-neutral payload objects and gives GC a common reachability handle. |
| ReplicaSetState, ExtentMappingRecord, PlacementTransitionRecord | Replica set id, placement ref, primary replica, replica members, quorum, failure domains, transition state. | Describes replicated placement and maintenance transitions without changing the logical allocation vocabulary. |
| ECStripeRecord and ECShardRecord | `stripe_id`, `stripe_generation`, `stripe_unit_bytes`, `data_shards`, `coding_shards`, shard role, zone, node, store, shard object id, checksum. | Describes EC physical layout after a PhysicalObjectRef has selected the EC backend. |
| SnapshotRecord, snapshot root pages, CloneRecord, clone delta pages | Snapshot id, source volume, root id, cut revision, clone base root, materialized volume id, allocation page geometry, delta counts. | Publishes stable read views and copy-on-write lineage without copying all payload objects at snapshot or clone creation time. |
| Backup target, policy, run, artifact, hold, and purge-plan records | Target id/type/root, policy generation/schedule/retention, run id/state, artifact state, restore drill result, integrity/readback evidence, retention hold, and purge dry-run decision. | Provides the enterprise Backup/DR control-plane surface. Artifact `available` requires integrity recheck plus userspace and kernel readback evidence. Remote DR adds replication-link, recovery-point, and shipping-manifest product state separately before remote shipping or failover support. |
| Security provider, policy, binding, data-key, lease, rotation, audit, and crypto-erase records | Provider id/type/status and redacted credential refs, policy generation and disabled-key behavior, volume binding and active key version, data-key id/version/generation/state and redacted wrapped refs, lease purpose/expiry/revocation, rotation progress, audit hash-chain entries, and protected-reference erase evidence. | Provides the enterprise Security/Compliance control-plane surface. Encrypted payload headers carry key identity/version, while plaintext keys are returned only through active lease-bound unwrap calls and are not persisted in metadata or summaries. |
| MutationOperationRecord | `operation_id`, `kind`, `state`, placement revision, allocation revision, fencing epoch, affected extents/pages, completed pages, retired physical objects, error state. | Provides idempotency, replay, and recovery for writes, discards, placement changes, snapshot/clone operations, and maintenance work. |
| NodeMembershipRecord, NodeHealthDetailRecord, TopologyZoneRecord | Node id, store id, zone, health, drain/maintenance state, topology revision. | Constrains placement and maintenance decisions and feeds gateway-facing published summaries. |

## Replicated Metadata Shape

For replicated volumes, `VolumeSpecRecord.redundancy_backend` is `replicated` and the replication factor defines how many backend copies are required. Placement metadata resolves a logical placement reference to a ReplicaSetState with replica members, primary preference, write quorum, read quorum, and failure-domain spread.

| Layer | Replicated Metadata | Contract |
|----|----|----|
| Volume policy | `redundancy_backend=replicated`, `replication_factor`, placement policy, topology mode. | Defines intended redundancy. It does not name logical allocation chunks directly. |
| Placement | Replica set id, placement ref, replica node/store members, primary replica id, write/read quorum, failure domains. | Defines where replicated payload writes can be sent and which quorum makes them durable. |
| Allocation | AllocationEntry points to a PhysicalObjectRef with `backend_type=replicated`. | The AllocationEntry remains the logical read-view truth. |
| Backend descriptor | Replicated descriptor fields such as physical chunk start and chunk count, plus placement-derived replica membership. | Replica Physical Chunk ids are backend-private. Snapshot, clone, and GC logic operate on PhysicalObjectRefs. |

## Erasure Coding Metadata Shape

For erasure-coded volumes, the volume spec carries the EC profile: codec id, data shard count, parity shard count, stripe unit size, failure-domain rule, and placement caps. The logical allocation layer is unchanged; only the PhysicalObjectRef backend type and descriptor differ.

| Layer | EC Metadata | Contract |
|----|----|----|
| Volume policy | `redundancy_backend=ec`, `ec_profile_id`, `ec_codec_id`, `ec_data_shards`, `ec_parity_shards`, `ec_stripe_unit_bytes`, failure-domain limits. | Defines how a backend payload object is encoded and how shards must be spread. |
| Allocation | AllocationEntry points to a PhysicalObjectRef with `backend_type=ec`. | Read-view, snapshot, clone, and GC logic still see one PhysicalObjectRef for the logical span. |
| Backend descriptor | EC descriptor fields: profile id, stripe id, stripe generation, stripe unit bytes, data/coding shard counts, stripe offset, data shard refs, coding shard refs. | The descriptor is consumed by EC backend read/write/delete paths after logical resolution. |
| Stripe and shard records | ECStripeRecord tracks stripe state and generation. ECShardRecord tracks role, role index, zone, node, store, shard object id, checksum, and size. | EC Stripe and EC Shard are physical layout terms. They must not replace AllocationEntry as the logical metadata unit. |

## Snapshot And Clone Metadata Shape

Snapshots and clones are metadata read views over AllocationEntries. Snapshot creation records an immutable snapshot root for the source volume at a cut revision. Clone creation records a clone that first resolves clone-owned delta pages and then falls back to the base snapshot root.

| Record | Key Metadata | Read-View Behavior |
|----|----|----|
| SnapshotRecord | `snapshot_id`, `source_volume_id`, `snapshot_root_id`, state, cut volume revision, allocation geometry, source size, clone reference count. | Names an immutable root. The root's AllocationEntries keep old PhysicalObjectRefs reachable. |
| Snapshot root pages | Allocation page geometry and captured AllocationEntries for each page at the snapshot cut. | Reads resolve directly from the root. Missing entries read as zero according to the same sparse allocation rule. |
| CloneRecord | `clone_id`, `source_snapshot_id`, `clone_base_root_id`, state, materialized volume id, size, delta page/object counts. | Names a copy-on-write view over a snapshot root until materialization creates independent live allocation pages. |
| Clone delta pages | Clone-owned AllocationEntries keyed by page number. | Clone reads resolve delta first, then base snapshot root, then zero. Clone writes allocate new PhysicalObjects and update delta pages. |
| Delete and GC metadata | Snapshot clone reference counts, operation records, retired object refs, live allocation pages, snapshot roots, and clone delta pages. | A snapshot referenced by a clone is protected. GC marks reachable PhysicalObjectRefs from every live read view and pending operation before reclaiming old payload objects. |

## Published Views

The gateway needs SBS target information to route requests, but it should consume a gateway-facing published view from `sbs-service`. That view can include endpoint, usability, priority, and preference. Detailed health, membership, topology, repair, and drain reasoning stays inside SBS authority.

## Sparse Zero Allocation

A newly created or expanded logical range can read as zero without allocating payload objects. Metadata can represent the range as zero or unallocated. The read path materializes zero bytes for the caller; it does not create a physical zero object just because a read occurred.

[\<- Previous](03-linux-host-control-and-data-plane.md) [Next: Logical Storage Geometry -\>](05-logical-storage-geometry.md)
