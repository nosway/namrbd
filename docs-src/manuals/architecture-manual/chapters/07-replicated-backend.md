Chapter 8

# Replicated Backend

## Replicated Path

- Replica set
- Replica physical chunks
- Quorum semantics
- Metadata visibility

<div class="summary" markdown="1">

The replicated backend stores a Physical Object as replicated payload chunks across selected `sbs-data` targets. The replicated descriptor explains how to read, write, and delete that object. It is a backend-specific descriptor under the same logical storage substrate used by snapshots, clones, discard, and GC.

Replica physical chunk references are physical backend details. They do not replace AllocationEntry and PhysicalObjectRef as the logical storage truth.

</div>

<figure class="architecture-figure" markdown="1">

![Replicated backend storage path diagram showing ReplicaSetState, replica refs, quorum, and sbs-data payload chunks](../assets/diagrams/replicated-backend-storage-path.svg)

<figcaption>Replicated backend behavior begins below the PhysicalObjectRef boundary. The common mapping decides visibility; the replicated descriptor decides replica membership, write quorum, eligible reads, and DATA-only repair, drain, and rebalance work.</figcaption>

</figure>

<div class="diagram" markdown="1">

<div class="diagram-title">Replicated object concept</div>

<div class="flow" markdown="1">

<div class="box-accent">

AllocationEntry\
logical truth

</div>

<div class="arrow">-\></div>

<div class="box">

PhysicalObjectRef\
backend=replicated

</div>

<div class="arrow">-\></div>

<div class="box">

ReplicaSetState\
placement + quorum

</div>

<div class="arrow">-\></div>

<div class="box-soft">

replica chunks\
Pebble payload on sbs-data

</div>

</div>

</div>

## What Is Different In Replicated Mode

Replicated mode does not change logical range resolution. It changes the backend descriptor and payload execution below `PhysicalObjectRef`. The backend stores equivalent payload chunks on multiple `sbs-data` targets and uses ReplicaSetState to decide read eligibility, write quorum, repair, rebalance, and drain behavior.

| Concern | Common Logical Rule | Replicated Backend Rule |
|----|----|----|
| Zero read | `AllocationEntry kind=zero` returns zeroes without payload access. | No replica is contacted because no PhysicalObjectRef is produced. |
| Data read | The read view resolves an AllocationEntry to a replicated PhysicalObjectRef. | The backend chooses an eligible replica target and reads the named physical chunk. |
| Write | New data becomes visible only after AllocationEntry metadata commit. | Payload is written to enough replicas for quorum before the metadata commit publishes the new ref. |
| Maintenance | Reachability and read-view roots decide which objects must stay protected. | Repair/rebalance/drain copy or replace replica chunks, then publish replacement descriptor state. |

## Records And Store Objects

The replicated backend is concrete, but it is still layered. TiKV records decide which object is visible and where replicas should live. Local Pebble stores on `sbs-data` nodes hold the actual payload chunks and local execution metadata.

| Layer | Representative Record / Key | Purpose |
|----|----|----|
| Volume policy | `VolumeSpecRecord`, `admin/volumes/<id>/spec` | Defines replicated backend, replication factor, geometry, and placement policy. |
| Logical mapping | `AllocationPageRecord`, `volumes/<id>/allocation/pages/<page>` | Publishes AllocationEntries that point to replicated PhysicalObjectRefs or zero state. |
| Object descriptor | `PhysicalObjectRecord` or compatibility `physical_chunk_start` | Names the replicated backend object and descriptor used after logical resolution. |
| Placement and quorum | `ReplicaSetState`, `volumes/<id>/replicasets/<replica_set_id>` | Names replica members, primary preference, read quorum, write quorum, epoch, and failure domains. |
| Local payload | Node-local Pebble chunk/store keys managed by `sbs-data` | Holds the actual replicated payload bytes and local store state. It is not global reachability truth. |

## Descriptor Contents

| Field Class | Purpose |
|----|----|
| Placement reference | Connects the object to placement/failure-domain planning. |
| Replica set identity | Names the target set used to store replicated chunks. |
| Replica physical chunk refs | Backend-specific physical read/delete identities. |
| Quorum metadata | Documents write/read quorum expectations for the object. |

## Write Path Shape

<div class="diagram" markdown="1">

<div class="diagram-title">Replicated write visibility</div>

<div class="flow" markdown="1">

<div class="box-accent">write payload to replicas</div>

<div class="arrow">-\></div>

<div class="box">prepare PhysicalObjectRef</div>

<div class="arrow">-\></div>

<div class="box">commit AllocationEntry</div>

<div class="arrow">-\></div>

<div class="box-soft">committed read observes data</div>

</div>

</div>

Metadata commit remains the visibility point. If payload reaches replicas but the metadata commit fails, the payload is unreferenced cleanup work rather than visible volume data.

## How Replicated Writes Work

| Step | Action | Authority Boundary |
|----|----|----|
| 1\. Resolve placement | `sbs-service` uses volume policy, topology, and ReplicaSetState to choose eligible replica targets. | Placement is TiKV/SBS authority, not gateway or kernel state. |
| 2\. Persist payload | The backend writes payload chunks to selected `sbs-data` stores and waits for the required write quorum. | Quorum proves backend durability for that object, not logical visibility by itself. |
| 3\. Prepare descriptor | The write prepares a replicated PhysicalObjectRef or compatibility descriptor with chunk identity, count, generation, and checksum where available. | Descriptor fields remain backend-private below PhysicalObjectRef. |
| 4\. Commit metadata | `sbs-service` commits AllocationPageRecords and advances the volume revision in TiKV. | This metadata commit is the visible read-view boundary. |
| 5\. Retire old refs | Old PhysicalObjectRefs become protected or reclaimable only after reachability analysis. | Snapshots, clones, pending operations, and GC roots can keep old chunks alive. |

## Read Path Shape

A read first resolves the logical range to AllocationEntries. A zero entry returns zeroes. An allocated entry dispatches the PhysicalObjectRef to the replicated backend reader, which chooses eligible replica targets according to the descriptor and target availability.

## Failure And Maintenance States

| State / Operation | Replicated Backend Meaning | Observable Boundary |
|----|----|----|
| Healthy replica set | Enough members are healthy to satisfy write and read quorum while preserving failure-domain policy. | ReplicaSetState, node health, and gateway published target view agree. |
| Degraded read | The backend can read from an alternate eligible replica when a preferred target is unavailable. | Path choice is backend availability, not a change to AllocationEntry truth. |
| Primary failover | ReplicaSetState epoch and primary preference change after fencing stale writers. | Readers should see a new placement epoch, not a new logical object solely because the primary changed. |
| Repair / rebalance / drain | Maintenance copies or moves backend chunks, validates topology, and publishes placement transition state. | Logical visibility remains through existing PhysicalObjectRefs until metadata publishes a replacement descriptor. |

[\<- Previous](06-logical-to-physical-mapping.md) [Next: Erasure Coding Backend -\>](08-erasure-coding-backend.md)
