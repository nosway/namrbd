Chapter 9

Enterprise edition only

# Erasure Coding Backend

## EC Model

- EC profile
- Stripe
- Shard
- Topology safety

<div class="summary" markdown="1">

Erasure Coding is a backend descriptor under the same AllocationEntry and PhysicalObjectRef model. EC changes how payload is encoded, placed, reconstructed, and maintained. It does not create a new metadata authority or bypass read-view and reachability rules.

An EC-backed write becomes visible only when SBS metadata commits an AllocationEntry that points at a stable EC PhysicalObjectRef. EC placement, repair, rebuild, scrub, drain, and expansion remain `sbs-service` authority.

</div>

<figure class="architecture-figure" markdown="1">

![Erasure coding backend storage path diagram showing EC profile, stripe generation, data shards, parity shards, and degraded reconstruction](../assets/diagrams/ec-backend-storage-path.svg)

<figcaption>EC backend behavior begins below the PhysicalObjectRef boundary. The common mapping decides visibility; the EC descriptor decides stripe generation, data/parity shard layout, full-stripe writes, partial-write merge, and degraded reconstruction behavior.</figcaption>

</figure>

<div class="diagram" markdown="1">

<div class="diagram-title">EC Object Concept</div>

<div class="flow" markdown="1">

<div class="box-accent">

AllocationEntry\
logical truth

</div>

<div class="arrow">-\></div>

<div class="box">

PhysicalObjectRef\
backend=ec

</div>

<div class="arrow">-\></div>

<div class="box-soft">

EC descriptor\
profile + stripe generation

</div>

<div class="arrow">-\></div>

<div class="box">

data/parity shards\
placed by zone policy

</div>

</div>

</div>

## What Is Different In EC Mode

EC mode keeps the same logical resolver but changes the payload unit below `PhysicalObjectRef`. A data-bearing entry resolves to an EC descriptor that names a profile, stripe generation, shard refs, checksums, and topology evidence. Reads may use data shards directly or reconstruct from data/parity shards; writes publish a new stripe generation or replacement PhysicalObjectRef rather than overwriting a reachable stripe in place.

| Concern | Common Logical Rule | EC Backend Rule |
|----|----|----|
| Zero read | `AllocationEntry kind=zero` returns zeroes without payload access. | No shard is contacted because no EC PhysicalObjectRef is produced. |
| Healthy read | The read view resolves a data/shared entry to an EC PhysicalObjectRef. | The backend reads the needed data shards when enough are available. |
| Degraded read | The visible object identity still comes from committed metadata. | The backend reads a sufficient set of data/parity shards and reconstructs missing data within the profile tolerance. |
| Write | New data becomes visible only after AllocationEntry metadata commit. | Full-stripe writes encode `k` data plus `m` parity shards; partial writes read the old view as needed and publish a new stripe generation or replacement ref. |
| Maintenance | Reachability and read-view roots decide which objects must stay protected. | Rebuild/scrub/rebalance/drain operate on shard refs and must preserve topology safety before publishing replacement shard metadata. |

## EC Records And Shard Layout

EC is written as the enterprise backend in this edition, not as a separate future model. The common logical mapping still points at a PhysicalObjectRef. The backend descriptor below that reference names the EC profile, stripe generation, shard refs, checksums, and topology evidence needed to read or rebuild the object.

| Layer | Representative Record / Key | Purpose |
|----|----|----|
| Volume policy | `VolumeSpecRecord`, `admin/volumes/<id>/spec` | Stores `backend=ec`, EC profile id, failure domain, topology mode, and expansion policy. |
| Logical mapping | `AllocationPageRecord`, `volumes/<id>/allocation/pages/<page>` | Publishes AllocationEntries that point at EC PhysicalObjectRefs or zero state. |
| EC object descriptor | `PhysicalObjectRecord` with EC descriptor fields | Names stripe id, stripe generation, profile, shard refs, checksums, and zone shard counts. |
| Shard payload | Node-local `sbs-data` Pebble shard keys | Holds encoded data or parity shard bytes. Local inventory is not global reachability truth. |
| Maintenance operation | `ECMaintenanceOperationRecord` or volume operation record | Persists rebuild, scrub, rebalance, or drain progress, idempotency, blocked reason, and resume point. |

## EC Profile

| Profile Field | Meaning |
|----|----|
| `codec_id` | Systematic Reed-Solomon profile identifier for the current baseline. |
| `k` data shards | Number of data shards needed for normal data layout. |
| `m` parity shards | Number of parity shards; also the default one-zone shard cap. |
| `stripe_unit_bytes` | Payload unit size for each shard in a stripe. |
| `failure_domain` | Current managed domain is zone. |

## Topology-Safe Placement

Strict one-zone-tolerant EC placement requires that a single zone loss cannot remove more shards than parity can reconstruct. The simple review rule is:

    max_shards_in_any_single_zone <= m

If the selected failure-domain policy cannot satisfy this bound, strict placement should fail or report weak placement rather than silently publishing a weaker stripe.

## Read And Maintenance

| Operation | Architecture Meaning |
|----|----|
| Healthy read | Read data shards directly when enough data shards are available. |
| Degraded read | Reconstruct missing data from available data/parity shards. |
| Partial write | Publish a new stripe generation or PhysicalObjectRef rather than overwriting a reachable object in place. |
| Rebuild/scrub/drain | Run under SBS maintenance authority while preserving read-view and reachability roots. |

## How EC Writes And Reads Work

| Path | Flow | Visibility Boundary |
|----|----|----|
| Full-stripe write | Split payload into `k` data shards, compute `m` parity shards, place shards by topology policy, persist shard payloads, then publish the EC PhysicalObjectRef. | The AllocationEntry commit makes the stripe visible. |
| Partial write | Read the old view as needed, produce a new stripe generation or replacement PhysicalObjectRef, and commit metadata by swap. | Readers never depend on in-place overwrite of a reachable EC stripe. |
| Healthy read | Resolve logical mapping, choose the needed data shards, read shard payloads, and return the requested bytes. | Read identity comes from the read view and committed AllocationEntry. |
| Degraded read | Read any sufficient set of data/parity shards and reconstruct missing data within profile tolerance. | Degraded reconstruction does not publish new placement by itself. |

## Maintenance State Machines

| Operation | States | Architecture Rule |
|----|----|----|
| Rebuild | `queued -> planning -> copying -> verifying -> committing -> complete`, with `blocked` and `failed` exits. | Choose topology-safe targets, reconstruct missing shards, verify payload, then publish replacement shard refs. |
| Scrub | `queued -> scanning -> verifying -> repairing_optional -> complete`. | Detect checksum or shard drift without changing logical read-view identity. Repair uses the rebuild authority path. |
| Rebalance | `queued -> planning -> moving -> verifying -> committing -> complete`. | Improve balance only when topology safety is preserved. Byte balance is lower priority than failure-domain safety. |
| Drain | `preflight -> planning -> moving -> verifying -> committing -> drained`, or `blocked`. | Do not move shards away from a node or zone if the post-drain topology would be unsafe unless an explicit weak mode is admitted and reported. |

[\<- Previous](07-replicated-backend.md) [Next: Write Visibility And Ordering -\>](09-write-visibility-and-ordering.md)
