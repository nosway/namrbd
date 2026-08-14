Chapter 17

Edition boundary: Community edition CSI flows and Enterprise edition only EC restore shapes are both present.

# Kubernetes/CSI Integration Case

## CSI Case

- Identity
- Controller
- Node
- Kubernetes objects

<div class="summary" markdown="1">

Kubernetes uses NAMRBD through `namrbd-csi-driver`, CSI sidecars, and Kubernetes storage objects. The driver is a translation layer. The core storage semantics remain in NAMRBD APIs, gateway, kernel host path, and SBS metadata.

The CSI driver maps CSI calls to NAMRBD controller and node operations. It does not own snapshot, clone, placement, fencing, topology, read-view, discard, or GC semantics.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Kubernetes integration shape</div>

<div class="flow" markdown="1">

<div class="box-accent">Kubernetes PVC/PV/Snapshot</div>

<div class="arrow">-\></div>

<div class="box">CSI sidecars</div>

<div class="arrow">-\></div>

<div class="box">namrbd-csi-driver</div>

<div class="arrow">-\></div>

<div class="box-soft">NAMRBD APIs and Linux host path</div>

</div>

</div>

## Object Mapping

| Kubernetes / CSI Object | NAMRBD Meaning |
|----|----|
| PVC / PV | Provisioned NAMRBD volume handle and size/policy. |
| StorageClass | Volume policy: backend, topology, expansion, and related parameters. |
| VolumeSnapshot | NAMRBD snapshot id and snapshot root status. |
| VolumeSnapshotClass | Snapshot policy and deletion behavior mapping. |
| VolumeContentSource snapshot | Create-volume-from-snapshot restore primitive. |

## CSI Service Mapping

| CSI Service | NAMRBD Backend Shape |
|----|----|
| Identity | Reports driver identity, capabilities, and readiness. |
| Controller | Calls SBS/admin APIs for create/delete volume, snapshot, restore, expansion, and status. |
| Node | Prepares local device/session, publishes raw block or filesystem path, handles node-side expansion. |

## CSI Restore Shapes <span class="edition-boundary-inline">Includes Enterprise edition only EC restore</span>

`CreateVolume` with `VolumeContentSource` snapshot is the CSI-facing restore primitive. The CSI driver should expose one volume-like result, while NAMRBD decides which backend shape implements it for the selected edition and StorageClass policy.

| Restore Shape | NAMRBD Behavior | When It Applies |
|----|----|----|
| Clone-like view | Create a target read view with a base snapshot root and target delta. Reads fall back to the source snapshot until ranges are written or materialized. | Useful when edition/backend allows space-efficient restore and the target can keep a protected source dependency. |
| Materialized independent volume | Resolve the snapshot view into target-owned allocation pages and backend descriptors, then release the source dependency after verification. | Required when the restore must be independent immediately, when policy forbids source dependency, or when backend conversion is needed. |
| Edition/backend-selected | Community replicated restore can default to the supported replicated shape, while enterprise EC restore can preserve EC profile/topology or materialize across backend policy as allowed. | StorageClass parameters and edition capability decide whether both shapes are offered or one shape is admitted. |

## Why This Chapter Is Late

CSI is easier to review after the core architecture is clear. `CreateVolumeFromSnapshot` depends on snapshot roots and clone/materialization behavior, and it must produce a volume-like target with size, topology, expansion, and fencing behavior. Node publish depends on Linux host attach and device path behavior. StorageClass topology depends on the SBS topology and placement model. Kubernetes-specific parameters map to existing NAMRBD policy rather than creating a second storage model. Discard exposure depends on backend reclaim and kernel operation identity.

<div class="note" markdown="1">

**Discard exposure.** Kubernetes discard is disabled by default. Enabled manifests require explicit evidence that backend and kernel discard semantics are valid in that target environment.

</div>

[\<- Previous](15-observability-and-validation.md) [Next: Edition And Release Boundaries -\>](17-edition-and-release-boundaries.md)
