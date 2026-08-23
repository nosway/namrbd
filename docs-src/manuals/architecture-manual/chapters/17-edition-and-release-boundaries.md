Chapter 18

# Capability And Release Boundaries

<div class="summary" markdown="1">

NAMRBD is an open-source distributed block storage platform. The public source
tree contains the replicated core and several integrations, but source
availability, release validation, and support are separate states. Enterprise
capabilities described below are under development and validation; they are not
general-availability commitments.

</div>

## 1. Status Vocabulary

| Term | Meaning |
| --- | --- |
| Available in public source | Code or deployment assets are present and can be built or inspected. |
| Validated for v1.0 | The exact release path has matching release evidence. |
| Supported | The release support boundary names the path and its known limits. |
| Advanced feature | Enterprise development or validation is in progress. No availability or delivery date is promised. |

The [Feature Status](../../../feature-status.md) page is the concise public
authority for these classifications.

## 2. Public Platform Boundary

The public architecture includes the Linux host and kernel control path,
`namrbd-gateway`, SBS service/data authority, replicated placement and payload
storage, volume lifecycle, manual replicated snapshot/restore/read-view
building blocks, discard/write-zeroes semantics, CSI assets, a basic iSCSI
gateway, observability, the operations console, and observe-first MCP tools.

The current supported v1.0 volume claim is the replicated userspace gateway
path. Kernel datapath I/O, snapshot/restore, CSI, protocol gateway, and external
initiator paths remain available for development or evaluation but do not gain
v1.0 support merely by appearing in source. Basic iSCSI export is capped at
three distinct volumes in the public build.

## 3. Advanced Features

The following features are being developed and validated for the Enterprise
edition:

| Capability | Intended function |
| --- | --- |
| Erasure-coded storage | Full-stripe userspace EC placement, encoding, rebuild, scrub, and maintenance. |
| Automated backup and recovery | Backup targets, policies, runs, retention, restore drills, and recovery evidence. |
| Security and governance | KMS-backed keys, encryption, rotation, audit, crypto erase, encrypted backup evidence, and scoped governance/WORM. |
| Performance and QoS | Workload classification, dynamic rate control, performance tiers, dependency budgets, and scale-oriented observability. |
| Advanced iSCSI and scale | Larger export fleets, redundant target paths, MPIO/ALUA, fencing, bounded reload, and membership operations. |
| Remote replication and DR | Replication links, recovery points, data shipping, standby import, promotion/demotion, and failover orchestration. |
| Data mobility and repack | Controlled movement between placement or geometry layouts with progress, verification, and rollback. |
| Deduplication | Scoped replicated-data dedupe, reference safety, and reclaim workflows. |

Some control-plane records, commands, or scoped validation paths may already
exist in the canonical Enterprise work. That does not establish end-to-end
availability or support. Remote transfer and failover, broad live migration,
broad inline dedupe, live EC dedupe, external KMS compatibility, and public
performance claims remain explicitly unpromoted until their exact release gates
exist.

NVMe/TCP remains exploratory future work and is not a current platform or
Enterprise support claim.

## 4. Release Guardrails

A release claim must name the exact edition, topology, access path, restart
boundary, rollback path, validation artifact, and known limits. The following
inferences are invalid:

- code exists, therefore the feature is supported;
- a fixture passes, therefore a deployment was validated;
- one internal topology ran, therefore a public scale claim exists;
- a userspace path passed, therefore kernel, CSI, or protocol paths passed; or
- an architecture design exists, therefore its command or API is generally
  available.

The v1.0 release makes no general performance benchmark claim. Experimental
measurements must state their exact build, topology, workload, durability
semantics, latency distribution, error count, and excluded paths.

## 5. Review Questions

| Question | Expected answer |
| --- | --- |
| Is the code public? | Check the exported repository, not a private design document. |
| Is it validated for v1.0? | Check Feature Status and matching release evidence. |
| Does an adapter advertise it? | CSI, iSCSI, kernel, GUI, and MCP surfaces must follow the validated backend boundary. |
| Is an Enterprise design a product promise? | No. It remains an Advanced feature until an explicit release promotes it. |

[\<- Previous](16-kubernetes-csi-integration-case.md) [Next: Performance Engineering -\>](18-extreme-io-performance-engineering.md)
