Appendix B

# Reference Map

## Appendix

Where each architecture chapter gets its source material.

<div class="summary" markdown="1">

This appendix maps the HTML architecture chapters to the current design sources used to write them. Architecture authority wins over operational guides when the two disagree.

</div>

## Reference Map Aid

Use this appendix as a compact reviewer aid: first find the chapter that owns the concept, then check the primary source row for the design authority, and finally use operational guides only for current command spelling or deployment examples.

<div class="diagram" markdown="1">

<div class="diagram-title">Reviewer source order</div>

<div class="flow" markdown="1">

<div class="box-accent">architecture chapter</div>

<div class="arrow">checks</div>

<div class="box">architecture authority doc</div>

<div class="arrow">then</div>

<div class="box">interface spec</div>

<div class="arrow">then</div>

<div class="box-soft">operations guide</div>

</div>

</div>

| Chapter | Primary Sources |
|----|----|
| 00-02 | [Reading Guide](00-reading-guide.md), [Platform Overview](01-platform-overview.md), [Components And Ownership](02-components-and-ownership.md), and current component interface summaries. |
| 03 | [Linux Host Control And Data Plane](03-linux-host-control-and-data-plane.md) plus the kernel UAPI and gateway route implementation. |
| 04 | [Metadata Authority](04-metadata-authority.md) and the SBS service/data ownership boundary. |
| 05-06 | [Logical Storage Geometry](05-logical-storage-geometry.md) and [Logical-To-Physical Mapping](06-logical-to-physical-mapping.md). |
| 07 | [Replicated Backend](07-replicated-backend.md) and shared gateway/SBS interface summaries. |
| 08 | [Erasure Coding Backend](08-erasure-coding-backend.md) and topology placement summaries. |
| 09 | [Write Visibility And Ordering](09-write-visibility-and-ordering.md) and the storage substrate visibility model. |
| 10-11 | [Read Views, Snapshots, And Clones](10-read-views-snapshots-and-clones.md) and [Reachability And GC](11-reachability-and-gc.md). |
| 12 | [Zero, Discard, And Reclaim](12-zero-discard-and-reclaim.md). |
| 13 | [Kernel-Gateway Dataplane](13-kernel-gateway-dataplane.md). |
| 14 | [Topology, Placement, And Expansion](14-topology-placement-and-expansion.md). |
| 15 | [Observability And Validation](15-observability-and-validation.md) plus release evidence summaries. |
| 16 | [Kubernetes/CSI Integration Case](16-kubernetes-csi-integration-case.md). |
| 17 | [Edition And Release Boundaries](17-edition-and-release-boundaries.md) and current public support matrix wording. |
| Appendix C | `kernel/uapi/namrbd_netlink.h`, `gateway/httpapi/server.go`, `cmd/namrbd-iscsi-gateway`, `cmd/sbsctl`, `iscsi`, `third_party/gotgt`, `proto/sbs/admin/v1/*.proto`, `proto/sbs/v1/volume.proto`, `cmd/sbs-service/main.go`, `cmd/sbs-data/main.go`, and component interface summaries. |

## First Edition Rule

This HTML edition intentionally presents the current architecture. Every chapter should trace to at least one current authority source. Earlier project notes remain valuable for project history, and operational guides are useful for current user-visible names, but neither is used as first-edition teaching flow when it conflicts with the current authority set.

[\<- Previous](appendix-glossary.md) [Next: Interface Specifications -\>](appendix-interface-specifications.md)
