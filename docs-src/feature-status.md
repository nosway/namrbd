# Feature Status

NAMRBD uses three different terms deliberately:

- **Available in public source** means the code or deployment assets are in
  this repository and can be built or inspected.
- **Validated for v1.0** means the named release path has matching release
  evidence and is inside the current support boundary.
- **Advanced feature** means Enterprise development or validation is in
  progress. It is not a general-availability or support commitment.

## Public Platform

| Capability | Public source | v1.0 status |
| --- | --- | --- |
| Replicated userspace block volumes through `namrbd-gateway` and SBS | Included | Validated release path. This is the current supported volume claim. |
| Volume lifecycle, topology-aware placement, and grow-only expansion | Included | Available as part of the replicated platform; use the userspace gateway boundary for the v1.0 support claim. |
| Manual replicated snapshot, restore, and immutable read-view workflows | Included | Available in source; not yet validated as a supported v1.0 release surface. |
| Discard, write-zeroes, and reclaim observability | Included | Available in source with userspace evidence; backend and deployment-specific claims require matching release evidence. |
| Kubernetes CSI provisioning and snapshot restore assets | Included | Integration preview; not yet validated as a supported v1.0 release surface. |
| Linux kernel block/control modules | Included | Buildable on Linux with matching headers; kernel datapath I/O is outside the current v1.0 support boundary. |
| Basic iSCSI gateway and `sbsctl iscsi` control | Included, limited to three distinct exported volumes | Integration preview; protocol gateway and external-initiator support are not yet validated for v1.0. |
| Health, metrics, alerts, Grafana dashboard, operations console, and observe-first MCP tools | Included | Public operations surfaces; support follows the underlying validated deployment path. |
| Local Compose quickstart and kind CSI demo | Included | Development and evaluation workflows, not production topology claims. |

The current release does not publish a general performance benchmark. Results
from a particular topology, workload, or experimental mode must not be treated
as a product-wide performance commitment.

## Advanced Features

NAMRBD is developing and validating the following capabilities for the
Enterprise edition. These descriptions summarize intended function only. They
do not promise availability, compatibility, scale, performance, or a delivery
date.

| Advanced capability | Development direction |
| --- | --- |
| Erasure-coded storage | Full-stripe userspace EC placement, encoding, rebuild, scrub, and maintenance workflows. |
| Automated backup and recovery | Backup targets, schedules and policies, run records, retention holds, restore drills, and recovery evidence. |
| Security and governance | KMS-backed data keys, payload encryption, key rotation, audit, crypto erase, encrypted backup evidence, and scoped governance/WORM controls. |
| Performance and QoS | Workload classification, dynamic rate controls, performance tiers, dependency budgets, and scale-oriented observability. |
| Advanced iSCSI and large-scale operations | Export scale beyond the public cap, redundant target paths, MPIO/ALUA, registry reload, fencing, and controlled membership workflows. |
| Remote replication and disaster recovery | Replication links, recovery points, shipping manifests/workers, standby import, promotion/demotion, and failover orchestration. Some control-plane records exist, while end-to-end remote transfer and failover remain under validation. |
| Data mobility and repack | Controlled movement between placement or storage geometries with progress, verification, rollback, and recovery boundaries. Broad live migration is not claimed. |
| Deduplication | Scoped replicated-data dedupe, reference safety, and reclaim workflows. Broad inline dedupe and live EC dedupe are not claimed. |

The [Community and Enterprise Edition Boundary](manuals/edition-boundary.md)
defines the required `[Enterprise Edition Only]` marker and fail-closed CLI/API
behavior. Detailed Enterprise manuals describe engineering and operational
contracts for these development areas without expanding the release support
boundary.

NVMe/TCP is tracked separately as exploratory future work. It is not a current
open-source or Enterprise support claim.
