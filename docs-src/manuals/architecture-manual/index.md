First edition

Edition boundary: Community edition architecture overview and Enterprise edition only capability summaries are both present.

# NAMRBD Platform Architecture

<div class="summary" markdown="1">

This HTML edition explains NAMRBD's public architecture for developers, operators, and reviewers. It starts from ordinary Linux block-device usage, then follows the request path through the kernel module, gateway, SBS cluster, metadata stores, logical storage substrate, backends, read views, Backup/DR control-plane state, security/compliance state, scoped Governance/WORM state, the optional iSCSI standard target gateway, and validation harnesses.

NAMRBD expands to Network Attached Multipath Resilient Block Device and is pronounced \[nae-mur-bee-dee\]. The name is descriptive: network-attached access, multiple host/gateway paths, and resilient block-device behavior are first-order architecture concerns.

The name should also make these product features explicit:

- network-connected block storage built as a scalable distributed cluster;
- multipath connections for high availability between hosts and gateways;
- standard iSCSI target access as a gateway option: Community includes the basic gateway, `sbsctl iscsi`, and LUN export capped at 3 distinct iSCSI-exported volumes, while iSCSI HA, MPIO/ALUA, advanced security/audit, and scale observability remain Enterprise-only;
- scoped Governance/WORM support for block-native derived objects and userspace gateway sealed-target write rejection, without regulatory certification or object-store API compatibility claims;
- automatic recovery as much as practical when internal components fail;
- product direction that prioritizes Linux and Kubernetes (K8S) environments.

Kubernetes and CSI are covered as an integration case after the core Linux and storage model is established.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Reader path</div>

<div class="flow" markdown="1">

<div class="box-accent">Linux host path</div>

<div class="arrow">-\></div>

<div class="box">Authority boundaries</div>

<div class="arrow">-\></div>

<div class="box">Storage substrate</div>

<div class="arrow">-\></div>

<div class="box">Read views and GC</div>

<div class="arrow">-\></div>

<div class="box">Security/Compliance</div>

<div class="arrow">-\></div>

<div class="box-soft">Kubernetes case</div>

</div>

</div>

## Output Shape

The document is intentionally split into chapter-level HTML files. This keeps each chapter easy to review, edit, and replace while the index page remains the entry point.

## Operations Guides

Standalone installation and operations guides for the HA metadata dependencies are also available in this HTML edition. They use the shared architecture stylesheet while remaining separate runbook-style documents.

| Guide | Operational Focus |
|----|----|
| [NAMRBD installation guide](../installation-guide.md) | Standard install, build, multi-node bring-up, gateway/SBS/kernel/CSI/iSCSI install flow, and install verification gates. |
| [Developer build path](../installation-guide.md#2-developer-build-and-test) | Community source layout, command binary builds, repo-local test caches, and edition-boundary checks for GitHub contributors. |
| [NAMRBD admin guide](../admin-guide.md) | Component ownership, day-2 operations, observability, release guardrails, Backup/DR, performance, security, iSCSI, Governance/WORM evidence, troubleshooting, security notes, and iSCSI operations. |
| [NAMRBD user manual](../user-manual.md) | Quick start, volume attach/use flow, snapshot/restore, enterprise Backup/DR, performance/security/Governance-WORM boundaries, iSCSI access, Kubernetes user flow, validation commands, and FAQ. |
| [etcd HA cluster install and operations](../etcd-ha-cluster-install-operations-guide.md) | Gateway control-plane authority, Raft quorum, bootstrap, NAMRBD gateway and `namrbdctl` integration, backup, recovery, and member replacement. |
| [TiKV HA cluster install and operations](../tikv-ha-cluster-install-operations-guide.md) | SBS metadata and optional RawKV object-store dependency, PD/TiKV quorum, TiUP deployment, `sbs-service` integration, maintenance, backup, and upgrade flow. |

## Primary Review Questions

| Question | Where To Start |
|----|----|
| Which component owns a state transition? | [Components And Ownership](chapters/02-components-and-ownership.md) |
| How does a logical range become a physical payload object? | [Logical-To-Physical Mapping](chapters/06-logical-to-physical-mapping.md) |
| Why are snapshots and clones safe after overwrite or discard? | [Read Views](chapters/10-read-views-snapshots-and-clones.md) and [Reachability And GC](chapters/11-reachability-and-gc.md) |
| Where does Backup/DR state live? | [Metadata Authority](chapters/04-metadata-authority.md), [Observability](chapters/15-observability-and-harness.md), and [Edition Boundaries](chapters/17-edition-and-release-boundaries.md) |
| Where does security/compliance state live? | [Metadata Authority](chapters/04-metadata-authority.md), [Observability](chapters/15-observability-and-harness.md), and [Interface Specifications](chapters/appendix-interface-specifications.md) |
| Where does iSCSI fit without changing storage authority? | [Interface Specifications](chapters/appendix-interface-specifications.md) and [Edition Boundaries](chapters/17-edition-and-release-boundaries.md) |
| How does Kubernetes use NAMRBD without owning storage semantics? | [Kubernetes/CSI Integration Case](chapters/16-kubernetes-csi-integration-case.md) |
| Where are component interface surfaces summarized? | [Interface Specifications](chapters/appendix-interface-specifications.md) |
| How does NAMRBD resolve tail latency under high concurrent load? | [Extreme I/O Performance Engineering](chapters/18-extreme-io-performance-engineering.md) |

<div class="chapter-nav" markdown="1">

[Start Reading -\>](chapters/00-reading-guide.md)

</div>
