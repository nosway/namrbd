Chapter 1

Edition boundary: Community edition reading path and Enterprise edition only architecture topics are both present.

# Reading Guide

## Reader Route

1.  Linux host path
2.  Ownership boundaries
3.  Metadata authority
4.  Storage substrate
5.  Read views and GC
6.  Kubernetes case

<div class="summary" markdown="1">

This first edition is for developers and reviewers who need to understand the current NAMRBD internal architecture. It explains the ordinary Linux block-device path first, then builds up the storage metadata and backend model, and only later treats Kubernetes/CSI as an integration case.

NAMRBD expands to Network Attached Multipath Resilient Block Device and is pronounced \[nae-mur-bee-dee\]. Treat the name as a compact statement of scope: a network-attached block device with multipath and resilience as core design properties.

The name points to four product features: scalable distributed block storage over a network, multipath connections for high availability, automatic recovery from internal component failures where practical, and first-priority support for Linux and Kubernetes (K8S) environments.

Use this guide as an architecture map, not as an installation procedure. Each chapter should make ownership, current authority sources, and compatibility terminology clear in the body text.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Concept map for this edition</div>

<div class="flow" markdown="1">

<div class="box-accent">Platform actors</div>

<div class="arrow">-\></div>

<div class="box">Host control/data plane</div>

<div class="arrow">-\></div>

<div class="box">Metadata authority</div>

<div class="arrow">-\></div>

<div class="box">Logical geometry and mapping</div>

<div class="arrow">-\></div>

<div class="box-soft">Backends, read views, GC, and operations</div>

</div>

</div>

## What This Edition Covers

| Area | Included Focus |
|----|----|
| Linux host path | Kernel module, `namrbdctl`, gateway attach, dataplane path selection, block I/O flow. |
| SBS cluster | `sbs-service` authority, `sbs-data` local execution, TiKV metadata, local Pebble payload stores. |
| Storage substrate | AllocationEntry, PhysicalObjectRef, replicated descriptors, EC descriptors, read-view resolver. |
| Correctness | Metadata commit visibility, snapshot/clone roots, reachability, GC, zero/discard operation identity. |
| Backup/DR | Enterprise Backup/DR target, policy, run, restore-drilled artifact availability, hold, purge-plan, and status control-plane state. |
| Kubernetes | CSI driver as a thin translation layer after the core storage model is clear. |

## Source Priority

When this HTML edition needs a design decision, it follows the current architecture authority before operational guides. The source of truth for public boundaries is the current architecture, interface, and support matrix wording, with Backup/DR, storage, discard, CSI, and snapshot/restore sections taking priority for their respective product behavior. Operational guides are useful name checks, but they do not override the architecture authority.

The reading order keeps Linux/internal architecture ahead of Kubernetes integration and avoids turning compatibility field names into new design precedent. Historical names may appear only when the text is explicitly describing a compatibility or archival surface.

<div class="diagram" markdown="1">

<div class="diagram-title">Architecture reading order</div>

<div class="flow" markdown="1">

<div class="box-accent">Host and gateway</div>

<div class="arrow">-\></div>

<div class="box">SBS authority</div>

<div class="arrow">-\></div>

<div class="box">Logical storage</div>

<div class="arrow">-\></div>

<div class="box">Read views</div>

<div class="arrow">-\></div>

<div class="box-soft">CSI case</div>

</div>

</div>

## Reading Paths

| Reader | Start With | Then Follow |
|----|----|----|
| New architecture reader | Chapters 2-7. | Backends, consistency, read views, GC, and Kubernetes after the storage model is clear. |
| GitHub contributor | [Developer build path](../../installation-guide.md#2-developer-build-and-test), then Chapters 2-5. | Use the ownership boundaries and harness chapter before changing product behavior. |
| Kernel/runtime reviewer | Chapters 4 and 14. | Compare lane/path behavior with Chapter 10 ordering and Chapter 7 metadata visibility. |
| Storage metadata reviewer | Chapters 5-7. | Continue into Chapters 8-13 for backend, read-view, GC, zero, and discard behavior. |
| Operations/release reviewer | Chapters 15-18. | Use Chapter 16 to connect evidence, skipped gates, and release guardrails. |

[\<- Index](../index.md) [Next: Platform Overview -\>](01-platform-overview.md)
