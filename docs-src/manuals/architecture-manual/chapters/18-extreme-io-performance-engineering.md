Chapter 19

Status: engineering and research guidance; no public benchmark claim

# Performance Engineering And Benchmark Discipline

## Chapters

1.  [1. Reading Guide](00-reading-guide.md)
2.  [2. Platform Overview](01-platform-overview.md)
3.  [3. Components & Ownership](02-components-and-ownership.md)
4.  [4. Linux Host Control & Data Plane](03-linux-host-control-and-data-plane.md)
5.  [5. Metadata Authority](04-metadata-authority.md)
6.  [6. Logical Storage Geometry](05-logical-storage-geometry.md)
7.  [7. Logical to Physical Mapping](06-logical-to-physical-mapping.md)
8.  [8. Replicated Backend](07-replicated-backend.md)
9.  [9. Erasure Coding Backend](08-erasure-coding-backend.md)
10. [10. Write Visibility & Ordering](09-write-visibility-and-ordering.md)
11. [11. Read Views & Clones](10-read-views-snapshots-and-clones.md)
12. [12. Reachability & GC](11-reachability-and-gc.md)
13. [13. Zero, Discard & Reclaim](12-zero-discard-and-reclaim.md)
14. [14. Kernel Gateway Dataplane](13-kernel-gateway-dataplane.md)
15. [15. Topology & Placement](14-topology-placement-and-expansion.md)
16. [16. Observability & Validation](15-observability-and-validation.md)
17. [17. Kubernetes CSI Integration](16-kubernetes-csi-integration-case.md)
18. [18. Edition & Release Boundaries](17-edition-and-release-boundaries.md)
19. [19. Performance Engineering](18-extreme-io-performance-engineering.md)

<div class="summary" markdown="1">

This chapter defines how NAMRBD performance work should be measured and
reported. The v1.0 public release makes no general IOPS, bandwidth, latency, or
CPU-efficiency claim. A result is meaningful only for the exact build,
topology, durability mode, workload, and failure state that produced it.

</div>

## 1. Current Claim Boundary

NAMRBD has multiple data paths and experimental operating modes. Their presence
in source does not establish production support or a performance advantage.
Published results must identify at least:

- the commit or immutable image digest;
- userspace gateway, kernel, CSI, or protocol path;
- replicated or erasure-coded backend and the exact geometry;
- node and gateway count, CPU, memory, network, and storage media;
- block size, read/write mix, queue depth, concurrency, and test duration;
- flush, fencing, quorum, and durability semantics;
- p50, p95, and p99 latency together with throughput and error count; and
- process restarts, dependency loss, degraded modes, and excluded paths.

A local quickstart, synthetic fixture, or model-based scale test is useful
engineering evidence, but it is not a public deployment benchmark.

## 2. Correctness Before Throughput

Every performance experiment must preserve the same storage contracts as the
baseline path. At minimum, the test should verify write visibility, flush and
ordering, fencing, restart recovery, replica consistency, snapshot/read-view
immutability when applicable, and payload reference safety. A faster result
that weakens one of those contracts is a different behavior, not an
optimization of the same behavior.

Performance reports should keep successful operations and errors separate and
record the first and last error. Tail latency must not be hidden behind an
average, and a retrying request must not be counted as a clean first-attempt
success.

## 3. Bottleneck Classification

| Area | Evidence to collect | Typical investigation |
| --- | --- | --- |
| Gateway CPU | CPU profile, allocation rate, request parsing and copy cost | Reduce allocations or copies only after proving the affected call path. |
| Metadata round trip | request/transaction latency, conflict and retry counts | Separate normal contention from stale-writer or fencing failures. |
| Payload I/O | per-replica latency, quorum completion, slow-node distribution | Check device, network, and replica-tail behavior without weakening durability. |
| Queueing and contention | queue depth, scheduler delay, mutex/block profile | Narrow the contention domain; avoid broad hot-path serialization. |
| Background work | GC, reconciliation, watch, resync, and maintenance counters | Bound work per pass and keep standing scans distinct from on-demand risk. |
| Coding or transformation | CPU profile and bytes transformed per operation | Compare scalar and accelerated paths using identical data-protection semantics. |

## 4. Research Candidates

Techniques such as shared-memory rings, lock-free queues, cache-line alignment,
zero-copy transfer, `io_uring` polling, asynchronous flush pipelines, and SIMD
coding may be investigated when profiling identifies a matching bottleneck.
They are not described here as implemented NAMRBD capabilities. In particular,
this repository does not claim syscall-free I/O, zero-percent CPU overhead,
AVX-512 or NEON acceleration, or a fixed IOPS level.

Before any candidate becomes a product or Enterprise claim, the implementation
must be visible in the relevant source boundary, have a correctness regression
gate, include a portable fallback where required, and be measured on a stated
topology. Experimental results must remain labeled experimental until the
corresponding release evidence exists.

## 5. Reporting Checklist

Use the following outcome categories consistently:

- **Implemented:** the named source path exists and focused tests exercise it.
- **Measured:** an artifact records the exact environment, workload, raw
  samples, errors, and summary statistics.
- **Validated:** correctness and performance gates passed for the stated
  release path.
- **Supported:** the capability appears in the release support boundary with
  matching evidence and known limits.

Do not infer a later category from an earlier one. This keeps engineering
experiments useful without turning them into accidental product commitments.

[\<- Previous](17-edition-and-release-boundaries.md) [Next: Glossary -\>](appendix-glossary.md)
