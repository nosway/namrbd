# ADR 0003: Qualify The Userspace Gateway First

- Status: Accepted
- Date: 2026-08-25
- Deciders: NAMRBD maintainers
- Supersedes: none
- Superseded by: none

## Context

NAMRBD can expose block I/O through a userspace gateway API and through an
out-of-tree Linux kernel module that forwards block requests to gateways. The
kernel path adds kernel-version and header compatibility, block-layer retry and
timeout behavior, persistent connection management, module packaging, host
privilege, and crash/reload risk. Those concerns can obscure defects in the
storage consistency contract itself.

The userspace path exercises volume metadata, gateway admission, SBS routing,
payload I/O, generations, idempotency, snapshots, discard, and observability
while remaining easier to run under ordinary Go tests and process-level tools.

## Decision

Qualify the replicated userspace gateway path as the primary release and
correctness baseline before making a kernel datapath support claim. New storage
semantics must first have userspace package tests and end-to-end evidence.

Keep the kernel module source and its interface contract, but treat buildability
and runtime I/O support as separate claims. Kernel validation must bind kernel
version, headers, module version, gateway protocol, attachment ID, generation,
path-plan revision, device size, and runtime path status.

This decision is an evidence order, not a plan to remove the native block-device
path.

## Alternatives Considered

### Kernel-first qualification

This validates the final Linux block interface earliest, but failures span
kernel, network, gateway, and SBS layers, making storage-contract diagnosis and
portable CI substantially harder.

### Userspace-only product

This reduces host integration cost but gives up the native block-device path
that is a core NAMRBD design goal.

### Qualify both paths as one release gate

This appears comprehensive but lets platform availability block basic storage
iteration and risks treating one path's evidence as proof for the other.

## Consequences

### Positive

- Storage correctness and gateway/SBS ownership can be tested without loading a
  kernel module.
- CI and developer feedback are faster and work on non-Linux development hosts.
- Kernel support statements remain tied to explicit platform evidence.

### Negative And Operational Cost

- The primary release path does not by itself prove `/dev/namrbdX` behavior.
- Kernel regressions can lag userspace feature work unless a dedicated Linux
  matrix is maintained.
- Operators need a clear distinction between source availability, successful
  compilation, and validated runtime I/O.

## Validation And Revisit Triggers

Every feature claim identifies the path that produced its evidence. Kernel
qualification uses Linux build and I/O tests rather than inheriting userspace
status. Revisit when the kernel matrix and automated attach/I/O/failover tests
are broad and reliable enough for kernel and userspace paths to share a release
gate without hiding failures.

