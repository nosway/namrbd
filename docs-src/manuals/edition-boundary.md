# Community and Enterprise Edition Boundary

NAMRBD uses edition labels to describe product capability boundaries, not the
visibility of an individual source file. The Community edition is the public,
replicated-storage platform. Advanced capabilities described as Enterprise are
under development and validation and are not general-availability, delivery,
compatibility, performance, or support commitments.

## Edition Summary

| Capability family | Community edition | Enterprise development |
| --- | --- | --- |
| Replicated volumes | Volume lifecycle, topology-aware replicated placement, userspace gateway I/O | Larger operational envelopes and policy integration |
| Snapshots and recovery | Manual replicated snapshot, immutable read view, and restore building blocks | Policy-driven backup targets, runs, retention, restore drills, and recovery evidence |
| iSCSI | Basic single-target access, limited to three distinct exported volumes | Larger fleets, redundant paths, fencing, MPIO, and ALUA |
| Erasure coding | Not registered as a Community command or mutation surface | EC profiles, placement, encoding, scrub, repair, and drain workflows |
| Security and KMS | Public authentication and transport/configuration controls | Provider-backed keys, payload encryption, rotation, audit, and crypto erase |
| Performance and QoS | Public metrics and ordinary maintenance controls | Workload policies, rate control, background budgets, warmup, and guarded acceleration |
| Remote replication and DR | No remote failover support claim | Recovery points, shipping records, standby lifecycle, and failover orchestration |
| Data mobility and repack | No live geometry conversion surface | Target-volume repack with protected-reference verification |
| Deduplication | No foreground or background dedupe surface | Scoped replicated-data post-process/repack dedupe and safe reclaim |

The current, release-specific status remains authoritative in
[Feature Status](../feature-status.md). The detailed Enterprise manuals describe
engineering and operator contracts for work in progress; they do not expand the
published support boundary.

## Required Documentation Label

Use the exact label **`[Enterprise Edition Only]`** wherever a reader could
otherwise mistake an advanced capability for a Community feature.

- Put the label in the first visible callout on every Enterprise-only page.
- Add it to a section heading when only that section is Enterprise-only.
- Add it in a table cell when Community and Enterprise behavior are compared.
- Put it immediately before an Enterprise-only command, API, configuration
  field, metric, or example.
- Follow the label with an availability statement such as “under development
  and validation.” Do not use “supported,” “production ready,” or “generally
  available” without release evidence that explicitly authorizes that claim.

Standard page callout:

> **[Enterprise Edition Only]** This capability is under development and
> validation. The page is an engineering and operations contract, not a
> general-availability or support commitment.

Standard mixed-page section:

```markdown
## [Enterprise Edition Only] Policy-driven backup

This capability is under development and validation for the Enterprise
edition.
```

Do not use a small footer, color alone, or an unexplained lock icon as the only
edition marker. The boundary must remain visible in plain text, copied snippets,
screen readers, and rendered monochrome output.

## CLI Behavior When A Capability Is Disabled

Community binaries do not register Enterprise-only top-level commands, flags,
configuration keys, or help entries. A user invoking one therefore receives
the normal unknown-command or usage failure and a non-zero exit status. The
Community reference must not advertise an Enterprise command merely to print a
license message.

Shared commands must apply these rules:

1. Do not accept an Enterprise-only flag and silently ignore it.
2. Do not convert an EC, encrypted, deduplicated, or remote-replica request into
   a replicated or plaintext operation.
3. If read-only inspection of a shared resource format is safe, identify the
   required capability without mutating the resource.
4. Reject attach, open-for-write, repair, migration, deletion, reclaim, or
   policy mutation when the required capability is unavailable.
5. Return a stable error that names the edition and required capability. Do not
   expose internal milestone names in a user-facing error.

Recommended error shape:

```text
operation requires NAMRBD Enterprise edition (required capability: erasure-coding)
```

Automation must key on a structured error code or capability field when one is
available, not on the human-readable sentence.

## API Behavior When A Capability Is Disabled

An Enterprise-only API should be absent from the Community listener, service
registration, reflection output, generated Community reference, and discovery
response. If an edition-neutral API accepts a resource that carries a required
capability, it must fail closed before any state change.

The error response should identify:

- a stable unavailable-capability code;
- the operation and resource identifier;
- the required capability;
- whether read-only inspection remains possible; and
- a correlation or request identifier for audit and support.

The transport-specific status code is part of that API's reviewed contract.
Clients must not infer that every Enterprise rejection uses the same HTTP or
gRPC status. Retrying without changing edition, license, or capability state is
not a recovery action.

## Mixed-edition Safety

Mixed-edition deployments must fail fast or enter an explicitly documented
read-only/degraded state. They must never elect a Community process to perform
an Enterprise-only mutation. Before an edition change, record the active volume
geometry, encryption/key state, protected references, attachments, and recovery
points, and prove that the target edition can open every resource safely.

Downgrade is blocked while an Enterprise-only resource or policy remains
active. A successful export, restore, or repack into a Community-supported
replicated format must be verified before the Enterprise state is retired.

## Author Checklist

- Is every advanced page or mixed section marked `[Enterprise Edition Only]`?
- Does the text say “under development and validation” instead of implying GA?
- Are Community commands, APIs, configuration, and help free of Enterprise-only
  names?
- Does the failure path reject mutation without silently changing durability or
  security semantics?
- Are prerequisites, authority, observability, rollback, and known limitations
  stated?
- Is every support or performance statement bound to release evidence?

