Appendix A

Edition boundary: Community edition terms and Enterprise edition only feature terms are both present.

# Glossary

## Appendix

Current terminology for architecture review.

<div class="summary" markdown="1">

This glossary lists the current terms used by the architecture chapters. Use these names in new docs, logs, API text, and review comments unless a compatibility field is being quoted exactly.

Use Allocation Chunk for logical units, reserve Replica Physical Chunk for replicated backend payloads, and distinguish EC stripes from EC shards.

</div>

## Glossary Groups

<div class="grid" markdown="1">

<div class="mini-card" markdown="1">

### Logical mapping

Allocation Page, Allocation Chunk, AllocationEntry, zero state, PhysicalObjectRef.

</div>

<div class="mini-card" markdown="1">

### Backend payload

Physical Object, Replica Physical Chunk, EC Stripe, EC Shard, local store object.

</div>

<div class="mini-card" markdown="1">

### Read views

Live volume, Snapshot Root, Clone Delta, materialized clone, flatten.

</div>

<div class="mini-card" markdown="1">

### Operations

Discard, zero fallback, reclaimable object, rebuild, scrub, rebalance, drain.

</div>

<div class="mini-card" markdown="1">

### Backup/DR

Backup Target, Backup Policy, Backup Run, Backup Artifact, Recovery Point, Restore Drill, DR Replication Link, DR Shipping Manifest.

</div>

<div class="mini-card" markdown="1">

### Performance/Ops

Performance Policy, Cap Scope, Budget Lease, Restore Warmup State, Diff Index, Guarded EC Journal.

</div>

</div>

| Term | Meaning |
|----|----|
| Allocation Chunk | Logical volume allocation unit inside an Allocation Page. |
| Allocation Page | Logical metadata page containing AllocationEntries. |
| Placement Extent | Placement, replica-set, and failure-domain planning unit. |
| AllocationEntry | Mapping from a logical range to zero state or a PhysicalObjectRef. |
| Physical Object | Backend-neutral persisted payload object. |
| PhysicalObjectRef | Metadata reference to a Physical Object plus opaque backend descriptor. |
| Replica Physical Chunk | Replicated backend payload chunk. |
| EC Stripe | Encoded object group for an EC backend range. |
| EC Shard | Data or parity shard inside an EC stripe. |
| Read View | Explicit identity used to resolve live, snapshot, clone, or materialized reads. |
| Snapshot Root | Immutable allocation metadata captured at snapshot cut. |
| Clone Delta | Clone-owned mapping that overrides base snapshot data. |
| Backup Target | Enterprise Backup/DR destination abstraction for artifact manifests and backup object chunks. The initial product boundary is local validation target support, not released remote object-store DR. |
| Backup Policy | `sbs-service`-owned enterprise control record for scheduled snapshot/backup intent, retention rules, dry-run planning, and next-run observability. |
| Backup Run | Service-owned operation record for one backup attempt. A run may create or update an artifact, but it is not itself proof that a recovery point is available. |
| Backup Artifact | Manifest plus target objects copied from a source snapshot/read-view. It becomes `available` only after integrity recheck plus userspace and kernel restore readback evidence. |
| Recovery Point | Operator-visible protected point that can restore a new ordinary volume. Backup/DR counts it as successful only after the backing artifact is available and protection state is visible. |
| Restore Drill | Backup/DR validation that restores a backup artifact into an ordinary volume and reads it back through the required userspace and, when applicable, kernel path before artifact availability is advertised. |
| Artifact Availability | State transition that marks a backup artifact as usable for successful recovery-point reporting. It is separate from copied or integrity-checked artifact states. |
| Changed-Block Listing | Correctness-first list of changed logical ranges between a base and head read-view. It must include ambiguous resolver state conservatively and does not require a fast diff index. |
| Retention Hold | Backup/DR control record that blocks purge planning for a protected artifact or snapshot reference. |
| Backup Purge Plan | Dry-run plan that separates protected references, blocked destructive actions, recycle-bin state, and explicit purge candidates before any artifact, snapshot, or payload delete is allowed. |
| Backup/DR Status | Product-state summary from `sbs-service` covering recovery point age, artifact availability, restore drill result, protected bytes, delete protection, and edition leakage status. |
| DR Replication Link | remote DR product control-plane record that binds source cluster, target cluster, source volume, and target standby volume identity. U-CTRL-003A can mark shipping-worker admission while standby import, promote, and failover support remain false. |
| DR Shipping Worker | remote DR product control-plane record that admits a worker against a bound DR shipping manifest and records heartbeat, endpoint, credential boundary, and transfer-plan observables without claiming remote transfer completion. |
| DR Shipping Manifest | remote DR product control-plane record that binds a recovery point to manifest integrity, payload roots, read-view identity, key policy, and governance metadata before remote transfer is claimed. |
| Security Provider | Security/Compliance key-provider authority record. The current closed product boundary includes fixture/provider-backed metadata and redacted health evidence; live external KMS network credentials remain conditional follow-up evidence. |
| Security Policy | Enterprise Security/Compliance policy record that binds encryption requirement, scope, cipher suite, rotation policy, disabled-key behavior, and audit requirement to volumes through service-owned bindings. |
| Data Key | Per-policy or per-volume key record with data-key id, provider id, key id, key version, generation, state, and redacted wrapped reference. Plaintext key bytes are not persisted in metadata or summaries. |
| Key Access Lease | Short-lived `sbs-service` authority that admits read, write, restore, backup, or rotation use of a data key version. Gateway unwrap requests must match an active lease and requester. |
| Data-Key Rotation | Security/Compliance transition that preserves old key-version reads, advances new writes to the target key version, records re-encrypt progress/resume evidence, and keeps allocation headers aligned with the active key version. |
| Crypto Erase | Security/Compliance terminal key-authority action that destroys data-key access only after protected references, leases, rotations, backup artifacts, holds, and active attachments allow it. Post-erase gateway/SBS reads must fail closed. |
| Performance Policy | Enterprise Performance policy record or fixture summary describing performance tier, IOPS/bandwidth caps, burst allowance, cap scope, throttle mode, and foreground priority. |
| Volume Performance Binding | Association between a volume and a performance policy generation. It makes the effective policy explicit without moving read-view or metadata commit authority into the gateway. |
| Cap Scope | Performance label for where an I/O cap is authoritative: fixture-only, per-gateway, or cluster-volume. A cluster-volume cap requires a shared `sbs-service` budget authority. |
| Throttle Mode | Performance admission behavior for over-cap requests. `wait` delays before dispatch, while `reject` returns a throttle-specific error before dispatch. |
| Shared Budget Lease | Short-lived `sbs-service` grant of foreground budget tokens and bytes for a volume, budget class, and window. Gateways consume it before dispatch when `cap_scope=cluster_volume`. |
| Background Work Budget | Performance budget view for repair, rebuild, scrub, backup copy, restore warmup, and diff-index work. Live metadata mutation exists for maintenance-owned concurrency and selected background classes, but it must not imply every worker has live budget enforcement. |
| Restore Warmup State | Access-cost readiness state for a Backup/DR-valid restored volume, such as `cold`, `warming`, `ready`, `failed`, or skipped/disabled. Worker-scaffold runs can advance metadata readiness, but the state does not make a backup artifact successful. |
| Diff Index | Optional Performance changed-range metadata acceleration record keyed by read-view identity and coverage. Complete indexes may accelerate only after validation and a later product fast-path gate; partial, stale, or missing indexes fall back, under-copy is rejected, and scanner-scaffold records keep product acceleration disabled. |
| Guarded EC Journal | Guarded Performance EC performance concept for same-stripe batching or service-owned write journaling. Live control-plane intent can be recorded, but it is not product-active or a product tier until correctness, replay, reachability, multi-gateway, backup, and diff-index gates pass. |
| Closure Evidence | Validation result package that records ok/error counts, first and last error, deploy/restart state, skipped/cached/required validation gates, and observability fields. It is validation evidence, not product metadata. |
| Reclaimable Object | PhysicalObjectRef absent from authoritative roots and eligible for backend delete. |
| Zone | Operator-defined primary SBS failure domain. |
| Node | SBS cluster member identity for one `sbs-data` endpoint. |
| Store | Node-local payload store managed by `sbs-data`. |

[\<- Previous](17-edition-and-release-boundaries.md) [Next: Reference Map -\>](appendix-reference-map.md)
