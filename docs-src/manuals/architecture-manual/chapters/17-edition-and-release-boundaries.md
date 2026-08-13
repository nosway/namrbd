Chapter 18

Edition boundary: Community edition baseline and Enterprise edition only capability boundaries are both present.

# Edition And Release Boundaries

## Boundary

- Shared surfaces
- Enterprise capabilities
- Release guardrails

<div class="summary" markdown="1">

Edition and release boundaries keep architecture surfaces clear. Shared capabilities such as topology, expansion, host/gateway/SBS layering, core metadata rules, replicated snapshot/restore, and replicated discard should stay visible in the community baseline. Enterprise capabilities such as EC, user-visible clone/materialize, Backup/DR automation, advanced DR/security/performance tiers, and migration/repack must be exposed only through intended surfaces.

Supported product surfaces are `namrbdctl`, `sbsctl`, `namrbd-debug`, admin APIs, gateway APIs, and CSI where applicable. Release scans should prevent unsupported command surfaces or source trees from reappearing as active product interfaces.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Release Boundary Concept</div>

<div class="flow" markdown="1">

<div class="box-accent">shared architecture</div>

<div class="arrow">exposes</div>

<div class="box">community surfaces</div>

<div class="arrow">extends through</div>

<div class="box">enterprise capabilities</div>

<div class="arrow">guarded by</div>

<div class="box-soft">release scans + harness evidence</div>

</div>

</div>

## Capability Boundary

| Area | Boundary |
|----|----|
| Core Linux path | Shared platform architecture: kernel, gateway, SBS service/data, metadata stores. |
| Topology | Shared because replicated and EC placement both need explicit failure domains. |
| Expansion | Shared grow-only, geometry-preserving size semantics. |
| EC backend | Enterprise capability in the current baseline. |
| Snapshot/restore | Manual replicated snapshot, manual restore-from-snapshot, read-only snapshot/read-view safety, restore-size validation, and basic delete/reference guardrails are community baseline features. |
| Clone/materialize | User-visible clone, materialize, flatten, and repack automation remain enterprise capabilities. |
| Backup/DR automation | Backup/DR target/policy/run/artifact/hold/status APIs, remote DR replication-link/recovery-point/shipping-manifest/shipping-worker state, and later remote DR automation are enterprise-only. The current DR control-plane state does not claim remote transfer completion, standby import, promote/demote, failover, or public DR CLI support. Automated QA evidence is a release gate rather than a community product surface. Current release evidence covers Windows, cluster-wide QoS, and kernel/StorageClass claim rows; iSCSI HA remains a deferred enterprise HA follow-up, preferably MPIO-linked, with VIP handoff only as a last-resort fallback for non-MPIO environments. |
| Security/Compliance | Security provider, policy, data-key, lease, rotation, audit, crypto erase, and encrypted backup artifact surfaces are enterprise-only. The shared kernel/gateway/SBS architecture remains edition-neutral, but key authority and compliance workflows do not become community command surfaces. |
| Governance/WORM | Governance/WORM is officially limited to scoped support: block-native derived objects, local Governance/WORM fixtures, and userspace gateway sealed-target write rejection. Public governance API/CLI, ordinary live-volume WORM, regulatory certification, S3/Azure API compatibility, kernel/iSCSI/NVMe protected-state support, ransomware recovery support, and remote DR support are not claimed. |
| Kubernetes/CSI | Adapter surface whose capabilities must reflect backend edition support. |
| Operations observability, GUI, and MCP | Community includes read-only health/status, replicated SBS observability, capacity/reclaim evidence views, operation summaries, warnings, membership status, basic iSCSI status within the three-volume export cap, the read-only `/console/` operations dashboard, GUI view descriptors, observe-first MCP tool descriptors, and evidence bundles that exclude secrets and private deployment paths. Enterprise-only scope includes EC advanced analytics, Backup/DR attribution, dedupe/repack, advanced security/audit, iSCSI HA, MPIO/ALUA, large-scale trend analytics, and any approved mutating GUI/MCP workflow. |

## Supported Command Surfaces

<div class="grid" markdown="1">

<div class="mini-card" markdown="1">

### `namrbdctl`

Host, gateway, attachment, and kernel-facing control workflows.

</div>

<div class="mini-card" markdown="1">

### `sbsctl`

SBS cluster, volume, topology, snapshot, restore, maintenance, and enterprise backup/security workflows.

</div>

<div class="mini-card" markdown="1">

### `namrbd-debug`

Low-level validation, inspection, and focused developer/debug workflows.

</div>

<div class="mini-card" markdown="1">

### Admin APIs

Authoritative backend operations used by operators, tooling, and CSI translation.

</div>

</div>

## Backup/DR Boundary <span class="edition-boundary-inline">Enterprise edition only</span>

Product-state APIs persist enterprise Backup/DR targets, policies, runs, restore-drilled artifact availability, retention holds, purge dry-run guardrails, and status in `sbs-service`. The remote DR control plane adds DR replication-link, recovery-point, shipping-manifest, and shipping-worker admission state plus `sbsctl dr link`, `sbsctl dr recovery-point`, `sbsctl dr shipping-manifest`, and `sbsctl dr shipping-worker` inspection. They do not add a community backup scheduler, destructive purge executor, encryption feature, or remote DR automation.

Fixture summaries and lab closure artifacts are verification evidence, not product state. Product state is visible through enterprise `sbsctl backup`, `sbsctl dr link`, `sbsctl dr recovery-point`, and `sbsctl dr shipping-manifest` commands and the `sbs.admin.v1.AdminService` Backup/DR RPC group.

## Security/Compliance Boundary <span class="edition-boundary-inline">Enterprise edition only</span>

Product-state APIs persist enterprise security providers, policies, volume bindings, data keys, key-access leases, rotation plans, audit events, crypto erase plans, and encrypted backup artifact evidence in `sbs-service`. Gateways consume this authority for attach admission and encrypted read/write/restore paths; encrypted payload metadata carries key identity and key version, not plaintext key material.

Security validation evidence must identify the provider mode, deployed gateway/SBS build, kernel module state, key admission path, backup artifact restore/readback path, crypto erase behavior, and rotation/re-encrypt worker result. Fixture-provider evidence closes only the fixture-provider product boundary. Live external KMS network credentials, external-provider destroy evidence, and broader kernel readback remain conditional follow-up claims rather than Community or baseline release promises.

## Release/Access Boundary

A release/access package is ready only when the exact checkout records `release_access_closure_ready=true`, `closure_blockers=[]`, feature regression results, kernel/Kubernetes regression results, and long-running soak evidence. Windows SBS-backed basic I/O, QoS cluster-volume lease durability, and kernel/StorageClass performance exposure are support claims only when their current evidence rows are present.

iSCSI HA is intentionally not part of the current support claim. NAMRBD's preferred future iSCSI HA shape is MPIO-linked multi-portal access to one LUN identity. Active/passive VIP handoff remains a fallback validation path for environments where iSCSI MPIO cannot be used.

## Governance/WORM Boundary <span class="edition-boundary-inline">Enterprise edition only</span>

Governance/WORM scoped support requires a signoff record with `governance_worm_closed=true`, `governance_worm_closure_type=scoped_support_signoff`, `support_claimed=true`, `compliance_claimed=false`, and `remote_dr_product_slices_require_remote_dr_evidence=true`.

The admitted scope is narrow: block-native Governance/WORM controls for derived objects and userspace gateway sealed-target write rejection. The validation record should include `gateway_live_smoke_result=ok`, whether a remote lab was used, who executed it, `sealed_response_status=409`, and `rejection_code=worm_sealed_read_only`.

Scoped Governance/WORM support does not claim SEC/FINRA, MiFID/FCA, or HIPAA certification; S3 Object Lock or Azure Blob immutable storage API compatibility; ordinary writable live-volume WORM semantics; public governance API/CLI registration; kernel, iSCSI, or NVMe/TCP protected-state support; ransomware recovery support; or remote DR support.

## Release Guardrails

Release checks should confirm that active build, smoke, docs, and export surfaces point at current commands and APIs. The goal is not merely to compile binaries; it is to keep the published interface aligned with the authority model explained in this document. Release evidence should show that public-facing features match their edition boundary, command surfaces route to the current authority owner, and unsupported surfaces are excluded.

## Boundary Review Questions

| Question | Expected Answer |
|----|----|
| Does this feature change shared metadata truth? | Shared truth must remain readable across editions, even when an enterprise-only backend descriptor is present. |
| Does this command expose enterprise behavior? | The command must be gated, documented, and validated as enterprise rather than appearing as an accidental community surface. |
| Does CSI advertise the capability? | CSI capability output must follow the selected edition/backend, not merely the driver's compiled code path. |
| Does release evidence prove the boundary? | Release guardrails should show included commands, excluded surfaces, active docs, and skipped or executed lab gates. |

[\<- Previous](16-kubernetes-csi-integration-case.md) [Next: Glossary -\>](appendix-glossary.md)
