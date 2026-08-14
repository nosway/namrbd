Chapter 16

Edition boundary: Community edition validation fields and Enterprise edition only evidence fields are both present.

# Observability And Validation

## Validation Work Loop

- Contract map
- Observable fields
- Local gates
- Remote validation notes

<div class="summary" markdown="1">

NAMRBD treats code, smoke checks, load checks, logs, docs, deployment state, and regression targets as one validation workflow. Architecture reviews should name the broken contract, touched path, expected observable, and regression risk.

A validation claim is useful only when the observable proves the intended mode was active. JSON-producing scripts write JSON only to stdout; logs and diagnostics go to stderr.

</div>

## Common Observable Fields

| Area | Representative Observables |
|----|----|
| CSI sanity | `upstream_csi_test_version`, capabilities, `ok_count`, `error_count`, first/last error. |
| Discard | `operation`, `policy`, `discard_bytes`, `logical_zero_bytes`, `reclaimable_bytes`, alignment. |
| Topology/EC | EC profile id, zone shard counts, degraded/rebuild state, blocked reason. |
| Kernel/gateway | attachment id, generation, path-plan revision, device size, runtime path status. |
| Backup/DR | `evidence_mode`, policy generation, target id, artifact id, recovery point age, restore drill result, artifact availability, integrity status, protected bytes, retained artifact count, delete-protection status, and community leakage status. |
| Security/Compliance | `security_policy_id`, `key_provider_status`, `data_key_id`, `key_version`, `key_state`, lease purpose, unwrap evidence, rotation state/progress, crypto erase state, plaintext-leak flags, and audit hash-chain status. |
| Closure | required gates, skipped gates, deploy/restart state, first/last error. |

## Read-Only Operations Query Envelope

The operations query surface exposes stable JSON views for tools, reports, GUI screens, and observe-first MCP descriptors. It is not a storage or membership authority; it preserves the authority named by the underlying component and makes stale, partial, or failed collection visible to the caller.

| Field | Meaning |
|----|----|
| `schema_version` | `namrbd.sbs.observability.v1` for SBS cluster observability owned by NAMRBD. |
| `source_authority` | The component/API whose state is being reported, such as `sbs-service` AdminService, gateway control-plane state, or `sbs-data` health detail. |
| `collector_freshness_seconds` | How fresh the assembled view is. Consumers must not hide stale or partial state. |
| `warning_count`, `first_error`, `last_error` | Collection health for incident triage and automated summaries. |
| `rbac_checked`, `tenant_scope_checked`, `redaction_applied` | Safety markers that must be present before the result is shown to operators or AI tools. |
| `read_only_mode_enforced`, `unsupported_claim_visible` | Mutation blocking and unsupported-feature boundaries remain explicit in GUI and MCP views. |

The current Community-safe `sbs-service` URLs include `/api/v1/sbs/cluster`, `/api/v1/sbs/nodes`, `/api/v1/sbs/volumes`, `/api/v1/sbs/maintenance`, `/api/v1/sbs/capacity`, `/api/v1/sbs/reclaim`, `/api/v1/membership/status`, `/api/v1/operations/summary`, `/api/v1/operations/warnings`, `/api/v1/query/views`, `/api/v1/mcp/tools`, `/api/v1/gui/summary`, and `/api/v1/workflow/hardening`. MCP and GUI rows are descriptors for read-only integration; they do not claim a standalone MCP server, a full GUI product surface, or mutation support.

The read-only operations console at `/console/` is a static dashboard served by the same `sbs-service` administration endpoint. It consumes the operations query envelope, uses `/api/v1/sbs/cluster` as its primary snapshot, and visualizes status, topology, capacity, maintenance, warnings, membership authority, and reclaim evidence. It must not read raw storage metadata, scrape logs, or bypass the source authority fields exposed by the API.

Evidence bundles built from this surface should capture product/build identity, source authority and freshness, query snapshots, operation history, warning/error summaries, redaction state, runbook suggestions, and reasons that evidence is unavailable. They are useful for support and incident review only when secrets, tokens, raw payloads, and private deployment paths are excluded.

## Validation Gate Categories

| Category | Purpose | Minimum Evidence |
|----|----|----|
| Static and render gates | Catch syntax, schema, generated artifact, and documentation rendering regressions before runtime work. | Command, changed files, first error if any, and generated output path when applicable. |
| Unit and contract gates | Exercise the smallest package, script function, jq filter, or API contract touched by the change. | Package or fixture name, `ok_count`, `error_count`, and contract-specific observables. |
| Smoke gates | Run a small end-to-end path for attach, I/O, snapshot, restore, discard, EC, topology, or CSI behavior. | Mode fields proving the intended path was active, first/last error, and resulting metadata or device state. |
| Load and soak gates | Exercise performance, concurrency, failover, and long-running behavior without weakening correctness claims. | Run duration, request counts, latency/error summary, active topology/path state, and warning lines. |
| Remote deployment gates | Validate changes that require real gateway, SBS, data-node, kernel, or multi-node topology behavior. | Sync/deploy state, restart state, deployment size, final summary table, and explicit note if a remote validation run was skipped. |

<div class="diagram" markdown="1">

<div class="diagram-title">Validation work loop</div>

<div class="flow" markdown="1">

<div class="box-accent">contract</div>

<div class="arrow">-\></div>

<div class="box">failure mode</div>

<div class="arrow">-\></div>

<div class="box">touched validation surface</div>

<div class="arrow">-\></div>

<div class="box-soft">expected observable</div>

</div>

</div>

Result summaries should include `ok_count`, `error_count`, first error, last error, and enough deploy, restart, remote, or topology state to avoid implying validation that did not happen. Backup/DR validation additionally records whether restore-readback evidence was skipped, cached, or required, and which orchestrator and kernel host roles were used when a remote validation run was actually executed.

Security/Compliance validation uses the same rule. Evidence can prove encrypted payload observability, kernel key admission, encrypted backup artifact restore/readback, crypto erase post-read failure, and data-key rotation paths only for the exact provider, build, and deployment under test. It must not claim live external KMS network use, external-provider destroy, or broader kernel readback unless a later gate explicitly records that evidence.

[\<- Previous](14-topology-placement-and-expansion.md) [Next: Kubernetes/CSI Integration Case -\>](16-kubernetes-csi-integration-case.md)
