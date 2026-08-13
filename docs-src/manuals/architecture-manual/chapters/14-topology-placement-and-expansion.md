Chapter 15

# Topology, Placement, And Expansion

## Topology

- Zone
- Node
- Store
- Placement policy

<div class="summary" markdown="1">

Topology describes where payload can live and what correlated failures the placement policy can tolerate. Expansion describes how logical volume size grows without changing geometry or forcing new payload allocation.

`sbs-service` owns topology records, placement policy, repair, rebuild, rebalance, drain, and expansion metadata. Gateway and kernel own routing and reload behavior, not membership or maintenance authority.

</div>

<figure class="architecture-figure" markdown="1">

![Topology placement diagram showing zones, nodes, stores, placement planning, and maintenance state](../assets/diagrams/topology-placement.svg)

<figcaption>Shared English SVG for topology and placement. Topology describes failure domains; placement and maintenance publish safe metadata transitions.</figcaption>

</figure>

## Topology Terms

| Term | Meaning |
|----|----|
| Zone | Operator-defined primary failure domain. |
| Node | SBS cluster member identity for one `sbs-data` service endpoint. |
| Store | Local payload store managed by an `sbs-data` node. |
| Placement policy | Strict or prefer behavior for preserving failure-domain spread. |

<div class="diagram" markdown="1">

<div class="diagram-title">Placement hierarchy</div>

<div class="flow" markdown="1">

<div class="box-accent">zone</div>

<div class="arrow">contains</div>

<div class="box">nodes</div>

<div class="arrow">each with</div>

<div class="box">stores</div>

<div class="arrow">used by</div>

<div class="box-soft">replica or EC placement</div>

</div>

</div>

## Placement

Replicated and EC placement both consume the same topology model. Replicated placement spreads replicas across zones when policy requires it. EC placement must keep shard counts within the parity tolerance of the selected EC profile and satisfy the selected failure-domain policy before it publishes placement.

## Topology Installation Workflow

A typical installation declares zones first, joins nodes with a zone assignment, validates the topology, then creates volumes whose placement policy consumes that topology. Store details are reported by `sbs-data` and admitted through node membership and store health; topology does not become a separate kernel or gateway source of truth.

    sbsctl topology zone create --zone zone-a
    sbsctl topology zone create --zone zone-b
    sbsctl topology zone create --zone zone-c

    sbsctl node join --node-id data-01 --zone zone-a
    sbsctl node join --node-id data-02 --zone zone-b
    sbsctl node join --node-id data-03 --zone zone-c

    sbsctl topology validate --output json
    sbsctl topology summary --output json
    sbsctl volume create --failure-domain zone --topology-mode strict ...

Operational workflows may later use `sbsctl topology zone update --zone <zone> --disable` to stop new placement into a zone, `sbsctl node update-topology --node-id <node> --zone <zone>` for controlled reassignment, and maintenance commands to drain or rebalance data after preflight proves the post-change topology remains safe.

Membership operations should be run as plan, preflight, apply, synchronize, verify, rollback, and audit. SBS node join, topology update, store tuning, drain, remove, and force-remove are `sbs-service` AdminService authority. Gateway membership and liveness remain gateway/control-plane authority. Basic iSCSI status can report portals, targets, LUNs, initiators, and sessions, but it is not cluster-wide iSCSI HA authority. Operator views should use `/api/v1/membership/status` and preserve `source_authority`, freshness, warning/error, RBAC, redaction, and unsupported-claim fields.

GUI and MCP membership integrations start as read-only proposal surfaces. They may summarize intended changes and evidence, but applying membership changes requires the owning product API, a human approval point, rollback guidance, and an audit record. Force-remove remains a break-glass operation because it can sever the normal evidence chain.

## Maintenance State Machines

| Operation | States | Decision Point |
|----|----|----|
| Rebuild | `queued -> preflight -> reconstructing/copying -> verifying -> committing -> complete` | Target placement must preserve failure-domain policy before replacement refs are published. |
| Scrub | `queued -> scanning -> verifying -> repair_needed|clean -> complete` | Scrub reports corruption or drift without changing logical contents unless it enters a repair/rebuild path. |
| Rebalance | `queued -> planning -> moving -> verifying -> committing -> complete` | Do not accept a byte-better move that weakens strict topology or breaks node/store spread. |
| Drain | `requested -> impact_report -> moving -> verifying -> committing -> drained`, or `blocked`. | Preflight must explain unsafe impact before copying starts. Weak mode, if used, must be explicit and observable. |

## Grow-Only Expansion

<div class="diagram" markdown="1">

<div class="diagram-title">Online expansion sequence</div>

<div class="flow" markdown="1">

<div class="box-accent">sbsctl volume expand</div>

<div class="arrow">-\></div>

<div class="box">SBS metadata size grows</div>

<div class="arrow">-\></div>

<div class="box">gateway reload-size</div>

<div class="arrow">-\></div>

<div class="box-soft">kernel device resize</div>

</div>

</div>

Expansion is not a fencing event. It preserves attachment identity and generation, avoids geometry mutation, and does not materialize payload for the new range. Newly exposed ranges read as zero until written.

[\<- Previous](13-kernel-gateway-dataplane.md) [Next: Observability And Harness -\>](15-observability-and-harness.md)
