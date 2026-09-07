# Platform Capabilities

The public NAMRBD source tree includes the replicated storage core and its
operator-facing integrations:

- `namrbd-gateway`
- `namrbdctl`
- `sbs-service`
- `sbs-data`
- `sbsctl`
- `namrbd-debug`
- `namrbd-mcp`
- `namrbd-iscsi-gateway`
- `namrbd-csi-driver`
- Linux kernel block/control module source
- Kubernetes CSI manifests under `deploy/kubernetes/csi`
- Local SBS quickstart assets under `examples/quickstart`
- Public observability assets under `deploy/observability`
- Strict cluster manifest validation, canonical export/render/plan, signed host
  preflight admission, and file-backed rollout/standby/maintenance state
- Bounded SBS cluster aggregates plus revision-pinned node/volume page and
  point views, including explicit stale/partial/rebuild-required state

These entries describe what is present in the public source tree. They do not
promote every integration to a supported v1.1 release surface. The
[Feature Status](feature-status.md) page records the current validation and
support boundary, and summarizes advanced Enterprise work separately.

The shipped exact logical 160-node manifest is a deterministic software and
workflow fixture. A remote qualification also ran 160 data processes across
18 physical hosts. Neither result qualifies 160 independent physical servers
or creates a public scale/performance/support promise; that requires a
dedicated physical fleet-scale qualification.
