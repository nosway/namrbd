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

These entries describe what is present in the public source tree. They do not
promote every integration to a supported v1.0 release surface. The
[Feature Status](feature-status.md) page records the current validation and
support boundary, and summarizes advanced Enterprise work separately.
