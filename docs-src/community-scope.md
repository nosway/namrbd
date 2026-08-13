# Community Scope

Community Edition includes the replicated storage core:

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

Enterprise-only behavior is not claimed by the Community source tree. Examples
include EC-backed product availability, automated Backup/DR, advanced mobility,
dedupe workflows, security/KMS/encryption product surfaces, performance-tier
controls, and advanced iSCSI HA/scale operations.
