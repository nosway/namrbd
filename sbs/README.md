# SBS Storage Packages

`sbs/` contains the storage authority, node-local implementations, protocol
bindings, and operational views used by NAMRBD gateways and tools.

| Path | Role |
| --- | --- |
| `cluster/` | Cluster repositories and services for control, placement, replication, maintenance, payload reachability, and edition-gated advanced work |
| `local/` | Local SBS client/store implementation used by development and tests |
| `admin/v1` | Generated cluster administration and operation bindings |
| `internalapi/v1` | Generated service-to-service authority bindings |
| `v1` | Generated SBS data-plane volume bindings |
| `observability` | Stable health/operation snapshot assembly |

`sbs-service` is the writer for cluster metadata and transitions. `sbs-data`
owns node-local payload execution. Gateways route through published views and
do not become storage authorities. Preserve generation, idempotency, read-view,
protected-reference, and reclaim contracts together when changing a repository
or maintenance path.

Generated `*/v1/*.pb.go` files follow the `.proto` sources in `proto/`; do not
edit generated bindings by hand.

