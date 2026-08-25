# Command Entry Points

`cmd/` contains executable composition and CLI wiring. Business rules should
normally live in `gateway/`, `sbs/`, `iscsi/`, or `internal/`; a command parses
configuration, constructs dependencies, starts listeners, and maps errors to a
process or CLI exit contract.

| Directory | Role |
| --- | --- |
| `namrbd-gateway` | Gateway control, HTTP, and persistent dataplane process |
| `namrbdctl` | Host/gateway volume and attachment CLI |
| `sbs-service` | Cluster metadata, admin, placement, and maintenance authority |
| `sbs-data` | Node-local SBS volume/payload execution service |
| `sbsctl` | SBS administration, status, snapshot, maintenance, and iSCSI CLI |
| `namrbd-csi-driver` | Kubernetes CSI Identity, Controller, and Node process |
| `namrbd-iscsi-gateway` | Standard iSCSI target gateway process |
| `namrbd-mcp` | Observe-first operations MCP provider |
| `namrbd-debug` | Internal diagnostic binary; not a shipped public command |
| `ec-codec-bench` | Internal codec benchmark utility |
| `namrbd-iscsictl` | Deprecated/local iSCSI control utility; public users use `sbsctl iscsi` |

Community and Enterprise command surfaces are selected at build time. A
Community binary must not register Enterprise-only top-level commands, flags,
configuration, or help.

Test a command package directly while iterating, then run the edition-appropriate
repository gate. Keep machine-readable output on stdout and diagnostics on
stderr.

