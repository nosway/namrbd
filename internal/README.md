# Internal Shared Packages

Go's `internal` rule restricts these packages to this module. They provide
shared implementation details and are not a compatibility promise for external
consumers.

| Package | Role |
| --- | --- |
| `adminclient` | Leader-aware SBS admin gRPC client and membership projection |
| `cliux` | CLI aliases and presentation helpers |
| `csi/driver` | CSI request translation and node/controller implementation |
| `depavail` | Dependency availability model, health reporting, and thresholds |
| `depbudget` | Inventory and budget model for metadata dependency access |
| `envcompat` | Reviewed environment-variable compatibility behavior |
| `mcpops` | Observe-first MCP operation descriptors, redaction, and errors |
| `sbsdataclient` | Client for node-local SBS data operations |
| `serviceconfig` | Shared YAML schema, loading, validation, overrides, and reload policy |
| `structuredlog` | Common structured logging helpers |
| `tikvopts` | TiKV client and TLS option mapping |

Avoid turning `internal/` into a generic utility bucket. Put ownership-specific
logic with its owning product package, keep secrets out of logs and errors, and
add package tests for configuration precedence, retries, and transport mapping.

