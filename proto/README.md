# Protocol Definitions

`proto/` is the source of truth for NAMRBD protobuf service and message
definitions.

| Path | Contract |
| --- | --- |
| `sbs/v1` | Gateway-to-`sbs-data` volume I/O |
| `sbs/admin/v1` | Operator and `sbs-service` administration/operation API |
| `sbs/internalapi/v1` | Service-to-service authority, placement, allocation, and write-session APIs |

Generated Go bindings are checked in under the corresponding `sbs/*/v1`
directories. Change the `.proto` first, preserve field numbers, reserve removed
numbers/names, regenerate with the repository's pinned process, and run API
reference plus affected client/server tests.

The combined admin schema can contain edition-gated messages. Community
listeners, help, and published references must expose only the reviewed
Community surface; schema presence alone does not enable a feature.

