# gRPC and Proto reference

NAMRBD's protobuf sources are the canonical wire-schema definitions. All RPCs
currently defined by the repository are unary RPCs; there are no streaming
methods.

## Schema inventory

| Proto source | Service | RPCs | Messages | Enums | Intended audience |
| --- | --- | ---: | ---: | ---: | --- |
| `proto/sbs/admin/v1/admin.proto` | `sbs.admin.v1.AdminService` | 237 | 561 | 16 | Control-plane clients; edition-combined schema |
| `proto/sbs/admin/v1/operations.proto` | `sbs.admin.v1.OperationsService` | 4 | 9 | 1 | Control-plane clients |
| `proto/sbs/v1/volume.proto` | `sbs.v1.VolumeService` | 18 | 42 | 3 | Gateway-to-SBS data path |
| `proto/sbs/internalapi/v1/chunk_id_allocator.proto` | `sbs.internalapi.v1.ChunkIDAllocatorService` | 1 | 2 | 0 | Internal only |
| `proto/sbs/internalapi/v1/ec_metadata.proto` | `sbs.internalapi.v1.ECMetadataService` | 6 | 18 | 0 | Internal only |
| `proto/sbs/internalapi/v1/persistent_reservation.proto` | `sbs.internalapi.v1.PersistentReservationAuthorityService` | 2 | 5 | 0 | Internal only; Enterprise implementation |
| `proto/sbs/internalapi/v1/placement_apply.proto` | `sbs.internalapi.v1.PlacementApplyService` | 1 | 4 | 1 | Internal only |
| `proto/sbs/internalapi/v1/placement_resolver.proto` | `sbs.internalapi.v1.PlacementResolverService` | 4 | 15 | 3 | Internal only |
| `proto/sbs/internalapi/v1/write_session.proto` | `sbs.internalapi.v1.WriteSessionService` | 12 | 29 | 2 | Internal only |
| `proto/sbs/internalapi/v1/payload_encryption.proto` | No service | 0 | 1 | 0 | Internal message schema |

The repository total is 9 services, 285 RPCs, 686 messages, and 26 enums.
Source-relative generated Go files live under `sbs/admin/v1`,
`sbs/internalapi/v1`, and `sbs/v1`. Generation is configured by `buf.yaml` and
`buf.gen.yaml`.

## Runtime listeners

| Process | Default endpoint | Registered services | Registration source |
| --- | --- | --- | --- |
| `sbs-service` | `0.0.0.0:9443` | Admin, Operations, Volume proxy, PlacementApply, WriteSession, ECMetadata, ChunkIDAllocator, PlacementResolver, PersistentReservationAuthority | `cmd/sbs-service/main.go` |
| `sbs-data` | `0.0.0.0:9444` | Volume | `cmd/sbs-data/main.go` |
| `namrbd-csi-driver` | `unix:///tmp/namrbd-csi.sock` | CSI Identity, Controller, Node | `cmd/namrbd-csi-driver/main.go` |

The servers are created with a bare `grpc.NewServer()`. They do not currently
register gRPC reflection or the standard gRPC health service. Tools such as
`grpcurl` must therefore be given the local proto file and `proto` import path.
The table documents source defaults; deployments can override every listener.

## VolumeService status contract

`proto/sbs/v1/volume.proto` defines `ErrorDetail` with `code`, `message`, and
`retryable`. `gateway/sbsgrpc/server.go` maps service errors as follows:

| ErrorDetail code | gRPC status |
| --- | --- |
| `ERROR_CODE_NOT_FOUND` | `NOT_FOUND` |
| `ERROR_CODE_BAD_REQUEST` | `INVALID_ARGUMENT` |
| `ERROR_CODE_STALE_GENERATION` | `FAILED_PRECONDITION` |
| `ERROR_CODE_ATTACHMENT_MISMATCH` | `FAILED_PRECONDITION` |
| `ERROR_CODE_IDEMPOTENCY_CONFLICT` | `FAILED_PRECONDITION` |
| `ERROR_CODE_FENCED` | `FAILED_PRECONDITION` |
| `ERROR_CODE_SECURITY_REJECTED` | `PERMISSION_DENIED` |
| `ERROR_CODE_UNAVAILABLE` | `UNAVAILABLE` |
| `ERROR_CODE_TIMEOUT` | `DEADLINE_EXCEEDED` |
| unspecified or internal | `INTERNAL` |

Optional physical, EC, and writer-fence interfaces return `UNIMPLEMENTED` when
the configured backend does not implement them. Clients should inspect
`ErrorDetail` before classifying `FAILED_PRECONDITION`: without the detail,
`gateway/sbsgrpc/client.go` conservatively maps that status to stale generation
and cannot distinguish attachment, idempotency, or fencing conflicts.

## Internal service status contract

WriteSession, PlacementApply, and ChunkIDAllocator classify invalid input as
`INVALID_ARGUMENT`, metadata CAS conflicts as `ABORTED`, missing records as
`NOT_FOUND`, transient dependency failures as `UNAVAILABLE`, and other failures
as `INTERNAL`. PlacementResolver has the same mapping without a conflict class.
On the client side, `CANCELED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` are all
classified as the internal unavailable class. The executable mappings live in
`sbs/cluster/control/*_grpc.go`; ECMetadata reuses the write-session mapping.

AdminService does not define one common error-detail message. Method-specific
implementations in `cmd/sbs-service/*.go` use standard gRPC status codes, so a
generated structural reference must be accompanied by a reviewed status,
idempotency, and retry table rather than inferring semantics from message names.

## Bounded enumeration contract

Normal fleet reads use page or point methods rather than completing every
record family in one request. `ListVolumesPage`, `ListRepairsPage`,
`ListRebalancesPage`, and `ListOperationsPage` return an opaque continuation
token bound to the selected filters and a source/projection revision. A token
from another filter or an obsolete revision fails instead of restarting or
silently completing the list. The corresponding `sbsctl` commands default to
128 records per page and reject sizes above 512.

The retained unpaged `ListVolumes`, `ListRepairs`, `ListRebalances`, and
`ListOperations` RPCs are explicit expensive compatibility paths. Calls must
carry `ExpensiveCallAdmission` with a non-empty operator reason, a record budget
between 1 and 1,000,000, and a request deadline. Missing admission fails with
`FAILED_PRECONDITION`; exhausting the admitted record budget fails with
`RESOURCE_EXHAUSTED`. Clients must not use those RPCs for periodic polling.

`GetClusterStatusResponse` also exposes the bounded cluster-summary health,
reason, partial/stale/rebuild-required state, source and baseline revisions,
freshness age, and aggregate node counts. Missing or stale aggregate state does
not authorize a legacy scan fallback. `SetMaintenanceThrottleRequest` and its
response include `max_total_concurrent_movements`, which bounds the combined
repair, rebalance, and drain movement population in addition to per-kind
limits.

## CSI boundary

CSI proto files are not vendored as NAMRBD schemas. The wire contract is the
Container Storage Interface dependency pinned in `go.mod` at
`github.com/container-storage-interface/spec v1.13.0`. NAMRBD implements the
Identity, Controller, and Node services in `internal/csi/driver`; the upstream
CSI specification remains authoritative for their request and response
messages.

## Edition and generation boundary

`admin.proto` is an edition-combined schema while several implementations are
selected by the `enterprise` Go build tag. Publishing an unfiltered
`protoc-gen-doc` rendering as the Community API would therefore advertise
Enterprise-only names. Until the schema is split or a reviewed Community
allowlist is enforced, the full generated AdminService reference belongs only
in the repository-internal reference area.

The canonical source repository maintains a pinned `protoc-gen-doc` rendering
for complete structural coverage. That combined-edition artifact remains
outside the published Community documentation tree; it does not replace the
hand-reviewed listener, authority, error, retry, and edition contracts above.
