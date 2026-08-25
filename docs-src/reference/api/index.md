# API reference

This reference describes the reviewed NAMRBD Community HTTP and gRPC
surfaces. Runtime source remains authoritative when a generated artifact and
the executable disagree.

## HTTP/OpenAPI

The HTTP specifications use OpenAPI 3.1 JSON and are split by listener. This
keeps identically named operational routes such as `/healthz` and `/metrics`
from being mistaken for one shared server.

- [namrbd-gateway v1](rest/namrbd-gateway-v1.openapi.json) — stable volume,
  discovery, health, readiness, and metrics operations.
- [sbs-service observability v1](rest/sbs-service-observability-v1.openapi.json)
  — the read-only Phase-Y views and operational health surfaces.
- [sbs-data operational v1](rest/sbs-data-operational-v1.openapi.json) —
  health, readiness, metrics, summary, and store-health observations.
- [namrbd-iscsi-gateway observability v1](rest/namrbd-iscsi-gateway-observability-v1.openapi.json)
  — the optional observability listener.

Every operation carries these review fields:

- `x-namrbd-stability`: the support level of the operation;
- `x-namrbd-edition`: the product edition represented by the specification;
- `x-namrbd-authority`: the subsystem authoritative for the response;
- `x-namrbd-feature-gate`: the runtime condition required for the operation;
- `x-namrbd-source`: the repository source and literal registration or handler
  marker used to audit the operation.

The OpenAPI schemas intentionally leave `additionalProperties` enabled for
responses assembled from Go `map[string]any` values. Listed properties are the
fields the current handler writes; they are not a promise that no conditional
observability fields can appear. Plain-text errors produced with `http.Error`
are documented as plain text rather than converted into a fictional JSON error
envelope.

These APIs are primarily control and observability listeners. Inclusion here
does not imply that a listener is safe to expose directly to an untrusted
network. Deployment policy, TLS termination, and network access control remain
operator responsibilities.

## gRPC and Proto

See the [gRPC and Proto reference](grpc.md) for the canonical proto inventory,
listener ownership, status mapping, CSI boundary, and Community/Enterprise
edition caveat.

## Scope boundary

Public OpenAPI files contain only the reviewed Community surface. Debug and
non-public mutation routes, EC diagnostics, clone/snapshot debug I/O, iSCSI
HA/ALUA internals, and edition-combined AdminService documentation are
deliberately not published in these specifications. The canonical source
repository retains a separate boundary audit with the omitted source locations.
