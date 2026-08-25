# Gateway Packages

`gateway/` implements the replaceable routing and protocol layer between host
clients and SBS. It may cache published state briefly but does not own SBS
placement, repair, volume geometry, or payload durability.

| Package | Interface boundary |
| --- | --- |
| `auth` | Token claims and request authentication |
| `dataplane` | Persistent binary block-I/O sessions and frame handling |
| `httpapi` | HTTP control and userspace volume API |
| `metadata` | Gateway/control metadata, leases, attachments, and generation in etcd |
| `sbsgrpc` | Conversion and gRPC transport to SBS services |
| `service` | Volume, read/write, snapshot/read-view, discard, and GC orchestration |
| `store` | Small gateway state-store abstractions and implementations |

The gateway consumes gateway-facing target views from `sbs-service`; it must not
derive cluster placement by opening raw SBS metadata. Stale attachment or
generation errors are fencing failures, not ordinary retryable concurrency.
Test service semantics separately from HTTP/dataplane mapping, then add a
cross-package test when a transport changes the call shape.

