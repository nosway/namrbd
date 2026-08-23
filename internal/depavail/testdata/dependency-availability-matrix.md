# Dependency Availability Contract

This package-owned table is the executable documentation oracle for dependency
loss behavior. It is kept with the tests so a public source checkout does not
depend on private planning documents.

## Dependency Availability Matrix

| Dependency state | Data path (already-admitted I/O) | Control path | Membership change | Export failover | Grace / threshold | Observable |
| --- | --- | --- | --- | --- | --- | --- |
| etcd unavailable | Continues. Existing active gateways keep serving. | Read-only from local snapshot; fleet listing marked stale. | Rejected. | Suppressed. No promotion may occur while the fleet view cannot be trusted. | `etcd_unavailable_grace_seconds` default 300 | `etcd_unavailable_grace_seconds`, `iscsi_gateway_stale_count`, `failover_suppressed_on_etcd_loss` |
| etcd unavailable beyond grace | Continues, marked degraded. | Read-only. | Rejected. | Suppressed. | - | `first_error`, `last_error` |
| TiKV or PD unavailable | Continues on the cached projection and already-loaded serving mappings. | Read-only. | Rejected. | Suppressed. Failover records require TiKV. | `tikv_unavailable_grace_seconds` default 300 | `tikv_unavailable_grace_seconds`, `serving_continues_on_dependency_loss` |
| TiKV or PD unavailable beyond grace | Continues, marked degraded. New export admission blocked. | Read-only. | Rejected. | Suppressed. | - | `first_error`, `last_error` |
| Projection stale above healthy threshold | Continues. | Views marked degraded. | Rejected. | Suppressed. | `projection_stale_degraded_ms` default 5000 | `projection_lag_ms`, `stale_projection_count` |
| Projection stale above blocked threshold | Continues, marked degraded. | Views refuse to publish as healthy. | Rejected. | Suppressed. | `projection_stale_blocked_ms` default 15000 | `projection_lag_ms` |
| Both etcd and TiKV unavailable | Continues on cache. | Read-only. | Rejected. | Suppressed. | Lower of the two graces | `first_error`, `last_error` |

Dependency loss is fail-open for already-admitted serving and fail-closed for
promotion, admission, and membership changes. Suppressing failover while the
authority view is unavailable protects the active-writer boundary.
