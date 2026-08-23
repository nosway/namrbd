# Dependency Budget Contract

This package-owned contract keeps operational counter and threshold names
executable in a public source checkout. The model is sized for the internal
`t2_large` test tier: 100 SBS nodes, 32 gateways, 1000 volumes, and 1000 exports.
It is a validation shape, not a supported deployment claim.

## Emitted fields

The gateway, SBS, iSCSI, and dependency-availability paths expose the following
fields. A renamed field must update this contract in the same change:

`chunk_gc_pass_count`, `chunk_gc_rotation_count`,
`chunk_gc_swept_volume_count`, `chunk_gc_volume_error_count`,
`chunk_gc_volume_list_count`, `chunk_gc_volumes_per_pass`,
`dependency_readiness`, `etcd_availability`, `etcd_point_read_count`,
`etcd_prefix_scan_count`, `etcd_resync_count`,
`etcd_skipped_registry_validation_count`, `etcd_status_write_count`,
`etcd_unavailable_grace_seconds`, `export_admission_blocked_count`,
`failover_suppressed_count`, `failover_suppressed_on_etcd_loss`,
`first_error`, `iscsi_gateway_stale_count`,
`iscsi_registry_changed_export_count`, `iscsi_registry_live_reload_ready`,
`iscsi_registry_reload_bounded`, `iscsi_registry_reload_page_count`,
`iscsi_registry_reload_page_size`, `last_error`, `max_exports_per_process`,
`membership_change_rejected_count`, `path_plan_change_event_count`,
`path_plan_floor_wakeup_count`, `path_plan_resync_count`,
`path_plan_scan_count`, `path_plan_skipped_tick_count`,
`path_plan_watch_attached`, `projection_freshness`, `projection_lag_ms`,
`projection_stale_blocked_ms`, `projection_stale_degraded_ms`,
`registry_apply_failure_count`, `registry_config_generation`,
`registry_fetch_count`, `registry_reload_count`,
`registry_reload_first_error`, `registry_reload_last_error`,
`registry_reload_revision`, `registry_resync_count`,
`registry_skipped_generation_count`, `registry_stale_reject_count`,
`served_export_count`, `serving_continues_on_dependency_loss`,
`stale_projection_count`, `tikv_availability`, `tikv_batch_get_chunk_count`,
`tikv_batch_get_count`, `tikv_batch_get_key_count`, `tikv_full_scan_count`,
`tikv_point_get_count`, `tikv_registry_reload_scan_count`,
`tikv_txn_retry_count`, and `tikv_unavailable_grace_seconds`.

## Thresholds

| Measure | Budget | Source constant |
| --- | --- | --- |
| etcd status writes, fleet aggregate | 50/s | `BudgetEtcdStatusWritesPerSecond` |
| etcd watch consumers | 64 | `BudgetEtcdWatchConsumers` |
| etcd steady-state prefix scans per consumer | 0 | `BudgetEtcdSteadyStatePrefixScans` |
| TiKV hot-path full scans | 0 | `BudgetTiKVHotPathFullScans` |
| TiKV keys per BatchGet | 128 | `BudgetTiKVBatchGetKeys` |
| Exports per iSCSI gateway | 32 minimum | `BudgetExportsPerProcess` |

## Known limits

The etcd watch budget has no headroom at this test tier. Failover is
suppressed during dependency loss so an untrustworthy fleet view cannot promote
a competing writer. Unbounded on-demand scans remain visible through
`ListVolumes`, `ListGateways`, `ListExtentPages`, and
`validateGatewayRecordAgainstRegistry`; none should be placed on a timer.

The synthetic model is not live deployment evidence and must not describe large-scale operations as supported.
Contention, network partition, external
initiator, kernel I/O, and sustained-load behavior require separate executable
validation.
