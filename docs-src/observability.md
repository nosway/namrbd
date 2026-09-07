# Observability

Public observability assets live under `deploy/observability`.

## Bounded Fleet Observability

`GET /api/v1/sbs/cluster` is the default fleet overview. It reads the
transactionally maintained cluster-summary projection rather than completing
an unbounded scan of raw TiKV records. The response identifies its request as
`request_class=aggregate`, reports the projection source and revision, and
exposes `health`, `partial`, `stale`, `rebuild_required`, and freshness fields.
There is no automatic fallback from a missing or invalid projection to a raw
full-cluster scan.

Use the bounded collection surfaces when the aggregate requires investigation:

- `GET /api/v1/sbs/nodes` and `GET /api/v1/sbs/volumes` return a
  revision-pinned page with a server-issued continuation token. The default
  page size is 128 and the maximum is 512.
- `GET /api/v1/sbs/node?id=<node-id>` and
  `GET /api/v1/sbs/volume?id=<volume-id>` are point lookups.
- `automatic_page_completion=false` means the server does not silently follow
  every page. A caller that needs a complete export must advance the returned
  token explicitly and keep each response revision consistent.

Treat `partial`, `stale`, or `rebuild_required` as an observable control-plane
condition. Do not compensate with repeated legacy list calls or client-side
all-scans. Inspect page/point results, source revisions, TiKV pressure metrics,
and the fleet health codes first.

## Prometheus

Use `deploy/observability/prometheus/namrbd-community-scrape.json` as a sample
Prometheus scrape configuration. Replace the example hostnames with your SBS
service, SBS data, gateway, and iSCSI gateway endpoints.

The companion alert file is
`deploy/observability/prometheus/namrbd-community-alerts.json`. It is a
YAML-compatible Prometheus rule file encoded as JSON so the public Makefile can
lint it with `jq`.

## Grafana

Import `deploy/observability/grafana/namrbd-community-overview.json` into
Grafana and select a Prometheus datasource that scrapes the NAMRBD SBS
and gateway endpoints.

## Metric Catalog

`deploy/observability/metrics/namrbd-community-metrics-catalog.json` lists the
public SBS, gateway, and iSCSI gateway metric names and the endpoint that owns
each one.

The bounded fleet implementation exposes four groups of `sbs-service` metrics:

| Group | Representative metrics | Use |
|----|----|----|
| Cluster projection | `sbs_service_cluster_summary_health`, `_partial`, `_stale`, `_rebuild_required`, `_freshness_age_seconds`, `_source_revision`, `_baseline_source_revision` | Distinguish a healthy bounded aggregate from an incomplete, old, or rebuild-required projection. |
| Metadata pressure | `sbs_service_tikv_operations_total`, `sbs_service_tikv_operation_duration_seconds_total`, `sbs_service_tikv_hot_region_candidates_total` | Track point, batch, range-page, full-scan, and transaction-retry pressure without hiding operation class. |
| Fleet state | `sbs_service_fleet_health`, `sbs_service_fleet_apply_state`, `sbs_service_fleet_manifest_source_revision`, `sbs_service_fleet_observation_age_seconds`, fleet capacity and zone-node metrics | Correlate signed manifest state, drift, capacity, placement, and apply status. |
| Enumeration and work | `sbs_service_phase_ad_operation_list_*`, `sbs_service_phase_ad_legacy_expensive_calls_total`, `sbs_service_metadata_completion_total`, `sbs_service_maintenance_oldest_age_seconds`, `sbs_service_maintenance_claim_latency_seconds` | Detect legacy expensive reads, bounded-list behavior, and queue/claim health. |

The service summary also exposes raw TiKV pressure fields for incident
correlation. `tikv_point_get_count`, `tikv_batch_get_count`, and
`tikv_range_page_count` separate point, bounded-batch, and bounded-page reads;
their accumulated durations are `tikv_point_get_duration_nanos`,
`tikv_batch_get_duration_nanos`, and `tikv_range_page_duration_nanos`.
`tikv_hot_region_candidate_count` identifies requests whose observed key
distribution may concentrate load. Compare counts and durations over the same
observation interval; the accumulated nanosecond values are not per-request
latencies.

The `sbs_service_fleet_health{health_code=...}` label is bounded to stable
codes: `SBS_APPLY_PAUSED`, `SBS_HOST_CHECK_FAILED`, `SBS_CONFIG_DRIFT`,
`SBS_STRAY_NODE`, `SBS_STORAGE_CLAIM_MISMATCH`, and
`SBS_FLEET_CHECK_STALE`. Do not put node ids, addresses, operation ids, or
free-form errors in metric labels.

## Alerts

The starter alert set covers:

- SBS service readiness
- SBS service leader presence
- SBS data readiness
- gateway readiness
- iSCSI gateway readiness
- SBS node down state
- degraded replicated volumes
- repair and rebalance backlog
- maintenance transition failures
- retired payload cleanup failures
- low SBS data store available capacity ratio
- stale or rebuild-required bounded cluster summaries
- stale signed fleet observations and manifest/config/store drift

Tune alert thresholds to match the size and availability target of your
deployment before using them for paging.

For triage, compare the aggregate `source_revision` with
`baseline_source_revision`, then use revision-pinned pages and point lookups.
An increasing `full_scan` count, admitted legacy expensive calls, or hot-region
candidate count is a reason to inspect the caller and request shape; it is not
a reason to raise page limits blindly.

Validate all public observability assets with:

```bash
make observability-assets-check
```
