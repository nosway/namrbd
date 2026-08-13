# Observability

Public observability assets live under `deploy/observability`.

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

Tune alert thresholds to match the size and availability target of your
deployment before using them for paging.

Validate all public observability assets with:

```bash
make observability-assets-check
```
