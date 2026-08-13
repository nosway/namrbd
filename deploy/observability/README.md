# NAMRBD Community Observability Assets

This directory contains public, implementation-matched observability assets for
NAMRBD Community deployments.

## Endpoints

`sbs-service` exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

`sbs-data` exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

`namrbd-gateway` exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

It also exposes JSON debug metrics through its gateway HTTP API:

- `GET /api/v1/debug/gateway/metrics`
- `GET /api/v1/debug/sbs-cluster/metrics`

`namrbd-iscsi-gateway` exposes observability endpoints when started with
`--observability-listen`:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

The Prometheus examples in this directory scrape SBS service, SBS data,
gateway, and iSCSI gateway endpoints.

## Files

- `prometheus/namrbd-community-scrape.json` is a sample scrape configuration.
- `prometheus/namrbd-community-alerts.json` is a YAML-compatible Prometheus
  rules file encoded as JSON for portable linting.
- `grafana/namrbd-community-overview.json` is a starter Grafana dashboard.
- `metrics/namrbd-community-metrics-catalog.json` documents the public metric
  names, owners, and intended alert use.

Validate the assets from a source checkout with:

```bash
make observability-assets-check
```
