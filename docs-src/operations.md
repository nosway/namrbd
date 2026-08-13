# Operations

NAMRBD Community exposes simple health and readiness endpoints for SBS
operators.

Check the control service:

```bash
curl -fsS http://service-01.example.com:9081/healthz
curl -fsS http://service-01.example.com:9081/readyz
curl -fsS http://service-01.example.com:9081/metrics
```

Check the data service:

```bash
curl -fsS http://data-01.example.com:9082/healthz
curl -fsS http://data-01.example.com:9082/readyz
curl -fsS http://data-01.example.com:9082/metrics
```

Check the gateway:

```bash
curl -fsS http://gateway-01.example.com:9701/healthz
curl -fsS http://gateway-01.example.com:9701/readyz
curl -fsS http://gateway-01.example.com:9701/metrics
```

The gateway also exposes JSON debug metrics through:

```bash
curl -fsS http://gateway-01.example.com:9701/api/v1/debug/gateway/metrics
curl -fsS http://gateway-01.example.com:9701/api/v1/debug/sbs-cluster/metrics
```

Check the iSCSI gateway when it is started with
`--observability-listen :9090`:

```bash
curl -fsS http://iscsi-gateway-01.example.com:9090/healthz
curl -fsS http://iscsi-gateway-01.example.com:9090/readyz
curl -fsS http://iscsi-gateway-01.example.com:9090/metrics
```

Use `sbsctl` for product operations such as volume lifecycle, replicated
snapshot workflows, basic iSCSI control, and smoke I/O checks.
