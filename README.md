# NAMRBD Community Edition

NAMRBD (Network Attached Multipath Resilient Block Device) is a network-attached Linux block storage project. The Community
Edition focuses on replicated block volumes, host attach through kernel module, gateway forwarding,
SBS service/data-plane authority, Kubernetes CSI integration, basic snapshot
and restore workflows, discard/zero observability, and basic iSCSI
connectivity.

![NAMRBD platform overview](docs-src/manuals/architecture-manual/assets/diagrams/platform-overview.svg)

There is a sibling project NAMROS (https://github.com/nosway/namros) which provides S3 compatible object storage based on SBS backend of NAMRBD.
The relationship of two projects is shown in the ![architecture diagram]((docs-src/manuals/architecture-manual/assets/diagrams/namrbd-namros-interface-map.svg)

The public Community Edition source tree is intended to be usable as a normal
open-source checkout for building, testing, inspecting, and packaging
Community behavior.

## Community Scope

Community Edition includes:

- replicated volume lifecycle and placement;
- `namrbd-gateway`, `namrbdctl`, `sbs-service`, `sbs-data`, `sbsctl`,
  `namrbd-debug`, `namrbd-csi-driver`, `namrbd-iscsi-gateway`, and
  `namrbd-mcp`;
- Linux kernel block/control modules;
- Kubernetes CSI manifests under `deploy/kubernetes/csi`;
- public health, metrics, Grafana, alert, and metric catalog assets under
  `deploy/observability`;
- public Markdown documentation source under `docs-src`;
- manual replicated snapshot and restore-from-snapshot workflows;
- basic Kubernetes CSI replicated provisioning and snapshot restore surfaces;
- discard, write-zeroes, and zero/read-view correctness observability;
- basic iSCSI target and control CLI surfaces with the Community volume export
  limit.

Enterprise-only areas are intentionally not Community product claims. These
include EC-backed product availability, automated Backup/DR, advanced
mobility/repack/dedupe workflows, security/KMS/encryption product surfaces,
performance-tier controls, and advanced iSCSI HA/scale operations.

## Quickstart

Build the Community binaries from a clean checkout:

```bash
make build-community
make test-community
```

Build the local Community container image set:

```bash
make container-build-community-images
```

Run the local container quickstart. This starts `sbs-service`, `sbs-data`, and
`namrbd-gateway`, creates a small replicated volume, verifies `sbsctl`
write/read I/O, and checks gateway readiness plus Prometheus metrics:

```bash
make quickstart-compose-config
make quickstart-local-sbs-smoke
```

The same smoke is also exposed as:

```bash
make quickstart-local-all-smoke
```

Run the kind CSI PVC demo. This starts the local Compose quickstart, creates a
kind cluster, installs the CSI Helm chart, and waits for one block PVC to
become `Bound`:

```bash
make kind-csi-pvc-demo
```

Stop or reset it with:

```bash
make quickstart-local-down
make quickstart-local-reset
```

Kernel modules are built separately on a Linux host with matching kernel
headers:

```bash
make kernel-module
```

Quickstart files live under `examples/quickstart`. The kind CSI PVC demo lives
under `examples/kind-csi-pvc`. Kubernetes CSI deployment assets live under
`deploy/kubernetes/csi`; use the Helm chart for normal installs and create
credential Secrets outside git.

Validate the public observability and documentation source assets:

```bash
make observability-assets-check
make docs-source-check
```

Build the editable public docs when MkDocs is installed:

```bash
make docs-build
```

## Operations Assets

SBS service and data endpoints expose `/healthz`, `/readyz`, and `/metrics`.
`namrbd-gateway` also exposes `/healthz`, `/readyz`, `/metrics`, and JSON debug
metrics through `/api/v1/debug/gateway/metrics` and
`/api/v1/debug/sbs-cluster/metrics`. `namrbd-iscsi-gateway` exposes
`/healthz`, `/readyz`, and `/metrics` when started with
`--observability-listen`.

Prometheus scrape examples, starter alert rules, a Grafana overview dashboard,
and the metric catalog live under `deploy/observability`.

## Documentation

The manual set is published at <https://nosway.github.io/namrbd/>, built from
`docs-src/` on every push to `main`. No rendered HTML is committed, so the
sources cannot drift from what readers see.

`docs-src/` is the single authoring surface, with `mkdocs.yml` at the
repository root and the installation, user, admin, HA, and architecture
manuals under `docs-src/manuals/`. Build it locally with:

```bash
python -m pip install -r docs-src/requirements.txt
make docs-render-check
mkdocs serve
```

Internal planning notes, private harnesses, and generated working directories
are not part of the public documentation tree.

## License

Unless a file or directory says otherwise, NAMRBD source is licensed under the
Apache License, Version 2.0. The Linux kernel module under `kernel/module/` is
licensed under GPL-2.0-only. See `LICENSE`, `NOTICE`,
`THIRD_PARTY_NOTICES.md`, and the license texts under `LICENSES/`.
