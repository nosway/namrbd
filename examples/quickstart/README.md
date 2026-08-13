# NAMRBD Community Quickstart

This quickstart starts a local SBS authority/data pair and `namrbd-gateway`
with Docker Compose, creates one replicated volume, materializes it on the
local data service, checks a write/readback path through `sbsctl testio`, and
verifies gateway readiness and Prometheus metrics.

From the repository root:

```bash
make quickstart-compose-config
make quickstart-local-sbs-smoke
```

The smoke command builds the `sbs-service`, `sbs-data`, `sbsctl`, and
`namrbd-gateway` container targets when needed, then prints a JSON summary
with `result`, `ok_count`, `error_count`, `volume_id`,
`readback_matched`, and the gateway endpoint.

Stop the running quickstart services without deleting data:

```bash
make quickstart-local-down
```

Reset the quickstart volumes:

```bash
make quickstart-local-reset
```

## Scope

This is an entry-level Community runtime check. It validates local SBS
metadata, node membership, volume creation, materialization, and `sbsctl`
read/write I/O with replication factor 1. It also confirms that
`namrbd-gateway` starts in front of SBS and exports `/readyz` plus
Prometheus `/metrics`.

It does not load the Linux kernel module, expose a host block device, prove
iSCSI initiator access, or install the Kubernetes CSI driver. Those surfaces
are built and packaged in Community Edition, but they require their own host or
cluster prerequisites.

For a Kubernetes entry path, use the kind CSI PVC demo:

```bash
make kind-csi-pvc-demo
```

That demo installs the CSI Helm chart into kind and waits for one block PVC to
bind to a NAMRBD-backed CSI PV.

## Configuration

Defaults live in `.env.example`. Override image names, tag, ports, project
name, or IDs by exporting environment variables before running the make target.

Useful variables:

```bash
NAMRBD_IMAGE_TAG=local
NAMRBD_QUICKSTART_BIND_ADDRESS=127.0.0.1
NAMRBD_QUICKSTART_SBS_ADMIN_HTTP_PORT=19081
NAMRBD_QUICKSTART_SBS_DATA_HTTP_PORT=19082
NAMRBD_QUICKSTART_GATEWAY_HTTP_PORT=19701
NAMRBD_QUICKSTART_VOLUME_ID=00000001
NAMRBD_QUICKSTART_SKIP_BUILD=1
NAMRBD_QUICKSTART_CLEANUP_ON_EXIT=1
NAMRBD_QUICKSTART_INCLUDE_GATEWAY=1
```
