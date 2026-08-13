# NAMRBD CSI PVC Demo On kind

This demo starts the local Docker Compose quickstart, creates a kind cluster,
loads a locally built `namrbd-csi-driver` image into that cluster, installs the
CSI Helm chart, and waits for one demo PVC to become `Bound`.

From the repository root:

```bash
make kind-csi-pvc-demo
```

The script prints a JSON summary with `result`, `ok_count`, `error_count`,
`pvc_phase`, `pv_name`, and `volume_handle`.

## What It Proves

- The local `sbs-service` and `sbs-data` containers can serve as the SBS admin
  authority for a Kubernetes CSI controller running inside kind.
- The Helm chart can install the CSI controller in a fresh cluster.
- The external provisioner can call `namrbd-csi-driver`, create a replicated
  Community volume with replication factor 1, and bind a PVC to a CSI PV.

This demo intentionally stops at PVC binding. It does not mount the volume into
a workload pod, load the Linux kernel module, or prove host block-device I/O.

## Prerequisites

Install these tools on the host:

```bash
docker
kind
kubectl
helm
jq
```

The demo starts the Compose quickstart with
`NAMRBD_KIND_QUICKSTART_BIND_ADDRESS=0.0.0.0` so pods inside the kind node can
reach the host-published SBS admin port. Use it on a trusted local development
machine.

## Useful Commands

Check that the demo assets are present:

```bash
make kind-csi-pvc-demo-check
```

Run the full demo:

```bash
make kind-csi-pvc-demo
```

Inspect the PVC:

```bash
kubectl --context kind-namrbd-csi-demo get pvc namrbd-demo-pvc
kubectl --context kind-namrbd-csi-demo get pv
```

Clean Kubernetes demo objects:

```bash
examples/kind-csi-pvc/run.sh cleanup
```

Delete the kind cluster during cleanup:

```bash
NAMRBD_KIND_DELETE_CLUSTER_ON_CLEANUP=1 examples/kind-csi-pvc/run.sh cleanup
```

Also remove quickstart Compose volumes:

```bash
NAMRBD_KIND_CLEANUP_QUICKSTART_ON_CLEANUP=1 examples/kind-csi-pvc/run.sh cleanup
```

## Configuration

Common overrides:

```bash
NAMRBD_KIND_CLUSTER_NAME=namrbd-csi-demo
NAMRBD_IMAGE_TAG=local
NAMRBD_KIND_HOST_ADDRESS=host.docker.internal
NAMRBD_KIND_RESET_QUICKSTART=1
NAMRBD_KIND_WAIT_TIMEOUT=180s
```

When `NAMRBD_KIND_HOST_ADDRESS` is not set, the script tries the Docker `kind`
network gateway address and falls back to `host.docker.internal`.
