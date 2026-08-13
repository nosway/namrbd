# Quickstart

Build and test Community binaries:

```bash
make build-community
make test-community
```

Build the Community container image set:

```bash
make container-build-community-images
```

Render the local Compose configuration and run the local all-in-one smoke:

```bash
make quickstart-compose-config
make quickstart-local-sbs-smoke
```

The same Docker Compose smoke is also available as:

```bash
make quickstart-local-all-smoke
```

Stop or reset local quickstart resources:

```bash
make quickstart-local-down
make quickstart-local-reset
```

The quickstart starts `sbs-service`, `sbs-data`, and `namrbd-gateway`, creates
a small replicated volume, writes through `sbsctl testio`, verifies readback,
and checks gateway readiness plus Prometheus metrics.

Run the kind CSI PVC demo:

```bash
make kind-csi-pvc-demo
```

The kind demo starts the local Compose quickstart, builds and loads the local
CSI image into kind, installs the CSI Helm chart, and waits for one block PVC
to become `Bound`. It intentionally stops at PVC binding and does not mount
the volume into a workload pod.

Kernel modules are built separately on a Linux host with matching kernel
headers:

```bash
make kernel-module
```
