# NAMRBD CSI Kubernetes Deployment

This directory contains Community deployment assets for `namrbd-csi-driver`.
Use the Helm chart for normal installs and keep the raw manifest templates only
for review or custom rendering.

## Helm Install

Build and publish the CSI image first, then install the chart:

```bash
helm upgrade --install namrbd-csi deploy/kubernetes/csi/helm/namrbd-csi \
  --namespace namrbd-system \
  --create-namespace \
  --set image.repository=ghcr.io/nosway/namrbd-csi-driver \
  --set image.tag=local \
  --set config.adminEndpoint=namrbd-sbs-service:9897 \
  --set config.gatewayURL=http://namrbd-gateway:9701
```

The chart creates the `CSIDriver`, RBAC, controller `Deployment`, node
`DaemonSet`, Community replicated `StorageClass`, and `VolumeSnapshotClass` in
one render.

## kind PVC Binding Demo

For a local end-to-end provisioning demo, use:

```bash
make kind-csi-pvc-demo
```

The demo starts the local Docker Compose quickstart, creates a kind cluster,
loads the local `namrbd-csi-driver` image, installs this Helm chart, and waits
for one block PVC to become `Bound`.

The demo installs only the pieces required for PVC binding:

- `StorageClass.volumeBindingMode=Immediate`;
- `replication_factor=1`;
- `volumeSnapshotClass.create=false`;
- `sidecars.csiAttacher.enabled=false`;
- `sidecars.csiSnapshotter.enabled=false`;
- `sidecars.csiResizer.enabled=false`;
- `node.enabled=false`.

This proves the controller/provisioner path. It intentionally does not mount
the volume into a workload pod or validate host block-device I/O.

## Credentials And Secrets

Do not commit bearer tokens or TLS material into values files or rendered YAML.
Create the Secret separately, or let an External Secrets controller create it:

```bash
kubectl -n namrbd-system create secret generic namrbd-csi-credentials \
  --from-literal=bearer-token="$NAMRBD_CSI_BEARER_TOKEN" \
  --from-file=tls-ca.pem=./ca.pem
```

Then enable Secret references without embedding the values in git:

```bash
helm upgrade --install namrbd-csi deploy/kubernetes/csi/helm/namrbd-csi \
  --namespace namrbd-system \
  --set credentials.enabled=true \
  --set credentials.existingSecret=namrbd-csi-credentials
```

For sealed demo clusters you may set `credentials.create=true`, but pass secret
values only at deploy time with `--set` or `--set-file`; never store those
values in `values.yaml`.

The current Community CSI driver uses insecure SBS admin gRPC transport. The
`tls-ca.pem` key is reserved for deployments that carry TLS material through
the same Secret contract while the admin client TLS transport is enabled in a
future release.

## Render Helper

Copy `helm/namrbd-csi/values.env.example` to a local untracked `values.env`
file and render:

```bash
cp deploy/kubernetes/csi/helm/namrbd-csi/values.env.example \
  deploy/kubernetes/csi/helm/namrbd-csi/values.env
deploy/kubernetes/csi/helm/namrbd-csi/render.sh
```

`values.env` is intentionally not required by CI and should not contain raw
secrets.

## Raw Template Placeholders

The legacy files under `templates/` use these non-secret placeholders:

| Placeholder | Meaning |
| --- | --- |
| `__NAMESPACE__` | Kubernetes namespace for namespaced CSI resources. |
| `__DRIVER_NAME__` | CSI driver name, usually `csi.namrbd.io`. |
| `__CSI_DRIVER_IMAGE__` | Image containing `namrbd-csi-driver` and `namrbdctl`. |
| `__IMAGE_PULL_POLICY__` | Kubernetes image pull policy. |
| `__ADMIN_ENDPOINT__` | Primary SBS admin gRPC endpoint. |
| `__ADMIN_ENDPOINTS__` | Optional comma- or space-separated admin endpoint list. |
| `__GATEWAY_URL__` | Gateway URL used by node attach operations. |
| `__CLUSTER_ID__` | NAMRBD cluster identifier. |
| `__SBS_CLUSTER_ID__` | SBS cluster identifier. |
| `__CONTROLLER_REPLICAS__` | Controller Deployment replica count. |
| `__CSI_PROVISIONER_TIMEOUT__` | Timeout passed to the CSI provisioner sidecar. |
| `__CSI_PROVISIONER_IMAGE__` | CSI provisioner sidecar image. |
| `__CSI_ATTACHER_IMAGE__` | CSI attacher sidecar image. |
| `__CSI_SNAPSHOTTER_IMAGE__` | CSI snapshotter sidecar image. |
| `__CSI_RESIZER_IMAGE__` | CSI resizer sidecar image. |
| `__CSI_NODE_DRIVER_REGISTRAR_IMAGE__` | Node driver registrar sidecar image. |
| `__CSI_LIVENESSPROBE_IMAGE__` | CSI liveness probe sidecar image. |
| `__DISCARD_MOUNT_OPTIONS__` | YAML array for StorageClass `mountOptions`, for example `[]` or `["discard"]`. |
| `__DISCARD_EXPOSURE_STATE__` | Public discard exposure state, usually `disabled` until validated. |
| `__DISCARD_VALIDATION_PROFILE__` | Operator-facing validation profile label for discard exposure. |

Bearer token and TLS CA material are deliberately not raw-template
placeholders. Use a Kubernetes Secret or External Secrets instead.

## Runtime Notes

`NAMRBD_SBS_SERVICE_ENDPOINT` is the primary `sbs-service` AdminService gRPC
target. `NAMRBD_SBS_SERVICE_ENDPOINTS` provides its optional comma- or
space-separated leader-aware failover list.
Entries may use `node_id=endpoint`, for example
`svc-a=10.0.0.10:9443 svc-b=10.0.0.11:9443`.

The node plugin needs the `namrbdctl` helper inside the driver image. The
Community `namrbd-csi-driver` image target includes both
`/usr/local/bin/namrbd-csi-driver` and `/usr/local/bin/namrbdctl`.

Discard is disabled by default in the chart and raw templates. Enable
`mountOptions: ["discard"]` only after the backend, kernel module, and
filesystem discard behavior are validated for your cluster.
