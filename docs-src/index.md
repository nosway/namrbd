# NAMRBD Community

NAMRBD Community is the open source edition of NAMRBD for replicated Linux
block storage. It includes the replicated SBS control/data services, gateway
forwarding, host control tools, kernel module source, basic iSCSI access, and
Kubernetes CSI integration.

Start with the local build and quickstart path:

```bash
make build-community
make test-community
make quickstart-compose-config
make quickstart-local-sbs-smoke
```

`docs-src/` is the documentation source. Public manual sources live under
`docs-src/manuals/`, and the built site is published to
<https://nosway.github.io/namrbd/>. No rendered HTML is committed to the
repository.

## Public Assets

- Quickstart: `examples/quickstart`
- Kubernetes CSI templates: `deploy/kubernetes/csi`
- Observability assets: `deploy/observability`
- Manual source: `docs-src/manuals`
- Published site: <https://nosway.github.io/namrbd/>

Validate the public documentation source with:

```bash
make docs-source-check
```
