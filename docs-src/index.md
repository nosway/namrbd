# NAMRBD

NAMRBD is an open-source distributed block storage platform for Linux. The
public source includes replicated SBS control/data services, gateway
forwarding, host control tools, kernel module source, basic iSCSI access,
Kubernetes CSI integration, snapshots and restore building blocks, and public
operations assets.

Read [Feature Status](feature-status.md) to distinguish source availability
from the v1.0 release support boundary. Advanced Enterprise capabilities are
also summarized there as work under development and validation, rather than as
generally available features.

The [Edition Boundary Guide](manuals/edition-boundary.md) defines how Community
and Enterprise capabilities are labeled and how unavailable CLI/API surfaces
fail closed. Contributors should start with
[Developer Onboarding](manuals/developer-onboarding.md).

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
- REST and gRPC reference: [Reference / APIs](reference/api/index.md)
- CLI command reference: [Reference / CLI Commands](reference/cli/index.md)
- Daemon configuration reference: [Reference / Configuration](reference/config/index.md)
- Incident runbook: [Troubleshooting and FAQ](manuals/troubleshooting-and-faq.md)
- Compatibility: [OS, Kernel, and Kubernetes Matrix](manuals/compatibility-matrix.md)
- Edition behavior: [Community and Enterprise Boundary](manuals/edition-boundary.md)
- Contributor guide: [Developer Onboarding](manuals/developer-onboarding.md)
- Published site: <https://nosway.github.io/namrbd/>

Validate the public documentation source with:

```bash
make docs-source-check
```
