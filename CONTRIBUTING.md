# Contributing To NAMRBD

NAMRBD welcomes issues, fixes, documentation improvements, and compatibility
reports for the public open-source platform.

The public GitHub repository is generated from a canonical mixed-edition
development repository. Public pull requests are reviewed in GitHub as usual.
After acceptance, maintainers import the reviewed commit range into the
canonical repository, preserve contributor attribution, run the public
boundary checks, and re-export the public tree. Contributors do not need access
to the canonical repository.

## Contribution Flow

- Keep changes focused on replicated storage, gateway, SBS, kernel module,
  basic CSI, basic iSCSI, MCP, quickstart, and public observability behavior
  unless maintainers explicitly expand the public source boundary.
- Include focused tests or a short validation note with behavior changes.
- Keep public documentation product-facing and avoid private validation topology,
  internal planning notes, local user paths, or release evidence.
- Do not commit generated binaries, local build/cache output, `.DS_Store`, real
  `.env` files, bearer tokens, TLS private material, or rendered Secret
  manifests containing live credentials.

## Public Source Boundary

Public contributions should stay within replicated volume, gateway, SBS
replicated service/data-plane, kernel module, basic CSI, basic iSCSI, MCP,
quickstart, and public operations surfaces. Enterprise-only capability requests
should be tracked as design or product-scope issues unless a maintainer
explicitly moves the boundary.

## Formatting

Go sources must be formatted with `gofmt`. The public format gate intentionally
excludes vendored sources under `third_party/`; do not reformat vendored
upstream code unless the vendored patch policy explicitly calls for it.

```bash
make format-community-check
```

If the gate reports NAMRBD-owned files, run `gofmt -w` on those files and keep
the edit scoped.

## Validation

Run the public source gates before proposing a change:

```bash
make format-community-check
make build-community
make test-community
make vet-community
make observability-assets-check
make docs-source-check
make web-operations-dashboard-test
make docs-render-check
make quickstart-compose-config
make csi-helm-chart-check
```

For security-sensitive or dependency changes, also run:

```bash
make govulncheck-community
```

For kernel changes, run `make kernel-module` on a Linux host with matching
kernel headers.

## Documentation

Edit `docs-src/`. It is the only documentation source in this repository; the
manual set is built from it and published to GitHub Pages by the `Docs`
workflow. No rendered HTML is committed, so there is no second copy to keep in
sync.

Install the documentation toolchain and build the site locally with:

```bash
python -m pip install -r docs-src/requirements.txt
make docs-render-check
mkdocs serve --config-file mkdocs.yml
```

Manual sources wrap their bodies in component `<div>` blocks styled by
`docs-src/assets/namrbd-docs.css`. Any such block that contains Markdown must
carry `markdown="1"`, otherwise the body publishes as raw text. Page chrome —
top bar, sidebar, in-page table of contents, language switch — belongs to the
theme; do not reintroduce `layout`, `aside sidebar`, or `section content`
wrappers into the sources. `make docs-render-check` enforces both rules.

Use these rules:

- Update `docs-src/` when changing public user, deploy, quickstart, operation,
  observability, or contributor-facing content.
- Link other pages by their `.md` source path, not by the published `.html`
  path, so mkdocs can resolve and validate the link.
- Update `deploy/kubernetes/csi/README.md` and the Helm chart together when CSI
  deployment behavior changes.
- Update `deploy/observability` assets together when metric names, scrape jobs,
  alerts, or dashboards change.
- Reference diagrams from `docs-src/manuals/architecture-manual/assets/`, and
  embed them with Markdown image syntax so mkdocs resolves the published path.

## Secrets

Public manifests must not include raw bearer tokens or TLS material. Use
Kubernetes Secret creation, External Secrets, or deploy-time Helm values. Do not
commit local `values.env` files or rendered manifests that contain real
credential values.

## DCO And Sign-Off

NAMRBD does not require Developer Certificate of Origin sign-off at
this time. A `Signed-off-by` trailer is accepted but not required unless the
project maintainers announce a DCO policy change.
