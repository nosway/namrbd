# Developer Onboarding

This guide gets a contributor from a clean checkout to a focused code change,
package test, debugger session, and documentation check. Read
[Feature Status](../feature-status.md) before interpreting source presence as a
supported release claim.

## Toolchain

The root `go.mod` declares Go 1.26.6. Use that toolchain directly or a Go
installation capable of automatic toolchain selection. Install Git and `make`;
Docker Compose is needed only for the local container quickstart. Linux kernel
headers are needed only for `kernel/module` work.

Keep build caches inside the checkout when reproducing repository checks:

```bash
mkdir -p .cache/go-build .cache/go-mod
GOCACHE="$PWD/.cache/go-build" \
GOMODCACHE="$PWD/.cache/go-mod" \
go test ./...
```

The canonical and public repositories own `go.mod` and `go.sum` independently.
Run `go mod tidy -diff` in the checkout you are changing and do not copy module
metadata between repositories as part of a source sync.

## Codebase Map

| Directory | Responsibility | Start here |
| --- | --- | --- |
| `cmd/` | Process entry points and CLI wiring | The named binary directory, then the package it assembles |
| `internal/` | Repository-private shared clients, configuration, CSI, logging, and dependency helpers | `internal/serviceconfig`, `internal/adminclient`, or the caller's imported package |
| `gateway/` | Authentication, control/HTTP API, dataplane protocol, metadata adapters, SBS gRPC, and volume service | `gateway/service` for storage behavior; `gateway/httpapi` or `gateway/dataplane` for transports |
| `sbs/` | Cluster authority, placement, maintenance, payload, local stores, public/admin/internal gRPC types, and observability | `sbs/cluster`, then the specific authority package |
| `kernel/` | Linux block/control modules and shared UAPI | `kernel/uapi` before changing wire or ioctl/netlink layout |
| `proto/` | Protobuf source definitions | Edit `.proto` here; treat generated Go under `sbs/*/v1` as generated output |
| `iscsi/` | iSCSI target integration, registry application, fencing, and fleet state | `iscsi/control.go`, `iscsi/supervisor.go`, and `iscsi/fleet` |

Each directory has a local `README.md` with interfaces and dependency rules.
Architecture decisions are kept in the repository's
[ADR directory](https://github.com/nosway/namrbd/tree/main/docs/adr).

## Change Workflow

Before editing, write a small contract map:

```text
Contract:
Failure mode:
Touched validation:
Expected observable:
Regression risk:
```

Identify the authoritative writer and distinguish product behavior from test,
smoke, load, and reporting code. Start with the smallest package test that
executes the changed call shape, then expand to nearby packages and
`make test-community` when appropriate.

Useful public gates:

```bash
make format-community-check
make module-metadata-check
make test-community
make docs-source-check
make docs-render-check
```

Shell changes require `bash -n` plus a fixture or command that enters the
modified function. A JSON-producing command must keep JSON on stdout and send
logs or diagnostics to stderr.

## Debugging Go With Delve

Install Delve using your approved tool policy, then verify `dlv version`.
Package-test debugging is the fastest reproducible entry point:

```bash
mkdir -p .cache/go-build .cache/go-mod
GOCACHE="$PWD/.cache/go-build" \
GOMODCACHE="$PWD/.cache/go-mod" \
dlv test ./gateway/service -- \
  -test.run TestCachedMetadataRepositoryCachesExtentPages -test.v
```

At the Delve prompt, set a breakpoint by function or file/line, run `continue`,
inspect variables, and use `goroutines` plus `goroutine <id> stack` for blocked
concurrency.

Example VS Code configuration for the same test:

```json
{
  "name": "Debug gateway/service cache test",
  "type": "go",
  "request": "launch",
  "mode": "test",
  "program": "${workspaceFolder}/gateway/service",
  "args": [
    "-test.run",
    "TestCachedMetadataRepositoryCachesExtentPages",
    "-test.v"
  ],
  "env": {
    "GOCACHE": "${workspaceFolder}/.cache/go-build",
    "GOMODCACHE": "${workspaceFolder}/.cache/go-mod"
  }
}
```

To debug a daemon, use `dlv debug ./cmd/<binary> -- <binary flags>` only with a
disposable configuration and dependencies. The checked-in YAML files illustrate
schema and deployment intent; review defaults, endpoints, TLS, and storage paths
before starting a process. Do not point a debugger at a shared cluster.

## Where To Put A Change

- Transport parsing or status mapping belongs in the transport package; storage
  semantics belong in the service or SBS authority package.
- Gateway caches may accelerate reads but may not become placement authority.
- `sbs-service` owns cluster transitions; `sbs-data` executes node-local work.
- CSI and iSCSI adapt external protocols; they do not redefine volume,
  snapshot, fencing, or reclaim semantics.
- Update `.proto` definitions before generated bindings and run the applicable
  reference drift check.
- Add or supersede an ADR when a change alters durable authority, storage
  engine, external contract, or release-path priority.

## Before Opening A Pull Request

State the contract and failure mode, list exact validation commands and results,
identify processes that require restart/redeployment, and call out tests that
were not run. Keep Community help and documentation free of Enterprise-only
commands; follow the [Edition Boundary Guide](edition-boundary.md) for advanced
features.
