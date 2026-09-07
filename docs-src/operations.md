# Operations

NAMRBD exposes health, bounded fleet views, and explicit cluster lifecycle
commands. Start with aggregate or one-page reads; request point detail only for
the node, volume, or operation being investigated.

## Process health

Check the control service:

```bash
curl -fsS http://service-01.example.com:9081/healthz
curl -fsS http://service-01.example.com:9081/readyz
curl -fsS http://service-01.example.com:9081/metrics
```

Check `sbs-data`, the gateway, and the optional iSCSI gateway through their
configured `/healthz`, `/readyz`, and `/metrics` listeners. Gateway JSON debug
metrics remain available at `/api/v1/debug/gateway/metrics` and
`/api/v1/debug/sbs-cluster/metrics`.

A healthy process endpoint proves only that process's local condition. It does
not prove that the cluster-summary projection is fresh, that all hosts match the
desired manifest, or that maintenance work is complete.

## Normal fleet reads

Use the cluster aggregate for dashboards and routine polling:

```bash
curl -fsS http://service-01.example.com:9081/api/v1/sbs/cluster
sbsctl cluster status --output json
```

Interpret `collection_status`, `projection.health`, `projection.reason`,
`projection.partial`, `projection.stale`, and
`projection.rebuild_required` together. The response also exposes
`request_class` and `metadata_pressure`; normal polling must not report backend
full scans, full completion, or nested completion. A stale or missing aggregate
is returned as typed degraded state and never triggers an automatic legacy
scan.

List commands return one page by default. The default is 128 records and the
maximum is 512:

```bash
sbsctl node list --page-size 128 --output json
sbsctl volume list --health degraded --page-size 128 --output json
sbsctl operations list --state running --page-size 128 --output json
sbsctl repair list --page-size 128 --output json
sbsctl rebalance list --page-size 128 --output json
```

Continue only with the returned opaque `page_token`. Tokens bind the cursor,
filters, and projection/catalog revision; a mismatched or stale token fails
rather than restarting the enumeration. `sbsctl node list --all` is the only
convenience completion path and requires both `--budget` and `--reason`.

For one record, prefer `sbsctl node status`, `sbsctl volume status`,
`sbsctl operations show`, or the HTTP point views:

```bash
curl -fsS 'http://service-01.example.com:9081/api/v1/sbs/node?id=node1'
curl -fsS 'http://service-01.example.com:9081/api/v1/sbs/volume?id=00a1b2c3'
```

## Manifest-based cluster lifecycle

The strict cluster manifest path is intended for a reviewed exact desired
state, including large logical fleets. The shipped 160-node example contains
`node1` through `node160`, eight zones with twenty nodes each, three active
service nodes, and two standby candidates.

Validate and produce immutable artifacts before any execution:

```bash
sbsctl cluster manifest validate \
  --file configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --output json

sbsctl cluster manifest export \
  --file configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --format json \
  --output-file cluster.canonical.json \
  --output json

sbsctl cluster manifest render \
  --file configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --output-dir rendered-empty-directory \
  --output json

sbsctl cluster manifest plan \
  --file configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --plan-output cluster.plan.json \
  --output json
```

Validation rejects unknown or multi-document YAML, duplicate node IDs,
hostnames, addresses, device-by-id claims and filesystem UUIDs, wrong topology
or service placement, secret literals, unapproved artifact digests, incomplete
storage claims, and destructive provisioning. Canonicalization, rendering, and
planning are pure local operations. They perform zero live TiKV mutations,
storage actions, and daemon actions.

Run a read-only check on each intended host and sign the report with that
host's reviewed Ed25519 key:

```bash
sbsctl host check \
  --local \
  --manifest configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --bundle rendered-empty-directory/nodes/node1 \
  --node-id node1 \
  --plan-id <plan-id> \
  --signing-key-id node1-preflight \
  --signing-private-key /run/namrbd-secrets/node1-preflight.pem \
  --signed-report-output reports/node1.json \
  --output json
```

Central admission checks all signatures, freshness, manifest/plan/bundle
digests, and exact node coverage, then creates a new no-mutation join plan:

```bash
sbsctl cluster manifest admit \
  --file configs/sbs-cluster-160.example.yaml \
  --approved-artifact-digest sha256:<approved-digest> \
  --plan-id <plan-id> \
  --reports-dir reports \
  --trust-bundle host-preflight-trust.json \
  --join-plan-output admitted-join-plan.json \
  --output json
```

Start a file-backed rollout operation with `cluster manifest rollout start`.
Use `issue` to obtain one external-transport instruction and `record` to persist
its success or failure. The state machine itself does not copy artifacts,
restart daemons, or mutate TiKV. A failed wave pauses later waves; `retry`
reissues only failed work with the same stable identity, and `resume` requires
an operator reason. Preserve every operation revision as rollout evidence.

## Standby activation and host maintenance

Use `cluster manifest standby plan|issue|verify|status` for a single reviewed
standby activation. The plan requires evidence that one active service is
stopped and isolated, the candidate matches the admitted manifest, and quorum,
leader, and zone safety remain valid. Concurrent activation of both standby
candidates is not allowed.

Use `host maintenance plan|enter|exit|status` for host work. Review quorum,
replica safety, capacity, affected-set drain, active operations, and durable
work queues before stopping a daemon. Unsafe overrides require an exact check
ID, caller, incident/change ID, reason, and an expiry of at most 24 hours.

Maintenance throttles include a global `--total-movements` limit shared by
repair, rebalance, and drain, in addition to per-kind limits. The recorded
foreground-I/O qualification used a global movement limit of one; choose
production limits only from deployment-specific evidence.

See the [generated `sbsctl` reference](reference/cli/sbsctl.md),
[administrator guide](manuals/admin-guide.md), and
[observability guide](observability.md) for the complete command and response
contracts.
