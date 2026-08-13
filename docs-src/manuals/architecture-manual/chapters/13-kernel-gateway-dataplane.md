Chapter 14

# Kernel-Gateway Dataplane

## Dataplane

- blk-mq hctx
- lane
- gateway path
- request id

<div class="summary" markdown="1">

The kernel dataplane turns Linux block requests into gateway path requests. It uses blk-mq hardware contexts, lane assignment, persistent TCP connections, request identifiers, pending request tables, and response validation.

The kernel owns host-local path health and retry/no-path behavior. It does not own gateway process recovery, global path-plan policy, SBS placement, or SBS maintenance.

The gateway connection has two surfaces: HTTP JSON control calls for manifest and attachment state, and persistent TCP binary dataplane frames for foreground I/O.

</div>

<div class="diagram" markdown="1">

<div class="diagram-title">Kernel request dispatch</div>

<div class="flow" markdown="1">

<div class="box-accent">blk-mq hctx</div>

<div class="arrow">-\></div>

<div class="box">lane id</div>

<div class="arrow">-\></div>

<div class="box">preferred gateway path</div>

<div class="arrow">-\></div>

<div class="box">persistent TCP connection</div>

<div class="arrow">-\></div>

<div class="box-soft">gateway dataplane handler</div>

</div>

</div>

## Wire Protocol

The dataplane socket is not HTTP. After a lane selects a path, `namrbd_blk.ko` opens or reuses that path's TCP connection and sends NAMRBD binary frames with the `NMBR` magic, protocol version, opcode, flags, request id, volume id, generation, offset, length, and payload as needed. The gateway replies with a response frame that carries status, latency fields, path id, and optional read payload.

## Wire Frame Fields

| Frame Area | Representative Fields | Why It Exists |
|----|----|----|
| Common header | `magic`, `version`, `header_len`, `opcode`, `flags`, `request_id`, `payload_len` | Lets receiver validate framing, match responses, and reject unknown versions or malformed payload lengths. |
| Attachment identity | `volume_id`, `attachment_id`, `generation`, `path_id`, `lane_id` | Binds the request to the admitted attachment and detects stale manifests, stale sockets, and wrong-path replies. |
| I/O locator | `offset`, `length`, `sector`, `flush/fua/discard/write_zeroes flags` | Preserves the block operation identity that gateway maps to SBS reads, writes, flush, discard, or zero semantics. |
| Response status | `status`, `errno`, `completed_len`, `gateway_latency_us`, `backend_latency_us` | Completes the block request and records where time or failure was observed. |
| Authenticated v2 session | `token_id`, `session_id`, `sequence`, `nonce`, `auth_tag`, `expires_at` | Authenticates an admitted attachment, protects against replay on the socket, and supports revocation on generation or manifest change. |

| Wire Shape | When Used | What It Adds | Boundary |
|----|----|----|----|
| wire v1 | Default/plain dataplane frame path. | Binary request/response headers, read/write/flush/discard/write-zeroes opcodes, request id matching, generation checking, and payload framing. | Connection FIFO helps within one path, but it does not create cross-path or cross-gateway ordering. |
| wire v2 | Authenticated read/write dataplane when the manifest carries `dataplane_auth` and gateway is configured for token/HMAC sessions. | `HELLO` / `HELLO_ACK`, token claims, session id, sequence number, HMAC auth tag, replay detection, and session revocation on generation or attachment changes. | Authentication binds a socket to an admitted attachment and path set. It does not replace metadata fencing or SBS commit rules. |
| compatibility HTTP control | Control module attach/info/detach compatibility path. | Simple HTTP/1.1 JSON calls to gateway control endpoints for manifest fetch and detach. | Control HTTP is not used for block payload I/O once the device is attached. |

## Request Identity

Requests carry identifiers so responses can be matched back to pending work. The receive/completion worker validates response opcode, request id, volume id, and generation before completing the block request.

## Path Health

| Condition | Kernel Behavior | Non-Owner Boundary |
|----|----|----|
| One path fails | Mark degraded/down, remap or retry according to policy. | Does not mutate SBS topology. |
| All paths unavailable | Fail fast, queue, or timed queue according to no-path policy. | Does not restart gateway processes. |
| Manifest changes | Apply reconfiguration and recompute lane/path state. | Does not invent global path-plan authority. |

## Multipath Resilience

| Mechanism | Kernel Behavior | Observable Evidence |
|----|----|----|
| Endpoint inventory | The attach manifest or reconfigure manifest supplies multiple dataplane endpoints. The kernel keeps path ids, gateway ids, addresses, ports, priorities, TLS/server-name fields, and per-path counters. | `path_count`, path endpoint fields, `connected`, `submitted`, `completed`, `retries`, `conn_opens`, and `conn_resets`. |
| Lane affinity | Active lanes are mapped to preferred paths. Remap preserves surviving preferred paths where possible so unaffected lanes keep stable affinity. | `active_lane_count`, `nr_hw_queues`, `target_nr_hw_queues`, `lanes[].preferred_path_id`, `lane_remap_count`, and `last_lane_remap_reason`. |
| Fallback and retry | On path error the kernel marks state, closes or avoids unsuitable sockets, and retries another eligible path up to the active path limit. | `last_failed_path_id`, `last_failover_from_path_id`, `last_failover_to_path_id`, per-path `retries`, and path state masks. |
| No-path policy | When no path is eligible, `no_path_retry` chooses fail, unbounded queue, or timed retry behavior. | `no_path_state`, `no_path_queued_reqs`, `no_path_failed_reqs`, `last_no_path_reason`, and recommended path-plan actions. |

This resilience is intentionally host-local. It protects a mounted device from ordinary gateway dataplane path loss when another eligible path exists, but it is not a substitute for gateway process supervision, SBS replica/EC durability, repair/rebuild, fencing, or storage-level read-after-write ordering across gateways.

## Outstanding Requests

The current product/default path keeps one outstanding request per path connection. The transport has request-id machinery, but raising outstanding requests is a guarded performance experiment that requires write ordering, FLUSH/FUA, and read-after-write validation.

[\<- Previous](12-zero-discard-and-reclaim.md) [Next: Topology, Placement, And Expansion -\>](14-topology-placement-and-expansion.md)
