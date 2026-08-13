# NAMRBD gotgt Fork Notes

This directory is a NAMRBD-managed fork of `github.com/gostor/gotgt`.

- Upstream module: `github.com/gostor/gotgt`
- Imported baseline: `v0.2.2`
- Upstream license: Apache-2.0, preserved in `LICENSE`
- NAMRBD integration: root `go.mod` uses
  `replace github.com/gostor/gotgt => ./third_party/gotgt`

## Patch Policy

Keep upstream package paths unchanged so NAMRBD code can continue importing
`github.com/gostor/gotgt/...` while using this local fork. Prefer small,
reviewable patches with tests in this directory. Do not make broad style
rewrites when a focused protocol or backing-store fix is enough.

## Upstream Refresh Procedure

Use `scripts/gotgt-upstream-refresh.sh` from the repository root. The script
derives the current imported baseline from this file unless
`GOTGT_BASELINE_REF` is set.

1. Commit or stash the current NAMRBD fork changes first.

   ```bash
   make gotgt-upstream-status
   ```

2. Stage the upstream delta without touching `third_party/gotgt`.

   ```bash
   make gotgt-upstream-stage GOTGT_UPSTREAM_REF=<new-tag-or-commit>
   ```

   The stage step writes review artifacts under
   `.cache/gotgt-upstream-refresh/<ref>/`, including the upstream commit list,
   diffstat, name-status list, export tree, and binary-safe patch from the
   imported baseline to the requested ref.

3. Review the staged artifacts for target-stack behavior that may affect
   login negotiation, discovery, INQUIRY/VPD, REPORT SUPPORTED OPERATION CODES,
   READ/WRITE, SYNCHRONIZE CACHE, UNMAP, sense/status mapping, CHAP, and
   initiator allowlist hooks.

4. Apply the upstream delta only after review.

   ```bash
   make gotgt-upstream-apply GOTGT_UPSTREAM_REF=<new-tag-or-commit>
   ```

   The apply step uses `git apply --3way --directory=third_party/gotgt` and
   refuses to run when `third_party/gotgt` has local changes, unless
   `GOTGT_ALLOW_DIRTY=1` is set explicitly. Resolve any conflicts as NAMRBD
   fork changes, not as blind upstream wins.

5. Update `Imported baseline` in this file to the accepted upstream ref, keep
   existing NAMRBD patch notes current, and add a short note for each new
   NAMRBD-only patch or upstream conflict resolution.

6. Run the narrow fork and Phase Q gates before claiming the refresh.

   ```bash
   cd third_party/gotgt
   GOCACHE="$OLDPWD/.cache/go-build" GOMODCACHE="$OLDPWD/.cache/gomod" \
     go test ./pkg/scsi/backingstore/remote -run NAMRBD -v
   cd "$OLDPWD"

   GOCACHE="$PWD/.cache/go-build" GOMODCACHE="$PWD/.cache/gomod" \
     go test -buildvcs=false ./iscsi ./cmd/namrbd-iscsi-gateway ./cmd/namrbd-iscsictl
   make phase-q-closure-regression
   ```

   If the upstream delta changes SCSI command handling, login/session behavior,
   or backing-store dispatch, also run the u01/u11 Linux open-iscsi smoke after
   the change is synced to the lab.

## NAMRBD Patches

### Import Whitespace Hygiene

The upstream `v0.2.2` tree contains trailing whitespace and extra EOF blank
lines in a small set of README/test/helper files. NAMRBD trims those lines so
repo-level `git diff --check` remains usable after the fork import. This is a
mechanical import hygiene change and must not be interpreted as a protocol or
runtime behavior change.

### Remote Backing Store UNMAP Forwarding

Upstream `pkg/scsi/backingstore/remote/remote.go` advertised a remote
`Unmap(int64, int64)` hook through `api.RemoteBackingStore`, but
`RemBackingStore.Unmap` returned success without calling it. That made Linux
`blkdiscard` appear to complete while NAMRBD/SBS never observed an UNMAP row.

NAMRBD forwards each non-zero SCSI UNMAP block descriptor to
`RemBs.Unmap(offset, length)` and propagates the first error. The regression
coverage lives in
`pkg/scsi/backingstore/remote/remote_namrbd_test.go`.

### Remote Backing Store WRITE SAME Zero Forwarding

Upstream `pkg/scsi/backingstore.go` left `WRITE SAME` and `WRITE SAME(16)` as
a TODO. NAMRBD must not return successful completion for zero-like SCSI
commands without preserving the backend zero/discard operation identity.

NAMRBD adds optional zero hooks to the gotgt fork:

- `api.ZeroCapableBackingStore` for target-stack backing stores.
- `api.RemoteZeroCapableBackingStore` for remote NAMRBD adapters.
- `RemBackingStore.Zero` forwarding to the remote zero hook.

The patched command path maps zero-pattern `WRITE SAME` to backend `Zero` and
maps zero-pattern `WRITE SAME` with the UNMAP bit to backend `Unmap`.
Non-zero pattern and LBDATA/PBDATA variants are rejected until fixture evidence
proves a safe mapping. Regression coverage lives in
`pkg/scsi/sbc_test.go` and
`pkg/scsi/backingstore/remote/remote_namrbd_test.go`.

### iSCSI Login Negotiation Compatibility

Upstream `pkg/port/iscsit` handled several login negotiation cases by returning
an internal error, which closed the TCP connection instead of sending a normal
iSCSI Login Response. Windows native iSCSI Initiator probing can include
optional or extension keys and multi-step operational negotiation, so that
behavior made Windows compatibility difficult to diagnose and blocked an
istgt-style login path.

NAMRBD changes the initial login path to negotiate closer to `istgt`:

- `SecurityNegotiation` now selects `AuthMethod=None` in the response when the
  initiator offers it, and returns `AuthMethod=Reject` with login auth failure
  status when it does not.
- Unknown login text keys return `NotUnderstood` instead of closing the
  connection.
- Discovery-session-only operational keys return `Irrelevant`.
- Boolean OR/AND, digest-list, and numerical min/max keys are negotiated by key
  semantics rather than by the old "constant default or fatal error" path.
- Target login responses now carry the request ISID and assigned session TSIH,
  and session-bind failures send an initiator-error login response instead of a
  silent close where the response can still be built.

Regression coverage lives in `pkg/port/iscsit/login_namrbd_test.go`.
Windows native initiator support still requires a real Windows discovery,
login, read/write, readback, flush, and cleanup smoke before NAMRBD product
wording can claim Windows compatibility.

### iSCSI Normal Teardown Log Hygiene

Windows native iSCSI Initiator and Linux open-iscsi can close a connection
after discovery or after receiving a Logout Response while gotgt is waiting for
the next 48-byte BHS. Upstream logged that normal EOF as
`read BHS failed: EOF` at error level, logged normal connection close at warning
level, and treated discovery `CONN_STATE_FULL` as an unexpected state even
though it continued by reading the next request.

NAMRBD keeps true protocol/read failures visible, but reduces normal teardown
noise:

- `io.EOF` while waiting for a BHS is debug-level normal initiator close.
- partial BHS reads remain warning-level, and other read errors remain
  error-level.
- connection-close bookkeeping is debug-level because the preceding protocol
  event or real error carries the useful operator signal.
- Logout Response completion marks the connection closed instead of waiting for
  another BHS that a well-behaved initiator will not send.
- A closed TCP connection reported as Go's `net.ErrClosed` or the runtime
  message `use of closed network connection` while waiting for the next BHS is
  also debug-level normal initiator close. This covers the goroutine race where
  a Windows Logout Response and socket close happen before the receive loop
  observes the connection state transition.
- discovery `CONN_STATE_FULL` is handled as a normal full-feature state rather
  than an unexpected warning.

This patch is log/teardown hygiene only. It does not change SCSI command
semantics, SBS backend I/O, or target login negotiation.

### Implicit ALUA Target Port Group Reporting

Upstream `pkg/scsi` advertised target port group support through INQUIRY bits
and VPD page 0x83 descriptors, but did not implement REPORT TARGET PORT GROUPS.
NAMRBD uses that SCSI surface for iSCSI MPIO path-state evidence, so the fork
adds:

- ALUA fields on target port group config and runtime structs.
- Per-TPGT target port groups in the iSCSI target driver.
- MAINTENANCE IN service action 0x0a handling for REPORT TARGET PORT GROUPS.
- VPD page 0x83 fallback to the first real target port when the command path
  carries relative target port ID 0 during initiator login probing.
- VPD page 0x83 synthetic `SCSI Name,Port` fallback when runtime target port
  metadata is absent, so standard initiator probing cannot panic the target
  stack before ALUA path evidence is observable.
- Regression coverage in `pkg/scsi/spc_test.go`.

This patch reports implicit active optimized/standby path state. Explicit ALUA
state transitions, unit-attention behavior, and product MPIO support claims
remain gated by NAMRBD release evidence.

## Future Patch Candidates

- Windows native iSCSI Initiator lab validation and any remaining
  discovery/SCSI command compatibility discovered by that smoke.
- REPORT SUPPORTED OPERATION CODES, INQUIRY/VPD, and SCSI sense/status
  adjustments needed by standard initiators.
- CHAP and initiator allowlist runtime hooks.
- More precise discard capability advertisement when a backend cannot support
  UNMAP.
