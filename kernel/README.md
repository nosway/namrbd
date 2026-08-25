# Linux Kernel Integration

`kernel/` provides the out-of-tree Linux block/control modules and the shared
userspace ABI used to attach a NAMRBD block device and send I/O through gateway
paths.

- `module/` contains the `namrbd_blk` dataplane module, `namrbd_ctrl` control
  module, build files, and Linux-focused tests/helpers.
- `uapi/` contains headers shared with userspace. Treat structure layout,
  constants, and command identifiers as a versioned interface.

The kernel owns host-local device queues, path health, reconnect/no-path policy,
and application of an admitted manifest. It does not own gateway recovery, SBS
placement, maintenance, or global fencing. Attachment ID, generation,
path-plan revision, device size, and live path status must be checked together.

Build with `make kernel-module` on Linux using headers that match the running
kernel. A successful compile is not runtime-I/O or compatibility evidence;
record kernel/header/module versions and run attach/read/write/flush/discard and
path-failure tests for any support claim.

