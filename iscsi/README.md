# iSCSI Gateway Library

`iscsi/` adapts NAMRBD volumes to a standard iSCSI target implementation. It
owns protocol/target process behavior and application of an admitted export
registry; it does not own volume geometry, placement, attachment generation, or
storage durability.

Key areas include target control and authentication, the SBS volume adapter,
registry live reload, supervisor lifecycle, health/failover detection, and the
`fleet/` desired/applied registry model. A reload is complete only after the
target has applied the expected revision and each LUN maps to the intended
volume identity and generation.

Community supports basic single-target access with at most three distinct
exported volumes. More than three exports, redundant target paths, MPIO, and
ALUA must be marked `[Enterprise Edition Only]` and remain development/validation
work until matching evidence exists.

Tests should cover SCSI sense mapping, authentication, generation/fencing,
registry reconciliation, bounded reload behavior, disconnect/reconnect, and
cleanup. Protocol success does not substitute for payload readback and
single-writer verification.

