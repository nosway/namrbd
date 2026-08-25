# Architecture Decision Records

Architecture Decision Records (ADRs) capture durable technical choices that
are difficult to infer from the current code alone. They explain the context,
decision, trade-offs, and conditions that could justify revisiting a choice.

ADRs are repository records, not generated site pages. User and operator
manuals remain under `docs-src/`.

## States

- **Proposed:** under review and not yet authoritative.
- **Accepted:** the current design direction.
- **Superseded:** replaced by a later ADR; both records remain in history.
- **Deprecated:** retained for context but should not guide new work.

## Process

1. Copy `0000-template.md` to the next four-digit number.
2. Use a short, stable, lowercase filename.
3. Describe the problem and decision before implementation details.
4. List rejected alternatives and real consequences, including operational and
   migration cost.
5. Link the ADR from a change that implements or materially revises it.
6. Never rewrite an accepted decision to hide history. Add a superseding ADR
   and update the old record's status and superseded-by field.

## Index

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-metadata-authority-separation.md) | Accepted | Separate gateway/control, cluster, and node-local metadata authority |
| [0002](0002-pebble-for-local-sbs-storage.md) | Accepted | Use Pebble for local SBS metadata and payload objects |
| [0003](0003-userspace-gateway-first.md) | Accepted | Qualify the userspace gateway before the kernel datapath |

