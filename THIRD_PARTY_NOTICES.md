# Third-Party Notices

This file summarizes third-party source that is included directly in the NAMRBD
source tree. Go module dependencies resolved through `go.mod` are licensed by
their respective copyright holders and should be reviewed again before a
release artifact is published.

## Directly Included Source

| Path | Upstream | License | Notes |
| --- | --- | --- | --- |
| `third_party/gotgt` | `github.com/gostor/gotgt` | Apache-2.0 | NAMRBD-managed fork. Upstream license is preserved in `third_party/gotgt/LICENSE`; NAMRBD patch notes are in `third_party/gotgt/NAMRBD_PATCHES.md`. |
| `third_party/gotgt/pkg/api/client/transport/cancellable` | Go project derived transport code | BSD-3-Clause | License text is preserved in the source directory and mirrored in `LICENSES/BSD-3-Clause.txt`. |

## Release Checklist

Before publishing a Community source or binary release:

- regenerate the Go dependency inventory from the final exported tree;
- verify direct third-party source retains upstream license files;
- verify `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md`, and `LICENSES/` are
  included in the Community export;
- verify no real `.env`, private lab topology, temporary build/cache
  directories, or generated release artifact content is included.
