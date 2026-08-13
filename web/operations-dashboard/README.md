# NAMRBD Operations Dashboard

This package contains the read-only web operations dashboard.

Implementation decision:

- Runtime source root: `web/operations-dashboard`.
- Serving route: `/console/` from the `sbs-service` admin HTTP listener.
- Primary API: `/api/v1/sbs/cluster`.
- Runtime dependency policy: dependency-free static HTML/CSS/ES module for the
  Community MVP. A React/Vite dashboard can replace this shell later if the
  project adopts a frontend toolchain.
- Chart backend policy: the dashboard has a small optional billboard.js
  adapter. If a packaged `window.bb.generate` runtime is supplied, capacity and
  maintenance charts can render through billboard.js; otherwise the built-in
  SVG/CSS fallback remains active.
  - Candidate upstream: https://naver.github.io/billboard.js/release/latest/doc/
  - Source/license review anchor: https://github.com/naver/billboard.js
  - Current Community MVP status: not vendored; no external runtime dependency.
- Mutation policy: no product mutation calls, no active apply controls, and no
  raw log/private metadata fetches.

The dashboard can run against live same-origin APIs or bundled sample data:

```text
/console/
/console/?fixture=ok
/console/?fixture=degraded
/console/?fixture=stale
```

Validation:

```bash
make web-operations-dashboard-test
```

The package test is fixture/browser-free. Browser and deployment evidence should
be recorded separately before making broader GUI support claims.
