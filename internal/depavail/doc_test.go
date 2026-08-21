package depavail

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const entryPlanPath = "docs/phase-aa-entry-plan.md"

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test working directory")
		}
		dir = parent
	}
}

// docRow is one parsed row of the entry plan Section 4 table.
type docRow struct {
	label      string
	dataPath   string
	control    string
	membership string
	failover   string
	grace      string
	observable string
	raw        string
}

// section4Rows extracts the availability matrix. It fails rather than returning
// an empty slice if the section moves, because a silently empty oracle would
// turn every agreement assertion into a no-op.
func section4Rows(t *testing.T) []docRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), entryPlanPath))
	if err != nil {
		t.Fatalf("read %s: %v", entryPlanPath, err)
	}
	doc := string(raw)
	start := strings.Index(doc, "## 4. Dependency Availability Matrix")
	if start < 0 {
		t.Fatalf("%s no longer has a Section 4 heading; the matrix oracle is gone", entryPlanPath)
	}
	body := doc[start:]
	if end := strings.Index(body, "\n## "); end > 0 {
		body = body[:end]
	}

	var rows []docRow
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		if len(cells) != 7 {
			t.Fatalf("Section 4 row has %d columns, expected 7: %q", len(cells), line)
		}
		if cells[0] == "Dependency state" || strings.HasPrefix(cells[0], "---") {
			continue
		}
		rows = append(rows, docRow{
			label: cells[0], dataPath: cells[1], control: cells[2],
			membership: cells[3], failover: cells[4], grace: cells[5],
			observable: cells[6], raw: line,
		})
	}
	if len(rows) == 0 {
		t.Fatalf("parsed no rows from %s Section 4; the extraction is broken", entryPlanPath)
	}
	return rows
}

// declaredStates maps each document row label to the state it describes. A row
// label that stops matching fails the gate, which is the point: the document
// cannot grow or rename a row without someone deciding what state it means.
var declaredStates = map[string]State{
	"etcd unavailable":                         {Etcd: UnavailableWithinGrace, TiKV: Available, Projection: ProjectionFresh},
	"etcd unavailable beyond grace":            {Etcd: UnavailableBeyondGrace, TiKV: Available, Projection: ProjectionFresh},
	"TiKV or PD unavailable":                   {Etcd: Available, TiKV: UnavailableWithinGrace, Projection: ProjectionFresh},
	"TiKV or PD unavailable beyond grace":      {Etcd: Available, TiKV: UnavailableBeyondGrace, Projection: ProjectionFresh},
	"Projection stale above healthy threshold": {Etcd: Available, TiKV: Available, Projection: ProjectionDegraded},
	"Projection stale above blocked threshold": {Etcd: Available, TiKV: Available, Projection: ProjectionBlocked},
	"Both etcd and TiKV unavailable":           {Etcd: UnavailableWithinGrace, TiKV: UnavailableWithinGrace, Projection: ProjectionFresh},
}

// Every declared row must resolve to exactly what it declares.
func TestResolveAgreesWithEveryDeclaredRow(t *testing.T) {
	rows := section4Rows(t)
	if len(rows) != len(declaredStates) {
		t.Fatalf("Section 4 declares %d rows but the test knows %d states; a row was added or removed without deciding what state it means",
			len(rows), len(declaredStates))
	}
	for _, row := range rows {
		state, ok := declaredStates[row.label]
		if !ok {
			t.Errorf("Section 4 row %q has no state mapping", row.label)
			continue
		}
		got := Resolve(state)

		if want, stated := dataPathFromCell(row.dataPath); stated && got.DataPath != want {
			t.Errorf("row %q data path: document says %s, Resolve says %s", row.label, want, got.DataPath)
		} else if !stated {
			t.Errorf("row %q data path cell %q states no behavior", row.label, row.dataPath)
		}

		control, controlStated := controlPathFromCell(row.control)
		views, viewsStated := viewsFromCell(row.control)
		if !controlStated && !viewsStated {
			t.Errorf("row %q control path cell %q states no behavior", row.label, row.control)
		}
		if controlStated && got.ControlPath != control {
			t.Errorf("row %q control path: document says %s, Resolve says %s", row.label, control, got.ControlPath)
		}
		if viewsStated && got.Views != views {
			t.Errorf("row %q views: document says %s, Resolve says %s", row.label, views, got.Views)
		}

		if !strings.HasPrefix(row.membership, "Rejected") {
			t.Errorf("row %q membership cell %q is not a rejection; every outage row rejects membership change", row.label, row.membership)
		} else if got.MembershipChange != DecisionRejected {
			t.Errorf("row %q membership: document rejects, Resolve says %s", row.label, got.MembershipChange)
		}

		if !strings.HasPrefix(row.failover, "Suppressed") {
			t.Errorf("row %q failover cell %q is not a suppression", row.label, row.failover)
		} else if got.ExportFailover != DecisionSuppressed {
			t.Errorf("row %q failover: document suppresses, Resolve says %s", row.label, got.ExportFailover)
		}

		if strings.Contains(strings.ToLower(row.raw), "export admission blocked") && got.ExportAdmission != DecisionBlocked {
			t.Errorf("row %q blocks export admission, Resolve says %s", row.label, got.ExportAdmission)
		}
	}
}

func dataPathFromCell(cell string) (DataPath, bool) {
	low := strings.ToLower(cell)
	if !strings.HasPrefix(low, "continues") {
		return "", false
	}
	if strings.Contains(low, "marked degraded") {
		return DataPathContinuesDegraded, true
	}
	return DataPathContinues, true
}

func controlPathFromCell(cell string) (ControlPath, bool) {
	low := strings.ToLower(cell)
	if strings.Contains(low, "read-only") {
		return ControlReadOnly, true
	}
	return "", false
}

func viewsFromCell(cell string) (ViewHealth, bool) {
	low := strings.ToLower(cell)
	switch {
	case strings.Contains(low, "refuse to publish as healthy"):
		return ViewUnhealthy, true
	case strings.Contains(low, "marked stale"), strings.Contains(low, "marked degraded"):
		return ViewDegraded, true
	}
	return "", false
}

// The grace column names the configurable thresholds and their defaults. A
// default changed in the document but not in the code, or the reverse, is the
// most likely way this specification goes quietly wrong.
func TestDocumentedGraceDefaultsMatchTheCode(t *testing.T) {
	rows := section4Rows(t)
	defaults := map[string]int{
		"etcd_unavailable_grace_seconds": DefaultEtcdUnavailableGraceSeconds,
		"tikv_unavailable_grace_seconds": DefaultTiKVUnavailableGraceSeconds,
		"projection_stale_degraded_ms":   DefaultProjectionStaleDegradedMS,
		"projection_stale_blocked_ms":    DefaultProjectionStaleBlockedMS,
	}
	named := regexp.MustCompile("`([a-z_]+)` default ([0-9]+)")
	seen := map[string]bool{}
	sawLowerRule := false
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.grace), "lower of the two") {
			sawLowerRule = true
		}
		for _, m := range named.FindAllStringSubmatch(row.grace, -1) {
			key := m[1]
			want, ok := defaults[key]
			if !ok {
				t.Errorf("row %q names threshold %s, which the code does not define", row.label, key)
				continue
			}
			seen[key] = true
			if got := m[2]; got != itoa(want) {
				t.Errorf("threshold %s: document default %s, code default %d", key, got, want)
			}
		}
	}
	for key := range defaults {
		if !seen[key] {
			t.Errorf("the code defines threshold %s but Section 4 never states its default", key)
		}
	}
	if !sawLowerRule {
		t.Error("Section 4 no longer states the lower-of-two-graces rule that EffectiveGraces implements")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// Observables named in the document must be fields something actually emits.
func TestDocumentedObservablesAreEmitted(t *testing.T) {
	rows := section4Rows(t)
	emitted := emittedFields(t)
	named := regexp.MustCompile("`([a-z][a-z0-9_]*)`")
	seen := 0
	for _, row := range rows {
		for _, m := range named.FindAllStringSubmatch(row.observable, -1) {
			seen++
			if !emitted[m[1]] {
				t.Errorf("row %q names observable %s, which no field emits", row.label, m[1])
			}
		}
	}
	if seen == 0 {
		t.Fatal("Section 4 names no observables; the extraction is broken")
	}
}

func emittedFields(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	tag := regexp.MustCompile(`json:"([a-z][a-z0-9_]*)"`)
	for _, rel := range []string{"internal/depavail/status.go", "internal/depavail/thresholds.go"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, m := range tag.FindAllStringSubmatch(string(raw), -1) {
			out[m[1]] = true
		}
	}
	if len(out) == 0 {
		t.Fatal("no json tags found; the extraction is broken")
	}
	return out
}
