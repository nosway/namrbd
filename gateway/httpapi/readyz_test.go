package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/depavail"
)

// Liveness and readiness must not be the same answer. Before AA-IMPL-004B both
// routes returned a constant "ok", which told an operator nothing about whether
// the gateway was serving on cache.
func TestReadyzReportsDependencyStateAndHealthzDoesNot(t *testing.T) {
	s := &Server{}
	tr := depavail.NewTracker(depavail.DefaultThresholds())
	tr.SetProjectionLag(30 * time.Second)
	tr.Refresh()
	s.SetDependencyTracker(tr)

	ready := httptest.NewRecorder()
	s.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("/readyz answered %d; a serving gateway must not be evicted for a metadata outage", ready.Code)
	}
	var got depavail.Report
	if err := json.Unmarshal(ready.Body.Bytes(), &got); err != nil {
		t.Fatalf("/readyz body is not JSON: %v\n%s", err, ready.Body.String())
	}
	if got.Status.Readiness != depavail.ReadinessBlocked {
		t.Errorf("/readyz reports %s at a 30s projection lag", got.Status.Readiness)
	}

	live := httptest.NewRecorder()
	s.Handler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if live.Code != http.StatusOK {
		t.Errorf("/healthz answered %d; the process is alive", live.Code)
	}
	if strings.Contains(live.Body.String(), "blocked") {
		t.Error("/healthz carries dependency state; liveness and readiness have been recollapsed")
	}
}

// A tracker installed after the mux was built must still be the one reported
// from, or wiring order silently decides what an operator sees.
func TestReadyzResolvesTheTrackerPerRequest(t *testing.T) {
	s := &Server{}
	mux := s.Handler()

	tr := depavail.NewTracker(depavail.DefaultThresholds())
	tr.SetProjectionLag(30 * time.Second)
	tr.Refresh()
	s.SetDependencyTracker(tr)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var got depavail.Report
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body: %v", err)
	}
	if got.Status.Readiness != depavail.ReadinessBlocked {
		t.Errorf("readiness %s; the tracker installed after Handler() was ignored", got.Status.Readiness)
	}
}

// A gateway with no tracker installed must not look broken.
func TestReadyzWithoutATrackerReportsHealthy(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("answered %d", rec.Code)
	}
	var got depavail.Report
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body: %v", err)
	}
	if got.Status.Readiness != depavail.ReadinessHealthy {
		t.Errorf("readiness %s with no tracker; silence is not evidence of an outage", got.Status.Readiness)
	}
}
