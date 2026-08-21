package depavail

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// No dependency state may produce a non-200. A 503 here would let an
// orchestrator evict a process that entry plan Section 4 requires to keep
// serving, which is how a fail-open specification becomes a fail-closed
// deployment.
func TestReadinessNeverReturnsAFailureStatus(t *testing.T) {
	for _, s := range AllStates() {
		tr := NewTracker(DefaultThresholds())
		tr.state.Store(s)
		rec := httptest.NewRecorder()
		ReadinessHandler(tr).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("state %+v answered %d; a serving process must not be evicted for a metadata outage", s, rec.Code)
		}
	}
}

// The body must carry all three readiness values, or the surface is a boolean
// with three names.
func TestReadinessBodyDistinguishesAllThreeStates(t *testing.T) {
	c := newClock()
	cases := []struct {
		name  string
		setup func(*Tracker)
		want  Readiness
	}{
		{"healthy", func(*Tracker) {}, ReadinessHealthy},
		{"degraded", func(tr *Tracker) {
			tr.Report(DependencyEtcd, errors.New("no leader"))
			tr.Refresh()
		}, ReadinessDegradedServingOnCache},
		{"blocked", func(tr *Tracker) {
			tr.SetProjectionLag(30 * time.Second)
			tr.Refresh()
		}, ReadinessBlocked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewTrackerWithClock(DefaultThresholds(), c.now)
			tc.setup(tr)
			rec := httptest.NewRecorder()
			ReadinessHandler(tr).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			var got Report
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("body is not JSON: %v\n%s", err, rec.Body.String())
			}
			if got.Status.Readiness != tc.want {
				t.Errorf("readiness %s, want %s", got.Status.Readiness, tc.want)
			}
			if tc.want != ReadinessHealthy && len(got.Behavior.Reasons) == 0 {
				t.Error("an unhealthy body carries no reason")
			}
			if got.Behavior.DataPath == "" {
				t.Error("the body does not say what happens to serving")
			}
		})
	}
}

func TestReadinessRejectsNonGet(t *testing.T) {
	rec := httptest.NewRecorder()
	ReadinessHandler(NewTracker(DefaultThresholds())).
		ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST answered %d", rec.Code)
	}
}

// Scraping the endpoint must not change what it reports.
func TestScrapingDoesNotMoveACounter(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.SetProjectionLag(9 * time.Second)
	tr.Refresh()

	read := func() int64 {
		rec := httptest.NewRecorder()
		ReadinessHandler(tr).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		var got Report
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("body: %v", err)
		}
		return got.Status.StaleProjectionCount
	}
	first := read()
	for i := 0; i < 10; i++ {
		read()
	}
	if last := read(); last != first {
		t.Errorf("stale_projection_count moved from %d to %d across scrapes", first, last)
	}
}
