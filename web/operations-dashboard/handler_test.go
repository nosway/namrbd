package opsdashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperationsDashboardHandlerServesIndexAndAssets(t *testing.T) {
	handler := Handler()

	for _, path := range []string{"/console", "/console/", "/console/membership"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "NAMRBD Operations") {
			t.Fatalf("%s did not serve dashboard index", path)
		}
		if got := rec.Header().Get("X-NAMRBD-Dashboard"); got != "read-only" {
			t.Fatalf("%s dashboard header=%q", path, got)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/console/app.js", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("app.js status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Primary") && !strings.Contains(rec.Body.String(), "/api/v1/sbs/cluster") {
		t.Fatalf("app.js did not include primary API client")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/console/assets/namrbd-logo.svg", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("logo status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "image/svg+xml") {
		t.Fatalf("logo Content-Type=%q", got)
	}
	if !strings.Contains(rec.Body.String(), "<svg") {
		t.Fatalf("logo response did not contain svg data")
	}
}

func TestOperationsDashboardHandlerRejectsMutationMethods(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/console/", nil)
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow=%q", got)
	}
}

func TestOperationsDashboardHandlerServesFixtureNoStore(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/console/fixtures/ok/sbs_cluster.json", nil)
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fixture status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("fixture Cache-Control=%q", got)
	}
	if !strings.Contains(rec.Body.String(), "namrbd.sbs.observability.v1") {
		t.Fatalf("fixture does not contain observability schema")
	}
}
