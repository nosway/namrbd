package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/sbs/local"
)

func TestReadyzReportsDependencySurface(t *testing.T) {
	handler := observabilityMux("", "", nil, false)

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("/readyz answered %d", ready.Code)
	}
	var report depavail.Report
	if err := json.Unmarshal(ready.Body.Bytes(), &report); err != nil {
		t.Fatalf("/readyz body is not dependency JSON: %v\n%s", err, ready.Body.String())
	}
	if report.Status.Readiness != depavail.ReadinessHealthy {
		t.Errorf("/readyz readiness=%s, want %s", report.Status.Readiness, depavail.ReadinessHealthy)
	}

	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("/healthz answered %d", live.Code)
	}
	if strings.Contains(live.Body.String(), "dependency_readiness") {
		t.Fatalf("/healthz contains dependency state: %s", live.Body.String())
	}
}

func TestObservabilityMuxMultiStoreSmoke(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
		BuildVersion: "test-build",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodGet, "/debug/summary", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read summary: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("summary status=%d body=%s", resp.StatusCode, string(body))
	}
	summary := string(body)
	for _, want := range []string{
		`"id":"fast"`,
		`"shards":2`,
		`"allocation_pages":`,
		`"state":"healthy"`,
		`"capacity_bytes":`,
		`"available_bytes":`,
		`"pebble_disk_usage_bytes":`,
		`"compaction_pending_bytes":`,
		`"id":"bulk"`,
		`"allocation_weight":50`,
		`"weight":50`,
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q in %s", want, summary)
		}
	}

	req = httptest.NewRequest(http.MethodPost, "/debug/store-state?store_id=fast&state=failed", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-state response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-state status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"state":"failed"`) {
		t.Fatalf("store-state response did not include failed state: %s", string(body))
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read metrics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", resp.StatusCode, string(body))
	}
	metrics := string(body)
	for _, want := range []string{
		`sbs_data_store_state{store_id="fast",shard_id="0",state="failed"} 1`,
		`sbs_data_store_state{store_id="fast",shard_id="1",state="failed"} 1`,
		`sbs_data_store_state{store_id="bulk",shard_id="0",state="healthy"} 1`,
		`sbs_data_store_state{store_id="bulk",shard_id="1",state="healthy"} 1`,
		`sbs_data_store_allocation_weight{store_id="fast"} 100`,
		`sbs_data_store_allocation_weight{store_id="bulk"} 50`,
		`sbs_data_store_capacity_bytes{store_id="fast"}`,
		`sbs_data_store_available_bytes{store_id="fast"}`,
		`sbs_data_store_pebble_disk_usage_bytes{store_id="fast"}`,
		`sbs_data_store_compaction_pending_bytes{store_id="fast"}`,
		`sbs_data_store_compaction_in_progress_bytes{store_id="fast"}`,
		`sbs_data_allocation_pages_total`,
		`sbs_data_extent_pages_total`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q in %s", want, metrics)
		}
	}
}

func TestObservabilityMuxChunkGCSweep(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path:         filepath.Join(dir, "meta"),
		BuildVersion: "test-build",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/materialize-volume?volume_id=0000007b&size_bytes=1048576&block_size=4096", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read materialize response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("materialize status=%d body=%s", resp.StatusCode, string(body))
	}

	for i := 0; i < 2; i++ {
		req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/debug/write-pattern?volume_id=0000007b&offset_bytes=0&length_bytes=65536&fill_byte=%02x", 0x41+i), nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp = rec.Result()
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Read write-pattern response: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("write-pattern status=%d body=%s", resp.StatusCode, string(body))
		}
	}

	req = httptest.NewRequest(http.MethodPost, "/debug/chunk-gc", strings.NewReader(`{"volume_id":"0000007b","limit":16,"protected_refs":[{"chunk_id":1}]}`))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read protected chunk-gc response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("protected chunk-gc status=%d body=%s", resp.StatusCode, string(body))
	}
	var protectedPayload struct {
		Result struct {
			DeletedCount  int `json:"deleted_count"`
			RetainedCount int `json:"retained_count"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &protectedPayload); err != nil {
		t.Fatalf("Unmarshal protected chunk-gc response: %v body=%s", err, string(body))
	}
	if protectedPayload.Result.DeletedCount != 0 || protectedPayload.Result.RetainedCount != 1 {
		t.Fatalf("protected chunk-gc result=%+v body=%s", protectedPayload.Result, string(body))
	}

	req = httptest.NewRequest(http.MethodPost, "/debug/chunk-gc?volume_id=0000007b&limit=16", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read unprotected chunk-gc response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unprotected chunk-gc status=%d body=%s", resp.StatusCode, string(body))
	}
	var unprotectedPayload struct {
		Result struct {
			DeletedCount  int `json:"deleted_count"`
			RetainedCount int `json:"retained_count"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &unprotectedPayload); err != nil {
		t.Fatalf("Unmarshal unprotected chunk-gc response: %v body=%s", err, string(body))
	}
	if unprotectedPayload.Result.DeletedCount != 1 || unprotectedPayload.Result.RetainedCount != 0 {
		t.Fatalf("unprotected chunk-gc result=%+v body=%s", unprotectedPayload.Result, string(body))
	}
}

func TestObservabilityMuxRejectsInvalidStoreState(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-state?store_id=fast&state=bogus", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read invalid store-state response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid store-state status=%d body=%s", resp.StatusCode, string(body))
	}
}

func TestObservabilityMuxDisablesLabStoreDebugEndpointsByDefault(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, false)

	for _, path := range []string{
		"/debug/materialize-volume?volume_id=0000007b&size_bytes=1048576&block_size=4096",
		"/debug/write-pattern?volume_id=0000007b&offset_bytes=0&length_bytes=4096&fill_byte=41",
		"/debug/allocation-pages?volume_id=0000007b",
		"/debug/store-shards",
		"/debug/store-state?store_id=fast&state=failed",
		"/debug/store-config-reload",
		"/debug/chunk-gc?volume_id=0000007b&limit=16",
	} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		if resp.StatusCode != http.StatusNotFound {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("%s status=%d body=%s", path, resp.StatusCode, string(body))
		}
	}
}

func TestObservabilityMuxReloadsStoreConfigFromStartupPath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := os.WriteFile(configPath, []byte(`
stores:
  - id: fast
    path: `+filepath.Join(dir, "fast")+`
    shards: 2
    weight: 0
  - id: bulk
    path: `+filepath.Join(dir, "bulk")+`
    shards: 2
    weight: 80
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), configPath, client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-config-reload", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read reload response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reload status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"config_path":"`+configPath+`"`) {
		t.Fatalf("reload response missing config path: %s", string(body))
	}
	if !strings.Contains(string(body), `"id":"fast"`) || !strings.Contains(string(body), `"weight":0`) {
		t.Fatalf("reload response missing updated fast store: %s", string(body))
	}
	if !strings.Contains(string(body), `"id":"bulk"`) || !strings.Contains(string(body), `"weight":80`) {
		t.Fatalf("reload response missing updated bulk store: %s", string(body))
	}
}

func TestObservabilityMuxRejectsStoreConfigReloadWithoutPath(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-config-reload", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read reload response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("reload status=%d body=%s", resp.StatusCode, string(body))
	}
}

func TestObservabilityMuxReloadsStoreWeightsInline(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, false)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-weights", strings.NewReader(`{"stores":[{"store_id":"fast","weight":0},{"store_id":"bulk","weight":80}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-weights response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-weights status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"id":"fast"`) || !strings.Contains(string(body), `"weight":0`) {
		t.Fatalf("store-weights response missing fast update: %s", string(body))
	}
	if !strings.Contains(string(body), `"id":"bulk"`) || !strings.Contains(string(body), `"weight":80`) {
		t.Fatalf("store-weights response missing bulk update: %s", string(body))
	}
	if !strings.Contains(string(body), `"persisted":false`) {
		t.Fatalf("store-weights response should report runtime-only update: %s", string(body))
	}
}

func TestObservabilityMuxPersistsStoreWeightsWhenConfigPathExists(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := os.WriteFile(configPath, []byte(`
stores:
  - id: fast
    path: `+filepath.Join(dir, "fast")+`
    shards: 2
    weight: 100
  - id: bulk
    path: `+filepath.Join(dir, "bulk")+`
    shards: 2
    weight: 50
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), configPath, client, false)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-weights", strings.NewReader(`{"stores":[{"store_id":"fast","weight":0},{"store_id":"bulk","weight":80}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-weights response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-weights status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"persisted":true`) {
		t.Fatalf("store-weights response should report persisted update: %s", string(body))
	}
	reloaded, err := local.LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if reloaded[0].Weight != 0 || reloaded[1].Weight != 80 {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

func TestObservabilityMuxReloadsStoreTuningInline(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, false)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-tuning", strings.NewReader(`{"stores":[{"store_id":"fast","weight":0},{"store_id":"bulk","weight":80}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-tuning response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-tuning status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"weight":0`) {
		t.Fatalf("store-tuning response missing fast tuning update: %s", string(body))
	}
	if !strings.Contains(string(body), `"persisted":false`) {
		t.Fatalf("store-tuning response should report runtime-only update: %s", string(body))
	}
}

func TestObservabilityMuxPersistsStoreTuningWhenConfigPathExists(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := os.WriteFile(configPath, []byte(`
stores:
  - id: fast
    path: `+filepath.Join(dir, "fast")+`
    shards: 2
    weight: 100
  - id: bulk
    path: `+filepath.Join(dir, "bulk")+`
    shards: 2
    weight: 50
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), configPath, client, false)

	req := httptest.NewRequest(http.MethodPost, "/debug/store-tuning", strings.NewReader(`{"stores":[{"store_id":"fast","weight":0},{"store_id":"bulk","weight":80}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-tuning response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-tuning status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"persisted":true`) {
		t.Fatalf("store-tuning response should report persisted update: %s", string(body))
	}
	reloaded, err := local.LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if reloaded[0].Weight != 0 || reloaded[1].Weight != 80 {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

func TestObservabilityMuxAdminStoreTuningPrimaryPathPersistsConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := os.WriteFile(configPath, []byte(`
stores:
  - id: fast
    path: `+filepath.Join(dir, "fast")+`
    shards: 2
    weight: 100
  - id: bulk
    path: `+filepath.Join(dir, "bulk")+`
    shards: 2
    weight: 50
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), configPath, client, false)

	req := httptest.NewRequest(http.MethodPost, "/admin/store-tuning", strings.NewReader(`{"stores":[{"store_id":"fast","weight":0},{"store_id":"bulk","weight":80}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read admin store-tuning response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin store-tuning status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"persisted":true`) || !strings.Contains(string(body), `"weight":0`) {
		t.Fatalf("admin store-tuning response missing persisted tuning update: %s", string(body))
	}

	reloaded, err := local.LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if reloaded[0].Weight != 0 {
		t.Fatalf("fast tuning not persisted: %+v", reloaded)
	}
	if reloaded[1].Weight != 80 {
		t.Fatalf("bulk tuning not persisted: %+v", reloaded)
	}
}

func TestObservabilityMuxWritePatternAndExtentPages(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/materialize-volume?volume_id=0000007b&size_bytes=1048576&block_size=4096&prefix=smoke-7b&allocation_chunk_size_bytes=65536&allocation_page_bytes=262144", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read materialize response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("materialize status=%d body=%s", resp.StatusCode, string(body))
	}
	var materializePayload struct {
		AllocationChunkSizeBytes uint32 `json:"allocation_chunk_size_bytes"`
		AllocationPageBytes      uint32 `json:"allocation_page_bytes"`
		ChunkSizeBytes           uint32 `json:"chunk_size_bytes"`
		ExtentPageBytes          uint32 `json:"extent_page_bytes"`
	}
	if err := json.Unmarshal(body, &materializePayload); err != nil {
		t.Fatalf("Unmarshal materialize response: %v body=%s", err, string(body))
	}
	if materializePayload.AllocationChunkSizeBytes != 65536 || materializePayload.AllocationPageBytes != 262144 ||
		materializePayload.ChunkSizeBytes != 65536 || materializePayload.ExtentPageBytes != 262144 {
		t.Fatalf("materialized geometry allocation_chunk=%d allocation_page=%d chunk=%d page=%d body=%s",
			materializePayload.AllocationChunkSizeBytes, materializePayload.AllocationPageBytes,
			materializePayload.ChunkSizeBytes, materializePayload.ExtentPageBytes, string(body))
	}

	req = httptest.NewRequest(http.MethodPost, "/debug/write-pattern?volume_id=0000007b&offset_bytes=0&length_bytes=1048576&fill_byte=7f", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read write-pattern response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write-pattern status=%d body=%s", resp.StatusCode, string(body))
	}

	req = httptest.NewRequest(http.MethodGet, "/debug/allocation-pages?volume_id=0000007b", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read allocation-pages response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("allocation-pages status=%d body=%s", resp.StatusCode, string(body))
	}
	var payload struct {
		Pages []struct {
			PageBytes      uint32 `json:"page_bytes"`
			ChunkSizeBytes uint32 `json:"chunk_size_bytes"`
			Extents        []struct {
				Kind               string `json:"kind"`
				StoreID            string `json:"store_id"`
				ShardID            uint32 `json:"shard_id"`
				PhysicalChunkStart uint64 `json:"physical_chunk_start"`
			} `json:"extents"`
		} `json:"pages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("Unmarshal extent-pages response: %v body=%s", err, string(body))
	}
	storeIDs := make(map[string]struct{})
	shardRefs := make(map[string]struct{})
	for _, page := range payload.Pages {
		if page.PageBytes != 262144 || page.ChunkSizeBytes != 65536 {
			t.Fatalf("extent page geometry page=%d chunk=%d body=%s", page.PageBytes, page.ChunkSizeBytes, string(body))
		}
		for _, extent := range page.Extents {
			if extent.Kind != "data" {
				continue
			}
			if extent.StoreID == "" {
				t.Fatalf("data extent missing store_id: %s", string(body))
			}
			storeIDs[extent.StoreID] = struct{}{}
			shardRefs[fmt.Sprintf("%s:%d", extent.StoreID, extent.ShardID)] = struct{}{}
		}
	}
	if len(storeIDs) < 2 {
		t.Fatalf("expected writes to distribute across at least two stores: %s", string(body))
	}
	if len(shardRefs) < 2 {
		t.Fatalf("expected writes to touch at least two store/shard refs: %s", string(body))
	}

	req = httptest.NewRequest(http.MethodGet, "/debug/store-shards", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read store-shards response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("store-shards status=%d body=%s", resp.StatusCode, string(body))
	}
	var shardPayload struct {
		Shards []struct {
			StoreID   string `json:"store_id"`
			ShardID   uint32 `json:"shard_id"`
			Path      string `json:"path"`
			ChunkKeys int    `json:"chunk_keys"`
		} `json:"shards"`
	}
	if err := json.Unmarshal(body, &shardPayload); err != nil {
		t.Fatalf("Unmarshal store-shards response: %v body=%s", err, string(body))
	}
	storeChunkCounts := make(map[string]int)
	for _, shard := range shardPayload.Shards {
		if shard.Path == "" {
			t.Fatalf("shard path missing in %s", string(body))
		}
		storeChunkCounts[shard.StoreID] += shard.ChunkKeys
	}
	if storeChunkCounts["fast"] == 0 || storeChunkCounts["bulk"] == 0 {
		t.Fatalf("expected non-zero chunk keys on both stores: %s", string(body))
	}
}

func TestObservabilityMuxMaterializeRejectsGeometryChange(t *testing.T) {
	dir := t.TempDir()
	client, err := local.Open(local.Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []local.StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	handler := observabilityMux(filepath.Join(dir, "meta"), "", client, true)

	req := httptest.NewRequest(http.MethodPost, "/debug/materialize-volume?volume_id=0000007c&size_bytes=1048576&block_size=4096&prefix=smoke-7c&allocation_chunk_size_bytes=65536&allocation_page_bytes=262144", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read initial materialize response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial materialize status=%d body=%s", resp.StatusCode, string(body))
	}

	req = httptest.NewRequest(http.MethodPost, "/debug/materialize-volume?volume_id=0000007c&size_bytes=1048576&block_size=4096&prefix=smoke-7c&allocation_chunk_size_bytes=65536&allocation_page_bytes=524288", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp = rec.Result()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Read conflicting materialize response: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("conflicting materialize status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "volume geometry is immutable") {
		t.Fatalf("conflicting materialize response missing immutable geometry error: %s", string(body))
	}
}
