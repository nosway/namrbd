package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
)

func TestSplitCommaList(t *testing.T) {
	got := splitCommaList("127.0.0.1:2379, 127.0.0.2:2379 ,,")
	want := []string{"127.0.0.1:2379", "127.0.0.2:2379"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected endpoints: got=%v want=%v", got, want)
	}
}

func TestGatewayCanonicalZeroBase64ForLength(t *testing.T) {
	for _, length := range []int{0, 1, 2, 3, 4, 4096, 65536} {
		got, ok := gatewayCanonicalZeroBase64ForLength(uint64(length))
		if !ok {
			t.Fatalf("zero base64 length=%d was not cacheable", length)
		}
		want := base64.StdEncoding.EncodeToString(make([]byte, length))
		if got != want {
			t.Fatalf("unexpected zero base64 length=%d: got=%q want=%q", length, got, want)
		}
	}
}

func TestDecodeGatewayReadOperationResponseZeroFastPathValidatesLength(t *testing.T) {
	encoded, ok := gatewayCanonicalZeroBase64ForLength(4096)
	if !ok {
		t.Fatal("zero base64 length=4096 was not cacheable")
	}
	respBody := []byte(fmt.Sprintf(`{"volume_id":"00000065","offset_bytes":0,"length_bytes":4096,"data_base64":%q}`, encoded))
	if _, err := decodeGatewayReadOperationResponse(respBody, nil, 4096); err != nil {
		t.Fatalf("decode zero operation response failed: %v", err)
	}
	if _, err := decodeGatewayReadOperationResponse(respBody, nil, 8192); err == nil {
		t.Fatal("expected length mismatch to fail")
	}
}

func TestNewObjectStore(t *testing.T) {
	objects, cleanup, err := newObjectStore(context.Background(), &commandConfig{storeBackend: "memory"})
	if err != nil {
		t.Fatalf("newObjectStore(memory) failed: %v", err)
	}
	defer cleanup()
	if _, ok := objects.(*store.MemoryStore); !ok {
		t.Fatalf("expected memory store, got %T", objects)
	}
}

func TestParseRequiredVolumeID(t *testing.T) {
	if got, err := parseRequiredVolumeID("00000065"); err != nil || got != 101 {
		t.Fatalf("expected volume id 101, got=%d err=%v", got, err)
	}
	if _, err := parseRequiredVolumeID("123"); err == nil {
		t.Fatalf("expected invalid id to fail")
	}
}

func TestParseByteSize(t *testing.T) {
	cases := map[string]uint64{
		"4096": 4096,
		"4k":   4 << 10,
		"16m":  16 << 20,
		"2g":   2 << 30,
	}
	for raw, want := range cases {
		got, err := parseByteSize(raw)
		if err != nil {
			t.Fatalf("parseByteSize(%q) failed: %v", raw, err)
		}
		if got != want {
			t.Fatalf("parseByteSize(%q)=%d want=%d", raw, got, want)
		}
	}
	for _, raw := range []string{"", "0", "4x"} {
		if _, err := parseByteSize(raw); err == nil {
			t.Fatalf("parseByteSize(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestPercentile(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	if got := percentile(values, 50); got != 3 {
		t.Fatalf("p50=%f want=3", got)
	}
	if got := percentile(values, 95); got <= 4 || got > 5 {
		t.Fatalf("p95=%f outside expected interpolation range", got)
	}
}

func TestGatewayBuildPayloadMismatchReportsFirstByteAndSegments(t *testing.T) {
	want := make([]byte, 8<<10)
	got := make([]byte, len(want))
	for i := range want {
		want[i] = byte(i % 251)
		got[i] = want[i]
	}
	got[4096] ^= 0xff
	got[4097] ^= 0xff

	report := gatewayBuildPayloadMismatch(128<<10, want, got)
	firstMismatchOffset := uint64(132 << 10)
	if report.FirstMismatchOffsetBytes != firstMismatchOffset {
		t.Fatalf("first mismatch offset=%d want=%d", report.FirstMismatchOffsetBytes, firstMismatchOffset)
	}
	if report.FirstMismatchInReadBytes != 4<<10 {
		t.Fatalf("first mismatch in read=%d want=%d", report.FirstMismatchInReadBytes, 4<<10)
	}
	if report.WindowOffsetBytes != firstMismatchOffset-16 || report.WindowLengthBytes != 32 {
		t.Fatalf("unexpected window offset=%d length=%d", report.WindowOffsetBytes, report.WindowLengthBytes)
	}
	if len(report.DifferingSegments) != 1 {
		t.Fatalf("differing segment count=%d want=1", len(report.DifferingSegments))
	}
	segment := report.DifferingSegments[0]
	if segment.Index != 1 || segment.OffsetBytes != firstMismatchOffset || segment.LengthBytes != 4<<10 {
		t.Fatalf("unexpected segment: %+v", segment)
	}
}

func TestRunGatewayWriteLoad(t *testing.T) {
	writeCount := 0
	detachCount := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			body = `{"attachment_id":"att-00000065-0001","generation":1}`
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			writeCount++
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			detachCount++
			body = `{"status":"detached"}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.Result != "ok" || report.OKCount != 2 || report.ErrorCount != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if writeCount != 2 || detachCount != 1 {
		t.Fatalf("unexpected request counts: writes=%d detach=%d", writeCount, detachCount)
	}
}

func TestRunGatewayWriteLoadReportsAttachFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach") {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader(`{"error":"etcdserver: mvcc: database space exceeded"}`)),
				Header:     make(http.Header),
			}, nil
		}
		t.Fatalf("unexpected request after attach failure: %s %s", r.Method, r.URL.Path)
		return nil, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err == nil {
		t.Fatalf("expected attach failure")
	}
	if report.Result != "error" || report.ErrorCount != 1 || report.OKCount != 0 {
		t.Fatalf("unexpected failure report: %+v", report)
	}
	if !strings.Contains(report.FirstError, "attach failed:") || !strings.Contains(report.FirstError, "database space exceeded") {
		t.Fatalf("first_error=%q, want attach database-space detail", report.FirstError)
	}
	if report.LastError != report.FirstError {
		t.Fatalf("last_error=%q first_error=%q", report.LastError, report.FirstError)
	}
}

func TestRunGatewayWriteLoadCanVerifyUsingSourceVolumeSeed(t *testing.T) {
	sourceVolumeID := "00000065"
	targetVolumeID := service.CanonicalVolumeID(102)
	readCount := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/read") {
			readCount++
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				LengthBytes uint64 `json:"length_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode read request: %v", err)
			}
			payload := gatewayDeterministicPayload(sourceVolumeID, req.OffsetBytes, req.LengthBytes)
			body := fmt.Sprintf(`{"data_base64":%q}`, base64.StdEncoding.EncodeToString(payload))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:          "http://gateway.example.test",
		VolumeID:            102,
		HostID:              "test-host",
		DeviceID:            7,
		SizeRaw:             "8k",
		BSRaw:               "4k",
		RW:                  "read",
		Prefill:             false,
		IODepth:             1,
		NumJobs:             1,
		Concurrency:         1,
		PayloadPattern:      "deterministic",
		PayloadVerifyVolume: sourceVolumeID,
		Verify:              true,
		Timeout:             5 * time.Second,
		Attach:              false,
		Detach:              false,
		HTTPClient:          client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.VolumeID != targetVolumeID || report.PayloadVerifyVolume != sourceVolumeID {
		t.Fatalf("unexpected volume/verify seed report: %+v", report)
	}
	if report.Result != "ok" || report.VerifyOKCount != 2 || report.VerifyErrorCount != 0 {
		t.Fatalf("unexpected verify report: %+v", report)
	}
	if readCount != 4 {
		t.Fatalf("read count=%d want=4", readCount)
	}
}

func TestRunGatewayWriteLoadRejectsInvalidVerifyVolumeSeed(t *testing.T) {
	_, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:          "http://gateway.example.test",
		VolumeID:            102,
		HostID:              "test-host",
		DeviceID:            7,
		SizeRaw:             "4k",
		BSRaw:               "4k",
		IODepth:             1,
		NumJobs:             1,
		Concurrency:         1,
		PayloadPattern:      "deterministic",
		PayloadVerifyVolume: "not-hex",
		Verify:              true,
		Attach:              false,
		Detach:              false,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			t.Fatalf("unexpected HTTP request: %s %s", r.Method, r.URL)
			return nil, nil
		})},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid --verify-volume") {
		t.Fatalf("err=%v want invalid --verify-volume", err)
	}
}

func TestRunGatewayWriteLoadDistributesAcrossKernelLanes(t *testing.T) {
	writeCounts := map[string]int{}
	var writeCountsMu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write") {
			writeCountsMu.Lock()
			writeCounts[r.URL.Host]++
			writeCountsMu.Unlock()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURLs: "http://gateway-a.example.test,http://gateway-b.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     2,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Attach:      false,
		Detach:      false,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.GatewayPolicy != "kernel-lane" || report.ActiveLaneCount != 2 {
		t.Fatalf("unexpected gateway lane policy: %+v", report)
	}
	if writeCounts["gateway-a.example.test"] != 2 || writeCounts["gateway-b.example.test"] != 2 {
		t.Fatalf("unexpected write distribution: writes=%v report=%+v", writeCounts, report.GatewayRequestCounts)
	}
	if report.GatewayRequestCounts["http://gateway-a.example.test"] != 2 ||
		report.GatewayRequestCounts["http://gateway-b.example.test"] != 2 {
		t.Fatalf("unexpected report gateway counts: %+v", report.GatewayRequestCounts)
	}
}

func TestGatewayWriteLoadTargetsSequentialBlocksAcrossActiveLanes(t *testing.T) {
	targets := newGatewayTargetSet([]string{
		"http://gateway-a.example.test",
		"http://gateway-b.example.test",
		"http://gateway-c.example.test",
		"http://gateway-d.example.test",
	}, 4, 4, 16)

	var got []int
	for idx := 0; idx < 8; idx++ {
		_, laneID := targets.targetForWriteLoadIndex(idx, 64)
		got = append(got, laneID)
	}
	want := []int{0, 1, 2, 3, 0, 1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("first sequential write-load lanes=%v want %v", got, want)
	}
}

func TestRunGatewayReplayLoadReplaysNormalizedTrace(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "kernel-origin-trace.jsonl")
	traceJSONL := strings.Join([]string{
		`{"seq":1,"op":"write","offset_bytes":0,"length_bytes":4096,"gateway_id":"gw-gateway-a.example.test","path_id":0,"status_code":0,"replay_eligible":true}`,
		`{"seq":2,"op":"write_zeroes","offset_bytes":4096,"length_bytes":4096,"gateway_id":"gw-gateway-b.example.test","path_id":1,"status_code":0,"replay_eligible":true}`,
		`{"seq":3,"op":"flush","offset_bytes":0,"length_bytes":0,"gateway_id":"gw-gateway-b.example.test","path_id":1,"status_code":0,"replay_eligible":true}`,
		`{"seq":4,"op":"read","offset_bytes":0,"length_bytes":4096,"gateway_id":"gw-gateway-a.example.test","path_id":0,"status_code":0,"replay_eligible":true}`,
		`{"seq":5,"op":"write","offset_bytes":8192,"length_bytes":4096,"path_id":0,"status_code":14,"replay_eligible":false}`,
	}, "\n") + "\n"
	if err := os.WriteFile(tracePath, []byte(traceJSONL), 0o600); err != nil {
		t.Fatalf("write trace fixture: %v", err)
	}

	type rangeKey struct {
		offset uint64
		length uint64
	}
	stored := map[rangeKey][]byte{}
	hostCounts := map[string]int{}
	detachCounts := map[string]int{}
	var mu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			body = `{"attachment_id":"att-00000065-0001","generation":1}`
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				LengthBytes uint64 `json:"length_bytes"`
				DataBase64  string `json:"data_base64"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode write request: %v", err)
			}
			payload, err := base64.StdEncoding.DecodeString(req.DataBase64)
			if err != nil {
				t.Fatalf("decode write payload: %v", err)
			}
			mu.Lock()
			hostCounts[r.URL.Host]++
			stored[rangeKey{offset: req.OffsetBytes, length: req.LengthBytes}] = payload
			mu.Unlock()
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/zero"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				LengthBytes uint64 `json:"length_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode zero request: %v", err)
			}
			mu.Lock()
			hostCounts[r.URL.Host]++
			stored[rangeKey{offset: req.OffsetBytes, length: req.LengthBytes}] = make([]byte, req.LengthBytes)
			mu.Unlock()
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/flush"):
			mu.Lock()
			hostCounts[r.URL.Host]++
			mu.Unlock()
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/read"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				LengthBytes uint64 `json:"length_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode read request: %v", err)
			}
			mu.Lock()
			hostCounts[r.URL.Host]++
			payload := stored[rangeKey{offset: req.OffsetBytes, length: req.LengthBytes}]
			if payload == nil {
				payload = make([]byte, req.LengthBytes)
			}
			mu.Unlock()
			body = fmt.Sprintf(`{"data_base64":%q}`, base64.StdEncoding.EncodeToString(payload))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			mu.Lock()
			detachCounts[r.URL.Host]++
			mu.Unlock()
			body = `{"status":"detached"}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayReplayLoad(gatewayReplayLoadOptions{
		GatewayURLs:    "http://gateway-a.example.test,http://gateway-b.example.test",
		ActiveLanes:    2,
		TraceJSONL:     tracePath,
		VolumeID:       101,
		HostID:         "test-host",
		DeviceID:       8,
		Mode:           "saturating",
		Concurrency:    1,
		PayloadPattern: "zero",
		Verify:         true,
		Attach:         true,
		Detach:         true,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("runGatewayReplayLoad failed: %v", err)
	}
	if report.Result != "ok" || report.TraceOperationCount != 5 || report.RequestCount != 4 || report.SkippedOperationCount != 1 {
		t.Fatalf("unexpected replay report counts: %+v", report)
	}
	if report.WriteOKCount != 1 || report.ZeroOKCount != 1 || report.FlushOKCount != 1 || report.ReadOKCount != 1 || report.VerifyOKCount != 2 {
		t.Fatalf("unexpected operation counts: %+v", report)
	}
	if report.ReplaySelectionCounts["gateway_id"] != 4 {
		t.Fatalf("expected gateway_id replay selection for all replay ops: %+v", report.ReplaySelectionCounts)
	}
	if report.ClaimClassification != "kernel_origin_shape_only" || report.SupportClaimed || report.PublicBenchmarkClaimed || report.KernelPayloadReplayed {
		t.Fatalf("unexpected claim fields: %+v", report)
	}
	if report.GatewayRequestCounts["http://gateway-a.example.test"] != 2 || report.GatewayRequestCounts["http://gateway-b.example.test"] != 2 {
		t.Fatalf("unexpected replay gateway request counts: %+v", report.GatewayRequestCounts)
	}
	if len(report.ReplayLaneCounts) != 2 ||
		report.ReplayLaneCounts[0].LaneID != 0 || report.ReplayLaneCounts[0].GatewayID != "gw-gateway-a.example.test" || report.ReplayLaneCounts[0].RequestCount != 2 ||
		report.ReplayLaneCounts[1].LaneID != 1 || report.ReplayLaneCounts[1].GatewayID != "gw-gateway-b.example.test" || report.ReplayLaneCounts[1].RequestCount != 2 {
		t.Fatalf("unexpected replay lane counts: %+v", report.ReplayLaneCounts)
	}
	if hostCounts["gateway-a.example.test"] == 0 || hostCounts["gateway-b.example.test"] == 0 {
		t.Fatalf("expected both gateway hosts to receive replay/verify traffic: %+v", hostCounts)
	}
	if detachCounts["gateway-a.example.test"] != 1 || detachCounts["gateway-b.example.test"] != 1 {
		t.Fatalf("expected cleanup detach on both gateway hosts, got %+v", detachCounts)
	}
}

func TestRunGatewayReplayLoadPrefersGatewayIDOverPathID(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "kernel-origin-trace.jsonl")
	traceJSONL := strings.Join([]string{
		`{"seq":1,"op":"write","offset_bytes":0,"length_bytes":4096,"gateway_id":"gw-gateway-a.example.test","path_id":0,"status_code":0,"replay_eligible":true}`,
		`{"seq":2,"op":"write","offset_bytes":4096,"length_bytes":4096,"gateway_id":"gw-gateway-b.example.test","path_id":0,"status_code":0,"replay_eligible":true}`,
	}, "\n") + "\n"
	if err := os.WriteFile(tracePath, []byte(traceJSONL), 0o600); err != nil {
		t.Fatalf("write trace fixture: %v", err)
	}

	hostCounts := map[string]int{}
	detachCounts := map[string]int{}
	var mu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			body = `{"attachment_id":"att-00000065-0001","generation":1}`
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			mu.Lock()
			hostCounts[r.URL.Host]++
			mu.Unlock()
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			mu.Lock()
			detachCounts[r.URL.Host]++
			mu.Unlock()
			body = `{"status":"detached"}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayReplayLoad(gatewayReplayLoadOptions{
		GatewayURLs:    "http://gateway-a.example.test,http://gateway-b.example.test",
		ActiveLanes:    2,
		TraceJSONL:     tracePath,
		VolumeID:       101,
		HostID:         "test-host",
		DeviceID:       8,
		Mode:           "saturating",
		Concurrency:    1,
		PayloadPattern: "zero",
		Verify:         false,
		Attach:         true,
		Detach:         true,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("runGatewayReplayLoad failed: %v", err)
	}
	if report.GatewayRequestCounts["http://gateway-a.example.test"] != 1 || report.GatewayRequestCounts["http://gateway-b.example.test"] != 1 {
		t.Fatalf("expected gateway_id-based split, got %+v", report.GatewayRequestCounts)
	}
	if report.ReplaySelectionCounts["gateway_id"] != 2 {
		t.Fatalf("expected gateway_id replay selection, got %+v", report.ReplaySelectionCounts)
	}
	if hostCounts["gateway-a.example.test"] != 1 || hostCounts["gateway-b.example.test"] != 1 {
		t.Fatalf("expected one replay request per gateway host, got %+v", hostCounts)
	}
	if detachCounts["gateway-a.example.test"] != 1 || detachCounts["gateway-b.example.test"] != 1 {
		t.Fatalf("expected cleanup detach on both gateway hosts, got %+v", detachCounts)
	}
}

func TestRunGatewayWriteLoadCanOverrideActiveLanes(t *testing.T) {
	writeCounts := map[string]int{}
	var writeCountsMu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write") {
			writeCountsMu.Lock()
			writeCounts[r.URL.Host]++
			writeCountsMu.Unlock()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURLs: "http://gateway-a.example.test,http://gateway-b.example.test,http://gateway-c.example.test,http://gateway-d.example.test",
		ActiveLanes: 4,
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "4k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     4,
		Concurrency: 4,
		Timeout:     5 * time.Second,
		Attach:      false,
		Detach:      false,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.ActiveLaneCount != 4 {
		t.Fatalf("active_lane_count=%d want 4", report.ActiveLaneCount)
	}
	for _, host := range []string{"gateway-a.example.test", "gateway-b.example.test", "gateway-c.example.test", "gateway-d.example.test"} {
		if writeCounts[host] != 1 {
			t.Fatalf("host %s writes=%d want 1; all writes=%v", host, writeCounts[host], writeCounts)
		}
	}
}

func TestRunGatewayWriteLoadReportsDetachEvidence(t *testing.T) {
	detachCounts := map[string]int{}
	var mu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			body = `{"attachment_id":"att-00000065-0001","generation":1}`
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			mu.Lock()
			detachCounts[r.URL.Host]++
			mu.Unlock()
			body = `{"status":"detached"}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURLs: "http://gateway-a.example.test,http://gateway-b.example.test",
		ActiveLanes: 2,
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		RW:          "write",
		IODepth:     1,
		NumJobs:     2,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if !report.Detached || report.DetachError != "" {
		t.Fatalf("missing clean detach evidence: %+v", report)
	}
	if report.DetachWarning != "" || report.DetachAttemptCount != 2 || report.DetachOKCount != 2 || report.DetachConflictCount != 0 {
		t.Fatalf("unexpected clean detach summary: %+v", report)
	}
	if detachCounts["gateway-a.example.test"] != 1 || detachCounts["gateway-b.example.test"] != 1 {
		t.Fatalf("expected cleanup detach on both gateway hosts, got %+v", detachCounts)
	}
}

func TestRunGatewayWriteLoadIgnoresPeerDetachConflictAfterSuccessfulFanout(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"attachment_id":"att-00000065-0001","generation":1}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			if r.URL.Host == "gateway-b.example.test" {
				return &http.Response{
					StatusCode: http.StatusConflict,
					Body:       io.NopCloser(strings.NewReader(`volume is attached to another host`)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"detached"}`)),
				Header:     make(http.Header),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURLs: "http://gateway-a.example.test,http://gateway-b.example.test",
		ActiveLanes: 2,
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		RW:          "write",
		IODepth:     1,
		NumJobs:     2,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.Result != "ok" || !report.Detached || report.DetachError != "" || report.ErrorCount != 0 {
		t.Fatalf("unexpected peer conflict report: %+v", report)
	}
	if report.DetachAttemptCount != 2 || report.DetachOKCount != 1 || report.DetachConflictCount != 1 {
		t.Fatalf("unexpected peer conflict detach counts: %+v", report)
	}
	if !strings.Contains(report.DetachWarning, "gateway-b.example.test") ||
		!strings.Contains(report.DetachWarning, "volume is attached to another host") {
		t.Fatalf("missing peer conflict warning: %+v", report)
	}
}

func TestRunGatewayWriteLoadFailsWhenDetachFails(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"attachment_id":"att-00000065-0001","generation":1}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader(`{"error":"close timed out"}`)),
				Header:     make(http.Header),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "4k",
		BSRaw:       "4k",
		RW:          "write",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err == nil {
		t.Fatalf("expected detach failure")
	}
	if report.Result != "error" || report.Detached || report.DetachError == "" {
		t.Fatalf("unexpected detach failure report: %+v", report)
	}
	if !strings.Contains(report.FirstError, "detach failed") || !strings.Contains(report.LastError, "detach failed") {
		t.Fatalf("missing detach error summary: %+v", report)
	}
}

func TestRunGatewayWriteLoadFailsWhenOnlyDetachConflictObserved(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"attachment_id":"att-00000065-0001","generation":1}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}, nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/detach"):
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader(`volume is attached to another host`)),
				Header:     make(http.Header),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "4k",
		BSRaw:       "4k",
		RW:          "write",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      true,
		Detach:      true,
		HTTPClient:  client,
	})
	if err == nil {
		t.Fatalf("expected detach conflict failure")
	}
	if report.Result != "error" || report.Detached || report.DetachError == "" || report.DetachOKCount != 0 {
		t.Fatalf("unexpected detach conflict report: %+v", report)
	}
	if !strings.Contains(report.DetachError, "detach conflicts without successful cleanup") {
		t.Fatalf("missing strict detach conflict error: %+v", report)
	}
}

func TestRunGatewayWriteLoadReportsPhaseOThrottleWait(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write") {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"status":"ok",
					"phase_o_throttle":{
						"policy_id":"gateway-local",
						"policy_generation":1,
						"cap_scope":"lab_only",
						"throttle_mode":"wait",
						"requested_tokens":1,
						"granted_tokens":1,
						"requested_bytes":4096,
						"granted_bytes":4096,
						"throttled_ops":1,
						"throttled_bytes":0,
						"throttle_wait_ms":7,
						"rejected_ops":0,
						"iops_cap":1,
						"enforced_before_dispatch":true,
						"cluster_wide_cap_support":false,
						"gateway_restart_required":true,
						"remote_lab_validation_state":"required"
					}
				}`)),
				Header: make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      false,
		Detach:      false,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.PhaseOThrottle == nil || !report.PhaseOThrottle.Observed {
		t.Fatalf("missing phase_o_throttle report: %+v", report)
	}
	if report.PhaseOThrottle.CapScope != "lab_only" ||
		report.PhaseOThrottle.ThrottleMode != "wait" ||
		report.PhaseOThrottle.ThrottleWaitCount != 2 ||
		report.PhaseOThrottle.ThrottleWaitTotalMS != 14 ||
		report.PhaseOThrottle.ThrottleWaitMaxMS != 7 ||
		report.PhaseOThrottle.RequestedTokens != 2 ||
		report.PhaseOThrottle.GrantedTokens != 2 ||
		report.PhaseOThrottle.ClusterWideCapSupport {
		t.Fatalf("unexpected phase_o_throttle report: %+v", report.PhaseOThrottle)
	}
}

func TestRunGatewayWriteLoadReportsPhaseOThrottleReject(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write") {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Body: io.NopCloser(strings.NewReader(`{
					"error":"phase_o_throttle_rejected",
					"rejection_reason":"cap_exceeded",
					"phase_o_throttle":{
						"policy_id":"gateway-local",
						"policy_generation":1,
						"cap_scope":"per_gateway",
						"throttle_mode":"reject",
						"requested_tokens":1,
						"granted_tokens":0,
						"requested_bytes":4096,
						"granted_bytes":0,
						"throttled_ops":1,
						"throttled_bytes":0,
						"throttle_wait_ms":0,
						"rejected_ops":1,
						"rejection_reason":"cap_exceeded",
						"iops_cap":1,
						"enforced_before_dispatch":true,
						"cluster_wide_cap_support":false,
						"gateway_restart_required":true,
						"remote_lab_validation_state":"required"
					}
				}`)),
				Header: make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "4k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      false,
		Detach:      false,
		HTTPClient:  client,
	})
	if err == nil {
		t.Fatalf("expected rejected load error")
	}
	if report.PhaseOThrottle == nil || !report.PhaseOThrottle.Observed {
		t.Fatalf("missing phase_o_throttle report: %+v", report)
	}
	if report.ErrorCount != 1 ||
		report.PhaseOThrottle.RejectedOps != 1 ||
		report.PhaseOThrottle.RejectionReasons["cap_exceeded"] != 1 ||
		report.PhaseOThrottle.GrantedTokens != 0 ||
		report.PhaseOThrottle.CapScope != "per_gateway" ||
		report.PhaseOThrottle.ThrottleMode != "reject" {
		t.Fatalf("unexpected rejected report: %+v", report)
	}
}

func TestRunGatewayWriteLoadReportsPhaseOSharedBudgetLease(t *testing.T) {
	var writeCount int
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write") {
			writeCount++
			body := fmt.Sprintf(`{
				"status":"ok",
				"phase_o_throttle":{
					"policy_id":"cluster-volume-policy",
					"policy_generation":3,
					"cap_scope":"cluster_volume",
					"throttle_mode":"wait",
					"requested_tokens":1,
					"granted_tokens":1,
					"denied_tokens":0,
					"requested_bytes":4096,
					"granted_bytes":4096,
					"denied_bytes":0,
					"throttled_ops":%d,
					"throttled_bytes":%d,
					"throttle_wait_ms":%d,
					"rejected_ops":0,
					"lease_id":"lease-%d",
					"lease_generation":%d,
					"shared_budget_authority":true,
					"gateway_consumes_lease":true,
					"iops_cap":1,
					"enforced_before_dispatch":true,
					"cluster_wide_cap_support":true,
					"gateway_restart_required":true,
					"remote_lab_validation_state":"required"
				}
			}`, writeCount-1, (writeCount-1)*4096, (writeCount-1)*7, writeCount, writeCount)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:  "http://gateway.example.test",
		VolumeID:    101,
		HostID:      "test-host",
		DeviceID:    7,
		SizeRaw:     "8k",
		BSRaw:       "4k",
		IODepth:     1,
		NumJobs:     1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Attach:      false,
		Detach:      false,
		HTTPClient:  client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.PhaseOThrottle == nil || !report.PhaseOThrottle.Observed {
		t.Fatalf("missing phase_o_throttle report: %+v", report)
	}
	throttle := report.PhaseOThrottle
	if throttle.CapScope != "cluster_volume" ||
		throttle.ThrottleMode != "wait" ||
		!throttle.ClusterWideCapSupport ||
		!throttle.SharedBudgetAuthority ||
		!throttle.GatewayConsumesLease ||
		!throttle.EnforcedBeforeDispatch ||
		throttle.LeaseCount != 2 ||
		throttle.FirstLeaseID != "lease-1" ||
		throttle.LastLeaseID != "lease-2" ||
		throttle.MaxLeaseGeneration != 2 ||
		throttle.RequestedTokens != 2 ||
		throttle.GrantedTokens != 2 ||
		throttle.ThrottleWaitCount != 1 ||
		throttle.ThrottleWaitTotalMS != 7 ||
		throttle.ThrottledOps != 1 ||
		throttle.ThrottledBytes != 4096 {
		t.Fatalf("unexpected shared lease report: %+v", throttle)
	}
}

func TestRunGatewayWriteLoadVerifiesDeterministicPayload(t *testing.T) {
	writes := map[uint64]string{}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				DataBase64  string `json:"data_base64"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode write request: %v", err)
			}
			writes[req.OffsetBytes] = req.DataBase64
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/read"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode read request: %v", err)
			}
			dataBase64, ok := writes[req.OffsetBytes]
			if !ok {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
					Header:     make(http.Header),
				}, nil
			}
			body = `{"data_base64":` + strconv.Quote(dataBase64) + `}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:     "http://gateway.example.test",
		VolumeID:       101,
		HostID:         "test-host",
		DeviceID:       7,
		SizeRaw:        "8k",
		BSRaw:          "4k",
		IODepth:        1,
		NumJobs:        2,
		Concurrency:    1,
		Timeout:        5 * time.Second,
		PayloadPattern: "deterministic",
		Verify:         true,
		Attach:         false,
		Detach:         false,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.VerifyCount != 2 || report.VerifyOKCount != 2 || report.VerifyErrorCount != 0 {
		t.Fatalf("unexpected verify report: %+v", report)
	}
	if len(writes) != 2 {
		t.Fatalf("unexpected unique write offsets: %d", len(writes))
	}
	for offset, dataBase64 := range writes {
		got, err := base64.StdEncoding.DecodeString(dataBase64)
		if err != nil {
			t.Fatalf("decode stored write: %v", err)
		}
		want := gatewayDeterministicPayload("00000065", offset, 4<<10)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("payload mismatch at offset=%d", offset)
		}
	}
}

func TestRunGatewayWriteLoadVerifiesFullStripePayloadAboveOneMiBResponse(t *testing.T) {
	writes := map[uint64]string{}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				DataBase64  string `json:"data_base64"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode write request: %v", err)
			}
			writes[req.OffsetBytes] = req.DataBase64
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/read"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode read request: %v", err)
			}
			dataBase64, ok := writes[req.OffsetBytes]
			if !ok {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
					Header:     make(http.Header),
				}, nil
			}
			body = `{"data_base64":` + strconv.Quote(dataBase64) + `}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	const fullStripeBytes = 6 * 128 << 10
	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:     "http://gateway.example.test",
		VolumeID:       101,
		HostID:         "test-host",
		DeviceID:       7,
		SizeRaw:        strconv.Itoa(fullStripeBytes),
		BSRaw:          strconv.Itoa(fullStripeBytes),
		IODepth:        1,
		NumJobs:        1,
		Concurrency:    1,
		Timeout:        5 * time.Second,
		PayloadPattern: "deterministic",
		Verify:         true,
		Attach:         false,
		Detach:         false,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad failed: %v", err)
	}
	if report.VerifyCount != 1 || report.VerifyOKCount != 1 || report.VerifyErrorCount != 0 {
		t.Fatalf("unexpected verify report: %+v", report)
	}
}

func TestRunGatewayWriteLoadReadModePrefillsAndReads(t *testing.T) {
	writes := map[uint64]string{}
	readCount := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/write"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
				DataBase64  string `json:"data_base64"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode write request: %v", err)
			}
			writes[req.OffsetBytes] = req.DataBase64
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/read"):
			var req struct {
				OffsetBytes uint64 `json:"offset_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode read request: %v", err)
			}
			readCount++
			dataBase64, ok := writes[req.OffsetBytes]
			if !ok {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
					Header:     make(http.Header),
				}, nil
			}
			body = `{"data_base64":` + strconv.Quote(dataBase64) + `}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
		GatewayURL:     "http://gateway.example.test",
		VolumeID:       101,
		HostID:         "test-host",
		DeviceID:       7,
		SizeRaw:        "8k",
		BSRaw:          "4k",
		RW:             "read",
		IODepth:        1,
		NumJobs:        1,
		Concurrency:    1,
		Timeout:        5 * time.Second,
		PayloadPattern: "deterministic",
		Prefill:        true,
		Attach:         false,
		Detach:         false,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("runGatewayWriteLoad(read) failed: %v", err)
	}
	if report.RW != "read" || report.PrefillCount != 2 || len(writes) != 2 {
		t.Fatalf("unexpected prefill report: %+v writes=%d", report, len(writes))
	}
	if report.ReadCount != 2 || report.ReadOKCount != 2 || report.WriteCount != 0 || readCount != 2 {
		t.Fatalf("unexpected read/write counts: report=%+v readCount=%d", report, readCount)
	}
	if report.ReadIOPS <= 0 || report.TotalIOPS <= 0 || report.WriteIOPS != 0 {
		t.Fatalf("unexpected throughput metrics: %+v", report)
	}
}

func TestRunFileLoadWritesAndVerifiesDeterministicPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local-load.dat")
	report, err := runFileLoad(fileLoadOptions{
		Path:           path,
		SizeRaw:        "8k",
		BSRaw:          "4k",
		RW:             "write",
		IODepth:        1,
		NumJobs:        1,
		Concurrency:    1,
		PayloadPattern: "deterministic",
		Verify:         true,
		Reset:          true,
	})
	if err != nil {
		t.Fatalf("runFileLoad(write) failed: %v", err)
	}
	if report.Result != "ok" || report.OKCount != 2 || report.WriteOKCount != 2 || report.VerifyOKCount != 2 {
		t.Fatalf("unexpected report: %+v", report)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	want := append(
		gatewayDeterministicPayload("file:"+path, 0, 4<<10),
		gatewayDeterministicPayload("file:"+path, 4<<10, 4<<10)...,
	)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("file payload mismatch")
	}
}

func TestRunFileLoadReadModePrefillsAndReads(t *testing.T) {
	report, err := runFileLoad(fileLoadOptions{
		Path:           filepath.Join(t.TempDir(), "local-read.dat"),
		SizeRaw:        "8k",
		BSRaw:          "4k",
		RW:             "read",
		IODepth:        1,
		NumJobs:        1,
		Concurrency:    1,
		PayloadPattern: "deterministic",
		Prefill:        true,
		Reset:          true,
	})
	if err != nil {
		t.Fatalf("runFileLoad(read) failed: %v", err)
	}
	if report.RW != "read" || report.PrefillCount != 2 || report.ReadOKCount != 2 || report.WriteCount != 0 {
		t.Fatalf("unexpected read report: %+v", report)
	}
	if report.ReadIOPS <= 0 || report.TotalIOPS <= 0 || report.WriteIOPS != 0 {
		t.Fatalf("unexpected throughput metrics: %+v", report)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestInspectVolumeLayoutSummarizesPages(t *testing.T) {
	repo := service.NewInMemoryMetadataRepository([]service.VolumeSpec{
		{
			ID:              service.HexVolumeID(101),
			Name:            "devA",
			Prefix:          "devA",
			SizeBytes:       8 << 20,
			BlockSize:       service.DefaultBlockSize,
			ChunkSizeBytes:  service.DefaultAllocationChunkSize,
			ExtentPageBytes: service.DefaultAllocationPageSize,
		},
	})
	ctx := context.Background()
	_, err := repo.PutExtentPage(ctx, service.AllocationPageRecord{
		VolumeID:       service.HexVolumeID(101),
		PageNo:         0,
		PageBytes:      service.DefaultAllocationPageSize,
		ChunkSizeBytes: service.DefaultAllocationChunkSize,
		Extents: []service.AllocationChunkRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: service.AllocationChunkKindData, PhysicalChunkStart: 1},
			{LogicalChunkStart: 2, ChunkCount: 62, Kind: service.AllocationChunkKindZero},
		},
	}, 0)
	if err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}
	report, err := inspectVolumeLayout(ctx, repo, 101)
	if err != nil {
		t.Fatalf("inspectVolumeLayout failed: %v", err)
	}
	if report.PageCount != 1 || len(report.Pages) != 1 {
		t.Fatalf("unexpected page count: %+v", report)
	}
	if report.ExtentCount != 2 || report.DataExtentCount != 1 || report.ZeroExtentCount != 1 {
		t.Fatalf("unexpected extent summary: %+v", report)
	}
}

func TestValidateExtentsDetectsBrokenPage(t *testing.T) {
	repo := fakeMetadataRepository{
		volume: service.VolumeSpec{
			ID:              service.HexVolumeID(101),
			Name:            "devA",
			Prefix:          "devA",
			SizeBytes:       4 << 20,
			BlockSize:       service.DefaultBlockSize,
			ChunkSizeBytes:  service.DefaultAllocationChunkSize,
			ExtentPageBytes: service.DefaultAllocationPageSize,
		},
		extentPages: []service.AllocationPageRecord{
			{
				VolumeID:       service.HexVolumeID(101),
				PageNo:         0,
				PageBytes:      service.DefaultAllocationPageSize,
				ChunkSizeBytes: service.DefaultAllocationChunkSize,
				Extents: []service.AllocationChunkRecord{
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: service.AllocationChunkKindData, PhysicalChunkStart: 0},
				},
			},
		},
	}
	report, err := validateExtents(context.Background(), repo, 101)
	if err != nil {
		t.Fatalf("validateExtents failed: %v", err)
	}
	if report.OK || report.IssueCount == 0 {
		t.Fatalf("expected invalid extent report: %+v", report)
	}
}

type fakeMetadataRepository struct {
	volume      service.VolumeSpec
	extentPages []service.AllocationPageRecord
}

func (f fakeMetadataRepository) EnsureVolume(context.Context, service.VolumeSpec) error { return nil }
func (f fakeMetadataRepository) CreateVolume(context.Context, service.VolumeCreateRequest) (service.VolumeSpec, error) {
	return f.volume, nil
}
func (f fakeMetadataRepository) UpdateVolume(context.Context, uint64, service.VolumeUpdateRequest) (service.VolumeSpec, error) {
	return f.volume, nil
}
func (f fakeMetadataRepository) DeleteVolume(context.Context, uint64) error { return nil }
func (f fakeMetadataRepository) GetVolume(context.Context, uint64) (service.VolumeSpec, error) {
	return f.volume, nil
}
func (f fakeMetadataRepository) GetVolumeStatus(context.Context, uint64) (service.VolumeStatusRecord, error) {
	return service.VolumeStatusRecord{}, nil
}
func (f fakeMetadataRepository) PutVolumeStatus(context.Context, service.VolumeStatusRecord) error {
	return nil
}
func (f fakeMetadataRepository) ListVolumes(context.Context) ([]service.VolumeSpec, error) {
	return []service.VolumeSpec{f.volume}, nil
}
func (f fakeMetadataRepository) SetVolumeState(context.Context, uint64, service.VolumeLifecycleState) (service.VolumeSpec, error) {
	return f.volume, nil
}
func (f fakeMetadataRepository) GetAttachment(context.Context, uint64) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, nil
}
func (f fakeMetadataRepository) GetGeneration(context.Context, uint64) (uint64, error) {
	return 0, nil
}
func (f fakeMetadataRepository) UnsafeClearAttachment(context.Context, uint64) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, nil
}
func (f fakeMetadataRepository) UnsafeSetGeneration(context.Context, uint64, uint64) (uint64, error) {
	return 0, nil
}
func (f fakeMetadataRepository) Attach(context.Context, service.AttachRequest) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, nil
}
func (f fakeMetadataRepository) Detach(context.Context, service.DetachRequest) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, nil
}
func (f fakeMetadataRepository) GetGateway(context.Context, string) (service.GatewayRecord, error) {
	return service.GatewayRecord{}, nil
}
func (f fakeMetadataRepository) ListGateways(context.Context) ([]service.GatewayRecord, error) {
	return nil, nil
}
func (f fakeMetadataRepository) PutGateway(context.Context, service.GatewayRecord) error { return nil }
func (f fakeMetadataRepository) GetExtentPage(_ context.Context, volumeID, pageNo uint64) (service.AllocationPageRecord, error) {
	for _, rec := range f.extentPages {
		if uint64(rec.VolumeID) == volumeID && rec.PageNo == pageNo {
			return rec, nil
		}
	}
	return service.AllocationPageRecord{VolumeID: service.HexVolumeID(volumeID), PageNo: pageNo}, nil
}
func (f fakeMetadataRepository) ListExtentPages(context.Context, uint64) ([]service.AllocationPageRecord, error) {
	return append([]service.AllocationPageRecord(nil), f.extentPages...), nil
}
func (f fakeMetadataRepository) PutExtentPage(context.Context, service.AllocationPageRecord, int64) (service.AllocationPageRecord, error) {
	if len(f.extentPages) > 0 {
		return f.extentPages[0], nil
	}
	return service.AllocationPageRecord{}, nil
}
func (f fakeMetadataRepository) AllocateChunkIDs(context.Context, uint64, uint32) (uint64, error) {
	return 1, nil
}
func (f fakeMetadataRepository) PutChunkGarbage(context.Context, service.AllocationChunkGarbageRecord) error {
	return nil
}
func (f fakeMetadataRepository) ListChunkGarbage(context.Context, uint64, int) ([]service.AllocationChunkGarbageRecord, error) {
	return nil, nil
}
func (f fakeMetadataRepository) DeleteChunkGarbage(context.Context, uint64, uint64) error { return nil }
