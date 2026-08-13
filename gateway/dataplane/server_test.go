package dataplane

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/auth"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/protocol/wirev1"
	"github.com/nosway/namrbd/protocol/wirev2"
)

func newTestServer() *Server {
	mem := store.NewMemoryStore()
	svc := service.New(mem, []store.Volume{
		{ID: 101, Prefix: "devA", SizeBytes: uint64(service.DefaultAllocationPageSize)},
	})
	_ = svc.Write(context.Background(), 101, 0, 4096, make([]byte, 4096))
	return New(svc, Config{
		PathID:              1,
		GatewayID:           "gw-a",
		MaxIOSize:           128 * 1024,
		MaxSegments:         32,
		MaxInflightRequests: 64,
		MaxInflightBytes:    4 * 1024 * 1024,
	})
}

func TestNewUsesWireSafeDefaultMaxIOSize(t *testing.T) {
	s := New(nil, Config{})
	if s.cfg.MaxIOSize != DefaultMaxIOSize {
		t.Fatalf("default max io size=%d, want %d", s.cfg.MaxIOSize, DefaultMaxIOSize)
	}
	if s.cfg.MaxZeroLikeIOSize != DefaultMaxZeroLikeIOSize {
		t.Fatalf("default zero-like max io size=%d, want %d", s.cfg.MaxZeroLikeIOSize, DefaultMaxZeroLikeIOSize)
	}
}

func TestHandleRead(t *testing.T) {
	s := newTestServer()
	h := wirev1.NewRequestHeader(wirev1.OpRead, 1, 101, 1, 0, 0)
	h.LengthBytes = 4096
	resp, payload := s.handleRequest(h, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if len(payload) != 4096 {
		t.Fatalf("unexpected payload length: %d", len(payload))
	}
}

func TestHandleWriteAndProbe(t *testing.T) {
	s := newTestServer()
	data := make([]byte, 4096)
	data[0] = 0xAB
	payload := wirev1.EncodeWritePayload(wirev1.WriteTag{}, data)
	h := wirev1.NewRequestHeader(wirev1.OpWrite, 2, 101, 1, 0, 0)
	h.LengthBytes = uint32(len(payload))
	resp, _ := s.handleRequest(h, payload)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected write status: %d", resp.StatusCode)
	}

	readHdr := wirev1.NewRequestHeader(wirev1.OpRead, 3, 101, 1, 0, 0)
	readHdr.LengthBytes = 4096
	resp, out := s.handleRequest(readHdr, nil)
	if resp.StatusCode != wirev1.StatusOK || out[0] != 0xAB {
		t.Fatalf("unexpected read-after-write status=%d first=%x", resp.StatusCode, out[0])
	}

	probeHdr := wirev1.NewRequestHeader(wirev1.OpPathProbe, 4, 101, 1, 0, 0)
	resp, out = s.handleRequest(probeHdr, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected probe status: %d", resp.StatusCode)
	}
	cap, err := ParsePathCapability(out)
	if err != nil {
		t.Fatalf("ParsePathCapability failed: %v", err)
	}
	if cap.MaxInflightRequests != 64 || cap.MaxIOSize != 128*1024 {
		t.Fatalf("unexpected capability: %+v", cap)
	}
	if cap.MaxZeroLikeIOSize != DefaultMaxZeroLikeIOSize {
		t.Fatalf("unexpected zero-like capability: %+v", cap)
	}
}

func TestHandleRequestSeparatesDataAndZeroLikeLimits(t *testing.T) {
	s := newTestServer()

	readHdr := wirev1.NewRequestHeader(wirev1.OpRead, 11, 101, 1, 0, 0)
	readHdr.LengthBytes = s.cfg.MaxIOSize + 4096
	resp, _ := s.handleRequest(readHdr, nil)
	if resp.StatusCode != wirev1.ErrInvalidRange {
		t.Fatalf("oversize read status=%d want invalid range", resp.StatusCode)
	}

	discardHdr := wirev1.NewRequestHeader(wirev1.OpDiscard, 12, 101, 1, 0, 0)
	discardHdr.LengthBytes = s.cfg.MaxIOSize + 4096
	resp, payload := s.handleRequest(discardHdr, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("zero-like range within zero-like cap status=%d", resp.StatusCode)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty discard payload")
	}

	tooLargeDiscard := wirev1.NewRequestHeader(wirev1.OpDiscard, 13, 101, 1, 0, 0)
	tooLargeDiscard.LengthBytes = s.cfg.MaxZeroLikeIOSize + 4096
	resp, _ = s.handleRequest(tooLargeDiscard, nil)
	if resp.StatusCode != wirev1.ErrInvalidRange {
		t.Fatalf("oversize discard status=%d want invalid range", resp.StatusCode)
	}
}

func TestHandleWriteEmitsCompletedTraceWhenEnabled(t *testing.T) {
	s := newTestServer()
	s.cfg.TraceCompletedRequests = true
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	data := make([]byte, 4096)
	data[0] = 0xCD
	payload := wirev1.EncodeWritePayload(wirev1.WriteTag{}, data)
	h := wirev1.NewRequestHeader(wirev1.OpWrite, 22, 101, 1, 4096, 0)
	h.LengthBytes = uint32(len(payload))
	resp, _ := s.handleRequest(h, payload)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected write status: %d", resp.StatusCode)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("expected one structured trace line, got %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("decode trace: %v line=%s", err, lines[0])
	}
	if rec["component"] != "gateway.dataplane" || rec["event"] != "dataplane_request_completed" {
		t.Fatalf("unexpected trace identity: %+v", rec)
	}
	if rec["op"] != "write" || rec["status_name"] != "ok" {
		t.Fatalf("unexpected trace op/status: %+v", rec)
	}
	if got := uint64(rec["offset_bytes"].(float64)); got != 4096 {
		t.Fatalf("offset_bytes=%d want 4096", got)
	}
	if got := uint64(rec["length_bytes"].(float64)); got != 4096 {
		t.Fatalf("length_bytes=%d want logical write size 4096", got)
	}
	if got := uint64(rec["path_id"].(float64)); got != 1 {
		t.Fatalf("path_id=%d want 1", got)
	}
	if rec["gateway_id"] != "gw-a" {
		t.Fatalf("gateway_id=%v want gw-a", rec["gateway_id"])
	}
}

func TestHandleFlush(t *testing.T) {
	s := newTestServer()
	h := wirev1.NewRequestHeader(wirev1.OpFlush, 5, 101, 1, 0, 0)
	resp, payload := s.handleRequest(h, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected flush status: %d", resp.StatusCode)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty flush payload")
	}
}

func TestHandleDiscardAndWriteZeroesRemainDistinct(t *testing.T) {
	s := newTestServer()

	discard := wirev1.NewRequestHeader(wirev1.OpDiscard, 6, 101, 1, 0, 0)
	discard.LengthBytes = uint32(service.DefaultAllocationChunkSize)
	resp, payload := s.handleRequest(discard, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected discard status: %d", resp.StatusCode)
	}
	if resp.Base.Op != wirev1.OpDiscardResp {
		t.Fatalf("discard response op=%x want %x", resp.Base.Op, wirev1.OpDiscardResp)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty discard payload")
	}

	writeZeroes := wirev1.NewRequestHeader(wirev1.OpWriteZeroes, 7, 101, 1, 0, 0)
	writeZeroes.LengthBytes = 4096
	resp, payload = s.handleRequest(writeZeroes, nil)
	if resp.StatusCode != wirev1.StatusOK {
		t.Fatalf("unexpected write-zeroes status: %d", resp.StatusCode)
	}
	if resp.Base.Op != wirev1.OpWriteZeroesResp {
		t.Fatalf("write-zeroes response op=%x want %x", resp.Base.Op, wirev1.OpWriteZeroesResp)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty write-zeroes payload")
	}

	metrics := s.svc.MetricsSnapshot()
	if metrics.ByOperation["discard"].Count != 1 {
		t.Fatalf("discard metrics=%+v want one discard", metrics.ByOperation["discard"])
	}
	if metrics.ByOperation["zero"].Count != 1 {
		t.Fatalf("zero metrics=%+v want one write-zeroes-backed zero", metrics.ByOperation["zero"])
	}
	if metrics.IOIdentity == nil || metrics.IOIdentity.ByDiscardPolicy[service.DiscardPolicyTrueReclaim] != 1 {
		t.Fatalf("unexpected discard identity metrics: %+v", metrics.IOIdentity)
	}
	if metrics.IOIdentity.LastObservation == nil || metrics.IOIdentity.LastObservation.Operation != service.IOOperationZero {
		t.Fatalf("write-zeroes should leave zero as last observation: %+v", metrics.IOIdentity.LastObservation)
	}
}

func TestSupportedOpsMaskIncludesDiscardAndWriteZeroes(t *testing.T) {
	mask := supportedOpsMask()
	for _, op := range []uint32{wirev1.OpRead, wirev1.OpWrite, wirev1.OpFlush, wirev1.OpDiscard, wirev1.OpWriteZeroes} {
		if mask&(1<<op) == 0 {
			t.Fatalf("supportedOpsMask missing op %d: mask=0x%x", op, mask)
		}
	}
}

func TestMapSBSError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int32
	}{
		{
			name: "not found",
			err:  &service.SBSError{Code: service.SBSErrorCodeNotFound},
			want: wirev1.ErrNoSuchVolume,
		},
		{
			name: "stale generation",
			err:  &service.SBSError{Code: service.SBSErrorCodeStaleGeneration},
			want: wirev1.ErrGenerationMismatch,
		},
		{
			name: "retryable unavailable",
			err:  &service.SBSError{Code: service.SBSErrorCodeUnavailable, Retryable: true},
			want: wirev1.ErrRetryable,
		},
		{
			name: "unavailable",
			err:  &service.SBSError{Code: service.SBSErrorCodeUnavailable},
			want: wirev1.ErrNoHealthyReplica,
		},
		{
			name: "timeout",
			err:  &service.SBSError{Code: service.SBSErrorCodeTimeout},
			want: wirev1.ErrTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapError(tt.err); got != tt.want {
				t.Fatalf("mapError=%d want=%d", got, tt.want)
			}
			if got := mapErrorV2(tt.err); got != tt.want {
				t.Fatalf("mapErrorV2=%d want=%d", got, tt.want)
			}
		})
	}
}

func TestValidateHelloSessionRejectsMismatchedClaims(t *testing.T) {
	s := newTestServer()
	if _, err := s.svc.Attach(101, "host-a", 7); err != nil {
		t.Fatalf("attach: %v", err)
	}
	st, err := s.svc.VolumeState(101)
	if err != nil {
		t.Fatalf("volume state: %v", err)
	}
	verified := &auth.VerifiedToken{
		Claims: auth.DataplaneTokenClaims{
			VolumeID:       101,
			AttachmentID:   st.AttachmentID,
			DeviceID:       7,
			HostID:         "host-a",
			GatewayID:      "gw-a",
			Generation:     st.Generation,
			AllowedPathIDs: []uint32{1},
		},
		ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	hello := &wirev2.HelloPayload{
		Token:           "token",
		ClientNonce:     "nonce",
		DeviceID:        9,
		HostID:          "host-a",
		SupportedAuth:   []string{auth.AuthModeTokenHMACV1},
		RequestedPathID: 1,
	}
	if err := s.validateHelloSession(hello, verified, st); err == nil {
		t.Fatal("expected hello validation to reject mismatched device_id")
	}
}

func TestSessionStillCurrentDetectsGenerationMismatch(t *testing.T) {
	s := newTestServer()
	st, err := s.svc.Attach(101, "host-a", 7)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	sess := &Session{
		ID:           1,
		VolumeID:     101,
		AttachmentID: st.AttachmentID,
		Generation:   st.Generation,
		HostID:       "host-a",
		DeviceID:     7,
		ExpiresAt:    time.Now().UTC().Add(time.Minute),
	}
	if _, err := s.svc.Detach(101, "host-a", st.AttachmentID); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if code, ok := s.sessionStillCurrent(sess); ok || code != wirev2.ErrGenerationMatch {
		t.Fatalf("expected generation mismatch, got ok=%v code=%d", ok, code)
	}
}
