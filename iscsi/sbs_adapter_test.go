package iscsi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gotgtscsi "github.com/gostor/gotgt/pkg/scsi"

	"github.com/nosway/namrbd/gateway/service"
)

func TestSBSBackendAdapterReadWriteFlushClose(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	adapter, summary, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	if summary.BackendMode != SBSBackendMode || summary.BackendAdapter != SBSBackendAdapterName {
		t.Fatalf("unexpected backend summary: %#v", summary)
	}
	if adapter.Size() != 64*1024 {
		t.Fatalf("adapter size=%d, want %d", adapter.Size(), 64*1024)
	}

	payload := bytes.Repeat([]byte("q3-adapter"), 8192/10+1)[:8192]
	n, err := adapter.WriteAt(payload, 4096)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteAt wrote %d, want %d", n, len(payload))
	}
	if len(client.writes) != 2 {
		t.Fatalf("write split count=%d, want 2", len(client.writes))
	}
	for _, write := range client.writes {
		if write.Context.GatewayID != "gw-a" || write.Context.AttachmentID != "att-00a1b2c3-0007" || write.Context.Generation != 7 {
			t.Fatalf("write context did not carry gateway attachment generation: %#v", write.Context)
		}
		if write.Context.IdempotencyKey == "" {
			t.Fatalf("write context missing idempotency key: %#v", write.Context)
		}
	}

	readback := make([]byte, len(payload))
	n, err = adapter.ReadAt(readback, 4096)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("ReadAt read %d, want %d", n, len(payload))
	}
	if !bytes.Equal(payload, readback) {
		t.Fatalf("readback mismatch")
	}
	if _, err := adapter.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	closeSummary, err := adapter.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !closeSummary.CloseRecorded || closeSummary.BytesWritten != uint64(len(payload)) || closeSummary.BytesRead != uint64(len(payload)) || closeSummary.FlushCount != 1 {
		t.Fatalf("unexpected close summary: %#v", closeSummary)
	}
	if len(client.flushes) != 1 || len(client.closes) != 1 {
		t.Fatalf("flush/close counts got flush=%d close=%d", len(client.flushes), len(client.closes))
	}
	operations := adapter.Operations()
	if got, want := operationNames(operations), []string{"open", "write", "read", "flush", "close"}; !equalStrings(got, want) {
		t.Fatalf("operations=%v, want %v", got, want)
	}
	for _, op := range operations {
		if op.Result != "ok" || op.BackendMode != SBSBackendMode || op.BackendAdapter != SBSBackendAdapterName {
			t.Fatalf("unexpected operation record: %#v", op)
		}
	}
	operationsPath := filepath.Join(t.TempDir(), "operations.jsonl")
	if err := WriteSBSAdapterOperationsFile(operationsPath, operations); err != nil {
		t.Fatalf("WriteSBSAdapterOperationsFile: %v", err)
	}
	raw, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatalf("read operations: %v", err)
	}
	if !strings.Contains(string(raw), `"operation":"write"`) || !strings.Contains(string(raw), `"operation":"close"`) {
		t.Fatalf("operations JSONL missing expected rows: %s", raw)
	}
}

func TestSBSBackendAdapterDefaultSessionIDDoesNotReuseIdempotencyNamespaceAcrossOpens(t *testing.T) {
	client := &idempotencyConflictSBSClient{
		SBSClient: testSBSClient(64*1024, 4096, 4096),
		seen:      map[string]service.WriteRequest{},
	}
	cfg := testSBSConfig()
	cfg.SessionID = ""

	if err := writeSBSAdapterPayloadOnce(client, cfg, bytes.Repeat([]byte{0xa1}, 4096)); err != nil {
		t.Fatalf("first adapter write failed: %v", err)
	}
	if err := writeSBSAdapterPayloadOnce(client, cfg, bytes.Repeat([]byte{0xb2}, 4096)); err != nil {
		t.Fatalf("second adapter write reused default idempotency namespace: %v", err)
	}
	if len(client.keys) != 2 || len(client.sessions) != 2 {
		t.Fatalf("writes recorded keys=%v sessions=%v", client.keys, client.sessions)
	}
	if client.keys[0] == client.keys[1] {
		t.Fatalf("default idempotency key reused across adapter opens: %q", client.keys[0])
	}
	if client.sessions[0] == client.sessions[1] {
		t.Fatalf("default session id reused across adapter opens: %q", client.sessions[0])
	}
	for _, session := range client.sessions {
		if !strings.HasPrefix(session, "iscsi-session:fixture:") {
			t.Fatalf("default session id %q does not retain export prefix", session)
		}
	}
}

func TestSBSBackendAdapterExplicitSessionIDPreservesIdempotencyConflict(t *testing.T) {
	client := &idempotencyConflictSBSClient{
		SBSClient: testSBSClient(64*1024, 4096, 4096),
		seen:      map[string]service.WriteRequest{},
	}
	cfg := testSBSConfig()
	cfg.SessionID = "session-static"

	if err := writeSBSAdapterPayloadOnce(client, cfg, bytes.Repeat([]byte{0xa1}, 4096)); err != nil {
		t.Fatalf("first adapter write failed: %v", err)
	}
	err := writeSBSAdapterPayloadOnce(client, cfg, bytes.Repeat([]byte{0xb2}, 4096))
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) || sbsErr.Code != service.SBSErrorCodeIdempotencyConflict {
		t.Fatalf("second explicit-session write error=%v, want idempotency_conflict", err)
	}
	if len(client.keys) != 2 || client.keys[0] != client.keys[1] {
		t.Fatalf("explicit session did not preserve stable key: keys=%v", client.keys)
	}
}

func TestSBSBackendAdapterCapsTransferBelowGRPCEnvelope(t *testing.T) {
	client := newSpySBSClient(testSBSClient(8*1024*1024, 4096, service.DefaultAllocationPageSize))
	adapter, summary, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	if summary.BackendMaxIOSize != service.DefaultAllocationPageSize {
		t.Fatalf("backend max io=%d, want %d", summary.BackendMaxIOSize, service.DefaultAllocationPageSize)
	}
	if summary.BackendEffectiveMaxIOSize != SBSDefaultWireSafeMaxIOSize {
		t.Fatalf("effective max io=%d, want %d", summary.BackendEffectiveMaxIOSize, SBSDefaultWireSafeMaxIOSize)
	}

	payload := bytes.Repeat([]byte("grpc-envelope-safe"), int(service.DefaultAllocationPageSize)/18+1)[:service.DefaultAllocationPageSize]
	n, err := adapter.WriteAt(payload, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteAt wrote %d, want %d", n, len(payload))
	}
	if len(client.writes) != 2 {
		t.Fatalf("write split count=%d, want 2", len(client.writes))
	}
	var total uint64
	for _, write := range client.writes {
		if write.LengthBytes > uint64(SBSDefaultWireSafeMaxIOSize) {
			t.Fatalf("write chunk length=%d exceeds effective max=%d", write.LengthBytes, SBSDefaultWireSafeMaxIOSize)
		}
		total += write.LengthBytes
	}
	if total != uint64(len(payload)) {
		t.Fatalf("write chunk total=%d, want %d", total, len(payload))
	}
	ops := adapter.Operations()
	last := ops[len(ops)-1]
	if last.Operation != "write" || last.ChunkCount != 2 || last.MaxChunkBytes > uint64(SBSDefaultWireSafeMaxIOSize) || last.EffectiveMaxIOSize != SBSDefaultWireSafeMaxIOSize {
		t.Fatalf("unexpected chunked write operation: %#v", last)
	}
}

func TestSBSBackendAdapterBridgesLogical512ToBackend4096(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}

	base := bytes.Repeat([]byte{0x11}, 4096)
	if _, err := adapter.WriteAt(base, 0); err != nil {
		t.Fatalf("aligned WriteAt failed: %v", err)
	}
	partial := bytes.Repeat([]byte{0x5a}, 512)
	if n, err := adapter.WriteAt(partial, 512); err != nil || n != len(partial) {
		t.Fatalf("partial WriteAt got n=%d err=%v, want %d nil", n, err, len(partial))
	}

	readback := make([]byte, 4096)
	if n, err := adapter.ReadAt(readback, 0); err != nil || n != len(readback) {
		t.Fatalf("ReadAt got n=%d err=%v, want %d nil", n, err, len(readback))
	}
	want := append([]byte(nil), base...)
	copy(want[512:1024], partial)
	if !bytes.Equal(readback, want) {
		t.Fatalf("partial write did not preserve neighboring backend-block bytes")
	}

	smallRead := make([]byte, 512)
	if n, err := adapter.ReadAt(smallRead, 512); err != nil || n != len(smallRead) {
		t.Fatalf("partial ReadAt got n=%d err=%v, want %d nil", n, err, len(smallRead))
	}
	if !bytes.Equal(smallRead, partial) {
		t.Fatalf("partial readback mismatch")
	}

	if len(client.reads) < 3 {
		t.Fatalf("expected backend reads for RMW and partial read, got %d", len(client.reads))
	}
	rmwRead := client.reads[0]
	if rmwRead.OffsetBytes != 0 || rmwRead.LengthBytes != 4096 {
		t.Fatalf("partial write RMW read=%#v, want 4KiB backend block at offset 0", rmwRead)
	}
	rmwWrite := client.writes[1]
	if rmwWrite.OffsetBytes != 0 || rmwWrite.LengthBytes != 4096 {
		t.Fatalf("partial write backend write=%#v, want 4KiB backend block at offset 0", rmwWrite)
	}
	summary := adapter.Summary()
	if summary.Result != "ok" || summary.BytesWritten != 4096+512 || summary.BytesRead != 4096+512 {
		t.Fatalf("unexpected bridge summary: %#v", summary)
	}
	operations := adapter.Operations()
	if got, want := operationNames(operations), []string{"open", "write", "write", "read", "read"}; !equalStrings(got, want) {
		t.Fatalf("operations=%v, want %v", got, want)
	}
}

func TestSBSBackendAdapterUnmapMapsToDiscard(t *testing.T) {
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), testSBSClient(64*1024, 4096, 4096), testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	payload := bytes.Repeat([]byte("discard-me"), 4096/10+1)[:4096]
	if _, err := adapter.WriteAt(payload, 4096); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := adapter.Unmap(4096, 4096); err != nil {
		t.Fatalf("Unmap failed: %v", err)
	}
	readback := make([]byte, 4096)
	if _, err := adapter.ReadAt(readback, 4096); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readback, make([]byte, 4096)) {
		t.Fatalf("UNMAP did not zero the SBS fixture range")
	}
	summary := adapter.Summary()
	if summary.UnmapBytes != 4096 || summary.BytesRead != 4096 || summary.BytesWritten != 4096 {
		t.Fatalf("unexpected UNMAP summary: %#v", summary)
	}
}

func TestSBSBackendAdapterZeroMapsAlignedRangeToZero(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	payload := bytes.Repeat([]byte("zero-me"), 4096/7+1)[:4096]
	if _, err := adapter.WriteAt(payload, 4096); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := adapter.Zero(4096, 4096); err != nil {
		t.Fatalf("Zero failed: %v", err)
	}
	if len(client.zeros) != 1 {
		t.Fatalf("zero request count=%d, want 1", len(client.zeros))
	}
	if len(client.writes) != 1 {
		t.Fatalf("write request count=%d, want only initial write", len(client.writes))
	}
	zero := client.zeros[0]
	if zero.OffsetBytes != 4096 || zero.LengthBytes != 4096 {
		t.Fatalf("zero request=%#v, want offset=4096 length=4096", zero)
	}
	if !strings.Contains(zero.Context.IdempotencyKey, ":zero:") {
		t.Fatalf("zero idempotency key does not preserve zero identity: %#v", zero.Context)
	}
	readback := make([]byte, 4096)
	if _, err := adapter.ReadAt(readback, 4096); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readback, make([]byte, 4096)) {
		t.Fatalf("zero did not clear the SBS fixture range")
	}
	summary := adapter.Summary()
	if summary.ZeroBytes != 4096 || summary.BytesWritten != 4096 {
		t.Fatalf("unexpected zero summary: %#v", summary)
	}
	ops := adapter.Operations()
	zeroOp := ops[len(ops)-2]
	if zeroOp.Operation != "zero" || zeroOp.ZeroSemantic == nil || !*zeroOp.ZeroSemantic || zeroOp.PayloadBytes == nil || *zeroOp.PayloadBytes != 0 {
		t.Fatalf("unexpected zero operation row: %#v", zeroOp)
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if !strings.Contains(string(summaryJSON), `"zero_bytes":4096`) {
		t.Fatalf("summary JSON missing zero_bytes: %s", summaryJSON)
	}
	opJSON, err := json.Marshal(zeroOp)
	if err != nil {
		t.Fatalf("marshal zero operation: %v", err)
	}
	if !strings.Contains(string(opJSON), `"zero_semantic":true`) || !strings.Contains(string(opJSON), `"payload_bytes":0`) {
		t.Fatalf("zero operation JSON missing observability fields: %s", opJSON)
	}
	operationsPath := filepath.Join(t.TempDir(), "zero-operations.jsonl")
	if err := WriteSBSAdapterOperationsFile(operationsPath, ops); err != nil {
		t.Fatalf("WriteSBSAdapterOperationsFile: %v", err)
	}
	rawOps, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatalf("read zero operations: %v", err)
	}
	if !strings.Contains(string(rawOps), `"zero_semantic":true`) || !strings.Contains(string(rawOps), `"payload_bytes":0`) {
		t.Fatalf("zero operations JSONL missing observability fields: %s", rawOps)
	}
}

func TestSBSBackendAdapterPartialZeroPreservesBackendBlockNeighbors(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	base := bytes.Repeat([]byte{0x44}, 4096)
	if _, err := adapter.WriteAt(base, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := adapter.Zero(512, 512); err != nil {
		t.Fatalf("partial Zero failed: %v", err)
	}
	if len(client.zeros) != 0 {
		t.Fatalf("partial zero used backend Zero %d times, want RMW write fallback", len(client.zeros))
	}
	if len(client.writes) != 2 {
		t.Fatalf("write request count=%d, want initial write plus RMW fallback", len(client.writes))
	}
	readback := make([]byte, 4096)
	if _, err := adapter.ReadAt(readback, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	want := append([]byte(nil), base...)
	clear(want[512:1024])
	if !bytes.Equal(readback, want) {
		t.Fatalf("partial zero did not preserve neighboring backend-block bytes")
	}
	ops := adapter.Operations()
	zeroOp := ops[len(ops)-2]
	if zeroOp.Operation != "zero" || zeroOp.ZeroSemantic == nil || *zeroOp.ZeroSemantic || zeroOp.PayloadBytes == nil || *zeroOp.PayloadBytes != 4096 {
		t.Fatalf("unexpected partial zero operation row: %#v", zeroOp)
	}
	opJSON, err := json.Marshal(zeroOp)
	if err != nil {
		t.Fatalf("marshal partial zero operation: %v", err)
	}
	if !strings.Contains(string(opJSON), `"zero_semantic":false`) || !strings.Contains(string(opJSON), `"payload_bytes":4096`) {
		t.Fatalf("partial zero operation JSON missing observability fields: %s", opJSON)
	}
	operationsPath := filepath.Join(t.TempDir(), "partial-zero-operations.jsonl")
	if err := WriteSBSAdapterOperationsFile(operationsPath, []SBSAdapterOperationRecord{zeroOp}); err != nil {
		t.Fatalf("WriteSBSAdapterOperationsFile: %v", err)
	}
	rawOps, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatalf("read partial zero operations: %v", err)
	}
	if !strings.Contains(string(rawOps), `"zero_semantic":false`) || !strings.Contains(string(rawOps), `"payload_bytes":4096`) {
		t.Fatalf("partial zero operations JSONL missing observability fields: %s", rawOps)
	}
}

func TestSBSBackendAdapterPartialUnmapPreservesBackendBlockNeighbors(t *testing.T) {
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), testSBSClient(64*1024, 4096, 4096), testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	base := bytes.Repeat([]byte{0x33}, 4096)
	if _, err := adapter.WriteAt(base, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := adapter.Unmap(512, 512); err != nil {
		t.Fatalf("partial Unmap failed: %v", err)
	}
	readback := make([]byte, 4096)
	if _, err := adapter.ReadAt(readback, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	want := append([]byte(nil), base...)
	clear(want[512:1024])
	if !bytes.Equal(readback, want) {
		t.Fatalf("partial UNMAP did not preserve neighboring backend-block bytes")
	}
	summary := adapter.Summary()
	if summary.UnmapBytes != 512 || summary.BytesWritten != 4096 {
		t.Fatalf("unexpected partial UNMAP summary: %#v", summary)
	}
}

func TestSBSBackendAdapterMapsStaleGeneration(t *testing.T) {
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), testSBSClient(64*1024, 4096, 4096), testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	adapter.cfg.Generation = 6
	_, err = adapter.WriteAt(make([]byte, 4096), 4096)
	if err == nil {
		t.Fatalf("WriteAt unexpectedly accepted stale generation")
	}
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) || sbsErr.Code != service.SBSErrorCodeStaleGeneration {
		t.Fatalf("error=%v, want stale generation", err)
	}
	summary := adapter.Summary()
	if !summary.StaleGatewayRejected || summary.SBSErrorCode != string(service.SBSErrorCodeStaleGeneration) || summary.SenseKey != "data_protect" {
		t.Fatalf("unexpected stale summary: %#v", summary)
	}
}

func TestSBSBackendAdapterRejectsStandbyGatewayIOBeforeBackendWrite(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	cfg := testSBSConfig()
	cfg.ISCSIGatewayID = "gw-b"
	cfg.ActiveISCSIGatewayID = "gw-a"
	adapter, summary, err := OpenSBSBackendAdapter(context.Background(), client, cfg)
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	if summary.ISCSIGatewayID != "gw-b" || summary.ActiveISCSIGatewayID != "gw-a" {
		t.Fatalf("summary did not preserve local/active gateway split: %#v", summary)
	}
	_, err = adapter.WriteAt(make([]byte, 4096), 4096)
	if err == nil {
		t.Fatalf("WriteAt unexpectedly accepted standby gateway")
	}
	var cond *SCSIConditionError
	if !errors.As(err, &cond) || cond.SenseKey != "data_protect" || !cond.StandbyWriteRejected {
		t.Fatalf("error=%v, want standby data_protect condition", err)
	}
	key, asc, ok := cond.GotgtSense()
	if !ok || key != gotgtscsi.DATA_PROTECT || asc != gotgtscsi.ASC_WRITE_PROTECT {
		t.Fatalf("GotgtSense()=(%#x,%#x,%t), want DATA_PROTECT/ASC_WRITE_PROTECT", key, asc, ok)
	}
	if len(client.writes) != 0 {
		t.Fatalf("standby write reached backend: %#v", client.writes)
	}
	summary = adapter.Summary()
	if summary.Result != "error" || !summary.StandbyWriteRejected || summary.StaleGatewayRejected || summary.SenseKey != "data_protect" {
		t.Fatalf("unexpected standby rejection summary: %#v", summary)
	}
	if summary.ActivePathIOAllowed || summary.ActivePathWriteAllowed || summary.StandbyPathWriteAllowed {
		t.Fatalf("standby summary advertised write authority: %#v", summary)
	}
	ops := adapter.Operations()
	if got, want := operationNames(ops), []string{"profile", "write"}; !equalStrings(got, want) {
		t.Fatalf("operations=%v, want %v", got, want)
	}
	last := ops[len(ops)-1]
	if last.Result != "error" || last.ISCSIGatewayID != "gw-b" || last.ActiveISCSIGatewayID != "gw-a" || !last.StandbyWriteRejected || last.SenseKey != "data_protect" {
		t.Fatalf("unexpected standby operation row: %#v", last)
	}
}

func TestSBSBackendAdapterStandbyProfileOpenDoesNotStealActiveWriter(t *testing.T) {
	client := newSpySBSClient(testSBSClient(64*1024, 4096, 4096))
	activeCfg := testSBSConfig()
	active, _, err := OpenSBSBackendAdapter(context.Background(), client, activeCfg)
	if err != nil {
		t.Fatalf("open active adapter: %v", err)
	}
	standbyCfg := testSBSConfig()
	standbyCfg.ISCSIGatewayID = "gw-b"
	standbyCfg.ActiveISCSIGatewayID = activeCfg.ActiveISCSIGatewayID
	standby, standbySummary, err := OpenSBSBackendAdapter(context.Background(), client, standbyCfg)
	if err != nil {
		t.Fatalf("open standby adapter: %v", err)
	}
	if standbySummary.BackendVolumeHandle != "" || standbySummary.ActivePathIOAllowed {
		t.Fatalf("standby adapter should expose profile without writer handle: %#v", standbySummary)
	}
	if got, want := operationNames(standby.Operations()), []string{"profile"}; !equalStrings(got, want) {
		t.Fatalf("standby operations=%v, want %v", got, want)
	}
	if _, err := active.WriteAt(make([]byte, 4096), 4096); err != nil {
		t.Fatalf("active write failed after standby profile open: %v", err)
	}
	if len(client.writes) != 1 {
		t.Fatalf("active write count=%d, want 1", len(client.writes))
	}
	if _, err := standby.Close(context.Background()); err != nil {
		t.Fatalf("standby close failed: %v", err)
	}
	if _, err := active.Close(context.Background()); err != nil {
		t.Fatalf("active close failed: %v", err)
	}
}

func TestSBSBackendAdapterMapsSecurityRejectedRead(t *testing.T) {
	client := &securityRejectSBSClient{
		SBSClient: testSBSClient(64*1024, 4096, 4096),
		operation: "read",
	}
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), client, testSBSConfig())
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	payload := bytes.Repeat([]byte("protected"), 4096/9+1)[:4096]
	if _, err := adapter.WriteAt(payload, 4096); err != nil {
		t.Fatalf("WriteAt failed before security reject read: %v", err)
	}
	_, err = adapter.ReadAt(make([]byte, len(payload)), 4096)
	if err == nil {
		t.Fatalf("ReadAt unexpectedly accepted security rejected read")
	}
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) || sbsErr.Code != service.SBSErrorCodeSecurityRejected {
		t.Fatalf("error=%v, want security_rejected", err)
	}
	summary := adapter.Summary()
	if !summary.SecurityRejected || summary.StaleGatewayRejected || summary.SBSErrorCode != string(service.SBSErrorCodeSecurityRejected) || summary.SenseKey != "data_protect" {
		t.Fatalf("unexpected security rejected summary: %#v", summary)
	}
	operations := adapter.Operations()
	if got, want := operationNames(operations), []string{"open", "write", "read"}; !equalStrings(got, want) {
		t.Fatalf("operations=%v, want %v", got, want)
	}
	last := operations[len(operations)-1]
	if last.Result != "error" || !last.SecurityRejected || last.SenseKey != "data_protect" || last.SBSErrorCode != string(service.SBSErrorCodeSecurityRejected) {
		t.Fatalf("unexpected security rejected operation: %#v", last)
	}
}

func TestRunSBSAdapterSelfTestWritesSecurityRejectArtifacts(t *testing.T) {
	dir := t.TempDir()
	summaryPath := filepath.Join(dir, "summary.json")
	operationsPath := filepath.Join(dir, "operations.jsonl")
	summary, err := RunSBSAdapterSelfTest(SBSAdapterSelfTestOptions{
		SizeBytes:               64 * 1024,
		SecurityRejectOperation: "write",
		SummaryJSONPath:         summaryPath,
		OperationJSONLPath:      operationsPath,
	})
	if err == nil {
		t.Fatalf("RunSBSAdapterSelfTest unexpectedly accepted security rejected write")
	}
	if summary.Result != "error" || !summary.SecurityRejected || summary.BytesWritten != 0 || summary.SenseKey != "data_protect" {
		t.Fatalf("unexpected security reject summary: %#v", summary)
	}
	rawSummary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if !strings.Contains(string(rawSummary), `"security_rejected": true`) && !strings.Contains(string(rawSummary), `"security_rejected":true`) {
		t.Fatalf("summary did not record security_rejected: %s", rawSummary)
	}
	rawOps, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatalf("read operations: %v", err)
	}
	if !strings.Contains(string(rawOps), `"operation":"write"`) || !strings.Contains(string(rawOps), `"security_rejected":true`) {
		t.Fatalf("operations JSONL missing security rejected write row: %s", rawOps)
	}
}

func TestSBSBackendAdapterRoundsAdvertisedCapacityDown(t *testing.T) {
	cfg := testSBSConfig()
	cfg.LogicalBlockSize = 512
	_, summary, err := OpenSBSBackendAdapter(context.Background(), testSBSClient(4097, 1, 512), cfg)
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter failed: %v", err)
	}
	if summary.AdvertisedLUNBytes != 4096 || summary.BackendAlignmentResult != SBSAlignmentRoundedDown {
		t.Fatalf("unexpected advertised capacity summary: %#v", summary)
	}

	_, summary, err = OpenSBSBackendAdapter(context.Background(), testSBSClient(8191, 4096, 4096), cfg)
	if err != nil {
		t.Fatalf("OpenSBSBackendAdapter with 4KiB backend failed: %v", err)
	}
	if summary.AdvertisedLUNBytes != 4096 || summary.BackendAlignmentResult != SBSAlignmentRoundedDown {
		t.Fatalf("unexpected backend-rounded capacity summary: %#v", summary)
	}
}

type spySBSClient struct {
	service.SBSClient
	writes  []service.WriteRequest
	reads   []service.ReadRequest
	flushes []service.FlushRequest
	zeros   []service.ZeroRequest
	closes  []service.CloseVolumeRequest
}

func newSpySBSClient(next service.SBSClient) *spySBSClient {
	return &spySBSClient{SBSClient: next}
}

func (c *spySBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	cp := *req
	cp.Data = append([]byte(nil), req.Data...)
	c.writes = append(c.writes, cp)
	return c.SBSClient.Write(ctx, req)
}

func (c *spySBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	cp := *req
	c.reads = append(c.reads, cp)
	return c.SBSClient.Read(ctx, req)
}

func (c *spySBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	cp := *req
	c.flushes = append(c.flushes, cp)
	return c.SBSClient.Flush(ctx, req)
}

func (c *spySBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	cp := *req
	c.zeros = append(c.zeros, cp)
	return c.SBSClient.Zero(ctx, req)
}

func (c *spySBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	cp := *req
	c.closes = append(c.closes, cp)
	return c.SBSClient.CloseVolume(ctx, req)
}

type idempotencyConflictSBSClient struct {
	service.SBSClient
	seen     map[string]service.WriteRequest
	keys     []string
	sessions []string
}

func (c *idempotencyConflictSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	if req == nil {
		return nil, &service.SBSError{Code: service.SBSErrorCodeBadRequest, Message: "nil request"}
	}
	key := req.Context.IdempotencyKey
	c.keys = append(c.keys, key)
	c.sessions = append(c.sessions, req.Context.SessionID)
	if previous, ok := c.seen[key]; ok {
		if previous.VolumeID != req.VolumeID ||
			previous.OffsetBytes != req.OffsetBytes ||
			previous.LengthBytes != req.LengthBytes ||
			!bytes.Equal(previous.Data, req.Data) {
			return nil, &service.SBSError{
				Code:    service.SBSErrorCodeIdempotencyConflict,
				Message: "same idempotency key used with different request body",
			}
		}
	} else {
		cp := *req
		cp.Data = append([]byte(nil), req.Data...)
		c.seen[key] = cp
	}
	return c.SBSClient.Write(ctx, req)
}

func writeSBSAdapterPayloadOnce(client service.SBSClient, cfg SBSAdapterConfig, payload []byte) error {
	adapter, _, err := OpenSBSBackendAdapter(context.Background(), client, cfg)
	if err != nil {
		return err
	}
	if _, err := adapter.WriteAt(payload, 0); err != nil {
		return err
	}
	_, err = adapter.Close(context.Background())
	return err
}

func testSBSClient(sizeBytes uint64, blockSize uint32, maxIOSize uint32) service.SBSClient {
	return service.NewInMemorySBSClient([]service.VolumeSpec{{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "phase-q-sbs-adapter",
		Prefix:          "phase-q",
		SizeBytes:       sizeBytes,
		BlockSize:       blockSize,
		ChunkSizeBytes:  maxIOSize,
		ExtentPageBytes: maxIOSize,
		AccessMode:      service.VolumeAccessModeExclusive,
		State:           service.VolumeStateAvailable,
	}})
}

func testSBSConfig() SBSAdapterConfig {
	return SBSAdapterConfig{
		ExportID:             "fixture",
		VolumeID:             "00a1b2c3",
		TargetIQN:            "iqn.2026-06.io.namrbd:iscsi.fixture",
		LUNID:                0,
		LUNWWN:               LUNWWN("fixture"),
		ISCSIGatewayID:       "gw-a",
		ActiveISCSIGatewayID: "gw-a",
		ExportLeaseID:        "lease-fixture",
		ExportEpoch:          1,
		AttachmentID:         "att-00a1b2c3-0007",
		Generation:           7,
		SBSHostID:            "iscsi-export:fixture",
		SBSDeviceID:          StableSCSIDeviceID(LUNWWN("fixture")),
		SessionID:            "session-fixture",
		LogicalBlockSize:     DefaultLogicalBlock,
	}
}

func operationNames(operations []SBSAdapterOperationRecord) []string {
	out := make([]string, 0, len(operations))
	for _, op := range operations {
		out = append(out, op.Operation)
	}
	return out
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
